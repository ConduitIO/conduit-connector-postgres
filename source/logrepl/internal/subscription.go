// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pgOutputPlugin          = "pgoutput"
	closeReplicationTimeout = time.Second * 2
)

// Subscription manages a subscription to a logical replication slot.
type Subscription struct {
	SlotName      string
	Publication   string
	Tables        []string
	StartLSN      pglogrepl.LSN
	Handler       Handler
	StatusTimeout time.Duration
	FlushInterval time.Duration
	TXSnapshotID  string

	conn *pgxpool.Conn
	pool *pgxpool.Pool

	stop context.CancelFunc

	ready   chan struct{}
	done    chan struct{}
	doneErr error

	walWritten   pglogrepl.LSN
	walFlushed   pglogrepl.LSN
	serverWALEnd pglogrepl.LSN
	SDKBatchSize int
}

type Handler interface {
	Handle(context.Context, pglogrepl.Message, pglogrepl.LSN) (pglogrepl.LSN, error)
	Flush(ctx context.Context) error
}

// CreateSubscription initializes the logical replication subscriber by creating the replication slot.
func CreateSubscription(
	ctx context.Context,
	pool *pgxpool.Pool,
	slotName,
	publication string,
	tables []string,
	startLSN pglogrepl.LSN,
	h Handler,
) (*Subscription, error) {
	var err error

	// Request a replication connection
	conn, err := pool.Acquire(cpool.WithReplication(ctx))
	if err != nil {
		return nil, fmt.Errorf("could not establish replication connection: %w", err)
	}
	defer func() { // release connection on error
		if err != nil {
			conn.Release()
		}
	}()

	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn.Conn().PgConn(),
		slotName,
		pgOutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			SnapshotAction: "EXPORT_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		// If creating the replication slot fails with code 42710, this means
		// the replication slot already exists.
		if !IsPgDuplicateErr(err) {
			return nil, err
		}

		sdk.Logger(ctx).Warn().
			Msgf("replication slot %q already exists", slotName)
	}

	slotInfo, err := ReadReplicationSlot(ctx, pool, slotName)
	if err != nil {
		return nil, err
	}

	// Reset positional data, start LSN is not valid.
	if startLSN == 0 {
		startLSN = slotInfo.RestartLSN
	}

	// Reset start LSN to the last known available WAL location from this slot.
	if startLSN < slotInfo.RestartLSN {
		sdk.Logger(ctx).Warn().
			Stringer("start_lsn", startLSN).
			Stringer("restart_lsn", slotInfo.RestartLSN).
			Stringer("confirmed_flush_lsn", slotInfo.ConfirmedFlushLSN).
			Msgf("restart LSN is earlier than available WAL, resetting to last restart point")

		startLSN = slotInfo.RestartLSN
	}

	return &Subscription{
		SlotName:      slotName,
		Publication:   publication,
		Tables:        tables,
		StartLSN:      startLSN,
		Handler:       h,
		StatusTimeout: 10 * time.Second,
		FlushInterval: time.Second,
		TXSnapshotID:  result.SnapshotName,

		conn: conn,
		pool: pool,

		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}, nil
}

// Run logical replication listener and block until error or ctx is canceled.
func (s *Subscription) Run(ctx context.Context) error {
	defer s.doneReplication()

	if err := s.startReplication(ctx); err != nil {
		close(s.ready) // ready to fail.
		return err
	}

	lctx, cancel := context.WithCancel(ctx)
	s.stop = cancel
	s.walWritten = s.StartLSN
	s.walFlushed = s.StartLSN

	if err := s.listen(lctx); err != nil {
		s.doneErr = err
		return err
	}

	return nil
}

// listen receives changes from the replication slot until context is cancelled or an error is encountered.
func (s *Subscription) listen(ctx context.Context) error {
	// signal that the subscription is ready and is receiving messages
	close(s.ready)
	nextStatusUpdateAt := time.Now().Add(s.StatusTimeout)
	nextFlush := time.Now().Add(s.FlushInterval)

	for {
		if time.Now().After(nextStatusUpdateAt) {
			err := s.sendStandbyStatusUpdate(ctx)
			if err != nil {
				return err
			}
			nextStatusUpdateAt = time.Now().Add(s.StatusTimeout)
		}

		if time.Now().After(nextFlush) {
			err := s.Handler.Flush(ctx)
			if err != nil {
				return fmt.Errorf("handler failed flushing messages: %v", err)
			}
			nextFlush = time.Now().Add(s.FlushInterval)
		}

		msg, err := s.receiveMessage(ctx, nextStatusUpdateAt)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				sdk.Logger(ctx).Trace().Msg("deadline exceeded while receiving message")
				continue
			}
			return err
		}

		if msg == nil {
			return fmt.Errorf("replication failed: nil message received, should not happen")
		}

		copyDataMsg, ok := msg.(*pgproto3.CopyData)
		if !ok {
			return fmt.Errorf("unexpected message type %T, value: %v", msg, msg)
		}

		switch copyDataMsg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			if err := s.handlePrimaryKeepaliveMessage(ctx, copyDataMsg); err != nil {
				return err
			}
		case pglogrepl.XLogDataByteID:
			if err := s.handleXLogData(ctx, copyDataMsg); err != nil {
				return err
			}
		default:
			sdk.Logger(ctx).Trace().
				Bytes("message", copyDataMsg.Data).
				Msg("ignoring unknown copy data message")
		}
	}
}

// handlePrimaryKeepaliveMessage will handle the primary keepalive message and
// send a reply if requested.
func (s *Subscription) handlePrimaryKeepaliveMessage(ctx context.Context, copyDataMsg *pgproto3.CopyData) error {
	sdk.Logger(ctx).Trace().Msg("handling primary keepalive message")

	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyDataMsg.Data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}

	atomic.StoreUint64((*uint64)(&s.serverWALEnd), uint64(pkm.ServerWALEnd))

	if pkm.ReplyRequested {
		if err := s.sendStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("failed to send status: %w", err)
		}
	}

	return nil
}

// handleXLogData will parse the logical replication message and forward it to
// the handler.
func (s *Subscription) handleXLogData(ctx context.Context, copyDataMsg *pgproto3.CopyData) error {
	xld, err := pglogrepl.ParseXLogData(copyDataMsg.Data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse xlog data: %w", err)
	}

	if xld.WALStart > 0 && xld.WALStart <= s.StartLSN {
		// skip stuff that's in the past
		return nil
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("invalid message: %w", err)
	}

	writtenLSN, err := s.Handler.Handle(ctx, logicalMsg, xld.WALStart)
	if err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	if writtenLSN > 0 {
		s.walWritten = writtenLSN
	}

	return nil
}

// Ack stores the LSN as flushed. Next time WAL positions are flushed, Postgres
// will know it can purge WAL logs up to this LSN.
func (s *Subscription) Ack(lsn pglogrepl.LSN) {
	// store with atomic to prevent race conditions with sending status update
	atomic.StoreUint64((*uint64)(&s.walFlushed), uint64(lsn))
}

// Stop signals to the subscription it should stop. Call Wait to block until the
// subscription actually stops running.
func (s *Subscription) Stop() {
	if s.stop != nil {
		s.stop()
	}
}

// Wait will block until the subscription is stopped. If the context gets
// cancelled in the meantime it will return the context error, otherwise nil is
// returned.
func (s *Subscription) Wait(ctx context.Context, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
	case <-s.done:
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return nil
	}
}

func (s *Subscription) Teardown(ctx context.Context) error {
	defer func() {
		if s.conn != nil {
			s.conn.Release()
		}
	}()

	s.Stop()

	select {
	case <-s.ready:
		return s.Wait(ctx, closeReplicationTimeout)
	default:
		return nil
	}
}

// Ready returns a channel that is closed when the subscription is ready and
// receiving messages.
func (s *Subscription) Ready() <-chan struct{} {
	return s.ready
}

// Done returns a channel that is closed when the subscription is done.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

// Err returns an error that might have happened when the subscription stopped
// running.
func (s *Subscription) Err() error {
	return s.doneErr
}

// startReplication starts replication with a specific start LSN.
func (s *Subscription) startReplication(ctx context.Context) error {
	// N.B. Snapshots may take long time and connection may timeout.
	// 		Safer to refresh the connection before replication begins.

	s.conn.Release()

	conn, err := s.pool.Acquire(cpool.WithReplication(ctx))
	if err != nil {
		return fmt.Errorf("could not establish replication connection: %w", err)
	}

	s.conn = conn

	pluginArgs := []string{
		`"proto_version" '1'`,
		fmt.Sprintf(`"publication_names" '%s'`, s.Publication),
	}

	if err := pglogrepl.StartReplication(
		ctx,
		s.conn.Conn().PgConn(),
		s.SlotName,
		s.StartLSN,
		pglogrepl.StartReplicationOptions{
			Timeline:   0,
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pluginArgs,
		},
	); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	return nil
}

// sendStandbyCopyDone sends the status message to server indicating that
// replication is done.
func (s *Subscription) sendStandbyCopyDone(ctx context.Context) error {
	sdk.Logger(ctx).Trace().Msg("sending standby copy done message")
	_, err := pglogrepl.SendStandbyCopyDone(ctx, s.conn.Conn().PgConn())
	if err != nil {
		return fmt.Errorf("failed to send standby copy done: %w", err)
	}
	return nil
}

// sendStandbyStatusUpdate sends the status message to server indicating which LSNs
// have been processed.
func (s *Subscription) sendStandbyStatusUpdate(ctx context.Context) error {
	// load with atomic to prevent race condition with ack
	walFlushed := pglogrepl.LSN(atomic.LoadUint64((*uint64)(&s.walFlushed)))

	if walFlushed > s.walWritten {
		return fmt.Errorf("walWrite (%s) should be >= walFlush (%s)", s.walWritten, walFlushed)
	}

	// N.B. Manage replication slot lag, by responding with the last server LSN, when
	//      all previous slot relevant msgs have been written and flushed
	replyWithWALEnd := walFlushed == s.walWritten && walFlushed < s.serverWALEnd

	sdk.Logger(ctx).Trace().
		Stringer("wal_write", s.walWritten).
		Stringer("wal_flush", walFlushed).
		Stringer("server_wal_end", s.serverWALEnd).
		Bool("server_wal_end_sent", replyWithWALEnd).
		Msg("sending standby status update")

	if replyWithWALEnd {
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn.Conn().PgConn(), pglogrepl.StandbyStatusUpdate{
			WALWritePosition: s.serverWALEnd,
		}); err != nil {
			return fmt.Errorf("failed to send standby status update with server end lsn: %w", err)
		}

		return nil
	}

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn.Conn().PgConn(), pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.walWritten,
		WALFlushPosition: walFlushed,
		WALApplyPosition: walFlushed,
		ReplyRequested:   false,
	}); err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	return nil
}

// receiveMessage tries to receive a message from the replication stream. If the
// deadline is reached before a message is received it returns
// context.DeadlineExceeded.
func (s *Subscription) receiveMessage(ctx context.Context, deadline time.Time) (pgproto3.BackendMessage, error) {
	wctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	sdk.Logger(ctx).Trace().Msg("receiving message")
	msg, err := s.conn.Conn().PgConn().ReceiveMessage(wctx)
	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}
	return msg, nil
}

// doneReplication performs the replication closing tasks on completition and
// closes the done channel. If any errors are encountered, will be available through Err().
func (s *Subscription) doneReplication() {
	tctx, cancel := context.WithTimeout(context.Background(), closeReplicationTimeout)
	defer cancel()

	if err := s.sentStandbyDone(tctx); err != nil {
		s.doneErr = errors.Join(s.doneErr, err)
	}

	close(s.done)
}

// sentStandbyDone signals replication done and submits the last flushed LSN.
func (s *Subscription) sentStandbyDone(ctx context.Context) error {
	var errs []error

	// send copy done message indicating replication is done
	if err := s.sendStandbyCopyDone(ctx); err != nil {
		sdk.Logger(ctx).Error().
			Err(err).
			Msg("failed to send standby copy done")
		errs = append(errs, err)
	}
	// send last status update
	if err := s.sendStandbyStatusUpdate(ctx); err != nil {
		sdk.Logger(ctx).Error().
			Err(err).
			Msg("failed to send final status update")
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
