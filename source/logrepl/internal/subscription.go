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
	"sync"
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
	TXSnapshotID  string

	conn *pgxpool.Conn
	pool *pgxpool.Pool

	stop context.CancelFunc

	ready   chan struct{}
	done    chan struct{}
	doneErr error

	// Resume is what this subscription treats as already delivered (#331).
	// CreateSubscription sets the legacy point {LSN: StartLSN}, using the
	// start LSN after it was moved up to the slot's restart_lsn: with no
	// change key to go on, that is the earliest point Postgres can re-send
	// from anyway. The CDC iterator replaces it with the exact point, built
	// from the position's own change key and raw LSN, when the checkpointed
	// position carries a key. Set before Run.
	Resume ResumePoint

	walWritten   pglogrepl.LSN
	walFlushed   pglogrepl.LSN
	serverWALEnd pglogrepl.LSN

	// change is the key of the change being handled: the commit LSN of the
	// transaction whose messages are being received (from its BeginMessage)
	// and the ordinal of the current change within it. emitted is the key of
	// the last change the handler emitted a record for. Both are only touched
	// on the goroutine running Run (CurrentChange is called from the handler,
	// on that goroutine).
	change  ChangeKey
	emitted ChangeKey

	// acked is the key of the last record the engine acked (see Ack). Acks
	// arrive on another goroutine, hence the mutex.
	ackedMu sync.Mutex
	acked   ChangeKey

	// reportedFlush is the highest flush position sent to the server so far.
	// The report never goes below it (see reportedPositions). Only touched on
	// the goroutine running Run.
	reportedFlush pglogrepl.LSN
}

type Handler func(context.Context, pglogrepl.Message, pglogrepl.LSN) (pglogrepl.LSN, error)

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
		TXSnapshotID:  result.SnapshotName,
		Resume:        ResumePoint{LSN: startLSN},

		conn: conn,
		pool: pool,

		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}, nil
}

// Run the logical replication listener and block until it returns an error,
// or the context is canceled.
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

	for {
		if time.Now().After(nextStatusUpdateAt) {
			err := s.sendStandbyStatusUpdate(ctx)
			if err != nil {
				return err
			}
			nextStatusUpdateAt = time.Now().Add(s.StatusTimeout)
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

	if len(xld.WALData) > 0 {
		switch pglogrepl.MessageType(xld.WALData[0]) {
		case pglogrepl.MessageTypeStreamStart, pglogrepl.MessageTypeStreamStop,
			pglogrepl.MessageTypeStreamCommit, pglogrepl.MessageTypeStreamAbort:
			// The change keys below assume whole transactions arrive at
			// commit, in commit order. Streamed in-progress transactions
			// (pgoutput's "streaming" option) break that, so refuse loudly
			// instead of mis-keying records (#331).
			return fmt.Errorf("unsupported streamed-transaction message %q: this connector requires pgoutput streaming to be off",
				string(xld.WALData[0]))
		}
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("invalid message: %w", err)
	}

	// Invariant 3 (#331): skip only changes the resume point proves were
	// delivered before the restart. The decision uses the change key
	// (transaction commit LSN, ordinal within it), never the change's own LSN
	// (WALStart): a transaction that committed after the checkpoint can carry
	// lower LSNs, and the rows of one multi-row insert share one LSN.
	// Relation, Type, Origin, Begin and Commit messages are never skipped.
	switch m := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		s.change = ChangeKey{CommitLSN: m.FinalLSN}
	case *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		s.change.Seq++
		if s.Resume.Delivered(s.change) {
			sdk.Logger(ctx).Trace().
				Stringer("commit_lsn", s.change.CommitLSN).
				Uint64("seq", s.change.Seq).
				Stringer("lsn", xld.WALStart).
				Msg("skipping change delivered before the restart")
			return nil
		}
	}

	writtenLSN, err := s.Handler(ctx, logicalMsg, xld.WALStart)
	if err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	if writtenLSN > 0 {
		s.walWritten = writtenLSN
		s.emitted = s.change
	}

	return nil
}

// CurrentChange returns the key of the change being handled. The handler
// calls it, on the subscription goroutine, to put the key in the record's
// position.
func (s *Subscription) CurrentChange() ChangeKey {
	return s.change
}

// Ack stores the LSN as flushed and key as the last acked change. Next time
// WAL positions are flushed, Postgres will know it can purge WAL logs up to
// this LSN. Acks must arrive in the order records were emitted (FIFO), which
// the engine guarantees.
func (s *Subscription) Ack(lsn pglogrepl.LSN, key ChangeKey) {
	// store with atomic to prevent race conditions with sending status update
	atomic.StoreUint64((*uint64)(&s.walFlushed), uint64(lsn))
	s.ackedMu.Lock()
	s.acked = key
	s.ackedMu.Unlock()
}

// allAcked reports whether the last emitted record has been acked. Keys are
// unique and acks are FIFO, so that means every emitted record has been
// acked. Before anything is emitted both keys are zero.
func (s *Subscription) allAcked() bool {
	s.ackedMu.Lock()
	defer s.ackedMu.Unlock()
	return s.acked == s.emitted
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
	serverWALEnd := pglogrepl.LSN(atomic.LoadUint64((*uint64)(&s.serverWALEnd)))

	// There is deliberately no "walFlushed > walWritten is an error" check
	// (#331): with interleaved transactions a record with a lower LSN can be
	// emitted after one with a higher LSN was acked, so walFlushed > walWritten
	// is a normal state, and failing on it killed the subscription.
	write, flush := reportedPositions(s.walWritten, walFlushed, serverWALEnd, s.reportedFlush, s.allAcked())

	sdk.Logger(ctx).Trace().
		Stringer("wal_write", s.walWritten).
		Stringer("wal_flush", walFlushed).
		Stringer("server_wal_end", serverWALEnd).
		Stringer("reported_write", write).
		Stringer("reported_flush", flush).
		Msg("sending standby status update")

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn.Conn().PgConn(), pglogrepl.StandbyStatusUpdate{
		WALWritePosition: write,
		WALFlushPosition: flush,
		WALApplyPosition: flush,
		ReplyRequested:   false,
	}); err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	s.reportedFlush = flush
	return nil
}

// reportedPositions decides the write and flush positions a standby status
// update reports. The server stores the flush position as the slot's
// confirmed_flush_lsn, which decides the WAL Postgres may discard and the
// transactions it will not send again after a restart.
//
//   - The baseline is walFlushed, the change LSN of the last record the engine
//     acked. Reporting it is safe even when a record still in flight has a
//     lower or equal LSN (interleaved transactions, rows of one multi-row
//     insert): that record's transaction commits after the acked change's
//     LSN, so the server would re-send it.
//   - Only when allAcked (the last emitted record's change key equals the last
//     acked one) may the report go further, to serverWALEnd (the WAL end from
//     the last keepalive). The test compares change keys, not LSNs: rows of a
//     multi-row insert share an LSN, so "walFlushed == walWritten" would claim
//     everything is acked after the first of them is.
//   - The flush position never goes below lastReported. A flush position that
//     was safe when reported stays safe: transactions arrive in commit order,
//     anything that arrives later commits after it, and the server re-sends
//     any transaction whose commit is past confirmed_flush_lsn.
//   - write is at least flush.
func reportedPositions(walWritten, walFlushed, serverWALEnd, lastReported pglogrepl.LSN, allAcked bool) (write, flush pglogrepl.LSN) {
	flush = walFlushed

	// Invariant 1: report past the acked records only when no emitted record
	// is unacked. Reporting serverWALEnd while a record is in flight would let
	// Postgres discard the WAL that record depends on, and a crash before the
	// destination wrote it would lose it.
	if allAcked {
		flush = maxLSN(flush, serverWALEnd)
	}

	// Invariant 2: the reported flush position never decreases.
	flush = maxLSN(flush, lastReported)
	write = maxLSN(walWritten, flush)
	return write, flush
}

func maxLSN(first pglogrepl.LSN, rest ...pglogrepl.LSN) pglogrepl.LSN {
	m := first
	for _, l := range rest {
		if l > m {
			m = l
		}
	}
	return m
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
