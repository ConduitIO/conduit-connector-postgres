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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

const (
	pgDuplicateObjectErrorCode = "42710"
	pgOutputPlugin             = "pgoutput"
)

// Subscription manages a subscription to a logical replication slot.
type Subscription struct {
	ConnConfig    pgconn.Config
	SlotName      string
	Publication   string
	Tables        []string
	StartLSN      pglogrepl.LSN
	Handler       Handler
	StatusTimeout time.Duration

	stop    context.CancelFunc
	ready   chan struct{}
	done    chan struct{}
	doneErr error

	// cleanup is the function that gets called on teardown.
	// Cleanup functions that get added here on initialization act as deferred
	// functions.
	cleanup func(ctx context.Context) error

	walWritten pglogrepl.LSN
	walFlushed pglogrepl.LSN
}

type Handler func(context.Context, pglogrepl.Message, pglogrepl.LSN) error

func NewSubscription(
	config pgconn.Config,
	slotName,
	publication string,
	tables []string,
	startLSN pglogrepl.LSN,
	h Handler,
) *Subscription {
	return &Subscription{
		ConnConfig:    config,
		SlotName:      slotName,
		Publication:   publication,
		Tables:        tables,
		StartLSN:      startLSN,
		Handler:       h,
		StatusTimeout: 10 * time.Second,

		ready: make(chan struct{}),
		done:  make(chan struct{}),
		// cleanup does nothing by default
		cleanup: func(ctx context.Context) error { return nil },
	}
}

// Start replication and block until error or ctx is canceled.
func (s *Subscription) Start(ctx context.Context) (err error) {
	defer func() {
		// use fresh context for cleanup
		cleanupErr := s.cleanup(context.Background())
		if err == nil {
			// return close connection error
			err = cleanupErr
		} else if cleanupErr != nil {
			// an error is already returned, let's log this one instead
			sdk.Logger(ctx).Err(cleanupErr).Msg("failed to cleanup subscription")
		}
		s.doneErr = err // store error so it can be retrieved later

		select {
		case <-s.ready:
			// ready is already closed
		default:
			close(s.ready)
		}
		close(s.done)
	}()

	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	err = s.CreatePublication(ctx, conn)
	if err != nil {
		return err
	}
	_, err = s.CreateReplicationSlot(ctx, conn)
	if err != nil {
		return err
	}
	err = s.StartReplication(ctx, conn)
	if err != nil {
		return err
	}

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.stop = cancel
	s.walWritten = s.StartLSN
	s.walFlushed = s.StartLSN

	return s.listen(lctx, conn)
}

// listen runs until context is cancelled or an error is encountered.
func (s *Subscription) listen(ctx context.Context, conn *pgconn.PgConn) error {
	// signal that the subscription is ready and is receiving messages
	close(s.ready)
	nextStatusUpdateAt := time.Now().Add(s.StatusTimeout)
	for {
		if time.Now().After(nextStatusUpdateAt) {
			err := s.sendStandbyStatusUpdate(ctx, conn)
			if err != nil {
				return err
			}
			nextStatusUpdateAt = time.Now().Add(s.StatusTimeout)
		}

		msg, err := s.receiveMessage(ctx, conn, nextStatusUpdateAt)
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
			return fmt.Errorf("unexpected message type %T", msg)
		}

		switch copyDataMsg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			err := s.handlePrimaryKeepaliveMessage(ctx, conn, copyDataMsg)
			if err != nil {
				return err
			}
		case pglogrepl.XLogDataByteID:
			err := s.handleXLogData(ctx, copyDataMsg)
			if err != nil {
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
func (s *Subscription) handlePrimaryKeepaliveMessage(ctx context.Context, conn *pgconn.PgConn, copyDataMsg *pgproto3.CopyData) error {
	sdk.Logger(ctx).Trace().Msg("handling primary keepalive message")

	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyDataMsg.Data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}
	if pkm.ReplyRequested {
		if err = s.sendStandbyStatusUpdate(ctx, conn); err != nil {
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

	if err = s.Handler(ctx, logicalMsg, xld.WALStart); err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	if xld.WALStart > 0 {
		s.walWritten = xld.WALStart
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
func (s *Subscription) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
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

// connect establishes a replication connection and adds a cleanup function
// which closes the connection afterwards.
func (s *Subscription) connect(ctx context.Context) (*pgconn.PgConn, error) {
	if s.ConnConfig.RuntimeParams == nil {
		s.ConnConfig.RuntimeParams = make(map[string]string)
	}
	// enable replication on connection
	s.ConnConfig.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, &s.ConnConfig)
	if err != nil {
		return nil, fmt.Errorf("could not establish replication connection: %w", err)
	}

	// add cleanup to close connection
	s.addCleanup(func(ctx context.Context) error {
		if err := conn.Close(ctx); err != nil {
			return fmt.Errorf("failed to close replication connection: %w", err)
		}
		return nil
	})
	return conn, nil
}

// CreatePublication creates a publication if it doesn't exist yet. If a
// publication with that name already exists it returns no error. If a
// publication is created a cleanup function is added which deletes the
// publication afterwards.
func (s *Subscription) CreatePublication(ctx context.Context, conn *pgconn.PgConn) error {
	if err := CreatePublication(
		ctx,
		conn,
		s.Publication,
		CreatePublicationOptions{Tables: s.Tables},
	); err != nil {
		// If creating the publication fails with code 42710, this means
		// the publication already exists.
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != pgDuplicateObjectErrorCode {
			return err
		}
	} else {
		// publication was created successfully, drop it when we're done
		s.addCleanup(func(ctx context.Context) error {
			err := DropPublication(ctx, conn, s.Publication, DropPublicationOptions{})
			if err != nil {
				return fmt.Errorf("failed to drop publication: %w", err)
			}
			return nil
		})
	}
	return nil
}

// CreateReplicationSlot creates a temporary replication slot which will be
// deleted once the connection is closed. If a replication slot with that name
// already exists it returns no error.
func (s *Subscription) CreateReplicationSlot(ctx context.Context, conn *pgconn.PgConn) (string, error) {
	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		s.SlotName,
		pgOutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true, // replication slot is dropped when we disconnect
			SnapshotAction: "USE_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		// If creating the replication slot fails with code 42710, this means
		// the replication slot already exists.
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != pgDuplicateObjectErrorCode {
			return "", err
		}
	}

	return result.ConsistentPoint, nil
}

// StartReplication starts replication with a specific start LSN and adds two
// cleanup functions, one for sending the last status update and one for sending
// the standby copy done signal.
func (s *Subscription) StartReplication(ctx context.Context, conn *pgconn.PgConn) error {
	pluginArgs := []string{
		`"proto_version" '1'`,
		fmt.Sprintf(`"publication_names" '%s'`, s.Publication),
	}

	if err := pglogrepl.StartReplication(
		ctx,
		conn,
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

	// add cleanup for sending copy done message indicating replication has done
	s.addCleanup(func(ctx context.Context) error {
		if err := s.sendStandbyCopyDone(ctx, conn); err != nil {
			return fmt.Errorf("failed to send standby copy done: %w", err)
		}
		return nil
	})

	// add cleanup for sending last status update
	s.addCleanup(func(ctx context.Context) error {
		if err := s.sendStandbyStatusUpdate(ctx, conn); err != nil {
			return fmt.Errorf("failed to send final status update: %w", err)
		}
		return nil
	})

	return nil
}

// addCleanup will add the function to the cleanup functions that are called
// when the subscription is stopped. Functions will get stacked and taken off
// the stack as they are called (same as deferred functions).
func (s *Subscription) addCleanup(newCleanup func(context.Context) error) {
	oldCleanup := s.cleanup
	s.cleanup = func(ctx context.Context) error {
		// first call new cleanup, then old cleanup to have the same ordering as
		// deferred functions
		newErr := newCleanup(ctx)
		oldErr := oldCleanup(ctx)
		switch {
		case oldErr != nil && newErr == nil:
			return fmt.Errorf("cleanup error: %w", oldErr)
		case oldErr == nil && newErr != nil:
			return fmt.Errorf("cleanup error: %w", newErr)
		case oldErr != nil && newErr != nil:
			return fmt.Errorf("[%s] %w", newErr, oldErr)
		}
		return nil
	}
}

// sendStandbyCopyDone sends the status message to server indicating that
// replication is done.
func (s *Subscription) sendStandbyCopyDone(ctx context.Context, conn *pgconn.PgConn) error {
	sdk.Logger(ctx).Trace().Msg("sending standby copy done message")
	_, err := pglogrepl.SendStandbyCopyDone(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to send standby copy done: %w", err)
	}
	return nil
}

// sendStandbyStatusUpdate sends the status message to server indicating which LSNs
// have been processed.
func (s *Subscription) sendStandbyStatusUpdate(ctx context.Context, conn *pgconn.PgConn) error {
	// load with atomic to prevent race condition with ack
	walFlushed := pglogrepl.LSN(atomic.LoadUint64((*uint64)(&s.walFlushed)))

	if walFlushed > s.walWritten {
		return fmt.Errorf("walWrite (%s) should be >= walFlush (%s)", s.walWritten, walFlushed)
	}

	sdk.Logger(ctx).Trace().
		Str("walWrite", s.walWritten.String()).
		Str("walFlush", walFlushed.String()).
		Str("walApply", walFlushed.String()).
		Msg("sending standby status update")

	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.walWritten,
		WALFlushPosition: walFlushed,
		WALApplyPosition: walFlushed,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	return nil
}

// receiveMessage tries to receive a message from the replication stream. If the
// deadline is reached before a message is received it returns
// context.DeadlineExceeded.
func (s *Subscription) receiveMessage(ctx context.Context, conn *pgconn.PgConn, deadline time.Time) (pgproto3.BackendMessage, error) {
	wctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	sdk.Logger(ctx).Trace().Msg("receiving message")
	msg, err := conn.ReceiveMessage(wctx)
	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}
	return msg, nil
}
