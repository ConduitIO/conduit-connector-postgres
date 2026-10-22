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

	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
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

	// RestartLSN is the replication slot's restart_lsn as read at subscription
	// creation. When the slot was freshly created on this run (TXSnapshotID is
	// non-empty), this is the slot's consistent point — the WAL position the
	// exported snapshot is consistent with — and it is the snapshot low watermark
	// used by DBZ-3 Area 1's resumable-snapshot reconciliation. On a resume (the
	// slot already existed, TXSnapshotID is empty) this reflects the slot's
	// current restart_lsn, which may have advanced past the original snapshot
	// point; callers must therefore only treat it as the low watermark when
	// TXSnapshotID is non-empty and otherwise carry the persisted watermark
	// forward from the position.
	RestartLSN pglogrepl.LSN

	// ConfirmedFlushLSN is the slot's confirmed_flush_lsn as read at
	// subscription creation (F0 in the offset-import design).
	ConfirmedFlushLSN pglogrepl.LSN

	// Import is the Debezium offset import's partial rule (#341 D4), nil
	// outside import mode. Set before Run; used only on the Run goroutine.
	Import *dbzoffset.Filter
	// ImportCeiling is Lc in import mode, zero otherwise. Until the first
	// ack of this run the reported flush position is capped at it, and
	// floored at ImportFloor (#341 D6). Set before Run.
	ImportCeiling pglogrepl.LSN
	ImportFloor   pglogrepl.LSN
	// importWarnAt is when the next "import pending first ack" warning is
	// due. Only touched on the Run goroutine.
	importWarnAt time.Time

	pool *pgxpool.Pool

	// connMu guards conn, runActive and tornDown. conn is a pooled
	// connection: it must be released exactly once, and never while anything
	// still uses it, because the pool hands it to another owner (another
	// pipeline) the moment it is released. Ownership therefore follows Run:
	//
	//   - while Run is active (runActive), Run is the only user and the only
	//     one that releases conn: startReplication swaps it, endRun releases
	//     it on exit once Teardown asked for it;
	//   - Teardown releases conn only if Run is not active (never started, or
	//     already finished). If Teardown's wait for Run times out, it leaves
	//     the release to Run's exit instead of releasing under it (#352).
	connMu    sync.Mutex
	conn      *pgxpool.Conn
	runActive bool // Run started and has not finished
	tornDown  bool // Teardown was called: release conn once Run is done with it

	// stopMu guards stop and stopped: Stop and Teardown are called
	// from another goroutine, possibly before Run has created its context
	// (#346: stopping right after the snapshot handoff overtook a starting
	// subscription and left it unstoppable).
	stopMu  sync.Mutex
	stop    context.CancelFunc
	stopped bool // Stop was called, possibly before Run began

	// afterAcquire is a test seam, nil in production. It runs in
	// startReplication right after the replication connection was stored.
	afterAcquire func()

	// startErr is why Run never reached the receive loop (startReplication
	// failed, or Run was refused after Teardown). It is written once, before
	// ready is closed, and read only after <-Ready().
	startErr error

	// beforeRelease is a test seam, nil in production. It runs at the top of
	// startReplication, before the old connection is released.
	beforeRelease func()

	// beforeAcquire is a test seam, nil in production. It runs inside
	// startReplication after the old connection was released and before the
	// new one is acquired: the window #346 is about.
	beforeAcquire func()

	// replicating is set by Run once START_REPLICATION succeeded. Only then is
	// there a stream to send the final status update and CopyDone on.
	replicating bool

	ready   chan struct{}
	done    chan struct{}
	doneErr error

	walWritten   pglogrepl.LSN
	walFlushed   pglogrepl.LSN
	serverWALEnd pglogrepl.LSN

	// Resume is what this subscription treats as already delivered (#331).
	// CreateSubscription sets the legacy point {LSN: StartLSN}, using the
	// start LSN after it was moved up to the slot's restart_lsn: with no
	// change key to go on, that is the earliest point Postgres can re-send
	// from anyway. The CDC iterator replaces it with the exact point, built
	// from the position's own change key and raw LSN, when the checkpointed
	// position carries a key. Set before Run.
	Resume ResumePoint

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
	// ackSeen is set by the first Ack of this run. Guarded by ackedMu.
	ackSeen bool

	// reportedFlush is the highest flush position sent to the server so far.
	// The report never goes below it (see reportedPositions). Only touched on
	// the goroutine running Run.
	reportedFlush pglogrepl.LSN
}

type Handler func(context.Context, pglogrepl.Message, pglogrepl.LSN) (pglogrepl.LSN, error)

// AfterSlotCreateHook is a test seam, nil in production. It runs right after
// CREATE_REPLICATION_SLOT succeeded and may fail the call, to test what
// callers do when a slot was created by a call that then errors. It is only
// assigned in _test.go files (enforced by TestSeamsOnlyAssignedInTests in
// the logrepl package).
var AfterSlotCreateHook func() error

// SlotCreatedError wraps an error returned by CreateSubscription after the
// slot had already been created. The slot exists (and keeps its consistent
// point) although the call failed, so a caller that resumes from a saved
// position must still treat the creation as a taken gap (invariant 3): the
// next attempt finds a healthy slot and cannot tell.
type SlotCreatedError struct {
	// ConsistentPoint is the created slot's consistent point.
	ConsistentPoint string
	// Err is the failure after the creation.
	Err error
}

func (e *SlotCreatedError) Error() string {
	return fmt.Sprintf("replication slot was created (consistent point %s), but the call failed: %v", e.ConsistentPoint, e.Err)
}

func (e *SlotCreatedError) Unwrap() error { return e.Err }

// CreatedSlot reports whether the call that returned sub and err created the
// replication slot, and the slot's consistent point: sub for a call that
// succeeded (Subscription.RestartLSN is the consistent point of a freshly
// created slot), a *SlotCreatedError for one that failed after the creation.
func CreatedSlot(sub *Subscription, err error) (string, bool) {
	var ce *SlotCreatedError
	if errors.As(err, &ce) {
		return ce.ConsistentPoint, true
	}
	if err == nil && sub != nil && sub.TXSnapshotID != "" {
		return sub.RestartLSN.String(), true
	}
	return "", false
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
	return newSubscription(ctx, pool, slotName, publication, tables, startLSN, h, true)
}

// OpenSubscription is CreateSubscription for a slot that must already exist:
// it never issues CREATE_REPLICATION_SLOT. Import mode uses it (#341, Nits:
// "Slot creation in import mode"): a slot created at Open would start at the
// current WAL and skip every change since Debezium's offset. A missing slot
// fails with ErrMissingSlot.
func OpenSubscription(
	ctx context.Context,
	pool *pgxpool.Pool,
	slotName,
	publication string,
	tables []string,
	startLSN pglogrepl.LSN,
	h Handler,
) (*Subscription, error) {
	return newSubscription(ctx, pool, slotName, publication, tables, startLSN, h, false)
}

func newSubscription(
	ctx context.Context,
	pool *pgxpool.Pool,
	slotName,
	publication string,
	tables []string,
	startLSN pglogrepl.LSN,
	h Handler,
	createSlot bool,
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

	var result pglogrepl.CreateReplicationSlotResult
	if createSlot {
		result, err = pglogrepl.CreateReplicationSlot(
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
			err = nil

			sdk.Logger(ctx).Warn().
				Msgf("replication slot %q already exists", slotName)
		}
	}

	// created: CREATE_REPLICATION_SLOT succeeded in this call (a duplicate
	// error leaves SnapshotName empty).
	created := result.SnapshotName != ""

	// AfterSlotCreateHook is a test seam, see its doc.
	if hook := AfterSlotCreateHook; created && hook != nil {
		if err = hook(); err != nil {
			err = &SlotCreatedError{ConsistentPoint: result.ConsistentPoint, Err: err}
			return nil, err
		}
	}

	slotInfo, err := ReadReplicationSlot(ctx, pool, slotName)
	if err != nil {
		if created {
			// The slot exists now although this call failed: tell the
			// caller, or the next attempt finds a healthy slot and nobody
			// knows a gap was taken (see SlotCreatedError).
			err = &SlotCreatedError{ConsistentPoint: result.ConsistentPoint, Err: err}
		}
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
		SlotName:          slotName,
		Publication:       publication,
		Tables:            tables,
		StartLSN:          startLSN,
		Handler:           h,
		StatusTimeout:     10 * time.Second,
		TXSnapshotID:      result.SnapshotName,
		Resume:            ResumePoint{LSN: startLSN},
		RestartLSN:        slotInfo.RestartLSN,
		ConfirmedFlushLSN: slotInfo.ConfirmedFlushLSN,

		conn: conn,
		pool: pool,

		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}, nil
}

// Run the logical replication listener and block until it returns an error,
// or the context is canceled.
func (s *Subscription) Run(ctx context.Context) error {
	// endRun is registered first so it runs last, after doneReplication has
	// used the connection for the final status update.
	started := s.beginRun()
	defer s.endRun(started)
	defer s.doneReplication()
	if !started {
		err := errors.New("subscription was torn down before it started")
		s.startErr, s.doneErr = err, err
		close(s.ready) // ready to fail.
		return err
	}

	// The cancel function is published before anything blocking happens, so a
	// Stop that arrives while replication is still starting is honoured.
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.stopMu.Lock()
	s.stop = cancel
	if s.stopped {
		cancel()
	}
	s.stopMu.Unlock()

	if err := s.startReplication(lctx); err != nil {
		s.startErr, s.doneErr = err, err
		close(s.ready) // ready to fail.
		return err
	}
	// Invariant 3 (import mode): refuse a slot that moved past Lc before
	// anything is sent on the stream. This runs before replicating is set,
	// so doneReplication sends no final standby status update: on servers
	// before "Don't retreat slot's confirmed_flush LSN" (14.19, 15.14, 16.10,
	// 17.6) that update would move confirmed_flush_lsn back, the next start
	// would pass every check, and the changes committed between Lc and where
	// the slot had moved would be lost (found by the #413 review). The
	// connection is closed, not returned to the pool, because it is in COPY
	// mode.
	if err := s.checkImportSlot(lctx); err != nil {
		s.closeConnForRefusal()
		s.startErr, s.doneErr = err, err
		close(s.ready) // ready to fail.
		return err
	}
	s.replicating = true

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
				if ctx.Err() != nil {
					// It is the subscription's own context that expired, not
					// the status-update deadline. Retrying can never succeed
					// and used to spin forever, so Teardown never returned
					// (#347).
					return ctx.Err()
				}
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
			logUnknownCopyData(ctx, copyDataMsg.Data)
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
		if s.Import != nil {
			s.Import.Begin(m.FinalLSN, m.Xid, m.CommitTime)
		}
	case *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		// The ordinal is counted before any skip decision, so the records
		// delivered after skipped changes carry their real keys (#341 D4).
		s.change.Seq++
		if s.Resume.Delivered(s.change) {
			sdk.Logger(ctx).Trace().
				Stringer("commit_lsn", s.change.CommitLSN).
				Uint64("seq", s.change.Seq).
				Stringer("lsn", xld.WALStart).
				Msg("skipping change delivered before the restart")
			return nil
		}
		// Invariant 3 (#341 D4, partial rule): inside the transaction the
		// Debezium offset was taken in, skip only changes below the offset's
		// lsn, which Debezium emitted before the offset's record. Changes at
		// that lsn and above are delivered.
		if s.Import != nil && s.Import.Change(xld.WALStart) {
			sdk.Logger(ctx).Trace().
				Stringer("commit_lsn", s.change.CommitLSN).
				Uint64("seq", s.change.Seq).
				Stringer("lsn", xld.WALStart).
				Msg("skipping change Debezium delivered before its offset")
			return nil
		}
	case *pglogrepl.CommitMessage:
		if s.Import != nil {
			s.closeImportTransaction(ctx)
		}
	}

	writtenLSN, err := s.Handler(ctx, logicalMsg, xld.WALStart)
	if err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	if writtenLSN > 0 {
		s.walWritten = writtenLSN
		// Invariant 1 (slot advance): the handler returns a non-zero LSN for
		// every change it was handed, including a change it deliberately
		// drops without emitting a record (the D4 skip after a drift marker:
		// "the marker, then nothing"). Those changes MUST still advance the
		// emitted key. No record carries their key, so it is never acked and
		// allAcked stays false. If a skipped change did not advance it, the
		// marker would be the last emitted key, its ack would open the gate,
		// and the next status update would report serverWALEnd: the slot would
		// move past the rest of the marker's transaction (and past every
		// transaction after it), whose changes were skipped, never delivered,
		// and so would not be re-sent on the operator's approving restart.
		// Rows lost. Pinned by TestDrift335_HaltApprovalMidTransaction (#337),
		// which restarts mid-transaction after the marker's ack and expects the
		// rest of the transaction.
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
	s.ackSeen = true
	s.ackedMu.Unlock()
}

// FirstAckSeen reports whether this run has received an ack.
func (s *Subscription) FirstAckSeen() bool {
	s.ackedMu.Lock()
	defer s.ackedMu.Unlock()
	return s.ackSeen
}

// closeImportTransaction ends the current transaction for the import's
// partial rule and logs the duplicate set when it closes (#341, Q5: a count
// at INFO; no public metadata key).
func (s *Subscription) closeImportTransaction(ctx context.Context) {
	wasArmed := s.Import.Armed()
	s.Import.Commit()
	if wasArmed && !s.Import.Armed() {
		st := s.Import.Stats()
		sdk.Logger(ctx).Info().
			Stringer("commit_lsn", st.MatchedCommitLSN).
			Uint64("skipped_changes", st.ChangesSkipped).
			Uint64("duplicates", st.DeliveredAtBoundary).
			Msg("debezium offset import: the offset's transaction was streamed; " +
				"its changes at the offset's LSN were delivered again (possible duplicates)")
	}
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
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.stopped = true
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

// Teardown stops the subscription and waits for Run to finish, at most until
// ctx ends. It releases the pooled replication connection only when Run no
// longer uses it. If the wait ends before Run does (ctx expired), the
// connection is NOT released here: Run releases it on its way out, so a
// connection is never handed back to the pool under a live user.
//
// Run's exit is itself bounded: the final status update and CopyDone run with
// a deadline of closeReplicationTimeout on the connection, so a hung server
// cannot keep Run, the connection and the replication slot busy forever.
//
// A Run that exits on its own (an error, or the context ending) keeps the
// connection until Teardown is called; callers must always call Teardown.
func (s *Subscription) Teardown(ctx context.Context) error {
	s.Stop()

	if !s.markTornDown() {
		// Run is not active (never started, so it will refuse to start, or
		// already finished): nothing to wait for, the connection is released.
		return nil
	}

	// Run is active: wait for it, including when it is still starting. The
	// connection stays Run's until it exits (#346).
	return s.Wait(ctx, closeReplicationTimeout)
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

	if s.beforeRelease != nil {
		s.beforeRelease()
	}
	s.releaseConn() // nothing may touch the old connection from here on
	if s.beforeAcquire != nil {
		s.beforeAcquire()
	}

	conn, err := s.pool.Acquire(cpool.WithReplication(ctx))
	if err != nil {
		return fmt.Errorf("could not establish replication connection: %w", err)
	}

	s.setConn(conn)
	if s.afterAcquire != nil {
		s.afterAcquire()
	}

	pluginArgs := []string{
		`"proto_version" '1'`,
		fmt.Sprintf(`"publication_names" '%s'`, s.Publication),
	}

	if err := pglogrepl.StartReplication(
		ctx,
		conn.Conn().PgConn(),
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
	conn, err := s.pgConn()
	if err != nil {
		return err
	}
	_, err = pglogrepl.SendStandbyCopyDone(ctx, conn)
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

	// There is deliberately no "walFlushed > walWritten is an error" check
	// (#331): with interleaved transactions a record with a lower LSN can be
	// emitted after one with a higher LSN was acked, so walFlushed > walWritten
	// is a normal state, and failing on it killed the subscription.
	serverWALEnd := pglogrepl.LSN(atomic.LoadUint64((*uint64)(&s.serverWALEnd)))

	// Manage replication slot lag: when every emitted record has been acked,
	// report the server's WAL end instead of the last acked record. The gate lives in reportedPositions.
	write, flush := reportedPositions(s.walWritten, walFlushed, serverWALEnd, s.reportedFlush, s.allAcked())
	firstAckSeen := s.FirstAckSeen() // read once: both branches must see the same answer
	if s.ImportCeiling != 0 && firstAckSeen {
		// D6: import mode floors every report at F0, also after the first
		// ack. The first acked change can sit below F0 (its transaction
		// began before it), and before "Don't retreat slot's confirmed_flush
		// LSN" (14.19, 15.14, 16.10, 17.6) a lower report moves the slot back:
		// more duplicates, never loss, but avoidable.
		flush = maxLSN(flush, s.ImportFloor)
		write = maxLSN(write, flush)
	}
	if s.ImportCeiling != 0 && !firstAckSeen {
		write, flush = capImportFlush(write, flush, s.ImportCeiling, s.ImportFloor)
		if now := time.Now(); now.After(s.importWarnAt) {
			s.importWarnAt = now.Add(time.Minute)
			sdk.Logger(ctx).Warn().
				Stringer("lsn_commit", s.ImportCeiling).
				Msgf("import pending first ack; slot held at %s (commit one write to a table of this pipeline to complete the import)", s.ImportCeiling)
		}
	}

	// Invariant 1 / DBZ-3 B0 kill point (chaospoint.StandbyStatusUpdate):
	// walFlushed is loaded and the report is decided, but the wire send
	// below has not happened yet. A kill landing exactly here proves the
	// window between "the engine knows what it has durably acked" and "the
	// server has been told" — if walFlushed < s.walWritten at this point,
	// Postgres's view of confirmed_flush_lsn must not advance past
	// walFlushed on the next status update after recovery, or the slot
	// could prune WAL for records this process never actually acked.
	// No-op outside the conduitchaos build (see internal/chaospoint).
	chaospoint.Reach(chaospoint.StandbyStatusUpdate)

	sdk.Logger(ctx).Trace().
		Stringer("wal_write", s.walWritten).
		Stringer("wal_flush", walFlushed).
		Stringer("server_wal_end", serverWALEnd).
		Stringer("reported_write", write).
		Stringer("reported_flush", flush).
		Msg("sending standby status update")

	pgConn, err := s.pgConn()
	if err != nil {
		return err
	}
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, pgConn, pglogrepl.StandbyStatusUpdate{
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

// checkImportSlot re-reads the slot once START_REPLICATION holds it (the
// slot is active from then on, so nothing else can move it). In import mode
// its confirmed_flush_lsn must still be at or below Lc: Postgres decodes from
// there, and with F > Lc the transactions committing in [Lc, F) would never
// be sent, nor covered by Debezium's offset (D5 check 5). The preflight
// checked this before the subscription was created; this closes the window
// between that check and the stream start.
//
// It also takes the flush floor (F0, D6) from this read, made while the
// stream holds the slot, so the floor is what the server will decode from,
// never a value read before the slot could still move.
func (s *Subscription) checkImportSlot(ctx context.Context) error {
	if s.ImportCeiling == 0 {
		return nil
	}
	slot, err := ReadReplicationSlot(ctx, s.pool, s.SlotName)
	if err != nil {
		return err
	}
	if slot.RestartLSN > slot.ConfirmedFlushLSN {
		// A slot never needs WAL only from past the point it confirmed. This
		// shape means confirmed_flush_lsn was moved back (a retreat), and the
		// slot no longer proves what it holds.
		return &dbzoffset.Error{
			Code: dbzoffset.CodeSlotInvalidated, Path: "logrepl.slotName",
			Message: fmt.Sprintf("replication slot %q has restart_lsn %s past its confirmed_flush_lsn %s; its confirmed position was moved back",
				s.SlotName, slot.RestartLSN, slot.ConfirmedFlushLSN),
			Fix: "The slot cannot prove which changes it still holds. Go back to Debezium with the manual runbook (`conduit migrate kafka-connect --check-rollback`) and cut over again with a new slot, or re-snapshot.",
		}
	}
	s.ImportFloor = slot.ConfirmedFlushLSN
	if slot.ConfirmedFlushLSN > s.ImportCeiling {
		// Invariant 3.
		return &dbzoffset.Error{
			Code: dbzoffset.CodeSlotAhead, Path: "logrepl.slotName",
			Message: fmt.Sprintf("replication slot %q moved to confirmed_flush_lsn %s after the preflight, past Debezium's lsn_commit %s",
				s.SlotName, slot.ConfirmedFlushLSN, s.ImportCeiling),
			Fix: "Something else consumed Conduit's slot. Stop it, go back to Debezium with the manual runbook (`conduit migrate kafka-connect --check-rollback`), then cut over again.",
		}
	}
	return nil
}

// closeConnForRefusal closes the replication connection after
// START_REPLICATION succeeded but the stream is refused. Nothing more is
// sent on it (no status update, no CopyDone), and a closed connection is
// discarded by the pool instead of being reused in COPY mode.
func (s *Subscription) closeConnForRefusal() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeReplicationTimeout)
	defer cancel()
	_ = s.conn.Conn().Close(ctx)
}

// capImportFlush applies the import's flush cap (#341 D6) before the first
// ack of a run: flush = max(min(flush, ceiling), floor).
//
// Without the cap a keepalive could move confirmed_flush_lsn past Lc before
// anything was acked. A crash then would leave the position empty, the import
// would be applied again, and the slot check (F <= Lc) would refuse: a crash
// would become a stuck pipeline. The floor is F0, the slot's
// confirmed_flush_lsn at start (F0 <= Lc, checked at Open), so the report
// never moves the slot backwards on servers before the fix "Don't retreat
// slot's confirmed_flush LSN".
//
// Invariant 1: the cap only lowers what reportedPositions decided; the floor
// raises it at most to a position the slot already had.
func capImportFlush(write, flush, ceiling, floor pglogrepl.LSN) (pglogrepl.LSN, pglogrepl.LSN) {
	if flush > ceiling {
		flush = ceiling
	}
	flush = maxLSN(flush, floor)
	write = maxLSN(write, flush)
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
	pgConn, err := s.pgConn()
	if err != nil {
		return nil, err
	}
	msg, err := pgConn.ReceiveMessage(wctx)
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

	// Without a started stream there is no connection in replication mode to
	// talk on (startReplication released the old one), and using it
	// panicked with a nil pointer dereference (#346).
	if s.replicating && s.connUsable() {
		// SendStandbyCopyDone waits for the server's reply and ignores tctx,
		// so a hung server would keep Run, the connection and the slot busy
		// forever. A deadline on the socket bounds every step below.
		cancelDeadline := s.setConnDeadline(time.Now().Add(closeReplicationTimeout))
		if err := s.sentStandbyDone(tctx); err != nil {
			s.doneErr = errors.Join(s.doneErr, err)
		}
		cancelDeadline()
	}

	close(s.done)
}

// sentStandbyDone signals replication done and submits the last flushed LSN.
//
// The final status update MUST precede the CopyDone: once the server
// receives CopyDone the logical-replication stream is over and walsender
// stops processing further client messages, so a status update sent after it
// never advances confirmed_flush_lsn. That was the original (buggy) order
// here, and it was invisible until the B1 chaos harness asserted the slot's
// flush position after a halt (FM10): the final standby carried the marker
// LSN but the server never recorded it. In production the failure mode is
// benign (confirmed_flush lagging only means the server retains more WAL
// than it must), which is why it shipped unnoticed — but B1's escape hatch
// explicitly depends on the slot confirming the marker position (D2), so
// the order matters now.
func (s *Subscription) sentStandbyDone(ctx context.Context) error {
	var errs []error

	// send last status update while the stream is still active
	if err := s.sendStandbyStatusUpdate(ctx); err != nil {
		sdk.Logger(ctx).Error().
			Err(err).
			Msg("failed to send final status update")
		errs = append(errs, err)
	}
	// send copy done message indicating replication is done
	if err := s.sendStandbyCopyDone(ctx); err != nil {
		sdk.Logger(ctx).Error().
			Err(err).
			Msg("failed to send standby copy done")
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// beginRun marks Run active. It reports false if Teardown already ran: the
// connection is gone and Run must not touch it.
func (s *Subscription) beginRun() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.tornDown {
		return false
	}
	s.runActive = true
	return true
}

// endRun marks Run finished and, if Teardown was called while it was active,
// releases the connection Teardown left to it.
func (s *Subscription) endRun(started bool) {
	if !started {
		return
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.runActive = false
	if s.tornDown {
		s.releaseConnLocked()
	}
}

// markTornDown records that Teardown ran. If Run is not active it releases
// the connection and reports false; if Run is active it reports true and
// leaves the release to Run's exit (endRun).
func (s *Subscription) markTornDown() (runActive bool) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.tornDown = true
	if s.runActive {
		return true
	}
	s.releaseConnLocked()
	return false
}

// setConn stores the connection Run acquired.
func (s *Subscription) setConn(conn *pgxpool.Conn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.conn = conn
}

// releaseConn releases the pooled connection at most once and forgets it. Only
// Run calls it, while active.
func (s *Subscription) releaseConn() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.releaseConnLocked()
}

func (s *Subscription) releaseConnLocked() {
	if s.conn != nil {
		s.conn.Release()
		s.conn = nil
	}
}

// connUsable reports whether there is a connection that is still open. A
// connection that pgconn already closed (it closes the socket asynchronously
// after a failed read or write) must not be written to: that races the close
// and a deadline set here would overwrite the one pgconn set.
func (s *Subscription) connUsable() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn != nil && !s.conn.Conn().PgConn().IsClosed()
}

func (s *Subscription) hasConn() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn != nil
}

// pgConn returns the current connection's wire connection, or an error if
// there is none. Only Run's goroutine calls it; the connection stays valid
// while Run is active because nobody else releases it (see connMu).
func (s *Subscription) pgConn() (*pgconn.PgConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		return nil, errors.New("replication connection was released")
	}
	return s.conn.Conn().PgConn(), nil
}

// setConnDeadline sets a read/write deadline on the connection's socket and
// returns a function that clears it.
func (s *Subscription) setConnDeadline(t time.Time) (cancelDeadline func()) {
	pc, err := s.pgConn()
	if err != nil {
		return func() {}
	}
	nc := pc.Conn()
	_ = nc.SetDeadline(t)
	return func() { _ = nc.SetDeadline(time.Time{}) }
}

// StartErr returns why Run did not reach the receive loop, or nil. It is only
// valid after Ready() was closed.
func (s *Subscription) StartErr() error {
	return s.startErr
}

// logUnknownCopyData logs a copy data message the subscription does not
// handle. Only its size and type byte are logged, never the payload, which
// can carry row data.
func logUnknownCopyData(ctx context.Context, data []byte) {
	sdk.Logger(ctx).Trace().
		Int("length", len(data)).
		Uint8("type", data[0]).
		Msg("ignoring unknown copy data message")
}
