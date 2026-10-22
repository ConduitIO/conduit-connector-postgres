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

package logrepl

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CDCConfig holds configuration values for CDCIterator.
type CDCConfig struct {
	LSN pglogrepl.LSN
	// TxCommitLSN is the commit LSN of the checkpointed record's transaction,
	// 0 when the position does not carry one. With it, a restart skips
	// exactly what was delivered (#331).
	TxCommitLSN pglogrepl.LSN
	// TxSeq is the checkpointed change's ordinal within that transaction, 0
	// when the position does not carry one.
	TxSeq           uint64
	SlotName        string
	PublicationName string
	Tables          []string
	TableKeys       map[string]string
	WithAvroSchema  bool
	// BatchSize is the maximum size of a batch that will be read from the DB
	// in one go and processed by the CDCHandler.
	BatchSize int
	// StartPosition is the position the connector was started/resumed with. Its
	// DBZ-3 carry-forward fields (e.g. SnapshotLowWatermarkLSN) are threaded into
	// every CDC-mode position the handler emits so they survive the snapshot->CDC
	// handoff and every subsequent CDC restart. See CDCHandler.buildPosition.
	StartPosition position.Position
	// SchemaDriftPolicy is what the connector does on schema drift (halt by
	// default). Validated via ParseSchemaDriftPolicy in Config.Validate.
	SchemaDriftPolicy SchemaDriftPolicy
	// Import is the Debezium offset import (#341), nil outside import mode.
	// It is set only when the pipeline has no position; the caller has run
	// dbzoffset.Preflight.
	Import *dbzoffset.Import
	// AllowWALGap accepts the data loss of resuming from a saved position on
	// a replication slot that had to be created again (see checkSlotCoversPosition).
	AllowWALGap bool
	// IdentityCheckTimeout bounds the slot identity check's peek (#442);
	// zero means defaultIdentityCheckTimeout.
	IdentityCheckTimeout time.Duration
	// BeaconFailureTimeout is how long failing beacons may hold an idle slot
	// back (internal.Beacons.Abandoned); zero means the default.
	BeaconFailureTimeout time.Duration
}

// CDCIterator asynchronously listens for events from the logical replication
// slot and returns them to the caller through NextN.
type CDCIterator struct {
	config  CDCConfig
	sub     *internal.Subscription
	handler *CDCHandler

	// batchesCh is a channel shared between this iterator and a CDCHandler,
	// to which the CDCHandler is sending batches of records.
	// Using a shared queue here would be the fastest option. However,
	// we also need to watch for a context that can get cancelled,
	// and for the subscription that can end, so using a channel is
	// the best option at the moment.
	batchesCh chan []opencdc.Record

	// recordsForNextRead contains records from the previous batch (returned by the CDCHandler),
	// that weren't return by this iterator's ReadN method.
	recordsForNextRead []opencdc.Record

	// importDone is set by the first Ack in import mode, after which the
	// version 2 position rules and the import plays no part.
	importDone atomic.Bool

	// slotIdentity is the slot identity this run writes into its positions
	// and beacons (#442).
	slotIdentity string
	// walHistory is the server's WAL history this run writes into its
	// positions (#447 B3).
	walHistory internal.WALHistory
}

// afterSlotCheck is a test seam, nil in production. It runs between checkSlot
// and the subscription, the window in which a slot can vanish. It is only
// assigned in _test.go files (enforced by TestSeamsOnlyAssignedInTests).
var afterSlotCheck func()

// afterIdentityCheck is a test seam, nil in production. It runs between the
// slot identity check and the subscription, the window in which a slot can be
// swapped for another under the same name (#442).
var afterIdentityCheck func()

// afterSlotArmed is a test seam, nil in production. It runs after the slot
// was opened or created and stamped, before NewCDCIterator returns; an error
// fails the attempt there, as a crash before anything is saved would.
var afterSlotArmed func() error

// NewCDCIterator initializes logical replication by creating the publication and subscription manager.
func NewCDCIterator(ctx context.Context, pool *pgxpool.Pool, c CDCConfig) (*CDCIterator, error) {
	// Invariant 3: before anything is created, so a refusal leaves no slot
	// behind that a retry would take for a healthy one.
	if c.Import == nil {
		if err := checkSlot(ctx, pool, c); err != nil {
			return nil, err
		}
	}

	if afterSlotCheck != nil {
		afterSlotCheck()
	}

	server, err := internal.ReadServerInfo(ctx, pool)
	if err != nil {
		return nil, err
	}
	// Invariant 3 (#447 B3): an LSN means something only within one WAL
	// history. Checked before anything else looks at LSNs.
	history, err := internal.ReadWALHistory(ctx, pool)
	if err != nil {
		return nil, err
	}
	if c.Import == nil && hasSavedPosition(c) {
		if err := checkWALHistory(ctx, pool, c, history); err != nil {
			return nil, err
		}
	}
	c.StartPosition.SystemIdentifier, c.StartPosition.Timeline = history.SystemID, history.Timeline

	// Import mode never creates the publication (#341 S16): one created
	// after the slot's start makes pgoutput skip the earlier WAL of its
	// tables. The preflight checked that it exists and covers the tables.
	if c.Import == nil {
		if err := internal.CreatePublication(
			ctx,
			pool,
			c.PublicationName,
			internal.CreatePublicationOptions{Tables: c.Tables},
		); err != nil {
			// If creating the publication fails with code 42710, this means
			// the publication already exists.
			if !internal.IsPgDuplicateErr(err) {
				return nil, err
			}

			sdk.Logger(ctx).Warn().
				Msgf("Publication %q already exists.", c.PublicationName)
		}
	}

	ident, err := resolveSlotIdentity(ctx, pool, c, server)
	if err != nil {
		return nil, err
	}
	// Every position of this run carries the identity (the handler copies it
	// from its base position).
	c.StartPosition.SlotIdentity = ident.identity

	if afterIdentityCheck != nil {
		afterIdentityCheck()
	}

	// Using a buffered channel here so that the handler can send a batch
	// to the channel and start building a new batch.
	// This is useful when the first batch in the channel didn't reach BatchSize (which is sdk.batch.size).
	// The handler can prepare the next batch, and the CDCIterator can use them
	// to return the maximum number of records.
	batchesCh := make(chan []opencdc.Record, 1)
	handler := NewCDCHandler(
		ctx,
		internal.NewRelationSetWithEncoding(types.EncodingFor(c.WithAvroSchema)),
		c.TableKeys,
		batchesCh,
		c.WithAvroSchema,
		c.BatchSize,
		// todo make configurable
		time.Second,
		c.StartPosition,
		c.SchemaDriftPolicy,
	)

	newSub := subscriptionOpener(c)
	sub, err := newSub(
		ctx,
		pool,
		c.SlotName,
		c.PublicationName,
		c.Tables,
		c.LSN,
		handler.Handle,
	)
	// Before the error return: a slot created by a call that then failed is
	// a taken gap too (see internal.SlotCreatedError).
	warnIfSlotCreatedForSavedPosition(ctx, c, sub, err)
	if err != nil {
		return nil, subscriptionError(ctx, pool, c, err)
	}

	// rotate gives the run a new identity once it accepted a slot that is
	// not the saved position's (see slotIdentityCheck.rotated). It runs
	// before the stream starts, so the handler and the beacons are not in
	// use yet.
	rotate := func(id string) {
		handler.basePosition.SlotIdentity = id
		if sub.Beacons != nil {
			sub.Beacons.SetIdentity(id)
		}
	}
	if err := armSlotIdentity(ctx, pool, c, sub, ident, server, rotate); err != nil {
		_ = sub.Teardown(ctx)
		return nil, err
	}

	if key := (internal.ChangeKey{CommitLSN: c.TxCommitLSN, Seq: c.TxSeq}); key.Known() {
		// Exact resume point from a position that carries its change key
		// (format version 2). It uses the position's raw LSN, not the start
		// LSN CreateSubscription may have moved up to the slot's restart_lsn:
		// the key, not the LSN, decides. Without a key, CreateSubscription's
		// legacy point applies (see internal.ResumePoint). Set before Run.
		sub.Resume = internal.ResumePoint{Key: key, LSN: c.LSN}
	}
	if c.Import != nil {
		if err := applyImport(ctx, sub, c.Import); err != nil {
			_ = sub.Teardown(ctx)
			return nil, err
		}
	}
	// The handler stamps each record's position with the subscription's
	// current change key. Set before the subscription goroutine starts.
	handler.changeKey = sub.CurrentChange
	handler.txInfo = sub.CurrentTx

	return &CDCIterator{
		config:       c,
		batchesCh:    batchesCh,
		sub:          sub,
		handler:      handler,
		slotIdentity: ident.identity,
		walHistory:   history,
	}, nil
}

// subscriptionOpener picks how NewCDCIterator gets its subscription: creating
// the slot only for a fresh start (or the explicit allowWALGap opt-in).
func subscriptionOpener(c CDCConfig) func(context.Context, *pgxpool.Pool, string, string, []string, pglogrepl.LSN, internal.Handler) (*internal.Subscription, error) {
	newSub := internal.CreateSubscription
	switch {
	case c.Import != nil:
		// Import mode never creates the slot: a slot created now would start
		// at the current WAL and miss everything since Debezium's offset.
		newSub = internal.OpenSubscription
	case hasSavedPosition(c) && !c.AllowWALGap:
		// Invariant 3: a saved position never creates a slot. checkSlot
		// already looked, but a slot dropped after that read must not be
		// recreated at the current WAL either.
		newSub = internal.OpenSubscription
	}
	return newSub
}

// warnIfSlotCreatedForSavedPosition logs the accepted data loss when the call
// created the slot although the config carries a saved position, whether the
// call then succeeded or failed.
func warnIfSlotCreatedForSavedPosition(ctx context.Context, c CDCConfig, sub *internal.Subscription, err error) {
	// A slot was created in this call only if CREATE_REPLICATION_SLOT
	// succeeded (internal.CreatedSlot). With a saved position that is the
	// accepted gap (allowWALGap is the only way here), including a slot
	// dropped after checkSlot and one created by a call that then failed: the
	// retry would find a healthy slot and log nothing.
	//
	// A third party that drops the slot and creates it again under the same
	// name between checkSlot and our CREATE makes the create fail with a
	// duplicate error, so nothing is warned here. That slot is caught by the
	// slot identity (#442): VerifyHeld sees its confirmed_flush_lsn past the
	// one the identity check accepted (armSlotIdentity).
	if c.Import != nil || !hasSavedPosition(c) {
		return
	}
	if point, created := internal.CreatedSlot(sub, err); created {
		warnGapAccepted(ctx, c, point)
	}
}

// subscriptionError maps the error of opening or creating the subscription
// to the coded error the operator acts on.
func subscriptionError(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, err error) error {
	if !errors.Is(err, internal.ErrMissingSlot) {
		return fmt.Errorf("failed to initialize subscription: %w", err)
	}
	if c.Import != nil {
		// The slot went away after the preflight. Import mode never
		// creates one: say so with the import's code.
		return &dbzoffset.Error{
			Code: dbzoffset.CodeSlotMissing, Path: slotNamePath,
			Message: fmt.Sprintf("replication slot %q does not exist (it was dropped after the preflight)", c.SlotName),
			Fix:     "Go back to Debezium with the manual runbook, gated by `conduit migrate kafka-connect --check-rollback`, then redo the cutover. Import mode never creates a slot.",
		}
	}
	return newSlotGapError(c.SlotName, slotGapPositions{saved: describeSavedPosition(c), newSlotStart: currentWAL(ctx, pool)})
}

// resolveSlotIdentity decides the slot identity of this run and, for a saved
// position, checks that the existing slot is the position's slot.
//
// Invariant 3 (#442): a slot that exists and is healthy can still be a
// different slot under the same name, created after the pipeline stopped.
// Checked here, before the subscription takes the slot, and again once the
// stream holds it (armSlotIdentity's VerifyHeld), for a slot swapped in
// between.
func resolveSlotIdentity(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, server internal.ServerInfo) (slotIdentityCheck, error) {
	if c.Import != nil || !hasSavedPosition(c) {
		// A fresh start or an import: a new identity history begins here
		// (none on a server that cannot carry beacons).
		if !server.BeaconsSupported() {
			return slotIdentityCheck{}, nil
		}
		return slotIdentityCheck{identity: internal.NewSlotIdentity()}, nil
	}
	ident, err := checkSlotIdentity(ctx, pool, c, server)
	if err != nil {
		return ident, err
	}
	switch ident.outcome {
	case identityCovered:
		// #447 re-review S2: covered only if the bound still names the
		// position's transaction in this WAL.
		if err := verifyCoveredTransaction(ctx, pool, c); err != nil {
			return ident, err
		}
	case identityUnprovable:
		if !c.AllowWALGap {
			return ident, newSlotUnprovableError(c.SlotName, describeSavedPosition(c), ident.confirmed, server)
		}
		warnReplacedAccepted(ctx, c, ident.confirmed)
		ident.rotate()
	case identityReplaced:
		if !c.AllowWALGap {
			return ident, newSlotReplacedError(c.SlotName, describeSavedPosition(c), ident.confirmed)
		}
		warnReplacedAccepted(ctx, c, ident.confirmed)
		ident.rotate()
	case identityNotChecked:
		// The slot is missing: this run either refuses (no allowWALGap) or
		// accepts a slot that is not the position's (it creates one, or one
		// came back). Either way the new slot must not carry the position's
		// identity.
		ident.rotate()
	}
	logSlotIdentity(ctx, c, ident)
	if !server.BeaconsSupported() {
		// This server cannot keep a beacon ahead of the slot, so this run's
		// positions carry no identity: a later start must not mistake the
		// unprotected stretch for a proven one.
		ident.identity = ""
	}
	return ident, nil
}

// armSlotIdentity finishes the slot identity work once the subscription
// opened (or created) the slot: the gap and replacement warnings, the check
// once the stream holds the slot, and the run's first beacon.
func armSlotIdentity(ctx context.Context, pool *pgxpool.Pool, c CDCConfig, sub *internal.Subscription, ident slotIdentityCheck, server internal.ServerInfo, rotate func(string)) error {
	saved := c.Import == nil && hasSavedPosition(c)
	if saved {
		// A third party that drops the slot and creates it again under the
		// same name between checkSlot and our CREATE makes the create fail
		// with a duplicate error, so TXSnapshotID stays empty. That slot is
		// caught by VerifyHeld: its confirmed_flush_lsn is past the one the
		// identity check accepted.
		switch {
		case sub.TXSnapshotID != "":
			// Created by this call: warned by warnIfSlotCreatedForSavedPosition.
		case ident.outcome == identityNotChecked:
			// The slot was missing when its identity was checked, and someone
			// created it again before the subscription opened it: it cannot
			// be the position's slot.
			if !c.AllowWALGap {
				return newSlotReplacedError(c.SlotName, describeSavedPosition(c), sub.ConfirmedFlushLSN)
			}
			warnReplacedAccepted(ctx, c, sub.ConfirmedFlushLSN)
		}
	}
	limit := heldConfirmedLimit(sub, ident)
	if server.BeaconsSupported() && ident.identity != "" {
		sub.Beacons = internal.NewBeacons(pool, c.SlotName, ident.identity)
		sub.Beacons.SetPreStreamLimit(limit)
		sub.Beacons.SetFailureTimeout(c.BeaconFailureTimeout)
		warnEmitMessagePublic(ctx, pool, server.Version)
		sub.StreamStarted = streamHolderHook(pool, c.SlotName, server.Version, sub.Beacons)
	}
	// Invariant 3 (#442, #447 B2): VerifyHeld runs on every start but an
	// import (which has checkImportSlot): the slot is not held by anyone
	// between this point and the stream start, which can be hours later
	// after a snapshot, and on Postgres 17 not even right after CREATE. A
	// slot replaced in that window has a confirmed_flush_lsn past limit.
	if c.Import == nil {
		sub.VerifyHeld = verifyHeldSlot(c, limit, sub.Beacons, false, server.Version, rotate)
	}
	// Invariant 3 (#442): stamp the slot with this run's identity. The stamp
	// is the beacon a restart finds before the next one is written, and it
	// fails the start loudly when the role may not write beacons, instead of
	// an idle slot that silently never advances. Emit's guard writes it only
	// while the slot is inactive and not past limit, so a stamp can never
	// land in a slot that replaced this run's (#447 B1).
	//
	// Every start stamps now, which matters for a snapshot that runs for
	// hours before the stream starts. The guard makes that safe for a slot
	// swapped in after the identity check too (the first version stamped a
	// position's existing slot only inside VerifyHeld for that reason): such
	// a slot is past limit, so the stamp is refused and the start with it.
	if sub.Beacons != nil {
		err := sub.Beacons.Emit(ctx)
		switch {
		case errors.Is(err, internal.ErrSlotInUse) && sub.VerifyHeld != nil,
			errors.Is(err, internal.ErrSlotNotHeld) && c.AllowWALGap && sub.VerifyHeld != nil:
			// Another connection streams from the slot (the stream start
			// fails on it with its own error), or the slot was swapped after
			// the check and allowWALGap accepts that: stamp once the stream
			// holds the slot, after VerifyHeld warned and gave the accepted
			// slot an identity of its own.
			sub.VerifyHeld = verifyHeldSlot(c, limit, sub.Beacons, true, server.Version, rotate)
		case err != nil:
			return beaconError(c, server.Version, err)
		}
	}
	if afterSlotArmed != nil {
		return afterSlotArmed()
	}
	return nil
}

// streamHolderHook returns Subscription.StreamStarted for beacons: it
// records which walsender holds the slot, by PID and backend_start, or
// refuses to stream (#447 B1, re-review N1 and N2).
func streamHolderHook(pool *pgxpool.Pool, slot string, serverVersion int, beacons *internal.Beacons) func(context.Context, uint32, error) error {
	return func(ctx context.Context, pid uint32, err error) error {
		if err == nil {
			var start time.Time
			if start, err = internal.WalsenderStart(ctx, pool, pid); err == nil {
				beacons.SetStreamHolder(pid, start)
				return nil
			}
		}
		e := newSlotBeaconError(slot, serverVersion,
			fmt.Errorf("could not identify this run's walsender, so no beacon could be written safely: %w", err))
		// Not a permission problem: a role always sees its own backends in
		// pg_stat_activity, so this is the connection or the server.
		e.Fix = "Check the connection to the server and its logs; the start reads the replication " +
			"connection's backend PID and pg_stat_activity.backend_start. Restart the pipeline once the server is reachable"
		return e
	}
}

// heldConfirmedLimit is the highest confirmed_flush_lsn the slot may have
// until the stream holds it: what the identity check accepted, or, for a
// slot this run created or a fresh start, what the subscription read right
// after creating or opening it. Nothing holds the slot in between (on
// Postgres 17 it is inactive right after CREATE_REPLICATION_SLOT), and a slot
// that replaced it was created later, so its confirmed_flush_lsn is past
// this.
func heldConfirmedLimit(sub *internal.Subscription, ident slotIdentityCheck) pglogrepl.LSN {
	if sub.TXSnapshotID == "" && ident.confirmed != 0 {
		return ident.confirmed
	}
	return sub.ConfirmedFlushLSN
}

// beaconError maps a failed stamp: a slot that is not this run's any more is
// refused like a replaced one, anything else is slot_beacon_failed.
func beaconError(c CDCConfig, serverVersion int, err error) error {
	if errors.Is(err, internal.ErrSlotNotHeld) {
		e := newSlotReplacedError(c.SlotName, describeSavedPosition(c), 0)
		e.Message = fmt.Sprintf("replication slot %q changed after this start checked it (%v): it was dropped and created again, "+
			"or another client moved it. Common causes: a third party recreating the slot, another pipeline or tool consuming it", c.SlotName, err)
		return e
	}
	return newSlotBeaconError(c.SlotName, serverVersion, err)
}

// verifyHeldSlot returns the check run once the stream holds the slot
// (Subscription.VerifyHeld). With stamp, it also writes the run's first
// beacon once the slot is accepted.
//
// Invariant 3 (#442): nothing but this run can move the slot between the
// identity check and the stream start, and this run sends nothing before the
// check. A confirmed_flush_lsn past limit means the slot was dropped and
// created again (or moved by another client) in between.
func verifyHeldSlot(c CDCConfig, limit pglogrepl.LSN, beacons *internal.Beacons, stamp bool, serverVersion int, rotate func(string)) func(context.Context, internal.ReadReplicationSlotResult) error {
	return func(ctx context.Context, slot internal.ReadReplicationSlotResult) error {
		if slot.ConfirmedFlushLSN > limit {
			if !c.AllowWALGap {
				return newSlotReplacedError(c.SlotName, describeSavedPosition(c), slot.ConfirmedFlushLSN)
			}
			warnReplacedAccepted(ctx, c, slot.ConfirmedFlushLSN)
			// The accepted slot gets an identity of its own, stamped now.
			rotate(internal.NewSlotIdentity())
			stamp = true
		}
		if beacons != nil && stamp {
			if err := beacons.Emit(ctx); err != nil {
				return beaconError(c, serverVersion, err)
			}
		}
		return nil
	}
}

// applyImport replaces the subscription's resume point with the import's
// (#341 D4, D6). The import writes no position: this is rebuilt from the
// setting at every start with an empty position, so a crash before the
// first ack rebuilds the same point (invariant 2).
func applyImport(ctx context.Context, sub *internal.Subscription, imp *dbzoffset.Import) error {
	plan := imp.Plan
	// Invariant 3 (D5 check 5): Postgres decodes from the slot's
	// confirmed_flush_lsn. The preflight checked F <= Lc; the slot was read
	// again by OpenSubscription, and is checked again once streaming holds
	// it (Subscription.checkImportSlot).
	if sub.ConfirmedFlushLSN > plan.SkipCommitsBelow {
		return &dbzoffset.Error{
			Code: dbzoffset.CodeSlotAhead, Path: slotNamePath,
			Message: fmt.Sprintf("replication slot %q moved to confirmed_flush_lsn %s, past Debezium's lsn_commit %s",
				sub.SlotName, sub.ConfirmedFlushLSN, plan.SkipCommitsBelow),
			Fix: "Something else consumed Conduit's slot. Stop it, go back to Debezium with the manual runbook (`conduit migrate kafka-connect --check-rollback`), then cut over again.",
		}
	}
	commitLSN, seq, _ := plan.ResumeKey() // ParseSetting guarantees Lc != 0
	// Invariant 3 (base rule): skip every transaction whose commit LSN is
	// below Lc, as the exact rule with the key (Lc-1, MaxUint64).
	sub.Resume = internal.ResumePoint{
		Key: internal.ChangeKey{CommitLSN: commitLSN, Seq: seq},
		LSN: plan.SkipCommitsBelow,
	}
	sub.Import = dbzoffset.NewFilter(plan)
	// Invariant 1 (D6): until the first ack, never report a flush position
	// past Lc, so a crash before it can re-apply the same import.
	sub.ImportCeiling = plan.SkipCommitsBelow
	sub.ImportFloor = sub.ConfirmedFlushLSN

	ev := sdk.Logger(ctx).Info().
		Str("connector", imp.Setting.Connector).
		Str("strategy", "fresh").
		Stringer("lsn_commit", plan.SkipCommitsBelow).
		Stringer("lsn", plan.Offset.LSN).
		Str("message_type", string(plan.Offset.MessageType)).
		Stringer("case", plan.Case).
		Stringer("slot_confirmed_flush_lsn", sub.ConfirmedFlushLSN).
		Stringer("slot_restart_lsn", sub.RestartLSN).
		Str("expected_duplicates", plan.ExpectedDuplicates())
	if plan.Partial != nil {
		ev = ev.Uint32("partial_xid", plan.Partial.Xid).
			Stringer("partial_skip_changes_below", plan.Partial.SkipChangesBelow)
	}
	ev.Msg("debezium offset import: resuming from Debezium's committed offset (skip commits below lsn_commit)")
	return nil
}

// StartSubscriber starts the logical replication service in the background.
// Blocks until the subscription becomes ready.
func (i *CDCIterator) StartSubscriber(ctx context.Context) error {
	sdk.Logger(ctx).Info().
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msgf("Starting logical replication at %s", i.sub.StartLSN)

	go func() {
		if err := i.sub.Run(ctx); err != nil {
			sdk.Logger(ctx).Error().
				Err(err).
				Msg("replication exited with an error")
		}
	}()

	<-i.sub.Ready()
	if err := i.sub.StartErr(); err != nil {
		return fmt.Errorf("logical replication did not start: %w", err)
	}

	sdk.Logger(ctx).Info().
		Str("slot", i.config.SlotName).
		Str("publication", i.config.PublicationName).
		Msg("Logical replication started")

	return nil
}

// NextN returns up to n records from the internal channel with records.
// NextN is allowed to block until either at least one record is available
// or the context gets canceled.
func (i *CDCIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if !i.subscriberReady() {
		return nil, errors.New("logical replication has not been started")
	}

	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0, got %d", n)
	}

	// D3 step 4: once the marker's ack has armed the halt, NextN surfaces the
	// terminal drift error instead of records — including any records from a
	// previous batch, which cannot exist past the marker (the marker is the
	// last record emitted, D4). The check is on entry so the error is
	// deterministic regardless of how many records the caller asks for.
	if err := i.handler.driftHaltError(); err != nil {
		return nil, err
	}

	// First, we check if there are any records from the previous batch
	// that we can start with. The capacity hint is n, but the leftover from a
	// previous batch can exceed n when the batch size is larger than the
	// caller's requested count, so clamp the capacity to at least the leftover
	// length (make with cap < len panics).
	capHint := n
	if len(i.recordsForNextRead) > capHint {
		capHint = len(i.recordsForNextRead)
	}
	recs := make([]opencdc.Record, len(i.recordsForNextRead), capHint)
	copy(recs, i.recordsForNextRead)
	i.recordsForNextRead = nil

	// NextN needs to wait until at least 1 record is available.
	if len(recs) == 0 {
		batch, err := i.nextRecordsBatchBlocking(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch next batch of records (blocking): %w", err)
		}
		recs = batch
	}

	// We add any already available batches (i.e., we're not blocking waiting for any new batches to arrive)
	// to return at most n records.
	for len(recs) < n {
		batch, err := i.nextRecordsBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch next batch of records: %w", err)
		}
		if batch == nil {
			break
		}
		recs = i.appendRecordsWithLimit(recs, batch, n)
	}

	sdk.Logger(ctx).Trace().
		Int("records", len(recs)).
		Int("records_for_next_read", len(i.recordsForNextRead)).
		Msg("CDCIterator.NextN returning records")
	return recs, nil
}

// nextRecordsBatchBlocking waits for the next batch of records to arrive,
// or for the context to be done, or for the subscription to be done,
// whichever comes first.
func (i *CDCIterator) nextRecordsBatchBlocking(ctx context.Context) ([]opencdc.Record, error) {
	if i.handler.driftMarkerPending() {
		// F6 / AC4: while a drift marker is pending, prefer the marker batch
		// over a racing sub.Done (the select would otherwise choose randomly
		// among ready cases) so the subscription dying can never strand the
		// marker — the halt is acked-gated (D3), and a stranded marker means
		// the approval checkpoint is never persisted. Also select the halt
		// channel: if the marker's ack arms the halt while this call is
		// blocked, the terminal error must surface here rather than leaving
		// the caller blocked forever.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case batch := <-i.batchesCh:
			sdk.Logger(ctx).Trace().
				Int("records", len(batch)).
				Msg("CDCIterator.NextN received batch of records (blocking)")
			return batch, nil
		case <-i.handler.driftHaltCh:
			if err := i.handler.driftHaltError(); err != nil {
				return nil, err
			}
			return nil, errors.New("drift halt channel closed without an armed halt (this smells like a bug)")
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-i.sub.Done():
		if err := i.sub.Err(); err != nil {
			return nil, fmt.Errorf("logical replication error: %w", err)
		}
		if err := ctx.Err(); err != nil {
			// subscription is done because the context is canceled, we went
			// into the wrong case by chance
			return nil, err
		}
		// subscription stopped without an error and the context is still
		// open, this is a strange case, shouldn't actually happen
		return nil, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
	case batch := <-i.batchesCh:
		sdk.Logger(ctx).Trace().
			Int("records", len(batch)).
			Msg("CDCIterator.NextN received batch of records (blocking)")
		return batch, nil
	}
}

func (i *CDCIterator) nextRecordsBatch(ctx context.Context) ([]opencdc.Record, error) {
	if i.handler.driftMarkerPending() {
		// F6 / AC4, non-blocking variant: while a marker is pending, a dead
		// subscription must not surface an error that would discard the
		// marker the caller already holds (the NextN top-up loop returns an
		// error immediately, dropping recs). Treat it as no-more-records so
		// the caller returns the marker batch it got.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case batch := <-i.batchesCh:
			sdk.Logger(ctx).Trace().
				Int("records", len(batch)).
				Msg("CDCIterator.NextN received batch of records")
			return batch, nil
		default:
			return nil, nil
		}
	}

	select {
	case <-ctx.Done():
		// Return what we have with the error
		return nil, ctx.Err()
	case <-i.sub.Done():
		if err := i.sub.Err(); err != nil {
			return nil, fmt.Errorf("logical replication error: %w", err)
		}
		if err := ctx.Err(); err != nil {
			// Return what we have with the context error
			return nil, err
		}
		// Return what we have with subscription stopped error
		return nil, fmt.Errorf("subscription stopped, no more data to fetch (this smells like a bug)")
	case batch := <-i.batchesCh:
		sdk.Logger(ctx).Trace().
			Int("records", len(batch)).
			Msg("CDCIterator.NextN received batch of records")

		return batch, nil
	default:
		// No more records currently available
		return nil, nil
	}
}

// appendRecordsWithLimit appends records to dst from src, until the given limit is reached,
// or all records from src have been moved.
// If some records from src are not moved (probably because they lack emotions),
// they are saved to recordsForNextRead.
func (i *CDCIterator) appendRecordsWithLimit(dst []opencdc.Record, src []opencdc.Record, limit int) []opencdc.Record {
	if len(src) == 0 || len(dst) > limit {
		return src
	}

	needed := limit - len(dst)
	if needed > len(src) {
		needed = len(src)
	}

	dst = append(dst, src[:needed]...)
	i.recordsForNextRead = src[needed:]

	return dst
}

// Ack forwards the acknowledgment to the subscription.
func (i *CDCIterator) Ack(ctx context.Context, sdkPos opencdc.Position) error {
	pos, err := position.ParseSDKPosition(sdkPos)
	if err != nil {
		return err
	}

	if pos.Type != position.TypeCDC {
		return fmt.Errorf("invalid type %q for CDC position", pos.Type.String())
	}

	lsn, err := pos.LSN()
	if err != nil {
		return err
	}

	if lsn == 0 {
		return fmt.Errorf("cannot ack zero position")
	}

	commit, err := pos.TxCommit()
	if err != nil {
		return err
	}
	key := internal.ChangeKey{CommitLSN: commit, Seq: pos.TxSeq}
	i.sub.Ack(lsn, key)

	// The engine calls Ack only after the position is durable, so this is
	// the import's durable completion signal (#341 S8).
	if i.config.Import != nil && i.importDone.CompareAndSwap(false, true) {
		sdk.Logger(ctx).Info().
			Stringer("tx_commit_lsn", commit).
			Uint64("tx_seq", pos.TxSeq).
			Str("last_lsn", pos.LastLSN).
			Msgf("import complete at (%s, %d); remove logrepl.import.debeziumOffset", commit, pos.TxSeq)
	}

	// D3 step 3: arming is acked-gated, never sighting-gated — the halt
	// surfaces only once the engine acked the marker (or anything past it),
	// proving the checkpoint the marker carries is durable. The marker is
	// recognized by its change key, not its LSN (see maybeArmDriftHalt). This
	// is the boundary the escape hatch depends on: the ack moves the slot's
	// confirmed_flush_lsn to exactly the point the connector has seen and no
	// further, and the engine's persisted position is the operator's approval.
	i.handler.maybeArmDriftHalt(lsn, key, pos)

	return nil
}

// Teardown stops the CDC subscription and blocks until the subscription is done
// or the context gets canceled. If the subscription stopped with an unexpected
// error, the error is returned.
func (i *CDCIterator) Teardown(ctx context.Context) error {
	if i.handler != nil {
		i.handler.stopMarkerWarning()
	}
	if i.sub != nil {
		return i.sub.Teardown(ctx)
	}

	return nil
}

// subscriberReady returns true when the subscriber is running.
func (i *CDCIterator) subscriberReady() bool {
	select {
	case <-i.sub.Ready():
		return true
	default:
		return false
	}
}

// SlotIdentity returns the slot identity this run carries in its positions
// (#442).
func (i *CDCIterator) SlotIdentity() string {
	return i.slotIdentity
}

// TXSnapshotID returns the transaction snapshot which is received
// when the replication slot is created. The value can be empty, when the
// iterator is resuming.
func (i *CDCIterator) TXSnapshotID() string {
	return i.sub.TXSnapshotID
}

// LowWatermarkLSN returns the replication slot's restart_lsn captured when the
// slot was read at subscription creation (DBZ-3 Area 1). It is only the snapshot
// low watermark — the consistent point the initial snapshot is correlated with —
// when this run freshly created the slot, i.e. when TXSnapshotID() is non-empty.
// On a resume it reflects the slot's current restart_lsn and must not be used as
// the watermark; the persisted position's SnapshotLowWatermarkLSN is authoritative
// in that case. See CombinedIterator for the gating.
func (i *CDCIterator) LowWatermarkLSN() pglogrepl.LSN {
	return i.sub.RestartLSN
}

// SetSnapshotLowWatermarkLSN re-seeds the SnapshotLowWatermarkLSN that the
// handler carries forward onto every CDC-mode position it builds (DBZ-3 Area 1,
// acceptance criterion 11).
//
// It exists to fix the in-run snapshot->CDC handoff: NewCDCIterator seeds the
// handler's base position once, at construction, from the position the connector
// started with. On a first run that start position has no watermark (it is
// captured only when the slot is created, which happens inside NewCDCIterator
// itself), so without this call the watermark would ride snapshot records but be
// silently dropped the instant CDC took over — criterion 11 would hold only for
// the resumed case, not the first-run same-run handoff. CombinedIterator calls
// this at the handoff (useCDCIterator) with the effective watermark.
//
// Concurrency: this MUST be called before StartSubscriber. The handler's base
// position is only read on the single subscription goroutine (via Handle), which
// does not run until StartSubscriber launches it, so re-seeding beforehand needs
// no locking — the same single-writer-before-start discipline the base position
// relied on at construction.
func (i *CDCIterator) SetSnapshotLowWatermarkLSN(lsn string) {
	i.handler.setBasePositionLowWatermark(lsn)
}
