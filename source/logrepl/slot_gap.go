// Copyright © 2026 Meroxa, Inc.
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

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Code is a stable, machine-actionable error code. Codes are a public
// contract: a code is never renamed or reused for a different cause.
type Code string

const (
	// CodeSlotWALGap: the pipeline has a saved position and its replication
	// slot does not exist, so a new slot would skip every change in between.
	CodeSlotWALGap Code = "postgres.source.slot_wal_gap"
	// CodeSlotInvalidated: the slot exists but Postgres invalidated it (its
	// WAL is gone), so it cannot be streamed from. It has its own code
	// because the recovery differs (the slot must be dropped first) and
	// because it applies to a fresh start too; logrepl.allowWALGap does not
	// apply.
	CodeSlotInvalidated Code = "postgres.source.slot_invalidated"
)

// slotNamePath is the config path of the slot setting, which slot errors point at.
const slotNamePath = "logrepl.slotName"

// SlotError is a refusal to start because of the state of the replication
// slot. Its message starts with the code, like every coded error in this
// connector. Callers that keep the value use errors.As or CodeOf.
type SlotError struct {
	// Code is the stable code.
	Code Code
	// Path is the config path the error is about.
	Path string
	// Message says what is wrong, with the slot, the positions and the cause.
	Message string
	// Fix says what to do about it.
	Fix string
}

func (e *SlotError) Error() string {
	s := fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Path)
	if e.Fix != "" {
		s += ". Fix: " + e.Fix
	}
	return s
}

// CodeOf returns the code of the first *SlotError in err's chain, or "".
func CodeOf(err error) Code {
	var e *SlotError
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// slotGapPositions carries the two positions a gap error names.
type slotGapPositions struct{ saved, newSlotStart string }

func newSlotGapError(slot string, p slotGapPositions) *SlotError {
	return &SlotError{
		Code: CodeSlotWALGap,
		Path: slotNamePath,
		Message: fmt.Sprintf("replication slot %q does not exist, but the pipeline has a saved position (%s). "+
			"A new slot would start at WAL %s, so every change between the two would be skipped and lost. "+
			"Common causes: the slot was dropped by hand, removed by logrepl.autoCleanup while the position was kept, "+
			"invalidated and dropped, or not carried over by a failover or promotion to another server "+
			"(slots are not copied to a standby before Postgres 17, or without failover=true)",
			slot, p.saved, p.newSlotStart),
		Fix: "If you changed logrepl.slotName, set it back. Otherwise either re-snapshot (stop the pipeline, delete this " +
			"source connector and create it again so it has no position; with snapshotMode initial every table is read " +
			"again and every row is delivered again), or accept the loss by setting `logrepl.allowWALGap: true` " +
			"(DATA LOSS: the skipped changes are never delivered, so repair the destination another way) and remove " +
			"the setting after the pipeline has resumed",
	}
}

func newSlotInvalidatedError(slot string, h internal.SlotHealth) *SlotError {
	detail := "wal_status=" + h.WALStatus
	if h.InvalidationReason != "" {
		detail += ", invalidation_reason=" + h.InvalidationReason
	}
	return &SlotError{
		Code: CodeSlotInvalidated,
		Path: slotNamePath,
		Message: fmt.Sprintf("replication slot %q exists but Postgres invalidated it (%s): the WAL it needs has been removed, "+
			"so it cannot be streamed from and the changes it held are gone. Common causes: max_slot_wal_keep_size, "+
			"or a long outage of the pipeline", slot, detail),
		Fix: fmt.Sprintf("Drop the slot (`SELECT pg_drop_replication_slot('%s')`), then re-snapshot: stop the pipeline, delete this "+
			"source connector and create it again so it has no position (a pipeline with no position only needs the slot dropped). "+
			"With snapshotMode initial every row is delivered again. logrepl.allowWALGap does not apply: an invalidated "+
			"slot cannot be streamed from", slot),
	}
}

// hasSavedPosition reports whether the config resumes from a saved position,
// which implies a slot that still covers it.
func hasSavedPosition(c CDCConfig) bool {
	return c.StartPosition.ImpliesSlot() || c.LSN != 0 || c.TxCommitLSN != 0
}

// checkSlot refuses to start on a slot that cannot be used.
//
// Invariant 3: a missing slot meets a saved position here and in
// NewCDCIterator's OpenSubscription, which never creates one. Without that,
// CreateSubscription creates the slot at the current WAL and the subscription
// resets its start LSN to the new slot's restart_lsn, which drops every change
// in between without a word.
//
//   - An invalidated slot (wal_status lost, or an invalidation_reason) is
//     refused for every start, fresh or not. It cannot be streamed from, and
//     its restart_lsn may be NULL.
//   - A missing slot with a saved position is refused unless AllowWALGap.
//
// It runs before anything is created, for the friendly message and so a
// refusal leaves no slot behind. The decision itself does not rest on this
// read: with a saved position the subscription is opened without creating the
// slot, so a slot dropped after this read is refused there too.
//
// It deliberately looks only at whether the slot exists and is usable. A
// healthy slot's confirmed_flush_lsn is routinely past the saved record LSN:
// the connector itself reports the server's WAL end once everything is acked.
// Whether an existing slot is the one the position was taken from is
// checkSlotIdentity's question (#442).
func checkSlot(ctx context.Context, pool *pgxpool.Pool, c CDCConfig) error {
	health, err := internal.ReadSlotHealth(ctx, pool, c.SlotName)
	switch {
	case err == nil:
		if health.Invalidated() {
			return newSlotInvalidatedError(c.SlotName, health)
		}
		return nil
	case !errors.Is(err, internal.ErrMissingSlot):
		return fmt.Errorf("failed to check replication slot %q: %w", c.SlotName, err)
	}

	if !hasSavedPosition(c) {
		return nil // fresh start: a new slot is exactly what is wanted
	}
	if c.AllowWALGap {
		// The DATA LOSS warning is logged by warnGapAccepted once the slot
		// has actually been created (NewCDCIterator), not here: a slot that
		// reappears between this read and the create loses nothing, and one
		// that vanishes after this read is created without passing here.
		return nil
	}
	return newSlotGapError(c.SlotName, slotGapPositions{saved: describeSavedPosition(c), newSlotStart: currentWAL(ctx, pool)})
}

// warnGapAccepted logs the accepted data loss of logrepl.allowWALGap. It is
// called only when CreateSubscription really created the slot for a saved
// position, so it fires exactly once per gap taken, whether the slot was
// already missing at the check or was dropped after it, and whether the call
// then succeeded or failed.
func warnGapAccepted(ctx context.Context, c CDCConfig, consistentPoint string) {
	sdk.Logger(ctx).Warn().
		Str("code", string(CodeSlotWALGap)).
		Str("slot", c.SlotName).
		Str("saved_position", describeSavedPosition(c)).
		Str("new_slot_consistent_point", consistentPoint).
		Msg("DATA LOSS ACCEPTED (logrepl.allowWALGap): the replication slot was gone and has been created again; " +
			"every change between the saved position and the new slot's start is skipped and will not be delivered")
}

func describeSavedPosition(c CDCConfig) string {
	p := c.StartPosition
	switch {
	case c.TxCommitLSN != 0:
		return fmt.Sprintf("last acked transaction committed at LSN %s", c.TxCommitLSN)
	case c.LSN != 0:
		return fmt.Sprintf("last acked change at LSN %s", c.LSN)
	case p.SnapshotLowWatermarkLSN != "":
		return fmt.Sprintf("snapshot in progress, taken from a slot that started at LSN %s", p.SnapshotLowWatermarkLSN)
	default:
		return "snapshot in progress"
	}
}

// currentWAL returns where a slot created now would start, for the error
// message only. A standby, or a failed query, gives "unknown".
func currentWAL(ctx context.Context, pool *pgxpool.Pool) string {
	var lsn string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return "unknown"
	}
	return lsn
}
