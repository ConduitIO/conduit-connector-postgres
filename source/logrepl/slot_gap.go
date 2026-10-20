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

// CodeSlotGap is the stable error code for a resume whose replication slot no
// longer exists. It is a public contract: it is never renamed or reused.
const CodeSlotGap = "postgres.slot.wal_gap"

// slotNamePath is the config path of the slot setting, which slot errors point at.
const slotNamePath = "logrepl.slotName"

// SlotGapError is returned by Open when the source has a saved position but
// its replication slot is gone. Postgres keeps WAL for a change only while a
// slot needs it, so a slot created now starts at the current WAL: every change
// between the saved position and that point would be skipped, and the pipeline
// would carry on as if nothing happened. The message starts with the code, like
// every coded error in this connector.
type SlotGapError struct {
	// Code is CodeSlotGap.
	Code string
	// Path is the config path the error is about.
	Path string
	// Slot is the missing replication slot.
	Slot string
	// SavedPosition describes where the pipeline stopped (an LSN, or the
	// progress of an unfinished snapshot).
	SavedPosition string
	// NewSlotStart is the WAL position a new slot would start at, or "unknown".
	NewSlotStart string
}

func (e *SlotGapError) Error() string {
	return fmt.Sprintf("%s: replication slot %q does not exist, but the pipeline has a saved position (%s); "+
		"a new slot would start at WAL %s, so every change between the two would be skipped and lost (%s). "+
		"Fix: either re-snapshot (stop the pipeline, delete this source connector and create it again so it has no "+
		"position; with snapshotMode initial every table is read again and every row is delivered again), "+
		"or accept the loss by setting `logrepl.allowWALGap: true` (DATA LOSS: the skipped changes are never "+
		"delivered, so repair the destination another way) and remove the setting after the pipeline has resumed",
		e.Code, e.Slot, e.SavedPosition, e.NewSlotStart, e.Path)
}

// checkSlotCoversPosition refuses to start from a saved position when the
// slot is missing and would be created now.
//
// Invariant 3: this is the only place a missing slot meets a saved position.
// Without it CreateSubscription creates the slot at the current WAL and the
// subscription resets its start LSN to the new slot's restart_lsn, which drops
// every change in between without a word.
//
// It checks before the slot is created, not after: a check that ran after
// creation would leave a slot behind, and the next attempt would find it and
// start without complaint.
//
// It deliberately looks only at whether the slot exists. A slot's restart_lsn
// and confirmed_flush_lsn may be past the saved record LSN on a healthy
// pipeline: the connector itself advances the slot past the last record when
// the stream is idle. A slot that was dropped and recreated by someone else
// between runs cannot be told from such a slot by the slot alone; that needs
// the position to carry a slot identity, which it does not today.
func checkSlotCoversPosition(ctx context.Context, pool *pgxpool.Pool, c CDCConfig) error {
	hasPosition := c.StartPosition.ImpliesSlot() || c.LSN != 0 || c.TxCommitLSN != 0
	if !hasPosition {
		return nil // fresh start: a new slot is exactly what is wanted
	}
	_, err := internal.ReadReplicationSlot(ctx, pool, c.SlotName)
	switch {
	case err == nil:
		return nil
	case !errors.Is(err, internal.ErrMissingSlot):
		return fmt.Errorf("failed to check replication slot %q: %w", c.SlotName, err)
	}

	gap := &SlotGapError{
		Code:          CodeSlotGap,
		Path:          slotNamePath,
		Slot:          c.SlotName,
		SavedPosition: describeSavedPosition(c),
		NewSlotStart:  currentWAL(ctx, pool),
	}
	if c.AllowWALGap {
		sdk.Logger(ctx).Warn().
			Str("code", gap.Code).
			Str("slot", gap.Slot).
			Str("saved_position", gap.SavedPosition).
			Str("new_slot_start", gap.NewSlotStart).
			Msg("DATA LOSS ACCEPTED (logrepl.allowWALGap): the replication slot is gone and is created again; " +
				"every change between the saved position and the new slot's start is skipped and will not be delivered")
		return nil
	}
	return gap
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
