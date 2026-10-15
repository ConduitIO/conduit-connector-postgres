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

	"github.com/jackc/pgx/v5"
)

// Stable codes for the slot preconditions outside the offset import
// (conduit-connector-postgres#341, S13). The postgres.import.slot_* codes are
// for the import path only.
const (
	ErrorCodeSlotMissing        = "postgres.logrepl.slot_missing"
	ErrorCodeSlotPlugin         = "postgres.logrepl.slot_plugin"
	ErrorCodeSlotInvalidated    = "postgres.logrepl.slot_invalidated"
	ErrorCodePublicationMissing = "postgres.logrepl.publication_missing"
	// ErrorCodeSlotRecreated tags the WARN logged when the connector creates
	// a slot for a pipeline that already has a position, which is allowed
	// only with logrepl.requireExistingSlot: false.
	ErrorCodeSlotRecreated = "postgres.logrepl.slot_recreated"
)

// MustUseExistingSlot decides, from logrepl.requireExistingSlot (nil when
// unset) and whether the pipeline has a position, whether Open may create
// the slot and the publication (S13, decided tri-state):
//
//   - unset: refuse when the position is non-empty (snapshot positions
//     included), create as before when it is empty;
//   - true: always refuse;
//   - false: always allow.
//
// A position means a slot existed. Creating a new one starts at the current
// WAL and silently skips every change since the old slot's position
// (invariant 3), so the default refuses.
func MustUseExistingSlot(require *bool, hasPosition bool) bool {
	if require != nil {
		return *require
	}
	return hasPosition
}

// SlotQuerier is the subset of pgx used by CheckExistingSlot.
type SlotQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CheckExistingSlot verifies that the slot and the publication exist and
// that the slot can be read: logical, pgoutput, on this database, not lost
// or invalidated. It fails with the postgres.logrepl.* codes and never
// creates anything.
func CheckExistingSlot(ctx context.Context, q SlotQuerier, slot, publication string) error {
	var plugin, slotType, walStatus, invalidation string
	var sameDB bool
	err := q.QueryRow(ctx, `SELECT COALESCE(plugin, ''), slot_type, COALESCE(database = current_database(), false),
			COALESCE(wal_status, ''), COALESCE(to_jsonb(s)->>'invalidation_reason', '')
		FROM pg_replication_slots s WHERE slot_name = $1`, slot).
		Scan(&plugin, &slotType, &sameDB, &walStatus, &invalidation)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%s: the replication slot %q does not exist, and logrepl.requireExistingSlot forbids "+
			"creating it: a new slot would start at the current WAL and skip every change since the old one. "+
			"If the slot was dropped by mistake, the changes since the pipeline's position are not recoverable "+
			"from Postgres: re-snapshot (a new pipeline with snapshotMode: initial), or, during a migration, go "+
			"back to Debezium with the manual rollback while `conduit migrate kafka-connect --check-rollback` "+
			"passes. To start fresh on purpose, set logrepl.requireExistingSlot: false for one start, then set "+
			"it back", ErrorCodeSlotMissing, slot)
	case err != nil:
		return fmt.Errorf("failed to read replication slot %q: %w", slot, err)
	case slotType != "logical" || plugin != "pgoutput" || !sameDB:
		return fmt.Errorf("%s: the slot %q uses plugin %q (type %s) or another database; this connector reads "+
			"pgoutput only, on this database. Point logrepl.slotName at a pgoutput slot. A slot cannot change "+
			"its plugin", ErrorCodeSlotPlugin, slot, plugin, slotType)
	case walStatus == "lost" || invalidation != "":
		return fmt.Errorf("%s: the slot %q lost its WAL (wal_status = %q, invalidation_reason = %q, often "+
			"max_slot_wal_keep_size). The changes it held are gone: re-snapshot, and size "+
			"max_slot_wal_keep_size for the longest expected stall", ErrorCodeSlotInvalidated, slot, walStatus, invalidation)
	}

	var pubExists bool
	if err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication).Scan(&pubExists); err != nil {
		return fmt.Errorf("failed to read publication %q: %w", publication, err)
	}
	if !pubExists {
		return fmt.Errorf("%s: the publication %q does not exist, and logrepl.requireExistingSlot forbids "+
			"creating it: a publication created after the slot's start makes pgoutput skip the earlier WAL of "+
			"its tables. Recreate the slot and publication together (publication first), then re-snapshot, or "+
			"restore the publication only if you know no WAL since the slot's start touched its tables",
			ErrorCodePublicationMissing, publication)
	}
	return nil
}

// SlotExists reports whether the replication slot exists.
func SlotExists(ctx context.Context, q SlotQuerier, slot string) (bool, error) {
	var exists bool
	err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&exists)
	return exists, err
}
