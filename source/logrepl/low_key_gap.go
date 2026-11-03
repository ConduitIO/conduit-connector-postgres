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
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The snapshot of v0.14.0 to v0.19.0 started every table at "key > 0", so it
// skipped every row keyed 0 or below
// (docs/design-documents/20261102-snapshot-keys-at-or-below-zero.md). A
// resumed snapshot repairs its tables itself (snapshot.FetchWorker's low
// pass). A pipeline that finished its snapshot on one of those versions
// cannot be repaired from the slot, which only holds changes made after the
// snapshot, so the connector reports it once.

// CodeSnapshotLowKeysSkipped is logged, not returned: the saved position was
// written before format version 5, so the snapshot that started its history
// may have run on a version that skipped the rows keyed 0 or below, and the
// table still has such rows. They were delivered only if a later change to
// them was. It is a warning, not a refusal: a pipeline that never ran a
// snapshot, or whose rows were updated since, has lost nothing.
const CodeSnapshotLowKeysSkipped Code = "postgres.source.snapshot_low_keys_skipped"

// lowKeyGapNeedsCheck reports whether a start from pos has to look for rows
// keyed 0 or below that an old snapshot may have skipped. Otherwise the
// position can be marked LowKeyGapChecked as it is:
//   - it already is;
//   - a fresh start (no slot implied, a Debezium offset import included) has
//     no snapshot behind it, or takes one with this build;
//   - a resumed snapshot repairs each table's entry with the low pass.
func lowKeyGapNeedsCheck(pos position.Position, withSnapshot bool) bool {
	switch {
	case pos.LowKeyGapChecked, !pos.ImpliesSlot():
		return false
	case pos.Type == position.TypeSnapshot && withSnapshot:
		return false
	default:
		return true
	}
}

// checkLowKeyGap runs the one-time check of a position written before format
// version 5 and marks pos LowKeyGapChecked when it is done. Every table that
// has rows keyed 0 or below gets a CodeSnapshotLowKeysSkipped warning with
// the remediation. When the check cannot run, it says so under the same code
// and leaves pos unmarked, so the next start checks again.
//
// The mark rides on every later position (CDCHandler.positionAt, the snapshot
// iterator's positions), so the warning is logged until the first record of
// this run is acked, then never again.
func (c *CombinedIterator) checkLowKeyGap(ctx context.Context, pos *position.Position) {
	if !lowKeyGapNeedsCheck(*pos, c.conf.WithSnapshot) {
		pos.LowKeyGapChecked = true
		return
	}
	tables, err := tablesWithLowKeys(ctx, c.pool, c.conf.Tables, c.conf.TableKeys)
	if err != nil {
		sdk.Logger(ctx).Warn().
			Err(err).
			Str("code", string(CodeSnapshotLowKeysSkipped)).
			Msg("could not check for rows keyed 0 or below that a snapshot taken by v0.14.0 to v0.19.0 skipped; " +
				"the check runs again on the next start")
		return
	}
	for _, t := range tables {
		key := c.conf.TableKeys[t]
		sdk.Logger(ctx).Warn().
			Str("code", string(CodeSnapshotLowKeysSkipped)).
			Str("table", t).
			Str("key", key).
			Msgf("table %s has rows whose key %s is 0 or below, and the saved position was written before position format version 5. "+
				"The initial snapshot of v0.14.0 to v0.19.0 skipped those rows: if this pipeline's snapshot ran on "+
				"one of those versions, the rows that were not changed since were never delivered. Recover by "+
				"re-running the snapshot of the table (a new pipeline, or a reset position), or by touching the rows "+
				"so the stream delivers them: UPDATE %s SET %s = %s WHERE %s <= 0. This check runs once",
				internal.WrapSQLIdent(t), internal.WrapSQLIdent(key),
				internal.WrapSQLIdent(t), internal.WrapSQLIdent(key), internal.WrapSQLIdent(key), internal.WrapSQLIdent(key))
	}
	pos.LowKeyGapChecked = true
}

// tablesWithLowKeys returns the tables, in the given order, that have at
// least one row keyed 0 or below. Each probe is an index lookup on the
// primary key.
func tablesWithLowKeys(ctx context.Context, pool *pgxpool.Pool, tables []string, keys map[string]string) ([]string, error) {
	var out []string
	for _, t := range tables {
		var exists bool
		q := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s <= 0)", internal.WrapSQLIdent(t), internal.WrapSQLIdent(keys[t]))
		if err := pool.QueryRow(ctx, q).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check table %s for keys at or below zero: %w", t, err)
		}
		if exists {
			out = append(out, t)
		}
	}
	return out, nil
}
