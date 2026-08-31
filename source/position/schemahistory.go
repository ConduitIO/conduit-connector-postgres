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

package position

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// DBZ-3 Area 2, step 2: durable per-table schema history.
//
// Step 1 detects drift by diffing successive RelationMessages held in an
// in-memory cache. That cache is empty on every process start, so a restart
// resets the connector's idea of "what this table looked like" — a DDL applied
// while the connector was down is invisible, and the first RelationMessage
// after the restart is accepted as ground truth with nothing to compare it to.
//
// This carries a small, bounded record of observed shapes in the position
// itself, so continuity survives restarts the same way snapshot and CDC
// positions do. It deliberately introduces NO new storage backend: invariant 5
// (state and checkpoint writes are atomic) is satisfied by riding in the
// existing position payload rather than by inventing a second thing to keep
// crash-consistent.

// DefaultSchemaHistoryVersions bounds how many distinct shapes are retained per
// table.
//
// Positions are checkpointed with every batch, so an unbounded history would
// grow the payload without limit — invariant 2 says positions are monotonic and
// crash-safe, not that they may grow forever. Ten is enough to answer "what did
// this table look like recently" while keeping the payload small; older entries
// are pruned oldest-first.
//
// A var, not a const, so tests can lower it. Production code must not reassign.
var DefaultSchemaHistoryVersions = 10

// SchemaVersion is one observed shape of one table.
//
// It stores a hash rather than the full column set on purpose: the position is
// checkpointed constantly, and carrying every column name and type for every
// historical shape would bloat it. The hash answers the only question this
// needs to answer across a restart — "is the shape I am seeing now the same one
// I last saw?" — and FirstSeenLSN says when that shape first appeared, which is
// what an operator needs to correlate drift with their own DDL.
type SchemaVersion struct {
	// ColumnSetHash identifies the shape. See HashColumnSet.
	ColumnSetHash string `json:"hash"`
	// FirstSeenLSN is the LSN at which this shape was first observed. Empty if
	// the shape was recorded outside CDC (e.g. during snapshot).
	FirstSeenLSN string `json:"first_seen_lsn,omitempty"`
}

// SchemaHistories maps a table identity to its retained shapes, oldest first.
// The last element is the most recently observed shape.
type SchemaHistories map[string][]SchemaVersion

// HashColumnSet produces a stable identifier for a set of columns.
//
// Stability matters more than it looks: the hash is compared against one
// computed by a DIFFERENT PROCESS on the other side of a restart, possibly by a
// different build. So the input is sorted, and the field separator cannot occur
// in a Postgres identifier, which rules out the classic collision where
// ("a|b", "c") and ("a", "b|c") hash identically.
//
// The identity triple (name, dataType, typeModifier) is deliberately the same
// one the RelationMessage diff uses. If the two disagreed, a shape could pass
// the hash comparison while the diff reported drift, or vice versa.
func HashColumnSet(cols []ColumnIdentity) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		// \x1f (unit separator) cannot appear in a Postgres identifier.
		parts = append(parts, fmt.Sprintf("%s\x1f%d\x1f%d", c.Name, c.DataType, c.TypeModifier))
	}
	sort.Strings(parts)

	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1e")))

	return hex.EncodeToString(sum[:])
}

// ColumnIdentity is the subset of a column that defines a table's shape.
// Mirrors the identity the logrepl RelationMessage diff uses; kept here so the
// position package does not depend on pglogrepl.
type ColumnIdentity struct {
	Name         string
	DataType     uint32
	TypeModifier int32
}

// LastSchemaVersion returns the most recently observed shape for a table, and
// whether any is recorded.
func (p *Position) LastSchemaVersion(table string) (SchemaVersion, bool) {
	versions := p.SchemaHistory[table]
	if len(versions) == 0 {
		return SchemaVersion{}, false
	}

	return versions[len(versions)-1], true
}

// SetFirstSeenLSN replaces the placeholder FirstSeenLSN of the most recently
// recorded shape for a table with the LSN of the first DML that actually used
// that shape, and reports whether it updated.
//
// RecordSchemaVersion is called from handleRelation, which only ever sees the
// RelationMessage's WALStart of 0, so every recorded FirstSeenLSN starts as
// "0/0" — a meaningless value in the drift-halt message ("last durable shape
// first seen at LSN 0/0" told an operator nothing). pgoutput always sends the
// first DML using a relation's shape immediately after its RelationMessage, so
// that DML's LSN is the true first-seen position, and the DML handlers backfill
// it here before building the position. A shape that already carries a real
// LSN is left untouched: the first DML backfills, later ones must not
// clobber it with a later LSN.
func (p *Position) SetFirstSeenLSN(table, lsn string) bool {
	return p.SchemaHistory.SetFirstSeenLSN(table, lsn)
}

// SetFirstSeenLSN is Position.SetFirstSeenLSN's standalone form for a
// SchemaHistories value, so a copied history can be backfilled independently
// of the live one (the B1 drift marker's staging snapshot needs the same
// backfill as the live history; see CDCHandler.emitDriftMarker).
func (h SchemaHistories) SetFirstSeenLSN(table, lsn string) bool {
	versions := h[table]
	if len(versions) == 0 {
		return false
	}
	last := &versions[len(versions)-1]
	if last.FirstSeenLSN != "" && last.FirstSeenLSN != "0/0" {
		return false
	}
	last.FirstSeenLSN = lsn
	return true
}

// Clone returns a deep copy of the histories: the map and every per-table
// version slice. A position that must be fixed at a point in time — the B1
// drift marker, which is staged when the drift is seen but emitted later —
// must not share slices with the live history, which keeps recording versions
// in between (adversarial-review Blocker 1 on the B1 drift policy: the marker
// position must reflect the state the halt decision was made against, or a
// DDL that lands between staging and emission gets checkpointed by the first
// marker and silently admitted on the restart).
func (h SchemaHistories) Clone() SchemaHistories {
	if h == nil {
		return nil
	}
	out := make(SchemaHistories, len(h))
	for table, versions := range h {
		out[table] = append([]SchemaVersion(nil), versions...)
	}
	return out
}

// RecordSchemaVersion appends a newly observed shape for a table and prunes the
// history to DefaultSchemaHistoryVersions, oldest first.
//
// Re-observing the CURRENT shape is a no-op. This matters: a RelationMessage is
// re-sent for reasons other than DDL — after a reconnect, or when a new
// subscriber attaches — and appending on every sighting would fill the history
// with duplicates of one shape and prune away the genuinely older ones that
// carry the drift signal.
//
// It returns true if a new version was recorded.
func (p *Position) RecordSchemaVersion(table, hash, lsn string) bool {
	if p.SchemaHistory == nil {
		p.SchemaHistory = make(SchemaHistories)
	}

	if last, ok := p.LastSchemaVersion(table); ok && last.ColumnSetHash == hash {
		return false
	}

	versions := p.SchemaHistory[table]
	versions = append(versions, SchemaVersion{
		ColumnSetHash: hash,
		FirstSeenLSN:  lsn,
	})

	// Prune oldest-first: the newest shapes are the ones a drift decision is
	// made against.
	if n := DefaultSchemaHistoryVersions; n > 0 && len(versions) > n {
		versions = versions[len(versions)-n:]
	}
	p.SchemaHistory[table] = versions

	return true
}
