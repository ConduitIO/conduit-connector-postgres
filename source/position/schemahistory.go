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
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
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
// different build. So the columns are put in a canonical order before hashing.
//
// The hash input must also be injective: two different column sets must never
// produce the same input. A quoted Postgres identifier may contain any byte
// except NUL, so no separator byte is safe to join names with; an earlier
// encoding joined fields with 0x1f and records with 0x1e and was ambiguous for
// a column named like "a<0x1f>25<0x1f>-1<0x1e>b" (it equalled the two columns
// "a" and "b"), which made a real schema change classify as no change. Each
// record is therefore written self-delimiting: a uvarint name length, the name
// bytes, then the data type (uvarint) and type modifier (zigzag varint).
// Concatenated self-delimiting records parse back to exactly one sequence, and
// the canonical order makes that sequence a function of the set.
//
// The identity triple (name, dataType, typeModifier) is deliberately the same
// one the RelationMessage diff uses. If the two disagreed, a shape could pass
// the hash comparison while the diff reported drift, or vice versa.
//
// The old separator-joined form is not in any release tag; positions written by
// main builds before #408 hold the old form. MatchesLegacyHash verifies those.
func HashColumnSet(cols []ColumnIdentity) string {
	sorted := slices.Clone(cols)
	slices.SortFunc(sorted, func(a, b ColumnIdentity) int {
		return cmp.Or(
			strings.Compare(a.Name, b.Name),
			cmp.Compare(a.DataType, b.DataType),
			cmp.Compare(a.TypeModifier, b.TypeModifier),
		)
	})

	var buf []byte
	for _, c := range sorted {
		buf = binary.AppendUvarint(buf, uint64(len(c.Name)))
		buf = append(buf, c.Name...)
		buf = binary.AppendUvarint(buf, uint64(c.DataType))
		buf = binary.AppendVarint(buf, int64(c.TypeModifier))
	}
	sum := sha256.Sum256(buf)

	return hex.EncodeToString(sum[:])
}

// legacyHashColumnSet is the hash input HashColumnSet used before #408: records
// "name US type US typmod" sorted as strings and joined with RS. It is ambiguous
// when a name contains US or RS, so it exists ONLY to verify hashes already
// stored in positions by main builds (see MatchesLegacyHash); never write it.
//
// No release tag contains it, so it can be deleted once no position written by
// a main build before #408 can still be resumed, i.e. after the first
// connector release cut from a main that includes #408 has been out for a
// deprecation period (two minor versions, per the repo's policy).
func legacyHashColumnSet(cols []ColumnIdentity) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("%s\x1f%d\x1f%d", c.Name, c.DataType, c.TypeModifier))
	}
	slices.Sort(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1e")))
	return hex.EncodeToString(sum[:])
}

// MatchesLegacyHash reports whether hash, read from a stored position, is the
// old-form hash of cols. It lets a position written before #408 be resumed
// without reading the unchanged shape as drift.
//
// A match is accepted only when no name in cols contains the old separator
// bytes (0x1f, 0x1e). Then the old input for cols parses unambiguously, so an
// equal hash means the recorded shape was cols. Residual limit: the recorded
// shape is not visible, only its hash, so a recorded shape whose names did
// contain those bytes and imitated cols (the very collision #408 fixes) cannot
// be told apart. That needs such a name to have existed in a main-build
// position, and applies once, at the first restart after upgrading.
func MatchesLegacyHash(cols []ColumnIdentity, hash string) bool {
	for _, c := range cols {
		if strings.ContainsAny(c.Name, "\x1f\x1e") {
			return false
		}
	}
	return legacyHashColumnSet(cols) == hash
}

// ReplaceLastSchemaHash swaps the hash of the most recently recorded shape of a
// table, keeping its FirstSeenLSN. It is used to re-record a verified legacy
// hash in the current encoding without adding a version to the history.
func (p *Position) ReplaceLastSchemaHash(table, hash string) bool {
	versions := p.SchemaHistory[table]
	if len(versions) == 0 {
		return false
	}
	versions[len(versions)-1].ColumnSetHash = hash
	return true
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
// Builds before #335 recorded shapes when the RelationMessage arrived, which
// carries a WALStart of 0, so their positions can hold a FirstSeenLSN of
// "0/0" — a meaningless value in the drift-halt message ("last durable shape
// first seen at LSN 0/0" told an operator nothing). The connector now records
// a shape at the LSN of the first delivered change that uses it, and the DML
// handlers still backfill here so a placeholder carried in from an older
// position is repaired. A shape that already carries a real LSN is left
// untouched: the first DML backfills, later ones must not clobber it with a
// later LSN.
func (p *Position) SetFirstSeenLSN(table, lsn string) bool {
	return p.SchemaHistory.SetFirstSeenLSN(table, lsn)
}

// SetFirstSeenLSN is Position.SetFirstSeenLSN's standalone form for a
// SchemaHistories value.
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
