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

package internal

import (
	"fmt"
	"sort"
	"strings"

	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/jackc/pglogrepl"
)

// DBZ-3 Area 2, step 1: detect schema drift by diffing successive
// RelationMessages for the same relation ID.
//
// pgoutput never carries DDL statements — unlike the MySQL binlog, which
// carries the original ALTER TABLE text — so the only signal that a table's
// shape changed is that Postgres sends a fresh RelationMessage before the first
// record using the new shape. RelationSet.Add previously overwrote the cached
// relation silently, which is precisely how a schema change reached the record
// path with nobody deciding anything about it (invariant 6: schema handling
// never silently mangles data).
//
// Column identity is (Name, DataType, TypeModifier), matching how the Avro
// schema is already derived per column. Position is deliberately NOT part of
// identity: a column added in the middle shifts every later column's position
// without changing any of them.

// ColumnChangeKind classifies one column-level difference.
type ColumnChangeKind string

const (
	// ColumnAdded is a column present after but not before. Widening.
	ColumnAdded ColumnChangeKind = "added"
	// ColumnDropped is a column present before but not after. Narrowing —
	// downstream consumers may already depend on it.
	ColumnDropped ColumnChangeKind = "dropped"
	// ColumnTypeChanged is a column whose name persists but whose DataType or
	// TypeModifier differs. May be widening (varchar(10)->varchar(20)) or
	// narrowing (bigint->int); this layer reports the change and does not judge
	// compatibility, which is the policy layer's job.
	ColumnTypeChanged ColumnChangeKind = "type_changed"
)

// ColumnChange is a single difference between two shapes of one relation.
type ColumnChange struct {
	Kind ColumnChangeKind
	Name string

	// OldDataType/OldTypeModifier are set for dropped and type_changed.
	OldDataType     uint32
	OldTypeModifier int32
	// NewDataType/NewTypeModifier are set for added and type_changed.
	NewDataType     uint32
	NewTypeModifier int32
}

// String renders one change in the form an operator-facing error uses.
func (c ColumnChange) String() string {
	switch c.Kind {
	case ColumnAdded:
		return fmt.Sprintf("column %q added (type %d)", c.Name, c.NewDataType)
	case ColumnDropped:
		return fmt.Sprintf("column %q dropped (was type %d)", c.Name, c.OldDataType)
	case ColumnTypeChanged:
		return fmt.Sprintf("column %q type changed (%d/%d -> %d/%d)",
			c.Name, c.OldDataType, c.OldTypeModifier, c.NewDataType, c.NewTypeModifier)
	default:
		return fmt.Sprintf("column %q changed", c.Name)
	}
}

// SchemaDiff is the set of column changes between two shapes of one relation.
// The zero value means no drift.
type SchemaDiff struct {
	RelationID uint32
	Namespace  string
	RelationNa string
	Changes    []ColumnChange
}

// HasDrift reports whether anything changed.
func (d SchemaDiff) HasDrift() bool { return len(d.Changes) > 0 }

// IsIncompatible reports whether the diff removes a column or retypes one in a
// way that changes its Avro shape, as opposed to only adding columns or
// widening within the same Avro shape.
//
// The distinction is load-bearing for the evolve policy: adding a column is safe
// to accept automatically because nothing downstream can already depend on it;
// dropping one, or changing one's Avro schema (e.g. numeric precision/scale,
// which alters the Avro decimal logical type), can break a consumer that does.
// Length-only changes to types whose Avro mapping ignores TypeModifier (varchar,
// text) are compatible and evolve accepts them (Q2 in the B1 design doc: the
// judgement is per-type against the Avro extraction rules, via
// schema.TypeChangePreservesSchema, not "any type change blocks"). Policy
// treats the two differently; see the drift policy in the DBZ-3 design doc.
func (d SchemaDiff) IsIncompatible() bool {
	for _, c := range d.Changes {
		switch c.Kind {
		case ColumnDropped:
			return true
		case ColumnTypeChanged:
			if !schema.Avro.TypeChangePreservesSchema(
				c.OldDataType, c.OldTypeModifier,
				c.NewDataType, c.NewTypeModifier,
			) {
				return true
			}
		}
	}
	return false
}

// String renders the whole diff for an operator-facing message. Changes are
// sorted by column name so the text is stable across runs — an error message
// whose wording depends on map iteration order is unusable in an alert.
func (d SchemaDiff) String() string {
	if !d.HasDrift() {
		return "no schema drift"
	}

	parts := make([]string, 0, len(d.Changes))
	for _, c := range d.Changes {
		parts = append(parts, c.String())
	}
	sort.Strings(parts)

	return fmt.Sprintf("table %s.%s (relation %d): %s",
		d.Namespace, d.RelationNa, d.RelationID, strings.Join(parts, "; "))
}

// diffRelations compares two shapes of the same relation.
//
// A rename is reported as drop+add. pgoutput's RelationMessage exposes no
// column-OID or rename tracking, so a rename is genuinely indistinguishable
// from dropping one column and adding another — the heuristics that could guess
// (matching position and type, no intervening DML) can still be wrong, and
// DeVaris confirmed on 2026-08-03 that guessing is not worth its complexity.
// Reporting drop+add is correct and noisier; guessing would be quieter and
// sometimes wrong.
func diffRelations(before, after *pglogrepl.RelationMessage) SchemaDiff {
	diff := SchemaDiff{
		RelationID: after.RelationID,
		Namespace:  after.Namespace,
		RelationNa: after.RelationName,
	}

	beforeCols := make(map[string]*pglogrepl.RelationMessageColumn, len(before.Columns))
	for _, c := range before.Columns {
		beforeCols[c.Name] = c
	}
	afterCols := make(map[string]*pglogrepl.RelationMessageColumn, len(after.Columns))
	for _, c := range after.Columns {
		afterCols[c.Name] = c
	}

	// Iterate the message slices, not the maps, so ordering is deterministic
	// before the sort in String().
	for _, a := range after.Columns {
		b, existed := beforeCols[a.Name]
		if !existed {
			diff.Changes = append(diff.Changes, ColumnChange{
				Kind: ColumnAdded, Name: a.Name,
				NewDataType: a.DataType, NewTypeModifier: a.TypeModifier,
			})
			continue
		}
		if b.DataType != a.DataType || b.TypeModifier != a.TypeModifier {
			diff.Changes = append(diff.Changes, ColumnChange{
				Kind: ColumnTypeChanged, Name: a.Name,
				OldDataType: b.DataType, OldTypeModifier: b.TypeModifier,
				NewDataType: a.DataType, NewTypeModifier: a.TypeModifier,
			})
		}
	}

	for _, b := range before.Columns {
		if _, still := afterCols[b.Name]; !still {
			diff.Changes = append(diff.Changes, ColumnChange{
				Kind: ColumnDropped, Name: b.Name,
				OldDataType: b.DataType, OldTypeModifier: b.TypeModifier,
			})
		}
	}

	return diff
}

// Update caches the relation and reports what changed relative to the previously
// cached shape.
//
// It replaces Add, which overwrote silently. The first RelationMessage for a
// relation reports no drift: there is no prior shape to compare against, and
// treating every column as newly added on first sight would make the very first
// message of every run look like drift. Continuity across a RESTART is a
// separate problem — a fresh process has an empty cache and would likewise see
// no drift — which is what the durable schema history in the position solves
// (Area 2 step 2, not this change).
func (rs *RelationSet) Update(r *pglogrepl.RelationMessage) SchemaDiff {
	prev, existed := rs.relations[r.RelationID]
	rs.relations[r.RelationID] = r

	if !existed {
		return SchemaDiff{RelationID: r.RelationID, Namespace: r.Namespace, RelationNa: r.RelationName}
	}

	return diffRelations(prev, r)
}
