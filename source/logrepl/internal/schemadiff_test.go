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
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func rel(id uint32, cols ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID: id, Namespace: "public", RelationName: "t", Columns: cols,
	}
}

func col(name string, dataType uint32, typeMod int32) *pglogrepl.RelationMessageColumn {
	return &pglogrepl.RelationMessageColumn{Name: name, DataType: dataType, TypeModifier: typeMod}
}

// Test_RelationSet_Update_DetectsDrift is the core Area 2 step 1 test.
//
// Each case is a DDL an operator can actually run, named as such, because the
// point of this layer is to turn "a new RelationMessage arrived" into "somebody
// ran ALTER TABLE and here is what they did".
func Test_RelationSet_Update_DetectsDrift(t *testing.T) {
	const relID = 42

	tests := []struct {
		name             string
		before           []*pglogrepl.RelationMessageColumn
		after            []*pglogrepl.RelationMessageColumn
		wantKinds        []ColumnChangeKind
		wantNames        []string
		wantIncompatible bool
	}{
		{
			name:      "ADD COLUMN",
			before:    []*pglogrepl.RelationMessageColumn{col("id", 23, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("id", 23, -1), col("email", 25, -1)},
			wantKinds: []ColumnChangeKind{ColumnAdded}, wantNames: []string{"email"},
			wantIncompatible: false,
		},
		{
			name:      "DROP COLUMN",
			before:    []*pglogrepl.RelationMessageColumn{col("id", 23, -1), col("email", 25, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("id", 23, -1)},
			wantKinds: []ColumnChangeKind{ColumnDropped}, wantNames: []string{"email"},
			wantIncompatible: true,
		},
		{
			name:      "ALTER COLUMN TYPE",
			before:    []*pglogrepl.RelationMessageColumn{col("n", 23, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("n", 20, -1)},
			wantKinds: []ColumnChangeKind{ColumnTypeChanged}, wantNames: []string{"n"},
			wantIncompatible: true,
		},
		{
			// varchar(10) -> varchar(20): same DataType, different TypeModifier.
			// The Avro mapping for varchar is String regardless of TypeModifier
			// (source/schema/avro.go), so the change preserves the Avro shape and
			// the evolve policy admits it (Q2 in the B1 design doc). This case
			// pins that the compatibility judgement is per-type, not "any type
			// change blocks".
			name:      "ALTER COLUMN length only (TypeModifier)",
			before:    []*pglogrepl.RelationMessageColumn{col("s", 1043, 14)},
			after:     []*pglogrepl.RelationMessageColumn{col("s", 1043, 24)},
			wantKinds: []ColumnChangeKind{ColumnTypeChanged}, wantNames: []string{"s"},
			wantIncompatible: false,
		},
		{
			// text -> varchar and varchar -> text: same Avro String shape.
			name:      "ALTER COLUMN text to varchar",
			before:    []*pglogrepl.RelationMessageColumn{col("s", 25, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("s", 1043, 24)},
			wantKinds: []ColumnChangeKind{ColumnTypeChanged}, wantNames: []string{"s"},
			wantIncompatible: false,
		},
		{
			// numeric(10,2) -> numeric(12,4): the Avro decimal logical type
			// carries precision and scale, so the Avro schema changes and the
			// change is incompatible — a consumer reading the decimal bytes
			// against the old schema gets mis-scaled values.
			// typmod = ((precision << 16) | scale) + 4 (VARHDRSZ).
			name:      "ALTER COLUMN numeric precision/scale",
			before:    []*pglogrepl.RelationMessageColumn{col("n", 1700, 655366)}, // 4 + ((10 << 16) | 2)
			after:     []*pglogrepl.RelationMessageColumn{col("n", 1700, 786440)}, // 4 + ((12 << 16) | 4)
			wantKinds: []ColumnChangeKind{ColumnTypeChanged}, wantNames: []string{"n"},
			wantIncompatible: true,
		},
		{
			// A rename is indistinguishable from drop+add in pgoutput, and per
			// the 2026-08-03 decision we report it as such rather than guess.
			name:             "RENAME COLUMN reported as drop+add",
			before:           []*pglogrepl.RelationMessageColumn{col("old", 25, -1)},
			after:            []*pglogrepl.RelationMessageColumn{col("new", 25, -1)},
			wantKinds:        []ColumnChangeKind{ColumnAdded, ColumnDropped},
			wantNames:        []string{"new", "old"},
			wantIncompatible: true,
		},
		{
			// Adding a column in the MIDDLE shifts every later column's position
			// while changing none of them. Identity keyed on position rather than
			// name would report the whole tail as changed.
			name:      "ADD COLUMN in the middle does not report the shifted tail",
			before:    []*pglogrepl.RelationMessageColumn{col("a", 23, -1), col("c", 25, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("a", 23, -1), col("b", 16, -1), col("c", 25, -1)},
			wantKinds: []ColumnChangeKind{ColumnAdded}, wantNames: []string{"b"},
			wantIncompatible: false,
		},
		{
			name:      "no change",
			before:    []*pglogrepl.RelationMessageColumn{col("id", 23, -1), col("email", 25, -1)},
			after:     []*pglogrepl.RelationMessageColumn{col("id", 23, -1), col("email", 25, -1)},
			wantKinds: nil, wantNames: nil, wantIncompatible: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			rs := NewRelationSet()

			// First message establishes the baseline and must report nothing.
			first := rs.Update(rel(relID, tt.before...))
			is.True(!first.HasDrift())

			got := rs.Update(rel(relID, tt.after...))
			is.Equal(len(got.Changes), len(tt.wantKinds))
			is.Equal(got.HasDrift(), len(tt.wantKinds) > 0)
			is.Equal(got.IsIncompatible(), tt.wantIncompatible)

			gotKinds := map[ColumnChangeKind]int{}
			gotNames := map[string]bool{}
			for _, c := range got.Changes {
				gotKinds[c.Kind]++
				gotNames[c.Name] = true
			}
			for _, k := range tt.wantKinds {
				is.True(gotKinds[k] > 0)
			}
			for _, n := range tt.wantNames {
				is.True(gotNames[n])
			}
		})
	}
}

// Test_RelationSet_Update_FirstMessageIsNotDrift pins that the very first
// RelationMessage for a relation reports nothing.
//
// Getting this wrong would make every run's first message look like a full-table
// schema change, which under the default halt policy would stop every pipeline
// on startup.
func Test_RelationSet_Update_FirstMessageIsNotDrift(t *testing.T) {
	is := is.New(t)
	rs := NewRelationSet()

	d := rs.Update(rel(7, col("id", 23, -1), col("name", 25, -1)))
	is.True(!d.HasDrift())
	is.True(!d.IsIncompatible())
	is.Equal(d.String(), "no schema drift")
}

// Test_RelationSet_Update_IsolatesRelations pins that drift in one table does
// not leak into another. Relation IDs are the cache key, and a shared or
// mis-keyed cache would report a neighbouring table's DDL against the wrong one.
func Test_RelationSet_Update_IsolatesRelations(t *testing.T) {
	is := is.New(t)
	rs := NewRelationSet()

	rs.Update(rel(1, col("id", 23, -1)))
	rs.Update(rel(2, col("id", 23, -1)))

	// Drift table 1 only.
	d1 := rs.Update(rel(1, col("id", 23, -1), col("extra", 25, -1)))
	is.True(d1.HasDrift())

	// Table 2 unchanged: no drift.
	d2 := rs.Update(rel(2, col("id", 23, -1)))
	is.True(!d2.HasDrift())
}

// Test_SchemaDiff_String_IsStable pins that the operator-facing message is
// deterministic. An alert whose text reorders between runs cannot be
// deduplicated or matched, so the sort is behaviour, not cosmetics.
func Test_SchemaDiff_String_IsStable(t *testing.T) {
	is := is.New(t)
	rs := NewRelationSet()

	rs.Update(rel(9, col("a", 23, -1), col("b", 25, -1), col("c", 16, -1)))
	d := rs.Update(rel(9, col("a", 23, -1), col("z", 25, -1)))

	first := d.String()
	for range 20 {
		is.Equal(d.String(), first)
	}
	is.True(strings.Contains(first, "public.t"))
	is.True(strings.Contains(first, "relation 9"))
}
