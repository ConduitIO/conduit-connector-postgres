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
	"github.com/jackc/pgx/v5/pgtype"
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
			is.Equal(got.IsIncompatible(true), tt.wantIncompatible)

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
	is.True(!d.IsIncompatible(true))
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

// Test_SchemaDiff_TSVectorEvolve pins how the evolve policy treats a column
// that becomes or stops being a tsvector. tsvector is emitted as its text form
// and maps to the Avro string, the same as text and varchar, so changing
// between them leaves the Avro schema as it was and evolve admits it. Any
// change to a type with a different Avro shape still blocks.
func Test_SchemaDiff_TSVectorEvolve(t *testing.T) {
	for _, tc := range []struct {
		name         string
		from, to     uint32
		incompatible bool
	}{
		{"text to tsvector", pgtype.TextOID, pgtype.TSVectorOID, false},
		{"varchar to tsvector", pgtype.VarcharOID, pgtype.TSVectorOID, false},
		{"tsvector to text", pgtype.TSVectorOID, pgtype.TextOID, false},
		{"tsvector to varchar", pgtype.TSVectorOID, pgtype.VarcharOID, false},
		{"tsvector to int4", pgtype.TSVectorOID, pgtype.Int4OID, true},
		{"int8 to tsvector", pgtype.Int8OID, pgtype.TSVectorOID, true},
		{"tsvector to bytea", pgtype.TSVectorOID, pgtype.ByteaOID, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			rs := NewRelationSet()
			rs.Update(rel(1, col("id", pgtype.Int8OID, -1), col("c", tc.from, -1)))
			diff := rs.Update(rel(1, col("id", pgtype.Int8OID, -1), col("c", tc.to, -1)))
			is.True(diff.HasDrift())
			is.Equal(diff.IsIncompatible(true), tc.incompatible)
		})
	}
}

// An unconstrained numeric is an Avro string (#375), so changing a column
// between text/varchar and it keeps the Avro shape. Without an Avro schema the
// record holds a big.Rat for it and a string for text, so the value's type
// changes and evolve must not accept it.
func Test_SchemaDiff_UnconstrainedNumericVersusText(t *testing.T) {
	numeric := int32(-1)
	for _, tc := range []struct {
		name               string
		fromOID, toOID     uint32
		fromMod, toMod     int32
		wantAvro, wantNone bool // incompatible with Avro on / off
	}{
		{"text to numeric", pgtype.TextOID, pgtype.NumericOID, -1, numeric, false, true},
		{"numeric to text", pgtype.NumericOID, pgtype.TextOID, numeric, -1, false, true},
		{"varchar to numeric", pgtype.VarcharOID, pgtype.NumericOID, 14, numeric, false, true},
		{"numeric to varchar", pgtype.NumericOID, pgtype.VarcharOID, numeric, 14, false, true},
		// numeric(p,s) is a decimal in both modes
		{"text to numeric(10,2)", pgtype.TextOID, pgtype.NumericOID, -1, 10<<16 | 2 + 4, true, true},
		{"numeric to numeric(10,2)", pgtype.NumericOID, pgtype.NumericOID, numeric, 10<<16 | 2 + 4, true, true},
		{"numeric unchanged type, text to text", pgtype.TextOID, pgtype.VarcharOID, -1, 14, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			rs := NewRelationSet()
			rs.Update(rel(1, col("id", pgtype.Int8OID, -1), col("c", tc.fromOID, tc.fromMod)))
			diff := rs.Update(rel(1, col("id", pgtype.Int8OID, -1), col("c", tc.toOID, tc.toMod)))
			is.True(diff.HasDrift())
			is.Equal(diff.IsIncompatible(true), tc.wantAvro)
			is.Equal(diff.IsIncompatible(false), tc.wantNone)
		})
	}
}
