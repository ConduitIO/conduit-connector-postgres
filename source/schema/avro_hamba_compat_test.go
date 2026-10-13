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

package schema_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// This file pins the Avro output of the Postgres schema extractor across the
// codec swap from github.com/hamba/avro/v2 (archived, three unfixed decoder
// advisories) to github.com/iskorotkov/avro/v2.
//
// Why it matters (data path): updateAvroSchema and extractSchemas register
// avroSch.String() with the Conduit schema registry and attach the result to
// every record. If the new codec printed a different string for the same
// table, every existing pipeline would register a new schema version on
// upgrade, and anything keyed on the schema text or its fingerprint would
// see a change nobody asked for.
//
// testdata/hamba_schema_fixtures.json was produced by running the same
// compatCases() against the extractor built on hamba/avro v2.31.0 (and, for
// the schema strings, v2.30.0, which v0.14.3 shipped; the two agree).
// To ADD a case, build the tree against hamba (git stash the import swap),
// run with HAMBA_FIXTURE_GEN=1, and commit only the new entries. Never
// regenerate existing entries with the new codec: they are the frozen record
// of what the old library produced.
//
// One deliberate exception (#375): the "numeric unconstrained" entry and the
// six typeChanges between text/varchar and an unconstrained numeric changed when
// that column became an Avro string. They were edited in place (not regenerated
// with the new codec), and a fresh review regenerated them with the hamba
// generator on the pre-swap tree and found them byte-identical. The new schema is
// a plain string, which hamba and iskorotkov encode identically (0x02 0x30 for
// "0").

const (
	pgBool        = "bool"
	pgBytea       = "bytea"
	pgFloat4      = "float4"
	pgFloat8      = "float8"
	pgInt2        = "int2"
	pgInt4        = "int4"
	pgInt8        = "int8"
	pgText        = "text"
	pgVarchar     = "varchar"
	pgJSON        = "json"
	pgJSONb       = "jsonb"
	pgTimestamptz = "timestamptz"
	pgTimestamp   = "timestamp"
	pgDate        = "date"
	pgUUID        = "uuid"
	pgNumeric     = "numeric"
)

type compatCol struct {
	Name   string `json:"name"`
	PGType string `json:"pgType"`
	TypMod int32  `json:"typMod"`
}

type compatCase struct {
	Name       string      `json:"name"`
	SchemaName string      `json:"schemaName"`
	Cols       []compatCol `json:"cols"`
	KeyCols    []string    `json:"keyCols"`
}

type compatFixture struct {
	compatCase
	Schema string `json:"schema"`
	// RowHex is the Avro binary encoding of rowFor(Cols) under Schema, keyed
	// by the zone time.Local was set to. local-timestamp-micros is encoded
	// from the value's wall clock in time.Local, so the bytes depend on the
	// host zone; both codecs must agree in every zone, and the fixtures must
	// not depend on where they were generated or where CI runs.
	RowHex map[string]string `json:"rowHex"`
}

type typeChangeCase struct {
	OldType string `json:"oldType"`
	OldMod  int32  `json:"oldMod"`
	NewType string `json:"newType"`
	NewMod  int32  `json:"newMod"`
	Want    bool   `json:"want"`
}

type compatFixtureFile struct {
	Schemas     []compatFixture  `json:"schemas"`
	TypeChanges []typeChangeCase `json:"typeChanges"`
}

// numeric(p,s) type modifier, as Postgres stores it: ((p << 16) | s) + 4.
func numericMod(precision, scale int32) int32 { return (precision<<16 | scale) + 4 }

func compatCases() []compatCase {
	all := []compatCol{
		{"c_bool", pgBool, -1},
		{"c_bytea", pgBytea, -1},
		{"c_float4", pgFloat4, -1},
		{"c_float8", pgFloat8, -1},
		{"c_int2", pgInt2, -1},
		{"c_int4", pgInt4, -1},
		{"c_int8", pgInt8, -1},
		{"c_text", pgText, -1},
		{"c_varchar", pgVarchar, 14},
		{"c_json", pgJSON, -1},
		{"c_jsonb", pgJSONb, -1},
		{"c_timestamptz", pgTimestamptz, -1},
		{"c_timestamp", pgTimestamp, -1},
		{"c_date", pgDate, -1},
		{"c_uuid", pgUUID, -1},
		{"c_numeric", pgNumeric, numericMod(8, 2)},
	}
	cases := []compatCase{{
		Name: "every mapped type", SchemaName: "t_payload", Cols: all,
	}, {
		Name: "key schema, single column", SchemaName: "t_key", Cols: all, KeyCols: []string{"c_int8"},
	}, {
		Name: "key schema, composite", SchemaName: "t_key", Cols: all, KeyCols: []string{"c_uuid", "c_text", "c_numeric"},
	}, {
		Name: "numeric unconstrained", SchemaName: "n_payload",
		Cols: []compatCol{{"n", pgNumeric, -1}},
	}}
	for _, nm := range []struct{ p, s int32 }{{1, 0}, {10, 3}, {18, 18}, {38, 0}, {38, 20}, {1000, 500}} {
		cases = append(cases, compatCase{
			Name: fmt.Sprintf("numeric(%d,%d)", nm.p, nm.s), SchemaName: "n_payload",
			Cols: []compatCol{{"n", pgNumeric, numericMod(nm.p, nm.s)}},
		})
	}
	return cases
}

func typeChangeCases() []typeChangeCase {
	names := []struct {
		t string
		m int32
	}{
		{pgBool, -1}, {pgBytea, -1}, {pgFloat4, -1}, {pgFloat8, -1}, {pgInt2, -1}, {pgInt4, -1},
		{pgInt8, -1}, {pgText, -1}, {pgVarchar, 14}, {pgVarchar, 255}, {pgJSON, -1}, {pgJSONb, -1},
		{pgTimestamptz, -1}, {pgTimestamp, -1}, {pgDate, -1}, {pgUUID, -1},
		{pgNumeric, numericMod(8, 2)}, {pgNumeric, numericMod(8, 3)}, {pgNumeric, numericMod(10, 2)},
		{pgNumeric, -1},
	}
	var out []typeChangeCase
	for _, o := range names {
		for _, n := range names {
			out = append(out, typeChangeCase{OldType: o.t, OldMod: o.m, NewType: n.t, NewMod: n.m})
		}
	}
	return out
}

// compatCmpOpts compares decoded Avro values with the Go values that were
// encoded: decimals by value, and times by instant or, for local timestamps
// (which carry no zone on the wire), by wall clock.
var compatCmpOpts = []cmp.Option{
	cmp.Comparer(func(a, b *big.Rat) bool { return a.Cmp(b) == 0 }),
	cmp.Comparer(func(a, b time.Time) bool {
		const layout = "2006-01-02T15:04:05.000000"
		return a.Equal(b) || a.Format(layout) == b.Format(layout)
	}),
}

// compatZones are the values time.Local is set to while encoding rows.
var compatZones = map[string]*time.Location{
	"UTC":       time.UTC,
	"UTC-07":    time.FixedZone("UTC-07", -7*60*60),
	"UTC+05:30": time.FixedZone("UTC+05:30", 5*60*60+30*60),
}

// withLocal sets time.Local for the duration of f.
func withLocal(loc *time.Location, f func()) {
	orig := time.Local
	time.Local = loc
	defer func() { time.Local = orig }()
	f()
}

func compatFields(t *testing.T, cols []compatCol) []pgconn.FieldDescription {
	t.Helper()
	m := pgtype.NewMap()
	fields := make([]pgconn.FieldDescription, 0, len(cols))
	for _, c := range cols {
		typ, ok := m.TypeForName(c.PGType)
		if !ok {
			t.Fatalf("unknown pg type %q", c.PGType)
		}
		fields = append(fields, pgconn.FieldDescription{Name: c.Name, DataTypeOID: typ.OID, TypeModifier: c.TypMod})
	}
	return fields
}

// rowFor returns one value per column, shaped the way the SDK hands them to
// the Avro encoder (see avrolizeMap in avro_test.go).
func rowFor(cols []compatCol) map[string]any {
	ts := time.Date(2022, 3, 14, 15, 16, 17, 123456000, time.UTC)
	row := make(map[string]any, len(cols))
	for _, c := range cols {
		switch c.PGType {
		case pgBool:
			row[c.Name] = true
		case pgBytea, pgJSON, pgJSONb:
			row[c.Name] = []byte(`{"a":[1,2,3]}`)
		case pgFloat4:
			row[c.Name] = float32(15.5)
		case pgFloat8:
			row[c.Name] = 16.16
		case pgInt2, pgInt4:
			row[c.Name] = 2147483647
		case pgInt8:
			row[c.Name] = int64(9223372036854775807)
		case pgText, pgVarchar:
			row[c.Name] = "foo bar baz \u00e9"
		case pgTimestamptz, pgTimestamp:
			row[c.Name] = ts
		case pgDate:
			row[c.Name] = time.Date(2022, 3, 14, 0, 0, 0, 0, time.UTC)
		case pgUUID:
			row[c.Name] = "bd94ee0b-564f-4088-bf4e-8d5e626caf66"
		case pgNumeric:
			if c.TypMod == -1 {
				// unconstrained numeric is an Avro string (#375)
				row[c.Name] = "0"
			} else {
				row[c.Name] = numericValue(c.TypMod)
			}
		}
	}
	return row
}

// numericValue returns a value that fits numeric(p,s) for the given type
// modifier: the smallest representable unit for scale > 0, 7 for scale 0, and
// 0 for an unconstrained numeric (whose derived scale is not usable).
func numericValue(typMod int32) *big.Rat {
	if typMod < 4 {
		return new(big.Rat)
	}
	scale := (typMod - 4) & 65535
	if scale == 0 {
		return big.NewRat(7, 1)
	}
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(big.NewInt(1), den)
}

func extractCompat(t *testing.T, c compatCase) string {
	t.Helper()
	sch, err := schema.Avro.Extract(c.SchemaName, compatFields(t, c.Cols), c.KeyCols...)
	if err != nil {
		t.Fatalf("%s: extract: %v", c.Name, err)
	}
	return sch.String()
}

func TestAvroExtract_HambaCompat(t *testing.T) {
	const path = "testdata/hamba_schema_fixtures.json"

	if os.Getenv("HAMBA_FIXTURE_GEN") == "1" {
		generateHambaFixtures(t, path)
		return
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f compatFixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	cases := compatCases()
	if len(f.Schemas) != len(cases) {
		t.Fatalf("fixture has %d schemas, compatCases() has %d: add fixtures with HAMBA_FIXTURE_GEN=1 against hamba", len(f.Schemas), len(cases))
	}

	for i, fx := range f.Schemas {
		c := cases[i]
		t.Run(c.Name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(fx.Name, c.Name)

			// The schema string registered with the Conduit schema registry
			// must not change on upgrade.
			got := extractCompat(t, c)
			if diff := cmp.Diff(fx.Schema, got); diff != "" {
				t.Fatalf("schema text differs from what hamba/avro produced (-hamba +current):\n%s", diff)
			}

			// Bytes encoded under the hamba-era schema decode, and the same
			// row encodes to identical bytes, with the current codec.
			sch, err := avro.Parse(fx.Schema)
			is.NoErr(err)
			is.Equal(len(fx.RowHex), len(compatZones))
			for zone, loc := range compatZones {
				t.Run("TZ="+zone, func(t *testing.T) {
					is := is.New(t)
					wantHex, ok := fx.RowHex[zone]
					is.True(ok) // fixture has an encoding for every zone
					withLocal(loc, func() {
						enc, err := avro.Marshal(sch, rowFor(c.Cols))
						is.NoErr(err)
						is.Equal(hex.EncodeToString(enc), wantHex)

						// The hamba-encoded bytes decode to the values that
						// were encoded, not merely to the right field count.
						old, err := hex.DecodeString(wantHex)
						is.NoErr(err)
						decoded := map[string]any{}
						is.NoErr(avro.Unmarshal(sch, old, &decoded))
						want := rowFor(c.Cols)
						if len(c.KeyCols) > 0 {
							for k := range want {
								if !slices.Contains(c.KeyCols, k) {
									delete(want, k)
								}
							}
						}
						if diff := cmp.Diff(want, decoded, compatCmpOpts...); diff != "" {
							t.Fatalf("decoded values differ (-encoded +decoded):\n%s", diff)
						}
					})
				})
			}
		})
	}

	t.Run("TypeChangePreservesSchema", func(t *testing.T) {
		cases := typeChangeCases()
		if len(f.TypeChanges) != len(cases) {
			t.Fatalf("fixture has %d type changes, typeChangeCases() has %d", len(f.TypeChanges), len(cases))
		}
		m := pgtype.NewMap()
		for i, fx := range f.TypeChanges {
			c := cases[i]
			ot, _ := m.TypeForName(c.OldType)
			nt, _ := m.TypeForName(c.NewType)
			got := schema.Avro.TypeChangePreservesSchema(ot.OID, c.OldMod, nt.OID, c.NewMod)
			if got != fx.Want {
				t.Errorf("%s(%d) -> %s(%d): got %v, hamba said %v", c.OldType, c.OldMod, c.NewType, c.NewMod, got, fx.Want)
			}
		}
	})
}

// generateHambaFixtures writes the fixture file. Only valid when this package
// is built against github.com/hamba/avro/v2; see the file comment.
func generateHambaFixtures(t *testing.T, path string) {
	var out compatFixtureFile
	for _, c := range compatCases() {
		s := extractCompat(t, c)
		sch, err := avro.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		rowHex := map[string]string{}
		for zone, loc := range compatZones {
			withLocal(loc, func() {
				enc, err := avro.Marshal(sch, rowFor(c.Cols))
				if err != nil {
					t.Fatal(err)
				}
				rowHex[zone] = hex.EncodeToString(enc)
			})
		}
		out.Schemas = append(out.Schemas, compatFixture{compatCase: c, Schema: s, RowHex: rowHex})
	}
	m := pgtype.NewMap()
	for _, c := range typeChangeCases() {
		ot, _ := m.TypeForName(c.OldType)
		nt, _ := m.TypeForName(c.NewType)
		c.Want = schema.Avro.TypeChangePreservesSchema(ot.OID, c.OldMod, nt.OID, c.NewMod)
		out.TypeChanges = append(out.TypeChanges, c)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
