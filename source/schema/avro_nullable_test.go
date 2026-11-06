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

package schema

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// nullabilityTypes is every Postgres type the extractor maps, with a type
// modifier where it changes the schema.
var nullabilityTypes = []struct {
	pgType string
	typMod int32
}{
	{"bool", -1},
	{"bytea", -1},
	{"float4", -1},
	{"float8", -1},
	{"int2", -1},
	{"int4", -1},
	{"int8", -1},
	{"text", -1},
	{"varchar", 14},
	{"json", -1},
	{"jsonb", -1},
	{"tsvector", -1},
	{"timestamptz", -1},
	{"timestamp", -1},
	{"date", -1},
	{"uuid", -1},
	{"numeric", 10<<16 | 2 + 4}, // numeric(10,2): bytes, decimal
	{"numeric", -1},             // unconstrained: string (#375)
}

// sampleFor returns a non-NULL value of the shape the connector hands the
// encoder for a NOT NULL field of type s (see types.FormatFor with the Avro
// encoding). A nullable column's value is that passed through
// types.NullableAvroValue.
func sampleFor(t *testing.T, s avro.Schema) any {
	t.Helper()
	ps, ok := s.(*avro.PrimitiveSchema)
	if !ok {
		t.Fatalf("not a primitive schema: %s", s)
	}
	if ls := ps.Logical(); ls != nil {
		switch ls.Type() {
		case avro.Decimal:
			return big.NewRat(3, 2)
		case avro.TimestampMicros, avro.Date:
			return time.Date(2022, 3, 14, 0, 0, 0, 0, time.UTC)
		case avro.LocalTimestampMicros:
			// The wall clock's microseconds, as types.FormatFor emits a
			// timestamp under EncodingAvro (#302).
			return int64(1647270977000000)
		case avro.UUID:
			return "bd94ee0b-564f-4088-bf4e-8d5e626caf66"
		}
		t.Fatalf("no sample for logical type %s", ls.Type())
	}
	switch ps.Type() {
	case avro.Boolean:
		return true
	case avro.Bytes:
		return []byte{7}
	case avro.Float:
		return float32(1.5)
	case avro.Double:
		return 1.5
	case avro.Int:
		return int32(7)
	case avro.Long:
		return int64(7)
	case avro.String:
		return "x"
	}
	t.Fatalf("no sample for %s", ps.Type())
	return nil
}

func extractOne(t *testing.T, pgType string, typMod int32, notNull bool) *avro.RecordSchema {
	t.Helper()
	typ, ok := pgtype.NewMap().TypeForName(pgType)
	if !ok {
		t.Fatalf("unknown type %q", pgType)
	}
	sch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "c", DataTypeOID: typ.OID, TypeModifier: typMod},
	}, NotNull{"c": notNull})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	return sch
}

// mustText is the schema text the connector registers (Text).
func mustText(t *testing.T, sch avro.Schema) string {
	t.Helper()
	b, err := Text(sch)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestAvroExtract_NullableColumns is the unit regression test for #326: a
// column that is not NOT NULL is ["null", T] with a default of null, for every
// mapped type, and a NULL in it is encoded by the SDK's serde and decoded as
// null. A NOT NULL column keeps the plain type, and a NULL in it fails to
// encode instead of becoming a zero value (invariant 6).
func TestAvroExtract_NullableColumns(t *testing.T) {
	for _, tc := range nullabilityTypes {
		t.Run(fmt.Sprintf("%s(%d)", tc.pgType, tc.typMod), func(t *testing.T) {
			// NOT NULL: a plain, required field. Its text is pinned per type
			// by the hamba fixtures (avro_hamba_compat_test.go).
			plain := extractOne(t, tc.pgType, tc.typMod, true)
			pf := plain.Fields()[0]
			if pf.HasDefault() {
				t.Fatalf("NOT NULL field has a default: %s", mustText(t, plain))
			}
			if _, isUnion := pf.Type().(*avro.UnionSchema); isUnion {
				t.Fatalf("NOT NULL field is a union: %s", mustText(t, plain))
			}
			typeJSON := pf.Type().String()

			nullable := extractOne(t, tc.pgType, tc.typMod, false)
			want := `{"name":"t_payload","type":"record","fields":[{"name":"c","type":["null",` + typeJSON + `],"default":null}]}`
			if got := mustText(t, nullable); got != want {
				t.Fatalf("nullable schema\n got: %s\nwant: %s", got, want)
			}

			t.Run("nullable column", func(t *testing.T) {
				is := is.New(t)
				serde, err := cavro.Parse([]byte(mustText(t, nullable)))
				is.NoErr(err)

				enc, err := serde.Marshal(opencdc.StructuredData{"c": nil})
				is.NoErr(err) // the #326 failure: "*avro.null is unsupported for Avro <T>"
				var got map[string]any
				is.NoErr(serde.Unmarshal(enc, &got))
				v, present := got["c"]
				is.True(present)
				is.Equal(v, nil) // NULL decodes as null, not a zero value

				typ, _ := pgtype.NewMap().TypeForName(tc.pgType)
				enc, err = serde.Marshal(opencdc.StructuredData{"c": types.NullableAvroValue(typ.OID, sampleFor(t, pf.Type()))})
				is.NoErr(err)
				got = nil
				is.NoErr(serde.Unmarshal(enc, &got))
				is.True(got["c"] != nil) // a value stays a value
			})

			t.Run("NOT NULL column", func(t *testing.T) {
				is := is.New(t)
				serde, err := cavro.Parse([]byte(mustText(t, plain)))
				is.NoErr(err)

				_, err = serde.Marshal(opencdc.StructuredData{"c": nil})
				is.True(err != nil) // never coerced to a zero value
				_, err = serde.Marshal(opencdc.StructuredData{})
				is.True(err != nil) // a missing NOT NULL column is not NULL either

				enc, err := serde.Marshal(opencdc.StructuredData{"c": sampleFor(t, pf.Type())})
				is.NoErr(err)
				var got map[string]any
				is.NoErr(serde.Unmarshal(enc, &got))
				is.True(got["c"] != nil)
			})
		})
	}
}

// TestAvroExtract_MissingFieldTakesNullDefault pins the encoder behavior the
// CDC handler's unchanged-TOAST refusal (requireAllColumns) depends on: a
// nullable field that is missing from the data is encoded as its default, null.
// If this ever stops being true the refusal is still correct, only no longer
// the only thing in the way.
func TestAvroExtract_MissingFieldTakesNullDefault(t *testing.T) {
	is := is.New(t)
	serde, err := cavro.Parse([]byte(mustText(t, extractOne(t, "text", -1, false))))
	is.NoErr(err)
	enc, err := serde.Marshal(opencdc.StructuredData{})
	is.NoErr(err)
	var got map[string]any
	is.NoErr(serde.Unmarshal(enc, &got))
	v, present := got["c"]
	is.True(present)
	is.Equal(v, nil)
}

// TestAvroExtract_NullabilityRequired pins that nullability is never assumed:
// a column without an entry in NotNull fails the extraction, so a caller that
// did not read the catalog cannot make every field optional (#326).
func TestAvroExtract_NullabilityRequired(t *testing.T) {
	is := is.New(t)
	fields := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "c", DataTypeOID: pgtype.TextOID, TypeModifier: -1},
	}
	for _, nn := range []NotNull{nil, {}, {"id": true}} {
		_, err := Avro.Extract("t_payload", fields, nn)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), "nullability unknown"))
	}

	// Only the included fields need an entry: the key schema of a table whose
	// key is NOT NULL is plain whatever the other columns are.
	sch, err := Avro.Extract("t_key", fields, NotNull{"id": true}, "id")
	is.NoErr(err)
	is.Equal(mustText(t, sch), `{"name":"t_key","type":"record","fields":[{"name":"id","type":"long"}]}`)
}

// TestAvroExtract_NullableRegistryCompatibility records the schema-registry
// verdict for the upgrade (#326), with the library's own resolution rules:
// before the fix every column was registered as T; after it a nullable column
// is ["null", T]. The upgrade is BACKWARD compatible (a reader with the new
// schema reads data written with the old one: T resolves into the union) and
// not FORWARD compatible (a reader still on the old schema cannot read a null
// written with the new one). A subject on FULL or FORWARD compatibility
// therefore rejects the new version.
func TestAvroExtract_NullableRegistryCompatibility(t *testing.T) {
	for _, tc := range nullabilityTypes {
		t.Run(fmt.Sprintf("%s(%d)", tc.pgType, tc.typMod), func(t *testing.T) {
			is := is.New(t)
			parse := func(sch *avro.RecordSchema) avro.Schema {
				s, err := avro.ParseWithCache(mustText(t, sch), "", &avro.SchemaCache{})
				is.NoErr(err)
				return s
			}
			before := parse(extractOne(t, tc.pgType, tc.typMod, true)) // what v0.20 registered
			after := parse(extractOne(t, tc.pgType, tc.typMod, false))
			c := avro.NewSchemaCompatibility()
			is.NoErr(c.Compatible(after, before))       // BACKWARD: new reader, old data
			is.True(c.Compatible(before, after) != nil) // not FORWARD: old reader, new data
		})
	}

	// The registered default of null is what makes adding a nullable column a
	// BACKWARD-compatible change; without it (the canonical form) it is not.
	is := is.New(t)
	fields := []pgconn.FieldDescription{{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1}}
	old, err := Avro.Extract("t_payload", fields, NotNull{"id": true})
	is.NoErr(err)
	added, err := Avro.Extract("t_payload", append(fields, pgconn.FieldDescription{Name: "c", DataTypeOID: pgtype.TextOID, TypeModifier: -1}), NotNull{"id": true, "c": false})
	is.NoErr(err)
	c := avro.NewSchemaCompatibility()
	// Fresh caches: Parse would return the named record parsed first.
	withDefault, err := avro.ParseWithCache(mustText(t, added), "", &avro.SchemaCache{})
	is.NoErr(err)
	is.NoErr(c.Compatible(withDefault, old))
	is.True(withDefault.(*avro.RecordSchema).Fields()[0].HasDefault())
	canonical, err := avro.ParseWithCache(added.String(), "", &avro.SchemaCache{})
	is.NoErr(err)
	// A new checker: results are cached by fingerprint, which is computed
	// from the canonical form and so is the same for both.
	is.True(avro.NewSchemaCompatibility().Compatible(canonical, old) != nil)
}
