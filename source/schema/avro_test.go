// Copyright © 2024 Meroxa, Inc.
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
	"cmp"
	"context"
	"fmt"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func Test_AvroExtract(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	c := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := setupAvroTestTable(ctx, t, c)
	insertAvroTestRow(ctx, t, c, table)

	rows, err := c.Query(ctx, "SELECT * FROM "+table)
	is.NoErr(err)
	defer rows.Close()

	rows.Next()

	values, err := rows.Values()
	is.NoErr(err)

	fields := rows.FieldDescriptions()

	sch, err := Avro.Extract(table, fields)
	is.NoErr(err)

	t.Run("schema is parsable", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(err)
		// Compare the text: the extractor's primitive schemas are shared, and
		// the Avro library caches a fingerprint in them once any test
		// (TestJSONStringSchema_CompatibleWithBytesSchema) computes one.
		is.Equal(sch.String(), avroTestSchema(t, table).String())

		_, err = avro.Parse(sch.String())
		is.NoErr(err)
	})

	t.Run("serde row", func(t *testing.T) {
		is := is.New(t)

		row := avrolizeMap(fields, values)

		sch, err := avro.Parse(sch.String())
		is.NoErr(err)

		data, err := avro.Marshal(sch, row)
		is.NoErr(err)
		is.True(len(data) > 0)

		decoded := make(map[string]any)
		is.NoErr(avro.Unmarshal(sch, data, &decoded))

		is.Equal(len(decoded), len(row))
		is.Equal(row["col_boolean"], decoded["col_boolean"])
		is.Equal(row["col_bytea"], decoded["col_bytea"])
		is.Equal(row["col_varchar"], decoded["col_varchar"])
		is.Equal(row["col_date"], decoded["col_date"])
		is.Equal(row["col_float4"], decoded["col_float4"])
		is.Equal(row["col_float8"], decoded["col_float8"])

		colInt2 := int(row["col_int2"].(int16))
		is.Equal(colInt2, decoded["col_int2"])

		colInt4 := int(row["col_int4"].(int32))
		is.Equal(colInt4, decoded["col_int4"])

		is.Equal(row["col_int8"], decoded["col_int8"])

		numRow := row["col_numeric"].(*big.Rat)
		numDecoded := decoded["col_numeric"].(*big.Rat)
		is.Equal(numRow.RatString(), numDecoded.RatString())

		is.Equal(row["col_text"], decoded["col_text"])

		rowTS, colTS := row["col_timestamp"].(time.Time), decoded["col_timestamp"].(time.Time)
		is.Equal(rowTS.UTC().String(), colTS.UTC().String())

		rowTSTZ, colTSTZ := row["col_timestamptz"].(time.Time), decoded["col_timestamptz"].(time.Time)
		is.Equal(rowTSTZ.UTC().String(), colTSTZ.UTC().String())

		is.Equal(row["col_uuid"], decoded["col_uuid"])

		// json keeps its text as stored, jsonb its canonical text; both are
		// strings, numbers are not rounded and keys are not reordered.
		is.Equal(decoded["col_json"], `{"b": 1,  "a": [1.0, 12345678901234567890]}`)
		is.Equal(decoded["col_jsonb"], `{"a": [1.0, 12345678901234567890], "b": 1}`)
	})
}

func setupAvroTestTable(ctx context.Context, t *testing.T, conn test.Querier) string {
	is := is.New(t)
	table := test.RandomIdentifier(t)

	query := `
		CREATE TABLE %s (
		  col_boolean       boolean,
		  col_bytea         bytea,
		  col_varchar       varchar(10),
		  col_date          date,
		  col_float4        float4,
		  col_float8        float8,
		  col_int2          int2,
		  col_int4          int4,
		  col_int8          int8,
		  col_numeric       numeric(8,2),
		  col_text          text,
		  col_timestamp     timestamp,
		  col_timestamptz   timestamptz,
		  col_uuid          uuid,
		  col_json          json,
		  col_jsonb         jsonb
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	return table
}

func insertAvroTestRow(ctx context.Context, t *testing.T, conn test.Querier, table string) {
	is := is.New(t)
	query := `
		INSERT INTO %s (
			col_boolean,
			col_bytea,
			col_varchar,
			col_date,
			col_float4,
			col_float8,
			col_int2,
			col_int4,
			col_int8,
			col_numeric,
			col_text,
			col_timestamp,
			col_timestamptz,
			col_uuid,
			col_json,
			col_jsonb
		) VALUES (
		  true,                                       -- col_boolean
		  '\x07',                                     -- col_bytea
		  '9',                                        -- col_varchar
		  '2022-03-14',                               -- col_date
		  15,                                         -- col_float4
		  16.16,                                      -- col_float8
		  32767,                                      -- col_int2
		  2147483647,                                 -- col_int4
		  9223372036854775807,                        -- col_int8
		  '292929.29',                                -- col_numeric
		  'foo bar baz',                              -- col_text
		  '2022-03-14 15:16:17',                      -- col_timestamp
		  '2022-03-14 15:16:17-08',                   -- col_timestamptz
		  'bd94ee0b-564f-4088-bf4e-8d5e626caf66',     -- col_uuid
		  '{"b": 1,  "a": [1.0, 12345678901234567890]}', -- col_json
		  '{"b": 1,  "a": [1.0, 12345678901234567890]}'  -- col_jsonb
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func avroTestSchema(t *testing.T, table string) avro.Schema {
	is := is.New(t)

	fields := []*avro.Field{
		assert(avro.NewField("col_boolean", avro.NewPrimitiveSchema(avro.Boolean, nil))),
		assert(avro.NewField("col_bytea", avro.NewPrimitiveSchema(avro.Bytes, nil))),
		assert(avro.NewField("col_varchar", avro.NewPrimitiveSchema(avro.String, nil))),
		assert(avro.NewField("col_float4", avro.NewPrimitiveSchema(avro.Float, nil))),
		assert(avro.NewField("col_float8", avro.NewPrimitiveSchema(avro.Double, nil))),
		assert(avro.NewField("col_int2", avro.NewPrimitiveSchema(avro.Int, nil))),
		assert(avro.NewField("col_int4", avro.NewPrimitiveSchema(avro.Int, nil))),
		assert(avro.NewField("col_int8", avro.NewPrimitiveSchema(avro.Long, nil))),
		assert(avro.NewField("col_text", avro.NewPrimitiveSchema(avro.String, nil))),
		assert(avro.NewField("col_numeric", avro.NewPrimitiveSchema(
			avro.Bytes,
			avro.NewDecimalLogicalSchema(8, 2),
		))),
		assert(avro.NewField("col_date", avro.NewPrimitiveSchema(
			avro.Int,
			avro.NewPrimitiveLogicalSchema(avro.Date),
		))),
		assert(avro.NewField("col_timestamp", avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.LocalTimestampMicros),
		))),
		assert(avro.NewField("col_timestamptz", avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
		))),
		assert(avro.NewField("col_uuid", avro.NewPrimitiveSchema(
			avro.String,
			avro.NewPrimitiveLogicalSchema(avro.UUID),
		))),
		assert(avro.NewField("col_json", avro.NewPrimitiveSchema(avro.String, nil))),
		assert(avro.NewField("col_jsonb", avro.NewPrimitiveSchema(avro.String, nil))),
	}

	slices.SortFunc(fields, func(a, b *avro.Field) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	s, err := avro.NewRecordSchema(table, "", fields)
	is.NoErr(err)

	return s
}

func avrolizeMap(fields []pgconn.FieldDescription, values []any) map[string]any {
	row := make(map[string]any)

	for i, f := range fields {
		switch f.DataTypeOID {
		case pgtype.NumericOID:
			n := new(big.Rat)
			n.SetString(fmt.Sprint(types.Format(0, values[i])))
			row[f.Name] = n
		case pgtype.UUIDOID:
			row[f.Name] = fmt.Sprint(values[i])
		case pgtype.JSONOID, pgtype.JSONBOID:
			row[f.Name] = assert(types.Format(f.DataTypeOID, values[i]))
		default:
			row[f.Name] = values[i]
		}
	}

	return row
}

func assert[T any](a T, err error) T {
	if err != nil {
		panic(err)
	}

	return a
}

// An unconstrained numeric is an Avro string (#375); numeric(p,s) stays a
// decimal.
func TestAvroExtract_UnconstrainedNumericIsString(t *testing.T) {
	is := is.New(t)
	for _, tc := range []struct {
		typMod int32
		want   string
	}{
		{-1, `{"name":"t_payload","type":"record","fields":[{"name":"n","type":"string"}]}`},
		{10<<16 | 2 + 4, `{"name":"t_payload","type":"record","fields":[{"name":"n","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`},
	} {
		t.Run(fmt.Sprintf("typmod=%d", tc.typMod), func(t *testing.T) {
			is := is.New(t)
			sch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
				{Name: "n", DataTypeOID: pgtype.NumericOID, TypeModifier: tc.typMod},
			})
			is.NoErr(err)
			is.Equal(sch.String(), tc.want)
		})
	}
}

// json and jsonb are Avro strings, as Debezium emits them
// (io.debezium.data.Json); they were bytes before v0.21.0.
func TestAvroExtract_JSONIsString(t *testing.T) {
	is := is.New(t)
	sch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "j", DataTypeOID: pgtype.JSONOID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	})
	is.NoErr(err)
	is.Equal(sch.String(), `{"name":"t_payload","type":"record","fields":[{"name":"j","type":"string"},{"name":"jb","type":"string"}]}`)

	// A jsonb key column (jsonb has a btree opclass, so it can be a primary
	// key) is a string in the key schema too.
	key, err := Avro.Extract("t_key", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	}, "jb")
	is.NoErr(err)
	is.Equal(key.String(), `{"name":"t_key","type":"record","fields":[{"name":"jb","type":"string"}]}`)
}

// The registered text of a json or jsonb field carries Debezium's Kafka
// Connect name, as Confluent's Avro converter writes it for Debezium:
// connect.name and connect.version on the field's type. The canonical form
// (String) stays "string": the properties are not part of the Avro type.
func TestMarshal_JSONCarriesDebeziumName(t *testing.T) {
	is := is.New(t)
	const jsonType = `{"type":"string","connect.name":"io.debezium.data.Json","connect.version":1}`

	sch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "j", DataTypeOID: pgtype.JSONOID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	})
	is.NoErr(err)
	text, err := Marshal(sch)
	is.NoErr(err)
	is.Equal(string(text), `{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},`+
		`{"name":"j","type":`+jsonType+`},{"name":"jb","type":`+jsonType+`}]}`)
	is.Equal(sch.String(), `{"name":"t_payload","type":"record","fields":[{"name":"id","type":"long"},{"name":"j","type":"string"},{"name":"jb","type":"string"}]}`)

	key, err := Avro.Extract("t_key", []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "jb", DataTypeOID: pgtype.JSONBOID, TypeModifier: -1},
	}, "jb")
	is.NoErr(err)
	keyText, err := Marshal(key)
	is.NoErr(err)
	is.Equal(string(keyText), `{"name":"t_key","type":"record","fields":[{"name":"jb","type":`+jsonType+`}]}`)

	// What a registry hands back parses to the same properties.
	parsed, err := avro.Parse(string(text))
	is.NoErr(err)
	for _, name := range []string{"j", "jb"} {
		var f *avro.Field
		for _, pf := range parsed.(*avro.RecordSchema).Fields() {
			if pf.Name() == name {
				f = pf
			}
		}
		is.True(f != nil)
		ps, ok := f.Type().(*avro.PrimitiveSchema)
		is.True(ok)
		is.Equal(ps.Type(), avro.String)
		is.Equal(ps.Prop("connect.name"), DebeziumJSONName)
		is.Equal(ps.Prop("connect.version"), float64(1)) // a JSON number
	}
}

// Registering Marshal's text instead of String's changes nothing for a
// schema without properties: every other type the extractor emits has the
// same text either way. Only json and jsonb fields gain their properties.
func TestMarshal_EqualsStringWithoutProps(t *testing.T) {
	var fields []pgconn.FieldDescription
	for name := range Avro.avroMap {
		if name == "json" || name == "jsonb" {
			continue
		}
		typ, ok := Avro.pgMap.TypeForName(name)
		if !ok {
			t.Fatalf("no pgtype for %q", name)
		}
		fields = append(fields, pgconn.FieldDescription{Name: "f_" + name, DataTypeOID: typ.OID, TypeModifier: -1})
	}
	fields = append(fields,
		pgconn.FieldDescription{Name: "n_unconstrained", DataTypeOID: pgtype.NumericOID, TypeModifier: -1},
		pgconn.FieldDescription{Name: "n_scale0", DataTypeOID: pgtype.NumericOID, TypeModifier: 5<<16 + 4},
		pgconn.FieldDescription{Name: "n_scale3", DataTypeOID: pgtype.NumericOID, TypeModifier: 16<<16 | 3 + 4},
	)
	for _, f := range fields {
		t.Run(f.Name, func(t *testing.T) {
			is := is.New(t)
			sch, err := Avro.Extract("t_payload", []pgconn.FieldDescription{f})
			is.NoErr(err)
			text, err := Marshal(sch)
			is.NoErr(err)
			is.Equal(string(text), sch.String())
		})
	}
}

func TestTypeChangePreservesValues(t *testing.T) {
	for _, tc := range []struct {
		name           string
		oldOID, newOID uint32
		oldMod, newMod int32
		avro, noAvro   bool
	}{
		{"text to unconstrained numeric", pgtype.TextOID, pgtype.NumericOID, -1, -1, true, false},
		{"unconstrained numeric to varchar", pgtype.NumericOID, pgtype.VarcharOID, -1, 20, true, false},
		{"text to varchar", pgtype.TextOID, pgtype.VarcharOID, -1, 20, true, true},
		{"unconstrained to constrained numeric", pgtype.NumericOID, pgtype.NumericOID, -1, 10<<16 | 2 + 4, false, false},
		{"constrained numeric widened", pgtype.NumericOID, pgtype.NumericOID, 10<<16 | 2 + 4, 12<<16 | 2 + 4, false, false},
		{"unconstrained numeric unchanged", pgtype.NumericOID, pgtype.NumericOID, -1, -1, true, true},
		{"json to jsonb", pgtype.JSONOID, pgtype.JSONBOID, -1, -1, true, true},
		{"jsonb to json", pgtype.JSONBOID, pgtype.JSONOID, -1, -1, true, true},
		{"bytea to jsonb", pgtype.ByteaOID, pgtype.JSONBOID, -1, -1, false, false},
		{"jsonb to bytea", pgtype.JSONBOID, pgtype.ByteaOID, -1, -1, false, false},
		{"text to jsonb", pgtype.TextOID, pgtype.JSONBOID, -1, -1, false, false},
		{"json to varchar", pgtype.JSONOID, pgtype.VarcharOID, -1, 20, false, false},
		{"jsonb to unconstrained numeric", pgtype.JSONBOID, pgtype.NumericOID, -1, -1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(Avro.TypeChangePreservesValues(true, tc.oldOID, tc.oldMod, tc.newOID, tc.newMod), tc.avro)
			is.Equal(Avro.TypeChangePreservesValues(false, tc.oldOID, tc.oldMod, tc.newOID, tc.newMod), tc.noAvro)
		})
	}
}
