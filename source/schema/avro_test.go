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
	"github.com/hamba/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func Test_AvroExtract(t *testing.T) {
	ctx := context.Background()
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

	t.Run("schema is parsable", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(err)
		is.Equal(sch, avroTestSchema(t, table))

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
		  col_uuid          uuid
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
			col_uuid
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
		  'bd94ee0b-564f-4088-bf4e-8d5e626caf66'      -- col_uuid
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
		assert(avro.NewField("col_numeric",
			assert(avro.NewFixedSchema(string(avro.Decimal),
				avroNS,
				18,
				avro.NewDecimalLogicalSchema(8, 2),
			)))),
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
	}

	slices.SortFunc(fields, func(a, b *avro.Field) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	s, err := avro.NewRecordSchema(table, avroNS, fields)
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
