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

	"github.com/conduitio/conduit-connector-postgres/source/common"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/hamba/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func Test_AvroExtract(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	c := test.ConnectSimple(ctx, t, test.RegularConnString)
	connPool, err := cpool.New(ctx, test.RegularConnString)
	is.NoErr(err)

	table := setupAvroTestTable(ctx, t, c)
	tableInfoFetcher := common.NewTableInfoFetcher(connPool)
	err = tableInfoFetcher.Refresh(ctx, table)
	is.NoErr(err)

	insertAvroTestRow(ctx, t, c, table)

	rows, err := c.Query(ctx, "SELECT * FROM "+table)
	is.NoErr(err)
	defer rows.Close()

	rows.Next()

	values, err := rows.Values()
	is.NoErr(err)

	fields := rows.FieldDescriptions()

	sch, err := Avro.Extract(table, tableInfoFetcher.GetTable(table), fields)
	is.NoErr(err)

	t.Run("schema is parsable", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(err)
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
	})
}

func setupAvroTestTable(ctx context.Context, t *testing.T, conn test.Querier) string {
	is := is.New(t)
	table := test.RandomIdentifier(t)

	query := `
       CREATE TABLE %s (
         id                      bigserial PRIMARY KEY,
         col_bytea               bytea,
         col_bytea_not_null      bytea NOT NULL,
         col_varchar             varchar(10),
         col_varchar_not_null    varchar(10) NOT NULL,
         col_date                date,
         col_date_not_null       date NOT NULL,
         col_float4              float4,
         col_float4_not_null     float4 NOT NULL,
         col_float8              float8,
         col_float8_not_null     float8 NOT NULL,
         col_int2                int2,
         col_int2_not_null       int2 NOT NULL,
         col_int4                int4,
         col_int4_not_null       int4 NOT NULL,
         col_int8                int8,
         col_int8_not_null       int8 NOT NULL,
         col_numeric             numeric(8,2),
         col_numeric_not_null    numeric(8,2) NOT NULL,
         col_text                text,
         col_text_not_null       text NOT NULL,
         col_timestamp           timestamp,
         col_timestamp_not_null  timestamp NOT NULL,
         col_timestamptz         timestamptz,
         col_timestamptz_not_null timestamptz NOT NULL,
         col_uuid                uuid,
         col_uuid_not_null       uuid NOT NULL,
         col_json                json,
         col_json_not_null       json NOT NULL,
         col_jsonb               jsonb,
         col_jsonb_not_null      jsonb NOT NULL,
         col_bool                bool,
         col_bool_not_null       bool NOT NULL,
         col_serial              serial,
         col_serial_not_null     serial NOT NULL,
         col_smallserial         smallserial,
         col_smallserial_not_null smallserial NOT NULL,
         col_bigserial           bigserial,
         col_bigserial_not_null  bigserial NOT NULL
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
          col_bytea,
          col_bytea_not_null,
          col_varchar,
          col_varchar_not_null,
          col_date,
          col_date_not_null,
          col_float4,
          col_float4_not_null,
          col_float8,
          col_float8_not_null,
          col_int2,
          col_int2_not_null,
          col_int4,
          col_int4_not_null,
          col_int8,
          col_int8_not_null,
          col_numeric,
          col_numeric_not_null,
          col_text,
          col_text_not_null,
          col_timestamp,
          col_timestamp_not_null,
          col_timestamptz,
          col_timestamptz_not_null,
          col_uuid,
          col_uuid_not_null,
          col_json,
          col_json_not_null,
          col_jsonb,
          col_jsonb_not_null,
          col_bool,
          col_bool_not_null,
          col_serial,
          col_serial_not_null,
          col_smallserial,
          col_smallserial_not_null,
          col_bigserial,
          col_bigserial_not_null
       ) VALUES (
         '\x07',                                     -- col_bytea
         '\x08',                                     -- col_bytea_not_null
         '9',                                        -- col_varchar
         '10',                                       -- col_varchar_not_null
         '2022-03-14',                               -- col_date
         '2022-03-15',                               -- col_date_not_null
         15,                                         -- col_float4
         16,                                         -- col_float4_not_null
         16.16,                                      -- col_float8
         17.17,                                      -- col_float8_not_null
         32767,                                      -- col_int2
         32766,                                      -- col_int2_not_null
         2147483647,                                 -- col_int4
         2147483646,                                 -- col_int4_not_null
         9223372036854775807,                        -- col_int8
         9223372036854775806,                        -- col_int8_not_null
         '292929.29',                                -- col_numeric
         '292928.28',                                -- col_numeric_not_null
         'foo bar baz',                              -- col_text
         'foo bar baz not null',                     -- col_text_not_null
         '2022-03-14 15:16:17',                      -- col_timestamp
         '2022-03-14 15:16:18',                      -- col_timestamp_not_null
         '2022-03-14 15:16:17-08',                   -- col_timestamptz
         '2022-03-14 15:16:18-08',                   -- col_timestamptz_not_null
         'bd94ee0b-564f-4088-bf4e-8d5e626caf66',     -- col_uuid
         'bd94ee0b-564f-4088-bf4e-8d5e626caf67',     -- col_uuid_not_null
         '{"key": "value"}',                         -- col_json
         '{"key": "value_not_null"}',                -- col_json_not_null
         '{"key": "value"}',                         -- col_jsonb
         '{"key": "value_not_null"}',                -- col_jsonb_not_null
         true,                                       -- col_bool
         false,                                      -- col_bool_not_null
         100,                                        -- col_serial
         101,                                        -- col_serial_not_null
         200,                                        -- col_smallserial
         201,                                        -- col_smallserial_not_null
         300,                                        -- col_bigserial
         301                                         -- col_bigserial_not_null
       )`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func avroTestSchema(t *testing.T, table string) avro.Schema {
	is := is.New(t)

	fields := []*avro.Field{
		// Primary key - bigserial (not null)
		assert(avro.NewField("id", avro.NewPrimitiveSchema(avro.Long, nil))),

		// bytea fields
		assert(avro.NewField("col_bytea", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Bytes, nil),
		})))),
		assert(avro.NewField("col_bytea_not_null", avro.NewPrimitiveSchema(avro.Bytes, nil))),

		// varchar fields
		assert(avro.NewField("col_varchar", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.String, nil),
		})))),
		assert(avro.NewField("col_varchar_not_null", avro.NewPrimitiveSchema(avro.String, nil))),

		// date fields
		assert(avro.NewField("col_date", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date)),
		})))),
		assert(avro.NewField("col_date_not_null", avro.NewPrimitiveSchema(
			avro.Int,
			avro.NewPrimitiveLogicalSchema(avro.Date),
		))),

		// float4 fields
		assert(avro.NewField("col_float4", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Float, nil),
		})))),
		assert(avro.NewField("col_float4_not_null", avro.NewPrimitiveSchema(avro.Float, nil))),

		// float8 fields
		assert(avro.NewField("col_float8", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Double, nil),
		})))),
		assert(avro.NewField("col_float8_not_null", avro.NewPrimitiveSchema(avro.Double, nil))),

		// int2 fields
		assert(avro.NewField("col_int2", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Int, nil),
		})))),
		assert(avro.NewField("col_int2_not_null", avro.NewPrimitiveSchema(avro.Int, nil))),

		// int4 fields
		assert(avro.NewField("col_int4", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Int, nil),
		})))),
		assert(avro.NewField("col_int4_not_null", avro.NewPrimitiveSchema(avro.Int, nil))),

		// int8 fields
		assert(avro.NewField("col_int8", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Long, nil),
		})))),
		assert(avro.NewField("col_int8_not_null", avro.NewPrimitiveSchema(avro.Long, nil))),

		// numeric fields
		assert(avro.NewField("col_numeric", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(8, 2)),
		})))),
		assert(avro.NewField("col_numeric_not_null", avro.NewPrimitiveSchema(
			avro.Bytes,
			avro.NewDecimalLogicalSchema(8, 2),
		))),

		// text fields
		assert(avro.NewField("col_text", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.String, nil),
		})))),
		assert(avro.NewField("col_text_not_null", avro.NewPrimitiveSchema(avro.String, nil))),

		// timestamp fields
		assert(avro.NewField("col_timestamp", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.LocalTimestampMicros)),
		})))),
		assert(avro.NewField("col_timestamp_not_null", avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.LocalTimestampMicros),
		))),

		// timestamptz fields
		assert(avro.NewField("col_timestamptz", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros)),
		})))),
		assert(avro.NewField("col_timestamptz_not_null", avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
		))),

		// uuid fields
		assert(avro.NewField("col_uuid", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID)),
		})))),
		assert(avro.NewField("col_uuid_not_null", avro.NewPrimitiveSchema(
			avro.String,
			avro.NewPrimitiveLogicalSchema(avro.UUID),
		))),

		// json fields (represented as strings in Avro)
		assert(avro.NewField("col_json", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Bytes, nil),
		})))),
		assert(avro.NewField("col_json_not_null", avro.NewPrimitiveSchema(avro.Bytes, nil))),

		// jsonb fields (represented as strings in Avro)
		assert(avro.NewField("col_jsonb", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Bytes, nil),
		})))),
		assert(avro.NewField("col_jsonb_not_null", avro.NewPrimitiveSchema(avro.Bytes, nil))),

		// bool fields
		assert(avro.NewField("col_bool", assert(avro.NewUnionSchema([]avro.Schema{
			avro.NewPrimitiveSchema(avro.Null, nil),
			avro.NewPrimitiveSchema(avro.Boolean, nil),
		})))),
		assert(avro.NewField("col_bool_not_null", avro.NewPrimitiveSchema(avro.Boolean, nil))),

		// serial fields (represented as int in Avro)
		assert(avro.NewField("col_serial", avro.NewPrimitiveSchema(avro.Int, nil))),
		assert(avro.NewField("col_serial_not_null", avro.NewPrimitiveSchema(avro.Int, nil))),

		// smallserial fields (represented as int in Avro)
		assert(avro.NewField("col_smallserial", avro.NewPrimitiveSchema(avro.Int, nil))),
		assert(avro.NewField("col_smallserial_not_null", avro.NewPrimitiveSchema(avro.Int, nil))),

		// bigserial fields (represented as long in Avro)
		assert(avro.NewField("col_bigserial", avro.NewPrimitiveSchema(avro.Long, nil))),
		assert(avro.NewField("col_bigserial_not_null", avro.NewPrimitiveSchema(avro.Long, nil))),
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
			n.SetString(fmt.Sprint(types.Format(0, values[i], true)))
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
