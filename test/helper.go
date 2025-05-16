// Copyright © 2022 Meroxa, Inc.
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

package test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// RepmgrConnString is a replication user connection string for the test postgres.
const RepmgrConnString = "postgres://repmgr:repmgrmeroxa@127.0.0.1:5433/meroxadb?sslmode=disable"

// RegularConnString is a non-replication user connection string for the test postgres.
const RegularConnString = "postgres://meroxauser:meroxapass@127.0.0.1:5433/meroxadb?sslmode=disable"

// TestTableAvroSchemaV1 is the Avro schema representation of the test table
// defined through testTableCreateQuery.
// The fields are sorted by name.
const TestTableAvroSchemaV1 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {
            "name": "column4",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 16,
                "scale": 3
            }
        },
        {
            "name": "column5",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 5
            }
        },
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableAvroSchemaV2 is TestTableAvroSchemaV1 with `column6` (local-timestamp-micros) added.
const TestTableAvroSchemaV2 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column101","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {
            "name": "column4",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 16,
                "scale": 3
            }
        },
        {
            "name": "column5",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 5
            }
        },
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableAvroSchemaV3 is TestTableAvroSchemaV1 with `column4` and `column5` dropped.
const TestTableAvroSchemaV3 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column101","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableKeyAvroSchema is the Avro schema for the test table's key column.
const TestTableKeyAvroSchema = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
        {"name":"id","type":"long"}
    ]
}`

// When updating this table, TestTableAvroSchemaV1 needs to be updated too.
const testTableCreateQuery = `
		CREATE TABLE %q (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean,
		column4 numeric(16,3),
		column5 numeric(5),
		column6 jsonb,
		column7 json,
		"UppercaseColumn1" integer
	)`

type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func ConnectPool(ctx context.Context, t *testing.T, connString string) *pgxpool.Pool {
	is := is.New(t)
	pool, err := cpool.New(ctx, connString)
	is.NoErr(err)
	t.Cleanup(func() {
		// close connection with fresh context
		is := is.New(t)
		is.NoErr(csync.RunTimeout(context.Background(), pool.Close, time.Second*10))
	})
	return pool
}

func ConnectSimple(ctx context.Context, t *testing.T, connString string) *pgx.Conn {
	is := is.New(t)
	pool := ConnectPool(ctx, t, connString)
	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	t.Cleanup(func() {
		conn.Release()
	})
	return conn.Conn()
}

// SetupTestTable creates a new table and returns its name.
func SetupEmptyTestTable(ctx context.Context, t *testing.T, conn Querier) string {
	table := RandomIdentifier(t)
	SetupEmptyTestTableWithName(ctx, t, conn, table)
	return table
}

func SetupEmptyTestTableWithName(ctx context.Context, t *testing.T, conn Querier, table string) {
	is := is.New(t)

	query := fmt.Sprintf(testTableCreateQuery, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %q`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})
}

func SetupTestTableWithName(ctx context.Context, t *testing.T, conn Querier, table string) {
	is := is.New(t)
	SetupEmptyTestTableWithName(ctx, t, conn, table)

	query := `
		INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
		VALUES ('1', 'foo', 123, false, 12.2, 4, '{"foo": "bar"}', '{"foo": "baz"}', 1),
		('2', 'bar', 456, true, 13.42, 8, '{"foo": "bar"}', '{"foo": "baz"}', 2),
		('3', 'baz', 789, false, null, 9, '{"foo": "bar"}', '{"foo": "baz"}', 3),
		('4', null, null, null, 91.1, null, null, null, null)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

// SetupTestTable creates a new table and returns its name.
func SetupTestTable(ctx context.Context, t *testing.T, conn Querier) string {
	table := RandomIdentifier(t)
	SetupTestTableWithName(ctx, t, conn, table)
	return table
}

// SetupTableAllTypes creates a new table with all types and returns its name.
func SetupTableAllTypes(ctx context.Context, t *testing.T, conn Querier) string {
	is := is.New(t)
	table := RandomIdentifier(t)
	query := `
		CREATE TABLE %s (
  		  id 				bigserial PRIMARY KEY,
		  col_bit           bit(8),
		  col_varbit        varbit(8),
		  col_boolean       boolean,
		  col_box           box,
		  col_bytea         bytea,
		  col_char          char(3),
		  col_varchar       varchar(10),
		  col_cidr          cidr,
		  col_circle        circle,
		  col_date          date,
		  col_float4        float4,
		  col_float8        float8,
		  col_inet          inet,
		  col_int2          int2,
		  col_int4          int4,
		  col_int8          int8,
		  col_interval      interval,
		  col_json          json,
		  col_jsonb         jsonb,
		  col_line          line,
		  col_lseg          lseg,
		  col_macaddr       macaddr,
		  col_macaddr8      macaddr8,
		  col_money         money,
		  col_numeric       numeric(8,2),
		  col_path          path,
		  col_pg_lsn        pg_lsn,
		  col_pg_snapshot   pg_snapshot,
		  col_point         point,
		  col_polygon       polygon,
		  col_serial2       serial2,
		  col_serial4       serial4,
		  col_serial8       serial8,
		  col_text          text,
		  col_time          time,
		  col_timetz        timetz,
		  col_timestamp     timestamp,
		  col_timestamptz   timestamptz,
		  col_tsquery       tsquery,
		  col_tsvector      tsvector,
		  col_uuid          uuid,
		  col_xml           xml
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})
	return table
}

func InsertRowAllTypes(ctx context.Context, t *testing.T, conn Querier, table string) {
	is := is.New(t)
	query := `
		INSERT INTO %s (
		  col_bit,
		  col_varbit,
		  col_boolean,
		  col_box,
		  col_bytea,
		  col_char,
		  col_varchar,
		  col_cidr,
		  col_circle,
		  col_date,
		  col_float4,
		  col_float8,
		  col_inet,
		  col_int2,
		  col_int4,
		  col_int8,
		  col_interval,
		  col_json,
		  col_jsonb,
		  col_line,
		  col_lseg,
		  col_macaddr,
		  col_macaddr8,
		  col_money,
		  col_numeric,
		  col_path,
		  col_pg_lsn,
		  col_pg_snapshot,
		  col_point,
		  col_polygon,
		  col_serial2,
		  col_serial4,
		  col_serial8,
		  col_text,
		  col_time,
		  col_timetz,
		  col_timestamp,
		  col_timestamptz,
		  col_tsquery,
		  col_tsvector,
		  col_uuid,
		  col_xml
		) VALUES (
		  B'00000001',                                -- col_bit
		  B'00000010',                                -- col_varbit
		  true,                                       -- col_boolean
		  '(3,4),(5,6)',                              -- col_box
		  '\x07',                                     -- col_bytea
		  '8',                                        -- col_char
		  '9',                                        -- col_varchar
		  '192.168.100.128/25',                       -- col_cidr
		  '<(11,12),13>',                             -- col_circle
		  '2022-03-14',                               -- col_date
		  15,                                         -- col_float4
		  16.16,                                      -- col_float8
		  '192.168.0.17',                             -- col_inet
		  32767,                                      -- col_int2
		  2147483647,                                 -- col_int4
		  9223372036854775807,                        -- col_int8
		  '18 seconds',                               -- col_interval
		  '{"foo": "bar"}',                            -- col_json
		  '{"foo": "baz"}',                            -- col_jsonb
		  '{19,20,21}',                               -- col_line
		  '((22,23),(24,25))',                        -- col_lseg
		  '08:00:2b:01:02:26',                        -- col_macaddr
		  '08:00:2b:01:02:03:04:27',                  -- col_macaddr8
		  '$28',                                      -- col_money
		  '292929.29',                                -- col_numeric
		  '[(30,31),(32,33),(34,35)]',                -- col_path
		  '36/37',                                    -- col_pg_lsn
		  '10:20:10,14,15',                           -- col_pg_snapshot
		  '(38,39)',                                  -- col_point
		  '((40,41),(42,43),(44,45))',                -- col_polygon
		  32767,                                      -- col_serial2
		  2147483647,                                 -- col_serial4
		  9223372036854775807,                        -- col_serial8
		  'foo bar baz',                              -- col_text
		  '04:05:06.789',                             -- col_time
		  '04:05:06.789-08',                          -- col_timetz
		  '2022-03-14 15:16:17',                      -- col_timestamp
		  '2022-03-14 15:16:17-08',                   -- col_timestamptz
		  'fat & (rat | cat)',                        -- col_tsquery
		  'a fat cat sat on a mat and ate a fat rat', -- col_tsvector
		  'bd94ee0b-564f-4088-bf4e-8d5e626caf66',     -- col_uuid
		  '<foo>bar</foo>'                            -- col_xml
		)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

func CreateReplicationSlot(t *testing.T, conn Querier, slotName string) {
	is := is.New(t)

	_, err := conn.Exec(
		context.Background(),
		"SELECT pg_create_logical_replication_slot($1, $2)",
		slotName,
		"pgoutput",
	)
	is.NoErr(err)

	t.Cleanup(func() {
		_, err := conn.Exec(
			context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
			slotName,
		)
		is.NoErr(err)
	})
}

func CreatePublication(t *testing.T, conn Querier, pubName string, tables []string) {
	is := is.New(t)

	quotedTables := make([]string, 0, len(tables))
	for _, t := range tables {
		// don't use internal.WrapSQLIdent to prevent import cycle
		quotedTables = append(quotedTables, strconv.Quote(t))
	}

	_, err := conn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, strings.Join(quotedTables, ",")),
	)
	is.NoErr(err)

	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", pubName))
		is.NoErr(err)
	})
}

func RandomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}

func IsPgError(is *is.I, err error, wantCode string) {
	is.True(err != nil)
	var pgerr *pgconn.PgError
	ok := errors.As(err, &pgerr)
	is.True(ok) // expected err to be a *pgconn.PgError
	is.Equal(pgerr.Code, wantCode)
}

func Context(t *testing.T) context.Context {
	ctx := context.Background()
	if testing.Short() || !testing.Verbose() {
		return ctx
	}

	writer := zerolog.NewTestWriter(t)
	logger := zerolog.New(writer).Level(zerolog.InfoLevel)
	return logger.WithContext(ctx)
}
