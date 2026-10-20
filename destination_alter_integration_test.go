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

package postgres

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// alterTestModes are the query exec modes the column-type-change tests run
// in: the default (describe_exec, which describes every statement) and the
// two cached modes, which rely on the schema guard on a direct connection.
var alterTestModes = map[string]string{
	"default":         test.RegularConnString,
	"cache_statement": test.RegularConnString + "&default_query_exec_mode=cache_statement",
	"cache_describe":  test.RegularConnString + "&default_query_exec_mode=cache_describe",
}

// forEachAlterMode runs fn as a subtest for every mode in alterTestModes.
func forEachAlterMode(t *testing.T, fn func(t *testing.T, url string)) {
	for name, url := range alterTestModes {
		t.Run(name, func(t *testing.T) { fn(t, url) })
	}
}

// Regression test for #367. A cached statement keeps the parameter types
// Postgres inferred when it was first prepared. After ALTER COLUMN ... TYPE on
// a running pipeline it would keep sending parameters typed for the old
// column, and Postgres would convert them by assignment cast without an error:
// 5/2 into a column that had become integer was stored as 3 (invariant 6). The
// default (describe_exec) describes every statement, and in a cached mode the
// schema guard aborts the batch and retries with fresh types, so the value is
// refused, never rounded, and no row is stored.
func TestDestination_Write_ColumnTypeChangedWhileRunning(t *testing.T) {
	forEachAlterMode(t, func(t *testing.T, url string) {
		is := is.New(t)
		ctx := test.Context(t)
		conn := test.ConnectSimple(ctx, t, test.RegularConnString)
		table := test.RandomIdentifier(t)
		_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, amt numeric(12,2))`, table))
		is.NoErr(err)

		d := openJDBCTestDestinationURL(t, is, url)
		rec := func(id int, amt *big.Rat) opencdc.Record {
			return jdbcTestRecord(table, opencdc.OperationCreate,
				opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "amt": amt})
		}

		n, err := d.Write(ctx, []opencdc.Record{rec(1, big.NewRat(1, 1))})
		is.NoErr(err)
		is.Equal(n, 1)

		_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN amt TYPE integer`, table))
		is.NoErr(err)

		// A batch of values the new type cannot hold exactly: the guard aborts the
		// batch and retries with fresh types, so each write is refused (2.5 and 3.5
		// are not integers) and none of them is stored — atomically, no partial
		// rows.
		n, err = d.Write(ctx, []opencdc.Record{rec(2, big.NewRat(5, 2)), rec(3, big.NewRat(7, 2))})
		is.True(err != nil) // refused, never rounded to 3 and 4
		is.Equal(n, 0)

		var count int
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (2, 3)`, table)).Scan(&count))
		is.Equal(count, 0)

		// The destination stays usable after the schema change: a value that fits
		// the new type is written exactly, in order after the first row.
		n, err = d.Write(ctx, []opencdc.Record{rec(4, big.NewRat(7, 1))})
		is.NoErr(err)
		is.Equal(n, 1)

		var got int
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT amt FROM %s WHERE id = 4`, table)).Scan(&got))
		is.Equal(got, 7)
	})
}

// A widening change (integer -> bigint) is handled transparently: the guard
// fires, clears the statement cache and retries, and the write succeeds with
// the value stored type-exactly — no pipeline-visible error.
func TestDestination_Write_ColumnTypeWidenedWhileRunning(t *testing.T) {
	forEachAlterMode(t, func(t *testing.T, url string) {
		is := is.New(t)
		ctx := test.Context(t)
		conn := test.ConnectSimple(ctx, t, test.RegularConnString)
		table := test.RandomIdentifier(t)
		_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, n integer)`, table))
		is.NoErr(err)

		d := openJDBCTestDestinationURL(t, is, url)
		rec := func(id int, n int64) opencdc.Record {
			return jdbcTestRecord(table, opencdc.OperationCreate,
				opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "n": n})
		}

		n, err := d.Write(ctx, []opencdc.Record{rec(1, 5)})
		is.NoErr(err)
		is.Equal(n, 1)

		_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN n TYPE bigint`, table))
		is.NoErr(err)

		n, err = d.Write(ctx, []opencdc.Record{rec(2, 6)})
		is.NoErr(err)
		is.Equal(n, 1)

		var got int64
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT n FROM %s WHERE id = 2`, table)).Scan(&got))
		is.Equal(got, int64(6))
	})
}

// A value that overflows the OLD type (int) but fits the NEW one (bigint) and is
// the first write after the ALTER cannot be encoded into the still-cached int
// parameter, so it fails before the guard can run. The destination retries the
// batch once on an empty statement cache, which re-prepares with the bigint
// parameter, so the value is stored exactly without a pipeline-visible error,
// never silently coerced.
func TestDestination_Write_ColumnTypeWidenedOverflowRecovers(t *testing.T) {
	forEachAlterMode(t, func(t *testing.T, url string) {
		is := is.New(t)
		ctx := test.Context(t)
		conn := test.ConnectSimple(ctx, t, test.RegularConnString)
		table := test.RandomIdentifier(t)
		_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, n integer)`, table))
		is.NoErr(err)

		d := openJDBCTestDestinationURL(t, is, url)
		rec := func(id int, n int64) opencdc.Record {
			return jdbcTestRecord(table, opencdc.OperationCreate,
				opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "n": n})
		}

		n, err := d.Write(ctx, []opencdc.Record{rec(1, 5)})
		is.NoErr(err)
		is.Equal(n, 1)

		_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN n TYPE bigint`, table))
		is.NoErr(err)

		big := int64(3_000_000_000)
		n, err = d.Write(ctx, []opencdc.Record{rec(2, big)})
		is.NoErr(err)
		is.Equal(n, 1)

		var got int64
		is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT n FROM %s WHERE id = 2`, table)).Scan(&got))
		is.Equal(got, big)
	})
}

// The mode is describe_exec unless the URL sets default_query_exec_mode
// itself (#436: cache_statement, the default in v0.17.0 and v0.17.1, converts
// values silently behind a statement-sharing pooler); a cache mode reports
// true so Open enables the schema guard and warns.
func TestDestinationQueryExecMode(t *testing.T) {
	for url, want := range map[string]struct {
		mode   pgx.QueryExecMode
		caches bool
	}{
		"postgres://u@h/db":                                         {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?sslmode=disable":                         {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?default_query_exec_mode=cache_statement": {pgx.QueryExecModeCacheStatement, true},
		"postgres://u@h/db?default_query_exec_mode=cache_describe":  {pgx.QueryExecModeCacheDescribe, true},
		"postgres://u@h/db?default_query_exec_mode=exec":            {pgx.QueryExecModeExec, false},
		"postgres://u@h/db?default_query_exec_mode=describe_exec":   {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?default_query_exec_mode=simple_protocol": {pgx.QueryExecModeSimpleProtocol, false},
		"host=h user=u dbname=db":                                   {pgx.QueryExecModeDescribeExec, false},
		"host=h user=u default_query_exec_mode=cache_statement":     {pgx.QueryExecModeCacheStatement, true},
	} {
		t.Run(url, func(t *testing.T) {
			is := is.New(t)
			cfg, err := pgx.ParseConfig(url)
			is.NoErr(err)
			caches := setDestinationQueryExecMode(cfg, url)
			is.Equal(cfg.DefaultQueryExecMode, want.mode)
			is.Equal(caches, want.caches)
		})
	}
}
