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

// Regression test for #367. pgx's default mode caches prepared statements with
// the parameter types Postgres inferred when they were first prepared. After
// ALTER COLUMN ... TYPE on a running pipeline the destination kept sending
// parameters typed for the old column, and Postgres converted them by
// assignment: 5/2 into a column that had become integer was stored as 3, with
// no error (invariant 6). A fresh destination rejects it, and so must a running
// one.
func TestDestination_Write_ColumnTypeChangedWhileRunning(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, amt numeric(12,2))`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": 1}, opencdc.StructuredData{"id": 1, "amt": big.NewRat(1, 1)})})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN amt TYPE integer`, table))
	is.NoErr(err)

	n, err = d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": 2}, opencdc.StructuredData{"id": 2, "amt": big.NewRat(5, 2)})})
	is.True(err != nil) // 2.5 is not an integer: refused, never rounded to 3
	is.Equal(n, 0)

	var count int
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2`, table)).Scan(&count))
	is.Equal(count, 0)
}

// The mode is describe_exec unless the URL sets default_query_exec_mode; an
// explicit cache mode is kept and reported, so Open can warn.
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
