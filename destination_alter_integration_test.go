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
	"os"
	"path/filepath"
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
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

	table := strings.ToLower(test.RandomIdentifier(t))
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, amt numeric(12,2))`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table)) })

	d := openNumericDestination(ctx, t, "")
	is.NoErr(writeAmount(ctx, d, table, 1, big.NewRat(1, 1)))

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ALTER COLUMN amt TYPE integer`, table))
	is.NoErr(err)

	// Same destination instance, same connection: no restart.
	err = writeAmount(ctx, d, table, 2, big.NewRat(5, 2))
	var stored []string
	rows, qerr := conn.Query(ctx, fmt.Sprintf(`SELECT amt::text FROM %q WHERE id = 2`, table))
	is.NoErr(qerr)
	for rows.Next() {
		var v string
		is.NoErr(rows.Scan(&v))
		stored = append(stored, v)
	}
	is.NoErr(rows.Err())
	t.Logf("write of 5/2 after ALTER: err=%v, stored=%v", err, stored)

	is.True(err != nil)      // 2.5 is not an integer: refused, never rounded to 3
	is.Equal(len(stored), 0) // and nothing was stored

	// A whole value still goes through on the same connection.
	is.NoErr(writeAmount(ctx, d, table, 3, big.NewRat(7, 1)))
	is.Equal(amountText(ctx, t, conn, fmt.Sprintf("%q", table), 3), "7")
}

// The mode is describe_exec unless the connection settings set
// default_query_exec_mode; an explicit mode is kept, and a cache mode is
// reported so Open can warn.
func TestDestinationQueryExecMode(t *testing.T) {
	for url, want := range map[string]struct {
		mode   pgx.QueryExecMode
		caches bool
	}{
		"postgres://u@h/db":                                         {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?sslmode=disable":                         {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?default_query_exec_mode=cache_statement": {pgx.QueryExecModeCacheStatement, true},
		"postgres://u@h/db?default_query_exec_mode=cache_describe":  {pgx.QueryExecModeCacheDescribe, true},
		"postgres://u@h/db?default_query_exec_mode=describe_exec":   {pgx.QueryExecModeDescribeExec, false},
		"postgres://u@h/db?default_query_exec_mode=exec":            {pgx.QueryExecModeExec, false},
		"postgres://u@h/db?default_query_exec_mode=simple_protocol": {pgx.QueryExecModeSimpleProtocol, false},
		"host=h user=u dbname=db":                                   {pgx.QueryExecModeDescribeExec, false},
		"host=h user=u default_query_exec_mode=cache_statement":     {pgx.QueryExecModeCacheStatement, true},
		"host=h user=u default_query_exec_mode=exec":                {pgx.QueryExecModeExec, false},
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

// A mode set in a service file is explicit too: the URL does not mention it,
// but pgx reads it, so the destination must keep it rather than override it.
func TestDestinationQueryExecMode_ServiceFile(t *testing.T) {
	is := is.New(t)
	file := filepath.Join(t.TempDir(), "pg_service.conf")
	is.NoErr(os.WriteFile(file, []byte("[svc]\nhost=h\nuser=u\ndefault_query_exec_mode=simple_protocol\n"), 0o600))
	t.Setenv("PGSERVICEFILE", file)

	url := "postgres://u@h/db?service=svc"
	cfg, err := pgx.ParseConfig(url)
	is.NoErr(err)
	is.Equal(setDestinationQueryExecMode(cfg, url), false)
	is.Equal(cfg.DefaultQueryExecMode, pgx.QueryExecModeSimpleProtocol)
}

// Open applies the mode to the connection it opens: describe_exec when the URL
// is silent, the URL's own mode when it sets one.
func TestDestination_Open_QueryExecMode(t *testing.T) {
	for _, tc := range []struct {
		suffix string
		want   pgx.QueryExecMode
	}{
		{"", pgx.QueryExecModeDescribeExec},
		{"&default_query_exec_mode=cache_statement", pgx.QueryExecModeCacheStatement},
		{"&default_query_exec_mode=exec", pgx.QueryExecModeExec},
	} {
		t.Run(tc.want.String(), func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			d := &Destination{stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
			d.config.URL = test.RegularConnString + tc.suffix
			d.config.Table = "unused"
			is.NoErr(d.Open(ctx))
			t.Cleanup(func() { is.NoErr(d.Teardown(ctx)) })
			is.Equal(d.conn.Config().DefaultQueryExecMode, tc.want)
		})
	}
}
