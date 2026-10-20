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
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// poolerTestModes are the destination URLs the pooler test checks. Only modes
// that must be correct behind a statement-sharing pooler belong here; the
// cached modes are not (see TestDestination_Write_PoolerSharedStaleStatement).
var poolerTestModes = map[string]string{
	"default":       test.BouncerConnString,
	"describe_exec": test.BouncerConnString + "&default_query_exec_mode=describe_exec",
}

// Regression test for #436, a silent conversion shipped in v0.17.0 and
// v0.17.1. Behind PgBouncer with prepared-statement tracking, a statement one
// client prepared on a server connection is handed to every client that sends
// the same SQL text. Clients that ran before an ALTER COLUMN ... TYPE leave the
// INSERT prepared with the old parameter types. A destination that connects
// after the ALTER, in a cached mode, learns a fresh schema fingerprint (so the
// guard passes) but runs the old statement: 5/2 into a column that became
// integer was stored as 3, and 5.5 into a column that became float8 as 5.
//
// The default (describe_exec) prepares nothing named, so it never gets
// another client's statement. The test fails with
// default_query_exec_mode=cache_statement in poolerTestModes.
func TestDestination_Write_PoolerSharedStaleStatement(t *testing.T) {
	cases := []struct {
		name       string
		oldT, newT string
		v1, v2     any
		want       string // the stored text; "" means the write must be refused
	}{
		{"numeric to integer, 5/2", "numeric(12,2)", "integer", big.NewRat(1, 1), big.NewRat(5, 2), ""},
		{"bigint to float8, 5.5", "bigint", "float8", int64(1), 5.5, "5.5"},
	}
	for mode, url := range poolerTestModes {
		for _, c := range cases {
			t.Run(mode+"/"+c.name, func(t *testing.T) {
				is := is.New(t)
				ctx := test.Context(t)
				conn := test.ConnectSimple(ctx, t, test.RegularConnString)
				table := test.RandomIdentifier(t)
				_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, v %s)`, table, c.oldT))
				is.NoErr(err)
				rec := func(id int, v any) []opencdc.Record {
					return []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
						opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "v": v})}
				}

				// Clients from before the ALTER (v0.17.x pipelines, which
				// default to cache_statement) prepare the INSERT on the
				// pool's server connections. Several clients, several
				// writes each, so both server connections get it.
				id := 1
				for a := 0; a < 4; a++ {
					old := openPoolerTestDestination(t, is,
						test.BouncerConnString+"&default_query_exec_mode=cache_statement")
					for k := 0; k < 3; k++ {
						_, err := old.Write(ctx, rec(id, c.v1))
						is.NoErr(err)
						id++
					}
				}

				_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN v TYPE %s`, table, c.newT))
				is.NoErr(err)

				// New destinations connect after the ALTER.
				for i := 0; i < 10; i++ {
					d := openPoolerTestDestination(t, is, url)
					rid := 1000 + i
					_, err := d.Write(ctx, rec(rid, c.v2))
					got, stored := storedValueText(ctx, t, conn, table, rid)
					if c.want == "" {
						if err == nil || stored {
							t.Fatalf("write %d: err=%v stored=%q, want the value refused and nothing stored "+
								"(a stale shared statement converts it)", i, err, got)
						}
						continue
					}
					is.NoErr(err)
					if got != c.want {
						t.Fatalf("write %d: stored %q, want %q (a stale shared statement converted it)", i, got, c.want)
					}
				}
			})
		}
	}
}

// The default query exec mode is describe_exec (#436), and only an explicit
// cached mode turns the schema guard on.
func TestDestination_Open_DefaultIsDescribeExec(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	d := newRawDestinationURL(ctx, t, "unused", test.RegularConnString)
	is.Equal(d.conn.Config().DefaultQueryExecMode, pgx.QueryExecModeDescribeExec)
	is.True(!d.guardSchema)

	d = newRawDestinationURL(ctx, t, "unused", cacheStatementURL)
	is.Equal(d.conn.Config().DefaultQueryExecMode, pgx.QueryExecModeCacheStatement)
	is.True(d.guardSchema)
}

func openPoolerTestDestination(t *testing.T, is *is.I, url string) sdk.Destination {
	t.Helper()
	ctx := test.Context(t)
	d := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"url": url}, d.Config(),
		Connector.NewSpecification().DestinationParams))
	is.NoErr(d.Open(ctx))
	t.Cleanup(func() { _ = d.Teardown(context.Background()) })
	return d
}

// storedValueText returns column v of row id as text, and whether the row
// exists.
func storedValueText(ctx context.Context, t *testing.T, conn *pgx.Conn, table string, id int) (string, bool) {
	t.Helper()
	var v *string
	err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT v::text FROM %s WHERE id = $1`, table), id).Scan(&v)
	if err == pgx.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		return "<null>", true
	}
	return *v, true
}
