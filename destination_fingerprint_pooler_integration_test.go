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
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// bouncerCacheStatementURL is cache_statement through the single-connection
// pool, so every client gets the statements the others prepared.
const bouncerCacheStatementURL = test.BouncerSingleConnString + "&default_query_exec_mode=cache_statement"

func numericRecord(table string, id int, v any) []opencdc.Record {
	return []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "v": v})}
}

// Regression test (#437 review): ABA plus a killed preparer. A client learns
// the fingerprint of a numeric column (fp0), the column becomes integer, and
// the client prepares the fp0-tagged INSERT, so the pooled server connection
// now holds a statement named for fp0 with an int4 parameter. The client dies
// without deallocating it. The column is altered back to numeric. Without the
// pg_attribute row version in the fingerprint, the live fingerprint is fp0
// again, new clients ask for the fp0-tagged statement, get the int4 one, and
// 2.5 is stored as 2.00 with no error. With it, the fingerprint after the
// second ALTER is new, so the poisoned statement is never asked for.
func TestDestination_Write_FingerprintABAKilledPreparer(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, v numeric(12,2))`, table))
	is.NoErr(err)

	// The exact tagged SQL a destination sends under fp0.
	probe := newRawDestinationURL(ctx, t, table, bouncerCacheStatementURL)
	fp0, err := probe.learnFingerprint(ctx, table)
	is.NoErr(err)
	p, err := probe.planWrites(ctx, numericRecord(table, 1, 1.0))
	is.NoErr(err)
	sql := statementText(p.writes[0].sql, fp0)

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN v TYPE integer`, table))
	is.NoErr(err)

	// The preparer: prepares the fp0-tagged text under the integer schema,
	// then dies abruptly, so nothing deallocates it on the server connection.
	cfg, err := pgx.ParseConfig(bouncerCacheStatementURL)
	is.NoErr(err)
	preparer, err := pgx.ConnectConfig(ctx, cfg)
	is.NoErr(err)
	_, err = preparer.Exec(ctx, sql, p.writes[0].args...)
	is.NoErr(err)
	is.NoErr(preparer.PgConn().Conn().Close())
	_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s`, table))
	is.NoErr(err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN v TYPE numeric(12,2)`, table))
	is.NoErr(err)

	for i := 0; i < 3; i++ {
		d := openPoolerTestDestination(t, is, bouncerCacheStatementURL)
		_, err := d.Write(ctx, numericRecord(table, 10+i, 2.5))
		is.NoErr(err)
		got, ok := storedValueText(ctx, t, conn, table, 10+i)
		is.True(ok)
		if got != "2.50" {
			t.Fatalf("client %d: stored %q, want 2.50 (it ran a statement prepared for the integer column)", i, got)
		}
	}
}

// hookConn calls onWrite before every write to the server.
type hookConn struct {
	net.Conn
	onWrite func(p []byte)
}

func (h *hookConn) Write(p []byte) (int, error) {
	h.onWrite(p)
	return h.Conn.Write(p)
}

// Regression test (#437 review): an ALTER that commits between a destination
// learning a table's fingerprint and preparing its fingerprint-tagged INSERT.
// The statement tagged with the old fingerprint is created with the new
// types. The destination's own guard sees the change and retries; a client
// that connects later must store values exactly, whether the column stays
// integer or is altered back to numeric (ABA). The ALTER is run from a dial
// hook at the moment the destination sends the INSERT's Parse, not after a
// sleep.
func TestDestination_Write_FingerprintAlterBetweenLearnAndPrepare(t *testing.T) {
	for _, aba := range []bool{false, true} {
		t.Run(fmt.Sprintf("aba=%v", aba), func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			conn := test.ConnectSimple(ctx, t, test.RegularConnString)
			table := test.RandomIdentifier(t)
			_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, v numeric(12,2))`, table))
			is.NoErr(err)

			d := newRawDestinationURL(ctx, t, table, bouncerCacheStatementURL)
			var fired atomic.Int32
			cfg := d.connConfig.Copy()
			cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &hookConn{Conn: c, onWrite: func(p []byte) {
					if strings.Contains(string(p), "INSERT INTO") && strings.Contains(string(p), "fp:") &&
						fired.Add(1) == 1 {
						_, err := conn.Exec(context.Background(),
							fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN v TYPE integer`, table))
						if err != nil {
							t.Errorf("ALTER from the hook: %v", err)
						}
					}
				}}, nil
			}
			hooked, err := pgx.ConnectConfig(ctx, cfg)
			is.NoErr(err)
			is.NoErr(d.conn.Close(ctx))
			d.conn, d.connConfig = hooked, cfg

			// The batch the ALTER interrupts: the guard fires, the retry runs
			// against the integer column and stores 7 exactly.
			n, err := d.Write(ctx, numericRecord(table, 1, big.NewRat(7, 1)))
			is.NoErr(err)
			is.Equal(n, 1)
			is.True(fired.Load() >= 1) // the ALTER ran (the retry sends a Parse too)
			is.True(d.stats.schemaChanges.Load() >= 1)

			want := "" // integer: 5/2 must be refused
			if aba {
				_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN v TYPE numeric(12,2)`, table))
				is.NoErr(err)
				want = "2.50"
			}
			for i := 0; i < 3; i++ {
				c := openPoolerTestDestination(t, is, bouncerCacheStatementURL)
				_, err := c.Write(ctx, numericRecord(table, 10+i, big.NewRat(5, 2)))
				got, stored := storedValueText(ctx, t, conn, table, 10+i)
				switch {
				case want == "" && (err == nil || stored):
					t.Fatalf("client %d: err=%v stored=%q, want 5/2 refused by the integer column", i, err, got)
				case want != "" && err != nil:
					t.Fatalf("client %d: %v", i, err)
				case want != "" && got != want:
					t.Fatalf("client %d: stored %q, want %q", i, got, want)
				}
			}
		})
	}
}

// In cache_describe mode a fired schema guard must not send DEALLOCATE ALL:
// pgx has already invalidated the failed batch's descriptions, and behind a
// pooler that shares server connections DEALLOCATE ALL would remove other
// clients' prepared statements. The test stands in for another client with a
// statement prepared on the destination's own session. A write that fails on
// the server must not deallocate either, in cache_describe or in the uncached
// default.
func TestDestination_Write_KeepsOtherPreparedStatements(t *testing.T) {
	for name, url := range map[string]string{
		"cache_describe": test.RegularConnString + "&default_query_exec_mode=cache_describe",
		"default":        test.RegularConnString,
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			table, conn := cacheTestTable(ctx, t)
			_, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT n_small CHECK (n < 100)`, table))
			is.NoErr(err)
			d := newRawDestinationURL(ctx, t, table, url)
			_, err = d.conn.PgConn().Exec(ctx, `PREPARE other_client_stmt AS SELECT 1`).ReadAll()
			is.NoErr(err)
			kept := func() bool {
				var n int
				is.NoErr(d.conn.QueryRow(ctx,
					`SELECT count(*) FROM pg_prepared_statements WHERE name = 'other_client_stmt'`).Scan(&n))
				return n == 1
			}

			_, err = d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
			is.NoErr(err)
			_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN n TYPE bigint`, table))
			is.NoErr(err)
			// A schema change (the guard fires in cache_describe), then a
			// write that fails on the server (the check constraint).
			_, err = d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 2, 6)})
			is.NoErr(err)
			_, err = d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 3, 500)})
			is.True(err != nil)

			is.True(kept())
			is.Equal(d.stats.reconnects.Load(), uint64(0))
			got, ok := storedN(ctx, t, conn, table, 2)
			is.True(ok)
			is.Equal(got, int64(6))
		})
	}
}
