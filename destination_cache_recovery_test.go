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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// cacheStatementURL selects the cached mode explicitly: the default is
// describe_exec, which caches nothing, so the cache-recovery paths need it.
var cacheStatementURL = test.RegularConnString + "&default_query_exec_mode=cache_statement"

// bothModes is the default (describe_exec) and cache_statement, for behavior
// that must hold in either.
var bothModes = map[string]string{"default": test.RegularConnString, "cache_statement": cacheStatementURL}

// cacheTestTable creates a table (id int PRIMARY KEY, n integer) and returns
// its name and an admin connection.
func cacheTestTable(ctx context.Context, t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, n integer)`, table)); err != nil {
		t.Fatal(err)
	}
	return table, conn
}

func cacheTestRecord(table string, id int, n int64) opencdc.Record {
	return jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": id}, opencdc.StructuredData{"id": id, "n": n})
}

func storedN(ctx context.Context, t *testing.T, conn *pgx.Conn, table string, id int) (int64, bool) {
	t.Helper()
	var n int64
	err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT n FROM %s WHERE id = $1`, table), id).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return n, true
}

// Regression test for SHOULD-1 of the #433 review. After ALTER widened a
// column, the second record of a batch overflows the still-cached int4
// parameter. pgx fails the batch while encoding it, after the first record was
// already sent, and closes the connection (pgconn's Pipeline.Close on unsynced
// requests). The destination used to skip clearing the cache on a closed
// connection and never replaced it, so the same batch failed on every retry and
// every later Write failed too. Now it reconnects with an empty statement cache
// and retries, and both records are stored exactly.
func TestDestination_Write_EncodeFailureMidBatchReconnects(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	table, conn := cacheTestTable(ctx, t)
	d := newRawDestinationURL(ctx, t, table, cacheStatementURL)

	n, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN n TYPE bigint`, table))
	is.NoErr(err)

	const big = int64(3_000_000_000)
	n, err = d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 2, 6), cacheTestRecord(table, 3, big)})
	is.NoErr(err)
	is.Equal(n, 2)
	is.True(!d.conn.IsClosed())
	is.Equal(d.stats.reconnects.Load(), uint64(1))

	got, ok := storedN(ctx, t, conn, table, 2)
	is.True(ok)
	is.Equal(got, int64(6))
	got, ok = storedN(ctx, t, conn, table, 3)
	is.True(ok)
	is.Equal(got, big)
}

// A value that does not fit the column at all (no ALTER involved) fails the
// batch on every attempt. The error names the record that holds it (index 1),
// not the first record of the batch, nothing is stored, and the destination
// stays usable: before, the connection pgx closed on this path was never
// replaced, so every later Write failed.
func TestDestination_Write_EncodeFailureNamesRecordAndRecovers(t *testing.T) {
	for name, url := range bothModes {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			table, conn := cacheTestTable(ctx, t)
			d := newRawDestinationURL(ctx, t, table, url)

			n, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5), cacheTestRecord(table, 2, 3_000_000_000)})
			is.True(err != nil)
			is.Equal(n, 0)
			is.True(strings.Contains(err.Error(), "record 1 ")) // the record that does not fit
			is.True(!strings.Contains(err.Error(), "record 0")) // not the first one
			_, ok := storedN(ctx, t, conn, table, 1)
			is.True(!ok) // invariant 1: the batch is all or nothing

			n, err = d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 3, 7)})
			is.NoErr(err)
			is.Equal(n, 1)
			got, ok := storedN(ctx, t, conn, table, 3)
			is.True(ok)
			is.Equal(got, int64(7))
		})
	}
}

// SHOULD-2 of the #433 review: behind PgBouncer in transaction mode with
// max_prepared_statements=0, a batch can reach a server that does not have the
// cached statement (26000). The test reproduces that without a pooler by
// deallocating the session's statements behind pgx's back. The batch is rolled
// back by the error, so the destination clears its cache and retries it.
func TestDestination_Write_MissingPreparedStatementIsRetried(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	table, conn := cacheTestTable(ctx, t)
	d := newRawDestinationURL(ctx, t, table, cacheStatementURL)

	_, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
	is.NoErr(err)

	_, err = d.conn.PgConn().Exec(ctx, "DEALLOCATE ALL").ReadAll()
	is.NoErr(err)

	n, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 2, 6)})
	is.NoErr(err)
	is.Equal(n, 1)
	is.Equal(d.stats.staleStatements.Load(), uint64(1))
	got, ok := storedN(ctx, t, conn, table, 2)
	is.True(ok)
	is.Equal(got, int64(6))
}

// The other half of SHOULD-2: the server already has a prepared statement with
// the name pgx picks for a statement it has not cached (42P05). A pooler
// produces this when another client prepared the same SQL on that server; the
// test prepares pgx's name for the schema-fingerprint query on the session
// itself. Nothing was written, so the destination clears its cache and
// retries.
func TestDestination_Write_DuplicatePreparedStatementIsRetried(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	table, conn := cacheTestTable(ctx, t)
	d := newRawDestinationURL(ctx, t, table, cacheStatementURL)

	// pgx's stmtcache.StatementName: the name is stable for the SQL text.
	digest := sha256.Sum256([]byte(schemaFingerprintSQL))
	name := "stmtcache_" + hex.EncodeToString(digest[0:24])
	_, err := d.conn.PgConn().Exec(ctx, fmt.Sprintf(`PREPARE %s AS SELECT 1`, name)).ReadAll()
	is.NoErr(err)

	n, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
	is.NoErr(err)
	is.Equal(n, 1)
	// Fails if pgx changed its naming, so the test cannot pass vacuously.
	is.Equal(d.stats.staleStatements.Load(), uint64(1))
	got, ok := storedN(ctx, t, conn, table, 1)
	is.True(ok)
	is.Equal(got, int64(5))
}

// SHOULD-4 and a NIT of the #433 review: a fired schema guard is logged at Info
// with the table name and the running count, and the warning about records
// that carry drift metadata is logged once per Write, not once per attempt.
func TestDestination_Write_GuardFireIsLoggedOnce(t *testing.T) {
	is := is.New(t)
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).Level(zerolog.InfoLevel).WithContext(test.Context(t))
	table, conn := cacheTestTable(ctx, t)
	d := newRawDestinationURL(ctx, t, table, cacheStatementURL)

	_, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra text`, table))
	is.NoErr(err)

	// A normal record that carries the drift metadata: written, and warned
	// about.
	rec := cacheTestRecord(table, 2, 6)
	rec.Metadata[logrepl.MetadataSchemaDrift] = "true"
	logs.Reset()
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)
	is.Equal(n, 1)

	var guardLines, driftWarnings int
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		is.NoErr(json.Unmarshal([]byte(line), &entry))
		msg, _ := entry["message"].(string)
		switch {
		case strings.HasPrefix(msg, "destination table schema changed"):
			guardLines++
			is.Equal(entry["level"], "info")
			is.Equal(entry["table"], table)
			is.Equal(entry["schema_changes_total"], float64(1))
		case strings.HasPrefix(msg, "records carry schema-drift metadata"):
			driftWarnings++
		}
	}
	is.Equal(guardLines, 1)
	is.Equal(driftWarnings, 1)
	is.Equal(d.stats.schemaChanges.Load(), uint64(1))
}

// NIT of the #433 review: a write to a table that does not exist fails with a
// stable error code, in the default mode (where Postgres reports 42P01) and in
// a cached mode (where the fingerprint read finds no table).
func TestDestination_Write_MissingTableHasErrorCode(t *testing.T) {
	for name, url := range bothModes {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			table := test.RandomIdentifier(t) // never created
			d := newRawDestinationURL(ctx, t, table, url)

			n, err := d.Write(ctx, []opencdc.Record{cacheTestRecord(table, 1, 5)})
			is.True(err != nil)
			is.Equal(n, 0)
			is.True(strings.HasPrefix(err.Error(), ErrorCodeDestinationTableNotFound+": "))
		})
	}
}

// SHOULD-2: a default_query_exec_mode set in a service file (selected with
// service= or PGSERVICE) is explicit and must be kept. Before, only the URL
// and keyword/value text were searched, so cache_statement replaced it, and a
// user who set describe_exec for a pooler in the service file got
// cache_statement.
func TestDestinationQueryExecMode_ServiceFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "pg_service.conf")
	if err := os.WriteFile(file, []byte(
		"[pooled]\nhost=h\nuser=u\ndbname=db\ndefault_query_exec_mode=describe_exec\n\n"+
			"[plain]\nhost=h\nuser=u\ndbname=db\n\n"+
			"[cached]\nhost=h\nuser=u\ndbname=db\ndefault_query_exec_mode=cache_statement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PGSERVICEFILE", file)

	for _, tc := range []struct {
		name, url, env string
		mode           pgx.QueryExecMode
		caches         bool
	}{
		{"url service", "postgres://u@h/db?service=pooled", "", pgx.QueryExecModeDescribeExec, false},
		{"keyword service", "service=pooled", "", pgx.QueryExecModeDescribeExec, false},
		{"PGSERVICE", "postgres://u@h/db", "pooled", pgx.QueryExecModeDescribeExec, false},
		{"service without mode", "service=plain", "", pgx.QueryExecModeDescribeExec, false},
		{"service sets a cached mode", "postgres://u@h/db?service=cached", "", pgx.QueryExecModeCacheStatement, true},
		{
			"url overrides service", "postgres://u@h/db?service=pooled&default_query_exec_mode=exec", "",
			pgx.QueryExecModeExec, false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			t.Setenv("PGSERVICE", tc.env)
			cfg, err := pgx.ParseConfig(tc.url)
			is.NoErr(err)
			caches := setDestinationQueryExecMode(cfg, tc.url)
			is.Equal(cfg.DefaultQueryExecMode, tc.mode)
			is.Equal(caches, tc.caches)
		})
	}
}

func TestIsStaleStatement(t *testing.T) {
	is := is.New(t)
	is.True(isStaleStatement(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "26000"})))
	is.True(isStaleStatement(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "42P05"})))
	is.True(!isStaleStatement(&pgconn.PgError{Code: "22012"}))
	is.True(!isStaleStatement(fmt.Errorf("not a server error")))
}

// NIT of the #436 review: the per-attempt recovery lines are rate-limited per
// kind, and the next line carries the number suppressed in between.
func TestRecoveryLogIsRateLimited(t *testing.T) {
	is := is.New(t)
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).Level(zerolog.InfoLevel).WithContext(context.Background())
	d := &Destination{}

	d.recoveryLog(ctx, "reconnect").Msg("first")
	d.recoveryLog(ctx, "reconnect").Msg("suppressed")
	d.recoveryLog(ctx, "reconnect").Msg("suppressed")
	d.recoveryLog(ctx, "stale_statement").Msg("other kind")
	// The interval passes.
	d.recoveryLogs.last["reconnect"] = time.Now().Add(-2 * recoveryLogInterval)
	d.recoveryLog(ctx, "reconnect").Msg("after")

	var got []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		is.NoErr(json.Unmarshal([]byte(line), &entry))
		got = append(got, fmt.Sprintf("%s/%v", entry["message"], entry["suppressed_since_last"]))
	}
	is.Equal(got, []string{"first/0", "other kind/0", "after/2"})
}
