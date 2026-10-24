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

//go:build restore

// Package restore tests a resume on a server restored on the same timeline
// to a point before the saved position (#447 re-review S2, round-4 B1 and
// B2). Run it with test/restore/run.sh, which drives the phases and the
// volume snapshot for each scenario (RESTORE_SCENARIO).
package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func connect(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("RESTORE_URL")
	if url == "" {
		t.Fatal("INFRA: RESTORE_URL is not set; run test/restore/run.sh")
	}
	pool, err := cpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func exec(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func insert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		exec(ctx, t, pool, fmt.Sprintf("INSERT INTO %s (key) VALUES ($1)", table), k)
	}
}

func parse(t *testing.T, p opencdc.Position) position.Position {
	t.Helper()
	pp, err := position.ParseSDKPosition(p)
	if err != nil {
		t.Fatal(err)
	}
	return pp
}

func bound(t *testing.T, p opencdc.Position) pglogrepl.LSN {
	t.Helper()
	b, err := parse(t, p).CoveredThrough()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func lsnQuery(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q string) pglogrepl.LSN {
	t.Helper()
	var l pglogrepl.LSN
	if err := pool.QueryRow(ctx, q).Scan(&l); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return l
}

func config(table string, pos opencdc.Position) logrepl.Config {
	return logrepl.Config{
		Position: pos, Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: table, SlotName: table, BatchSize: 10,
	}
}

// deliver resumes from pos, writes keys, acks want records and stops. It
// returns the last position.
func deliver(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, pos opencdc.Position, keys ...string) opencdc.Position {
	t.Helper()
	is := is.New(t)
	it, err := logrepl.NewCombinedIterator(ctx, pool, config(table, pos))
	is.NoErr(err)
	insert(ctx, t, pool, table, keys...)
	var last opencdc.Position
	for n := 0; n < len(keys); {
		recs, err := it.NextN(ctx, 10)
		is.NoErr(err)
		for _, r := range recs {
			is.NoErr(it.Ack(ctx, r.Position))
			last = r.Position
			n++
		}
	}
	is.NoErr(it.Teardown(ctx))
	return last
}

// readAll reads and acks until nothing arrives for d or the iterator fails.
// Records without a payload (a drift marker) are named "<marker>".
func readAll(ctx context.Context, t *testing.T, it *logrepl.CombinedIterator, d time.Duration) ([]string, opencdc.Position, error) {
	t.Helper()
	is := is.New(t)
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var got []string
	var last opencdc.Position
	for {
		recs, err := it.NextN(rctx, 10)
		if err != nil {
			return got, last, err
		}
		for _, r := range recs {
			k := "<marker>"
			if sd, ok := r.Payload.After.(opencdc.StructuredData); ok {
				k = fmt.Sprint(sd["key"])
			}
			got = append(got, k)
			is.NoErr(it.Ack(ctx, r.Position))
			last = r.Position
		}
	}
}

// fillPast writes rows until the server's WAL is past lsn by a margin.
func fillPast(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, lsn pglogrepl.LSN) {
	t.Helper()
	for i := 0; lsnQuery(ctx, t, pool, "SELECT pg_current_wal_lsn()") < lsn+8192; i++ {
		insert(ctx, t, pool, table, fmt.Sprintf("m%d", i))
	}
}

// expectRefused resumes from p1 on the restored server and requires the
// refusal; a resume would skip the rows the restored history wrote at the
// LSNs p1 claims delivered.
func expectRefused(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, p1 opencdc.Position) {
	t.Helper()
	b := bound(t, p1)
	var confirmed pglogrepl.LSN
	if err := pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", table).Scan(&confirmed); err != nil {
		t.Fatal(err)
	}
	t.Logf("restored slot at %s: covered by P1's bound %s = %v", confirmed, b, confirmed <= b)
	it, err := logrepl.NewCombinedIterator(ctx, pool, config(table, p1))
	if err == nil {
		got, _, _ := readAll(ctx, t, it, 5*time.Second)
		_ = it.Teardown(ctx)
		t.Fatalf("SILENT SKIP: resumed on the restored server, delivered %v; the rows the restored history wrote after the restore point are skipped", got)
	}
	t.Logf("refused: %v", err)
	if logrepl.CodeOf(err) != logrepl.CodeWALHistoryChanged {
		t.Fatalf("refused with %q, want %q", logrepl.CodeOf(err), logrepl.CodeWALHistoryChanged)
	}
}

func TestRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	is := is.New(t)
	pool := connect(ctx, t)
	dir, table := os.Getenv("RESTORE_DIR"), os.Getenv("RESTORE_TABLE")
	save := func(name string, p opencdc.Position) {
		is.NoErr(os.WriteFile(filepath.Join(dir, name), p, 0o600)) //nolint:gosec // run.sh's temp directory
	}
	load := func(name string) opencdc.Position {
		b, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // run.sh's temp directory
		is.NoErr(err)
		return b
	}
	phase := os.Getenv("RESTORE_PHASE")
	if phase == "" {
		t.Skip("run test/restore/run.sh")
	}
	switch scenario := os.Getenv("RESTORE_SCENARIO"); scenario {
	case "", "basic":
		restoreBasic(ctx, t, pool, table, phase, save, load)
	case "walinspect":
		restoreWALInspect(ctx, t, pool, table, phase, save, load)
	case "drift":
		restoreDrift(ctx, t, pool, table, phase, save, load)
	default:
		t.Fatalf("unknown RESTORE_SCENARIO %q", scenario)
	}
}

type saveFunc func(string, opencdc.Position)
type loadFunc func(string) opencdc.Position

// restoreBasic: P1 is a record's position (#447 re-review S2).
func restoreBasic(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, phase string, save saveFunc, load loadFunc) {
	is := is.New(t)
	switch phase {
	case "A":
		exec(ctx, t, pool, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", table))
		save("p0", deliver(ctx, t, pool, table, nil, "r1"))
	case "B":
		p1 := deliver(ctx, t, pool, table, load("p0"), "r2", "r3")
		save("p1", p1)
		pp := parse(t, p1)
		t.Logf("P1 bound %s, xid %d, commit time %d", bound(t, p1), pp.TxXid, pp.TxCommitTime)
	case "C":
		// Restored to the snapshot taken after phase A: r2 and r3 are gone
		// from this server, and its WAL is behind P1.
		p1 := load("p1")
		var keys []string
		rows, err := pool.Query(ctx, fmt.Sprintf("SELECT key FROM %s ORDER BY id", table))
		is.NoErr(err)
		for rows.Next() {
			var k string
			is.NoErr(rows.Scan(&k))
			keys = append(keys, k)
		}
		rows.Close()
		is.Equal(keys, []string{"r1"}) // the restore took the server back to before r2 and r3
		insert(ctx, t, pool, table, "n1", "n2", "n3")
		fillPast(ctx, t, pool, table, bound(t, p1))
		expectRefused(ctx, t, pool, table, p1)
	}
}

// restoreWALInspect (#447 round-4 B1, from the review's reproduction): with
// pg_walinspect installed, the restored history gives the next transaction
// P1's xid (xids are reused from the restore point) and lets its row's heap
// record straddle P1's commit LSN, so that transaction commits right after
// it. pg_get_wal_record_info at P1's commit LSN then returns that commit
// record (the next record for an LSN inside one), and its xid matches: the
// check that trusted it resumed and lost n1 and n2. The check reads the slot
// instead and compares the commit LSN and time too.
func restoreWALInspect(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, phase string, save saveFunc, load loadFunc) {
	is := is.New(t)
	var version int
	is.NoErr(pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version))
	if version < 150000 {
		t.Skipf("pg_walinspect needs Postgres 15+, server is %d", version)
	}
	switch phase {
	case "A":
		exec(ctx, t, pool, "CREATE EXTENSION pg_walinspect")
		exec(ctx, t, pool, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", table))
		exec(ctx, t, pool, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN key SET STORAGE PLAIN", table))
		save("p0", deliver(ctx, t, pool, table, nil, "r1"))
	case "B":
		p1 := deliver(ctx, t, pool, table, load("p0"), "r2", "r3")
		save("p1", p1)
		pp := parse(t, p1)
		t.Logf("P1: commit %s xid %d time %d", pp.TxCommitLSN, pp.TxXid, pp.TxCommitTime)
	case "C":
		p1 := load("p1")
		pp := parse(t, p1)
		l3, err := pglogrepl.ParseLSN(pp.TxCommitLSN)
		is.NoErr(err)
		nextXid := func() uint64 {
			var x uint64
			is.NoErr(pool.QueryRow(ctx, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint").Scan(&x))
			return x
		}
		insertLSN := func() pglogrepl.LSN { return lsnQuery(ctx, t, pool, "SELECT pg_current_wal_insert_lsn()") }
		insert(ctx, t, pool, table, "n1", "n2")
		for nextXid() < uint64(pp.TxXid) {
			exec(ctx, t, pool, "SELECT pg_current_xact_id()")
		}
		is.Equal(nextXid(), uint64(pp.TxXid)) // the next transaction gets P1's xid
		// WAL that takes no xid, until close to P1's commit LSN.
		for insertLSN()+600 < l3 {
			exec(ctx, t, pool, "SELECT pg_logical_emit_message(false, 'app', repeat('z', 200))")
		}
		gap := int(l3 - insertLSN()) //nolint:gosec // a few hundred bytes, checked below
		if gap < 0 {
			t.Fatalf("INFRA: the restored WAL is already past P1's commit LSN")
		}
		// n3: one row whose heap record straddles P1's commit LSN, in the
		// transaction with P1's xid.
		exec(ctx, t, pool, fmt.Sprintf("UPDATE %s SET key = $1 WHERE key = 'r1'", table), "n3-"+fmt.Sprintf("%0*d", gap+200, 0))
		fillPast(ctx, t, pool, table, l3)
		var start, rm, typ, xid string
		is.NoErr(pool.QueryRow(ctx, "SELECT start_lsn::text, resource_manager, record_type, coalesce(xid::text, '') FROM pg_get_wal_record_info($1)", l3).
			Scan(&start, &rm, &typ, &xid))
		t.Logf("pg_get_wal_record_info(%s): a record starting at %s, %s %s, xid %s (P1's xid %d)", l3, start, rm, typ, xid, pp.TxXid)
		expectRefused(ctx, t, pool, table, p1)
	}
}

// restoreDrift (#447 round-4 B2, from the review's reproduction): the halted
// pipeline's last position is a schema-drift marker, whose key sits one
// below its change's commit LSN. It names the transaction that commits
// after it, and a resume from it on the restored server is refused like any
// other position.
func restoreDrift(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, phase string, save saveFunc, load loadFunc) {
	is := is.New(t)
	switch phase {
	case "A":
		exec(ctx, t, pool, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", table))
		it, err := logrepl.NewCombinedIterator(ctx, pool, config(table, nil))
		is.NoErr(err)
		insert(ctx, t, pool, table, "r1")
		got, last, _ := readAll(ctx, t, it, 3*time.Second)
		t.Logf("A delivered %v", got)
		is.NoErr(it.Teardown(ctx))
		save("p0", last)
	case "B":
		it, err := logrepl.NewCombinedIterator(ctx, pool, config(table, load("p0")))
		is.NoErr(err)
		insert(ctx, t, pool, table, "r2")
		exec(ctx, t, pool, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra int", table))
		insert(ctx, t, pool, table, "r3")
		got, last, err := readAll(ctx, t, it, 5*time.Second)
		t.Logf("B delivered %v, stopped with: %v", got, err)
		_ = it.Teardown(ctx)
		pp := parse(t, last)
		t.Logf("P1 (last acked): marker %q, commit %s seq %d xid %d", pp.SchemaDriftMarker, pp.TxCommitLSN, pp.TxSeq, pp.TxXid)
		is.True(pp.SchemaDriftMarker != "") // the halt's marker
		is.True(pp.TxXid != 0)              // names its change's transaction
		save("p1", last)
	case "C":
		p1 := load("p1")
		insert(ctx, t, pool, table, "n1", "n2")
		fillPast(ctx, t, pool, table, bound(t, p1))
		expectRefused(ctx, t, pool, table, p1)
	}
}
