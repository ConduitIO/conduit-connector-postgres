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
// to a point before the saved position (#447 re-review S2). Run it with
// test/restore/run.sh, which drives the phases and the volume snapshot.
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

func insert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (key) VALUES ($1)", table), k); err != nil {
			t.Fatal(err)
		}
	}
}

func bound(t *testing.T, p opencdc.Position) pglogrepl.LSN {
	t.Helper()
	pp, err := position.ParseSDKPosition(p)
	if err != nil {
		t.Fatal(err)
	}
	b, err := pp.CoveredThrough()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// deliver resumes from pos, writes keys, acks want records and stops. It
// returns the last position.
func deliver(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, pos opencdc.Position, keys ...string) opencdc.Position {
	t.Helper()
	is := is.New(t)
	it, err := logrepl.NewCombinedIterator(ctx, pool, logrepl.Config{
		Position: pos, Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: table, SlotName: table, BatchSize: 10,
	})
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
	switch os.Getenv("RESTORE_PHASE") {
	case "A":
		_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", table))
		is.NoErr(err)
		save("p0", deliver(ctx, t, pool, table, nil, "r1"))
	case "B":
		p1 := deliver(ctx, t, pool, table, load("p0"), "r2", "r3")
		save("p1", p1)
		pp, err := position.ParseSDKPosition(p1)
		is.NoErr(err)
		t.Logf("P1 bound %s, xid %d, commit time %d", bound(t, p1), pp.TxXid, pp.TxCommitTime)
	case "C":
		// Restored to the snapshot taken after phase A: r2 and r3 are gone
		// from this server, and its WAL is behind P1.
		p1 := load("p1")
		b := bound(t, p1)
		var keys []string
		rows, err := pool.Query(ctx, fmt.Sprintf("SELECT key FROM %s ORDER BY id", table))
		is.NoErr(err)
		for rows.Next() {
			var k string
			is.NoErr(rows.Scan(&k))
			keys = append(keys, k)
		}
		rows.Close()
		t.Logf("rows on the restored server: %v", keys)
		is.Equal(keys, []string{"r1"}) // the restore took the server back to before r2 and r3
		insert(ctx, t, pool, table, "n1", "n2", "n3")
		for i := 0; ; i++ {
			var cur pglogrepl.LSN
			is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&cur))
			if cur > b+4096 {
				t.Logf("restored server's WAL %s is past P1's bound %s", cur, b)
				break
			}
			insert(ctx, t, pool, table, fmt.Sprintf("m%d", i))
		}
		var confirmed pglogrepl.LSN
		is.NoErr(pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", table).Scan(&confirmed))
		t.Logf("restored slot at %s: covered by P1's bound %s = %v", confirmed, b, confirmed <= b)
		it, err := logrepl.NewCombinedIterator(ctx, pool, logrepl.Config{
			Position: p1, Tables: []string{table}, TableKeys: map[string]string{table: "id"},
			PublicationName: table, SlotName: table, BatchSize: 10,
		})
		if err == nil {
			_ = it.Teardown(ctx)
			rows, _ := pool.Query(ctx, "SELECT lsn::text, xid::text, encode(substr(data,1,21),'hex') FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version','1','publication_names',$2::text) WHERE get_byte(data,0)=66", table, table)
			for rows.Next() {
				var a, b, c string
				_ = rows.Scan(&a, &b, &c)
				t.Logf("BEGIN %s %s %s", a, b, c)
			}
			rows.Close()
			t.Fatal("SILENT SKIP: resumed on the restored server; n1..n3 (committed at LSNs P1 claims delivered) would be skipped")
		}
		t.Logf("refused: %v", err)
		is.Equal(logrepl.CodeOf(err), logrepl.CodeWALHistoryChanged)
	default:
		t.Skip("run test/restore/run.sh")
	}
}
