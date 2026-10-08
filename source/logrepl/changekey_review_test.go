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

package logrepl

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func r2CF(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	var cf string
	if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&cf); err != nil {
		t.Fatal(err)
	}
	l, err := pglogrepl.ParseLSN(cf)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func posOf(t *testing.T, r opencdc.Record) position.Position {
	t.Helper()
	p, err := position.ParseSDKPosition(r.Position)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func r2Raw(t *testing.T, p opencdc.Position) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// Tests contributed in the #333 review (round 2).

// Determinism of tx_seq across re-send with:
//   - a published-but-unconfigured second table (changes + TRUNCATE in tx)
//   - an unpublished table (never sent)
//   - a rolled-back savepoint
//   - mid-tx ALTER TABLE (Relation re-sent) and a Relation message that the
//     restarted session sends but run1 did not (run1 saw the table earlier)
//   - COPY inside the tx (shared LSN)
//
// Checkpoint after every prefix, restart, expect exactly the suffix.
func TestChangeKey_DeterministicAcrossResendEveryPrefix(t *testing.T) {
	for k := 1; k <= 8; k++ {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			second := test.SetupEmptyTestTable(ctx, t, pool) // published, not in Tables
			unpub := test.SetupEmptyTestTable(ctx, t, pool)  // not published
			_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %q FOR TABLE %q, %q`, table, table, second))
			is.NoErr(err)
			cleanupSlot(t, pool, table)

			run1 := newInterleaveCombined(ctx, t, pool, table, nil)
			// warm-up so run1 already has the Relation messages cached
			_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('warm')`, table))
			is.NoErr(err)
			_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('warm2')`, second))
			is.NoErr(err)
			w := readN(ctx, t, run1, 2, 10*time.Second)
			is.NoErr(run1.Ack(ctx, w[0].Position))
			is.NoErr(run1.Ack(ctx, w[1].Position))

			tx, err := pool.Begin(ctx)
			is.NoErr(err)
			ex := func(q string) {
				_, err := tx.Exec(ctx, q)
				is.NoErr(err)
			}
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('u1')`, unpub))
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p1')`, table))
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('s1')`, second))
			ex(`SAVEPOINT a`)
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('rb1')`, table))
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('rb2')`, second))
			ex(`ROLLBACK TO SAVEPOINT a`)
			ex(fmt.Sprintf(`TRUNCATE %q`, second)) // Truncate message, no record
			ex(fmt.Sprintf(`UPDATE %q SET column1='u2'`, unpub))
			ex(fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, table)) // Relation re-sent
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
			_, err = tx.CopyFrom(ctx, pgx.Identifier{table}, []string{"column1"}, pgx.CopyFromRows([][]any{{"c1"}, {"c2"}, {"c3"}}))
			is.NoErr(err)
			ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('s2')`, second))
			ex(fmt.Sprintf(`UPDATE %q SET column1='p3' WHERE column1='p1'`, table))
			is.NoErr(tx.Commit(ctx))

			recs := readN(ctx, t, run1, 8, 15*time.Second)
			all := make([]string, 0, len(recs))
			for _, r := range recs {
				all = append(all, column1(t, r))
				if k == 1 {
					t.Logf("%s %v", column1(t, r), r2Raw(t, r.Position))
				}
			}
			is.Equal(all, []string{"p1", "s1", "p2", "c1", "c2", "c3", "s2", "p3"})
			for i := 0; i < k; i++ {
				is.NoErr(run1.Ack(ctx, recs[i].Position))
			}
			_ = run1.Teardown(ctx)

			run2 := newInterleaveCombined(ctx, t, pool, table, recs[k-1].Position)
			defer func() { _ = run2.Teardown(ctx) }()
			is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), all[k:])
		})
	}
}

// Idle WAL-end reporting still advances confirmed_flush once everything
// is acked (no slot-bloat regression), and never decreases.
func TestFlushGate_IdleAndAfterCopyAdvances(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	unrelated := test.SetupEmptyTestTable(ctx, t, pool)

	it, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: table, SlotName: table, BatchSize: 1,
	})
	is.NoErr(err)
	it.sub.StatusTimeout = 300 * time.Millisecond
	is.NoErr(it.StartSubscriber(ctx))
	t.Cleanup(func() {
		_ = it.Teardown(ctx)
		_ = Cleanup(ctx, CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	// Phase A: nothing ever emitted; unrelated WAL must still move the slot.
	start := r2CF(ctx, t, pool, table)
	var cur string
	for i := 0; i < 30; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1,50)`, unrelated))
		is.NoErr(err)
		time.Sleep(150 * time.Millisecond)
	}
	is.NoErr(pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&cur))
	a := r2CF(ctx, t, pool, table)
	t.Logf("phase A: start=%s cf=%s current=%s", start, a, cur)
	is.True(a > start)

	// Phase B: COPY 3 rows, ack all, then unrelated WAL: slot passes commit.
	copyRows(ctx, t, pool, table, "c1", "c2", "c3")
	recs := readN(ctx, t, it, 3, 10*time.Second)
	for _, r := range recs {
		is.NoErr(it.Ack(ctx, r.Position))
	}
	pos := posOf(t, recs[2])
	commit, err := pos.TxCommit()
	is.NoErr(err)
	prev := r2CF(ctx, t, pool, table)
	passed := false
	for i := 0; i < 40; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1,50)`, unrelated))
		is.NoErr(err)
		time.Sleep(150 * time.Millisecond)
		c := r2CF(ctx, t, pool, table)
		if c < prev {
			t.Fatalf("confirmed_flush went backwards %s -> %s", prev, c)
		}
		prev = c
		if c > commit {
			passed = true
		}
	}
	t.Logf("phase B: commit=%s cf=%s", commit, prev)
	is.True(passed)
}

// Big COPY, all but the last row acked: flush must hold below commit;
// restart delivers exactly the last row. Then acking it lets the slot move.
func TestCopy_BigAllButLastRowAcked(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	unrelated := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	it, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: table, SlotName: table, BatchSize: 100,
	})
	is.NoErr(err)
	it.sub.StatusTimeout = 300 * time.Millisecond
	is.NoErr(it.StartSubscriber(ctx))

	n := 2000
	vals := make([]string, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("r%05d", i)
	}
	copyRows(ctx, t, pool, table, vals...)
	recs := readN(ctx, t, it, n, 30*time.Second)
	commit, err := posOf(t, recs[0]).TxCommit()
	is.NoErr(err)
	for i := 0; i < n-1; i++ {
		is.NoErr(it.Ack(ctx, recs[i].Position))
	}
	for i := 0; i < 20; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1,50)`, unrelated))
		is.NoErr(err)
		time.Sleep(150 * time.Millisecond)
		if c := r2CF(ctx, t, pool, table); c >= commit {
			t.Fatalf("cf %s >= commit %s with 1 row unacked", c, commit)
		}
	}
	_ = it.Teardown(ctx)

	run2 := newInterleaveCombined(ctx, t, pool, table, recs[n-2].Position)
	defer func() { _ = run2.Teardown(ctx) }()
	is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), []string{vals[n-1]})
}

// Legacy fallback paths with COPY: a v0.14.2-shaped position (only
// last_lsn) and a 1a4bb8b-shaped position (tx_commit_lsn, no tx_seq) of
// row 1 must not lose rows 2..5 (duplicates allowed).
func TestCopy_LegacyPositionsNoLoss(t *testing.T) {
	for _, mode := range []string{"v0142", "commitOnly", "seqOnly"} {
		t.Run(mode, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)
			run1 := newInterleaveCombined(ctx, t, pool, table, nil)
			copyRows(ctx, t, pool, table, "c1", "c2", "c3", "c4", "c5")
			recs := readN(ctx, t, run1, 5, 10*time.Second)
			is.NoErr(run1.Ack(ctx, recs[0].Position))
			_ = run1.Teardown(ctx)

			m := r2Raw(t, recs[0].Position)
			switch mode {
			case "v0142":
				delete(m, "version")
				delete(m, "tx_commit_lsn")
				delete(m, "tx_seq")
			case "commitOnly":
				delete(m, "tx_seq")
			case "seqOnly":
				delete(m, "tx_commit_lsn")
			}
			legacy, err := json.Marshal(m)
			is.NoErr(err)
			run2 := newInterleaveCombined(ctx, t, pool, table, legacy)
			defer func() { _ = run2.Teardown(ctx) }()
			got := drainColumn1(ctx, t, run2, 4*time.Second)
			t.Logf("%s: %v", mode, got)
			seen := map[string]bool{}
			for _, g := range got {
				seen[g] = true
			}
			for _, v := range []string{"c2", "c3", "c4", "c5"} {
				is.True(seen[v])
			}
		})
	}
}
