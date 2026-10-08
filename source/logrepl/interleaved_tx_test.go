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
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
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

// Regression tests for ConduitIO/conduit-connector-postgres#331: a
// transaction that starts before another but commits after it delivers its
// changes with LOWER LSNs than changes already delivered (pgoutput reports
// each change's own LSN; transactions arrive in commit order).

// interleave makes T1 (long) insert first and commit last, with T2 (short)
// inserting and committing in between. The stream then delivers T2's row,
// then T1's row at a lower change LSN.
func interleave(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	is := is.New(t)
	tx1, err := pool.Begin(ctx)
	is.NoErr(err)
	_, err = tx1.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('t1-long')`, table))
	is.NoErr(err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('t2-short')`, table))
	is.NoErr(err)
	is.NoErr(tx1.Commit(ctx))
}

func column1(t *testing.T, rec opencdc.Record) string {
	t.Helper()
	after, ok := rec.Payload.After.(opencdc.StructuredData)
	if !ok {
		t.Fatalf("unexpected payload %T", rec.Payload.After)
	}
	v, _ := after["column1"].(string)
	return v
}

func lsnOf(t *testing.T, rec opencdc.Record) string {
	t.Helper()
	pos, err := position.ParseSDKPosition(rec.Position)
	if err != nil {
		t.Fatalf("parse position: %v", err)
	}
	return pos.LastLSN
}

// readN reads exactly n records or fails after timeout.
func readN(ctx context.Context, t *testing.T, it interface {
	NextN(context.Context, int) ([]opencdc.Record, error)
}, n int, timeout time.Duration,
) []opencdc.Record {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var out []opencdc.Record
	for len(out) < n {
		recs, err := it.NextN(cctx, n-len(out))
		if err != nil {
			got := make([]string, 0, len(out))
			for _, r := range out {
				got = append(got, column1(t, r))
			}
			t.Fatalf("read %d of %d records (%v), then: %v", len(out), n, got, err)
		}
		out = append(out, recs...)
	}
	return out
}

// expectNoMore asserts nothing else is delivered within d.
func expectNoMore(ctx context.Context, t *testing.T, it interface {
	NextN(context.Context, int) ([]opencdc.Record, error)
}, d time.Duration,
) {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	recs, err := it.NextN(cctx, 1)
	if err == nil {
		t.Fatalf("unexpected extra delivery: %q at %s", column1(t, recs[0]), lsnOf(t, recs[0]))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func newInterleaveCombined(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, pos opencdc.Position) *CombinedIterator {
	t.Helper()
	is := is.New(t)
	it, err := NewCombinedIterator(ctx, pool, Config{
		Position:        pos,
		SlotName:        table,
		PublicationName: table,
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		WithSnapshot:    false,
		WithAvroSchema:  false,
		BatchSize:       1,
	})
	is.NoErr(err)
	return it
}

func cleanupSlot(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{
			URL: pool.Config().ConnString(), SlotName: table, PublicationName: table,
		})
	})
}

// TestInterleavedTx_RestartDeliversLaterCommittedTx is #331 item 1: the
// engine acks (and checkpoints) the first record delivered, T2's, and the
// process stops before T1's record is acked. The restart must deliver T1's
// row. Before the fix the resume guard compared the per-change LSN against
// the checkpoint and dropped it (T1's change LSN is below T2's).
//
// With the commit-LSN resume point the restart delivers T1's row and only
// that: T2's already-acked row is not repeated.
func TestInterleavedTx_RestartDeliversLaterCommittedTx(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	run1 := newInterleaveCombined(ctx, t, pool, table, nil)
	interleave(ctx, t, pool, table)
	recs := readN(ctx, t, run1, 2, 10*time.Second)
	is.Equal(column1(t, recs[0]), "t2-short")
	is.Equal(column1(t, recs[1]), "t1-long")
	t.Logf("delivered t2-short at %s, then t1-long at %s", lsnOf(t, recs[0]), lsnOf(t, recs[1]))

	// The engine persisted and acked T2's record only, then stopped.
	is.NoErr(run1.Ack(ctx, recs[0].Position))
	_ = run1.Teardown(ctx)

	run2 := newInterleaveCombined(ctx, t, pool, table, recs[0].Position)
	defer func() { _ = run2.Teardown(ctx) }()
	got := readN(ctx, t, run2, 1, 10*time.Second)
	is.Equal(column1(t, got[0]), "t1-long") // not lost
	expectNoMore(ctx, t, run2, 2*time.Second)
}

// TestInterleavedTx_AckOfHigherLSNDoesNotKillSubscription is #331 item 2:
// after T2's record (higher LSN) is acked while T1's (lower LSN) has been
// emitted, walFlushed > walWritten. Before the fix the next standby status
// update returned "walWrite (...) should be >= walFlush (...)" and the
// subscription died. Now the status update goes through reportedPositions,
// the subscription keeps running, and acking T1's record advances the slot.
func TestInterleavedTx_AckOfHigherLSNDoesNotKillSubscription(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)

	it, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		BatchSize:       1,
	})
	is.NoErr(err)
	it.sub.StatusTimeout = time.Second
	is.NoErr(it.StartSubscriber(ctx))
	t.Cleanup(func() {
		_ = it.Teardown(ctx)
		_ = Cleanup(ctx, CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	interleave(ctx, t, pool, table)
	recs := readN(ctx, t, it, 2, 10*time.Second)
	is.NoErr(it.Ack(ctx, recs[0].Position)) // T2, the higher LSN

	// Three status periods with walFlushed > walWritten.
	select {
	case <-it.sub.Done():
		t.Fatalf("subscription died with T1 in flight: %v", it.sub.Err())
	case <-time.After(3500 * time.Millisecond):
	}

	is.NoErr(it.Ack(ctx, recs[1].Position)) // T1
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('after')`, table))
	is.NoErr(err)
	got := readN(ctx, t, it, 1, 10*time.Second)
	is.Equal(column1(t, got[0]), "after")
}

// TestInterleavedTx_UpgradeFromV0142Position is the upgrade test: the golden
// testdata/v0.14.2-cdc.json was serialized by the v0.14.2 connector
// (source/position at tag v0.14.2). A pipeline upgraded from v0.14.2 resumes
// from such a position, which has no commit LSN. The fixed connector must
// not lose T1's row from it. It may repeat T2's row (the legacy resume point
// cannot tell which transaction the checkpointed change belonged to, so it
// re-delivers transactions that committed at or after it): at-least-once,
// one transaction's prefix at most, on the first restart after the upgrade.
func TestInterleavedTx_UpgradeFromV0142Position(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	run1 := newInterleaveCombined(ctx, t, pool, table, nil)
	interleave(ctx, t, pool, table)
	recs := readN(ctx, t, run1, 2, 10*time.Second)
	is.NoErr(run1.Ack(ctx, recs[0].Position))
	_ = run1.Teardown(ctx)

	// The v0.14.2 checkpoint of T2's record, byte layout from the golden file.
	golden, err := os.ReadFile("../position/testdata/v0.14.2-cdc.json")
	is.NoErr(err)
	var fields map[string]any
	is.NoErr(json.Unmarshal(golden, &fields))
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	is.Equal(keys, []string{"last_lsn", "type"}) // exactly what v0.14.2 wrote
	fields["last_lsn"] = lsnOf(t, recs[0])
	legacy, err := json.Marshal(fields)
	is.NoErr(err)

	run2 := newInterleaveCombined(ctx, t, pool, table, legacy)
	defer func() { _ = run2.Teardown(ctx) }()
	seen := map[string]int{}
	cctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	for {
		got, err := run2.NextN(cctx, 1)
		if err != nil {
			break
		}
		for _, r := range got {
			seen[column1(t, r)]++
		}
	}
	t.Logf("deliveries after resuming from the v0.14.2 position: %v", seen)
	is.Equal(seen["t1-long"], 1) // not lost
	is.True(seen["t2-short"] <= 1)
}

// drainColumn1 reads everything delivered within d and returns column1 of
// each record, in order.
func drainColumn1(ctx context.Context, t *testing.T, it interface {
	NextN(context.Context, int) ([]opencdc.Record, error)
}, d time.Duration,
) []string {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	out := []string{}
	for {
		recs, err := it.NextN(cctx, 10)
		if err != nil {
			return out
		}
		for _, r := range recs {
			out = append(out, column1(t, r))
		}
	}
}

func copyRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, values ...string) {
	t.Helper()
	rows := make([][]any, 0, len(values))
	for _, v := range values {
		rows = append(rows, []any{v})
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{table}, []string{"column1"}, pgx.CopyFromRows(rows)); err != nil {
		t.Fatalf("copy: %v", err)
	}
}

// TestCopy_RestartAfterFirstRowDeliversTheRest: COPY writes its rows with
// one heap_multi_insert WAL record, so every decoded row has the same change
// LSN. Ack only the first row and restart: the other four must be delivered,
// once. Keying resume on the change LSN loses them (all four are "<=" the
// checkpoint's LSN).
func TestCopy_RestartAfterFirstRowDeliversTheRest(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	run1 := newInterleaveCombined(ctx, t, pool, table, nil)
	copyRows(ctx, t, pool, table, "c1", "c2", "c3", "c4", "c5")
	recs := readN(ctx, t, run1, 5, 10*time.Second)
	for _, r := range recs {
		is.Equal(lsnOf(t, r), lsnOf(t, recs[0])) // the premise: one LSN for all rows
	}
	is.NoErr(run1.Ack(ctx, recs[0].Position))
	_ = run1.Teardown(ctx)

	run2 := newInterleaveCombined(ctx, t, pool, table, recs[0].Position)
	defer func() { _ = run2.Teardown(ctx) }()
	is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), []string{"c2", "c3", "c4", "c5"})
}

// TestCopy_FlushGateHoldsWhileRowsUnacked: with the first COPY row acked and
// the other four in flight, walFlushed == walWritten (same LSN) but records
// are unacked. The slot's confirmed_flush_lsn must stay below the
// transaction's commit through several status updates, or Postgres would not
// re-send it after a crash (invariant 1).
func TestCopy_FlushGateHoldsWhileRowsUnacked(t *testing.T) {
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
	it.sub.StatusTimeout = 500 * time.Millisecond
	is.NoErr(it.StartSubscriber(ctx))
	t.Cleanup(func() {
		_ = it.Teardown(ctx)
		_ = Cleanup(ctx, CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	copyRows(ctx, t, pool, table, "c1", "c2", "c3", "c4", "c5")
	recs := readN(ctx, t, it, 5, 10*time.Second)
	pos, err := position.ParseSDKPosition(recs[0].Position)
	is.NoErr(err)
	commit, err := pos.TxCommit()
	is.NoErr(err)
	is.True(commit != 0)
	is.NoErr(it.Ack(ctx, recs[0].Position)) // c2..c5 in flight

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		// unrelated WAL moves the keepalive WAL end past the commit
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1, 100)`, unrelated))
		is.NoErr(err)
		var cf string
		is.NoErr(pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, table).Scan(&cf))
		flush, err := pglogrepl.ParseLSN(cf)
		is.NoErr(err)
		if flush >= commit {
			t.Fatalf("confirmed_flush_lsn %s reached the commit %s of the COPY with 4 rows unacked: a crash now loses them", flush, commit)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestInterleavedTx_ThreeOverlappingEveryPrefix: three overlapping
// transactions, a rolled-back savepoint, TOASTed values. Checkpoint after
// every prefix k of the six delivered records, restart, and expect exactly
// the remaining suffix: no loss, no duplicate.
func TestInterleavedTx_ThreeOverlappingEveryPrefix(t *testing.T) {
	big := strings.Repeat("x", 200000) // TOASTed
	for k := 1; k <= 6; k++ {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)
			run1 := newInterleaveCombined(ctx, t, pool, table, nil)

			ins := func(tx pgx.Tx, v string) {
				payload := "{}"
				if len(v)%2 == 1 {
					payload = fmt.Sprintf(`{"b":"%s"}`, big)
				}
				_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1, column6) VALUES ($1, $2)`, table), v, payload)
				is.NoErr(err)
			}
			t1, err := pool.Begin(ctx)
			is.NoErr(err)
			t2, err := pool.Begin(ctx)
			is.NoErr(err)
			t3, err := pool.Begin(ctx)
			is.NoErr(err)
			ins(t1, "a1")
			_, err = t1.Exec(ctx, "SAVEPOINT s")
			is.NoErr(err)
			ins(t1, "a-rolledback")
			_, err = t1.Exec(ctx, "ROLLBACK TO SAVEPOINT s")
			is.NoErr(err)
			ins(t2, "b1")
			ins(t3, "c1")
			ins(t1, "a2x")
			is.NoErr(t3.Commit(ctx))
			ins(t2, "b2x")
			is.NoErr(t2.Commit(ctx))
			ins(t1, "a3")
			is.NoErr(t1.Commit(ctx))

			recs := readN(ctx, t, run1, 6, 15*time.Second)
			all := make([]string, 0, len(recs))
			for _, r := range recs {
				all = append(all, column1(t, r))
			}
			is.Equal(all, []string{"c1", "b1", "b2x", "a1", "a2x", "a3"})
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
