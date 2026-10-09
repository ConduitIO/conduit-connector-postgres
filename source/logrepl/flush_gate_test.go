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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// Docker-gated integration tests for the DBZ-3 B2 flush gate
// (docs/design-documents/20261007-dbz3-b2-heartbeats.md). They run under
// `make test` against test/docker-compose.yml. The advance source under test
// is the keepalive WAL end: unrelated WAL moves the walsender's sent position
// past a held record, which is exactly what the gate must not report while
// the record is unacked.

// testCDCIteratorGate is testCDCIteratorPolicy with Avro schema attachment
// off (these tests are about positions, and the test table has nullable
// columns the Avro extractor rejects, #326), a batch size of 1, and a 1s
// standby status period.
func testCDCIteratorGate(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) *CDCIterator {
	t.Helper()
	is := is.New(t)

	i, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:            []string{table},
		TableKeys:         map[string]string{table: "id"},
		PublicationName:   table,
		SlotName:          table,
		BatchSize:         1,
		SchemaDriftPolicy: SchemaDriftPolicyHalt,
	})
	is.NoErr(err)
	i.sub.StatusTimeout = time.Second
	is.NoErr(i.StartSubscriber(ctx))

	t.Cleanup(func() {
		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(ctx, CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})
	return i
}

// slotFlushLSN reads the slot's confirmed_flush_lsn.
func slotFlushLSN(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	var s string
	err := pool.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&s)
	if err != nil {
		t.Fatalf("read confirmed_flush_lsn of %s: %v", slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", s, err)
	}
	return lsn
}

// slotSentLSN is how far the walsender serving slot has sent (0 if none).
func slotSentLSN(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	var s *string
	err := pool.QueryRow(ctx, `
		SELECT r.sent_lsn::text FROM pg_stat_replication r
		JOIN pg_replication_slots s ON s.active_pid = r.pid
		WHERE s.slot_name = $1`, slot).Scan(&s)
	if err != nil || s == nil {
		return 0
	}
	lsn, err := pglogrepl.ParseLSN(*s)
	if err != nil {
		t.Fatalf("parse sent_lsn %q: %v", *s, err)
	}
	return lsn
}

func currentWALLSN(ctx context.Context, t *testing.T, pool *pgxpool.Pool) pglogrepl.LSN {
	t.Helper()
	var s string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&s); err != nil {
		t.Fatalf("read pg_current_wal_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse pg_current_wal_lsn %q: %v", s, err)
	}
	return lsn
}

func recordLSN(t *testing.T, rec opencdc.Record) pglogrepl.LSN {
	t.Helper()
	pos, err := position.ParseSDKPosition(rec.Position)
	if err != nil {
		t.Fatalf("parse position: %v", err)
	}
	lsn, err := pos.LSN()
	if err != nil {
		t.Fatalf("parse position LSN: %v", err)
	}
	return lsn
}

// writeUnrelatedWAL inserts into a table outside the publication: WAL the
// slot must get past, delivered to the connector only as a keepalive WAL end.
func writeUnrelatedWAL(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1, 200)`, table)); err != nil {
		t.Fatalf("unrelated insert: %v", err)
	}
}

// waitFor polls cond every 50ms until it returns true or timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFlushGate_IdleSlotAdvance is acceptance criterion 7.4: an idle
// publication with unrelated WAL elsewhere in the database must not pin the
// slot. With nothing in flight, the gate is open and the connector confirms
// the keepalive WAL end. This passed on main before B2 too (design doc,
// Finding H1); it is the regression test for that path, which now goes
// through reportedPositions.
func TestFlushGate_IdleSlotAdvance(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)     // published, never written
	unrelated := test.SetupEmptyTestTable(ctx, t, pool) // not in the publication
	_ = testCDCIteratorGate(ctx, t, pool, table)

	start := slotFlushLSN(ctx, t, pool, table)
	writeUnrelatedWAL(ctx, t, pool, unrelated)
	target := currentWALLSN(ctx, t, pool)

	waitFor(t, 15*time.Second, fmt.Sprintf("confirmed_flush_lsn to reach %s (started at %s)", target, start), func() bool {
		writeUnrelatedWAL(ctx, t, pool, unrelated)
		return slotFlushLSN(ctx, t, pool, table) >= target
	})
}

// TestFlushGate_NeverPassesUnackedRecord is acceptance criterion 7.5,
// deterministic form. One record is emitted and deliberately not acked
// while unrelated WAL moves the walsender (and so the keepalive WAL end)
// past it. confirmed_flush_lsn must stay below the point where the record's
// transaction committed for as long as it is unacked, and advance once it is
// acked.
//
// The upper bound is pg_current_wal_lsn() read right after the insert
// committed, which is at or past the commit record. Postgres re-sends a
// transaction only if its commit is past confirmed_flush_lsn, so a flush
// position at or past that bound would mean the record is gone if the process
// crashed now. That is exactly what confirming the WAL end without the
// walFlushed == walWritten gate does: this test fails with the gate removed.
func TestFlushGate_NeverPassesUnackedRecord(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	unrelated := test.SetupEmptyTestTable(ctx, t, pool)
	i := testCDCIteratorGate(ctx, t, pool, table)

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('in-flight')`, table))
	is.NoErr(err)
	committedBy := currentWALLSN(ctx, t, pool)

	recs, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	rec := recs[0] // emitted, not acked
	is.True(recordLSN(t, rec) < committedBy)

	// Move the walsender well past the record, then keep checking through
	// several status updates (StatusTimeout is 1s).
	waitFor(t, 10*time.Second, "the walsender to send past the in-flight record", func() bool {
		writeUnrelatedWAL(ctx, t, pool, unrelated)
		return slotSentLSN(ctx, t, pool, table) > committedBy
	})
	checkUntil := time.Now().Add(4 * time.Second)
	for time.Now().Before(checkUntil) {
		writeUnrelatedWAL(ctx, t, pool, unrelated)
		if flush := slotFlushLSN(ctx, t, pool, table); flush >= committedBy {
			t.Fatalf("confirmed_flush_lsn %s reached %s while the record at %s is unacked "+
				"(walsender sent up to %s): Postgres could now discard it",
				flush, committedBy, recordLSN(t, rec), slotSentLSN(ctx, t, pool, table))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Ack it: nothing is outstanding, so the slot follows the WAL end again.
	is.NoErr(i.Ack(ctx, rec.Position))
	target := currentWALLSN(ctx, t, pool)
	waitFor(t, 10*time.Second, fmt.Sprintf("confirmed_flush_lsn to reach %s after the ack", target), func() bool {
		writeUnrelatedWAL(ctx, t, pool, unrelated)
		return slotFlushLSN(ctx, t, pool, table) >= target
	})
}

// TestFlushGate_ConcurrentDataNeverConfirmsUnacked is acceptance criterion
// 7.5 under concurrent traffic: a writer inserts published rows
// continuously, a second writer produces unrelated WAL, and the reader acks
// with a lag so records are always in flight. At every sample the slot's
// confirmed_flush_lsn must be below the commit bound of the oldest record
// that has been read but not acked.
//
// Sampling is race-free in the direction that matters: the reader goroutine
// is the only one that acks, and it computes the oldest unacked record before
// reading the slot, so the slot can only reflect acks made before the sample.
func TestFlushGate_ConcurrentDataNeverConfirmsUnacked(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	unrelated := test.SetupEmptyTestTable(ctx, t, pool)
	i := testCDCIteratorGate(ctx, t, pool, table)

	const rows = 150
	var (
		bounds   sync.Map // row id (string) -> pglogrepl.LSN at or past its commit
		writerWG sync.WaitGroup
		writeErr error
		noiseErr error
		stop     = make(chan struct{})
	)
	writerWG.Add(2)
	go func() {
		defer writerWG.Done()
		for n := 0; n < rows; n++ {
			var id int64
			err := pool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('c') RETURNING id`, table)).Scan(&id)
			if err != nil {
				writeErr = err
				return
			}
			var s string
			if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&s); err != nil {
				writeErr = err
				return
			}
			lsn, err := pglogrepl.ParseLSN(s)
			if err != nil {
				writeErr = err
				return
			}
			bounds.Store(fmt.Sprint(id), lsn)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	go func() {
		defer writerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1, 50)`, unrelated)); err != nil {
				noiseErr = err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	boundOf := func(rec opencdc.Record) pglogrepl.LSN {
		id := fmt.Sprint(rec.Key.(opencdc.StructuredData)["id"])
		// The writer stores the bound right after the commit; the record can
		// be read before that. Wait for it.
		var v any
		waitFor(t, 5*time.Second, "commit bound of row "+id, func() bool {
			var ok bool
			v, ok = bounds.Load(id)
			return ok
		})
		return v.(pglogrepl.LSN)
	}

	var (
		inFlight  []opencdc.Record
		read      int
		samples   int
		lastFlush pglogrepl.LSN
	)
	for read < rows || len(inFlight) > 0 {
		if read < rows {
			recs, err := i.NextN(ctx, 1)
			is.NoErr(err)
			inFlight = append(inFlight, recs...)
			read += len(recs)
		}
		// Ack with a lag: keep up to 5 in flight while reading, drain at the end.
		for len(inFlight) > 5 || (read >= rows && len(inFlight) > 0) {
			is.NoErr(i.Ack(ctx, inFlight[0].Position))
			inFlight = inFlight[1:]
			if read < rows {
				break
			}
		}

		if len(inFlight) > 0 {
			oldest := boundOf(inFlight[0])
			flush := slotFlushLSN(ctx, t, pool, table)
			samples++
			if flush >= oldest {
				t.Fatalf("confirmed_flush_lsn %s at or past the commit bound %s of unacked record %s (%d in flight)",
					flush, oldest, recordLSN(t, inFlight[0]), len(inFlight))
			}
			if flush < lastFlush {
				t.Fatalf("confirmed_flush_lsn went backwards: %s after %s", flush, lastFlush)
			}
			lastFlush = flush
		}
	}
	close(stop)
	writerWG.Wait()
	is.NoErr(writeErr)
	is.NoErr(noiseErr)

	// Witness that the scenario really was concurrent: many samples taken
	// with records in flight.
	is.True(samples > rows/2)
}
