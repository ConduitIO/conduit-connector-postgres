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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// Docker-gated integration tests for the DBZ-3 B2 heartbeat
// (docs/design-documents/20261007-dbz3-b2-heartbeats.md). They run under
// `make test` against test/docker-compose.yml.

const testHeartbeatInterval = 200 * time.Millisecond

// testCDCIteratorHeartbeat is testCDCIteratorPolicy with the heartbeat
// configured. hb.Enabled=false gives a plain iterator with the same setup.
// Avro schema attachment is off: these tests are about positions, and the
// test table has nullable columns the Avro extractor rejects (#326).
func testCDCIteratorHeartbeat(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, hb HeartbeatConfig) *CDCIterator {
	t.Helper()
	is := is.New(t)

	if hb.Enabled {
		t.Cleanup(func() {
			_, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+hb.qualifiedName())
			is.NoErr(err)
		})
	}

	i, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:            []string{table},
		TableKeys:         map[string]string{table: "id"},
		PublicationName:   table,
		SlotName:          table,
		BatchSize:         1,
		SchemaDriftPolicy: SchemaDriftPolicyHalt,
		Heartbeat:         hb,
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

// testHeartbeatConfig returns an enabled config with a short, unique table
// name. Deriving it from the (long) test table name would exceed Postgres's
// 63-byte identifier limit, which HeartbeatConfig.Validate rejects.
func testHeartbeatConfig() HeartbeatConfig {
	return HeartbeatConfig{
		Enabled:  true,
		Interval: testHeartbeatInterval,
		Schema:   DefaultHeartbeatSchema,
		Table:    fmt.Sprintf("hb_%d", time.Now().UnixNano()),
	}
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

func observedHeartbeatLSN(t *testing.T, i *CDCIterator) pglogrepl.LSN {
	t.Helper()
	lsn, err := pglogrepl.ParseLSN(i.HeartbeatStatus().LastObservedLSN)
	if err != nil {
		t.Fatalf("parse observed heartbeat LSN: %v", err)
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

// TestHeartbeat_IdleSlotAdvance is acceptance criterion 7.4: an
// idle publication with unrelated WAL elsewhere in the database must not pin
// the slot.
//
// "hb" proves the heartbeat path: confirmed_flush_lsn reaches the
// LSN of a heartbeat the connector observed. "echo" is the same
// scenario with heartbeats off. It passes on main too (design doc, Finding
// H1): the connector already confirms the keepalive WAL end when nothing is
// in flight. It stays as the regression test for that path, which now goes
// through the same gate.
func TestHeartbeat_IdleSlotAdvance(t *testing.T) {
	for _, withHeartbeat := range []bool{true, false} {
		name := "echo"
		if withHeartbeat {
			name = "hb"
		}
		t.Run(name, func(t *testing.T) {
			ctx := test.Context(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)     // published, never written
			unrelated := test.SetupEmptyTestTable(ctx, t, pool) // not in the publication

			hb := HeartbeatConfig{}
			if withHeartbeat {
				hb = testHeartbeatConfig()
			}
			i := testCDCIteratorHeartbeat(ctx, t, pool, table, hb)

			start := slotFlushLSN(ctx, t, pool, table)
			writeUnrelated := func() {
				_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1, 200)`, unrelated))
				if err != nil {
					t.Fatalf("unrelated insert: %v", err)
				}
			}
			writeUnrelated()
			target := currentWALLSN(ctx, t, pool) // WAL the slot must get past

			if withHeartbeat {
				waitFor(t, 10*time.Second, "a heartbeat past the unrelated WAL", func() bool {
					writeUnrelated()
					return observedHeartbeatLSN(t, i) > target
				})
				target = observedHeartbeatLSN(t, i)
			}

			waitFor(t, 15*time.Second, fmt.Sprintf("confirmed_flush_lsn to reach %s (started at %s)", target, start), func() bool {
				writeUnrelated()
				return slotFlushLSN(ctx, t, pool, table) >= target
			})
		})
	}
}

// TestHeartbeat_FlushNeverPassesUnackedRecord is acceptance criterion 7.5,
// deterministic form. One record is emitted and deliberately not acked while
// heartbeats keep arriving past it. confirmed_flush_lsn must stay below the
// point where the record's transaction committed for as long as it is
// unacked, and advance past the heartbeats once it is acked.
//
// The upper bound is pg_current_wal_lsn() read right after the insert
// committed, which is at or past the commit record. Postgres re-sends a
// transaction only if its commit is past confirmed_flush_lsn, so a flush
// position at or past that bound would mean the record is gone if the process
// crashed now. That is exactly what reporting a heartbeat LSN without the
// walFlushed == walWritten gate does: this test fails with the gate removed.
func TestHeartbeat_FlushNeverPassesUnackedRecord(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	i := testCDCIteratorHeartbeat(ctx, t, pool, table, testHeartbeatConfig())

	// Heartbeats flow and the gate is open: the slot follows them.
	waitFor(t, 10*time.Second, "first heartbeat", func() bool { return observedHeartbeatLSN(t, i) > 0 })

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('in-flight')`, table))
	is.NoErr(err)
	committedBy := currentWALLSN(ctx, t, pool)

	recs, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	rec := recs[0] // emitted, not acked
	is.True(recordLSN(t, rec) < committedBy)

	// Wait until heartbeats past the record have been observed, then keep
	// checking through several status updates (StatusTimeout is 1s).
	waitFor(t, 10*time.Second, "a heartbeat past the in-flight record", func() bool {
		return observedHeartbeatLSN(t, i) > committedBy
	})
	checkUntil := time.Now().Add(4 * time.Second)
	for time.Now().Before(checkUntil) {
		if flush := slotFlushLSN(ctx, t, pool, table); flush >= committedBy {
			t.Fatalf("confirmed_flush_lsn %s reached %s while the record at %s is unacked "+
				"(heartbeat observed at %s): Postgres could now discard it",
				flush, committedBy, recordLSN(t, rec), observedHeartbeatLSN(t, i))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Ack it: nothing is outstanding, so the slot follows the heartbeats again.
	is.NoErr(i.Ack(ctx, rec.Position))
	target := observedHeartbeatLSN(t, i)
	waitFor(t, 10*time.Second, fmt.Sprintf("confirmed_flush_lsn to reach heartbeat %s after the ack", target), func() bool {
		return slotFlushLSN(ctx, t, pool, table) >= target
	})
}

// TestHeartbeat_ConcurrentDataNeverConfirmsUnacked is acceptance criterion
// 7.5 under concurrent heartbeat and data traffic: a writer inserts rows
// continuously, the reader acks with a lag so records are always in flight,
// and heartbeats arrive every 200ms throughout. At every sample the slot's
// confirmed_flush_lsn must be below the commit bound of the oldest record
// that has been read but not acked.
//
// Sampling is race-free in the direction that matters: the reader goroutine
// is the only one that acks, and it computes the oldest unacked record before
// reading the slot, so the slot can only reflect acks made before the sample.
func TestHeartbeat_ConcurrentDataNeverConfirmsUnacked(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	i := testCDCIteratorHeartbeat(ctx, t, pool, table, testHeartbeatConfig())
	waitFor(t, 10*time.Second, "first heartbeat", func() bool { return observedHeartbeatLSN(t, i) > 0 })
	firstHeartbeat := observedHeartbeatLSN(t, i)

	const rows = 150
	var (
		bounds   sync.Map // row id (string) -> pglogrepl.LSN at or past its commit
		writerWG sync.WaitGroup
		writeErr error
	)
	writerWG.Add(1)
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
				t.Fatalf("confirmed_flush_lsn %s at or past the commit bound %s of unacked record %s "+
					"(%d in flight, heartbeat observed at %s)",
					flush, oldest, recordLSN(t, inFlight[0]), len(inFlight), observedHeartbeatLSN(t, i))
			}
			if flush < lastFlush {
				t.Fatalf("confirmed_flush_lsn went backwards: %s after %s", flush, lastFlush)
			}
			lastFlush = flush
		}
	}
	writerWG.Wait()
	is.NoErr(writeErr)

	// Witnesses that the scenario really was concurrent: many samples with
	// records in flight, and heartbeats observed during the run.
	is.True(samples > rows/2)
	is.True(observedHeartbeatLSN(t, i) > firstHeartbeat)
}

// TestHeartbeat_SetupAddsTableToExistingPublication is the upgrade path from
// a v0.14.x pipeline (design doc, Decision 8): the publication already exists
// without the heartbeat table. Enabling heartbeats adds the table; opening
// again is a no-op.
func TestHeartbeat_SetupAddsTableToExistingPublication(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	test.CreatePublication(t, pool, table, []string{table}) // as a v0.14 connector left it

	hb := testHeartbeatConfig()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+hb.qualifiedName())
	})

	members := func() []string {
		rows, err := pool.Query(ctx, "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename", table)
		is.NoErr(err)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var name string
			is.NoErr(rows.Scan(&name))
			out = append(out, name)
		}
		is.NoErr(rows.Err())
		return out
	}

	is.NoErr(setupHeartbeat(ctx, pool, hb, table))
	is.Equal(members(), []string{table, hb.Table})
	is.NoErr(setupHeartbeat(ctx, pool, hb, table)) // idempotent
	is.Equal(members(), []string{table, hb.Table})
}

// TestHeartbeat_SetupFailureFailsOpen: with heartbeats enabled, a setup that
// cannot create the table fails NewCDCIterator (and so Open) with the stable
// code, instead of running without heartbeats.
func TestHeartbeat_SetupFailureFailsOpen(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", table))
	})

	hb := testHeartbeatConfig()
	hb.Schema = "no_such_schema_" + table
	_, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:            []string{table},
		TableKeys:         map[string]string{table: "id"},
		PublicationName:   table,
		SlotName:          table,
		BatchSize:         1,
		SchemaDriftPolicy: SchemaDriftPolicyHalt,
		Heartbeat:         hb,
	})
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), ErrorCodeHeartbeatSetupFailed))
}

// TestHeartbeat_WriteFailureDoesNotStall: when heartbeat writes start failing
// (here the table is dropped under the connector), the failure is counted and
// logged, data keeps flowing, and acks keep advancing the slot (design doc,
// Decision 6).
func TestHeartbeat_WriteFailureDoesNotStall(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	hb := testHeartbeatConfig()
	i := testCDCIteratorHeartbeat(ctx, t, pool, table, hb)
	waitFor(t, 10*time.Second, "first heartbeat", func() bool { return observedHeartbeatLSN(t, i) > 0 })

	_, err := pool.Exec(ctx, "DROP TABLE "+hb.qualifiedName())
	is.NoErr(err)
	waitFor(t, 10*time.Second, "heartbeat write failures", func() bool {
		return i.HeartbeatStatus().ConsecutiveWriteFailures >= 2
	})

	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('still-flowing')`, table))
	is.NoErr(err)
	recs, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	is.NoErr(i.Ack(ctx, recs[0].Position))

	lsn := recordLSN(t, recs[0])
	waitFor(t, 10*time.Second, "the ack to reach the slot", func() bool {
		return slotFlushLSN(ctx, t, pool, table) >= lsn
	})
}

// TestHeartbeatWriter_QuotesIdentifiers guards the generated SQL: both parts
// of the table name are quoted separately, so a mixed-case or dotted name is
// used as configured.
func TestHeartbeatWriter_QuotesIdentifiers(t *testing.T) {
	is := is.New(t)
	c := HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: "Ops", Table: "my.heartbeat"}
	is.Equal(c.qualifiedName(), internal.WrapSQLIdent("Ops")+"."+internal.WrapSQLIdent("my.heartbeat"))
	w := newHeartbeatWriter(nil, c, "slot", time.Now)
	is.True(strings.Contains(w.query, `INSERT INTO "Ops"."my.heartbeat" AS t`))
}
