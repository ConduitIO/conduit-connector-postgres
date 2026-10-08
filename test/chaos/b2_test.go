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

//go:build conduitchaos

// DBZ-3 B2 (the flush gate) on the B0 kill harness. See
// docs/design-documents/20261007-dbz3-b2-heartbeats.md.

package chaos

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// TestB2_KillWithWALEndPastUnackedRecord: SIGKILL while a CDC record has
// been delivered but not acked and the walsender has sent unrelated WAL past
// it, so the keepalive WAL end is beyond the record. The flush position the
// connector reports must never reach the record (the walFlushed ==
// walWritten gate), so after the kill the slot still holds it, and the
// restart delivers it: no gap.
//
// The window is built deterministically. The child's read loop parks at
// RecordSeen for the first CDC record (snapshot rows are 1-4), which holds
// that record in flight while the rest of the connector keeps running and
// sending status updates. The parent writes unrelated WAL until the
// walsender has sent past it, waits for two status replies from the child
// after that, and only then checks the slot and kills.
//
// Perturbation proof: removing the gate in internal.reportedPositions makes
// the slot confirm the WAL end past the record's commit, and this test fails
// on the confirmed_flush_lsn check before the kill; with that check skipped,
// the restart never delivers the record.
func TestB2_KillWithWALEndPastUnackedRecord(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	replPool, regPool, table, slot, pub, ledgerPath := b1Setup(t)
	unrelated, err := randChaosName()
	is.NoErr(err)
	test.SetupEmptyTestTableWithName(ctx, t, regPool, unrelated) // not in the publication

	const firstCDCRecord = 5 // 4 seeded snapshot rows, then the in-flight insert
	cp := b2SpawnChild(t, b2ChildSpec{
		run: 1, total: firstCDCRecord,
		park:       fmt.Sprintf("%s:%d", chaospoint.RecordSeen, firstCDCRecord),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", 4, 60*time.Second)

	var id int64
	err = regPool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('in-flight') RETURNING id`, table)).Scan(&id)
	is.NoErr(err)
	committedBy := b2CurrentWAL(t, regPool) // at or past the insert's commit record

	cp.waitForMarker(t, "PARKED", 60*time.Second) // the record is delivered, not durable, not acked

	// Unrelated WAL past the record, sent by the walsender, and two status
	// replies from the child after that: the connector has had the chance to
	// confirm a WAL end past the record, twice.
	pollUntil(t, 30*time.Second, func() string { return "walsender to send past the in-flight record" }, func() bool {
		b2WriteUnrelated(t, regPool, unrelated)
		return b2SentLSN(t, replPool, slot) > committedBy
	})
	b2WaitReplies(t, replPool, slot, 2, 45*time.Second)

	b2AssertSlotBelow(t, replPool, slot, committedBy, "before the kill")
	cp.sigkill(t)
	b2AssertSlotBelow(t, replPool, slot, committedBy, "after the kill")

	entries := b1ReadLedger(t, ledgerPath)
	is.Equal(len(entries), 4) // only the snapshot rows were durable before the kill
	lastRun1 := entries[len(entries)-1]

	// Run 2: resumes from the last snapshot checkpoint and delivers the
	// in-flight record.
	cp2 := b2SpawnChild(t, b2ChildSpec{
		run: 2, total: 1,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &lastRun1)
	is.Equal(len(entries), 5)
	last := entries[4]
	is.Equal(last.Run, 2)
	is.Equal(last.Op, "cdc")
	is.Equal(last.Key, string(opencdc.StructuredData{"id": id}.Bytes())) // the in-flight row, not lost
}

type b2ChildSpec struct {
	run        int
	total      int
	park       string
	ledgerPath string
	table      string
	slot       string
	pub        string
}

func b2SpawnChild(t *testing.T, s b2ChildSpec) *childProcess {
	t.Helper()
	env := []string{
		envRealChild + "=" + envValueTrue,
		envURL + "=" + RepmgrConnString,
		envTable + "=" + s.table,
		envSlot + "=" + s.slot,
		envPub + "=" + s.pub,
		envLedger + "=" + s.ledgerPath,
		envTotal + "=" + strconv.Itoa(s.total),
		envBatchSize + "=3",
		envRun + "=" + strconv.Itoa(s.run),
	}
	if s.park != "" {
		env = append(env, "PGCHAOS_PARK="+s.park)
	}
	return spawnChildWithEnv(t, env)
}

func b2WriteUnrelated(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf(`INSERT INTO %q (column1) SELECT 'x' FROM generate_series(1, 200)`, table)); err != nil {
		t.Fatalf("unrelated insert: %v", err)
	}
}

func b2CurrentWAL(t *testing.T, pool *pgxpool.Pool) pglogrepl.LSN {
	t.Helper()
	var s string
	if err := pool.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&s); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return lsn
}

// b2SentLSN is how far the walsender serving slot has sent.
func b2SentLSN(t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	var s *string
	err := pool.QueryRow(context.Background(), `
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

// b2WaitReplies waits until the child has sent n more standby status
// updates (pg_stat_replication.reply_time changes n times).
func b2WaitReplies(t *testing.T, pool *pgxpool.Pool, slot string, n int, timeout time.Duration) {
	t.Helper()
	replyTime := func() time.Time {
		var ts *time.Time
		err := pool.QueryRow(context.Background(), `
			SELECT r.reply_time FROM pg_stat_replication r
			JOIN pg_replication_slots s ON s.active_pid = r.pid
			WHERE s.slot_name = $1`, slot).Scan(&ts)
		if err != nil || ts == nil {
			return time.Time{}
		}
		return *ts
	}
	prev := replyTime()
	for i := 0; i < n; i++ {
		pollUntil(t, timeout, func() string { return fmt.Sprintf("status reply %d of %d after %s", i+1, n, prev) }, func() bool {
			return replyTime().After(prev)
		})
		prev = replyTime()
	}
}

func b2AssertSlotBelow(t *testing.T, pool *pgxpool.Pool, slot string, bound pglogrepl.LSN, when string) {
	t.Helper()
	state, err := ReadSlotState(context.Background(), pool, slot)
	if err != nil {
		t.Fatalf("read slot state %s: %v", when, err)
	}
	flush, err := pglogrepl.ParseLSN(state.ConfirmedFlushLSN)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", state.ConfirmedFlushLSN, err)
	}
	if flush >= bound {
		t.Fatalf("%s: confirmed_flush_lsn %s reached %s while the record committed before it was unacked "+
			"(slot %+v): a crash now loses the record", when, flush, bound, state)
	}
}
