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
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func slotGapInsert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
				VALUES ('%s', 'r', 1, false, 1.1, 1, '{"a":1}', '{"a":1}', 1)`,
			table, k))
		if err != nil {
			t.Fatalf("insert %q: %v", k, err)
		}
	}
}

// slotGapDropSlot drops the replication slot the way an operator, autoCleanup
// or a slot invalidation would. The slot stays active until the walsender of
// the torn-down iterator exits, so it retries.
func slotGapDropSlot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) {
	t.Helper()
	var err error
	for i := 0; i < 50; i++ {
		_, err = pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("drop slot %q: %v", slot, err)
}

// slotGapReadKeys reads from the iterator until it has seen the wanted number
// of CDC records or the deadline passes, acking everything. It returns the
// record keys it saw.
func slotGapReadKeys(ctx context.Context, t *testing.T, it *CombinedIterator, want int) []string {
	t.Helper()
	var keys []string
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for len(keys) < want {
		recs, err := it.NextN(ctx, 10)
		if err != nil {
			break
		}
		for _, r := range recs {
			pos, err := position.ParseSDKPosition(r.Position)
			if err != nil || pos.Type != position.TypeCDC {
				continue
			}
			k := r.Payload.After.(opencdc.StructuredData)["key"]
			if b, ok := k.([]byte); ok {
				k = string(b)
			}
			keys = append(keys, fmt.Sprint(k))
			_ = it.Ack(ctx, r.Position)
		}
	}
	sort.Strings(keys)
	return keys
}

// slotGapRun1 runs a CDC pipeline, acks one record and stops it with the slot
// left in place. It returns the persisted position.
func slotGapRun1(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) opencdc.Position {
	t.Helper()
	is := is.New(t)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "acked")

	var pos opencdc.Position
	deadline := time.Now().Add(20 * time.Second)
	for pos == nil && time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		recs, err := it.NextN(rctx, 10)
		cancel()
		if err != nil {
			continue
		}
		for _, r := range recs {
			is.NoErr(it.Ack(ctx, r.Position))
			pos = r.Position
		}
	}
	is.True(pos != nil) // run 1 delivered and acked a record
	is.NoErr(it.Teardown(ctx))
	return pos
}

func slotGapConfig(table string, pos opencdc.Position, snapshot bool) Config {
	return Config{
		Position:        pos,
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    snapshot,
		BatchSize:       2,
	}
}

// slotGapDiagnose is what the pre-fix code does: Open succeeds, so the test
// shows what the pipeline silently loses.
func slotGapDiagnose(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, it *CombinedIterator) {
	t.Helper()
	slotGapInsert(ctx, t, pool, table, "after-reopen")
	got := slotGapReadKeys(ctx, t, it, 1)
	_ = it.Teardown(ctx)
	t.Fatalf("SILENT SKIP: Open succeeded on a recreated slot; delivered %v, but rows [lost-1 lost-2] written while the slot was gone were never delivered", got)
}

// Invariant 3: a source that resumes from a CDC position whose slot was dropped
// (and is recreated at today's WAL) must refuse to open instead of skipping
// everything between the saved LSN and the new slot.
func TestCombinedIterator_SlotDropped_CDCPosition_FailsLoudly(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)

	slotGapDropSlot(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "lost-1", "lost-2")

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it)
	}

	var gap *SlotError
	is.True(errors.As(err, &gap)) // coded error
	is.Equal(gap.Code, CodeSlotWALGap)
	msg := err.Error()
	for _, want := range []string{string(CodeSlotWALGap), table, "logrepl.allowWALGap", "logrepl.slotName", "set it back"} {
		is.True(strings.Contains(msg, want)) // error names the code, slot, setting and path
	}

	// The refusal must not have created the slot: otherwise a retry would find
	// a slot, see no gap, and silently skip.
	var n int
	is.NoErr(pool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&n))
	is.Equal(n, 0)

	// And it keeps failing on every retry.
	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	is.True(errors.As(err, &gap))
}

// A snapshot-typed position was written by a run whose slot (and exported
// snapshot) no longer exist. Resuming under a new slot would read the rest of
// the tables at a later snapshot and never see changes to the rows already
// read.
func TestCombinedIterator_SlotDropped_SnapshotPosition_FailsLoudly(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})
	slotGapInsert(ctx, t, pool, table, "s1", "s2", "s3", "s4", "s5", "s6")

	i1, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, true))
	is.NoErr(err)
	recs, err := i1.NextN(ctx, 2)
	is.NoErr(err)
	pos := recs[len(recs)-1].Position
	is.NoErr(i1.Ack(ctx, pos))
	pp, err := position.ParseSDKPosition(pos)
	is.NoErr(err)
	is.Equal(pp.Type, position.TypeSnapshot)
	is.NoErr(i1.Teardown(ctx))

	slotGapDropSlot(ctx, t, pool, table)
	// Changes to rows the first run already read.
	_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %q SET column1='changed' WHERE key='s1'`, table))
	is.NoErr(err)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, true))
	if err == nil {
		_ = it.Teardown(ctx)
		t.Fatal("SILENT SKIP: Open succeeded on a recreated slot with a snapshot position; the UPDATE of an already-read row is never delivered")
	}
	var gap *SlotError
	is.True(errors.As(err, &gap))
	is.Equal(gap.Code, CodeSlotWALGap)
}

// An intact slot resumes as before, including after the pipeline sat idle and
// the slot's confirmed_flush_lsn moved past the saved record LSN. This is why
// the check is "the slot is missing", not "the slot's LSN is past the saved
// LSN": the connector itself reports the server's WAL end once everything is
// acked, so a healthy slot is routinely ahead of the last record.
func TestCombinedIterator_IntactSlot_ResumesAfterIdle(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	other := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)
	pp, err := position.ParseSDKPosition(pos)
	is.NoErr(err)
	saved, err := pp.LSN()
	is.NoErr(err)

	// Move WAL on an unrelated table and let the slot follow it.
	slotGapInsert(ctx, t, pool, other, "noise")
	_, err = pool.Exec(ctx, "SELECT pg_switch_wal()")
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "while-stopped")

	slot, err := internal.ReadReplicationSlot(ctx, pool, table)
	is.NoErr(err)
	t.Logf("saved LSN %s, slot restart_lsn %s, confirmed_flush_lsn %s", saved, slot.RestartLSN, slot.ConfirmedFlushLSN)
	// On main the slot's confirmed_flush_lsn is routinely ahead of the saved
	// record (the connector reports the server's WAL end once everything is
	// acked), and the check must not read that as a gap. v0.14 reports the
	// record LSN itself, so here it equals the saved LSN; the point of the
	// control is the same: a healthy idle slot resumes and is not refused.
	is.True(slot.ConfirmedFlushLSN >= saved)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	is.NoErr(err)
	keys := slotGapReadKeys(ctx, t, it, 1)
	is.Equal(keys, []string{"while-stopped"})
	t.Logf("saved LSN %s resumed on the intact slot", saved)
	is.NoErr(it.Teardown(ctx))
}

// logrepl.allowWALGap is the explicit opt-in: the pipeline starts on the new
// slot, delivers what happens from then on, and the skipped rows stay lost.
func TestCombinedIterator_SlotDropped_AllowWALGap(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)
	slotGapDropSlot(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "lost-1", "lost-2")

	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true
	it, err := NewCombinedIterator(ctx, pool, conf)
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "after-reopen")
	is.Equal(slotGapReadKeys(ctx, t, it, 1), []string{"after-reopen"}) // the gap is real, and accepted
	is.NoErr(it.Teardown(ctx))
}

// alterSystem sets cluster settings for a test and restores every one of them
// (ALTER SYSTEM ... RESET) when the test ends.
func alterSystem(ctx context.Context, t *testing.T, pool *pgxpool.Pool, settings map[string]string) {
	t.Helper()
	reload := func() {
		if _, err := pool.Exec(context.Background(), "SELECT pg_reload_conf()"); err != nil {
			t.Errorf("pg_reload_conf: %v", err)
		}
	}
	for name, value := range settings {
		if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", name, value)); err != nil {
			t.Fatalf("alter system %s: %v", name, err)
		}
		t.Cleanup(func() {
			if _, err := pool.Exec(context.Background(), "ALTER SYSTEM RESET "+name); err != nil {
				t.Errorf("alter system reset %s: %v", name, err)
			}
			reload()
		})
	}
	reload()
}

// An invalidated slot that still exists cannot be streamed from. It is refused
// with its own code for a fresh start and for a resume, instead of an uncoded
// scan error (NULL restart_lsn) or a late failure at the snapshot handoff.
//
// This test changes cluster-wide settings for a few seconds (and restores
// them); a slot of another test that is inactive in that window could be
// invalidated too.
func TestCombinedIterator_InvalidatedSlot_FailsLoudly(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	other := test.SetupTestTable(ctx, t, pool)

	// An unused slot that has to keep WAL.
	test.CreateReplicationSlot(t, pool, table)
	test.CreatePublication(t, pool, table, []string{table})

	alterSystem(ctx, t, pool, map[string]string{"max_slot_wal_keep_size": "1MB", "wal_keep_size": "0"})

	var status string
	for deadline := time.Now().Add(30 * time.Second); status != "lost" && time.Now().Before(deadline); {
		slotGapInsert(ctx, t, pool, other, "noise")
		_, err := pool.Exec(ctx, "SELECT pg_switch_wal()")
		is.NoErr(err)
		_, err = pool.Exec(ctx, "CHECKPOINT")
		is.NoErr(err)
		is.NoErr(pool.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&status))
		if status != "lost" {
			time.Sleep(300 * time.Millisecond)
		}
	}
	is.Equal(status, "lost")

	for name, pos := range map[string]opencdc.Position{
		"fresh start":  {},
		"cdc position": opencdc.Position(`{"version":2,"type":2,"last_lsn":"0/1000","tx_commit_lsn":"0/1000","tx_seq":1}`),
		"snapshot":     opencdc.Position(`{"version":2,"type":1}`),
	} {
		_, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, name == "snapshot"))
		var se *SlotError
		if !errors.As(err, &se) {
			t.Fatalf("%s: err = %v, want *SlotError", name, err)
		}
		is.Equal(se.Code, CodeSlotInvalidated)
		for _, want := range []string{string(CodeSlotInvalidated), table, "wal_status=lost", "pg_drop_replication_slot", "re-snapshot"} {
			is.True(strings.Contains(err.Error(), want))
		}
		// logrepl.allowWALGap does not apply to an invalidated slot.
		conf := slotGapConfig(table, pos, name == "snapshot")
		conf.AllowWALGap = true
		_, err = NewCombinedIterator(ctx, pool, conf)
		is.Equal(CodeOf(err), CodeSlotInvalidated)
	}
}
