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

//go:build failover

// Package failover tests that a replication slot's identity (#442,
// docs/design-documents/20261021-slot-identity.md) survives a legitimate
// failover: a Postgres 17+ failover slot synchronized to a standby, which is
// then promoted. Run it with test/failover/run.sh.
package failover

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func connect(ctx context.Context, t *testing.T, env string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(env)
	if url == "" {
		t.Fatalf("INFRA: %s is not set; run test/failover/run.sh", env)
	}
	pool, err := cpool.New(ctx, url) // the connector's pool: replication connections on demand
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

func lsnOf(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q string, args ...any) pglogrepl.LSN {
	t.Helper()
	var l pglogrepl.LSN
	if err := pool.QueryRow(ctx, q, args...).Scan(&l); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return l
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !ok(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func insert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, key string) {
	t.Helper()
	exec(ctx, t, pool, fmt.Sprintf("INSERT INTO %s (key) VALUES ($1)", table), key)
}

func config(table string, pos opencdc.Position) logrepl.Config {
	return logrepl.Config{
		Position: pos, Tables: []string{table}, TableKeys: map[string]string{table: "id"},
		PublicationName: table, SlotName: table, BatchSize: 10,
	}
}

func keyOf(r opencdc.Record) string {
	k := r.Payload.After.(opencdc.StructuredData)["key"]
	if b, ok := k.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(k)
}

// A failover slot synchronized to a standby keeps the pipeline's identity
// across the standby's promotion: the beacon that bounds the slot's
// confirmed_flush_lsn is in the WAL the standby received, so the resume on
// the new primary proves the slot and delivers what was written before the
// failover. A slot dropped and created again on the new primary is refused.
func TestFailover_SlotIdentitySurvivesPromotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	is := is.New(t)
	primary := connect(ctx, t, "FAILOVER_PRIMARY_URL")
	standby := connect(ctx, t, "FAILOVER_STANDBY_URL")
	if sss := os.Getenv("FAILOVER_SYNC_STANDBY_SLOTS"); sss == "" {
		t.Log("synchronized_standby_slots is unset on the primary: the design does not promise this case (see the design doc)")
	}

	table := fmt.Sprintf("failover_%d", time.Now().UnixNano()%1_000_000_000)
	other := table + "_other"
	exec(ctx, t, primary, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", table))
	exec(ctx, t, primary, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", other))
	exec(ctx, t, primary, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", table, table))
	// failover=true (Postgres 17): the standby synchronizes this slot.
	exec(ctx, t, primary, "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)", table)

	// Run 1 on the primary: one acked record, then idle long enough for the
	// connector to advance the slot past the saved position, a beacon at a
	// time.
	it, err := logrepl.NewCombinedIterator(ctx, primary, config(table, nil))
	is.NoErr(err)
	insert(ctx, t, primary, table, "acked")
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos := recs[0].Position
	pp, err := position.ParseSDKPosition(pos)
	is.NoErr(err)
	bound, err := pp.CoveredThrough()
	is.NoErr(err)
	slotConfirmed := func(pool *pgxpool.Pool) pglogrepl.LSN {
		return lsnOf(ctx, t, pool, "SELECT coalesce(confirmed_flush_lsn, '0/0') FROM pg_replication_slots WHERE slot_name = $1", table)
	}
	waitFor(t, "the idle slot to advance past the saved position", 3*time.Minute, func() bool {
		insert(ctx, t, primary, other, "noise")
		return slotConfirmed(primary) > bound
	})
	is.NoErr(it.Teardown(ctx))
	confirmed := slotConfirmed(primary)
	t.Logf("primary: saved position bound %s, slot confirmed_flush_lsn %s", bound, confirmed)

	// The standby synchronizes the slot (sync_replication_slots) and replays
	// a row written after the stop.
	insert(ctx, t, primary, table, "while-stopped")
	end := lsnOf(ctx, t, primary, "SELECT pg_current_wal_flush_lsn()")
	waitFor(t, "the standby to synchronize the slot and replay the last row", 3*time.Minute, func() bool {
		var synced bool
		var cf pglogrepl.LSN
		err := standby.QueryRow(ctx,
			"SELECT synced, coalesce(confirmed_flush_lsn, '0/0') FROM pg_replication_slots WHERE slot_name = $1", table).Scan(&synced, &cf)
		if err != nil {
			return false
		}
		return synced && cf == confirmed && lsnOf(ctx, t, standby, "SELECT pg_last_wal_replay_lsn()") >= end
	})
	is.True(slotConfirmed(standby) > bound) // the synced slot is past the saved position too

	// Fail over.
	exec(ctx, t, standby, "SELECT pg_promote(true, 60)")
	waitFor(t, "the promotion", time.Minute, func() bool {
		var rec bool
		_ = standby.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&rec)
		return !rec
	})

	// Run 2 on the new primary resumes on the synced slot: no false positive.
	var logs syncBuffer
	lctx := zerolog.New(&logs).WithContext(ctx)
	it, err = logrepl.NewCombinedIterator(lctx, standby, config(table, pos))
	is.NoErr(err)
	recs, err = it.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(keyOf(recs[0]), "while-stopped")
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos2 := recs[0].Position
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), "identity beacon found"))
	t.Logf("run 2 on the promoted standby: %s", logs.String())

	// The promotion moved the new primary to a new timeline. The position
	// from the old one is on the shared part of the history, so it resumed
	// above. A position past the switch point is not: the old timeline's
	// changes after it are not in this server's WAL (#447 B3).
	p2, err := position.ParseSDKPosition(pos2)
	is.NoErr(err)
	is.True(p2.Timeline > pp.Timeline) // positions follow the new timeline
	// The switch point, from the new timeline's history file ("1<TAB>lsn<TAB>reason").
	var history string
	is.NoErr(standby.QueryRow(ctx,
		"SELECT pg_read_file('pg_wal/' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8) || '.history')").Scan(&history))
	fields := strings.Fields(strings.SplitN(strings.TrimSpace(history), "\n", 2)[0])
	is.True(len(fields) >= 2)
	switchPoint, err := pglogrepl.ParseLSN(fields[1])
	is.NoErr(err)
	t.Logf("timeline switch point %s, saved position bound %s", switchPoint, bound)
	is.True(bound <= switchPoint) // why the resume above passed
	past := pp
	ahead := (switchPoint + 0x100).String()
	past.LastLSN, past.TxCommitLSN = ahead, ahead
	_, err = logrepl.NewCombinedIterator(ctx, standby, config(table, past.ToSDKPosition()))
	is.Equal(logrepl.CodeOf(err), logrepl.CodeWALHistoryChanged)
	t.Logf("position past the switch point: %v", err)

	// A slot recreated under the same name on the new primary is refused.
	waitFor(t, "the slot to be released", 30*time.Second, func() bool {
		_, err := standby.Exec(ctx, "SELECT pg_drop_replication_slot($1)", table)
		return err == nil
	})
	insert(ctx, t, standby, table, "lost")
	exec(ctx, t, standby, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", table)
	_, err = logrepl.NewCombinedIterator(ctx, standby, config(table, pos2))
	is.Equal(logrepl.CodeOf(err), logrepl.CodeSlotReplaced)
}
