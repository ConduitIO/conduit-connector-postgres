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

package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func TestBeaconContent_Verify(t *testing.T) {
	is := is.New(t)
	key := NewSlotIdentity()
	is.Equal(len(key), 64)
	is.True(key != NewSlotIdentity()) // random

	c := []byte(beaconContent(key, "slot", "n1", 0x100))
	is.True(!strings.Contains(string(c), key)) // the key is never in the WAL
	is.True(verifyBeacon(c, key, "slot", 0x100))
	is.True(verifyBeacon(c, key, "slot", 0x80))
	is.True(!verifyBeacon(c, key, "slot", 0x101))           // written before the slot's confirmed_flush_lsn: a copy
	is.True(!verifyBeacon(c, NewSlotIdentity(), "slot", 0)) // another key
	is.True(!verifyBeacon(c, key, "other", 0))              // another slot

	// Tampering with any authenticated field breaks the MAC.
	is.True(!verifyBeacon([]byte(strings.Replace(string(c), "n=n1", "n=n2", 1)), key, "slot", 0))
	is.True(!verifyBeacon([]byte(strings.Replace(string(c), "l=0/100", "l=0/200", 1)), key, "slot", 0))
	// The v1 format named the identity in clear; it proves nothing now.
	is.True(!verifyBeacon([]byte("v=1;id="+key+";n=x"), key, "slot", 0))
	for _, junk := range []string{"", "v=2", "v=2;n=x;l=0/100", "v=2;n=x;l=zz;mac=00", "v=2;n=x;l=0/100;mac=zz"} {
		is.True(!verifyBeacon([]byte(junk), key, "slot", 0))
	}
}

func TestIdentityFingerprint(t *testing.T) {
	is := is.New(t)
	key := NewSlotIdentity()
	fp := IdentityFingerprint(key)
	is.Equal(len(fp), 8)
	is.True(!strings.Contains(key, fp)) // not a prefix of the key
	is.Equal(fp, IdentityFingerprint(key))
	is.Equal(IdentityFingerprint(""), "")
}

// The rounds grow by 8 and never pass the clamp, so the last round asks for
// exactly the maximum and exhaustion is decided against it.
func TestNextFindBeaconLimit(t *testing.T) {
	is := is.New(t)
	is.Equal(nextFindBeaconLimit(1024, 1<<24), 8192)
	is.Equal(nextFindBeaconLimit(1<<21, 1<<24), 1<<24)
	is.Equal(nextFindBeaconLimit(1<<24, 1<<24), 1<<24)
	is.Equal(nextFindBeaconLimit(1024, 3000), 3000)
	limit, rounds := min(findBeaconFirstRound, 1<<24), 1
	for limit < 1<<24 {
		limit = nextFindBeaconLimit(limit, 1<<24)
		rounds++
		is.True(limit <= 1<<24)
	}
	is.Equal(rounds, 6) // 1024, 8192, ..., 2^23, then 2^24
}

// Only the pending beacon moves the cap, to the WAL position read before it
// was written; other prefixes, other content and a late copy do not.
func TestBeacons_Observe(t *testing.T) {
	is := is.New(t)
	b := NewBeacons(nil, "slot", "abc")
	b.pending = &pendingBeacon{content: beaconContent("abc", "slot", "1", 0x100), start: 0x100, at: time.Now()}

	is.True(!b.observe(&pglogrepl.LogicalDecodingMessage{Prefix: "other", Content: []byte(b.pending.content), LSN: 0x180}))
	// A valid beacon with another nonce (an earlier one, or a copy) is not
	// the one this emission recorded.
	is.True(!b.observe(&pglogrepl.LogicalDecodingMessage{Prefix: BeaconPrefix, Content: []byte(beaconContent("abc", "slot", "2", 0x100)), LSN: 0x180}))
	is.Equal(b.Cap(), pglogrepl.LSN(0))

	is.True(b.observe(&pglogrepl.LogicalDecodingMessage{Prefix: BeaconPrefix, Content: []byte(beaconContent("abc", "slot", "1", 0x100)), LSN: 0x180}))
	is.Equal(b.Cap(), pglogrepl.LSN(0x100)) // the start, not the end the stream reports
	is.Equal(b.lastEnd, pglogrepl.LSN(0x180))
	is.True(b.pending == nil)

	// The same beacon again (a re-send after a reconnect) is no longer pending.
	is.True(!b.observe(&pglogrepl.LogicalDecodingMessage{Prefix: BeaconPrefix, Content: []byte(beaconContent("abc", "slot", "1", 0x100)), LSN: 0x180}))
}

// maybeRequest keeps an idle server idle: it does not ask for a beacon while
// one is on its way, or for less than minBeaconWAL of new WAL.
func TestBeacons_MaybeRequestThresholds(t *testing.T) {
	is := is.New(t)
	now := time.Now()
	b := NewBeacons(nil, "slot", "abc")
	b.now = func() time.Time { return now }
	emitted := make(chan struct{}, 10)
	b.emit = func(context.Context) error { emitted <- struct{}{}; return errors.New("not emitted in this test") }
	b.cap, b.lastEnd = 0x1000, 0x1048
	ctx := context.Background()

	b.maybeRequest(ctx, 0x1048+minBeaconWAL-1, 0x900)
	is.True(b.pending == nil) // our own beacon and a running_xacts record do not count
	is.Equal(len(emitted), 0)

	b.pending = &pendingBeacon{content: "x", at: now}
	b.maybeRequest(ctx, 0x100000, 0x900)
	is.Equal(b.pending.content, "x") // one beacon at a time
	is.Equal(len(emitted), 0)

	b.pending.at = now.Add(-staleBeaconAfter - time.Second)
	b.maybeRequest(ctx, 0x100000, 0x900)
	select {
	case <-emitted: // a stale beacon is no longer waited for
	case <-time.After(5 * time.Second):
		t.Fatal("no new beacon after the pending one went stale")
	}
	// The failed emission frees the slot for the next request.
	for deadline := time.Now().Add(5 * time.Second); ; {
		b.mu.Lock()
		p := b.pending
		b.mu.Unlock()
		if p == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a failed emission left a placeholder behind")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.maybeRequest(ctx, 0x100000, 0x900)
	select {
	case <-emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("no retry after a failed emission")
	}
}

// Records acked past the cap ask for a beacon even with little new WAL, so
// an idle slot moves past the last acked transaction instead of decoding it
// again after a restart.
func TestBeacons_MaybeRequestAfterAcks(t *testing.T) {
	is := is.New(t)
	b := NewBeacons(nil, "slot", "abc")
	emitted := make(chan struct{}, 1)
	b.emit = func(context.Context) error { emitted <- struct{}{}; return nil }
	b.cap, b.lastEnd = 0x1000, 0x1048

	b.maybeRequest(context.Background(), 0x1100, 0x1000) // acked up to the cap only
	is.Equal(len(emitted), 0)
	b.maybeRequest(context.Background(), 0x1100, 0x1080) // acked past it
	select {
	case <-emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("no beacon after records were acked past the cap")
	}
}

// beaconSlot creates a pgoutput slot and a publication for a new table.
func beaconSlot(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (slot, table string) {
	t.Helper()
	slot = test.RandomIdentifier(t)
	table = test.SetupEmptyTestTable(ctx, t, pool)
	test.CreatePublication(t, pool, slot, []string{table})
	test.CreateReplicationSlot(t, pool, slot)
	return slot, table
}

func confirmedFlush(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	s, err := ReadReplicationSlot(ctx, pool, slot)
	if err != nil {
		t.Fatal(err)
	}
	return s.ConfirmedFlushLSN
}

func requireBeacons(ctx context.Context, t *testing.T, pool *pgxpool.Pool) ServerInfo {
	t.Helper()
	info, err := ReadServerInfo(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !info.BeaconsSupported() {
		t.Skipf("server %d cannot carry beacons", info.Version)
	}
	return info
}

// FindBeacon finds a beacon the slot still decodes, and only with its own
// identity: a slot whose confirmed_flush_lsn is at the WAL insert position
// read before the beacon (Beacons.Cap) still decodes it, one confirmed past
// the beacon's commit does not. Emit returns only once the beacon is flushed,
// so it is found at once, with no wait for the WAL writer.
func TestFindBeacon(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	requireBeacons(ctx, t, pool)
	slot, _ := beaconSlot(ctx, t, pool)

	found, _, err := FindBeacon(ctx, pool, slot, slot, "abc", confirmedFlush(ctx, t, pool, slot))
	is.NoErr(err)
	is.True(!found) // a new slot holds no beacon

	b := NewBeacons(pool, slot, "abc")
	is.NoErr(b.Emit(ctx))
	start := b.pending.start
	var afterCommit pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&afterCommit))

	found, end, err := FindBeacon(ctx, pool, slot, slot, "abc", confirmedFlush(ctx, t, pool, slot))
	is.NoErr(err)
	is.True(found) // durable when Emit returns: no wait
	is.True(end > start)

	found, _, err = FindBeacon(ctx, pool, slot, slot, "other", confirmedFlush(ctx, t, pool, slot))
	is.NoErr(err)
	is.True(!found) // another pipeline's identity is not ours

	_, err = pool.Exec(ctx, "SELECT pg_replication_slot_advance($1, $2)", slot, start)
	is.NoErr(err)
	is.Equal(confirmedFlush(ctx, t, pool, slot), start)
	found, _, err = FindBeacon(ctx, pool, slot, slot, "abc", confirmedFlush(ctx, t, pool, slot))
	is.NoErr(err)
	is.True(found) // confirmed at the beacon's start: still decoded

	_, err = pool.Exec(ctx, "SELECT pg_replication_slot_advance($1, $2)", slot, afterCommit)
	is.NoErr(err)
	found, _, err = FindBeacon(ctx, pool, slot, slot, "abc", confirmedFlush(ctx, t, pool, slot))
	is.NoErr(err)
	is.True(!found) // confirmed past its commit: skipped
}

// A slot dropped and created again after the identity check, in the window
// before START_REPLICATION takes it, is refused by VerifyHeld before anything
// is sent on the stream.
func TestSubscription_VerifyHeldRefusesSwappedSlot(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	slot, table := beaconSlot(ctx, t, pool)

	sub, err := OpenSubscription(ctx, pool, slot, slot, []string{table}, 0, func(context.Context, pglogrepl.Message, pglogrepl.LSN) (pglogrepl.LSN, error) {
		return 0, nil
	})
	is.NoErr(err)
	limit := sub.ConfirmedFlushLSN
	errSwapped := errors.New("swapped")
	var seen pglogrepl.LSN
	sub.VerifyHeld = func(_ context.Context, s ReadReplicationSlotResult) error {
		seen = s.ConfirmedFlushLSN
		if s.ConfirmedFlushLSN > limit {
			return errSwapped
		}
		return nil
	}
	sub.beforeAcquire = func() {
		// The slot is not held here (the creating connection was released).
		_, err := pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
		is.NoErr(err)
		_, err = pool.Exec(ctx, "SELECT pg_logical_emit_message(false, 'test', 'wal')")
		is.NoErr(err)
		_, err = pool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)
		is.NoErr(err)
	}

	runErr := sub.Run(ctx)
	is.True(errors.Is(runErr, errSwapped))
	<-sub.Ready()
	is.True(errors.Is(sub.StartErr(), errSwapped))
	is.True(seen > limit)
	is.NoErr(sub.Teardown(ctx))
	// Nothing was reported to the new slot.
	is.Equal(confirmedFlush(ctx, t, pool, slot), seen)
}

// Any role may call pg_logical_emit_message. A beacon written by someone who
// knows the slot but not the key, one in the v1 format that named the
// identity in clear, and a genuine beacon copied into a replaced slot are
// all refused (security review of #442).
func TestFindBeacon_RefusesForgedAndCopiedBeacons(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	requireBeacons(ctx, t, pool)
	slot, _ := beaconSlot(ctx, t, pool)
	key := NewSlotIdentity()
	emit := func(content string) {
		t.Helper()
		_, err := pool.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, $2::text)", BeaconPrefix, content)
		is.NoErr(err)
	}
	found := func() bool {
		t.Helper()
		ok, _, err := FindBeacon(ctx, pool, slot, slot, key, confirmedFlush(ctx, t, pool, slot))
		is.NoErr(err)
		return ok
	}

	var now pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&now))
	emit("v=1;id=" + key + ";n=x")                                   // the identity in clear
	emit(beaconContent(NewSlotIdentity(), slot, "forged", now))      // the right slot, a guessed key
	emit("v=2;n=forged;l=" + now.String() + ";mac=" + randomHex(32)) // a random MAC
	is.True(!found())

	// A genuine beacon, copied into the slot after it was recreated.
	b := NewBeacons(pool, slot, key)
	is.NoErr(b.Emit(ctx))
	is.True(found())
	genuine := b.pending.content
	_, err := pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	is.NoErr(err)
	_, err = pool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)
	is.NoErr(err)
	emit(genuine)
	is.True(!found()) // written before the new slot's confirmed_flush_lsn
}

// A slot with more changes in front of any beacon than the search may read
// fails with ErrBeaconSearchExhausted at the clamped limit, never with a
// round that asks for more.
func TestFindBeacon_ExhaustedAtClampedLimit(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	requireBeacons(ctx, t, pool)
	slot, table := beaconSlot(ctx, t, pool)
	for i := 0; i < 5; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (key) VALUES ('%d')`, table, i))
		is.NoErr(err)
	}
	defer func(old int) { findBeaconMaxRows = old }(findBeaconMaxRows)
	findBeaconMaxRows = 4 // below the first round of 1024

	_, _, err := FindBeacon(ctx, pool, slot, slot, NewSlotIdentity(), confirmedFlush(ctx, t, pool, slot))
	is.True(errors.Is(err, ErrBeaconSearchExhausted))
	is.True(strings.Contains(err.Error(), "read "))
}
