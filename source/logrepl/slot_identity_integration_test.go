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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// Integration tests for the slot identity (#442,
// docs/design-documents/20261021-slot-identity.md). They need Postgres 14+
// (identity beacons), which `make test` runs.

func requireIdentityServer(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	info, err := internal.ReadServerInfo(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !info.BeaconsSupported() {
		t.Skipf("server %d cannot carry slot identity beacons", info.Version)
	}
}

// slotIDRecreate drops the slot and creates it again under the same name,
// as an operator or another tool would. lost rows are written while it is
// gone. The slot stays active briefly after a Teardown, so the drop retries.
func slotIDRecreate(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, lost ...string) {
	t.Helper()
	slotGapDropSlot(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, lost...)
	if _, err := pool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", table); err != nil {
		t.Fatalf("recreate slot %q: %v", table, err)
	}
}

func slotIDConfirmed(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) pglogrepl.LSN {
	t.Helper()
	s, err := internal.ReadReplicationSlot(ctx, pool, slot)
	if err != nil {
		t.Fatal(err)
	}
	return s.ConfirmedFlushLSN
}

func slotIDBound(t *testing.T, p opencdc.Position) pglogrepl.LSN {
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

// slotIDLegacy rewrites a position as v0.17.3 wrote it: format version 3, no
// slot identity.
func slotIDLegacy(t *testing.T, p opencdc.Position) opencdc.Position {
	t.Helper()
	pp, err := position.ParseSDKPosition(p)
	if err != nil {
		t.Fatal(err)
	}
	pp.Version, pp.SlotIdentity = 3, ""
	pp.SystemIdentifier, pp.Timeline = "", 0
	b, err := json.Marshal(pp)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func slotIDLogs(ctx context.Context) (context.Context, *syncBuffer) {
	var logs syncBuffer
	return zerolog.New(&logs).WithContext(ctx), &logs
}

func slotIDSetup(ctx context.Context, t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	requireIdentityServer(ctx, t, pool)
	table := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})
	return pool, table
}

// Invariant 3: a slot dropped and created again under the same name between
// two runs exists and is healthy, and its confirmed_flush_lsn is past the
// saved position like a slot the connector advanced while idle. It holds no
// beacon of the pipeline, so the resume is refused with slot_replaced
// instead of silently skipping the rows written while the slot was gone.
// Before #442 this opened and lost lost-1 and lost-2.
func TestSlotIdentity_RecreatedBetweenRuns_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	pp, err := position.ParseSDKPosition(pos)
	is.NoErr(err)
	is.Equal(pp.Version, position.CurrentPositionVersion)
	is.True(pp.SlotIdentity != "")

	slotIDRecreate(ctx, t, pool, table, "lost-1", "lost-2")
	recreated := slotIDConfirmed(ctx, t, pool, table)
	is.True(recreated > slotIDBound(t, pos)) // looks like an idle-advanced slot

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it)
	}
	var se *SlotError
	is.True(errors.As(err, &se))
	is.Equal(se.Code, CodeSlotReplaced)
	for _, want := range []string{string(CodeSlotReplaced), table, recreated.String(), "logrepl.allowWALGap", "re-snapshot", "logrepl.slotName"} {
		is.True(strings.Contains(err.Error(), want)) // names the code, slot, LSN, fixes and path
	}
	// The refusal consumed nothing and wrote no beacon a retry could find.
	is.Equal(slotIDConfirmed(ctx, t, pool, table), recreated)
	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)

	// logrepl.allowWALGap accepts the loss with one warning ...
	lctx, logs := slotIDLogs(ctx)
	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true
	it, err = NewCombinedIterator(lctx, pool, conf)
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "after-reopen")
	var pos2 opencdc.Position
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	is.Equal(string(recs[0].Payload.After.(opencdc.StructuredData)["key"].([]byte)), "after-reopen")
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos2 = recs[0].Position
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), dataLossWarn), 1)
	is.True(strings.Contains(logs.String(), string(CodeSlotReplaced)))

	// ... and the new slot carries an identity of its own from then on (the
	// old one must never prove it): the next start from a position saved on
	// it resumes without the opt-in.
	is.True(posOf(t, recs[0]).SlotIdentity != pp.SlotIdentity)
	is.True(posOf(t, recs[0]).SlotIdentity != "")
	slotGapInsert(ctx, t, pool, table, "run-3")
	it, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, pos2, false))
	is.NoErr(err)
	is.Equal(slotGapReadKeys(ctx, t, it, 1), []string{"run-3"})
	is.NoErr(it.Teardown(ctx))
}

// Any role that can connect may call pg_logical_emit_message (PUBLIC has
// EXECUTE by default). Beacons written into a replaced slot by someone who
// knows the slot and even the position's identity field, but cannot make the
// MAC, do not make it pass (security review of #442). The default grant is
// reported with slot_beacon_public.
func TestSlotIdentity_ForgedBeacons_StillRefused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "acked")
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos := recs[0].Position
	is.NoErr(it.Teardown(ctx))
	identity := mustParse(t, pos).SlotIdentity
	is.True(strings.Contains(logs.String(), string(CodeSlotBeaconPublic)))
	is.True(!strings.Contains(logs.String(), identity)) // never logged

	slotIDRecreate(ctx, t, pool, table, "lost-1")
	var now pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&now))
	for _, forged := range []string{
		"v=1;id=" + identity + ";n=forged",
		"v=2;n=forged;l=" + now.String() + ";mac=" + strings.Repeat("ab", 32),
	} {
		_, err := pool.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, $2::text)", internal.BeaconPrefix, forged)
		is.NoErr(err)
	}

	lctx, logs = slotIDLogs(ctx)
	_, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)
	is.True(!strings.Contains(err.Error(), identity))
	is.True(!strings.Contains(logs.String(), identity))
}

// The #445 review case: an attempt with logrepl.allowWALGap creates the slot
// (CREATE_REPLICATION_SLOT succeeds) and stamps it, then fails before
// anything is saved. The slot is healthy and the saved position predates it.
// A retry without the opt-in must still refuse it: the attempt stamped the
// slot with a new identity, not the saved position's, so the slot holds no
// beacon that proves it for that position.
func TestSlotIdentity_HalfFinishedCreate_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	slotGapDropSlot(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "lost-1", "lost-2")

	errInjected := errors.New("injected failure after the slot was created")
	afterSlotArmed = func() error { return errInjected }
	t.Cleanup(func() { afterSlotArmed = nil })
	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true
	_, err := NewCombinedIterator(ctx, pool, conf)
	is.True(errors.Is(err, errInjected))
	afterSlotArmed = nil

	// The failed attempt left a healthy slot behind, past the position.
	slotGapWaitInactive(ctx, t, pool, table)
	is.True(slotIDConfirmed(ctx, t, pool, table) > slotIDBound(t, pos))

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it)
	}
	is.Equal(CodeOf(err), CodeSlotReplaced)

	// With the opt-in, the retry accepts it again, with the warning again.
	lctx, logs := slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, conf)
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), dataLossWarn), 1)
}

// slotGapWaitInactive waits until no connection holds the slot.
func slotGapWaitInactive(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; {
		s, err := internal.ReadReplicationSlot(ctx, pool, slot)
		if err != nil {
			t.Fatal(err)
		}
		if !s.Active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %q still active", slot)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// No false positive: the connector's own idle advance moves the slot past
// the saved position (the "allAcked -> report the server's WAL end" path),
// but only up to a beacon, so the slot still holds one after its
// confirmed_flush_lsn and resumes. This is the case that made a plain
// "confirmed_flush_lsn > saved position" rule unusable.
func TestSlotIdentity_IdleAdvancedSlot_ResumesWithoutFalsePositive(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	other := test.SetupTestTable(ctx, t, pool)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "acked")
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos := recs[0].Position
	bound := slotIDBound(t, pos)

	// Unrelated WAL while the pipeline is idle. The status update every 10s
	// advances the slot, a beacon at a time.
	var confirmed pglogrepl.LSN
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		slotGapInsert(ctx, t, pool, other, fmt.Sprintf("noise-%d", time.Now().UnixNano()))
		if confirmed = slotIDConfirmed(ctx, t, pool, table); confirmed > bound {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	is.NoErr(it.Teardown(ctx))
	confirmed = slotIDConfirmed(ctx, t, pool, table)
	t.Logf("saved position bound %s, confirmed_flush_lsn after the idle advance %s", bound, confirmed)
	is.True(confirmed > bound) // the slot is past the saved position, as a recreated one would be

	slotGapInsert(ctx, t, pool, table, "while-stopped")
	lctx, logs := slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
	is.NoErr(err)
	is.Equal(slotGapReadKeys(ctx, t, it, 1), []string{"while-stopped"})
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), identityBeacon.String())) // proven by the beacon, not the bound
	for _, code := range []Code{CodeSlotReplaced, CodeSlotIdentityUnverified, CodeSlotWALGap} {
		is.True(!strings.Contains(logs.String(), string(code)))
	}
}

// The window between the identity check and the stream start: a slot
// dropped and created again there passes the check (it ran on the old slot)
// and is refused by VerifyHeld once the stream holds it. The refusal writes
// no beacon, so a retry is refused too.
func TestSlotIdentity_RecreatedAfterIdentityCheck_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	afterIdentityCheck = func() { slotIDRecreate(ctx, t, pool, table, "lost-1") }
	t.Cleanup(func() { afterIdentityCheck = nil })

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it)
	}
	is.Equal(CodeOf(err), CodeSlotReplaced)
	afterIdentityCheck = nil

	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// A slot that was there for checkSlot, gone for the identity check, and back
// (created by someone else) when the subscription opens it cannot be the
// position's slot: refused, not opened.
func TestSlotIdentity_MissingAtIdentityCheckThenBack_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	afterSlotCheck = func() { slotGapDropSlot(ctx, t, pool, table) }
	afterIdentityCheck = func() {
		slotGapInsert(ctx, t, pool, table, "lost-1")
		if _, err := pool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", table); err != nil {
			t.Errorf("recreate slot: %v", err)
		}
	}
	t.Cleanup(func() { afterSlotCheck, afterIdentityCheck = nil, nil })

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it)
	}
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// The #445 limit: with logrepl.allowWALGap, a slot dropped and created again
// by someone else between checkSlot and our CREATE made the CREATE fail with
// a duplicate and went unwarned. It is now warned about.
func TestSlotIdentity_AllowWALGap_WarnsWhenSlotRecreatedAfterCheck(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	afterIdentityCheck = func() { slotIDRecreate(ctx, t, pool, table, "lost-1") }
	t.Cleanup(func() { afterIdentityCheck = nil })

	lctx, logs := slotIDLogs(ctx)
	conf := slotGapConfig(table, pos, false)
	conf.AllowWALGap = true
	it, err := NewCombinedIterator(lctx, pool, conf)
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), dataLossWarn), 1)
	is.True(strings.Contains(logs.String(), string(CodeSlotReplaced)))
}

// A snapshot position from a run whose slot was replaced: the snapshot was
// read at the old slot's consistent point, and the new slot would miss every
// change to rows already read.
func TestSlotIdentity_SnapshotPosition_RecreatedRefused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	slotGapInsert(ctx, t, pool, table, "s1", "s2", "s3", "s4", "s5", "s6")

	i1, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, true))
	is.NoErr(err)
	recs, err := i1.NextN(ctx, 2)
	is.NoErr(err)
	pos := recs[len(recs)-1].Position
	is.NoErr(i1.Ack(ctx, pos))
	is.NoErr(i1.Teardown(ctx))
	pp, err := position.ParseSDKPosition(pos)
	is.NoErr(err)
	is.Equal(pp.Type, position.TypeSnapshot)
	is.True(pp.SlotIdentity != "") // snapshot records carry it too

	// An interrupted snapshot resumes on its intact slot: the stamp written
	// when the slot was created is after the slot's consistent point.
	i2, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, true))
	is.NoErr(err)
	is.NoErr(i2.Teardown(ctx))

	slotIDRecreate(ctx, t, pool, table)
	_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %q SET column1='changed' WHERE key='s1'`, table))
	is.NoErr(err)
	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, true))
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// Upgrade from v0.17.3: its positions have no slot identity. The upgrade
// run cannot prove a slot that is past the saved position (v0.17.3 advanced
// idle slots to the WAL end with no beacon), so it takes the slot over with
// a slot_identity_unverified warning, stamps it, and from then on a
// recreated slot is refused.
func TestSlotIdentity_UpgradeFromV0173_AdoptsThenEnforces(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	other := test.SetupTestTable(ctx, t, pool)

	legacy := slotIDLegacy(t, slotGapRun1(ctx, t, pool, table))
	is.True(!strings.Contains(string(legacy), "slot_identity"))
	is.True(strings.Contains(string(legacy), `"version":3`))
	// v0.17.3's idle advance: to the WAL end, no beacon.
	slotGapInsert(ctx, t, pool, other, "noise")
	slotGapAdvance(ctx, t, pool, table)
	is.True(slotIDConfirmed(ctx, t, pool, table) > slotIDBound(t, legacy))
	slotGapInsert(ctx, t, pool, table, "while-stopped")

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	is.NoErr(err)
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeSlotIdentityUnverified)), 1)
	is.True(strings.Contains(logs.String(), `"level":"warn"`))

	upgraded := posOf(t, recs[0])
	is.Equal(upgraded.Version, position.CurrentPositionVersion)
	is.Equal(len(upgraded.SlotIdentity), 64) // a new random key, not one derived from the position
	is.True(upgraded.SystemIdentifier != "") // the legacy position learns its WAL history (#447 B3)
	is.True(upgraded.Timeline >= 1)

	slotIDRecreate(ctx, t, pool, table, "lost-1")
	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, recs[0].Position, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// Upgrade: a v0.17.3 position whose slot is not past it is proven by the
// position alone, with no warning.
func TestSlotIdentity_UpgradeFromV0173_CoveredSlotIsProven(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	test.CreatePublication(t, pool, table, []string{table})
	test.CreateReplicationSlot(t, pool, table)

	// A position committed after the slot's start, as v0.17.3 writes it.
	var now string
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&now))
	legacy := opencdc.Position(fmt.Sprintf(`{"version":3,"type":2,"last_lsn":%q,"tx_commit_lsn":%q,"tx_seq":1}`, now, now))
	is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, legacy))

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), identityCovered.String()))
	is.True(!strings.Contains(logs.String(), string(CodeSlotIdentityUnverified)))
}

// Upgrade: a run that resumes from a v0.17.3 position and stops before it
// delivers anything leaves that position in place. Its key was random and is
// lost with it, so the next run cannot prove the slot by a beacon either: it
// is adopted again with the warning (or, on servers before 14.19, 15.14,
// 16.10 and 17.6, proven by the position, because the first run's first status
// update moved confirmed_flush_lsn back to the saved LSN, which loses
// nothing). It is never refused: a legacy position has no key to fail.
func TestSlotIdentity_UpgradeFromV0173_NoRecordsThenAdoptedAgain(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	other := test.SetupTestTable(ctx, t, pool)

	legacy := slotIDLegacy(t, slotGapRun1(ctx, t, pool, table))
	slotGapInsert(ctx, t, pool, other, "noise")
	slotGapAdvance(ctx, t, pool, table)

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeSlotIdentityUnverified)), 1)

	lctx, logs = slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), string(CodeSlotIdentityUnverified)) || strings.Contains(logs.String(), identityCovered.String()))
	is.True(!strings.Contains(logs.String(), string(CodeSlotReplaced)))
}

// A role that may not call pg_logical_emit_message cannot keep a beacon
// ahead of its slot. The start fails with slot_beacon_failed and the GRANT
// that fixes it, instead of running with an idle slot that never advances.
// The function's privileges are per database, so the test revokes them in a
// database of its own.
func TestSlotIdentity_BeaconPermissionDenied(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	admin := test.ConnectPool(ctx, t, test.RepmgrConnString)
	requireIdentityServer(ctx, t, admin)

	name := test.RandomIdentifier(t)
	_, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN REPLICATION PASSWORD 'pw'`, name))
	is.NoErr(err)
	_, err = admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s OWNER %s`, name, name))
	is.NoErr(err)
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", name)
		_, _ = admin.Exec(bg, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
		_, _ = admin.Exec(bg, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, name))
	})

	dbAdminURL := strings.Replace(test.RepmgrConnString, "/meroxadb?", "/"+name+"?", 1)
	dbAdmin := test.ConnectPool(ctx, t, dbAdminURL)
	info, err := internal.ReadServerInfo(ctx, dbAdmin)
	is.NoErr(err)
	for _, q := range []string{
		"REVOKE EXECUTE ON FUNCTION " + internal.EmitMessageSignature(info.Version) + " FROM PUBLIC",
		fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, key text)", name),
		fmt.Sprintf("ALTER TABLE %s OWNER TO %s", name, name),
	} {
		_, err = dbAdmin.Exec(ctx, q)
		is.NoErr(err)
	}

	roleURL := strings.Replace(dbAdminURL, "repmgr:repmgrmeroxa", name+":pw", 1)
	pool := test.ConnectPool(ctx, t, roleURL)
	conf := Config{
		Tables: []string{name}, TableKeys: map[string]string{name: "id"},
		PublicationName: name, SlotName: name, BatchSize: 2,
	}
	_, err = NewCombinedIterator(ctx, pool, conf)
	var se *SlotError
	is.True(errors.As(err, &se))
	is.Equal(se.Code, CodeSlotBeaconFailed)
	is.True(strings.Contains(err.Error(), "GRANT EXECUTE ON FUNCTION "+internal.EmitMessageSignature(info.Version)))
}

func mustParse(t *testing.T, p opencdc.Position) position.Position {
	t.Helper()
	pp, err := position.ParseSDKPosition(p)
	if err != nil {
		t.Fatal(err)
	}
	return pp
}

// A server that cannot carry beacons (before Postgres 14) keeps the check of
// v0.17.4: a slot past the saved position is opened, with a
// slot_identity_unsupported warning, and the stream does not ask for logical
// decoding messages (pgoutput before 14 rejects the option). Runs only
// against such a server.
func TestSlotIdentity_UnsupportedServer_WarnsAndResumes(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	info, err := internal.ReadServerInfo(ctx, pool)
	is.NoErr(err)
	if info.BeaconsSupported() {
		t.Skipf("server %d carries beacons; this test needs one before Postgres 14", info.Version)
	}
	table := test.SetupTestTable(ctx, t, pool)
	other := test.SetupTestTable(ctx, t, pool)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, other, "noise")
	slotGapAdvance(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "while-stopped")

	is.Equal(mustParse(t, pos).SlotIdentity, "") // no identity is written where none can be kept

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
	is.NoErr(err)
	is.Equal(slotGapReadKeys(ctx, t, it, 1), []string{"while-stopped"})
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), string(CodeSlotIdentityUnsupported)))

	// #447 N5: a position that carries an identity (written on a primary
	// that can keep one) is never accepted unproven here.
	withID := slotIDRewrite(t, pos, func(p *position.Position) { p.SlotIdentity = internal.NewSlotIdentity() })
	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, withID, false))
	is.Equal(CodeOf(err), CodeSlotIdentityUnprovable)
}

// #447 review B1: the walsender of a running pipeline dies while the
// connector's pool stays healthy (a half-open replication connection), the
// slot goes inactive, and someone drops and recreates it with rows written in
// between. A beacon the old run writes after that must not vouch for the
// replacement: the write fails (the slot is not held by this run's stream),
// and the next start is refused. Before the fix the beacon was written, the
// new slot decoded it, and the restart delivered only "after".
func TestSlotIdentity_BeaconAfterWalsenderLoss_DoesNotVouchForReplacement(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "acked")
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos := recs[0].Position

	// The stream's walsender goes away; the connector does not notice yet.
	_, err = pool.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1", table)
	is.NoErr(err)
	slotGapWaitInactive(ctx, t, pool, table)
	slotIDRecreate(ctx, t, pool, table, "lost-1", "lost-2")

	// The old run writes its next beacon.
	err = it.cdcIterator.sub.Beacons.Emit(ctx)
	is.True(err != nil) // refused: the slot is not held by this run's stream
	t.Logf("emit after the walsender loss: %v", err)
	_ = it.Teardown(ctx)

	slotGapInsert(ctx, t, pool, table, "after")
	it2, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it2)
	}
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// #447 review B2: a fresh start creates the slot and snapshots for a long
// time before the stream starts. On Postgres 17 the slot is inactive right
// after CREATE, so it can be dropped and recreated mid-snapshot. The stream
// must refuse the replacement at the handoff. Before the fix it streamed from
// the new slot and lost "during-snapshot" and "lost-while-gone".
func TestSlotIdentity_FreshStartSlotReplacedMidSnapshot_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	slotGapInsert(ctx, t, pool, table, "s1", "s2", "s3", "s4", "s5", "s6")

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, true))
	is.NoErr(err)
	defer func() { _ = it.Teardown(context.Background()) }()
	recs, err := it.NextN(ctx, 2)
	is.NoErr(err)
	for _, r := range recs {
		is.NoErr(it.Ack(ctx, r.Position))
	}

	slotGapInsert(ctx, t, pool, table, "during-snapshot")
	slotGapWaitInactive(ctx, t, pool, table)
	slotIDRecreate(ctx, t, pool, table, "lost-while-gone")
	slotGapInsert(ctx, t, pool, table, "after")

	var cdcKeys []string
	var nextErr error
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline) && nextErr == nil && len(cdcKeys) < 3; {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		recs, err := it.NextN(rctx, 10)
		rcancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			nextErr = err
			break
		}
		for _, r := range recs {
			if mustParse(t, r.Position).Type == position.TypeCDC {
				cdcKeys = append(cdcKeys, string(r.Payload.After.(opencdc.StructuredData)["key"].([]byte)))
			}
			is.NoErr(it.Ack(ctx, r.Position))
		}
	}
	if nextErr == nil {
		t.Fatalf("SILENT SKIP: the stream started on the replaced slot; delivered %v, never during-snapshot and lost-while-gone", cdcKeys)
	}
	is.Equal(CodeOf(nextErr), CodeSlotReplaced)
}

// slotIDRewrite returns the position with its fields changed by f, written
// with the current format version.
func slotIDRewrite(t *testing.T, p opencdc.Position, f func(*position.Position)) opencdc.Position {
	t.Helper()
	pp := mustParse(t, p)
	f(&pp)
	return pp.ToSDKPosition()
}

// #447 review B3: an LSN means something only within one WAL history. The
// slot (and even its beacons) can look fine on a server that is not on the
// position's history, and the position's change key would skip the changes
// the new history has at the same LSNs. Each case is refused with
// wal_history_changed; logrepl.allowWALGap accepts it with a warning that is
// not the DATA LOSS one (after a pg_upgrade nothing is lost).
func TestSlotIdentity_WALHistoryChanged_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	pp := mustParse(t, pos)
	is.True(pp.SystemIdentifier != "") // every position names its history
	is.True(pp.Timeline >= 1)
	var current pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&current))

	for name, rewrite := range map[string]func(*position.Position){
		// A different server at the same address, or a cluster restored as a
		// new one, or pg_upgrade.
		"other cluster": func(p *position.Position) { p.SystemIdentifier = "1" },
		// Restored from a backup taken on a later timeline.
		"earlier timeline": func(p *position.Position) { p.Timeline++ },
		// Restored from a backup, snapshot or VM image taken before the
		// position: same cluster and timeline, but the WAL is behind it.
		"WAL behind the position": func(p *position.Position) {
			ahead := (current + 64<<20).String()
			p.LastLSN, p.TxCommitLSN = ahead, ahead
		},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			bad := slotIDRewrite(t, pos, rewrite)
			it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, bad, false))
			if err == nil {
				_ = it.Teardown(ctx)
				t.Fatal("SILENT SKIP: resumed on another WAL history")
			}
			is.Equal(CodeOf(err), CodeWALHistoryChanged)
			is.True(strings.Contains(err.Error(), "pg_upgrade"))

			lctx, logs := slotIDLogs(ctx)
			conf := slotGapConfig(table, bad, false)
			conf.AllowWALGap = true
			it, err = NewCombinedIterator(lctx, pool, conf)
			if err == nil {
				_ = it.Teardown(ctx)
			}
			is.True(strings.Contains(logs.String(), "WAL HISTORY CHANGE ACCEPTED"))
			is.True(!strings.Contains(logs.String(), "DATA LOSS ACCEPTED (logrepl.allowWALGap): the replication slot was gone"))
		})
	}
}

// A legacy position (no system identifier) cannot have its WAL history
// checked: the start says so once, with wal_history_unverified and a
// fingerprint of the server's system identifier, and the next position
// names the history. A position that carries one is checked silently.
func TestSlotIdentity_WALHistoryUnverified_WarnsOnlyForLegacy(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	pos := slotGapRun1(ctx, t, pool, table)
	sysid := mustParse(t, pos).SystemIdentifier
	is.True(sysid != "")

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, slotIDLegacy(t, pos), false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeWALHistoryUnverified)), 1)
	is.True(strings.Contains(logs.String(), internal.IdentityFingerprint(sysid)))
	is.True(!strings.Contains(logs.String(), sysid)) // a fingerprint, not the identifier

	lctx, logs = slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeWALHistoryUnverified)), 0)
}
