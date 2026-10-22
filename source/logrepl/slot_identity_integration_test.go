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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
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
	pp.TxXid, pp.TxCommitTime = 0, 0
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
		"v=3;n=forged;l=" + now.String() + ";r=" + now.String() + ";mac=" + strings.Repeat("ab", 32),
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

	// A position committed after the slot's start, as v0.17.3 writes it: the
	// commit LSN of a transaction the slot decodes.
	slotGapInsert(ctx, t, pool, table, "r1")
	var data []byte
	is.NoErr(pool.QueryRow(ctx, `SELECT data FROM pg_logical_slot_peek_binary_changes($1::name, NULL, NULL,
		'proto_version', '1', 'publication_names', $2::text) WHERE get_byte(data, 0) = 66 LIMIT 1`, table, table).Scan(&data))
	msg, err := pglogrepl.Parse(data)
	is.NoErr(err)
	commitLSN := msg.(*pglogrepl.BeginMessage).FinalLSN
	legacy := opencdc.Position(fmt.Sprintf(`{"version":3,"type":2,"last_lsn":%q,"tx_commit_lsn":%q,"tx_seq":1}`, commitLSN, commitLSN))
	is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, legacy))

	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), identityCovered.String()))
	is.True(!strings.Contains(logs.String(), string(CodeSlotIdentityUnverified)))
}

// #447 round-4 B2 (the reviewer's S2 for legacy positions): a pre-v0.17.5
// position names no xid, but its change key still names a commit LSN. On a
// server restored on the same timeline whose new WAL passed it, nothing
// commits there any more, and the covered resume is refused instead of
// skipping what the new history wrote at those LSNs.
func TestSlotIdentity_UpgradeFromV0173_CoveredButNoCommitThere_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	test.CreatePublication(t, pool, table, []string{table})
	test.CreateReplicationSlot(t, pool, table)

	// The slot's start is covered, but no transaction commits at the
	// position's commit LSN: the first one the slot decodes commits later.
	var now string
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&now))
	slotGapInsert(ctx, t, pool, table, "n1")
	legacy := opencdc.Position(fmt.Sprintf(`{"version":3,"type":2,"last_lsn":%q,"tx_commit_lsn":%q,"tx_seq":1}`, now, now))
	is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, legacy))

	_, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, legacy, false))
	is.Equal(CodeOf(err), CodeWALHistoryChanged)
	is.True(strings.Contains(err.Error(), "committed at LSN "+now))
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

	// The v4 position first: the legacy start below takes the slot over
	// with a new key, after which the v4 position's key proves nothing.
	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeWALHistoryUnverified)), 0)

	lctx, logs = slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, slotIDLegacy(t, pos), false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.Equal(strings.Count(logs.String(), string(CodeWALHistoryUnverified)), 1)
	is.True(strings.Contains(logs.String(), `"level":"warn","code":"`+string(CodeWALHistoryUnverified)))
	is.True(strings.Contains(logs.String(), internal.IdentityFingerprint(sysid)))
	is.True(!strings.Contains(logs.String(), sysid)) // a fingerprint, not the identifier
}

// #447 re-review S2: a covered resume (the slot at or below the position's
// commit) also checks that the commit LSN still names the position's
// transaction: the slot decodes it again, with the same xid and commit
// timestamp. A position whose transaction is not there (here: its xid, its
// commit time or its commit LSN changed, as on a server restored on the same
// timeline whose new WAL reuses the LSNs and the xids; the real restores are
// test/restore) is refused with wal_history_changed. With pg_walinspect
// installed the result is the same (#447 round-4 B1: the check once read
// the commit record there, compared only the xid, and took the next record
// for an LSN inside one).
func TestSlotIdentity_CoveredResume_VerifiesTheTransaction(t *testing.T) {
	for _, walinspect := range []bool{false, true} {
		t.Run(fmt.Sprintf("pg_walinspect=%v", walinspect), func(t *testing.T) {
			ctx := test.Context(t)
			ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
			defer cancel()
			is := is.New(t)
			pool, table := slotIDSetup(ctx, t)
			if walinspect {
				info, err := internal.ReadServerInfo(ctx, pool)
				is.NoErr(err)
				if info.Version < 150000 {
					t.Skip("pg_walinspect needs Postgres 15")
				}
				if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_walinspect"); err != nil {
					t.Skipf("pg_walinspect not available: %v", err)
				}
				t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP EXTENSION IF EXISTS pg_walinspect") })
			}

			pos := slotGapRun1(ctx, t, pool, table)
			pp := mustParse(t, pos)
			is.True(pp.TxXid != 0)
			is.True(pp.TxCommitTime != 0)
			// The run stopped right after its ack, so the slot is at or below
			// the commit: covered.
			is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, pos))

			// The refusals first: they change nothing, so the slot stays
			// covered for the real position after them.
			for name, f := range map[string]func(*position.Position){
				"other xid":                           func(p *position.Position) { p.TxXid++ },
				"same xid and LSN, other commit time": func(p *position.Position) { p.TxCommitTime++ },
				"same xid and time, a commit LSN one byte before the commit (inside a record)": func(p *position.Position) {
					l, err := pglogrepl.ParseLSN(p.TxCommitLSN)
					if err != nil {
						t.Fatal(err)
					}
					p.TxCommitLSN = (l - 1).String()
				},
			} {
				t.Logf("refused: %s", name)
				bad := slotIDRewrite(t, pos, f)
				_, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, bad, false))
				is.Equal(CodeOf(err), CodeWALHistoryChanged)
				is.True(strings.Contains(err.Error(), "restored"))
			}
			is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, pos))

			lctx, logs := slotIDLogs(ctx)
			it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, pos, false))
			is.NoErr(err) // the real transaction: accepted
			is.NoErr(it.Teardown(ctx))
			is.True(strings.Contains(logs.String(), identityCovered.String()))
		})
	}
}

// #447 re-review S3: a legacy position (no WAL history) still gets the check
// that needs none: a position past the server's current WAL is refused.
func TestSlotIdentity_LegacyPositionPastCurrentWAL_Refused(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	pos := slotGapRun1(ctx, t, pool, table)
	var current pglogrepl.LSN
	is.NoErr(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&current))
	legacy := slotIDLegacy(t, slotIDRewrite(t, pos, func(p *position.Position) {
		ahead := (current + 64<<20).String()
		p.LastLSN, p.TxCommitLSN = ahead, ahead
	}))
	is.True(!strings.Contains(string(legacy), "system_identifier"))
	lctx, logs := slotIDLogs(ctx)
	it, err := NewCombinedIterator(lctx, pool, slotGapConfig(table, legacy, false))
	if err == nil {
		_ = it.Teardown(ctx)
		t.Fatal("SILENT SKIP: a legacy position past the server's WAL was accepted")
	}
	is.Equal(CodeOf(err), CodeWALHistoryChanged)
	// Refused by this check, before the transaction check (which would
	// refuse it too: nothing commits past the WAL end).
	is.True(strings.Contains(err.Error(), "past the server's current WAL position"))
	is.Equal(strings.Count(logs.String(), string(CodeWALHistoryUnverified)), 1)
}

// #447 re-review S1: a standby never proves a position's identity. A slot it
// can decode is a slot of the standby, never the primary's, and it would find
// the beacons the primary wrote for its own slot (reproduced on 17 with a
// standby-local slot under the same name: test/failover). The check refuses
// without peeking. Driven here with the standby's ServerInfo against a
// primary slot whose beacon is right there.
func TestSlotIdentity_StandbyNeverProvesAnIdentity(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	pos := slotGapRun1(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "noise")
	slotGapAdvance(ctx, t, pool, table) // past the bound: the beacon would be the proof
	pp := mustParse(t, pos)
	lsn, _ := pp.LSN()
	commit, _ := pp.TxCommit()
	c := CDCConfig{LSN: lsn, TxCommitLSN: commit, TxSeq: pp.TxSeq, SlotName: table, PublicationName: table, StartPosition: pp}
	info, err := internal.ReadServerInfo(ctx, pool)
	is.NoErr(err)
	for _, v := range []int{160000, 170000, info.Version} {
		res, err := checkSlotIdentity(ctx, pool, c, internal.ServerInfo{Version: v, InRecovery: true})
		is.NoErr(err)
		is.Equal(res.outcome, identityUnprovable)
	}
}

// #447 re-review S4: beacons that keep failing are abandoned (the slot
// advances uncapped, the disk is safe) and a restart in that state is
// refused. The connector keeps trying: once beacons can be written again the
// first one delivered re-arms the identity, and a restart is accepted again.
// Driven by revoking pg_logical_emit_message in a database of its own.
func TestSlotIdentity_BeaconsAbandonedThenRecover(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
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
	dbAdmin := test.ConnectPool(ctx, t, strings.Replace(test.RepmgrConnString, "/meroxadb?", "/"+name+"?", 1))
	info, err := internal.ReadServerInfo(ctx, dbAdmin)
	is.NoErr(err)
	sig := internal.EmitMessageSignature(info.Version)
	for _, q := range []string{
		fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, key text)", name),
		fmt.Sprintf("ALTER TABLE %s OWNER TO %s", name, name),
		"CREATE TABLE noise (id bigserial PRIMARY KEY, v text)",
		"GRANT ALL ON noise TO PUBLIC", "GRANT ALL ON SEQUENCE noise_id_seq TO PUBLIC",
	} {
		_, err = dbAdmin.Exec(ctx, q)
		is.NoErr(err)
	}
	pool := test.ConnectPool(ctx, t, strings.Replace(strings.Replace(test.RepmgrConnString, "/meroxadb?", "/"+name+"?", 1), "repmgr:repmgrmeroxa", name+":pw", 1))
	conf := Config{Tables: []string{name}, TableKeys: map[string]string{name: "id"},
		PublicationName: name, SlotName: name, BatchSize: 2, BeaconFailureTimeout: 2 * time.Second}
	ins := func(k string) {
		t.Helper()
		_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (key) VALUES ($1)", name), k)
		is.NoErr(err)
	}
	advancedPast := func(b pglogrepl.LSN) bool {
		t.Helper()
		for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
			_, _ = pool.Exec(ctx, "INSERT INTO noise (v) VALUES (repeat('x', 600))")
			if slotIDConfirmed(ctx, t, admin, name) > b+1024 {
				return true
			}
			time.Sleep(500 * time.Millisecond)
		}
		return false
	}
	run := func(p opencdc.Position) (*CombinedIterator, opencdc.Position, *syncBuffer) {
		t.Helper()
		lctx, logs := slotIDLogs(ctx)
		c := conf
		c.Position = p
		it, err := NewCombinedIterator(lctx, pool, c)
		is.NoErr(err)
		ins(fmt.Sprintf("k-%d", time.Now().UnixNano()))
		recs, err := it.NextN(ctx, 1)
		is.NoErr(err)
		is.NoErr(it.Ack(ctx, recs[0].Position))
		return it, recs[0].Position, logs
	}

	// 1. Abandoned, then stopped before beacons work again: refused.
	it, pos, logs := run(nil)
	_, err = dbAdmin.Exec(ctx, "REVOKE EXECUTE ON FUNCTION "+sig+" FROM PUBLIC")
	is.NoErr(err)
	is.True(advancedPast(slotIDBound(t, pos))) // uncapped: the disk is safe
	is.True(strings.Contains(logs.String(), `"level":"error","code":"`+internal.CodeBeaconUnavailable))
	is.NoErr(it.Teardown(ctx))
	_, err = dbAdmin.Exec(ctx, "GRANT EXECUTE ON FUNCTION "+sig+" TO PUBLIC")
	is.NoErr(err)
	c := conf
	c.Position = pos
	_, err = NewCombinedIterator(ctx, pool, c)
	is.Equal(CodeOf(err), CodeSlotReplaced) // before a re-arm: refused

	// 2. Abandoned while running, then beacons work again: the identity
	// re-arms, and a restart is accepted.
	c.AllowWALGap = true // get past the refusal above once
	c.Position = pos
	lctx, logs := slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, c)
	is.NoErr(err)
	ins("after-takeover")
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	pos2 := recs[0].Position
	_, err = dbAdmin.Exec(ctx, "REVOKE EXECUTE ON FUNCTION "+sig+" FROM PUBLIC")
	is.NoErr(err)
	is.True(advancedPast(slotIDBound(t, pos2)))
	is.True(strings.Contains(logs.String(), `"level":"error","code":"`+internal.CodeBeaconUnavailable))
	_, err = dbAdmin.Exec(ctx, "GRANT EXECUTE ON FUNCTION "+sig+" TO PUBLIC")
	is.NoErr(err)
	for deadline := time.Now().Add(90 * time.Second); !strings.Contains(logs.String(), "can be proven again"); {
		if time.Now().After(deadline) {
			t.Fatal("beacons did not re-arm after EXECUTE was granted again")
		}
		_, _ = pool.Exec(ctx, "INSERT INTO noise (v) VALUES (repeat('x', 600))")
		time.Sleep(500 * time.Millisecond)
	}
	is.NoErr(it.Teardown(ctx))
	c.AllowWALGap = false
	c.Position = pos2
	lctx, logs = slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, c)
	is.NoErr(err) // re-armed: proven again
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), identityBeacon.String()) || strings.Contains(logs.String(), identityCovered.String()))
}

// #447 re-review S5 (M1), integration: a beacon that committed although
// Emit returned an error still raises the cap when the stream delivers it.
func TestSlotIdentity_CommittedButEmitErrored_StreamRaisesCap(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	defer func() { _ = it.Teardown(context.Background()) }()
	b := it.cdcIterator.sub.Beacons
	for deadline := time.Now().Add(20 * time.Second); b.Cap() == 0; { // the stamp came back
		if time.Now().After(deadline) {
			t.Fatal("the stamp never came back")
		}
		time.Sleep(50 * time.Millisecond)
	}
	before := b.Cap()
	// Every beacon the stream asks for from now on commits, then Emit
	// returns an error, as if the COMMIT response was lost.
	var lost atomic.Int32
	errLost := errors.New("connection lost while reading the COMMIT response")
	internal.AfterBeaconCommit = func() error { lost.Add(1); return errLost }
	t.Cleanup(func() { internal.AfterBeaconCommit = nil })
	slotGapInsert(ctx, t, pool, table, "acked") // acked past the cap: the stream asks for a beacon
	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	for deadline := time.Now().Add(60 * time.Second); b.Cap() <= before; {
		if time.Now().After(deadline) {
			t.Fatalf("the committed beacon did not raise the cap (%d emissions errored after their commit)", lost.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	is.True(lost.Load() > 0) // the cap was raised by a beacon whose Emit errored
}

// #447 re-review N1: the guard identifies this run's walsender by PID and
// backend_start. A different walsender that holds the replacement slot
// under the same PID (PID reuse, simulated by pointing the old run at the
// impostor's PID with the old walsender's backend_start) is not it: the
// beacon is refused and the restart is refused. From the review's scratch
// test, which accepted the replaced slot before this.
func TestSlotIdentity_PIDReuse_Refused(t *testing.T) {
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
	var oldStart time.Time
	is.NoErr(pool.QueryRow(ctx, `SELECT a.backend_start FROM pg_replication_slots s JOIN pg_stat_activity a ON a.pid = s.active_pid
		WHERE s.slot_name = $1`, table).Scan(&oldStart))

	_, err = pool.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1", table)
	is.NoErr(err)
	slotGapWaitInactive(ctx, t, pool, table)
	slotIDRecreate(ctx, t, pool, table, "lost-1", "lost-2")

	// An impostor walsender on the replacement slot.
	cfg := pool.Config().ConnConfig.Config.Copy()
	cfg.RuntimeParams["replication"] = "database"
	rc, err := pgconn.ConnectConfig(ctx, cfg)
	is.NoErr(err)
	is.NoErr(pglogrepl.StartReplication(ctx, rc, table, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", table)}}))
	var pid int32
	for i := 0; i < 50 && pid == 0; i++ {
		_ = pool.QueryRow(ctx, "SELECT coalesce(active_pid,0) FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&pid)
		time.Sleep(100 * time.Millisecond)
	}
	is.Equal(int64(pid), int64(rc.PID()))
	it.cdcIterator.sub.Beacons.SetStreamHolder(rc.PID(), oldStart) // the old walsender's PID, reused
	err = it.cdcIterator.sub.Beacons.Emit(ctx)
	is.True(errors.Is(err, internal.ErrSlotNotHeld))
	_ = rc.Close(ctx)
	_ = it.Teardown(ctx)
	slotGapWaitInactive(ctx, t, pool, table)

	slotGapInsert(ctx, t, pool, table, "after")
	it2, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, pos, false))
	if err == nil {
		slotGapDiagnose(ctx, t, pool, table, it2)
	}
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// #447 re-review S1, on a real standby (run by test/failover/run.sh, which
// sets FAILOVER_PRIMARY_URL and FAILOVER_STANDBY_URL; skipped otherwise). An
// operator creates a slot on the standby under the pipeline's slot name,
// after a row the pipeline never delivered. The standby decodes the beacons
// the primary wrote for its own slot, so a beacon search there "found" one
// (the review reproduced it on 17). The check must refuse it unproven.
func TestSlotIdentity_StandbyLocalSlot_Refused(t *testing.T) {
	pu, su := os.Getenv("FAILOVER_PRIMARY_URL"), os.Getenv("FAILOVER_STANDBY_URL")
	if pu == "" || su == "" {
		t.Skip("needs the failover stack: test/failover/run.sh")
	}
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 240*time.Second)
	defer cancel()
	is := is.New(t)
	primary := test.ConnectPool(ctx, t, pu)
	standby := test.ConnectPool(ctx, t, su)
	table := fmt.Sprintf("standby_local_%d", time.Now().UnixNano()%1e9)
	test.SetupEmptyTestTableWithName(ctx, t, primary, table)
	t.Cleanup(func() {
		_, _ = standby.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", table)
		_ = Cleanup(context.Background(), CleanupConfig{URL: pu, SlotName: table, PublicationName: table})
	})

	pos := slotGapRun1(ctx, t, primary, table) // run 1 on the primary
	bound := slotIDBound(t, pos)
	slotGapInsert(ctx, t, primary, table, "lost") // never delivered

	// A slot on the standby under the same name (logical decoding on a
	// standby needs a standby snapshot from the primary).
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
				_, _ = primary.Exec(context.Background(), "SELECT pg_log_standby_snapshot()")
			}
		}
	}()
	_, err := standby.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", table)
	close(stop)
	is.NoErr(err)
	local := slotIDConfirmed(ctx, t, standby, table)
	is.True(local > bound) // created after the undelivered row

	// Run 2 on the primary stamps the primary's slot; the standby replays it.
	it, err := NewCombinedIterator(ctx, primary, slotGapConfig(table, pos, false))
	is.NoErr(err)
	time.Sleep(2 * time.Second)
	is.NoErr(it.Teardown(ctx))
	var end pglogrepl.LSN
	is.NoErr(primary.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&end))
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var r pglogrepl.LSN
		is.NoErr(standby.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn()").Scan(&r))
		if r >= end {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the standby did not replay")
		}
	}

	server, err := internal.ReadServerInfo(ctx, standby)
	is.NoErr(err)
	is.True(server.InRecovery)
	pp := mustParse(t, pos)
	lsn, _ := pp.LSN()
	commit, _ := pp.TxCommit()
	res, err := checkSlotIdentity(ctx, standby, CDCConfig{
		LSN: lsn, TxCommitLSN: commit, TxSeq: pp.TxSeq, SlotName: table, PublicationName: table, StartPosition: pp,
	}, server)
	is.NoErr(err)
	t.Logf("standby-local slot at %s, position bound %s: %s", local, bound, res.outcome)
	is.Equal(res.outcome, identityUnprovable) // before the fix: "identity beacon found"
}

// #447 round-4 B3: another client consumes the pipeline's own idle slot
// past a transaction the pipeline received but never acked (here with
// pg_logical_slot_get_binary_changes, one change at a time). The slot is
// the pipeline's and still decodes the stamp written when the run opened it,
// but the stamp certifies only the slot position that run checked, which is
// before the consumed transaction: the restart is refused instead of
// resuming past it. From the review's scratch test, which delivered only
// [after] and lost R.
func TestSlotIdentity_ConsumedByAnotherClientBelowTheLastBeacon_Refused(t *testing.T) {
	ctx, cancel := context.WithTimeout(test.Context(t), 2*time.Minute)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	p := slotGapRun1(ctx, t, pool, table)
	slotGapWaitInactive(ctx, t, pool, table)
	slotGapInsert(ctx, t, pool, table, "R-consumed-by-another-client")

	// Run 2 stamps (after R), receives R, and stops before the ack.
	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, p, false))
	is.NoErr(err)
	rctx, c := context.WithTimeout(ctx, 10*time.Second)
	recs, err := it.NextN(rctx, 10)
	c()
	is.NoErr(err)
	is.True(len(recs) > 0)
	// Hold R unacked past a status interval (10 seconds), so the stream
	// gets the chance to ask for beacons while it is in flight: none may
	// certify past R.
	time.Sleep(12 * time.Second)
	is.NoErr(it.Teardown(ctx))
	slotGapWaitInactive(ctx, t, pool, table)
	before := slotIDConfirmed(ctx, t, pool, table)

	// Another client consumes until it has taken R (the key column is
	// bytea, so its text form is hex).
	for i := 0; ; i++ {
		is.True(i < 50)
		var cnt int
		var hit bool
		is.NoErr(pool.QueryRow(ctx, `SELECT count(*), coalesce(bool_or(position('R-consumed'::bytea IN data) > 0
			       OR position(convert_to(encode('R-consumed'::bytea, 'hex'), 'UTF8') IN data) > 0), false)
			FROM pg_logical_slot_get_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', $2)`, table, table).Scan(&cnt, &hit))
		if hit {
			break
		}
		is.True(cnt > 0)
	}
	after := slotIDConfirmed(ctx, t, pool, table)
	t.Logf("another client moved the slot %s -> %s; position bound %s", before, after, slotIDBound(t, p))
	is.True(after > slotIDBound(t, p))

	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, p, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// A beacon writer of a run that has stopped, in stream mode and before the
// stream, cannot write into a slot that was dropped and created again under
// the same name, and the restart is refused. From the review's scratch test
// of 85da517.
func TestSlotIdentity_LateBeaconFromAStoppedRun_Refused(t *testing.T) {
	ctx, cancel := context.WithTimeout(test.Context(t), 2*time.Minute)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, nil, false))
	is.NoErr(err)
	slotGapInsert(ctx, t, pool, table, "a1")
	var p opencdc.Position
	for deadline := time.Now().Add(20 * time.Second); p == nil && time.Now().Before(deadline); {
		rctx, c := context.WithTimeout(ctx, 5*time.Second)
		recs, err := it.NextN(rctx, 10)
		c()
		if err != nil {
			continue
		}
		for _, r := range recs {
			is.NoErr(it.Ack(ctx, r.Position))
			p = r.Position
		}
	}
	is.True(p != nil)
	old := it.cdcIterator.sub.Beacons
	is.True(old != nil)
	is.NoErr(it.Teardown(ctx))
	slotGapWaitInactive(ctx, t, pool, table)

	slotIDRecreate(ctx, t, pool, table, "lost-1")

	err = old.Emit(ctx) // stream mode: the old run's walsender is gone
	is.True(errors.Is(err, internal.ErrSlotNotHeld))
	pre := internal.NewBeacons(pool, table, "k") // before the stream, with a limit the new slot is past
	pre.SetPreStreamLimit(slotIDConfirmed(ctx, t, pool, table) - 1)
	err = pre.Emit(ctx)
	is.True(errors.Is(err, internal.ErrSlotNotHeld))

	_, err = NewCombinedIterator(ctx, pool, slotGapConfig(table, p, false))
	is.Equal(CodeOf(err), CodeSlotReplaced)
}

// #447 round-4 S1: a position whose transaction was a prepared transaction
// resumes covered. Its commit is a COMMIT PREPARED record, whose xid
// pg_walinspect reports as 0 (the real one is only in its description), so
// the check that read it there refused the resume. The check reads the slot,
// which decodes the transaction at COMMIT PREPARED with its own xid and
// commit time, the ones the position recorded. pg_walinspect is installed
// when the server allows it, to show it plays no part.
func TestSlotIdentity_PreparedTransactionPosition_ResumesCovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(test.Context(t), 2*time.Minute)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	var maxPrepared int
	is.NoErr(pool.QueryRow(ctx, "SELECT current_setting('max_prepared_transactions')::int").Scan(&maxPrepared))
	if maxPrepared == 0 {
		t.Skip("max_prepared_transactions is 0 (test/conf.d sets it for the test stack)")
	}
	_, _ = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_walinspect")

	it, err := NewCombinedIterator(ctx, pool, slotGapConfig(table, opencdc.Position{}, false))
	is.NoErr(err)
	gid := "conduit-" + table
	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	_, err = conn.Exec(ctx, "BEGIN")
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (key) VALUES ('prepared')`, table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf("PREPARE TRANSACTION '%s'", gid))
	is.NoErr(err)
	conn.Release()
	_, err = pool.Exec(ctx, fmt.Sprintf("COMMIT PREPARED '%s'", gid))
	is.NoErr(err)

	recs, err := it.NextN(ctx, 1)
	is.NoErr(err)
	is.NoErr(it.Ack(ctx, recs[0].Position))
	p := recs[0].Position
	is.NoErr(it.Teardown(ctx))
	slotGapWaitInactive(ctx, t, pool, table)
	pp, err := position.ParseSDKPosition(p)
	is.NoErr(err)
	is.True(pp.TxXid != 0)
	is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, p)) // covered: the transaction check runs

	lctx, logs := slotIDLogs(ctx)
	it, err = NewCombinedIterator(lctx, pool, slotGapConfig(table, p, false))
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
	is.True(strings.Contains(logs.String(), identityCovered.String()))
}

// #447 security review of 453669f, end to end: while a large transaction is
// being streamed and every record received so far is acked, the walsender's
// keepalives (it asks for a reply every wal_sender_timeout/2, here 4
// seconds) carry a WAL end at that transaction's commit, never past it
// (internal.TestWalsender_KeepaliveInsideATransactionNeverPassesItsCommit).
// So neither the idle advance nor a beacon's r passes it: the slot stays at
// or below the commit until the transaction is acked, and a pipeline
// stopped in the middle of it gets the rest after a restart.
func TestSlotIdentity_LargeTransactionInFlight_SlotNeverPassesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(test.Context(t), 3*time.Minute)
	defer cancel()
	is := is.New(t)
	admin, table := slotIDSetup(ctx, t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString+"&options=-c%20wal_sender_timeout%3D8000")
	cfg := slotGapConfig(table, nil, false)
	cfg.BatchSize = 500

	it, err := NewCombinedIterator(ctx, pool, cfg)
	is.NoErr(err)
	const rows, stopAt = 100000, 60000
	_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (key, column1) SELECT convert_to('t' || g, 'UTF8'), 'x' FROM generate_series(1, %d) g`, table, rows))
	is.NoErr(err)

	var commit pglogrepl.LSN
	var last opencdc.Position
	var maxConfirmed pglogrepl.LSN
	reachedCommit := false
	for n := 0; n < stopAt; {
		recs, err := it.NextN(ctx, 500)
		is.NoErr(err)
		for _, r := range recs {
			if n == stopAt {
				break // the rest of this batch is never acked
			}
			pp := mustParse(t, r.Position)
			c, err := pglogrepl.ParseLSN(pp.TxCommitLSN)
			is.NoErr(err)
			if commit == 0 {
				commit = c
			}
			is.Equal(c, commit) // all from the one transaction
			is.NoErr(it.Ack(ctx, r.Position))
			last = r.Position
			n++
		}
		confirmed := slotIDConfirmed(ctx, t, admin, table)
		if confirmed > maxConfirmed {
			maxConfirmed = confirmed
		}
		reachedCommit = reachedCommit || confirmed == commit
		if confirmed > commit {
			t.Fatalf("the slot moved to %s, past the commit %s of the transaction in flight (%d of %d rows acked)", confirmed, commit, n, rows)
		}
		if b := it.cdcIterator.sub.Beacons; b != nil && b.Cap() > commit {
			t.Fatalf("a beacon certifies %s, past the commit %s of the transaction in flight", b.Cap(), commit)
		}
	}
	t.Logf("transaction committed at %s; slot at most %s while it was in flight (at its commit: %v)", commit, maxConfirmed, reachedCommit)
	is.NoErr(it.Teardown(ctx))
	slotGapWaitInactive(ctx, t, admin, table)
	is.True(slotIDConfirmed(ctx, t, admin, table) <= commit)

	// The restart delivers the rest of the transaction, exactly.
	it2, err := NewCombinedIterator(ctx, admin, slotGapConfig(table, last, false))
	is.NoErr(err)
	defer func() { _ = it2.Teardown(context.Background()) }()
	got := 0
	for got < rows-stopAt {
		rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
		recs, err := it2.NextN(rctx, 500)
		rcancel()
		is.NoErr(err)
		for _, r := range recs {
			is.NoErr(it2.Ack(ctx, r.Position))
			got++
		}
	}
	is.Equal(got, rows-stopAt)
}

// #447 fresh-context review S1: a check that cannot read the slot (here it
// runs out of logrepl.slotIdentity.checkTimeout) refuses the start with a
// code and a fix, never an uncoded error, and allowWALGap accepts the slot
// unproven.
func TestSlotIdentity_CheckCannotReadTheSlot_CodedRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(test.Context(t), 2*time.Minute)
	defer cancel()
	is := is.New(t)
	pool, table := slotIDSetup(ctx, t)
	p := slotGapRun1(ctx, t, pool, table)
	slotGapWaitInactive(ctx, t, pool, table)
	is.True(slotIDConfirmed(ctx, t, pool, table) <= slotIDBound(t, p)) // covered: the transaction check peeks

	c := slotGapConfig(table, p, false)
	c.IdentityCheckTimeout = time.Microsecond
	_, err := NewCombinedIterator(ctx, pool, c)
	is.Equal(CodeOf(err), CodeSlotIdentityCheckFailed)
	is.True(strings.Contains(err.Error(), "checkTimeout"))

	c.AllowWALGap = true
	it, err := NewCombinedIterator(ctx, pool, c)
	is.NoErr(err)
	is.NoErr(it.Teardown(ctx))
}
