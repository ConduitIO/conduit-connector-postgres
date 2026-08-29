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

// DBZ-3 B1 acceptance criteria on the B0 kill harness. Each scenario drives a
// real postgres.Source child (the same re-exec protocol as smoke_test.go),
// with the new B1 machinery: ALTER-triggered drift markers, the acked-gated
// halt, and the resume-from-checkpoint escape hatch.
//
// The harness model of the engine (see child.go): the ledger is the durable
// downstream, an entry is fsynced before its record is acked, and a restart
// resumes from the last entry's raw position — the same bytes the engine
// would have checkpointed. The FM1/FM2 windows are the two sides of the
// marker's durability, injected with the child-side chaospoint reaches; FM3
// is injected with the production-side reach inside emitDriftMarker.
//
// A note on the resume boundary: the connector resumes AT the checkpoint LSN
// (START_REPLICATION FROM it), so the record at that exact LSN is re-read
// after every restart — a legitimate at-least-once redelivery. b1AssertNoUnexpectedDups
// allows exactly that one expected duplicate per resume; anything else fails.

package chaos

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// b1HaltTrap is the D5 revert-trap sentence, verbatim (the logrepl constant
// is unexported; its exact wording is pinned word-for-word by
// source/logrepl/drift_policy_test.go's Test_HandleRelation_HaltEmitsMarker).
// AC5 asserts both halt messages contain it.
const b1HaltTrap = "Restart this pipeline to approve the change, or revert the DDL"

// b1DriftColumn is the column every scenario adds (or, in AC5, removes).
const b1DriftColumn = "column101"

type b1ChildSpec struct {
	run          int
	total        int
	haltExpected bool
	park         string // "name:nth" for PGCHAOS_PARK, "" to not park
	ledgerPath   string
	table        string
	slot         string
	pub          string
}

// b1Setup creates the per-scenario infrastructure: a fresh table (4 seeded
// rows), a slot, a publication, and a ledger path. Returns the replication
// pool (slot introspection) and the regular pool (DDL/DML).
func b1Setup(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool, string, string, string, string) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()

	replPool := requireChaosStack(t)
	regPool := test.ConnectPool(ctx, t, RegularConnString)

	table, err := randChaosName()
	is.NoErr(err)
	test.SetupTestTableWithName(ctx, t, regPool, table)

	slot, err := randChaosName()
	is.NoErr(err)
	pub, err := randChaosName()
	is.NoErr(err)
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = replPool.Exec(cleanupCtx, "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name=$1", slot)
		_, _ = replPool.Exec(cleanupCtx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", pub))
	})

	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	return replPool, regPool, table, slot, pub, ledgerPath
}

func b1SpawnChild(t *testing.T, s b1ChildSpec) *childProcess {
	t.Helper()
	env := []string{
		envRealChild + "=" + envValueTrue,
		envURL + "=" + RepmgrConnString,
		envTable + "=" + s.table,
		envSlot + "=" + s.slot,
		envPub + "=" + s.pub,
		envLedger + "=" + s.ledgerPath,
		envTotal + "=" + strconv.Itoa(s.total),
		envBatchSize + "=3", // >0, so the SDK's batch/read-ahead middleware is actually in the loop
		envRun + "=" + strconv.Itoa(s.run),
	}
	if s.haltExpected {
		env = append(env, envHaltExpected+"="+envValueTrue)
	}
	if s.park != "" {
		env = append(env, "PGCHAOS_PARK="+s.park)
	}
	return spawnChildWithEnv(t, env)
}

// b1Baseline drives the child through the snapshot phase (4 seeded rows) and
// one baseline CDC insert. The baseline DML is load-bearing: pgoutput sends
// the RelationMessage lazily on the first DML that uses the table (verified
// 2026-08-29), so this DML pins the pre-ALTER shape — without it, the
// ALTER-triggered relation would be the first sight and NOT drift.
func b1Baseline(t *testing.T, cp *childProcess, regPool *pgxpool.Pool, table string) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()

	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", 4, 60*time.Second)

	_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('baseline')`, table))
	is.NoErr(err)
	cp.waitForCount(t, "ACKED ", 5, 60*time.Second)
}

// b1DriftTrigger runs the DDL and the first DML using the new shape, which
// emits the drift marker (the marker rides that DML's LSN; the DML itself is
// skipped, D4). ddl is a fmt template with two verbs: the table name and the
// column name, e.g. `ALTER TABLE %q ADD COLUMN %s timestamp`. Callers wait
// for DRIFT_PERSISTED/PARKED/HALTED after this.
func b1DriftTrigger(t *testing.T, regPool *pgxpool.Pool, table, column, ddl string) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()

	_, err := regPool.Exec(ctx, fmt.Sprintf(ddl, table, column))
	is.NoErr(err)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('drift-trigger')`, table))
	is.NoErr(err)
}

func b1ReadLedger(t *testing.T, path string) []LedgerEntry {
	t.Helper()
	entries, bad, err := ReadLedger(path)
	is := is.New(t)
	is.NoErr(err)
	is.Equal(len(bad), 0) // no torn/corrupt lines in any B1 scenario
	return entries
}

func b1DriftEntries(t *testing.T, entries []LedgerEntry) []LedgerEntry {
	t.Helper()
	var out []LedgerEntry
	for _, e := range entries {
		if e.Drift {
			out = append(out, e)
		}
	}
	return out
}

func b1DecodePosition(t *testing.T, rawBase64 string) (position.Position, error) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(rawBase64)
	if err != nil {
		return position.Position{}, fmt.Errorf("decode raw position: %w", err)
	}
	return position.ParseSDKPosition(raw)
}

// b1MarkerLSN returns the marker entry's position LSN — the LSN of the first
// DML that used the new shape, which is what the D5 message must report.
func b1MarkerLSN(t *testing.T, marker LedgerEntry) (pglogrepl.LSN, error) {
	t.Helper()
	pos, err := b1DecodePosition(t, marker.RawPosition)
	if err != nil {
		return 0, err
	}
	return pos.LSN()
}

func b1AssertNoGaps(t *testing.T, entries []LedgerEntry) {
	t.Helper()
	is := is.New(t)
	is.Equal(FindGaps(entries), []uint64(nil))
}

// b1AssertNoUnexpectedDups asserts the ledger has no duplicate deliveries.
// The connector resumes AT the checkpoint LSN and the subscription guard
// re-reads-but-skips the record at exactly that LSN, so a normal restart
// never redelivers: a duplicate group with more than one member is always
// unexpected. The one allowance is prevLast (nil when there was no prior
// run): if the run before the resume's last ledgered record was delivered
// twice — exactly twice — that is the harness's own at-least-once tolerance
// for a record that was in flight at a kill boundary (the parent cannot know
// whether the child's ack of the final record reached the slot before the
// kill), so the scenario explicitly blesses it. In B1 scenarios prevLast is
// the marker, whose delivery identity is the skipped boundary DML's LSN; per
// D4 that DML is re-read-but-skipped on every restart, never delivered, so
// the allowance is defensive in the B1 suite.
func b1AssertNoUnexpectedDups(t *testing.T, entries []LedgerEntry, prevLast *LedgerEntry) {
	t.Helper()
	dups := FindDuplicates(entries)
	if len(dups) == 0 {
		return
	}
	if prevLast != nil && len(dups) == 1 && len(dups[0].Seqs) == 2 {
		want := prevLast.Op + "\x00" + prevLast.Table + "\x00" + prevLast.DeliveryKey
		got := dups[0].Op + "\x00" + dups[0].Table + "\x00" + dups[0].DeliveryKey
		if got == want {
			return
		}
	}
	t.Fatalf("unexpected duplicate deliveries: %+v", dups)
}

// b1AssertNoHalt fails the test if the child ever reported HALTED.
func b1AssertNoHalt(t *testing.T, cp *childProcess) {
	t.Helper()
	if _, ok := cp.line("HALTED"); ok {
		t.Fatalf("child halted when it must not\n%s", cp.diagnostics())
	}
}

// TestB1_AC1_AC2_AC8_HaltAndWedgeRegression is acceptance criteria 1, 2, and
// 8 in one scenario:
//
// AC1: halt-on-drift with exactly one marker, the marker's position durable
// (its history records the new shape), and the halt only after the marker is
// acked (the harness acks every record it ledgers, so a halt here already
// implies the marker was acked — the ack-gating itself is proven
// deterministically at the iterator level, TestCDCIterator_DriftHalt_AckGating).
// AC8: the marker rides a post-handoff position, so it carries
// SnapshotLowWatermarkLSN.
// FM10: acking the marker advances the slot's confirmed_flush_lsn to the
// marker's LSN.
// AC2 (the wedge regression): a restart after the halt resumes from the
// marker, the approved shape dedupes, and data flows again — no second halt.
//
// Perturbation proof: disabling drift detection in the connector (the marker
// never emits) makes waitForMarker("DRIFT_PERSISTED") time out — this test
// fails. Perturbation proof for AC2: resuming from an older checkpoint (the
// child resumes from the ledger's last entry; a resume from an earlier entry
// re-derives the drift) makes run 2 halt — this test fails on
// b1AssertNoHalt.
func TestB1_AC1_AC2_AC8_HaltAndWedgeRegression(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	replPool, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	// Run 1: snapshot + baseline CDC, then ALTER + first-new-shape DML ->
	// marker -> HALTED.
	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	cp.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp.waitForMarker(t, "HALTED", 30*time.Second)
	cp.waitExit(t, 30*time.Second)

	entries := b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, nil)
	drift := b1DriftEntries(t, entries)
	is.Equal(len(drift), 1) // AC1: exactly one marker emitted
	marker := drift[0]
	is.Equal(marker.Run, 1)
	is.Equal(marker.Op, "cdc") // the marker is a CDC-mode record (FM9)

	markerPos, err := b1DecodePosition(t, marker.RawPosition)
	is.NoErr(err)
	markerLSN, err := markerPos.LSN()
	is.NoErr(err)
	is.True(markerLSN > 0)
	// AC8: the marker is a post-handoff record, so its position carries the
	// snapshot low-watermark through the handoff.
	is.True(markerPos.SnapshotLowWatermarkLSN != "")
	// The marker's history is durable: the new shape is recorded at the
	// marker's position, which is what makes the restart the approval.
	_, ok := markerPos.LastSchemaVersion("public." + table)
	is.True(ok)

	// FM10: acking the marker advanced confirmed_flush_lsn to the marker LSN.
	// The slot's flush position converges asynchronously: the halted child's
	// teardown sends the final standby status (subscription.sentStandbyDone),
	// but the server needs a moment to process it, so poll — a single read
	// right after HALTED races the wire. A never-arriving confirmation is
	// still a hard failure (the timeout), so this can't pass by luck.
	state, err := ReadSlotState(ctx, replPool, slot)
	is.NoErr(err)
	is.True(!state.Active) // the halted child tore the replication connection down
	deadline := time.Now().Add(10 * time.Second)
	for {
		slotLSN, lerr := pglogrepl.ParseLSN(state.ConfirmedFlushLSN)
		is.NoErr(lerr)
		if slotLSN >= markerLSN {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("confirmed_flush_lsn %s never reached marker LSN %s (teardown standby lost? stderr tail: %s)",
				state.ConfirmedFlushLSN, markerLSN, cp.stderrTail())
		}
		time.Sleep(100 * time.Millisecond)
		state, err = ReadSlotState(ctx, replPool, slot)
		is.NoErr(err)
	}

	// The halt message: D5 coded error with the revert-trap sentence.
	stderr := cp.stderr.String()
	is.True(strings.Contains(stderr, logrepl.ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(stderr, b1HaltTrap))

	// Run 2 (AC2): the restart after the halt must NOT halt again. It resumes
	// from the marker's position; the first relation message (same shape,
	// deduped) is a no-op, the boundary DML at the marker's LSN is re-read
	// from WAL and skipped again by the subscription guard (design doc D4 —
	// it was replaced by the marker, it is never emitted as a row), and the
	// insert made after the approval flows. Exactly one record is delivered:
	// the post-approval insert.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp2.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp2)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	// The marker's delivery identity (the skipped boundary DML's LSN) can
	// never recur — the boundary is re-read-but-skipped on every restart
	// (D4) — so the allowance below is defensive; the real assertion is that
	// nothing ELSE duplicated.
	b1AssertNoUnexpectedDups(t, entries, &marker)
	is.Equal(len(b1DriftEntries(t, entries)), 1) // still exactly one marker: no second marker on the wedge regression
}

// TestB1_AC3_FM1_ApprovalByCrash is AC3's FM1 window: SIGKILL between the
// marker becoming durable (fsynced to the ledger) and its ack. The marker IS
// the durable approval checkpoint, so the restart — which the design doc
// calls the approval — resumes from the marker and never halts again. The
// window is asserted to be OBSERVABLE (the drift entry exists in the ledger
// with its position intact), not prevented.
//
// Perturbation proof: parking at DriftMarkerSeen instead (the FM2 window —
// the marker not yet durable) makes run 2 halt, because the restart resumes
// below the marker; this test fails on b1AssertNoHalt. The window
// classification is what's under test.
func TestB1_AC3_FM1_ApprovalByCrash(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: false,
		park:       fmt.Sprintf("%s:%d", chaospoint.DriftMarkerAppended, 1),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	// The child parks at DriftMarkerAppended: the marker is appended (fsynced)
	// but not yet acked. The park BLOCKS inside chaospoint.Reach, which prints
	// PARKED before the DRIFT_PERSISTED line after it — so PARKED is the wait,
	// and the ledger assertion below proves the fsync actually happened.
	cp.waitForMarker(t, "PARKED", 60*time.Second)
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	drift := b1DriftEntries(t, entries)
	is.Equal(len(drift), 1) // AC3: the FM1 window is observable — the marker exists, durable
	marker := drift[0]
	is.Equal(marker.Run, 1)

	// Run 2: the crash-before-ack restart IS the approval. Resumes from the
	// marker, dedupes the shape, and delivers — no halt.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp2.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp2)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker)
	is.Equal(len(b1DriftEntries(t, entries)), 1) // no second marker: the approval was durable
}

// TestB1_AC3_FM2_CrashBeforeMarkerDurable is AC3's FM2 window: SIGKILL after
// the marker was delivered to the child (it is in the iterator's channel) but
// before it was appended to the ledger. Nothing about the marker is durable,
// so the restart resumes below it, re-derives the drift across the restart,
// and halts again — the operator sees the halt this time, and the restart
// (run 3) is the approval.
//
// Perturbation proof: parking at DriftMarkerAppended instead (the FM1 window)
// makes run 2 resume cleanly and this test's HALTED wait time out — the
// window classification is what's under test.
func TestB1_AC3_FM2_CrashBeforeMarkerDurable(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: false,
		park:       fmt.Sprintf("%s:%d", chaospoint.DriftMarkerSeen, 1),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	cp.waitForMarker(t, "PARKED", 30*time.Second)
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	is.Equal(len(b1DriftEntries(t, entries)), 0) // the marker was never durable
	prevLast := entries[len(entries)-1]          // the resume boundary: the baseline DML

	// Run 2: resumes below the marker, re-derives driftAcrossRestart, halts.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-crash')`, table))
	is.NoErr(err)
	cp2.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp2.waitForMarker(t, "HALTED", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	stderr := cp2.stderr.String()
	is.True(strings.Contains(stderr, logrepl.ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(stderr, b1HaltTrap))

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &prevLast)
	drift2 := b1DriftEntries(t, entries)
	is.Equal(len(drift2), 1) // exactly one marker in the run that halted
	marker2 := drift2[0]
	is.Equal(marker2.Run, 2)

	// Run 3: the restart is the approval — clean resume.
	cp3 := b1SpawnChild(t, b1ChildSpec{
		run: 3, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp3.waitForMarker(t, "RESUME ", 30*time.Second)
	cp3.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp3.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp3.waitForMarker(t, "DONE", 60*time.Second)
	cp3.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp3)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker2)
	is.Equal(len(b1DriftEntries(t, entries)), 1) // no duplicate markers that grow history
}

// TestB1_AC3_FM3_CrashInHandlerBeforeMarkerEmission is AC3's FM3 window,
// parked in PRODUCTION code: chaospoint.DriftVersionRecorded is the first
// statement of emitDriftMarker (source/logrepl/handler.go), so the kill lands
// after the new shape was detected but before any marker record exists. The
// position that carries the new shape is only ever written as part of the
// marker's own persist-before-ack (FM3's "there is no separate position-write
// step"), so nothing about the drift is durable — the restart re-derives it
// (driftAcrossRestart) and halts again, exactly the status quo ante of the
// wedge: no worse, no approval.
//
// This scenario is itself the proof that the chaospoint seam works from the
// production side of the marker path: the park fires inside the connector's
// own handler code, not in the harness.
func TestB1_AC3_FM3_CrashInHandlerBeforeMarkerEmission(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: false,
		park:       fmt.Sprintf("%s:%d", chaospoint.DriftVersionRecorded, 1),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	cp.waitForMarker(t, "PARKED", 30*time.Second)
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	is.Equal(len(b1DriftEntries(t, entries)), 0) // the marker never existed
	prevLast := entries[len(entries)-1]

	// Run 2: re-derives the drift across the restart and halts.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-crash')`, table))
	is.NoErr(err)
	cp2.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp2.waitForMarker(t, "HALTED", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	stderr := cp2.stderr.String()
	is.True(strings.Contains(stderr, logrepl.ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(stderr, b1HaltTrap))

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &prevLast)
	drift2 := b1DriftEntries(t, entries)
	is.Equal(len(drift2), 1)
	is.Equal(drift2[0].Run, 2)

	// Run 3: the restart is the approval — clean resume.
	cp3 := b1SpawnChild(t, b1ChildSpec{
		run: 3, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp3.waitForMarker(t, "RESUME ", 30*time.Second)
	cp3.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp3.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp3.waitForMarker(t, "DONE", 60*time.Second)
	cp3.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp3)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &drift2[0])
	is.Equal(len(b1DriftEntries(t, entries)), 1)
}

// TestB1_AC5_RevertAfterApproval is acceptance criterion 5 (FM4): exactly
// two halt cycles, then a clean resume — with the revert-trap sentence in
// BOTH halt messages. Run 1 halts on the ADD. The operator approves by
// restarting (run 2 resumes cleanly from the marker) and then REVERTS the
// DDL: the revert's relation message diffs against the still-checkpointed
// new shape and halts once more. Run 3 restarts (approval) and resumes.
// Not a loop: each halt is a distinct, operator-initiated decision.
//
// Perturbation proof: if the revert were treated as no drift (e.g. the
// relation set ignoring drops), run 2 would NOT halt — waitForMarker("HALTED")
// times out and this test fails.
func TestB1_AC5_RevertAfterApproval(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	// Run 1: ADD COLUMN halts.
	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	cp.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp.waitForMarker(t, "HALTED", 30*time.Second)
	cp.waitExit(t, 30*time.Second)

	stderr1 := cp.stderr.String()
	is.True(strings.Contains(stderr1, b1HaltTrap)) // trap in halt message 1

	entries := b1ReadLedger(t, ledgerPath)
	marker1 := b1DriftEntries(t, entries)
	is.Equal(len(marker1), 1)

	// Run 2: approval by restart, then the revert — the DROP diffs against the
	// approved shape and halts again.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q DROP COLUMN %s`)

	cp2.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp2.waitForMarker(t, "HALTED", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	stderr2 := cp2.stderr.String()
	is.True(strings.Contains(stderr2, b1HaltTrap)) // trap in halt message 2 (AC5: both)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker1[0])
	drift2 := b1DriftEntries(t, entries)
	is.Equal(len(drift2), 2) // exactly one marker per halt cycle
	is.Equal(drift2[1].Run, 2)

	// Run 3: restart (approval of the reverted shape) resumes cleanly.
	cp3 := b1SpawnChild(t, b1ChildSpec{
		run: 3, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp3.waitForMarker(t, "RESUME ", 30*time.Second)
	cp3.waitForMarker(t, "OPENED", 30*time.Second)
	_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp3.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp3.waitForMarker(t, "DONE", 60*time.Second)
	cp3.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp3)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &drift2[1])
	is.Equal(len(b1DriftEntries(t, entries)), 2) // no third halt cycle
}

// TestB1_AC7_RestartHaltTruthfulMessage is acceptance criterion 7 (FM7): the
// halt message that follows a DDL-while-down restart is TRUTHFUL — it names
// the table, reports real LSNs on both sides of the change, and says the
// change really happened — without inventing anything. Three truths are
// pinned here, all discovered by running the harness:
//
//  1. On a resume, pgoutput does NOT re-send the pre-DDL relation message:
//     the handler's first in-run sight of the table is the NEW shape, so the
//     change classifies as driftAcrossRestart and the message is the
//     hash-only form — both hashes, "changed while the connector was not
//     running", and the DDL-history pointer — with NO column names
//     (verified 2026-08-29 in the harness; the relation message is sent
//     lazily with the first DML, and the resume starts at the last durable
//     checkpoint, past the pre-DDL boundary record).
//  2. The message's "observed at LSN" is the marker's real LSN (the first
//     new-shape DML), never the RelationMessage's WALStart 0. A message
//     saying "observed at LSN 0/0" would be a fabricated position — the
//     connector DID observe the change, at a real WAL position.
//  3. The "last durable shape first seen at LSN" is the OLD shape's real
//     first DML LSN, backfilled from the relation message's WALStart-0
//     placeholder by position.SetFirstSeenLSN (the marker's position carries
//     both shapes, and this test checks the reported LSN against the ledger's
//     first run-1 CDC entry). A "0/0" here would tell an operator nothing.
//
// Perturbation proof: if the D5 error were built at relation-message time
// (with WALStart 0), the NOT-"observed at LSN 0/0" assertion fails —
// verified during development (the first run of this scenario showed exactly
// that bug). If the message fabricated a column diff (the FM7-forbidden
// regression), the NOT-Contains(b1DriftColumn) and Contains(DDL-history
// sentence) assertions fail. If SetFirstSeenLSN were dropped, the "first seen
// at LSN 0/0" assertion fails.
func TestB1_AC7_RestartHaltTruthfulMessage(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	// Run 1: clean run to a checkpoint with the OLD shape. The baseline DML is
	// the first CDC record and the old shape's real first-seen LSN.
	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 5, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	cp.waitForMarker(t, "DONE", 60*time.Second)
	cp.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp)

	// The ALTER happens while the connector is down; no DML follows it, so no
	// relation message exists until the next run's first DML.
	_, err := regPool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN %s timestamp`, table, b1DriftColumn))
	is.NoErr(err)

	// Run 2: the resume starts at the last durable checkpoint, past the
	// pre-DDL boundary record, so pgoutput never re-sends the old shape's
	// relation message — the first sight is the new shape and the halt is the
	// hash-only driftAcrossRestart form (FM7).
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-alter')`, table))
	is.NoErr(err)
	cp2.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp2.waitForMarker(t, "HALTED", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	entries := b1ReadLedger(t, ledgerPath)
	drift := b1DriftEntries(t, entries)
	is.Equal(len(drift), 1)
	markerLSN, err := b1MarkerLSN(t, drift[0])
	is.NoErr(err)

	// The marker's position carries the durable history: both shapes, with the
	// old shape's first-seen backfilled to the first run-1 CDC DML (the
	// baseline insert) — assert what the message must report against what the
	// ledger actually proves.
	markerPos, err := b1DecodePosition(t, drift[0].RawPosition)
	is.NoErr(err)
	versions := markerPos.SchemaHistory["public."+table]
	is.Equal(len(versions), 2) // old shape, then the drifted shape
	oldHash, newHash := versions[0].ColumnSetHash, versions[1].ColumnSetHash
	is.True(oldHash != newHash)

	var firstRun1CDC *LedgerEntry
	for i := range entries {
		if entries[i].Run == 1 && !entries[i].Drift && entries[i].Op == "cdc" {
			firstRun1CDC = &entries[i]
			break
		}
	}
	is.True(firstRun1CDC != nil) // the baseline DML exists
	firstRun1Pos, err := b1DecodePosition(t, firstRun1CDC.RawPosition)
	is.NoErr(err)
	firstRun1LSN, err := firstRun1Pos.LSN()
	is.NoErr(err)
	is.Equal(versions[0].FirstSeenLSN, firstRun1LSN.String()) // SetFirstSeenLSN backfilled the real LSN

	stderr := cp2.stderr.String()
	is.True(strings.Contains(stderr, logrepl.ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(stderr, "public."+table)) // the table is named
	// The FM7 hash-only form: both hashes, the across-restart sentence, and
	// the DDL-history pointer — and NO fabricated column diff.
	is.True(strings.Contains(stderr, "changed while the connector was not running"))
	is.True(strings.Contains(stderr, "schema hash "+oldHash+" -> "+newHash))
	is.True(strings.Contains(stderr, "last durable shape first seen at LSN "+firstRun1LSN.String()))
	is.True(strings.Contains(stderr, "Compare against your DDL history for the exact columns"))
	is.True(!strings.Contains(stderr, b1DriftColumn)) // hash-only: no invented column names
	is.True(strings.Contains(stderr, b1HaltTrap))
	is.True(strings.Contains(stderr, "observed at LSN "+markerLSN.String())) // the marker's real LSN
	is.True(!strings.Contains(stderr, "observed at LSN 0/0"))                // never a fabricated 0/0 position
	is.True(!strings.Contains(stderr, "first seen at LSN 0/0"))              // the old shape's first-seen is real

	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, firstRun1CDC)

	// Run 3: approval by restart — clean resume.
	cp3 := b1SpawnChild(t, b1ChildSpec{
		run: 3, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp3.waitForMarker(t, "RESUME ", 30*time.Second)
	cp3.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp3.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp3.waitForMarker(t, "DONE", 60*time.Second)
	cp3.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp3)
}

// TestB1_AC9_StackedDDLBetweenSightingAndAck is acceptance criterion 9 (FM8):
// a second DDL before the first marker is acked records the second shape but
// emits NO second marker — exactly one marker per halt — and the restart
// halts again for the second shape (no silent admission of the second DDL).
// Run 1 parks at DriftMarkerAppended (first marker durable, unacked, killed);
// the second ALTER lands while the connector is down; run 2 resumes from the
// first marker, sees the second shape across the restart, and halts; run 3
// approves and resumes.
//
// Perturbation proof: if the stacked second shape were silently admitted (no
// drift on the restart), run 2's HALTED never arrives and this test times
// out.
func TestB1_AC9_StackedDDLBetweenSightingAndAck(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	// Run 1: first DDL (column101), marker durable at DriftMarkerAppended,
	// killed before the ack.
	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 7, haltExpected: false,
		park:       fmt.Sprintf("%s:%d", chaospoint.DriftMarkerAppended, 1),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)
	b1DriftTrigger(t, regPool, table, b1DriftColumn, `ALTER TABLE %q ADD COLUMN %s timestamp`)

	// Park blocks inside chaospoint.Reach before DRIFT_PERSISTED prints — PARKED
	// is the wait; the ledger assertion below proves the marker's fsync.
	cp.waitForMarker(t, "PARKED", 60*time.Second)
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	drift1 := b1DriftEntries(t, entries)
	is.Equal(len(drift1), 1) // one marker, exactly, for the first DDL
	marker1 := drift1[0]

	// Second DDL while the connector is down (before the first marker's ack).
	_, err := regPool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN column102 timestamp`, table))
	is.NoErr(err)

	// Run 2: the second shape halts again on the restart.
	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 7, haltExpected: true,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-second-ddl')`, table))
	is.NoErr(err)
	cp2.waitForMarker(t, "DRIFT_PERSISTED", 60*time.Second)
	cp2.waitForMarker(t, "HALTED", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	stderr := cp2.stderr.String()
	is.True(strings.Contains(stderr, logrepl.ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(stderr, b1HaltTrap))

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker1)
	drift := b1DriftEntries(t, entries)
	is.Equal(len(drift), 2) // one marker in run 2 as well — never two for the stacked pair
	marker2 := drift[1]
	is.Equal(marker2.Run, 2)

	// Run 3: approval by restart — clean resume.
	cp3 := b1SpawnChild(t, b1ChildSpec{
		run: 3, total: 1, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp3.waitForMarker(t, "RESUME ", 30*time.Second)
	cp3.waitForMarker(t, "OPENED", 30*time.Second)
	_, err = regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post-approval')`, table))
	is.NoErr(err)
	cp3.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp3.waitForMarker(t, "DONE", 60*time.Second)
	cp3.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp3)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker2)
	is.Equal(len(b1DriftEntries(t, entries)), 2) // no third marker for the second shape
}
