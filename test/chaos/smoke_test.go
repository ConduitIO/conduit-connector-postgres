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

package chaos

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

// TestSmoke_NoKillOpenReadAckTeardown is B0-2's headline result (harness
// plan finding F-1): a full Open -> repeated ReadN -> ledger -> Ack ->
// Teardown run against real Postgres, through the snapshot->CDC handoff,
// with NO kill anywhere. It ships before any SIGKILL scenario specifically
// so a broken supervision model - Ack, the SDK batch middleware's
// read-ahead goroutine, or the schema handling, none of which
// source_integration_test.go's existing ParseConfig+Open+ReadN(ctx,1)
// coverage touches - fails loud here, not silently three files into a kill
// scenario that assumed it all just worked.
//
// It asserts a perfectly clean ledger: every seeded row and every row
// inserted mid-run is delivered exactly once, in order, snapshot records
// first then CDC records, none marked resumed (this is an uninterrupted
// single run), no corrupt lines.
func TestSmoke_NoKillOpenReadAckTeardown(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	replPool := requireChaosStack(t) // INFRA-fatal preflight; also usable for slot introspection below

	regPool := test.ConnectPool(ctx, t, RegularConnString)

	table := test.RandomIdentifier(t)
	test.SetupTestTableWithName(ctx, t, regPool, table) // creates the table AND seeds 4 rows

	slot, err := randChaosName()
	is.NoErr(err)
	pub, err := randChaosName()
	is.NoErr(err)

	t.Cleanup(func() {
		// Belt-and-braces direct cleanup on top of TestMain's pre/post
		// sweep (reaper.go) - a passing test should never rely on the
		// sweep to make itself green.
		cleanupCtx := context.Background()
		_, _ = replPool.Exec(cleanupCtx, "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name=$1", slot)
		_, _ = replPool.Exec(cleanupCtx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", pub))
	})

	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")

	const (
		seeded = 4 // rows test.SetupTestTableWithName inserts
		extra  = 3 // rows inserted mid-run, to exercise the CDC path
		total  = seeded + extra
	)

	cp := spawnChildWithEnv(t, []string{
		envRealChild + "=" + envValueTrue,
		envURL + "=" + RepmgrConnString,
		envTable + "=" + table,
		envSlot + "=" + slot,
		envPub + "=" + pub,
		envLedger + "=" + ledgerPath,
		envTotal + "=" + strconv.Itoa(total),
		envBatchSize + "=3", // >0, so the SDK's batch/read-ahead middleware is actually in the loop
		envRun + "=1",
	})

	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", seeded, 60*time.Second)

	// Prove the CDC path fires too: insert more rows directly, bypassing
	// the connector entirely, only once we know the snapshot phase itself
	// is done (all `seeded` rows acked).
	for i := 0; i < extra; i++ {
		_, err := regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ($1)`, table), fmt.Sprintf("extra-%d", i))
		is.NoErr(err)
	}

	cp.waitForMarker(t, "DONE", 60*time.Second)
	cp.waitExit(t, 30*time.Second)

	entries, bad, err := ReadLedger(ledgerPath)
	is.NoErr(err)
	is.Equal(len(bad), 0) // no torn/corrupt lines on a clean, uninterrupted run
	is.Equal(len(entries), total)
	is.Equal(FindGaps(entries), []uint64(nil))
	is.Equal(FindDuplicates(entries), []Duplicate(nil))

	var snapshotCount, cdcCount int
	sawCDCAfterSnapshot := false
	for _, e := range entries {
		is.Equal(e.Run, 1)
		is.Equal(e.Table, table)
		is.Equal(e.Resumed, false) // single uninterrupted run: never resumed

		switch e.Op {
		case "snapshot":
			if cdcCount > 0 {
				t.Fatalf("snapshot delivery (seq %d) observed after a CDC delivery", e.Seq)
			}
			snapshotCount++
		case "cdc":
			cdcCount++
			sawCDCAfterSnapshot = true
		default:
			t.Fatalf("unexpected op %q at seq %d", e.Op, e.Seq)
		}
	}
	is.Equal(snapshotCount, seeded)
	is.Equal(cdcCount, extra)
	is.True(sawCDCAfterSnapshot)

	// Parent-side slot introspection (pgstate.go) actually works, and
	// reflects a cleanly torn-down child: no active replication connection
	// left behind.
	state, err := ReadSlotState(ctx, replPool, slot)
	is.NoErr(err)
	is.Equal(state.Name, slot)
	is.True(!state.Active)
}
