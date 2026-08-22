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
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
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
// It asserts a perfectly clean ledger: no corrupt lines, no gap or
// duplicate DELIVERY (by position - DeliveryKey), snapshot records strictly
// before CDC records, none marked resumed (this is an uninterrupted single
// run) - AND, independently, that the exact set of ROW keys observed
// (LedgerEntry.Key, the connector's own "id" per record) is precisely the
// `seeded` seed keys plus the `extra` keys inserted mid-run, each exactly
// once. That second check is load-bearing: DeliveryKey is derived from the
// record's position, so a connector that redelivered row 1 under four
// distinct snapshot cursors and never delivered rows 2-4 would still pass
// every position-based check above it - a bug this test would otherwise
// miss entirely.
// NOTE: .github/workflows/chaos.yml greps its own output for this exact test
// name to prove the conduitchaos-tagged suite actually RAN - without it, a
// dropped or typo'd build tag makes `go test` compile the untagged variant,
// run only the ledger unit tests, and report ok while proving nothing.
// Renaming this function disarms that guard silently: the build still
// compiles, chaos-build still passes, and the chaos job fails with a message
// blaming the build tag. Update the workflow in the same commit as any rename.
func TestSmoke_NoKillOpenReadAckTeardown(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	replPool := requireChaosStack(t) // INFRA-fatal preflight; also usable for slot introspection below

	regPool := test.ConnectPool(ctx, t, RegularConnString)

	// randChaosName(), not test.RandomIdentifier(t): the latter's suffix is
	// time.Now().UnixMicro()%1000, only 1000 distinct values - the same
	// collision risk finding F-8 flagged for slots/publications under
	// nightly's -count=3 applies just as much to the table name, and slots
	// and publications were moved off it for exactly that reason. No
	// exemption for the table here.
	table, err := randChaosName()
	is.NoErr(err)
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
	gotKeys := make([]string, 0, len(entries))
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
		default:
			t.Fatalf("unexpected op %q at seq %d", e.Op, e.Seq)
		}
		gotKeys = append(gotKeys, e.Key)
	}
	is.Equal(snapshotCount, seeded)
	is.Equal(cdcCount, extra) // cdcCount > 0 already proves a CDC delivery was observed after the snapshot loop above

	// The row-identity check the doc comment above promises: the exact set
	// of keys delivered must be precisely ids 1..total (the table's
	// bigserial primary key, starting at 1 on this fresh table - `seeded`
	// then `extra` rows, inserted in that order, nothing else ever writes
	// to it) - each exactly once. This is independent of, and stronger
	// than, the position-based FindGaps/FindDuplicates checks above: those
	// only prove no DELIVERY POSITION repeated or went missing, not that
	// every ROW was actually seen.
	wantKeys := make([]string, 0, total)
	for id := 1; id <= total; id++ {
		wantKeys = append(wantKeys, string(opencdc.StructuredData{"id": int64(id)}.Bytes()))
	}
	sort.Strings(wantKeys)
	sort.Strings(gotKeys)
	is.Equal(gotKeys, wantKeys)

	// Parent-side slot introspection (pgstate.go) actually works, and
	// reflects a cleanly torn-down child: no active replication connection
	// left behind.
	state, err := ReadSlotState(ctx, replPool, slot)
	is.NoErr(err)
	is.Equal(state.Name, slot)
	is.True(!state.Active)
}
