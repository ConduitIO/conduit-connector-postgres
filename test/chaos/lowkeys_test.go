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
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/matryer/is"
)

// TestSnapshot_KillAfterAckingANegativeKey: the snapshot of a table keyed
// from -3 is killed after the rows keyed -3 and -1 are durable and acked,
// with the row keyed 0 delivered but not yet durable. The restart resumes
// after -1 and delivers 0 to 4. Every row is delivered exactly once.
//
// v0.14.0 to v0.19.0 started a new snapshot at "key > 0" and never read the
// rows keyed -3, -1 and 0
// (docs/design-documents/20261102-snapshot-keys-at-or-below-zero.md).
func TestSnapshot_KillAfterAckingANegativeKey(t *testing.T) {
	is := is.New(t)
	_, regPool, table, slot, pub, ledgerPath := b1Setup(t) // seeds ids 1 to 4
	_, err := regPool.Exec(context.Background(), fmt.Sprintf(`INSERT INTO %q (id, column1) VALUES (-3, 'a'), (-1, 'b'), (0, 'c')`, table))
	is.NoErr(err)

	spawn := func(run, total int, park string) *childProcess {
		env := []string{
			envRealChild + "=" + envValueTrue,
			envURL + "=" + RepmgrConnString,
			envTable + "=" + table,
			envSlot + "=" + slot,
			envPub + "=" + pub,
			envLedger + "=" + ledgerPath,
			envTotal + "=" + strconv.Itoa(total),
			// 1: snapshot.fetchSize is 1 too, so every row is its own
			// FETCH and the kill lands between two pages.
			envBatchSize + "=1",
			envRun + "=" + strconv.Itoa(run),
		}
		if park != "" {
			env = append(env, "PGCHAOS_PARK="+park)
		}
		return spawnChildWithEnv(t, env)
	}

	// Park on the third record (key 0): delivered, not durable, not acked.
	cp := spawn(1, 7, fmt.Sprintf("%s:%d", chaospoint.RecordSeen, 3))
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", 2, 60*time.Second)
	cp.waitForMarker(t, "PARKED", 60*time.Second)
	cp.sigkill(t)
	is.Equal(ledgerKeys(t, ledgerPath), keysOf(-3, -1))

	cp2 := spawn(2, 5, "")
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)

	entries := b1ReadLedger(t, ledgerPath)
	is.Equal(ledgerKeys(t, ledgerPath), keysOf(-3, -1, 0, 1, 2, 3, 4))
	is.Equal(FindGaps(entries), []uint64(nil))
	is.Equal(FindDuplicates(entries), []Duplicate(nil))
	for _, e := range entries {
		is.Equal(e.Op, "snapshot")
		is.Equal(e.Resumed, e.Run == 2)
	}
}
