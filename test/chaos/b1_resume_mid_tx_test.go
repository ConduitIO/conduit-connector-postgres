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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/matryer/is"
)

// TestB1_335_ApprovalByCrashMidTransaction is #335 on the kill harness. The
// drift is inside a transaction: INSERT p1, ALTER TABLE ... ADD COLUMN,
// INSERT p2, p3, p4. Run 1 delivers p1, then the marker in p2's place. The
// child is SIGKILLed after the marker is durable and before its ack (the FM1
// window: the crash is the approval). The marker's checkpoint sits inside the
// transaction and already records the post-ALTER shape, so the restart makes
// Postgres re-send the transaction from its start, beginning with the
// pre-ALTER Relation message ahead of p1, which the resume point skips.
//
// The restart must deliver exactly p3 and p4: no second marker, no halt. Before
// the fix, the replayed pre-ALTER shape was decided on arrival as a change
// made while the connector was down, so run 2 emitted a second marker in p3's
// place and halted; this test then fails waiting for DONE.
func TestB1_335_ApprovalByCrashMidTransaction(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	cp := b1SpawnChild(t, b1ChildSpec{
		run: 1, total: 10, haltExpected: false,
		park:       fmt.Sprintf("%s:%d", chaospoint.DriftMarkerAppended, 1),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	b1Baseline(t, cp, regPool, table)

	tx, err := regPool.Begin(ctx)
	is.NoErr(err)
	ids := map[string]int64{}
	insert := func(v string) {
		var id int64
		is.NoErr(tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('%s') RETURNING id`, table, v)).Scan(&id))
		ids[v] = id
	}
	insert("p1")
	_, err = tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN %s timestamp`, table, b1DriftColumn))
	is.NoErr(err)
	insert("p2")
	insert("p3")
	insert("p4")
	is.NoErr(tx.Commit(ctx))

	cp.waitForMarker(t, "PARKED", 60*time.Second) // marker durable, not acked
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	drift := b1DriftEntries(t, entries)
	is.Equal(len(drift), 1)
	marker := drift[0]
	is.Equal(len(entries), 7) // 4 snapshot rows, baseline, p1, marker
	is.Equal(entries[5].Key, string(opencdc.StructuredData{"id": ids["p1"]}.Bytes()))
	markerPos, err := b1DecodePosition(t, marker.RawPosition)
	is.NoErr(err)
	is.True(markerPos.TxSeq > 1) // the checkpoint is inside the transaction, after p1

	cp2 := b1SpawnChild(t, b1ChildSpec{
		run: 2, total: 2, haltExpected: false,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	b1AssertNoHalt(t, cp2)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &marker)
	is.Equal(len(b1DriftEntries(t, entries)), 1) // decided once: no second marker
	is.Equal(len(entries), 9)
	is.Equal(entries[7].Run, 2)
	is.Equal(entries[7].Key, string(opencdc.StructuredData{"id": ids["p3"]}.Bytes()))
	is.Equal(entries[8].Key, string(opencdc.StructuredData{"id": ids["p4"]}.Bytes()))
}
