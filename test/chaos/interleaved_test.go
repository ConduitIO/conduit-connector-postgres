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

// TestInterleavedTx_KillBeforeLaterCommittedRecordAck is #331 on the kill
// harness. T1 inserts first and commits last; T2 inserts and commits in
// between. The stream delivers T2's row, then T1's at a lower change LSN.
// The child ledgers and acks T2's record, then parks on T1's record
// (delivered, not durable, not acked) and is SIGKILLed. The restart resumes
// from T2's checkpoint and must deliver T1's row.
//
// Before the fix the resume guard compared T1's change LSN against the
// checkpoint and dropped it, so run 2 never reached DONE: the record was
// lost.
func TestInterleavedTx_KillBeforeLaterCommittedRecordAck(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	_, regPool, table, slot, pub, ledgerPath := b1Setup(t)

	const t1Record = 6 // 4 seeded snapshot rows, then T2's row, then T1's
	cp := b2SpawnChild(t, b2ChildSpec{
		run: 1, total: t1Record,
		park:       fmt.Sprintf("%s:%d", chaospoint.RecordSeen, t1Record),
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", 4, 60*time.Second)

	tx1, err := regPool.Begin(ctx)
	is.NoErr(err)
	var t1ID, t2ID int64
	is.NoErr(tx1.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('t1-long') RETURNING id`, table)).Scan(&t1ID))
	is.NoErr(regPool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('t2-short') RETURNING id`, table)).Scan(&t2ID))
	is.NoErr(tx1.Commit(ctx))

	cp.waitForCount(t, "ACKED ", 5, 60*time.Second) // T2's record durable and acked
	cp.waitForMarker(t, "PARKED", 60*time.Second)   // T1's record in flight
	cp.sigkill(t)

	entries := b1ReadLedger(t, ledgerPath)
	is.Equal(len(entries), 5)
	t2Entry := entries[4]
	is.Equal(t2Entry.Key, string(opencdc.StructuredData{"id": t2ID}.Bytes()))

	cp2 := b2SpawnChild(t, b2ChildSpec{
		run: 2, total: 1,
		ledgerPath: ledgerPath, table: table, slot: slot, pub: pub,
	})
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "DONE", 30*time.Second)
	cp2.waitExit(t, 30*time.Second)

	entries = b1ReadLedger(t, ledgerPath)
	b1AssertNoGaps(t, entries)
	b1AssertNoUnexpectedDups(t, entries, &t2Entry)
	is.Equal(len(entries), 6)
	is.Equal(entries[5].Run, 2)
	is.Equal(entries[5].Key, string(opencdc.StructuredData{"id": t1ID}.Bytes())) // T1's row, not lost
}
