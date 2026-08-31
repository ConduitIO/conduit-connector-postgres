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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

// TestCDCIterator_DriftHalt_AckGating is AC4 on the iterator: the marker is
// served, the halt does NOT fire until the marker's position is acked (D3), and
// the ack turns the next read into the terminal coded error.
//
// Perturbation proof: deleting the maybeArmDriftHalt call in CDCIterator.Ack
// leaves the third read blocking forever and the second read erroring on the
// timeout context — this test fails on the "must block" assertion.
func TestCDCIterator_DriftHalt_AckGating(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	i := testCDCIterator(ctx, t, pool, table, true) // halt is the default policy
	<-i.sub.Ready()

	// Baseline DML first: pgoutput sends the RelationMessage lazily (on the
	// first DML that uses the table), so a DML before the ALTER fixes the
	// initial shape; otherwise the ALTER-triggered relation would be first
	// sight and NOT drift.
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
		VALUES (29, 'baseline', 1, false, 1.1, 2)`, table))
	is.NoErr(err)
	rr, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.True(len(rr) == 1)

	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN column101 timestamp;`, table))
	is.NoErr(err)

	// The first DML with the new shape triggers the marker's emission (pgoutput
	// sends the RelationMessage with WALStart 0, so the marker rides the DML's
	// LSN); the DML itself is skipped (D4).
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
		VALUES (30, 'test-1', 100, false, 12.3, 14)`, table))
	is.NoErr(err)

	// The marker is served to the engine.
	rr, err = i.NextN(ctx, 1)
	is.NoErr(err)
	is.True(len(rr) == 1)
	is.Equal(rr[0].Metadata[MetadataSchemaDrift], "true")

	// Marker not yet acked: NextN must block, NOT error — surfacing the halt
	// here would race the engine's ack ordering (the SDK read-ahead means the
	// engine has not acked yet) and could strand the approval checkpoint.
	blockCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = i.NextN(blockCtx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected NextN to block until timeout, got: %v", err)
	}

	// Acking the marker arms the halt (D3).
	is.NoErr(i.Ack(ctx, rr[0].Position))
	_, err = i.NextN(ctx, 1)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt))
}

// TestCDCIterator_DriftHalt_SubStopRace is AC4 on the F6 race: the subscription
// dies while the marker is queued in batchesCh. The pending-branch select must
// prefer the marker over sub.Done — a dead subscription may never erase the
// operator's approval checkpoint, which is the entire point of the escape
// hatch.
//
// Determinism: the marker flushes on the 1s handler tick, so after a 1.5s sleep
// the marker is provably in batchesCh; only then is the subscription stopped,
// so both select cases are ready and the marker preference is what's under
// test. Perturbation proof: adding the sub.Done case to the pending branch
// makes the select choose randomly and this test fails nondeterministically.
func TestCDCIterator_DriftHalt_SubStopRace(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	i := testCDCIterator(ctx, t, pool, table, true)
	<-i.sub.Ready()

	// Baseline DML first (see TestCDCIterator_DriftHalt_AckGating).
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
		VALUES (29, 'baseline', 1, false, 1.1, 2)`, table))
	is.NoErr(err)
	rr, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.True(len(rr) == 1)

	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN column101 timestamp;`, table))
	is.NoErr(err)

	// First DML with the new shape emits the marker (and is itself skipped).
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
		VALUES (30, 'test-1', 100, false, 12.3, 14)`, table))
	is.NoErr(err)

	// Wait past the 1s flush interval so the marker batch is in batchesCh.
	time.Sleep(1500 * time.Millisecond)

	// Subscription dies while the marker is queued. Wait for it to actually
	// die, so both select cases (sub.Done and the marker batch) are genuinely
	// ready when NextN runs — the marker preference is what's under test.
	i.sub.Stop()
	select {
	case <-i.sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not stop")
	}

	// The marker is still served, and its ack still arms the halt.
	rr, err = i.NextN(ctx, 1)
	is.NoErr(err)
	is.True(len(rr) == 1)
	is.Equal(rr[0].Metadata[MetadataSchemaDrift], "true")

	is.NoErr(i.Ack(ctx, rr[0].Position))
	_, err = i.NextN(ctx, 1)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt))
}
