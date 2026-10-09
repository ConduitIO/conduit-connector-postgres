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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// TestDriftHalt_InterleavedAckDoesNotArmBeforeMarker is the #334 review
// finding on the drift halt's arming rule. The halt must arm only when the
// marker itself (or something after it in stream order) is acked. It used to
// compare change LSNs, and with interleaved transactions a change delivered
// BEFORE the marker can carry a HIGHER LSN than the marker:
//
//	tx A: BEGIN; ALTER TABLE a ...; INSERT a1 (LSN x)   -- the marker rides a1
//	tx B: INSERT b1 into another published table (LSN y > x); COMMIT
//	tx A: COMMIT
//
// B commits first, so b1 is delivered first, then A's marker at the lower LSN
// x. Acking b1 (lsn y >= x) armed the halt before the marker was delivered,
// and the next NextN returned postgres.schema_drift.halt: the marker, and with
// it the operator's approval checkpoint, never reached the engine
// (invariant 1: the halt is acked-gated, never sighting-gated).
//
// Perturbation proof: with the old `lsn < markerLSN` comparison in
// maybeArmDriftHalt, acking b1 arms the halt, so this fails at the "not armed"
// assertion (and, without it, at the NextN that returns the halt error instead
// of the marker).
func TestDriftHalt_InterleavedAckDoesNotArmBeforeMarker(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	a := test.SetupEmptyTestTable(ctx, t, pool)
	b := test.SetupEmptyTestTable(ctx, t, pool)

	it, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:          []string{a, b},
		TableKeys:       map[string]string{a: "id", b: "id"},
		PublicationName: a,
		SlotName:        a,
		BatchSize:       1,
		// halt is the default policy
	})
	is.NoErr(err)
	it.sub.StatusTimeout = time.Second
	is.NoErr(it.StartSubscriber(ctx))
	t.Cleanup(func() {
		_ = it.Teardown(ctx)
		_ = Cleanup(ctx, CleanupConfig{URL: pool.Config().ConnString(), SlotName: a, PublicationName: a})
	})

	// Baseline DML on both tables first: pgoutput sends a RelationMessage
	// lazily, and a shape seen for the first time is not drift.
	for _, tbl := range []string{a, b} {
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('base')`, tbl))
		is.NoErr(err)
	}
	for _, r := range readN(ctx, t, it, 2, 10*time.Second) {
		is.NoErr(it.Ack(ctx, r.Position))
	}

	txA, err := pool.Begin(ctx)
	is.NoErr(err)
	_, err = txA.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, a))
	is.NoErr(err)
	_, err = txA.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('a1')`, a))
	is.NoErr(err)
	// b1 is written after a1 (higher LSN) and commits before tx A does.
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('b1')`, b))
	is.NoErr(err)
	is.NoErr(txA.Commit(ctx))

	first := readN(ctx, t, it, 1, 10*time.Second)
	is.Equal(column1(t, first[0]), "b1") // tx B committed first

	// Wait until the subscription has emitted the marker, so the ack below
	// runs against a pending marker (otherwise nothing could arm).
	deadline := time.Now().Add(10 * time.Second)
	for !it.handler.driftMarkerPending() {
		if time.Now().After(deadline) {
			t.Fatal("drift marker was never emitted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Ack b1: its LSN is above the marker's, its change key is below.
	is.NoErr(it.Ack(ctx, first[0].Position))
	is.True(!it.handler.driftHaltArmed.Load()) // not armed by an earlier change

	// The marker must be delivered, not preempted by the halt error.
	nctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	got, err := it.NextN(nctx, 1)
	is.NoErr(err) // the bug: postgres.schema_drift.halt here
	is.Equal(len(got), 1)
	is.Equal(got[0].Metadata[MetadataSchemaDrift], "true")

	// The marker's own ack arms the halt, and the next read surfaces it.
	is.NoErr(it.Ack(ctx, got[0].Position))
	is.True(it.handler.driftHaltArmed.Load())
	_, err = it.NextN(nctx, 1)
	if err == nil || !strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt) {
		t.Fatalf("expected %s after the marker's ack, got: %v", ErrorCodeSchemaDriftHalt, err)
	}
}

// TestInterleavedTx_ResumeFromHotfixShapedPosition is the upgrade test for
// positions written by the v0.14.x hotfix (v0.14.3): format version 2 with the
// change key (tx_commit_lsn, tx_seq) and none of the DBZ-3 fields (no
// schema_history, no snapshot_low_watermark_lsn); see
// source/position/testdata/v0.14.x-hotfix-cdc.json. Three overlapping
// transactions, a rolled-back savepoint and TOASTed values deliver six
// records. Checkpoint after each prefix, rewrite the checkpoint to the hotfix
// shape, restart under the default halt policy and expect exactly the
// remaining records: no schema-drift marker (a position without a history is
// first sight of every shape, not drift), no duplicate, no loss.
func TestInterleavedTx_ResumeFromHotfixShapedPosition(t *testing.T) {
	big := strings.Repeat("x", 200000) // TOASTed
	for k := 1; k <= 5; k++ {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)
			run1 := newInterleaveCombined(ctx, t, pool, table, nil)

			ins := func(tx pgx.Tx, v string) {
				payload := "{}"
				if len(v)%2 == 1 {
					payload = fmt.Sprintf(`{"b":"%s"}`, big)
				}
				_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1, column6) VALUES ($1, $2)`, table), v, payload)
				is.NoErr(err)
			}
			t1, err := pool.Begin(ctx)
			is.NoErr(err)
			t2, err := pool.Begin(ctx)
			is.NoErr(err)
			t3, err := pool.Begin(ctx)
			is.NoErr(err)
			ins(t1, "a1")
			_, err = t1.Exec(ctx, "SAVEPOINT s")
			is.NoErr(err)
			ins(t1, "a-rolledback")
			_, err = t1.Exec(ctx, "ROLLBACK TO SAVEPOINT s")
			is.NoErr(err)
			ins(t2, "b1")
			ins(t3, "c1")
			ins(t1, "a2x")
			is.NoErr(t3.Commit(ctx))
			ins(t2, "b2x")
			is.NoErr(t2.Commit(ctx))
			ins(t1, "a3")
			is.NoErr(t1.Commit(ctx))

			recs := readN(ctx, t, run1, 6, 15*time.Second)
			all := make([]string, 0, len(recs))
			for _, r := range recs {
				all = append(all, column1(t, r))
			}
			is.Equal(all, []string{"c1", "b1", "b2x", "a1", "a2x", "a3"})
			for i := 0; i < k; i++ {
				is.NoErr(run1.Ack(ctx, recs[i].Position))
			}
			_ = run1.Teardown(ctx)

			// Reduce the checkpoint to what the hotfix wrote.
			cp := posOf(t, recs[k-1])
			hotfix, err := json.Marshal(map[string]any{
				"version":       2,
				"type":          int(cp.Type),
				"last_lsn":      cp.LastLSN,
				"tx_commit_lsn": cp.TxCommitLSN,
				"tx_seq":        cp.TxSeq,
			})
			is.NoErr(err)
			is.True(!strings.Contains(string(hotfix), "schema_history"))

			run2 := newInterleaveCombined(ctx, t, pool, table, hotfix)
			defer func() { _ = run2.Teardown(ctx) }()
			got := drainColumn1(ctx, t, run2, 4*time.Second)
			is.Equal(got, all[k:]) // exactly the rest: no marker, duplicate or loss
		})
	}
}
