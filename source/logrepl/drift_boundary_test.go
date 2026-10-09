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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// Tests for #338: the approving restart after a drift halt resumes AT the
// change that decided the drift, so that row is delivered (once, decoded
// against the approved shape) instead of being dropped.

type boundaryCase struct {
	name   string
	policy SchemaDriftPolicy
	// baseline rows are inserted, one transaction each, before the drift so
	// the table's shape is known. Run 1 delivers them.
	baseline []string
	// tx runs in one transaction after the baseline.
	tx func(ctx context.Context, t *testing.T, tx pgx.Tx, table string)
	// before are the rows of tx that run 1 delivers ahead of the marker.
	before []string
	// boundary and after are what the approving restart must deliver: the
	// deciding change first, then the rest of the stream.
	want []string
	// reformatAck acks the marker as a semantically equal but re-marshaled
	// position (indented, keys sorted), as an engine or middleware might.
	reformatAck bool
}

func boundaryCases() []boundaryCase {
	exec := func(t *testing.T, tx pgx.Tx, q string) {
		t.Helper()
		if _, err := tx.Exec(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return []boundaryCase{
		{
			// The deciding change is the third change of its transaction,
			// followed by COPY rows that share one LSN.
			name: "halt_midtx_copy", policy: SchemaDriftPolicyHalt,
			before: []string{"p1"}, want: []string{"p2", "c1", "c2", "c3", "p3"},
			tx: func(ctx context.Context, t *testing.T, tx pgx.Tx, table string) {
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p1')`, table))
				exec(t, tx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
				if _, err := tx.CopyFrom(ctx, pgx.Identifier{table}, []string{"column1"},
					pgx.CopyFromRows([][]any{{"c1"}, {"c2"}, {"c3"}})); err != nil {
					t.Fatal(err)
				}
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p3')`, table))
			},
		},
		{
			// The deciding change is the first and only change of its
			// transaction: "one below" is the previous commit, and the
			// marker is the last change of the stream, so the slot's flush
			// gate has nothing after it to stay closed on.
			name: "halt_first_only", policy: SchemaDriftPolicyHalt,
			baseline: []string{"b1"}, want: []string{"p2"},
			tx: func(_ context.Context, t *testing.T, tx pgx.Tx, table string) {
				exec(t, tx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
			},
		},
		{
			name: "halt_first_more", policy: SchemaDriftPolicyHalt,
			baseline: []string{"b1"}, want: []string{"p2", "p3"},
			tx: func(_ context.Context, t *testing.T, tx pgx.Tx, table string) {
				exec(t, tx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p3')`, table))
			},
		},
		{
			// A narrowing change halts under evolve too.
			name: "evolve_midtx", policy: SchemaDriftPolicyEvolve,
			before: []string{"p1"}, want: []string{"p2", "p3"},
			tx: func(_ context.Context, t *testing.T, tx pgx.Tx, table string) {
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p1')`, table))
				exec(t, tx, fmt.Sprintf(`ALTER TABLE %q DROP COLUMN column2`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p3')`, table))
			},
		},
		{
			name: "evolve_first", policy: SchemaDriftPolicyEvolve,
			baseline: []string{"b1"}, want: []string{"p2"},
			tx: func(_ context.Context, t *testing.T, tx pgx.Tx, table string) {
				exec(t, tx, fmt.Sprintf(`ALTER TABLE %q DROP COLUMN column2`, table))
				exec(t, tx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
			},
		},
	}
}

// boundaryRun1 runs the stream up to the halt and returns the marker. It
// delivers the baseline and the rows ahead of the marker, checks that acking
// the record just before the marker does NOT arm the halt (the marker's
// position key is shared with that record's), then acks the marker and checks
// the halt and the slot. It also returns the record delivered just before the
// marker.
func boundaryRun1(ctx context.Context, t *testing.T, tc boundaryCase, table string) (marker, prev opencdc.Record) {
	t.Helper()
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)

	run1 := newInterleaveCombinedPolicy(ctx, t, pool, table, nil, tc.policy)
	for _, b := range tc.baseline {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('%s')`, table, b))
		is.NoErr(err)
	}
	tx, err := pool.Begin(ctx)
	is.NoErr(err)
	tc.tx(ctx, t, tx, table)
	is.NoErr(tx.Commit(ctx))

	pre := append(append([]string{}, tc.baseline...), tc.before...)
	recs := readN(ctx, t, run1, len(pre)+1, 15*time.Second)
	for i, want := range pre {
		is.Equal(column1(t, recs[i]), want)
	}
	marker = recs[len(pre)]
	is.Equal(marker.Metadata[MetadataSchemaDrift], "true") // the marker replaces the deciding change (D4)

	// Acking everything before the marker must not surface the halt: the
	// marker is not yet acked, so the approval checkpoint has not reached the
	// engine (invariant 1). The marker's position key equals the key of the
	// record before it in the mid-transaction cases.
	for _, r := range recs[:len(pre)] {
		is.NoErr(run1.Ack(ctx, r.Position))
	}
	blockCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	_, err = run1.NextN(blockCtx, 1)
	cancel()
	is.True(err != nil && !strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt)) // blocked, not halted

	ackPos := marker.Position
	if tc.reformatAck {
		ackPos = reformatPosition(t, marker.Position)
		is.True(string(ackPos) != string(marker.Position))
	}
	is.NoErr(run1.Ack(ctx, ackPos))
	// Bounded, so a halt that fails to arm is an assertion, not a timeout.
	haltCtx, haltCancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = run1.NextN(haltCtx, 1)
	haltCancel()
	is.True(err != nil && strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt)) // the halt armed on the marker's ack
	_ = run1.Teardown(ctx)

	// Invariant 1: the slot must not move past the deciding change, or
	// Postgres would not re-send its transaction on the restart. Teardown
	// sends the final standby status; wait for the server to apply it (the
	// slot reaches the marker's LSN) and check it did not go further.
	mp := posOf(t, marker)
	mlsn, err := mp.LSN()
	is.NoErr(err)
	var cf pglogrepl.LSN
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		cf = r2CF(ctx, t, pool, table)
		if cf >= mlsn || time.Now().After(deadline) {
			break
		}
	}
	is.True(cf == mlsn) // reached the marker's LSN, and not past it

	return marker, recs[len(pre)-1]
}

// TestDrift338_Approval is the #338 regression test.
// Run 1 halts on a drift and acks the marker. The restart is the operator's
// approval: it must deliver the deciding change exactly once, then the rest
// of the stream, with no second halt, under both policies. Run 3 restarts
// from the last delivered record and must deliver nothing twice.
//
// Before the fix the marker carried the deciding change's own key, so the
// approving restart skipped that row: want [p2 ...] got [... without p2].
func TestDrift338_Approval(t *testing.T) {
	for _, tc := range boundaryCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			marker, _ := boundaryRun1(ctx, t, tc, table)

			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, marker.Position, tc.policy)
			recs := readN(ctx, t, run2, len(tc.want), 15*time.Second)
			var got []string
			for _, r := range recs {
				got = append(got, column1(t, r)) // fails on a second marker
				is.NoErr(run2.Ack(ctx, r.Position))
			}
			is.Equal(got, tc.want)
			expectNoMore(ctx, t, run2, 2*time.Second)

			// The stream continues past the approval.
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('post')`, table))
			is.NoErr(err)
			post := readN(ctx, t, run2, 1, 15*time.Second)
			is.Equal(column1(t, post[0]), "post")
			is.NoErr(run2.Ack(ctx, post[0].Position))
			_ = run2.Teardown(ctx)

			// Exactly once: a further restart from the last record delivers
			// nothing that was already delivered.
			run3 := newInterleaveCombinedPolicy(ctx, t, pool, table, post[0].Position, tc.policy)
			defer func() { _ = run3.Teardown(ctx) }()
			expectNoMore(ctx, t, run3, 3*time.Second)
		})
	}
}

// TestDrift338_CrashBeforeAck covers the window between the
// marker being durable (the engine persisted its position) and its ack: the
// process dies, and the restart resumes from the marker without the halt ever
// having surfaced. The restart is still the approval, and the deciding change
// is delivered once.
func TestDrift338_CrashBeforeAck(t *testing.T) {
	for _, tc := range boundaryCases()[:2] {
		t.Run(tc.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			run1 := newInterleaveCombinedPolicy(ctx, t, pool, table, nil, tc.policy)
			for _, b := range tc.baseline {
				_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('%s')`, table, b))
				is.NoErr(err)
			}
			tx, err := pool.Begin(ctx)
			is.NoErr(err)
			tc.tx(ctx, t, tx, table)
			is.NoErr(tx.Commit(ctx))
			n := len(tc.baseline) + len(tc.before) + 1
			recs := readN(ctx, t, run1, n, 15*time.Second)
			marker := recs[n-1]
			is.Equal(marker.Metadata[MetadataSchemaDrift], "true")
			for _, r := range recs[:n-1] {
				is.NoErr(run1.Ack(ctx, r.Position))
			}
			_ = run1.Teardown(ctx) // dies before the marker's ack

			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, marker.Position, tc.policy)
			defer func() { _ = run2.Teardown(ctx) }()
			is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), tc.want)
		})
	}
}

// legacyMarkerPosition rewrites a marker position to what a build without
// #338 wrote: the deciding change's own key, instead of its predecessor.
func legacyMarkerPosition(t *testing.T, marker opencdc.Record) opencdc.Position {
	t.Helper()
	p := posOf(t, marker)
	commit, err := p.TxCommit()
	if err != nil {
		t.Fatal(err)
	}
	if p.TxSeq == math.MaxUint64 {
		commit, p.TxSeq = commit+1, 1
	} else {
		p.TxSeq++
	}
	p.TxCommitLSN = commit.String()
	return p.ToSDKPosition()
}

// TestDrift338_Upgrade pins the upgrade path. A marker
// written by a build without #338 carries the deciding change's own key and
// is read by the unchanged resume rule: the boundary row is skipped as it
// always was (the documented D5 loss, accepted for v0.15.0), and nothing else
// is lost or repeated. The new build must neither halt again nor re-deliver
// rows the old marker already covers.
func TestDrift338_Upgrade(t *testing.T) {
	for _, tc := range boundaryCases()[:3] {
		t.Run(tc.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			marker, _ := boundaryRun1(ctx, t, tc, table)
			old := legacyMarkerPosition(t, marker)

			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, old, tc.policy)
			defer func() { _ = run2.Teardown(ctx) }()
			// Everything but the boundary row, which the old marker covers.
			is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), tc.want[1:])
		})
	}
}

// TestDrift338_PositionFormat pins that the marker's position is an
// ordinary version 2 position with a Known change key: no new field, so every
// build with the change-key resume reads it as exact.
func TestDrift338_PositionFormat(t *testing.T) {
	tc := boundaryCases()[0]
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	marker, prev := boundaryRun1(ctx, t, tc, table)
	mp, pp := posOf(t, marker), posOf(t, prev)
	is.Equal(mp.Version, position.CurrentPositionVersion)
	is.Equal(mp.TxCommitLSN, pp.TxCommitLSN) // same transaction
	is.Equal(mp.TxSeq, pp.TxSeq)             // the key of the record just before the deciding change
	raw := r2Raw(t, marker.Position)
	for k := range raw {
		switch k {
		case "version", "type", "last_lsn", "tx_commit_lsn", "tx_seq", "schema_history", "snapshot_low_watermark_lsn":
		default:
			t.Fatalf("marker position carries an unexpected field %q", k)
		}
	}
}

// reformatPosition returns the same position re-marshaled: indented, keys in
// sorted order. Semantically equal, byte-different.
func reformatPosition(t *testing.T, p opencdc.Position) opencdc.Position {
	t.Helper()
	m := map[string]json.RawMessage{} // RawMessage keeps tx_seq's 64-bit value exact
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatal(err)
	}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestDrift338_ReformattedMarkerAck: the engine (or middleware) acks a
// semantically equal but re-marshaled marker position. The halt must still
// surface. Arming on the position's bytes turned this into a silent stall:
// NextN blocked, nothing was logged, and the slot was held.
func TestDrift338_ReformattedMarkerAck(t *testing.T) {
	for _, tc := range boundaryCases()[:2] { // mid-transaction and seq-1 markers
		tc.reformatAck = true
		t.Run(tc.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			marker, _ := boundaryRun1(ctx, t, tc, table) // asserts the halt armed
			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, marker.Position, tc.policy)
			defer func() { _ = run2.Teardown(ctx) }()
			is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), tc.want)
		})
	}
}

// newCombinedTables is newInterleaveCombinedPolicy for several tables in one
// publication.
func newCombinedTables(ctx context.Context, t *testing.T, pool *pgxpool.Pool, slot string, tables []string, pos opencdc.Position) *CombinedIterator {
	t.Helper()
	keys := map[string]string{}
	for _, tb := range tables {
		keys[tb] = "id"
	}
	it, err := NewCombinedIterator(ctx, pool, Config{
		Position:          pos,
		SlotName:          slot,
		PublicationName:   slot,
		Tables:            tables,
		TableKeys:         keys,
		BatchSize:         1,
		SchemaDriftPolicy: SchemaDriftPolicyHalt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return it
}

// TestDrift338_InterleavedBoundary: the deciding change is the first change of
// its transaction (A: ALTER, INSERT a1) while another transaction (B, another
// table) is open across it. B inserts before A's insert and commits after it,
// in both commit orders.
//
//   - B commits first: B's row is delivered before the marker and acked, so the
//     approving restart must deliver exactly [a1].
//   - A commits first: B's row is in the stream after the marker, so run 1
//     skips it (D4) and the restart must deliver [a1, b1].
//
// The first-change predecessor is what keeps B's row out of the first case and
// A's row in. The legacy rule (no key, B's commit is past a1's LSN) re-delivers
// b1; a predecessor of Seq 1 drops a1.
func TestDrift338_InterleavedBoundary(t *testing.T) {
	for _, tt := range []struct {
		name    string
		bFirst  bool
		want    []string
		beforeM []string
	}{
		{name: "b_first", bFirst: true, want: []string{"a1"}, beforeM: []string{"base", "b1"}},
		{name: "a_first", bFirst: false, want: []string{"a1", "b1"}, beforeM: []string{"base"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			tableA := test.SetupEmptyTestTable(ctx, t, pool)
			tableB := tableA + "_b"
			test.SetupEmptyTestTableWithName(ctx, t, pool, tableB)
			cleanupSlot(t, pool, tableA)
			tables := []string{tableA, tableB}

			run1 := newCombinedTables(ctx, t, pool, tableA, tables, nil)
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('base')`, tableA))
			is.NoErr(err)
			base := readN(ctx, t, run1, 1, 15*time.Second)
			is.Equal(column1(t, base[0]), "base")
			is.NoErr(run1.Ack(ctx, base[0].Position))

			txB, err := pool.Begin(ctx)
			is.NoErr(err)
			_, err = txB.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('b1')`, tableB))
			is.NoErr(err)
			txA, err := pool.Begin(ctx)
			is.NoErr(err)
			_, err = txA.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, tableA))
			is.NoErr(err)
			_, err = txA.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('a1')`, tableA))
			is.NoErr(err)
			if tt.bFirst {
				is.NoErr(txB.Commit(ctx))
				is.NoErr(txA.Commit(ctx))
			} else {
				is.NoErr(txA.Commit(ctx))
				is.NoErr(txB.Commit(ctx))
			}

			pre := tt.beforeM[1:] // "base" was read above
			recs := readN(ctx, t, run1, len(pre)+1, 15*time.Second)
			for i, w := range pre {
				is.Equal(column1(t, recs[i]), w)
			}
			marker := recs[len(pre)]
			is.Equal(marker.Metadata[MetadataSchemaDrift], "true")
			for _, r := range recs {
				is.NoErr(run1.Ack(ctx, r.Position))
			}
			haltCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_, err = run1.NextN(haltCtx, 1)
			cancel()
			is.True(err != nil && strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt))
			_ = run1.Teardown(ctx)

			run2 := newCombinedTables(ctx, t, pool, tableA, tables, marker.Position)
			defer func() { _ = run2.Teardown(ctx) }()
			is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), tt.want)
		})
	}
}

// TestDrift338_RedeliveredIfUnackedAfterApproval: the approving restart
// delivers the deciding row but the process stops before the engine acks it.
// The checkpoint is still the marker, so the next restart delivers the row
// again, exactly once, followed by the rest of the stream. This is
// at-least-once for the one record that was in flight, not a loss and not a
// repeat beyond that record.
func TestDrift338_RedeliveredIfUnackedAfterApproval(t *testing.T) {
	for _, tc := range boundaryCases()[:2] {
		t.Run(tc.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			marker, _ := boundaryRun1(ctx, t, tc, table)

			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, marker.Position, tc.policy)
			first := readN(ctx, t, run2, 1, 15*time.Second)
			is.Equal(column1(t, first[0]), tc.want[0]) // the deciding row
			_ = run2.Teardown(ctx)                     // never acked

			run3 := newInterleaveCombinedPolicy(ctx, t, pool, table, marker.Position, tc.policy)
			defer func() { _ = run3.Teardown(ctx) }()
			is.Equal(drainColumn1(ctx, t, run3, 4*time.Second), tc.want) // once, then the rest
		})
	}
}
