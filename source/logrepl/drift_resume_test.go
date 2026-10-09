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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// Tests for #335: the schema-drift decision is made on the first change that
// is actually delivered after a resume, not on a Relation message that
// Postgres replays ahead of changes the resume point skips.

// usersRow is an InsertMessage for public.users (RelationID 1) carrying n
// text-format columns, enough to decode against shapeV1 (n=2) or shapeV2
// (n=3).
func usersRow(n int) *pglogrepl.InsertMessage {
	vals := []string{"7", "a@example.com", "42"}
	cols := make([]*pglogrepl.TupleDataColumn, 0, n)
	for _, v := range vals[:n] {
		cols = append(cols, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)})
	}
	return &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple:      &pglogrepl.TupleData{ColumnNum: uint16(n), Columns: cols}, //nolint:gosec // n <= 3
	}
}

// checkpointWith returns the position a run that accepted the given shapes,
// in order, would checkpoint.
func checkpointWith(t *testing.T, shapes ...[]*pglogrepl.RelationMessageColumn) position.Position {
	t.Helper()
	ctx := context.Background()
	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)
	lsn := pglogrepl.LSN(0)
	for i, s := range shapes {
		lsn += 100
		if _, marker := relate(ctx, t, h, relMsg(s...), lsn); marker {
			t.Fatalf("building the checkpoint emitted a marker for shape %d", i)
		}
	}
	p, err := position.ParseSDKPosition(h.buildPosition(lsn + 50))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// drainBatches returns every record the handler has emitted so far.
func drainBatches(out chan []opencdc.Record) []opencdc.Record {
	var recs []opencdc.Record
	for {
		select {
		case b := <-out:
			recs = append(recs, b...)
		default:
			return recs
		}
	}
}

func markersIn(recs []opencdc.Record) []opencdc.Record {
	var m []opencdc.Record
	for _, r := range recs {
		if r.Metadata[MetadataSchemaDrift] == "true" {
			m = append(m, r)
		}
	}
	return m
}

// Test_Drift335_ReplayedPreAlterShapeIsNotDrift is the #335 bug at the handler
// level. The checkpoint already records the post-ALTER shape [v1, v2]. The
// restart replays the transaction: the pre-ALTER Relation message (v1)
// arrives, but the changes after it are at or below the checkpoint and the
// subscription skips them, so they never reach the handler. Then the post-ALTER
// Relation message (v2) arrives and its first delivered change must flow, with
// no marker, under either policy.
//
// Before the fix, the replayed v1 was classified on arrival as a change made
// while the connector was down (driftAcrossRestart), which halts even under
// evolve, and the next delivered change emitted a marker.
func Test_Drift335_ReplayedPreAlterShapeIsNotDrift(t *testing.T) {
	for _, policy := range []SchemaDriftPolicy{SchemaDriftPolicyHalt, SchemaDriftPolicyEvolve} {
		t.Run(string(policy), func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			h, out := newHandlerWithOut(t, checkpointWith(t, shapeV1, shapeV2), policy)

			_, err := h.Handle(ctx, relMsg(shapeV1...), 0) // replayed; its changes are skipped
			is.NoErr(err)
			_, err = h.Handle(ctx, relMsg(shapeV2...), 0)
			is.NoErr(err)
			_, err = h.Handle(ctx, usersRow(3), 400) // first delivered change
			is.NoErr(err)

			recs := drainBatches(out)
			is.Equal(len(recs), 1)
			is.Equal(len(markersIn(recs)), 0)
			is.True(!h.driftMarkerPending())
			is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 2) // unchanged
		})
	}
}

// Test_Drift335_ReplayedShapeOnlyNeverDecided pins that a Relation message
// followed only by skipped changes decides nothing and commits nothing, even
// when an unrelated table's change is delivered after it.
func Test_Drift335_ReplayedShapeOnlyNeverDecided(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	start := checkpointWith(t, shapeV1, shapeV2)
	h, out := newHandlerWithOut(t, start, SchemaDriftPolicyHalt)

	orders := &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1)},
	}
	_, err := h.Handle(ctx, relMsg(shapeV1...), 0) // replayed, changes skipped
	is.NoErr(err)
	_, err = h.Handle(ctx, orders, 0)
	is.NoErr(err)
	_, err = h.Handle(ctx, &pglogrepl.InsertMessage{
		RelationID: 2,
		Tuple: &pglogrepl.TupleData{ColumnNum: 1, Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
		}},
	}, 400)
	is.NoErr(err)

	recs := drainBatches(out)
	is.Equal(len(recs), 1)
	is.Equal(len(markersIn(recs)), 0)
	p, err := position.ParseSDKPosition(recs[0].Position)
	is.NoErr(err)
	is.Equal(p.SchemaHistory["public.users"], start.SchemaHistory["public.users"]) // v1 not committed
}

// Test_Drift335_RevertStillHalts pins B1 AC5 under the delivery-time
// decision: a real revert is a Relation message followed by a DELIVERED
// change, so it is decided against the durable shape and halts. Evolve halts
// too: the revert drops a column.
func Test_Drift335_RevertStillHalts(t *testing.T) {
	for _, policy := range []SchemaDriftPolicy{SchemaDriftPolicyHalt, SchemaDriftPolicyEvolve} {
		t.Run(string(policy), func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			h, out := newHandlerWithOut(t, checkpointWith(t, shapeV1, shapeV2), policy)

			_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
			_, err := h.Handle(ctx, usersRow(3), 400)
			is.NoErr(err)
			_, _ = h.Handle(ctx, relMsg(shapeV1...), 0) // ALTER TABLE ... DROP COLUMN age
			_, err = h.Handle(ctx, usersRow(2), 500)
			is.NoErr(err)

			recs := drainBatches(out)
			is.Equal(len(recs), 2)
			m := markersIn(recs)
			is.Equal(len(m), 1)
			is.Equal(m[0].Metadata[MetadataSchemaDriftNarrowing], "true")
			is.True(strings.Contains(m[0].Metadata[MetadataSchemaDriftDiff], `column "age" dropped`))
		})
	}
}

// Test_Drift335_DriftWhileDown pins that genuine drift made while the
// connector was down is still acted on per policy, and decided exactly once.
//
// With the old shape replayed (the restart resumes inside a transaction that
// used it), this process has the durable shape's columns, so the diff is
// exact: halt halts with the diff, evolve accepts an additive change and
// halts on a narrowing one. Without a replay, only the hash survives, so the
// change is driftAcrossRestart, which halts under every policy (B1: evolve
// cannot prove an unseen change was additive).
func Test_Drift335_DriftWhileDown(t *testing.T) {
	tests := []struct {
		name       string
		policy     SchemaDriftPolicy
		durable    []*pglogrepl.RelationMessageColumn
		replay     []*pglogrepl.RelationMessageColumn // nil: no replay of the old shape
		current    []*pglogrepl.RelationMessageColumn
		wantMarker bool
		wantDiff   string // "" with a marker: across-restart, no diff metadata
		wantNarrow string
	}{
		{name: "halt, additive, replayed", policy: SchemaDriftPolicyHalt, durable: shapeV1, replay: shapeV1, current: shapeV2,
			wantMarker: true, wantDiff: `column "age" added`, wantNarrow: "false"},
		{name: "halt, additive, not replayed", policy: SchemaDriftPolicyHalt, durable: shapeV1, current: shapeV2,
			wantMarker: true},
		{name: "evolve, additive, replayed", policy: SchemaDriftPolicyEvolve, durable: shapeV1, replay: shapeV1, current: shapeV2,
			wantMarker: false},
		{name: "evolve, additive, not replayed", policy: SchemaDriftPolicyEvolve, durable: shapeV1, current: shapeV2,
			wantMarker: true},
		{name: "evolve, narrowing, replayed", policy: SchemaDriftPolicyEvolve, durable: shapeV2, replay: shapeV2, current: shapeV1,
			wantMarker: true, wantDiff: `column "age" dropped`, wantNarrow: "true"},
		{name: "evolve, narrowing, not replayed", policy: SchemaDriftPolicyEvolve, durable: shapeV2, current: shapeV1,
			wantMarker: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			h, out := newHandlerWithOut(t, checkpointWith(t, tt.durable), tt.policy)

			if tt.replay != nil {
				_, _ = h.Handle(ctx, relMsg(tt.replay...), 0) // its changes are skipped
			}
			_, _ = h.Handle(ctx, relMsg(tt.current...), 0)
			// Several delivered changes with the new shape: the drift must be
			// decided once, not per change.
			for _, lsn := range []pglogrepl.LSN{400, 410, 420} {
				_, err := h.Handle(ctx, usersRow(len(tt.current)), lsn)
				is.NoErr(err)
			}

			recs := drainBatches(out)
			m := markersIn(recs)
			if !tt.wantMarker {
				is.Equal(len(m), 0)
				is.Equal(len(recs), 3) // every change delivered
				last, ok := h.basePosition.LastSchemaVersion("public.users")
				is.True(ok)
				is.Equal(last.ColumnSetHash, position.HashColumnSet(columnIdentities(relMsg(tt.current...))))
				return
			}
			is.Equal(len(recs), 1) // the marker, then nothing (D4)
			is.Equal(len(m), 1)    // exactly once
			is.Equal(m[0].Metadata[MetadataSchemaDriftLSN], pglogrepl.LSN(400).String())
			if tt.wantDiff == "" {
				_, hasDiff := m[0].Metadata[MetadataSchemaDriftDiff]
				is.True(!hasDiff) // across restart: never a fabricated diff
			} else {
				is.True(strings.Contains(m[0].Metadata[MetadataSchemaDriftDiff], tt.wantDiff))
				is.Equal(m[0].Metadata[MetadataSchemaDriftNarrowing], tt.wantNarrow)
			}
		})
	}
}

// Test_Drift335_ShapesBeforeDeliveryDecideOnce pins that several Relation
// messages before one delivered change produce one decision, against the shape
// that change uses, with the cumulative diff from the durable shape.
func Test_Drift335_ShapesBeforeDeliveryDecideOnce(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	shapeV3 := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("email", 25, -1), relCol("age", 23, -1), relCol("city", 25, -1)}

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
	_, _ = h.Handle(ctx, relMsg(shapeV3...), 0)
	_, err := h.Handle(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 300)
	is.NoErr(err)

	m := markersIn(drainBatches(out))
	is.Equal(len(m), 1)
	diff := m[0].Metadata[MetadataSchemaDriftDiff]
	is.True(strings.Contains(diff, `column "age" added`))
	is.True(strings.Contains(diff, `column "city" added`))
}

// TestDrift335_HaltApprovalMidTransaction is the #335 halt-policy path end to
// end. A transaction adds a column between two inserts. Run 1 halts on the
// drift, the marker is acked, and the restart (the operator's approval)
// resumes from the marker, inside that transaction. Postgres re-sends the
// transaction, starting with the pre-ALTER Relation message. The restart must
// deliver the deciding change (p2, #338) and the rest of the transaction, with
// no second marker and no second halt.
//
// Before the fix, run 2 read the replayed pre-ALTER shape as drift while down
// and emitted a second marker instead of c1.
func TestDrift335_HaltApprovalMidTransaction(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	cleanupSlot(t, pool, table)

	run1 := newInterleaveCombinedPolicy(ctx, t, pool, table, nil, SchemaDriftPolicyHalt)

	tx, err := pool.Begin(ctx)
	is.NoErr(err)
	ex := func(q string) {
		_, err := tx.Exec(ctx, q)
		is.NoErr(err)
	}
	ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p1')`, table))
	ex(fmt.Sprintf(`ALTER TABLE %q ADD COLUMN extra int`, table))
	ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p2')`, table))
	_, err = tx.CopyFrom(ctx, pgx.Identifier{table}, []string{"column1"}, pgx.CopyFromRows([][]any{{"c1"}, {"c2"}, {"c3"}}))
	is.NoErr(err)
	ex(fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('p3')`, table))
	is.NoErr(tx.Commit(ctx))

	recs := readN(ctx, t, run1, 2, 15*time.Second)
	is.Equal(column1(t, recs[0]), "p1")
	is.Equal(recs[1].Metadata[MetadataSchemaDrift], "true") // the marker replaces p2 (D4); the restart delivers it (#338)
	is.NoErr(run1.Ack(ctx, recs[0].Position))
	is.NoErr(run1.Ack(ctx, recs[1].Position))
	_, err = run1.NextN(ctx, 1)
	is.True(err != nil && strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt))
	_ = run1.Teardown(ctx)

	run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, recs[1].Position, SchemaDriftPolicyHalt)
	defer func() { _ = run2.Teardown(ctx) }()
	is.Equal(drainColumn1(ctx, t, run2, 4*time.Second), []string{"p2", "c1", "c2", "c3", "p3"})
}

// TestDrift335_DriftWhileDownMidTransaction pins that a real schema change
// made while the connector was down is still acted on when the restart
// resumes inside an earlier transaction: run 1 acks only the first of two
// rows of a transaction, then, while it is down, a column is added (or
// dropped) and a row inserted.
func TestDrift335_DriftWhileDownMidTransaction(t *testing.T) {
	tests := []struct {
		name       string
		policy     SchemaDriftPolicy
		ddl        string
		wantRows   []string
		wantMarker bool
		wantNarrow string
	}{
		{name: "halt additive", policy: SchemaDriftPolicyHalt, ddl: `ALTER TABLE %q ADD COLUMN extra int`,
			wantRows: []string{"b"}, wantMarker: true, wantNarrow: "false"},
		{name: "evolve additive", policy: SchemaDriftPolicyEvolve, ddl: `ALTER TABLE %q ADD COLUMN extra int`,
			wantRows: []string{"b", "c"}},
		{name: "evolve narrowing", policy: SchemaDriftPolicyEvolve, ddl: `ALTER TABLE %q DROP COLUMN column2`,
			wantRows: []string{"b"}, wantMarker: true, wantNarrow: "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := test.Context(t)
			is := is.New(t)
			pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
			table := test.SetupEmptyTestTable(ctx, t, pool)
			cleanupSlot(t, pool, table)

			run1 := newInterleaveCombinedPolicy(ctx, t, pool, table, nil, tt.policy)
			tx, err := pool.Begin(ctx)
			is.NoErr(err)
			for _, v := range []string{"a", "b"} {
				_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('%s')`, table, v))
				is.NoErr(err)
			}
			is.NoErr(tx.Commit(ctx))
			recs := readN(ctx, t, run1, 2, 15*time.Second)
			is.NoErr(run1.Ack(ctx, recs[0].Position))
			_ = run1.Teardown(ctx)

			// While the connector is down.
			_, err = pool.Exec(ctx, fmt.Sprintf(tt.ddl, table))
			is.NoErr(err)
			_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('c')`, table))
			is.NoErr(err)

			run2 := newInterleaveCombinedPolicy(ctx, t, pool, table, recs[0].Position, tt.policy)
			defer func() { _ = run2.Teardown(ctx) }()
			want := len(tt.wantRows)
			if tt.wantMarker {
				want++
			}
			got := readN(ctx, t, run2, want, 15*time.Second)
			var rows []string
			var markers []opencdc.Record
			for _, r := range got {
				if r.Metadata[MetadataSchemaDrift] == "true" {
					markers = append(markers, r)
					continue
				}
				rows = append(rows, column1(t, r))
			}
			is.Equal(rows, tt.wantRows)
			if !tt.wantMarker {
				is.Equal(len(markers), 0)
				expectNoMore(ctx, t, run2, 2*time.Second)
				return
			}
			is.Equal(len(markers), 1)
			is.Equal(markers[0].Metadata[MetadataSchemaDriftNarrowing], tt.wantNarrow)
			for _, r := range got {
				is.NoErr(run2.Ack(ctx, r.Position))
			}
			_, err = run2.NextN(ctx, 1)
			is.True(err != nil && strings.Contains(err.Error(), ErrorCodeSchemaDriftHalt))
		})
	}
}
