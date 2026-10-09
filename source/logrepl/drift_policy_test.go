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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// newHandlerWithOut is newHandlerWithPosition plus the send side of the output
// channel, which the handler only exposes as chan<-; tests that must observe
// emitted batches read it here.
func newHandlerWithOut(t *testing.T, p position.Position, policy SchemaDriftPolicy) (*CDCHandler, chan []opencdc.Record) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	out := make(chan []opencdc.Record, 16)
	return NewCDCHandler(ctx, internal.NewRelationSet(), map[string]string{"users": "id"},
		out, false, 1, time.Hour, p, policy), out
}

func TestParseSchemaDriftPolicy(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    SchemaDriftPolicy
		wantErr string
	}{
		{name: "empty defaults to halt", in: "", want: SchemaDriftPolicyHalt},
		{name: "halt", in: "halt", want: SchemaDriftPolicyHalt},
		{name: "evolve", in: "evolve", want: SchemaDriftPolicyEvolve},
		{
			name:    "dlq rejected with stable code",
			in:      "dlq",
			wantErr: ErrorCodeSchemaDriftPolicyUnsupported + ": schema drift policy \"dlq\" is not supported in this version",
		},
		{
			name:    "unknown rejected with stable code",
			in:      "bogus",
			wantErr: ErrorCodeSchemaDriftPolicyUnsupported + ": unknown schema drift policy \"bogus\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			got, err := ParseSchemaDriftPolicy(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tt.wantErr) {
					t.Fatalf("expected error prefix %q, got: %v", tt.wantErr, err)
				}
				return
			}
			is.NoErr(err)
			is.Equal(got, tt.want)
		})
	}
}

// expectNoBatch fails if the handler has emitted anything.
func expectNoBatch(t *testing.T, out chan []opencdc.Record) {
	t.Helper()
	select {
	case batch := <-out:
		t.Fatalf("expected no records, got %d: %v", len(batch), batch[0].Metadata)
	default:
	}
}

// Test_HandleRelation_HaltEmitsMarker pins the D1 marker contract end to end:
// exact metadata, nil key/payload, position = buildPosition(first-new-shape
// DML LSN) carrying the new shape, and that DML's LSN returned for Handle (D2).
//
// pgoutput delivers the RelationMessage with WALStart 0 (verified 2026-08-29),
// and the relation message decides nothing (#335): the marker is emitted by
// the first delivered DML that uses the new shape, at that DML's LSN.
func Test_HandleRelation_HaltEmitsMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)

	// The relation message alone emits nothing and stages nothing.
	writtenLSN, err := h.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(err)
	is.Equal(writtenLSN, pglogrepl.LSN(0))
	is.True(!h.driftMarkerPending())
	expectNoBatch(t, out)

	// The first DML with the new shape emits the marker. Handle returns that
	// DML's LSN (D2) so walWritten advances past the marker.
	gotLSN, err := h.Handle(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	is.NoErr(err)
	is.Equal(gotLSN, pglogrepl.LSN(210))

	batch := <-out
	is.Equal(len(batch), 1)
	rec := batch[0]

	is.Equal(rec.Operation, opencdc.OperationCreate)
	is.Equal(rec.Key, nil)
	is.Equal(rec.Payload.After, nil)

	is.Equal(rec.Metadata[MetadataSchemaDrift], "true")
	is.Equal(rec.Metadata[MetadataSchemaDriftTable], "public.users")
	is.Equal(rec.Metadata[MetadataSchemaDriftLSN], "0/D2")
	is.Equal(rec.Metadata[MetadataSchemaDriftPolicy], "halt")
	is.Equal(rec.Metadata[MetadataSchemaDriftNarrowing], "false") // ADD COLUMN is compatible; D1 wants the explicit value
	is.True(strings.Contains(rec.Metadata[MetadataSchemaDriftDiff], `column "age" added (type 23)`))

	// Position semantics: LastLSN is the first-new-shape DML LSN, and the
	// history it carries already contains the new shape, first seen there
	// (invariant-1-safe boundary).
	p, err := position.ParseSDKPosition(rec.Position)
	is.NoErr(err)
	lsn, err := p.LSN()
	is.NoErr(err)
	is.Equal(lsn, pglogrepl.LSN(210))
	v, ok := p.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(v.ColumnSetHash, position.HashColumnSet(columnIdentities(relMsg(shapeV2...))))
	is.Equal(v.FirstSeenLSN, pglogrepl.LSN(210).String())

	// D3: acked-gated — the error is stored, but not surfaced until the ack.
	is.True(!h.driftHaltArmed.Load())
	is.Equal(h.driftHaltError(), nil)
	h.maybeArmDriftHalt(lsn, internal.ChangeKey{}, nil)
	is.True(h.driftHaltArmed.Load())
	is.True(strings.HasPrefix(h.driftHaltError().Error(), ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(h.driftHaltError().Error(), haltRevertTrap))
	// #338: the boundary row is delivered after the restart, so the message
	// no longer discloses a dropped record.
	is.True(!strings.Contains(h.driftHaltError().Error(), "not delivered"))
}

// Test_HandleRelation_EvolveAcceptsAdditive pins that evolve admits a purely
// additive change silently: no marker, no pending halt, version recorded.
func Test_HandleRelation_EvolveAcceptsAdditive(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	kind, marker := relate(ctx, t, h, relMsg(shapeV2...), 200)

	is.Equal(kind, driftInProcess)
	is.True(!marker)
	is.True(!h.driftMarkerPending())
	expectNoBatch(t, out)
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 2)
}

// Test_HandleRelation_EvolveHaltsOnNarrowing pins that evolve still halts on an
// incompatible change (a drop here), with the narrowing metadata set.
func Test_HandleRelation_EvolveHaltsOnNarrowing(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)

	_, _ = relate(ctx, t, h, relMsg(shapeV2...), 100)
	_, err := h.Handle(ctx, relMsg(shapeV1...), 0) // drop "age"
	is.NoErr(err)
	expectNoBatch(t, out)

	err = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	is.NoErr(err)

	batch := <-out
	is.Equal(batch[0].Metadata[MetadataSchemaDriftNarrowing], "true")
	is.True(strings.Contains(batch[0].Metadata[MetadataSchemaDriftDiff], `column "age" dropped (was type 23)`))
}

// Test_HandleRelation_AcrossRestartMarkerOmitsDiff pins FM7/AC7 at the metadata
// level: the across-restart marker carries table/policy/lsn but no narrowing or
// diff keys — the columns are not recoverable, and fabricating them would send
// an operator chasing a diff that never happened.
func Test_HandleRelation_AcrossRestartMarkerOmitsDiff(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h1 := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	_, _ = relate(ctx, t, h1, relMsg(shapeV1...), 100)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2, out := newHandlerWithOut(t, resumed, SchemaDriftPolicyHalt)

	_, err = h2.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(err)
	err = h2.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	is.NoErr(err)

	batch := <-out
	meta := batch[0].Metadata
	is.Equal(meta[MetadataSchemaDrift], "true")
	is.Equal(meta[MetadataSchemaDriftTable], "public.users")
	is.Equal(meta[MetadataSchemaDriftPolicy], "halt")
	_, hasNarrowing := meta[MetadataSchemaDriftNarrowing]
	is.True(!hasNarrowing)
	_, hasDiff := meta[MetadataSchemaDriftDiff]
	is.True(!hasDiff)
}

// Test_HaltError_AcrossRestartMessage pins AC7 on the message: table, hash
// transition, FirstSeenLSN; no fabricated column diff.
func Test_HaltError_AcrossRestartMessage(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h1 := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	_, _ = relate(ctx, t, h1, relMsg(shapeV1...), 100)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2, _ := newHandlerWithOut(t, resumed, SchemaDriftPolicyHalt)

	_, _ = h2.Handle(ctx, relMsg(shapeV2...), 0)
	_ = h2.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	h2.maybeArmDriftHalt(210, internal.ChangeKey{}, nil)

	msg := h2.driftHaltError().Error()
	is.True(strings.HasPrefix(msg, ErrorCodeSchemaDriftHalt+": "))
	is.True(strings.Contains(msg, "public.users"))
	is.True(strings.Contains(msg, "schema hash "))
	is.True(strings.Contains(msg, "0/64")) // prev.FirstSeenLSN
	is.True(strings.Contains(msg, haltRevertTrap))
	is.True(!strings.Contains(msg, "not delivered")) // #338: no dropped boundary record to disclose
	is.True(!strings.Contains(msg, "age"))           // AC7: never fabricates a column diff
}

// Test_HandleRelation_StackedDDL_OneMarker pins FM8/AC9: a second DDL while a
// halt is pending is neither decided nor committed to the live history, and
// gets no marker — exactly one marker per halt — and the second shape halts on
// restart (no durable state claims it, so the restart re-derives it).
func Test_HandleRelation_StackedDDL_OneMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	shapeV3 := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("email", 25, -1), relCol("age", 23, -1), relCol("city", 25, -1)}

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, err := h.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(err)

	// The first DML emits exactly one marker, at its own LSN (D2).
	_, err = h.Handle(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	is.NoErr(err)
	is.Equal(h.driftMarkerLSN.Load(), uint64(210))
	batch := <-out
	is.Equal(len(batch), 1)
	marker := batch[0]

	// Second DDL and its DML while the marker is pending (not yet acked).
	_, err = h.Handle(ctx, relMsg(shapeV3...), 0)
	is.NoErr(err)
	_, err = h.Handle(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 310)
	is.NoErr(err)
	is.Equal(h.driftMarkerLSN.Load(), uint64(210)) // no second marker
	expectNoBatch(t, out)

	// The second shape was not committed: a position serialized now cannot
	// checkpoint it and dedupe the drift away on a restart.
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 2) // [v1, v2]

	// Review Blocker 1: the marker's position carries [v1, v2], never v3.
	p, err := position.ParseSDKPosition(marker.Position)
	is.NoErr(err)
	v, ok := p.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(v.ColumnSetHash, position.HashColumnSet(columnIdentities(relMsg(shapeV2...))))
	is.Equal(len(p.SchemaHistory["public.users"]), 2)
	is.Equal(v.FirstSeenLSN, pglogrepl.LSN(210).String())

	// Restart from the marker position: v3 is NOT in it, so the restart
	// re-derives the second DDL as drift and halts again — FM8's "no silent
	// admission" holds at the marker's own checkpoint.
	h2, _ := newHandlerWithOut(t, p, SchemaDriftPolicyHalt)
	kind, marked := relate(ctx, t, h2, relMsg(shapeV3...), 500)
	is.Equal(kind, driftAcrossRestart)
	is.True(marked)
}

// Test_HandleRelation_SecondTableDrift_NoSecondMarker pins that drift in a
// second table while a halt is pending behaves like stacked DDL on the first:
// no second marker, one halt.
func Test_HandleRelation_SecondTableDrift_NoSecondMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	other := &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1)},
	}
	_, _ = relate(ctx, t, h, other, 300)

	_, err := h.Handle(ctx, relMsg(shapeV2...), 0)
	is.NoErr(err)
	_, err = h.Handle(ctx, &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("total", 1700, 655366)},
	}, 0)
	is.NoErr(err)

	// The users DML emits the users marker.
	err = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 410)
	is.NoErr(err)
	batch := <-out
	is.Equal(len(batch), 1)
	is.Equal(batch[0].Metadata[MetadataSchemaDriftTable], "public.users")

	// The orders DML is skipped (D4) and its drift gets no second marker.
	err = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 2}, 420)
	is.NoErr(err)
	expectNoBatch(t, out)
	is.Equal(h.driftMarkerLSN.Load(), uint64(410))
}

// Test_HandleRelation_DMLSkippedAfterMarker pins D4 at the handler level: after
// the marker, DML for ANY relation is dropped, never emitted — including a
// message whose relation the handler has never seen (the skip must fire before
// any relation lookup).
func Test_HandleRelation_DMLSkippedAfterMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)

	// The first DML emits the marker and is itself skipped (the marker, then
	// nothing). Drain the marker batch.
	err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	is.NoErr(err)
	batch := <-out
	is.Equal(len(batch), 1)

	// Inserts for the drifted table, an unrelated table, and a delete: none may
	// be emitted (global skip, D4).
	for _, lsn := range []pglogrepl.LSN{220, 230, 240} {
		err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, lsn)
		is.NoErr(err)
		err = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 99}, lsn)
		is.NoErr(err)
		err = h.handleDelete(ctx, &pglogrepl.DeleteMessage{RelationID: 99}, lsn)
		is.NoErr(err)
	}

	expectNoBatch(t, out)
}

// Test_HandleRelation_DriftDecidedOnlyByOwnRelation pins adversarial-review
// should-fix 3 on the B1 drift policy, which the delivery-time decision now
// gives by construction: a drift is decided only by a DML of the drifted
// relation itself. An unrelated table's DML is emitted normally at its own LSN
// and its position does not carry the undecided shape — a restart from it,
// before the drifted table's own DML, must re-derive the drift and halt, never
// dedupe it (re-review should-fix).
func Test_HandleRelation_DriftDecidedOnlyByOwnRelation(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	orders := &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1)},
	}
	_, _ = relate(ctx, t, h, orders, 100)
	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 150)
	_, err := h.Handle(ctx, relMsg(shapeV2...), 0) // users drift, undecided
	is.NoErr(err)

	err = h.handleInsert(ctx, &pglogrepl.InsertMessage{
		RelationID: 2,
		Tuple: &pglogrepl.TupleData{
			ColumnNum: 1,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
			},
		},
	}, 300)
	is.NoErr(err)
	is.Equal(h.driftMarkerLSN.Load(), uint64(0)) // no marker yet

	batch := <-out
	is.Equal(len(batch), 1)
	is.Equal(batch[0].Metadata[MetadataSchemaDrift], "") // a normal record, not a marker
	p, err := position.ParseSDKPosition(batch[0].Position)
	is.NoErr(err)
	recLSN, err := p.LSN()
	is.NoErr(err)
	is.Equal(recLSN, pglogrepl.LSN(300))
	is.Equal(len(p.SchemaHistory["public.users"]), 1) // v1 only; v2 is undecided

	h2, _ := newHandlerWithOut(t, p, SchemaDriftPolicyHalt)
	kind, _ := relate(ctx, t, h2, relMsg(shapeV2...), 500)
	is.Equal(kind, driftAcrossRestart) // re-derives the drift; never silent admission

	// The users DML fires the marker at its own LSN and is itself skipped (D4).
	err = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 400)
	is.NoErr(err)
	is.Equal(h.driftMarkerLSN.Load(), uint64(400))

	batch = <-out
	is.Equal(len(batch), 1)
	is.Equal(batch[0].Metadata[MetadataSchemaDrift], "true")
	is.Equal(batch[0].Metadata[MetadataSchemaDriftTable], "public.users")
	p, err = position.ParseSDKPosition(batch[0].Position)
	is.NoErr(err)
	markerLSN, err := p.LSN()
	is.NoErr(err)
	is.Equal(markerLSN, pglogrepl.LSN(400))
	is.Equal(len(p.SchemaHistory["public.users"]), 2)
}

// Test_MaybeArmDriftHalt_AckGating pins the D3 boundary on the handler: acks
// below the marker LSN do not arm, the marker's own ack does, and a re-ordered
// ack at or past it also arms (defensive `>=`).
func Test_MaybeArmDriftHalt_AckGating(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, _ := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
	// The first DML emits the marker at LSN 200.
	_ = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 200)

	h.maybeArmDriftHalt(150, internal.ChangeKey{}, nil) // below the marker: must not arm
	is.True(!h.driftHaltArmed.Load())

	ch := h.driftHaltCh
	h.maybeArmDriftHalt(200, internal.ChangeKey{}, nil) // the marker's own ack
	is.True(h.driftHaltArmed.Load())
	select {
	case <-ch:
	default:
		t.Fatal("driftHaltCh not closed on arming")
	}

	// Second arming is a no-op: the channel is closed exactly once, the error
	// is stable.
	h.maybeArmDriftHalt(999, internal.ChangeKey{}, nil)
	msg := h.driftHaltError().Error()
	is.True(strings.HasPrefix(msg, ErrorCodeSchemaDriftHalt+": "))
}

// Test_MaybeArmDriftHalt_ArmsOnKeyNotLSN pins the arming rule of #334's review
// at the unit level. The marker rides a change with a LOW LSN (200) in a
// transaction that commits late (commit 0x500). A change from a transaction
// that committed earlier carries a HIGHER LSN (900): acking it must not arm.
// The marker's own ack arms even though the LSN it carries is lower than the
// one acked before.
func Test_MaybeArmDriftHalt_ArmsOnKeyNotLSN(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, _ := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	cur := internal.ChangeKey{CommitLSN: 0x400, Seq: 1}
	h.changeKey = func() internal.ChangeKey { return cur }

	_, _ = relate(ctx, t, h, relMsg(shapeV1...), 100)
	_, _ = h.Handle(ctx, relMsg(shapeV2...), 0)
	cur = internal.ChangeKey{CommitLSN: 0x500, Seq: 1}
	// The first DML with the new shape emits the marker at LSN 200.
	_ = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 200)
	is.True(h.driftMarkerPending())

	// An earlier-committed transaction's change: higher LSN, lower key.
	h.maybeArmDriftHalt(900, internal.ChangeKey{CommitLSN: 0x400, Seq: 3}, nil)
	is.True(!h.driftHaltArmed.Load())

	// The marker's own ack arms, although its LSN (200) is below the LSN of
	// the change acked before it (900).
	h.maybeArmDriftHalt(200, internal.ChangeKey{CommitLSN: 0x500, Seq: 1}, nil)
	is.True(h.driftHaltArmed.Load())
}
