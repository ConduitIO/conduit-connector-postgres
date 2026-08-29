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

// Test_HandleRelation_HaltEmitsMarker pins the D1 marker contract end to end:
// exact metadata, nil key/payload, position = buildPosition(first-new-shape
// DML LSN) carrying the new shape, and that DML's LSN returned for Handle (D2).
//
// pgoutput delivers the RelationMessage with WALStart 0 (verified 2026-08-29),
// so the marker is emitted by the first DML that uses the new shape, which
// always follows the relation message; that DML's LSN is the marker's position.
func Test_HandleRelation_HaltEmitsMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	kind, writtenLSN := h.handleRelation(ctx, relMsg(shapeV2...), 200)

	is.Equal(kind, driftInProcess)
	is.Equal(writtenLSN, pglogrepl.LSN(0)) // relation message carries no position; marker is staged

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

	wantMeta := map[string]string{
		MetadataSchemaDrift:          "true",
		MetadataSchemaDriftTable:     "public.users",
		MetadataSchemaDriftLSN:       "0/D2",
		MetadataSchemaDriftPolicy:    "halt",
		MetadataSchemaDriftNarrowing: "false", // ADD COLUMN is compatible; D1 wants the explicit value
	}
	is.True(strings.Contains(rec.Metadata[MetadataSchemaDriftDiff], `column "age" added (type 23)`))
	is.Equal(rec.Metadata[MetadataSchemaDriftTable], wantMeta[MetadataSchemaDriftTable])
	is.Equal(rec.Metadata[MetadataSchemaDriftLSN], wantMeta[MetadataSchemaDriftLSN])
	is.Equal(rec.Metadata[MetadataSchemaDriftPolicy], wantMeta[MetadataSchemaDriftPolicy])
	is.Equal(rec.Metadata[MetadataSchemaDriftNarrowing], wantMeta[MetadataSchemaDriftNarrowing])

	// Position semantics: LastLSN is the first-new-shape DML LSN, and the
	// history it carries already contains the new shape (invariant-1-safe
	// boundary).
	p, err := position.ParseSDKPosition(rec.Position)
	is.NoErr(err)
	lsn, err := p.LSN()
	is.NoErr(err)
	is.Equal(lsn, pglogrepl.LSN(210))
	v, ok := p.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(v.FirstSeenLSN, pglogrepl.LSN(200).String())

	// D3: acked-gated — the error is stored, but not surfaced until the ack.
	is.True(!h.driftHaltArmed.Load())
	is.Equal(h.driftHaltError(), nil)
	h.maybeArmDriftHalt(lsn)
	is.True(h.driftHaltArmed.Load())
	is.True(strings.HasPrefix(h.driftHaltError().Error(), ErrorCodeSchemaDriftHalt))
	is.True(strings.Contains(h.driftHaltError().Error(), haltRevertTrap))
}

// Test_HandleRelation_EvolveAcceptsAdditive pins that evolve admits a purely
// additive change silently: no marker, no pending halt, version recorded.
func Test_HandleRelation_EvolveAcceptsAdditive(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	kind, writtenLSN := h.handleRelation(ctx, relMsg(shapeV2...), 200)

	is.Equal(kind, driftInProcess)
	is.Equal(writtenLSN, pglogrepl.LSN(0))
	is.True(!h.driftMarkerPending())
	select {
	case batch := <-out:
		t.Fatalf("evolve emitted records for an additive change: %v", batch)
	default:
	}
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 2)
}

// Test_HandleRelation_EvolveHaltsOnNarrowing pins that evolve still halts on an
// incompatible change (a drop here), with the narrowing metadata set.
func Test_HandleRelation_EvolveHaltsOnNarrowing(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)

	_, _ = h.handleRelation(ctx, relMsg(shapeV2...), 100)
	kind, writtenLSN := h.handleRelation(ctx, relMsg(shapeV1...), 200) // drop "age"

	is.Equal(kind, driftInProcess)
	is.Equal(writtenLSN, pglogrepl.LSN(0)) // staged, not yet emitted

	err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
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
	_, _ = h1.handleRelation(ctx, relMsg(shapeV1...), 100)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2, out := newHandlerWithOut(t, resumed, SchemaDriftPolicyHalt)

	kind, _ := h2.handleRelation(ctx, relMsg(shapeV2...), 200)
	is.Equal(kind, driftAcrossRestart)

	// The marker is emitted by the first DML using the new shape.
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
	_, _ = h1.handleRelation(ctx, relMsg(shapeV1...), 100)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2, _ := newHandlerWithOut(t, resumed, SchemaDriftPolicyHalt)

	_, _ = h2.handleRelation(ctx, relMsg(shapeV2...), 200)
	_ = h2.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 210)
	h2.maybeArmDriftHalt(210)

	msg := h2.driftHaltError().Error()
	is.True(strings.HasPrefix(msg, ErrorCodeSchemaDriftHalt+": "))
	is.True(strings.Contains(msg, "public.users"))
	is.True(strings.Contains(msg, "schema hash "))
	is.True(strings.Contains(msg, "0/64")) // prev.FirstSeenLSN
	is.True(strings.Contains(msg, haltRevertTrap))
	is.True(!strings.Contains(msg, "age")) // AC7: never fabricates a column diff
}

// Test_HandleRelation_StackedDDL_OneMarker pins FM8/AC9: a second DDL while a
// halt is pending records the version but emits no second marker and returns no
// LSN — exactly one marker per halt, and the second shape halts on restart.
func Test_HandleRelation_StackedDDL_OneMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	shapeV3 := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("email", 25, -1), relCol("age", 23, -1), relCol("city", 25, -1)}

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	kind, lsn := h.handleRelation(ctx, relMsg(shapeV2...), 200)
	is.Equal(kind, driftInProcess)
	is.Equal(lsn, pglogrepl.LSN(0)) // staged, not yet emitted
	is.Equal(h.driftMarkerLSN.Load(), uint64(0))

	// Second DDL before the marker is emitted/acked.
	kind, lsn = h.handleRelation(ctx, relMsg(shapeV3...), 300)
	is.Equal(kind, driftInProcess)
	is.Equal(lsn, pglogrepl.LSN(0)) // no second marker
	is.Equal(h.driftMarkerLSN.Load(), uint64(0))

	// The first DML emits exactly one marker, at its own LSN (D2).
	err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 310)
	is.NoErr(err)
	is.Equal(h.driftMarkerLSN.Load(), uint64(310))

	batch := <-out
	is.Equal(len(batch), 1)
	select {
	case batch := <-out:
		t.Fatalf("expected exactly one marker, got another batch: %v", batch)
	default:
	}
	// The second shape is recorded so it halts on restart.
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 3)
}

// Test_HandleRelation_SecondTableDrift_NoSecondMarker pins that drift in a
// second table while a halt is pending behaves like stacked DDL on the first:
// version recorded, no second marker, one halt.
func Test_HandleRelation_SecondTableDrift_NoSecondMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	_, lsn := h.handleRelation(ctx, relMsg(shapeV2...), 200)
	is.Equal(lsn, pglogrepl.LSN(0)) // staged, not yet emitted

	other := &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1)},
	}
	_, _ = h.handleRelation(ctx, other, 300)
	_, lsn = h.handleRelation(ctx, &pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("total", 1700, 655366)},
	}, 400)
	is.Equal(lsn, pglogrepl.LSN(0)) // FM8: second table's drift while one is staged

	// First DML emits the single staged marker, for the FIRST drifted table.
	err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 410)
	is.NoErr(err)

	batch := <-out
	is.Equal(len(batch), 1)
	is.Equal(batch[0].Metadata[MetadataSchemaDriftTable], "public.users")
}

// Test_HandleRelation_DMLSkippedAfterMarker pins D4 at the handler level: after
// the marker, DML for ANY relation is dropped, never emitted — including a
// message whose relation the handler has never seen (the skip must fire before
// any relation lookup).
func Test_HandleRelation_DMLSkippedAfterMarker(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	_, _ = h.handleRelation(ctx, relMsg(shapeV2...), 200)

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

	select {
	case batch := <-out:
		t.Fatalf("expected nothing after the marker, got %d records", len(batch))
	default:
	}
}

// Test_MaybeArmDriftHalt_AckGating pins the D3 boundary on the handler: acks
// below the marker LSN do not arm, the marker's own ack does, and a re-ordered
// ack at or past it also arms (defensive `>=`).
func Test_MaybeArmDriftHalt_AckGating(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, _ := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)
	_, _ = h.handleRelation(ctx, relMsg(shapeV2...), 200)
	// The first DML emits the marker at LSN 200.
	_ = h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1}, 200)

	h.maybeArmDriftHalt(150) // below the marker: must not arm
	is.True(!h.driftHaltArmed.Load())

	ch := h.driftHaltCh
	h.maybeArmDriftHalt(200) // the marker's own ack
	is.True(h.driftHaltArmed.Load())
	select {
	case <-ch:
	default:
		t.Fatal("driftHaltCh not closed on arming")
	}

	// Second arming is a no-op: the channel is closed exactly once, the error
	// is stable.
	h.maybeArmDriftHalt(999)
	msg := h.driftHaltError().Error()
	is.True(strings.HasPrefix(msg, ErrorCodeSchemaDriftHalt+": "))
}
