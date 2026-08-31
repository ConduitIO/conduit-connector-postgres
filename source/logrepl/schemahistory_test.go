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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// relMsg builds a RelationMessage for public.users with the given columns.
func relMsg(cols ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns:      cols,
	}
}

func relCol(name string, dt uint32, tm int32) *pglogrepl.RelationMessageColumn {
	return &pglogrepl.RelationMessageColumn{Name: name, DataType: dt, TypeModifier: tm}
}

// newHandlerWithPosition builds a handler as the connector does on start, with
// the position it resumed from and the drift policy in effect.
func newHandlerWithPosition(t *testing.T, p position.Position, policy SchemaDriftPolicy) *CDCHandler {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	out := make(chan []opencdc.Record, 16)
	return NewCDCHandler(ctx, internal.NewRelationSet(), map[string]string{"users": "id"},
		out, false, 1, time.Hour, p, policy)
}

var (
	shapeV1 = []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("email", 25, -1)}
	shapeV2 = []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("email", 25, -1), relCol("age", 23, -1)}
)

// Test_HandleRelation_FirstSightIsNotDrift pins that a fresh connector does not
// report its very first RelationMessage as drift. Getting this wrong would mean
// every pipeline start emits a schema-drift warning — and once step 3 makes halt
// the default, every pipeline start would refuse to run.
func Test_HandleRelation_FirstSightIsNotDrift(t *testing.T) {
	is := is.New(t)
	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	kind, _ := h.handleRelation(context.Background(), relMsg(shapeV1...), 100)
	is.Equal(kind, driftInitial)

	// The shape is now durable.
	v, ok := h.basePosition.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(v.FirstSeenLSN, pglogrepl.LSN(100).String())
}

// Test_HandleRelation_RepeatIsSilent pins that a re-sent RelationMessage for an
// unchanged shape reports nothing. Postgres re-sends on reconnect and when a new
// subscriber attaches; treating those as drift would make the mechanism cry wolf
// on every reconnect.
func Test_HandleRelation_RepeatIsSilent(t *testing.T) {
	is := is.New(t)
	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	ctx := context.Background()

	kind, _ := h.handleRelation(ctx, relMsg(shapeV1...), 100)
	is.Equal(kind, driftInitial)
	kind, _ = h.handleRelation(ctx, relMsg(shapeV1...), 200)
	is.Equal(kind, driftNone)
	kind, _ = h.handleRelation(ctx, relMsg(shapeV1...), 300)
	is.Equal(kind, driftNone)

	// And no duplicate versions accumulated, which would eventually prune away
	// the older shapes that carry the drift signal.
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 1)
}

// Test_HandleRelation_InProcessDrift pins the step-1 path still fires: a DDL
// applied while the connector is running produces a fully described diff. It
// also pins the re-review should-fix on the B1 drift policy: the staged shape
// is held in the staging snapshot only, never committed to the live history at
// the sighting — a sighting-time commit would leak the shape into unrelated
// records' positions and dedupe the drift away on a restart before the drifted
// table's own DML.
func Test_HandleRelation_InProcessDrift(t *testing.T) {
	is := is.New(t)
	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	ctx := context.Background()

	kind, _ := h.handleRelation(ctx, relMsg(shapeV1...), 100)
	is.Equal(kind, driftInitial)
	kind, _ = h.handleRelation(ctx, relMsg(shapeV2...), 200)
	is.Equal(kind, driftInProcess)
	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), 1) // v2 not leaked: live history stays [v1]
	is.Equal(len(h.driftPendingHistory["public.users"]), 2)        // the staged snapshot carries [v1, v2]
}

// Test_HandleRelation_DriftAcrossRestart is the whole point of this change.
//
// A DDL applied while the connector was DOWN is invisible to the in-memory
// diff — the new process's relation cache is empty, so the first
// RelationMessage after the restart has nothing to compare against and is
// accepted as ground truth. Only the durable history catches it.
//
// Mutation check: deleting the SchemaHistory carry-forward, or the
// LastSchemaVersion comparison, turns this result into driftInitial.
func Test_HandleRelation_DriftAcrossRestart(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	// Run 1: connector sees shapeV1 and checkpoints.
	h1 := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	kind, _ := h1.handleRelation(ctx, relMsg(shapeV1...), 100)
	is.Equal(kind, driftInitial)
	checkpoint := h1.buildPosition(150)

	// Connector stops. Someone runs ALTER TABLE users ADD COLUMN age int.

	// Run 2: fresh process, empty relation cache, resumes from the checkpoint.
	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2 := newHandlerWithPosition(t, resumed, SchemaDriftPolicyHalt)

	kind, _ = h2.handleRelation(ctx, relMsg(shapeV2...), 200)
	is.Equal(kind, driftAcrossRestart)
}

// Test_HandleRelation_NoDriftAcrossCleanRestart is the counterpart: restarting
// with an UNCHANGED schema must be silent. A mechanism that flagged every
// restart as drift would be worse than none, because operators would learn to
// ignore it.
func Test_HandleRelation_NoDriftAcrossCleanRestart(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h1 := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	_, _ = h1.handleRelation(ctx, relMsg(shapeV1...), 100)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2 := newHandlerWithPosition(t, resumed, SchemaDriftPolicyHalt)

	kind, _ := h2.handleRelation(ctx, relMsg(shapeV1...), 200)
	is.Equal(kind, driftNone)
}

// Test_HandleRelation_LegacyPositionSeedsWithoutDrift pins the upgrade path. A
// position written before this change carries no history, so the first
// RelationMessage after upgrading has nothing to compare against and must be
// recorded as initial rather than reported as drift.
func Test_HandleRelation_LegacyPositionSeedsWithoutDrift(t *testing.T) {
	is := is.New(t)

	legacy, err := position.ParseSDKPosition([]byte(`{"type":2,"last_lsn":"0/16B3748"}`))
	is.NoErr(err)
	is.Equal(len(legacy.SchemaHistory), 0)

	h := newHandlerWithPosition(t, legacy, SchemaDriftPolicyHalt)
	kind, _ := h.handleRelation(context.Background(), relMsg(shapeV2...), 100)
	is.Equal(kind, driftInitial)
}

// Test_BuildPosition_CarriesSchemaHistoryOnEveryRecord is the regression test
// for the failure mode buildPosition's own comment warns about, applied to the
// new field.
//
// The carry-forward has to be unconditional per record: positions are
// checkpointed per batch and a restart lands on whichever one was last
// persisted. If ANY single CDC position dropped SchemaHistory, a restart landing
// on that position would resume with an empty history and silently lose drift
// detection — intermittently, which is the worst kind.
//
// Mutation check: removing the SchemaHistory line from buildPosition fails this.
func Test_BuildPosition_CarriesSchemaHistoryOnEveryRecord(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 100)

	for lsn := range pglogrepl.LSN(20) {
		p, err := position.ParseSDKPosition(h.buildPosition(lsn + 101))
		is.NoErr(err)

		v, ok := p.LastSchemaVersion("public.users")
		is.True(ok) // every emitted position must carry the history
		is.Equal(v.FirstSeenLSN, pglogrepl.LSN(100).String())
	}
}

// Test_SchemaHistory_StaysBounded pins that a table churning through many shapes
// cannot grow the position without limit. Positions are written on every batch,
// so an unbounded history is a real operational hazard, not a theoretical one.
func Test_SchemaHistory_StaysBounded(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	// Evolve policy, not halt: each iteration is a typeMod-only change on an
	// int4 column, which the Avro compatibility judgement (Q2) treats as
	// preserved, so evolve accepts it without emitting a marker. Under halt the
	// loop would emit a marker per change and block on the unbuffered consumer.
	h := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyEvolve)

	// Counters are typed rather than converted from the loop index: a
	// int -> int32/uint64 conversion trips gosec's overflow check, and silencing
	// it would be noise for a bound of 50.
	var typeMod int32
	lsn := pglogrepl.LSN(100)
	for range 50 {
		cols := append([]*pglogrepl.RelationMessageColumn{relCol("id", 23, -1)},
			relCol("churn", 23, typeMod))
		_, _ = h.handleRelation(ctx, relMsg(cols...), lsn)
		typeMod++
		lsn++
	}

	is.Equal(len(h.basePosition.SchemaHistory["public.users"]), position.DefaultSchemaHistoryVersions)

	// And the retained window ends at the most recent shape.
	last, ok := h.basePosition.LastSchemaVersion("public.users")
	is.True(ok)
	is.Equal(last.FirstSeenLSN, pglogrepl.LSN(149).String())
}
