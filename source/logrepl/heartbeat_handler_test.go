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

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func heartbeatRelMsg(id uint32) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   id,
		Namespace:    DefaultHeartbeatSchema,
		RelationName: DefaultHeartbeatTable,
		Columns:      []*pglogrepl.RelationMessageColumn{relCol("slot_name", 25, -1), relCol("beat", 20, -1)},
	}
}

// Test_Handle_HeartbeatNeverEmitted pins Decision 4 of the B2 design doc: a
// heartbeat relation never enters schema history (so it never rides a
// position and can never trigger a drift halt), and a heartbeat change emits
// no record and reports no written LSN (so walWritten does not move), while
// its LSN becomes the heartbeat-observed LSN.
func Test_Handle_HeartbeatNeverEmitted(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	h.enableHeartbeat(DefaultHeartbeatSchema, DefaultHeartbeatTable)

	written, err := h.Handle(ctx, heartbeatRelMsg(42), 0)
	is.NoErr(err)
	is.Equal(written, pglogrepl.LSN(0))
	_, inHistory := h.basePosition.LastSchemaVersion(DefaultHeartbeatSchema + "." + DefaultHeartbeatTable)
	is.True(!inHistory) // the heartbeat table has no schema history

	for _, tc := range []struct {
		msg pglogrepl.Message
		lsn pglogrepl.LSN
	}{
		{&pglogrepl.InsertMessage{RelationID: 42}, 100},
		{&pglogrepl.UpdateMessage{RelationID: 42}, 200},
		{&pglogrepl.DeleteMessage{RelationID: 42}, 300},
	} {
		written, err := h.Handle(ctx, tc.msg, tc.lsn)
		is.NoErr(err)
		is.Equal(written, pglogrepl.LSN(0)) // a heartbeat is never an emitted record
		is.Equal(h.lastHeartbeatLSN(), tc.lsn)
	}
	is.True(!h.lastHeartbeatObserved().IsZero())

	// A changed heartbeat table shape (someone ALTERs it) is still not drift.
	recreated := heartbeatRelMsg(42)
	recreated.Columns = append(recreated.Columns, relCol("extra", 25, -1))
	_, err = h.Handle(ctx, recreated, 0)
	is.NoErr(err)
	is.True(!h.driftMarkerPending())
	is.True(h.driftPendingRel == nil)

	select {
	case batch := <-out:
		t.Fatalf("heartbeat changes must never be emitted, got %d records", len(batch))
	case <-time.After(50 * time.Millisecond):
	}
}

// Test_Handle_HeartbeatRecreatedTable: a dropped and recreated heartbeat table
// arrives under a new relation ID; the newest one is the heartbeat.
func Test_Handle_HeartbeatRecreatedTable(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, _ := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	h.enableHeartbeat(DefaultHeartbeatSchema, DefaultHeartbeatTable)

	_, err := h.Handle(ctx, heartbeatRelMsg(42), 0)
	is.NoErr(err)
	_, err = h.Handle(ctx, heartbeatRelMsg(43), 0)
	is.NoErr(err)

	written, err := h.Handle(ctx, &pglogrepl.UpdateMessage{RelationID: 43}, 500)
	is.NoErr(err)
	is.Equal(written, pglogrepl.LSN(0))
	is.Equal(h.lastHeartbeatLSN(), pglogrepl.LSN(500))
}

// Test_Handle_HeartbeatDisabled: with heartbeats off, a table that happens to
// carry the heartbeat name is an ordinary table (the pre-B2 behavior).
func Test_Handle_HeartbeatDisabled(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, _ := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)

	_, err := h.Handle(ctx, heartbeatRelMsg(42), 0)
	is.NoErr(err)
	_, inHistory := h.basePosition.LastSchemaVersion(DefaultHeartbeatSchema + "." + DefaultHeartbeatTable)
	is.True(inHistory) // handled like any other relation
	is.Equal(h.lastHeartbeatLSN(), pglogrepl.LSN(0))
}

// Test_Handle_HeartbeatDuringDriftPending: while a B1 drift marker is pending,
// heartbeat changes are still consumed as heartbeats. They neither emit nor
// skip-and-report an LSN, so they cannot open or close the flush gate, and
// they never fire the staged marker.
func Test_Handle_HeartbeatDuringDriftPending(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h, out := newHandlerWithOut(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	h.enableHeartbeat(DefaultHeartbeatSchema, DefaultHeartbeatTable)

	_, err := h.Handle(ctx, heartbeatRelMsg(42), 0)
	is.NoErr(err)
	_, _ = h.handleRelation(ctx, relMsg(shapeV1...), 0)
	_, _ = h.handleRelation(ctx, relMsg(shapeV2...), 0) // stages a drift on users

	written, err := h.Handle(ctx, &pglogrepl.UpdateMessage{RelationID: 42}, 150)
	is.NoErr(err)
	is.Equal(written, pglogrepl.LSN(0))
	is.True(h.driftPendingRel != nil) // still staged: a heartbeat never fires the marker
	is.True(!h.driftMarkerPending())

	select {
	case batch := <-out:
		t.Fatalf("expected no records, got %d", len(batch))
	default:
	}
}

func TestHeartbeatConfig_Validate(t *testing.T) {
	valid := HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: DefaultHeartbeatSchema, Table: DefaultHeartbeatTable}

	tests := []struct {
		name     string
		cfg      HeartbeatConfig
		tables   []string
		wantCode string
	}{
		{name: "disabled is always valid", cfg: HeartbeatConfig{}, tables: []string{DefaultHeartbeatTable}},
		{name: "valid", cfg: valid, tables: []string{"users"}},
		{name: "wildcard does not conflict", cfg: valid, tables: []string{"*"}},
		{
			name: "heartbeat table listed as a source table", cfg: valid,
			tables: []string{"users", DefaultHeartbeatTable}, wantCode: ErrorCodeHeartbeatTableConflict,
		},
		{
			name: "same name in another schema does not conflict",
			cfg:  HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: "ops", Table: "users"}, tables: []string{"users"},
		},
		{
			name: "non-positive interval",
			cfg:  HeartbeatConfig{Enabled: true, Schema: "public", Table: "hb"}, wantCode: ErrorCodeHeartbeatInvalidConfig,
		},
		{
			name: "empty table",
			cfg:  HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: "public"}, wantCode: ErrorCodeHeartbeatInvalidConfig,
		},
		{
			name:     "table name over 63 bytes (Postgres would truncate it)",
			cfg:      HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: "public", Table: strings.Repeat("h", 64)},
			wantCode: ErrorCodeHeartbeatInvalidConfig,
		},
		{
			name: "empty schema",
			cfg:  HeartbeatConfig{Enabled: true, Interval: time.Second, Table: "hb"}, wantCode: ErrorCodeHeartbeatInvalidConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			err := tt.cfg.Validate(tt.tables)
			if tt.wantCode == "" {
				is.NoErr(err)
				return
			}
			is.True(err != nil)
			is.True(strings.HasPrefix(err.Error(), tt.wantCode))
		})
	}
}
