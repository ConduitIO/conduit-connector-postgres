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

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// logrepl.requireExistingSlot (#341 S13). Before it, a pipeline with a
// position whose slot had been dropped silently got a new slot at the
// current WAL and skipped everything since its position.

type requireSlotFixture struct {
	ctx              context.Context
	logs             *lockedBuffer
	conn             *pgx.Conn
	table, slot, pub string
}

func newRequireSlotFixture(t *testing.T) *requireSlotFixture {
	t.Helper()
	is := is.New(t)
	logs := &lockedBuffer{}
	ctx := zerolog.New(logs).WithContext(context.Background())
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	suffix := time.Now().UnixNano() % 100000000
	f := &requireSlotFixture{ctx: ctx, logs: logs, conn: conn,
		table: fmt.Sprintf("rs_%d", suffix), slot: fmt.Sprintf("rs_slot_%d", suffix), pub: fmt.Sprintf("rs_pub_%d", suffix)}
	_, err := conn.Exec(ctx, "CREATE TABLE "+f.table+" (id bigint primary key, v text)")
	is.NoErr(err)
	t.Cleanup(func() {
		bg := context.Background()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			var exists bool
			_ = conn.QueryRow(bg, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", f.slot).Scan(&exists)
			if !exists {
				break
			}
			if _, err := conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", f.slot); err == nil {
				break
			}
		}
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+f.pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+f.table)
	})
	return f
}

func (f *requireSlotFixture) open(t *testing.T, require string, pos opencdc.Position) (sdk.Source, error) {
	t.Helper()
	cfg := map[string]string{
		"url": test.RepmgrConnString, "tables": f.table, "cdcMode": "logrepl", "snapshotMode": "never",
		"logrepl.slotName": f.slot, "logrepl.publicationName": f.pub, "logrepl.autoCleanup": "false",
		"logrepl.withAvroSchema":         "false",
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}
	if require != "" {
		cfg["logrepl.requireExistingSlot"] = require
	}
	src := Connector.NewSource()
	if err := sdk.Util.ParseConfig(f.ctx, cfg, src.Config(), Connector.NewSpecification().SourceParams); err != nil {
		t.Fatal(err)
	}
	if err := src.Open(f.ctx, pos); err != nil {
		_ = src.Teardown(context.Background())
		return nil, err
	}
	return src, nil
}

func (f *requireSlotFixture) slotExists(t *testing.T) bool {
	t.Helper()
	exists, err := logrepl.SlotExists(f.ctx, f.conn, f.slot)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

// positionAfterOneRecord runs the pipeline once (creating the slot and
// publication), and returns the acked position of one record.
func (f *requireSlotFixture) positionAfterOneRecord(t *testing.T) opencdc.Position {
	t.Helper()
	is := is.New(t)
	src, err := f.open(t, "", nil)
	is.NoErr(err)
	_, err = f.conn.Exec(f.ctx, "INSERT INTO "+f.table+" VALUES (1, 'x')")
	is.NoErr(err)
	_, recs := readIDs(f.ctx, t, src, 1)
	is.NoErr(src.Ack(f.ctx, recs[0].Position))
	is.NoErr(src.Teardown(context.Background()))
	return recs[0].Position
}

func (f *requireSlotFixture) dropSlot(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		_, err := f.conn.Exec(f.ctx, "SELECT pg_drop_replication_slot($1)", f.slot)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
}

// The regression: unset, a position, the slot dropped. This used to create
// a new slot and resume from the current WAL.
func TestRequireExistingSlot_DefaultRefusesForAPosition(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	pos := f.positionAfterOneRecord(t)
	f.dropSlot(t)

	_, err := f.open(t, "", pos)
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), logrepl.ErrorCodeSlotMissing+": "))
	is.True(!f.slotExists(t)) // nothing created
}

func TestRequireExistingSlot_DefaultCreatesForANewPipeline(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	src, err := f.open(t, "", nil)
	is.NoErr(err)
	is.NoErr(src.Teardown(context.Background()))
	is.True(f.slotExists(t))
}

func TestRequireExistingSlot_TrueRefusesEvenForANewPipeline(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	_, err := f.open(t, "true", nil)
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), logrepl.ErrorCodeSlotMissing+": "))
	is.True(!f.slotExists(t))
}

func TestRequireExistingSlot_FalseRecreatesWithAWarning(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	pos := f.positionAfterOneRecord(t)
	f.dropSlot(t)

	src, err := f.open(t, "false", pos)
	is.NoErr(err)
	is.NoErr(src.Teardown(context.Background()))
	is.True(f.slotExists(t))
	is.True(strings.Contains(f.logs.String(), logrepl.ErrorCodeSlotRecreated))
}

func TestRequireExistingSlot_PublicationMissing(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	pos := f.positionAfterOneRecord(t)
	_, err := f.conn.Exec(f.ctx, "DROP PUBLICATION "+f.pub)
	is.NoErr(err)

	_, err = f.open(t, "", pos)
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), logrepl.ErrorCodePublicationMissing+": "))
	var exists bool
	is.NoErr(f.conn.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", f.pub).Scan(&exists))
	is.True(!exists)
}

// With the slot and publication present, a restart from a position is
// unchanged.
func TestRequireExistingSlot_DefaultResumesNormally(t *testing.T) {
	is := is.New(t)
	f := newRequireSlotFixture(t)
	pos := f.positionAfterOneRecord(t)
	src, err := f.open(t, "", pos)
	is.NoErr(err)
	_, err = f.conn.Exec(f.ctx, "INSERT INTO "+f.table+" VALUES (2, 'y')")
	is.NoErr(err)
	ids, _ := readIDs(f.ctx, t, src, 1)
	is.Equal(ids, []string{"2"})
	is.NoErr(src.Teardown(context.Background()))
}

func TestMustUseExistingSlot(t *testing.T) {
	is := is.New(t)
	yes, no := true, false
	is.True(!logrepl.MustUseExistingSlot(nil, false))
	is.True(logrepl.MustUseExistingSlot(nil, true))
	is.True(logrepl.MustUseExistingSlot(&yes, false))
	is.True(!logrepl.MustUseExistingSlot(&no, true))
}
