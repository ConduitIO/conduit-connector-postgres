// Copyright © 2025 Meroxa, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestHandler_Batching_BatchSizeReached(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second, position.Position{}, SchemaDriftPolicyHalt)
	want := make([]opencdc.Record, 5)
	for i := 0; i < cap(want); i++ {
		rec := newTestRecord(i)
		underTest.addToBatch(ctx, rec)
		want[i] = rec
	}

	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
	is.True(gotRecs)
	is.Equal(recs, want)
}

// TestHandler_Batching_FlushInterval tests if the handler flushes
// a batch once the flush interval passes, even if the batch size is not reached.
func TestHandler_Batching_FlushInterval(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	flushInterval := time.Second
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, flushInterval, position.Position{}, SchemaDriftPolicyHalt)

	want := make([]opencdc.Record, 3)
	for i := 0; i < cap(want); i++ {
		rec := newTestRecord(i)
		underTest.addToBatch(ctx, rec)
		want[i] = rec
	}

	start := time.Now()
	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, 1200*time.Millisecond)

	is.NoErr(err)
	is.True(gotRecs)
	is.Equal(recs, want)
	is.True(time.Since(start) >= flushInterval)
}

func TestHandler_Batching_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second, position.Position{}, SchemaDriftPolicyHalt)
	cancel()
	<-ctx.Done()
	underTest.addToBatch(ctx, newTestRecord(0))

	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(context.Background(), time.Second)
	is.Equal(recs, nil)
	is.True(!gotRecs)
	is.Equal(err, context.DeadlineExceeded)
}

// TestCDCHandler_buildPosition_CarriesForwardWatermark asserts DBZ-3 acceptance
// criterion 11: every CDC-mode position (not just the first after handoff)
// carries the DBZ-3 carry-forward fields from the start position forward
// unchanged, with only Type/LastLSN updated per record, and stamps the current
// position format version. This is the regression test for the load-bearing wiring
// the design doc's "Position carry-forward is an implementation requirement"
// section calls out: before the fix, buildPosition minted a field-sparse
// Position{Type: CDC, LastLSN} and dropped SnapshotLowWatermarkLSN on every
// record. The test fails against that old behavior (watermark comes back empty)
// and passes with the fix.
func TestCDCHandler_buildPosition_CarriesForwardWatermark(t *testing.T) {
	is := is.New(t)

	const watermark = "0/1500000"
	// Construct the handler directly (white-box) to avoid needing a DB or the
	// flushing goroutine — buildPosition only reads basePosition.
	h := &CDCHandler{
		basePosition: position.Position{
			// A resumed connector is seeded with a snapshot-typed start position
			// that already carries the low watermark from a prior run.
			Type:                    position.TypeSnapshot,
			SnapshotLowWatermarkLSN: watermark,
		},
	}

	lsns := []pglogrepl.LSN{0x1500010, 0x1500020, 0x1500030}
	for _, lsn := range lsns {
		got, err := position.ParseSDKPosition(h.buildPosition(lsn))
		is.NoErr(err)

		// Per-record fields updated.
		is.Equal(got.Type, position.TypeCDC)
		is.Equal(got.LastLSN, lsn.String())
		// Carry-forward field preserved on EVERY record, not just the first.
		is.Equal(got.SnapshotLowWatermarkLSN, watermark)
		// Format version stamped.
		is.Equal(got.Version, position.CurrentPositionVersion)
		// Snapshot-phase cursor state is intentionally not carried into CDC positions.
		is.Equal(len(got.Snapshots), 0)
	}
}

// TestCDCHandler_HandoffReseed_FirstRunSameRun is the regression test for the
// load-bearing DBZ-3 Area 1 fix that slice 1 could not cover: the FIRST-run
// same-run snapshot->CDC handoff. On a first run the snapshot low watermark is
// captured only when the replication slot is created — which happens after the
// CDCHandler is constructed — so the handler starts with an EMPTY watermark
// (unlike the resumed case slice 1 tested, where the persisted position already
// carries it). Without the handoff re-seed, the watermark would ride snapshot
// records but be silently dropped the instant CDC took over, so criterion 11
// would hold only for a resume and regress intermittently on a first run.
//
// This exercises the exact seam CombinedIterator.useCDCIterator uses
// (CDCIterator.SetSnapshotLowWatermarkLSN -> setBasePositionLowWatermark) and
// asserts EVERY subsequent CDC position carries the watermark, not just the first.
func TestCDCHandler_HandoffReseed_FirstRunSameRun(t *testing.T) {
	is := is.New(t)

	const watermark = "0/1600000"
	// First-run handler: seeded from an initial (empty) start position exactly as
	// NewCDCIterator seeds it before the slot — and thus the watermark — exists.
	h := &CDCHandler{basePosition: position.Position{Type: position.TypeInitial}}

	// Before the handoff re-seed, buildPosition cannot carry a watermark. This
	// pins the gap the re-seed closes (and would catch a regression that seeded
	// the watermark too early or not at all).
	before, err := position.ParseSDKPosition(h.buildPosition(0x1600010))
	is.NoErr(err)
	is.Equal(before.SnapshotLowWatermarkLSN, "")

	// Simulate the handoff through the public iterator seam CombinedIterator uses.
	it := &CDCIterator{handler: h}
	it.SetSnapshotLowWatermarkLSN(watermark)

	// After the handoff, the watermark rides EVERY CDC position, not just the first.
	lsns := []pglogrepl.LSN{0x1600020, 0x1600030, 0x1600040}
	for _, lsn := range lsns {
		got, err := position.ParseSDKPosition(h.buildPosition(lsn))
		is.NoErr(err)
		is.Equal(got.Type, position.TypeCDC)
		is.Equal(got.LastLSN, lsn.String())
		is.Equal(got.SnapshotLowWatermarkLSN, watermark)
		is.Equal(got.Version, position.CurrentPositionVersion)
	}
}

// newRelationSetForToastTests builds a RelationSet with a single 3-column
// relation (id, small_col, big_col) registered under RelationID 1, used by
// the handleUpdate invariant-6 regression tests below.
func newRelationSetForToastTests() *internal.RelationSet {
	rs := internal.NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: "toast_test",
		ColumnNum:    3,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
			{Name: "small_col", DataType: pgtype.TextOID},
			{Name: "big_col", DataType: pgtype.TextOID},
		},
	})
	return rs
}

// TestHandler_HandleUpdate_UnchangedToastOmittedByDefault is a regression
// test for invariant 6 (schema handling never silently mangles data) at the
// handler level. With the default REPLICA IDENTITY, OldTuple carries only
// the key column, so a TOASTed column left unchanged by the UPDATE (DataType
// 'u' on the new tuple, no data at all in the old tuple) cannot be
// recovered. The resulting record must omit it from the payload rather than
// emit it as NULL.
func TestHandler_HandleUpdate_UnchangedToastOmittedByDefault(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, newRelationSetForToastTests(), map[string]string{"toast_test": "id"}, ch, false, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)

	msg := &pglogrepl.UpdateMessage{
		RelationID: 1,
		NewTuple: &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("changed")},
				{DataType: pglogrepl.TupleDataTypeToast}, // unchanged, no bytes on the wire
			},
		},
		// No OldTuple: default REPLICA IDENTITY, key column didn't change.
	}

	err := h.handleUpdate(ctx, msg, 1)
	is.NoErr(err)

	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
	is.True(gotRecs)
	is.Equal(len(recs), 1)

	after, ok := recs[0].Payload.After.(opencdc.StructuredData)
	is.True(ok)
	is.Equal(after["small_col"], "changed")

	// The defect under test: big_col must never surface as NULL.
	gotVal, isPresent := after["big_col"]
	is.True(!isPresent) // big_col must be omitted, not present-as-nil
	is.Equal(gotVal, nil)
}

// TestHandler_HandleUpdate_UnchangedToastBackfilledFromOldTuple covers the
// REPLICA IDENTITY FULL case: when the old tuple does carry the unchanged
// column's real value, handleUpdate backfills it into the payload instead of
// leaving the field omitted.
func TestHandler_HandleUpdate_UnchangedToastBackfilledFromOldTuple(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, newRelationSetForToastTests(), map[string]string{"toast_test": "id"}, ch, false, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)

	msg := &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: 'O',
		OldTuple: &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("original")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("original-big-value")},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("changed")},
				{DataType: pglogrepl.TupleDataTypeToast}, // unchanged, no bytes on the wire
			},
		},
	}

	err := h.handleUpdate(ctx, msg, 1)
	is.NoErr(err)

	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
	is.True(gotRecs)
	is.Equal(len(recs), 1)

	after, ok := recs[0].Payload.After.(opencdc.StructuredData)
	is.True(ok)
	is.Equal(after["small_col"], "changed")
	is.Equal(after["big_col"], "original-big-value")
}

func newTestRecord(id int) opencdc.Record {
	return opencdc.Record{
		Key: opencdc.StructuredData{
			"id": id,
		},
	}
}

// TestHandler_Truncate_WarningIsRateLimited: a storm of TRUNCATEs on one table
// must not flood the log, and the swallowed ones must be counted.
func TestHandler_Truncate_WarningIsRateLimited(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	rs := internal.NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{RelationID: 7, Namespace: "public", RelationName: "orders"})
	now := time.Unix(1000, 0)
	h := &CDCHandler{relationSet: rs, now: func() time.Time { return now }}

	trunc := &pglogrepl.TruncateMessage{RelationIDs: []uint32{7}}
	for i := uint64(0); i < 50; i++ {
		lsn, err := h.Handle(ctx, trunc, pglogrepl.LSN(100+i))
		is.NoErr(err)
		is.Equal(lsn, pglogrepl.LSN(0)) // logging only: nothing emitted, no LSN claimed
	}
	is.Equal(strings.Count(buf.String(), "TRUNCATE was NOT propagated"), 1)
	is.Equal(h.truncateWarns["public.orders"].suppressed, uint64(49))

	now = now.Add(truncateWarnInterval + time.Second)
	_, err := h.Handle(ctx, trunc, pglogrepl.LSN(200))
	is.NoErr(err)
	out := buf.String()
	is.Equal(strings.Count(out, "TRUNCATE was NOT propagated"), 2)
	is.True(strings.Contains(out, `"suppressed":49`)) // 1 warning + 49 swallowed = the 50 of the burst
	is.Equal(h.truncateWarns["public.orders"].suppressed, uint64(0))
}

// TestHandler_Truncate_StateIsBounded: the rate-limit state must not grow with
// the number of distinct tables ever truncated (relation ids are recycled by
// DROP/CREATE), only with those truncated within the last interval.
func TestHandler_Truncate_StateIsBounded(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	rs := internal.NewRelationSet()
	now := time.Unix(1000, 0)
	h := &CDCHandler{relationSet: rs, now: func() time.Time { return now }}

	for id := uint32(1); id <= 1000; id++ {
		rs.Add(&pglogrepl.RelationMessage{RelationID: id, Namespace: "public", RelationName: fmt.Sprintf("t%d", id)})
		_, err := h.Handle(ctx, &pglogrepl.TruncateMessage{RelationIDs: []uint32{id}}, pglogrepl.LSN(id))
		is.NoErr(err)
		now = now.Add(truncateWarnInterval) // each table's window has expired by the next truncate
	}
	is.Equal(strings.Count(buf.String(), "TRUNCATE was NOT propagated"), 1000)
	is.True(len(h.truncateWarns) <= 2) // not 1000
}

// TestHandler_Truncate_EvictionReportsPendingCount: a table whose window
// expired with swallowed TRUNCATEs must not lose that count silently when
// another table's warning evicts it.
func TestHandler_Truncate_EvictionReportsPendingCount(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	rs := internal.NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "orders"})
	rs.Add(&pglogrepl.RelationMessage{RelationID: 2, Namespace: "public", RelationName: "users"})
	now := time.Unix(1000, 0)
	h := &CDCHandler{relationSet: rs, now: func() time.Time { return now }}

	for i := uint64(0); i < 4; i++ { // 1 warning + 3 swallowed
		_, err := h.Handle(ctx, &pglogrepl.TruncateMessage{RelationIDs: []uint32{1}}, pglogrepl.LSN(10+i))
		is.NoErr(err)
	}
	now = now.Add(truncateWarnInterval + time.Second)
	_, err := h.Handle(ctx, &pglogrepl.TruncateMessage{RelationIDs: []uint32{2}}, pglogrepl.LSN(50))
	is.NoErr(err)

	var summaries []map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, "(summary)") {
			var m map[string]any
			is.NoErr(json.Unmarshal([]byte(line), &m))
			summaries = append(summaries, m)
		}
	}
	is.Equal(len(summaries), 1)
	is.Equal(summaries[0]["table"], "public.orders")
	is.Equal(summaries[0]["suppressed"], float64(3))
	_, stillThere := h.truncateWarns["public.orders"]
	is.True(!stillThere) // evicted
}
