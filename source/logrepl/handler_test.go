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
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func TestHandler_Batching_BatchSizeReached(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second)
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
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, flushInterval)

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
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second)
	cancel()
	<-ctx.Done()
	underTest.addToBatch(ctx, newTestRecord(0))

	recs, gotRecs, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(context.Background(), time.Second)
	is.Equal(recs, nil)
	is.True(!gotRecs)
	is.Equal(err, context.DeadlineExceeded)
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
	h := NewCDCHandler(ctx, newRelationSetForToastTests(), map[string]string{"toast_test": "id"}, ch, false, 1, time.Hour)

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
	h := NewCDCHandler(ctx, newRelationSetForToastTests(), map[string]string{"toast_test": "id"}, ch, false, 1, time.Hour)

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
