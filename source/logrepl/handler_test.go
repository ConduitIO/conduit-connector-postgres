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
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestHandler_Batching_BatchSizeReached(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second, position.Position{})
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
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, flushInterval, position.Position{})

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
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second, position.Position{})
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

func newTestRecord(id int) opencdc.Record {
	return opencdc.Record{
		Key: opencdc.StructuredData{
			"id": id,
		},
	}
}
