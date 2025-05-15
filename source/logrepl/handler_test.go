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

func TestHandler_Batching_WaitForTimeout(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second)
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
	is.True(time.Since(start) >= time.Second)
}

func TestHandler_Batching_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	is := is.New(t)

	ch := make(chan []opencdc.Record, 1)
	underTest := NewCDCHandler(ctx, nil, nil, ch, false, 5, time.Second)
	cancel()
	<-ctx.Done()
	underTest.addToBatch(ctx, newTestRecord(0))

	_, recordReceived := <-ch
	is.True(!recordReceived)
}

func newTestRecord(id int) opencdc.Record {
	return opencdc.Record{
		Key: opencdc.StructuredData{
			"id": id,
		},
	}
}
