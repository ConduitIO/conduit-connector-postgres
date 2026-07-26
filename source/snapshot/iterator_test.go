// Copyright © 2024 Meroxa, Inc.
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

package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

// Test_Iterator_buildRecord_ResumedTag is a white-box unit test (no DB) for the
// DBZ-3 Area 1 resumed-snapshot tag. When the iterator was constructed for a
// resume (SnapshotResumed=true), every emitted record must carry
// postgres.snapshot.resumed=true and carry the captured low watermark forward on
// its position; a first-run snapshot (SnapshotResumed=false) must NOT carry the
// tag, so a resumed record is distinguishable downstream and a first-run record
// is never mislabeled.
func Test_Iterator_buildRecord_ResumedTag(t *testing.T) {
	const watermark = "0/1A2B3C4"
	fd := FetchData{
		Key:      opencdc.StructuredData{"id": int64(1)},
		Payload:  opencdc.StructuredData{"id": int64(1), "name": "x"},
		Position: position.SnapshotPosition{LastRead: 1, SnapshotEnd: 10},
		Table:    "orders",
	}

	t.Run("resumed tags every record and carries watermark", func(t *testing.T) {
		is := is.New(t)
		i := &Iterator{
			conf: Config{SnapshotResumed: true},
			lastPosition: position.Position{
				Snapshots:               make(position.SnapshotPositions),
				SnapshotLowWatermarkLSN: watermark,
			},
		}

		rec := i.buildRecord(fd)
		is.Equal(rec.Metadata[MetadataSnapshotResumed], "true")
		is.Equal(rec.Metadata[opencdc.MetadataCollection], "orders")

		pos, err := position.ParseSDKPosition(rec.Position)
		is.NoErr(err)
		is.Equal(pos.Type, position.TypeSnapshot)
		is.Equal(pos.SnapshotLowWatermarkLSN, watermark) // watermark rides snapshot positions
	})

	t.Run("first run is not tagged resumed", func(t *testing.T) {
		is := is.New(t)
		i := &Iterator{
			conf:         Config{SnapshotResumed: false},
			lastPosition: position.Position{Snapshots: make(position.SnapshotPositions)},
		}

		rec := i.buildRecord(fd)
		_, ok := rec.Metadata[MetadataSnapshotResumed]
		is.True(!ok) // a first-run snapshot record must never be labeled resumed
	})
}

func Test_Iterator_NextN(t *testing.T) {
	var (
		ctx   = test.Context(t)
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = test.SetupTestTable(ctx, t, pool)
	)

	t.Run("success", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TableKeys: map[string]string{
				table: "id",
			},
			FetchSize: 2,
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		// Get first 2 records
		records, err := i.NextN(ctx, 2)
		is.NoErr(err)
		is.Equal(len(records), 2)
		for _, r := range records {
			is.Equal(r.Operation, opencdc.OperationSnapshot)
			is.Equal(r.Metadata[opencdc.MetadataCollection], table)
		}

		// Get the remaining 2 records
		records, err = i.NextN(ctx, 2)
		is.NoErr(err)
		is.Equal(len(records), 2)
		for _, r := range records {
			is.Equal(r.Operation, opencdc.OperationSnapshot)
			is.Equal(r.Metadata[opencdc.MetadataCollection], table)
		}

		// Ack all records
		for j := 1; j <= 4; j++ {
			err = i.Ack(ctx, nil)
			is.NoErr(err)
		}

		// Should return ErrIteratorDone
		_, err = i.NextN(ctx, 1)
		is.Equal(err, ErrIteratorDone)
	})

	t.Run("next waits for acks", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TableKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		// Get all 4 records in multiple calls since NextN is non-blocking
		var allRecords []opencdc.Record
		for len(allRecords) < 4 {
			records, err := i.NextN(ctx, 4)
			is.NoErr(err)
			allRecords = append(allRecords, records...)
		}
		is.Equal(len(allRecords), 4)

		// Only ack 3 records
		for j := 1; j <= 3; j++ {
			err = i.Ack(ctx, nil)
			is.NoErr(err)
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond*10)
		defer cancel()

		// No more records, but NextN blocks because we haven't acked all records
		_, err = i.NextN(ctxTimeout, 1)
		is.True(errors.Is(err, context.DeadlineExceeded))

		// Ack the last record
		err = i.Ack(ctx, nil)
		is.NoErr(err)

		// Now NextN won't block
		_, err = i.NextN(ctx, 1)
		is.Equal(err, ErrIteratorDone)
	})

	t.Run("context cancelled", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TableKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err = i.NextN(cancelCtx, 1)
		is.Equal(err.Error(), "iterator stopped: context canceled")
	})

	t.Run("tomb exited", func(t *testing.T) {
		is := is.New(t)
		cancelCtx, cancel := context.WithCancel(ctx)

		i, err := NewIterator(cancelCtx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TableKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		cancel()

		_, err = i.NextN(ctx, 1)
		is.True(errors.Is(err, context.Canceled))
	})

	t.Run("invalid n", func(t *testing.T) {
		is := is.New(t)

		i, err := NewIterator(ctx, pool, Config{
			Position: position.Position{}.ToSDKPosition(),
			Tables:   []string{table},
			TableKeys: map[string]string{
				table: "id",
			},
		})
		is.NoErr(err)
		defer func() {
			is.NoErr(i.Teardown(ctx))
		}()

		_, err = i.NextN(ctx, 0)
		is.True(err != nil)
		is.Equal(err.Error(), "n must be greater than 0, got 0")

		_, err = i.NextN(ctx, -1)
		is.True(err != nil)
		is.Equal(err.Error(), "n must be greater than 0, got -1")
	})
}
