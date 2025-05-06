// Copyright © 2022 Meroxa, Inc.
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	is := is.New(t)

	errs := Config{
		Tables: []string{
			"t1", "t2", "t3", "t4",
		},
		TableKeys: map[string]string{
			"t1": "k1", "t4": "k4",
		},
	}.Validate()

	is.Equal(errs, errors.Join(
		errors.New(`missing key for table "t2"`),
		errors.New(`missing key for table "t3"`),
	))
}

func TestCombinedIterator_New(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	t.Run("fails to parse initial position", func(t *testing.T) {
		is := is.New(t)

		_, err := NewCombinedIterator(ctx, nil, Config{
			Position: opencdc.Position(`{`),
		})
		is.Equal(err.Error(), "failed to create logrepl iterator: invalid position: unexpected end of JSON input")
	})

	t.Run("snapshot and cdc", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        opencdc.Position{},
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)

		is.True(i.snapshotIterator != nil)
		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.snapshotIterator)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})

	t.Run("initial cdc only", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        opencdc.Position{},
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    false,
		})
		is.NoErr(err)

		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.cdcIterator)
		is.Equal(i.snapshotIterator, nil)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})

	t.Run("position cdc only", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        opencdc.Position(`{"type":2, "last_lsn":"0/0"}`),
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)

		is.True(i.cdcIterator != nil)
		is.Equal(i.activeIterator, i.cdcIterator)
		is.Equal(i.snapshotIterator, nil)

		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(context.Background(), CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})
}

func TestCombinedIterator_NextN(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, time.Second*120)
	defer cancel()

	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	i, err := NewCombinedIterator(ctx, pool, Config{
		Position:        opencdc.Position{},
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
	})
	is.NoErr(err)

	// Add a record to the table for CDC mode testing
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, column1, column2, column3, column4, column5, column6, column7)
			VALUES (6, 'bizz', 1010, false, 872.2, 101, '{"foo12": "bar12"}', '{"foo13": "bar13"}')`,
		table,
	))
	is.NoErr(err)

	var lastPos opencdc.Position
	expectedRecords := testRecords()

	t.Run("invalid_n_value", func(t *testing.T) {
		is := is.New(t)
		_, err := i.NextN(ctx, 0)
		is.True(err != nil)
		is.True(err.Error() == "n must be greater than 0, got 0")

		_, err = i.NextN(ctx, -1)
		is.True(err != nil)
		is.True(err.Error() == "n must be greater than 0, got -1")
	})

	t.Run("nextN_snapshot_batch", func(t *testing.T) {
		is := is.New(t)

		// Request 3 records in batch (snapshot mode)
		records, err := i.NextN(ctx, 3)
		is.NoErr(err)
		is.True(len(records) > 0)

		for _, r := range records {
			pos, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			is.Equal(pos.Type, position.TypeSnapshot)

			// check it's a valid record with an id
			data := r.Payload.After.(opencdc.StructuredData)
			_, hasID := data["id"]
			is.True(hasID)

			is.NoErr(i.Ack(ctx, r.Position))
		}
	})

	t.Run("nextN_snapshot_to_cdc_transition", func(t *testing.T) {
		is := is.New(t)

		transitionComplete := false
		retryCount := 0
		maxRetries := 10

		for retryCount < maxRetries && !transitionComplete {
			// Request more records - we might get remaining snapshot records
			records, err := i.NextN(ctx, 2)
			is.NoErr(err)

			if len(records) == 0 {
				retryCount++
				continue
			}

			for _, r := range records {
				pos, err := position.ParseSDKPosition(r.Position)
				is.NoErr(err)

				if pos.Type == position.TypeCDC {
					lsn, err := pos.LSN()
					is.NoErr(err)
					is.True(lsn != 0)

					// Store position for next test
					lastPos = r.Position
					transitionComplete = true
				}

				is.NoErr(i.Ack(ctx, r.Position))
			}

			retryCount++
		}

		is.True(transitionComplete)
		if !transitionComplete {
			t.Fatalf("Failed to transition from snapshot to CDC mode")
		}
		// interrupt repl connection - handle case when connection might already be closed
		var terminated bool
		err = pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE
			query ILIKE '%%CREATE_REPLICATION_SLOT %s%%' and pid <> pg_backend_pid()
		`,
			table,
		)).Scan(&terminated)

		is.NoErr(i.Teardown(ctx))
	})

	t.Run("nextN_connector_resume_cdc", func(t *testing.T) {
		is := is.New(t)

		pos, err := position.ParseSDKPosition(lastPos)
		is.NoErr(err)
		is.Equal(pos.Type, position.TypeCDC)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        lastPos,
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    false,
		})
		is.NoErr(err)

		// Verify we're in CDC mode
		cdcMode := i.activeIterator == i.cdcIterator
		is.True(cdcMode)

		// Insert two more records for testing CDC batch
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, column1, column2, column3, column4, column5, column6, column7)
				VALUES (7, 'buzz', 10101, true, 121.9, 51, '{"foo7": "bar7"}', '{"foo8": "bar8"}')`,
			table,
		))
		is.NoErr(err)

		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, column1, column2, column3, column4, column5, column6, column7)
				VALUES (8, 'fizz', 20202, false, 232.8, 62, '{"foo9": "bar9"}', '{"foo10": "bar10"}')`,
			table,
		))
		is.NoErr(err)

		// Request 2 records in CDC mode
		records := make([]opencdc.Record, 0, 2)
		var retries int
		maxRetries := 10
		for retries < maxRetries {
			records, err = i.NextN(ctx, 2)
			is.NoErr(err)

			if len(records) > 0 {
				t.Logf("Got %d records after %d retries", len(records), retries)
				break
			}

			t.Logf("No CDC records returned, retry %d/%d", retries+1, maxRetries)
			retries++
		}

		is.True(len(records) > 0)

		if len(records) > 0 {
			pos, err := position.ParseSDKPosition(records[0].Position)
			is.NoErr(err)
			is.Equal(pos.Type, position.TypeCDC)

			lsn, err := pos.LSN()
			is.NoErr(err)
			is.True(lsn != 0)

			is.Equal("", cmp.Diff(
				expectedRecords[6],
				records[0].Payload.After.(opencdc.StructuredData),
			))

			is.NoErr(i.Ack(ctx, records[0].Position))
		}
		is.NoErr(i.Teardown(ctx))
	})
	t.Run("nextN_context_cancellation", func(t *testing.T) {
		is := is.New(t)

		i, err := NewCombinedIterator(ctx, pool, Config{
			Position:        opencdc.Position{},
			Tables:          []string{table},
			TableKeys:       map[string]string{table: "id"},
			PublicationName: table,
			SlotName:        table,
			WithSnapshot:    true,
		})
		is.NoErr(err)

		cancelCtx, cancelFn := context.WithCancel(ctx)
		cancelFn()

		// Request should fail with context canceled
		_, err = i.NextN(cancelCtx, 2)
		is.True(errors.Is(err, context.Canceled))

		is.NoErr(i.Teardown(ctx))
	})

	is.NoErr(Cleanup(context.Background(), CleanupConfig{
		URL:             pool.Config().ConnString(),
		SlotName:        table,
		PublicationName: table,
	}))
}

func testRecords() []opencdc.StructuredData {
	return []opencdc.StructuredData{
		{},
		{
			"id":               int64(1),
			"key":              []uint8("1"),
			"column1":          "foo",
			"column2":          int32(123),
			"column3":          false,
			"column4":          12.2,
			"column5":          int64(4),
			"column6":          []byte(`{"foo": "bar"}`),
			"column7":          []byte(`{"foo": "baz"}`),
			"UppercaseColumn1": int32(1),
		},
		{
			"id":               int64(2),
			"key":              []uint8("2"),
			"column1":          "bar",
			"column2":          int32(456),
			"column3":          true,
			"column4":          13.42,
			"column5":          int64(8),
			"column6":          []byte(`{"foo": "bar"}`),
			"column7":          []byte(`{"foo": "baz"}`),
			"UppercaseColumn1": int32(2),
		},
		{
			"id":               int64(3),
			"key":              []uint8("3"),
			"column1":          "baz",
			"column2":          int32(789),
			"column3":          false,
			"column4":          nil,
			"column5":          int64(9),
			"column6":          []byte(`{"foo": "bar"}`),
			"column7":          []byte(`{"foo": "baz"}`),
			"UppercaseColumn1": int32(3),
		},
		{
			"id":               int64(4),
			"key":              []uint8("4"),
			"column1":          nil,
			"column2":          nil,
			"column3":          nil,
			"column4":          91.1,
			"column5":          nil,
			"column6":          nil,
			"column7":          nil,
			"UppercaseColumn1": nil,
		},
		{
			"id":               int64(6),
			"key":              nil,
			"column1":          "bizz",
			"column2":          int32(1010),
			"column3":          false,
			"column4":          872.2,
			"column5":          int64(101),
			"column6":          []byte(`{"foo12": "bar12"}`),
			"column7":          []byte(`{"foo13": "bar13"}`),
			"UppercaseColumn1": nil,
		},
		{
			"id":               int64(7),
			"key":              nil,
			"column1":          "buzz",
			"column2":          int32(10101),
			"column3":          true,
			"column4":          121.9,
			"column5":          int64(51),
			"column6":          []byte(`{"foo7": "bar7"}`),
			"column7":          []byte(`{"foo8": "bar8"}`),
			"UppercaseColumn1": nil,
		},
	}
}
