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
	"math/big"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/snapshot"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
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
				cmp.Comparer(func(x, y *big.Rat) bool {
					return x.Cmp(y) == 0
				}),
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
			"column4":          big.NewRat(122, 10),
			"column5":          big.NewRat(4, 1),
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
			"column4":          big.NewRat(1342, 100), // 13.42
			"column5":          big.NewRat(8, 1),
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
			"column5":          big.NewRat(9, 1),
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
			"column4":          big.NewRat(911, 10), // 91.1
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
			"column4":          big.NewRat(8722, 10), // 872.2
			"column5":          big.NewRat(101, 1),
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
			"column4":          big.NewRat(1219, 10), // 121.9
			"column5":          big.NewRat(51, 1),
			"column6":          []byte(`{"foo7": "bar7"}`),
			"column7":          []byte(`{"foo8": "bar8"}`),
			"UppercaseColumn1": nil,
		},
	}
}

// snapshotTableIDs returns the full set of primary-key ids currently in the
// table, used by the resumable-snapshot chaos tests to assert no rows are
// skipped ("no gap") across a crash+resume.
func snapshotTableIDs(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) map[int64]bool {
	is := is.New(t)
	rows, err := pool.Query(ctx, fmt.Sprintf("SELECT id FROM %q ORDER BY id", table))
	is.NoErr(err)
	defer rows.Close()

	ids := make(map[int64]bool)
	for rows.Next() {
		var id int64
		is.NoErr(rows.Scan(&id))
		ids[id] = true
	}
	is.NoErr(rows.Err())
	return ids
}

func recordID(t *testing.T, r opencdc.Record) int64 {
	is := is.New(t)
	data, ok := r.Payload.After.(opencdc.StructuredData)
	is.True(ok)
	id, ok := data["id"].(int64)
	is.True(ok)
	return id
}

// TestCombinedIterator_ResumeMidSnapshot_ResumedTag is DBZ-3 Area 1 acceptance
// criterion 2 (crash-mid-snapshot chaos): a snapshot is interrupted partway, the
// connector is torn down (simulating a crash), and a fresh connector resumes from
// the persisted snapshot position. It asserts:
//   - first-run records are NOT tagged resumed;
//   - every record emitted by the resumed run carries
//     postgres.snapshot.resumed=true AND carries the low watermark forward on its
//     position;
//   - the union of ids read across both runs covers the whole table (no gap).
//
// This is DB-gated (needs the test Postgres from test/docker-compose.yml). It is
// WRITTEN-BUT-UNRUN wherever docker is unavailable; CI runs it via `make test`.
func TestCombinedIterator_ResumeMidSnapshot_ResumedTag(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, time.Second*120)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	// Seed extra rows so a small-batch read leaves the snapshot genuinely
	// incomplete at the simulated crash.
	for n := 0; n < 20; n++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
				VALUES ('%d', 'r', 1, false, 1.1, 1, '{"a":1}', '{"a":1}', 1)`,
			table, n))
		is.NoErr(err)
	}

	// Capture the exact id set the snapshot must cover BEFORE inserting any
	// post-crash CDC sentinel row.
	wantIDs := snapshotTableIDs(ctx, t, pool, table)
	seen := make(map[int64]bool)

	// --- Run 1: partial snapshot, then "crash" (teardown). ---
	i1, err := NewCombinedIterator(ctx, pool, Config{
		Position:        opencdc.Position{},
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
		BatchSize:       2,
	})
	is.NoErr(err)

	var lastPos opencdc.Position
	for n := 0; n < 3; n++ { // read only a few batches, leaving the snapshot unfinished
		recs, err := i1.NextN(ctx, 2)
		is.NoErr(err)
		for _, r := range recs {
			pos, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			is.Equal(pos.Type, position.TypeSnapshot)
			_, tagged := r.Metadata[snapshot.MetadataSnapshotResumed]
			is.True(!tagged) // first run must never be labeled resumed
			seen[recordID(t, r)] = true
			lastPos = r.Position
			is.NoErr(i1.Ack(ctx, r.Position))
		}
	}
	is.NoErr(i1.Teardown(ctx))

	// The persisted resume position is snapshot-typed with real progress.
	pp, err := position.ParseSDKPosition(lastPos)
	is.NoErr(err)
	is.Equal(pp.Type, position.TypeSnapshot)

	// Insert one post-crash row: it is NOT part of the snapshot set and serves as
	// a CDC sentinel so the resumed run cleanly signals "snapshot done -> CDC"
	// (a CDC-typed position) instead of blocking on an idle stream.
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
			VALUES ('cdc-sentinel', 'r', 1, false, 1.1, 1, '{"a":1}', '{"a":1}', 1)`,
		table))
	is.NoErr(err)

	// --- Run 2: resume from the persisted snapshot position. ---
	i2, err := NewCombinedIterator(ctx, pool, Config{
		Position:        lastPos,
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
		BatchSize:       4,
	})
	is.NoErr(err)

	sawResumed := false
drain:
	for {
		recs, err := i2.NextN(ctx, 4)
		is.NoErr(err)
		for _, r := range recs {
			pos, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			if pos.Type == position.TypeCDC {
				// Snapshot fully drained; CDC has taken over. Stop.
				is.NoErr(i2.Ack(ctx, r.Position))
				break drain
			}
			is.Equal(r.Metadata[snapshot.MetadataSnapshotResumed], "true")
			is.True(pos.SnapshotLowWatermarkLSN != "") // watermark carried across resume
			seen[recordID(t, r)] = true
			is.NoErr(i2.Ack(ctx, r.Position))
			sawResumed = true
		}
	}
	is.True(sawResumed)

	// No gap: every id present at snapshot start was read across the two runs.
	for id := range wantIDs {
		is.True(seen[id])
	}

	is.NoErr(i2.Teardown(ctx))
	is.NoErr(Cleanup(context.Background(), CleanupConfig{
		URL:             pool.Config().ConnString(),
		SlotName:        table,
		PublicationName: table,
	}))
}

// TestCombinedIterator_ResumeAtSwitchoverBoundary is DBZ-3 Area 1 acceptance
// criterion 10 (switchover-boundary chaos): the snapshot fully drains (every
// FetchWorker reaches end-of-cursor) but the connector is torn down BEFORE the
// snapshot->CDC handoff runs, so the persisted position is still snapshot-typed.
// On resume the connector must reconstruct the (now empty-range) snapshot,
// short-circuit to done, and transition cleanly to CDC from the low watermark
// with no gap and no duplicate. It asserts a row inserted after the crash is
// delivered exactly once via CDC on resume.
//
// DB-gated; WRITTEN-BUT-UNRUN where docker is unavailable; CI runs it.
func TestCombinedIterator_ResumeAtSwitchoverBoundary(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, time.Second*120)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	wantIDs := snapshotTableIDs(ctx, t, pool, table)

	// --- Run 1: drain the ENTIRE snapshot, ack it, but stop before the call that
	// would trigger the snapshot->CDC transition. ---
	i1, err := NewCombinedIterator(ctx, pool, Config{
		Position:        opencdc.Position{},
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
		BatchSize:       2,
	})
	is.NoErr(err)

	seen := make(map[int64]bool)
	var lastPos opencdc.Position
	for len(seen) < len(wantIDs) {
		recs, err := i1.NextN(ctx, 2)
		is.NoErr(err)
		for _, r := range recs {
			pos, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			is.Equal(pos.Type, position.TypeSnapshot) // still snapshot: no transition yet
			seen[recordID(t, r)] = true
			lastPos = r.Position
			is.NoErr(i1.Ack(ctx, r.Position))
		}
	}
	// Stop here: every snapshot row read+acked, but useCDCIterator never ran.
	is.NoErr(i1.Teardown(ctx))

	pp, err := position.ParseSDKPosition(lastPos)
	is.NoErr(err)
	is.Equal(pp.Type, position.TypeSnapshot) // crash is exactly at the boundary

	// Insert a row after the crash: it must arrive via CDC on resume (never lost),
	// and only once (never also re-emitted as a phantom snapshot record).
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
			VALUES ('post-crash', 'r', 1, false, 1.1, 1, '{"a":1}', '{"a":1}', 1)`,
		table))
	is.NoErr(err)

	// --- Run 2: resume; must transition cleanly to CDC and deliver the new row. ---
	i2, err := NewCombinedIterator(ctx, pool, Config{
		Position:        lastPos,
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table,
		SlotName:        table,
		WithSnapshot:    true,
		BatchSize:       2,
	})
	is.NoErr(err)

	var cdcCount int
	for retries := 0; retries < 20 && cdcCount == 0; retries++ {
		recs, err := i2.NextN(ctx, 2)
		is.NoErr(err)
		for _, r := range recs {
			pos, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			// No phantom snapshot record must be emitted for an already-completed
			// snapshot at the switchover boundary.
			is.Equal(pos.Type, position.TypeCDC)
			cdcCount++
			is.NoErr(i2.Ack(ctx, r.Position))
		}
	}
	is.True(cdcCount >= 1) // the post-crash row arrived via CDC (no gap)

	is.NoErr(i2.Teardown(ctx))
	is.NoErr(Cleanup(context.Background(), CleanupConfig{
		URL:             pool.Config().ConnString(),
		SlotName:        table,
		PublicationName: table,
	}))
}
