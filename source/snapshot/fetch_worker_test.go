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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

func Test_NewFetcher(t *testing.T) {
	t.Run("with initial position", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- sdk.Record), FetchConfig{})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("with missing position data", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- sdk.Record), FetchConfig{
			Position: position.Position{
				Type: position.TypeSnapshot,
			},
		})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("resume from position", func(t *testing.T) {
		is := is.New(t)

		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- sdk.Record), FetchConfig{
			Position: position.Position{
				Type: position.TypeSnapshot,
				Snapshot: position.SnapshotPositions{
					"mytable": {SnapshotEnd: 10, LastRead: 5},
				},
			},
			Table: "mytable",
		})

		is.Equal(f.snapshotEnd, int64(10))
		is.Equal(f.lastRead, int64(5))
	})
}

func Test_FetchConfigValidate(t *testing.T) {
	t.Run("multiple errors", func(t *testing.T) {
		is := is.New(t)
		err := (&FetchConfig{}).Validate()

		is.True(errors.Is(err, errTableRequired))
		is.True(errors.Is(err, errKeyRequired))
	})

	t.Run("missing table", func(t *testing.T) {
		is := is.New(t)
		tableErr := (&FetchConfig{Key: "id"}).Validate()

		is.True(errors.Is(tableErr, errTableRequired))
	})

	t.Run("missing table key", func(t *testing.T) {
		is := is.New(t)
		keyErr := (&FetchConfig{Table: "table"}).Validate()

		is.True(errors.Is(keyErr, errKeyRequired))
	})

	t.Run("invalid position", func(t *testing.T) {
		is := is.New(t)
		positionErr := (&FetchConfig{
			Table:    "mytable",
			Key:      "id",
			Position: position.Position{Type: position.TypeCDC},
		}).Validate()

		is.True(errors.Is(positionErr, errInvalidCDCType))
	})

	t.Run("success", func(t *testing.T) {
		is := is.New(t)
		err := (&FetchConfig{
			Table: "mytable",
			Key:   "id",
		}).Validate()

		is.True(err == nil)
	})
}

func Test_FetcherValidate(t *testing.T) {
	var (
		ctx   = context.Background()
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = test.SetupTestTable(ctx, t, pool)
	)

	t.Run("success", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: table,
				Key:   "id",
			},
		}

		is.NoErr(f.Validate(ctx))
	})

	t.Run("table missing", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: "missing_table",
				Key:   "id",
			},
		}

		err := f.Validate(ctx)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), `table "missing_table" does not exist`))
	})

	t.Run("key is invalid", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: table,
				Key:   "column1",
			},
		}

		err1 := f.Validate(ctx)
		is.True(err1 != nil)
		is.True(strings.Contains(err1.Error(), `invalid key "column1", not a primary key`))

		f.conf.Key = "missing_key"
		err2 := f.Validate(ctx)
		is.True(err2 != nil)
		is.True(strings.Contains(
			err2.Error(),
			fmt.Sprintf(`key "missing_key" not present on table %q`, table),
		))
	})
}

func Test_FetcherRun_Initial(t *testing.T) {
	var (
		pool    = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table   = test.SetupTestTable(context.Background(), t, pool)
		is      = is.New(t)
		records = make(chan sdk.Record)
		ctx     = context.Background()
		tt      = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, records, FetchConfig{
		Table: table,
		Key:   "id",
	})

	tt.Go(func() error {
		ctx = tt.Context(ctx)
		defer close(records)

		if err := f.Validate(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var rr []sdk.Record
	for r := range records {
		rr = append(rr, r)
	}

	is.NoErr(tt.Err())
	is.True(len(rr) == 4)

	expectedMatch := []sdk.StructuredData{
		{"id": int64(1), "key": []uint8{49}, "column1": "foo", "column2": int32(123), "column3": false},
		{"id": int64(2), "key": []uint8{50}, "column1": "bar", "column2": int32(456), "column3": true},
		{"id": int64(3), "key": []uint8{51}, "column1": "baz", "column2": int32(789), "column3": false},
		{"id": int64(4), "key": []uint8{52}, "column1": nil, "column2": nil, "column3": nil},
	}

	for i, r := range rr {
		is.Equal(r.Key, sdk.StructuredData{"id": int64(i + 1)})

		is.True(r.Payload.Before == nil)
		is.Equal(r.Payload.After, expectedMatch[i])

		matchPos := fmt.Sprintf(`{"type":1,"snapshot":{%q:{"last_read":%d,"snapshot_end":4}}}`, table, i+1)
		if i+1 == 4 { // last
			matchPos = fmt.Sprintf(`{"type":1,"snapshot":{%q:{"last_read":%d,"snapshot_end":4,"done":true}}}`, table, i+1)
		}
		is.Equal(string(r.Position), matchPos)
	}
}

func Test_FetcherRun_Resume(t *testing.T) {
	var (
		pool    = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table   = test.SetupTestTable(context.Background(), t, pool)
		is      = is.New(t)
		records = make(chan sdk.Record)
		ctx     = context.Background()
		tt      = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, records, FetchConfig{
		Table: table,
		Key:   "id",
		Position: position.Position{
			Type: position.TypeSnapshot,
			Snapshot: position.SnapshotPositions{
				table: {
					SnapshotEnd: 3,
					LastRead:    2,
				},
			},
		},
	})

	tt.Go(func() error {
		ctx = tt.Context(ctx)
		defer close(records)

		if err := f.Validate(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var rr []sdk.Record
	for r := range records {
		rr = append(rr, r)
	}

	is.NoErr(tt.Err())
	is.True(len(rr) == 1)

	// validate generated record
	is.Equal(rr[0].Key, sdk.StructuredData{"id": int64(3)})
	is.True(rr[0].Payload.Before == nil)
	is.Equal(rr[0].Payload.After, sdk.StructuredData{
		"id":      int64(3),
		"key":     []uint8{51},
		"column1": "baz",
		"column2": int32(789),
		"column3": false,
	})

	is.Equal(
		string(rr[0].Position),
		fmt.Sprintf(`{"type":1,"snapshot":{%q:{"last_read":3,"snapshot_end":3,"done":true}}}`, table),
	)
}

func Test_withSnapshot(t *testing.T) {
	var (
		is   = is.New(t)
		ctx  = context.Background()
		pool = test.ConnectPool(ctx, t, test.RegularConnString)
	)

	conn1, conn1Err := pool.Acquire(ctx)
	is.NoErr(conn1Err)
	t.Cleanup(func() { conn1.Release() })

	tx1, tx1Err := conn1.Begin(ctx)
	is.NoErr(tx1Err)
	t.Cleanup(func() { is.NoErr(tx1.Rollback(ctx)) })

	var snapshot string
	queryErr := tx1.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot)
	is.NoErr(queryErr)

	t.Run("with valid snapshot", func(t *testing.T) {
		is := is.New(t)

		c, err := pool.Acquire(ctx)
		is.NoErr(err)
		t.Cleanup(func() { c.Release() })

		tx, txErr := c.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		})
		is.NoErr(txErr)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })

		f := FetchWorker{conf: FetchConfig{Snapshot: snapshot}}

		is.NoErr(f.withSnapshot(ctx, tx))
	})

	t.Run("with invalid snapshot", func(t *testing.T) {
		is := is.New(t)

		c, err := pool.Acquire(ctx)
		is.NoErr(err)
		t.Cleanup(func() { c.Release() })

		tx, txErr := c.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		})
		is.NoErr(txErr)
		t.Cleanup(func() { is.NoErr(tx.Rollback(ctx)) })

		f := FetchWorker{conf: FetchConfig{Snapshot: "invalid"}}

		snapErr := f.withSnapshot(ctx, tx)
		is.True(strings.Contains(snapErr.Error(), `invalid snapshot identifier: "invalid"`))
	})

	t.Run("without snapshot", func(t *testing.T) {
		is := is.New(t)

		f := FetchWorker{conf: FetchConfig{}}

		snapErr := f.withSnapshot(ctx, nil)
		is.NoErr(snapErr)
	})
}

func Test_send(t *testing.T) {
	is := is.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	f := FetchWorker{conf: FetchConfig{}}

	cancel()

	err := f.send(ctx, sdk.Record{})
	fmt.Printf("%+v", err)

	is.True(err != nil)
	is.Equal(err.Error(), "fetcher send ctx: context canceled")
}

func Test_FetchWorker_buildRecord(t *testing.T) {
	var (
		is  = is.New(t)
		now = time.Now().UTC()

		// special case fields
		fields       = []string{"id", "time"}
		values       = []any{1, now}
		expectValues = []any{1, now.String()}
	)

	r := (&FetchWorker{
		conf: FetchConfig{Table: "mytable", Key: "id"},
	}).buildRecord(fields, values)

	data := r.Payload.After.(sdk.StructuredData)
	for i, k := range fields {
		is.Equal(data[k], expectValues[i])
	}
}

func Test_FetchWorker_updateFetchLimit(t *testing.T) {
	var (
		is    = is.New(t)
		ctx   = context.Background()
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = test.SetupTestTable(ctx, t, pool)
	)

	tx, err := pool.Begin(ctx)
	is.NoErr(err)
	t.Cleanup(func() { is.NoErr(tx.Rollback(ctx)) })

	tests := []struct {
		desc     string
		w        *FetchWorker
		expected int64
		wantErr  error
	}{
		{
			desc: "success",
			w: &FetchWorker{conf: FetchConfig{
				Table: table,
				Key:   "id",
			}},
			expected: 4,
		},
		{
			desc:     "skip update when set",
			w:        &FetchWorker{snapshotEnd: 10},
			expected: 10,
		},
		{
			desc: "fails to get range",
			w: &FetchWorker{conf: FetchConfig{
				Table: table,
				Key:   "notid",
			}},
			wantErr: errors.New(`ERROR: column "notid" does not exist`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			is := is.New(t)

			err := tc.w.updateFetchLimit(ctx, tx)
			if tc.wantErr != nil {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tc.wantErr.Error()))
			} else {
				is.NoErr(err)
				is.Equal(tc.w.snapshotEnd, tc.expected)
			}
		})
	}
}
