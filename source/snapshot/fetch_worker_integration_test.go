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
	"math/big"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

func Test_NewFetcher(t *testing.T) {
	t.Run("with initial position", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- []FetchData), FetchConfig{})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("with missing position data", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- []FetchData), FetchConfig{
			Position: position.Position{
				Type: position.TypeSnapshot,
			},
		})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("resume from position", func(t *testing.T) {
		is := is.New(t)

		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- []FetchData), FetchConfig{
			Position: position.Position{
				Type: position.TypeSnapshot,
				Snapshots: position.SnapshotPositions{
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
		ctx   = test.Context(t)
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = strings.ToUpper(test.RandomIdentifier(t))
	)

	// uppercase table name is required to test primary key fetching
	test.SetupTableWithName(ctx, t, pool, table)

	t.Run("success", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(
			pool,
			make(chan<- FetchData),
			FetchConfig{
				Table: table,
				Key:   "id",
			},
		)

		is.NoErr(f.Init(ctx))
	})

	t.Run("table missing", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(
			pool,
			make(chan<- FetchData),
			FetchConfig{
				Table: "missing_table",
				Key:   "id",
			},
		)

		err := f.Init(ctx)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), `table "missing_table" does not exist`))
	})

	t.Run("key is wrong type", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(
			pool,
			make(chan<- FetchData),
			FetchConfig{
				Table: table,
				Key:   "column3",
			},
		)

		err := f.Init(ctx)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), `failed to validate key: key "column3" of type "boolean" is unsupported`))
	})

	t.Run("key is not pk", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(
			pool,
			make(chan<- FetchData),
			FetchConfig{
				Table: table,
				Key:   "column2",
			},
		)

		err := f.Init(ctx)
		is.NoErr(err) // no error, only a warning
	})

	t.Run("missing key", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(
			pool,
			make(chan<- FetchData),
			FetchConfig{
				Table: table,
				Key:   "missing_key",
			},
		)

		err := f.Init(ctx)
		is.True(err != nil)
		ok := strings.Contains(err.Error(), fmt.Sprintf(`key "missing_key" not present on table %q`, table))
		if !ok {
			t.Logf("error: %s", err.Error())
		}
		is.True(ok)
	})
}

func Test_FetcherRun_EmptySnapshot(t *testing.T) {
	var (
		is       = is.New(t)
		ctx      = test.Context(t)
		pool     = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table    = test.SetupEmptyTable(context.Background(), t, pool)
		out      = make(chan []FetchData)
		testTomb = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, out, FetchConfig{
		Table: table,
		Key:   "id",
	})

	testTomb.Go(func() error {
		ctx = testTomb.Context(ctx)
		defer close(out)

		if err := f.Validate(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var gotFetchData []FetchData
	for data := range out {
		gotFetchData = append(gotFetchData, data...)
	}

	is.NoErr(testTomb.Err())
	is.True(len(gotFetchData) == 0)
}

func Test_FetcherRun_Initial(t *testing.T) {
	var (
		pool  = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table = test.SetupTable(context.Background(), t, pool)
		is    = is.New(t)
		out   = make(chan []FetchData)
		ctx   = test.Context(t)
		tt    = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, out, FetchConfig{
		Table: table,
		Key:   "id",
	})

	tt.Go(func() error {
		ctx = tt.Context(ctx)
		defer close(out)

		if err := f.Init(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var gotFetchData []FetchData
	for data := range out {
		gotFetchData = append(gotFetchData, data...)
	}

	is.NoErr(tt.Err())
	is.Equal(len(gotFetchData), 4)

	expectedMatch := []opencdc.StructuredData{
		{"id": int64(1), "key": []uint8{49}, "column1": "foo", "column2": int32(123), "column3": false, "column4": big.NewRat(122, 10), "UppercaseColumn1": int32(1)},
		{"id": int64(2), "key": []uint8{50}, "column1": "bar", "column2": int32(456), "column3": true, "column4": big.NewRat(1342, 100), "UppercaseColumn1": int32(2)},
		{"id": int64(3), "key": []uint8{51}, "column1": "baz", "column2": int32(789), "column3": false, "column4": big.NewRat(836, 25), "UppercaseColumn1": int32(3)},
		{"id": int64(4), "key": []uint8{52}, "column1": "qux", "column2": int32(444), "column3": false, "column4": big.NewRat(911, 10), "UppercaseColumn1": int32(4)},
	}

	for i, got := range gotFetchData {
		t.Run(fmt.Sprintf("payload_%d", i+1), func(t *testing.T) {
			is := is.New(t)
			is.Equal(got.Key, opencdc.StructuredData{"id": int64(i + 1)})
			is.Equal("", cmp.Diff(
				expectedMatch[i],
				got.Payload,
				cmp.Comparer(func(x, y *big.Rat) bool {
					return x.Cmp(y) == 0
				}),
			))

			is.Equal(got.Position, position.SnapshotPosition{
				LastRead:    int64(i + 1),
				SnapshotEnd: 4,
			})
			is.Equal(got.Table, table)
		})
	}
}

func Test_FetcherRun_Resume(t *testing.T) {
	var (
		pool  = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table = test.SetupTable(context.Background(), t, pool)
		is    = is.New(t)
		out   = make(chan []FetchData)
		ctx   = test.Context(t)
		tt    = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, out, FetchConfig{
		Table: table,
		Key:   "id",
		Position: position.Position{
			Type: position.TypeSnapshot,
			Snapshots: position.SnapshotPositions{
				table: {
					SnapshotEnd: 3,
					LastRead:    2,
				},
			},
		},
	})

	tt.Go(func() error {
		ctx = tt.Context(ctx)
		defer close(out)

		if err := f.Init(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var dd []FetchData
	for d := range out {
		dd = append(dd, d...)
	}

	is.NoErr(tt.Err())
	is.True(len(dd) == 1)

	// validate generated record
	is.Equal(dd[0].Key, opencdc.StructuredData{"id": int64(3)})
	is.Equal(
		"",
		cmp.Diff(
			dd[0].Payload,
			opencdc.StructuredData{
				"id":               int64(3),
				"key":              []uint8{51},
				"column1":          "baz",
				"column2":          int32(789),
				"column3":          false,
				"column4":          big.NewRat(836, 25),
				"UppercaseColumn1": int32(3),
			},
			cmp.Comparer(func(x, y *big.Rat) bool {
				return x.Cmp(y) == 0
			}),
		),
	)

	is.Equal(dd[0].Position, position.SnapshotPosition{
		LastRead:    3,
		SnapshotEnd: 3,
	})
	is.Equal(dd[0].Table, table)
}

func Test_withSnapshot(t *testing.T) {
	var (
		is   = is.New(t)
		ctx  = test.Context(t)
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

		f := FetchWorker{conf: FetchConfig{TXSnapshotID: snapshot}}

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

		f := FetchWorker{conf: FetchConfig{TXSnapshotID: "invalid"}}

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

	ctx := test.Context(t)
	ctx, cancel := context.WithCancel(ctx)
	f := FetchWorker{conf: FetchConfig{}}

	cancel()

	err := f.send(ctx, []FetchData{{}})

	is.Equal(err, context.Canceled)
}

func Test_FetchWorker_updateSnapshotEnd(t *testing.T) {
	var (
		is    = is.New(t)
		ctx   = test.Context(t)
		pool  = test.ConnectPool(ctx, t, test.RegularConnString)
		table = strings.ToUpper(test.RandomIdentifier(t))
	)

	test.SetupTableWithName(ctx, t, pool, table)

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
			desc: "success with capitalized key",
			w: &FetchWorker{conf: FetchConfig{
				Table: table,
				Key:   "UppercaseColumn1",
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

			err := tc.w.updateSnapshotEnd(ctx, tx)
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

func Test_FetchWorker_createCursor(t *testing.T) {
	var (
		pool  = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table = test.SetupTable(context.Background(), t, pool)
		is    = is.New(t)
		ctx   = test.Context(t)
	)

	f := FetchWorker{
		lastRead:    10,
		snapshotEnd: 15,
		cursorName:  "cursor123",
		conf: FetchConfig{
			Table: table,
			Key:   "id",
		},
	}

	tx, err := pool.Begin(ctx)
	is.NoErr(err)

	cleanup, err := f.createCursor(ctx, tx)
	is.NoErr(err)

	t.Cleanup(func() {
		cleanup()
	})

	var cursorDef string
	is.NoErr(tx.QueryRow(context.Background(), "SELECT statement FROM pg_cursors WHERE name=$1", f.cursorName).
		Scan(&cursorDef),
	)
	is.Equal(
		cursorDef,
		fmt.Sprintf(`DECLARE cursor123 CURSOR FOR(SELECT * FROM %q WHERE "id" > 10 AND "id" <= 15 ORDER BY "id")`, table),
	)

	is.NoErr(tx.Rollback(ctx))
}
