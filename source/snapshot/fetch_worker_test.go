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
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- FetchData), FetchConfig{})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("with missing position data", func(t *testing.T) {
		is := is.New(t)
		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- FetchData), FetchConfig{
			Position: position.Position{
				Type: position.TypeSnapshot,
			},
		})

		is.Equal(f.snapshotEnd, int64(0))
		is.Equal(f.lastRead, int64(0))
	})

	t.Run("resume from position", func(t *testing.T) {
		is := is.New(t)

		f := NewFetchWorker(&pgxpool.Pool{}, make(chan<- FetchData), FetchConfig{
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

	t.Run("key is wrong type", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: table,
				Key:   "column3",
			},
		}

		err := f.Validate(ctx)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), `failed to validate key: key "column3" of type "boolean" is unsupported`))
	})

	t.Run("key is not pk", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: table,
				Key:   "column2",
			},
		}

		err := f.Validate(ctx)
		is.NoErr(err) // no error, only a warning
	})

	t.Run("missing key", func(t *testing.T) {
		is := is.New(t)
		f := FetchWorker{
			db: pool,
			conf: FetchConfig{
				Table: table,
				Key:   "missing_key",
			},
		}

		err := f.Validate(ctx)
		is.True(err != nil)
		ok := strings.Contains(err.Error(), fmt.Sprintf(`key "missing_key" not present on table %q`, table))
		if !ok {
			t.Logf("error: %s", err.Error())
		}
		is.True(ok)
	})
}

func Test_FetcherRun_Initial(t *testing.T) {
	var (
		pool  = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table = test.SetupTestTable(context.Background(), t, pool)
		is    = is.New(t)
		out   = make(chan FetchData)
		ctx   = context.Background()
		tt    = &tomb.Tomb{}
	)

	f := NewFetchWorker(pool, out, FetchConfig{
		Table: table,
		Key:   "id",
	})

	tt.Go(func() error {
		ctx = tt.Context(ctx)
		defer close(out)

		if err := f.Validate(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var dd []FetchData
	for data := range out {
		dd = append(dd, data)
	}

	is.NoErr(tt.Err())
	is.True(len(dd) == 4)

	expectedMatch := []sdk.StructuredData{
		{"id": int64(1), "key": []uint8{49}, "column1": "foo", "column2": int32(123), "column3": false},
		{"id": int64(2), "key": []uint8{50}, "column1": "bar", "column2": int32(456), "column3": true},
		{"id": int64(3), "key": []uint8{51}, "column1": "baz", "column2": int32(789), "column3": false},
		{"id": int64(4), "key": []uint8{52}, "column1": nil, "column2": nil, "column3": nil},
	}

	for i, d := range dd {
		is.Equal(d.Key, sdk.StructuredData{"id": int64(i + 1)})
		is.Equal(d.Payload, expectedMatch[i])

		is.Equal(d.Position, position.SnapshotPosition{
			LastRead:    int64(i + 1),
			SnapshotEnd: 4,
		})
		is.Equal(d.Table, table)
	}
}

func Test_FetcherRun_Resume(t *testing.T) {
	var (
		pool  = test.ConnectPool(context.Background(), t, test.RegularConnString)
		table = test.SetupTestTable(context.Background(), t, pool)
		is    = is.New(t)
		out   = make(chan FetchData)
		ctx   = context.Background()
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

		if err := f.Validate(ctx); err != nil {
			return err
		}
		return f.Run(ctx)
	})

	var dd []FetchData
	for d := range out {
		dd = append(dd, d)
	}

	is.NoErr(tt.Err())
	is.True(len(dd) == 1)

	// validate generated record
	is.Equal(dd[0].Key, sdk.StructuredData{"id": int64(3)})
	is.Equal(dd[0].Payload, sdk.StructuredData{
		"id":      int64(3),
		"key":     []uint8{51},
		"column1": "baz",
		"column2": int32(789),
		"column3": false,
	})

	is.Equal(dd[0].Position, position.SnapshotPosition{
		LastRead:    3,
		SnapshotEnd: 3,
	})
	is.Equal(dd[0].Table, table)
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

	ctx, cancel := context.WithCancel(context.Background())
	f := FetchWorker{conf: FetchConfig{}}

	cancel()

	err := f.send(ctx, FetchData{})

	is.Equal(err, context.Canceled)
}

func Test_FetchWorker_buildRecordData(t *testing.T) {
	var (
		is  = is.New(t)
		now = time.Now().UTC()

		// special case fields
		fields       = []string{"id", "time"}
		values       = []any{1, now}
		expectValues = []any{1, now.String()}
	)

	key, payload := (&FetchWorker{
		conf: FetchConfig{Table: "mytable", Key: "id"},
	}).buildRecordData(fields, values)

	is.Equal(len(payload), 2)
	for i, k := range fields {
		is.Equal(payload[k], expectValues[i])
	}

	is.Equal(len(key), 1)
	is.Equal(key["id"], 1)
}

func Test_FetchWorker_updateSnapshotEnd(t *testing.T) {
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
