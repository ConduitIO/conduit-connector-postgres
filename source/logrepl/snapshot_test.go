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

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/matryer/is"
)

var (
	columns = []string{"id", "key", "column1", "column2", "column3"}
	key     = "key"
)

func TestAtomicSnapshot(t *testing.T) {
	is := is.New(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	ctx := context.Background()
	table := test.SetupTestTable(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, Config{
		TableName:     table,
		Columns:       columns,
		KeyColumnName: key,
	})
	t.Cleanup(func() { is.NoErr(s.Teardown(ctx)) })

	// add a record to our table after snapshot started
	insertQuery := fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3)
				VALUES (5, 'bizz', 456, false)`, table)
	_, err := pool.Exec(ctx, insertQuery)
	is.NoErr(err)

	// assert record does not appear in snapshot
	for i := 0; i <= 4; i++ {
		r, err := s.Next(ctx)
		if err != nil {
			is.True(errors.Is(err, ErrSnapshotComplete))
			is.Equal(r, sdk.Record{})
		}
	}
}

func TestSnapshotInterrupted(t *testing.T) {
	is := is.New(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	ctx := context.Background()
	table := test.SetupTestTable(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, Config{
		TableName:     table,
		Columns:       columns,
		KeyColumnName: key,
	})
	now := time.Now()

	rec, err := s.Next(ctx)
	is.NoErr(err)

	is.True(rec.CreatedAt.After(now))
	rec.CreatedAt = time.Time{} // reset time for comparison
	is.Equal(rec.Metadata["action"], "snapshot")
	is.Equal(rec, sdk.Record{
		Position: sdk.Position(fmt.Sprintf("s:%s:0", table)),
		Key: sdk.StructuredData{
			"key": []uint8("1"),
		},
		Payload: sdk.StructuredData{
			"id":      int64(1),
			"column1": "foo",
			"column2": int32(123),
			"column3": bool(false),
		},
		Metadata: map[string]string{
			"action": actionSnapshot,
			"table":  table,
		},
	})
	is.True(errors.Is(s.Teardown(ctx), ErrSnapshotInterrupt))
}

func TestFullIteration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, Config{
		TableName:     table,
		Columns:       columns,
		KeyColumnName: key,
	})

	for i := 0; i < 4; i++ {
		rec, err := s.Next(ctx)
		is.Equal(rec.Position, sdk.Position(fmt.Sprintf("s:%s:%d", table, i)))
		is.NoErr(err)
	}

	r, err := s.Next(ctx)
	is.Equal(r, sdk.Record{})
	is.True(errors.Is(err, ErrSnapshotComplete))
	is.NoErr(s.Teardown(ctx))
}

func TestInitialSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, pool)

	s := createTestSnapshotIterator(ctx, t, pool, Config{
		TableName:     table,
		Columns:       columns,
		KeyColumnName: key,
	})

	is.True(s.LSN() != "")

	count := 0
	for {
		_, err := s.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrSnapshotComplete) {
				break
			}
		}
		count++
	}

	is.Equal(count, 4)
	is.NoErr(s.Teardown(ctx))
}

// creates a snapshot iterator for testing that hands its connection's cleanup.
func createTestSnapshotIterator(ctx context.Context, t *testing.T,
	pool *pgxpool.Pool, cfg Config) *SnapshotIterator {
	is := is.New(t)

	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	s, err := NewSnapshotIterator(context.Background(), conn.Conn(), Config{
		TableName:     cfg.TableName,
		Columns:       cfg.Columns,
		KeyColumnName: cfg.KeyColumnName,
	})
	is.NoErr(err)
	t.Cleanup(conn.Release)
	return s
}
