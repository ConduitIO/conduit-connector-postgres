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

	"github.com/conduitio/conduit-connector-postgres/common"
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
	name := createTestSnapshot(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, SnapshotConfig{
		SnapshotName: name,
		Table:        table,
		Columns:      columns,
		KeyColumn:    key,
	})
	t.Cleanup(func() { is.NoErr(s.Teardown(ctx)) })

	// add a record to our table after snapshot started
	insertQuery := fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3)
				VALUES (5, 'bizz', 456, false)`, table)
	_, err := pool.Exec(ctx, insertQuery)
	is.NoErr(err)

	// assert record does not appear in snapshot
	for i := 0; i < 5; i++ {
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
	name := createTestSnapshot(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, SnapshotConfig{
		SnapshotName: name,
		Table:        table,
		Columns:      columns,
		KeyColumn:    key,
	})

	rec, err := s.Next(ctx)
	is.NoErr(err)

	is.Equal(rec, sdk.Record{
		Operation: sdk.OperationSnapshot,
		Position:  sdk.Position(fmt.Sprintf("%s:1", table)),
		Key: sdk.StructuredData{
			"key": []uint8("1"),
		},
		Payload: sdk.Change{
			Before: nil,
			After: sdk.StructuredData{
				"id":      int64(1),
				"key":     []uint8("1"),
				"column1": "foo",
				"column2": int32(123),
				"column3": false,
			},
		},
		Metadata: map[string]string{
			common.MetadataPostgresTable: table,
			sdk.MetadataReadAt:           rec.Metadata[sdk.MetadataReadAt],
		},
	})
	is.True(errors.Is(s.Teardown(ctx), ErrSnapshotInterrupt))
}

func TestFullIteration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, pool)
	name := createTestSnapshot(ctx, t, pool)
	s := createTestSnapshotIterator(ctx, t, pool, SnapshotConfig{
		SnapshotName: name,
		Table:        table,
		Columns:      columns,
		KeyColumn:    key,
	})

	for i := 0; i < 4; i++ {
		rec, err := s.Next(ctx)
		is.Equal(rec.Position, sdk.Position(fmt.Sprintf("%s:%d", table, i+1)))
		is.NoErr(err)
	}

	r, err := s.Next(ctx)
	is.Equal(r, sdk.Record{})
	is.True(errors.Is(err, ErrSnapshotComplete))
	is.NoErr(s.Teardown(ctx))
}

// createTestSnapshot starts a transaction that stays open while a snapshot test
// runs. Otherwise, Postgres deletes the snapshot as soon as the transaction
// commits or rolls back, and our snapshot iterator won't find a snapshot with
// the specified name.
// https://www.postgresql.org/docs/current/sql-set-transaction.html
func createTestSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	is := is.New(t)
	conn, err := pool.Acquire(ctx)
	is.NoErr(err)

	tx, err := conn.Begin(ctx)
	is.NoErr(err)

	var name string
	query := `SELECT * FROM pg_catalog.pg_export_snapshot();`
	row := tx.QueryRow(ctx, query)
	is.NoErr(err)
	err = row.Scan(&name)
	is.NoErr(err)

	t.Cleanup(func() {
		is.NoErr(tx.Commit(ctx))
		conn.Release()
	})

	return name
}

// creates a snapshot iterator for testing that hands its connection's cleanup.
func createTestSnapshotIterator(ctx context.Context, t *testing.T,
	pool *pgxpool.Pool, cfg SnapshotConfig) *SnapshotIterator {
	is := is.New(t)

	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	s, err := NewSnapshotIterator(context.Background(), conn.Conn(), SnapshotConfig{
		SnapshotName: cfg.SnapshotName,
		Table:        cfg.Table,
		Columns:      cfg.Columns,
		KeyColumn:    cfg.KeyColumn,
	})
	is.NoErr(err)
	t.Cleanup(conn.Release)
	return s
}
