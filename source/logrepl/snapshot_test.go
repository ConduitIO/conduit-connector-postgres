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
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/matryer/is"
)

func TestLifecycle(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, pool)
	name := createTestSnapshot(t, pool)

	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	s, err := NewSnapshotIterator(context.Background(), conn.Conn(), SnapshotConfig{
		SnapshotName: name,
		Table:        table,
		Columns:      []string{"id", "key", "column1", "column2", "column3"},
		KeyColumn:    "key",
	})
	is.NoErr(err)
	t.Cleanup(func() { conn.Release() })

	now := time.Now()
	rec, err := s.Next(ctx)
	is.NoErr(err)

	is.True(rec.CreatedAt.After(now))
	is.Equal(rec.Metadata["action"], "snapshot")
	rec.CreatedAt = time.Time{} // reset time for comparison

	is.Equal(rec, sdk.Record{
		Position: sdk.Position(fmt.Sprintf("%s:0", table)),
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
	is.NoErr(s.Teardown(ctx)) // TODO: Should return an error
}

// createTestSnapshot starts a transaction that stays open while a snapshot test
// runs. Otherwise, Postgres deletes the snapshot as soon as the transaction
// commits or rolls back, and our snapshot iterator won't find a snapshot with
// the specified name.
// https://www.postgresql.org/docs/current/sql-set-transaction.html
func createTestSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	ctx := context.Background()
	is := is.New(t)

	conn, err := pool.Acquire(ctx)
	is.NoErr(err)

	tx, err := conn.Begin(ctx)
	is.NoErr(err)

	query := `SELECT * FROM pg_catalog.pg_export_snapshot();`
	rows, err := tx.Query(ctx, query)
	is.NoErr(err)
	var name string
	is.True(rows.Next())
	err = rows.Scan(&name)
	is.NoErr(err)

	t.Cleanup(func() {
		rows.Close()
		is.NoErr(tx.Commit(ctx))
		conn.Release()
	})

	return name
}
