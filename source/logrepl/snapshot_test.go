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
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/matryer/is"
)

func TestLifecycle(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, pool)

	tx, name := createTestSnapshot(t, pool)
	t.Cleanup(func() { tx.Commit(ctx) })

	snapshotConn, err := pool.Acquire(ctx)
	is.NoErr(err)
	t.Cleanup(func() { snapshotConn.Release() })

	s, err := NewSnapshotIterator(context.Background(), snapshotConn.Conn(), SnapshotConfig{
		SnapshotName: name,
		Table:        table,
		Columns:      []string{"id", "key", "column1", "column2", "column3"},
		KeyColumn:    "key",
	})
	is.NoErr(err)
	t.Cleanup(func() { s.Teardown(ctx) })

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
}

// createTestSnapshot starts a transaction that stays open while a snapshot run.
// Otherwise, postgres deletes the snapshot as soon as this tx commits,
// an our snapshot iterator won't find a snapshot at the specifiedname.
// https://www.postgresql.org/docs/current/sql-set-transaction.html
func createTestSnapshot(t *testing.T, pool *pgxpool.Pool) (pgx.Tx, string) {
	ctx := context.Background()
	is := is.New(t)

	tx, err := pool.Begin(ctx)
	is.NoErr(err)
	query := `SELECT * FROM pg_catalog.pg_export_snapshot();`
	rows, err := tx.Query(context.Background(), query)
	is.NoErr(err)

	var name *string
	is.True(rows.Next())
	err = rows.Scan(&name)
	is.NoErr(err)

	return tx, *name
}
