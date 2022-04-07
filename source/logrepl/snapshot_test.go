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

var (
	columns = []string{"id", "key", "column1", "column2", "column3"}
	key     = "key"
)

func TestSnapshotIterator_Teardown(t *testing.T) {
	is := is.New(t)
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		setup   func(t *testing.T) *SnapshotIterator
		args    args
		wantErr bool
		wanted  error
	}{
		{
			name: "should return interrupt when next never called",
			setup: func(t *testing.T) *SnapshotIterator {
				s, _ := createTestSnapshotIterator(t, columns, key)
				return s
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
			wanted:  ErrSnapshotInterrupt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.setup(t)
			if err := s.Teardown(tt.args.ctx); (err != nil) != tt.wantErr {
				if tt.wantErr {
					is.Equal(tt.wanted, err)
				} else {
					t.Errorf("SnapshotIterator.Teardown() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestSnapshotAtomicity(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RegularConnString)

	// start our snapshot iterator
	s, table := createTestSnapshotIterator(t, columns, key)
	t.Cleanup(func() { is.NoErr(s.Teardown(ctx)) })
	is.Equal(s.complete, false)

	// add a record to our table after snapshot started
	insertQuery := fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3)
				VALUES (5, 'bizz', 456, false)`, table)
	_, err := pool.Exec(ctx, insertQuery)
	is.NoErr(err)

	// assert record does not appear in snapshot
	for i := 0; i < 5; i++ {
		r, err := s.Next(ctx)
		if err != nil {
			is.Equal(err, ErrSnapshotComplete)
			is.Equal(r, sdk.Record{})
			is.Equal(s.complete, true)
		}
	}
}

func TestFullIteration(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	s, table := createTestSnapshotIterator(t, []string{"id", "key"}, "key")
	for i := 0; i < 4; i++ {
		rec, err := s.Next(ctx)
		is.Equal(rec.Position, sdk.Position(fmt.Sprintf("%s:%d", table, i)))
		is.NoErr(err)
	}
	r, err := s.Next(ctx)
	is.Equal(r, sdk.Record{})
	is.Equal(err.Error(), ErrSnapshotComplete.Error())
}

func TestLifecycleErrInterrupt(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	s, table := createTestSnapshotIterator(t,
		[]string{"id", "key", "column1", "column2", "column3"}, "key")

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
	is.Equal(ErrSnapshotInterrupt.Error(), s.Teardown(ctx).Error())
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

// createTestSnapshotIterator creates a new test table, starts a snapshot
// on it, then creates a test SnapshotIterator with the ID of that snapshot.
// * It returns that SnapshotIterator and the string name of the test table.
// * This function handles its own pooled connection cleanup, but _not_ the
// SnapshotIterator's Teardown.
func createTestSnapshotIterator(t *testing.T, columns []string, key string) (*SnapshotIterator, string) {
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
		Columns:      columns,
		KeyColumn:    key,
	})
	is.NoErr(err)
	t.Cleanup(func() { conn.Release() })
	return s, table
}
