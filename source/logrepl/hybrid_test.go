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

	"github.com/jackc/pgx/v4"
	"github.com/matryer/is"
)

func TestHybridTransition(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	dctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*3))
	t.Cleanup(cancel)

	h := createTestHybridIterator(ctx, t)

	count := 0
	go func() {
		<-dctx.Done()
		// check the count after deadline reached.
		is.True(count == 8)
	}()

	for {
		rec, err := h.Next(ctx)
		is.NoErr(err)
		count++
		fmt.Printf("rec: %v\n", rec)
	}
}

func createTestHybridIterator(ctx context.Context, t *testing.T) *Hybrid {
	is := is.New(t)

	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	cfg := conn.Config()
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	is.NoErr(err)

	t.Cleanup(func() {
		is.NoErr(conn.Close(ctx))
	})

	table := test.SetupTestTable(ctx, t, conn)

	h, err := NewHybridIterator(ctx, conn, Config{
		TableName:       table,
		SlotName:        table,
		PublicationName: table,
		KeyColumnName:   "key",
		Columns:         []string{"id", "key", "column1", "column2", "column3"},
		SnapshotMode:    "initial",
	})
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(h.Teardown(ctx))
	})

	go func() {
		pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
		count := 4
		for i := 0; i < count; i++ {
			query := `INSERT INTO %s (key, column1, column2, column3)
				VALUES ('5', 'bazz', 123, false),
				('6', 'bizz', 456, true),
				('7', 'buzz', 789, false),
				('8', null, null, null)`
			query = fmt.Sprintf(query, table)
			c, err := pool.Acquire(ctx)
			is.NoErr(err)
			_, err = c.Exec(ctx, query)
			is.NoErr(err)
			i++
		}
	}()

	return h
}
