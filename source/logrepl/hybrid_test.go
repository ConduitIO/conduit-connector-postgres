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

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"

	"github.com/matryer/is"
)

func TestHybridSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	h := createTestHybridIterator(ctx, t)

	go func() {
		conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
		_, err := conn.Exec(ctx, fmt.Sprintf(`insert into %s values ( 4, null, null,
		null, null, null, '{"biz":"baz"}')`, h.config.TableName))
		is.NoErr(err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`insert into %s values ( 5, null, null,
		null, null, null, '{"fiz":"buzz"}')`, h.config.TableName))
		is.NoErr(err)
	}()

	count := 0
	records := []sdk.Record{}
	for count < 4 {
		rec, err := h.Next(ctx)
		is.NoErr(err)
		count++
		records = append(records, rec)
		t.Logf("%d ----- %v\n", count, rec)
	}

	is.True(len(records) == 4)
	is.NoErr(h.Teardown(ctx))
}

// createTestHybridIterator creates a hybrid iterator with a replication
// capable connection to Postgres and a prepared test table and handles closing
// its connection and test cleanup.
func createTestHybridIterator(ctx context.Context, t *testing.T) *Hybrid {
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	cfg := conn.Config()
	cfg.RuntimeParams["replication"] = "database"
	replconn, err := pgx.ConnectConfig(ctx, cfg)
	is.NoErr(err)
	t.Cleanup(func() { is.NoErr(replconn.Close(ctx)) })
	table := test.SetupTestTableV2(ctx, t, replconn)
	h, err := NewHybridIterator(ctx, replconn, Config{
		TableName:       table,
		SlotName:        table,
		PublicationName: table,
		KeyColumnName:   "h",
		Columns:         []string{"a", "b", "c", "d", "e", "f", "g"},
		SnapshotMode:    "initial",
	})
	is.NoErr(err)
	return h
}
