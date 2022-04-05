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
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/matryer/is"
)

func TestLifecycle(t *testing.T) {
	i := is.New(t)
	ctx := context.Background()

	testConn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.SetupTestTable(ctx, t, test.ConnectSimple(ctx, t, test.RegularConnString))

	_, err := testConn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ;")
	i.NoErr(err)

	query := `SELECT * FROM pg_catalog.pg_export_snapshot();`
	rows, err := testConn.Query(context.Background(), query)
	i.NoErr(err)

	var name *string
	i.True(rows.Next())
	err = rows.Scan(&name)
	i.NoErr(err)

	snapshotConn := test.ConnectSimple(ctx, t, test.RegularConnString)
	s, err := NewSnapshotIterator(context.Background(), snapshotConn, SnapshotConfig{
		SnapshotName: *name,
		URI:          test.RegularConnString,
		Table:        table,
		Columns:      []string{"id", "key", "column1", "column2", "column3"},
		KeyColumn:    "key",
	})
	i.NoErr(err)

	now := time.Now()
	rec, err := s.Next(ctx)
	i.NoErr(err)

	i.True(rec.CreatedAt.After(now))
	i.Equal(rec.Metadata["action"], "snapshot")
	rec.CreatedAt = time.Time{} // reset time for comparison

	i.Equal(rec, sdk.Record{
		Position: sdk.Position("0"),
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

	err = s.Teardown(ctx)
	i.NoErr(err)

	rows.Close()
	i.NoErr(err)
}
