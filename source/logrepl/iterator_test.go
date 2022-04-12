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

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestCombinedIterator(t *testing.T) {
	is := is.New(t)
	pool := test.ConnectPool(context.Background(), t, test.RepmgrConnString)

	t.Run("should switch into CDC mode when snapshot finished", func(t *testing.T) {
		ctx := context.Background()

		table := test.SetupTestTable(ctx, t, pool)

		conn, err := pool.Acquire(ctx)
		is.NoErr(err)

		ci, err := NewIterator(ctx, conn.Conn(), Config{
			Position:        nil,
			Columns:         []string{"id", "key", "column1", "column2", "column3"},
			KeyColumnName:   "key",
			TableName:       table,
			SlotName:        table,
			PublicationName: table,
		})
		is.NoErr(err)

		// insert a record after snapshot has started
		insert := `INSERT INTO %s (id, column1, column2, column3)
			VALUES (6, 'bizz', 456, false)`
		_, err = pool.Exec(ctx, insert)
		is.NoErr(err)

		for i := 0; i < 5; i++ {
			rec, err := ci.Next(ctx)
			is.NoErr(err)

			// 5th call should return inserted CDC record
			if i == 4 {
				is.Equal(rec, sdk.Record{
					Key: sdk.StructuredData{"id": int64(6)},
					Metadata: map[string]string{
						"table":  table,
						"action": "insert",
					},
					Payload: sdk.StructuredData{
						"id":      int64(6),
						"column1": "bizz",
						"column2": int32(456),
						"column3": false,
						"key":     nil,
					},
				})
			}
		}

	})
}
