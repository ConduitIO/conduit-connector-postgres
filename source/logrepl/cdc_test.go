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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func TestIterator_Next(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	i := testIterator(ctx, t, pool, table)
	t.Cleanup(func() {
		is.NoErr(i.Teardown(ctx))
	})

	// wait for subscription to be ready
	<-i.sub.Ready()

	tests := []struct {
		name       string
		setupQuery string
		want       sdk.Record
		wantErr    bool
	}{
		{
			name: "should detect insert",
			setupQuery: `INSERT INTO %s (id, column1, column2, column3)
				VALUES (6, 'bizz', 456, false)`,
			wantErr: false,
			want: sdk.Record{
				Operation: sdk.OperationCreate,
				Metadata: map[string]string{
					sdk.MetadataCollection: table,
				},
				Key: sdk.StructuredData{"id": int64(6)},
				Payload: sdk.Change{
					Before: nil,
					After: sdk.StructuredData{
						"id":      int64(6),
						"column1": "bizz",
						"column2": int32(456),
						"column3": false,
						"key":     nil,
					},
				},
			},
		},
		{
			name: "should detect update",
			setupQuery: `UPDATE %s
				SET column1 = 'test cdc updates' 
				WHERE key = '1'`,
			wantErr: false,
			want: sdk.Record{
				Operation: sdk.OperationUpdate,
				Metadata: map[string]string{
					sdk.MetadataCollection: table,
				},
				Key: sdk.StructuredData{"id": int64(1)},
				Payload: sdk.Change{
					Before: nil, // TODO
					After: sdk.StructuredData{
						"id":      int64(1),
						"column1": "test cdc updates",
						"column2": int32(123),
						"column3": false,
						"key":     []uint8("1"),
					},
				},
			},
		},
		{
			name:       "should detect delete",
			setupQuery: `DELETE FROM %s WHERE id = 3`,
			wantErr:    false,
			want: sdk.Record{
				Operation: sdk.OperationDelete,
				Metadata: map[string]string{
					sdk.MetadataCollection: table,
				},
				Key: sdk.StructuredData{"id": int64(3)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			now := time.Now()

			// execute change
			query := fmt.Sprintf(tt.setupQuery, table)
			_, err := pool.Exec(ctx, query)
			is.NoErr(err)

			// fetch the change
			nextCtx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			got, err := i.Next(nextCtx)
			is.NoErr(err)

			readAt, err := got.Metadata.GetReadAt()
			is.NoErr(err)
			is.True(readAt.After(now)) // ReadAt should be after now
			is.True(len(got.Position) > 0)
			tt.want.Metadata[sdk.MetadataReadAt] = got.Metadata[sdk.MetadataReadAt]
			tt.want.Position = got.Position

			is.Equal(got, tt.want)
			is.NoErr(i.Ack(ctx, got.Position))
		})
	}
}

func testIterator(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) *CDCIterator {
	is := is.New(t)
	config := Config{
		Tables:          []string{table},
		PublicationName: table, // table is random, reuse for publication name
		SlotName:        table, // table is random, reuse for slot name
	}

	i, err := NewCDCIterator(ctx, pool, config)
	is.NoErr(err)
	return i
}
