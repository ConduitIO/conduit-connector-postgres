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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func TestCDCIterator_New(t *testing.T) {
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)

	tests := []struct {
		name    string
		setup   func(t *testing.T) CDCConfig
		pgconf  *pgconn.Config
		wantErr error
	}{
		{
			name:   "publication already exists",
			pgconf: &pool.Config().ConnConfig.Config,
			setup: func(t *testing.T) CDCConfig {
				is := is.New(t)
				table := test.SetupTestTable(ctx, t, pool)
				test.CreatePublication(t, pool, table, []string{table})

				t.Cleanup(func() {
					is.NoErr(Cleanup(ctx, CleanupConfig{
						URL:      pool.Config().ConnString(),
						SlotName: table,
					}))
				})

				return CDCConfig{
					SlotName:        table,
					PublicationName: table,
					Tables:          []string{table},
				}
			},
		},
		{
			name: "fails to connect",
			pgconf: func() *pgconn.Config {
				c := pool.Config().ConnConfig.Config
				c.Port = 31337

				return &c
			}(),
			setup: func(*testing.T) CDCConfig {
				return CDCConfig{}
			},
			wantErr: errors.New("could not establish replication connection"),
		},
		{
			name:   "fails to create publication",
			pgconf: &pool.Config().ConnConfig.Config,
			setup: func(*testing.T) CDCConfig {
				return CDCConfig{
					PublicationName: "foobar",
				}
			},
			wantErr: errors.New("requires at least one table"),
		},
		{
			name:   "fails to create subscription",
			pgconf: &pool.Config().ConnConfig.Config,
			setup: func(t *testing.T) CDCConfig {
				is := is.New(t)
				table := test.SetupTestTable(ctx, t, pool)

				t.Cleanup(func() {
					is.NoErr(Cleanup(ctx, CleanupConfig{
						URL:             pool.Config().ConnString(),
						PublicationName: table,
					}))
				})

				return CDCConfig{
					SlotName:        "invalid,name_/",
					PublicationName: table,
					Tables:          []string{table},
				}
			},
			wantErr: errors.New("ERROR: syntax error (SQLSTATE 42601)"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			config := tt.setup(t)

			_, err := NewCDCIterator(ctx, tt.pgconf, config)
			if tt.wantErr != nil {
				if match := strings.Contains(err.Error(), tt.wantErr.Error()); !match {
					t.Logf("%s != %s", err.Error(), tt.wantErr.Error())
					is.True(match)
				}
			} else {
				is.NoErr(err)
			}
		})
	}
}

func TestCDCIterator_Next(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	i := testCDCIterator(ctx, t, pool, table, true)

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

func TestCDCIterator_Next_Fail(t *testing.T) {
	ctx := context.Background()

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	t.Run("fail when sub is done", func(t *testing.T) {
		is := is.New(t)

		i := testCDCIterator(ctx, t, pool, table, true)
		<-i.sub.Ready()

		is.NoErr(i.Teardown(ctx))

		_, err := i.Next(ctx)
		expectErr := "logical replication error:"

		match := strings.Contains(err.Error(), expectErr)
		if !match {
			t.Logf("%s != %s", err.Error(), expectErr)
		}
		is.True(match)
	})

	t.Run("fail when subscriber is not started", func(t *testing.T) {
		is := is.New(t)

		i := testCDCIterator(ctx, t, pool, table, false)

		_, nexterr := i.Next(ctx)
		is.Equal(nexterr.Error(), "logical replication has not been started")
	})
}

func TestCDCIterator_Ack(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		pos     sdk.Position
		wantErr error
	}{
		{
			name:    "failed to parse position",
			pos:     sdk.Position([]byte("{")),
			wantErr: errors.New("invalid position: unexpected end of JSON input"),
		},
		{
			name: "position of wrong type",
			pos: position.Position{
				Type: position.TypeSnapshot,
			}.ToSDKPosition(),
			wantErr: errors.New(`invalid type "Snapshot" for CDC position`),
		},
		{
			name: "failed to parse LSN",
			pos: position.Position{
				Type:    position.TypeCDC,
				LastLSN: "garble",
			}.ToSDKPosition(),
			wantErr: errors.New("failed to parse LSN: expected integer"),
		},
		{
			name: "invalid position LSN",
			pos: position.Position{
				Type: position.TypeCDC,
			}.ToSDKPosition(),
			wantErr: errors.New("cannot ack zero position"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			i := &CDCIterator{}

			err := i.Ack(ctx, tt.pos)
			if tt.wantErr != nil {
				is.Equal(err.Error(), tt.wantErr.Error())
			} else {
				is.NoErr(err)
			}
		})
	}
}

func TestCDCIterator_Teardown(t *testing.T) {
	t.Skip("This causes a data race in the the postgres connection being closed during forced teardown")

	ctx := context.Background()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	i := testCDCIterator(ctx, t, pool, table, true)

	// wait for subscription to be ready
	<-i.sub.Ready()

	cctx, cancel := context.WithCancel(ctx)
	cancel()

	_ = i.Teardown(cctx)
	// is.Equal(err.Error(), "context canceled")
	_ = is
}

func Test_withReplication(t *testing.T) {
	is := is.New(t)

	c := withReplication(&pgconn.Config{})
	is.Equal(c.RuntimeParams["replication"], "database")
}

func testCDCIterator(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, start bool) *CDCIterator {
	is := is.New(t)
	config := CDCConfig{
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table, // table is random, reuse for publication name
		SlotName:        table, // table is random, reuse for slot name
	}

	i, err := NewCDCIterator(ctx, &pool.Config().ConnConfig.Config, config)
	is.NoErr(err)

	if start {
		is.NoErr(i.StartSubscriber(ctx))
	}

	t.Cleanup(func() {
		is.NoErr(i.Teardown(ctx))
		is.NoErr(Cleanup(ctx, CleanupConfig{
			URL:             pool.Config().ConnString(),
			SlotName:        table,
			PublicationName: table,
		}))
	})

	return i
}
