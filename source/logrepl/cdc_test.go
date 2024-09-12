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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func TestCDCIterator_New(t *testing.T) {
	ctx := context.Background()
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)

	tests := []struct {
		name    string
		setup   func(t *testing.T) CDCConfig
		wantErr error
	}{
		{
			name: "publication already exists",
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
			name: "fails to create publication",
			setup: func(*testing.T) CDCConfig {
				return CDCConfig{
					PublicationName: "foobar",
				}
			},
			wantErr: errors.New("requires at least one table"),
		},
		{
			name: "fails to create subscription",
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

			i, err := NewCDCIterator(ctx, pool, config)
			if tt.wantErr != nil {
				if match := strings.Contains(err.Error(), tt.wantErr.Error()); !match {
					t.Logf("%s != %s", err.Error(), tt.wantErr.Error())
					is.True(match)
				}
			} else {
				is.NoErr(err)
			}
			if i != nil {
				is.NoErr(i.Teardown(ctx))
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
		name    string
		setup   func(t *testing.T)
		want    opencdc.Record
		wantErr bool
	}{
		{
			name: "should detect insert",
			setup: func(t *testing.T) {
				is := is.New(t)
				query := fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
							VALUES (6, 'bizz', 456, false, 12.3, 14)`, table)
				_, err := pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationCreate,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataPayloadSchemaSubject: table,
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(6)},
				Payload: opencdc.Change{
					Before: nil,
					After: opencdc.StructuredData{
						"id":      int64(6),
						"column1": "bizz",
						"column2": int32(456),
						"column3": false,
						"column4": 12.3,
						"column5": int64(14),
						"key":     nil,
					},
				},
			},
		},
		{
			name: "should detect update",
			setup: func(t *testing.T) {
				is := is.New(t)
				query := fmt.Sprintf(`UPDATE %s SET column1 = 'test cdc updates' WHERE key = '1'`, table)
				_, err := pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationUpdate,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataPayloadSchemaSubject: table,
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(1)},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"id":      int64(1),
						"column1": "test cdc updates",
						"column2": int32(123),
						"column3": false,
						"column4": 12.2,
						"column5": int64(4),
						"key":     []uint8("1"),
					},
				},
			},
		},
		{
			name: "should detect full update",
			setup: func(t *testing.T) {
				is := is.New(t)
				_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
				is.NoErr(err)
				query := fmt.Sprintf(`UPDATE %s SET column1 = 'test cdc full updates' WHERE key = '1'`, table)
				_, err = pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationUpdate,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataPayloadSchemaSubject: table,
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(1)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":      int64(1),
						"column1": "test cdc updates",
						"column2": int32(123),
						"column3": false,
						"column4": 12.2,
						"column5": int64(4),
						"key":     []uint8("1"),
					},
					After: opencdc.StructuredData{
						"id":      int64(1),
						"column1": "test cdc full updates",
						"column2": int32(123),
						"column3": false,
						"column4": 12.2,
						"column5": int64(4),
						"key":     []uint8("1"),
					},
				},
			},
		},
		{
			name: "should detect delete",
			setup: func(t *testing.T) {
				is := is.New(t)
				_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", table))
				is.NoErr(err)
				query := fmt.Sprintf(`DELETE FROM %s WHERE id = 4`, table)
				_, err = pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationDelete,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataPayloadSchemaSubject: table,
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(4)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":      int64(4),
						"column1": nil,
						"column2": nil,
						"column3": nil,
						"column4": nil,
						"column5": nil,
						"key":     nil,
					},
				},
			},
		},
		{
			name: "should detect full delete",
			setup: func(t *testing.T) {
				is := is.New(t)
				_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
				is.NoErr(err)
				query := fmt.Sprintf(`DELETE FROM %s WHERE id = 3`, table)
				_, err = pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationDelete,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataPayloadSchemaSubject: table,
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(3)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":      int64(3),
						"key":     []uint8("3"),
						"column1": "baz",
						"column2": int32(789),
						"column3": false,
						"column4": nil,
						"column5": int64(9),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			now := time.Now()

			tt.setup(t)

			// fetch the change
			nextCtx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			got, err := i.Next(nextCtx)
			is.NoErr(err)

			readAt, err := got.Metadata.GetReadAt()
			is.NoErr(err)
			is.True(readAt.After(now)) // ReadAt should be after now
			is.True(len(got.Position) > 0)
			tt.want.Metadata[opencdc.MetadataReadAt] = got.Metadata[opencdc.MetadataReadAt]
			tt.want.Position = got.Position

			is.Equal("", cmp.Diff(tt.want, got, cmpopts.IgnoreUnexported(opencdc.Record{})))
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

func TestCDCIterator_EnsureLSN(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)

	i := testCDCIterator(ctx, t, pool, table, true)
	<-i.sub.Ready()

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, column1, column2, column3, column4, column5)
				VALUES (6, 'bizz', 456, false, 12.3, 14)`, table))
	is.NoErr(err)

	r, err := i.Next(ctx)
	is.NoErr(err)

	p, err := position.ParseSDKPosition(r.Position)
	is.NoErr(err)

	lsn, err := p.LSN()
	is.NoErr(err)

	writeLSN, flushLSN, err := fetchSlotStats(t, pool, table) // table is the slot name
	is.NoErr(err)

	is.Equal(lsn, writeLSN)
	is.True(flushLSN < lsn)

	is.NoErr(i.Ack(ctx, r.Position))
	time.Sleep(2 * time.Second) // wait for at least two status updates

	writeLSN, flushLSN, err = fetchSlotStats(t, pool, table) // table is the slot name
	is.NoErr(err)

	is.True(lsn <= writeLSN)
	is.True(lsn <= flushLSN)
	is.Equal(writeLSN, flushLSN)
}

func TestCDCIterator_Ack(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		pos     opencdc.Position
		wantErr error
	}{
		{
			name:    "failed to parse position",
			pos:     opencdc.Position([]byte("{")),
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

func testCDCIterator(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, start bool) *CDCIterator {
	is := is.New(t)
	config := CDCConfig{
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table, // table is random, reuse for publication name
		SlotName:        table, // table is random, reuse for slot name
	}

	i, err := NewCDCIterator(ctx, pool, config)
	is.NoErr(err)

	i.sub.StatusTimeout = 1 * time.Second

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

func fetchSlotStats(t *testing.T, c test.Querier, slotName string) (pglogrepl.LSN, pglogrepl.LSN, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	var writeLSN, flushLSN pglogrepl.LSN
	for {
		query := fmt.Sprintf(`SELECT write_lsn, flush_lsn
								FROM pg_stat_replication s JOIN pg_replication_slots rs ON s.pid = rs.active_pid
								WHERE rs.slot_name = '%s'`, slotName)

		err := c.QueryRow(ctx, query).Scan(&writeLSN, &flushLSN)
		if err == nil {
			return writeLSN, flushLSN, nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, 0, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}
