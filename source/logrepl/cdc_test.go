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
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hamba/avro/v2"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

func TestCDCIterator_New(t *testing.T) {
	ctx := test.Context(t)
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
				table := test.SetupTable(ctx, t, pool)
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
				table := test.SetupTable(ctx, t, pool)

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

func TestCDCIterator_Operation_NextN(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTable(ctx, t, pool)
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
				query := fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
							VALUES (6, '6', 'bizz', 456, false, 12.3, 61)`, table)
				_, err := pool.Exec(ctx, query)
				is.NoErr(err)
			},
			wantErr: false,
			want: opencdc.Record{
				Operation: opencdc.OperationCreate,
				Metadata: map[string]string{
					opencdc.MetadataCollection:           table,
					opencdc.MetadataKeySchemaSubject:     table + "_key",
					opencdc.MetadataKeySchemaVersion:     "1",
					opencdc.MetadataPayloadSchemaSubject: table + "_payload",
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(6)},
				Payload: opencdc.Change{
					Before: nil,
					After: opencdc.StructuredData{
						"id":               int64(6),
						"column1":          "bizz",
						"column2":          int32(456),
						"column3":          false,
						"column4":          big.NewRat(123, 10),
						"key":              []uint8("6"),
						"UppercaseColumn1": int32(61),
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
					opencdc.MetadataKeySchemaSubject:     table + "_key",
					opencdc.MetadataKeySchemaVersion:     "1",
					opencdc.MetadataPayloadSchemaSubject: table + "_payload",
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(1)},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"id":               int64(1),
						"column1":          "test cdc updates",
						"column2":          int32(123),
						"column3":          false,
						"column4":          big.NewRat(122, 10),
						"key":              []uint8("1"),
						"UppercaseColumn1": int32(1),
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
					opencdc.MetadataKeySchemaSubject:     table + "_key",
					opencdc.MetadataKeySchemaVersion:     "1",
					opencdc.MetadataPayloadSchemaSubject: table + "_payload",
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(1)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":               int64(1),
						"column1":          "test cdc updates",
						"column2":          int32(123),
						"column3":          false,
						"column4":          big.NewRat(122, 10),
						"key":              []uint8("1"),
						"UppercaseColumn1": int32(1),
					},
					After: opencdc.StructuredData{
						"id":               int64(1),
						"column1":          "test cdc full updates",
						"column2":          int32(123),
						"column3":          false,
						"column4":          big.NewRat(122, 10),
						"key":              []uint8("1"),
						"UppercaseColumn1": int32(1),
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
					opencdc.MetadataKeySchemaSubject:     table + "_key",
					opencdc.MetadataKeySchemaVersion:     "1",
					opencdc.MetadataPayloadSchemaSubject: table + "_payload",
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(4)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":               int64(4),
						"column1":          nil,
						"column2":          nil,
						"column3":          nil,
						"column4":          nil,
						"key":              nil,
						"UppercaseColumn1": nil,
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
					opencdc.MetadataKeySchemaSubject:     table + "_key",
					opencdc.MetadataKeySchemaVersion:     "1",
					opencdc.MetadataPayloadSchemaSubject: table + "_payload",
					opencdc.MetadataPayloadSchemaVersion: "1",
				},
				Key: opencdc.StructuredData{"id": int64(3)},
				Payload: opencdc.Change{
					Before: opencdc.StructuredData{
						"id":               int64(3),
						"key":              []uint8("3"),
						"column1":          "baz",
						"column2":          int32(789),
						"column3":          false,
						"column4":          big.NewRat(836, 25),
						"UppercaseColumn1": int32(3),
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
			records, err := i.NextN(nextCtx, 1)
			is.NoErr(err)

			got := records[0]

			readAt, err := got.Metadata.GetReadAt()
			is.NoErr(err)
			is.True(readAt.After(now)) // ReadAt should be after now
			is.True(len(got.Position) > 0)
			tt.want.Metadata[opencdc.MetadataReadAt] = got.Metadata[opencdc.MetadataReadAt]
			tt.want.Position = got.Position

			is.Equal("", cmp.Diff(
				tt.want,
				got,
				cmpopts.IgnoreUnexported(opencdc.Record{}),
				test.BigRatComparer,
			))
			is.NoErr(i.Ack(ctx, got.Position))
		})
	}
}

func TestCDCIterator_EnsureLSN(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTable(ctx, t, pool)

	i := testCDCIterator(ctx, t, pool, table, true)
	<-i.sub.Ready()

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
				VALUES (6, '6', 'bizz', 456, false, 12.3, 6)`, table))
	is.NoErr(err)

	rr, err := i.NextN(ctx, 1)
	is.NoErr(err)
	is.True(len(rr) > 0)

	r := rr[0]

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
	ctx := test.Context(t)

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

func TestCDCIterator_NextN_InternalBatching(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTable(ctx, t, pool)

	is := is.New(t)
	underTest := testCDCIterator(ctx, t, pool, table, true)
	<-underTest.sub.Ready()

	insertTestRows(ctx, is, pool, table, 1, 1)
	// wait until the CDCHandler flushes this one record
	// so that we force the CDCIterator to wait for another batch
	time.Sleep(time.Second * 2)
	insertTestRows(ctx, is, pool, table, 2, 5)

	// we request 2 records, expect records 1 and 2
	got, err := underTest.NextN(ctx, 2)
	is.NoErr(err)
	verifyOpenCDCRecords(is, got, table, 1, 2)
	time.Sleep(200 * time.Millisecond)

	// we request 2 records, expect records 3 and 4
	got, err = underTest.NextN(ctx, 2)
	is.NoErr(err)
	verifyOpenCDCRecords(is, got, table, 3, 4)
	time.Sleep(200 * time.Millisecond)

	// we request 2 records, expect record 5
	got, err = underTest.NextN(ctx, 2)
	is.NoErr(err)
	verifyOpenCDCRecords(is, got, table, 5, 5)
}

func insertTestRows(ctx context.Context, is *is.I, pool *pgxpool.Pool, table string, from int, to int) {
	for i := from; i <= to; i++ {
		_, err := pool.Exec(
			ctx,
			fmt.Sprintf(
				`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
				VALUES (%d, '%d', 'test-%d', %d, false, 12.3, %d)`, table, i+10, i+10, i, i*100, i+10,
			),
		)
		is.NoErr(err)
	}
}

func TestCDCIterator_NextN(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTable(ctx, t, pool)

	t.Run("retrieve exact N records", func(t *testing.T) {
		is := is.New(t)
		i := testCDCIterator(ctx, t, pool, table, true)
		<-i.sub.Ready()

		for j := 1; j <= 3; j++ {
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
				VALUES (%d, '%d', 'test-%d', %d, false, 12.3, 4)`, table, j+10, j+10, j, j*100))
			is.NoErr(err)
		}

		var allRecords []opencdc.Record
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		// Collect records until we have all 3
		for len(allRecords) < 3 {
			records, err := i.NextN(attemptCtx, 3-len(allRecords))
			is.NoErr(err)
			// Only proceed if we got at least one record
			is.True(len(records) > 0)
			allRecords = append(allRecords, records...)
		}

		is.Equal(len(allRecords), 3)

		for j, r := range allRecords {
			is.Equal(r.Operation, opencdc.OperationCreate)
			is.Equal(r.Key.(opencdc.StructuredData)["id"], int64(j+11))
			data := r.Payload.After.(opencdc.StructuredData)
			is.Equal(data["column1"], fmt.Sprintf("test-%d", j+1))
			//nolint:gosec // no risk to overflow
			is.Equal(data["column2"], (int32(j)+1)*100)
		}
	})

	t.Run("retrieve fewer records than requested", func(t *testing.T) {
		is := is.New(t)
		i := testCDCIterator(ctx, t, pool, table, true)
		<-i.sub.Ready()

		for j := 1; j <= 2; j++ {
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
				VALUES (%d, '%d', 'test-%d', %d, false, 12.3, 4)`, table, j+20, j+20, j, j*100))
			is.NoErr(err)
		}

		// Will keep calling NextN until all records are received
		records := make([]opencdc.Record, 0, 2)
		for len(records) < 2 {
			recordsTmp, err := i.NextN(ctx, 5)
			is.NoErr(err)
			records = append(records, recordsTmp...)
		}

		// nothing else to fetch
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, err := i.NextN(ctxWithTimeout, 5)
		is.True(errors.Is(err, context.DeadlineExceeded))

		for j, r := range records {
			is.Equal(r.Operation, opencdc.OperationCreate)
			is.Equal(r.Key.(opencdc.StructuredData)["id"], int64(j+21))
			change := r.Payload
			data := change.After.(opencdc.StructuredData)
			is.Equal(data["column1"], fmt.Sprintf("test-%d", j+1))
			//nolint:gosec // no risk to overflow
			is.Equal(data["column2"], (int32(j)+1)*100)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		is := is.New(t)
		i := testCDCIterator(ctx, t, pool, table, true)
		<-i.sub.Ready()

		ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		_, err := i.NextN(ctxTimeout, 5)
		is.True(errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("subscriber not started", func(t *testing.T) {
		is := is.New(t)
		i := testCDCIterator(ctx, t, pool, table, false)

		_, err := i.NextN(ctx, 5)
		is.Equal(err.Error(), "logical replication has not been started")
	})

	t.Run("invalid N values", func(t *testing.T) {
		is := is.New(t)
		i := testCDCIterator(ctx, t, pool, table, true)
		<-i.sub.Ready()

		_, err := i.NextN(ctx, 0)
		is.True(strings.Contains(err.Error(), "n must be greater than 0"))

		_, err = i.NextN(ctx, -1)
		is.True(strings.Contains(err.Error(), "n must be greater than 0"))
	})
}

func verifyOpenCDCRecords(is *is.I, got []opencdc.Record, tableName string, fromID, toID int) {
	is.Helper()

	// Build the expected records slice
	var want []opencdc.Record

	for i := fromID; i <= toID; i++ {
		id := int64(i + 10)
		record := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Key: opencdc.StructuredData{
				"id": id,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"id":      id,
					"key":     []uint8(fmt.Sprintf("%d", id)),
					"column1": fmt.Sprintf("test-%d", i),
					"column2": int32(i) * 100, //nolint:gosec // fine, we know the value is small enough
					"column3": false,
					"column4": big.NewRat(123, 10),
					// UppercaseColumn1 is a Postgres interger (4 bytes)
					"UppercaseColumn1": int32(id),
				},
			},
			Metadata: opencdc.Metadata{
				opencdc.MetadataCollection: tableName,
			},
		}

		want = append(want, record)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(opencdc.Record{}),
		cmpopts.IgnoreFields(opencdc.Record{}, "Position", "Metadata"),
		test.BigRatComparer,
	}
	is.Equal("", cmp.Diff(want, got, cmpOpts...)) // mismatch (-want +got)
}

func testCDCIterator(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, start bool) *CDCIterator {
	is := is.New(t)
	config := CDCConfig{
		Tables:          []string{table},
		TableKeys:       map[string]string{table: "id"},
		PublicationName: table, // table is random, reuse for publication name
		SlotName:        table, // table is random, reuse for slot name
		WithAvroSchema:  true,
		BatchSize:       2,
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

	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
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

func TestCDCIterator_Schema(t *testing.T) {
	ctx := test.Context(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTable(ctx, t, pool)

	i := testCDCIterator(ctx, t, pool, table, true)
	<-i.sub.Ready()

	t.Run("initial table schema", func(t *testing.T) {
		is := is.New(t)

		_, err := pool.Exec(
			ctx,
			fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, "UppercaseColumn1")
				VALUES (6, '6', 'bizz', 456, false, 12.3, 6)`, table),
		)
		is.NoErr(err)

		rr, err := i.NextN(ctx, 1)
		is.NoErr(err)
		is.True(len(rr) > 0)

		r := rr[0]

		assertPayloadSchemaOK(ctx, is, test.TestTableAvroSchemaV1, table, r)
		assertKeySchemaOK(ctx, is, table, r)
	})

	t.Run("column added", func(t *testing.T) {
		is := is.New(t)

		_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN column101 timestamp;`, table))
		is.NoErr(err)

		_, err = pool.Exec(
			ctx,
			fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column4, column101, "UppercaseColumn1")
				VALUES (7, decode('aabbcc', 'hex'), 'example data 1', 100, true, 12345.678, '2023-09-09 10:00:00', 7);`, table),
		)
		is.NoErr(err)

		rr, err := i.NextN(ctx, 1)
		is.NoErr(err)
		is.True(len(rr) > 0)

		r := rr[0]

		assertPayloadSchemaOK(ctx, is, test.TestTableAvroSchemaV2, table, r)
		assertKeySchemaOK(ctx, is, table, r)
	})

	t.Run("column removed", func(t *testing.T) {
		is := is.New(t)

		_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN column4;`, table))
		is.NoErr(err)

		_, err = pool.Exec(
			ctx,
			fmt.Sprintf(`INSERT INTO %s (id, key, column1, column2, column3, column101, "UppercaseColumn1")
				VALUES (8, decode('aabbcc', 'hex'), 'example data 1', 100, true, '2023-09-09 10:00:00', 8);`, table),
		)
		is.NoErr(err)

		rr, err := i.NextN(ctx, 1)
		is.NoErr(err)
		is.True(len(rr) > 0)

		r := rr[0]

		assertPayloadSchemaOK(ctx, is, test.TestTableAvroSchemaV3, table, r)
		assertKeySchemaOK(ctx, is, table, r)
	})
}

func assertPayloadSchemaOK(ctx context.Context, is *is.I, wantSchemaTemplate string, table string, r opencdc.Record) {
	gotConduitSch, err := getPayloadSchema(ctx, r)
	is.NoErr(err)

	want, err := avro.Parse(fmt.Sprintf(wantSchemaTemplate, table+"_payload"))
	is.NoErr(err)

	got, err := avro.ParseBytes(gotConduitSch.Bytes)
	is.NoErr(err)

	is.Equal(want.String(), got.String())
}

func assertKeySchemaOK(ctx context.Context, is *is.I, table string, r opencdc.Record) {
	gotConduitSch, err := getKeySchema(ctx, r)
	is.NoErr(err)

	want, err := avro.Parse(fmt.Sprintf(test.TestTableKeyAvroSchema, table+"_key"))
	is.NoErr(err)

	got, err := avro.ParseBytes(gotConduitSch.Bytes)
	is.NoErr(err)

	is.Equal(want.String(), got.String())
}

func getPayloadSchema(ctx context.Context, r opencdc.Record) (schema.Schema, error) {
	payloadSubj, err := r.Metadata.GetPayloadSchemaSubject()
	if err != nil {
		return schema.Schema{}, fmt.Errorf("GetPayloadSchemaSubject failed: %w", err)
	}

	payloadV, err := r.Metadata.GetPayloadSchemaVersion()
	if err != nil {
		return schema.Schema{}, fmt.Errorf("GetPayloadSchemaVersion failed: %w", err)
	}

	payloadSch, err := sdkschema.Get(ctx, payloadSubj, payloadV)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed getting schema: %w", err)
	}

	return payloadSch, nil
}

func getKeySchema(ctx context.Context, r opencdc.Record) (schema.Schema, error) {
	keySubj, err := r.Metadata.GetKeySchemaSubject()
	if err != nil {
		return schema.Schema{}, fmt.Errorf("GetKeySchemaSubject failed: %w", err)
	}

	keyV, err := r.Metadata.GetKeySchemaVersion()
	if err != nil {
		return schema.Schema{}, fmt.Errorf("GetKeySchemaVersion failed: %w", err)
	}

	keySch, err := sdkschema.Get(ctx, keySubj, keyV)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed getting schema: %w", err)
	}

	return keySch, nil
}
