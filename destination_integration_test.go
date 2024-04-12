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

package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	tableName := test.SetupTestTable(ctx, t, conn)

	d := NewDestination()
	err := d.Configure(
		ctx,
		map[string]string{
			"url":   test.RegularConnString,
			"table": "{{ index .Metadata \"opencdc.collection\" }}",
		},
	)
	is.NoErr(err)
	err = d.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := d.Teardown(ctx)
		is.NoErr(err)
	}()

	tests := []struct {
		name   string
		record sdk.Record
	}{{
		name: "snapshot",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationSnapshot,
			Metadata:  map[string]string{MetadataOpenCDCCollection: tableName},
			Key:       sdk.StructuredData{"id": 5000},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": "foo",
					"column2": 123,
					"column3": true,
				},
			},
		},
	}, {
		name: "create",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationCreate,
			Metadata:  map[string]string{MetadataOpenCDCCollection: tableName},
			Key:       sdk.StructuredData{"id": 5},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": "foo",
					"column2": 456,
					"column3": false,
				},
			},
		},
	}, {
		name: "insert on update (upsert)",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationUpdate,
			Metadata:  map[string]string{MetadataOpenCDCCollection: tableName},
			Key:       sdk.StructuredData{"id": 6},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": "bar",
					"column2": 567,
					"column3": true,
				},
			},
		},
	}, {
		name: "update on conflict",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationUpdate,
			Metadata:  map[string]string{MetadataOpenCDCCollection: tableName},
			Key:       sdk.StructuredData{"id": 1},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": "foobar",
					"column2": 567,
					"column3": true,
				},
			},
		},
	}, {
		name: "delete",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Metadata:  map[string]string{MetadataOpenCDCCollection: tableName},
			Operation: sdk.OperationDelete,
			Key:       sdk.StructuredData{"id": 4},
		},
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is = is.New(t)
			id := tt.record.Key.(sdk.StructuredData)["id"]

			i, err := d.Write(ctx, []sdk.Record{tt.record})
			is.NoErr(err)
			is.Equal(i, 1)

			got, err := queryTestTable(ctx, conn, tableName, id)
			switch tt.record.Operation {
			case sdk.OperationCreate, sdk.OperationSnapshot, sdk.OperationUpdate:
				is.NoErr(err)
				is.Equal(tt.record.Payload.After, got)
			case sdk.OperationDelete:
				is.Equal(err, pgx.ErrNoRows)
			}
		})
	}
}

func TestDestination_Batch(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	tableName := test.SetupTestTable(ctx, t, conn)

	d := NewDestination()
	err := d.Configure(ctx, map[string]string{"url": test.RegularConnString, "table": tableName})
	is.NoErr(err)
	err = d.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := d.Teardown(ctx)
		is.NoErr(err)
	}()

	records := []sdk.Record{{
		Position:  sdk.Position("foo1"),
		Operation: sdk.OperationCreate,
		Key:       sdk.StructuredData{"id": 5},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"column1": "foo1",
				"column2": 1,
				"column3": false,
			},
		},
	}, {
		Position:  sdk.Position("foo2"),
		Operation: sdk.OperationCreate,
		Key:       sdk.StructuredData{"id": 6},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"column1": "foo2",
				"column2": 2,
				"column3": true,
			},
		},
	}, {
		Position:  sdk.Position("foo3"),
		Operation: sdk.OperationCreate,
		Key:       sdk.StructuredData{"id": 7},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"column1": "foo3",
				"column2": 3,
				"column3": false,
			},
		},
	}}

	i, err := d.Write(ctx, records)
	is.NoErr(err)
	is.Equal(i, len(records))

	for _, rec := range records {
		got, err := queryTestTable(ctx, conn, tableName, rec.Key.(sdk.StructuredData)["id"])
		is.NoErr(err)
		is.Equal(rec.Payload.After, got)
	}
}

func queryTestTable(ctx context.Context, conn test.Querier, tableName string, id any) (sdk.StructuredData, error) {
	row := conn.QueryRow(
		ctx,
		fmt.Sprintf("SELECT column1, column2, column3 FROM %s WHERE id = $1", tableName),
		id,
	)

	var (
		col1 string
		col2 int
		col3 bool
	)
	err := row.Scan(&col1, &col2, &col3)
	if err != nil {
		return nil, err
	}

	return sdk.StructuredData{
		"column1": col1,
		"column2": col2,
		"column3": col3,
	}, nil
}
