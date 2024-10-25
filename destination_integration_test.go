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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
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
		record opencdc.Record
	}{
		{
			name: "snapshot",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationSnapshot,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 5000},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1": "foo",
						"column2": 123,
						"column3": true,
					},
				},
			},
		}, {
			name: "create",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationCreate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 5},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1": "foo",
						"column2": 456,
						"column3": false,
					},
				},
			},
		}, {
			name: "insert on update (upsert)",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationUpdate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 6},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1": "bar",
						"column2": 567,
						"column3": true,
					},
				},
			},
		}, {
			name: "update on conflict",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Operation: opencdc.OperationUpdate,
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Key:       opencdc.StructuredData{"id": 1},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"column1": "foobar",
						"column2": 567,
						"column3": true,
					},
				},
			},
		}, {
			name: "delete",
			record: opencdc.Record{
				Position:  opencdc.Position("foo"),
				Metadata:  map[string]string{opencdc.MetadataCollection: tableName},
				Operation: opencdc.OperationDelete,
				Key:       opencdc.StructuredData{"id": 4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is = is.New(t)
			id := tt.record.Key.(opencdc.StructuredData)["id"]

			i, err := d.Write(ctx, []opencdc.Record{tt.record})
			is.NoErr(err)
			is.Equal(i, 1)

			got, err := queryTestTable(ctx, conn, tableName, id)
			switch tt.record.Operation {
			case opencdc.OperationCreate, opencdc.OperationSnapshot, opencdc.OperationUpdate:
				is.NoErr(err)
				is.Equal(tt.record.Payload.After, got)
			case opencdc.OperationDelete:
				is.Equal(err, pgx.ErrNoRows)
			}
		})
	}
}

func TestDestination_Batch(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
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

	records := []opencdc.Record{{
		Position:  opencdc.Position("foo1"),
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": 5},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"column1": "foo1",
				"column2": 1,
				"column3": false,
			},
		},
	}, {
		Position:  opencdc.Position("foo2"),
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": 6},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"column1": "foo2",
				"column2": 2,
				"column3": true,
			},
		},
	}, {
		Position:  opencdc.Position("foo3"),
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": 7},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
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
		got, err := queryTestTable(ctx, conn, tableName, rec.Key.(opencdc.StructuredData)["id"])
		is.NoErr(err)
		is.Equal(rec.Payload.After, got)
	}
}

func queryTestTable(ctx context.Context, conn test.Querier, tableName string, id any) (opencdc.StructuredData, error) {
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

	return opencdc.StructuredData{
		"column1": col1,
		"column2": col2,
		"column3": col3,
	}, nil
}
