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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v4"
	"github.com/matryer/is"
)

func TestSnapshotIterator_Next(t *testing.T) {
	i := is.New(t)
	ctx := context.Background()
	db := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	type fields struct {
		uri          string
		table        string
		columns      []string
		keyColumn    string
		snapshotName string
	}
	type args struct {
		in0 context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    sdk.Record
		wantErr bool
	}{
		{
			name: "should return the first record",
			fields: fields{
				table:        test.SetupTestTable(ctx, t, db),
				columns:      []string{"id", "key", "column1", "column2", "column3"},
				keyColumn:    "key",
				uri:          test.RepmgrConnString,
				snapshotName: createTestSnapshot(t, db),
			},
			args: args{
				in0: context.Background(),
			},
			want: sdk.Record{
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
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSnapshotIterator(context.Background(), SnapshotConfig{
				SnapshotName: tt.fields.snapshotName,
				Table:        tt.fields.table,
				Columns:      tt.fields.columns,
				KeyColumn:    tt.fields.keyColumn,
				URI:          tt.fields.uri,
			})
			i.NoErr(err)

			t.Cleanup(func() {
				i.NoErr(s.Teardown(context.Background()))
			})

			now := time.Now()
			got, err := s.Next(tt.args.in0)
			if (err != nil) != tt.wantErr {
				t.Errorf("SnapshotIterator.Next() error = %v, wantErr %v", err, tt.wantErr)
			}

			i.True(got.Metadata["table"] == tt.fields.table)
			delete(got.Metadata, "table") // delete so we don't compare in diff
			i.True(got.CreatedAt.After(now))
			got.CreatedAt = time.Time{} // reset so we don't compare in diff

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SnapshotIterator.Next() = %v, want %v", got, tt.want)
				if diff := cmp.Diff(got, tt.want); diff != "" {
					t.Log(diff)
				}
			}
		})
	}
}

func TestIteration(t *testing.T) {
	i := is.New(t)
	ctx := context.Background()
	db := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	s, err := NewSnapshotIterator(context.Background(), SnapshotConfig{
		SnapshotName: createTestSnapshot(t, db),
		Table:        test.SetupTestTable(ctx, t, db),
		Columns:      []string{"id", "key", "column1", "column2", "column3"},
		KeyColumn:    "key",
		URI:          test.RepmgrConnString,
	})
	i.NoErr(err)

	now := time.Now()
	for idx := 0; idx < 2; idx++ {
		rec, err := s.Next(ctx)
		i.Equal(string(rec.Position), strconv.FormatInt(int64(idx), 10))
		i.NoErr(err)
		t.Log(rec)
		rec.CreatedAt.After(now)
	}

	i.NoErr(s.Teardown(ctx))
}

func createTestSnapshot(t *testing.T, db *pgx.Conn) string {
	var n *string
	query := `SELECT * FROM pg_catalog.pg_export_snapshot();`
	row := db.QueryRow(context.Background(), query)
	err := row.Scan(&n)
	if err != nil {
		t.Logf("failed to scan name: %s", err)
		t.Fail()
	}
	return *n
}
