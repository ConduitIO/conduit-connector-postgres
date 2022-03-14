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

package cdc

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/v4"
	"github.com/matryer/is"
)

const (
	// CDCTestURL is the URI for the _logical replication_ server and user.
	// This is separate from the DB_URL used above since it requires a different
	// user and permissions for replication.
	CDCTestURL = "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable"
)

func TestIterator_Next(t *testing.T) {
	is := is.New(t)
	db := getTestPostgres(t)
	i := getDefaultIterator(t)
	t.Cleanup(func() {
		is.NoErr(i.Teardown())
	})
	tests := []struct {
		name    string
		want    sdk.Record
		action  func(t *testing.T, db *pgx.Conn)
		wantErr bool
	}{
		{
			name: "should detect insert",
			action: func(t *testing.T, db *pgx.Conn) {
				rows, err := db.Query(context.Background(), `insert into
				records2(id, column1, column2, column3)
				values (6, 'bizz', 456, false);`)
				is.NoErr(err)
				defer rows.Close()
			},
			wantErr: false,
			want: sdk.Record{
				Key: sdk.StructuredData{"id": int64(6)},
				Metadata: map[string]string{
					"table":  "records2",
					"action": "insert",
				},
				Payload: sdk.StructuredData{
					"column1": string("bizz"),
					"column2": int32(456),
					"column3": bool(false),
				},
			},
		},
		{
			name: "should detect update",
			action: func(t *testing.T, db *pgx.Conn) {
				rows, err := db.Query(context.Background(),
					`update records2 * set column1 = 'test cdc updates' 
					where key = '1';`)
				is.NoErr(err)
				defer rows.Close()
			},
			wantErr: false,
			want: sdk.Record{
				Key: sdk.StructuredData{"id": int64(1)},
				Metadata: map[string]string{
					"table":  "records2",
					"action": "update",
				},
				Payload: sdk.StructuredData{
					"column1": string("test cdc updates"),
					"column2": int32(123),
					"column3": bool(false),
					"key":     []uint8("1"),
				},
			},
		},
		{
			name: "should detect delete",
			action: func(t *testing.T, db *pgx.Conn) {
				rows, err := db.Query(context.Background(),
					`delete from records2 where id = 3;`)
				is.NoErr(err)
				defer rows.Close()
			},
			wantErr: false,
			want: sdk.Record{
				Key: sdk.StructuredData{"id": int64(3)},
				Metadata: map[string]string{
					"table":  "records2",
					"action": "delete",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			tt.action(t, db)
			time.Sleep(1 * time.Second)

			got, err := i.Next(context.Background())
			is.NoErr(err)

			diff := cmp.Diff(
				got,
				tt.want,
				cmpopts.IgnoreFields(
					sdk.Record{},
					"CreatedAt",
					"Position", // TODO: Assert what we can about position
				))
			if diff != "" {
				t.Errorf("%s", diff)
			}
			is.True(got.CreatedAt.After(now)) // CreatedAt should be After now
			is.NoErr(i.Ack(context.Background(), got.Position))
		})
	}
}

// getDefaultIterator
func getDefaultIterator(t *testing.T) *Iterator {
	is := is.New(t)
	_ = getTestPostgres(t)
	ctx := context.Background()
	randPublication := fmt.Sprintf("confuit%d", rand.Int())
	randSlotName := fmt.Sprintf("conduit%d", rand.Int())
	config := Config{
		URL:             CDCTestURL,
		TableName:       "records2",
		PublicationName: randPublication,
		SlotName:        randSlotName,
	}
	i, err := NewCDCIterator(ctx, config)
	is.NoErr(err)
	is.Equal(i.config.KeyColumnName, "id")
	is.Equal([]string{"id", "key", "column1", "column2", "column3"},
		i.config.Columns)
	return i
}

// getTestPostgres is a testing helper that fails if it can't setup a Postgres
// connection and returns a DB and the connection string.
// * It starts and migrates a db with 5 rows for Test_Read* and Test_Open*
func getTestPostgres(t *testing.T) *pgx.Conn {
	is := is.New(t)
	prepareDB := []string{
		`DROP TABLE IF EXISTS records2;`,
		`CREATE TABLE IF NOT EXISTS records2 (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean);`,
		`INSERT INTO records2(key, column1, column2, column3)
		VALUES('1', 'foo', 123, false),
		('2', 'bar', 456, true),
		('3', 'baz', 789, false),
		('4', null, null, null);`,
	}
	conn, err := pgx.Connect(context.Background(), CDCTestURL)
	is.NoErr(err)
	conn = migrate(t, conn, prepareDB)
	is.NoErr(err)
	return conn
}

// migrate will run a set of migrations on a database to prepare it for a test
// it fails the test if any migrations are not applied.
func migrate(t *testing.T, conn *pgx.Conn, migrations []string) *pgx.Conn {
	is := is.New(t)
	for _, migration := range migrations {
		_, err := conn.Exec(context.Background(), migration)
		is.NoErr(err)
	}
	return conn
}
