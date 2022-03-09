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

package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/lib/pq"
	"github.com/matryer/is"
)

// SnapshotTestURL is a non-replication user url for the test postgres d
const SnapshotTestURL = "postgres://meroxauser:meroxapass@localhost:5432/meroxadb?sslmode=disable"

func TestSnapshotterReads(t *testing.T) {
	is := is.New(t)
	db := getTestPostgres(t)
	s, err := NewSnapshotter(db, "records3", []string{"id",
		"column1", "key"}, "key")
	is.NoErr(err)
	i := 0
	for {
		if next := s.HasNext(); !next {
			break
		}
		i++
		_, err := s.Next(context.Background())
		is.NoErr(err)
	}
	is.Equal(4, i)
	is.NoErr(s.Teardown())
	is.True(s.snapshotComplete == true) // failed to mark snapshot complete
}

func TestSnapshotterTeardown(t *testing.T) {
	is := is.New(t)
	db := getTestPostgres(t)
	s, err := NewSnapshotter(db, "records3", []string{"id",
		"column1", "key"}, "key")
	is.NoErr(err)
	is.True(s.HasNext()) // failed to queue up record
	_, err = s.Next(context.Background())
	is.NoErr(err)
	is.True(!s.snapshotComplete) // snapshot prematurely marked complete
	got := s.Teardown()
	is.True(errors.Is(got, ErrSnapshotInterrupt)) // failed to get snapshot interrupt
}

func TestPrematureDBClose(t *testing.T) {
	is := is.New(t)
	db := getTestPostgres(t)
	s, err := NewSnapshotter(db, "records3", []string{"id",
		"column1", "key"}, "key")
	is.NoErr(err)
	next1 := s.HasNext()
	is.Equal(true, next1)
	teardownErr := s.Teardown()
	is.True(errors.Is(teardownErr, ErrSnapshotInterrupt)) // failed to get snapshot interrupt error
	_, err = s.Next(context.Background())
	is.True(err != nil)
	next2 := s.HasNext()
	is.Equal(false, next2)
	rec, err := s.Next(context.Background())
	is.Equal(rec, sdk.Record{})
	is.True(errors.Is(err, ErrNoRows)) // failed to get snapshot incomplete
}

// getTestPostgres is a testing helper that fails if it can't setup a Postgres
// connection and returns a DB and the connection string.
// * It starts and migrates a db with 5 rows for Test_Read* and Test_Open*
func getTestPostgres(t *testing.T) *sql.DB {
	is := is.New(t)
	prepareDB := []string{
		// drop any existing data
		`DROP TABLE IF EXISTS records3;`,
		// setup records3 table
		`CREATE TABLE IF NOT EXISTS records3 (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean);`,
		// seed values
		`INSERT INTO records3(key, column1, column2, column3)
		VALUES('1', 'foo', 123, false),
		('2', 'bar', 456, true),
		('3', 'baz', 789, false),
		('4', null, null, null);`,
	}
	db, err := sql.Open("postgres", SnapshotTestURL)
	is.NoErr(err)
	db = migrate(t, db, prepareDB)
	is.NoErr(err)
	return db
}

// migrate will run a set of migrations on a database to prepare it for a test
// it fails the test if any migrations are not applied.
func migrate(t *testing.T, db *sql.DB, migrations []string) *sql.DB {
	is := is.New(t)
	for _, migration := range migrations {
		_, err := db.Exec(migration)
		is.NoErr(err)
	}
	return db
}
