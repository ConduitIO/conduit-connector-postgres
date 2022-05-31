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

package test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/matryer/is"
)

// RepmgrConnString is a replication user connection string for the test postgres.
const RepmgrConnString = "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable"

// RegularConnString is a non-replication user connection string for the test postgres.
const RegularConnString = "postgres://meroxauser:meroxapass@localhost:5432/meroxadb?sslmode=disable"

type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func ConnectPool(ctx context.Context, t *testing.T, connString string) *pgxpool.Pool {
	is := is.New(t)
	pool, err := pgxpool.Connect(ctx, connString)
	is.NoErr(err)
	t.Cleanup(func() {
		// close connection with fresh context
		pool.Close()
	})
	return pool
}

func ConnectSimple(ctx context.Context, t *testing.T, connString string) *pgx.Conn {
	is := is.New(t)
	pool := ConnectPool(ctx, t, connString)
	conn, err := pool.Acquire(ctx)
	is.NoErr(err)
	t.Cleanup(func() {
		conn.Release()
	})
	return conn.Conn()
}

// SetupTestTable creates a new table and returns its name.
func SetupTestTable(ctx context.Context, t *testing.T, conn Querier) string {
	is := is.New(t)

	table := RandomIdentifier(t)

	query := `
		CREATE TABLE %s (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})

	query = `
		INSERT INTO %s (key, column1, column2, column3)
		VALUES ('1', 'foo', 123, false),
		('2', 'bar', 456, true),
		('3', 'baz', 789, false),
		('4', null, null, null)`
	query = fmt.Sprintf(query, table)
	_, err = conn.Exec(ctx, query)
	is.NoErr(err)

	return table
}

// SetupTestTableV2 returns a test table with a random identifier and two
// test records that have int2, int4, int8, varchar, text, date, json, and a
// bigserial primary key column.
func SetupTestTableV2(ctx context.Context, t *testing.T, conn Querier) string {
	is := is.New(t)

	table := RandomIdentifier(t)

	_, err := conn.Exec(ctx, fmt.Sprintf(`create table %s (a int2, b int4, c int8, d varchar, e text, f date, g json, h bigserial primary key)`, table))
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %s`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(ctx, query)
		is.NoErr(err)
	})

	_, err = conn.Exec(ctx, fmt.Sprintf(`insert into %s values (0, 1, 2, 'abc	', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`, table))
	is.NoErr(err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`insert into %s values ( 3, null, null, null, null, null, '{"foo":"bar"}')`, table))
	is.NoErr(err)

	return table
}

func RandomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}

func IsPgError(is *is.I, err error, wantCode string) {
	is.True(err != nil)
	var pgerr *pgconn.PgError
	ok := errors.As(err, &pgerr)
	is.True(ok) // expected err to be a *pgconn.PgError
	is.Equal(pgerr.Code, wantCode)
}
