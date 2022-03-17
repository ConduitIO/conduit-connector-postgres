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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/matryer/is"
)

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

	table := fmt.Sprintf("conduit_%v_%d", strings.ToLower(t.Name()), time.Now().UnixMicro()%1000)

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
		_, err := conn.Exec(ctx, query)
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
