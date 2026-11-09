// Copyright © 2026 Meroxa, Inc.
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

package schema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// blockingQuerier is a catalog that never answers: Query returns only when
// its context ends, with an error that, like some pgx errors, does not wrap
// the context error.
type blockingQuerier struct{}

func (blockingQuerier) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	<-ctx.Done()
	return nil, errors.New("conn closed")
}

// TestNotNullQueryTimeout_Default pins the bound the design depends on: the
// CDC catalog read must fail well before wal_sender_timeout (60s by default)
// ends the walsender while the replication connection is not read.
func TestNotNullQueryTimeout_Default(t *testing.T) {
	is := is.New(t)
	is.Equal(NotNullQueryTimeout, 10*time.Second)
	is.True(NotNullQueryTimeout < 60*time.Second)
}

// TestLoadNotNull_Bounded: the catalog read runs on the replication goroutine
// in CDC, so it must not hang there; an expired deadline or a cancelled
// context yields an error that says what was read and wraps the cause.
func TestLoadNotNull_Bounded(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		is := is.New(t)
		old := NotNullQueryTimeout
		NotNullQueryTimeout = 20 * time.Millisecond
		t.Cleanup(func() { NotNullQueryTimeout = old })

		start := time.Now()
		_, err := LoadNotNull(context.Background(), blockingQuerier{}, 42)
		is.True(err != nil)
		is.True(time.Since(start) < 5*time.Second) // bounded, not the caller's lifetime
		is.True(errors.Is(err, context.DeadlineExceeded))
		is.True(strings.Contains(err.Error(), "column nullability of relation 42 timed out after 20ms"))
	})
	t.Run("cancelled", func(t *testing.T) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := LoadNotNull(ctx, blockingQuerier{}, 42)
		is.True(errors.Is(err, context.Canceled))
		is.True(strings.Contains(err.Error(), "column nullability of relation 42 was cancelled"))
	})
	t.Run("cancelled, real catalog", func(t *testing.T) {
		is := is.New(t)
		ctx := test.Context(t)
		conn := test.ConnectSimple(ctx, t, test.RegularConnString)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := LoadNotNull(cctx, conn, 1259) // pg_class
		is.True(errors.Is(err, context.Canceled))
		is.True(strings.Contains(err.Error(), "column nullability of relation 1259"))
	})
}

// TestLoadNotNull_NotValidConstraint pins the Postgres 18 case of
// notNullQuery: a NOT NULL constraint added NOT VALID sets attnotnull although
// existing rows may still hold NULL, so the column must read as nullable until
// the constraint is validated.
//
// The suite's database (test/docker-compose.yml) is Postgres 17, where NOT
// NULL is never NOT VALID, so the test runs against CONDUIT_PG18_TEST_URL when
// it is set (any superuser or table owner on Postgres 18+), against the
// suite's database otherwise, and skips below 18. For example:
//
//	docker run -d -e POSTGRES_PASSWORD=pw -p 56402:5432 postgres:18
//	CONDUIT_PG18_TEST_URL=postgres://postgres:pw@127.0.0.1:56402/postgres?sslmode=disable \
//	  go test -run TestLoadNotNull_NotValidConstraint ./source/schema/
func TestLoadNotNull_NotValidConstraint(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	url := os.Getenv("CONDUIT_PG18_TEST_URL")
	if url == "" {
		url = test.RegularConnString
	}
	conn, err := pgx.Connect(ctx, url)
	is.NoErr(err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	var version int
	is.NoErr(conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version))
	if version < 180000 {
		t.Skipf("server_version_num %d: NOT NULL ... NOT VALID needs Postgres 18; set CONDUIT_PG18_TEST_URL to run this test", version)
	}

	table := fmt.Sprintf("notvalid_%d", time.Now().UnixNano())
	exec := func(sql string) {
		t.Helper()
		_, err := conn.Exec(ctx, sql)
		is.NoErr(err)
	}
	exec(fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, a int, b int, c int NOT NULL)", table))
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
	exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 1, NULL, 1)", table))
	exec(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_b_nn NOT NULL b NOT VALID", table, table))

	var oid uint32
	is.NoErr(conn.QueryRow(ctx, "SELECT $1::regclass::oid", table).Scan(&oid))
	var attNotNull bool
	is.NoErr(conn.QueryRow(ctx, "SELECT attnotnull FROM pg_attribute WHERE attrelid = $1 AND attname = 'b'", oid).Scan(&attNotNull))
	is.True(attNotNull) // the trap: attnotnull alone says NOT NULL, the row holds NULL

	nn, err := LoadNotNull(ctx, conn, oid)
	is.NoErr(err)
	is.Equal(nn, NotNull{"id": true, "a": false, "b": false, "c": true})

	exec(fmt.Sprintf("UPDATE %s SET b = 2", table))
	exec(fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s_b_nn", table, table))
	nn, err = LoadNotNull(ctx, conn, oid)
	is.NoErr(err)
	is.Equal(nn, NotNull{"id": true, "a": false, "b": true, "c": true}) // validated: NOT NULL
}
