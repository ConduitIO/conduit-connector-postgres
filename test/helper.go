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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// RepmgrConnString is a replication user connection string for the test postgres.
var RepmgrConnString = "postgres://repmgr:repmgrmeroxa@127.0.0.1:" + testPort("PG_TEST_PORT", "5433") + "/meroxadb?sslmode=disable"

// RegularConnString is a non-replication user connection string for the test postgres.
var RegularConnString = "postgres://meroxauser:meroxapass@127.0.0.1:" + testPort("PG_TEST_PORT", "5433") + "/meroxadb?sslmode=disable"

// testPort returns the host port of a test service: the environment variable
// env (also read by test/docker-compose.yml) or def. It lets the suite run
// next to another stack that holds the default ports.
func testPort(env, def string) string {
	if p := os.Getenv(env); p != "" {
		return p
	}
	return def
}

// BouncerConnString connects the regular user through the test PgBouncer
// (test/docker-compose.yml, test/pgbouncer/pgbouncer.ini): transaction pooling,
// two server connections, and prepared-statement tracking
// (max_prepared_statements=200), so prepared statements are shared between the
// clients of a server connection.
var BouncerConnString = "postgres://meroxauser:meroxapass@127.0.0.1:" + testPort("PGBOUNCER_TEST_PORT", "6432") + "/meroxadb?sslmode=disable"

// TestTableAvroSchemaV1 is the Avro schema representation of the test table
// defined through testTableCreateQuery.
// The fields are sorted by name.
const TestTableAvroSchemaV1 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {
            "name": "column4",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 16,
                "scale": 3
            }
        },
        {
            "name": "column5",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 5
            }
        },
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableAvroSchemaV2 is TestTableAvroSchemaV1 with `column6` (local-timestamp-micros) added.
const TestTableAvroSchemaV2 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column101","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {
            "name": "column4",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 16,
                "scale": 3
            }
        },
        {
            "name": "column5",
            "type":
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 5
            }
        },
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableAvroSchemaV3 is TestTableAvroSchemaV1 with `column4` and `column5` dropped.
const TestTableAvroSchemaV3 = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
	{"name":"UppercaseColumn1","type":"int"},
        {"name":"column1","type":"string"},
        {"name":"column101","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"column2","type":"int"},
        {"name":"column3","type":"boolean"},
        {"name":"column6","type":"bytes"},
        {"name":"column7","type":"bytes"},
        {"name":"id","type":"long"},
        {"name":"key","type":"bytes"}
    ]
}`

// TestTableKeyAvroSchema is the Avro schema for the test table's key column.
const TestTableKeyAvroSchema = `{
    "type": "record",
    "name": "%s",
    "fields":
    [
        {"name":"id","type":"long"}
    ]
}`

// When updating this table, TestTableAvroSchemaV1 needs to be updated too.
const testTableCreateQuery = `
		CREATE TABLE %q (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean,
		column4 numeric(16,3),
		column5 numeric(5),
		column6 jsonb,
		column7 json,
		"UppercaseColumn1" integer
	)`

type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func ConnectPool(ctx context.Context, t *testing.T, connString string) *pgxpool.Pool {
	is := is.New(t)
	pool, err := cpool.New(ctx, connString)
	is.NoErr(err)
	t.Cleanup(func() {
		// close connection with fresh context
		is := is.New(t)
		is.NoErr(csync.RunTimeout(context.Background(), pool.Close, time.Second*10))
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
func SetupEmptyTestTable(ctx context.Context, t *testing.T, conn Querier) string {
	table := RandomIdentifier(t)
	SetupEmptyTestTableWithName(ctx, t, conn, table)
	return table
}

func SetupEmptyTestTableWithName(ctx context.Context, t *testing.T, conn Querier, table string) {
	is := is.New(t)

	query := fmt.Sprintf(testTableCreateQuery, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE %q`
		query = fmt.Sprintf(query, table)
		_, err := conn.Exec(context.Background(), query)
		is.NoErr(err)
	})
}

func SetupTestTableWithName(ctx context.Context, t *testing.T, conn Querier, table string) {
	is := is.New(t)
	SetupEmptyTestTableWithName(ctx, t, conn, table)

	query := `
		INSERT INTO %q (key, column1, column2, column3, column4, column5, column6, column7, "UppercaseColumn1")
		VALUES ('1', 'foo', 123, false, 12.2, 4, '{"foo": "bar"}', '{"foo": "baz"}', 1),
		('2', 'bar', 456, true, 13.42, 8, '{"foo": "bar"}', '{"foo": "baz"}', 2),
		('3', 'baz', 789, false, null, 9, '{"foo": "bar"}', '{"foo": "baz"}', 3),
		('4', null, null, null, 91.1, null, null, null, null)`
	query = fmt.Sprintf(query, table)
	_, err := conn.Exec(ctx, query)
	is.NoErr(err)
}

// SetupTestTable creates a new table and returns its name.
func SetupTestTable(ctx context.Context, t *testing.T, conn Querier) string {
	table := RandomIdentifier(t)
	SetupTestTableWithName(ctx, t, conn, table)
	return table
}

func CreateReplicationSlot(t *testing.T, conn Querier, slotName string) {
	is := is.New(t)

	_, err := conn.Exec(
		context.Background(),
		"SELECT pg_create_logical_replication_slot($1, $2)",
		slotName,
		"pgoutput",
	)
	is.NoErr(err)

	t.Cleanup(func() {
		_, err := conn.Exec(
			context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
			slotName,
		)
		is.NoErr(err)
	})
}

func CreatePublication(t *testing.T, conn Querier, pubName string, tables []string) {
	is := is.New(t)

	quotedTables := make([]string, 0, len(tables))
	for _, t := range tables {
		// don't use internal.WrapSQLIdent to prevent import cycle
		quotedTables = append(quotedTables, strconv.Quote(t))
	}

	_, err := conn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, strings.Join(quotedTables, ",")),
	)
	is.NoErr(err)

	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", pubName))
		is.NoErr(err)
	})
}

// maxIdentifierLen is Postgres' NAMEDATALEN-1. Longer identifiers are
// silently truncated, which makes two distinct long names collapse into one.
const maxIdentifierLen = 63

var (
	// randomIdentifierSeq makes identifiers unique within a process.
	randomIdentifierSeq atomic.Uint64
	// randomIdentifierProc makes identifiers unique across test processes
	// (go test runs each package as its own process against the same server).
	randomIdentifierProc = newProcessID()
)

func newProcessID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("test: reading random bytes: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// RandomIdentifier returns a table/slot/publication-safe identifier derived
// from the test name. The result is always lowercase, consists only of
// [a-z0-9_] (valid as an unquoted Postgres identifier) and is at most 63 bytes.
//
// Uniqueness comes from the suffix "_<process id>_<counter>", where the
// process id is random per process and the counter is atomic, so results are
// unique within and across test processes. The test-name part is sanitized and
// truncated to whatever room the suffix leaves, so a long test name can never
// push the unique suffix past Postgres' 63-byte limit (where it would be
// silently cut off).
func RandomIdentifier(t *testing.T) string {
	const prefix = "conduit_"
	suffix := fmt.Sprintf("_%s_%d", randomIdentifierProc, randomIdentifierSeq.Add(1))

	name := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, strings.ToLower(t.Name()))

	if room := maxIdentifierLen - len(prefix) - len(suffix); len(name) > room {
		name = name[:room]
	}
	return prefix + name + suffix
}

func IsPgError(is *is.I, err error, wantCode string) {
	is.True(err != nil)
	var pgerr *pgconn.PgError
	ok := errors.As(err, &pgerr)
	is.True(ok) // expected err to be a *pgconn.PgError
	is.Equal(pgerr.Code, wantCode)
}

func Context(t *testing.T) context.Context {
	ctx := context.Background()
	if testing.Short() || !testing.Verbose() {
		return ctx
	}

	writer := zerolog.NewTestWriter(t)
	logger := zerolog.New(writer).Level(zerolog.InfoLevel)
	return logger.WithContext(ctx)
}
