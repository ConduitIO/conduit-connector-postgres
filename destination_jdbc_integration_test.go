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

package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func openJDBCTestDestination(t *testing.T, is *is.I) sdk.Destination {
	t.Helper()
	ctx := test.Context(t)
	d := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx,
		map[string]string{"url": test.RegularConnString},
		d.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(d.Open(ctx))
	t.Cleanup(func() { is.NoErr(d.Teardown(ctx)) })
	return d
}

func jdbcTestRecord(table string, op opencdc.Operation, key, after opencdc.Data) opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("%s/%v", table, key)),
		Operation: op,
		Metadata:  map[string]string{opencdc.MetadataCollection: table},
		Key:       key,
		Payload:   opencdc.Change{After: after},
	}
}

// Regression test for #343: Write removed the key columns from the payload of
// the records it was given.
func TestDestination_Write_DoesNotMutateRecords(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v text)`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	rec := jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": 1}, opencdc.StructuredData{"id": 1, "v": "a"})
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)
	is.Equal(n, 1)
	is.Equal(rec.Payload.After, opencdc.StructuredData{"id": 1, "v": "a"})
	is.Equal(rec.Key, opencdc.StructuredData{"id": 1})
}

// A table whose columns are all key columns (a link table) could not be
// written: the upsert had an empty SET list, a syntax error. A replay of the
// same record must also succeed.
func TestDestination_Write_AllColumnsAreKeyColumns(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b))`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	rec := jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"a": 1, "b": 2}, opencdc.StructuredData{"a": 1, "b": 2})
	for i := 0; i < 2; i++ { // the second write is a redelivery
		n, err := d.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
	}

	var count int
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE a = 1 AND b = 2`, table)).Scan(&count))
	is.Equal(count, 1)
}

// A create or update without fields fails instead of inserting only the key
// and acking a change that was not applied.
func TestDestination_Write_EmptyPayloadFails(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v text)`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	for _, op := range []opencdc.Operation{opencdc.OperationCreate, opencdc.OperationUpdate, opencdc.OperationSnapshot} {
		n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, op, opencdc.StructuredData{"id": 1}, nil)})
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), ErrorCodeDestinationPayloadEmpty))
		is.Equal(n, 0)
	}
}

// Regression test for #402. JSON numbers went through float64: int8 max was
// stored as int8 min on amd64, integers above 2^53 were rounded, and a delete
// for 9007199254740993 removed 9007199254740992.
func TestDestination_Write_JSONIntegersAreExact(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, n bigint, arr bigint[])`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	n, err := d.Write(ctx, []opencdc.Record{
		jdbcTestRecord(table, opencdc.OperationCreate, opencdc.RawData(`{"id":9007199254740992}`),
			opencdc.RawData(`{"id":9007199254740992,"n":1,"arr":[1]}`)),
		jdbcTestRecord(table, opencdc.OperationCreate, opencdc.RawData(`{"id":9007199254740993}`),
			opencdc.RawData(`{"id":9007199254740993,"n":9223372036854775807,"arr":[9223372036854775807,-9223372036854775808]}`)),
	})
	is.NoErr(err)
	is.Equal(n, 2)

	var got int64
	var arr []int64
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT n, arr FROM %s WHERE id = 9007199254740993`, table)).Scan(&got, &arr))
	is.Equal(got, int64(9223372036854775807))
	is.Equal(arr, []int64{9223372036854775807, -9223372036854775808})

	n, err = d.Write(ctx, []opencdc.Record{
		jdbcTestRecord(table, opencdc.OperationDelete, opencdc.RawData(`{"id":9007199254740993}`), nil),
	})
	is.NoErr(err)
	is.Equal(n, 1)

	var ids []int64
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT id FROM %s ORDER BY id`, table))
	is.NoErr(err)
	for rows.Next() {
		var id int64
		is.NoErr(rows.Scan(&id))
		ids = append(ids, id)
	}
	is.NoErr(rows.Err())
	is.Equal(ids, []int64{9007199254740992})
}

// Names with a double quote were escaped Go-style (\"), which broke the
// statement. They are quoted as Postgres identifiers now.
func TestDestination_Write_QuotesIdentifiers(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s ("i""d" bigint PRIMARY KEY, "v""x" text, "select" text)`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	n, err := d.Write(ctx, []opencdc.Record{
		jdbcTestRecord(table, opencdc.OperationCreate, opencdc.StructuredData{`i"d`: 1},
			opencdc.StructuredData{`i"d`: 1, `v"x`: "a", "select": "b"}),
		jdbcTestRecord(table, opencdc.OperationCreate, opencdc.StructuredData{`i"d`: 2},
			opencdc.StructuredData{`i"d`: 2, `v"x`: "c", "select": "d"}),
		jdbcTestRecord(table, opencdc.OperationDelete, opencdc.StructuredData{`i"d`: 2}, nil),
	})
	is.NoErr(err)
	is.Equal(n, 3)

	var vx, sel string
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT "v""x", "select" FROM %s WHERE "i""d" = 1`, table)).Scan(&vx, &sel))
	is.Equal(vx, "a")
	is.Equal(sel, "b")
	var count int
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count))
	is.Equal(count, 1)
}
