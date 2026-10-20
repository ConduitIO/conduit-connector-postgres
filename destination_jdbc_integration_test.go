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
	return openJDBCTestDestinationURL(t, is, test.RegularConnString)
}

// openJDBCTestDestinationURL is openJDBCTestDestination with a connection
// string of the test's choosing.
func openJDBCTestDestinationURL(t *testing.T, is *is.I, url string) sdk.Destination {
	t.Helper()
	ctx := test.Context(t)
	d := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx,
		map[string]string{"url": url},
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

// pgx.Identifier.Sanitize strips NUL bytes and Postgres truncates names over
// 63 bytes, so such a field or table name would silently write into another
// column or table. They are refused, and nothing of the batch is stored.
func TestDestination_Write_RejectsAliasingIdentifiers(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	long := strings.Repeat("v", 63)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v text, %s text)`, table, long))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	for name, rec := range map[string]opencdc.Record{
		"NUL in field": jdbcTestRecord(table, opencdc.OperationCreate, opencdc.StructuredData{"id": 1},
			opencdc.StructuredData{"id": 1, "v\x00": "x"}),
		"NUL in key column": jdbcTestRecord(table, opencdc.OperationDelete, opencdc.StructuredData{"id\x00": 1}, nil),
		"NUL in table": jdbcTestRecord(table+"\x00", opencdc.OperationCreate, opencdc.StructuredData{"id": 1},
			opencdc.StructuredData{"id": 1, "v": "x"}),
		"field over 63 bytes": jdbcTestRecord(table, opencdc.OperationCreate, opencdc.StructuredData{"id": 1},
			opencdc.StructuredData{"id": 1, long + "x": "x"}),
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			ok := jdbcTestRecord(table, opencdc.OperationCreate, opencdc.StructuredData{"id": 2},
				opencdc.StructuredData{"id": 2, "v": "y"})
			n, err := d.Write(ctx, []opencdc.Record{ok, rec})
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), ErrorCodeDestinationInvalidIdentifier))
			is.Equal(n, 0)
			var count int
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count))
			is.Equal(count, 0)
		})
	}

	// Exactly 63 bytes is a valid name and is written.
	n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.StructuredData{"id": 3}, opencdc.StructuredData{"id": 3, long: "z"})})
	is.NoErr(err)
	is.Equal(n, 1)
}

// A null key value became IS NULL and deleted every row with a NULL in that
// column. It is refused.
func TestDestination_Delete_NullKeyValueFails(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint, v text)`, table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (NULL, 'a'), (NULL, 'b'), (1, 'c')`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	n, err := d.Write(ctx, []opencdc.Record{
		jdbcTestRecord(table, opencdc.OperationDelete, opencdc.StructuredData{"id": nil}, nil),
	})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), ErrorCodeDestinationKeyColumnMissing))
	is.Equal(n, 0)
	var count int
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count))
	is.Equal(count, 3)
}

// JSON numbers that are not int64/uint64 literals stay exact in numeric, text
// and jsonb columns; whole values in exponent or decimal form still go into
// integer columns; a fraction into an integer column fails.
func TestDestination_Write_JSONNumbersAreExact(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint PRIMARY KEY, n numeric, big numeric, i bigint, i2 bigint, f float8, t text, j jsonb, na numeric[])`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.RawData(`{"id":1}`),
		opencdc.RawData(`{"id":1,"n":12345678901234567.89,"big":18000000000000000000.5,"i":1e3,"i2":7.0,`+
			`"f":1.5,"t":12345678901234567.89,"j":{"x":12345678901234567.89},"na":[1.25,12345678901234567.89]}`))})
	is.NoErr(err)
	is.Equal(n, 1)

	var nv, big, iv, i2, f, tv, jv, na string
	is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT n::text, big::text, i::text, i2::text, f::text, t, j::text, na::text FROM %s WHERE id = 1`, table)).
		Scan(&nv, &big, &iv, &i2, &f, &tv, &jv, &na))
	is.Equal(nv, "12345678901234567.89")
	is.Equal(big, "18000000000000000000.5")
	is.Equal(iv, "1000")
	is.Equal(i2, "7")
	is.Equal(f, "1.5")
	is.Equal(tv, "12345678901234567.89")
	is.Equal(jv, `{"x": 12345678901234567.89}`)
	is.Equal(na, "{1.25,12345678901234567.89}")

	_, err = d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationCreate,
		opencdc.RawData(`{"id":2}`), opencdc.RawData(`{"id":2,"i":1.5}`))})
	is.True(err != nil) // a fraction is never truncated into an integer column
}

// squirrel turns a slice in a delete's WHERE into IN (...): {"id":[1,2]}
// deleted two rows, a composite key with a list deleted every match, and an
// empty list became 1=0, a no-op that was acked. Key values must be scalars.
func TestDestination_Delete_NonScalarKeyValueFails(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (a bigint, b bigint, v text, PRIMARY KEY (a, b))`, table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 'x'), (1, 2, 'y'), (1, 3, 'z')`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	for name, key := range map[string]opencdc.Data{
		"raw list":          opencdc.RawData(`{"a":1,"b":[1,2]}`),
		"structured list":   opencdc.StructuredData{"a": 1, "b": []any{1, 2, 3}},
		"typed list":        opencdc.StructuredData{"a": 1, "b": []int64{1, 2}},
		"empty list":        opencdc.RawData(`{"a":1,"b":[]}`),
		"object":            opencdc.RawData(`{"a":1,"b":{"x":1}}`),
		"list in first col": opencdc.StructuredData{"a": []any{1}, "b": 1},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(table, opencdc.OperationDelete, key, nil)})
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), ErrorCodeDestinationKeyValueNotScalar))
			is.Equal(n, 0)
			var count int
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count))
			is.Equal(count, 3)
		})
	}
}

// A null key value never conflicts, so a keyed upsert on a nullable unique
// column appended a new row on every redelivery. Non-scalar key values are
// refused for upserts too.
func TestDestination_Upsert_KeyValueMustBeScalar(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (email text UNIQUE, v text)`, table))
	is.NoErr(err)

	d := openJDBCTestDestination(t, is)
	for name, tc := range map[string]struct {
		key  opencdc.StructuredData
		code string
	}{
		"null":   {opencdc.StructuredData{"email": nil}, ErrorCodeDestinationKeyColumnMissing},
		"list":   {opencdc.StructuredData{"email": []any{"a@x"}}, ErrorCodeDestinationKeyValueNotScalar},
		"object": {opencdc.StructuredData{"email": map[string]any{"a": 1}}, ErrorCodeDestinationKeyValueNotScalar},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			rec := jdbcTestRecord(table, opencdc.OperationCreate, tc.key, opencdc.StructuredData{"email": "a@x", "v": "a"})
			for i := 0; i < 2; i++ { // the second write is a redelivery
				n, err := d.Write(ctx, []opencdc.Record{rec})
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tc.code))
				is.Equal(n, 0)
			}
			var count int
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count))
			is.Equal(count, 0)
		})
	}

	// A bytea key is a scalar.
	btable := test.RandomIdentifier(t)
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bytea PRIMARY KEY, v text)`, btable))
	is.NoErr(err)
	n, err := d.Write(ctx, []opencdc.Record{jdbcTestRecord(btable, opencdc.OperationCreate,
		opencdc.StructuredData{"id": []byte{1, 2}}, opencdc.StructuredData{"v": "a"})})
	is.NoErr(err)
	is.Equal(n, 1)
}

// An integer column refuses a number with a huge exponent quickly instead of
// building it with big.Rat.
func TestJSONNumber_HugeExponentIsRefused(t *testing.T) {
	is := is.New(t)
	_, err := jsonNumber("1e1000000").Int64Value()
	is.True(err != nil)
	v, err := jsonNumber("1e3").Int64Value()
	is.NoErr(err)
	is.Equal(v.Int64, int64(1000))
}
