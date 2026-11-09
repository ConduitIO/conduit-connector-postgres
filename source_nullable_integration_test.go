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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// Regression tests for #326: with logrepl.withAvroSchema (the default), a NULL
// in a nullable column of any type failed to encode ("avro: *avro.null is
// unsupported for Avro <T>") and stopped the source. They drive a real source
// with the default SDK middleware, so records are Avro-encoded exactly as in a
// pipeline, and decode them with the schema attached to the record.

// nullableTypes is every Postgres type the Avro extractor maps, with a value
// for its NOT NULL twin.
var nullableTypes = []struct{ name, sqlType, value string }{
	{"bool", "boolean", "true"},
	{"bytea", "bytea", `'\x07'`},
	{"float4", "float4", "1.5"},
	{"float8", "float8", "1.5"},
	{"int2", "int2", "7"},
	{"int4", "int4", "7"},
	{"int8", "int8", "7"},
	{"text", "text", "'x'"},
	{"varchar", "varchar(14)", "'x'"},
	{"json", "json", `'{"a":1}'`},
	{"jsonb", "jsonb", `'{"a":1}'`},
	{"tsvector", "tsvector", "'a b'"},
	{"timestamptz", "timestamptz", "'2022-03-14 15:16:17+00'"},
	{"timestamp", "timestamp", "'2022-03-14 15:16:17'"},
	{"date", "date", "'2022-03-14'"},
	{"uuid", "uuid", "'bd94ee0b-564f-4088-bf4e-8d5e626caf66'"},
	{"decimal", "numeric(10,2)", "1.50"},
	{"numeric", "numeric", "1.5"},
}

var nullableTableSeq atomic.Int64

// createNullableTable creates a table with, for every mapped type, a nullable
// column n_<type> and a NOT NULL column nn_<type>.
func createNullableTable(ctx context.Context, t *testing.T, conn test.Querier) string {
	t.Helper()
	is := is.New(t)
	table := fmt.Sprintf("nullable_%d_%d", time.Now().UnixMicro()%1000000, nullableTableSeq.Add(1))
	cols := []string{"id bigint PRIMARY KEY"}
	for _, ty := range nullableTypes {
		cols = append(cols, fmt.Sprintf("n_%s %s", ty.name, ty.sqlType), fmt.Sprintf("nn_%s %s NOT NULL", ty.name, ty.sqlType))
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(cols, ", ")))
	is.NoErr(err)
	dropReplication(ctx, t, table)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP TABLE "+table)
		is.NoErr(err)
	})
	return table
}

// insertNulls inserts a row with NULL in every nullable column and a value in
// every NOT NULL column.
func insertNulls(ctx context.Context, t *testing.T, conn test.Querier, table string, id int) {
	t.Helper()
	cols, vals := []string{"id"}, []string{fmt.Sprint(id)}
	for _, ty := range nullableTypes {
		cols = append(cols, "n_"+ty.name, "nn_"+ty.name)
		vals = append(vals, "NULL", ty.value)
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(vals, ", ")))
	is.New(t).NoErr(err)
}

// insertValues inserts a row with a value in every column.
func insertValues(ctx context.Context, t *testing.T, conn test.Querier, table string, id int) {
	t.Helper()
	cols, vals := []string{"id"}, []string{fmt.Sprint(id)}
	for _, ty := range nullableTypes {
		cols = append(cols, "n_"+ty.name, "nn_"+ty.name)
		vals = append(vals, ty.value, ty.value)
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(vals, ", ")))
	is.New(t).NoErr(err)
}

// readRecords reads exactly n records. ReadN can return more records than it
// was asked for, so every record of every batch is kept.
func readRecords(ctx context.Context, t *testing.T, s sdk.Source, n int) []opencdc.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var out []opencdc.Record
	for len(out) < n {
		recs, err := s.ReadN(ctx, n-len(out))
		switch {
		case errors.Is(err, sdk.ErrBackoffRetry):
			time.Sleep(50 * time.Millisecond)
			continue
		case err != nil:
			t.Fatalf("read: %v", err)
		}
		out = append(out, recs...)
	}
	if len(out) != n {
		t.Fatalf("read %d records, want %d", len(out), n)
	}
	return out
}

// assertValueRow checks a decoded row produced by insertValues: a value in a
// nullable column stays a value, logical types included.
func assertValueRow(t *testing.T, decoded map[string]any) {
	t.Helper()
	for _, ty := range nullableTypes {
		for _, col := range []string{"n_" + ty.name, "nn_" + ty.name} {
			if decoded[col] == nil {
				t.Errorf("%s: value decoded as NULL", col)
			}
		}
		// The nullable column and its NOT NULL twin hold the same value.
		if a, b := fmt.Sprint(decoded["n_"+ty.name]), fmt.Sprint(decoded["nn_"+ty.name]); a != b {
			t.Errorf("%s: nullable column decoded as %s, NOT NULL twin as %s", ty.name, a, b)
		}
	}
}

func openNullableSource(ctx context.Context, t *testing.T, table, snapshotMode string) sdk.Source {
	t.Helper()
	is := is.New(t)
	s := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url":                     test.RepmgrConnString,
		"tables":                  table,
		"snapshotMode":            snapshotMode,
		"cdcMode":                 "logrepl",
		"logrepl.slotName":        table,
		"logrepl.publicationName": table,
		"logrepl.withAvroSchema":  "true",
	}, s.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(s.Open(ctx, nil))
	t.Cleanup(func() { _ = s.Teardown(context.Background()) })
	return s
}

// decodeWith decodes Avro-encoded data with the schema the record names.
func decodeWith(ctx context.Context, t *testing.T, rec opencdc.Record, data opencdc.Data, key bool) (map[string]any, schema.Schema) {
	t.Helper()
	is := is.New(t)
	raw, ok := data.(opencdc.RawData)
	is.True(ok) // Avro-encoded by the SDK middleware
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	version, verr := rec.Metadata.GetPayloadSchemaVersion()
	if key {
		subject, err = rec.Metadata.GetKeySchemaSubject()
		version, verr = rec.Metadata.GetKeySchemaVersion()
	}
	is.NoErr(err)
	is.NoErr(verr)
	sch, err := schema.Get(ctx, subject, version)
	is.NoErr(err)
	var decoded map[string]any
	is.NoErr(sch.Unmarshal(raw.Bytes(), &decoded))
	return decoded, sch
}

// schemaFields returns the fields of a registered Avro record schema as
// name -> raw JSON of the field.
func schemaFields(t *testing.T, sch schema.Schema) map[string]map[string]json.RawMessage {
	t.Helper()
	var rs struct {
		Fields []map[string]json.RawMessage `json:"fields"`
	}
	is.New(t).NoErr(json.Unmarshal(sch.Bytes, &rs))
	out := make(map[string]map[string]json.RawMessage, len(rs.Fields))
	for _, f := range rs.Fields {
		var name string
		is.New(t).NoErr(json.Unmarshal(f["name"], &name))
		out[name] = f
	}
	return out
}

// assertNullRow checks a decoded row produced by insertNulls, and the schema
// it was encoded with.
func assertNullRow(t *testing.T, decoded map[string]any, sch schema.Schema) {
	t.Helper()
	fields := schemaFields(t, sch)
	for _, ty := range nullableTypes {
		n, nn := "n_"+ty.name, "nn_"+ty.name
		v, present := decoded[n]
		if !present || v != nil {
			t.Errorf("%s: got %#v (present %v), want NULL", n, v, present)
		}
		if decoded[nn] == nil {
			t.Errorf("%s: NOT NULL column decoded as NULL", nn)
		}
		// Debezium's shape for an optional column: null first, default null.
		if !strings.HasPrefix(string(fields[n]["type"]), `["null",`) || string(fields[n]["default"]) != "null" {
			t.Errorf("%s: field %s, want [\"null\", T] with default null", n, fields[n])
		}
		if strings.HasPrefix(string(fields[nn]["type"]), "[") {
			t.Errorf("%s: NOT NULL field %s is a union", nn, fields[nn])
		}
		if _, hasDefault := fields[nn]["default"]; hasDefault {
			t.Errorf("%s: NOT NULL field %s has a default", nn, fields[nn])
		}
	}
}

func assertKeyNotNull(ctx context.Context, t *testing.T, rec opencdc.Record) {
	t.Helper()
	is := is.New(t)
	key, sch := decodeWith(ctx, t, rec, rec.Key, true)
	is.True(key["id"] != nil)
	is.Equal(string(schemaFields(t, sch)["id"]["type"]), `"long"`) // primary key: NOT NULL, plain
}

func TestSource_Avro_NullInEveryType_Snapshot(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := createNullableTable(ctx, t, conn)
	insertNulls(ctx, t, conn, table, 1)
	insertValues(ctx, t, conn, table, 2)

	s := openNullableSource(ctx, t, table, "initial")
	// before #326: "avro: *avro.null is unsupported for Avro ..."
	recs := readRecords(ctx, t, s, 2)
	is.New(t).Equal(recs[0].Operation, opencdc.OperationSnapshot)

	decoded, sch := decodeWith(ctx, t, recs[0], recs[0].Payload.After, false)
	assertNullRow(t, decoded, sch)
	assertKeyNotNull(ctx, t, recs[0])

	decoded, _ = decodeWith(ctx, t, recs[1], recs[1].Payload.After, false)
	assertValueRow(t, decoded)
}

func TestSource_Avro_NullInEveryType_CDC(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := createNullableTable(ctx, t, conn)
	_, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
	is.New(t).NoErr(err)

	s := openNullableSource(ctx, t, table, "never")

	t.Run("insert", func(t *testing.T) {
		is := is.New(t)
		insertNulls(ctx, t, conn, table, 1)
		rec, err := readOne(ctx, t, s)
		is.NoErr(err) // before #326: "avro: *avro.null is unsupported for Avro ..."
		is.Equal(rec.Operation, opencdc.OperationCreate)
		decoded, sch := decodeWith(ctx, t, rec, rec.Payload.After, false)
		assertNullRow(t, decoded, sch)
		assertKeyNotNull(ctx, t, rec)
	})

	t.Run("insert values", func(t *testing.T) {
		is := is.New(t)
		insertValues(ctx, t, conn, table, 3)
		rec, err := readOne(ctx, t, s)
		is.NoErr(err)
		decoded, _ := decodeWith(ctx, t, rec, rec.Payload.After, false)
		assertValueRow(t, decoded)
		_, err = conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3", table))
		is.NoErr(err)
		rec, err = readOne(ctx, t, s)
		is.NoErr(err)
		is.Equal(rec.Operation, opencdc.OperationDelete)
		decoded, _ = decodeWith(ctx, t, rec, rec.Payload.Before, false)
		assertValueRow(t, decoded) // Before under REPLICA IDENTITY FULL
	})

	t.Run("delete under REPLICA IDENTITY FULL: Before carries the NULLs", func(t *testing.T) {
		is := is.New(t)
		_, err := conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 1", table))
		is.NoErr(err)
		rec, err := readOne(ctx, t, s)
		is.NoErr(err)
		is.Equal(rec.Operation, opencdc.OperationDelete)
		decoded, sch := decodeWith(ctx, t, rec, rec.Payload.Before, false)
		assertNullRow(t, decoded, sch)
	})

	t.Run("DROP NOT NULL while streaming: the column becomes nullable", func(t *testing.T) {
		is := is.New(t)
		// pgoutput sends a new Relation message after the ALTER; the handler
		// must read nullability again instead of keeping NOT NULL.
		_, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN nn_int4 DROP NOT NULL", table))
		is.NoErr(err)
		insertNulls(ctx, t, conn, table, 2)
		rec, err := readOne(ctx, t, s)
		is.NoErr(err)
		is.Equal(rec.Operation, opencdc.OperationCreate)

		_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET nn_int4 = NULL WHERE id = 2", table))
		is.NoErr(err)
		rec, err = readOne(ctx, t, s)
		is.NoErr(err)
		is.Equal(rec.Operation, opencdc.OperationUpdate)
		decoded, sch := decodeWith(ctx, t, rec, rec.Payload.After, false)
		v, present := decoded["nn_int4"]
		is.True(present)
		is.Equal(v, nil)
		f := schemaFields(t, sch)["nn_int4"]
		is.Equal(string(f["type"]), `["null","int"]`)
		is.Equal(string(f["default"]), "null")
	})
}

// TestSource_Avro_UnchangedToastRefused: under DEFAULT identity an UPDATE that
// leaves a TOASTed nullable column unchanged carries no value for it. The
// payload schema gives that column a default of null, so encoding the record
// would write NULL over the real value downstream. The source must stop with
// a coded error instead (invariant 6; #317, #388).
func TestSource_Avro_UnchangedToastRefused(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("nullable_toast_%d_%d", time.Now().UnixMicro()%1000000, nullableTableSeq.Add(1))
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, small int, big text)", table))
	is.NoErr(err)
	// Store big out of line so an UPDATE that does not touch it sends it as
	// unchanged-TOAST.
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big SET STORAGE EXTERNAL", table))
	is.NoErr(err)
	dropReplication(ctx, t, table)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

	s := openNullableSource(ctx, t, table, "never")
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, repeat('x', 100000))", table))
	is.NoErr(err)
	rec, err := readOne(ctx, t, s)
	is.NoErr(err)
	is.Equal(rec.Operation, opencdc.OperationCreate)

	_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET small = 2 WHERE id = 1", table))
	is.NoErr(err)
	_, err = readOne(ctx, t, s)
	is.True(err != nil) // without the refusal, big decodes as NULL
	is.True(strings.Contains(err.Error(), logrepl.ErrorCodeAvroUnchangedToast))
	is.True(strings.Contains(err.Error(), `"big"`))
}

// TestSource_Avro_UnchangedToastFullIdentity is the remedy the
// postgres.avro.unchanged_toast error and the README recommend: with REPLICA
// IDENTITY FULL, Postgres sends the old row, the handler fills the unchanged
// TOASTed column from it before the refusal check, and the update is emitted
// with the real value in After and Before, Avro-encoded. A refusal moved above
// that backfill would stop exactly the pipelines the fix tells operators to
// build.
func TestSource_Avro_UnchangedToastFullIdentity(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("nullable_toastfull_%d_%d", time.Now().UnixMicro()%1000000, nullableTableSeq.Add(1))
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, small int, big text, ts timestamp)", table))
	is.NoErr(err)
	// Out of line and uncompressed, so an UPDATE that does not touch big
	// sends it as unchanged-TOAST.
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big SET STORAGE EXTERNAL", table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
	is.NoErr(err)
	dropReplication(ctx, t, table)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

	s := openNullableSource(ctx, t, table, "never")
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, repeat('x', 100000), '2022-03-14 15:16:17')", table))
	is.NoErr(err)
	rec, err := readOne(ctx, t, s)
	is.NoErr(err)
	is.Equal(rec.Operation, opencdc.OperationCreate)
	is.NoErr(s.Ack(ctx, rec.Position))

	_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET small = 2 WHERE id = 1", table))
	is.NoErr(err)
	rec, err = readOne(ctx, t, s)
	is.NoErr(err) // the remedy works: no postgres.avro.unchanged_toast
	is.Equal(rec.Operation, opencdc.OperationUpdate)

	want := strings.Repeat("x", 100000)
	after, _ := decodeWith(ctx, t, rec, rec.Payload.After, false)
	is.Equal(after["small"], 2)
	is.Equal(after["big"], want) // the real value, not NULL
	before, _ := decodeWith(ctx, t, rec, rec.Payload.Before, false)
	is.Equal(before["small"], 1)
	is.Equal(before["big"], want)
	is.True(before["ts"] != nil)
	is.NoErr(s.Ack(ctx, rec.Position))
}

// TestSource_Avro_NullBeforeSetNotNull: the source lags behind a SET NOT
// NULL, so a change that stored NULL in the column is decoded after the
// catalog says NOT NULL. The column's field is widened to ["null", T], a new
// payload schema version is registered, and the NULL is delivered as null;
// the source does not stop and the operator does nothing.
func TestSource_Avro_NullBeforeSetNotNull(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("nullable_lag_%d_%d", time.Now().UnixMicro()%1000000, nullableTableSeq.Add(1))
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, c int)", table))
	is.NoErr(err)
	dropReplication(ctx, t, table)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

	// Create the slot, then stop: the changes below wait in the WAL.
	s := openNullableSource(ctx, t, table, "never")
	is.NoErr(s.Teardown(ctx))
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, NULL)", table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET c = 5 WHERE id = 1", table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN c SET NOT NULL", table))
	is.NoErr(err)

	s = openNullableSource(ctx, t, table, "never")
	recs := readRecords(ctx, t, s, 2) // before: postgres.avro.null_in_not_null_column stopped the source
	insert, update := recs[0], recs[1]
	is.Equal(insert.Operation, opencdc.OperationCreate)
	got, sch := decodeWith(ctx, t, insert, insert.Payload.After, false)
	v, present := got["c"]
	is.True(present)
	is.Equal(v, nil) // NULL, not 0
	is.Equal(string(schemaFields(t, sch)["c"]["type"]), `["null","int"]`)

	// The version before it is the schema built from the catalog, with c NOT
	// NULL: the widening registered a new version.
	is.True(sch.Version > 1)
	narrow, err := schema.Get(ctx, sch.Subject, sch.Version-1)
	is.NoErr(err)
	is.Equal(string(schemaFields(t, narrow)["c"]["type"]), `"int"`)
	is.NoErr(s.Ack(ctx, insert.Position))

	is.Equal(update.Operation, opencdc.OperationUpdate)
	got, usch := decodeWith(ctx, t, update, update.Payload.After, false)
	is.Equal(got["c"], 5)
	is.Equal(usch.Version, sch.Version) // stays widened, no flip back
	is.NoErr(s.Ack(ctx, update.Position))
}

// TestSource_Open_WarnsToastIdentity: with Avro schemas on, Open warns about a
// table whose replica identity cannot carry unchanged TOASTed values; it
// still opens. Without Avro there is nothing to warn about.
func TestSource_Open_WarnsToastIdentity(t *testing.T) {
	for _, withAvro := range []bool{true, false} {
		t.Run(fmt.Sprintf("avro=%v", withAvro), func(t *testing.T) {
			is := is.New(t)
			// The subscription goroutine logs too: a buffer safe for that.
			logs := &lockedBuffer{}
			ctx := zerolog.New(logs).WithContext(test.Context(t))
			conn := test.ConnectSimple(ctx, t, test.RegularConnString)
			table := fmt.Sprintf("nullable_toastwarn_%d_%d", time.Now().UnixMicro()%1000000, nullableTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, big text)", table))
			is.NoErr(err)
			dropReplication(ctx, t, table)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

			s := NewSource()
			is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
				"url":                     test.RepmgrConnString,
				"tables":                  table,
				"snapshotMode":            "never",
				"cdcMode":                 "logrepl",
				"logrepl.slotName":        table,
				"logrepl.publicationName": table,
				"logrepl.withAvroSchema":  fmt.Sprint(withAvro),
			}, s.Config(), Connector.NewSpecification().SourceParams))
			is.NoErr(s.Open(ctx, nil)) // a warning, never a refusal
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })

			warnings := strings.Count(logs.String(), `"code":"`+logrepl.WarnCodeAvroToastIdentity+`"`)
			if withAvro {
				is.Equal(warnings, 1) // once per table per Open
			} else {
				is.Equal(warnings, 0)
			}
		})
	}
}
