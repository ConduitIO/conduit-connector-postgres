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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// json and jsonb are emitted as strings holding the exact text Postgres
// prints for the value (Debezium parity: io.debezium.data.Json is a string),
// on the snapshot and the CDC path, with and without an Avro schema. Before,
// they were bytes in the Avro schema and []byte values.
//
// The documents are ones a JSON round trip would change: unsorted and
// duplicate keys, whitespace, a number beyond float64, a trailing zero,
// non-ASCII text and escapes, deep nesting, the null literal, and a document
// large enough to be TOASTed.
var jsonDocs = []string{
	`{"z": 1,  "a": [12345678901234567890, 1.50, -0, 1e400], "a": "dup"}`,
	`{"emoji": "😀", "ja": "日本語", "esc": "é\n\t\"\\", "nul": null}`,
	`[` + strings.Repeat(`{"n":[`, 50) + `1` + strings.Repeat(`]}`, 50) + `]`,
	`null`,
	`"just a string"`,
	`12345678901234567890.000`,
	`{"big": "` + strings.Repeat("ab", 64<<10) + `"}`,
}

// debeziumJSONType is the Avro type of a json or jsonb field in the schema
// the source registers: what Debezium's json fields carry under Confluent's
// Avro converter.
const debeziumJSONType = `{"type":"string","connect.name":"io.debezium.data.Json","connect.version":1}`

func TestSource_JSONIsExactString(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/avro=%v", path, avro), func(t *testing.T) {
				is := is.New(t)
				table := fmt.Sprintf("jsonstr_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
				_, err := conn.Exec(ctx, fmt.Sprintf(
					"CREATE TABLE %s (id bigint PRIMARY KEY, n int, j json, jb jsonb)", table,
				))
				is.NoErr(err)
				// FULL, so that the UPDATE below backfills the unchanged
				// TOASTed documents from the old row image (a second decode).
				// EXTERNAL keeps the large document out of line uncompressed,
				// so it is really TOASTed.
				_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %[1]s REPLICA IDENTITY FULL, "+
					"ALTER COLUMN j SET STORAGE EXTERNAL, ALTER COLUMN jb SET STORAGE EXTERNAL", table))
				is.NoErr(err)
				dropReplication(ctx, t, table)
				t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

				insertAll := func() {
					for i, d := range jsonDocs {
						_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, 0, $2::text::json, $2::text::jsonb)", table), i+1, d)
						is.NoErr(err)
					}
					if !avro {
						// The Avro schema the source attaches has no nullable
						// fields, so the SDK cannot encode any NULL under it
						// (#326, every type, not only json). A NULL json is
						// covered below the SDK middleware, in source/types.
						_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, 0, NULL, NULL)", table), len(jsonDocs)+1)
						is.NoErr(err)
					}
				}
				rows := len(jsonDocs)
				if !avro {
					rows++
				}
				mode := "never"
				if path == "snapshot" {
					mode = "initial"
					insertAll()
				}
				s := NewSource()
				is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
					"url": test.RepmgrConnString, "tables": table, "snapshotMode": mode,
					"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
					"logrepl.withAvroSchema": fmt.Sprint(avro),
				}, s.Config(), Connector.NewSpecification().SourceParams))
				is.NoErr(s.Open(ctx, nil))
				t.Cleanup(func() { _ = s.Teardown(context.Background()) })
				if path == "cdc" {
					insertAll()
				}

				want := jsonWant(ctx, t, conn, table)
				check := func(rec opencdc.Record) {
					t.Helper()
					after := jsonPayload(ctx, t, rec, avro)
					id := fmt.Sprint(after["id"])
					w, ok := want[id]
					is.True(ok) // unexpected id
					is.Equal(after["j"], w[0])
					is.Equal(after["jb"], w[1])
				}
				for _, rec := range readAcked(ctx, t, s, rows) {
					check(rec)
				}

				if path == "cdc" {
					// The documents are unchanged, TOASTed ones are not in the
					// new tuple and come from the old row image.
					_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = 1", table))
					is.NoErr(err)
					for _, rec := range readAcked(ctx, t, s, rows) {
						is.Equal(rec.Operation, opencdc.OperationUpdate)
						check(rec)
					}
				}
			})
		}
	}
}

// jsonArrayShapes are json[]/jsonb[] literals by row id: empty, two
// dimensions with a NULL, and lower bound 0.
var jsonArrayShapes = map[int64]string{
	3: `{}`,
	4: `{{"{\"a\": 1}",NULL},{"[1, 2]","\"x\""}}`,
	5: `[0:1]={"{\"b\": 2}",3}`,
}

// json[] and jsonb[] have no Avro schema (no array has), so they are read
// without one. Their elements are strings, like the scalars. Before, pgx
// unmarshalled them into maps and float64s, which reordered keys and rounded
// 12345678901234567890.
func TestSource_JSONArrayElementsAreExactStrings(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	for _, path := range []string{"snapshot", "cdc"} {
		t.Run(path, func(t *testing.T) {
			is := is.New(t)
			table := fmt.Sprintf("jsonarr_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, ja json[], jba jsonb[])", table))
			is.NoErr(err)
			dropReplication(ctx, t, table)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

			insert := func() {
				_, err := conn.Exec(ctx, fmt.Sprintf(
					"INSERT INTO %s VALUES (1, ARRAY[$1::text::json, NULL, $2::text::json], ARRAY[$1::text::jsonb, NULL, $2::text::jsonb])", table,
				),
					jsonDocs[0], jsonDocs[1])
				is.NoErr(err)
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2, NULL, NULL)", table))
				is.NoErr(err)
				// Shapes the snapshot (binary, pgx) and CDC (text, the
				// connector's parser) decode with different code: empty,
				// two-dimensional (flattened in storage order) and a
				// non-default lower bound.
				for id, lit := range jsonArrayShapes {
					_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::text::json[], $2::text::jsonb[])", table), id, lit)
					is.NoErr(err)
				}
			}
			mode := "never"
			if path == "snapshot" {
				mode = "initial"
				insert()
			}
			s := NewSource()
			is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
				"url": test.RepmgrConnString, "tables": table, "snapshotMode": mode,
				"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
				"logrepl.withAvroSchema": "false",
			}, s.Config(), Connector.NewSpecification().SourceParams))
			is.NoErr(s.Open(ctx, nil))
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })
			if path == "cdc" {
				insert()
			}

			// What Postgres prints for each element.
			elems := func(col string, id int64) []any {
				var out []*string
				is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT array_agg(e::text ORDER BY o) FROM %s, unnest(%s) WITH ORDINALITY AS u(e, o) WHERE id = $1", table, col), id).Scan(&out))
				res := make([]any, len(out))
				for i, e := range out {
					if e != nil {
						res[i] = *e
					}
				}
				return res
			}
			wantJA, wantJBA := elems("ja", 1), elems("jba", 1)
			is.Equal(wantJA[0], jsonDocs[0]) // json keeps the text as written
			is.Equal(elems("ja", 3), []any{})
			is.Equal(len(elems("ja", 4)), 4)
			is.Equal(elems("ja", 5), []any{`{"b": 2}`, "3"})

			seen := 0
			for _, rec := range readAcked(ctx, t, s, 2+len(jsonArrayShapes)) {
				after := jsonPayload(ctx, t, rec, false)
				id, err := strconv.ParseInt(fmt.Sprint(after["id"]), 10, 64)
				is.NoErr(err)
				switch {
				case id == 1:
					is.Equal(after["ja"], wantJA)
					is.Equal(after["jba"], wantJBA)
				case id == 2:
					is.Equal(after["ja"], nil)
					is.Equal(after["jba"], nil)
				case jsonArrayShapes[id] != "":
					is.Equal(after["ja"], elems("ja", id))   // the exact text of each element
					is.Equal(after["jba"], elems("jba", id)) // in storage order
				default:
					t.Fatalf("unexpected id %v", after["id"])
				}
				seen++
			}
			is.Equal(seen, 2+len(jsonArrayShapes))
		})
	}
}

// A jsonb column can be the primary key (jsonb has a btree opclass). CDC
// emits the key as the same string as the payload; the snapshot does not
// support a jsonb key and refuses the table at Open instead of guessing.
func TestSource_JSONBKeyIsString(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	for _, path := range []string{"snapshot", "cdc"} {
		t.Run(path, func(t *testing.T) {
			is := is.New(t)
			table := fmt.Sprintf("jsonkey_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (k jsonb PRIMARY KEY, v int)", table))
			is.NoErr(err)
			dropReplication(ctx, t, table)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
			insert := func() {
				_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES ('{"b": 2, "a": "é"}', 1)`, table))
				is.NoErr(err)
			}
			mode := "never"
			if path == "snapshot" {
				mode = "initial"
				insert()
			}
			s := NewSource()
			is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
				"url": test.RepmgrConnString, "tables": table, "snapshotMode": mode,
				"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
			}, s.Config(), Connector.NewSpecification().SourceParams))
			err = s.Open(ctx, nil)
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })
			if path == "snapshot" {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), `key "k" of type "jsonb" is unsupported`))
				return
			}
			is.NoErr(err)
			insert()
			rec := readAcked(ctx, t, s, 1)[0]

			const want = `{"a": "é", "b": 2}` // jsonb's canonical text
			is.Equal(jsonPayload(ctx, t, rec, true)["k"], want)

			raw, ok := rec.Key.(opencdc.RawData)
			is.True(ok) // expected an Avro-encoded key
			subject, err := rec.Metadata.GetKeySchemaSubject()
			is.NoErr(err)
			version, err := rec.Metadata.GetKeySchemaVersion()
			is.NoErr(err)
			sch, err := schema.Get(ctx, subject, version)
			is.NoErr(err)
			is.True(strings.Contains(string(sch.Bytes), `{"name":"k","type":`+debeziumJSONType+`}`))
			var key map[string]any
			is.NoErr(sch.Unmarshal(raw.Bytes(), &key))
			is.Equal(key["k"], want)
		})
	}
}

// readAcked reads exactly n records and acks each. The snapshot can return
// more than the n asked of ReadN (a whole fetched batch), and it waits for
// every record it returned to be acked, so readOne does not fit here.
func readAcked(ctx context.Context, t *testing.T, s sdk.Source, n int) []opencdc.Record {
	t.Helper()
	is := is.New(t)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []opencdc.Record
	for len(out) < n {
		recs, err := s.ReadN(ctx, n-len(out))
		if errors.Is(err, sdk.ErrBackoffRetry) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		is.NoErr(err)
		for _, rec := range recs {
			is.NoErr(s.Ack(ctx, rec.Position))
		}
		out = append(out, recs...)
	}
	is.Equal(len(out), n)
	return out
}

// jsonWant returns, per id, the text Postgres prints for j and jb (nil for
// NULL).
func jsonWant(ctx context.Context, t *testing.T, conn *pgx.Conn, table string) map[string][2]any {
	t.Helper()
	is := is.New(t)
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, j::text, jb::text FROM %s", table))
	is.NoErr(err)
	defer rows.Close()
	want := map[string][2]any{}
	for rows.Next() {
		var id int64
		var j, jb *string
		is.NoErr(rows.Scan(&id, &j, &jb))
		var w [2]any
		if j != nil {
			w[0] = *j
		}
		if jb != nil {
			w[1] = *jb
		}
		want[fmt.Sprint(id)] = w
	}
	is.NoErr(rows.Err())
	// json keeps the document exactly as written.
	is.Equal(want["1"][0], jsonDocs[0])
	return want
}

// jsonPayload returns the after payload, decoded with the record's Avro
// schema when avro is set. With Avro it also checks that every json and jsonb
// field of the registered schema is an Avro string with Debezium's
// connect.name and connect.version.
func jsonPayload(ctx context.Context, t *testing.T, rec opencdc.Record, avro bool) map[string]any {
	t.Helper()
	is := is.New(t)
	if !avro {
		sd, ok := rec.Payload.After.(opencdc.StructuredData)
		is.True(ok) // expected structured payload
		return sd
	}
	raw, ok := rec.Payload.After.(opencdc.RawData)
	is.True(ok) // expected an Avro-encoded payload
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	sch, err := schema.Get(ctx, subject, version)
	is.NoErr(err)

	var parsed struct {
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	is.NoErr(json.Unmarshal(sch.Bytes, &parsed))
	for _, f := range parsed.Fields {
		switch f.Name {
		case "j", "jb", "k":
			// json and jsonb are Avro strings named as Debezium names them;
			// a nullable column is the union with null (#326).
			got := string(f.Type)
			is.True(got == debeziumJSONType || got == `["null",`+debeziumJSONType+`]`)
		}
	}

	var decoded map[string]any
	is.NoErr(sch.Unmarshal(raw.Bytes(), &decoded))
	return decoded
}
