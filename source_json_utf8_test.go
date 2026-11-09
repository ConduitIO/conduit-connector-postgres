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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// badJSON is a json document whose text is not valid UTF-8: the string value
// holds the bytes 0x80 0xff after `{"secretvalue":"`. Only a SQL_ASCII
// database accepts it.
const badJSON = `convert_from('\x7b2273656372657476616c7565223a2280ff227d'::bytea, 'SQL_ASCII')`

// badJSONOffset is the offset of the first invalid byte in badJSON's text.
const badJSONOffset = 16

// badJSONBytes is the text of badJSON as a json value stores it.
var badJSONBytes = []byte("{\"secretvalue\":\"\x80\xff\"}")

// mustB64Decode decodes a value the source emitted as base64.
func mustB64Decode(t *testing.T, v any) []byte {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("value is %T, not a base64 string", v)
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("not standard base64: %v", err)
	}
	return b
}

// dropReplicationIn drops the slot and the publication called name in the
// database of connString when the test ends.
func dropReplicationIn(ctx context.Context, t *testing.T, connString, name string) {
	t.Helper()
	conn := test.ConnectSimple(ctx, t, connString)
	t.Cleanup(func() {
		ctx := context.Background()
		for i := 0; i < 20; i++ { // the slot stays active until its connection is gone
			_, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, name)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP PUBLICATION IF EXISTS %q`, name))
	})
}

// openJSONSource opens a source on table with logical replication; extra
// sets more parameters (json.invalidUTF8, logrepl.withAvroSchema).
func openJSONSource(ctx context.Context, t *testing.T, connString, table, snapshotMode string, pos opencdc.Position, extra ...string) sdk.Source {
	t.Helper()
	is := is.New(t)
	s := NewSource()
	cfg := map[string]string{
		"url": connString, "tables": table, "snapshotMode": snapshotMode,
		"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
	}
	for i := 0; i+1 < len(extra); i += 2 {
		cfg[extra[i]] = extra[i+1]
	}
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, s.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(s.Open(ctx, pos))
	return s
}

// warnCtx returns a context whose logger writes warnings and above to the
// returned buffer, as JSON lines.
func warnCtx(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	t.Cleanup(func() { zerolog.SetGlobalLevel(prev) })
	var buf bytes.Buffer
	return zerolog.New(&buf).Level(zerolog.WarnLevel).WithContext(context.Background()), &buf
}

// encodingWarnings returns the log lines of buf that carry
// WarnCodeJSONServerEncoding.
func encodingWarnings(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	return codedLines(t, buf, WarnCodeJSONServerEncoding)
}

// codedLines returns the log lines of buf whose "code" is code.
func codedLines(t *testing.T, buf *bytes.Buffer, code string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if m["code"] == code {
			out = append(out, m)
		}
	}
	return out
}

// Open warns when the database stores json text unchecked (server_encoding
// SQL_ASCII) and a captured table has json, jsonb or array-of-json columns,
// naming every such column and what json.invalidUTF8 will do with such text.
// It does not warn for a UTF8 database, or for a SQL_ASCII one whose captured
// tables have no json columns.
func TestSource_JSONServerEncodingWarning(t *testing.T) {
	for _, tc := range []struct {
		name       string
		regular    string
		repmgr     string
		columns    string
		halt       bool
		wantColumn []string
	}{{
		name:    "sql_ascii with json columns",
		regular: test.SQLASCIIRegularConnString, repmgr: test.SQLASCIIRepmgrConnString,
		columns:    "j json, jb jsonb, ja json[], jba jsonb[], t text",
		wantColumn: []string{".j (json)", ".jb (jsonb)", ".ja (json[])", ".jba (jsonb[])"},
	}, {
		name:    "sql_ascii with json columns, halt",
		regular: test.SQLASCIIRegularConnString, repmgr: test.SQLASCIIRepmgrConnString,
		columns: "j json", halt: true,
		wantColumn: []string{".j (json)"},
	}, {
		name:    "sql_ascii without json columns",
		regular: test.SQLASCIIRegularConnString, repmgr: test.SQLASCIIRepmgrConnString,
		columns: "t text, b bytea",
	}, {
		name:    "utf8 with json columns",
		regular: test.RegularConnString, repmgr: test.RepmgrConnString,
		columns: "j json, jb jsonb, ja json[], jba jsonb[]",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx, buf := warnCtx(t)
			conn := test.ConnectSimple(ctx, t, tc.regular)
			table := fmt.Sprintf("jsonenc_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, %s)", table, tc.columns))
			is.NoErr(err)
			dropReplicationIn(ctx, t, tc.repmgr, table)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

			setting := "base64"
			if tc.halt {
				setting = "halt"
			}
			s := openJSONSource(ctx, t, tc.repmgr, table, "never", nil, "json.invalidUTF8", setting)
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })

			warnings := encodingWarnings(t, buf)
			if tc.wantColumn == nil {
				is.Equal(len(warnings), 0) // no warning expected
				return
			}
			is.Equal(len(warnings), 1)
			w := warnings[0]
			is.Equal(w["level"], "warn")
			is.Equal(w["server_encoding"], "SQL_ASCII")
			is.Equal(w["runbook"], types.JSONNotUTF8Runbook)
			cols, ok := w["columns"].([]any)
			is.True(ok)
			is.Equal(len(cols), len(tc.wantColumn))
			for i, want := range tc.wantColumn {
				is.Equal(cols[i], table+want)
			}
			is.Equal(w["json_invalid_utf8"], setting)
			msg, _ := w["message"].(string)
			is.True(strings.Contains(msg, "never replaces characters"))
			is.True(strings.Contains(msg, "json.invalidUTF8"))          // the option
			is.True(strings.Contains(msg, types.MetadataJSONRawBytes))  // the metadata key
			is.True(strings.Contains(msg, "move the database to UTF8")) // the remediation
			if tc.halt {
				is.True(strings.Contains(msg, types.ErrorCodeJSONNotUTF8))
				is.True(strings.Contains(msg, "stops on the same change again")) // the availability risk
			} else {
				is.True(strings.Contains(msg, "reads that base64 text as if it were the JSON value")) // consumer impact
				is.True(strings.Contains(msg, "Set json.invalidUTF8 to halt"))                        // fail closed
			}
		})
	}
}

// readUntilError reads and acks records until the source returns an error,
// and returns the records and the error. It fails the test if no error comes
// within the timeout.
func readUntilError(ctx context.Context, t *testing.T, s sdk.Source) ([]opencdc.Record, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []opencdc.Record
	for {
		recs, err := s.ReadN(ctx, 10)
		switch {
		case errors.Is(err, sdk.ErrBackoffRetry):
			time.Sleep(50 * time.Millisecond)
			continue
		case err != nil:
			if ctx.Err() != nil {
				t.Fatalf("no error from the source within the timeout (read %d records)", len(out))
			}
			return out, err
		}
		for _, rec := range recs {
			if err := s.Ack(ctx, rec.Position); err != nil {
				t.Fatalf("ack: %v", err)
			}
		}
		out = append(out, recs...)
	}
}

// With json.invalidUTF8 halt, json text that is not valid UTF-8 halts the
// source with postgres.source.json_not_utf8, naming the table, the column and
// the row (key columns and a key hash), never the value. Nothing is replaced
// or skipped (invariant 6): a restart from the last acked position reads the
// same row again and halts with the same error, and the rows after it are
// not delivered. For a snapshot row, fixing the value as the runbook says
// lets the restarted source go on; for a change in the WAL it does not, and
// restarting with json.invalidUTF8 base64 delivers it, base64-encoded, with
// nothing lost.
func TestSource_JSONNotUTF8HaltsAgainAfterRestart(t *testing.T) {
	for _, path := range []string{"snapshot", "cdc"} {
		t.Run(path, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
			table := fmt.Sprintf("jsonutf8_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, jb jsonb, j json)", table))
			is.NoErr(err)
			dropReplicationIn(ctx, t, test.SQLASCIIRepmgrConnString, table)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

			insert := func(id int, j string) {
				t.Helper()
				_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES ($1, '{"ok": 1}', %s::json)`, table, j), id)
				is.NoErr(err)
			}
			good := `'{"ok": 1}'`

			mode := "never"
			if path == "snapshot" {
				mode = "initial"
				insert(1, good)
				insert(2, badJSON)
				insert(3, good)
			}
			s := openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, nil, "json.invalidUTF8", "halt")
			var pos opencdc.Position
			if path == "cdc" {
				insert(1, good)
				recs := readAcked(ctx, t, s, 1)
				pos = recs[0].Position
				insert(2, badJSON)
				insert(3, good)
			}

			recs, err1 := readUntilError(ctx, t, s)
			for _, rec := range recs {
				// Only the row before the bad one can have been delivered.
				is.Equal(fmt.Sprint(jsonPayload(ctx, t, rec, true)["id"]), "1")
				pos = rec.Position
			}
			is.NoErr(s.Teardown(ctx))

			var je *types.JSONNotUTF8Error
			is.True(errors.As(err1, &je)) // expected the json UTF-8 error in the chain
			msg := err1.Error()
			is.True(strings.Contains(msg, types.ErrorCodeJSONNotUTF8))
			is.True(strings.Contains(msg, fmt.Sprintf(`json value in column "j" of table %q (row columns id, key hash `, table)))
			is.True(strings.Contains(msg, fmt.Sprintf("first invalid byte at offset %d", badJSONOffset)))
			is.True(strings.Contains(msg, types.JSONNotUTF8Runbook))
			is.True(strings.Contains(msg, "set json.invalidUTF8 to base64")) // the fix
			is.True(!strings.Contains(msg, "secretvalue"))                   // never the value
			is.True(!strings.Contains(msg, "\x80"))

			// Restart from the last acked position: the same row, the same
			// error, nothing delivered past it.
			s = openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, pos, "json.invalidUTF8", "halt")
			recs, err2 := readUntilError(ctx, t, s)
			is.Equal(len(recs), 0) // nothing new before the bad row
			is.Equal(err2.Error(), err1.Error())
			is.NoErr(s.Teardown(ctx))

			// The runbook: find the row by table and column, convert its
			// text from the encoding the writer used (here LATIN1) to UTF-8.
			ids := findNotUTF8Rows(ctx, t, table, "j")
			is.Equal(ids, []int64{2})
			_, err = conn.Exec(ctx, fmt.Sprintf(
				`UPDATE %[1]s SET j = convert_from(convert(convert_to(j::text, 'SQL_ASCII'), 'LATIN1', 'UTF8'), 'SQL_ASCII')::json WHERE id = 2`, table))
			is.NoErr(err)
			is.Equal(len(findNotUTF8Rows(ctx, t, table, "j")), 0)

			s = openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, pos, "json.invalidUTF8", "halt")
			if path == "cdc" {
				// The change is in the WAL with its original bytes: fixing
				// the row does not get the source past it.
				_, err3 := readUntilError(ctx, t, s)
				is.Equal(err3.Error(), err1.Error())
				is.NoErr(s.Teardown(ctx))

				// base64 delivers it, then row 3, then the fixing UPDATE.
				s = openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, pos, "json.invalidUTF8", "base64")
				t.Cleanup(func() { _ = s.Teardown(context.Background()) })
				recs := readAcked(ctx, t, s, 3)
				after := jsonPayload(ctx, t, recs[0], true)
				is.Equal(fmt.Sprint(after["id"]), "2")
				is.Equal(recs[0].Metadata[types.MetadataJSONRawBytes], `["j"]`)
				is.Equal(mustB64Decode(t, after["j"]), badJSONBytes)
				is.Equal(fmt.Sprint(jsonPayload(ctx, t, recs[1], true)["id"]), "3")
				_, ok := recs[1].Metadata[types.MetadataJSONRawBytes]
				is.True(!ok)
				is.Equal(recs[2].Operation, opencdc.OperationUpdate)
				return
			}
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })
			// The snapshot goes on after the last acked row; with none acked
			// (the bad row failed the first fetched batch) it starts over, so
			// row 1 comes again (at-least-once).
			// LATIN1 0x80 0xff are U+0080 U+00FF.
			want := map[string]string{"2": "{\"secretvalue\":\"\u0080\u00ff\"}", "3": `{"ok": 1}`}
			if pos == nil {
				want["1"] = `{"ok": 1}`
			}
			got := map[string]string{}
			for _, rec := range readAcked(ctx, t, s, len(want)) {
				after := jsonPayload(ctx, t, rec, true)
				got[fmt.Sprint(after["id"])] = after["j"].(string)
			}
			is.Equal(got, want)
		})
	}
}

// findNotUTF8Rows runs the runbook's query: the ids of the rows whose column
// text is not valid UTF-8.
func findNotUTF8Rows(ctx context.Context, t *testing.T, table, column string) []int64 {
	t.Helper()
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
	_, err := conn.Exec(ctx, jsonNotUTF8FunctionSQL)
	is.NoErr(err)
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE NOT pg_temp.is_utf8(convert_to(%s::text, 'SQL_ASCII')) ORDER BY id`, table, column))
	is.NoErr(err)
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		is.NoErr(rows.Scan(&id))
		ids = append(ids, id)
	}
	is.NoErr(rows.Err())
	return ids
}

// jsonNotUTF8FunctionSQL is the helper function of the runbook, verbatim.
const jsonNotUTF8FunctionSQL = `CREATE FUNCTION pg_temp.is_utf8(b bytea) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  PERFORM convert_from(b, 'UTF8');
  RETURN true;
EXCEPTION WHEN character_not_in_repertoire OR untranslatable_character THEN
  RETURN false;
END $$`

// rawText returns, per id, the bytes of column's text as Postgres stores
// them (convert_to in SQL_ASCII returns them unchanged), nil for NULL.
func rawText(ctx context.Context, t *testing.T, table, column string) map[string][]byte {
	t.Helper()
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT id, convert_to(%s::text, 'SQL_ASCII') FROM %s`, column, table))
	is.NoErr(err)
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var id int64
		var b []byte
		is.NoErr(rows.Scan(&id, &b))
		out[fmt.Sprint(id)] = b
	}
	is.NoErr(rows.Err())
	return out
}

// rawArrayElements returns, per id, the bytes of each element's text of a
// json[] or jsonb[] column (nil for a NULL element).
func rawArrayElements(ctx context.Context, t *testing.T, table, column string) map[string][][]byte {
	t.Helper()
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT id, array_agg(convert_to(e::text, 'SQL_ASCII') ORDER BY n) FROM %s, unnest(%s) WITH ORDINALITY AS u(e, n) GROUP BY id`, table, column))
	is.NoErr(err)
	defer rows.Close()
	out := map[string][][]byte{}
	for rows.Next() {
		var id int64
		var b [][]byte
		is.NoErr(rows.Scan(&id, &b))
		out[fmt.Sprint(id)] = b
	}
	is.NoErr(rows.Err())
	return out
}

// With json.invalidUTF8 base64, the default, json text that is not valid
// UTF-8 is delivered, never dropped or replaced (invariant 6, invariant 3):
// the field holds the standard base64 of the exact bytes Postgres stores, the
// record's metadata postgres.json.raw_bytes lists the column, and every
// valid value, in the same row or another, is the exact text with no
// metadata key. An array column with one invalid element has every non-NULL
// element base64. The source warns once per table and column, naming them and
// never the value. Snapshot and CDC, with and without Avro (arrays need
// Avro off). In CDC an UPDATE with REPLICA IDENTITY FULL that fixes the value
// names the base64 Before column in postgres.json.raw_bytes.before.
func TestSource_JSONNotUTF8Base64(t *testing.T) {
	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/avro=%v", path, avro), func(t *testing.T) {
				is := is.New(t)
				ctx, buf := warnCtx(t)
				conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
				table := fmt.Sprintf("jsonb64_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
				cols := "id bigint PRIMARY KEY, jb jsonb, j json"
				if !avro {
					cols += ", ja jsonb[]"
				}
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", table, cols))
				is.NoErr(err)
				_, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
				is.NoErr(err)
				dropReplicationIn(ctx, t, test.SQLASCIIRepmgrConnString, table)
				t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

				good := `'{"ok": 1, "u": "日本"}'`
				insert := func(id int, jb, j string) {
					t.Helper()
					q := fmt.Sprintf(`INSERT INTO %s VALUES ($1, %s::jsonb, %s::json)`, table, jb, j)
					if !avro {
						arr := "ARRAY['{\"a\": 1}'::jsonb, NULL]"
						if id == 2 {
							arr = "ARRAY['{\"a\": 1}'::jsonb, NULL, " + badJSON + "::jsonb]"
						}
						q = fmt.Sprintf(`INSERT INTO %s VALUES ($1, %s::jsonb, %s::json, %s)`, table, jb, j, arr)
					}
					_, err := conn.Exec(ctx, q, id)
					is.NoErr(err)
				}
				rows := func() {
					insert(1, good, good)
					insert(2, good, badJSON) // j invalid
					insert(3, good, good)
					insert(4, badJSON, badJSON) // both invalid
				}

				mode := "never"
				if path == "snapshot" {
					mode = "initial"
					rows()
				}
				s := openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, nil,
					"logrepl.withAvroSchema", fmt.Sprint(avro))
				t.Cleanup(func() { _ = s.Teardown(context.Background()) })
				n := 4
				if path == "cdc" {
					rows()
					// Fix row 2: the full old row has the invalid text.
					_, err := conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET j = %s::json WHERE id = 2`, table, good))
					is.NoErr(err)
					n = 5
				}

				recs := readAcked(ctx, t, s, n)
				wantJ := rawText(ctx, t, table, "j")
				wantJB := rawText(ctx, t, table, "jb")
				if path == "cdc" {
					wantJ["2"] = badJSONBytes // as inserted; the UPDATE is checked below
				}
				for _, rec := range recs[:4] {
					after := jsonPayload(ctx, t, rec, avro)
					id := fmt.Sprint(after["id"])
					var wantRaw []string
					for col, want := range map[string][]byte{"j": wantJ[id], "jb": wantJB[id]} {
						got := after[col]
						if !utf8.Valid(want) {
							wantRaw = append(wantRaw, col)
							is.Equal(mustB64Decode(t, got), want) // round trip: the stored bytes
							continue
						}
						is.Equal(got, string(want)) // valid text, unchanged
					}
					if !avro {
						elems := rawArrayElements(ctx, t, table, "ja")[id]
						ja := after["ja"].([]any)
						is.Equal(len(ja), len(elems))
						if id == "2" {
							wantRaw = append(wantRaw, "ja")
							for i, e := range elems {
								if e == nil {
									is.Equal(ja[i], nil)
									continue
								}
								is.Equal(mustB64Decode(t, ja[i]), e) // every non-NULL element
							}
						} else {
							is.Equal(ja, []any{`{"a": 1}`, nil})
						}
					}
					want, ok := types.JSONRawBytesMetadata(wantRaw)
					got, present := rec.Metadata[types.MetadataJSONRawBytes]
					is.Equal(present, ok)
					is.Equal(got, want)
					_, present = rec.Metadata[types.MetadataJSONRawBytesBefore]
					is.True(!present)
				}
				if path == "cdc" {
					upd := recs[4]
					is.Equal(upd.Operation, opencdc.OperationUpdate)
					wantBefore := `["j"]`
					if !avro {
						wantBefore = `["j","ja"]` // the full old row has the array too
					}
					is.Equal(upd.Metadata[types.MetadataJSONRawBytesBefore], wantBefore)
					_, present := upd.Metadata[types.MetadataJSONRawBytes]
					is.Equal(present, !avro) // ja of row 2 is still base64 in After without Avro
					after := jsonPayload(ctx, t, upd, avro)
					is.Equal(after["j"], `{"ok": 1, "u": "日本"}`)
				}

				// One warning per column, never the value.
				warns := codedLines(t, buf, types.ErrorCodeJSONNotUTF8)
				seen := map[string]int{}
				for _, w := range warns {
					is.Equal(w["table"], table)
					seen[w["column"].(string)]++
					is.True(!strings.Contains(fmt.Sprint(w), "secretvalue"))
				}
				wantSeen := map[string]int{"j": 1, "jb": 1}
				if !avro {
					wantSeen["ja"] = 1
				}
				is.Equal(seen, wantSeen)
			})
		}
	}
}
