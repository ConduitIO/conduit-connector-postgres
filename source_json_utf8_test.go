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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

func openJSONSource(ctx context.Context, t *testing.T, connString, table, snapshotMode string, pos opencdc.Position) sdk.Source {
	t.Helper()
	is := is.New(t)
	s := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url": connString, "tables": table, "snapshotMode": snapshotMode,
		"cdcMode": "logrepl", "logrepl.slotName": table, "logrepl.publicationName": table,
	}, s.Config(), Connector.NewSpecification().SourceParams))
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
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if m["code"] == WarnCodeJSONServerEncoding {
			out = append(out, m)
		}
	}
	return out
}

// Open warns when the database stores json text unchecked (server_encoding
// SQL_ASCII) and a captured table has json, jsonb or array-of-json columns,
// naming every such column. It does not warn for a UTF8 database, or for a
// SQL_ASCII one whose captured tables have no json columns.
func TestSource_JSONServerEncodingWarning(t *testing.T) {
	for _, tc := range []struct {
		name       string
		regular    string
		repmgr     string
		columns    string
		wantColumn []string
	}{{
		name:    "sql_ascii with json columns",
		regular: test.SQLASCIIRegularConnString, repmgr: test.SQLASCIIRepmgrConnString,
		columns:    "j json, jb jsonb, ja json[], jba jsonb[], t text",
		wantColumn: []string{".j (json)", ".jb (jsonb)", ".ja (json[])", ".jba (jsonb[])"},
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

			s := openJSONSource(ctx, t, tc.repmgr, table, "never", nil)
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
			msg, _ := w["message"].(string)
			is.True(strings.Contains(msg, types.ErrorCodeJSONNotUTF8))
			is.True(strings.Contains(msg, "halts on the same change again")) // the availability risk
			is.True(strings.Contains(msg, "move the database to UTF8"))      // the remediation
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

// json text that is not valid UTF-8 halts the source with
// postgres.source.json_not_utf8, naming the table, the column and the row
// (key columns and a key hash), never the value. Nothing is replaced or
// skipped (invariant 6): a restart from the last acked position reads the
// same row again and halts with the same error, and the rows after it are
// not delivered. For a snapshot row, fixing the value as the runbook says
// lets the restarted source go on; for a change in the WAL it does not.
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
			s := openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, nil)
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
			is.True(!strings.Contains(msg, "secretvalue")) // never the value
			is.True(!strings.Contains(msg, "\x80"))

			// Restart from the last acked position: the same row, the same
			// error, nothing delivered past it.
			s = openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, pos)
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

			s = openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, table, mode, pos)
			t.Cleanup(func() { _ = s.Teardown(context.Background()) })
			if path == "cdc" {
				// The change is in the WAL with its original bytes: fixing
				// the row does not get the source past it.
				_, err3 := readUntilError(ctx, t, s)
				is.Equal(err3.Error(), err1.Error())
				return
			}
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
