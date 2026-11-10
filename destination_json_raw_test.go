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
	"encoding/base64"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// decodeJSONRawBytes replaces exactly the listed columns, in the key and the
// payload, with the bytes their base64 holds, keeps NULL array elements, and
// leaves the caller's maps and every unlisted column unchanged.
func TestDecodeJSONRawBytes(t *testing.T) {
	is := is.New(t)
	bad := "{\"s\":\"\x80\xff\"}"
	key := opencdc.StructuredData{"k": b64(bad), "id": int64(1)}
	payload := opencdc.StructuredData{
		"k":     b64(bad),
		"j":     b64(bad),
		"ja":    []any{b64(bad), nil, b64(`{"a": 1}`)},
		"plain": b64(`{"a": 1}`), // looks like base64 but is not listed
		"a,b":   b64(bad),
	}
	meta := opencdc.Metadata{types.MetadataJSONRawBytes: `["a,b","j","ja","k"]`}

	gotKey, gotPayload, err := decodeJSONRawBytes(meta, "t", key, payload)
	is.NoErr(err)
	is.Equal(gotKey, opencdc.StructuredData{"k": bad, "id": int64(1)})
	is.Equal(gotPayload, opencdc.StructuredData{
		"k": bad, "j": bad, "ja": []any{bad, nil, `{"a": 1}`}, "plain": b64(`{"a": 1}`), "a,b": bad,
	})
	// The record belongs to the caller (#343).
	is.Equal(key["k"], b64(bad))
	is.Equal(payload["j"], b64(bad))
	is.Equal(payload["ja"].([]any)[0], b64(bad))

	// Without the metadata, or with an empty list, nothing is decoded.
	for _, m := range []opencdc.Metadata{nil, {types.MetadataJSONRawBytes: `[]`}} {
		k, p, err := decodeJSONRawBytes(m, "t", key, payload)
		is.NoErr(err)
		is.Equal(k, key)
		is.Equal(p, payload)
	}

	// Before is never written, so its metadata is ignored.
	_, p, err := decodeJSONRawBytes(opencdc.Metadata{types.MetadataJSONRawBytesBefore: `["j"]`}, "t", key, payload)
	is.NoErr(err)
	is.Equal(p["j"], b64(bad))
}

// Every way the metadata or a listed value can be unusable fails the record
// with ErrorCodeDestinationJSONRawBytesInvalid, naming the table (and the
// column, where there is one) and never the value.
func TestDecodeJSONRawBytes_Errors(t *testing.T) {
	secret := "customer-SECRET-4711"
	for name, tc := range map[string]struct {
		meta    string
		payload opencdc.StructuredData
		want    string
	}{
		"metadata not JSON":        {meta: `j`, want: `has metadata postgres.json.raw_bytes that is not a JSON array`},
		"metadata empty":           {meta: ``, want: `that is not a JSON array`},
		"metadata null":            {meta: `null`, want: `that is not a JSON array`},
		"metadata a string":        {meta: `"j"`, want: `that is not a JSON array`},
		"metadata not strings":     {meta: `[1]`, want: `that is not a JSON array`},
		"metadata trailing data":   {meta: `["j"] x`, want: `that is not a JSON array`},
		"column missing":           {meta: `["gone"]`, want: `lists column "gone", which is not in the record's key or payload`},
		"value not base64":         {meta: `["j"]`, payload: opencdc.StructuredData{"j": secret + "!"}, want: `column "j" of table "t" is listed in metadata postgres.json.raw_bytes, but its value is not standard padded base64`},
		"value unpadded":           {meta: `["j"]`, payload: opencdc.StructuredData{"j": "/w"}, want: `its value is not standard padded base64`},
		"value null":               {meta: `["j"]`, payload: opencdc.StructuredData{"j": nil}, want: `its value is null`},
		"value a number":           {meta: `["j"]`, payload: opencdc.StructuredData{"j": int64(4711)}, want: `its value is a int64`},
		"value bytes":              {meta: `["j"]`, payload: opencdc.StructuredData{"j": []byte(secret)}, want: `its value is a []uint8`},
		"array element not base64": {meta: `["j"]`, payload: opencdc.StructuredData{"j": []any{b64("{}"), secret + "!"}}, want: `its value is not standard padded base64`},
		"array element not string": {meta: `["j"]`, payload: opencdc.StructuredData{"j": []any{b64("{}"), int64(4711)}}, want: `element 2 is a int64`},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			payload := tc.payload
			if payload == nil {
				payload = opencdc.StructuredData{"j": b64("{}")}
			}
			_, _, err := decodeJSONRawBytes(opencdc.Metadata{types.MetadataJSONRawBytes: tc.meta}, "t", nil, payload)
			is.True(err != nil)
			msg := err.Error()
			is.True(strings.HasPrefix(msg, ErrorCodeDestinationJSONRawBytesInvalid+": "))
			is.True(strings.Contains(msg, tc.want))
			is.True(strings.Contains(msg, `table "t"`))
			is.True(!strings.Contains(msg, secret)) // never the value
			is.True(!strings.Contains(msg, "4711"))
		})
	}
}

// openTestDestination opens a destination writing to table through url.
func openTestDestination(ctx context.Context, t *testing.T, url, table string) sdk.Destination {
	t.Helper()
	is := is.New(t)
	d := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"url": url, "table": table},
		d.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(d.Open(ctx))
	t.Cleanup(func() { _ = d.Teardown(context.Background()) })
	return d
}

// A Postgres-to-Postgres pipeline between SQL_ASCII databases keeps json
// text that is not valid UTF-8 byte for byte: the source emits it as base64
// and lists the column in postgres.json.raw_bytes, and the destination
// writes the decoded bytes, never the base64 text, into json, jsonb and
// jsonb[] columns and into text and text[] columns. Valid values and NULL
// elements are unchanged. In the query exec modes describe_exec (the
// default), cache_statement and exec (without the array column: pgx cannot
// encode a list for a parameter of unknown type, whatever its values), and
// for deletes by a jsonb key.
func TestDestination_JSONRawBytesRoundTripSQLASCII(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.SQLASCIIRegularConnString)
	src := fmt.Sprintf("jsonrawsrc_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, j json, jb jsonb, jba jsonb[], ok json)", src))
	is.NoErr(err)
	dropReplicationIn(ctx, t, test.SQLASCIIRepmgrConnString, src)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+src) })
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %[1]s VALUES
		(1, %[2]s::json, %[2]s::jsonb, ARRAY[%[2]s::jsonb, NULL, '{"a": 1}'], '{"ok": "日本"}'),
		(2, '{"z": 1, "a": 2}', '{"z": 1}', ARRAY['{"a": 1}'::jsonb, NULL], NULL)`, src, badJSON))
	is.NoErr(err)

	s := openJSONSource(ctx, t, test.SQLASCIIRepmgrConnString, src, "initial", nil, "logrepl.withAvroSchema", "false")
	recs := readAcked(ctx, t, s, 2)
	is.NoErr(s.Teardown(ctx))
	for _, rec := range recs {
		if fmt.Sprint(rec.Payload.After.(opencdc.StructuredData)["id"]) == "1" {
			is.Equal(rec.Metadata[types.MetadataJSONRawBytes], `["j","jb","jba"]`) // what the destination decodes
		} else {
			_, ok := rec.Metadata[types.MetadataJSONRawBytes]
			is.True(!ok)
		}
	}

	for _, mode := range []string{"", "cache_statement", "exec"} {
		url := test.SQLASCIIRegularConnString
		if mode != "" {
			url += "&default_query_exec_mode=" + mode
		}
		for _, target := range []struct{ name, cols string }{
			{"json", "id bigint PRIMARY KEY, j json, jb jsonb, jba jsonb[], ok json"},
			{"text", "id bigint PRIMARY KEY, j text, jb text, jba text[], ok text"},
		} {
			t.Run(fmt.Sprintf("mode=%s/%s", mode, target.name), func(t *testing.T) {
				is := is.New(t)
				dst := fmt.Sprintf("jsonrawdst_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", dst, target.cols))
				is.NoErr(err)
				t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+dst) })

				d := openTestDestination(ctx, t, url, dst)
				write := recs
				if mode == "exec" {
					write = make([]opencdc.Record, len(recs))
					for i, rec := range recs {
						after := maps.Clone(rec.Payload.After.(opencdc.StructuredData))
						delete(after, "jba")
						rec.Payload.After = after
						if _, ok := rec.Metadata[types.MetadataJSONRawBytes]; ok {
							rec.Metadata = maps.Clone(rec.Metadata)
							rec.Metadata[types.MetadataJSONRawBytes] = `["j","jb"]`
						}
						write[i] = rec
					}
				}
				// The upsert path too: the second write updates the rows.
				for range 2 {
					n, err := d.Write(ctx, write)
					is.NoErr(err)
					is.Equal(n, len(write))
				}

				for _, col := range []string{"j", "jb", "ok"} {
					is.Equal(rawText(ctx, t, dst, col), rawText(ctx, t, src, col)) // byte for byte
				}
				if mode != "exec" {
					is.Equal(rawArrayElements(ctx, t, dst, "jba"), rawArrayElements(ctx, t, src, "jba"))
				}
				is.Equal(rawText(ctx, t, dst, "j")["1"], badJSONBytes)
			})
		}

		t.Run(fmt.Sprintf("mode=%s/delete by jsonb key", mode), func(t *testing.T) {
			is := is.New(t)
			dst := fmt.Sprintf("jsonrawkey_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (k jsonb PRIMARY KEY, v bigint)", dst))
			is.NoErr(err)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+dst) })
			d := openTestDestination(ctx, t, url, dst)

			key := opencdc.StructuredData{"k": base64.StdEncoding.EncodeToString(badJSONBytes)}
			meta := opencdc.Metadata{types.MetadataJSONRawBytes: `["k"]`}
			_, err = d.Write(ctx, []opencdc.Record{{
				Operation: opencdc.OperationCreate, Key: key, Metadata: meta,
				Payload: opencdc.Change{After: opencdc.StructuredData{"k": key["k"], "v": int64(1)}},
			}})
			is.NoErr(err)
			var stored []byte
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT convert_to(k->>'secretvalue', 'SQL_ASCII') FROM %s", dst)).Scan(&stored))
			is.Equal(stored, []byte("\x80\xff"))

			_, err = d.Write(ctx, []opencdc.Record{{Operation: opencdc.OperationDelete, Key: key, Metadata: meta}})
			is.NoErr(err)
			var n int
			is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+dst).Scan(&n))
			is.Equal(n, 0) // matched by the bytes, not by the base64
		})
	}
}

// A UTF8 database cannot store bytes that are not valid UTF-8: the
// destination sends the decoded bytes and Postgres rejects them with its own
// error (SQLSTATE 22021), for a json and for a text column. It never stores
// the base64 text instead, and nothing of the batch is committed.
func TestDestination_JSONRawBytesUTF8Rejects(t *testing.T) {
	for _, colType := range []string{"json", "jsonb", "text"} {
		t.Run(colType, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			conn := test.ConnectSimple(ctx, t, test.RegularConnString)
			dst := fmt.Sprintf("jsonrawutf8_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, j %s)", dst, colType))
			is.NoErr(err)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+dst) })
			d := openTestDestination(ctx, t, test.RegularConnString, dst)

			_, err = d.Write(ctx, []opencdc.Record{{
				Operation: opencdc.OperationCreate,
				Key:       opencdc.StructuredData{"id": int64(1)},
				Metadata:  opencdc.Metadata{types.MetadataJSONRawBytes: `["j"]`},
				Payload:   opencdc.Change{After: opencdc.StructuredData{"j": base64.StdEncoding.EncodeToString(badJSONBytes)}},
			}})
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "SQLSTATE 22021"))
			is.True(!strings.Contains(err.Error(), "secretvalue"))
			var n int
			is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+dst).Scan(&n))
			is.Equal(n, 0)
		})
	}
}
