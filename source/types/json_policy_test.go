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

package types

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// notUTF8 is json text with the bytes 0x80 0xff in a string: not valid UTF-8.
const notUTF8 = "{\"k\":\"\x80\xff\"}"

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// The encoding decision of json.invalidUTF8 base64: valid text is the exact
// string, text that is not valid UTF-8 is the padded standard base64 of its
// bytes with raw set, NULL stays NULL. Arrays are decided per column: one
// invalid element makes every non-NULL element base64.
func TestJSONPolicy_FormatColumn_Base64(t *testing.T) {
	for _, p := range []*JSONPolicy{nil, NewJSONPolicy(JSONInvalidUTF8Base64), NewJSONPolicy("")} {
		for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
			for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
				for _, tc := range []struct {
					name    string
					in      any
					want    any
					wantRaw bool
				}{
					{"valid bytes", []byte(jsonDoc), jsonDoc, false},
					{"valid string", jsonDoc, jsonDoc, false},
					{"valid empty", []byte{}, "", false},
					{"json null literal", []byte("null"), "null", false},
					{"SQL NULL", nil, nil, false},
					{"nil bytes", []byte(nil), nil, false},
					{"invalid bytes", []byte(notUTF8), b64(notUTF8), true},
					{"invalid string", notUTF8, b64(notUTF8), true},
					{"invalid raw message", json.RawMessage(notUTF8), b64(notUTF8), true},
					{"one byte, padded", []byte{0xff}, "/w==", true},
					{"truncated rune", []byte("\"\xe6\x97\""), b64("\"\xe6\x97\""), true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						is := is.New(t)
						got, raw, err := p.FormatColumn(enc, oid, -1, tc.in)
						is.NoErr(err)
						is.Equal(got, tc.want)
						is.Equal(raw, tc.wantRaw)
						if s, ok := got.(string); ok && raw {
							dec, err := base64.StdEncoding.DecodeString(s)
							is.NoErr(err)
							is.Equal(string(dec), func() string { // round trip: the original bytes
								switch v := tc.in.(type) {
								case []byte:
									return string(v)
								case json.RawMessage:
									return string(v)
								default:
									return v.(string)
								}
							}())
						}
					})
				}
			}
		}
	}
}

func TestJSONPolicy_FormatColumn_Base64Arrays(t *testing.T) {
	p := NewJSONPolicy(JSONInvalidUTF8Base64)
	for _, oid := range []uint32{pgtype.JSONArrayOID, pgtype.JSONBArrayOID} {
		for _, tc := range []struct {
			name    string
			in      any
			want    any
			wantRaw bool
		}{
			{"all valid", []any{[]byte(`{"a":1}`), nil, []byte("[]")}, []any{`{"a":1}`, nil, "[]"}, false},
			{"mixed", []any{[]byte(`{"a":1}`), nil, []byte(notUTF8)}, []any{b64(`{"a":1}`), nil, b64(notUTF8)}, true},
			{"all invalid", []any{[]byte(notUTF8), []byte{0x80}}, []any{b64(notUTF8), "gA=="}, true},
			{"empty array", []any{}, []any{}, false},
			{"SQL NULL", nil, nil, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				is := is.New(t)
				got, raw, err := p.FormatColumn(EncodingNative, oid, -1, tc.in)
				is.NoErr(err)
				is.Equal(got, tc.want)
				is.Equal(raw, tc.wantRaw)
			})
		}
	}
}

// json.invalidUTF8 halt keeps the coded error, and never a value in it.
func TestJSONPolicy_FormatColumn_Halt(t *testing.T) {
	p := NewJSONPolicy(JSONInvalidUTF8Halt)
	for _, tc := range []struct {
		oid uint32
		in  any
	}{
		{pgtype.JSONOID, []byte(notUTF8)},
		{pgtype.JSONBOID, notUTF8},
		{pgtype.JSONArrayOID, []any{[]byte(`{}`), []byte(notUTF8)}},
	} {
		is := is.New(t)
		got, raw, err := p.FormatColumn(EncodingAvro, tc.oid, -1, tc.in)
		var je *JSONNotUTF8Error
		is.True(errors.As(err, &je))
		is.Equal(got, nil)
		is.True(!raw)
		is.True(strings.Contains(err.Error(), ErrorCodeJSONNotUTF8+":"))
		is.True(strings.Contains(err.Error(), "json.invalidUTF8")) // the fix names the setting
		is.True(!strings.Contains(err.Error(), "\x80"))

		// Valid text is unchanged under halt too.
		v, raw, err := p.FormatColumn(EncodingAvro, pgtype.JSONOID, -1, []byte(jsonDoc))
		is.NoErr(err)
		is.Equal(v, jsonDoc)
		is.True(!raw)
	}
}

// Only halt halts: the zero value and anything unknown is base64.
func TestNewJSONPolicy_DefaultIsBase64(t *testing.T) {
	is := is.New(t)
	is.True(NewJSONPolicy(JSONInvalidUTF8Halt).Halts())
	is.True(!NewJSONPolicy(JSONInvalidUTF8Base64).Halts())
	is.True(!NewJSONPolicy("").Halts())
	is.True(!(*JSONPolicy)(nil).Halts())
}

// Errors of other types are not turned into base64.
func TestJSONPolicy_FormatColumn_OtherErrorsPass(t *testing.T) {
	is := is.New(t)
	_, raw, err := NewJSONPolicy(JSONInvalidUTF8Base64).FormatColumn(EncodingNative, pgtype.JSONOID, -1, map[string]any{"a": 1})
	is.True(err != nil) // an unmarshalled value is still an error
	is.True(!raw)
}

func TestJSONRawBytesMetadata(t *testing.T) {
	is := is.New(t)
	_, ok := JSONRawBytesMetadata(nil)
	is.True(!ok)
	in := []string{"b", "a,b", "a", "b", `q"uote`}
	v, ok := JSONRawBytesMetadata(in)
	is.True(ok)
	is.Equal(v, `["a","a,b","b","q\"uote"]`)               // sorted, unique, a comma stays inside one name
	is.Equal(in, []string{"b", "a,b", "a", "b", `q"uote`}) // the input is not reordered
	var back []string
	is.NoErr(json.Unmarshal([]byte(v), &back))
	is.Equal(back, []string{"a", "a,b", "b", `q"uote`})
}

// One warning per (table, column) for the life of a policy, naming the table,
// the column and the row reference, never a value.
func TestJSONPolicy_WarnRawBytesOnce(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	p := NewJSONPolicy(JSONInvalidUTF8Base64)

	p.WarnRawBytes(&logger, "snapshot", "t1", "columns id, key hash abc", []string{"j"})
	p.WarnRawBytes(&logger, "cdc", "t1", "columns id, key hash def", []string{"j", "jb"})
	p.WarnRawBytes(&logger, "cdc", "t1", "columns id, key hash def", []string{"jb", "j"})
	p.WarnRawBytes(&logger, "cdc", "t2", "", []string{"j"})

	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		is.NoErr(json.Unmarshal([]byte(l), &m))
		lines = append(lines, m)
	}
	is.Equal(len(lines), 3)
	for i, want := range [][2]string{{"t1", "j"}, {"t1", "jb"}, {"t2", "j"}} {
		is.Equal(lines[i]["level"], "warn")
		is.Equal(lines[i]["code"], ErrorCodeJSONNotUTF8)
		is.Equal(lines[i]["table"], want[0])
		is.Equal(lines[i]["column"], want[1])
		is.Equal(lines[i]["metadata"], MetadataJSONRawBytes)
	}
	is.Equal(lines[0]["row"], "columns id, key hash abc")
	msg, _ := lines[0]["message"].(string)
	is.True(strings.Contains(msg, "A consumer that does not check postgres.json.raw_bytes reads this base64 text as if it were the JSON value"))
	is.True(strings.Contains(msg, "set json.invalidUTF8 to halt")) // the fail-closed option
	is.Equal(lines[0]["path"], "snapshot")

	// Another policy (another source) warns again.
	buf.Reset()
	NewJSONPolicy(JSONInvalidUTF8Base64).WarnRawBytes(&logger, "cdc", "t1", "", []string{"j"})
	is.Equal(strings.Count(buf.String(), "\n"), 1)
}
