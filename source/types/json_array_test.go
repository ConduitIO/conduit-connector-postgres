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
	"errors"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// arrayOut writes elements the way Postgres's array_out does: every element
// quoted, '"' and '\' escaped, NULL bare.
func arrayOut(elems []any) []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		if e == nil {
			b.WriteString("NULL")
			continue
		}
		b.WriteByte('"')
		for _, c := range e.([]byte) {
			if c == '"' || c == '\\' {
				b.WriteByte('\\')
			}
			b.WriteByte(c)
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.Bytes()
}

// Regression test: json[]/jsonb[] in the text format (what pgoutput sends)
// kept bytes that are not valid UTF-8 only by accident of the format; pgx's
// text array parser turned them into U+FFFD, so CDC changed the element text
// silently and neither halt nor base64 could see it.
func TestRegisterJSONText_ArrayTextKeepsBytes(t *testing.T) {
	m := pgtype.NewMap()
	RegisterJSONText(m)
	for _, oid := range []uint32{pgtype.JSONArrayOID, pgtype.JSONBArrayOID} {
		is := is.New(t)
		typ, ok := m.TypeForOID(oid)
		is.True(ok)
		src := []byte("{\"{\\\"k\\\":\\\"\x80\xff\\\"}\",NULL,\"{\\\"a\\\": 1}\"}")
		got, err := typ.Codec.DecodeValue(m, oid, pgtype.TextFormatCode, src)
		is.NoErr(err)
		is.Equal(got, []any{[]byte("{\"k\":\"\x80\xff\"}"), nil, []byte(`{"a": 1}`)})

		// And so the formatter sees the invalid element.
		_, err = FormatColumn(EncodingNative, oid, -1, got)
		var je *JSONNotUTF8Error
		is.True(errors.As(err, &je))
	}
}

func TestParseJSONTextArray(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want []any
	}{
		{"empty", `{}`, []any{}},
		{"null and values", `{"{\"a\": 1}",NULL,"[]"}`, []any{[]byte(`{"a": 1}`), nil, []byte(`[]`)}},
		{"quoted NULL is text", `{"NULL"}`, []any{[]byte(`NULL`)}},
		{"bare element", `{1,true,null}`, []any{[]byte(`1`), []byte(`true`), []byte(`null`)}},
		{"escapes", `{"\"\\x\""}`, []any{[]byte(`"\x"`)}},
		{"two dimensions, flattened", `{{"1","2"},{"3","4"}}`, []any{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}},
		{"explicit bounds", `[0:1]={"1","2"}`, []any{[]byte("1"), []byte("2")}},
		{"invalid utf-8 kept", "{\"\x80\",\xff}", []any{[]byte("\x80"), []byte("\xff")}},
		{"empty string element", `{""}`, []any{[]byte{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, err := parseJSONTextArray([]byte(tc.in))
			is.NoErr(err)
			is.Equal(got, tc.want)
		})
	}
	for _, bad := range []string{``, `x`, `{"a"`, `{"a\`, `[1:2]`, `{"a"}x`, `{`} {
		_, err := parseJSONTextArray([]byte(bad))
		if !errors.Is(err, errJSONArrayText) {
			t.Fatalf("%q: got %v, want errJSONArrayText", bad, err)
		}
	}
}

// For any elements, parsing what array_out writes gives the same bytes back,
// and the result agrees with pgx's parser wherever the text is valid UTF-8.
func FuzzParseJSONTextArray(f *testing.F) {
	f.Add([]byte(`{"a": 1}`), []byte("\x80\xff"), true)
	f.Add([]byte(`"\`), []byte(`NULL`), false)
	f.Add([]byte{}, []byte("日本"), true)
	m := pgtype.NewMap()
	f.Fuzz(func(t *testing.T, a, b []byte, withNull bool) {
		elems := []any{append([]byte{}, a...), append([]byte{}, b...)}
		if withNull {
			elems = append(elems, nil)
		}
		src := arrayOut(elems)
		got, err := parseJSONTextArray(src)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(got) != len(elems) {
			t.Fatalf("got %d elements, want %d", len(got), len(elems))
		}
		for i := range elems {
			if elems[i] == nil {
				if got[i] != nil {
					t.Fatalf("element %d: want NULL", i)
				}
				continue
			}
			if !bytes.Equal(got[i].([]byte), elems[i].([]byte)) {
				t.Fatalf("element %d: bytes differ", i)
			}
		}
		// pgx's default text codec on valid input: same elements.
		def, ok := m.TypeForOID(pgtype.TextArrayOID)
		if !ok {
			t.Fatal("no text[]")
		}
		v, err := def.Codec.DecodeValue(m, pgtype.TextArrayOID, pgtype.TextFormatCode, src)
		if err != nil {
			return
		}
		pg := v.([]any)
		for i := range elems {
			if elems[i] == nil || !utf8.Valid(elems[i].([]byte)) {
				continue
			}
			if pg[i].(string) != string(got[i].([]byte)) {
				t.Fatalf("element %d differs from pgx", i)
			}
		}
	})
}
