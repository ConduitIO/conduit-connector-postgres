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
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// jsonDoc is a document that a decoder which unmarshals JSON changes: the keys
// are not sorted, one number is beyond float64's exact range, one has a
// trailing zero, and there is non-ASCII text and an escape.
const jsonDoc = `{"z": 1, "a": [12345678901234567890, 1.50, null], "n": {"é": "日本é"}}`

func TestFormatColumn_JSONIsExactString(t *testing.T) {
	large := `{"k":"` + strings.Repeat("x", 1<<20) + `"}`
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
			for _, tc := range []struct {
				name string
				in   any
				want any
			}{
				{"bytes", []byte(jsonDoc), jsonDoc},
				{"string", jsonDoc, jsonDoc},
				{"large", []byte(large), large},
				{"json null literal", []byte("null"), "null"},
				{"empty object", []byte("{}"), "{}"},
				{"SQL NULL", nil, nil},
				{"nil bytes", []byte(nil), nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					is := is.New(t)
					got, err := FormatColumn(enc, oid, -1, tc.in)
					is.NoErr(err)
					is.Equal(got, tc.want)
				})
			}
		}
	}
}

func TestFormatColumn_JSONArrayElementsAreStrings(t *testing.T) {
	is := is.New(t)
	for _, oid := range []uint32{pgtype.JSONArrayOID, pgtype.JSONBArrayOID} {
		got, err := FormatFor(EncodingNative, oid, []any{[]byte(jsonDoc), nil, []byte("null")})
		is.NoErr(err)
		is.Equal(got, []any{jsonDoc, nil, "null"})

		got, err = FormatFor(EncodingNative, oid, nil)
		is.NoErr(err)
		is.Equal(got, nil)
	}
}

// A value that a JSON-unmarshalling codec produced has already lost its text;
// the formatter refuses it instead of re-marshalling an approximation.
func TestFormatColumn_JSONUnmarshalledValueIsAnError(t *testing.T) {
	is := is.New(t)
	_, err := FormatFor(EncodingAvro, pgtype.JSONBOID, map[string]any{"a": 1.0})
	is.True(err != nil)
	_, err = FormatFor(EncodingNative, pgtype.JSONArrayOID, []any{map[string]any{"a": 1.0}})
	is.True(err != nil)
}

func TestFormatColumn_JSONInvalidUTF8IsAnError(t *testing.T) {
	bad := []byte("{\"a\":\"caf\xe9\"}") // Latin-1 é, as SQL_ASCII would store it
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
			is := is.New(t)
			got, err := FormatColumn(enc, oid, -1, bad)
			is.Equal(got, nil)
			var je *JSONNotUTF8Error
			is.True(errors.As(err, &je))
			is.Equal(je.Offset, 9)
			// the value is never part of the message
			is.True(!strings.Contains(err.Error(), "caf"))

			located, ok := LocateError(err, "t", "c")
			is.True(ok)
			located = WithRow(located, "id=7")
			is.True(strings.HasPrefix(located.Error(), ErrorCodeJSONNotUTF8+`: json value in column "c" of table "t" (row id=7)`))

			_, ok = LocateErrorSnapshot(err, "t", "c")
			is.True(ok)
		}
	}

	is := is.New(t)
	_, err := FormatFor(EncodingNative, pgtype.JSONBArrayOID, []any{[]byte("{}"), bad})
	var je *JSONNotUTF8Error
	is.True(errors.As(err, &je))
}

// RegisterJSONText makes pgx hand over the exact text for json, jsonb and
// their arrays, in text and binary format. Without it pgx unmarshals the
// elements of json[] and jsonb[] (the array types keep the default element
// codec), which rounds 12345678901234567890 and drops the key order.
func TestRegisterJSONText(t *testing.T) {
	m := pgtype.NewMap()
	RegisterJSONText(m)

	decode := func(t *testing.T, oid uint32, format int16, src []byte) any {
		t.Helper()
		typ, ok := m.TypeForOID(oid)
		if !ok {
			t.Fatalf("oid %d not registered", oid)
		}
		v, err := typ.Codec.DecodeValue(m, oid, format, src)
		if err != nil {
			t.Fatal(err)
		}
		got, err := FormatFor(EncodingAvro, oid, v)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("json text", func(t *testing.T) {
		is := is.New(t)
		is.Equal(decode(t, pgtype.JSONOID, pgtype.TextFormatCode, []byte(jsonDoc)), jsonDoc)
	})
	t.Run("jsonb text", func(t *testing.T) {
		is := is.New(t)
		is.Equal(decode(t, pgtype.JSONBOID, pgtype.TextFormatCode, []byte(jsonDoc)), jsonDoc)
	})
	t.Run("jsonb binary", func(t *testing.T) {
		is := is.New(t)
		is.Equal(decode(t, pgtype.JSONBOID, pgtype.BinaryFormatCode, append([]byte{1}, jsonDoc...)), jsonDoc)
	})
	t.Run("NULL", func(t *testing.T) {
		is := is.New(t)
		is.Equal(decode(t, pgtype.JSONBOID, pgtype.TextFormatCode, nil), nil)
	})
	t.Run("jsonb[] text", func(t *testing.T) {
		is := is.New(t)
		// array_out quotes elements and escapes quotes and backslashes
		src := `{"{\"z\": 1, \"a\": [12345678901234567890]}",NULL,"null","\"s\\\\\""}`
		is.Equal(
			decode(t, pgtype.JSONBArrayOID, pgtype.TextFormatCode, []byte(src)),
			[]any{`{"z": 1, "a": [12345678901234567890]}`, nil, "null", `"s\\"`},
		)
	})
	t.Run("json[] text", func(t *testing.T) {
		is := is.New(t)
		src := `{"{\"z\": 1, \"a\": [12345678901234567890]}",NULL}`
		is.Equal(
			decode(t, pgtype.JSONArrayOID, pgtype.TextFormatCode, []byte(src)),
			[]any{`{"z": 1, "a": [12345678901234567890]}`, nil},
		)
	})
}

func TestJSONNoopUnmarshal(t *testing.T) {
	is := is.New(t)

	var dst any
	data := []byte(`{"foo":"bar"}`)

	is.NoErr(jsonNoopUnmarshal(data, &dst))
	is.Equal(data, dst.([]byte))
	data[0] = 'x'
	is.Equal(dst.([]byte)[0], byte('{')) // a copy, not the driver's buffer

	err := jsonNoopUnmarshal(data, dst)
	is.True(err != nil)
	is.Equal(err.Error(), "json: Unmarshal(non-pointer []uint8)")

	err = jsonNoopUnmarshal(data, nil)
	is.True(err != nil)
	is.Equal(err.Error(), "json: Unmarshal(nil)")
}
