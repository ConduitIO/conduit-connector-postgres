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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
)

// ErrorCodeJSONNotUTF8 is the code of a json or jsonb value whose text is not
// valid UTF-8. With json.invalidUTF8 set to halt it prefixes the error that
// stops the source (JSONNotUTF8Error). With base64, the default, it is the
// "code" of the warning JSONPolicy.WarnRawBytes logs for the column.
const ErrorCodeJSONNotUTF8 = "postgres.source.json_not_utf8"

// JSONNotUTF8Runbook is the operator runbook for ErrorCodeJSONNotUTF8: what
// the warning or the error means, how to find the rows, how consumers decode
// the base64 values and how to fix the data. The error, the warning and the
// open-time warning link it.
const JSONNotUTF8Runbook = "https://github.com/ConduitIO/conduit-connector-postgres#runbook-postgressourcejson_not_utf8"

// JSONNotUTF8Error reports a json or jsonb value whose text is not valid
// UTF-8 when json.invalidUTF8 is halt. Postgres validates the encoding of
// json text in every server encoding except SQL_ASCII, which stores bytes
// unchecked; that is the one way to get here. Offset is the byte offset of
// the first invalid byte. The value itself is never part of the message.
// Table and Column are set by the caller that knows them (LocateError), Row
// (key columns and a salted key hash, never key values) by WithRow.
type JSONNotUTF8Error struct {
	Table  string
	Column string
	Row    string
	Offset int
}

func (e *JSONNotUTF8Error) Error() string {
	where := ""
	switch {
	case e.Table != "" && e.Column != "":
		where = fmt.Sprintf(" in column %q of table %q", e.Column, e.Table)
	case e.Column != "":
		where = fmt.Sprintf(" in column %q", e.Column)
	}
	if e.Row != "" {
		where += fmt.Sprintf(" (row %s)", e.Row)
	}
	return fmt.Sprintf("%s: json value%s is not valid UTF-8 (first invalid byte at offset %d), "+
		"and json.invalidUTF8 is halt, so the source stops instead of emitting it. "+
		"A restart resumes at the same position and stops here again. "+
		"Fix: set json.invalidUTF8 to base64 (the default) and restart: the value is delivered as the base64 of its bytes "+
		"and the record's metadata postgres.json.raw_bytes lists the column, so consumers can decode it; "+
		"or correct the value (enough for a snapshot row, not for a change already in the WAL). "+
		"The database server_encoding is most likely SQL_ASCII, which stores bytes unchecked. Runbook: %s",
		ErrorCodeJSONNotUTF8, where, e.Offset, JSONNotUTF8Runbook)
}

// LocateJSON returns a copy of err with its table and column set, and true,
// when err is or wraps a *JSONNotUTF8Error. For any other error it returns err
// and false.
func LocateJSON(err error, table, column string) (error, bool) { //nolint:revive // the error is the result, ok says whether it was located
	var je *JSONNotUTF8Error
	if !errors.As(err, &je) {
		return err, false
	}
	located := *je
	located.Table, located.Column = table, column
	return &located, true
}

// RegisterJSONText makes m decode json, jsonb, json[] and jsonb[] values to
// the exact bytes Postgres sent, without unmarshalling them. pgx's default
// codecs unmarshal JSON into Go maps and float64s, which reorders object keys
// and rounds numbers beyond 2^53: a changed value (invariant 6). The array
// types are registered again because a pgx array type holds the element type
// it was created with, so overriding json alone leaves json[] on the default
// codec; their text format is parsed by jsonArrayCodec, which keeps bytes
// that are not valid UTF-8.
func RegisterJSONText(m *pgtype.Map) {
	jsonType := &pgtype.Type{
		Name:  "json",
		OID:   pgtype.JSONOID,
		Codec: &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: jsonNoopUnmarshal},
	}
	jsonbType := &pgtype.Type{
		Name:  "jsonb",
		OID:   pgtype.JSONBOID,
		Codec: &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: jsonNoopUnmarshal},
	}
	m.RegisterType(jsonType)
	m.RegisterType(jsonbType)
	// The array codecs decode the text format byte for byte (jsonArrayCodec):
	// pgx's own text array parser replaces bytes that are not valid UTF-8.
	m.RegisterType(&pgtype.Type{Name: "_json", OID: pgtype.JSONArrayOID, Codec: jsonArrayCodec{&pgtype.ArrayCodec{ElementType: jsonType}}})
	m.RegisterType(&pgtype.Type{Name: "_jsonb", OID: pgtype.JSONBArrayOID, Codec: jsonArrayCodec{&pgtype.ArrayCodec{ElementType: jsonbType}}})
}

// jsonNoopUnmarshal copies src into dst, which must be a *any. It is the
// Unmarshal of the codecs RegisterJSONText registers.
func jsonNoopUnmarshal(src []byte, dst any) error {
	dstptr, ok := dst.(*any)
	if dst == nil || !ok {
		return &json.InvalidUnmarshalError{Type: reflect.TypeOf(dst)}
	}
	v := make([]byte, len(src))
	copy(v, src)
	*dstptr = v
	return nil
}

// formatJSON returns the text of a json or jsonb value as a string, exactly
// as Postgres sent it (jsonb in its canonical form, json as stored). NULL is
// the untyped nil; the JSON literal null is the string "null". Text that is
// not valid UTF-8 is a *JSONNotUTF8Error, never a string with replaced bytes;
// JSONPolicy.FormatColumn decides what the source does with it. Any other Go
// type means the value was decoded by a codec that unmarshalled it
// (RegisterJSONText was not applied), which has already changed it, so that
// is an error too rather than a re-marshalled approximation.
func formatJSON(v any) (any, error) {
	var s string
	switch t := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		if t == nil {
			return nil, nil
		}
		s = string(t)
	case string:
		s = t
	case json.RawMessage:
		if t == nil {
			return nil, nil
		}
		s = string(t)
	default:
		return nil, unexpectedType("json", v)
	}
	if !utf8.ValidString(s) {
		return nil, &JSONNotUTF8Error{Offset: invalidUTF8Offset(s)}
	}
	return s, nil
}

func formatJSONArray(v any) (any, error) {
	return mapArray(v, "json[]", formatJSON)
}

func invalidUTF8Offset(s string) int {
	for i, r := range s {
		if r == utf8.RuneError {
			if _, size := utf8.DecodeRuneInString(s[i:]); size == 1 {
				return i
			}
		}
	}
	return len(s)
}
