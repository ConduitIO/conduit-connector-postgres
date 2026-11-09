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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
)

// JSONInvalidUTF8 is what the source does with a json or jsonb value whose
// text is not valid UTF-8 (the json.invalidUTF8 source parameter).
type JSONInvalidUTF8 string

const (
	// JSONInvalidUTF8Base64 emits the value as the standard base64 (RFC 4648,
	// padded) of its bytes and lists its column in the record metadata
	// MetadataJSONRawBytes (or MetadataJSONRawBytesBefore). It is the default.
	JSONInvalidUTF8Base64 JSONInvalidUTF8 = "base64"
	// JSONInvalidUTF8Halt stops the source with ErrorCodeJSONNotUTF8.
	JSONInvalidUTF8Halt JSONInvalidUTF8 = "halt"
)

// MetadataJSONRawBytes names the columns whose value in the record's Key and
// Payload.After is the base64 of bytes that are not valid UTF-8, not JSON
// text. MetadataJSONRawBytesBefore does the same for Payload.Before. The
// value is a JSON array of column names, sorted by byte order, without
// duplicates, for example ["doc","tags"]: a column name can hold a comma or
// any other character, and a JSON array of strings cannot be read two ways.
// The key is absent when no column of that part of the record was encoded.
// For a json[] or jsonb[] column every non-NULL element is base64, see
// JSONPolicy.FormatColumn.
const (
	MetadataJSONRawBytes       = "postgres.json.raw_bytes"
	MetadataJSONRawBytesBefore = "postgres.json.raw_bytes.before"
)

// JSONRawBytesMetadata returns the value of MetadataJSONRawBytes (or
// MetadataJSONRawBytesBefore) for the given columns, and false when there are
// none. It sorts and deduplicates a copy of columns.
func JSONRawBytesMetadata(columns []string) (string, bool) {
	if len(columns) == 0 {
		return "", false
	}
	cols := slices.Clone(columns)
	slices.Sort(cols)
	cols = slices.Compact(cols)
	b, err := json.Marshal(cols)
	if err != nil {
		// A []string always marshals.
		panic(fmt.Sprintf("marshal column names: %v", err))
	}
	return string(b), true
}

// JSONPolicy applies a JSONInvalidUTF8 setting to the values of one source,
// snapshot and CDC alike, and remembers which (table, column) pairs it has
// already warned about. A nil *JSONPolicy is the default, base64, with a
// process-wide warning memory. It is safe for concurrent use: the snapshot's
// fetch workers and the CDC handler share one.
type JSONPolicy struct {
	invalidUTF8 JSONInvalidUTF8

	mu     sync.Mutex
	warned map[[2]string]struct{}
}

// defaultJSONPolicy stands in for a nil *JSONPolicy.
var defaultJSONPolicy = NewJSONPolicy(JSONInvalidUTF8Base64)

// NewJSONPolicy returns the policy for the given setting. Any value other than
// JSONInvalidUTF8Halt is base64: the source never halts by default.
func NewJSONPolicy(invalidUTF8 JSONInvalidUTF8) *JSONPolicy {
	if invalidUTF8 != JSONInvalidUTF8Halt {
		invalidUTF8 = JSONInvalidUTF8Base64
	}
	return &JSONPolicy{invalidUTF8: invalidUTF8, warned: map[[2]string]struct{}{}}
}

func (p *JSONPolicy) orDefault() *JSONPolicy {
	if p == nil {
		return defaultJSONPolicy
	}
	return p
}

// Halts reports whether the policy halts on json text that is not valid
// UTF-8.
func (p *JSONPolicy) Halts() bool {
	return p.orDefault().invalidUTF8 == JSONInvalidUTF8Halt
}

// FormatColumn is FormatColumn, except for a json, jsonb, json[] or jsonb[]
// value with text that is not valid UTF-8. Under JSONInvalidUTF8Halt that is
// the *JSONNotUTF8Error FormatColumn returns. Under JSONInvalidUTF8Base64 it
// is the value with its raw bytes in standard base64, and raw is true: the
// caller must list the column in MetadataJSONRawBytes (or
// MetadataJSONRawBytesBefore). For json[] and jsonb[] the decision is per
// column: when any element is not valid UTF-8, every non-NULL element is
// base64 and NULL elements stay NULL, so a consumer decodes a listed array
// column element by element without guessing which elements were encoded.
//
// Invariant 6: the bytes are never replaced (U+FFFD) or dropped, and an
// encoded value is never emitted without raw being true. Valid text is the
// exact string FormatColumn returns.
func (p *JSONPolicy) FormatColumn(enc Encoding, oid uint32, typeMod int32, v any) (out any, raw bool, err error) {
	out, err = FormatColumn(enc, oid, typeMod, v)
	if err == nil || p.Halts() {
		return out, false, err
	}
	var je *JSONNotUTF8Error
	if !errors.As(err, &je) {
		return out, false, err
	}
	b64, err := jsonBase64(oid, v)
	if err != nil {
		return nil, false, err
	}
	return b64, true, nil
}

// jsonBase64 returns v, a json, jsonb, json[] or jsonb[] value as pgx decoded
// it with RegisterJSONText, with every non-NULL text replaced by the standard
// base64 of its bytes.
func jsonBase64(oid uint32, v any) (any, error) {
	switch oid {
	case pgtype.JSONOID, pgtype.JSONBOID:
		return jsonTextBase64(v)
	case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
		return mapArray(v, "json[]", jsonTextBase64)
	default:
		return nil, fmt.Errorf("base64 of json text: unexpected type OID %d", oid)
	}
}

func jsonTextBase64(v any) (any, error) {
	var b []byte
	switch t := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		if t == nil {
			return nil, nil
		}
		b = t
	case string:
		b = []byte(t)
	case json.RawMessage:
		if t == nil {
			return nil, nil
		}
		b = t
	default:
		return nil, unexpectedType("json", v)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// WarnRawBytes logs, once per (table, column) for the life of p, a warning
// coded ErrorCodeJSONNotUTF8 that the column held json text that is not valid
// UTF-8 and was emitted as base64. It names the table, the column and the row
// by its key columns and a salted key hash (row, from internal.RowRef), never
// a value. path is "snapshot" or "cdc".
func (p *JSONPolicy) WarnRawBytes(logger *zerolog.Logger, path, table, row string, columns []string) {
	p = p.orDefault()
	for _, col := range columns {
		if !p.firstWarning(table, col) {
			continue
		}
		logger.Warn().
			Str("code", ErrorCodeJSONNotUTF8).
			Str("table", table).
			Str("column", col).
			Str("row", row).
			Str("path", path).
			Str("metadata", MetadataJSONRawBytes).
			Str("runbook", JSONNotUTF8Runbook).
			Msg("json text that is not valid UTF-8 was emitted as the base64 of its bytes " +
				"(json.invalidUTF8: base64); the record's metadata postgres.json.raw_bytes " +
				"(postgres.json.raw_bytes.before for Before) lists the column. " +
				"A consumer that does not check postgres.json.raw_bytes reads this base64 text as if it were the JSON value; " +
				"set json.invalidUTF8 to halt to fail closed if consumers cannot be updated. " +
				"Logged once per table and column; later values are encoded the same way without a warning")
	}
}

func (p *JSONPolicy) firstWarning(table, column string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := [2]string{table, column}
	if _, ok := p.warned[k]; ok {
		return false
	}
	p.warned[k] = struct{}{}
	return true
}
