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
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// jsonArrayCodec is pgx's array codec for json[] and jsonb[] with a text
// decoder that keeps every byte. pgx's text array parser reads the input rune
// by rune and writes it back with WriteRune, so a byte that is not valid
// UTF-8 becomes U+FFFD before any formatter sees it: in a SQL_ASCII database
// the json text of an element would be silently changed, with no way to
// notice (invariant 6). Logical replication (pgoutput) sends arrays in the
// text format, so CDC would hit it on every such element. The binary format,
// which the snapshot's queries use for arrays, is parsed by pgx byte for
// byte and is left to it.
type jsonArrayCodec struct {
	*pgtype.ArrayCodec
}

// DecodeValue returns the elements of a json[] or jsonb[] value as []any of
// []byte (the element's exact text) and nil (NULL), flattened in storage
// order like pgx's ArrayCodec.DecodeValue. A NULL array is nil.
func (c jsonArrayCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	if format == pgtype.TextFormatCode {
		return parseJSONTextArray(src)
	}
	return c.ArrayCodec.DecodeValue(m, oid, format, src)
}

var errJSONArrayText = errors.New("invalid json[] text")

// parseJSONTextArray parses the text form Postgres writes for a json[] or
// jsonb[] (array_out): an optional dimension decoration such as "[0:1]=",
// then nested braces with comma-separated elements. An element is NULL
// (unquoted), or its text, double-quoted with backslash escapes when it holds
// a special character (json text nearly always does) or bare otherwise. It
// works on bytes, never on runes, so the bytes of an element are kept exactly,
// valid UTF-8 or not. Dimensions are not kept: the result is flat, as pgx's.
// The error never quotes the input.
func parseJSONTextArray(src []byte) ([]any, error) {
	i, err := skipArrayDimensions(src)
	if err != nil {
		return nil, err
	}
	if i >= len(src) || src[i] != '{' {
		return nil, fmt.Errorf("%w: expected '{' at offset %d", errJSONArrayText, i)
	}

	out := []any{}
	depth := 0
	for i < len(src) {
		switch c := src[i]; {
		case c == '{':
			depth++
			i++
		case c == '}':
			depth--
			i++
			if depth == 0 {
				if j := skipArraySpace(src, i); j != len(src) {
					return nil, fmt.Errorf("%w: trailing data at offset %d", errJSONArrayText, j)
				}
				return out, nil
			}
		case c == ',' || isArraySpace(c):
			i++
		case c == '"':
			elem, next, err := quotedArrayElement(src, i+1)
			if err != nil {
				return nil, err
			}
			out = append(out, elem)
			i = next
		default:
			elem, next := bareArrayElement(src, i)
			out = append(out, elem)
			i = next
		}
	}
	return nil, fmt.Errorf("%w: missing '}'", errJSONArrayText)
}

// skipArrayDimensions returns the offset after leading space and an explicit
// bounds decoration ("[lower:upper]...="), if there is one.
func skipArrayDimensions(src []byte) (int, error) {
	i := skipArraySpace(src, 0)
	if i >= len(src) || src[i] != '[' {
		return i, nil
	}
	for i < len(src) && src[i] != '=' {
		i++
	}
	if i == len(src) {
		return 0, fmt.Errorf("%w: dimension decoration without '='", errJSONArrayText)
	}
	return skipArraySpace(src, i+1), nil
}

// bareArrayElement reads an unquoted element starting at src[i]: nil for
// NULL, a copy of its bytes otherwise, and the offset after it.
func bareArrayElement(src []byte, i int) (any, int) {
	start := i
	for i < len(src) && src[i] != ',' && src[i] != '}' && !isArraySpace(src[i]) {
		i++
	}
	if string(src[start:i]) == "NULL" {
		return nil, i
	}
	return append([]byte{}, src[start:i]...), i
}

// quotedArrayElement reads a double-quoted element whose text starts at
// src[i], and returns its bytes with the escapes removed and the offset after
// the closing quote.
func quotedArrayElement(src []byte, i int) ([]byte, int, error) {
	elem := []byte{}
	for i < len(src) {
		switch src[i] {
		case '\\':
			if i+1 == len(src) {
				return nil, 0, fmt.Errorf("%w: escape at the end", errJSONArrayText)
			}
			elem = append(elem, src[i+1])
			i += 2
		case '"':
			return elem, i + 1, nil
		default:
			elem = append(elem, src[i])
			i++
		}
	}
	return nil, 0, fmt.Errorf("%w: unterminated quoted element", errJSONArrayText)
}

func isArraySpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func skipArraySpace(src []byte, i int) int {
	for i < len(src) && isArraySpace(src[i]) {
		i++
	}
	return i
}
