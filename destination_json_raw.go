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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/types"
)

// ErrorCodeDestinationJSONRawBytesInvalid prefixes the error for a record
// whose types.MetadataJSONRawBytes metadata is not a JSON array of column
// names, names a column the record does not have, or lists a column whose
// value is not standard base64.
const ErrorCodeDestinationJSONRawBytesInvalid = "postgres.destination.json_raw_bytes_invalid"

// decodeJSONRawBytes returns key and payload with the value of every column
// listed in the record metadata types.MetadataJSONRawBytes replaced by the
// bytes its base64 holds. The source lists a json, jsonb, json[] or jsonb[]
// column there when its text was not valid UTF-8 and it emitted the base64
// of the bytes instead (json.invalidUTF8: base64). The value is a string, or
// for an array a list of strings and nils; it is returned in the same shape,
// each string holding the decoded bytes. Unlisted columns are unchanged. The
// returned maps are copies when anything was decoded: the record belongs to
// the caller (#343).
//
// The bytes reach the server as they are; nothing here or in pgx checks or
// changes them. Postgres reads a parameter in the session's client_encoding,
// which pgx leaves at the database encoding unless the url sets it:
//   - SQL_ASCII database: client_encoding is SQL_ASCII, which takes any byte
//     but NUL, so the bytes are stored exactly as the source read them.
//   - UTF8 database: Postgres checks them as UTF-8 and rejects them with its
//     own error, SQLSTATE 22021, so the write fails loudly. Such a database
//     cannot hold them.
//   - Any other encoding: Postgres reads them in that encoding, as it reads
//     every other value the destination sends there.
//
// types.MetadataJSONRawBytesBefore is ignored: the destination never writes
// Payload.Before.
//
// Invariant 6: a listed value is written as the bytes it encodes, never as
// its base64 text. A metadata value that cannot be read, a listed column the
// record does not have, or a listed value that is not base64 fails the
// record with ErrorCodeDestinationJSONRawBytesInvalid instead of guessing.
// The errors name the table and the column, never the value.
func decodeJSONRawBytes(
	meta opencdc.Metadata, table string, key, payload opencdc.StructuredData,
) (outKey, outPayload opencdc.StructuredData, err error) {
	v, ok := meta[types.MetadataJSONRawBytes]
	if !ok {
		return key, payload, nil
	}
	var cols []string
	if err := json.Unmarshal([]byte(v), &cols); err != nil || cols == nil {
		return nil, nil, fmt.Errorf("%s: the record for table %q has metadata %s that is not a JSON "+
			"array of column names. Fix: leave the metadata as the postgres source wrote it, or remove it if "+
			"no value of the record is base64", ErrorCodeDestinationJSONRawBytesInvalid, table, types.MetadataJSONRawBytes)
	}
	if len(cols) == 0 {
		return key, payload, nil
	}
	outKey, outPayload = maps.Clone(key), maps.Clone(payload)
	for _, col := range cols {
		kv, inKey := key[col]
		pv, inPayload := payload[col]
		if !inKey && !inPayload {
			return nil, nil, fmt.Errorf("%s: metadata %s of the record for table %q lists column %q, "+
				"which is not in the record's key or payload. Fix: leave the metadata as the postgres source "+
				"wrote it; a processor that drops or renames such a column must update the metadata",
				ErrorCodeDestinationJSONRawBytesInvalid, types.MetadataJSONRawBytes, table, col)
		}
		if inKey {
			if outKey[col], err = decodeRawJSONValue(kv, table, col); err != nil {
				return nil, nil, err
			}
		}
		if inPayload {
			if outPayload[col], err = decodeRawJSONValue(pv, table, col); err != nil {
				return nil, nil, err
			}
		}
	}
	return outKey, outPayload, nil
}

// decodeRawJSONValue decodes one listed value: a base64 string, or a list
// whose elements are base64 strings or nil (a NULL element of a json[] or
// jsonb[]). The bytes are returned as a string: pgx writes a string into a
// json, jsonb or text parameter as it is, in every query exec mode, while a
// []byte would be sent as bytea where the parameter type is not described.
func decodeRawJSONValue(v any, table, col string) (any, error) {
	switch t := v.(type) {
	case string:
		return decodeRawJSONText(t, table, col)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			if e == nil {
				continue // a NULL element stays NULL
			}
			s, ok := e.(string)
			if !ok {
				return nil, rawJSONValueError(table, col, fmt.Sprintf("element %d is a %T, not a base64 string", i+1, e))
			}
			d, err := decodeRawJSONText(s, table, col)
			if err != nil {
				return nil, err
			}
			out[i] = d
		}
		return out, nil
	case nil:
		return nil, rawJSONValueError(table, col, "its value is null, not a base64 string")
	default:
		return nil, rawJSONValueError(table, col, fmt.Sprintf("its value is a %T, not a base64 string", v))
	}
}

func decodeRawJSONText(s, table, col string) (string, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil {
		return "", rawJSONValueError(table, col, "its value is not standard padded base64")
	}
	return string(b), nil
}

func rawJSONValueError(table, col, what string) error {
	return fmt.Errorf("%s: column %q of table %q is listed in metadata %s, but %s. Fix: leave the value as the "+
		"postgres source wrote it; a processor that changes such a column must remove it from the metadata",
		ErrorCodeDestinationJSONRawBytesInvalid, col, table, types.MetadataJSONRawBytes, what)
}
