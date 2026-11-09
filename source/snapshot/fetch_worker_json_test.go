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

package snapshot

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

const notUTF8 = "{\"k\":\"\x80\xff\"}"

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

var jsonFields = []pgconn.FieldDescription{
	{Name: "id", DataTypeOID: pgtype.Int8OID},
	{Name: "j", DataTypeOID: pgtype.JSONOID},
	{Name: "jb", DataTypeOID: pgtype.JSONBOID},
	{Name: "ja", DataTypeOID: pgtype.JSONBArrayOID},
}

// json.invalidUTF8 base64 in the snapshot: invalid text is the base64 of its
// bytes and named in rawJSON, valid columns of the same row are unchanged, an
// array with one invalid element is base64 element by element. The key, when
// it is such a column, is base64 too and named as a payload column.
func Test_FetchWorker_buildRecordData_JSONRawBytes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key         string
		values      []any
		wantPayload opencdc.StructuredData
		wantKey     opencdc.StructuredData
		wantRaw     []string
	}{{
		name:        "all valid",
		key:         "id",
		values:      []any{int64(1), []byte(`{"a": 1}`), []byte(`{}`), []any{[]byte(`1`), nil}},
		wantPayload: opencdc.StructuredData{"id": int64(1), "j": `{"a": 1}`, "jb": `{}`, "ja": []any{"1", nil}},
		wantKey:     opencdc.StructuredData{"id": int64(1)},
	}, {
		name:        "invalid j and array",
		key:         "id",
		values:      []any{int64(1), []byte(notUTF8), []byte(`{}`), []any{[]byte(`1`), nil, []byte(notUTF8)}},
		wantPayload: opencdc.StructuredData{"id": int64(1), "j": b64(notUTF8), "jb": `{}`, "ja": []any{b64(`1`), nil, b64(notUTF8)}},
		wantKey:     opencdc.StructuredData{"id": int64(1)},
		wantRaw:     []string{"j", "ja"},
	}, {
		name:        "NULLs",
		key:         "id",
		values:      []any{int64(1), nil, nil, nil},
		wantPayload: opencdc.StructuredData{"id": int64(1), "j": nil, "jb": nil, "ja": nil},
		wantKey:     opencdc.StructuredData{"id": int64(1)},
	}, {
		name:        "invalid jsonb key",
		key:         "jb",
		values:      []any{int64(1), []byte(`{}`), []byte(notUTF8), nil},
		wantPayload: opencdc.StructuredData{"id": int64(1), "j": `{}`, "jb": b64(notUTF8), "ja": nil},
		wantKey:     opencdc.StructuredData{"jb": b64(notUTF8)},
		wantRaw:     []string{"jb"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			w := &FetchWorker{conf: FetchConfig{Table: "t", Key: tc.key, JSON: types.NewJSONPolicy(types.JSONInvalidUTF8Base64)}}
			key, payload, raw, err := w.buildRecordData(jsonFields, tc.values)
			is.NoErr(err)
			is.Equal(payload, tc.wantPayload)
			is.Equal(key, tc.wantKey)
			is.Equal(raw, tc.wantRaw)
		})
	}
}

// json.invalidUTF8 halt keeps the coded error, located by table, column and
// row (key columns and a hash, never values).
func Test_FetchWorker_buildRecordData_JSONHalt(t *testing.T) {
	is := is.New(t)
	w := &FetchWorker{conf: FetchConfig{Table: "t", Key: "id", JSON: types.NewJSONPolicy(types.JSONInvalidUTF8Halt)}}
	_, _, raw, err := w.buildRecordData(jsonFields, []any{int64(7), []byte(`{}`), []byte(notUTF8), nil})
	var je *types.JSONNotUTF8Error
	is.True(errors.As(err, &je))
	is.Equal(je.Table, "t")
	is.Equal(je.Column, "jb")
	is.True(je.Row != "")
	is.Equal(raw, nil)
}

// The iterator names the base64 columns in postgres.json.raw_bytes, and adds
// nothing when there are none.
func Test_Iterator_buildRecord_JSONRawBytes(t *testing.T) {
	is := is.New(t)
	i := &Iterator{lastPosition: position.Position{Snapshots: make(position.SnapshotPositions)}}
	fd := FetchData{
		Key:      opencdc.StructuredData{"id": int64(1)},
		Payload:  opencdc.StructuredData{"id": int64(1)},
		Position: position.SnapshotPosition{LastRead: 1, SnapshotEnd: 1},
		Table:    "t",
		RawJSON:  []string{"jb", "a,b", "j"},
	}
	rec := i.buildRecord(fd)
	is.Equal(rec.Metadata[types.MetadataJSONRawBytes], `["a,b","j","jb"]`)

	fd.RawJSON = nil
	rec = i.buildRecord(fd)
	_, ok := rec.Metadata[types.MetadataJSONRawBytes]
	is.True(!ok)
}
