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

package logrepl

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// rawJSON is json text with the bytes 0x80 0xff in a string, which only a
// SQL_ASCII database stores.
const rawJSON = "{\"k\":\"\x80\xff\"}"

var rawJSONB64 = base64.StdEncoding.EncodeToString([]byte(rawJSON))

// newJSONRawTestHandler registers a relation (id int8 key, j json, jb jsonb,
// and ja jsonb[] without Avro, which has no schema for arrays) and returns a
// handler for it with the given json.invalidUTF8 policy.
func newJSONRawTestHandler(ctx context.Context, t *testing.T, withAvro bool, p types.JSONInvalidUTF8, keyColumn string) (*CDCHandler, chan []opencdc.Record) {
	t.Helper()
	name := fmt.Sprintf("json_raw_test_%d", beforeTestRelationSeq.Add(1))
	cols := []*pglogrepl.RelationMessageColumn{
		{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
		{Name: "j", DataType: pgtype.JSONOID},
		{Name: "jb", DataType: pgtype.JSONBOID},
	}
	if !withAvro {
		cols = append(cols, &pglogrepl.RelationMessageColumn{Name: "ja", DataType: pgtype.JSONBArrayOID})
	}
	rs := internal.NewRelationSetWithEncoding(types.EncodingFor(withAvro))
	rs.Add(&pglogrepl.RelationMessage{RelationID: 1, RelationName: name, Columns: cols})
	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, rs, map[string]string{name: keyColumn}, ch, withAvro, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)
	h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
		return schema.NotNull{"id": true, "j": false, "jb": false, "ja": false}, nil
	}
	h.SetJSONPolicy(types.NewJSONPolicy(p))
	return h, ch
}

// row builds a tuple for the test relation: id, j, jb (and ja without Avro).
func row(withAvro bool, id, j, jb string, ja *pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	cols := []*pglogrepl.TupleDataColumn{txt(id), txt(j), txt(jb)}
	if !withAvro {
		if ja == nil {
			ja = null()
		}
		cols = append(cols, ja)
	}
	return tuple(cols...)
}

// json.invalidUTF8 base64 in CDC: a value whose text is not valid UTF-8 is the
// base64 of its bytes, its column is named in postgres.json.raw_bytes (Key and
// After) or postgres.json.raw_bytes.before (Before), valid values in the same
// record are untouched, and the record encodes (Avro too) and is delivered.
func TestHandler_JSONRawBytes_Base64(t *testing.T) {
	type want struct {
		key, before, after opencdc.Data
		raw, rawBefore     string // "" means the metadata key is absent
	}
	for _, withAvro := range []bool{true, false} {
		valid := `{"ok": 1}`
		for _, tc := range []struct {
			name   string
			handle func(context.Context, *CDCHandler) error
			want   want
		}{{
			name: "insert, invalid j",
			handle: func(ctx context.Context, h *CDCHandler) error {
				return h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: row(withAvro, "1", rawJSON, valid, nil)}, 1)
			},
			want: want{
				key:   opencdc.StructuredData{"id": int64(1)},
				after: payload(withAvro, map[string]any{"id": int64(1), "j": rawJSONB64, "jb": valid}),
				raw:   `["j"]`,
			},
		}, {
			name: "insert, all valid",
			handle: func(ctx context.Context, h *CDCHandler) error {
				return h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: row(withAvro, "1", valid, valid, nil)}, 1)
			},
			want: want{
				key:   opencdc.StructuredData{"id": int64(1)},
				after: payload(withAvro, map[string]any{"id": int64(1), "j": valid, "jb": valid}),
			},
		}, {
			name: "update fixing the value, full old row",
			handle: func(ctx context.Context, h *CDCHandler) error {
				return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
					RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
					OldTuple: row(withAvro, "1", rawJSON, valid, nil),
					NewTuple: row(withAvro, "1", valid, valid, nil),
				}, 1)
			},
			want: want{
				key:       opencdc.StructuredData{"id": int64(1)},
				before:    payload(withAvro, map[string]any{"id": int64(1), "j": rawJSONB64, "jb": valid}),
				after:     payload(withAvro, map[string]any{"id": int64(1), "j": valid, "jb": valid}),
				rawBefore: `["j"]`,
			},
		}, {
			name: "update, unchanged TOASTed j backfilled from the full old row",
			handle: func(ctx context.Context, h *CDCHandler) error {
				newTuple := row(withAvro, "1", "", valid, nil)
				newTuple.Columns[1] = unchangedToast()
				return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
					RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
					OldTuple: row(withAvro, "1", rawJSON, rawJSON, nil),
					NewTuple: newTuple,
				}, 1)
			},
			want: want{
				key:       opencdc.StructuredData{"id": int64(1)},
				before:    payload(withAvro, map[string]any{"id": int64(1), "j": rawJSONB64, "jb": rawJSONB64}),
				after:     payload(withAvro, map[string]any{"id": int64(1), "j": rawJSONB64, "jb": valid}),
				raw:       `["j"]`,
				rawBefore: `["j","jb"]`,
			},
		}, {
			name: "update, key-only old row: no Before, nothing named for it",
			handle: func(ctx context.Context, h *CDCHandler) error {
				return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
					RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
					OldTuple: row(withAvro, "0", rawJSON, rawJSON, nil),
					NewTuple: row(withAvro, "1", valid, rawJSON, nil),
				}, 1)
			},
			want: want{
				key:   opencdc.StructuredData{"id": int64(1)},
				after: payload(withAvro, map[string]any{"id": int64(1), "j": valid, "jb": rawJSONB64}),
				raw:   `["jb"]`,
			},
		}, {
			name: "delete, full old row",
			handle: func(ctx context.Context, h *CDCHandler) error {
				return h.handleDelete(ctx, &pglogrepl.DeleteMessage{
					RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
					OldTuple: row(withAvro, "1", valid, rawJSON, nil),
				}, 1)
			},
			want: want{
				key:       opencdc.StructuredData{"id": int64(1)},
				before:    payload(withAvro, map[string]any{"id": int64(1), "j": valid, "jb": rawJSONB64}),
				rawBefore: `["jb"]`,
			},
		}} {
			t.Run(fmt.Sprintf("%s/avro=%v", tc.name, withAvro), func(t *testing.T) {
				ctx := context.Background()
				is := is.New(t)
				h, ch := newJSONRawTestHandler(ctx, t, withAvro, types.JSONInvalidUTF8Base64, "id")
				is.NoErr(tc.handle(ctx, h))
				rec, encErr := receiveAndEncode(ctx, t, ch)
				is.NoErr(encErr) // the record encodes: it is delivered, not dropped
				is.Equal(rec.Key, tc.want.key)
				is.Equal(rec.Payload.Before, tc.want.before)
				is.Equal(rec.Payload.After, tc.want.after)
				raw, ok := rec.Metadata[types.MetadataJSONRawBytes]
				is.Equal(ok, tc.want.raw != "")
				is.Equal(raw, tc.want.raw)
				rawBefore, ok := rec.Metadata[types.MetadataJSONRawBytesBefore]
				is.Equal(ok, tc.want.rawBefore != "")
				is.Equal(rawBefore, tc.want.rawBefore)
			})
		}
	}
}

// payload is the expected After/Before: without Avro the test relation also
// has ja, NULL in these cases.
func payload(withAvro bool, m map[string]any) opencdc.Data {
	if !withAvro {
		m["ja"] = nil
	}
	return opencdc.StructuredData(m)
}

// json[]/jsonb[] are decided per column: one invalid element makes every
// non-NULL element base64, so a consumer decodes a listed column element by
// element.
func TestHandler_JSONRawBytes_ArrayPerColumn(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch := newJSONRawTestHandler(ctx, t, false, types.JSONInvalidUTF8Base64, "id")
	ja := txt("{\"{\\\"a\\\": 1}\",NULL,\"" + "{\\\"k\\\":\\\"\x80\xff\\\"}" + "\"}")
	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: row(false, "1", "{}", "{}", ja)}, 1))
	rec, encErr := receiveAndEncode(ctx, t, ch)
	is.NoErr(encErr)
	after := rec.Payload.After.(opencdc.StructuredData)
	is.Equal(after["ja"], []any{base64.StdEncoding.EncodeToString([]byte(`{"a": 1}`)), nil, rawJSONB64})
	is.Equal(after["j"], "{}")
	is.Equal(rec.Metadata[types.MetadataJSONRawBytes], `["ja"]`)
}

// A json key column that is not valid UTF-8 is base64 in the key, and the key
// is covered by postgres.json.raw_bytes, also for a delete, whose key comes
// from the old row.
func TestHandler_JSONRawBytes_Key(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch := newJSONRawTestHandler(ctx, t, false, types.JSONInvalidUTF8Base64, "jb")
	is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
		OldTuple: row(false, "1", "{}", rawJSON, nil),
	}, 1))
	rec, encErr := receiveAndEncode(ctx, t, ch)
	is.NoErr(encErr)
	is.Equal(rec.Key, opencdc.Data(opencdc.StructuredData{"jb": rawJSONB64}))
	is.Equal(rec.Payload.Before, nil)
	is.Equal(rec.Metadata[types.MetadataJSONRawBytes], `["jb"]`)
	_, ok := rec.Metadata[types.MetadataJSONRawBytesBefore]
	is.True(!ok)
}

// json.invalidUTF8 halt keeps the coded error, in the new row and in a full
// old row (Before is not dropped quietly), and emits nothing. A key-only old
// tuple of an UPDATE ('K', the key changed) is never emitted, so invalid
// text in it does not halt: the record is delivered without Before. A
// DELETE's key-only old tuple is the key, so it halts.
func TestHandler_JSONRawBytes_Halt(t *testing.T) {
	valid := `{"ok": 1}`
	for _, tc := range []struct {
		name      string
		keyColumn string
		handle    func(context.Context, *CDCHandler) error
		noHalt    bool
	}{{
		name: "insert",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: row(true, "1", rawJSON, valid, nil)}, 1)
		},
	}, {
		name: "update, invalid only in the full old row",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
				RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
				OldTuple: row(true, "1", rawJSON, valid, nil),
				NewTuple: row(true, "1", valid, valid, nil),
			}, 1)
		},
	}, {
		name:      "update, invalid only in the key-only old tuple",
		keyColumn: "jb",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
				RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
				OldTuple: tuple(null(), null(), txt(rawJSON)),
				NewTuple: row(true, "1", valid, valid, nil),
			}, 1)
		},
		noHalt: true,
	}, {
		name: "delete",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleDelete(ctx, &pglogrepl.DeleteMessage{
				RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
				OldTuple: row(true, "1", valid, rawJSON, nil),
			}, 1)
		},
	}, {
		name:      "delete, key-only old tuple",
		keyColumn: "jb",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleDelete(ctx, &pglogrepl.DeleteMessage{
				RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
				OldTuple: tuple(null(), null(), txt(rawJSON)),
			}, 1)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			is := is.New(t)
			keyColumn := tc.keyColumn
			if keyColumn == "" {
				keyColumn = "id"
			}
			h, ch := newJSONRawTestHandler(ctx, t, true, types.JSONInvalidUTF8Halt, keyColumn)
			err := tc.handle(ctx, h)
			if tc.noHalt {
				is.NoErr(err)
				rec, encErr := receiveAndEncode(ctx, t, ch)
				is.NoErr(encErr)
				is.Equal(rec.Payload.Before, nil)
				_, ok := rec.Metadata[types.MetadataJSONRawBytesBefore]
				is.True(!ok)
				_, ok = rec.Metadata[types.MetadataJSONRawBytes]
				is.True(!ok)
				return
			}
			var je *types.JSONNotUTF8Error
			is.True(errors.As(err, &je))
			is.True(je.Table != "" && je.Column != "")
			select {
			case recs := <-ch:
				t.Fatalf("emitted %d records on a halt", len(recs))
			default:
			}
		})
	}
}
