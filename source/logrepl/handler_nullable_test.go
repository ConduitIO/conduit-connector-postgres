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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// Handler-level regression tests for #326, without a database: the records
// go through the SDK's SourceWithEncoding middleware, as in a pipeline, and
// are decoded with the schema attached to them.

// receiveEncoded takes the one record the handler emitted and returns it as
// SourceWithEncoding encoded it.
func receiveEncoded(ctx context.Context, t *testing.T, ch chan []opencdc.Record) (opencdc.Record, error) {
	t.Helper()
	is := is.New(t)
	recs, ok, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
	is.True(ok)
	is.Equal(len(recs), 1)
	out, err := sdk.SourceWithEncoding{}.Wrap(&staticSource{recs: recs}).ReadN(ctx, 1)
	if err != nil {
		return opencdc.Record{}, err
	}
	return out[0], nil
}

// decodePayload decodes an encoded After with the record's payload schema and
// returns it with the registered schema text.
func decodePayload(ctx context.Context, t *testing.T, rec opencdc.Record) (map[string]any, string) {
	t.Helper()
	is := is.New(t)
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	sch, err := sdkschema.Get(ctx, subject, version)
	is.NoErr(err)
	var got map[string]any
	is.NoErr(sch.Unmarshal(rec.Payload.After.Bytes(), &got))
	return got, string(sch.Bytes)
}

func TestHandler_Avro_NullEncodesAsNull(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, name := newBeforeTestHandler(ctx, t, true)

	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(txt("1"), null(), null()),
	}, 1))
	rec, err := receiveEncoded(ctx, t, ch)
	is.NoErr(err) // before #326: "avro: *avro.null is unsupported for Avro int"

	got, text := decodePayload(ctx, t, rec)
	is.Equal(got["id"], int64(1))
	for _, col := range []string{"i", "t"} {
		v, present := got[col]
		is.True(present)
		is.Equal(v, nil) // NULL, not 0 or ""
	}
	is.Equal(text, `{"name":"`+name+`_payload","type":"record","fields":[`+
		`{"name":"i","type":["null","int"],"default":null},`+
		`{"name":"id","type":"long"},`+
		`{"name":"t","type":["null","string"],"default":null}]}`)
}

// TestHandler_Avro_UnchangedToastRefused: with Avro on, an UPDATE whose new row
// lacks an unchanged TOASTed column stops the source with a coded error. The
// payload schema gives t a default of null, so encoding the record would turn
// "unchanged" into NULL (invariant 6; #317).
func TestHandler_Avro_UnchangedToastRefused(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, name := newBeforeTestHandler(ctx, t, true)

	err := h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
		RelationID: 1, NewTuple: tuple(txt("1"), txt("11"), unchangedToast()),
	}, pglogrepl.LSN(0x16B3A10))
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), ErrorCodeAvroUnchangedToast+": "))
	is.True(strings.Contains(err.Error(), `"`+name+`"`))
	is.True(strings.Contains(err.Error(), `["t"]`))
	is.True(strings.Contains(err.Error(), "0/16B3A10"))
	is.True(strings.Contains(err.Error(), "withAvroSchema to false"))
	is.Equal(len(ch), 0) // nothing emitted

	// Without Avro the column stays omitted, as before.
	h, ch, _ = newBeforeTestHandler(ctx, t, false)
	is.NoErr(h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
		RelationID: 1, NewTuple: tuple(txt("1"), txt("11"), unchangedToast()),
	}, 1))
	rec, err := receiveEncoded(ctx, t, ch)
	is.NoErr(err)
	_, present := rec.Payload.After.(opencdc.StructuredData)["t"]
	is.True(!present)
}

// TestHandler_Avro_NullabilityFromCatalog pins where nullability comes from:
// the catalog, read once per Relation message, and never assumed.
func TestHandler_Avro_NullabilityFromCatalog(t *testing.T) {
	insert := func(ctx context.Context, h *CDCHandler) error {
		return h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("1"), txt("10"), txt("a")),
		}, 1)
	}
	fieldOf := func(t *testing.T, text, name string) string {
		t.Helper()
		var rs struct {
			Fields []json.RawMessage `json:"fields"`
		}
		is.New(t).NoErr(json.Unmarshal([]byte(text), &rs))
		for _, f := range rs.Fields {
			if strings.Contains(string(f), `"name":"`+name+`"`) {
				return string(f)
			}
		}
		t.Fatalf("no field %q in %s", name, text)
		return ""
	}

	t.Run("read once per Relation message", func(t *testing.T) {
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		rel, err := h.relationSet.Get(1)
		is.NoErr(err)
		var calls int
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			calls++
			if calls == 1 {
				return schema.NotNull{"id": true, "i": true, "t": false}, nil
			}
			return schema.NotNull{"id": true, "i": false, "t": false}, nil // DROP NOT NULL on i
		}

		is.NoErr(insert(ctx, h))
		rec, err := receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		_, text := decodePayload(ctx, t, rec)
		is.Equal(fieldOf(t, text, "i"), `{"name":"i","type":"int"}`)
		is.NoErr(insert(ctx, h))
		_, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		is.Equal(calls, 1) // cached between Relation messages

		h.handleRelation(rel) // pgoutput re-sends the relation after an ALTER
		is.NoErr(insert(ctx, h))
		rec, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		_, text = decodePayload(ctx, t, rec)
		is.Equal(calls, 2)
		is.Equal(fieldOf(t, text, "i"), `{"name":"i","type":["null","int"],"default":null}`)
	})

	t.Run("column not in the catalog is nullable", func(t *testing.T) {
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		// t was dropped after the change being decoded was written.
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true}, nil
		}
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("1"), txt("10"), null()),
		}, 1))
		rec, err := receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		got, text := decodePayload(ctx, t, rec)
		is.Equal(fieldOf(t, text, "t"), `{"name":"t","type":["null","string"],"default":null}`)
		is.Equal(got["t"], nil)
	})

	t.Run("no catalog: fail, do not assume", func(t *testing.T) {
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		h.loadNotNull = nil
		is.True(insert(ctx, h) != nil)
		is.Equal(len(ch), 0)
	})

	t.Run("catalog read fails: the change fails", func(t *testing.T) {
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		boom := errors.New("boom")
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) { return nil, boom }
		is.True(errors.Is(insert(ctx, h), boom))
		is.Equal(len(ch), 0)
		is.Equal(len(h.avroTexts), 0) // a failed read is not cached
	})

	t.Run("NULL in a column the catalog says is NOT NULL widens it", func(t *testing.T) {
		// The catalog is newer than the change (SET NOT NULL after it was
		// written): the column's field becomes nullable, a new version is
		// registered and the NULL is delivered as null, never as a zero value.
		var logs bytes.Buffer
		ctx := zerolog.New(&logs).WithContext(context.Background())
		is := is.New(t)
		h, ch, name := newBeforeTestHandler(ctx, t, true)
		rel, err := h.relationSet.Get(1)
		is.NoErr(err)
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true, "t": true}, nil
		}
		version := func(rec opencdc.Record) int {
			v, err := rec.Metadata.GetPayloadSchemaVersion()
			is.NoErr(err)
			return v
		}

		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("1"), txt("10"), txt("secret-a")),
		}, 1))
		rec, err := receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		_, text := decodePayload(ctx, t, rec)
		is.Equal(fieldOf(t, text, "i"), `{"name":"i","type":"int"}`)
		narrow := version(rec)

		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("2"), null(), txt("secret-b")),
		}, pglogrepl.LSN(0x16B3A10)))
		rec, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err) // before: the source stopped
		got, text := decodePayload(ctx, t, rec)
		v, present := got["i"]
		is.True(present)
		is.Equal(v, nil)
		is.Equal(fieldOf(t, text, "i"), `{"name":"i","type":["null","int"],"default":null}`)
		is.Equal(fieldOf(t, text, "t"), `{"name":"t","type":"string"}`) // only the NULL column
		widenedVersion := version(rec)
		is.True(widenedVersion > narrow)

		// A later NULL and a later value keep the widened version, also
		// across a Relation message, and do not warn again.
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("3"), null(), txt("secret-c")),
		}, 2))
		rec, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		is.Equal(version(rec), widenedVersion)
		h.handleRelation(rel)
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("4"), txt("40"), txt("secret-d")),
		}, 3))
		rec, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		is.Equal(version(rec), widenedVersion)

		out := logs.String()
		is.Equal(strings.Count(out, ErrorCodeAvroNullInNotNullColumn), 1) // once per table and column
		is.True(strings.Contains(out, `"level":"warn"`))
		is.True(strings.Contains(out, `"table":"`+name+`"`))
		is.True(strings.Contains(out, `"column":"i"`))
		is.True(!strings.Contains(out, "secret")) // never a value
	})

	t.Run("registry rejects the widened schema: the change fails with the code", func(t *testing.T) {
		ctx := context.Background()
		is := is.New(t)
		h, ch, name := newBeforeTestHandler(ctx, t, true)
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true, "t": true}, nil
		}
		rejected := errors.New("Schema being registered is incompatible with an earlier schema for subject")
		h.createSchema = func(ctx context.Context, typ cschema.Type, subject string, b []byte) (cschema.Schema, error) {
			if strings.Contains(string(b), `"null"`) {
				return cschema.Schema{}, rejected // FORWARD or FULL compatibility
			}
			return sdkschema.Create(ctx, typ, subject, b)
		}
		err := h.handleInsert(ctx, &pglogrepl.InsertMessage{
			RelationID: 1, Tuple: tuple(txt("1"), null(), txt("secret")),
		}, pglogrepl.LSN(0x16B3A10))
		is.True(err != nil)
		is.True(errors.Is(err, rejected))
		is.True(strings.HasPrefix(err.Error(), ErrorCodeAvroNullInNotNullColumn+": "))
		is.True(strings.Contains(err.Error(), `"`+name+`"`))
		is.True(strings.Contains(err.Error(), `["i"]`))
		is.True(strings.Contains(err.Error(), "0/16B3A10"))
		is.True(strings.Contains(err.Error(), name+"_payload"))
		is.True(strings.Contains(err.Error(), "BACKWARD"))
		is.True(!strings.Contains(err.Error(), "secret")) // never a value
		is.Equal(len(ch), 0)                              // nothing emitted
		is.True(h.avroTexts[1].notNull["i"])              // the narrow texts stay cached
		is.Equal(len(h.widened), 0)                       // nothing widened: a Relation message rebuilds the narrow schema
	})

	t.Run("NULL only in a full old row widens too", func(t *testing.T) {
		// REPLICA IDENTITY FULL: UPDATE c = 11 on a row that held NULL,
		// decoded after SET NOT NULL. Before carries the NULL and is encoded
		// with the payload schema, so the field must admit it.
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true, "t": true}, nil
		}
		assertBeforeNull := func(rec opencdc.Record) {
			t.Helper()
			subject, err := rec.Metadata.GetPayloadSchemaSubject()
			is.NoErr(err)
			version, err := rec.Metadata.GetPayloadSchemaVersion()
			is.NoErr(err)
			sch, err := sdkschema.Get(ctx, subject, version)
			is.NoErr(err)
			var before map[string]any
			is.NoErr(sch.Unmarshal(rec.Payload.Before.Bytes(), &before))
			v, present := before["i"]
			is.True(present)
			is.Equal(v, nil)
		}
		is.NoErr(h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
			RelationID:   1,
			OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple:     tuple(txt("1"), null(), txt("a")),
			NewTuple:     tuple(txt("1"), txt("11"), txt("a")),
		}, 1))
		rec, err := receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		assertBeforeNull(rec)

		// The same for a DELETE of such a row.
		h, ch, _ = newBeforeTestHandler(ctx, t, true)
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true, "t": true}, nil
		}
		is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
			RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(txt("1"), null(), txt("a")),
		}, 1))
		rec, err = receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		assertBeforeNull(rec)
	})

	t.Run("key-only old tuple: its NULLs are not values", func(t *testing.T) {
		// Under DEFAULT identity a DELETE carries a key-only old tuple ('K')
		// with NULL in every other column, NOT NULL ones included. Those
		// NULLs are never emitted and must not stop the source.
		ctx := context.Background()
		is := is.New(t)
		h, ch, _ := newBeforeTestHandler(ctx, t, true)
		h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
			return schema.NotNull{"id": true, "i": true, "t": true}, nil
		}
		is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
			RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(txt("1"), null(), null()),
		}, 1))
		_, err := receiveEncoded(ctx, t, ch)
		is.NoErr(err)
		is.Equal(len(h.widened), 0) // nothing widened
		is.True(h.avroTexts[1].notNull["i"])
		is.True(h.avroTexts[1].notNull["t"])
	})
}

// TestHandler_Avro_UpdateBeforeShaped: under REPLICA IDENTITY FULL the old row
// of an UPDATE is the record's Before, encoded with the same payload schema as
// After. A nullable timestamp must be shaped for its ["null", long] union in
// Before as well (types.NullableAvroValue), or the encode fails and the
// source stops on a perfectly ordinary update.
func TestHandler_Avro_UpdateBeforeShaped(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	name := fmt.Sprintf("before_test_%d", beforeTestRelationSeq.Add(1))
	rs := internal.NewRelationSetWithEncoding(types.EncodingFor(true))
	rs.Add(&pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: name,
		ColumnNum:    2,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
			{Name: "ts", DataType: pgtype.TimestampOID},
		},
	})
	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, rs, map[string]string{name: "id"}, ch, true, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)
	h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
		return schema.NotNull{"id": true, "ts": false}, nil
	}

	is.NoErr(h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(txt("1"), txt("2022-03-14 15:16:17")),
		NewTuple:     tuple(txt("1"), txt("2023-01-02 03:04:05")),
	}, 1))
	rec, err := receiveEncoded(ctx, t, ch)
	is.NoErr(err) // without shaping Before: the encoder finds no union branch for int64

	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	sch, err := sdkschema.Get(ctx, subject, version)
	is.NoErr(err)
	for _, tc := range []struct {
		data opencdc.Data
		want string
	}{
		{rec.Payload.Before, "2022-03-14 15:16:17"},
		{rec.Payload.After, "2023-01-02 03:04:05"},
	} {
		var got map[string]any
		is.NoErr(sch.Unmarshal(tc.data.Bytes(), &got))
		ts, ok := got["ts"].(time.Time)
		is.True(ok) // the value, not NULL
		// local-timestamp-micros decodes as a wall clock in the local zone.
		is.Equal(ts.Format(time.DateTime), tc.want)
	}
}
