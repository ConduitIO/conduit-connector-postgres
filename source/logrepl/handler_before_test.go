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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// Regression tests for conduit-connector-postgres#379 and #380 at the handler
// level. The records are passed through the SDK's SourceWithEncoding
// middleware, which every source built with sdk.SourceWithMiddleware runs.
// The pre-#379 tests stopped at the handler output and asserted a Before
// that the middleware could not encode; encoding here is what makes the
// halt visible without a database.

var beforeTestRelationSeq atomic.Int64

// newBeforeTestHandler registers a fresh relation (id int8 key, i int4, t
// text) and returns a handler for it. Each call uses a new relation name, so
// the global in-memory schema service never mixes subjects across tests.
func newBeforeTestHandler(ctx context.Context, t *testing.T, withAvro bool) (*CDCHandler, chan []opencdc.Record, string) {
	t.Helper()
	name := fmt.Sprintf("before_test_%d", beforeTestRelationSeq.Add(1))
	rs := internal.NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: name,
		ColumnNum:    3,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
			{Name: "i", DataType: pgtype.Int4OID},
			{Name: "t", DataType: pgtype.TextOID},
		},
	})
	ch := make(chan []opencdc.Record, 1)
	h := NewCDCHandler(ctx, rs, map[string]string{name: "id"}, ch, withAvro, 1, time.Hour)
	return h, ch, name
}

func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: cols} // RelationSet.Values reads Columns only
}

func txt(v string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)}
}

func null() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

// staticSource returns fixed records from ReadN, so a test can run them
// through SDK middleware.
type staticSource struct {
	sdk.UnimplementedSource
	recs []opencdc.Record
}

func (s *staticSource) ReadN(context.Context, int) ([]opencdc.Record, error) {
	return s.recs, nil
}

// receiveAndEncode takes the one record the handler emitted and passes it
// through sdk.SourceWithEncoding. It returns the record as the handler built
// it, and the encoding error.
func receiveAndEncode(ctx context.Context, t *testing.T, ch chan []opencdc.Record) (opencdc.Record, error) {
	t.Helper()
	is := is.New(t)
	recs, ok, err := cchan.ChanOut[[]opencdc.Record](ch).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
	is.True(ok)
	is.Equal(len(recs), 1)
	built := recs[0].Clone()
	_, err = sdk.SourceWithEncoding{}.Wrap(&staticSource{recs: recs}).ReadN(ctx, 1)
	return built, err
}

func TestHandler_Before_KeyOnlyOldTuple(t *testing.T) {
	// 'K' old tuple: DEFAULT identity (or USING INDEX). pgoutput sends every
	// column, NULL for the non-identity ones.
	keyOnly := tuple(txt("1"), null(), null())
	full := tuple(txt("1"), txt("10"), txt("a"))

	tests := []struct {
		name       string
		handle     func(context.Context, *CDCHandler) error
		wantBefore opencdc.Data
		wantAfter  opencdc.Data
		wantKey    opencdc.StructuredData
	}{{
		name: "delete, key-only old tuple: no Before",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleDelete(ctx, &pglogrepl.DeleteMessage{
				RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: keyOnly,
			}, 1)
		},
		wantKey: opencdc.StructuredData{"id": int64(1)},
	}, {
		name: "delete, full old tuple: Before is the old row",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleDelete(ctx, &pglogrepl.DeleteMessage{
				RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: full,
			}, 1)
		},
		wantKey:    opencdc.StructuredData{"id": int64(1)},
		wantBefore: opencdc.StructuredData{"id": int64(1), "i": int32(10), "t": "a"},
	}, {
		name: "key-changing update, key-only old tuple: no Before",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
				RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: keyOnly,
				NewTuple: tuple(txt("2"), txt("10"), txt("a")),
			}, 1)
		},
		wantKey:   opencdc.StructuredData{"id": int64(2)},
		wantAfter: opencdc.StructuredData{"id": int64(2), "i": int32(10), "t": "a"},
	}, {
		name: "key-changing update, full old tuple: Before is the old row",
		handle: func(ctx context.Context, h *CDCHandler) error {
			return h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
				RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: full,
				NewTuple: tuple(txt("2"), txt("10"), txt("a")),
			}, 1)
		},
		wantKey:    opencdc.StructuredData{"id": int64(2)},
		wantBefore: opencdc.StructuredData{"id": int64(1), "i": int32(10), "t": "a"},
		wantAfter:  opencdc.StructuredData{"id": int64(2), "i": int32(10), "t": "a"},
	}}

	for _, tc := range tests {
		for _, withAvro := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/avro=%v", tc.name, withAvro), func(t *testing.T) {
				ctx := context.Background()
				is := is.New(t)
				h, ch, _ := newBeforeTestHandler(ctx, t, withAvro)

				is.NoErr(tc.handle(ctx, h))
				rec, encErr := receiveAndEncode(ctx, t, ch)

				// The #379 halt: the encoder rejected the null-filled Before.
				is.NoErr(encErr)
				is.Equal(rec.Key, opencdc.Data(tc.wantKey))
				is.Equal(rec.Payload.Before, tc.wantBefore)
				is.Equal(rec.Payload.After, tc.wantAfter)
			})
		}
	}
}

// TestHandler_Delete_NullKeyHalts covers #380 at runtime: under REPLICA
// IDENTITY USING INDEX on an index without the key column, the old tuple
// carries NULL for the key. Open refuses that identity; if it is set while
// the pipeline runs, the delete must stop the source instead of shipping a
// null key, which a destination applies as a no-op and acks.
func TestHandler_Delete_NullKeyHalts(t *testing.T) {
	for _, withAvro := range []bool{true, false} {
		t.Run(fmt.Sprintf("avro=%v", withAvro), func(t *testing.T) {
			ctx := context.Background()
			is := is.New(t)
			h, ch, name := newBeforeTestHandler(ctx, t, withAvro)

			// The operator needs the change LSN and the commit LSN of its
			// transaction to find the change and to advance the slot past it.
			h.lastTXLSN = pglogrepl.LSN(0x16B3A40)
			err := h.handleDelete(ctx, &pglogrepl.DeleteMessage{
				RelationID:   1,
				OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
				OldTuple:     tuple(null(), txt("10"), null()), // identity index on (i)
			}, pglogrepl.LSN(0x16B3A10))
			is.True(err != nil)
			is.True(strings.HasPrefix(err.Error(), ErrorCodeReplicaIdentityExcludesKey+": "))
			is.True(strings.Contains(err.Error(), fmt.Sprintf("%q", name)))
			is.True(strings.Contains(err.Error(), "WAL LSN 0/16B3A10"))
			is.True(strings.Contains(err.Error(), "commits at LSN 0/16B3A40"))
			is.True(strings.Contains(err.Error(), "pg_replication_slot_advance"))
			is.Equal(len(ch), 0) // nothing emitted
		})
	}
}

// TestHandler_Delete_UnknownRelationNotGuarded pins that the #380 guard only
// applies to relations with a key column resolved at Open. Others keep the
// previous behaviour (an empty key), which is out of scope here.
func TestHandler_Delete_UnknownRelationNotGuarded(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, _ := newBeforeTestHandler(ctx, t, false)
	h.tableKeys = map[string]string{} // relation not resolved at Open

	is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
		OldTuple:     tuple(txt("1"), null(), null()),
	}, 1))
	rec, err := receiveAndEncode(ctx, t, ch)
	is.NoErr(err)
	is.Equal(rec.Key, opencdc.Data(opencdc.StructuredData{}))
}
