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
	"reflect"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// mutate edits every container inside v in place, the way a processor that
// sets a nested field does, and reports whether it found one.
func mutate(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only containers can be edited in place
	case reflect.Slice:
		if rv.Len() == 0 {
			return false
		}
		e := rv.Index(0)
		if e.Kind() == reflect.Interface && !e.IsNil() && mutate(e.Interface()) {
			return true
		}
		e.Set(reflect.Zero(e.Type()))
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			e.SetUint(0xEE)
		}
		return true
	case reflect.Map:
		rv.SetMapIndex(reflect.ValueOf("injected").Convert(rv.Type().Key()), reflect.Zero(rv.Type().Elem()))
		return true
	}
	return false
}

// TestHandleUpdate_RecordPartsOwnTheirValues is the regression test for
// #425: under REPLICA IDENTITY FULL an unchanged-TOAST column of the new
// tuple is backfilled from the old one, and the key column is in the key and
// in After. Editing a value in After must change neither Before nor Key.
func TestHandleUpdate_RecordPartsOwnTheirValues(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		oid  uint32
		old  string
	}{
		{"int4[]", pgtype.Int4ArrayOID, "{1,2,3}"},
		{"text[]", pgtype.TextArrayOID, `{a,b}`},
		{"bytea", pgtype.ByteaOID, `\x010203`},
		{"jsonb", pgtype.JSONBOID, `{"a":{"b":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "own_" + tc.name[:4]
			rs := internal.NewRelationSet()
			rs.Add(&pglogrepl.RelationMessage{RelationID: 1, RelationName: name, ColumnNum: 2,
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.ByteaOID, Flags: 1},
					{Name: "c", DataType: tc.oid},
				}})
			ch := make(chan []opencdc.Record, 1)
			h := NewCDCHandler(ctx, rs, map[string]string{name: "id"}, ch, false, 1, time.Hour, position.Position{}, SchemaDriftPolicyHalt)
			err := h.handleUpdate(ctx, &pglogrepl.UpdateMessage{
				RelationID:   1,
				OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
				OldTuple:     tuple(txt(`\x0a0b`), txt(tc.old)),
				NewTuple:     tuple(txt(`\x0a0b`), unchangedToast()),
			}, 0x100)
			if err != nil {
				t.Fatal(err)
			}
			r := (<-ch)[0]
			after := r.Payload.After.(opencdc.StructuredData)
			before := r.Payload.Before.(opencdc.StructuredData)
			key := r.Key.(opencdc.StructuredData)
			wantBefore, wantKey := fmt.Sprint(before), fmt.Sprint(key)

			if !mutate(after["c"]) {
				t.Fatalf("After.c (%T) has no container to edit; the test would prove nothing", after["c"])
			}
			if !mutate(after["id"]) {
				t.Fatalf("After.id (%T) has no container to edit", after["id"])
			}
			if got := fmt.Sprint(before); got != wantBefore {
				t.Errorf("editing After changed Before: %s -> %s", wantBefore, got)
			}
			if got := fmt.Sprint(key); got != wantKey {
				t.Errorf("editing After changed Key: %s -> %s", wantKey, got)
			}
		})
	}
}
