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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// newCollectionTestHandler returns a handler for the configured table
// public.users (key id) whose relation set also knows audit.users (key column
// uid), the same name in another schema, as a reused publication can stream
// it (#424). Relation 1 is public.users, 2 is audit.users. subjects collects
// every subject the handler registers.
func newCollectionTestHandler(withAvro bool) (*CDCHandler, chan []opencdc.Record, *[]string) {
	rs := internal.NewRelationSet()
	rs.Add(&pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "users", ColumnNum: 2,
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int8OID, Flags: 1}, {Name: "v", DataType: pgtype.TextOID}}})
	rs.Add(&pglogrepl.RelationMessage{RelationID: 2, Namespace: "audit", RelationName: "users", ColumnNum: 2,
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int8OID, Flags: 1}, {Name: "v", DataType: pgtype.TextOID}}})
	ch := make(chan []opencdc.Record, 4)
	h := NewCDCHandler(context.Background(), rs, map[string]string{"users": "id"}, ch, withAvro, 1, time.Hour,
		position.Position{}, SchemaDriftPolicyHalt)
	h.SetTableNamespaces(map[string]string{"users": "public"})
	subjects := &[]string{}
	h.loadNotNull = func(context.Context, uint32) (schema.NotNull, error) {
		return schema.NotNull{"id": true}, nil
	}
	h.createSchema = func(_ context.Context, typ cschema.Type, subject string, b []byte) (cschema.Schema, error) {
		*subjects = append(*subjects, subject)
		return cschema.Schema{Subject: subject, Version: 1, Type: typ, Bytes: b}, nil
	}
	return h, ch, subjects
}

// A same-name table in another schema must not take the configured table's
// collection, key column or Avro subjects (#424). Both tables here have an id
// column, the worst case: with the bare name, audit.users' rows got
// collection "users" and the key {id: 7}, so a destination routing on the
// collection upserted them over public.users' row 7.
func TestHandler_Collection_SameNameOtherSchema(t *testing.T) {
	ctx := context.Background()
	for _, withAvro := range []bool{false, true} {
		h, ch, subjects := newCollectionTestHandler(withAvro)
		is := is.New(t)
		for _, id := range []uint32{1, 2} {
			is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: id, Tuple: tuple(txt("7"), txt("x"))}, 1))
		}
		configured, other := (<-ch)[0], (<-ch)[0]

		// The configured table keeps the configured name, the one the
		// snapshot gives its records.
		is.Equal(configured.Metadata[opencdc.MetadataCollection], "users")
		is.Equal(configured.Key, opencdc.StructuredData{"id": int64(7)})

		is.Equal(other.Metadata[opencdc.MetadataCollection], "audit.users")
		is.Equal(other.Key, opencdc.StructuredData{}) // not a configured table: no key column

		if withAvro {
			is.Equal(*subjects, []string{"users_payload", "users_key", "audit.users_payload", "audit.users_key"})
			sub, err := configured.Metadata.GetPayloadSchemaSubject()
			is.NoErr(err)
			is.Equal(sub, "users_payload")
			sub, err = other.Metadata.GetPayloadSchemaSubject()
			is.NoErr(err)
			is.Equal(sub, "audit.users_payload")
		}
	}
}

// The delete path looks the key column up by the same name: a delete of the
// other schema's row must not be keyed as the configured table's.
func TestHandler_Collection_SameNameOtherSchema_Delete(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, _ := newCollectionTestHandler(false)
	is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID: 2, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(txt("7"), null()),
	}, 1))
	rec := (<-ch)[0]
	is.Equal(rec.Metadata[opencdc.MetadataCollection], "audit.users")
	is.Equal(rec.Key, opencdc.StructuredData{})
}

// Without SetTableNamespaces the handler names every relation by its bare
// name, as before #424; NewCDCIterator always sets it.
func TestHandler_Collection_NoNamespacesKeepsBareName(t *testing.T) {
	is := is.New(t)
	h, _, _ := newCollectionTestHandler(false)
	h.SetTableNamespaces(nil)
	is.Equal(h.tableName(&pglogrepl.RelationMessage{Namespace: "audit", RelationName: "users"}), "users")
}
