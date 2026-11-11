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
	"strings"
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
	"github.com/rs/zerolog"
)

// addRelation adds a users-shaped relation (id, v) to h's relation set.
func addRelation(h *CDCHandler, id uint32, namespace, name string) {
	h.relationSet.Add(&pglogrepl.RelationMessage{RelationID: id, Namespace: namespace, RelationName: name, ColumnNum: 2,
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: pgtype.Int8OID, Flags: 1}, {Name: "v", DataType: pgtype.TextOID}}})
}

// newCollectionTestHandler returns a handler for the configured table
// public.users (OID 1, key id) whose relation set also knows audit.users
// (OID 2), the same name in another schema, as a reused publication can
// stream it (#424). Both have an id column, the worst case. subjects collects
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
	h.SetTableNamespaces(map[string]ConfiguredTable{"users": {OID: 1, Namespace: "public"}})
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

// A configured table moved to another schema (ALTER TABLE ... SET SCHEMA)
// while the pipeline runs keeps its OID and its name, so it keeps its
// configured name and key column, and a delete with a missing key is still
// refused (invariant 3).
func TestHandler_Collection_ConfiguredTableMatchedByOID(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, _ := newCollectionTestHandler(false)
	addRelation(h, 1, "moved", "users") // the Relation message after SET SCHEMA

	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(txt("7"), txt("x"))}, 1))
	rec := (<-ch)[0]
	is.Equal(rec.Metadata[opencdc.MetadataCollection], "users")
	is.Equal(rec.Key, opencdc.StructuredData{"id": int64(7)})

	is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(txt("7"), null()),
	}, 2))
	rec = (<-ch)[0]
	is.Equal(rec.Metadata[opencdc.MetadataCollection], "users")
	is.Equal(rec.Key, opencdc.StructuredData{"id": int64(7)})

	err := h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(null(), null()),
	}, 3)
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), ErrorCodeReplicaIdentityExcludesKey))
}

// A configured table renamed while the pipeline runs is not matched by OID.
// In a rename-and-swap (users to users_old, a copy to users) the retired
// table's delete must not carry the configured collection and key: it would
// delete a live row of users downstream.
func TestHandler_Collection_RenamedTableNotMatchedByOID(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, _ := newCollectionTestHandler(false)
	addRelation(h, 1, "public", "users_old") // the configured table, renamed
	addRelation(h, 3, "public", "users")     // the copy, renamed to users

	is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 3, Tuple: tuple(txt("2"), txt("x"))}, 1))
	rec := (<-ch)[0]
	is.Equal(rec.Metadata[opencdc.MetadataCollection], "users")
	is.Equal(rec.Key, opencdc.StructuredData{"id": int64(2)})

	is.NoErr(h.handleDelete(ctx, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(txt("1"), null()),
	}, 2))
	rec = (<-ch)[0]
	is.Equal(rec.Metadata[opencdc.MetadataCollection], "public.users_old")
	is.Equal(rec.Key, opencdc.StructuredData{})
}

// A table dropped and created again under the same name has a new OID and is
// matched by (namespace, name); the old relation is gone, so this is not two
// relations for one configured name.
func TestHandler_Collection_DroppedAndRecreated(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	h, ch, _ := newCollectionTestHandler(false)
	addRelation(h, 3, "public", "users")
	for i, id := range []uint32{1, 3} {
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: id, Tuple: tuple(txt("8"), txt("y"))}, pglogrepl.LSN(i+1)))
		rec := (<-ch)[0]
		is.Equal(rec.Metadata[opencdc.MetadataCollection], "users")
		is.Equal(rec.Key, opencdc.StructuredData{"id": int64(8)})
	}
}

// The configured table moved with SET SCHEMA (matched by OID) and a new
// table under the configured name (matched by name) are two live relations
// for one configured name: the change is refused, in either order, and
// nothing is emitted for it.
func TestHandler_Collection_TwoRelationsOneConfiguredNameHalts(t *testing.T) {
	ctx := context.Background()
	for _, order := range [][2]uint32{{1, 3}, {3, 1}} {
		is := is.New(t)
		h, ch, _ := newCollectionTestHandler(false)
		addRelation(h, 1, "archive", "users") // the configured table, moved
		addRelation(h, 3, "public", "users")  // a new table in its place

		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: order[0], Tuple: tuple(txt("1"), txt("secret-a"))}, 1))
		<-ch
		err := h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: order[1], Tuple: tuple(txt("1"), txt("secret-b"))}, 2)
		is.True(err != nil)
		is.True(strings.HasPrefix(err.Error(), ErrorCodeConfiguredTableAmbiguous+": "))
		is.True(strings.Contains(err.Error(), "archive.users"))
		is.True(strings.Contains(err.Error(), "public.users"))
		// the new holder of the name was never snapshotted: the fix says how to read its rows
		is.True(strings.Contains(err.Error(), "never snapshotted"))
		is.True(strings.Contains(err.Error(), "re-snapshot"))
		is.True(strings.Contains(err.Error(), `tables: "users", snapshotMode: initial`))
		is.True(!strings.Contains(err.Error(), "secret")) // never a value
		is.Equal(len(ch), 0)
	}
}

// A schema name that is not a valid Avro name must not halt the source: the
// subject is schema-qualified, the Avro record name is the bare relation
// name, as it was before #424.
func TestHandler_Collection_SchemaNameNotAnAvroName(t *testing.T) {
	ctx := context.Background()
	for _, ns := range []string{"sales-eu", "1x"} {
		is := is.New(t)
		h, ch, subjects := newCollectionTestHandler(true)
		addRelation(h, 4, ns, "users")
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: 4, Tuple: tuple(txt("7"), txt("x"))}, 1))
		rec := (<-ch)[0]
		is.Equal(rec.Metadata[opencdc.MetadataCollection], ns+".users")
		is.Equal(*subjects, []string{ns + ".users_payload", ns + ".users_key"})
		sub, err := rec.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		is.Equal(sub, ns+".users_payload")
	}
}

// postgres.source.unconfigured_table is the only signal for this class: it is
// logged once per unconfigured relation, never for a configured one.
func TestHandler_Collection_UnconfiguredWarnedOnce(t *testing.T) {
	is := is.New(t)
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).WithContext(context.Background())
	h, ch, _ := newCollectionTestHandler(false)
	addRelation(h, 5, "sales", "orders")
	for i, id := range []uint32{1, 2, 2, 5, 2, 1, 5} {
		is.NoErr(h.handleInsert(ctx, &pglogrepl.InsertMessage{RelationID: id, Tuple: tuple(txt("7"), txt("x"))}, pglogrepl.LSN(i+1)))
		<-ch
	}
	out := logs.String()
	is.Equal(strings.Count(out, WarnCodeUnconfiguredTable), 2)
	is.Equal(strings.Count(out, `"table":"audit.users"`), 1)
	is.Equal(strings.Count(out, `"table":"sales.orders"`), 1)
	is.True(!strings.Contains(out, `"table":"users"`))
	is.True(strings.Contains(out, `"level":"warn"`))
}
