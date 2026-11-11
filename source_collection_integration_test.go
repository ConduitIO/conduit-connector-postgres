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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// The tests in this file pin that a table's snapshot and CDC records carry the
// same opencdc.collection (the configured table name), and that a table the
// pipeline did not configure never carries a configured table's (#424).

// openCollectionSource opens a source on url for tables with its own slot and
// publication, which the test cleans up.
func openCollectionSource(ctx context.Context, t *testing.T, url, tables, slot string, withAvro bool) (sdk.Source, error) {
	t.Helper()
	is := is.New(t)
	s := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url":                     url,
		"tables":                  tables,
		"snapshotMode":            "initial",
		"cdcMode":                 "logrepl",
		"logrepl.slotName":        slot,
		"logrepl.publicationName": slot,
		"logrepl.withAvroSchema":  fmt.Sprint(withAvro),
	}, s.Config(), Connector.NewSpecification().SourceParams))
	t.Cleanup(func() { is.NoErr(s.Teardown(context.Background())) })
	if err := s.Open(ctx, nil); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		is.NoErr(logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
			URL: test.RepmgrConnString, SlotName: slot, PublicationName: slot,
		}))
	})
	return s, nil
}

// createCollectionSchema creates a schema that is dropped with its tables when
// the test ends.
func createCollectionSchema(ctx context.Context, t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	is := is.New(t)
	id := test.RandomIdentifier(t)
	name := "s_" + id[max(0, len(id)-20):] // within the 63-byte identifier limit
	_, err := conn.Exec(ctx, "CREATE SCHEMA "+name)
	is.NoErr(err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP SCHEMA "+name+" CASCADE")
		is.NoErr(err)
	})
	return name
}

// createCollectionTable creates <schema>."<name>" (id bigint primary key, v
// text) and drops it when the test ends.
func createCollectionTable(ctx context.Context, t *testing.T, conn *pgx.Conn, schemaName, name string) {
	t.Helper()
	is := is.New(t)
	q := fmt.Sprintf("%s.%s", schemaName, pgx.Identifier{name}.Sanitize())
	_, err := conn.Exec(ctx, "CREATE TABLE "+q+" (id bigint PRIMARY KEY, v text)")
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+q)
	})
}

func execf(ctx context.Context, t *testing.T, conn *pgx.Conn, format string, args ...any) {
	t.Helper()
	_, err := conn.Exec(ctx, fmt.Sprintf(format, args...))
	is.New(t).NoErr(err)
}

func TestSource_Collection_SnapshotMatchesCDC(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	t.Run("unqualified table", func(t *testing.T) {
		is := is.New(t)
		table := test.RandomIdentifier(t)
		createCollectionTable(ctx, t, conn, "public", table)
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (1, 'snap')", table)

		s, err := openCollectionSource(ctx, t, test.RepmgrConnString, table, test.RandomIdentifier(t), true)
		is.NoErr(err)
		snap := readAcked(ctx, t, s, 1)[0]
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (2, 'cdc')", table)
		cdc := readAcked(ctx, t, s, 1)[0]

		is.Equal(snap.Operation, opencdc.OperationSnapshot)
		is.Equal(cdc.Operation, opencdc.OperationCreate)
		is.Equal(snap.Metadata[opencdc.MetadataCollection], table)
		is.Equal(cdc.Metadata[opencdc.MetadataCollection], table)
		snapSubject, err := snap.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		cdcSubject, err := cdc.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		is.Equal(snapSubject, cdcSubject)
	})

	t.Run("mixed-case table", func(t *testing.T) {
		is := is.New(t)
		table := strings.ToUpper(test.RandomIdentifier(t)[:1]) + test.RandomIdentifier(t)[1:]
		createCollectionTable(ctx, t, conn, "public", table)
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (1, 'snap')", pgx.Identifier{table}.Sanitize())

		s, err := openCollectionSource(ctx, t, test.RepmgrConnString, table, test.RandomIdentifier(t), false)
		is.NoErr(err)
		snap := readAcked(ctx, t, s, 1)[0]
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (2, 'cdc')", pgx.Identifier{table}.Sanitize())
		cdc := readAcked(ctx, t, s, 1)[0]

		is.Equal(snap.Metadata[opencdc.MetadataCollection], table)
		is.Equal(cdc.Metadata[opencdc.MetadataCollection], table)
	})

	// A table in a non-public schema is configured by its bare name and found
	// through the connection's search_path. Here a same-name table also
	// exists in public, after the other schema in the search_path: both phases
	// read the other schema's table and name it as configured.
	t.Run("non-public schema through search_path, same name in public", func(t *testing.T) {
		is := is.New(t)
		other := createCollectionSchema(ctx, t, conn)
		table := test.RandomIdentifier(t)
		createCollectionTable(ctx, t, conn, other, table)
		createCollectionTable(ctx, t, conn, "public", table)
		execf(ctx, t, conn, "INSERT INTO %s.%s VALUES (100, 'snap')", other, table)
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (1, 'public')", table)

		url := test.RepmgrConnString + "&search_path=" + other + ",public"
		s, err := openCollectionSource(ctx, t, url, table, test.RandomIdentifier(t), false)
		is.NoErr(err)
		snap := readAcked(ctx, t, s, 1)[0]
		execf(ctx, t, conn, "INSERT INTO public.%s VALUES (2, 'public')", table) // not published
		execf(ctx, t, conn, "INSERT INTO %s.%s VALUES (101, 'cdc')", other, table)
		cdc := readAcked(ctx, t, s, 1)[0]

		is.Equal(snap.Metadata[opencdc.MetadataCollection], table)
		is.Equal(snap.Key, opencdc.StructuredData{"id": int64(100)})
		is.Equal(cdc.Metadata[opencdc.MetadataCollection], table)
		is.Equal(cdc.Key, opencdc.StructuredData{"id": int64(101)})
	})
}

// A schema-qualified entry in tables is one quoted identifier, the name of a
// table that does not exist, so the source refuses to start: it never reads
// public.<table> under a collection that differs between the two phases.
func TestSource_Collection_SchemaQualifiedNameRefused(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := test.RandomIdentifier(t)
	createCollectionTable(ctx, t, conn, "public", table)

	_, err := openCollectionSource(ctx, t, test.RepmgrConnString, "public."+table, test.RandomIdentifier(t), false)
	is.True(err != nil)
	// The whole entry is the relation name Postgres looks for (truncated
	// to 63 bytes in the message): 42P01, undefined_table.
	want := `relation "public.`
	if !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "(SQLSTATE 42P01)") {
		t.Fatalf("error %q does not contain %q", err, want)
	}
}

// A publication that already exists under logrepl.publicationName is reused
// as it is. When it also streams a same-name table in another schema, that
// table's records must not carry the configured table's collection or key
// (#424): before the fix both said "<table>", and the other table's rows
// were keyed by the configured table's key column.
func TestSource_Collection_ReusedPublicationSameNameOtherSchema(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	other := createCollectionSchema(ctx, t, conn)
	table := test.RandomIdentifier(t)
	createCollectionTable(ctx, t, conn, "public", table)
	createCollectionTable(ctx, t, conn, other, table)
	execf(ctx, t, conn, "INSERT INTO public.%s VALUES (1, 'snap')", table)

	slot := test.RandomIdentifier(t)
	repl := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	execf(ctx, t, repl, "CREATE PUBLICATION %s FOR TABLE public.%s, %s.%s", slot, table, other, table)

	s, err := openCollectionSource(ctx, t, test.RepmgrConnString, table, slot, false)
	is.NoErr(err)
	snap := readAcked(ctx, t, s, 1)[0]
	is.Equal(snap.Metadata[opencdc.MetadataCollection], table)

	execf(ctx, t, conn, "INSERT INTO %s.%s VALUES (7, 'other')", other, table)
	execf(ctx, t, conn, "INSERT INTO public.%s VALUES (7, 'configured')", table)
	recs := readAcked(ctx, t, s, 2)

	is.Equal(recs[0].Metadata[opencdc.MetadataCollection], other+"."+table)
	is.Equal(recs[0].Key, opencdc.StructuredData{}) // not a configured table: no key column
	is.Equal(recs[1].Metadata[opencdc.MetadataCollection], table)
	is.Equal(recs[1].Key, opencdc.StructuredData{"id": int64(7)})
}
