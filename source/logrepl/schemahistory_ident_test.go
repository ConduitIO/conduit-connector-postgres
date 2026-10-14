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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

// quoteIdent quotes a Postgres identifier verbatim. Go's %q would escape the
// control bytes, which is exactly what this test must not do.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Test_SchemaHash_QuotedIdentifierBytes_RealPostgres checks, against a real
// server, the two facts the schema-drift identity depends on:
//
//  1. a quoted column name containing 0x1f and 0x1e reaches the connector in the
//     pgoutput RelationMessage byte for byte, so the hash input really can
//     contain them; and
//  2. a table with such a column and a table with the columns it imitates hash
//     differently, so the drift classification sees a change.
//
// Before the hash input was made injective, the two shapes hashed equal and
// classifyDrift returned driftNone, so no drift was reported.
func Test_SchemaHash_QuotedIdentifierBytes_RealPostgres(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	const weird = "a\x1f25\x1f-1\x1eb"

	plain := test.RandomIdentifier(t)
	odd := test.RandomIdentifier(t)
	pub := test.RandomIdentifier(t)
	slot := test.RandomIdentifier(t)

	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int4, a text, b text)`, plain))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, plain)) })
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int4, %s text)`, odd, quoteIdent(weird)))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, odd)) })

	test.CreatePublication(t, conn, pub, []string{plain, odd})
	test.CreateReplicationSlot(t, conn, slot)

	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %q VALUES (1, 'x', 'y')`, plain))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %q VALUES (1, 'x')`, odd))
	is.NoErr(err)

	rows, err := conn.Query(ctx,
		`SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL,
			'proto_version', '1', 'publication_names', $2)`, slot, pub)
	is.NoErr(err)
	defer rows.Close()

	rels := map[string]*pglogrepl.RelationMessage{}
	for rows.Next() {
		var data []byte
		is.NoErr(rows.Scan(&data))
		msg, err := pglogrepl.Parse(data)
		is.NoErr(err)
		if r, ok := msg.(*pglogrepl.RelationMessage); ok {
			rels[r.RelationName] = r
		}
	}
	is.NoErr(rows.Err())

	rp, ro := rels[plain], rels[odd]
	is.True(rp != nil && ro != nil) // both tables announced a RelationMessage

	// Fact 1: the separator bytes survive the wire.
	is.Equal(len(ro.Columns), 2)
	is.Equal(ro.Columns[1].Name, weird)

	// Fact 2: the shapes are distinguishable.
	hp := position.HashColumnSet(columnIdentities(rp))
	ho := position.HashColumnSet(columnIdentities(ro))
	is.True(hp != ho)
}

// Test_HandleRelation_DriftAcrossRestart_SeparatorBytesInColumnName is the
// consequence of the hash input being ambiguous. The durable history holds the
// shape {id, a, b}; after a restart the table has become {id, "a<US>25<US>-1<RS>b"}.
// That is a different shape and must be classified as drift. With a separator
// joined hash input both shapes hash equal and the result is driftNone, so the
// drift policy (halt) never fires.
func Test_HandleRelation_DriftAcrossRestart_SeparatorBytesInColumnName(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	before := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("a", 25, -1), relCol("b", 25, -1)}
	after := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("a\x1f25\x1f-1\x1eb", 25, -1)}

	h1 := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	kind, _ := relate(ctx, t, h1, relMsg(before...), 100)
	is.Equal(kind, driftInitial)
	checkpoint := h1.buildPosition(150)

	resumed, err := position.ParseSDKPosition(checkpoint)
	is.NoErr(err)
	h2 := newHandlerWithPosition(t, resumed, SchemaDriftPolicyHalt)

	kind, marker := relate(ctx, t, h2, relMsg(after...), 200)
	is.Equal(kind, driftAcrossRestart)
	is.True(marker) // halt policy: a drift marker replaces the change
}
