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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
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

// legacyHash is the hash input used before #408, written out here so the tests
// do not depend on the code under test: sorted "name US type US typmod" records
// joined with RS.
func legacyHash(cols ...*pglogrepl.RelationMessageColumn) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("%s\x1f%d\x1f%d", c.Name, c.DataType, c.TypeModifier))
	}
	slices.Sort(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1e")))
	return hex.EncodeToString(sum[:])
}

func positionWithHash(hash string) position.Position {
	return positionWithHashAndColumns(hash, 0)
}

// positionWithHashAndColumns is positionWithHash with the recorded column count
// (0: not recorded, as in a main-build position).
func positionWithHashAndColumns(hash string, columns int) position.Position {
	return position.Position{
		Type:          position.TypeCDC,
		SchemaHistory: position.SchemaHistories{"public.users": {{ColumnSetHash: hash, FirstSeenLSN: "0/64", Columns: columns}}},
	}
}

// A position written by a build from before #408 holds the old-form hash. When
// that hash is proven to be the shape now seen, the restart must not read it as
// drift under any policy, and the hash is re-recorded so the next start
// compares equal.
func Test_LegacyHash_Matching_IsAcceptedAndRerecorded(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h := newHandlerWithPosition(t, positionWithHash(legacyHash(shapeV1...)), SchemaDriftPolicyHalt)
	kind, marker := relate(ctx, t, h, relMsg(shapeV1...), 200)
	is.Equal(kind, driftNone)
	is.True(!marker)

	hist := h.basePosition.SchemaHistory["public.users"]
	is.Equal(len(hist), 1) // replaced in place, not appended
	is.Equal(hist[0].ColumnSetHash, position.HashColumnSet(columnIdentities(relMsg(shapeV1...))))
	is.Equal(hist[0].FirstSeenLSN, "0/64")
	is.Equal(hist[0].Columns, len(shapeV1)) // the replacement carries the count

	resumed, err := position.ParseSDKPosition(h.buildPosition(250))
	is.NoErr(err)
	h2 := newHandlerWithPosition(t, resumed, SchemaDriftPolicyHalt)
	kind, marker = relate(ctx, t, h2, relMsg(shapeV1...), 300)
	is.Equal(kind, driftNone)
	is.True(!marker)
}

// A name holding an old separator byte is never accepted on the strength of the
// old hash, even when that hash is the old hash of exactly this shape.
func Test_LegacyHash_NameWithSeparatorByte_StillHalts(t *testing.T) {
	is := is.New(t)
	weird := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("a\x1f25\x1f-1\x1eb", 25, -1)}

	h := newHandlerWithPosition(t, positionWithHash(legacyHash(weird...)), SchemaDriftPolicyHalt)
	kind, marker := relate(context.Background(), t, h, relMsg(weird...), 200)
	is.Equal(kind, driftAcrossRestart)
	is.True(marker)
}

// An old-form hash that is not this shape's is a real change.
func Test_LegacyHash_NotMatching_IsDrift(t *testing.T) {
	is := is.New(t)

	h := newHandlerWithPosition(t, positionWithHash(legacyHash(shapeV1...)), SchemaDriftPolicyHalt)
	kind, marker := relate(context.Background(), t, h, relMsg(shapeV2...), 200)
	is.Equal(kind, driftAcrossRestart)
	is.True(marker)
}

// The recorded column count closes the gap a hash alone leaves. The stored
// shape {id, "a<US>25<US>-1<RS>b"} has the same old-form hash as {id, a, b}, and
// the current names contain no separator byte, so the guarded check alone would
// accept it as unchanged. Its recorded count (2) differs from the current one
// (3), so it is drift.
func Test_LegacyHash_ColumnCountRejectsImitatingShape(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	imitating := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("a\x1f25\x1f-1\x1eb", 25, -1)}
	current := []*pglogrepl.RelationMessageColumn{relCol("id", 23, -1), relCol("a", 25, -1), relCol("b", 25, -1)}
	is.Equal(legacyHash(imitating...), legacyHash(current...)) // the premise

	h := newHandlerWithPosition(t, positionWithHashAndColumns(legacyHash(imitating...), len(imitating)), SchemaDriftPolicyHalt)
	kind, marker := relate(ctx, t, h, relMsg(current...), 200)
	is.Equal(kind, driftAcrossRestart)
	is.True(marker)

	// Without a recorded count (a main-build entry) nothing can tell them
	// apart, so the guarded behaviour applies: accepted. Documented limit.
	h2 := newHandlerWithPosition(t, positionWithHashAndColumns(legacyHash(imitating...), 0), SchemaDriftPolicyHalt)
	kind, _ = relate(ctx, t, h2, relMsg(current...), 200)
	is.Equal(kind, driftNone)
}

// A matching count is accepted, and every newly recorded entry carries the
// count.
func Test_LegacyHash_MatchingColumnCountAccepted_NewEntriesCarryCount(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	h := newHandlerWithPosition(t, positionWithHashAndColumns(legacyHash(shapeV1...), len(shapeV1)), SchemaDriftPolicyHalt)
	kind, marker := relate(ctx, t, h, relMsg(shapeV1...), 200)
	is.Equal(kind, driftNone)
	is.True(!marker)

	fresh := newHandlerWithPosition(t, position.Position{Type: position.TypeCDC}, SchemaDriftPolicyHalt)
	kind, _ = relate(ctx, t, fresh, relMsg(shapeV1...), 100)
	is.Equal(kind, driftInitial)
	is.Equal(fresh.basePosition.SchemaHistory["public.users"][0].Columns, len(shapeV1))

	// A shape accepted after a change is recorded with its count too.
	kind, _ = relate(ctx, t, fresh, relMsg(shapeV2...), 200)
	is.True(kind != driftNone)
	if fresh.driftMarkerPending() {
		// halt policy: the version is committed with the marker
		hist := fresh.basePosition.SchemaHistory["public.users"]
		is.Equal(hist[len(hist)-1].Columns, len(shapeV2))
	}
}
