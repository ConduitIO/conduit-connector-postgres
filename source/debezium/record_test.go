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

package debezium

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
	"pgregory.net/rapid"
)

var (
	tInt4    = &PGType{OID: 23, Name: pgInt4, Kind: 'b'}
	tText    = &PGType{OID: 25, Name: pgText, Kind: 'b'}
	tNumeric = &PGType{OID: 1700, Name: pgNumeric, Kind: 'b'}
	tBytea   = &PGType{OID: 17, Name: pgBytea, Kind: 'b'}
	tTextArr = &PGType{OID: 1009, Name: pgTextArray, Kind: 'b', Elem: tText}
	tTstz    = &PGType{OID: 1184, Name: pgTimestamptz, Kind: 'b'}
	tInt4Arr = &PGType{OID: 1007, Name: pgInt4Array, Kind: 'b', Elem: tInt4}
)

func testTypes() map[uint32]*PGType {
	m := map[uint32]*PGType{}
	for _, t := range []*PGType{tInt4, tText, tNumeric, tBytea, tTextArr, tTstz, tInt4Arr} {
		m[t.OID] = t
	}
	return m
}

func testSettings() Settings {
	s := DefaultSettings()
	s.Enabled = true
	s.TopicPrefix = "dbz"
	s.ProvideTransactionMetadata = true
	return s
}

// fixture is a builder with one table (id int4 PK, name text NOT NULL,
// qty int4 NOT NULL DEFAULT 7, note text, blob bytea, tags text[], at
// timestamptz NOT NULL DEFAULT now()) built without a catalog read.
func fixture(t *testing.T, identity uint8) (*Builder, *pglogrepl.RelationMessage) {
	t.Helper()
	rel := &pglogrepl.RelationMessage{
		RelationID: 1, Namespace: "public", RelationName: "t", ReplicaIdentity: identity,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: 23, TypeModifier: -1},
			{Name: pgName, DataType: 25, TypeModifier: -1},
			{Name: "qty", DataType: 23, TypeModifier: -1},
			{Name: "note", DataType: 25, TypeModifier: -1},
			{Name: "blob", DataType: 17, TypeModifier: -1},
			{Name: "tags", DataType: 1009, TypeModifier: -1},
			{Name: "at", DataType: 1184, TypeModifier: -1},
		},
	}
	cat := catalogTable{
		columns: []catalogColumn{
			{name: "id", typeOID: 23, notNull: true},
			{name: "name", typeOID: 25, notNull: true},
			{name: "qty", typeOID: 23, notNull: true, defaultX: "7"},
			{name: "note", typeOID: 25},
			{name: "blob", typeOID: 17},
			{name: "tags", typeOID: 1009},
			{name: "at", typeOID: 1184, notNull: true, defaultX: "now()"},
		},
		keyColumns: []string{"id"},
	}
	s := testSettings()
	tb, err := newTable(rel, cat, testTypes(), &s)
	if err != nil {
		t.Fatal(err)
	}
	if err := tb.checkIdentity(rel, cat); err != nil {
		t.Fatal(err)
	}
	b := NewBuilder(s, nil)
	b.Relation(rel)
	b.tables[rel.RelationID] = tb
	b.db = "db"
	return b, rel
}

func text(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(s)}
}

var (
	null      = &pglogrepl.TupleDataColumn{DataType: 'n'}
	unchanged = &pglogrepl.TupleDataColumn{DataType: 'u'}
)

func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols} //nolint:gosec // test sizes
}

func fullRow(id, name string) *pglogrepl.TupleData {
	return tuple(text(id), text(name), text("1"), null, text(`\x0102`), text(`{a,"b c",NULL}`), text("2024-03-15 08:11:12.5+00"))
}

func begin(b *Builder, xid uint32) {
	b.Begin(&pglogrepl.BeginMessage{Xid: xid, CommitTime: time.Date(2024, 1, 2, 3, 4, 5, 678901000, time.UTC)})
}

func TestBuilder_InsertCarriesEveryColumnAndTheSourceBlock(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	ctx := context.Background()
	begin(b, 700)
	rec, err := b.Insert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("1", "a")}, 0x100)
	is.NoErr(err)
	is.Equal(rec.Operation, opencdc.OperationCreate)
	is.Equal(rec.Key, opencdc.StructuredData{"id": int32(1)})
	is.Equal(rec.Payload.After, opencdc.StructuredData{
		"id": int32(1), "name": "a", "qty": int32(1), "note": nil, "blob": []byte{1, 2},
		"tags": []any{"a", "b c", nil}, "at": "2024-03-15T08:11:12.500000Z",
	})
	md := rec.Metadata
	is.Equal(md[MetaTopic], "dbz.public.t")
	is.Equal(md[MetaOp], "c")
	is.Equal(md[opencdc.MetadataCollection], "public.t")
	is.Equal(md[MetaSourcePfx+"version"], "3.7.0.Final")
	is.Equal(md[MetaSourcePfx+"connector"], "postgresql")
	is.Equal(md[MetaSourcePfx+"name"], "dbz")
	is.Equal(md[MetaSourcePfx+"snapshot"], "false")
	is.Equal(md[MetaSourcePfx+"db"], "db")
	is.Equal(md[MetaSourcePfx+"schema"], "public")
	is.Equal(md[MetaSourcePfx+"table"], "t")
	is.Equal(md[MetaSourcePfx+"txId"], "700")
	is.Equal(md[MetaSourcePfx+"lsn"], "256")
	is.Equal(md[MetaSourcePfx+"ts_ms"], "1704164645678")
	is.Equal(md[MetaSourcePfx+"ts_us"], "1704164645678901")
	is.Equal(md[MetaSourcePfx+"ts_ns"], "1704164645678901000")
	// No commit seen in this run yet: Debezium's last commit LSN is unknown.
	is.Equal(md[MetaSourcePfx+"sequence"], `[null,"256"]`)
	is.Equal(md[MetaTxID], "700:256")
	is.Equal(md[MetaTxTotal], "1")
	is.Equal(md[MetaTxCollOrder], "1")
	_, hasOldKey := md[MetaOldKey]
	is.True(!hasOldKey)
}

func TestBuilder_SequenceStartsAtThePreviousCommitEnd(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	ctx := context.Background()
	begin(b, 700)
	_, err := b.Insert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("1", "a")}, 0x100)
	is.NoErr(err)
	b.Commit(&pglogrepl.CommitMessage{CommitLSN: 0x180, TransactionEndLSN: 0x1a0})
	begin(b, 701)
	rec, err := b.Insert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("2", "b")}, 0x1b0)
	is.NoErr(err)
	is.Equal(rec.Metadata[MetaSourcePfx+"sequence"], `["416","432"]`) // 0x1a0, 0x1b0
	is.Equal(rec.Metadata[MetaTxTotal], "1")                          // counters restart per transaction
}

func TestBuilder_PrimaryKeyChangeCountsTwoEvents(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	ctx := context.Background()
	begin(b, 700)
	// DEFAULT identity: the old tuple is key-only ('K'); the other columns
	// are NULL in it.
	rec, err := b.Update(ctx, &pglogrepl.UpdateMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple: tuple(text("1"), null, null, null, null, null, null),
		NewTuple: fullRow("2", "a"),
	}, 0x100)
	is.NoErr(err)
	is.Equal(rec.Operation, opencdc.OperationUpdate)
	is.Equal(rec.Metadata[MetaOp], "u")
	is.Equal(rec.Metadata[MetaOldKey], `{"id":1}`)
	is.Equal(rec.Key, opencdc.StructuredData{"id": int32(2)})
	// D15: required columns the old tuple lacks hold the default or the
	// type's zero; optional ones are null.
	is.Equal(rec.Payload.Before, opencdc.StructuredData{
		"id": int32(1), "name": "", "qty": int32(7), "note": nil, "blob": nil, "tags": nil,
		"at": "1970-01-01T00:00:00.000000Z",
	})
	is.Equal(rec.Metadata[MetaTxTotal], "1")

	// The next change of the transaction is numbered after the delete and
	// the create the encoder makes of the key change.
	rec, err = b.Insert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("3", "c")}, 0x200)
	is.NoErr(err)
	is.Equal(rec.Metadata[MetaTxTotal], "3")
	is.Equal(rec.Metadata[MetaTxCollOrder], "3")
}

func TestBuilder_UpdateUnderDefaultIdentityHasNoBefore(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	begin(b, 700)
	rec, err := b.Update(context.Background(), &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: fullRow("1", "a")}, 0x100)
	is.NoErr(err)
	is.Equal(rec.Payload.Before, nil)
	_, hasOldKey := rec.Metadata[MetaOldKey]
	is.True(!hasOldKey)
}

func TestBuilder_UnchangedToast(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	newTuple := tuple(text("1"), unchanged, text("2"), unchanged, unchanged, unchanged, text("2024-03-15 08:11:12+00"))

	t.Run("placeholder per type", func(t *testing.T) {
		b, _ := fixture(t, 'd')
		begin(b, 700)
		rec, err := b.Update(ctx, &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: newTuple}, 0x100)
		is.NoErr(err)
		after := rec.Payload.After.(opencdc.StructuredData)
		p := "__debezium_unavailable_value"
		is.Equal(after["name"], p)
		is.Equal(after["note"], p)
		is.Equal(after["blob"], []byte(p))
		is.Equal(after["tags"], []any{p})
	})
	t.Run("backfilled from a full old row", func(t *testing.T) {
		b, _ := fixture(t, 'f')
		begin(b, 700)
		rec, err := b.Update(ctx, &pglogrepl.UpdateMessage{
			RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple: fullRow("1", "old"), NewTuple: newTuple,
		}, 0x100)
		is.NoErr(err)
		after := rec.Payload.After.(opencdc.StructuredData)
		is.Equal(after["name"], "old")
		is.Equal(after["blob"], []byte{1, 2})
		is.Equal(after["qty"], int32(2))
		is.Equal(rec.Payload.Before.(opencdc.StructuredData)["name"], "old") // FULL: full before
		_, hasOldKey := rec.Metadata[MetaOldKey]
		is.True(!hasOldKey) // same key: a plain update
	})
	t.Run("no placeholder form fails", func(*testing.T) {
		c, err := newColumn("n", tNumeric, -1, columnOptions{})
		is.NoErr(err)
		_, err = c.placeholderValue("x")
		is.Equal(CodeOf(err), CodeUnsupportedType)
		c, err = newColumn("a", tInt4Arr, -1, columnOptions{})
		is.NoErr(err)
		_, err = c.placeholderValue("x")
		is.Equal(CodeOf(err), CodeUnsupportedType)
	})
}

// postgres#425: Key, Before and After own their values, so editing After
// (as a processor does) changes neither of the others.
func TestBuilder_RecordPartsOwnTheirValues(t *testing.T) {
	is := is.New(t)
	b, rel := fixture(t, 'f')
	rel.Columns[0].DataType = 17 // a bytea key, so the key holds a slice
	b.Relation(rel)
	cat := catalogTable{columns: []catalogColumn{
		{name: "id", typeOID: 17, notNull: true}, {name: "name", typeOID: 25, notNull: true},
		{name: "qty", typeOID: 23, notNull: true}, {name: "note", typeOID: 25}, {name: "blob", typeOID: 17},
		{name: "tags", typeOID: 1009}, {name: "at", typeOID: 1184, notNull: true},
	}, keyColumns: []string{"id"}}
	tb, err := newTable(rel, cat, testTypes(), &b.s)
	is.NoErr(err)
	b.tables[1] = tb
	begin(b, 700)
	old := tuple(text(`\x0a0b`), text("n"), text("1"), null, text(`\x0102`), text(`{a,b}`), text("2024-03-15 08:11:12+00"))
	rec, err := b.Update(context.Background(), &pglogrepl.UpdateMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: old,
		NewTuple: tuple(text(`\x0a0b`), text("m"), text("2"), null, unchanged, unchanged, text("2024-03-15 08:11:12+00")),
	}, 0x100)
	is.NoErr(err)
	after := rec.Payload.After.(opencdc.StructuredData)
	after["blob"].([]byte)[0] = 0xEE
	after["tags"].([]any)[0] = "x"
	after["id"].([]byte)[0] = 0xEE
	before := rec.Payload.Before.(opencdc.StructuredData)
	is.Equal(before["blob"], []byte{1, 2})
	is.Equal(before["tags"], []any{"a", "b"})
	is.Equal(rec.Key, opencdc.StructuredData{"id": []byte{0x0a, 0x0b}})
}

func TestBuilder_DeleteNeedsTheOldKey(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	b, _ := fixture(t, 'd')
	begin(b, 700)

	rec, err := b.Delete(ctx, &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple: tuple(text("5"), null, null, null, null, null, null)}, 0x100)
	is.NoErr(err)
	is.Equal(rec.Operation, opencdc.OperationDelete)
	is.Equal(rec.Key, opencdc.StructuredData{"id": int32(5)})
	is.Equal(rec.Payload.After, nil)

	// An old tuple without the key value (identity changed under the
	// pipeline): refused, never a delete for a null key (invariant 3).
	_, err = b.Delete(ctx, &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple: tuple(null, null, null, null, null, null, null)}, 0x200)
	is.Equal(CodeOf(err), CodeDeleteKeyUnavailable)
	_, err = b.Delete(ctx, &pglogrepl.DeleteMessage{RelationID: 1}, 0x300)
	is.Equal(CodeOf(err), CodeDeleteKeyUnavailable)
}

func TestBuilder_ChangeOutsideATransactionFails(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	_, err := b.Insert(context.Background(), &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("1", "a")}, 0x100)
	is.True(err != nil)
}

func TestBuilder_TupleShapeMismatchIsCatalogSkew(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	begin(b, 700)
	_, err := b.Insert(context.Background(), &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(text("1"))}, 0x100)
	is.Equal(CodeOf(err), CodeCatalogSkew)
}

func TestBuilder_TransactionMetadataOff(t *testing.T) {
	is := is.New(t)
	b, _ := fixture(t, 'd')
	b.s.ProvideTransactionMetadata = false
	begin(b, 700)
	rec, err := b.Insert(context.Background(), &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("1", "a")}, 0x100)
	is.NoErr(err)
	for _, k := range []string{MetaTxID, MetaTxTotal, MetaTxCollOrder} {
		_, ok := rec.Metadata[k]
		is.True(!ok)
	}
}

func TestBuilder_RelationDropsTheBuiltShape(t *testing.T) {
	is := is.New(t)
	b, rel := fixture(t, 'd')
	b.Relation(rel)
	_, ok := b.tables[rel.RelationID]
	is.True(!ok) // the next change reads the catalog again
}

func TestCheckIdentity_IndexWithoutTheKey(t *testing.T) {
	is := is.New(t)
	rel := &pglogrepl.RelationMessage{
		RelationID: 1, Namespace: "public", RelationName: "t", ReplicaIdentity: 'i',
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23, TypeModifier: -1},
			{Flags: 1, Name: pgName, DataType: 25, TypeModifier: -1},
		},
	}
	cat := catalogTable{
		columns:    []catalogColumn{{name: "id", typeOID: 23, notNull: true}, {name: "name", typeOID: 25, notNull: true}},
		keyColumns: []string{"id"},
	}
	s := testSettings()
	tb, err := newTable(rel, cat, testTypes(), &s)
	is.NoErr(err)
	is.Equal(CodeOf(tb.checkIdentity(rel, cat)), CodeReplicaIdentityIndexNotKey)

	rel.Columns[0].Flags = 1 // the index contains the key
	is.NoErr(tb.checkIdentity(rel, cat))
}

func TestKeyJSON(t *testing.T) {
	is := is.New(t)
	types := map[uint32]*PGType{1700: tNumeric, 25: tText}
	mk := func(typmod int32) *table {
		rel := &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "k",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "n", DataType: 1700, TypeModifier: typmod},
				{Name: "s", DataType: 25, TypeModifier: -1},
			}}
		cat := catalogTable{columns: []catalogColumn{{name: "n", typeOID: 1700, notNull: true}, {name: "s", typeOID: 25, notNull: true}},
			keyColumns: []string{"s", "n"}}
		s := testSettings()
		tb, err := newTable(rel, cat, types, &s)
		is.NoErr(err)
		return tb
	}
	// numeric(10,2): a JSON number with its scale (decimal.format=NUMERIC),
	// in key order, not column order.
	got, err := mk((10<<16 | 2) + 4).keyJSON(opencdc.StructuredData{"n": "1.50", "s": `a"b`})
	is.NoErr(err)
	is.Equal(got, `{"s":"a\"b","n":1.50}`)
	// Unconstrained numeric: VariableScaleDecimal's struct, unscaled bytes in
	// two's complement.
	got, err = mk(-1).keyJSON(opencdc.StructuredData{"n": "-1.29", "s": "x"})
	is.NoErr(err)
	is.Equal(got, `{"s":"x","n":{"scale":2,"value":"/38="}}`) // -129: ff 7f
}

func TestTwosComplement(t *testing.T) {
	is := is.New(t)
	for text, want := range map[string][]byte{
		"0": {0x00}, "1": {0x01}, "127": {0x7f}, "128": {0x00, 0x80}, "-1": {0xff},
		"-128": {0x80}, "-129": {0xff, 0x7f}, "255": {0x00, 0xff}, "-256": {0xff, 0x00},
	} {
		n, _, ok := unscale(text)
		is.True(ok)
		is.Equal(twosComplement(n), want) // Java's BigInteger.toByteArray
	}
}

// A restart inside a transaction skips the changes delivered before it; the
// changes after them keep the orders the uninterrupted run gives them.
func TestBuilder_SkippedChangesKeepTheNumbering(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	b, _ := fixture(t, 'd')
	begin(b, 700)
	is.NoErr(b.Skipped(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("1", "a")}, 0x100))
	// A skipped primary-key change is two events.
	is.NoErr(b.Skipped(ctx, &pglogrepl.UpdateMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple: tuple(text("1"), null, null, null, null, null, null), NewTuple: fullRow("2", "a"),
	}, 0x200))
	// A skipped plain update under FULL is one.
	is.NoErr(b.Skipped(ctx, &pglogrepl.UpdateMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: fullRow("2", "a"), NewTuple: fullRow("2", "b"),
	}, 0x300))
	is.NoErr(b.Skipped(ctx, &pglogrepl.TruncateMessage{RelationIDs: []uint32{1}}, 0x350)) // not an event
	rec, err := b.Insert(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: fullRow("3", "c")}, 0x400)
	is.NoErr(err)
	is.Equal(rec.Metadata[MetaTxTotal], "5")
	is.Equal(rec.Metadata[MetaTxCollOrder], "5")
}

// Property: the VariableScaleDecimal bytes are Java's BigInteger.toByteArray:
// minimal, and they read back as the same signed integer.
func TestTwosComplementProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		digits := rapid.StringMatching(`-?[0-9]{1,60}`).Draw(t, "n")
		n, _, ok := unscale(digits)
		if !ok {
			t.Fatalf("unscale %q", digits)
		}
		b := twosComplement(n)
		back := new(big.Int).SetBytes(b)
		if b[0]&0x80 != 0 {
			back.Sub(back, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
		}
		if back.Cmp(n) != 0 {
			t.Fatalf("%s: read back %s", n, back)
		}
		// Minimal: dropping the first byte would change the sign or value.
		if len(b) > 1 && ((b[0] == 0 && b[1]&0x80 == 0) || (b[0] == 0xff && b[1]&0x80 != 0)) {
			t.Fatalf("%s: % x is not minimal", n, b)
		}
	})
}
