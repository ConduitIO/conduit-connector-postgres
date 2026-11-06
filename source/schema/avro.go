// Copyright © 2024 Meroxa, Inc.
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

package schema

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/iskorotkov/avro/v2"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var Avro = &avroExtractor{
	pgMap: pgtype.NewMap(),
	avroMap: map[string]*avro.PrimitiveSchema{
		"bool":    avro.NewPrimitiveSchema(avro.Boolean, nil),
		"bytea":   avro.NewPrimitiveSchema(avro.Bytes, nil),
		"float4":  avro.NewPrimitiveSchema(avro.Float, nil),
		"float8":  avro.NewPrimitiveSchema(avro.Double, nil),
		"int8":    avro.NewPrimitiveSchema(avro.Long, nil),
		"int4":    avro.NewPrimitiveSchema(avro.Int, nil),
		"int2":    avro.NewPrimitiveSchema(avro.Int, nil),
		"text":    avro.NewPrimitiveSchema(avro.String, nil),
		"varchar": avro.NewPrimitiveSchema(avro.String, nil),
		// json and jsonb are the exact JSON text, as Debezium emits them: an
		// Avro string carrying the Kafka Connect semantic type name. Before
		// v0.21.0 they were bytes. See jsonSchema.
		"jsonb": jsonSchema(),
		"json":  jsonSchema(),
		// tsvector is emitted as its canonical text form, see source/types.
		"tsvector": avro.NewPrimitiveSchema(avro.String, nil),
		"timestamptz": avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
		),
		"timestamp": avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.LocalTimestampMicros),
		),
		"date": avro.NewPrimitiveSchema(
			avro.Int,
			avro.NewPrimitiveLogicalSchema(avro.Date),
		),
		"uuid": avro.NewPrimitiveSchema(
			avro.String,
			avro.NewPrimitiveLogicalSchema(avro.UUID),
		),
	},
}

// DebeziumJSONName is the Kafka Connect schema name Debezium gives json and
// jsonb columns (io.debezium.data.Json, version 1). Confluent's Avro converter
// writes a named Connect schema as the Avro properties connect.name and
// connect.version on the field's type, so Debezium's json field type is
// {"type":"string","connect.name":"io.debezium.data.Json","connect.version":1};
// the source emits the same. Consumers that look for the name (Debezium
// SerDes, sinks that map it to a JSON column) see a json column.
const DebeziumJSONName = "io.debezium.data.Json"

// jsonSchema is the Avro schema of a json or jsonb value. It is a plain
// string: the connect.* properties are not part of the Avro type, schema
// resolution and compatibility checks ignore them, and the Parsing Canonical
// Form (and so the fingerprint) is the one of "string". They only reach a
// registry through Marshal, never through Schema.String.
func jsonSchema() *avro.PrimitiveSchema {
	return avro.NewPrimitiveSchema(avro.String, nil, avro.WithProps(map[string]any{
		"connect.name":    DebeziumJSONName,
		"connect.version": 1,
	}))
}

// Marshal returns the text of sch that the source registers. It is the full
// JSON form, with properties, not sch.String(): String is the Parsing
// Canonical Form, which drops every property, including the connect.name of
// json and jsonb fields. For a schema without properties the two are the same
// text (TestMarshal_EqualsStringWithoutProps).
func Marshal(sch avro.Schema) ([]byte, error) {
	b, err := json.Marshal(sch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal avro schema %q: %w", sch.String(), err)
	}
	return b, nil
}

type avroExtractor struct {
	pgMap   *pgtype.Map
	avroMap map[string]*avro.PrimitiveSchema
}

// ExtractLogrepl extracts an Avro schema from the given pglogrepl.RelationMessage.
// If `fieldNames` are specified, then only the given fields will be included in the schema.
func (a avroExtractor) ExtractLogrepl(schemaName string, rel *pglogrepl.RelationMessage, fieldNames ...string) (*avro.RecordSchema, error) {
	var fields []pgconn.FieldDescription

	for i := range rel.Columns {
		fields = append(fields, pgconn.FieldDescription{
			Name:         rel.Columns[i].Name,
			DataTypeOID:  rel.Columns[i].DataType,
			TypeModifier: rel.Columns[i].TypeModifier,
		})
	}

	return a.Extract(schemaName, fields, fieldNames...)
}

// Extract extracts an Avro schema from the given Postgres field descriptions.
// If `fieldNames` are specified, then only the given fields will be included in the schema.
func (a *avroExtractor) Extract(schemaName string, fields []pgconn.FieldDescription, fieldNames ...string) (*avro.RecordSchema, error) {
	var avroFields []*avro.Field

	for _, f := range fields {
		if len(fieldNames) > 0 && !slices.Contains(fieldNames, f.Name) {
			continue
		}

		t, ok := a.pgMap.TypeForOID(f.DataTypeOID)
		if !ok {
			return nil, fmt.Errorf("field %q with OID %d cannot be resolved", f.Name, f.DataTypeOID)
		}

		s, err := a.extractType(t, f.TypeModifier)
		if err != nil {
			return nil, err
		}

		af, err := avro.NewField(f.Name, s)
		if err != nil {
			return nil, fmt.Errorf("failed to create avro field %q: %w", f.Name, err)
		}

		avroFields = append(avroFields, af)
	}

	slices.SortFunc(avroFields, func(a, b *avro.Field) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	sch, err := avro.NewRecordSchema(schemaName, "", avroFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro schema: %w", err)
	}

	return sch, nil
}

// TypeChangePreservesValues reports whether a column whose DataType/TypeModifier
// pair changed from (oldType, oldTypeMod) to (newType, newTypeMod) still yields
// the same value for a consumer, for a source whose withAvroSchema option has
// the given value. It is the evolve-policy compatibility judgement (Q2 in the
// DBZ-3 B1 design doc): a change that preserves the shape is compatible and can
// be admitted automatically, one that doesn't must halt.
//
// With Avro on the judgement is whether the Avro schema is unchanged. It mirrors
// how Extract derives schemas: OIDs that map to a fixed primitive (e.g.
// varchar/text -> String, ignoring TypeModifier) are preserved under length-only
// changes; a constrained numeric changes precision/scale and therefore the Avro
// decimal schema. Without an Avro schema the value a consumer sees is the Go
// value, and an unconstrained numeric is a big.Rat there, not the string it is
// under Avro: a change between it and text/varchar changes the value's type, so
// it is not preserved. A change between json/jsonb and any other type is never
// preserved (see typeChangePreserves). Fail closed on anything the extractor
// cannot resolve: an unmapped type might differ in ways we cannot see.
func (a *avroExtractor) TypeChangePreservesValues(withAvroSchema bool, oldType uint32, oldTypeMod int32, newType uint32, newTypeMod int32) bool {
	oldT, oldOK := a.pgMap.TypeForOID(oldType)
	newT, newOK := a.pgMap.TypeForOID(newType)
	if !oldOK || !newOK {
		return false
	}
	return a.typeChangePreserves(withAvroSchema, oldT, oldTypeMod, newT, newTypeMod)
}

func (a *avroExtractor) typeChangePreserves(withAvroSchema bool, oldT *pgtype.Type, oldTypeMod int32, newT *pgtype.Type, newTypeMod int32) bool {
	// json and jsonb are strings, like text, varchar and an unconstrained
	// numeric, but a consumer parses them as JSON: a change between JSON and
	// any other type changes what the string means even though the Avro
	// schema does not change, so it is not preserved. json <-> jsonb is.
	if isJSON(oldT.OID) != isJSON(newT.OID) {
		return false
	}
	oldS, err := a.extractTypeFor(withAvroSchema, oldT, oldTypeMod)
	if err != nil {
		return false
	}
	newS, err := a.extractTypeFor(withAvroSchema, newT, newTypeMod)
	if err != nil {
		return false
	}

	return oldS.String() == newS.String()
}

func isJSON(oid uint32) bool {
	return oid == pgtype.JSONOID || oid == pgtype.JSONBOID
}

// extractTypeFor is extractType for the drift judgement. Without an Avro schema
// an unconstrained numeric keeps the shape it had before it became a string,
// so that it stays different from text and varchar (see
// TypeChangePreservesValues).
func (a *avroExtractor) extractTypeFor(withAvroSchema bool, t *pgtype.Type, typeMod int32) (avro.Schema, error) {
	if !withAvroSchema && t.OID == pgtype.NumericOID && typeMod == -1 {
		return avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(65535, 65531)), nil
	}
	return a.extractType(t, typeMod)
}

func (a *avroExtractor) extractType(t *pgtype.Type, typeMod int32) (avro.Schema, error) {
	if ps, ok := a.avroMap[t.Name]; ok {
		return ps, nil
	}

	switch t.OID {
	case pgtype.NumericOID:
		if typeMod == -1 {
			// An unconstrained numeric has no precision or scale, and any value
			// Postgres can store (up to 131072 digits before and 16383 after the
			// point, plus NaN and the infinities) is a valid one. No Avro
			// decimal(p,s) can hold them all with a usable encoding, so it is
			// the exact text of the value. See the design note on #375.
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		scale := int((typeMod - 4) & 65535)
		precision := int(((typeMod - 4) >> 16) & 65535)
		return avro.NewPrimitiveSchema(
			avro.Bytes,
			avro.NewDecimalLogicalSchema(precision, scale),
		), nil
	default:
		return nil, fmt.Errorf("cannot resolve field type %q ", t.Name)
	}
}
