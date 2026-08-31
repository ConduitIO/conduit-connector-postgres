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
	"fmt"
	"slices"

	"github.com/hamba/avro/v2"
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
		"jsonb":   avro.NewPrimitiveSchema(avro.Bytes, nil),
		"json":    avro.NewPrimitiveSchema(avro.Bytes, nil),
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

// TypeChangePreservesSchema reports whether a column whose DataType/TypeModifier
// pair changed from (oldType, oldTypeMod) to (newType, newTypeMod) still yields
// the same Avro schema — i.e. whether the change is invisible to a consumer that
// reads the Avro-encoded value. It is the evolve-policy compatibility judgement
// (Q2 in the DBZ-3 B1 design doc): a change that preserves the Avro shape is
// compatible and can be admitted automatically, one that doesn't must halt.
//
// The judgement mirrors how Extract derives schemas: OIDs that map to a fixed
// primitive (e.g. varchar/text -> String, ignoring TypeModifier) are preserved
// under length-only changes; numeric changes precision/scale and therefore
// changes the Avro decimal schema. Fail closed on anything the extractor cannot
// resolve: an unmapped type might differ in ways we cannot see.
func (a *avroExtractor) TypeChangePreservesSchema(oldType uint32, oldTypeMod int32, newType uint32, newTypeMod int32) bool {
	oldT, oldOK := a.pgMap.TypeForOID(oldType)
	newT, newOK := a.pgMap.TypeForOID(newType)
	if !oldOK || !newOK {
		return false
	}

	oldS, err := a.extractType(oldT, oldTypeMod)
	if err != nil {
		return false
	}
	newS, err := a.extractType(newT, newTypeMod)
	if err != nil {
		return false
	}

	return oldS.String() == newS.String()
}

func (a *avroExtractor) extractType(t *pgtype.Type, typeMod int32) (avro.Schema, error) {
	if ps, ok := a.avroMap[t.Name]; ok {
		return ps, nil
	}

	switch t.OID {
	case pgtype.NumericOID:
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
