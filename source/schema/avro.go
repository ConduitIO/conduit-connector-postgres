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

const (
	avroNS             = ""
	avroDecimalPadding = 8
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

	sch, err := avro.NewRecordSchema(schemaName, avroNS, avroFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro schema: %w", err)
	}

	return sch, nil
}

func (a *avroExtractor) extractType(t *pgtype.Type, typeMod int32) (avro.Schema, error) {
	if ps, ok := a.avroMap[t.Name]; ok {
		return ps, nil
	}

	switch t.OID {
	case pgtype.NumericOID:
		scale := int((typeMod - 4) & 65535)
		precision := int(((typeMod - 4) >> 16) & 65535)
		fs, err := avro.NewFixedSchema(
			fmt.Sprintf("%s_%d_%d", avro.Decimal, precision, scale),
			avroNS,
			precision+scale+avroDecimalPadding,
			avro.NewDecimalLogicalSchema(precision, scale),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create avro.FixedSchema: %w", err)
		}
		return fs, nil
	default:
		return nil, fmt.Errorf("cannot resolve field type %q ", t.Name)
	}
}
