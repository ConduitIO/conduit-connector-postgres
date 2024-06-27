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
	avroNS = "conduit.postgres"
	// The default decimal precision is pretty generous, but it is in excess of what
	// pgx provides by default. All numeric values by default are coded to float64/int64.
	// Ideally in the future the decimal precision can be adjusted to fit the definition in postgres.
	avroDecimalPrecision = 38
	// The size of the storage in which a decimal may be encoded depends on the underlying numeric definition.
	// Unfortunately similarly to the decimal precision, this is dependent on the size of the numeric, which
	// by default is constraint to 8 bytes. This default is generously allocating four times larger width.
	avroDecimalFixedSize = 8 * 4
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

func (a avroExtractor) ExtractLogrepl(rel *pglogrepl.RelationMessage, row *pglogrepl.TupleData) (avro.Schema, error) {
	var fields []pgconn.FieldDescription

	for i := range row.Columns {
		fields = append(fields, pgconn.FieldDescription{
			Name:         rel.Columns[i].Name,
			DataTypeOID:  rel.Columns[i].DataType,
			TypeModifier: rel.Columns[i].TypeModifier,
		})
	}

	return a.Extract(rel.RelationName, fields)
}

func (a *avroExtractor) Extract(name string, fields []pgconn.FieldDescription) (avro.Schema, error) {
	var avroFields []*avro.Field

	for _, f := range fields {
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

	sch, err := avro.NewRecordSchema(name, avroNS, avroFields)
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
			string(avro.Decimal),
			avroNS,
			avroDecimalFixedSize,
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

