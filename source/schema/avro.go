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
	"math"
	"slices"

	"github.com/hamba/avro/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	avroNS = "conduit.postgres"
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
			avro.Int,
			avro.NewPrimitiveLogicalSchema(avro.UUID),
		),
	},
}

type avroExtractor struct {
	pgMap   *pgtype.Map
	avroMap map[string]*avro.PrimitiveSchema
}

func (a *avroExtractor) Extract(name string, fields []pgconn.FieldDescription, values []any) (avro.Schema, error) {
	var avroFields []*avro.Field

	for i, f := range fields {
		t, ok := a.pgMap.TypeForOID(f.DataTypeOID)
		if !ok {
			return nil, fmt.Errorf("field %q with OID %d cannot be resolved", f.Name, f.DataTypeOID)
		}

		ps, err := a.extractType(t, values[i])
		if err != nil {
			return nil, err
		}

		af, err := avro.NewField(f.Name, ps)
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

func (a *avroExtractor) extractType(t *pgtype.Type, val any) (*avro.PrimitiveSchema, error) {
	if ps, ok := a.avroMap[t.Name]; ok {
		return ps, nil
	}

	switch tt := val.(type) {
	case pgtype.Numeric:
		// N.B.: Default to 38 positions and pick the exponent as the scale.
		return avro.NewPrimitiveSchema(
			avro.Fixed,
			avro.NewDecimalLogicalSchema(38, int(math.Abs(float64(tt.Exp)))),
		), nil
	default:
		return nil, fmt.Errorf("cannot resolve field %q of type %T", t.Name, tt)
	}
}
