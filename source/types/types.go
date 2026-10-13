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

package types

import (
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	Numeric = NumericFormatter{}
	UUID    = UUIDFormatter{}
)

// Encoding tells FormatFor how the formatted values are going to be
// serialized. It only changes the Go representation of values the serializer
// could not otherwise represent.
type Encoding int

const (
	// EncodingNative is for values that leave the connector as plain Go values
	// (JSON, protobuf struct), i.e. the connector does not attach an Avro
	// schema to the record.
	EncodingNative Encoding = iota
	// EncodingAvro is for values that the SDK encodes with the Avro schema
	// produced by source/schema (withAvroSchema).
	EncodingAvro
)

// EncodingFor returns the encoding for a source whose withAvroSchema option has
// the given value.
func EncodingFor(withAvroSchema bool) Encoding {
	if withAvroSchema {
		return EncodingAvro
	}
	return EncodingNative
}

// Format normalizes a value decoded by pgx into the representation the
// connector emits, using EncodingNative. See FormatFor.
func Format(oid uint32, v any) (any, error) {
	return FormatFor(EncodingNative, oid, v)
}

// FormatFor normalizes a value decoded by pgx into the representation the
// connector emits:
//
//   - numeric is a *big.Rat; NaN, Infinity and -Infinity are the strings "NaN",
//     "Infinity" and "-Infinity" (EncodingNative) or a
//     *NumericNotRepresentableError (EncodingAvro, whose decimal cannot hold
//     them). NULL is always the untyped nil, never a typed nil *big.Rat.
//   - date, timestamp and timestamptz 'infinity' and '-infinity' are the strings
//     "infinity" and "-infinity" (EncodingNative) or a
//     *TemporalNotRepresentableError (EncodingAvro, which has no such value).
//     Finite values are returned as decoded.
//   - the same holds per element of date[], timestamp[], timestamptz[] and
//     numeric[]: an infinite or NaN element is a string (EncodingNative) or an
//     error (EncodingAvro). An array without such an element is returned as
//     decoded, so finite arrays are unchanged.
//   - uuid is its text form.
//
// Everything else is returned as decoded.
func FormatFor(enc Encoding, oid uint32, v any) (any, error) {
	if oid == pgtype.UUIDOID {
		return UUID.Format(v)
	}

	if elem, ok := arrayElementOIDs[oid]; ok {
		return formatArray(enc, elem, v)
	}

	switch t := v.(type) {
	case pgtype.InfinityModifier:
		// Only date and timestamps decode to a bare InfinityModifier; every
		// other type's output is unchanged.
		if typ, ok := temporalTypeNames[oid]; ok {
			return formatInfinity(enc, typ, t)
		}
		return t, nil
	case pgtype.Numeric:
		return formatNumeric(enc, t)
	case *pgtype.Numeric:
		if t == nil {
			return nil, nil //nolint:nilnil // a nil pointer from pgx is NULL
		}
		return formatNumeric(enc, *t)
	case []uint8:
		if oid == pgtype.XMLOID {
			return string(t), nil
		}
		return t, nil
	default:
		return t, nil
	}
}

var arrayElementOIDs = map[uint32]uint32{
	pgtype.DateArrayOID:        pgtype.DateOID,
	pgtype.TimestampArrayOID:   pgtype.TimestampOID,
	pgtype.TimestamptzArrayOID: pgtype.TimestamptzOID,
	pgtype.NumericArrayOID:     pgtype.NumericOID,
}

// formatArray applies the scalar rules for non-finite values to each element of
// a decoded array. The slice is copied only when an element changes, and finite
// elements keep the exact value pgx decoded.
func formatArray(enc Encoding, elemOID uint32, v any) (any, error) {
	arr, ok := v.([]any)
	if !ok {
		return v, nil
	}
	var out []any
	for i, e := range arr {
		var (
			nv  any
			err error
		)
		switch t := e.(type) {
		case pgtype.InfinityModifier:
			typ, known := temporalTypeNames[elemOID]
			if !known {
				continue
			}
			nv, err = formatInfinity(enc, typ, t)
		case pgtype.Numeric:
			if _, nonFin := nonFinite(t); !nonFin {
				continue
			}
			nv, err = formatNumeric(enc, t)
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = make([]any, len(arr))
			copy(out, arr)
		}
		out[i] = nv
	}
	if out == nil {
		return v, nil
	}
	return out, nil
}
