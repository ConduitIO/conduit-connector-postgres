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
//   - uuid is its text form.
//
// Everything else is returned as decoded.
func FormatFor(enc Encoding, oid uint32, v any) (any, error) {
	if oid == pgtype.UUIDOID {
		return UUID.Format(v)
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
