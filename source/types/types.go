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
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	Numeric = NumericFormatter{}
	UUID    = UUIDFormatter{}
)

// Encoding tells Format how the formatted values are going to be serialized.
// It only changes the Go representation of values for which the serializer
// would otherwise produce host-dependent or wrong output; the logical value
// is the same either way.
type Encoding int

const (
	// EncodingNative is for values that leave the connector as plain Go values
	// (JSON, protobuf struct), i.e. the connector does not attach an Avro
	// schema to the record.
	EncodingNative Encoding = iota
	// EncodingAvro is for values that the SDK encodes with the Avro schema
	// produced by source/schema (logrepl.withAvroSchema, snapshot.withAvroSchema).
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
// connector emits. The output must not depend on the host time zone or on the
// pgx version that decoded the value:
//
//   - timestamp (without time zone) keeps its wall clock and is labeled UTC
//     (EncodingNative), or is an int64 of microseconds of that wall clock since
//     1970-01-01T00:00:00 (EncodingAvro, see formatTimestampAvro for why).
//   - timestamptz is always in UTC.
//   - date is midnight UTC of the calendar date.
//   - tsvector is its canonical text form.
//   - infinity and -infinity are the strings "infinity" and "-infinity"
//     (EncodingNative) or an error (EncodingAvro, which cannot represent them).
//   - the bounds of tsrange, tstzrange, daterange and their multiranges and
//     arrays get the same treatment (always the native form).
//   - numeric is a *big.Rat; NaN, Infinity and -Infinity are the strings "NaN",
//     "Infinity" and "-Infinity" (EncodingNative) or a
//     *NumericNotRepresentableError (EncodingAvro, whose decimal cannot hold
//     them). NULL is always the untyped nil, never a typed nil *big.Rat.
//   - numeric[] elements that are NaN, Infinity or -Infinity follow the same
//     rule (a string, or an error under EncodingAvro). Every other element,
//     NULLs included, is the pgtype.Numeric pgx decoded, so a numeric[] without
//     such an element is emitted exactly as before.
//   - arrays of the above are normalized element by element.
//
// Native values that time.Time cannot serialize to JSON (years before 0000 or
// after 9999) are emitted as the Postgres text form of the value.
func FormatFor(enc Encoding, oid uint32, v any) (any, error) {
	got, err := formatFor(enc, oid, v)
	if err != nil {
		return nil, err
	}
	// A nil pointer inside an interface is not nil: it passes `v == nil` checks
	// and the Avro encoder dereferences it (#368). A nil pointer decoded from
	// Postgres means NULL, so make that the untyped nil, whatever the type.
	if rv := reflect.ValueOf(got); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, nil //nolint:nilnil // NULL is an untyped nil record value
	}
	return got, nil
}

func formatFor(enc Encoding, oid uint32, v any) (any, error) {
	if oid == pgtype.UUIDOID {
		return UUID.Format(v)
	}

	if kind, ok := temporalKinds[oid]; ok {
		return formatTemporal(enc, kind, v)
	}
	if kind, ok := temporalArrayKinds[oid]; ok {
		return formatTemporalArray(enc, kind, v)
	}
	if kind, ok := rangeKinds[oid]; ok {
		return formatRange(kind, v)
	}
	if kind, ok := rangeArrayKinds[oid]; ok {
		return formatRangeArray(kind, v)
	}
	if oid == pgtype.NumericArrayOID {
		return formatNumericArray(enc, v)
	}
	switch oid {
	case pgtype.TSVectorOID:
		return formatTSVector(v)
	case pgtype.TSVectorArrayOID:
		return formatTSVectorArray(v)
	}

	switch t := v.(type) {
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

func unexpectedType(oid string, v any) error {
	return fmt.Errorf("unexpected %T for a %s value", v, oid)
}
