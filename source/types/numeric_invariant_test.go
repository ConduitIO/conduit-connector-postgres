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

package types

// These tests use only API that predates the numeric fix, so they can be run
// against the code before it and fail there.

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func numericCases(t *testing.T) []struct {
	name string
	in   any
	// text is set for NaN and the infinities.
	text string
	// null is set for NULL.
	null bool
	rat  *big.Rat
} {
	t.Helper()
	nan := pgtype.Numeric{NaN: true, Valid: true}
	inf := pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}
	negInf := pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	return []struct {
		name string
		in   any
		text string
		null bool
		rat  *big.Rat
	}{
		{name: "NaN", in: nan, text: "NaN"},
		{name: "NaN pointer", in: &nan, text: "NaN"},
		{name: "NaN from text", in: pgxNumeric(t, "NaN"), text: "NaN"},
		{name: "Infinity", in: inf, text: "Infinity"},
		{name: "Infinity pointer", in: &inf, text: "Infinity"},
		{name: "Infinity from text", in: pgxNumeric(t, "Infinity"), text: "Infinity"},
		{name: "-Infinity", in: negInf, text: "-Infinity"},
		{name: "-Infinity pointer", in: &negInf, text: "-Infinity"},
		{name: "-Infinity from text", in: pgxNumeric(t, "-Infinity"), text: "-Infinity"},
		{name: "untyped nil", in: nil, null: true},
		{name: "invalid numeric", in: pgtype.Numeric{}, null: true},
		{name: "invalid numeric pointer", in: &pgtype.Numeric{}, null: true},
		{name: "nil numeric pointer", in: (*pgtype.Numeric)(nil), null: true},
		{name: "zero", in: pgxNumeric(t, "0"), rat: big.NewRat(0, 1)},
		{name: "finite", in: pgxNumeric(t, "1.50"), rat: big.NewRat(3, 2)},
		{name: "negative", in: pgxNumeric(t, "-12.2121"), rat: big.NewRat(-122121, 10000)},
	}
}

// TestFormat_NeverTypedNil pins the invariant behind #368 for every input and
// both encodings: whatever FormatFor returns, it is either an untyped nil or a
// non-nil value. A typed nil would pass for a value and crash the encoder.
func TestFormat_NeverTypedNil(t *testing.T) {
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, tc := range numericCases(t) {
			got, _ := FormatFor(enc, pgtype.NumericOID, tc.in)
			if got == nil {
				continue
			}
			if rv := reflect.ValueOf(got); rv.Kind() == reflect.Pointer && rv.IsNil() {
				t.Errorf("%s/%s: FormatFor returned a typed nil %T", encName(enc), tc.name, got)
			}
		}
	}
	// Format is FormatFor with EncodingNative.
	got, err := Format(pgtype.NumericOID, pgtype.Numeric{NaN: true, Valid: true})
	is := is.New(t)
	is.NoErr(err)
	is.Equal(got, "NaN")
}

func TestBigRatFromNumeric_NoNilWithoutError(t *testing.T) {
	for _, tc := range numericCases(t) {
		var n pgtype.Numeric
		switch v := tc.in.(type) {
		case pgtype.Numeric:
			n = v
		case *pgtype.Numeric:
			if v == nil {
				continue
			}
			n = *v
		default:
			continue
		}
		r, err := Numeric.BigRatFromNumeric(n)
		if r == nil && err == nil {
			t.Errorf("%s: BigRatFromNumeric returned a nil *big.Rat and no error", tc.name)
		}
	}
}

func encName(e Encoding) string {
	if e == EncodingAvro {
		return "avro"
	}
	return "native"
}
