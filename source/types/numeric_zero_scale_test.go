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

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// binaryNumeric builds the bytes Postgres sends for a numeric in binary format:
// ndigits, weight, sign and dscale (16 bits each), then the base-10000 digits.
func binaryNumeric(ndigits, weight, sign, dscale uint16, digits ...uint16) []byte {
	b := []byte{}
	for _, x := range append([]uint16{ndigits, weight, sign, dscale}, digits...) {
		b = binary.BigEndian.AppendUint16(b, x)
	}
	return b
}

// TestWithZeroDisplayScale is the unit regression test for #391. It decodes
// real binary numerics with pgx, the way the snapshot does, and checks the text
// the connector emits for them against the text Postgres prints.
func TestWithZeroDisplayScale(t *testing.T) {
	m := pgtype.NewMap()

	cases := []struct {
		name string
		raw  []byte
		want string // Postgres' text for the value
	}{
		{"0", binaryNumeric(0, 0, 0, 0), "0"},
		{"0.0", binaryNumeric(0, 0, 0, 1), "0.0"},
		{"0.000", binaryNumeric(0, 0, 0, 3), "0.000"},
		{"0.00000", binaryNumeric(0, 0, 0, 5), "0.00000"},
		{"dscale above one digit group", binaryNumeric(0, 0, 0, 9), "0.000000000"},
		{"max dscale", binaryNumeric(0, 0, 0, 16383), "0." + strings.Repeat("0", 16383)},
		{"1.50", binaryNumeric(2, 0, 0, 2, 1, 5000), "1.50"},
		{"100.00", binaryNumeric(1, 0, 0, 2, 100), "100.00"},
		{"-0.5", binaryNumeric(1, 0xFFFF, 0x4000, 1, 5000), "-0.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			var n pgtype.Numeric
			is.NoErr(m.Scan(pgtype.NumericOID, pgtype.BinaryFormatCode, tc.raw, &n))
			fixed := WithZeroDisplayScale(tc.raw, n)

			got, err := FormatColumn(EncodingAvro, pgtype.NumericOID, -1, fixed)
			is.NoErr(err)
			is.Equal(got, tc.want)

			// Without Avro the value is a big.Rat: the same number as ever.
			rat, err := FormatFor(EncodingNative, pgtype.NumericOID, fixed)
			is.NoErr(err)
			want, ok := new(big.Rat).SetString(tc.want)
			is.True(ok)
			is.Equal(rat.(*big.Rat).Cmp(want), 0)
		})
	}
}

// WithZeroDisplayScale changes nothing but a finite zero with a display scale,
// and never panics or changes a value on input that is not what it expects.
func TestWithZeroDisplayScale_LeavesOtherValuesAlone(t *testing.T) {
	zero := pgtype.Numeric{Int: big.NewInt(0), Valid: true}
	zeroRaw := binaryNumeric(0, 0, 0, 3)

	cases := []struct {
		name string
		raw  []byte
		in   any
	}{
		{"NULL value", zeroRaw, pgtype.Numeric{}},
		{"nil", zeroRaw, nil},
		{"NaN", binaryNumeric(0, 0, 0xC000, 0), pgtype.Numeric{NaN: true, Valid: true}},
		{"Infinity", binaryNumeric(0, 0, 0xD000, 0), pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}},
		{"non-zero", binaryNumeric(1, 0, 0, 0, 7), pgtype.Numeric{Int: big.NewInt(7), Valid: true}},
		{"decoded zero but non-zero on the wire", binaryNumeric(1, 0, 0, 3, 7), zero},
		{"short raw", []byte{0, 0, 0}, zero},
		{"nil raw", nil, zero},
		{"zero dscale", binaryNumeric(0, 0, 0, 0), zero},
		{"negative dscale", binaryNumeric(0, 0, 0, 0xFFFF), zero},
		{"dscale above the maximum", binaryNumeric(0, 0, 0, 16384), zero},
		{"not a numeric", zeroRaw, "0"},
		{"pointer is not rewritten", zeroRaw, &zero},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(fmt.Sprint(WithZeroDisplayScale(tc.raw, tc.in)), fmt.Sprint(tc.in))
		})
	}
}

// BenchmarkWithZeroDisplayScale measures the per-value cost the snapshot pays
// for each numeric column of each row. The snapshot must stay within 10% of its
// previous throughput; a finite non-zero value is the common case.
func BenchmarkWithZeroDisplayScale(b *testing.B) {
	nonZero := pgtype.Numeric{Int: big.NewInt(150), Exp: -2, Valid: true}
	nonZeroRaw := binaryNumeric(2, 0, 0, 2, 1, 5000)
	zero := pgtype.Numeric{Int: big.NewInt(0), Valid: true}
	zeroRaw := binaryNumeric(0, 0, 0, 3)

	b.Run("non-zero", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = WithZeroDisplayScale(nonZeroRaw, nonZero)
		}
	})
	b.Run("zero", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = WithZeroDisplayScale(zeroRaw, zero)
		}
	})
}
