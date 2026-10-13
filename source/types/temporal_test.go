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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func TestFormatFor_TemporalInfinity(t *testing.T) {
	oids := map[string]uint32{"date": pgtype.DateOID, "timestamp": pgtype.TimestampOID, "timestamptz": pgtype.TimestamptzOID}
	mods := map[string]pgtype.InfinityModifier{"infinity": pgtype.Infinity, "-infinity": pgtype.NegativeInfinity}

	for typ, oid := range oids {
		for text, m := range mods {
			t.Run(typ+"/"+text, func(t *testing.T) {
				is := is.New(t)

				got, err := FormatFor(EncodingNative, oid, m)
				is.NoErr(err)
				is.Equal(got, text)

				got, err = FormatFor(EncodingAvro, oid, m)
				is.True(got == nil)
				var tre *TemporalNotRepresentableError
				is.True(errors.As(err, &tre))
				is.Equal(tre.Value, text)
				is.Equal(tre.Type, typ)

				located, ok := Locate(err, "public.events", "day")
				is.True(ok)
				msg := located.Error()
				is.True(strings.HasPrefix(msg, ErrorCodeTemporalNotRepresentable+": "))
				is.True(strings.Contains(msg, `"public.events"`))
				is.True(strings.Contains(msg, `"day"`))
				is.True(strings.Contains(msg, text))
				is.True(strings.Contains(msg, "logrepl.withAvroSchema"))
			})
		}
	}
	is.New(t).Equal(ErrorCodeTemporalNotRepresentable, "postgres.source.temporal_not_representable")
}

// No other type's output changes: finite values pass through as decoded, and
// an InfinityModifier under any other OID is not touched.
func TestFormatFor_TemporalUnchanged(t *testing.T) {
	is := is.New(t)
	now := time.Now()
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, oid := range []uint32{pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TextOID} {
			got, err := FormatFor(enc, oid, now)
			is.NoErr(err)
			is.Equal(got, now)
		}
		got, err := FormatFor(enc, pgtype.Int2OID, pgtype.Infinity)
		is.NoErr(err)
		is.Equal(got, pgtype.Infinity)
	}
}

func TestFormatFor_ArrayNonFinite(t *testing.T) {
	d := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		oid   uint32
		in    []any
		code  string
		nativ []any
	}{
		{"date", pgtype.DateArrayOID, []any{d, pgtype.Infinity}, ErrorCodeTemporalNotRepresentable, []any{d, "infinity"}},
		{"timestamp", pgtype.TimestampArrayOID, []any{pgtype.NegativeInfinity, nil}, ErrorCodeTemporalNotRepresentable, []any{"-infinity", nil}},
		{"timestamptz", pgtype.TimestamptzArrayOID, []any{d, pgtype.NegativeInfinity}, ErrorCodeTemporalNotRepresentable, []any{d, "-infinity"}},
		{"numeric NaN", pgtype.NumericArrayOID, []any{pgtype.Numeric{NaN: true, Valid: true}}, ErrorCodeNumericNotRepresentable, []any{"NaN"}},
		{"numeric Infinity", pgtype.NumericArrayOID, []any{pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}}, ErrorCodeNumericNotRepresentable, []any{"Infinity"}},
		{"numeric -Infinity", pgtype.NumericArrayOID, []any{pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}}, ErrorCodeNumericNotRepresentable, []any{"-Infinity"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			orig := append([]any(nil), tc.in...)

			got, err := FormatFor(EncodingNative, tc.oid, tc.in)
			is.NoErr(err)
			is.Equal(got, tc.nativ)
			is.Equal(tc.in, orig) // the decoded slice is not modified

			got, err = FormatFor(EncodingAvro, tc.oid, tc.in)
			is.True(got == nil)
			is.True(err != nil)
			is.True(strings.HasPrefix(err.Error(), tc.code))
		})
	}
}

// Arrays without a non-finite element are returned as decoded, not copied.
func TestFormatFor_ArrayFiniteUnchanged(t *testing.T) {
	is := is.New(t)
	d := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for oid, in := range map[uint32][]any{
			pgtype.DateArrayOID:    {d, nil},
			pgtype.NumericArrayOID: {pgxNumericVal("1.5"), nil},
			pgtype.TextArrayOID:    {"a", pgtype.Infinity},
		} {
			got, err := FormatFor(enc, oid, in)
			is.NoErr(err)
			is.True(&got.([]any)[0] == &in[0]) // same backing array
		}
	}
}

func pgxNumericVal(s string) pgtype.Numeric {
	var n pgtype.Numeric
	_ = n.Scan(s)
	return n
}
