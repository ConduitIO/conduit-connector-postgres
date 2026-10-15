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
	"fmt"
	"math"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TestFormatFor_FloatNonFinite is the unit regression test for #376. Without
// Avro, a float4/float8 NaN or infinity reached the payload as a Go float, which
// JSON cannot hold: opencdc.Record.Bytes panicked, and the pipeline replayed the
// row after every restart. They are now the strings "NaN", "Infinity" and
// "-Infinity", like a non-finite numeric (#368). Under Avro the float is
// unchanged: Avro float and double can hold them.
func TestFormatFor_FloatNonFinite(t *testing.T) {
	cases := []struct {
		name string
		in   any
		text string // set for NaN and the infinities
	}{
		{"float8 NaN", math.NaN(), "NaN"},
		{"float8 Infinity", math.Inf(1), "Infinity"},
		{"float8 -Infinity", math.Inf(-1), "-Infinity"},
		{"float4 NaN", float32(math.NaN()), "NaN"},
		{"float4 Infinity", float32(math.Inf(1)), "Infinity"},
		{"float4 -Infinity", float32(math.Inf(-1)), "-Infinity"},
		{"float8 finite", 1.5, ""},
		{"float8 max", math.MaxFloat64, ""},
		{"float8 negative zero", math.Copysign(0, -1), ""},
		{"float4 finite", float32(1.5), ""},
		{"float4 max", float32(math.MaxFloat32), ""},
	}

	for _, oid := range []uint32{pgtype.Float4OID, pgtype.Float8OID, 0} {
		for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("oid=%d/%s/%s", oid, encName(enc), tc.name), func(t *testing.T) {
					is := is.New(t)
					got, err := FormatFor(enc, oid, tc.in)
					is.NoErr(err)

					if tc.text != "" && enc == EncodingNative {
						is.Equal(got, tc.text) // canonical text, never a Go NaN or infinity
					} else {
						// Finite values keep their Go type; under Avro so do NaN
						// and the infinities.
						is.Equal(fmt.Sprintf("%T:%v", got, got), fmt.Sprintf("%T:%v", tc.in, tc.in))
					}

					// The record the SDK would serialize.
					rec := opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"v": got}}}
					if enc == EncodingNative {
						is.True(len(rec.Bytes()) > 0) // Bytes panics on a Go NaN or infinity
					}
				})
			}
		}
	}

	t.Run("NULL", func(t *testing.T) {
		for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
			got, err := FormatFor(enc, pgtype.Float8OID, nil)
			is.New(t).NoErr(err)
			is.New(t).True(got == nil)
		}
	})
}

func TestFormatFor_FloatArrayNonFinite(t *testing.T) {
	in := func() any {
		return []any{math.NaN(), 1.5, nil, math.Inf(1), math.Inf(-1)}
	}
	in4 := func() any {
		return []any{float32(math.NaN()), float32(1.5), nil, float32(math.Inf(1)), float32(math.Inf(-1))}
	}

	t.Run("native float8[]", func(t *testing.T) {
		is := is.New(t)
		got, err := FormatFor(EncodingNative, pgtype.Float8ArrayOID, in())
		is.NoErr(err)
		is.Equal(got, []any{"NaN", 1.5, nil, "Infinity", "-Infinity"})
		rec := opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"v": got}}}
		is.True(len(rec.Bytes()) > 0)
	})
	t.Run("native float4[]", func(t *testing.T) {
		is := is.New(t)
		got, err := FormatFor(EncodingNative, pgtype.Float4ArrayOID, in4())
		is.NoErr(err)
		is.Equal(got, []any{"NaN", float32(1.5), nil, "Infinity", "-Infinity"})
		rec := opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"v": got}}}
		is.True(len(rec.Bytes()) > 0)
	})
	t.Run("avro is unchanged", func(t *testing.T) {
		is := is.New(t)
		got, err := FormatFor(EncodingAvro, pgtype.Float8ArrayOID, in())
		is.NoErr(err)
		a, ok := got.([]any)
		is.True(ok)
		is.Equal(len(a), 5)
		is.True(math.IsNaN(a[0].(float64)))
		is.True(math.IsInf(a[3].(float64), 1))
		is.True(math.IsInf(a[4].(float64), -1))
	})
	t.Run("NULL array and empty array", func(t *testing.T) {
		is := is.New(t)
		got, err := FormatFor(EncodingNative, pgtype.Float8ArrayOID, nil)
		is.NoErr(err)
		is.True(got == nil)
		got, err = FormatFor(EncodingNative, pgtype.Float4ArrayOID, []any{})
		is.NoErr(err)
		is.Equal(got, []any{})
	})
	t.Run("finite array is unchanged", func(t *testing.T) {
		is := is.New(t)
		for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
			got, err := FormatFor(enc, pgtype.Float8ArrayOID, []any{1.5, nil, -2.0})
			is.NoErr(err)
			is.Equal(got, []any{1.5, nil, -2.0})
		}
	})
}
