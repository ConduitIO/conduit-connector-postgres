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
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TestFormatFor_Numeric is the regression test for #368: NaN and +/-Infinity
// used to come out of Format as a typed-nil *big.Rat, which is not nil, so the
// Avro encoder dereferenced it. Invariant 6: they must never become NULL or a
// number.
func TestFormatFor_Numeric(t *testing.T) {
	for _, enc := range []Encoding{EncodingNative, EncodingAvro} {
		for _, tc := range numericCases(t) {
			t.Run(encName(enc)+"/"+tc.name, func(t *testing.T) {
				is := is.New(t)
				got, err := FormatFor(enc, pgtype.NumericOID, tc.in)

				switch {
				case tc.null:
					is.NoErr(err)
					is.True(got == nil) // NULL is the untyped nil
				case tc.text != "" && enc == EncodingAvro:
					var nre *NumericNotRepresentableError
					is.True(errors.As(err, &nre)) // Avro cannot hold it: halt
					is.Equal(nre.Value, tc.text)
					is.True(got == nil)
					is.True(strings.HasPrefix(err.Error(), ErrorCodeNumericNotRepresentable))
				case tc.text != "":
					is.NoErr(err)
					is.Equal(got, tc.text) // canonical text, not NULL, not 0
				default:
					is.NoErr(err)
					r, ok := got.(*big.Rat)
					is.True(ok)
					is.True(r != nil)
					is.Equal(r.Cmp(tc.rat), 0)
				}
			})
		}
	}
}

func TestNumericNotRepresentableError(t *testing.T) {
	is := is.New(t)

	_, err := FormatFor(EncodingAvro, pgtype.NumericOID, pgtype.Numeric{NaN: true, Valid: true})
	located, ok := Locate(err, "public.orders", "total")
	located = WithRow(located, "id=7")
	is.True(ok)
	msg := located.Error()
	is.True(strings.HasPrefix(msg, ErrorCodeNumericNotRepresentable+": ")) // code first
	is.True(strings.Contains(msg, `"NaN"`))                                // the value
	is.True(strings.Contains(msg, `"total"`))
	is.True(strings.Contains(msg, "(row id=7)"))                                                            // the column
	is.True(strings.Contains(msg, `"public.orders"`))                                                       // the table
	is.True(strings.Contains(msg, "restart once with logrepl.withAvroSchema=false from the same position")) // CDC fix
	is.True(strings.Contains(msg, "set it back to true as soon as that record has been delivered"))         // switching back is required
	is.True(strings.Contains(msg, "#378"))
	is.True(strings.Contains(msg, "UPDATE it")) // snapshot fix
	is.True(strings.Contains(msg, "do not drop the replication slot"))
	is.True(strings.Contains(msg, "does not help once the change is in the WAL"))
	is.True(!strings.Contains(msg, "CHECK"))
	is.Equal(ErrorCodeNumericNotRepresentable, "postgres.source.numeric_not_representable")

	// The original is left alone and wrapped errors are found.
	var orig *NumericNotRepresentableError
	is.True(errors.As(err, &orig))
	is.Equal(orig.Table, "")
	wrapped := errors.Join(errors.New("outer"), err)
	_, ok = Locate(wrapped, "t", "c")
	is.True(ok)

	// Other errors are not touched.
	other := errors.New("boom")
	got, ok := Locate(other, "t", "c")
	is.True(!ok)
	is.Equal(got, other)
}
