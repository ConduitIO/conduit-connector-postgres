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

package internal

import (
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestRatToDecimalString(t *testing.T) {
	tests := []struct {
		name string
		in   *big.Rat
		want string // "" means: only the properties below are checked
	}{
		{"zero", big.NewRat(0, 1), "0"},
		{"integer", big.NewRat(-7, 1), "-7"},
		{"terminating", big.NewRat(123, 100), "1.23"},
		{"terminating negative", big.NewRat(-5, 2), "-2.5"},
		{"one eighth", big.NewRat(1, 8), "0.125"},
		{"small terminating", big.NewRat(49999, 10000000), "0.0049999"},
		{"one third", big.NewRat(1, 3), ""},
		{"negative two thirds", big.NewRat(-2, 3), ""},
		{"one seventh", big.NewRat(1, 7), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			got := RatToDecimalString(tt.in)
			if tt.want != "" {
				is.Equal(got, tt.want)
			}
			back, ok := new(big.Rat).SetString(got)
			is.True(ok) // the output must parse as a decimal
			if tt.in.Denom().BitLen() > 0 {
				if _, terminates := terminatingDigits(tt.in.Denom()); terminates {
					is.Equal(back.Cmp(tt.in), 0) // terminating values are exact
				} else {
					// truncated toward zero, by less than 10^-maxTruncationDigits
					diff := new(big.Rat).Sub(new(big.Rat).Abs(tt.in), new(big.Rat).Abs(back))
					is.True(diff.Sign() > 0)
					limit := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Exp(bigTen, big.NewInt(maxTruncationDigits), nil))
					is.True(diff.Cmp(limit) < 0)
					is.Equal(len(got[strings.IndexByte(got, '.')+1:]), maxTruncationDigits)
				}
			}
		})
	}
}

// TestRatToDecimalString_RoundingEquivalence checks the property the connector
// relies on: rounding RatToDecimalString's output to any column scale (what
// Postgres does on assignment) gives the same result as rounding the exact
// rational directly. The oracle below is independent of the implementation.
func TestRatToDecimalString_RoundingEquivalence(t *testing.T) {
	is := is.New(t)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic seed for a reproducible test, not security-sensitive
	scales := []int{0, 1, 2, 3, 6, 18, 100, 1000}

	check := func(r *big.Rat) {
		out := RatToDecimalString(r)
		back, ok := new(big.Rat).SetString(out)
		is.True(ok)
		for _, s := range scales {
			want := roundHalfAwayFromZero(r, s)
			got := roundHalfAwayFromZero(back, s)
			if want.Cmp(got) != 0 {
				t.Fatalf("scale %d: rounding %s via %q gave %s, exact rounding gives %s", s, r, shorten(out), got.RatString(), want.RatString())
			}
		}
	}

	// Adversarial: values just below, on and above rounding boundaries, with
	// and without terminating expansions.
	for _, s := range scales[:6] {
		boundary := new(big.Rat).SetFrac(big.NewInt(5), new(big.Int).Exp(bigTen, big.NewInt(int64(s+1)), nil))
		for _, d := range []int64{-1000003, -1, 0, 1, 1000003} {
			// boundary + d / (3 * 10^(s+30)): non-terminating offsets
			off := new(big.Rat).SetFrac(big.NewInt(d), new(big.Int).Mul(big.NewInt(3), new(big.Int).Exp(bigTen, big.NewInt(int64(s+30)), nil)))
			check(new(big.Rat).Add(boundary, off))
			check(new(big.Rat).Neg(new(big.Rat).Add(boundary, off)))
		}
	}
	for i := 0; i < 3000; i++ {
		num := rng.Int63n(1<<40) - 1<<39
		den := rng.Int63n(1<<20) + 1
		check(big.NewRat(num, den))
	}
}

// roundHalfAwayFromZero rounds r to s fractional digits, as Postgres does for
// numeric(p,s) assignment, and returns the result as a rational.
func roundHalfAwayFromZero(r *big.Rat, s int) *big.Rat {
	pow := new(big.Int).Exp(bigTen, big.NewInt(int64(s)), nil)
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(pow))
	neg := scaled.Sign() < 0
	scaled.Abs(scaled)
	// floor(scaled + 1/2)
	scaled.Add(scaled, big.NewRat(1, 2))
	q := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	if neg {
		q.Neg(q)
	}
	return new(big.Rat).SetFrac(q, pow)
}

func shorten(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

func BenchmarkRatToDecimalString(b *testing.B) {
	b.Run("terminating", func(b *testing.B) {
		r := big.NewRat(123456789, 1000000)
		b.ReportAllocs()
		for b.Loop() {
			_ = RatToDecimalString(r)
		}
	})
	b.Run("non-terminating", func(b *testing.B) {
		r := big.NewRat(1, 3)
		b.ReportAllocs()
		for b.Loop() {
			_ = RatToDecimalString(r)
		}
	})
}
