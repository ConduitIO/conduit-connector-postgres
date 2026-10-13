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
	"strings"
)

// maxTruncationDigits is the number of fractional digits kept for a rational
// whose decimal expansion does not terminate (1/3). Postgres allows a column
// scale of at most 1000, so keeping 1001 digits means the first dropped digit
// can never matter to a column's rounding (see RatToDecimalString).
const maxTruncationDigits = 1001

var (
	bigTwo  = big.NewInt(2)
	bigFive = big.NewInt(5)
	bigTen  = big.NewInt(10)

	// pow10Truncation is 10^maxTruncationDigits, computed once.
	pow10Truncation = new(big.Int).Exp(bigTen, big.NewInt(maxTruncationDigits), nil)
)

// RatToDecimalString renders r as a decimal string for a Postgres NUMERIC
// parameter without ever applying a column scale.
//
// The connector deliberately does not look up the column's scale. Postgres
// applies the column's typmod when a value is assigned (rounding half away
// from zero, or "numeric field overflow" if precision is exceeded), always
// against the live definition of the table the statement resolves. Any
// client-side lookup is stale the moment a concurrent ALTER COLUMN commits
// between the lookup and the batch, and was previously also cached for the
// life of the process.
//
// Invariant 6: no silent coercion. A rational whose denominator has only the
// prime factors 2 and 5 has a terminating decimal expansion and is sent
// exactly. Any other rational (1/3) is truncated toward zero at
// maxTruncationDigits digits. That is equivalent to Postgres rounding the
// exact value: a rounding boundary of a column with scale s is a decimal with
// s+1 digits, the expansion is non-terminating so it never equals a boundary,
// and truncation at more than s+1 digits keeps it on the same side of every
// boundary.
func RatToDecimalString(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}

	digits, terminates := terminatingDigits(r.Denom())
	if terminates {
		return r.FloatString(digits) // exact: no rounding occurs
	}
	return truncatedString(r, maxTruncationDigits)
}

// terminatingDigits reports whether 1/den has a terminating decimal expansion
// and, if so, how many fractional digits it needs.
func terminatingDigits(den *big.Int) (int, bool) {
	d := new(big.Int).Set(den)
	var twos, fives int
	m := new(big.Int)
	for {
		q, rem := new(big.Int).QuoRem(d, bigTwo, m)
		if rem.Sign() != 0 {
			break
		}
		d, twos = q, twos+1
	}
	for {
		q, rem := new(big.Int).QuoRem(d, bigFive, m)
		if rem.Sign() != 0 {
			break
		}
		d, fives = q, fives+1
	}
	if d.Cmp(big.NewInt(1)) != 0 {
		return 0, false
	}
	return max(twos, fives), true
}

// truncatedString renders r truncated toward zero at n fractional digits.
func truncatedString(r *big.Rat, n int) string {
	abs := new(big.Int).Abs(r.Num())
	pow := pow10Truncation
	if n != maxTruncationDigits {
		pow = new(big.Int).Exp(bigTen, big.NewInt(int64(n)), nil)
	}
	scaled := abs.Mul(abs, pow)
	scaled.Quo(scaled, r.Denom())

	s := scaled.String()
	if len(s) <= n {
		s = strings.Repeat("0", n-len(s)+1) + s
	}

	var sb strings.Builder
	sb.Grow(len(s) + 2)
	if r.Sign() < 0 {
		sb.WriteByte('-')
	}
	sb.WriteString(s[:len(s)-n])
	sb.WriteByte('.')
	sb.WriteString(s[len(s)-n:])
	return sb.String()
}
