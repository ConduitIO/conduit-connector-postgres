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

// maxExactDigits is the most fractional digits Postgres accepts in a NUMERIC
// input (NUMERIC_MAX_DISPLAY_SCALE). A terminating value that needs more is
// rejected with 22003 on every column, constrained or not, so it is truncated
// like a non-terminating one.
const maxExactDigits = 16383

var (
	bigFive = big.NewInt(5)
	bigTen  = big.NewInt(10)

	// maxExactDenominatorBits bounds the denominator of a value that can be sent
	// exactly: 2^a * 5^b with a, b <= maxExactDigits is at most 10^maxExactDigits.
	maxExactDenominatorBits = new(big.Int).Exp(bigTen, big.NewInt(maxExactDigits), nil).BitLen()

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
// exactly, up to maxExactDigits fractional digits. Any other rational (1/3), and
// a terminating one that needs more than maxExactDigits (Postgres rejects it),
// is truncated toward zero at maxTruncationDigits digits. That is equivalent to
// Postgres rounding the exact value: a rounding boundary of a column with scale
// s is a decimal with s+1 <= 1001 digits. The value never equals a boundary
// (a non-terminating expansion has no finite form, and a terminating one with
// more than maxExactDigits digits has more digits than any boundary), and
// truncation at 1001 digits keeps it on the same side of every boundary.
func RatToDecimalString(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}

	digits, terminates := terminatingDigits(r.Denom())
	if terminates && digits <= maxExactDigits {
		return r.FloatString(digits) // exact: no rounding occurs
	}
	return truncatedString(r, maxTruncationDigits)
}

// terminatingDigits reports whether 1/den has a terminating decimal expansion
// of at most maxExactDigits fractional digits and, if so, how many digits it
// needs. It reports false for a longer expansion without computing it, so the
// cost is bounded by maxExactDigits regardless of how large den is (1/5^200000
// used to take seconds).
func terminatingDigits(den *big.Int) (int, bool) {
	if den.BitLen() > maxExactDenominatorBits {
		return 0, false
	}
	d := new(big.Int).Set(den)
	twos := int(d.TrailingZeroBits())
	if twos > maxExactDigits {
		return 0, false
	}
	d.Rsh(d, uint(twos))
	var fives int
	m := new(big.Int)
	for {
		q, rem := new(big.Int).QuoRem(d, bigFive, m)
		if rem.Sign() != 0 {
			break
		}
		d, fives = q, fives+1
		if fives > maxExactDigits {
			return 0, false
		}
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
