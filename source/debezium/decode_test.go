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

package debezium

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matryer/is"
	"pgregory.net/rapid"
)

func col(t *testing.T, name string, typmod int32) *column {
	t.Helper()
	types := map[string]*PGType{
		pgInt2: {Name: pgInt2}, pgInt4: {Name: pgInt4}, pgInt8: {Name: pgInt8}, pgOid: {Name: pgOid},
		pgFloat4: {Name: pgFloat4}, pgFloat8: {Name: pgFloat8}, pgBool: {Name: pgBool}, pgText: {Name: pgText},
		pgNumeric: {Name: pgNumeric}, pgMoney: {Name: pgMoney}, pgDate: {Name: pgDate}, pgTime: {Name: pgTime},
		pgTimetz: {Name: pgTimetz}, pgTimestamp: {Name: pgTimestamp}, pgTimestamptz: {Name: pgTimestamptz},
		pgInterval: {Name: pgInterval}, pgBytea: {Name: pgBytea}, pgHstore: {Name: pgHstore},
		pgInt4Array: {Name: pgInt4Array, Elem: &PGType{Name: pgInt4}},
		pgTextArray: {Name: pgTextArray, Elem: &PGType{Name: pgText}},
	}
	c, err := newColumn("c", types[name], typmod, columnOptions{moneyFractionDigits: 2})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The cases are the golden workload's edge values (testdata/debezium) with
// what Debezium 2.7.4 and 3.7.0 wrote for them, plus the boundaries the
// workload does not reach.
func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		typ    string
		typmod int32
		in     string
		want   any
	}{
		{pgInt2, -1, "-32768", int16(-32768)},
		{pgInt8, -1, "-9223372036854775808", int64(math.MinInt64)},
		{pgOid, -1, "4294967295", int64(4294967295)},
		{pgFloat4, -1, "NaN", float32(math.NaN())},
		{pgFloat8, -1, "Infinity", math.Inf(1)},
		{pgFloat8, -1, "-Infinity", math.Inf(-1)},
		{pgFloat8, -1, "4.9e-324", 4.9e-324},
		{pgBool, -1, "t", true},
		{pgNumeric, (12<<16 | 4) + 4, "5.0000", "5.0000"},
		{pgNumeric, -1, "123456789012345678901234567890.123456789", "123456789012345678901234567890.123456789"},
		{pgMoney, -1, "-$5.00", "-5.00"},
		{pgMoney, -1, "$1,234.56", "1234.56"},
		{pgDate, -1, "2024-02-29", int32(19782)},
		{pgDate, -1, "1970-01-01", int32(0)},
		{pgDate, -1, "0001-01-01", int32(-719162)}, // was -106752: a Duration saturates at ~292 years
		{pgDate, -1, "0001-12-31 BC", int32(-719163)},
		{pgDate, -1, "5874897-12-31", int32(2145042905)}, // Postgres' last date (civil-from-days)
		{pgTime, -1, "23:59:59.999999", int64(86399999999)},
		{pgTime, 0, "00:00:01", int32(1000)},
		{pgTime, 3, "12:00:00.5", int32(43200500)},
		{pgTime, -1, "24:00:00", int64(86400000000)},
		{pgTimetz, -1, "13:14:15.5+05:30", "07:44:15.5Z"},
		{pgTimetz, -1, "23:59:59.999999-12", "11:59:59.999999Z"},
		{pgTimetz, -1, "00:00:00+00", "00:00:00Z"},
		{pgTimestamp, -1, "2262-04-11 23:47:16.854775", int64(9223372036854775)},
		{pgTimestamp, 3, "1677-09-21 00:12:43.146", int64(-9223372036854)},
		{pgTimestamp, 3, "1970-01-01 00:00:00.999", int64(999)},
		{pgTimestamp, -1, "1969-12-31 23:59:59.999999", int64(-1)},
		{pgTimestamptz, -1, "2024-03-15 08:11:12.5+00", "2024-03-15T08:11:12.500000Z"},
		{pgTimestamptz, -1, "1970-01-01 00:00:00+00", "1970-01-01T00:00:00.000000Z"},
		{pgTimestamptz, -1, "2024-02-29 13:14:15.123456-08", "2024-02-29T21:14:15.123456Z"},
		{pgTimestamptz, -1, "0044-03-15 12:00:00+00 BC", "-0043-03-15T12:00:00.000000Z"},
		{pgInterval, -1, "1 year 2 mons 3 days 04:05:06.789", int64(37091106789000)},
		{pgInterval, -1, "-1 days -00:00:01", int64(-86401000000)},
		{pgInterval, -1, "-178000000 years", int64(math.MinInt64)}, // Java's saturating (long) cast
		{pgBytea, -1, `\x00ff10`, []byte{0, 0xff, 0x10}},
		{pgBytea, -1, `\x`, []byte{}},
		{pgHstore, -1, `"k1"=>"v1", "k2"=>NULL`, `{"k1":"v1","k2":null}`},
		{pgHstore, -1, ``, `{}`},
		{pgInt4Array, -1, "{-1,NULL,2147483647}", []any{int32(-1), nil, int32(2147483647)}},
		{pgTextArray, -1, `{"","Unicode é中",NULL,"NULL"}`, []any{"", "Unicode é中", nil, "NULL"}},
		{pgTextArray, -1, `{}`, []any{}},
	} {
		t.Run(fmt.Sprintf("%s(%d) %s", tc.typ, tc.typmod, tc.in), func(t *testing.T) {
			is := is.New(t)
			got, err := col(t, tc.typ, tc.typmod).decode(tc.in)
			is.NoErr(err)
			if w, ok := tc.want.(float32); ok && math.IsNaN(float64(w)) {
				is.True(math.IsNaN(float64(got.(float32))))
				return
			}
			is.Equal(got, tc.want)
		})
	}
}

// Values Debezium's precise and adaptive modes cannot carry fail with a
// code; they are never rounded or replaced (invariant 6).
func TestDecode_Refused(t *testing.T) {
	for _, tc := range []struct {
		typ  string
		in   string
		code Code
	}{
		{pgNumeric, "NaN", CodeValueOutOfRange},
		{pgNumeric, "Infinity", CodeValueOutOfRange},
		{pgNumeric, "-Infinity", CodeValueOutOfRange},
		{pgInt4Array, "{{1,2},{3,4}}", CodeUnsupportedType},
	} {
		t.Run(tc.typ+" "+tc.in, func(t *testing.T) {
			is := is.New(t)
			_, err := col(t, tc.typ, -1).decode(tc.in)
			is.Equal(CodeOf(err), tc.code)
		})
	}
}

// Infinite, BC and five-digit-year temporal values, with what Debezium wrote
// for them in a capture of the golden workload extended with them
// (testdata/debezium/sql/03-temporal.sql; the events are in the
// golden/<version>/*/temporal.jsonl files). The infinity sentinels differ
// between the versions.
func TestDecode_TemporalEdges(t *testing.T) {
	type tc struct {
		typ    string
		typmod int32
		in     string
		v26    any
		v35    any
	}
	for _, c := range []tc{
		{pgTimestamp, -1, "infinity", int64(9223372036825200000), int64(math.MaxInt64)},
		{pgTimestamp, -1, "-infinity", int64(-9223372036832400000), int64(math.MinInt64)},
		{pgTimestamp, 3, "infinity", int64(9223372036825200), int64(9223372036825200000)},
		{pgTimestamp, 3, "-infinity", int64(-9223372036832400), int64(-9223372036832400000)},
		{pgTimestamptz, -1, "infinity", "infinity", "infinity"},
		{pgTimestamptz, -1, "-infinity", "-infinity", "-infinity"},
		{pgDate, -1, "infinity", int32(-2147472692), int32(2146033979)},
		{pgDate, -1, "-infinity", int32(-2147472691), int32(-2147472691)},
		{pgTimestamp, -1, "0044-03-15 12:00:00.123456 BC", int64(-63517780799876544), int64(-63517780799876544)},
		{pgTimestamp, 3, "0044-03-15 12:00:00.123 BC", int64(-63517780799877), int64(-63517780799877)},
		{pgTimestamptz, -1, "0044-03-15 12:00:00.123456+00 BC", "-0043-03-15T12:00:00.123456Z", "-0043-03-15T12:00:00.123456Z"},
		{pgDate, -1, "0044-03-15 BC", int32(-735160), int32(-735160)},
		{pgTimestamp, -1, "10000-01-01 00:00:00.5", int64(253402300800500000), int64(253402300800500000)},
		{pgTimestamp, 3, "10000-01-01 00:00:00.5", int64(253402300800500), int64(253402300800500)},
		{pgTimestamptz, -1, "10000-01-01 00:00:00.5+00", "+10000-01-01T00:00:00.500000Z", "+10000-01-01T00:00:00.500000Z"},
		{pgDate, -1, "10000-01-01", int32(2932897), int32(2932897)},
	} {
		for compat, want := range map[string]any{Compat26: c.v26, Compat35: c.v35} {
			t.Run(fmt.Sprintf("%s %s(%d) %s", compat, c.typ, c.typmod, c.in), func(t *testing.T) {
				is := is.New(t)
				col, err := newColumn("c", &PGType{Name: c.typ}, c.typmod, columnOptions{compat: compat})
				is.NoErr(err)
				got, err := col.decode(c.in)
				is.NoErr(err)
				is.Equal(got, want)
			})
		}
	}
}

func TestDecode_MoneyScale(t *testing.T) {
	is := is.New(t)
	c := col(t, pgMoney, -1)
	c.scale = 4
	got, err := c.decode("$1.50")
	is.NoErr(err)
	is.Equal(got, "1.5000")
	c.scale = 0
	got, err = c.decode("$12.00")
	is.NoErr(err)
	is.Equal(got, "12")
	_, err = c.decode("$12.50")
	is.Equal(CodeOf(err), CodeValueOutOfRange) // never drops a digit
}

// Property: a timestamp's text, as Postgres writes it with the pinned
// session settings, decodes to the microseconds it stands for, in both the
// zoned and the plain form.
func TestDecode_TimestampProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		lo := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
		hi := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()
		us := rapid.Int64Range(lo, hi).Draw(t, "us")
		tm := time.UnixMicro(us).UTC()
		text := tm.Format("2006-01-02 15:04:05.999999")

		c, err := newColumn("c", &PGType{Name: pgTimestamp}, -1, columnOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.decode(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if got != us {
			t.Fatalf("%s: got %v, want %d", text, got, us)
		}

		z, err := newColumn("c", &PGType{Name: pgTimestamptz}, -1, columnOptions{})
		if err != nil {
			t.Fatal(err)
		}
		zt, err := z.decode(text + "+00")
		if err != nil {
			t.Fatal(err)
		}
		if want := tm.Format("2006-01-02T15:04:05.000000Z"); zt != want {
			t.Fatalf("%s: got %v, want %s", text, zt, want)
		}

		d, err := newColumn("c", &PGType{Name: pgDate}, -1, columnOptions{})
		if err != nil {
			t.Fatal(err)
		}
		days, err := d.decode(tm.Format("2006-01-02"))
		if err != nil {
			t.Fatal(err)
		}
		if want := int32(floorDiv(us, 86_400_000_000)); days != want { //nolint:gosec // years 1 to 9999 fit
			t.Fatalf("%s: got %v days, want %d", text, days, want)
		}
	})
}
