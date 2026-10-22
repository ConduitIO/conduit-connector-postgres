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

package types_test

import (
	"errors"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TestGoldenFormat covers the values the golden row cannot carry because
// time.Time cannot be serialized (or the Avro encoder cannot hold them).
func TestGoldenFormat(t *testing.T) {
	// The wall clock 03:04:05.123456 in every zone pgx (any version) might have
	// labeled a timestamp with.
	label := func(loc *time.Location) time.Time {
		return time.Date(2024, 1, 2, 3, 4, 5, 123456000, loc)
	}
	wall := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
	la, kolkata := mustLoc(t, "America/Los_Angeles"), mustLoc(t, "Asia/Kolkata")

	t.Run("timestamp keeps the wall clock whatever the decoder labeled it with", func(t *testing.T) {
		for _, in := range []time.Time{label(time.UTC), label(time.Local), label(la), label(kolkata)} {
			is := is.New(t)
			got, err := types.Format(pgtype.TimestampOID, in)
			is.NoErr(err)
			is.Equal(got.(time.Time).String(), wall.String())
			is.Equal(got.(time.Time).Location(), time.UTC)
		}
	})

	t.Run("date keeps the calendar date whatever the decoder labeled it with", func(t *testing.T) {
		is := is.New(t)
		got, err := types.Format(pgtype.DateOID, time.Date(2024, 1, 2, 0, 0, 0, 0, la))
		is.NoErr(err)
		is.Equal(got.(time.Time).String(), "2024-01-02 00:00:00 +0000 UTC")
	})

	t.Run("timestamptz is the same instant in UTC", func(t *testing.T) {
		is := is.New(t)
		in := time.Date(2024, 1, 1, 19, 4, 5, 123456000, la)
		got, err := types.Format(pgtype.TimestamptzOID, in)
		is.NoErr(err)
		is.Equal(got.(time.Time).String(), wall.String())
		is.Equal(got.(time.Time).Location(), time.UTC)
	})

	t.Run("native infinity is text", func(t *testing.T) {
		for _, oid := range []uint32{pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID} {
			is := is.New(t)
			got, err := types.Format(oid, pgtype.Infinity)
			is.NoErr(err)
			is.Equal(got, "infinity")
			got, err = types.Format(oid, pgtype.NegativeInfinity)
			is.NoErr(err)
			is.Equal(got, "-infinity")
		}
	})

	t.Run("avro infinity is an actionable error", func(t *testing.T) {
		for _, oid := range []uint32{pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID} {
			is := is.New(t)
			_, err := types.FormatFor(types.EncodingAvro, oid, pgtype.Infinity)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "infinity"))
		}
	})

	t.Run("native values outside of 0000-9999 are Postgres text", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			oid  uint32
			in   time.Time
			want string
		}{
			{"timestamp BC", pgtype.TimestampOID, time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC), "0044-03-15 12:00:00 BC"},
			{"timestamp 1 BC", pgtype.TimestampOID, time.Date(-1, 12, 31, 23, 59, 59, 999999000, time.UTC), "0002-12-31 23:59:59.999999 BC"},
			{"timestamp > 9999", pgtype.TimestampOID, time.Date(12345, 6, 7, 8, 9, 10, 500000000, time.UTC), "12345-06-07 08:09:10.5"},
			{"timestamptz BC", pgtype.TimestamptzOID, time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC), "0044-03-15 12:00:00+00 BC"},
			{"timestamptz > 9999", pgtype.TimestamptzOID, time.Date(12345, 6, 7, 8, 9, 10, 0, la), "12345-06-07 15:09:10+00"}, // LMT is not -8
			{"date BC", pgtype.DateOID, time.Date(-43, 3, 15, 0, 0, 0, 0, time.UTC), "0044-03-15 BC"},
			{"date > 9999", pgtype.DateOID, time.Date(12345, 6, 7, 0, 0, 0, 0, time.UTC), "12345-06-07"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				is := is.New(t)
				got, err := types.Format(tc.oid, tc.in)
				is.NoErr(err)
				is.Equal(got, tc.want)
			})
		}
	})

	t.Run("year 0000 and 9999 stay time.Time", func(t *testing.T) {
		is := is.New(t)
		for _, y := range []int{0, 9999} {
			got, err := types.Format(pgtype.TimestampOID, time.Date(y, 12, 31, 23, 59, 59, 0, time.UTC))
			is.NoErr(err)
			_, ok := got.(time.Time)
			is.True(ok)

			// and the record serializer accepts it (it panics on others)
			rec := opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"ts": got}}}
			is.True(len(rec.Bytes()) > 0)
		}
	})

	t.Run("avro timestamp is the wall clock as microseconds", func(t *testing.T) {
		unixMicros := func(tm time.Time) int64 { return tm.Unix() * 1_000_000 }
		for _, tc := range []struct {
			name string
			in   time.Time
			want int64
		}{
			{"epoch", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
			{"micros", wall, 1704164645123456},
			{"before epoch", time.Date(1969, 12, 31, 23, 59, 59, 999999000, time.UTC), -1},
			{"BC", time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC), unixMicros(time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC))},
			{"> 9999", time.Date(12345, 6, 7, 8, 9, 10, 0, time.UTC), unixMicros(time.Date(12345, 6, 7, 8, 9, 10, 0, time.UTC))},
		} {
			t.Run(tc.name, func(t *testing.T) {
				is := is.New(t)
				// the same wall clock, labeled with different zones
				for _, loc := range []*time.Location{time.UTC, time.Local, la, kolkata} {
					in := time.Date(tc.in.Year(), tc.in.Month(), tc.in.Day(), tc.in.Hour(), tc.in.Minute(), tc.in.Second(), tc.in.Nanosecond(), loc)
					got, err := types.FormatFor(types.EncodingAvro, pgtype.TimestampOID, in)
					is.NoErr(err)
					is.Equal(got, tc.want)
				}
			})
		}
	})

	t.Run("avro timestamp out of int64 microseconds is an error", func(t *testing.T) {
		is := is.New(t)
		// Postgres allows timestamps up to 294276 AD; int64 microseconds end at 292277 AD.
		_, err := types.FormatFor(types.EncodingAvro, pgtype.TimestampOID, time.Date(294276, 1, 1, 0, 0, 0, 0, time.UTC))
		is.True(err != nil)
		_, err = types.FormatFor(types.EncodingAvro, pgtype.TimestamptzOID, time.Date(294276, 1, 1, 0, 0, 0, 0, time.UTC))
		is.True(err != nil)

		// the last second that fits, with the most microseconds that fit
		last := time.Unix(9223372036852, 999999000).UTC() // 9223372036852999999 us
		got, err := types.FormatFor(types.EncodingAvro, pgtype.TimestampOID, last)
		is.NoErr(err)
		is.Equal(got, int64(9223372036852999999))
	})

	t.Run("arrays are normalized element by element", func(t *testing.T) {
		is := is.New(t)
		got, err := types.Format(pgtype.TimestamptzArrayOID, []any{
			time.Date(2024, 1, 1, 19, 4, 5, 0, la), nil, pgtype.Infinity,
		})
		is.NoErr(err)
		is.Equal("", cmp.Diff([]any{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), nil, "infinity"}, got))

		_, err = types.Format(pgtype.TimestampArrayOID, "not an array")
		is.True(err != nil)
	})

	t.Run("tsvector is canonical text", func(t *testing.T) {
		is := is.New(t)
		got, err := types.Format(pgtype.TSVectorOID, pgtype.TSVector{Valid: true, Lexemes: []pgtype.TSVectorLexeme{
			{Word: "it's", Positions: []pgtype.TSVectorPosition{{Position: 1, Weight: pgtype.TSVectorWeightA}, {Position: 2, Weight: pgtype.TSVectorWeightD}, {Position: 3}}},
			{Word: `a\b`},
		}})
		is.NoErr(err)
		is.Equal(got, `'it''s':1A,2,3 'a\\b'`)

		got, err = types.Format(pgtype.TSVectorOID, pgtype.TSVector{})
		is.NoErr(err)
		is.Equal(got, nil)
	})
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// TestGoldenRanges decodes ranges from the text the walsender prints (the CDC
// path) and requires the bounds to be zone independent: tstzrange bounds are
// UTC (pgx 5.11 decodes them in time.Local), tsrange and daterange bounds keep
// their wall clock, infinite bounds are text.
func TestGoldenRanges(t *testing.T) {
	utcBound := func(y int, m time.Month, d, h, mi, s, ns int) time.Time {
		return time.Date(y, m, d, h, mi, s, ns, time.UTC)
	}
	strictTime := cmp.Comparer(func(x, y time.Time) bool {
		return x.Equal(y) && x.Location() == time.UTC && y.Location() == time.UTC
	})
	rng := func(lo, hi any, lt, ut pgtype.BoundType) pgtype.Range[any] {
		return pgtype.Range[any]{Lower: lo, Upper: hi, LowerType: lt, UpperType: ut, Valid: true}
	}

	for _, tc := range []struct {
		name string
		oid  uint32
		text string
		want any
	}{
		{"tstzrange", pgtype.TstzrangeOID, `["2024-01-02 03:04:05.123456+00","2024-01-02 08:34:05+05:30")`,
			rng(utcBound(2024, 1, 2, 3, 4, 5, 123456000), utcBound(2024, 1, 2, 3, 4, 5, 0), pgtype.Inclusive, pgtype.Exclusive)},
		{"tstzrange infinite upper", pgtype.TstzrangeOID, `["2024-01-02 03:04:05+00",infinity)`,
			rng(utcBound(2024, 1, 2, 3, 4, 5, 0), "infinity", pgtype.Inclusive, pgtype.Exclusive)},
		{"tstzrange unbounded", pgtype.TstzrangeOID, `["2024-01-02 03:04:05+00",)`,
			rng(utcBound(2024, 1, 2, 3, 4, 5, 0), nil, pgtype.Inclusive, pgtype.Unbounded)},
		{"tsrange keeps the wall clock", pgtype.TsrangeOID, `["2024-03-10 02:30:00","2024-09-29 02:30:00")`,
			rng(utcBound(2024, 3, 10, 2, 30, 0, 0), utcBound(2024, 9, 29, 2, 30, 0, 0), pgtype.Inclusive, pgtype.Exclusive)},
		{"daterange", pgtype.DaterangeOID, `[2024-01-02,2024-12-31)`,
			rng(utcBound(2024, 1, 2, 0, 0, 0, 0), utcBound(2024, 12, 31, 0, 0, 0, 0), pgtype.Inclusive, pgtype.Exclusive)},
		{"tstzrange BC bound is text", pgtype.TstzrangeOID, `["0044-03-15 12:00:00+00 BC","2024-01-02 03:04:05+00")`,
			rng("0044-03-15 12:00:00+00 BC", utcBound(2024, 1, 2, 3, 4, 5, 0), pgtype.Inclusive, pgtype.Exclusive)},
		{"tstzmultirange", pgtype.TstzmultirangeOID, `{["2024-01-02 03:04:05+00","2024-01-03 00:00:00+00"),["2024-02-01 00:00:00+00",)}`,
			pgtype.Multirange[pgtype.Range[any]]{
				rng(utcBound(2024, 1, 2, 3, 4, 5, 0), utcBound(2024, 1, 3, 0, 0, 0, 0), pgtype.Inclusive, pgtype.Exclusive),
				rng(utcBound(2024, 2, 1, 0, 0, 0, 0), nil, pgtype.Inclusive, pgtype.Unbounded),
			}},
		{"tstzrange[]", pgtype.TstzrangeArrayOID, `{"[\"2024-01-02 03:04:05+00\",\"2024-01-03 00:00:00+00\")"}`,
			[]any{rng(utcBound(2024, 1, 2, 3, 4, 5, 0), utcBound(2024, 1, 3, 0, 0, 0, 0), pgtype.Inclusive, pgtype.Exclusive)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, err := types.Format(tc.oid, decodeText(t, tc.oid, tc.text))
			is.NoErr(err)
			is.Equal("", cmp.Diff(tc.want, got, strictTime))
		})
	}
}

func TestTemporalNotRepresentableError(t *testing.T) {
	is := is.New(t)

	_, err := types.FormatFor(types.EncodingAvro, pgtype.DateOID, pgtype.Infinity)
	located, ok := types.LocateTemporal(err, "orders", "shipped_on")
	is.True(ok)
	msg := located.Error()
	is.True(strings.HasPrefix(msg, types.ErrorCodeTemporalNotRepresentable+": "))
	is.Equal(types.ErrorCodeTemporalNotRepresentable, "postgres.source.temporal_not_representable")
	for _, want := range []string{`"shipped_on"`, `"orders"`, "infinity", "logrepl.withAvroSchema to false", "WAL"} {
		is.True(strings.Contains(msg, want)) // msg: want
	}
	is.True(!strings.Contains(msg, "DLQ"))
	is.True(strings.Contains(msg, "does not help")) // CDC: the change is in the WAL

	// the snapshot form says the row can be fixed, and not the WAL advice
	snap, ok := types.LocateTemporalSnapshot(err, "orders", "shipped_on")
	is.True(ok)
	is.True(strings.HasPrefix(snap.Error(), types.ErrorCodeTemporalNotRepresentable+": "))
	is.True(strings.Contains(snap.Error(), "Changing or deleting the row also helps"))
	is.True(!strings.Contains(snap.Error(), "WAL"))

	// out-of-range microseconds carry the same code
	_, err = types.FormatFor(types.EncodingAvro, pgtype.TimestampOID, time.Date(294276, 1, 1, 0, 0, 0, 0, time.UTC))
	var tne *types.TemporalNotRepresentableError
	is.True(errors.As(err, &tne))

	// other errors are not touched
	_, ok = types.LocateTemporal(errors.New("boom"), "t", "c")
	is.True(!ok)
}

// TestTemporalArrayNonFinite pins what date[], timestamp[] and timestamptz[]
// do with infinity elements: strings without an Avro schema, the
// temporal_not_representable error with one. Finite elements next to them are
// normalized as in the scalar case.
func TestTemporalArrayNonFinite(t *testing.T) {
	la := mustLoc(t, "America/Los_Angeles")
	for _, tc := range []struct {
		name   string
		oid    uint32
		finite any
		want   any
	}{
		{"date[]", pgtype.DateArrayOID, time.Date(2024, 1, 2, 0, 0, 0, 0, la), time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"timestamp[]", pgtype.TimestampArrayOID, time.Date(2024, 1, 2, 3, 4, 5, 0, la), time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{"timestamptz[]", pgtype.TimestamptzArrayOID, time.Date(2024, 1, 1, 19, 4, 5, 0, la), time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			in := []any{tc.finite, pgtype.Infinity, nil, pgtype.NegativeInfinity}

			got, err := types.Format(tc.oid, in)
			is.NoErr(err)
			arr := got.([]any)
			is.Equal(len(arr), 4)
			is.Equal(arr[0].(time.Time).String(), tc.want.(time.Time).String())
			is.Equal(arr[1], "infinity")
			is.Equal(arr[2], nil)
			is.Equal(arr[3], "-infinity")

			_, err = types.FormatFor(types.EncodingAvro, tc.oid, in)
			var tne *types.TemporalNotRepresentableError
			is.True(errors.As(err, &tne))
			located, ok := types.LocateTemporal(err, "t", "c")
			is.True(ok)
			is.True(strings.HasPrefix(located.Error(), "postgres.source.temporal_not_representable: "))
		})
	}
}

// TestMultirangeArraysPassThrough: pgx 5.11 registers no type for the arrays of
// tsmultirange, tstzmultirange and datemultirange (OIDs 6152, 6153, 6155), so
// their values reach the formatter as the Postgres text. They must pass through,
// not be rejected as an unexpected type (that halted snapshots).
func TestMultirangeArraysPassThrough(t *testing.T) {
	for _, tc := range []struct {
		name string
		oid  uint32
		text string
	}{
		{"tsmultirange[]", pgtype.TsmultirangeArrayOID, `{"{[\"2024-01-02 03:04:05\",\"2024-03-10 02:30:00\")}"}`},
		{"tstzmultirange[]", pgtype.TstzmultirangeArrayOID, `{"{[\"2024-01-02 03:04:05+00\",\"2024-01-03 00:00:00+00\")}"}`},
		{"datemultirange[]", pgtype.DatemultirangeArrayOID, `{"{[2024-01-02,2024-12-31)}"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			for _, enc := range []types.Encoding{types.EncodingNative, types.EncodingAvro} {
				got, err := types.FormatFor(enc, tc.oid, tc.text)
				is.NoErr(err)
				is.Equal(got, tc.text)
			}
		})
	}
}

// An out-of-range timestamp is record data: the error names the type, not the value.
func TestTemporalNotRepresentableError_OutOfRangeDoesNotEchoTheValue(t *testing.T) {
	is := is.New(t)

	_, err := types.FormatFor(types.EncodingAvro, pgtype.TimestampOID, time.Date(294276, 1, 1, 0, 0, 0, 0, time.UTC))
	located, ok := types.LocateTemporal(err, "orders", "created")
	is.True(ok)
	is.True(!strings.Contains(located.Error(), "294276"))
	is.True(strings.Contains(located.Error(), "out of range"))
}
