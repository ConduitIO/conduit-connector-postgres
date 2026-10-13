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
