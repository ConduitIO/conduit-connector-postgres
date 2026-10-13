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

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// TemporalGroup selects which columns SetupTemporalTable creates.
type TemporalGroup int

const (
	// TemporalScalars are timestamp, timestamptz, date and tsvector values in
	// ordinary ranges, including wall clocks that do not exist in the DST-observing
	// zones the host-zone tests run in.
	TemporalScalars TemporalGroup = iota
	// TemporalArrays are arrays of the scalars, with a NULL element. The Avro
	// schema extractor does not support arrays.
	TemporalArrays
	// TemporalExtremes are BC dates and years above 9999, which pgx reads but
	// time.Time cannot serialize to JSON.
	TemporalExtremes
	// TemporalRanges are tsrange, tstzrange, daterange and a tstzmultirange,
	// including an unbounded and an infinite bound. No Avro encoding either.
	TemporalRanges
	// TemporalMultirangeArrays are tsmultirange[], tstzmultirange[] and
	// datemultirange[] (OIDs 6152, 6153, 6155). pgx 5.11 does not register these
	// types, so the values arrive as the Postgres text and must pass through.
	TemporalMultirangeArrays
	// TemporalInfinity are infinity and -infinity. They have no Avro encoding.
	TemporalInfinity
)

type temporalColumn struct {
	name, typ, literal string
	group              TemporalGroup
	// native and avro are the values the connector must emit with and without an
	// Avro schema. A nil avro means the same as native.
	native, avro any
}

func utc(y int, m time.Month, d, h, mi, s, ns int) time.Time {
	return time.Date(y, m, d, h, mi, s, ns, time.UTC)
}

// wallMicros is the Avro value of a timestamp column: microseconds of the wall clock.
func wallMicros(t time.Time) int64 {
	return t.Unix()*1_000_000 + int64(t.Nanosecond())/1000
}

const (
	typTimestamp   = "timestamp"
	typTimestamptz = "timestamptz"
	typDate        = "date"
	typTstzrange   = "tstzrange"
	textInfinity   = "infinity"
)

var (
	tsWall   = utc(2024, 1, 2, 3, 4, 5, 123456000)
	tsGapLA  = utc(2024, 3, 10, 2, 30, 0, 0) // does not exist in America/Los_Angeles
	tsGapNZ  = utc(2024, 9, 29, 2, 30, 0, 0) // does not exist in Pacific/Auckland
	tsBC     = utc(-43, 3, 15, 12, 0, 0, 0)
	tsBig    = utc(12345, 6, 7, 8, 9, 10, 500000000)
	tstzWall = utc(2024, 1, 2, 3, 4, 5, 123456000)
	date     = utc(2024, 1, 2, 0, 0, 0, 0)
	dateEnd  = utc(2024, 12, 31, 0, 0, 0, 0)
	dateBC   = utc(-43, 3, 15, 0, 0, 0, 0)
	dateBig  = utc(12345, 6, 7, 0, 0, 0, 0)
)

// temporalColumns lists the columns of the temporal test table. The tsvector
// columns have no expected value here: the tests compare them to what Postgres
// itself prints (see TemporalTSVectorText).
var temporalColumns = []temporalColumn{
	{"ts", typTimestamp, "'2024-01-02 03:04:05.123456'", TemporalScalars, tsWall, wallMicros(tsWall)},
	{"ts_gap_la", typTimestamp, "'2024-03-10 02:30:00'", TemporalScalars, tsGapLA, wallMicros(tsGapLA)},
	{"ts_gap_nz", typTimestamp, "'2024-09-29 02:30:00'", TemporalScalars, tsGapNZ, wallMicros(tsGapNZ)},
	{"tstz", typTimestamptz, "'2024-01-02 08:34:05.123456+05:30'", TemporalScalars, tstzWall, nil},
	{"date_col", typDate, "'2024-01-02'", TemporalScalars, date, nil},
	{"tsv", "tsvector", "$q$'a':1 'cat':3A,4B 'it''s' 'back\\\\slash'$q$::tsvector", TemporalScalars, nil, nil},

	{"ts_arr", "timestamp[]", "ARRAY[timestamp '2024-01-02 03:04:05.123456', NULL, timestamp '2024-03-10 02:30:00']", TemporalArrays,
		[]any{tsWall, nil, tsGapLA}, nil},
	{"tstz_arr", "timestamptz[]", "ARRAY[timestamptz '2024-01-02 08:34:05.123456+05:30']", TemporalArrays, []any{tstzWall}, nil},
	{"date_arr", "date[]", "ARRAY[date '2024-01-02', date '2024-12-31']", TemporalArrays, []any{date, dateEnd}, nil},

	{"tsv_arr", "tsvector[]", "ARRAY[$q$'a':1 'b'$q$::tsvector, $q$'c':2A$q$::tsvector]", TemporalArrays, nil, nil},

	{"ts_range", "tsrange", "'[2024-01-02 03:04:05.123456,2024-03-10 02:30:00)'", TemporalRanges,
		pgtype.Range[any]{Lower: tsWall, Upper: tsGapLA, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, nil},
	{"tstz_range", typTstzrange, "'[2024-01-02 08:34:05.123456+05:30,2024-01-03 00:00:00+00)'", TemporalRanges,
		pgtype.Range[any]{Lower: tstzWall, Upper: utc(2024, 1, 3, 0, 0, 0, 0), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, nil},
	{"tstz_range_open", typTstzrange, "'[2024-01-02 03:04:05.123456+00,)'", TemporalRanges,
		pgtype.Range[any]{Lower: tstzWall, LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true}, nil},
	{"tstz_range_inf", typTstzrange, "tstzrange('2024-01-02 03:04:05.123456+00', 'infinity')", TemporalRanges,
		pgtype.Range[any]{Lower: tstzWall, Upper: textInfinity, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, nil},
	{"date_range", "daterange", "'[2024-01-02,2024-12-31)'", TemporalRanges,
		pgtype.Range[any]{Lower: date, Upper: dateEnd, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, nil},
	{"tstz_multirange", "tstzmultirange", "'{[2024-01-02 08:34:05.123456+05:30,2024-01-03 00:00:00+00)}'", TemporalRanges,
		pgtype.Multirange[pgtype.Range[any]]{{Lower: tstzWall, Upper: utc(2024, 1, 3, 0, 0, 0, 0), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}}, nil},

	{"tsmr_arr", "tsmultirange[]", "ARRAY['{[2024-01-02 03:04:05,2024-03-10 02:30:00)}'::tsmultirange]", TemporalMultirangeArrays, nil, nil},
	{"tstzmr_arr", "tstzmultirange[]", "ARRAY['{[2024-01-02 08:34:05+05:30,2024-01-03 00:00:00+00)}'::tstzmultirange]", TemporalMultirangeArrays, nil, nil},
	{"datemr_arr", "datemultirange[]", "ARRAY['{[2024-01-02,2024-12-31)}'::datemultirange]", TemporalMultirangeArrays, nil, nil},

	{"ts_bc", typTimestamp, "'0044-03-15 12:00:00 BC'", TemporalExtremes, "0044-03-15 12:00:00 BC", wallMicros(tsBC)},
	{"ts_big", typTimestamp, "'12345-06-07 08:09:10.5'", TemporalExtremes, "12345-06-07 08:09:10.5", wallMicros(tsBig)},
	{"tstz_bc", typTimestamptz, "'0044-03-15 12:00:00+00 BC'", TemporalExtremes, "0044-03-15 12:00:00+00 BC", utc(-43, 3, 15, 12, 0, 0, 0)},
	{"tstz_big", typTimestamptz, "'12345-06-07 08:09:10+00'", TemporalExtremes, "12345-06-07 08:09:10+00", utc(12345, 6, 7, 8, 9, 10, 0)},
	{"date_bc", typDate, "'0044-03-15 BC'", TemporalExtremes, "0044-03-15 BC", dateBC},
	{"date_big", typDate, "'12345-06-07'", TemporalExtremes, "12345-06-07", dateBig},

	{"ts_inf", typTimestamp, "'infinity'", TemporalInfinity, "infinity", nil},
	{"ts_ninf", typTimestamp, "'-infinity'", TemporalInfinity, "-infinity", nil},
	{"tstz_inf", typTimestamptz, "'infinity'", TemporalInfinity, "infinity", nil},
	{"date_ninf", typDate, "'-infinity'", TemporalInfinity, "-infinity", nil},
}

func selected(groups []TemporalGroup) func(TemporalGroup) bool {
	return func(g TemporalGroup) bool {
		for _, s := range groups {
			if s == g {
				return true
			}
		}
		return false
	}
}

// CreateTemporalTable creates a table (id bigserial primary key plus the
// columns of the given groups) without rows and returns its name.
func CreateTemporalTable(ctx context.Context, t *testing.T, conn Querier, groups ...TemporalGroup) string {
	t.Helper()
	is := is.New(t)
	table := RandomIdentifier(t)
	in := selected(groups)

	var defs []string
	for _, c := range temporalColumns {
		if in(c.group) {
			defs = append(defs, c.name+" "+c.typ)
		}
	}

	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, %s)", table, strings.Join(defs, ", ")))
	is.NoErr(err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP TABLE "+table)
		is.NoErr(err)
	})
	return table
}

// InsertTemporalRow inserts one row with the values of the given groups.
// Postgres parses the values from text literals, so they do not depend on the
// time zone of the Go process or on how pgx encodes arguments.
func InsertTemporalRow(ctx context.Context, t *testing.T, conn Querier, table string, groups ...TemporalGroup) {
	t.Helper()
	is := is.New(t)
	in := selected(groups)

	var names, literals []string
	for _, c := range temporalColumns {
		if in(c.group) {
			names = append(names, c.name)
			literals = append(literals, c.literal)
		}
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(literals, ", ")))
	is.NoErr(err)
}

// SetupTemporalTable is CreateTemporalTable followed by InsertTemporalRow.
func SetupTemporalTable(ctx context.Context, t *testing.T, conn Querier, groups ...TemporalGroup) string {
	t.Helper()
	table := CreateTemporalTable(ctx, t, conn, groups...)
	InsertTemporalRow(ctx, t, conn, table, groups...)
	return table
}

// TemporalExpected returns, by column name, the value the connector must emit
// for the groups: with an Avro schema (avro true) or without. tsvector columns
// are returned as TemporalTSVectorText.
func TemporalExpected(ctx context.Context, t *testing.T, conn Querier, table string, avro bool, groups ...TemporalGroup) map[string]any {
	t.Helper()
	is := is.New(t)
	in := selected(groups)

	want := map[string]any{}
	for _, c := range temporalColumns {
		if !in(c.group) {
			continue
		}
		switch {
		case strings.HasSuffix(c.typ, "multirange[]"):
			// Unregistered in pgx: the value is the text Postgres prints.
			var txt string
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT %s::text FROM %s ORDER BY id LIMIT 1", c.name, table)).Scan(&txt))
			want[c.name] = txt
		case c.typ == "tsvector[]":
			var ss []string
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT %s::text[] FROM %s ORDER BY id LIMIT 1", c.name, table)).Scan(&ss))
			arr := make([]any, len(ss))
			for i := range ss {
				arr[i] = ss[i]
			}
			want[c.name] = arr
		case c.typ == "tsvector":
			// What Postgres itself prints is the canonical text form.
			var s string
			is.NoErr(conn.QueryRow(ctx, fmt.Sprintf("SELECT %s::text FROM %s ORDER BY id LIMIT 1", c.name, table)).Scan(&s))
			want[c.name] = s
		case avro && c.avro != nil:
			want[c.name] = c.avro
		default:
			want[c.name] = c.native
		}
	}
	return want
}
