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

package snapshot

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

// Test_FetcherRun_OutputIndependentOfSessionSettings pins what the snapshot
// emits for timestamptz, timestamp, date, float4, float8 (including the largest
// values), interval and bytea, and requires it to be the same whatever the
// session settings that change the text form of those types are.
//
// The snapshot must read them in the binary wire format. A text read makes the
// value depend on DateStyle, IntervalStyle, bytea_output, extra_float_digits
// and TimeZone: floats are silently rounded (extra_float_digits=0), the largest
// float8 fails to parse, dates, timestamps and intervals fail or change, and a
// timestamptz changes zone. That would be silent data change (Invariant 6).
// Fetching only numeric columns as text must leave every other column alone.
func Test_FetcherRun_OutputIndependentOfSessionSettings(t *testing.T) {
	const create = `CREATE TABLE %s (
		id bigint PRIMARY KEY,
		tz timestamptz, ts timestamp, d date,
		f4 float4, f8 float8,
		b bytea %s)`
	// interval has no Avro schema, so it is only read with Avro off.
	rows := []string{
		`(1, '2024-03-04 05:06:07.123456+00', '2024-03-04 05:06:07.123456', '2024-03-04',
		  1.00000012, 1.0000000000000002, '\x00ff10' %s)`,
		`(2, '2024-12-31 23:59:59.999999+05:30', '1999-12-31 23:59:59', '2999-12-31',
		  3.4028235e38, 1.7976931348623157e308, '\x' %s)`,
		`(3, '1970-01-01 00:00:00+00', '0001-01-01 00:00:00', '0001-01-01',
		  -3.4028235e38, -1.7976931348623157e308, '\x5c5c' %s)`,
	}
	intervals := []string{`, '1 year 2 mons 3 days 04:05:06.789'`, `, '-1 days -00:00:00.000001'`, `, '0'`}

	// Session settings that change the text output of the columns above, each
	// with a value that differs from the server default.
	settings := []struct{ name, value string }{
		{"DateStyle", "SQL, DMY"},
		{"IntervalStyle", "iso_8601"},
		{"bytea_output", "escape"},
		{"extra_float_digits", "0"},
		{"TimeZone", "Asia/Kolkata"},
	}

	for _, avro := range []bool{true, false} {
		t.Run(fmt.Sprintf("avro=%t", avro), func(t *testing.T) {
			var (
				ctx   = test.Context(t)
				is    = is.New(t)
				table = fmt.Sprintf("snap_guc_%d", time.Now().UnixNano()%1000000000)
			)
			setup := test.ConnectPool(ctx, t, test.RegularConnString)
			ivCol := ""
			if !avro {
				ivCol = ", iv interval"
			}
			_, err := setup.Exec(ctx, fmt.Sprintf(create, table, ivCol))
			is.NoErr(err)
			t.Cleanup(func() { _, _ = setup.Exec(context.Background(), "DROP TABLE "+table) })
			for i, r := range rows {
				iv := ""
				if !avro {
					iv = intervals[i]
				}
				_, err = setup.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", table, fmt.Sprintf(r, iv)))
				is.NoErr(err)
			}

			baseline := fetchAll(ctx, t, setup, table, avro)
			is.Equal(len(baseline), len(rows))

			// Sanity of the baseline itself: the values are the ones stored.
			is.Equal(baseline[0].Payload["f4"], float32(1.00000012))
			is.Equal(baseline[0].Payload["f8"], 1.0000000000000002)
			is.Equal(baseline[1].Payload["f4"], float32(3.4028235e38))
			is.Equal(baseline[1].Payload["f8"], 1.7976931348623157e308)
			is.Equal(baseline[2].Payload["f8"], -1.7976931348623157e308)

			for _, setting := range settings {
				t.Run(setting.name, func(t *testing.T) {
					is := is.New(t)
					// A connection parameter that is not a pgx option is sent to the
					// server as a session setting at connection start.
					connString := test.RegularConnString + "&" + url.PathEscape(setting.name) + "=" + url.PathEscape(setting.value)
					odd := test.ConnectPool(ctx, t, connString)

					// The setting is really in effect on the fetch connection.
					var in string
					is.NoErr(odd.QueryRow(ctx, "SELECT current_setting($1)", setting.name).Scan(&in))
					is.Equal(in, setting.value)

					got := fetchAll(ctx, t, odd, table, avro)
					is.Equal(len(got), len(baseline))
					for i := range baseline {
						for col, want := range baseline[i].Payload {
							is.Equal(got[i].Payload[col], want) // column must not depend on session settings
						}
						is.Equal(got[i].Key, baseline[i].Key)
					}
				})
			}
		})
	}
}

// fetchAll runs a FetchWorker over table and returns every record it emits.
func fetchAll(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, avro bool) []FetchData {
	t.Helper()
	is := is.New(t)

	out := make(chan []FetchData)
	tt := &tomb.Tomb{}
	f := NewFetchWorker(pool, out, FetchConfig{Table: table, Key: "id", WithAvroSchema: avro})
	tt.Go(func() error {
		runCtx := tt.Context(ctx)
		defer close(out)
		if err := f.Validate(runCtx); err != nil {
			return err
		}
		return f.Run(runCtx)
	})
	var got []FetchData
	for data := range out {
		got = append(got, data...)
	}
	is.NoErr(tt.Wait())
	return got
}
