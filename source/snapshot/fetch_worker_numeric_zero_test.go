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
	"math/big"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

// Test_FetcherRun_NumericZeroScale is the snapshot half of the regression test
// for #391. The snapshot reads in pgx's binary format, which returned every
// zero as 0 although Postgres (and CDC) print 0.000, 0.00000 or 0.0. With an
// Avro schema an unconstrained numeric is the exact text, so the two paths
// disagreed. Without Avro it is a big.Rat, which has no display scale.
func Test_FetcherRun_NumericZeroScale(t *testing.T) {
	cases := []struct {
		colType string
		in      string
		want    string // text Postgres prints, and the Avro string
	}{
		{"numeric", "0", "0"},
		{"numeric", "0.000", "0.000"},
		{"numeric", "0.00000", "0.00000"},
		{"numeric", "0.0", "0.0"},
		{"numeric", "-0.0", "0.0"}, // Postgres has no negative zero
		{"numeric", "0.0000000000000000000000000000000001", "0.0000000000000000000000000000000001"},
		{"numeric", "1.50", "1.50"}, // non-zero values are untouched
		{"numeric", "100.00", "100.00"},
		{"numeric", "NaN", "NaN"},
		{"numeric(10,3)", "0", "0.000"}, // constrained: the column rescales
		{"numeric(10,3)", "0.000", "0.000"},
	}

	for _, avro := range []bool{true, false} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("avro=%t/%s/%s", avro, tc.colType, tc.in), func(t *testing.T) {
				var (
					ctx   = test.Context(t)
					is    = is.New(t)
					pool  = test.ConnectPool(ctx, t, test.RegularConnString)
					table = fmt.Sprintf("snap_numzero_%d", time.Now().UnixNano()%1000000000)
				)
				if avro && (tc.colType == "numeric(10,3)" || tc.want == "NaN") {
					t.Skip("an Avro decimal has no text form and cannot hold NaN, covered without Avro and by the numeric tests")
				}
				// A column of another type before and after the numeric: the
				// fix must only touch the numeric column.
				_, err := pool.Exec(ctx, fmt.Sprintf(
					"CREATE TABLE %s (id bigint PRIMARY KEY, a text, v %s, b float8)", table, tc.colType))
				is.NoErr(err)
				t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+table) })
				_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'x', '%s', 2.5)", table, tc.in))
				is.NoErr(err)

				var pgText string
				is.NoErr(pool.QueryRow(ctx, fmt.Sprintf("SELECT v::text FROM %s", table)).Scan(&pgText))
				is.Equal(pgText, tc.want)

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
				is.Equal(len(got), 1)

				p := got[0].Payload
				is.Equal(p["a"], "x")
				is.Equal(p["b"], 2.5)
				v := p["v"]
				switch {
				case avro:
					is.Equal(v, tc.want) // exact Postgres text, the same as CDC
				case tc.want == "NaN":
					is.Equal(v, "NaN")
				default:
					r, ok := v.(*big.Rat)
					is.True(ok)
					want, _ := new(big.Rat).SetString(tc.want)
					is.Equal(r.Cmp(want), 0)
				}
			})
		}
	}
}
