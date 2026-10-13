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
	"strings"
	"testing"
	"time"

	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

// Test_FetcherRun_NumericNonFinite is the snapshot half of the regression test
// for #368. Without the fix NaN and +/-Infinity reached the payload as a typed
// nil *big.Rat: with an Avro schema the encoder dereferenced it (panic), without
// one the value was silently lost (a typed nil marshals as null).
func Test_FetcherRun_NumericNonFinite(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		text string // canonical text of a non-finite value
		null bool
	}{
		{name: "NaN", sql: "'NaN'", text: "NaN"},
		{name: "Infinity", sql: "'Infinity'", text: "Infinity"},
		{name: "NegInfinity", sql: "'-Infinity'", text: "-Infinity"},
		{name: "NULL", sql: "NULL", null: true},
		{name: "finite", sql: "'1.50'"},
	}

	for _, avro := range []bool{true, false} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("avro=%t/%s", avro, tc.name), func(t *testing.T) {
				var (
					ctx   = test.Context(t)
					is    = is.New(t)
					pool  = test.ConnectPool(ctx, t, test.RegularConnString)
					table = fmt.Sprintf("snap_numeric_%d", time.Now().UnixNano()%1000000000)
				)
				// An unconstrained numeric is an Avro string and holds NaN and the
				// infinities as text (#375); only numeric(p,s), an Avro decimal,
				// halts, and only NaN fits in one.
				colType := "numeric"
				halts := avro && tc.text == "NaN"
				if halts {
					colType = "numeric(8,2)"
				}
				_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, colType))
				is.NoErr(err)
				t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+table) })
				_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", table, tc.sql))
				is.NoErr(err)

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
				runErr := tt.Wait()

				if halts {
					// Invariant 6: the Avro decimal cannot hold it, so halt
					// with an actionable error instead of changing the value.
					is.True(runErr != nil)
					is.Equal(len(got), 0)
					msg := runErr.Error()
					is.True(strings.Contains(msg, "postgres.source.numeric_not_representable"))
					is.True(strings.Contains(msg, table))
					is.True(strings.Contains(msg, `"v"`))
					is.True(strings.Contains(msg, "(row id=1)"))
					is.True(strings.Contains(msg, fmt.Sprintf("%q", tc.text)))
					return
				}

				is.NoErr(runErr)
				is.Equal(len(got), 1)
				v, present := got[0].Payload["v"]
				is.True(present)
				switch {
				case tc.text != "":
					is.Equal(v, tc.text)
				case tc.null:
					is.True(v == nil) // untyped nil, not a typed nil *big.Rat
				case avro:
					is.Equal(v, "1.50") // exact text, scale kept
				default:
					r, ok := v.(*big.Rat)
					is.True(ok)
					is.Equal(r.Cmp(big.NewRat(3, 2)), 0)
				}

				if avro && !tc.null {
					// Encode as the SDK does with the schema the connector
					// registers. This is the step that panicked.
					rows, err := pool.Query(ctx, "SELECT * FROM "+table)
					is.NoErr(err)
					defer rows.Close()
					sch, err := schema.Avro.Extract(table+"_payload", rows.FieldDescriptions())
					is.NoErr(err)
					serde, err := cavro.Parse([]byte(sch.String()))
					is.NoErr(err)
					_, err = serde.Marshal(got[0].Payload)
					is.NoErr(err)
				}
			})
		}
	}
}
