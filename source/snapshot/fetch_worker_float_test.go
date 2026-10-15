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
	"math"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-postgres/source/schema"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

// Test_FetcherRun_FloatNonFinite is the snapshot half of the regression test
// for #376. Without an Avro schema a float4/float8 NaN or infinity reached the
// payload as a Go float, which JSON cannot hold: opencdc.Record.Bytes panicked.
// They are now the strings "NaN", "Infinity" and "-Infinity". With an Avro
// schema they stay floats, which Avro float and double hold; that is pinned
// here too, through the Avro encoding the SDK applies.
func Test_FetcherRun_FloatNonFinite(t *testing.T) {
	cases := []struct {
		colType string
		sql     string
		text    string // canonical text of a non-finite value
		null    bool
	}{
		{colType: "float8", sql: "'NaN'", text: "NaN"},
		{colType: "float8", sql: "'Infinity'", text: "Infinity"},
		{colType: "float8", sql: "'-Infinity'", text: "-Infinity"},
		{colType: "float4", sql: "'NaN'", text: "NaN"},
		{colType: "float4", sql: "'Infinity'", text: "Infinity"},
		{colType: "float4", sql: "'-Infinity'", text: "-Infinity"},
		{colType: "float8", sql: "NULL", null: true},
		{colType: "float4", sql: "NULL", null: true},
		{colType: "float8", sql: "1.5"},
		{colType: "float4", sql: "1.5"},
		{colType: "float8", sql: "'1.7976931348623157e308'"},
	}

	for _, avro := range []bool{true, false} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("avro=%t/%s/%s", avro, tc.colType, tc.sql), func(t *testing.T) {
				var (
					ctx   = test.Context(t)
					is    = is.New(t)
					pool  = test.ConnectPool(ctx, t, test.RegularConnString)
					table = fmt.Sprintf("snap_float_%d", time.Now().UnixNano()%1000000000)
				)
				_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, tc.colType))
				is.NoErr(err)
				t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+table) })
				_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", table, tc.sql))
				is.NoErr(err)

				got := runFetch(ctx, t, pool, table, avro)
				is.Equal(len(got), 1)
				v := got[0].Payload["v"]

				switch {
				case tc.null:
					is.True(v == nil)
				case tc.text != "" && !avro:
					is.Equal(v, tc.text) // canonical text, not a Go NaN or infinity
				case tc.text != "": // Avro: still a float, which Avro encodes
					f := toFloat64(is, v)
					switch tc.text {
					case "NaN":
						is.True(math.IsNaN(f))
					case "Infinity":
						is.True(math.IsInf(f, 1))
					default:
						is.True(math.IsInf(f, -1))
					}
				default:
					is.True(!math.IsNaN(toFloat64(is, v)) && !math.IsInf(toFloat64(is, v), 0)) // finite: unchanged
				}

				if !avro {
					// The step that panicked.
					rec := opencdc.Record{Payload: opencdc.Change{After: got[0].Payload}}
					is.True(len(rec.Bytes()) > 0)
					return
				}

				// Encode as the SDK does with the schema the connector registers,
				// and decode it back.
				rows, err := pool.Query(ctx, "SELECT * FROM "+table)
				is.NoErr(err)
				defer rows.Close()
				sch, err := schema.Avro.Extract(table+"_payload", rows.FieldDescriptions())
				is.NoErr(err)
				serde, err := cavro.Parse([]byte(sch.String()))
				is.NoErr(err)
				if tc.null {
					return // Avro cannot encode any NULL (#326)
				}
				b, err := serde.Marshal(got[0].Payload)
				is.NoErr(err)
				var back map[string]any
				is.NoErr(serde.Unmarshal(b, &back))
				switch tc.text {
				case "NaN":
					is.True(math.IsNaN(toFloat64(is, back["v"])))
				case "Infinity":
					is.True(math.IsInf(toFloat64(is, back["v"]), 1))
				case "-Infinity":
					is.True(math.IsInf(toFloat64(is, back["v"]), -1))
				default:
					is.Equal(toFloat64(is, back["v"]), toFloat64(is, v))
				}
			})
		}
	}
}

// Test_FetcherRun_FloatArrayNonFinite covers float4[] and float8[] element by
// element. Arrays have no Avro mapping in this connector, so only the
// non-Avro path is read.
func Test_FetcherRun_FloatArrayNonFinite(t *testing.T) {
	cases := []struct {
		colType string
		sql     string
		want    any
	}{
		{"float8[]", "ARRAY['NaN','Infinity','-Infinity',1.5,NULL]::float8[]", []any{"NaN", "Infinity", "-Infinity", 1.5, nil}},
		{"float4[]", "ARRAY['NaN','Infinity','-Infinity',1.5,NULL]::float4[]", []any{"NaN", "Infinity", "-Infinity", float32(1.5), nil}},
		{"float8[]", "ARRAY[1.5,2.5]::float8[]", []any{1.5, 2.5}},
		{"float4[]", "NULL", nil},
	}
	for _, tc := range cases {
		t.Run(tc.colType+"/"+tc.sql, func(t *testing.T) {
			var (
				ctx   = test.Context(t)
				is    = is.New(t)
				pool  = test.ConnectPool(ctx, t, test.RegularConnString)
				table = fmt.Sprintf("snap_floatarr_%d", time.Now().UnixNano()%1000000000)
			)
			_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, tc.colType))
			is.NoErr(err)
			t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+table) })
			_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", table, tc.sql))
			is.NoErr(err)

			got := runFetch(ctx, t, pool, table, false)
			is.Equal(len(got), 1)
			is.Equal(got[0].Payload["v"], tc.want)
			rec := opencdc.Record{Payload: opencdc.Change{After: got[0].Payload}}
			is.True(len(rec.Bytes()) > 0) // Bytes panics on a Go NaN or infinity
		})
	}
}

// runFetch runs a fetch worker over the table and returns what it sent.
func runFetch(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, avro bool) []FetchData {
	t.Helper()

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
	if err := tt.Wait(); err != nil {
		t.Fatalf("fetch worker failed: %v", err)
	}
	return got
}

func toFloat64(is *is.I, v any) float64 {
	switch f := v.(type) {
	case float64:
		return f
	case float32:
		return float64(f)
	}
	is.Fail() // expected a float, got something else
	return 0
}
