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

package postgres

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// With logrepl.withAvroSchema=false a float4/float8 NaN or infinity was a Go
// float in the payload, which JSON cannot hold: opencdc.Record.Bytes panicked
// and the replay after the restart panicked again (#376). They are now the
// strings "NaN", "Infinity" and "-Infinity", on the snapshot path and on the CDC
// path, for scalars and element by element in arrays. With Avro on they stay
// floats, which Avro float and double hold; that is pinned too (arrays have no
// Avro mapping in this connector, so they are only read without Avro).
func TestSource_FloatNonFinite(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	cases := []struct {
		name    string
		colType string
		sql     string
		// native is the check for the payload value without Avro.
		native func(is *is.I, got any)
		// text is set for a scalar NaN or infinity: with Avro it is a float.
		text  string
		array bool
		null  bool
	}{
		{name: "float8 NaN", colType: "float8", sql: "'NaN'", text: "NaN"},
		{name: "float8 Infinity", colType: "float8", sql: "'Infinity'", text: "Infinity"},
		{name: "float8 -Infinity", colType: "float8", sql: "'-Infinity'", text: "-Infinity"},
		{name: "float4 NaN", colType: "float4", sql: "'NaN'", text: "NaN"},
		{name: "float4 Infinity", colType: "float4", sql: "'Infinity'", text: "Infinity"},
		{name: "float4 -Infinity", colType: "float4", sql: "'-Infinity'", text: "-Infinity"},
		{name: "float8 finite", colType: "float8", sql: "1.5", native: func(is *is.I, got any) { is.Equal(got, 1.5) }},
		{name: "float4 finite", colType: "float4", sql: "1.5", native: func(is *is.I, got any) { is.Equal(got, float32(1.5)) }},
		{name: "float8 max", colType: "float8", sql: "'1.7976931348623157e308'", native: func(is *is.I, got any) { is.Equal(got, math.MaxFloat64) }},
		{name: "float8 NULL", colType: "float8", sql: "NULL", null: true},
		{
			name: "float8[] elements", colType: "float8[]", array: true,
			sql:    "ARRAY['NaN','Infinity','-Infinity',1.5,NULL]::float8[]",
			native: func(is *is.I, got any) { is.Equal(got, []any{"NaN", "Infinity", "-Infinity", 1.5, nil}) },
		},
		{
			name: "float4[] elements", colType: "float4[]", array: true,
			sql:    "ARRAY['NaN','Infinity','-Infinity',1.5,NULL]::float4[]",
			native: func(is *is.I, got any) { is.Equal(got, []any{"NaN", "Infinity", "-Infinity", float32(1.5), nil}) },
		},
		{
			name: "float8[] finite", colType: "float8[]", array: true, sql: "ARRAY[1.5,2.5]::float8[]",
			native: func(is *is.I, got any) { is.Equal(got, []any{1.5, 2.5}) },
		},
	}

	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			for _, tc := range cases {
				if avro && (tc.array || tc.null) {
					continue // no Avro mapping for arrays; Avro cannot encode any NULL (#326)
				}
				t.Run(fmt.Sprintf("%s/avro=%t/%s", path, avro, tc.name), func(t *testing.T) {
					is := is.New(t)
					table := fmt.Sprintf("floatnf_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
					_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, tc.colType))
					is.NoErr(err)
					dropReplication(ctx, t, table)
					t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
					insert := fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", table, tc.sql)

					mode := "never"
					if path == "snapshot" {
						mode = "initial"
						_, err = conn.Exec(ctx, insert)
						is.NoErr(err)
					}
					s := NewSource()
					is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
						"url": test.RepmgrConnString, "tables": table, "snapshotMode": mode,
						"cdcMode": "logrepl", "logrepl.slotName": table,
						"logrepl.publicationName": table, "logrepl.withAvroSchema": fmt.Sprint(avro),
					}, s.Config(), Connector.NewSpecification().SourceParams))
					is.NoErr(s.Open(ctx, nil))
					t.Cleanup(func() { _ = s.Teardown(context.Background()) })
					if path == "cdc" {
						_, err = conn.Exec(ctx, insert)
						is.NoErr(err)
					}

					rec, err := readOne(ctx, t, s)
					is.NoErr(err)
					got := payloadValue(ctx, t, rec, avro)

					if avro {
						// The payload went through the SDK's Avro encoding and back.
						f, ok := got.(float64)
						if f32, is32 := got.(float32); is32 {
							f, ok = float64(f32), true
						}
						is.True(ok) // a float
						switch tc.text {
						case "NaN":
							is.True(math.IsNaN(f))
						case "Infinity":
							is.True(math.IsInf(f, 1))
						case "-Infinity":
							is.True(math.IsInf(f, -1))
						default:
							tc.native(is, got)
						}
						return
					}

					switch {
					case tc.null:
						is.True(got == nil)
					case tc.text != "":
						is.Equal(got, tc.text) // canonical text, never a Go NaN or infinity
					default:
						tc.native(is, got)
					}
					assertBytesDoNotPanic(t, rec) // the call that panicked
				})
			}
		}
	}
}

func assertBytesDoNotPanic(t *testing.T, rec opencdc.Record) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("opencdc.Record.Bytes panicked: %v", r)
		}
	}()
	if len(rec.Bytes()) == 0 {
		t.Fatal("opencdc.Record.Bytes returned nothing")
	}
}
