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
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

// Arrays of date, timestamp, timestamptz and numeric with an infinite or NaN
// element. Without Avro, pgx's InfinityModifier element used to be emitted as
// the int 1 / -1 (silent corruption) and an infinite numeric element as a
// pgtype.Numeric that JSON cannot marshal. Arrays have no Avro mapping in this
// connector, so under Avro the source cannot encode any array and halts before
// emitting anything; these tests pin that nothing is emitted for them.
func TestSource_ArrayNonFinite(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	d := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name, typ, sql string
		native         func(is *is.I, got any)
		finite         bool
	}{
		{"date infinity", "date[]", "ARRAY['2024-01-02'::date,'infinity']", func(is *is.I, got any) { is.Equal(got, []any{d, "infinity"}) }, false},
		{"date -infinity", "date[]", "ARRAY['-infinity'::date]", func(is *is.I, got any) { is.Equal(got, []any{"-infinity"}) }, false},
		{"timestamp infinity", "timestamp[]", "ARRAY['infinity'::timestamp]", func(is *is.I, got any) { is.Equal(got, []any{"infinity"}) }, false},
		{"timestamptz -infinity", "timestamptz[]", "ARRAY['-infinity'::timestamptz,'infinity']", func(is *is.I, got any) { is.Equal(got, []any{"-infinity", "infinity"}) }, false},
		{"numeric NaN", "numeric[]", "ARRAY[1.5,'NaN'::numeric]", func(is *is.I, got any) {
			a, ok := got.([]any)
			is.True(ok)
			is.Equal(len(a), 2)
			_, isNum := a[0].(pgtype.Numeric) // finite elements are unchanged
			is.True(isNum)
			is.Equal(a[1], "NaN")
		}, false},
		{"numeric Infinity", "numeric[]", "ARRAY['Infinity'::numeric]", func(is *is.I, got any) { is.Equal(got, []any{"Infinity"}) }, false},
		{"numeric -Infinity", "numeric[]", "ARRAY['-Infinity'::numeric]", func(is *is.I, got any) { is.Equal(got, []any{"-Infinity"}) }, false},
		{"date finite", "date[]", "ARRAY['2024-01-02'::date]", func(is *is.I, got any) { is.Equal(got, []any{d}) }, true},
		{"numeric finite", "numeric[]", "ARRAY[1.5,2]", func(is *is.I, got any) {
			a, ok := got.([]any)
			is.True(ok)
			is.Equal(len(a), 2)
			_, isNum := a[0].(pgtype.Numeric)
			is.True(isNum)
		}, true},
	}

	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			for _, tc := range cases {
				if avro && tc.finite {
					continue // arrays have no Avro mapping, finite or not
				}
				t.Run(fmt.Sprintf("%s/avro=%t/%s", path, avro, tc.name), func(t *testing.T) {
					is := is.New(t)
					table := fmt.Sprintf("arrinf_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
					_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s)", table, tc.typ))
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
					if avro {
						is.True(err != nil) // halts; nothing may be emitted
						return
					}
					is.NoErr(err)
					tc.native(is, payloadValue(ctx, t, rec, false))
				})
			}
		}
	}
}
