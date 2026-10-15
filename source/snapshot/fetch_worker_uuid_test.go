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
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
	"gopkg.in/tomb.v2"
)

// Test_FetcherRun_UUIDNull is the snapshot half of the regression test for
// #392. A NULL uuid used to be formatted as "", which a consumer cannot tell
// from an empty value and which is not a uuid (Invariant 6). It must be the
// untyped nil, with and without an Avro schema; a real uuid keeps its
// canonical text.
func Test_FetcherRun_UUIDNull(t *testing.T) {
	const id = "bd94ee0b-564f-4088-bf4e-8d5e626caf66"

	cases := []struct {
		name string
		sql  string
		want any
	}{
		{name: "NULL", sql: "NULL", want: nil},
		{name: "value", sql: "'" + id + "'", want: id},
	}

	for _, avro := range []bool{true, false} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("avro=%t/%s", avro, tc.name), func(t *testing.T) {
				var (
					ctx   = test.Context(t)
					is    = is.New(t)
					pool  = test.ConnectPool(ctx, t, test.RegularConnString)
					table = fmt.Sprintf("snap_uuid_%d", time.Now().UnixNano()%1000000000)
				)
				_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v uuid)", table))
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
				is.NoErr(tt.Wait())

				is.Equal(len(got), 1)
				v, present := got[0].Payload["v"]
				is.True(present)
				is.Equal(v, tc.want) // a NULL uuid is the untyped nil, never ""
			})
		}
	}
}
