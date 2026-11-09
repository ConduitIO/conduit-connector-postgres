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

package logrepl

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// TestWarnToastIdentity: the Open-time warning that an UPDATE leaving a
// TOASTed value unchanged will stop an Avro pipeline, for every replica
// identity and column mix.
func TestWarnToastIdentity(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	cases := []struct {
		name     string
		ddl      []string // %[1]s table, %[2]s index
		identity string   // "" for no warning
		columns  string
	}{
		{name: "default_text", identity: "DEFAULT", columns: `["big","doc"]`, ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, n int, big text, doc jsonb)`,
		}},
		{name: "full_text", ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, big text)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY FULL`,
		}},
		{name: "default_fixed_width", ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, n bigint, ts timestamptz, u uuid)`,
		}},
		{name: "index_text", identity: "USING INDEX", columns: `["big"]`, ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, big text)`,
			`CREATE UNIQUE INDEX %[2]s ON %[1]s (i, id)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
		}},
		{name: "nothing_text", identity: "NOTHING", columns: `["big"]`, ddl: []string{
			`CREATE TABLE %[1]s (id int PRIMARY KEY, big text)`,
			`ALTER TABLE %[1]s REPLICA IDENTITY NOTHING`,
		}},
		{name: "plain_storage", ddl: []string{
			// SET STORAGE PLAIN: never TOASTed, so never sent as unchanged-TOAST.
			`CREATE TABLE %[1]s (id int PRIMARY KEY, small varchar(10))`,
			`ALTER TABLE %[1]s ALTER COLUMN small SET STORAGE PLAIN`,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			is := is.New(t)
			table := fmt.Sprintf("ti_%s_%d", c.name, time.Now().UnixNano())
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
			for _, q := range c.ddl {
				_, err := conn.Exec(ctx, fmt.Sprintf(q, table, "ux_"+table))
				is.NoErr(err)
			}
			var logs bytes.Buffer
			WarnToastIdentity(zerolog.New(&logs).WithContext(ctx), conn, table)
			out := logs.String()
			if c.identity == "" {
				is.Equal(out, "")
				return
			}
			is.Equal(strings.Count(out, "\n"), 1)
			is.True(strings.Contains(out, `"level":"warn"`))
			is.True(strings.Contains(out, `"code":"`+WarnCodeAvroToastIdentity+`"`))
			is.True(strings.Contains(out, `"table":"`+table+`"`))
			is.True(strings.Contains(out, `"replica_identity":"`+c.identity+`"`))
			is.True(strings.Contains(out, `"toastable_columns":`+c.columns))
			is.True(strings.Contains(out, ErrorCodeAvroUnchangedToast))
			is.True(strings.Contains(out, `ALTER TABLE \"`+table+`\" REPLICA IDENTITY FULL`))
		})
	}

	t.Run("catalog read fails: no refusal, no panic", func(t *testing.T) {
		is := is.New(t)
		var logs bytes.Buffer
		WarnToastIdentity(zerolog.New(&logs).WithContext(ctx), conn, "no_such_table_ti")
		is.True(strings.Contains(logs.String(), `"level":"warn"`))
		is.True(!strings.Contains(logs.String(), WarnCodeAvroToastIdentity))
	})
}
