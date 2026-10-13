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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// Under Avro, date/timestamp/timestamptz 'infinity' used to be written as the
// int 1 / -1 of pgx's InfinityModifier, i.e. 1970-01-02 and 1969-12-31 for a
// date: silent corruption. They must halt instead, and be strings without Avro.
func TestSource_TemporalInfinity(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	types := []string{"date", "timestamp", "timestamptz"}
	values := []struct{ name, sql, text string }{
		{"infinity", "'infinity'", "infinity"},
		{"neginfinity", "'-infinity'", "-infinity"},
		{"finite", "'2024-01-02'", ""},
		{"NULL", "NULL", ""},
	}
	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			for _, typ := range types {
				for _, v := range values {
					if avro && v.sql == "NULL" {
						continue // #326: the Avro schema is not nullable
					}
					t.Run(fmt.Sprintf("%s/avro=%t/%s/%s", path, avro, typ, v.name), func(t *testing.T) {
						is := is.New(t)
						table := fmt.Sprintf("tinf_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
						_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v %s)`, table, typ))
						is.NoErr(err)
						dropReplication(ctx, t, table)
						t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
						insert := fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", table, v.sql)
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
						if avro && v.text != "" {
							is.True(err != nil) // halt, never 1970-01-02 / 1969-12-31
							msg := err.Error()
							is.True(strings.Contains(msg, "postgres.source.temporal_not_representable"))
							is.True(strings.Contains(msg, table))
							is.True(strings.Contains(msg, `"v"`))
							is.True(strings.Contains(msg, fmt.Sprintf("%q", v.text)))
							is.True(strings.Contains(msg, "logrepl.withAvroSchema"))
							return
						}
						is.NoErr(err)
						got := payloadValue(ctx, t, rec, avro)
						switch {
						case v.text != "":
							is.Equal(got, v.text)
						case v.sql == "NULL":
							is.True(got == nil)
						default:
							_, ok := got.(time.Time)
							is.True(ok) // finite values keep their type
						}
					})
				}
			}
		}
	}
}
