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
	"github.com/matryer/is"
)

// A NULL in a uuid column was emitted as "" instead of nil (#392). The
// connector now emits the untyped nil on the snapshot path and on the CDC path,
// without Avro. With Avro a NULL cannot be encoded for any column type (#326),
// which is independent of this fix, so only the non-NULL value is read there.
// NULL elements of a uuid[] are nil as well.
func TestSource_UUIDNull(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	const id = "bd94ee0b-564f-4088-bf4e-8d5e626caf66"

	cases := []struct {
		name    string
		colType string
		sql     string
		want    any
		null    bool // involves a NULL, which Avro cannot encode (#326)
	}{
		{name: "NULL", colType: "uuid", sql: "NULL", want: nil, null: true},
		{name: "value", colType: "uuid", sql: "'" + id + "'", want: id},
		{name: "array of NULLs", colType: "uuid[]", sql: "ARRAY[NULL,NULL]::uuid[]", want: []any{nil, nil}, null: true},
	}

	for _, path := range []string{"snapshot", "cdc"} {
		for _, avro := range []bool{true, false} {
			for _, tc := range cases {
				if avro && (tc.null || tc.colType == "uuid[]") {
					continue
				}
				t.Run(fmt.Sprintf("%s/avro=%t/%s", path, avro, tc.name), func(t *testing.T) {
					is := is.New(t)
					table := fmt.Sprintf("uuidnull_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
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
					is.Equal(got, tc.want) // NULL is nil, never ""
					_ = rec.Bytes()        // must serialize
				})
			}
		}
	}
}
