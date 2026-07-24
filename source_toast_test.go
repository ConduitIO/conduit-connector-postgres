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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestSource_UnchangedToast_ThroughMiddleware is the regression test for
// conduit-connector-postgres#317 (unchanged TOASTed column emitted as NULL)
// and the FULL-identity half of #383 (the same UPDATE halting under Avro) on
// the v0.14 line. It drives NewSource(), so records go through the SDK's
// default middleware, including the Avro encoding that is on by default.
//
// The UPDATE changes a small column and leaves a ~12.8 KB out-of-line value
// alone; Postgres then sends that column as 'u' (no data). Before the fix the
// connector decoded 'u' as NULL: Avro on halted ("*avro.null is unsupported
// for Avro string"), Avro off wrote NULL over the real value.
//
// Not covered, still open as #383: REPLICA IDENTITY DEFAULT with Avro on. The
// column is omitted (correctly, never NULL) but the payload schema requires
// it, so the SDK fails to encode. Nothing in the WAL can supply the value.
func TestSource_UnchangedToast_ThroughMiddleware(t *testing.T) {
	big := strings.Repeat("0123456789abcdef", 800) // 12800 bytes, stored out of line

	cases := []struct {
		name      string
		identity  string // empty: DEFAULT
		withAvro  bool
		wantToast bool // true: After carries the unchanged value; false: the field is omitted
	}{
		{name: "full_avro_on", identity: "FULL", withAvro: true, wantToast: true},
		{name: "full_avro_off", identity: "FULL", withAvro: false, wantToast: true},
		{name: "default_avro_off", identity: "", withAvro: false, wantToast: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			conn := test.ConnectSimple(ctx, t, test.RegularConnString)

			table := test.RandomIdentifier(t)
			_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL, t text NOT NULL)`, table))
			is.NoErr(err)
			// EXTERNAL: out of line and uncompressed, so the value is TOASTed
			// whatever its content.
			_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN t SET STORAGE EXTERNAL`, table))
			is.NoErr(err)
			if tc.identity != "" {
				_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY %s`, table, tc.identity))
				is.NoErr(err)
			}
			_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 10, $1)`, table), big)
			is.NoErr(err)

			slot, pub := "s_"+table, "p_"+table
			s := NewSource()
			err = sdk.Util.ParseConfig(ctx, map[string]string{
				"url":                     test.RepmgrConnString,
				"tables":                  table,
				"snapshotMode":            "never",
				"cdcMode":                 "logrepl",
				"logrepl.slotName":        slot,
				"logrepl.publicationName": pub,
				"logrepl.withAvroSchema":  fmt.Sprint(tc.withAvro),
			}, s.Config(), Connector.NewSpecification().SourceParams)
			is.NoErr(err)
			t.Cleanup(func() {
				is.NoErr(s.Teardown(context.Background()))
				_ = logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
					URL: test.RepmgrConnString, SlotName: slot, PublicationName: pub,
				})
			})
			is.NoErr(s.Open(ctx, nil))

			_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET i = 11 WHERE id = 1`, table))
			is.NoErr(err)
			_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (9, 90, 'z')`, table))
			is.NoErr(err)

			recs := readNThroughMiddleware(ctx, t, s, 2)
			is.Equal(recs[1].Operation, opencdc.OperationCreate) // the source moved past the update

			got := recs[0]
			is.Equal(got.Operation, opencdc.OperationUpdate)
			after := decodeRow(ctx, t, got, got.Payload.After, false)
			is.Equal(after["i"], "11")
			if tc.wantToast {
				is.True(after["t"] == big) // the unchanged value is preserved (not is.Equal: a failure would print 12.8 KB)
			} else {
				_, present := after["t"]
				is.True(!present) // omitted means unchanged; never NULL
			}
		})
	}
}
