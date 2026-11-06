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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// The source emits json and jsonb as strings since v0.21.0 and as []byte
// before; records of both kinds can reach the destination (a pipeline that is
// upgraded with records in flight, a DLQ replay, another connector). Both
// must be written into json and jsonb columns as the document they hold,
// never as a JSON string literal wrapping it and never base64. The same for
// the string elements of a jsonb[] (the shape of a json[]/jsonb[] from the
// source without Avro).
func TestDestination_JSONFromStringAndBytes(t *testing.T) {
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	doc := `{"z": 1, "a": [12345678901234567890, 1.50], "u": "日本 😀 é"}`
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"string", doc},
		{"bytes", []byte(doc)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			table := fmt.Sprintf("jsondst_%d_%d", time.Now().UnixMicro()%1000000, numericTableSeq.Add(1))
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, j json, jb jsonb, jba jsonb[])", table))
			is.NoErr(err)
			t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

			d := NewDestination()
			is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
				"url": test.RegularConnString, "table": table,
			}, d.Config(), Connector.NewSpecification().DestinationParams))
			is.NoErr(d.Open(ctx))
			t.Cleanup(func() { _ = d.Teardown(context.Background()) })

			recs := []opencdc.Record{{
				Operation: opencdc.OperationCreate,
				Key:       opencdc.StructuredData{"id": int64(1)},
				Payload: opencdc.Change{After: opencdc.StructuredData{
					"j": tc.v, "jb": tc.v, "jba": []any{doc, nil, "null"},
				}},
			}, {
				Operation: opencdc.OperationCreate,
				Key:       opencdc.StructuredData{"id": int64(2)},
				Payload: opencdc.Change{After: opencdc.StructuredData{
					"j": nil, "jb": nil, "jba": nil,
				}},
			}}
			n, err := d.Write(ctx, recs)
			is.NoErr(err)
			is.Equal(n, len(recs))

			var j, jb, jbaFirst, jbaThird *string
			var jbaSecondNull, isObject *bool
			is.NoErr(conn.QueryRow(
				ctx, fmt.Sprintf(
					"SELECT j::text, jb::text, jba[1]::text, jba[2] IS NULL, jba[3]::text, jsonb_typeof(jb) = 'object' FROM %s WHERE id = 1", table,
				),
			).Scan(&j, &jb, &jbaFirst, &jbaSecondNull, &jbaThird, &isObject))
			is.Equal(*j, doc)                                                           // json stores the text as sent
			is.Equal(*jb, `{"a": [12345678901234567890, 1.50], "u": "日本 😀 é", "z": 1}`) // jsonb's canonical form of the same document
			is.True(*isObject)                                                          // an object, not a JSON string holding the document
			is.Equal(*jbaFirst, *jb)
			is.True(*jbaSecondNull)
			is.Equal(*jbaThird, "null")

			var nullJ, nullJB, nullJBA bool
			is.NoErr(conn.QueryRow(
				ctx, fmt.Sprintf(
					"SELECT j IS NULL, jb IS NULL, jba IS NULL FROM %s WHERE id = 2", table,
				),
			).Scan(&nullJ, &nullJB, &nullJBA))
			is.True(nullJ && nullJB && nullJBA) // SQL NULL, not the JSON null literal
		})
	}
}
