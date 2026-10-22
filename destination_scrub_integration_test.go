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
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// Needs the test database. Every way a Write can fail on a row value must
// return an error without the value: the server's own error, a value that
// does not encode (found by locateEncodeFailure and failWrite), a trigger's
// RAISE text, and a CHECK failure that quotes the row.
func TestDestination_Write_ErrorsDoNotContainRowValues(t *testing.T) {
	const secret = "customer-SECRET-4711"
	for name, tc := range map[string]struct {
		setup string // after CREATE TABLE %q (id int PRIMARY KEY, qty int, note text)
		after opencdc.StructuredData
	}{
		"value does not encode":    {"", opencdc.StructuredData{"qty": secret}},
		"unique violation":         {"CREATE UNIQUE INDEX ON %[1]q (note)", opencdc.StructuredData{"note": secret}},
		"check failure quotes row": {"ALTER TABLE %[1]q ADD CHECK (note <> '" + secret + "')", opencdc.StructuredData{"note": secret}},
		"trigger raise": {`CREATE FUNCTION %[1]q_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected ` + secret + `'; END $$;
			CREATE TRIGGER t BEFORE INSERT ON %[1]q FOR EACH ROW EXECUTE FUNCTION %[1]q_f()`, opencdc.StructuredData{"note": "x"}},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			ctx := test.Context(t)
			conn := test.ConnectSimple(ctx, t, test.RegularConnString)

			table := test.RandomIdentifier(t)
			_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int PRIMARY KEY, qty int, note text)`, table))
			is.NoErr(err)
			t.Cleanup(func() {
				_, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE %q CASCADE`, table))
				_, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION IF EXISTS %q`, table+"_f"))
			})
			if tc.setup != "" {
				_, err = conn.Exec(ctx, fmt.Sprintf(tc.setup, table))
				is.NoErr(err)
			}
			if strings.Contains(name, "unique") {
				_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (id, note) VALUES (100, $1)`, table), secret)
				is.NoErr(err)
			}

			d := NewDestination()
			is.NoErr(sdk.Util.ParseConfig(ctx,
				map[string]string{"url": test.RegularConnString, "table": table},
				d.Config(), Connector.NewSpecification().DestinationParams))
			is.NoErr(d.Open(ctx))
			defer func() { is.NoErr(d.Teardown(ctx)) }()

			after := tc.after
			after["id"] = 1
			_, err = d.Write(ctx, []opencdc.Record{{
				Operation: opencdc.OperationCreate,
				Key:       opencdc.StructuredData{"id": 1},
				Payload:   opencdc.Change{After: after},
			}})
			is.True(err != nil)
			is.True(!strings.Contains(err.Error(), secret))
		})
	}
}
