// Copyright © 2025 Meroxa, Inc.
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

package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestSQLIdentWrapping(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	cases := []struct {
		ident       string
		testName    string
		expectError bool
	}{
		{"just_a_name", "common case", false},
		{"слон", "unicode chars", false},
		{"test table", "spaces", false},
		{"TEST_table", "uppercase letters", false},
		{`'test_table'`, "single quotes", false},
		{"tes`t_table", "apostrophe", false},
		{`te"st_table`, "double quotes", true},
	}

	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			w := WrapSQLIdent(c.ident)

			t.Cleanup(func() {
				if c.expectError {
					return
				}

				query := fmt.Sprintf("DROP TABLE %s", w)
				_, err := conn.Exec(context.Background(), query)
				is.NoErr(err)
			})

			query := fmt.Sprintf("CREATE TABLE %s (%s int)", w, w)
			_, err := conn.Exec(context.Background(), query)

			if c.expectError {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), `(SQLSTATE 42601)`)) // syntax error
			} else {
				is.NoErr(err)
			}
		})
	}
}
