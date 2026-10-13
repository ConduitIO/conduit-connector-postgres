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
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// legacyURLEnv names an optional connection URL for an older server. The
// replica identity query reads pg_index, whose columns differ across versions
// (indnkeyatts exists from Postgres 11), so CI runs it on the test server and
// a developer can point it at a Postgres 10 with
// CONDUIT_POSTGRES_LEGACY_URL=postgres://user:pass@host:port/db.
const legacyURLEnv = "CONDUIT_POSTGRES_LEGACY_URL"

// TestCheckReplicaIdentity_ServerVersions runs CheckReplicaIdentity against
// the test server and, if set, the legacy server. On Postgres 10 a direct
// reference to pg_index.indnkeyatts failed Open for every table with
// SQLSTATE 42703.
func TestCheckReplicaIdentity_ServerVersions(t *testing.T) {
	servers := map[string]string{"test": test.RegularConnString}
	if u := os.Getenv(legacyURLEnv); u != "" {
		servers["legacy"] = u
	}
	for name, url := range servers {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, url)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close(context.Background()) })

			var versionNum int
			if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&versionNum); err != nil {
				t.Fatalf("server_version_num: %v", err)
			}
			t.Logf("server_version_num=%d", versionNum)

			type tc struct {
				name    string
				ddl     []string // %[1]s table, %[2]s index
				wantErr bool
				minVer  int
			}
			cases := []tc{
				{name: "default", ddl: []string{`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL)`}},
				{name: "full", ddl: []string{
					`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL)`,
					`ALTER TABLE %[1]s REPLICA IDENTITY FULL`,
				}},
				{name: "index_with_pk", ddl: []string{
					`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL)`,
					`CREATE UNIQUE INDEX %[2]s ON %[1]s (i, id)`,
					`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
				}},
				{name: "index_without_pk", wantErr: true, ddl: []string{
					`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL)`,
					`CREATE UNIQUE INDEX %[2]s ON %[1]s (i)`,
					`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
				}},
				{name: "index_include_pk", wantErr: true, minVer: 110000, ddl: []string{
					`CREATE TABLE %[1]s (id int PRIMARY KEY, i int NOT NULL)`,
					`CREATE UNIQUE INDEX %[2]s ON %[1]s (i) INCLUDE (id)`,
					`ALTER TABLE %[1]s REPLICA IDENTITY USING INDEX %[2]s`,
				}},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					is := is.New(t)
					if versionNum < c.minVer {
						t.Skipf("needs server_version_num >= %d", c.minVer)
					}
					table := fmt.Sprintf("ri_%s_%d", c.name, time.Now().UnixNano())
					index := "ux_" + table
					t.Cleanup(func() {
						_, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
					})
					for _, q := range c.ddl {
						_, err := conn.Exec(ctx, fmt.Sprintf(q, table, index))
						is.NoErr(err)
					}
					err := CheckReplicaIdentity(ctx, conn, table, "id")
					if !c.wantErr {
						is.NoErr(err)
						return
					}
					is.True(err != nil)
					is.True(strings.HasPrefix(err.Error(), ErrorCodeReplicaIdentityExcludesKey+": "))
				})
			}
		})
	}
}
