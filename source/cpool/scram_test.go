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

package cpool_test

import (
	"os"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// TestDefaultSCRAMCompat: the default channel binding of pgx 5.9 ("prefer") is
// mapped to "disable", what pgx 5.7 did; an explicit choice is kept.
func TestDefaultSCRAMCompat(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"default", "postgres://u:p@localhost/db", "disable"},
		{"explicit prefer maps too", "postgres://u:p@localhost/db?channel_binding=prefer", "disable"},
		{"explicit disable", "postgres://u:p@localhost/db?channel_binding=disable", "disable"},
		{"explicit require stays", "postgres://u:p@localhost/db?channel_binding=require", "require"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			cfg, err := pgx.ParseConfig(tc.url)
			is.NoErr(err)
			cpool.DefaultSCRAMCompat(&cfg.Config)
			is.Equal(cfg.ChannelBinding, tc.want)

			pcfg, err := pgxpool.ParseConfig(tc.url)
			is.NoErr(err)
			cpool.DefaultSCRAMCompat(&pcfg.ConnConfig.Config)
			is.Equal(pcfg.ConnConfig.ChannelBinding, tc.want)
		})
	}

	t.Run("pgx default is prefer", func(t *testing.T) {
		// If pgx stops defaulting to "prefer" this test says so, and the
		// mapping can be reviewed.
		is := is.New(t)
		cfg, err := pgx.ParseConfig("postgres://u:p@localhost/db")
		is.NoErr(err)
		is.Equal(cfg.ChannelBinding, "prefer")
	})
}

// TestPool_SCRAMChannelBinding connects to servers that pgx 5.9's default
// channel binding cannot authenticate against, and that v0.14.3 could:
//
//   - PGX_TLS_ED25519_URL: a Postgres with ssl=on and an Ed25519 certificate
//     (pgx: "SCRAM channel binding negotiation error (28000)");
//   - PGX_TLS_PROXY_URL: a TLS re-terminating proxy that passes the SASL
//     exchange through ("channel binding check failed"). PgBouncer is not an
//     example: it authenticates the client itself and does not advertise PLUS.
//
// Both need SCRAM-SHA-256 users and sslmode=require. The test is skipped when
// the variables are not set; CI has no such servers.
func TestPool_SCRAMChannelBinding(t *testing.T) {
	for _, env := range []string{"PGX_TLS_ED25519_URL", "PGX_TLS_PROXY_URL"} {
		t.Run(env, func(t *testing.T) {
			url := os.Getenv(env)
			if url == "" {
				t.Skipf("%s is not set", env)
			}
			is := is.New(t)

			ctx := t.Context()
			pool, err := cpool.New(ctx, url)
			is.NoErr(err)
			defer pool.Close()

			var one int
			is.NoErr(pool.QueryRow(ctx, "SELECT 1").Scan(&one))
			is.Equal(one, 1)
		})
	}
}
