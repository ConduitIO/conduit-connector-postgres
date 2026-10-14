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

// TestDefaultSCRAMCompat: the default channel binding of pgx 5.9 and later ("prefer") is
// mapped to "disable", what pgx 5.7 did; an explicit choice is kept.
func TestDefaultSCRAMCompat(t *testing.T) {
	tests := []struct {
		name string
		url  string
		env  string // PGCHANNELBINDING, read by pgx when the URL has no channel_binding
		want string
	}{
		{"default", "postgres://u:p@localhost/db", "", "disable"},
		{"explicit prefer maps too", "postgres://u:p@localhost/db?channel_binding=prefer", "", "disable"},
		{"explicit disable", "postgres://u:p@localhost/db?channel_binding=disable", "", "disable"},
		{"explicit require stays", "postgres://u:p@localhost/db?channel_binding=require", "", "require"},
		{"PGCHANNELBINDING=prefer maps too", "postgres://u:p@localhost/db", "prefer", "disable"},
		{"PGCHANNELBINDING=disable", "postgres://u:p@localhost/db", "disable", "disable"},
		{"PGCHANNELBINDING=require stays", "postgres://u:p@localhost/db", "require", "require"},
		{"URL wins over PGCHANNELBINDING", "postgres://u:p@localhost/db?channel_binding=require", "disable", "require"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			t.Setenv("PGCHANNELBINDING", tc.env)

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

// TestNew_AppliesSCRAMCompat: cpool.New applies the mapping to the pool it
// builds. pgxpool connects lazily, so no server is needed.
func TestNew_AppliesSCRAMCompat(t *testing.T) {
	for url, want := range map[string]string{
		"postgres://u:p@127.0.0.1:1/db":                         "disable",
		"postgres://u:p@127.0.0.1:1/db?channel_binding=require": "require",
	} {
		is := is.New(t)
		pool, err := cpool.New(t.Context(), url)
		is.NoErr(err)
		is.Equal(pool.Config().ConnConfig.ChannelBinding, want)
		pool.Close()
	}
}

// TestPool_SCRAMChannelBinding connects to servers that the default of pgx 5.9 and later
// channel binding cannot authenticate against, and that the pgx 5.7 releases could:
//
//   - PGX_TLS_ED25519_URL: a Postgres with ssl=on and an Ed25519 certificate
//     (pgx: "SCRAM channel binding negotiation error (28000)");
//   - PGX_TLS_PROXY_URL: a TLS re-terminating proxy that passes the SASL
//     exchange through ("channel binding check failed"). PgBouncer is not an
//     example: it authenticates the client itself and does not advertise PLUS.
//
// Both need SCRAM-SHA-256 users and sslmode=require. The test is skipped when
// the variables are not set, which is the case under `make test`. The `tls`
// workflow (`make test-tls`, test/tls/run.sh) starts both servers and fails if
// the test skips.
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
