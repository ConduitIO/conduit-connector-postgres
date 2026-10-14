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
	"os"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestDestination_Open_AppliesSCRAMCompat runs under `make test` (the TLS test
// below needs `make test-tls`): Open must map pgx's default
// channel_binding=prefer to disable. If Open goes back to pgx.Connect(url), the
// connection keeps prefer and this fails.
func TestDestination_Open_AppliesSCRAMCompat(t *testing.T) {
	tests := []struct {
		name string
		url  string
		env  string // PGCHANNELBINDING, read by pgx when the URL has no channel_binding
		want string
	}{
		{"default", test.RegularConnString, "", "disable"},
		{"explicit prefer", test.RegularConnString + "&channel_binding=prefer", "", "disable"},
		{"explicit disable", test.RegularConnString + "&channel_binding=disable", "", "disable"},
		{"env prefer", test.RegularConnString, "prefer", "disable"},
		{"env disable", test.RegularConnString, "disable", "disable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := t.Context()
			t.Setenv("PGCHANNELBINDING", tc.env)

			// not NewDestination(): the middleware wrapper hides the connection
			d := &Destination{stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
			is.NoErr(sdk.Util.ParseConfig(ctx,
				map[string]string{"url": tc.url, "table": "t"},
				d.Config(),
				Connector.NewSpecification().DestinationParams,
			))
			is.NoErr(d.Open(ctx))
			defer func() { is.NoErr(d.Teardown(ctx)) }()

			is.Equal(d.conn.Config().ChannelBinding, tc.want)
		})
	}
}

// TestDestination_Open_SCRAMChannelBinding opens the destination against the
// servers described in cpool.TestPool_SCRAMChannelBinding (PGX_TLS_ED25519_URL,
// PGX_TLS_PROXY_URL). Skipped when they are not set; `make test-tls` sets them.
func TestDestination_Open_SCRAMChannelBinding(t *testing.T) {
	for _, env := range []string{"PGX_TLS_ED25519_URL", "PGX_TLS_PROXY_URL"} {
		t.Run(env, func(t *testing.T) {
			url := os.Getenv(env)
			if url == "" {
				t.Skipf("%s is not set", env)
			}
			is := is.New(t)
			ctx := t.Context()

			d := NewDestination()
			is.NoErr(sdk.Util.ParseConfig(ctx,
				map[string]string{"url": url, "table": "t"},
				d.Config(),
				Connector.NewSpecification().DestinationParams,
			))
			is.NoErr(d.Open(ctx))
			is.NoErr(d.Teardown(ctx))
		})
	}
}
