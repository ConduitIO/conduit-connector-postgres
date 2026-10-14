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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

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
