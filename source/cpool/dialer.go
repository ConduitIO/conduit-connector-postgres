// Copyright © 2024 Meroxa, Inc.
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

package cpool

import (
	"context"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/jackc/pgx/v5/pgconn"
)

// ApplyHostDialer installs the dialer the host supplied on ctx (see package
// github.com/conduitio/conduit-commons/dialer) on cfg, so that every
// connection made from cfg opens its network connection through it.
//
// pgx copies cfg for each connection it makes, so the function installed here
// is captured once and is also used by connections opened later: pool growth,
// replacement of a closed connection, replication connections, and the extra
// connection pgx makes to send a query cancel request. It also applies to
// every fallback host of cfg, because pgx has a single DialFunc per config.
//
// pgx resolves host names before it dials, which would keep names away from
// the host's dialer. With a host dialer present, names are passed through
// unresolved so the dialer sees the address the user configured and decides
// how to resolve it.
//
// When ctx carries no dialer, cfg is left untouched and pgx keeps its own
// dialer and resolver.
func ApplyHostDialer(ctx context.Context, cfg *pgconn.Config) {
	dial, ok := dialer.FromContext(ctx)
	if !ok {
		return
	}
	cfg.DialFunc = pgconn.DialFunc(dial)
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
}
