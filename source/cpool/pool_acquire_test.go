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
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// TestPool_AcquireMatchesConnectionKind pins the pool behavior the connector
// relies on and that pgxpool changed internally between pgx 5.7 and 5.9 (the
// acquire loop became bounded, BeforeAcquire became a wrapper of PrepareConn):
// a request for a replication connection never gets a regular one and the
// other way around, even when the pool is full of idle connections of the
// other kind.
func TestPool_AcquireMatchesConnectionKind(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)

	pool, err := cpool.New(ctx, test.RepmgrConnString+"&pool_max_conns=3")
	is.NoErr(err)
	t.Cleanup(pool.Close)

	kind := func(c *pgxpool.Conn) string {
		return c.Conn().Config().RuntimeParams["replication"]
	}

	// Fill the pool with idle regular connections.
	var conns []*pgxpool.Conn
	for range 3 {
		c, err := pool.Acquire(ctx)
		is.NoErr(err)
		is.Equal(kind(c), "")
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Release()
	}

	// A replication request must skip, and replace, all of them.
	rc, err := pool.Acquire(cpool.WithReplication(ctx))
	is.NoErr(err)
	is.Equal(kind(rc), "database")
	rc.Release() // replication connections are disposed of on release

	// And a regular request afterwards gets a regular connection again.
	c, err := pool.Acquire(ctx)
	is.NoErr(err)
	is.Equal(kind(c), "")
	c.Release()
}
