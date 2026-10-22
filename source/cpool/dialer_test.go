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
	"errors"
	"net/url"
	"reflect"
	"testing"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-connector-postgres/test/hostdialer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
)

var errRefusingDialer = errors.New("refusing dialer: not allowed")

const exampleURL = "postgres://u@db.example.test:5432/db?sslmode=disable"

func TestNew_RefusingDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	d := &hostdialer.Dialer{Err: errRefusingDialer}
	ctx := dialer.WithDialContext(context.Background(), d.DialContext)

	// The URL points at the listener, so a connection that bypassed the
	// dialer would be counted.
	pool, err := New(ctx, "postgres://u:p@"+addr+"/db?sslmode=disable")
	is.NoErr(err)
	t.Cleanup(pool.Close)

	err = pool.Ping(context.Background())
	is.True(errors.Is(err, errRefusingDialer))
	is.Equal(d.Count(), 1)
	is.Equal(accepted(), 0)
}

// TestNew_DialerSeesConfiguredHost: with a host dialer the host name reaches it
// unresolved, so the dialer decides how to resolve it.
func TestNew_DialerSeesConfiguredHost(t *testing.T) {
	is := is.New(t)
	d := &hostdialer.Dialer{Err: errRefusingDialer}
	pool, err := New(dialer.WithDialContext(context.Background(), d.DialContext), exampleURL)
	is.NoErr(err)
	t.Cleanup(pool.Close)

	err = pool.Ping(context.Background())
	is.True(errors.Is(err, errRefusingDialer))
	is.Equal(d.Calls(), []string{"tcp db.example.test:5432"})
}

// TestNew_DialerUsedForEveryConnection: connections made after New, on a
// context that carries no dialer, still use the captured one: new acquires
// (pool growth, retry after a failed connect) and replication connections.
func TestNew_DialerUsedForEveryConnection(t *testing.T) {
	is := is.New(t)
	d := &hostdialer.Dialer{Err: errRefusingDialer}
	pool, err := New(dialer.WithDialContext(context.Background(), d.DialContext), exampleURL)
	is.NoErr(err)
	t.Cleanup(pool.Close)

	bg := context.Background() // no dialer on this context
	for i := 0; i < 3; i++ {
		_, err = pool.Acquire(bg)
		is.True(errors.Is(err, errRefusingDialer))
	}
	_, err = pool.Acquire(WithReplication(bg))
	is.True(errors.Is(err, errRefusingDialer))
	is.Equal(d.Count(), 4)
}

func TestNew_DialerAppliesToFallbackHosts(t *testing.T) {
	is := is.New(t)
	d := &hostdialer.Dialer{Err: errRefusingDialer}
	u := url.URL{Scheme: "postgres", User: url.UserPassword("u", "p"), Host: "a.example.test:5432,b.example.test:5433", Path: "/db", RawQuery: "sslmode=disable"}
	pool, err := New(dialer.WithDialContext(context.Background(), d.DialContext), u.String())
	is.NoErr(err)
	t.Cleanup(pool.Close)

	_, err = pool.Acquire(context.Background())
	is.True(errors.Is(err, errRefusingDialer))
	is.Equal(d.Calls(), []string{"tcp a.example.test:5432", "tcp b.example.test:5433"})
}

// TestNew_NoDialer: without a host dialer nothing changes, pgx dials the
// address itself.
func TestNew_NoDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	pool, err := New(context.Background(), "postgres://u:p@"+addr+"/db?sslmode=disable")
	is.NoErr(err)
	t.Cleanup(pool.Close)

	err = pool.Ping(context.Background())
	is.True(err != nil) // the listener hangs up before the handshake
	is.True(!errors.Is(err, errRefusingDialer))
	is.True(accepted() >= 1)
}

func TestApplyHostDialer_NoDialerLeavesConfigUntouched(t *testing.T) {
	is := is.New(t)
	cfg, err := pgconn.ParseConfig(exampleURL)
	is.NoErr(err)
	dial, lookup := cfg.DialFunc, cfg.LookupFunc

	ApplyHostDialer(context.Background(), cfg)

	is.True(cfg.DialFunc != nil && cfg.LookupFunc != nil)
	// Func values cannot be compared; the identical code pointer is enough to
	// show they were not replaced by the pass-through ones.
	is.Equal(funcPtr(cfg.DialFunc), funcPtr(dial))
	is.Equal(funcPtr(cfg.LookupFunc), funcPtr(lookup))
}

func funcPtr(f any) uintptr { return reflect.ValueOf(f).Pointer() }
