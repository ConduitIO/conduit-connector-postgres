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

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-connector-postgres/test/hostdialer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// These tests need no database. A connector pointed at a counting listener
// shows, by the listener's accepted count, whether a connection reached the
// network.

var errRefusingDialer = errors.New("refusing dialer: not allowed")

func refusingContext(t *testing.T) (context.Context, *hostdialer.Dialer) {
	t.Helper()
	d := &hostdialer.Dialer{Err: errRefusingDialer}
	return dialer.WithDialContext(context.Background(), d.DialContext), d
}

func TestDestination_Open_RefusingDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	ctx, d := refusingContext(t)

	dest := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"url": "postgres://u:p@" + addr + "/db?sslmode=disable"},
		dest.Config(), Connector.NewSpecification().DestinationParams))

	err := dest.Open(ctx)
	is.True(errors.Is(err, errRefusingDialer))
	is.Equal(d.Count(), 1)
	is.Equal(accepted(), 0)
}

func TestDestination_Open_NoDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	ctx := context.Background()

	dest := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"url": "postgres://u:p@" + addr + "/db?sslmode=disable"},
		dest.Config(), Connector.NewSpecification().DestinationParams))

	err := dest.Open(ctx)
	is.True(err != nil)
	is.True(!errors.Is(err, errRefusingDialer))
	is.True(accepted() >= 1) // connected directly, as before
}

func sourceConfig(t *testing.T, url string) map[string]string {
	t.Helper()
	return map[string]string{
		"url": url, "tables": "t1", "cdcMode": "logrepl", "snapshotMode": "never",
		"logrepl.slotName": "dialer_slot", "logrepl.publicationName": "dialer_pub",
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}
}

func TestSource_Open_RefusingDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	ctx, d := refusingContext(t)

	src := Connector.NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, sourceConfig(t, "postgres://u:p@"+addr+"/db?sslmode=disable"),
		src.Config(), Connector.NewSpecification().SourceParams))

	err := src.Open(ctx, nil)
	is.True(errors.Is(err, errRefusingDialer))
	is.True(d.Count() >= 1)
	is.Equal(accepted(), 0)
	is.NoErr(src.Teardown(context.Background()))
}

func TestSource_Open_NoDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	ctx := context.Background()

	src := Connector.NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, sourceConfig(t, "postgres://u:p@"+addr+"/db?sslmode=disable"),
		src.Config(), Connector.NewSpecification().SourceParams))

	err := src.Open(ctx, nil)
	is.True(err != nil)
	is.True(!errors.Is(err, errRefusingDialer))
	is.True(accepted() >= 1)
	is.NoErr(src.Teardown(context.Background()))
}

// TestSource_LifecycleOnDeleted_RefusingDialer covers the connection the
// slot and publication cleanup opens when a pipeline is deleted.
func TestSource_LifecycleOnDeleted_RefusingDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := hostdialer.CountingListener(t)
	ctx, d := refusingContext(t)

	src := &Source{}
	err := src.LifecycleOnDeleted(ctx, sourceConfig(t, "postgres://u:p@"+addr+"/db?sslmode=disable"))
	is.True(errors.Is(err, errRefusingDialer))
	is.True(d.Count() >= 1)
	is.Equal(accepted(), 0)
}
