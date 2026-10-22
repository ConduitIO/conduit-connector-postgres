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
	"net/url"
	"strings"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/conduitio/conduit-connector-postgres/test/hostdialer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// forwardingDialer returns a dialer that records its calls and forwards them to
// the real test server, a URL for the same server under a made-up host name
// (so a connection that skipped the dialer could not resolve it), and the
// context carrying the dialer.
func forwardingDialer(t *testing.T, connString string) (*hostdialer.Dialer, string, context.Context) {
	t.Helper()
	u, err := url.Parse(connString)
	if err != nil {
		t.Fatal(err)
	}
	d := &hostdialer.Dialer{Target: u.Host}
	u.Host = "db.example.test:5432"
	return d, u.String(), dialer.WithDialContext(test.Context(t), d.DialContext)
}

// TestDestination_HostDialer_ConnectAndReconnect: the destination connects
// through the host dialer and, after pgx closes its connection, reconnects
// through it again even though the reconnect runs on a context without one.
func TestDestination_HostDialer_ConnectAndReconnect(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	table, _ := newTeardownTable(ctx, t)
	d, hostURL, dctx := forwardingDialer(t, test.RegularConnString)

	dest := &Destination{stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"url": hostURL, "table": table},
		dest.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(dest.Open(dctx))
	t.Cleanup(func() { _ = dest.Teardown(context.Background()) })
	is.Equal(d.Calls(), []string{"tcp db.example.test:5432"})
	is.NoErr(dest.conn.Ping(ctx))

	is.NoErr(dest.conn.Close(ctx))
	is.NoErr(dest.ensureConn(ctx)) // ctx has no dialer: the captured one is used
	is.Equal(d.Count(), 2)
	is.NoErr(dest.conn.Ping(ctx))
}

// TestSource_HostDialer_SnapshotAndCDC: every connection a source opens (pool
// connections for discovery, snapshot reads and the replication connection)
// goes through the host dialer, and Teardown's cleanup connection does too.
func TestSource_HostDialer_SnapshotAndCDC(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := setupSourceTable(ctx, t, conn)
	insertSourceRow(ctx, t, conn, table)
	d, hostURL, dctx := forwardingDialer(t, test.RepmgrConnString)

	slot := strings.ToLower(table)
	cfg := map[string]string{
		"url": hostURL, "tables": table, "snapshotMode": "initial", "cdcMode": "logrepl",
		"logrepl.slotName": slot, "logrepl.publicationName": slot,
	}
	src := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, src.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(src.Open(dctx, nil))

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recs, err := src.ReadN(readCtx, 1) // from the snapshot, on a pool connection
	is.NoErr(err)
	is.Equal(len(recs), 1)
	// At least the pool connection and the replication connection.
	is.True(d.Count() >= 2)
	for _, c := range d.Calls() {
		is.Equal(c, "tcp db.example.test:5432")
	}

	is.NoErr(src.Teardown(ctx))
	before := d.Count()
	srcDel := &Source{}
	is.NoErr(srcDel.LifecycleOnDeleted(dctx, cfg)) // drops the slot and publication
	is.True(d.Count() > before)
}
