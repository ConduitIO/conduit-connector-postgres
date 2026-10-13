// Copyright © 2022 Meroxa, Inc.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TestSource_FailedCDCStartInOpenReleasesTheConnection: a second source opens
// on a slot another walsender still holds (a restart after a crash while the
// old walsender lingers). START_REPLICATION fails after the replication
// connection was acquired. Open must report that failure and clean up what it
// created: Source.Open never stores the iterator on failure, so nobody would
// ever tear it down, and every retry used to leak one walsender and make
// Teardown (pool.Close) wait for it.
func TestSource_FailedCDCStartInOpenReleasesTheConnection(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := fmt.Sprintf("open_fail_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
	t.Cleanup(func() {
		_ = logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{URL: test.RepmgrConnString, SlotName: table, PublicationName: table})
	})
	walsenders := func() int {
		var n int
		err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender' AND application_name = $1", table).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	newSrc := func() sdk.Source {
		src := Connector.NewSource()
		err := sdk.Util.ParseConfig(ctx, map[string]string{
			"url": test.RepmgrConnString + "&application_name=" + table, "tables": table, "cdcMode": "logrepl",
			"snapshotMode": "never", "logrepl.slotName": table, "logrepl.publicationName": table,
			"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
		}, src.Config(), Connector.NewSpecification().SourceParams)
		if err != nil {
			t.Fatal(err)
		}
		return src
	}

	a := newSrc()
	if err := a.Open(ctx, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if n := walsenders(); n != 1 {
		t.Fatalf("walsenders with A running = %d, want 1", n)
	}

	b := newSrc()
	err := b.Open(ctx, nil)
	if err == nil {
		t.Fatal("B.Open succeeded on a slot that is held by A")
	}
	if !strings.Contains(err.Error(), "logical replication did not start") {
		t.Fatalf("B.Open error = %v, want it to say the replication did not start", err)
	}
	start := time.Now()
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	terr := b.Teardown(tctx)
	cancel()
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("B.Teardown took %s (err=%v), want under 2s", took, terr)
	}
	time.Sleep(500 * time.Millisecond)
	if n := walsenders(); n != 1 {
		t.Fatalf("walsenders after B's failed open and Teardown = %d, want only A's", n)
	}

	tctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := a.Teardown(tctx); err != nil {
		t.Fatalf("A.Teardown: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if n := walsenders(); n != 0 {
		t.Fatalf("%d walsenders left after both sources were torn down", n)
	}
}
