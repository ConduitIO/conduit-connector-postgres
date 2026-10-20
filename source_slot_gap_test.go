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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TestSource_Open_SlotMissingWithPosition: Source.Open with a CDC position and
// no slot fails with the coded error and creates nothing; the opt-in
// logrepl.allowWALGap (parsed from the pipeline config) lets it start.
func TestSource_Open_SlotMissingWithPosition(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := fmt.Sprintf("slot_gap_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
	t.Cleanup(func() {
		_ = logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{URL: test.RepmgrConnString, SlotName: table, PublicationName: table})
	})
	slots := func() int {
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	newSrc := func(extra map[string]string) sdk.Source {
		cfg := map[string]string{
			"url": test.RepmgrConnString, "tables": table, "cdcMode": "logrepl",
			"snapshotMode": "never", "logrepl.slotName": table, "logrepl.publicationName": table,
			"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
		}
		for k, v := range extra {
			cfg[k] = v
		}
		src := Connector.NewSource()
		if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), Connector.NewSpecification().SourceParams); err != nil {
			t.Fatal(err)
		}
		return src
	}
	// A CDC position from an earlier run, as the engine hands it back.
	pos := opencdc.Position(`{"version":3,"type":2,"last_lsn":"0/1000","tx_commit_lsn":"0/1000","tx_seq":1}`)

	src := newSrc(nil)
	err := src.Open(ctx, pos)
	if err == nil {
		_ = src.Teardown(ctx)
		t.Fatal("Open succeeded on a missing slot with a saved position")
	}
	if logrepl.CodeOf(err) != "postgres.source.slot_wal_gap" {
		t.Fatalf("Open error = %v, want code postgres.source.slot_wal_gap", err)
	}
	if !strings.Contains(err.Error(), table) || !strings.Contains(err.Error(), "0/1000") {
		t.Fatalf("error does not name the slot and the saved LSN: %v", err)
	}
	_ = src.Teardown(ctx)
	if n := slots(); n != 0 {
		t.Fatalf("a refused Open left %d slot(s) behind", n)
	}

	src = newSrc(map[string]string{"logrepl.allowWALGap": "true"})
	if err := src.Open(ctx, pos); err != nil {
		t.Fatalf("Open with logrepl.allowWALGap=true: %v", err)
	}
	if n := slots(); n != 1 {
		t.Fatalf("slots after the accepted gap = %d, want 1", n)
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := src.Teardown(tctx); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
}
