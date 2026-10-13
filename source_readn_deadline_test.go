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
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TestSource_ReadN_DeadlineDoesNotWedgeCDC is the regression test for #347.
// A ReadN whose context expires while the source is in CDC mode must end only
// that call: rows written later are still returned, and Teardown returns.
func TestSource_ReadN_DeadlineDoesNotWedgeCDC(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	table := fmt.Sprintf("readn_deadline_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })

	src := Connector.NewSource()
	err := sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RepmgrConnString, "tables": table, "cdcMode": "logrepl",
		"logrepl.slotName": table, "logrepl.publicationName": table,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, src.Config(), Connector.NewSpecification().SourceParams)
	if err != nil {
		t.Fatal(err)
	}
	openCtx, cancelOpen := context.WithCancel(ctx)
	defer cancelOpen()
	if err := src.Open(openCtx, nil); err != nil { // empty table: straight to CDC
		t.Fatal(err)
	}

	// 1. an idle ReadN whose context expires
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = src.ReadN(rctx, 1)
	cancel()
	t.Logf("idle ReadN: %v", err)

	// 2. a row is inserted, the next ReadN must return it
	if _, err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	rctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	recs, err := src.ReadN(rctx, 1)
	cancel()
	if err != nil || len(recs) != 1 {
		t.Errorf("row inserted after the expired ReadN was not returned within 10s: %d records, err %v", len(recs), err)
	}

	// 3. Teardown must return
	cancelOpen()
	done := make(chan error, 1)
	go func() { done <- src.Teardown(ctx) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Teardown did not return within 30s")
	}
}

// TestSource_StopDuringStartup is the public-API probe for #346: Open on an
// empty table (straight to CDC), ReadN in a goroutine, cancel the open context
// after 0 to 5.5 ms, then Teardown. Whatever the timing, Teardown must return,
// nothing may panic, and the replication slot must be droppable afterwards
// (no replication connection left behind).
func TestSource_StopDuringStartup(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)
	const runs = 60

	for i := 0; i < runs; i++ {
		delay := time.Duration(float64(i) / float64(runs-1) * 5.5 * float64(time.Millisecond))
		name := fmt.Sprintf("stopstart_%d_%d", i, time.Now().UnixNano()%100000000)

		if _, err := conn.Exec(ctx, "CREATE TABLE "+name+" (id bigint primary key, v text)"); err != nil {
			t.Fatal(err)
		}

		src := Connector.NewSource()
		err := sdk.Util.ParseConfig(ctx, map[string]string{
			"url": test.RepmgrConnString, "tables": name, "cdcMode": "logrepl",
			"logrepl.slotName": name, "logrepl.publicationName": name,
			"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
		}, src.Config(), Connector.NewSpecification().SourceParams)
		if err != nil {
			t.Fatal(err)
		}

		openCtx, cancelOpen := context.WithCancel(ctx)
		if err := src.Open(openCtx, nil); err != nil {
			t.Fatalf("run %d: open: %v", i, err)
		}

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			_, _ = src.ReadN(openCtx, 1)
		}()

		time.Sleep(delay)
		cancelOpen()

		tdone := make(chan error, 1)
		go func() {
			tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			tdone <- src.Teardown(tctx)
		}()
		select {
		case <-tdone:
		case <-time.After(40 * time.Second):
			t.Fatalf("run %d (cancel at %v): Teardown did not return", i, delay)
		}
		select {
		case <-readDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("run %d: ReadN did not return after cancel", i)
		}

		// The slot must not stay held by a leaked replication connection. The
		// server notices a closed connection asynchronously, so allow it a
		// moment before calling it a leak.
		var dropErr error
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if _, dropErr = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name); dropErr == nil {
				break
			}
		}
		if dropErr != nil {
			t.Fatalf("run %d (cancel at %v): slot still in use 5s after Teardown: %v", i, delay, dropErr)
		}
		_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+name)
		_, _ = conn.Exec(ctx, "DROP TABLE "+name)
	}
}
