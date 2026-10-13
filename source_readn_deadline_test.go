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
