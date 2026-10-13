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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TestDestinationTeardownDuringWrite is the regression test for #354: Teardown used to close the pgx connection while a Write was still using it. The SDK
// calls Teardown without a prior Stop when a pipeline stops on an error, and
// cancels the open context first, so a Write can be in flight.
// It needs -race to see the data race that used to be reported.
func TestDestinationTeardownDuringWrite(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("teardown_race_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE "+table) }()

	dst := Connector.NewDestination()
	err := sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RegularConnString, "table": table,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, dst.Config(), Connector.NewSpecification().DestinationParams)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Open(ctx); err != nil {
		t.Fatal(err)
	}

	writing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		id := int64(0)
		for i := 0; ; i++ {
			recs := make([]opencdc.Record, 100)
			for j := range recs {
				id++
				recs[j] = opencdc.Record{
					Operation: opencdc.OperationCreate,
					Key:       opencdc.StructuredData{"id": id},
					Payload:   opencdc.Change{After: opencdc.StructuredData{"id": id, "name": "x"}},
				}
			}
			if i == 2 {
				close(writing)
			}
			if _, err := dst.Write(ctx, recs); err != nil {
				return // the connection was closed under us
			}
		}
	}()

	<-writing
	if err := dst.Teardown(ctx); err != nil {
		t.Fatal(err)
	}
	<-done

	// Invariant 1: after Teardown a Write must fail rather than report records
	// written to a connection that is gone.
	n, err := dst.Write(ctx, []opencdc.Record{{
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": int64(-1)},
		Payload:   opencdc.Change{After: opencdc.StructuredData{"id": int64(-1), "name": "late"}},
	}})
	if err == nil || n != 0 {
		t.Fatalf("Write after Teardown returned n=%d err=%v, want n=0 and an error", n, err)
	}
}
