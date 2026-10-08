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

package logrepl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
)

// BenchmarkCDCThroughput is the connector-local CDC throughput measurement
// for DBZ-3 B2 (parent design decision 4: connector-local, postgres->log
// shape, A/A floor beside every number). Each iteration inserts
// benchCDCRows rows in one statement, then reads and acks all of them
// through the CDC iterator, which is the hot path B2 touches (Handle's
// heartbeat check, the status update's gate). It reports records/s.
//
// Docker-gated like the rest of the package; benchmarks only run with
// -bench, so `make test` never runs this. Reproduce with:
//
//	go test -run '^$' -bench BenchmarkCDCThroughput -benchtime 5x -count 6 ./source/logrepl/
func BenchmarkCDCThroughput(b *testing.B) {
	for _, withHeartbeat := range []bool{false, true} {
		b.Run(fmt.Sprintf("heartbeat=%v", withHeartbeat), func(b *testing.B) {
			benchCDCThroughput(b, withHeartbeat)
		})
	}
}

// BenchmarkHandleHeartbeatCheck measures the per-message cost B2 adds to
// CDCHandler.Handle for a data change (not a heartbeat): the early
// handleHeartbeat check. No database needed.
func BenchmarkHandleHeartbeatCheck(b *testing.B) {
	ctx := context.Background()
	msg := &pglogrepl.InsertMessage{RelationID: 7}
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("heartbeat=%v", enabled), func(b *testing.B) {
			h := &CDCHandler{}
			if enabled {
				h.enableHeartbeat(DefaultHeartbeatSchema, DefaultHeartbeatTable)
				h.heartbeatRelID = 42
			}
			for n := 0; n < b.N; n++ {
				if h.handleHeartbeat(ctx, msg, 100) {
					b.Fatal("data change classified as heartbeat")
				}
			}
		})
	}
}

const benchCDCRows = 20000

func benchCDCThroughput(b *testing.B, withHeartbeat bool) {
	ctx := context.Background()
	pool, err := cpool.New(ctx, test.RepmgrConnString)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	table := fmt.Sprintf("bench_cdc_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id bigserial PRIMARY KEY, column1 varchar(256), column2 integer)`, table)); err != nil {
		b.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE %q`, table)) }()

	hb := HeartbeatConfig{}
	if withHeartbeat {
		hb = HeartbeatConfig{Enabled: true, Interval: time.Second, Schema: DefaultHeartbeatSchema, Table: table + "_hb"}
		defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+hb.qualifiedName()) }()
	}

	i, err := NewCDCIterator(ctx, pool, CDCConfig{
		Tables:            []string{table},
		TableKeys:         map[string]string{table: "id"},
		PublicationName:   table,
		SlotName:          table,
		BatchSize:         1000,
		SchemaDriftPolicy: SchemaDriftPolicyHalt,
		Heartbeat:         hb,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := i.StartSubscriber(ctx); err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = i.Teardown(ctx)
		_ = Cleanup(ctx, CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	}()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %q (column1, column2) SELECT 'row-' || g, g FROM generate_series(1, %d) g`, table, benchCDCRows)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for got := 0; got < benchCDCRows; {
			recs, err := i.NextN(ctx, 1000)
			if err != nil {
				b.Fatal(err)
			}
			for _, r := range recs {
				if err := i.Ack(ctx, r.Position); err != nil {
					b.Fatal(err)
				}
			}
			got += len(recs)
		}
	}
	b.ReportMetric(float64(b.N*benchCDCRows)/b.Elapsed().Seconds(), "records/s")
}
