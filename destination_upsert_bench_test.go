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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// BenchmarkDestination_Write_Upsert measures the end-to-end cost of the
// destination write path for the CDC-parity shape: a keyed upsert into a
// two-column table (id bigint PRIMARY KEY, payload text), matching the
// benchi/v021-cdc-parity conduit-pg arm's bench_dest_i. It is the number to
// compare when changing the write path (SQL generation, batching, query exec
// mode), and it reports allocations so a shape-memoization regression shows up
// as a jump back toward per-record squirrel builds.
//
// Run with -cpuprofile/-memprofile to attribute the cost. Two query exec modes
// are measured: the default (describe_exec, which describes every statement on
// the connection that runs it: two round trips per batch) and an explicit
// cache_statement with the schema guard (one round trip, correct under ALTER
// COLUMN ... TYPE on a direct connection only, #436). It is a microbenchmark of
// the write path on one machine: indicative, not a benchi result.
func BenchmarkDestination_Write_Upsert(b *testing.B) {
	for _, batchSize := range []int{1, 1000} {
		for _, mode := range []struct {
			name string
			url  string
		}{
			{"default", test.RegularConnString},                                            // describe_exec
			{"cache", test.RegularConnString + "&default_query_exec_mode=cache_statement"}, // + schema guard
		} {
			b.Run(fmt.Sprintf("batch=%d/mode=%s", batchSize, mode.name), func(b *testing.B) {
				benchmarkUpsert(b, mode.url, batchSize)
			})
		}
	}
}

func benchmarkUpsert(b *testing.B, url string, batchSize int) {
	b.Helper()
	ctx := b.Context()

	table := "bench_dest"
	if err := withBenchConn(ctx, func(conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, table))
		if err != nil {
			return err
		}
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id bigint PRIMARY KEY, payload text)`, table))
		return err
	}); err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = withBenchConn(ctx, func(conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %q`, table))
			return err
		})
	}()

	d := NewDestination()
	if err := sdk.Util.ParseConfig(ctx, map[string]string{"url": url, "table": table}, d.Config(),
		Connector.NewSpecification().DestinationParams); err != nil {
		b.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Teardown(context.Background()) })

	// The records are not mutated by Write, so one slice is reused, as the
	// engine does when it fills a batch.
	recs := make([]opencdc.Record, batchSize)
	for i := range recs {
		recs[i] = opencdc.Record{
			Position:  opencdc.Position(fmt.Sprintf("p/%d", i)),
			Operation: opencdc.OperationCreate,
			Metadata:  map[string]string{opencdc.MetadataCollection: table},
			Key:       opencdc.StructuredData{"id": int64(i)},
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"payload": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			}},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := d.Write(ctx, recs); err != nil {
			b.Fatal(err)
		}
	}
}

func withBenchConn(ctx context.Context, fn func(*pgx.Conn) error) error {
	conn, err := pgx.Connect(ctx, test.RegularConnString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return fn(conn)
}
