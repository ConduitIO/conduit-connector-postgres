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

package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// parityFields mirrors the v021-cdc-parity row shape: id bigint + payload text.
var parityFields = []pgconn.FieldDescription{
	{Name: "id", DataTypeOID: pgtype.Int8OID},
	{Name: "payload", DataTypeOID: pgtype.TextOID},
}

func parityValues(id int64) []any {
	return []any{id, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}
}

// wideFields mirrors a wider production table (8 columns) to show how the
// per-row cost scales with column count.
var wideFields = []pgconn.FieldDescription{
	{Name: "id", DataTypeOID: pgtype.Int8OID},
	{Name: "name", DataTypeOID: pgtype.TextOID},
	{Name: "email", DataTypeOID: pgtype.TextOID},
	{Name: "created_at", DataTypeOID: pgtype.TimestamptzOID},
	{Name: "count", DataTypeOID: pgtype.Int4OID},
	{Name: "amount", DataTypeOID: pgtype.NumericOID},
	{Name: "active", DataTypeOID: pgtype.BoolOID},
	{Name: "note", DataTypeOID: pgtype.TextOID},
}

func wideValues(id int64) []any {
	return []any{
		id,
		"John Doe",
		"john.doe@example.com",
		time.Unix(1_700_000_000, 0).UTC(),
		int32(42),
		mustNumeric("1234.56"),
		true,
		"some longer note string over here",
	}
}

func mustNumeric(s string) any {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		panic(err)
	}
	return n
}

// newBenchWorker returns a FetchWorker and Iterator wired for the parity shape,
// so the benchmarks can drive the exact unexported per-row code path.
func newBenchWorker(_ []pgconn.FieldDescription) (*FetchWorker, *Iterator) {
	f := &FetchWorker{
		conf: FetchConfig{
			Table:          "bench_source_0",
			Key:            "id",
			Position:       position.Position{Type: position.TypeInitial},
			FetchSize:      defaultFetchSize,
			WithAvroSchema: false,
		},
		snapshotEnd: 1_000_000,
	}
	lastPos := position.Position{
		Type:      position.TypeSnapshot,
		Snapshots: position.SnapshotPositions{"bench_source_0": {LastRead: 0, SnapshotEnd: 1_000_000}},
	}
	it := &Iterator{
		conf: Config{
			Tables:         []string{"bench_source_0"},
			TableKeys:      map[string]string{"bench_source_0": "id"},
			WithAvroSchema: false,
		},
		lastPosition: lastPos,
		codec:        position.NewSnapshotPositionCodec(lastPos),
		acks:         ackBarrier{},
	}
	return f, it
}

// BenchmarkBuildFetchData profiles fetch_worker.buildFetchData (decode + key
// coercion + per-column FormatColumn + map assembly) on the parity shape.
func BenchmarkBuildFetchData(b *testing.B) {
	f, _ := newBenchWorker(parityFields)
	vals := parityValues(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals[0] = int64(i)
		if _, err := f.buildFetchData(parityFields, vals); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildFetchDataWide is the same as above on the wide shape.
func BenchmarkBuildFetchDataWide(b *testing.B) {
	f, _ := newBenchWorker(wideFields)
	vals := wideValues(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals[0] = int64(i)
		if _, err := f.buildFetchData(wideFields, vals); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildRecord profiles iterator.buildRecord (position JSON marshal +
// metadata + SDK record construction) on the parity shape. This is the per-row
// cost that is pure Go CPU, independent of the database.
func BenchmarkBuildRecord(b *testing.B) {
	_, it := newBenchWorker(parityFields)
	d := FetchData{
		Key:      opencdc.StructuredData{"id": int64(1)},
		Payload:  opencdc.StructuredData{"id": int64(1), "payload": "xxxxxxxx"},
		Position: position.SnapshotPosition{LastRead: 1, SnapshotEnd: 1_000_000},
		Table:    "bench_source_0",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Position.LastRead = int64(i)
		_ = it.buildRecord(d)
	}
}

// BenchmarkBuildRecordWide is buildRecord with a multi-table position, which
// makes the JSON-marshaled position larger and the per-row cost higher.
func BenchmarkBuildRecordWide(b *testing.B) {
	_, it := newBenchWorker(parityFields)
	for t := 0; t < 8; t++ {
		it.lastPosition.Snapshots[fmt.Sprintf("bench_source_%d", t)] = position.SnapshotPosition{
			LastRead: 12345, SnapshotEnd: 1_000_000,
		}
	}
	d := FetchData{
		Key:      opencdc.StructuredData{"id": int64(1)},
		Payload:  opencdc.StructuredData{"id": int64(1), "payload": "xxxxxxxx"},
		Position: position.SnapshotPosition{LastRead: 1, SnapshotEnd: 1_000_000},
		Table:    "bench_source_0",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Position.LastRead = int64(i)
		_ = it.buildRecord(d)
	}
}

// BenchmarkDBCursorRaw measures the DB + pgx streaming ceiling for the same
// table BenchmarkEndToEnd snapshots: DECLARE/FETCH ALL with rows.Next() only
// (no value decode). It is the "how fast could the database stream rows" floor
// that the full snapshot path is compared against.
func BenchmarkDBCursorRaw(b *testing.B) {
	pool := benchDBPool(b)
	defer pool.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "DECLARE c CURSOR FOR SELECT * FROM bench_snapshot"); err != nil {
			b.Fatal(err)
		}
		rows, err := tx.Query(ctx, "FETCH ALL FROM c")
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		_ = tx.Rollback(ctx)
		if n != 1_000_000 {
			b.Fatalf("got %d rows, want 1000000", n)
		}
	}
}

// BenchmarkDBCursorValues is the streaming ceiling plus rows.Values() decode,
// isolating the cost pgx pays to decode every column into Go values.
func BenchmarkDBCursorValues(b *testing.B) {
	pool := benchDBPool(b)
	defer pool.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "DECLARE c CURSOR FOR SELECT * FROM bench_snapshot"); err != nil {
			b.Fatal(err)
		}
		rows, err := tx.Query(ctx, "FETCH ALL FROM c")
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			if _, err := rows.Values(); err != nil {
				b.Fatal(err)
			}
			n++
		}
		rows.Close()
		_ = tx.Rollback(ctx)
		if n != 1_000_000 {
			b.Fatalf("got %d rows, want 1000000", n)
		}
	}
}

func benchDBPool(b *testing.B) *pgxpool.Pool {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		b.Skip("PG_DSN not set; skipping DB-backed snapshot benchmark")
	}
	ctx := context.Background()
	pool, err := cpool.New(ctx, dsn)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	return pool
}

// BenchmarkEndToEnd runs the full snapshot iterator against a real Postgres
// and drains every record, measuring the wall-clock throughput and allocations
// of the whole production read path (cursor FETCH -> rows.Values -> decode ->
// build -> channel -> record construction -> ack).
//
// Requires PG_DSN pointing at a Postgres with wal_level=logical (the snapshot
// read itself only needs a normal connection; logical is asserted only for
// parity with the connector's environment). Skips when PG_DSN is unset.
func BenchmarkEndToEnd(b *testing.B) {
	ctx := context.Background()
	pool := benchDBPool(b)
	defer pool.Close()

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS bench_snapshot;
		CREATE TABLE bench_snapshot (id bigint PRIMARY KEY, payload text NOT NULL);
		INSERT INTO bench_snapshot
		SELECT g, md5(g::text) FROM generate_series(1, 1000000) g;`); err != nil {
		b.Fatalf("setup: %v", err)
	}

	it, err := NewIterator(ctx, pool, Config{
		Tables:         []string{"bench_snapshot"},
		TableKeys:      map[string]string{"bench_snapshot": "id"},
		FetchSize:      defaultFetchSize,
		WithAvroSchema: false,
	})
	if err != nil {
		b.Fatalf("NewIterator: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var count int
	start := time.Now()
	for {
		recs, err := it.NextN(ctx, defaultFetchSize)
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			b.Fatalf("NextN: %v", err)
		}
		for range recs {
			_ = it.Ack(ctx, nil)
		}
		count += len(recs)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	if count != 1_000_000 {
		b.Fatalf("got %d records, want 1000000", count)
	}
	b.ReportMetric(float64(count)/elapsed.Seconds(), "rec/s")
	b.ReportMetric(elapsed.Seconds(), "total_s")
}
