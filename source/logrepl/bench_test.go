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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// benchmarkRelation mirrors the v021-cdc-parity workload row shape exactly:
// `id bigint PRIMARY KEY, payload text NOT NULL` (see
// conduit/benchi/v021-cdc-parity/infra/init.sql). The parity benchmark is the
// throughput target this benchmark guards, so its schema is authoritative.
func benchmarkRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "bench_source_0",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
			{Name: "payload", DataType: pgtype.TextOID},
		},
	}
}

func benchmarkInsertTuple() *pglogrepl.TupleData {
	return &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("9223372036854775807")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")},
		},
	}
}

// BenchmarkRelationSetValues profiles the WAL-tuple -> map[string]any decode,
// the innermost per-column loop, on the parity row shape.
func BenchmarkRelationSetValues(b *testing.B) {
	rs := internal.NewRelationSet()
	rs.Add(benchmarkRelation())
	tuple := benchmarkInsertTuple()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := rs.Values(42, tuple)
		if err != nil {
			b.Fatal(err)
		}
		if len(values) != 2 {
			b.Fatalf("got %d values, want 2", len(values))
		}
	}
}

// BenchmarkCDCHandlerHandleInsert profiles the full WAL-message -> opencdc.Record
// conversion for an INSERT (decode + position + key + payload + metadata) on the
// parity row shape.
func BenchmarkCDCHandlerHandleInsert(b *testing.B) {
	h := newBenchHandler()

	tuple := benchmarkInsertTuple()
	msg := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      tuple,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.handleInsert(ctx, msg, 0x1); err != nil {
			b.Fatal(err)
		}
		// Drop the accumulated record so the batch does not grow unbounded.
		h.recordBatch = h.recordBatch[:0]
	}
}

// BenchmarkCDCHandlerHandleUpdate profiles the UPDATE path (decode old+new,
// Before+After), which is the heaviest per-record path in the parity workload.
func BenchmarkCDCHandlerHandleUpdate(b *testing.B) {
	h := newBenchHandler()

	oldTuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")},
		},
	}
	newTuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")},
		},
	}
	msg := &pglogrepl.UpdateMessage{
		RelationID:   42,
		OldTupleType: 'O',
		OldTuple:     oldTuple,
		NewTuple:     newTuple,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.handleUpdate(ctx, msg, 0x1); err != nil {
			b.Fatal(err)
		}
		h.recordBatch = h.recordBatch[:0]
	}
}

// newBenchHandler builds a handler wired for the parity relation, with the
// drift machinery in its steady (already-decided) state.
func newBenchHandler() *CDCHandler {
	rs := internal.NewRelationSet()
	rs.Add(benchmarkRelation())
	return &CDCHandler{
		relationSet:        rs,
		tableKeys:          map[string]string{"bench_source_0": "id"},
		withAvroSchema:     false,
		batchSize:          1 << 30, // never flush inside the timed loop
		flushInterval:      time.Hour,
		recordBatch:        make([]opencdc.Record, 0, 1<<20),
		basePosition:       position.Position{Type: position.TypeCDC},
		schemaDriftPolicy:  SchemaDriftPolicyHalt,
		undecidedRelations: map[uint32]struct{}{},
		seenShapes:         map[uint32]map[string]*pglogrepl.RelationMessage{},
	}
}
