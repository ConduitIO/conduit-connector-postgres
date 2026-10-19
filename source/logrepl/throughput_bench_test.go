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
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
)

// newThroughputHandler builds a handler wired for the parity relation, with the
// drift machinery in its steady (already-decided) state and a REAL change key
// (as production wires via handler.changeKey = sub.CurrentChange), so every
// emitted record carries TxCommitLSN + TxSeq and pays the full position
// JSON-marshal cost the production path does.
func newThroughputHandler(batchSize int, out chan<- []opencdc.Record) *CDCHandler {
	rs := internal.NewRelationSet()
	rs.Add(benchmarkRelation())
	h := &CDCHandler{
		relationSet:        rs,
		tableKeys:          map[string]string{"bench_source_0": "id"},
		withAvroSchema:     false,
		batchSize:          batchSize,
		flushInterval:      time.Hour,
		recordBatch:        make([]opencdc.Record, 0, batchSize),
		out:                out,
		basePosition:       position.Position{Type: position.TypeCDC},
		schemaDriftPolicy:  SchemaDriftPolicyHalt,
		undecidedRelations: map[uint32]struct{}{},
		seenShapes:         map[uint32]map[string]*pglogrepl.RelationMessage{},
		changeKey: func() internal.ChangeKey {
			return internal.ChangeKey{CommitLSN: 0xABCDEF01, Seq: 7}
		},
	}
	return h
}

// BenchmarkCDCSourceThroughput measures the connector source's end-to-end
// ceiling: a producer goroutine feeds the handler the parity 70/20/10
// INSERT/UPDATE/DELETE mix through Handle (the subscription goroutine's job),
// while the benchmark goroutine drains batches the way CDCIterator.NextN does
// and acks every record the way CDCIterator.Ack does (parse + LSN + TxCommit).
// It reports records/s and per-record cost, isolating the connector from the
// engine and the network.
func BenchmarkCDCSourceThroughput(b *testing.B) {
	for _, batchSize := range []int{1, 100, 1000, 2048, 4096} {
		b.Run("batch"+strconv.Itoa(batchSize), func(b *testing.B) {
			out := make(chan []opencdc.Record, 1)
			h := newThroughputHandler(batchSize, out)
			ctx := context.Background()

			// Mirror the 70/20/10 mix: 7 inserts, 2 updates, 1 delete per 10.
			ins := &pglogrepl.InsertMessage{RelationID: 42, Tuple: benchmarkInsertTuple()}
			oldT := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")},
			}}
			newT := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")},
			}}
			upd := &pglogrepl.UpdateMessage{RelationID: 42, OldTupleType: 'O', OldTuple: oldT, NewTuple: newT}
			del := &pglogrepl.DeleteMessage{RelationID: 42, OldTupleType: 'O', OldTuple: oldT}

			n := b.N
			var lsn pglogrepl.LSN = 0x1

			// producer: feed Handle in a goroutine, like the subscription.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < n; i++ {
					var m pglogrepl.Message
					switch i % 10 {
					case 0, 1, 2, 3, 4, 5, 6:
						m = ins
					case 7, 8:
						m = upd
					default:
						m = del
					}
					if _, err := h.Handle(ctx, m, lsn); err != nil {
						b.Errorf("handle: %v", err)
						return
					}
					lsn++
				}
				h.flush(ctx)
			}()

			// consumer: drain batches + ack, like NextN + Ack.
			b.ReportAllocs()
			b.ResetTimer()
			acked := 0
			for acked < n {
				batch := <-out
				for _, rec := range batch {
					pos, err := position.ParseSDKPosition(rec.Position)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := pos.LSN(); err != nil {
						b.Fatal(err)
					}
					if _, err := pos.TxCommit(); err != nil {
						b.Fatal(err)
					}
					acked++
				}
			}
			b.StopTimer()
			<-done

			elapsed := b.Elapsed()
			b.ReportMetric(float64(n)/elapsed.Seconds(), "records/s")
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(n), "ns/record")
		})
	}
}
