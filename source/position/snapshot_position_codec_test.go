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

package position

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"
)

// genSnapshotPosition builds a random snapshot-typed Position with the fields a
// snapshot position actually carries. The CDC-only fields (LastLSN,
// TxCommitLSN, TxSeq, SchemaDriftMarker) stay zero, as they are for every
// snapshot position.
func genSnapshotPosition(t *rapid.T) Position {
	p := Position{Type: TypeSnapshot}

	nTables := rapid.IntRange(0, 5).Draw(t, "nTables")
	if nTables > 0 {
		p.Snapshots = make(SnapshotPositions, nTables)
		for i := 0; i < nTables; i++ {
			name := rapid.String().Draw(t, "table")
			p.Snapshots[name] = SnapshotPosition{
				LastRead:    rapid.Int64().Draw(t, "lastRead"),
				SnapshotEnd: rapid.Int64().Draw(t, "snapshotEnd"),
			}
		}
	}

	if rapid.Bool().Draw(t, "hasLSN") {
		p.SnapshotLowWatermarkLSN = rapid.String().Draw(t, "lsn")
	}

	if rapid.Bool().Draw(t, "hasHistory") {
		p.SchemaHistory = make(SchemaHistories)
		nEntries := rapid.IntRange(1, 3).Draw(t, "nHistory")
		for i := 0; i < nEntries; i++ {
			name := rapid.String().Draw(t, "historyTable")
			versions := make([]SchemaVersion, rapid.IntRange(1, 3).Draw(t, "nVersions"))
			for j := range versions {
				versions[j] = SchemaVersion{ColumnSetHash: rapid.String().Draw(t, "hash")}
				if rapid.Bool().Draw(t, "hasFirstSeen") {
					versions[j].FirstSeenLSN = rapid.String().Draw(t, "firstSeen")
				}
			}
			p.SchemaHistory[name] = versions
		}
	}

	return p
}

// TestSnapshotPositionCodec_ByteIdentical is the correctness contract: the
// incremental codec must produce exactly the bytes json.Marshal produces for
// the same position, across a wide input space. A divergence here is a
// serialized-position change, which would break resume (invariant 2).
func TestSnapshotPositionCodec_ByteIdentical(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := genSnapshotPosition(t)
		codec := NewSnapshotPositionCodec(p)

		// Encode the position as-is, then encode again after mutating the
		// current table's last_read (the per-record path) to exercise the
		// scratch-buffer reuse across records.
		assertByteIdentical(t, p, codec.Encode(p))

		for name := range p.Snapshots {
			p.Snapshots[name] = SnapshotPosition{
				LastRead:    rapid.Int64().Draw(t, "lastRead2"),
				SnapshotEnd: p.Snapshots[name].SnapshotEnd,
			}
			assertByteIdentical(t, p, codec.Encode(p))
		}
	})
}

// TestSnapshotPositionCodec_GrowingSnapshots exercises the first-run/resume
// path where the snapshots map starts empty (or partially populated) and grows
// as each table's fetcher emits its first record. The codec must stay
// byte-identical while it rebuilds its cached order.
func TestSnapshotPositionCodec_GrowingSnapshots(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		full := genSnapshotPosition(t)
		codec := NewSnapshotPositionCodec(full)

		grown := Position{
			Type:                    TypeSnapshot,
			SnapshotLowWatermarkLSN: full.SnapshotLowWatermarkLSN,
			SchemaHistory:           full.SchemaHistory,
			Snapshots:               make(SnapshotPositions),
		}
		assertByteIdentical(t, grown, codec.Encode(grown))

		for name, sp := range full.Snapshots {
			grown.Snapshots[name] = sp
			assertByteIdentical(t, grown, codec.Encode(grown))
		}
	})
}

// fatalf is satisfied by both *testing.T and *rapid.T.
type fatalf interface{ Fatalf(string, ...any) }

func assertByteIdentical(t fatalf, p Position, got []byte) {
	want := p.ToSDKPosition()
	if !bytes.Equal(want, got) {
		t.Fatalf("codec output diverges from json.Marshal\nwant: %s\ngot:  %s", want, got)
	}
}

// TestSnapshotPositionCodec_ExactBytes pins the encoding for a representative
// position, including HTML-escaping and carry-forward fields, so a regression
// in the hand-assembled layout is caught without a randomized diff.
func TestSnapshotPositionCodec_ExactBytes(t *testing.T) {
	p := Position{
		Type: TypeSnapshot,
		Snapshots: SnapshotPositions{
			"bench<&table": {LastRead: 12345, SnapshotEnd: 1000000},
			"other":        {LastRead: 7, SnapshotEnd: 99},
		},
		SnapshotLowWatermarkLSN: "0/16B6C50",
		SchemaHistory: SchemaHistories{
			"bench<&table": {{ColumnSetHash: "abc123", FirstSeenLSN: "0/16B0000"}},
		},
	}

	got := NewSnapshotPositionCodec(p).Encode(p)
	want := p.ToSDKPosition()
	if !bytes.Equal(want, got) {
		t.Fatalf("codec output diverges from json.Marshal\nwant: %s\ngot:  %s", want, got)
	}
}

func BenchmarkSnapshotPositionCodecEncode(b *testing.B) {
	p := Position{
		Type:      TypeSnapshot,
		Snapshots: SnapshotPositions{"bench_snapshot": {LastRead: 0, SnapshotEnd: 1_000_000}},
	}
	codec := NewSnapshotPositionCodec(p)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Snapshots["bench_snapshot"] = SnapshotPosition{LastRead: int64(i), SnapshotEnd: 1_000_000}
		_ = codec.Encode(p)
	}
}

func BenchmarkPositionToSDKPosition(b *testing.B) {
	p := Position{
		Type:      TypeSnapshot,
		Snapshots: SnapshotPositions{"bench_snapshot": {LastRead: 0, SnapshotEnd: 1_000_000}},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Snapshots["bench_snapshot"] = SnapshotPosition{LastRead: int64(i), SnapshotEnd: 1_000_000}
		_ = p.ToSDKPosition()
	}
}
