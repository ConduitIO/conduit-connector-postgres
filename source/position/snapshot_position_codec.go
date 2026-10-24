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
	"encoding/json"
	"sort"
	"strconv"

	"github.com/conduitio/conduit-commons/opencdc"
)

// SnapshotPositionCodec incrementally encodes snapshot-typed positions whose
// only per-record change is one table's last_read. It produces bytes that are
// IDENTICAL to json.Marshal applied to the same Position (see
// ToSDKPosition), but it does the cheap work once and the per-record work
// without reflection.
//
// A snapshot position is almost entirely constant for the life of a snapshot:
// version, type, the table set, every table's snapshot_end, and the DBZ-3
// carry-forward fields (SnapshotLowWatermarkLSN, SchemaHistory) never change
// while the snapshot runs. The only thing that advances per record is the
// current table's last_read. The naive encoding — json.Marshal of the whole
// Position per record — pays the reflection cost of re-encoding the constant
// map and struct for every row, which the end-to-end snapshot profile shows
// dominating the source's per-record CPU (see source/snapshot/bench_test.go).
//
// The codec caches the derived byte fragments (the sorted table order, the
// JSON-escaped table names, the constant trailing fields) and assembles each
// record's position with strconv.AppendInt, so a record costs one output copy
// instead of several boxed reflect allocations.
//
// Byte-identity is the correctness contract: resume reads these positions back
// with ParseSDKPosition, and the destination/engine compare them as opaque
// bytes. A divergence would be a serialized-position change (invariant 2). The
// property test pins byte-identity against json.Marshal across a wide input
// space (see TestSnapshotPositionCodec_ByteIdentical).
//
// Not safe for concurrent use: it owns a scratch buffer and caches keyed on the
// current snapshot map size. The snapshot iterator drives it from its single
// NextN goroutine.
type SnapshotPositionCodec struct {
	// lsn and shLen are the values the cached tail was built from. Rebuild the
	// tail if either changes, so a future caller that mutates a carry-forward
	// field mid-snapshot cannot silently emit a stale position.
	lsn   string
	shLen int
	tail  []byte // ,"snapshot_low_watermark_lsn":... and/or ,"schema_history":..., or nil

	order []string // sorted snapshot keys (encoding/json sorts map keys)
	names [][]byte // JSON-encoded (escaped, quoted) table names, aligned with order

	buf []byte
}

// NewSnapshotPositionCodec builds a codec for p, caching every fragment that is
// constant across a snapshot. p must be snapshot-typed.
func NewSnapshotPositionCodec(p Position) *SnapshotPositionCodec {
	c := &SnapshotPositionCodec{}
	c.rebuildTail(p)
	c.rebuildOrder(p.Snapshots)
	return c
}

// Encode returns the snapshot position for p as bytes identical to
// p.ToSDKPosition(). It reads the current snapshots map (the only field that
// changes per record) and reassembles the position without re-encoding the
// constant parts.
func (c *SnapshotPositionCodec) Encode(p Position) opencdc.Position {
	if len(p.Snapshots) != len(c.order) {
		c.rebuildOrder(p.Snapshots)
	}
	c.rebuildTail(p)

	buf := c.buf[:0]
	buf = append(buf, `{"version":`...)
	buf = strconv.AppendInt(buf, int64(CurrentPositionVersion), 10)
	buf = append(buf, `,"type":`...)
	buf = strconv.AppendInt(buf, int64(TypeSnapshot), 10)

	if len(c.order) > 0 {
		buf = append(buf, `,"snapshots":{`...)
		for i, k := range c.order {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, c.names[i]...)
			buf = append(buf, `:{"last_read":`...)
			buf = strconv.AppendInt(buf, p.Snapshots[k].LastRead, 10)
			buf = append(buf, `,"snapshot_end":`...)
			buf = strconv.AppendInt(buf, p.Snapshots[k].SnapshotEnd, 10)
			buf = append(buf, '}')
		}
		buf = append(buf, '}')
	}

	buf = append(buf, c.tail...)
	buf = append(buf, '}')

	c.buf = buf

	// The record keeps this slice past the next Encode call, so it must be an
	// independent copy, never the reused scratch buffer.
	return append(opencdc.Position(nil), buf...)
}

func (c *SnapshotPositionCodec) rebuildTail(p Position) {
	if p.SnapshotLowWatermarkLSN == c.lsn && len(p.SchemaHistory) == c.shLen {
		return
	}
	c.lsn = p.SnapshotLowWatermarkLSN
	c.shLen = len(p.SchemaHistory)

	tail := make([]byte, 0, 64)
	if p.SnapshotLowWatermarkLSN != "" {
		tail = append(tail, `,"snapshot_low_watermark_lsn":`...)
		lsn, err := json.Marshal(p.SnapshotLowWatermarkLSN)
		if err != nil {
			// A string always marshals; this cannot fail in practice.
			panic(err)
		}
		tail = append(tail, lsn...)
	}
	if len(p.SchemaHistory) > 0 {
		tail = append(tail, `,"schema_history":`...)
		sh, err := json.Marshal(p.SchemaHistory)
		if err != nil {
			// SchemaHistories is plain data; this cannot fail in practice.
			panic(err)
		}
		tail = append(tail, sh...)
	}
	c.tail = tail
}

func (c *SnapshotPositionCodec) rebuildOrder(snapshots SnapshotPositions) {
	order := make([]string, 0, len(snapshots))
	for k := range snapshots {
		order = append(order, k)
	}
	sort.Strings(order)

	names := make([][]byte, len(order))
	for i, k := range order {
		name, err := json.Marshal(k)
		if err != nil {
			// A string always marshals; this cannot fail in practice.
			panic(err)
		}
		names[i] = name
	}

	c.order = order
	c.names = names
}
