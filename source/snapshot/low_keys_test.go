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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// The tests in this file cover rows keyed 0 or below
// (docs/design-documents/20261102-snapshot-keys-at-or-below-zero.md). Builds
// v0.14.0 to v0.19.0 started every new snapshot at "key > 0" and skipped them.

// delivery is one record a test read from the iterator, and acked.
type delivery struct {
	table string
	id    int64
	pos   position.Position
	raw   []byte
}

// lowKeyTable creates a table holding exactly the given ids.
func lowKeyTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool, ids ...int64) string {
	t.Helper()
	table := test.SetupEmptyTestTable(ctx, t, pool)
	for _, id := range ids {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (id, column1) VALUES ($1, 'x')`, table), id)
		is.New(t).NoErr(err)
	}
	return table
}

// readSnapshot runs a snapshot iterator over tables from the serialized
// position pos and acks every record it reads. With limit > 0 it stops after
// limit records and tears the iterator down without draining it, which is
// what a crash leaves behind: the acked records, and nothing after them. With
// limit 0 it reads until the snapshot is done.
func readSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, pos []byte, limit int, tables ...string) []delivery {
	t.Helper()
	is := is.New(t)
	keys := make(map[string]string, len(tables))
	for _, tb := range tables {
		keys[tb] = "id"
	}
	it, err := NewIterator(ctx, pool, Config{
		Position:  pos,
		Tables:    tables,
		TableKeys: keys,
		FetchSize: 1, // one FETCH per row: every pass boundary is hit
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	var out []delivery
	for limit == 0 || len(out) < limit {
		recs, err := it.NextN(ctx, 1)
		if errors.Is(err, ErrIteratorDone) {
			break
		}
		is.NoErr(err)
		for _, r := range recs {
			p, err := position.ParseSDKPosition(r.Position)
			is.NoErr(err)
			key, ok := r.Key.(opencdc.StructuredData)
			is.True(ok)
			id, ok := key["id"].(int64)
			is.True(ok)
			out = append(out, delivery{table: r.Metadata[opencdc.MetadataCollection], id: id, pos: p, raw: r.Position})
			is.NoErr(it.Ack(ctx, r.Position))
		}
	}
	return out
}

func idsOf(dd []delivery, table string) []int64 {
	var ids []int64
	for _, d := range dd {
		if d.table == table {
			ids = append(ids, d.id)
		}
	}
	return ids
}

// v019SnapshotPosition is a snapshot position exactly as v0.19.0 serialized
// it (format version 4): its entries have only last_read and snapshot_end.
// v0.14.6 writes the same entries (as format version 2), and this branch
// reads both the same way.
// The entries name rows read with "key > 0", so last_read is always >= 1.
func v019SnapshotPosition(entries map[string][2]int64) []byte {
	var parts []string
	for _, tb := range slices.Sorted(maps.Keys(entries)) {
		e := entries[tb]
		parts = append(parts, fmt.Sprintf(`%q:{"last_read":%d,"snapshot_end":%d}`, tb, e[0], e[1]))
	}
	return []byte(`{"version":4,"type":1,"snapshots":{` + strings.Join(parts, ",") +
		`},"snapshot_low_watermark_lsn":"0/1A2B3C8","slot_identity":"0123456789abcdef0123456789abcdef",` +
		`"system_identifier":"7431234567890123456","timeline":1}`)
}

// v0146SnapshotPosition is a snapshot position exactly as v0.14.6
// serialized it (format version 2).
func v0146SnapshotPosition(entries map[string][2]int64) []byte {
	var parts []string
	for _, tb := range slices.Sorted(maps.Keys(entries)) {
		e := entries[tb]
		parts = append(parts, fmt.Sprintf(`%q:{"last_read":%d,"snapshot_end":%d}`, tb, e[0], e[1]))
	}
	return []byte(`{"version":2,"type":1,"snapshots":{` + strings.Join(parts, ",") + `}}`)
}

// TestSnapshot_UpgradeFromV0146Position is TestSnapshot_UpgradeFromV019Position
// from a literal v0.14.6 position.
func TestSnapshot_UpgradeFromV0146Position(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	mid := lowKeyTable(ctx, t, pool, -7, -4, 0, 1, 2, 3)
	fresh := lowKeyTable(ctx, t, pool, -1, 0, 5)

	dd := readSnapshot(ctx, t, pool, v0146SnapshotPosition(map[string][2]int64{mid: {1, 3}}), 0, mid, fresh)
	is.Equal(idsOf(dd, mid), []int64{-7, -4, 0, 2, 3})
	is.Equal(idsOf(dd, fresh), []int64{-1, 0, 5})
}

func TestSnapshot_NewSnapshotReadsKeysAtOrBelowZero(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)

	for _, tc := range []struct {
		name string
		ids  []int64
	}{
		{name: "empty", ids: nil},
		{name: "all negative", ids: []int64{-9, -3, -1}},
		{name: "only zero", ids: []int64{0}},
		{name: "mixed", ids: []int64{-7, 0, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			table := lowKeyTable(ctx, t, pool, tc.ids...)
			dd := readSnapshot(ctx, t, pool, nil, 0, table)
			is.Equal(idsOf(dd, table), tc.ids)
			for _, d := range dd {
				is.True(d.pos.Snapshots[table].FromMin)
				is.Equal(d.pos.Snapshots[table].LowPassLastRead, (*int64)(nil))
			}
		})
	}
}

// TestSnapshot_EntryOnlyAfterARowIsRead pins the assumption FetchWorker's
// resume logic rests on: a table has an entry in a position only once one of
// its rows was read, so an entry's LastRead is a real key, and a table with
// no entry is a new snapshot (no lower bound).
func TestSnapshot_EntryOnlyAfterARowIsRead(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	empty := lowKeyTable(ctx, t, pool)
	full := lowKeyTable(ctx, t, pool, -1, 0, 1)

	dd := readSnapshot(ctx, t, pool, nil, 0, empty, full)
	is.Equal(len(dd), 3)
	for _, d := range dd {
		_, ok := d.pos.Snapshots[empty]
		is.True(!ok) // the empty table never got an entry
		is.Equal(d.pos.Snapshots[full].LastRead, d.id)
	}
}

// TestSnapshot_RoundTripCrashAndAddTable is a crash and resume through the
// serialized position: run 1 reads one row of table a (key -2) and stops; run
// 2 resumes from that position with table b added, which has no entry.
func TestSnapshot_RoundTripCrashAndAddTable(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	a := lowKeyTable(ctx, t, pool, -2, 0, 3)
	b := lowKeyTable(ctx, t, pool, -5, 0)

	run1 := readSnapshot(ctx, t, pool, nil, 1, a)
	is.Equal(idsOf(run1, a), []int64{-2})

	run2 := readSnapshot(ctx, t, pool, run1[len(run1)-1].raw, 0, a, b)
	is.Equal(idsOf(run2, a), []int64{0, 3})  // resumed after -2
	is.Equal(idsOf(run2, b), []int64{-5, 0}) // new: no lower bound
}

// TestSnapshot_UpgradeFromV019Position starts from a literal v0.19.0 snapshot
// position. Table mid was mid-snapshot (last_read 1 of 3), so its rows keyed
// 0 or below were skipped by v0.19.0; table fresh had not started (no entry).
// The rows keyed 0 or below of mid must be delivered once, the main pass
// must resume after 1, and fresh must be read whole.
func TestSnapshot_UpgradeFromV019Position(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)
	mid := lowKeyTable(ctx, t, pool, -7, -4, 0, 1, 2, 3)
	fresh := lowKeyTable(ctx, t, pool, -1, 0, 5)

	legacy := v019SnapshotPosition(map[string][2]int64{mid: {1, 3}})
	dd := readSnapshot(ctx, t, pool, legacy, 0, mid, fresh)

	is.Equal(idsOf(dd, mid), []int64{-7, -4, 0, 2, 3})
	is.Equal(idsOf(dd, fresh), []int64{-1, 0, 5})

	// Invariant 2: on table mid, LastRead and LowPassLastRead only move
	// forward, and FromMin is set from the first main-pass row on.
	var prev *position.SnapshotPosition
	for _, d := range dd {
		if d.table != mid {
			continue
		}
		e := d.pos.Snapshots[mid]
		is.Equal(e.SnapshotEnd, int64(3)) // the stored end is kept
		if d.id <= 0 {
			is.True(!e.FromMin)
			is.Equal(e.LastRead, int64(1))
			is.True(e.LowPassLastRead != nil && *e.LowPassLastRead == d.id)
		} else {
			is.True(e.FromMin)
			is.Equal(e.LastRead, d.id)
			is.Equal(e.LowPassLastRead, (*int64)(nil))
		}
		if prev != nil {
			is.True(e.LastRead >= prev.LastRead)
			if prev.LowPassLastRead != nil && e.LowPassLastRead != nil {
				is.True(*e.LowPassLastRead > *prev.LowPassLastRead)
			}
		}
		prev = &e
	}
}

// TestSnapshot_UpgradeCrashInLowPass crashes the upgrade of a v0.19.0
// position inside the low pass and right after it, and resumes each time
// from the last acked position. Every row is delivered, none twice.
func TestSnapshot_UpgradeCrashInLowPass(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(context.Background(), t, test.RegularConnString)

	for _, tc := range []struct {
		name  string
		crash int // rows acked before the crash
	}{
		{name: "after the first low-pass row", crash: 1},
		{name: "after the last low-pass row", crash: 3},
		{name: "after the first main-pass row", crash: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			table := lowKeyTable(ctx, t, pool, -7, -4, 0, 1, 2, 3)
			legacy := v019SnapshotPosition(map[string][2]int64{table: {1, 3}})

			run1 := readSnapshot(ctx, t, pool, legacy, tc.crash, table)
			is.Equal(len(run1), tc.crash)
			run2 := readSnapshot(ctx, t, pool, run1[len(run1)-1].raw, 0, table)

			got := append(idsOf(run1, table), idsOf(run2, table)...)
			is.Equal(got, []int64{-7, -4, 0, 2, 3})
		})
	}
}
