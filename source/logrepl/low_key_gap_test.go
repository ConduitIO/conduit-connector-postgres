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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"pgregory.net/rapid"
)

// lockedBuffer is a log sink that several goroutines may write to.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lowKeyWarnings returns the tables named by CodeSnapshotLowKeysSkipped
// warnings in the log, "" for one that names no table (the check failed).
func (b *lockedBuffer) lowKeyWarnings(t *testing.T) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if m["code"] == string(CodeSnapshotLowKeysSkipped) {
			tb, _ := m["table"].(string)
			out = append(out, tb)
		}
	}
	return out
}

func TestLowKeyGapNeedsCheck(t *testing.T) {
	cdc := position.Position{Type: position.TypeCDC, LastLSN: "0/10"}
	snap := position.Position{Type: position.TypeSnapshot, Snapshots: position.SnapshotPositions{"t": {LastRead: 1, SnapshotEnd: 2}}}
	checked := cdc
	checked.LowKeyGapChecked = true
	for _, tc := range []struct {
		name         string
		pos          position.Position
		withSnapshot bool
		want         bool
	}{
		{name: "fresh start", pos: position.Position{}, withSnapshot: true, want: false},
		{name: "fresh start without snapshot", pos: position.Position{}, withSnapshot: false, want: false},
		{name: "legacy cdc", pos: cdc, withSnapshot: true, want: true},
		{name: "legacy cdc, snapshot off", pos: cdc, withSnapshot: false, want: true},
		{name: "legacy snapshot resumed", pos: snap, withSnapshot: true, want: false},
		{name: "legacy snapshot abandoned", pos: snap, withSnapshot: false, want: true},
		{name: "checked", pos: checked, withSnapshot: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is.New(t).Equal(lowKeyGapNeedsCheck(tc.pos, tc.withSnapshot), tc.want)
		})
	}
}

// TestCDCPosition_PropertyCarriesLowKeyGapChecked: every CDC position the
// handler writes carries the run's LowKeyGapChecked, or a restart from it
// would run the one-time check of a legacy position again.
func TestCDCPosition_PropertyCarriesLowKeyGapChecked(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		checked := rapid.Bool().Draw(t, "checked")
		h := &CDCHandler{basePosition: position.Position{LowKeyGapChecked: checked}}
		for _, k := range streamKeys(t) {
			lsn := pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint64).Draw(t, "change lsn"))
			for _, raw := range [][]byte{h.buildPositionAt(lsn, k), h.buildMarkerPosition(lsn, k, "public.t")} {
				p, err := position.ParseSDKPosition(raw)
				if err != nil {
					t.Fatal(err)
				}
				if p.LowKeyGapChecked != checked {
					t.Fatalf("position %s: low_key_gap_checked %v, want %v", raw, p.LowKeyGapChecked, checked)
				}
			}
		}
	})
}

func TestCheckLowKeyGap(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	low := test.SetupEmptyTestTable(ctx, t, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (id) VALUES (-3), (0), (5)`, low))
	is.New(t).NoErr(err)
	high := test.SetupTestTable(ctx, t, pool) // ids 1 to 4

	legacyCDC := position.Position{Version: 4, Type: position.TypeCDC, LastLSN: "0/10"}
	for _, tc := range []struct {
		name        string
		pos         position.Position
		tables      []string
		wantWarned  []string
		wantChecked bool
	}{
		{name: "legacy cdc warns for the table with low keys", pos: legacyCDC, tables: []string{high, low}, wantWarned: []string{low}, wantChecked: true},
		{name: "legacy cdc without low keys is quiet", pos: legacyCDC, tables: []string{high}, wantChecked: true},
		{name: "fresh start is quiet", pos: position.Position{}, tables: []string{low}, wantChecked: true},
		{name: "a failed check says so and checks again next time", pos: legacyCDC, tables: []string{"no_such_table_lowkey"}, wantWarned: []string{""}, wantChecked: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var logs lockedBuffer
			ctx := zerolog.New(&logs).WithContext(ctx)
			keys := map[string]string{}
			for _, tb := range tc.tables {
				keys[tb] = "id"
			}
			c := &CombinedIterator{pool: pool, conf: Config{Tables: tc.tables, TableKeys: keys, WithSnapshot: true}}
			pos := tc.pos
			c.checkLowKeyGap(ctx, &pos)
			is.Equal(logs.lowKeyWarnings(t), tc.wantWarned)
			is.Equal(pos.LowKeyGapChecked, tc.wantChecked)
		})
	}
}

// lowKeyMessages returns the message of every CodeSnapshotLowKeysSkipped warning.
func (b *lockedBuffer) lowKeyMessages(t *testing.T) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if m["code"] == string(CodeSnapshotLowKeysSkipped) {
			msg, _ := m["message"].(string)
			out = append(out, msg)
		}
	}
	return out
}

// TestCheckLowKeyGap_LockedTableDoesNotBlock: the startup probe must not hang
// Open behind a lock. A transaction holds ACCESS EXCLUSIVE on the table, as a
// queued ALTER TABLE would; the probe gives up after lowKeyProbeTimeout,
// warns that the check did not finish, leaves the position unmarked so the
// next start retries, and the retry succeeds once the lock is gone.
func TestCheckLowKeyGap_LockedTableDoesNotBlock(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	table := test.SetupEmptyTestTable(ctx, t, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (id) VALUES (-3), (5)`, table))
	is.NoErr(err)

	lockTx, err := pool.Begin(ctx)
	is.NoErr(err)
	t.Cleanup(func() { _ = lockTx.Rollback(context.Background()) })
	_, err = lockTx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE`, table))
	is.NoErr(err)

	prev := lowKeyProbeTimeout
	lowKeyProbeTimeout = 300 * time.Millisecond
	t.Cleanup(func() { lowKeyProbeTimeout = prev })

	var logs lockedBuffer
	lctx := zerolog.New(&logs).WithContext(ctx)
	c := &CombinedIterator{pool: pool, conf: Config{Tables: []string{table}, TableKeys: map[string]string{table: "id"}, WithSnapshot: true}}
	pos := position.Position{Version: 4, Type: position.TypeCDC, LastLSN: "0/10"}

	start := time.Now()
	c.checkLowKeyGap(lctx, &pos)
	is.True(time.Since(start) < 10*time.Second) // returned, did not wait for the lock
	is.True(!pos.LowKeyGapChecked)              // retried on the next start
	msgs := logs.lowKeyMessages(t)
	is.Equal(len(msgs), 1)
	is.True(strings.Contains(msgs[0], "did not finish"))

	// Lock released: the next start's check completes and finds the table.
	is.NoErr(lockTx.Rollback(ctx))
	var logs2 lockedBuffer
	c.checkLowKeyGap(zerolog.New(&logs2).WithContext(ctx), &pos)
	is.True(pos.LowKeyGapChecked)
	is.Equal(logs2.lowKeyWarnings(t), []string{table})
}

// TestCheckLowKeyGap_PermissionDenied: a role that cannot SELECT from the
// table gets one clear warning, not a generic failure, and the start goes on.
func TestCheckLowKeyGap_PermissionDenied(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	admin := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupEmptyTestTable(ctx, t, admin)

	role := strings.ToLower(strings.ReplaceAll(table, "-", "_")) + "_nosel"
	_, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD 'pw'`, role))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %q`, role)) })

	cfg := admin.Config().ConnConfig
	url := fmt.Sprintf("postgres://%s:pw@%s:%d/%s?sslmode=disable", role, cfg.Host, cfg.Port, cfg.Database)
	limited := test.ConnectPool(ctx, t, url)

	var logs lockedBuffer
	c := &CombinedIterator{pool: limited, conf: Config{Tables: []string{table}, TableKeys: map[string]string{table: "id"}, WithSnapshot: true}}
	pos := position.Position{Version: 4, Type: position.TypeCDC, LastLSN: "0/10"}
	c.checkLowKeyGap(zerolog.New(&logs).WithContext(ctx), &pos)

	is.True(!pos.LowKeyGapChecked)
	msgs := logs.lowKeyMessages(t)
	is.Equal(len(msgs), 1)
	is.True(strings.Contains(msgs[0], "no SELECT privilege"))
	is.True(!strings.Contains(msgs[0], "permission denied")) // the clear message, not the raw error
}

// TestCombinedIterator_LegacyCDCPositionLowKeyCheck runs the startup check
// end to end. Run 1 snapshots a table with a row keyed 0 and streams one
// change. Its CDC position, with low_key_gap_checked removed, is the position
// a v0.19.0 build would have written. Run 2 resumes from it: it warns for the
// table, and its own positions carry the mark, so run 3 does not warn again.
func TestCombinedIterator_LegacyCDCPositionLowKeyCheck(t *testing.T) {
	ctx := test.Context(t)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	is := is.New(t)

	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	table := test.SetupTestTable(ctx, t, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (id, column1) VALUES (0, 'zero')`, table))
	is.NoErr(err)
	t.Cleanup(func() {
		_ = Cleanup(context.Background(), CleanupConfig{URL: pool.Config().ConnString(), SlotName: table, PublicationName: table})
	})

	conf := func(pos opencdc.Position) Config {
		return Config{
			Position: pos, Tables: []string{table}, TableKeys: map[string]string{table: "id"},
			PublicationName: table, SlotName: table, WithSnapshot: true, BatchSize: 1,
		}
	}
	// run streams until it has acked one CDC record and returns its position.
	run := func(ctx context.Context, pos opencdc.Position) opencdc.Position {
		it, err := NewCombinedIterator(ctx, pool, conf(pos))
		is.NoErr(err)
		defer func() { is.NoErr(it.Teardown(ctx)) }()
		_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %q SET column1 = 'touched' WHERE id = 1`, table))
		is.NoErr(err)
		for {
			recs, err := it.NextN(ctx, 1)
			is.NoErr(err)
			for _, r := range recs {
				is.NoErr(it.Ack(ctx, r.Position))
				p, err := position.ParseSDKPosition(r.Position)
				is.NoErr(err)
				is.True(p.LowKeyGapChecked) // every position of a run carries the mark
				if p.Type == position.TypeCDC {
					return r.Position
				}
			}
		}
	}

	cdc := run(ctx, nil)

	var legacy map[string]json.RawMessage
	is.NoErr(json.Unmarshal(cdc, &legacy))
	delete(legacy, "low_key_gap_checked")
	legacy["version"] = json.RawMessage("4")
	legacyPos, err := json.Marshal(legacy)
	is.NoErr(err)

	var logs2 lockedBuffer
	cdc2 := run(zerolog.New(&logs2).WithContext(ctx), legacyPos)
	is.Equal(logs2.lowKeyWarnings(t), []string{table})

	var logs3 lockedBuffer
	run(zerolog.New(&logs3).WithContext(ctx), cdc2)
	is.Equal(logs3.lowKeyWarnings(t), []string(nil))
}
