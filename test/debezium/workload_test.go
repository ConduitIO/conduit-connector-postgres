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

//go:build debezium

package debezium

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

// Change kinds, in Debezium's op letters. Conduit's operations map onto them.
const (
	opCreate = "c"
	opUpdate = "u"
	opDelete = "d"
)

// change is one row change's identity: (table, pk, version, op). The workload
// gives every row version a number no other change of that row shares, so
// the identity is unique.
type change struct {
	table string
	pk    int64
	ver   int64
	op    string
}

func (c change) key() string {
	return fmt.Sprintf("%s|%d|%d|%s", c.table, c.pk, c.ver, c.op)
}

// ledger is what the workload committed. A change is recorded only after its
// transaction's COMMIT returned, so the ledger never claims a row that was
// rolled back.
type ledger struct {
	mu      sync.Mutex
	changes map[string]change
}

func newLedger() *ledger { return &ledger{changes: map[string]change{}} }

func (l *ledger) add(cs ...change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range cs {
		l.changes[c.key()] = c
	}
}

func (l *ledger) snapshot() map[string]change {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]change, len(l.changes))
	for k, c := range l.changes {
		out[k] = c
	}
	return out
}

// liveRow is a row the workload may still update or delete.
type liveRow struct {
	pk  int64
	ver int64
}

// workloadConfig sizes the workload.
type workloadConfig struct {
	// bigRows is the number of rows the large transaction inserts into each
	// table. bigUpdates of each table's rows are then updated in the same
	// transaction.
	bigRows, bigUpdates int
	copyRows            int
	// smallBefore and smallAfter are single-row (or few-row) transactions
	// before and after the large transaction; smallTail follows the COPY.
	smallBefore, smallAfter, smallTail int
	// quiesce makes the workload stop after the large transaction and the
	// small transactions that follow it, until resume is closed.
	quiesce bool
}

// workload drives a ledgered write load on two tables, from one goroutine.
type workload struct {
	cfg    workloadConfig
	tables [2]string
	led    *ledger
	rng    *rand.Rand

	// Owned by the workload goroutine.
	live   map[string][]liveRow
	nextID map[string]int64
	ver    int64

	// bigXid and copyXid announce the xid of the large transaction and of the
	// COPY's transaction as soon as it is known, before its rows are written.
	bigXid, copyXid chan uint32
	// quiet is closed when a quiesce workload has paused; resume releases it.
	quiet, resume chan struct{}
	// done is closed when the workload has finished; err is valid after.
	done chan struct{}
	err  error

	// marker is the identity of the last row written.
	marker change
}

const markerPK = 900_000_000

// copyPayload makes the COPY's rows wide enough that Debezium needs a moment
// to emit them, which is the window the cut has to land in.
var copyPayload = strings.Repeat("c", 100)

func newWorkload(tables [2]string, seedRows int, cfg workloadConfig, led *ledger, seed int64) *workload {
	w := &workload{
		cfg: cfg, tables: tables, led: led, rng: rand.New(rand.NewSource(seed)), //nolint:gosec // reproducible, not security
		live:   map[string][]liveRow{},
		nextID: map[string]int64{},
		bigXid: make(chan uint32, 1), copyXid: make(chan uint32, 1),
		quiet: make(chan struct{}), resume: make(chan struct{}),
		done: make(chan struct{}),
	}
	for _, tb := range tables {
		for i := 1; i <= seedRows; i++ {
			w.live[tb] = append(w.live[tb], liveRow{pk: int64(i), ver: 0})
		}
		w.nextID[tb] = int64(seedRows) + 1
	}
	return w
}

// start runs the workload on conn in its own goroutine.
func (w *workload) start(ctx context.Context, conn *pgx.Conn) {
	go func() {
		defer close(w.done)
		w.err = w.run(ctx, conn)
	}()
}

func (w *workload) run(ctx context.Context, conn *pgx.Conn) error {
	for i := 0; i < w.cfg.smallBefore; i++ {
		if err := w.smallTx(ctx, conn); err != nil {
			return err
		}
	}
	if err := w.largeTx(ctx, conn); err != nil {
		return err
	}
	for i := 0; i < w.cfg.smallAfter; i++ {
		if err := w.smallTx(ctx, conn); err != nil {
			return err
		}
	}
	if w.cfg.quiesce {
		close(w.quiet)
		select {
		case <-w.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := w.copyTx(ctx, conn); err != nil {
		return err
	}
	for i := 0; i < w.cfg.smallTail; i++ {
		if err := w.smallTx(ctx, conn); err != nil {
			return err
		}
	}
	return w.markerTx(ctx, conn)
}

func (w *workload) nextVer() int64 {
	w.ver++
	return w.ver
}

// action is one planned statement of a small transaction.
type action struct {
	table string
	idx   int // index into live for update and delete
	c     change
}

// plan picks up to three statements on distinct rows: mostly inserts, then
// updates, then deletes.
func (w *workload) plan() []action {
	n := 1 + w.rng.Intn(3)
	used := map[string]map[int]bool{w.tables[0]: {}, w.tables[1]: {}}
	var out []action
	for i := 0; i < n; i++ {
		tb := w.tables[w.rng.Intn(2)]
		kind := w.rng.Intn(10)
		rows := w.live[tb]
		idx := -1
		if kind >= 6 && len(rows) > 0 {
			for try := 0; try < 8 && idx < 0; try++ {
				if j := w.rng.Intn(len(rows)); !used[tb][j] {
					idx = j
				}
			}
		}
		switch {
		case idx < 0:
			id := w.nextID[tb]
			w.nextID[tb]++
			out = append(out, action{table: tb, idx: -1, c: change{table: tb, pk: id, ver: w.nextVer(), op: opCreate}})
		case kind < 9:
			used[tb][idx] = true
			out = append(out, action{table: tb, idx: idx, c: change{table: tb, pk: rows[idx].pk, ver: w.nextVer(), op: opUpdate}})
		default:
			used[tb][idx] = true
			out = append(out, action{table: tb, idx: idx, c: change{table: tb, pk: rows[idx].pk, ver: rows[idx].ver, op: opDelete}})
		}
	}
	return out
}

// smallTx runs one short transaction of one to three statements.
func (w *workload) smallTx(ctx context.Context, conn *pgx.Conn) error {
	acts := w.plan()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("small tx: begin: %w", err)
	}
	for _, a := range acts {
		var sql string
		var args []any
		switch a.c.op {
		case opCreate:
			sql = fmt.Sprintf("INSERT INTO %s (id, ver, payload) VALUES ($1, $2, md5($1::bigint::text))", a.table)
			args = []any{a.c.pk, a.c.ver}
		case opUpdate:
			sql = fmt.Sprintf("UPDATE %s SET ver = $2, payload = md5($2::bigint::text) WHERE id = $1", a.table)
			args = []any{a.c.pk, a.c.ver}
		default:
			sql = fmt.Sprintf("DELETE FROM %s WHERE id = $1", a.table)
			args = []any{a.c.pk}
		}
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			_ = tx.Rollback(context.Background())
			return fmt.Errorf("small tx: %s: %w", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("small tx: commit: %w", err)
	}
	// Only now is the transaction durable and the ledger allowed to say so.
	cs := make([]change, 0, len(acts))
	for _, a := range acts {
		cs = append(cs, a.c)
	}
	w.led.add(cs...)
	w.applyLive(acts)
	return nil
}

// applyLive folds committed actions into the live set. Deletes are applied
// last and highest index first, so earlier indexes stay valid.
func (w *workload) applyLive(acts []action) {
	del := map[string][]int{}
	for _, a := range acts {
		switch a.c.op {
		case opCreate:
			w.live[a.table] = append(w.live[a.table], liveRow{pk: a.c.pk, ver: a.c.ver})
		case opUpdate:
			w.live[a.table][a.idx].ver = a.c.ver
		default:
			del[a.table] = append(del[a.table], a.idx)
		}
	}
	for tb, idxs := range del {
		for _, i := range sortedDesc(idxs) {
			rows := w.live[tb]
			rows[i] = rows[len(rows)-1]
			w.live[tb] = rows[:len(rows)-1]
		}
	}
}

func sortedDesc(in []int) []int {
	out := append([]int(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] > out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// xid32 is the current transaction's xid as Debezium and pgoutput report it.
const xid32 = "SELECT (txid_current() & 4294967295)::bigint"

// largeTx inserts bigRows rows into each table, then updates the first
// bigUpdates of each, in one transaction. The rows are wide enough that
// Debezium needs a while to emit them, which is the window the cut lands in.
func (w *workload) largeTx(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("large tx: begin: %w", err)
	}
	fail := func(err error) error {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("large tx: %w", err)
	}
	var xid int64
	if err := tx.QueryRow(ctx, xid32).Scan(&xid); err != nil {
		return fail(err)
	}
	w.bigXid <- uint32(xid) //nolint:gosec // masked to 32 bits above

	n, u := int64(w.cfg.bigRows), int64(w.cfg.bigUpdates)
	var cs []change
	for _, tb := range w.tables {
		id0, ver0 := w.nextID[tb], w.ver+1
		// Insert versions are ver0..ver0+n-1, update versions the next block.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s SELECT g, $2::bigint + (g - $1::bigint), repeat('x', 200) FROM generate_series($1::bigint, $1::bigint + $3::bigint - 1) g", tb),
			id0, ver0, n); err != nil {
			return fail(err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"UPDATE %s SET ver = ver + $2::bigint, payload = repeat('y', 200) WHERE id >= $1::bigint AND id < $1::bigint + $3::bigint", tb),
			id0, n, u); err != nil {
			return fail(err)
		}
		for i := int64(0); i < n; i++ {
			cs = append(cs, change{table: tb, pk: id0 + i, ver: ver0 + i, op: opCreate})
		}
		for i := int64(0); i < u; i++ {
			cs = append(cs, change{table: tb, pk: id0 + i, ver: ver0 + i + n, op: opUpdate})
		}
		w.nextID[tb] = id0 + n
		w.ver = ver0 + 2*n
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("large tx: commit: %w", err)
	}
	// The rows stay out of the live set: nothing else touches them.
	w.led.add(cs...)
	return nil
}

// copyTx loads copyRows rows into the first table with COPY, in one
// transaction. A COPY writes its rows in multi-insert records, so several
// rows share one change LSN: the S2 case that duplicates more than one row.
func (w *workload) copyTx(ctx context.Context, conn *pgx.Conn) error {
	tb := w.tables[0]
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("copy tx: begin: %w", err)
	}
	fail := func(err error) error {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("copy tx: %w", err)
	}
	var xid int64
	if err := tx.QueryRow(ctx, xid32).Scan(&xid); err != nil {
		return fail(err)
	}
	w.copyXid <- uint32(xid) //nolint:gosec // masked to 32 bits above

	rows := make([][]any, 0, w.cfg.copyRows)
	cs := make([]change, 0, w.cfg.copyRows)
	for i := 0; i < w.cfg.copyRows; i++ {
		id, ver := w.nextID[tb], w.nextVer()
		w.nextID[tb]++
		rows = append(rows, []any{id, ver, copyPayload})
		cs = append(cs, change{table: tb, pk: id, ver: ver, op: opCreate})
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{tb}, []string{"id", "ver", "payload"}, pgx.CopyFromRows(rows)); err != nil {
		return fail(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("copy tx: commit: %w", err)
	}
	w.led.add(cs...)
	return nil
}

// markerTx writes the last row of the workload. Conduit's reader knows the
// workload is over, as far as Conduit is concerned, when this row arrives.
func (w *workload) markerTx(ctx context.Context, conn *pgx.Conn) error {
	tb := w.tables[0]
	c := change{table: tb, pk: markerPK, ver: w.nextVer(), op: opCreate}
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, ver, payload) VALUES ($1, $2, 'marker')", tb), c.pk, c.ver); err != nil {
		return fmt.Errorf("marker tx: %w", err)
	}
	w.led.add(c)
	w.marker = c
	return nil
}
