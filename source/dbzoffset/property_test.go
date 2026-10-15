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

package dbzoffset

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"pgregory.net/rapid"
)

// The reference model for the import (design: Tests, "Unit and property").
//
// It generates a WAL history of interleaved transactions, replays it the way
// Postgres streams it (whole transactions, in commit order, changes in WAL
// order), feeds it to a model of Debezium that commits an offset at a random
// event, and to a fresh Conduit slot created at a random consistent point at
// or before that offset. The Conduit side runs the offset through the real
// ParseOffset, NewPlan and Filter. It checks:
//
//   - loss is zero: every change Debezium could see is delivered by Debezium
//     before the cut or by Conduit (invariant 3);
//   - duplicates stay inside the S2 bound for the offset's case.
//
// What it covers: interleaved long transactions, COPY rows sharing one LSN,
// multi-table TRUNCATE (one message here, one event per relation in
// Debezium, B2), commit records whose end equals the next change's LSN (PG 17,
// debezium/dbz#76: records are contiguous in the model, so this happens all
// the time), transactions Debezium never saw arriving before the offset's
// transaction (S2), BEGIN, END and heartbeat offsets, offsets without
// messageType or ts_usec, and signed decoderbufs xids.

type recKind int

const (
	recRow recKind = iota
	recCopy
	recTruncate
)

type walRecord struct {
	kind  recKind
	start pglogrepl.LSN
	units int // rows of a COPY, relations of a TRUNCATE, 1 for a row
}

type modelTx struct {
	id      int
	xid     uint32
	ts      time.Time
	records []walRecord
	commit  pglogrepl.LSN // start of the commit record (BeginMessage.FinalLSN)
	end     pglogrepl.LSN // end of the commit record (Debezium's lsn_commit)
	// hidden: every row of it is outside Debezium's publication (a row
	// filter, a column list or publish flags), and on PG 15+ Debezium does not
	// see the empty transaction at all. The pipeline's publication keeps it.
	hidden bool
}

// unit identifies one Debezium event's worth of data: a row, or one relation
// of a TRUNCATE.
type unit struct{ tx, rec, n int }

type dbzEvent struct {
	units  []unit // empty for BEGIN, END and heartbeat events
	offset map[string]any
}

type history struct {
	txs []*modelTx // in commit order
}

func genHistory(t *rapid.T) history {
	nTx := rapid.IntRange(1, 7).Draw(t, "transactions")
	xidBase := rapid.Uint32Range(3, 1<<32-64).Draw(t, "xidBase")
	lsn := pglogrepl.LSN(rapid.Uint64Range(8, 1<<40).Draw(t, "walStart"))
	advance := func(label string) pglogrepl.LSN {
		start := lsn
		lsn += pglogrepl.LSN(8 * rapid.Uint64Range(1, 8).Draw(t, label))
		return start
	}

	var open, committed []*modelTx
	started := 0
	for len(committed) < nTx {
		canOpen := started < nTx
		action := rapid.IntRange(0, 2).Draw(t, "action")
		switch {
		case canOpen && (len(open) == 0 || action == 0):
			tx := &modelTx{
				id:     started,
				xid:    xidBase + uint32(started),
				hidden: rapid.IntRange(0, 5).Draw(t, "hidden") == 0,
			}
			started++
			open = append(open, tx)
		case action == 1 || len(open[0].records) == 0:
			i := rapid.IntRange(0, len(open)-1).Draw(t, "writer")
			kind := recKind(rapid.IntRange(0, 2).Draw(t, "kind"))
			units := 1
			switch kind {
			case recCopy:
				units = rapid.IntRange(2, 4).Draw(t, "copyRows")
			case recTruncate:
				units = rapid.IntRange(1, 3).Draw(t, "truncateRels")
			case recRow:
			}
			open[i].records = append(open[i].records, walRecord{kind: kind, start: advance("recSize"), units: units})
		default:
			// Commit a transaction that has written something.
			var candidates []int
			for i, tx := range open {
				if len(tx.records) > 0 {
					candidates = append(candidates, i)
				}
			}
			i := candidates[rapid.IntRange(0, len(candidates)-1).Draw(t, "committer")]
			tx := open[i]
			tx.commit = advance("commitSize")
			tx.end = lsn
			tx.ts = time.UnixMicro(1_760_000_000_000_000).Add(time.Duration(tx.commit%(1<<30)) * time.Microsecond)
			open = append(open[:i], open[i+1:]...)
			committed = append(committed, tx)
		}
	}
	return history{txs: committed}
}

// debezium replays the history as Debezium would and returns its events.
func debezium(t *rapid.T, h history, startCommit pglogrepl.LSN) []dbzEvent {
	txMetadata := rapid.Bool().Draw(t, "txMetadata")
	signedXid := rapid.Bool().Draw(t, "signedXid")
	lsnCommit := startCommit

	txID := func(xid uint32) int64 {
		if signedXid {
			return int64(int32(xid)) //nolint:gosec // deliberate: older Debezium read decoderbufs xids as signed
		}
		return int64(xid)
	}
	offset := func(tx *modelTx, lsn pglogrepl.LSN, m MessageType) map[string]any {
		return map[string]any{
			"lsn": uint64(lsn), "lsn_proc": uint64(lsn), "lsn_commit": uint64(lsnCommit),
			"txId": txID(tx.xid), "ts_usec": tx.ts.UnixMicro(), "messageType": string(m),
		}
	}

	var events []dbzEvent
	for ti, tx := range h.txs {
		if tx.hidden {
			continue
		}
		if txMetadata {
			events = append(events, dbzEvent{offset: offset(tx, tx.records[0].start, MessageBegin)})
		}
		for ri, r := range tx.records {
			m := MessageInsert
			if r.kind == recTruncate {
				m = MessageTruncate
			}
			for n := 0; n < r.units; n++ {
				events = append(events, dbzEvent{units: []unit{{ti, ri, n}}, offset: offset(tx, r.start, m)})
			}
		}
		lsnCommit = tx.end
		if txMetadata || rapid.Bool().Draw(t, "heartbeat") {
			// An END record, or a heartbeat right after the commit: both carry
			// the commit's offset with M = COMMIT.
			events = append(events, dbzEvent{offset: offset(tx, tx.end, MessageCommit)})
		}
	}
	return events
}

func TestProperty_ImportLosesNothingAndBoundsDuplicates(t *testing.T) { //nolint:gocyclo // the model is kept in one place so it reads top to bottom
	rapid.Check(t, func(t *rapid.T) {
		h := genHistory(t)
		walStart := h.txs[0].records[0].start // nothing before this
		events := debezium(t, h, walStart)
		if len(events) == 0 {
			t.Skip("every transaction was hidden from Debezium")
		}

		cut := rapid.IntRange(0, len(events)-1).Draw(t, "cut")
		offset := events[cut].offset
		if rapid.IntRange(0, 4).Draw(t, "dropMessageType") == 0 {
			delete(offset, "messageType") // older 1.x
		}
		if rapid.IntRange(0, 4).Draw(t, "dropTs") == 0 {
			delete(offset, "ts_usec")
		}
		raw, err := json.Marshal(offset)
		if err != nil {
			t.Fatal(err)
		}
		o, err := ParseOffset(raw)
		if err != nil {
			t.Fatalf("a streaming offset was refused: %v (%s)", err, raw)
		}
		plan := NewPlan(o)
		lc := o.LSNCommit

		delivered := map[unit]bool{}
		for _, e := range events[:cut+1] {
			for _, u := range e.units {
				delivered[u] = true
			}
		}

		// Conduit's fresh slot: consistent point C at or before Lc. It
		// decodes in full every transaction committing at or after C (a
		// patched server, B1).
		c := pglogrepl.LSN(rapid.Uint64Range(uint64(walStart), uint64(lc)).Draw(t, "consistentPoint"))
		conduit := map[unit]bool{}
		f := NewFilter(plan)
		for ti, tx := range h.txs {
			if tx.commit < c {
				continue
			}
			f.Begin(tx.commit, tx.xid, tx.ts)
			for ri, r := range tx.records {
				// One pgoutput change per row; one TRUNCATE message for all
				// its relations.
				changes := r.units
				if r.kind == recTruncate {
					changes = 1
				}
				for n := 0; n < changes; n++ {
					if f.Change(r.start) {
						continue
					}
					if r.kind == recTruncate {
						for rel := 0; rel < r.units; rel++ {
							conduit[unit{ti, ri, rel}] = true
						}
					} else {
						conduit[unit{ti, ri, n}] = true
					}
				}
			}
			f.Commit()
		}

		// Invariant 3: no loss. A change in a transaction Debezium never saw,
		// committed before Lc, was dropped by Debezium's configuration and is
		// out of scope (S6); none can exist in the model, because Lc only
		// moves on transactions Debezium saw... unless a hidden one committed
		// between two of them, which is exactly that S6 case.
		for ti, tx := range h.txs {
			for ri, r := range tx.records {
				for n := 0; n < r.units; n++ {
					u := unit{ti, ri, n}
					if delivered[u] || conduit[u] {
						continue
					}
					if tx.hidden && tx.commit < lc {
						continue // S6: processed under Debezium's configuration
					}
					t.Fatalf("LOST tx %d (xid %d, commit %s) record %d unit %d at %s; offset %s, plan %+v",
						ti, tx.xid, tx.commit, ri, n, r.start, raw, plan)
				}
			}
		}

		// The duplicate bound (S2 table).
		var dups []unit
		for u := range conduit {
			if delivered[u] {
				dups = append(dups, u)
			}
		}
		sort.Slice(dups, func(i, j int) bool { return fmt.Sprint(dups[i]) < fmt.Sprint(dups[j]) })
		tIndex := -1
		for ti, tx := range h.txs {
			if o.HasTxID && tx.xid == o.TxID && !tx.hidden && tx.commit >= lc {
				tIndex = ti
			}
		}
		switch plan.Case {
		case CaseCommit, CaseBegin:
			if len(dups) != 0 {
				t.Fatalf("case %s must have no duplicates, got %v; offset %s", plan.Case, dups, raw)
			}
		case CaseDML:
			if plan.Partial == nil {
				t.Fatalf("a DML offset with xid and lsn must arm the partial rule; offset %s", raw)
			}
			if tIndex < 0 {
				t.Fatalf("the offset's transaction is not in the history; offset %s", raw)
			}
			for _, u := range dups {
				if u.tx != tIndex || h.txs[u.tx].records[u.rec].start != o.LSN {
					t.Fatalf("duplicate %v outside the bound (T's changes at L=%s); offset %s", u, o.LSN, raw)
				}
			}
			if len(dups) == 0 {
				t.Fatalf("the offset's own record must be redelivered (it is at L); offset %s", raw)
			}
			if f.Armed() {
				t.Fatalf("the partial rule's transaction was streamed but the rule is still armed")
			}
		case CaseUnknownType:
			for _, u := range dups {
				if u.tx != tIndex {
					t.Fatalf("duplicate %v outside the offset's transaction; offset %s", u, raw)
				}
			}
		case CaseTransactionalMessage:
			t.Fatalf("the model writes no logical messages")
		}
	})
}
