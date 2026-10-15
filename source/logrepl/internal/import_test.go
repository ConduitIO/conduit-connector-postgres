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

package internal

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/matryer/is"
	"pgregory.net/rapid"
)

// pgEpoch is the Postgres timestamp epoch pgoutput encodes commit times from.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func xlog(walStart pglogrepl.LSN, msg []byte) *pgproto3.CopyData {
	data := []byte{pglogrepl.XLogDataByteID}
	data = binary.BigEndian.AppendUint64(data, uint64(walStart))
	data = binary.BigEndian.AppendUint64(data, uint64(walStart)) // ServerWALEnd
	data = binary.BigEndian.AppendUint64(data, 0)                // ServerTime
	return &pgproto3.CopyData{Data: append(data, msg...)}
}

func beginMsg(finalLSN pglogrepl.LSN, ts time.Time, xid uint32) []byte {
	b := []byte{'B'}
	b = binary.BigEndian.AppendUint64(b, uint64(finalLSN))
	b = binary.BigEndian.AppendUint64(b, uint64(ts.Sub(pgEpoch).Microseconds())) //nolint:gosec // test timestamps are after 2000
	return binary.BigEndian.AppendUint32(b, xid)
}

func insertMsg() []byte {
	b := []byte{'I'}
	b = binary.BigEndian.AppendUint32(b, 16384) // relation id
	b = append(b, 'N')
	b = binary.BigEndian.AppendUint16(b, 1) // one column
	b = append(b, 't')
	b = binary.BigEndian.AppendUint32(b, 1)
	return append(b, '1')
}

func commitMsg(commitLSN, endLSN pglogrepl.LSN, ts time.Time) []byte {
	b := []byte{'C', 0}
	b = binary.BigEndian.AppendUint64(b, uint64(commitLSN))
	b = binary.BigEndian.AppendUint64(b, uint64(endLSN))
	return binary.BigEndian.AppendUint64(b, uint64(ts.Sub(pgEpoch).Microseconds())) //nolint:gosec // test timestamps are after 2000
}

type delivered struct {
	lsn pglogrepl.LSN
	key ChangeKey
}

// importSubscription builds a Subscription the way applyImport (cdc.go)
// does, with a handler that records every change it is handed.
func importSubscription(plan dbzoffset.Plan, got *[]delivered) *Subscription {
	s := &Subscription{}
	c, seq, _ := plan.ResumeKey()
	s.Resume = ResumePoint{Key: ChangeKey{CommitLSN: c, Seq: seq}, LSN: plan.SkipCommitsBelow}
	s.Import = dbzoffset.NewFilter(plan)
	s.Handler = func(_ context.Context, m pglogrepl.Message, lsn pglogrepl.LSN) (pglogrepl.LSN, error) {
		if _, ok := m.(*pglogrepl.InsertMessage); ok {
			*got = append(*got, delivered{lsn: lsn, key: s.CurrentChange()})
		}
		return lsn, nil
	}
	return s
}

func TestSubscription_ImportPartialRule(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 14, 12, 0, 0, 123456000, time.UTC)
	plan := dbzoffset.NewPlan(dbzoffset.Offset{
		LSN: 1100, LSNCommit: 1000, TxID: 7, HasTxID: true,
		TsUsec: ts.UnixMicro(), HasTsUsec: true, MessageType: dbzoffset.MessageInsert,
	})
	var got []delivered
	s := importSubscription(plan, &got)
	feed := func(lsn pglogrepl.LSN, msg []byte) {
		is.NoErr(s.handleXLogData(ctx, xlog(lsn, msg)))
	}

	// P: committed before Lc, skipped whole by the base rule.
	feed(0, beginMsg(900, ts, 5))
	feed(800, insertMsg())
	feed(980, commitMsg(900, 1000, ts))

	// Q: another xid, never seen by Debezium, committed after Lc: delivered.
	feed(0, beginMsg(1200, ts, 9))
	feed(500, insertMsg())
	feed(1200, commitMsg(1200, 1210, ts))

	// T: changes below L skipped, at L (a COPY sharing one LSN) and above
	// delivered, with their real ordinals.
	feed(0, beginMsg(1300, ts, 7))
	feed(1050, insertMsg())
	feed(1100, insertMsg())
	feed(1100, insertMsg())
	feed(1150, insertMsg())
	feed(1300, commitMsg(1300, 1310, ts))

	// After T the rule is disarmed: xid 7 again is delivered in full.
	feed(0, beginMsg(1400, ts, 7))
	feed(1050, insertMsg())
	feed(1400, commitMsg(1400, 1410, ts))

	is.Equal(got, []delivered{
		{500, ChangeKey{1200, 1}},
		{1100, ChangeKey{1300, 2}},
		{1100, ChangeKey{1300, 3}},
		{1150, ChangeKey{1300, 4}},
		{1050, ChangeKey{1400, 1}},
	})
	is.True(!s.Import.Armed())
	is.Equal(s.Import.Stats().DeliveredAtBoundary, uint64(2))
}

// The base rule through the exact resume rule with (Lc-1, MaxUint64) is
// SkipsTransaction, for every commit LSN and seq.
func TestImport_ResumeKeyEqualsBaseRule(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		lc := pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint64).Draw(t, "lc"))
		plan := dbzoffset.NewPlan(dbzoffset.Offset{LSNCommit: lc})
		c, seq, ok := plan.ResumeKey()
		if !ok {
			t.Fatal("no key")
		}
		r := ResumePoint{Key: ChangeKey{CommitLSN: c, Seq: seq}, LSN: lc}
		k := ChangeKey{
			CommitLSN: pglogrepl.LSN(rapid.Uint64().Draw(t, "commit")),
			Seq:       rapid.Uint64Range(1, math.MaxUint64).Draw(t, "seq"),
		}
		if r.Delivered(k) != plan.SkipsTransaction(k.CommitLSN) {
			t.Fatalf("Delivered(%v) = %v, SkipsTransaction = %v (Lc %s)", k, r.Delivered(k), plan.SkipsTransaction(k.CommitLSN), lc)
		}
	})
}

func TestCapImportFlush(t *testing.T) {
	tests := []struct {
		name                         string
		write, flush, ceiling, floor pglogrepl.LSN
		wantWrite, wantFlush         pglogrepl.LSN
	}{
		{"keepalive past Lc is capped", 5000, 5000, 1000, 800, 5000, 1000},
		{"below Lc is kept", 900, 900, 1000, 800, 900, 900},
		{"never below the slot's own F0", 100, 100, 1000, 800, 800, 800},
		{"F0 equal to Lc", 100, 3000, 1000, 1000, 1000, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			w, f := capImportFlush(tt.write, tt.flush, tt.ceiling, tt.floor)
			is.Equal(f, tt.wantFlush)
			is.Equal(w, tt.wantWrite)
		})
	}
}

// Invariant 1 under the cap: for any state, the capped report is at most
// what reportedPositions would report, and at most Lc unless F0 says the
// slot was already there.
func TestCapImportFlush_NeverRaises(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ceiling := pglogrepl.LSN(rapid.Uint64Range(1, 1<<40).Draw(t, "lc"))
		floor := pglogrepl.LSN(rapid.Uint64Range(0, uint64(ceiling)).Draw(t, "f0"))
		walWritten := pglogrepl.LSN(rapid.Uint64Range(0, 1<<41).Draw(t, "written"))
		walFlushed := pglogrepl.LSN(rapid.Uint64Range(0, 1<<41).Draw(t, "flushed"))
		serverEnd := pglogrepl.LSN(rapid.Uint64Range(0, 1<<41).Draw(t, "end"))
		last := pglogrepl.LSN(rapid.Uint64Range(0, uint64(ceiling)).Draw(t, "last"))
		_, flush := reportedPositions(walWritten, walFlushed, serverEnd, last, rapid.Bool().Draw(t, "allAcked"))
		_, capped := capImportFlush(0, flush, ceiling, floor)
		if capped > ceiling {
			t.Fatalf("capped flush %s above Lc %s", capped, ceiling)
		}
		if capped > flush && capped != floor {
			t.Fatalf("cap raised the flush from %s to %s (floor %s)", flush, capped, floor)
		}
	})
}
