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
	"math"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestNewPlan_Cases(t *testing.T) {
	cases := []struct {
		name        string
		offset      Offset
		wantCase    Case
		wantPartial *Partial
		wantDup     string
	}{
		{
			name:     "heartbeat after commit",
			offset:   Offset{LSN: 200, LSNCommit: 200, TxID: 7, HasTxID: true, MessageType: MessageCommit},
			wantCase: CaseCommit,
			wantDup:  "empty",
		},
		{
			name:     "non-transactional message",
			offset:   Offset{LSN: 300, LSNCommit: 300, MessageType: MessageMessage},
			wantCase: CaseCommit,
			wantDup:  "empty",
		},
		{
			name:     "transactional message",
			offset:   Offset{LSN: 320, LSNCommit: 300, TxID: 9, HasTxID: true, MessageType: MessageMessage},
			wantCase: CaseTransactionalMessage,
			wantDup:  "xid 9, all changes (the transaction in full)",
		},
		{
			name:     "begin",
			offset:   Offset{LSN: 250, LSNCommit: 200, TxID: 8, HasTxID: true, MessageType: MessageBegin},
			wantCase: CaseBegin,
			wantDup:  "empty",
		},
		{
			name:        "insert with commit time",
			offset:      Offset{LSN: 260, LSNCommit: 200, TxID: 8, HasTxID: true, TsUsec: 42, HasTsUsec: true, MessageType: MessageInsert},
			wantCase:    CaseDML,
			wantPartial: &Partial{Xid: 8, CommitTimeUsec: 42, MatchCommitTime: true, SkipChangesBelow: 260},
			wantDup:     "xid 8, changes at LSN 0/104",
		},
		{
			name:        "truncate without commit time",
			offset:      Offset{LSN: 260, LSNCommit: 200, TxID: 8, HasTxID: true, MessageType: MessageTruncate},
			wantCase:    CaseDML,
			wantPartial: &Partial{Xid: 8, SkipChangesBelow: 260},
			wantDup:     "xid 8, changes at LSN 0/104",
		},
		{
			name:     "dml without xid is not armed",
			offset:   Offset{LSN: 260, LSNCommit: 200, MessageType: MessageUpdate},
			wantCase: CaseDML,
			wantDup:  "the first transaction committing at or after 0/C8, in full",
		},
		{
			name:     "dml without lsn is not armed",
			offset:   Offset{LSNCommit: 200, TxID: 8, HasTxID: true, MessageType: MessageDelete},
			wantCase: CaseDML,
			wantDup:  "xid 8, all changes (the transaction in full)",
		},
		{
			name:     "no message type (1.x)",
			offset:   Offset{LSN: 260, LSNCommit: 200, TxID: 8, HasTxID: true},
			wantCase: CaseUnknownType,
			wantDup:  "xid 8, all changes (the transaction in full)",
		},
		{
			name:     "a type the rule does not know",
			offset:   Offset{LSN: 260, LSNCommit: 200, TxID: 8, HasTxID: true, MessageType: "NOOP"},
			wantCase: CaseUnknownType,
			wantDup:  "xid 8, all changes (the transaction in full)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			p := NewPlan(tc.offset)
			is.Equal(p.Case, tc.wantCase)
			is.Equal(p.Partial, tc.wantPartial)
			is.Equal(p.SkipCommitsBelow, tc.offset.LSNCommit)
			is.Equal(p.ExpectedDuplicates(), tc.wantDup)
		})
	}
}

// The base rule compares the START of a commit record with the END of the
// last one Debezium processed: strictly below, never at (debezium/dbz#76).
func TestPlan_SkipsTransaction(t *testing.T) {
	is := is.New(t)
	p := NewPlan(Offset{LSNCommit: 1000})
	is.True(p.SkipsTransaction(999))
	is.True(!p.SkipsTransaction(1000))
	is.True(!p.SkipsTransaction(1001))
	is.True(!p.SkipsTransaction(0)) // unknown commit LSN: never skipped
}

func TestPlan_ResumeKey(t *testing.T) {
	is := is.New(t)
	c, seq, ok := NewPlan(Offset{LSNCommit: 1000}).ResumeKey()
	is.True(ok)
	is.Equal(c, pglogrepl.LSN(999))
	is.Equal(seq, uint64(math.MaxUint64))

	// The zero plan must not wrap to "skip everything".
	_, _, ok = Plan{}.ResumeKey()
	is.True(!ok)
	is.True(!Plan{}.SkipsTransaction(1))
}

func TestFilter_PartialRule(t *testing.T) {
	is := is.New(t)
	ts := time.UnixMicro(1_760_000_000_000_000)
	p := NewPlan(Offset{LSN: 500, LSNCommit: 400, TxID: 77, HasTxID: true,
		TsUsec: ts.UnixMicro(), HasTsUsec: true, MessageType: MessageInsert})
	f := NewFilter(p)
	is.True(f.Armed())

	// A transaction committed before Lc: skipped whole.
	is.True(f.Begin(390, 70, ts))
	is.True(f.Change(100))
	f.Commit()

	// A transaction Debezium never saw (another xid) arrives first: delivered
	// in full, and the rule stays armed (S2).
	is.True(!f.Begin(410, 78, ts))
	is.True(!f.Change(300))
	f.Commit()
	is.True(f.Armed())

	// Same xid, wrong commit time: not T, delivered, still armed.
	is.True(!f.Begin(600, 77, ts.Add(time.Microsecond)))
	is.True(!f.Change(300))
	f.Commit()
	is.True(f.Armed())

	// T: changes below L skipped, changes at L (a COPY) and above delivered.
	is.True(!f.Begin(700, 77, ts))
	is.True(f.Change(300))
	is.True(f.Change(499))
	is.True(!f.Change(500))
	is.True(!f.Change(500))
	is.True(!f.Change(0)) // an unknown LSN is never skipped
	is.True(!f.Change(650))
	f.Commit()
	is.True(!f.Armed())

	// Disarmed: the same xid again is delivered in full.
	is.True(!f.Begin(800, 77, ts))
	is.True(!f.Change(300))
	f.Commit()

	is.Equal(f.Stats(), FilterStats{
		TransactionsSkipped: 1,
		ChangesSkipped:      2,
		MatchedCommitLSN:    700,
		DeliveredAtBoundary: 2,
	})
}

func TestFilter_NotArmed(t *testing.T) {
	is := is.New(t)
	for _, o := range []Offset{
		{LSN: 400, LSNCommit: 400, TxID: 77, HasTxID: true, MessageType: MessageCommit},
		{LSN: 350, LSNCommit: 300, TxID: 77, HasTxID: true, MessageType: MessageBegin},
		{LSN: 450, LSNCommit: 400, TxID: 77, HasTxID: true, MessageType: MessageMessage},
		{LSN: 450, LSNCommit: 400, TxID: 77, HasTxID: true},
	} {
		f := NewFilter(NewPlan(o))
		is.True(!f.Armed())
		is.True(!f.Begin(500, 77, time.Time{}))
		is.True(!f.Change(100)) // nothing inside a transaction at or after Lc is skipped
		f.Commit()
	}
}
