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
	"fmt"
	"math"
	"time"

	"github.com/jackc/pglogrepl"
)

// Case is the duplicate case of an offset, from the S2 table. It decides
// whether the partial rule is armed and what the duplicate set is.
type Case int

const (
	// CaseCommit: M = COMMIT (a heartbeat after a commit, a transaction
	// metadata END record) or a non-transactional MESSAGE (lsn ==
	// lsn_commit). The base rule covers the offset's transaction. 0
	// duplicates.
	CaseCommit Case = iota + 1
	// CaseBegin: M = BEGIN. Nothing of the offset's transaction was
	// delivered. 0 duplicates.
	CaseBegin
	// CaseDML: M is INSERT, UPDATE, DELETE or TRUNCATE (a change, or a
	// heartbeat emitted mid-transaction). With the xid and lsn present the
	// partial rule is armed, and the duplicates are the transaction's changes
	// at lsn.
	CaseDML
	// CaseTransactionalMessage: M = MESSAGE with lsn != lsn_commit. Postgres
	// reports a message's end LSN, so the partial rule is not used and the
	// offset's transaction is delivered in full.
	CaseTransactionalMessage
	// CaseUnknownType: no messageType (older 1.x), or a type the rule does
	// not distinguish. Base rule only; the offset's transaction is delivered
	// in full.
	CaseUnknownType
)

func (c Case) String() string {
	switch c {
	case CaseCommit:
		return "commit"
	case CaseBegin:
		return "begin"
	case CaseDML:
		return "dml"
	case CaseTransactionalMessage:
		return "transactional_message"
	case CaseUnknownType:
		return "unknown_type"
	default:
		return fmt.Sprintf("Case(%d)", int(c))
	}
}

// Plan is the resume point computed from a Debezium offset (D4). It is pure
// data, recomputed from the setting at every start with an empty position,
// and never persisted.
type Plan struct {
	// SkipCommitsBelow is Lc. The base rule skips every transaction whose
	// commit LSN (BeginMessage.FinalLSN) is strictly below it.
	SkipCommitsBelow pglogrepl.LSN
	// Partial is the in-memory rule for the transaction the offset was taken
	// in. Nil when the rule is not armed.
	Partial *Partial
	// Case is the S2 duplicate case.
	Case Case
	// Offset is the offset the plan was computed from, for logs and the dry
	// run.
	Offset Offset
}

// Partial skips the prefix of one transaction that Debezium already
// delivered (D4, as amended by B2 and S2).
type Partial struct {
	// Xid is X, the transaction's xid (BeginMessage.Xid).
	Xid uint32
	// CommitTimeUsec is ts_usec. When MatchCommitTime is set, the
	// transaction's commit time (BeginMessage.CommitTime) must equal it.
	// Safety does not depend on it (the LSN bound does); it keeps the
	// duplicate accounting pointed at the right transaction.
	CommitTimeUsec  int64
	MatchCommitTime bool
	// SkipChangesBelow is L. Changes of the transaction whose LSN is
	// strictly below it are skipped; every change at L or above is
	// delivered.
	SkipChangesBelow pglogrepl.LSN
}

// NewPlan computes the resume point for o. It never fails: o has passed
// ParseOffset, and every shape it cannot prove anything about falls back to
// the base rule, which delivers the offset's transaction in full.
func NewPlan(o Offset) Plan {
	p := Plan{SkipCommitsBelow: o.LSNCommit, Offset: o}
	switch {
	case o.MessageType == MessageCommit, o.IsNonTransactionalMessage():
		p.Case = CaseCommit
	case o.MessageType == MessageBegin:
		p.Case = CaseBegin
	case o.MessageType.IsDML():
		p.Case = CaseDML
		// Invariant 3: arm only with both X (which transaction) and L (where
		// its delivered prefix ends). Without either nothing is skipped.
		if o.HasTxID && o.LSN != 0 {
			p.Partial = &Partial{
				Xid:              o.TxID,
				CommitTimeUsec:   o.TsUsec,
				MatchCommitTime:  o.HasTsUsec,
				SkipChangesBelow: o.LSN,
			}
		}
	case o.MessageType == MessageMessage:
		p.Case = CaseTransactionalMessage
	default:
		p.Case = CaseUnknownType
	}
	return p
}

// SkipsTransaction reports whether the base rule skips the whole transaction
// with commit LSN commitLSN. A zero (unknown) commit LSN is never skipped.
//
// Invariant 3: commitLSN is the START of the transaction's commit record and
// Lc the END of the last commit record Debezium processed. Commit records do
// not overlap, so commitLSN < Lc means the transaction committed at or
// before the one Debezium processed last, and Debezium delivered it.
// commitLSN == Lc is a later transaction (debezium/dbz#76), and is
// delivered.
func (p Plan) SkipsTransaction(commitLSN pglogrepl.LSN) bool {
	return commitLSN != 0 && commitLSN < p.SkipCommitsBelow
}

// ResumeKey returns the base rule as a change key for the connector's exact
// resume rule: (Lc-1, MaxUint64), the same construction as
// ChangeKey.Predecessor. Under that rule a change is delivered already iff
// its commit LSN is at or below Lc-1, which is SkipsTransaction.
//
// ok is false for a Plan with no Lc (the zero Plan, or one built from an
// Offset that did not come from ParseOffset): Lc-1 would wrap to the largest
// LSN and skip everything.
func (p Plan) ResumeKey() (commitLSN pglogrepl.LSN, seq uint64, ok bool) {
	if p.SkipCommitsBelow == 0 {
		return 0, 0, false
	}
	return p.SkipCommitsBelow - 1, math.MaxUint64, true
}

// Matches reports whether a transaction is the one the partial rule is for:
// xid X, a commit LSN at or past Lc (anything below is skipped whole by the
// base rule), and, when ts_usec is known, the same commit time.
func (r Partial) Matches(p Plan, commitLSN pglogrepl.LSN, xid uint32, commitTime time.Time) bool {
	if xid != r.Xid || commitLSN == 0 || commitLSN < p.SkipCommitsBelow {
		return false
	}
	return !r.MatchCommitTime || commitTime.UnixMicro() == r.CommitTimeUsec
}

// SkipsChange reports whether the partial rule skips a change of the
// matched transaction with change LSN lsn. A zero (unknown) LSN is never
// skipped.
//
// Invariant 3: inside one transaction pgoutput and decoderbufs send changes
// in WAL order, so a change below L was emitted before the offset's record.
// Changes AT L are delivered: the rows of one COPY share an LSN, and a
// multi-table TRUNCATE is one message here but one event per relation in
// Debezium, so "the first n changes at L" cannot be skipped safely (B2).
func (r Partial) SkipsChange(lsn pglogrepl.LSN) bool {
	return lsn != 0 && lsn < r.SkipChangesBelow
}

// DuplicatesEmpty is what ExpectedDuplicates says when the plan predicts no
// duplicates. The harness keys its zero-duplicate assertion on it (Q5).
const DuplicatesEmpty = "empty"

// ExpectedDuplicates describes the duplicate set the plan predicts, for the
// INFO line at Open (Q5). It is a log line, not a contract.
func (p Plan) ExpectedDuplicates() string {
	switch {
	case p.Case == CaseCommit || p.Case == CaseBegin:
		return DuplicatesEmpty
	case p.Partial != nil:
		return fmt.Sprintf("xid %d, changes at LSN %s", p.Partial.Xid, p.Partial.SkipChangesBelow)
	case p.Offset.HasTxID:
		return fmt.Sprintf("xid %d, all changes (the transaction in full)", p.Offset.TxID)
	default:
		return fmt.Sprintf("the first transaction committing at or after %s, in full", p.SkipCommitsBelow)
	}
}

// Filter applies a Plan to one replication stream, in stream order. It holds
// the partial rule's arming state, so use one Filter per subscription run,
// on the goroutine that reads the stream. A restart builds a new Filter from
// the same Plan only while the position is still empty; after the first ack
// the version 2 position rules and no Filter is used (D4).
//
// The connector's subscription and the CLI's --peek run the same Filter, so
// the dry run predicts what the connector skips.
type Filter struct {
	plan Plan
	// armed is true until the partial rule's transaction has been streamed.
	// Transactions with another xid do not disarm it (S2): a transaction
	// Debezium never saw (filtered out of its publication) can arrive first.
	armed bool

	// The current transaction.
	skipTx bool // the base rule skips all of it
	inT    bool // it is the partial rule's transaction

	stats FilterStats
}

// FilterStats counts what a Filter decided, for the Q5 duplicate logging and
// for --peek.
type FilterStats struct {
	// TransactionsSkipped counts transactions the base rule skipped whole.
	TransactionsSkipped uint64
	// ChangesSkipped counts changes skipped by the partial rule.
	ChangesSkipped uint64
	// MatchedCommitLSN is the commit LSN of the partial rule's transaction,
	// once it has been seen. Zero before.
	MatchedCommitLSN pglogrepl.LSN
	// DeliveredAtBoundary counts the matched transaction's delivered changes
	// at L: the predicted duplicates for CaseDML.
	DeliveredAtBoundary uint64
}

// NewFilter returns a Filter for p, armed when p has a partial rule.
func NewFilter(p Plan) *Filter {
	return &Filter{plan: p, armed: p.Partial != nil}
}

// Begin starts a transaction (a BeginMessage). It reports whether the base
// rule skips the whole transaction.
func (f *Filter) Begin(commitLSN pglogrepl.LSN, xid uint32, commitTime time.Time) (skipTx bool) {
	f.skipTx = f.plan.SkipsTransaction(commitLSN)
	f.inT = !f.skipTx && f.armed && f.plan.Partial.Matches(f.plan, commitLSN, xid, commitTime)
	if f.skipTx {
		f.stats.TransactionsSkipped++
	}
	if f.inT {
		f.stats.MatchedCommitLSN = commitLSN
	}
	return f.skipTx
}

// Change reports whether the change with LSN lsn in the current transaction
// is skipped.
func (f *Filter) Change(lsn pglogrepl.LSN) (skip bool) {
	if f.skipTx {
		return true
	}
	if !f.inT {
		return false
	}
	if f.plan.Partial.SkipsChange(lsn) {
		f.stats.ChangesSkipped++
		return true
	}
	if lsn == f.plan.Partial.SkipChangesBelow {
		f.stats.DeliveredAtBoundary++
	}
	return false
}

// Commit ends the current transaction (a CommitMessage). The partial rule is
// disarmed once its transaction has been streamed.
func (f *Filter) Commit() {
	if f.inT {
		f.armed = false
	}
	f.skipTx, f.inT = false, false
}

// Armed reports whether the partial rule's transaction is still to come.
func (f *Filter) Armed() bool { return f.armed }

// Stats returns the counts so far.
func (f *Filter) Stats() FilterStats { return f.stats }
