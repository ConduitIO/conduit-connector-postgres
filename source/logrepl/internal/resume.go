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

import "github.com/jackc/pglogrepl"

// ResumePoint is the last change a restarted subscription may treat as
// already delivered, taken from the checkpointed position (#331).
//
// Postgres re-sends every transaction whose commit LSN is at or past the
// point it starts decoding from (the greater of the requested start LSN and
// the slot's confirmed_flush_lsn). Transactions arrive in commit order, but
// each change carries its own LSN, so a transaction that began before
// another and committed after it delivers changes with lower LSNs than ones
// already delivered. Comparing a change LSN against the checkpointed change
// LSN therefore drops records. The comparison has to start from the
// transaction's commit LSN.
type ResumePoint struct {
	// CommitLSN is the commit LSN of the checkpointed record's transaction.
	// Zero means unknown: a position written before format version 2, or a
	// subscription that does not resume from a CDC record.
	CommitLSN pglogrepl.LSN
	// LSN is the checkpointed record's own change LSN.
	LSN pglogrepl.LSN
}

// Delivered reports whether a change at changeLSN in the transaction that
// commits at commitLSN was already delivered (and acked) before the restart,
// so it must be skipped.
//
// With CommitLSN known the answer is exact. (commit LSN, change LSN) pairs
// increase in stream order: commit LSNs increase from transaction to
// transaction, and change LSNs increase within a transaction. So a change was
// delivered exactly when its pair is at or below the checkpoint's.
//
// Without CommitLSN (the legacy point) only transactions that committed
// before LSN are skipped. The checkpointed change belongs to a transaction
// that commits after LSN, and every transaction that committed before LSN
// was delivered ahead of that one, so FIFO acks mean it was fully acked. A
// transaction committing at or after LSN is delivered in full. That loses
// nothing, but it can repeat what was already acked: transactions that
// committed while the checkpointed record's transaction was open, and that
// transaction's own prefix (at-least-once). It happens once, on the first
// restart after upgrading from a v0.14.2 position; the first record
// delivered after it writes a version 2 position.
//
// Invariant 3: this is the only place a re-sent change is dropped on resume.
// It never drops a change that has not been acked.
func (r ResumePoint) Delivered(commitLSN, changeLSN pglogrepl.LSN) bool {
	if r.CommitLSN == 0 {
		return commitLSN < r.LSN
	}
	if commitLSN != r.CommitLSN {
		return commitLSN < r.CommitLSN
	}
	return changeLSN <= r.LSN
}
