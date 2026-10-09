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

// ChangeKey identifies one change in replication stream order (#331).
//
// Change LSNs cannot do this. They are not monotonic across transactions: a
// transaction that began before another and committed after it delivers
// lower change LSNs. They are not unique within one either: a multi-row
// insert (COPY, heap_multi_insert) is one WAL record, and every row decoded
// from it carries the same LSN. The key is therefore the transaction's commit
// LSN plus the change's ordinal within the transaction, counted from its
// BeginMessage. Postgres decodes transactions at commit, in commit order, and
// re-sends a transaction identically, so the key is unique and strictly
// increasing in stream order, and the same change gets the same key on every
// delivery.
type ChangeKey struct {
	// CommitLSN is the transaction's commit LSN (BeginMessage.FinalLSN). Zero
	// means unknown.
	CommitLSN pglogrepl.LSN
	// Seq is the 1-based ordinal of the change (Insert, Update, Delete,
	// Truncate) within its transaction. Zero means unknown.
	Seq uint64
}

// Known reports whether both parts of the key are set.
func (k ChangeKey) Known() bool {
	return k.CommitLSN != 0 && k.Seq != 0
}

// Before reports whether k precedes o in stream order. Commit LSNs increase in
// stream order and Seq increases within a transaction, so the order is
// lexicographic on (CommitLSN, Seq). Only meaningful when both keys are Known.
func (k ChangeKey) Before(o ChangeKey) bool {
	if k.CommitLSN != o.CommitLSN {
		return k.CommitLSN < o.CommitLSN
	}
	return k.Seq < o.Seq
}

// ResumePoint is the last change a restarted subscription may treat as
// already delivered, taken from the checkpointed position (#331). Postgres
// re-sends every transaction whose commit LSN is at or past the point it
// starts decoding from: the greater of the requested start LSN and the slot's
// confirmed_flush_lsn.
type ResumePoint struct {
	// Key is the checkpointed record's change key. When it is not Known (a
	// position written before format version 2), the legacy rule applies.
	Key ChangeKey
	// LSN is the checkpointed record's own change LSN (the legacy rule's
	// boundary) or, with no checkpoint at all, the subscription's start LSN.
	LSN pglogrepl.LSN
}

// Delivered reports whether the change identified by k was delivered (and
// acked) before the restart, so it must be skipped.
//
//   - A change whose commit LSN is unknown is always delivered again (never
//     skipped): nothing proves it was acked.
//   - Exact (the checkpoint's Key is Known): skip if k is at or below the
//     checkpoint's key. Keys increase strictly in stream order, so this skips
//     exactly what was delivered: no loss, no duplicate.
//   - Legacy (a v0.14.2 position, only a change LSN): skip only transactions
//     that committed before LSN. The checkpointed change belongs to a
//     transaction that commits after LSN, and every transaction that
//     committed before LSN was delivered ahead of it, so FIFO acks mean it
//     was fully acked. Transactions that commit at or after LSN are delivered
//     in full. Nothing is lost, but this can repeat records that were already
//     acked: up to everything that committed while the checkpointed record's
//     transaction was open, plus that transaction's acked prefix. That set is
//     bounded by how long that transaction was open, not by a small count. It
//     happens once, on the first restart after upgrading from a v0.14.2
//     position.
//
// Invariant 3: this is the only place a re-sent change is dropped on resume,
// and it never drops a change that was not acked.
func (r ResumePoint) Delivered(k ChangeKey) bool {
	if k.CommitLSN == 0 {
		return false
	}
	if !r.Key.Known() {
		return k.CommitLSN < r.LSN
	}
	if k.CommitLSN != r.Key.CommitLSN {
		return k.CommitLSN < r.Key.CommitLSN
	}
	return k.Seq != 0 && k.Seq <= r.Key.Seq
}
