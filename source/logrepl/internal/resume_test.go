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
	"math/rand"
	"sort"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestResumePoint_Delivered(t *testing.T) {
	// The #331 shape: T1 inserts at 0x868 and commits last at 0xA48; T2
	// inserts at 0x958 and commits at 0x9F0. Stream order: T2, then T1.
	exact := ResumePoint{CommitLSN: 0x9F0, LSN: 0x958} // checkpoint = T2's record
	legacy := ResumePoint{LSN: 0x958}                  // same checkpoint, v0.14.2 position

	tests := []struct {
		name          string
		point         ResumePoint
		commit, lsn   pglogrepl.LSN
		wantDelivered bool
	}{
		{"exact: the checkpointed record itself", exact, 0x9F0, 0x958, true},
		{"exact: T1, committed later with a lower change LSN", exact, 0xA48, 0x868, false},
		{"exact: a transaction committed earlier", exact, 0x900, 0x8F0, true},
		{"exact: same transaction, later change", exact, 0x9F0, 0x960, false},
		{"legacy: T1 is not skipped (the bug)", legacy, 0xA48, 0x868, false},
		{"legacy: T2 is re-delivered (documented duplicate)", legacy, 0x9F0, 0x958, false},
		{"legacy: transaction committed before the checkpoint", legacy, 0x900, 0x8F0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(tt.point.Delivered(tt.commit, tt.lsn), tt.wantDelivered)
		})
	}
}

// TestResumePoint_Model generates random interleaved workloads, replays
// them in the order logical replication delivers them (transaction by
// transaction in commit order, changes in LSN order within a transaction),
// acks a random FIFO prefix, and "crashes". Postgres then re-sends every
// transaction whose commit is at or past the checkpointed change LSN (the
// requested start point), and the resume point filters.
//
// Properties: the exact point re-delivers exactly the unacked changes (no
// loss, no duplicate). The legacy point (a v0.14.2 position) loses nothing,
// and duplicates only transactions that committed between the checkpointed
// change and its own transaction's commit, plus that transaction's prefix.
func TestResumePoint_Model(t *testing.T) {
	rng := rand.New(rand.NewSource(331)) //nolint:gosec // deterministic test input

	for seq := 0; seq < 3000; seq++ {
		stream := interleavedStream(rng)
		acked := 1 + rng.Intn(len(stream)) // FIFO prefix acked and checkpointed
		cp := stream[acked-1]

		for _, mode := range []string{"exact", "legacy"} {
			point := ResumePoint{CommitLSN: cp.commit, LSN: cp.lsn}
			if mode == "legacy" {
				point = ResumePoint{LSN: cp.lsn}
			}
			for i, c := range stream {
				if c.commit < cp.lsn {
					continue // Postgres does not re-send it
				}
				delivered := point.Delivered(c.commit, c.lsn)
				wasAcked := i < acked
				if !wasAcked && delivered {
					t.Fatalf("seq %d %s: unacked change (commit %s, lsn %s) skipped after resume from (%s, %s): LOST",
						seq, mode, c.commit, c.lsn, cp.commit, cp.lsn)
				}
				if wasAcked && !delivered {
					if mode == "exact" {
						t.Fatalf("seq %d exact: acked change (commit %s, lsn %s) re-delivered", seq, c.commit, c.lsn)
					}
					if c.commit > cp.commit {
						t.Fatalf("seq %d legacy: duplicate of a transaction committed after the checkpointed one (commit %s > %s)",
							seq, c.commit, cp.commit)
					}
				}
			}
		}
	}
}

type modelChange struct{ commit, lsn pglogrepl.LSN }

// interleavedStream generates a random interleaved workload and returns its
// changes in the order logical replication delivers them.
func interleavedStream(rng *rand.Rand) []modelChange {
	// Build transactions: each gets increasing change LSNs drawn from a
	// shared WAL counter while other transactions are open, and a commit
	// LSN when it ends.
	var (
		wal     pglogrepl.LSN = 0x1000
		open    []int
		changes = map[int][]pglogrepl.LSN{}
		commits = map[int]pglogrepl.LSN{}
		nextTx  int
	)
	next := func() pglogrepl.LSN { wal += pglogrepl.LSN(1 + rng.Uint64()%16); return wal }
	for step := 0; step < 40 || len(open) > 0; step++ {
		switch {
		case step < 40 && (len(open) == 0 || rng.Intn(3) == 0):
			open = append(open, nextTx)
			nextTx++
		case rng.Intn(2) == 0 && len(open) > 0:
			tx := open[rng.Intn(len(open))]
			changes[tx] = append(changes[tx], next())
		case len(open) > 0:
			i := rng.Intn(len(open))
			tx := open[i]
			if len(changes[tx]) == 0 {
				changes[tx] = append(changes[tx], next())
			}
			commits[tx] = next()
			open = append(open[:i], open[i+1:]...)
		}
	}

	// Delivery order: by commit LSN, then change LSN.
	var stream []modelChange
	for tx, c := range commits {
		for _, l := range changes[tx] {
			stream = append(stream, modelChange{c, l})
		}
	}
	sort.Slice(stream, func(i, j int) bool {
		if stream[i].commit != stream[j].commit {
			return stream[i].commit < stream[j].commit
		}
		return stream[i].lsn < stream[j].lsn
	})

	return stream
}
