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
	exact := ResumePoint{Key: ChangeKey{CommitLSN: 0x9F0, Seq: 1}, LSN: 0x958} // checkpoint = T2's record
	legacy := ResumePoint{LSN: 0x958}                                          // same checkpoint, v0.14.2 position
	// COPY of 5 rows in one transaction: every row has LSN 0x5B8, commit 0x818.
	copied := ResumePoint{Key: ChangeKey{CommitLSN: 0x818, Seq: 1}, LSN: 0x5B8} // checkpoint = row 1

	tests := []struct {
		name          string
		point         ResumePoint
		key           ChangeKey
		wantDelivered bool
	}{
		{"exact: the checkpointed record itself", exact, ChangeKey{0x9F0, 1}, true},
		{"exact: T1, committed later with a lower change LSN", exact, ChangeKey{0xA48, 1}, false},
		{"exact: a transaction committed earlier", exact, ChangeKey{0x900, 3}, true},
		{"exact: same transaction, later change", exact, ChangeKey{0x9F0, 2}, false},
		{"exact: unknown commit LSN is never skipped", exact, ChangeKey{0, 1}, false},
		{"copy: row 1 (checkpointed)", copied, ChangeKey{0x818, 1}, true},
		{"copy: row 2 shares the LSN but is not delivered", copied, ChangeKey{0x818, 2}, false},
		{"copy: row 5", copied, ChangeKey{0x818, 5}, false},
		{"legacy: T1 is not skipped (the bug)", legacy, ChangeKey{0xA48, 1}, false},
		{"legacy: T2 is re-delivered (documented duplicate)", legacy, ChangeKey{0x9F0, 1}, false},
		{"legacy: transaction committed before the checkpoint", legacy, ChangeKey{0x900, 1}, true},
		{"legacy: unknown commit LSN is never skipped", legacy, ChangeKey{0, 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(tt.point.Delivered(tt.key), tt.wantDelivered)
		})
	}
}

// TestResumePoint_Model generates random interleaved workloads, including
// multi-row inserts whose rows share one LSN, replays them in the order
// logical replication delivers them, acks a random FIFO prefix and
// "crashes". Postgres then re-sends every transaction whose commit is at or
// past the checkpointed change LSN (the requested start point), and the
// resume point filters.
//
// Properties: the exact point re-delivers exactly the unacked changes (no
// loss, no duplicate). The legacy point (a v0.14.2 position) loses nothing,
// and duplicates only transactions that committed no later than the
// checkpointed one.
func TestResumePoint_Model(t *testing.T) {
	rng := rand.New(rand.NewSource(331)) //nolint:gosec // deterministic test input

	for seq := 0; seq < 3000; seq++ {
		stream := interleavedStream(rng)
		acked := 1 + rng.Intn(len(stream)) // FIFO prefix acked and checkpointed
		cp := stream[acked-1]

		for _, mode := range []string{"exact", "legacy"} {
			point := ResumePoint{Key: cp.key(), LSN: cp.lsn}
			if mode == "legacy" {
				point = ResumePoint{LSN: cp.lsn}
			}
			for i, c := range stream {
				if c.commit < cp.lsn {
					continue // Postgres does not re-send it
				}
				delivered := point.Delivered(c.key())
				wasAcked := i < acked
				if !wasAcked && delivered {
					t.Fatalf("seq %d %s: unacked change (commit %s, seq %d, lsn %s) skipped after resume from (%s, %d): LOST",
						seq, mode, c.commit, c.seq, c.lsn, cp.commit, cp.seq)
				}
				if wasAcked && !delivered {
					if mode == "exact" {
						t.Fatalf("seq %d exact: acked change (commit %s, seq %d) re-delivered", seq, c.commit, c.seq)
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

type modelChange struct {
	commit, lsn pglogrepl.LSN
	seq         uint64
}

func (c modelChange) key() ChangeKey { return ChangeKey{CommitLSN: c.commit, Seq: c.seq} }

// interleavedStream generates a random interleaved workload and returns its
// changes in the order logical replication delivers them: by commit LSN, and
// within a transaction in WAL order, numbered from 1. Some writes are
// multi-row inserts: one WAL record, several changes with the same LSN.
func interleavedStream(rng *rand.Rand) []modelChange {
	var (
		wal     pglogrepl.LSN = 0x1000
		open    []int
		changes = map[int][]pglogrepl.LSN{}
		commits = map[int]pglogrepl.LSN{}
		nextTx  int
	)
	next := func() pglogrepl.LSN { wal += pglogrepl.LSN(1 + rng.Uint64()%16); return wal }
	write := func(tx int) {
		lsn := next()
		rows := 1
		if rng.Intn(4) == 0 {
			rows = 2 + rng.Intn(4) // multi-row insert: rows share the LSN
		}
		for r := 0; r < rows; r++ {
			changes[tx] = append(changes[tx], lsn)
		}
	}
	for step := 0; step < 40 || len(open) > 0; step++ {
		switch {
		case step < 40 && (len(open) == 0 || rng.Intn(3) == 0):
			open = append(open, nextTx)
			nextTx++
		case rng.Intn(2) == 0 && len(open) > 0:
			write(open[rng.Intn(len(open))])
		case len(open) > 0:
			i := rng.Intn(len(open))
			tx := open[i]
			if len(changes[tx]) == 0 {
				write(tx)
			}
			commits[tx] = next()
			open = append(open[:i], open[i+1:]...)
		}
	}

	var stream []modelChange
	for tx, c := range commits {
		var n uint64
		for _, l := range changes[tx] {
			n++
			stream = append(stream, modelChange{commit: c, lsn: l, seq: n})
		}
	}
	sort.Slice(stream, func(i, j int) bool {
		if stream[i].commit != stream[j].commit {
			return stream[i].commit < stream[j].commit
		}
		return stream[i].seq < stream[j].seq
	})
	return stream
}

func TestChangeKey_Before(t *testing.T) {
	tests := []struct {
		name string
		a, b ChangeKey
		want bool
	}{
		{"lower commit LSN", ChangeKey{0x900, 9}, ChangeKey{0x9F0, 1}, true},
		{"higher commit LSN", ChangeKey{0x9F0, 1}, ChangeKey{0x900, 9}, false},
		{"same commit, lower seq", ChangeKey{0x9F0, 1}, ChangeKey{0x9F0, 2}, true},
		{"same commit, higher seq", ChangeKey{0x9F0, 2}, ChangeKey{0x9F0, 1}, false},
		{"equal", ChangeKey{0x9F0, 2}, ChangeKey{0x9F0, 2}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is.New(t).Equal(tt.a.Before(tt.b), tt.want)
		})
	}
}
