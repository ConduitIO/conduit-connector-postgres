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
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/matryer/is"
)

func TestReportedPositions(t *testing.T) {
	tests := []struct {
		name                                       string
		walWritten, walFlushed, serverWALEnd, last pglogrepl.LSN
		allAcked                                   bool
		wantWrite, wantFlush                       pglogrepl.LSN
	}{
		{
			name:       "nothing outstanding, nothing newer: report the ack",
			walWritten: 100, walFlushed: 100, allAcked: true,
			wantWrite: 100, wantFlush: 100,
		},
		{
			name:       "nothing outstanding: advance to the keepalive WAL end",
			walWritten: 100, walFlushed: 100, serverWALEnd: 500, allAcked: true,
			wantWrite: 500, wantFlush: 500,
		},
		{
			name:       "record in flight: WAL end past it is NOT reported (invariant 1)",
			walWritten: 200, walFlushed: 100, serverWALEnd: 500,
			wantWrite: 200, wantFlush: 100,
		},
		{
			// COPY: all rows share one LSN. The first is acked, the rest are
			// in flight, so walFlushed == walWritten but the gate is closed.
			name:       "same-LSN rows in flight: equal LSNs do not open the gate",
			walWritten: 300, walFlushed: 300, serverWALEnd: 900,
			wantWrite: 300, wantFlush: 300,
		},
		{
			name:       "record in flight: keep the earlier, safe high-water mark (monotonic)",
			walWritten: 600, walFlushed: 100, serverWALEnd: 700, last: 500,
			wantWrite: 600, wantFlush: 500,
		},
		{
			name:       "stale WAL end below the ack is ignored",
			walWritten: 100, walFlushed: 100, serverWALEnd: 50, allAcked: true,
			wantWrite: 100, wantFlush: 100,
		},
		{
			// A later-committing transaction delivers a lower change LSN
			// (#331). All acked, and the report never goes back below what
			// was already reported.
			name:       "interleaved LSNs, all acked: never report below the high-water mark",
			walWritten: 0x868, walFlushed: 0x868, last: 0x958, allAcked: true,
			wantWrite: 0x958, wantFlush: 0x958,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			gotWrite, gotFlush := reportedPositions(tt.walWritten, tt.walFlushed, tt.serverWALEnd, tt.last, tt.allAcked)
			is.Equal(gotFlush, tt.wantFlush)
			is.Equal(gotWrite, tt.wantWrite)
		})
	}
}

// TestReportedPositions_Model drives a Subscription's gate through random
// interleaved workloads, including multi-row inserts whose rows share an LSN
// (interleavedStream). Records are emitted in delivery order, the engine acks
// the oldest outstanding one (FIFO), keepalives bring a WAL end past
// everything delivered so far, and status updates are sent.
//
// At every status update: the reported flush position is below the commit
// LSN of every emitted-but-unacked record's transaction, so Postgres will
// re-send it after a crash (acceptance criterion 7.5, invariant 1). The
// report never decreases, write >= flush, and with nothing outstanding the
// report reaches the WAL end.
//
// It fails if the gate compares LSNs instead of change keys (equal LSNs open
// it early) or if the gate is removed.
func TestReportedPositions_Model(t *testing.T) {
	rng := rand.New(rand.NewSource(20261007)) //nolint:gosec // deterministic test input, not security-relevant

	for seq := 0; seq < 2000; seq++ {
		stream := interleavedStream(rng)
		s := &Subscription{}
		var (
			outstanding []modelChange
			next        int
			walEnd      pglogrepl.LSN
		)
		for steps := 0; next < len(stream) || len(outstanding) > 0; steps++ {
			switch rng.Intn(4) {
			case 0: // the next change is delivered and emitted
				if next < len(stream) {
					c := stream[next]
					next++
					s.change = c.key()
					s.walWritten = c.lsn
					s.emitted = c.key()
					outstanding = append(outstanding, c)
					// once a transaction's last change (and its commit) is
					// sent, a keepalive may carry a WAL end past its commit;
					// mid-transaction it cannot
					if (next == len(stream) || stream[next].commit != c.commit) && c.commit > walEnd {
						walEnd = c.commit
					}
				}
			case 1: // the engine acks the oldest outstanding record
				if len(outstanding) > 0 {
					c := outstanding[0]
					outstanding = outstanding[1:]
					s.Ack(c.lsn, c.key())
				}
			case 2: // keepalive: the walsender has decoded up to just before
				// the next undelivered commit (that transaction would have
				// been sent first otherwise), or past everything at the end
				if next < len(stream) {
					walEnd = max(walEnd, stream[next].commit-1)
				} else {
					walEnd += pglogrepl.LSN(1 + rng.Uint64()%8)
				}
			case 3: // standby status update
				s.reportedFlush = checkReport(t, seq, s, walEnd, outstanding, next > 0)
			}
		}
	}
}

// checkReport runs one status-update decision for TestReportedPositions_Model
// and asserts its properties. It returns the reported flush position.
func checkReport(t *testing.T, seq int, s *Subscription, walEnd pglogrepl.LSN, outstanding []modelChange, emittedAny bool) pglogrepl.LSN {
	t.Helper()
	write, flush := reportedPositions(s.walWritten, s.walFlushed, walEnd, s.reportedFlush, s.allAcked())
	for _, c := range outstanding {
		if flush >= c.commit {
			t.Fatalf("sequence %d: reported flush %s at or past commit %s of unacked change (lsn %s, seq %d)",
				seq, flush, c.commit, c.lsn, c.seq)
		}
	}
	if flush < s.reportedFlush {
		t.Fatalf("sequence %d: reported flush went backwards: %s after %s", seq, flush, s.reportedFlush)
	}
	if write < flush {
		t.Fatalf("sequence %d: write %s below flush %s", seq, write, flush)
	}
	if len(outstanding) == 0 && emittedAny && flush < walEnd {
		t.Fatalf("sequence %d: nothing outstanding but flush %s below WAL end %s", seq, flush, walEnd)
	}
	return flush
}
