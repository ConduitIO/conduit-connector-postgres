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
		wantWrite, wantFlush                       pglogrepl.LSN
	}{
		{
			name:       "nothing outstanding, nothing newer: report the ack",
			walWritten: 100, walFlushed: 100,
			wantWrite: 100, wantFlush: 100,
		},
		{
			name:       "nothing outstanding: advance to the keepalive WAL end",
			walWritten: 100, walFlushed: 100, serverWALEnd: 500,
			wantWrite: 500, wantFlush: 500,
		},
		{
			name:       "record in flight: WAL end past it is NOT reported (invariant 1)",
			walWritten: 200, walFlushed: 100, serverWALEnd: 500,
			wantWrite: 200, wantFlush: 100,
		},
		{
			name:       "record in flight: keep the earlier, safe high-water mark (monotonic)",
			walWritten: 600, walFlushed: 100, serverWALEnd: 700, last: 500,
			wantWrite: 600, wantFlush: 500,
		},
		{
			name:       "stale WAL end below the ack is ignored",
			walWritten: 100, walFlushed: 100, serverWALEnd: 50,
			wantWrite: 100, wantFlush: 100,
		},
		{
			// A later-committing transaction delivers a lower change LSN
			// (#331). All acked (FIFO), so the gate is open, and the report
			// never goes back below what was already reported.
			name:       "interleaved LSNs, all acked: never report below the high-water mark",
			walWritten: 0x868, walFlushed: 0x868, last: 0x958,
			wantWrite: 0x958, wantFlush: 0x958,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			gotWrite, gotFlush := reportedPositions(tt.walWritten, tt.walFlushed, tt.serverWALEnd, tt.last)
			is.Equal(gotFlush, tt.wantFlush)
			is.Equal(gotWrite, tt.wantWrite)
		})
	}
}

// TestReportedPositions_Model drives reportedPositions through random
// sequences of the events a live subscription sees: a record is emitted, the
// engine acks the oldest outstanding record (FIFO), a keepalive brings a new
// WAL end, unrelated WAL is written, and a status update is sent. LSNs
// increase in stream order (one change per transaction; interleaving is
// covered by TestReportedPositions).
//
// At every status update it checks acceptance criterion 7.5: the reported
// flush position is below every emitted-but-unacked record, so the slot can
// never confirm past a record the engine has not durably handled. It also
// checks that the report never decreases, that write >= flush, and liveness:
// with nothing outstanding the report reaches the latest WAL end.
//
// Removing the walFlushed == walWritten gate in reportedPositions fails this
// test within the first few sequences.
func TestReportedPositions_Model(t *testing.T) {
	rng := rand.New(rand.NewSource(20261007)) //nolint:gosec // deterministic test input, not security-relevant

	const (
		sequences = 2000
		steps     = 200
	)

	for seq := 0; seq < sequences; seq++ {
		var (
			next                           pglogrepl.LSN = 0x1000
			walWritten, walFlushed, walEnd pglogrepl.LSN
			lastReported                   pglogrepl.LSN
			outstanding                    []pglogrepl.LSN
		)
		advance := func() pglogrepl.LSN {
			next += pglogrepl.LSN(1 + rng.Uint64()%64)
			return next
		}
		// The subscription starts with walWritten == walFlushed == StartLSN.
		walWritten = advance()
		walFlushed = walWritten

		for step := 0; step < steps; step++ {
			switch rng.Intn(5) {
			case 0: // a record is emitted
				lsn := advance()
				walWritten = lsn
				outstanding = append(outstanding, lsn)
			case 1: // the engine acks the oldest outstanding record
				if len(outstanding) > 0 {
					walFlushed = outstanding[0]
					outstanding = outstanding[1:]
				}
			case 2: // a keepalive: the walsender has sent everything up to here
				walEnd = advance()
			case 3: // WAL the publication filters out
				advance()
			case 4: // standby status update
				write, flush := reportedPositions(walWritten, walFlushed, walEnd, lastReported)

				if len(outstanding) > 0 && flush >= outstanding[0] {
					t.Fatalf("sequence %d step %d: reported flush %s at or past unacked record %s "+
						"(walWritten=%s walFlushed=%s walEnd=%s)",
						seq, step, flush, outstanding[0], walWritten, walFlushed, walEnd)
				}
				if flush < lastReported {
					t.Fatalf("sequence %d step %d: reported flush went backwards: %s after %s", seq, step, flush, lastReported)
				}
				if write < flush {
					t.Fatalf("sequence %d step %d: write %s below flush %s", seq, step, write, flush)
				}
				if len(outstanding) == 0 && flush < walEnd {
					t.Fatalf("sequence %d step %d: nothing outstanding but flush %s below WAL end %s", seq, step, flush, walEnd)
				}
				lastReported = flush
			}
		}
	}
}
