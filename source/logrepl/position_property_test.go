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

package logrepl

import (
	"math"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/jackc/pglogrepl"
	"pgregory.net/rapid"
)

// streamKeys draws the change keys of a replication stream in stream order:
// transactions with strictly increasing commit LSNs, each with one or more
// changes numbered from 1.
func streamKeys(t *rapid.T) []internal.ChangeKey {
	var keys []internal.ChangeKey
	commit := pglogrepl.LSN(rapid.Uint64Range(1, 1<<40).Draw(t, "first commit"))
	for range rapid.IntRange(1, 6).Draw(t, "transactions") {
		for seq := range rapid.Uint64Range(1, 5).Draw(t, "changes") {
			keys = append(keys, internal.ChangeKey{CommitLSN: commit, Seq: seq + 1})
		}
		commit += pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint32).Draw(t, "commit gap"))
	}
	return keys
}

// readKey is what a restart reads from a checkpointed position: the same
// steps as CDCIterator.Ack and CombinedIterator.initCDCIterator.
func readKey(t *rapid.T, sdkPos []byte) (internal.ChangeKey, pglogrepl.LSN) {
	p, err := position.ParseSDKPosition(sdkPos)
	if err != nil {
		t.Fatalf("parse %s: %v", sdkPos, err)
	}
	commit, err := p.TxCommit()
	if err != nil {
		t.Fatalf("commit LSN of %s: %v", sdkPos, err)
	}
	lsn, err := p.LSN()
	if err != nil {
		t.Fatalf("LSN of %s: %v", sdkPos, err)
	}
	return internal.ChangeKey{CommitLSN: commit, Seq: p.TxSeq}, lsn
}

// TestCDCPosition_PropertyResumeThroughSerialization checks invariants 2 and
// 3 across the position's serialized form. For a stream of changes, the CDC
// handler writes one position per change. Read back, those positions:
//
//   - carry exactly the change key and LSN they were written with;
//   - are strictly increasing in stream order (monotonic);
//   - resumed from any one of them, skip exactly the changes up to and
//     including it, and deliver every later one: no loss, no duplicate.
//
// The check count follows rapid: -rapid.checks or RAPID_CHECKS (default 100).
func TestCDCPosition_PropertyResumeThroughSerialization(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := &CDCHandler{}
		keys := streamKeys(t)

		read := make([]internal.ChangeKey, len(keys))
		lsns := make([]pglogrepl.LSN, len(keys))
		for i, k := range keys {
			// A change's own LSN is not ordered across transactions (#331),
			// so draw it freely.
			lsn := pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint64).Draw(t, "change lsn"))
			got, gotLSN := readKey(t, h.buildPositionAt(lsn, k))
			if got != k || gotLSN != lsn {
				t.Fatalf("change %d: wrote key %v at %s, read key %v at %s", i, k, lsn, got, gotLSN)
			}
			if i > 0 && !read[i-1].Before(got) {
				t.Fatalf("positions not increasing: %v then %v", read[i-1], got)
			}
			read[i], lsns[i] = got, gotLSN
		}

		cp := rapid.IntRange(0, len(keys)-1).Draw(t, "checkpoint")
		resume := internal.ResumePoint{Key: read[cp], LSN: lsns[cp]}
		for j, k := range keys {
			if want := j <= cp; resume.Delivered(k) != want {
				t.Fatalf("resumed from change %d %v: Delivered(change %d %v) = %v, want %v",
					cp, read[cp], j, k, resume.Delivered(k), want)
			}
		}
	})
}

// TestCDCPosition_PropertyUnknownKeyIsLegacy: a change key that is not Known
// is never written, so the position reads back without one and a restart
// takes the legacy resume rule rather than an exact rule built from half a
// key.
func TestCDCPosition_PropertyUnknownKeyIsLegacy(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := &CDCHandler{}
		k := internal.ChangeKey{
			CommitLSN: pglogrepl.LSN(rapid.Uint64().Draw(t, "commit")),
			Seq:       rapid.Uint64().Draw(t, "seq"),
		}
		if rapid.Bool().Draw(t, "zero commit") {
			k.CommitLSN = 0
		} else {
			k.Seq = 0
		}
		lsn := pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint64).Draw(t, "lsn"))
		got, _ := readKey(t, h.buildPositionAt(lsn, k))
		if got != (internal.ChangeKey{}) {
			t.Fatalf("unknown key %v was written as %v", k, got)
		}
	})
}
