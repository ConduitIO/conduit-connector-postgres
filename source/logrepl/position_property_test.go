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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
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

// TestCDCPosition_PropertyCarriesSlotIdentity (#442): every CDC position the
// handler writes, the drift marker's included, carries the run's slot
// identity unchanged and the current format version, whatever its change
// key. A restart from any of them must recognise its slot.
func TestCDCPosition_PropertyCarriesSlotIdentity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.StringMatching(`l?[0-9a-f]{64}`).Draw(t, "identity")
		h := &CDCHandler{basePosition: position.Position{SlotIdentity: id}}
		for _, k := range streamKeys(t) {
			lsn := pglogrepl.LSN(rapid.Uint64Range(1, math.MaxUint64).Draw(t, "change lsn"))
			raws := [][]byte{h.buildPositionAt(lsn, k), h.buildMarkerPosition(lsn, k, "public.t")}
			for _, raw := range raws {
				p, err := position.ParseSDKPosition(raw)
				if err != nil {
					t.Fatal(err)
				}
				if p.SlotIdentity != id || p.Version != position.CurrentPositionVersion {
					t.Fatalf("position %s: identity %q version %d, want %q version %d", raw, p.SlotIdentity, p.Version, id, position.CurrentPositionVersion)
				}
			}
		}
	})
}

// TestCDCPosition_TxIdentityNamesTheDecidingTransaction (#447 re-review S2,
// round-4 B2): a position names the xid and commit time of the transaction
// its change key decides about. For a record that is its own transaction.
// A drift marker's key sits one below its change's commit LSN (#338,
// ChangeKey.Predecessor); it names the transaction that commits right
// after, the marker's change's, so a resume from a marker checks that
// transaction (savedTransaction). Any other key that is not the current
// transaction's names none.
func TestCDCPosition_TxIdentityNamesTheDecidingTransaction(t *testing.T) {
	cur := internal.ChangeKey{CommitLSN: 0x5000, Seq: 1}
	commit := time.UnixMicro(1_792_000_000_000_000)
	h := &CDCHandler{
		changeKey: func() internal.ChangeKey { return cur },
		txInfo:    func() (uint32, time.Time) { return 777, commit },
	}
	pos := func(p opencdc.Position) position.Position {
		t.Helper()
		pp, err := position.ParseSDKPosition(p)
		if err != nil {
			t.Fatal(err)
		}
		return pp
	}
	if p := pos(h.buildPositionAt(0x4f00, cur)); p.TxXid != 777 || p.TxCommitTime != commit.UnixMicro() {
		t.Fatalf("record position: xid %d commit time %d, want 777 %d", p.TxXid, p.TxCommitTime, commit.UnixMicro())
	}
	m := pos(h.buildMarkerPosition(0x4f00, cur.Predecessor(), "public.t"))
	if m.TxXid != 777 || m.TxCommitTime != commit.UnixMicro() {
		t.Fatalf("marker position one below carries xid %d commit time %d, want its change's 777 %d", m.TxXid, m.TxCommitTime, commit.UnixMicro())
	}
	got, ok := savedTransaction(CDCConfig{StartPosition: m, TxCommitLSN: cur.CommitLSN - 1, TxSeq: math.MaxUint64})
	if !ok || got.FinalLSN != cur.CommitLSN || got.Xid != 777 || !got.CommitTime.Equal(commit) {
		t.Fatalf("a marker position names %+v (ok %v), want the transaction committing at %s", got, ok, cur.CommitLSN)
	}
	// A key that is neither the current transaction's nor its predecessor
	// names nothing.
	other := internal.ChangeKey{CommitLSN: cur.CommitLSN - 2, Seq: math.MaxUint64}
	if o := pos(h.buildMarkerPosition(0x4f00, other, "public.t")); o.TxXid != 0 || o.TxCommitTime != 0 {
		t.Fatalf("an unrelated key carries xid %d commit time %d, want none", o.TxXid, o.TxCommitTime)
	}
}

// sameTransaction (#447 round-4 B1): the commit LSN, the xid and the commit
// time must all match. A restored server reuses xids and can commit at the
// same LSN, so a different commit time alone refuses; a position without an
// xid (written before it was recorded) needs only a commit at the LSN.
func TestSameTransaction(t *testing.T) {
	at := time.UnixMicro(1_792_000_000_000_000)
	want := internal.TxAt{FinalLSN: 0x5000, Xid: 740, CommitTime: at}
	for _, c := range []struct {
		name string
		got  internal.TxAt
		same bool
	}{
		{"the same transaction", internal.TxAt{FinalLSN: 0x5000, Xid: 740, CommitTime: at}, true},
		{"same xid and LSN, another commit time", internal.TxAt{FinalLSN: 0x5000, Xid: 740, CommitTime: at.Add(time.Microsecond)}, false},
		{"same xid and time, a later commit (the LSN falls inside a record)", internal.TxAt{FinalLSN: 0x5010, Xid: 740, CommitTime: at}, false},
		{"another xid", internal.TxAt{FinalLSN: 0x5000, Xid: 741, CommitTime: at}, false},
	} {
		if got := sameTransaction(want, c.got); got != c.same {
			t.Errorf("%s: sameTransaction = %v, want %v", c.name, got, c.same)
		}
	}
	legacy := internal.TxAt{FinalLSN: 0x5000}
	if !sameTransaction(legacy, internal.TxAt{FinalLSN: 0x5000, Xid: 9, CommitTime: at}) {
		t.Error("a position without an xid: any commit at its LSN is it")
	}
	if sameTransaction(legacy, internal.TxAt{FinalLSN: 0x5008, Xid: 9, CommitTime: at}) {
		t.Error("a position without an xid: a commit at another LSN is not it")
	}
}
