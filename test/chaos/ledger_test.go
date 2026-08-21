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

// This file carries no build tag (see doc.go): it is docker-free — nothing
// here dials Postgres or spawns a process — and runs under a bare
// `go test ./...`, which is exactly what keeps the ledger analyzer inside
// `make test` (acceptance criterion B0.12).

package chaos

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/matryer/is"
)

// itoa is a tiny local alias so test bodies read as "DeliveryKey: itoa(i)"
// rather than importing strconv at every call site.
func itoa(i int) string { return strconv.Itoa(i) }

// seqEntries builds a minimal []LedgerEntry with only Seq populated, for
// FindGaps tests that don't care about any other field.
func seqEntries(seqs ...uint64) []LedgerEntry {
	out := make([]LedgerEntry, len(seqs))
	for i, s := range seqs {
		out[i] = LedgerEntry{Seq: s}
	}
	return out
}

// corruptLedgerLine bit-flips the payload of the given 1-indexed line in the
// ledger at path, invalidating its CRC without touching any other line —
// simulating a torn/bit-rotted write for ReadLedger's corruption detection.
func corruptLedgerLine(is *is.I, path string, lineNo int) {
	raw, err := os.ReadFile(path)
	is.NoErr(err)

	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	idx := lineNo - 1
	is.True(idx >= 0 && idx < len(lines))

	// Flip a character in the JSON payload (after the "<crc> " prefix) so the
	// stored CRC no longer matches — corrupting the CRC hex itself would only
	// prove hex-parsing works, not the checksum comparison.
	_, payload, ok := strings.Cut(lines[idx], " ")
	is.True(ok)
	is.True(len(payload) > 0)
	mutated := []byte(payload)
	mutated[0] ^= 0xFF
	lines[idx] = strings.SplitN(lines[idx], " ", 2)[0] + " " + string(mutated)

	is.NoErr(os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
}

// TestLedger_AppendAndReplay_RoundTrips proves the basic contract: every
// AppendSync'd entry is fsync'd, readable back via ReadLedger with no
// corrupt lines, in Seq order starting at 1, and a second OpenLedger against
// the same path picks up numbering where the first left off — the property
// a kill+restart pair depends on (harness plan §3, §6).
func TestLedger_AppendAndReplay_RoundTrips(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")

	l1, err := OpenLedger(path)
	is.NoErr(err)

	for i := 0; i < 3; i++ {
		_, err := l1.AppendSync(LedgerEntry{
			Run: 1, Op: "snapshot", Table: "t", DeliveryKey: itoa(i),
		})
		is.NoErr(err)
	}
	is.NoErr(l1.Close())

	entries, bad, err := ReadLedger(path)
	is.NoErr(err)
	is.Equal(len(bad), 0)
	is.Equal(len(entries), 3)
	for i, e := range entries {
		//nolint:gosec // i is a small, non-negative test loop index (range over a 3-element slice)
		is.Equal(e.Seq, uint64(i+1))
		is.Equal(e.DeliveryKey, itoa(i))
	}

	// A second run reopens the SAME ledger and must continue numbering from
	// where run 1 left off, not restart at 1 — this is what lets an
	// analyzer treat the two runs as one continuous delivery timeline.
	l2, err := OpenLedger(path)
	is.NoErr(err)
	e, err := l2.AppendSync(LedgerEntry{Run: 2, Op: "snapshot", Table: "t", DeliveryKey: itoa(3)})
	is.NoErr(err)
	is.Equal(e.Seq, uint64(4))
	is.NoErr(l2.Close())
}

// TestLedger_FindGaps_DetectsAnInjectedGap builds a ledger fixture with a
// deliberate hole (Seq 3 skipped) and asserts FindGaps reports exactly that
// hole — the "detects an injected gap" half of acceptance criterion B0.12.
func TestLedger_FindGaps_DetectsAnInjectedGap(t *testing.T) {
	is := is.New(t)

	entries := seqEntries(1, 2, 4, 5, 6)
	is.Equal(FindGaps(entries), []uint64{3})

	// A gap-free ledger reports no gaps.
	is.Equal(FindGaps(seqEntries(1, 2, 3)), []uint64(nil))

	// An empty ledger has no gaps by definition.
	is.Equal(FindGaps(nil), []uint64(nil))
}

// TestLedger_FindDuplicates_GroupsByDeliveryIdentity proves duplicates are
// grouped by (op, table, delivery key) — not by delivery key alone, so the
// same key legitimately reused across two different tables (or once as a
// snapshot delivery and once as a CDC delivery of the same row) is never
// misreported as a duplicate.
func TestLedger_FindDuplicates_GroupsByDeliveryIdentity(t *testing.T) {
	is := is.New(t)

	entries := []LedgerEntry{
		{Seq: 1, Op: "snapshot", Table: "t1", DeliveryKey: "5"},
		{Seq: 2, Op: "snapshot", Table: "t2", DeliveryKey: "5"}, // same key, different table: NOT a dup
		{Seq: 3, Op: "cdc", Table: "t1", DeliveryKey: "5"},      // same key, different op: NOT a dup
		{Seq: 4, Op: "snapshot", Table: "t1", DeliveryKey: "5"}, // dup of seq 1
	}

	dups := FindDuplicates(entries)
	is.Equal(len(dups), 1)
	is.Equal(dups[0].Op, "snapshot")
	is.Equal(dups[0].Table, "t1")
	is.Equal(dups[0].DeliveryKey, "5")
	is.Equal(dups[0].Seqs, []uint64{1, 4})
}

// TestLedger_OutOfBound_DetectsAnInjectedOutOfBoundDuplicate is the "out of
// bound duplicate" half of acceptance criterion B0.12: a duplicate whose
// first occurrence lands strictly before the boundary is the resumability
// bug the plan's DUP BOUND relation exists to catch (harness plan §5, §8
// row 2); a duplicate whose first occurrence lands at/after the boundary is
// expected re-processing of in-flight work and must NOT be flagged.
func TestLedger_OutOfBound_DetectsAnInjectedOutOfBoundDuplicate(t *testing.T) {
	is := is.New(t)

	const boundary = uint64(10)

	entries := []LedgerEntry{
		// Out-of-bound: first delivered at seq 2 (before the boundary),
		// then redelivered at seq 12 — the connector re-did work it should
		// have remembered as already durable.
		{Seq: 2, Op: "snapshot", Table: "t", DeliveryKey: "bad"},
		{Seq: 12, Op: "snapshot", Table: "t", DeliveryKey: "bad"},

		// In-bound: first delivered at seq 11 (at/after the boundary), then
		// redelivered at seq 15 — harmless re-processing of work that was
		// still in flight when the boundary was crossed.
		{Seq: 11, Op: "snapshot", Table: "t", DeliveryKey: "ok"},
		{Seq: 15, Op: "snapshot", Table: "t", DeliveryKey: "ok"},
	}

	dups := FindDuplicates(entries)
	is.Equal(len(dups), 2) // both are duplicates in the raw sense

	oob := OutOfBound(dups, boundary)
	is.Equal(len(oob), 1)
	is.Equal(oob[0].DeliveryKey, "bad")
}

// TestLedger_ReadLedger_DetectsCorruptLine proves the CRC check: a
// hand-corrupted line (bit-flipped payload, stale CRC) is reported as a
// CorruptLine rather than silently accepted or aborting the read of every
// other line in the file.
func TestLedger_ReadLedger_DetectsCorruptLine(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")

	l, err := OpenLedger(path)
	is.NoErr(err)
	_, err = l.AppendSync(LedgerEntry{Op: "snapshot", Table: "t", DeliveryKey: "1"})
	is.NoErr(err)
	_, err = l.AppendSync(LedgerEntry{Op: "snapshot", Table: "t", DeliveryKey: "2"})
	is.NoErr(err)
	is.NoErr(l.Close())

	corruptLedgerLine(is, path, 1)

	entries, bad, err := ReadLedger(path)
	is.NoErr(err)
	is.Equal(len(entries), 1) // only the untouched line 2 survives
	is.Equal(entries[0].DeliveryKey, "2")
	is.Equal(len(bad), 1)
	is.Equal(bad[0].LineNo, 1)
}

// TestLedger_OpenLedger_ResumesPastACorruptTail proves OpenLedger's replay
// tolerates a torn trailing line (the shape a kill mid-write, before fsync,
// could leave — AppendSync's doc comment) by resuming numbering from the
// last GOOD entry, not the corrupt one.
func TestLedger_OpenLedger_ResumesPastACorruptTail(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")

	l, err := OpenLedger(path)
	is.NoErr(err)
	_, err = l.AppendSync(LedgerEntry{Op: "snapshot", Table: "t", DeliveryKey: "1"})
	is.NoErr(err)
	_, err = l.AppendSync(LedgerEntry{Op: "snapshot", Table: "t", DeliveryKey: "2"})
	is.NoErr(err)
	is.NoErr(l.Close())

	corruptLedgerLine(is, path, 2)

	l2, err := OpenLedger(path)
	is.NoErr(err)
	e, err := l2.AppendSync(LedgerEntry{Op: "snapshot", Table: "t", DeliveryKey: "3"})
	is.NoErr(err)
	is.Equal(e.Seq, uint64(2)) // resumed after the last GOOD entry (seq 1), not the corrupt seq 2
	is.NoErr(l2.Close())
}
