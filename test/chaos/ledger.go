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

// This file carries no build tag (see doc.go) — the ledger format and its
// analyzer are plain data-structure code with no Postgres dependency, so
// they run under a bare `go test ./...` via ledger_test.go.
package chaos

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LedgerEntry is one durably-recorded delivery. A chaos child appends one of
// these — and fsyncs it — strictly before it acks the corresponding record
// upstream (see Ledger.AppendSync). The ledger is the harness's only
// downstream: whatever a real destination would have durably received, this
// is standing in for.
type LedgerEntry struct {
	// Seq is assigned by AppendSync, monotonically increasing from 1 across
	// the whole ledger file (i.e. across every run that has ever appended to
	// it, not reset per run) — the ledger is explicitly shared across a
	// kill+restart pair (see the harness plan §3), so a gap or duplicate
	// analysis has to reason about one continuous timeline regardless of
	// which OS process wrote which line.
	Seq uint64 `json:"seq"`

	// Run identifies which child process invocation wrote this entry (1 for
	// the first run, 2 for the run that resumes after a kill, ...). Not used
	// by the no-kill smoke scenario (always 1) but load-bearing for the
	// run-2-is-a-new-process proofs a later kill scenario needs (harness
	// plan §6).
	Run int `json:"run"`

	// Op is "snapshot" or "cdc" — which iterator produced the record this
	// entry represents.
	Op string `json:"op"`

	// Table is the source table name. Recorded directly from the harness's
	// own configuration (the scenarios in this package only ever read one
	// table), not decoded from record metadata — see child.go.
	Table string `json:"table"`

	// DeliveryKey identifies the specific delivery this entry records,
	// unique enough that two entries sharing a DeliveryKey are a duplicate
	// delivery of the same thing. child.go derives it from the record's
	// decoded position (the snapshot cursor's LastRead for snapshot records,
	// the LSN for CDC records) rather than the row's business key, so the
	// ledger analyzer needs no schema awareness.
	DeliveryKey string `json:"delivery_key"`

	// Key is opencdc.Record.Key.Bytes() - the row identity the CONNECTOR
	// itself attaches to the record (its "id" column, by default), captured
	// verbatim. This is deliberately independent of DeliveryKey (derived
	// from the record's position, not the row) and of Seq (assigned by this
	// ledger, not the connector): DeliveryKey/Seq can only prove "this
	// harness never durably recorded the same POSITION twice" - they say
	// nothing about whether every distinct ROW was actually delivered, so a
	// connector that redelivered row 1 under four different snapshot
	// cursors and never delivered rows 2-4 would still pass a
	// DeliveryKey-only duplicate/gap check. Key is what lets a scenario
	// assert the actual set of rows seen, independent of how many times or
	// under what position each one arrived.
	Key string `json:"key"`

	// Resumed mirrors source/snapshot.MetadataSnapshotResumed on the record
	// this entry represents — true only when a prior run's persisted
	// position caused this snapshot record to be re-emitted. Always false in
	// a single, uninterrupted run (e.g. the no-kill smoke scenario).
	Resumed bool `json:"resumed"`

	// RawPosition is the record's raw opencdc.Position bytes, so a later
	// scenario can independently recompute a RESUME_FROM hash (harness plan
	// §6) from the ledger rather than trusting the child's own claim about
	// what it resumed from.
	RawPosition string `json:"raw_position"`

	// WrittenAt is set by AppendSync at append time, wall-clock, for human
	// debugging only — no assertion in this package depends on it.
	WrittenAt time.Time `json:"written_at"`
}

// deliveryID is the composite identity AppendSync's duplicate analysis groups
// on: the same DeliveryKey in two different tables, or under two different
// ops, is not a duplicate of "the same thing" and must not be conflated.
func (e LedgerEntry) deliveryID() string {
	return e.Op + "\x00" + e.Table + "\x00" + e.DeliveryKey
}

// Ledger is an append-only, single-writer, fsync-before-return, CRC-checked
// JSON-lines file. It is deliberately dumb: no in-memory index, no
// compaction, no concurrent-writer support — a chaos child is the only
// process that ever appends to one, and it does so from a single goroutine
// (see child.go's read/ack loop), which is what makes the fsync-before-ack
// ordering below meaningful instead of merely decorative.
type Ledger struct {
	mu   sync.Mutex
	path string
	f    *os.File
	next uint64 // next Seq to assign
}

// lineFormat: "<8-hex-digit CRC32 (IEEE) of the JSON payload> <JSON payload>\n".
// The checksum is a prefix, not a field inside the JSON it covers, so
// computing it never has to special-case its own presence. json.Marshal
// always escapes control characters (including newline) inside string
// values, so the payload itself is guaranteed not to contain a raw '\n' —
// splitting the file on '\n' is safe.
const lineSep = ' '

// OpenLedger opens (creating if needed) the ledger at path for appending,
// replaying any existing content first to recover the next Seq to assign —
// this is what lets a second run (after a kill) keep appending to the SAME
// ledger a first run started, with Seq staying globally monotonic across
// both (see LedgerEntry.Seq's doc comment).
//
// A malformed or truncated trailing line (e.g. the file was truncated mid
// fsync by a kill landing inside AppendSync itself, not after it — see that
// method's doc) is tolerated: replay stops at the first bad line and OpenLedger
// continues from the Seq after the last GOOD line, exactly as if the bad
// tail had never been written. It is the caller's job (via ReadLedger) to
// decide whether a torn tail is itself a test failure.
func OpenLedger(path string) (*Ledger, error) {
	existing, _, err := ReadLedger(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("replay existing ledger %q: %w", path, err)
	}

	var next uint64 = 1
	if len(existing) > 0 {
		next = existing[len(existing)-1].Seq + 1
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open ledger %q: %w", path, err)
	}

	return &Ledger{path: path, f: f, next: next}, nil
}

// AppendSync assigns e the next Seq, marshals it, writes "<crc> <json>\n" as
// a single Write call, and fsyncs before returning.
//
// Invariant (models ConduitIO/conduit's post-045f283 persist-before-ack
// ordering — see doc.go's Ledger section for the "model, not the engine"
// caveat): callers MUST complete AppendSync before acking the corresponding
// record upstream. Acking first would make a harness-caused gap
// indistinguishable from a genuine connector bug — the entire point of
// fsync-before-ack is that once this call returns, the delivery is
// durable no matter what happens to the process next.
func (l *Ledger) AppendSync(e LedgerEntry) (LedgerEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e.Seq = l.next
	e.WrittenAt = time.Now().UTC()

	payload, err := json.Marshal(e)
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("marshal ledger entry: %w", err)
	}

	crc := crc32.ChecksumIEEE(payload)
	line := fmt.Sprintf("%08x%c%s\n", crc, lineSep, payload)

	if _, err := l.f.WriteString(line); err != nil {
		return LedgerEntry{}, fmt.Errorf("write ledger line: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return LedgerEntry{}, fmt.Errorf("fsync ledger: %w", err)
	}

	l.next++
	return e, nil
}

// Close closes the underlying file. It does not fsync — every durability
// guarantee this type makes is already satisfied per-line by AppendSync.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// CorruptLine describes one ledger line that failed its CRC check or could
// not be parsed as JSON at all.
type CorruptLine struct {
	LineNo int
	Reason string
	Raw    string
}

// ReadLedger reads and validates every line in the ledger at path, returning
// the entries that check out (in file order) and a report of every line that
// didn't. It never fails the read outright on a corrupt line — a genuinely
// torn trailing line (the one a kill mid-fsync-window could produce) is
// exactly the case this function exists to surface as data, not as a Go
// error that aborts analysis of everything that came before it.
//
// It DOES return an error (with os.IsNotExist true) if path does not exist,
// so OpenLedger's "does this ledger already have history" check can
// distinguish "no prior run" from "prior run's ledger is unreadable".
func ReadLedger(path string) ([]LedgerEntry, []CorruptLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var (
		entries []LedgerEntry
		bad     []CorruptLine
		lineNo  int
	)

	sc := bufio.NewScanner(f)
	// A ledger line can carry a full opencdc.Position round-tripped as JSON;
	// give the scanner generous headroom over its 64KiB default so a large
	// position never gets misreported as a corrupt/truncated line.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		lineNo++
		line := sc.Text()

		crcHex, payload, ok := strings.Cut(line, string(lineSep))
		if !ok {
			bad = append(bad, CorruptLine{LineNo: lineNo, Reason: "missing crc separator", Raw: line})
			continue
		}

		wantCRC, err := strconv.ParseUint(crcHex, 16, 32)
		if err != nil {
			bad = append(bad, CorruptLine{LineNo: lineNo, Reason: fmt.Sprintf("bad crc prefix: %v", err), Raw: line})
			continue
		}

		if gotCRC := crc32.ChecksumIEEE([]byte(payload)); uint32(wantCRC) != gotCRC {
			bad = append(bad, CorruptLine{
				LineNo: lineNo,
				Reason: fmt.Sprintf("crc mismatch: want %08x, got %08x", wantCRC, gotCRC),
				Raw:    line,
			})
			continue
		}

		var e LedgerEntry
		if err := json.Unmarshal([]byte(payload), &e); err != nil {
			bad = append(bad, CorruptLine{LineNo: lineNo, Reason: fmt.Sprintf("json: %v", err), Raw: line})
			continue
		}

		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return entries, bad, fmt.Errorf("scan ledger %q: %w", path, err)
	}

	return entries, bad, nil
}

// FindGaps returns every Seq missing from the ledger's [min, max] range,
// ascending. An empty ledger has no gaps by definition.
func FindGaps(entries []LedgerEntry) []uint64 {
	if len(entries) == 0 {
		return nil
	}

	seen := make(map[uint64]bool, len(entries))
	minSeq, maxSeq := entries[0].Seq, entries[0].Seq
	for _, e := range entries {
		seen[e.Seq] = true
		if e.Seq < minSeq {
			minSeq = e.Seq
		}
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}

	var gaps []uint64
	for s := minSeq; s <= maxSeq; s++ {
		if !seen[s] {
			gaps = append(gaps, s)
		}
	}
	return gaps
}

// Duplicate is every ledger line sharing a delivery identity (op+table+
// DeliveryKey) with at least one earlier line, in ascending Seq order.
type Duplicate struct {
	Op          string
	Table       string
	DeliveryKey string
	Seqs        []uint64
}

// FindDuplicates groups entries by delivery identity and returns every group
// with more than one member, ordered by each group's first-seen Seq.
func FindDuplicates(entries []LedgerEntry) []Duplicate {
	order := make([]string, 0)
	groups := make(map[string]*Duplicate)

	for _, e := range entries {
		id := e.deliveryID()
		d, ok := groups[id]
		if !ok {
			d = &Duplicate{Op: e.Op, Table: e.Table, DeliveryKey: e.DeliveryKey}
			groups[id] = d
			order = append(order, id)
		}
		d.Seqs = append(d.Seqs, e.Seq)
	}

	var out []Duplicate
	for _, id := range order {
		if d := groups[id]; len(d.Seqs) > 1 {
			out = append(out, *d)
		}
	}
	return out
}

// OutOfBound filters dups to the ones whose EARLIEST occurrence's Seq is
// strictly before boundary. A delivery whose first occurrence already
// landed before the boundary and then got redelivered means work the
// connector should have remembered as already durably done was redone from
// scratch — the resumability bug the harness plan's DUP BOUND relation (§5)
// and perturbation-matrix row 2 (§8) exist to catch. A duplicate whose first
// occurrence is at or after boundary is just expected re-processing of work
// that was still in flight at the boundary, not a bug.
func OutOfBound(dups []Duplicate, boundary uint64) []Duplicate {
	var out []Duplicate
	for _, d := range dups {
		first := d.Seqs[0]
		for _, s := range d.Seqs {
			if s < first {
				first = s
			}
		}
		if first < boundary {
			out = append(out, d)
		}
	}
	return out
}
