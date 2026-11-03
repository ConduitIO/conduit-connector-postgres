// Copyright © 2024 Meroxa, Inc.
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

package position

import (
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jackc/pglogrepl"
)

//go:generate stringer -type=Type -trimprefix Type

type Type int

const (
	TypeInitial Type = iota
	TypeSnapshot
	TypeCDC
)

// CurrentPositionVersion is the position format version this build writes
// (see ToSDKPosition). Version history, shared with the main branch so a
// position stays meaningful across upgrades:
//
//   - 0 (no "version" key): v0.14.2 and earlier. CDC positions carry only
//     LastLSN, the LSN of the record's own change.
//   - 1: the DBZ-3 format on main (adds snapshot low watermark and schema
//     history). Never written by v0.14.x. Read like 0 here: the extra
//     fields are ignored, and there is no change key.
//   - 2: CDC positions also carry the change key (#331): TxCommitLSN, the
//     commit LSN of the record's transaction, and TxSeq, the change's
//     ordinal within it. Written by this build.
//   - 3: reserved by main (v0.15.0): a schema-drift marker's position also
//     carries schema_drift_marker (the drifted table), which the
//     destination requires before it skips a marker (#418). Never written by
//     v0.14.x. Read like 2 here: the field is ignored and the change key is
//     used as usual.
//   - 4: reserved by main (v0.17.5): every position also carries
//     slot_identity, system_identifier and timeline (#442, #447). Never
//     written by v0.14.x. Read like 2 here: the fields are ignored.
//   - 5: snapshot entries say whether their scan covered the keys at or
//     below zero (from_min, low_pass_last_read), and every position carries
//     low_key_gap_checked (main's
//     docs/design-documents/20261102-snapshot-keys-at-or-below-zero.md).
//     Written by this build and by main. Versions 0 to 4 were written by
//     builds whose snapshot skipped every row keyed 0 or below (v0.14.0 to
//     v0.14.6, v0.15.0 to v0.19.0). A version 5 position from this branch
//     has no slot_identity; main reads it like any position without one.
//
// The format is additive only. Every field is omitempty, an older connector
// ignores keys it does not know, and a position with a higher version than
// this build knows is read, not rejected. What a reader may rely on is the
// presence of a field, not the version number: a CDC position without both
// TxCommitLSN and TxSeq (versions 0 and 1) gets the legacy resume point (see
// logrepl/internal.ResumePoint).
const CurrentPositionVersion = 5

type Position struct {
	// Version identifies the position format; see CurrentPositionVersion.
	// Zero (the key absent) is a v0.14.2-or-earlier position.
	Version   int               `json:"version,omitempty"`
	Type      Type              `json:"type"`
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
	LastLSN   string            `json:"last_lsn,omitempty"`

	// TxCommitLSN is the commit LSN (BeginMessage.FinalLSN) of the
	// transaction the CDC record at LastLSN belongs to. Logical replication
	// delivers transactions in commit order, but each change carries its own
	// LSN, so a transaction that began before another and committed after it
	// delivers LOWER change LSNs than ones already delivered (#331). Change
	// LSNs alone cannot say what a restart has already delivered.
	// (TxCommitLSN, TxSeq) can: commit LSNs increase in stream order, and
	// TxSeq numbers the changes within one transaction (see
	// logrepl/internal.ChangeKey). Empty on snapshot positions and on
	// positions written before format version 2.
	TxCommitLSN string `json:"tx_commit_lsn,omitempty"`

	// TxSeq is the 1-based ordinal of the record's change within its
	// transaction, counted from the BeginMessage. Together with TxCommitLSN it
	// is the change's key in stream order. LastLSN cannot serve, because the
	// rows of one multi-row insert (COPY) share one LSN. Zero on snapshot
	// positions and on positions written before format version 2; a position
	// with TxCommitLSN but no TxSeq is read as legacy.
	TxSeq uint64 `json:"tx_seq,omitempty"`

	// LowKeyGapChecked is true when this position's history cannot hide the
	// snapshot gap of v0.14.0 to v0.19.0, whose snapshot skipped every row
	// keyed 0 or below (docs/design-documents/20261102-snapshot-keys-at-or-below-zero.md).
	// It is set when a run starts fresh, when it resumes a snapshot (each
	// table's entry then repairs its own gap, see SnapshotPosition.FromMin),
	// and after the one-time startup check of a legacy CDC position has
	// reported the tables that still hold such rows. It is carried forward
	// onto every snapshot and CDC position. Absent on a position written
	// before format version 5: a resume from such a CDC position runs the
	// check (logrepl.checkLowKeyGap).
	LowKeyGapChecked bool `json:"low_key_gap_checked,omitempty"`
}

type SnapshotPositions map[string]SnapshotPosition

// SnapshotPosition is one table's snapshot progress. The iterator writes a
// table's entry only from a row it read (snapshot.Iterator.buildRecord), so
// an entry always names a real key.
type SnapshotPosition struct {
	// LastRead is the key of the last row read by the main pass, which reads
	// the keys in ascending order up to SnapshotEnd.
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`

	// FromMin is true when every row keyed at or below LastRead was read:
	// the scan started at the table's lowest key. Every entry written from
	// format version 5 on has it. An entry without it was written by a
	// v0.14.0 to v0.19.0 build, whose scan started at "key > 0", so its rows
	// keyed 0 or below were never read; resuming it first runs the low pass
	// over those keys (snapshot.FetchWorker).
	FromMin bool `json:"from_min,omitempty"`

	// LowPassLastRead is the key of the last row read by the low pass, the
	// catch-up scan of the keys at or below zero that a legacy entry (no
	// FromMin) still owes. It is set only on positions of low-pass rows, and
	// LastRead keeps the legacy value meanwhile, so each of the two fields
	// only moves forward (invariant 2). Nil when no low-pass row was read.
	LowPassLastRead *int64 `json:"low_pass_last_read,omitempty"`
}

func ParseSDKPosition(sdkPos opencdc.Position) (Position, error) {
	var p Position

	if len(sdkPos) == 0 {
		return p, nil
	}

	if err := json.Unmarshal(sdkPos, &p); err != nil {
		return p, fmt.Errorf("invalid position: %w", err)
	}
	return p, nil
}

// ToSDKPosition serializes the position, stamped with CurrentPositionVersion.
func (p Position) ToSDKPosition() opencdc.Position {
	p.Version = CurrentPositionVersion // p is a value copy

	v, err := json.Marshal(p)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

// ImpliesSlot reports whether the position was written by a run that held a
// replication slot, so resuming from it needs that slot (or one that still
// covers the position) to exist. It is true for every position a run has
// produced, snapshot or CDC, and false for the empty position of a fresh start.
//
// A snapshot position implies a slot too: the run took its snapshot at the
// slot's consistent point and relies on the slot to replay every change made
// after it.
func (p Position) ImpliesSlot() bool {
	return p.Type != TypeInitial || p.LastLSN != "" || p.TxCommitLSN != ""
}

// LSN returns the last LSN (Log Sequence Number) in the position.
func (p Position) LSN() (pglogrepl.LSN, error) {
	if p.LastLSN == "" {
		return 0, nil
	}

	lsn, err := pglogrepl.ParseLSN(p.LastLSN)
	if err != nil {
		return 0, err
	}

	return lsn, nil
}

// TxCommit returns the commit LSN of the position's transaction, or 0 when
// the position does not carry one (a snapshot position, or a CDC position
// written before format version 2).
func (p Position) TxCommit() (pglogrepl.LSN, error) {
	if p.TxCommitLSN == "" {
		return 0, nil
	}
	lsn, err := pglogrepl.ParseLSN(p.TxCommitLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse tx commit LSN: %w", err)
	}
	return lsn, nil
}
