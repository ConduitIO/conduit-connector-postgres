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

// CurrentPositionVersion is the format version this connector build writes into
// every position it serializes (see ToSDKPosition). Version history, shared with
// the release/v0.14.x branch so a position means the same thing on both:
//
//   - 0 (no "version" key): v0.14.2 and earlier. CDC positions carry only
//     LastLSN, the LSN of the record's own change.
//   - 1: DBZ-3 (this branch before #331): may carry SnapshotLowWatermarkLSN
//     and SchemaHistory.
//   - 2: CDC positions also carry TxCommitLSN, the commit LSN of the record's
//     transaction (#331). Written by this build and by the v0.14.x hotfix
//     (which has no DBZ-3 fields).
//
// Backward/forward compatibility contract (see the DBZ-3 design doc,
// docs/design-documents/20260724-dbz3-postgres-cdc-parity.md, "Upgrade / rollback"):
//   - The format is additive only: every field is omitempty, an older connector
//     ignores unknown keys, and a position whose Version is greater than
//     CurrentPositionVersion is read, not rejected (the "readable by N+1
//     versions" rule). A newer position degrades to the fields this build
//     understands.
//   - Code must key on a field's presence, never on the version number: a
//     version 2 position written by the v0.14.x hotfix has TxCommitLSN but no
//     DBZ-3 fields, and a version 1 position has DBZ-3 fields but no
//     TxCommitLSN. An absent SnapshotLowWatermarkLSN/SchemaHistory means
//     "behave as v0.14 did"; an absent TxCommitLSN means the legacy resume
//     point (logrepl/internal.ResumePoint).
const CurrentPositionVersion = 2

type Position struct {
	// Version identifies the position format. See CurrentPositionVersion for the
	// compatibility contract. Zero (the JSON key absent) means a legacy v0.14
	// position that predates DBZ-3's additive fields.
	Version   int               `json:"version,omitempty"`
	Type      Type              `json:"type"`
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
	LastLSN   string            `json:"last_lsn,omitempty"`

	// SnapshotLowWatermarkLSN is the replication slot's RestartLSN captured at
	// snapshot start, used by the resumable-snapshot consistency reconciliation
	// (DBZ-3 Area 1). It is threaded forward unchanged across every CDC-mode
	// position by CDCHandler.buildPosition so it survives the snapshot->CDC
	// handoff. Empty on a legacy (Version == 0) position and until Area 1's
	// capture-at-slot-creation logic lands. Populating it is a later DBZ-3 slice;
	// this field and its carry-forward wiring are the foundation that slice
	// attaches to.
	SnapshotLowWatermarkLSN string `json:"snapshot_low_watermark_lsn,omitempty"`

	// SchemaHistory records the recently-observed shapes of each table so schema
	// drift stays detectable across a restart (DBZ-3 Area 2 step 2). Without it
	// the in-memory RelationMessage cache starts empty on every process start,
	// so a DDL applied while the connector was down is invisible and the first
	// RelationMessage after a restart is accepted with nothing to compare it
	// against. Bounded per table — see DefaultSchemaHistoryVersions — because
	// this rides in the position payload, which is checkpointed constantly.
	// Empty on a legacy (Version == 0) position.
	SchemaHistory SchemaHistories `json:"schema_history,omitempty"`

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
}

type SnapshotPositions map[string]SnapshotPosition

type SnapshotPosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
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

// ToSDKPosition serializes the position, stamping it with CurrentPositionVersion
// so every position this connector build writes carries an explicit format
// version. Stamping here (rather than at each construction site) guarantees the
// version is set consistently on both snapshot- and CDC-mode positions and that
// re-serializing a parsed legacy position upgrades it to the current version on
// first write, with no forced migration step.
func (p Position) ToSDKPosition() opencdc.Position {
	p.Version = CurrentPositionVersion // p is a value copy; safe to mutate.

	v, err := json.Marshal(p)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
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
