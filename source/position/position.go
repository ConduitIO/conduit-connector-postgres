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
//   - 2: CDC positions also carry the change key (#331): TxCommitLSN, the
//     commit LSN of the record's transaction, and TxSeq, the change's ordinal
//     within it. Written by this build and by the v0.14.x hotfix (which has
//     no DBZ-3 fields).
//   - 3: the position of a schema-drift marker also carries SchemaDriftMarker
//     (docs/design-documents/20261015-drift-marker-source-signal.md). No
//     other position has it.
//   - 4: every position also carries SlotIdentity, the identity of the
//     replication slot the position was taken from (#442,
//     docs/design-documents/20261021-slot-identity.md). A position without
//     it (versions 0 to 3) is checked as a legacy position on resume.
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
//     DBZ-3 fields, and a version 1 position has DBZ-3 fields but no change
//     key. An absent SnapshotLowWatermarkLSN/SchemaHistory means
//     "behave as v0.14 did"; a missing TxCommitLSN or TxSeq means the legacy
//     resume point (logrepl/internal.ResumePoint).
const CurrentPositionVersion = 4

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

	// SchemaDriftMarker is set only on the position of a schema-drift marker
	// record, to the drifted table as "namespace.table". It is the proof the
	// destination needs before it skips and acks a marker without writing
	// it. Records from other systems carry their own positions, and the
	// classic engine refuses a processor that changes a record's position
	// (pkg/lifecycle/stream/processor.go). The arch-v2 engine lets a
	// processor rewrite positions (pkg/lifecycle-poc/funnel/batch.go), so
	// there a processor could forge the field; the record it forges is then
	// skipped, which a processor with that power could do to any record
	// anyway. It plays no part in resuming, and the source never carries it
	// forward to a later position.
	SchemaDriftMarker string `json:"schema_drift_marker,omitempty"`

	// SlotIdentity is the secret key of the replication slot history this
	// position belongs to (#442). The connector authenticates the logical
	// decoding messages (beacons) it keeps in the WAL after the slot's
	// confirmed_flush_lsn with it (HMAC-SHA256), so on resume a slot that
	// still holds a beacon authenticated with this key is the slot the
	// position was taken from, and a slot that was dropped and created again
	// under the same name, which starts after every beacon, is not. The key
	// itself never reaches the database or a log (logs show
	// internal.IdentityFingerprint). It is 256 random bits, also for a run
	// that resumes from a position written before format version 4. It is
	// carried forward onto every snapshot
	// and CDC position, and replaced by a new key whenever the connector
	// accepts a slot that is not the position's (logrepl.allowWALGap). Empty
	// on a position written before format version 4.
	SlotIdentity string `json:"slot_identity,omitempty"`

	// SystemIdentifier and Timeline name the WAL history the position was
	// written on (#447): the cluster's system identifier and its timeline
	// at the time, from IDENTIFY_SYSTEM. An LSN means something only within
	// one history, so a resume on another cluster, an earlier timeline, a
	// timeline that left this one before the position, or a server whose
	// WAL is behind the position (a restored backup or VM image) is refused
	// (logrepl.checkWALHistory). Carried forward onto every position, and
	// updated to the server's current values by every start. Empty on a
	// position written before format version 4.
	SystemIdentifier string `json:"system_identifier,omitempty"`
	Timeline         int32  `json:"timeline,omitempty"`

	// TxXid and TxCommitTime are the xid and the commit timestamp (Unix
	// microseconds) of the transaction that commits at TxCommitLSN, from
	// pgoutput's BEGIN message (#447 re-review S2). With TxCommitLSN they
	// name one transaction of one WAL history: a resume that the position's
	// bound covers checks that the slot decodes exactly this transaction at
	// TxCommitLSN, which a server restored to an earlier point on the same
	// timeline, whose new WAL reuses the LSNs, does not
	// (logrepl.verifyCoveredTransaction). Set with the change key; empty on
	// snapshot positions and on positions written before this check.
	TxXid        uint32 `json:"tx_xid,omitempty"`
	TxCommitTime int64  `json:"tx_commit_time,omitempty"`
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

// CoveredThrough returns the highest LSN a replication slot's
// confirmed_flush_lsn can be at while every change after this position is
// still decoded from it, or 0 when the position gives no such bound.
//
// Postgres decodes every transaction whose commit record is at or after the
// slot's confirmed_flush_lsn. The position's transaction commits at
// TxCommitLSN, and every change not yet delivered belongs to that transaction
// or to one that commits later, so a slot whose confirmed_flush_lsn is at or
// below TxCommitLSN loses nothing, whichever slot it is. A position without a
// change key falls back to LastLSN, the change's own LSN, which is below its
// commit. A snapshot position gives no bound: its slot was created after the
// snapshot started.
func (p Position) CoveredThrough() (pglogrepl.LSN, error) {
	if p.Type != TypeCDC {
		return 0, nil
	}
	if p.TxCommitLSN != "" && p.TxSeq != 0 {
		return p.TxCommit()
	}
	return p.LSN()
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
