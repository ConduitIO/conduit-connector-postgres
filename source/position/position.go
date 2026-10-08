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
//     fields are ignored, and there is no TxCommitLSN.
//   - 2: CDC positions also carry TxCommitLSN, the commit LSN of the
//     record's transaction (#331). Written by this build.
//
// The format is additive only. Every field is omitempty, an older connector
// ignores keys it does not know, and a position with a higher version than
// this build knows is read, not rejected. What a reader may rely on is the
// presence of a field, not the version number: a CDC position without
// TxCommitLSN (versions 0 and 1) gets the legacy resume point (see
// logrepl/internal.ResumePoint).
const CurrentPositionVersion = 2

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
	// LSNs alone cannot say what a restart has already delivered;
	// (TxCommitLSN, LastLSN) ordered lexicographically can, because commit
	// LSNs increase in stream order and change LSNs increase within one
	// transaction. Empty on snapshot positions and on positions written
	// before format version 2.
	TxCommitLSN string `json:"tx_commit_lsn,omitempty"`
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
