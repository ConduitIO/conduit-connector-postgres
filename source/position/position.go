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
// every position it serializes (see ToSDKPosition). It exists so that code can
// distinguish a position written by a DBZ-3-aware connector (Version >= 1, may
// carry SnapshotLowWatermarkLSN and future DBZ-3 fields) from a legacy v0.14
// position (Version == 0, guaranteed to carry none of them).
//
// Backward/forward compatibility contract (see the DBZ-3 design doc,
// docs/design-documents/20260724-dbz3-postgres-cdc-parity.md, "Upgrade / rollback"):
//   - A legacy v0.14 position has no "version" key, so it deserializes with
//     Version == 0. Version == 0 MUST be treated as "no low watermark recorded,
//     no schema history — behave exactly as v0.14 did" until a later event
//     naturally populates the new fields.
//   - All new fields are additive and omitempty, so a position written by this
//     version is still readable by an older connector (it ignores unknown keys)
//     and by a newer one. We deliberately do NOT reject a position whose Version
//     is greater than CurrentPositionVersion: the format is additive-only, so a
//     newer position stays structurally readable, and rejecting it would break
//     the "readable by N+1 versions" rule. A newer position read here simply
//     degrades to the fields this build understands.
const CurrentPositionVersion = 1

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
