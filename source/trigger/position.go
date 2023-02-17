// Copyright © 2022 Meroxa, Inc.
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

package trigger

import (
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Mode defines an iterator mode.
type Mode string

const (
	ModeSnapshot Mode = "snapshot"
	ModeCDC      Mode = "cdc"
)

// Position is a position.
type Position struct {
	// Mode is the current iterator mode.
	Mode Mode `json:"mode"`
	// LastProcessedVal is the most recently processed value of an orderingColumn column.
	LastProcessedVal any `json:"lastProcessedVal"`
	// LatestSnapshotValue is a value of the orderingColumn key
	// of the most recent value item at the moment the iterator is initialised.
	LatestSnapshotValue any `json:"latestSnapshotValue"`
	// CreatedAt is the time of the first connector start (in the format hhmmss).
	// This is used in the tracking table, trigger and function names.
	CreatedAt string `json:"createdAt"`
}

// ParseSDKPosition parses sdk.Position and returns Position.
func ParseSDKPosition(position sdk.Position) (*Position, error) {
	if position == nil {
		return &Position{
			Mode:      ModeSnapshot,
			CreatedAt: time.Now().Format("150405"),
		}, nil
	}

	pos := new(Position)
	if err := json.Unmarshal(position, pos); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return pos, nil
}

// marshal marshals Position and returns sdk.Position or an error.
func (p Position) marshal() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return positionBytes, nil
}
