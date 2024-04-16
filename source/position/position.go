package position

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
)

//go:generate stringer -type=Type -trimprefix Type

type Type int

const (
	TypeInitial Type = iota
	TypeSnapshot
	TypeCDC
)

type Position struct {
	Type     Type              `json:"type"`
	Snapshot SnapshotPositions `json:"snapshot,omitempty"`
	LastLSN  string            `json:"last_lsn,omitempty"`
}

type SnapshotPositions map[string]SnapshotPosition

type SnapshotPosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
	Done        bool  `json:"done,omitempty"`
}

func ParseSDKPosition(sdkPos sdk.Position) (Position, error) {
	var p Position

	if len(sdkPos) == 0 {
		return p, nil
	}

	if err := json.Unmarshal(sdkPos, &p); err != nil {
		return p, fmt.Errorf("invalid position: %w", err)
	}
	return p, nil
}

func (p Position) ToSDKPosition() sdk.Position {
	v, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	return sdk.Position(v)
}

func (p Position) LSN() (pglogrepl.LSN, error) {
	if p.LastLSN == "" {
		return 0, nil
	}

	lsn, err := pglogrepl.ParseLSN(p.LastLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN in position: %w", err)
	}

	return lsn, nil
}
