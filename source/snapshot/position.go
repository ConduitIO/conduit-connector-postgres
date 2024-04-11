package snapshot

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Type int

const (
	TypeInitial Type = iota
	TypeSnapshot
	TypeCDC
)

type Position struct {
	Type     Type
	Snapshot map[string]SnapshotPosition
}

type SnapshotPosition struct {
	LastRead    int64
	SnapshotEnd int64
	Done        bool
}

func ParseSDKPosition(sdkPos sdk.Position) (Position, error) {
	var p Position
	err := json.Unmarshal(sdkPos, &p)
	if err != nil {
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
