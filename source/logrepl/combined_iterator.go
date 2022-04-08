package logrepl

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	TypeSnapshot int = iota
	TypeCDC
)

// CombinedIterator binds a CDCIterator and a SnapshotIterator together for
// coordinating the hand-off from snapshot to CDC operation modes.
type CombinedIterator struct {
	// cdc holds a reference to a CDCIterator
	cdc *CDCIterator
	// snap holds a ref to a SnapshotIterator
	snap *SnapshotIterator
}

func NewCombinedIterator() (*CombinedIterator, error) {
	return nil, sdk.ErrUnimplemented
}

func (*CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	return sdk.Record{}, sdk.ErrUnimplemented
}

func (*CombinedIterator) Ack(context.Context, sdk.Position) error {
	return sdk.ErrUnimplemented
}

func (*CombinedIterator) Teardown(context.Context) error {
	return sdk.ErrUnimplemented
}
