package logrepl

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

const (
	TypeSnapshot int = iota
	TypeCDC
)

// CombinedIterator binds a CDCIterator and a SnapshotIterator together for
// coordinating the hand-off from snapshot to CDC operation modes.
type CombinedIterator struct {
	cdc  *CDCIterator
	snap *SnapshotIterator
}

func NewCombinedIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*CombinedIterator, error) {
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
