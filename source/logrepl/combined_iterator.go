package logrepl

import (
	"context"
	"fmt"

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
	// TODO: Need to figure out exactly when the subscription is committed / closes.
	// Otherwise our snapshot will not be available.
	cdc, err := NewCDCIterator(ctx, conn, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	snap, err := NewSnapshotIterator(ctx, conn, SnapshotConfig{
		KeyColumn:    cfg.KeyColumnName,
		Table:        cfg.TableName,
		Columns:      cfg.Columns,
		SnapshotName: cdc.sub.SnapshotName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	ci := &CombinedIterator{
		cdc:  cdc,
		snap: snap,
	}

	return ci, nil
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
