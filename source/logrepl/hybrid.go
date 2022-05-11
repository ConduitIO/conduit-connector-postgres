package logrepl

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

const SnapshotInitial = "initial"
const SnapshotNever = "never"

type Hybrid struct {
	config   Config
	conn     *pgx.Conn
	snapshot *SnapshotIterator
	cdc      *CDCIterator
}

func NewHybridIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*Hybrid, error) {
	h := &Hybrid{
		config: cfg,
		conn:   conn,
	}

	// NB: considering refactoring this out to a handler function
	switch h.config.SnapshotMode {
	case SnapshotInitial:
		return h.initialSnapshot(ctx, conn)
	case SnapshotNever:
		err := h.attachCDCIterator(ctx, conn)
		if err != nil {
			return nil, err
		}
		go h.cdc.Start(ctx)
		return h, nil
	default:
		err := h.attachCDCIterator(ctx, conn)
		if err != nil {
			return nil, err
		}
		return h, nil
	}
}

// initialSnapshot creates a Hybrid iterator and takes an initial Snapshot
// then starts listening for CDC messages.
func (h *Hybrid) initialSnapshot(ctx context.Context, conn *pgx.Conn) (*Hybrid, error) {
	err := h.attachCDCIterator(ctx, conn)
	if err != nil {
		return nil, err
	}

	err = h.cdc.CreatePublication(ctx, conn)
	if err != nil {
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		fmt.Printf("LOUDLY CLEANING UP THE SNAPSHOT TRANSACTION: %v\n", h)
		if err := tx.Commit(ctx); err != nil {
			// Okay so this conn is busy when we try to close it.
			// Why is that?
			fmt.Printf("failed to commit replication slot tx: %v", err)
		}
	}()

	err = h.cdc.CreateSnapshotReplicationSlot(ctx, tx.Conn())
	if err != nil {
		return nil, err
	}

	// Okay I think this might be a problem that makes this more complicated
	// and yet again another pain in my ass.
	// I'm not sure what this new connection does for transaction guarantees.
	// Neither connection is compatible with the other.

	// TODO: Handle this conn setup in a function to keep it cleaner
	config := conn.Config()
	delete(config.RuntimeParams, "replication")
	newconn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	snap, err := NewSnapshotIterator(ctx, newconn, SnapshotConfig{
		Table:     h.config.TableName,
		Columns:   h.config.Columns,
		KeyColumn: h.config.KeyColumnName,
	})
	if err != nil {
		return nil, err
	}
	h.snapshot = snap

	// this sets the h.snapshot.rows to its newconn
	err = snap.LoadRowsConn(ctx, newconn)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *Hybrid) StartCDC(ctx context.Context, conn *pgx.Conn) error {
	err := h.cdc.StartReplication(ctx, conn)
	if err != nil {
		return err
	}

	go h.cdc.Listen(ctx, conn)

	return nil
}

func (i *Hybrid) attachCDCIterator(ctx context.Context, conn *pgx.Conn) error {
	cdc, err := NewCDCIterator(ctx, conn, i.config)
	if err != nil {
		return fmt.Errorf("failed to create CDC iterator: %w", err)
	}
	i.cdc = cdc
	return nil
}

func (i *Hybrid) Ack(ctx context.Context, pos sdk.Position) error {
	// TODO: Handle cdc vs snapshot positions here
	if i.snapshot != nil {
		// naively return nil if snapshot is still present
		return nil
	}
	return i.cdc.Ack(ctx, pos)
}

func (i *Hybrid) Next(ctx context.Context) (sdk.Record, error) {
	if i.snapshot != nil {
		next, err := i.snapshot.Next(ctx)
		if err != nil {
			// TODO: check if err snapshot complete
			if errors.Is(err, ErrSnapshotComplete) {
				// TODO: This fails. We should figure out why.
				if err := i.snapshot.Teardown(ctx); err != nil {
					return sdk.Record{}, err
				}
				i.snapshot = nil
				go i.StartCDC(ctx, i.conn)
				return i.cdc.Next(ctx)
			}
			return sdk.Record{}, err
		}
		return next, nil
	}
	return i.cdc.Next(ctx)
}

func (i *Hybrid) Teardown(ctx context.Context) error {
	var err error
	if i.snapshot != nil {
		err = logOrReturnError(
			ctx,
			err,
			i.snapshot.Teardown(ctx),
			"failed to teardown snapshot iterator")
	}
	err = logOrReturnError(
		ctx,
		err,
		i.cdc.Teardown(ctx),
		"failed to teardown cdc iterator")

	return err
}
