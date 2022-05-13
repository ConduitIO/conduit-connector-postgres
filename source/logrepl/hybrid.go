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
	config Config

	snapshot *SnapshotIterator
	cdc      *CDCIterator

	conn *pgx.Conn
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
		if err := tx.Commit(ctx); err != nil {
			fmt.Printf("failed to commit replication slot tx: %v", err)
		}
	}()

	err = h.cdc.CreateSnapshotReplicationSlot(ctx, tx.Conn())
	if err != nil {
		return nil, err
	}

	config := conn.Config()

	// Deleting this line will not allow us to query rows.
	// Extended query protocol not supported in replication connections.
	delete(config.RuntimeParams, "replication")

	snapconn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := snapconn.Close(ctx); err != nil {
			fmt.Printf("SNAPCONN FAILED TO CLOSE: %v", err)
		}
	}()

	snaptx, err := snapconn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return nil, err
	}

	snap := &SnapshotIterator{config: SnapshotConfig{
		Table:     h.config.TableName,
		Columns:   h.config.Columns,
		KeyColumn: h.config.KeyColumnName,
	}}
	err = snap.LoadRowsConn(ctx, snaptx.Conn())
	if err != nil {
		return nil, err
	}
	h.snapshot = snap

	if err := snaptx.Commit(ctx); err != nil {
		fmt.Printf("SNAPTX failed to commit up snaptx: %v", err)
	}

	return h, nil
}

func (h *Hybrid) StartCDC(ctx context.Context, conn *pgx.Conn) error {
	err := h.cdc.StartReplication(ctx, conn)
	if err != nil {
		return err
	}
	go h.cdc.Listen(ctx, conn)
	<-h.cdc.Ready()
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
	// TODO: Handle cdc vs snapshot positions here.
	if i.snapshot != nil {
		return nil
	}
	if i.cdc != nil {
		return i.cdc.Ack(ctx, pos)
	}

	return nil
}

func (i *Hybrid) Next(ctx context.Context) (sdk.Record, error) {
	if i.snapshot != nil {
		next, err := i.snapshot.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrSnapshotComplete) {
				if err := i.snapshot.Teardown(ctx); err != nil {
					return sdk.Record{}, err
				}
				i.snapshot = nil

				go i.StartCDC(ctx, i.conn)

				<-i.cdc.Ready()
				return i.cdc.Next(ctx)
			}

			return sdk.Record{}, fmt.Errorf("snapshot failed: %w", err)
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
