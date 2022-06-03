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

	cdc  *CDCIterator
	copy *CopyDataWriter
	conn *pgx.Conn

	killswitch       context.CancelFunc
	snapshotComplete bool
}

func NewHybridIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*Hybrid, error) {
	h := &Hybrid{
		config: cfg,
		conn:   conn,
	}

	switch h.config.SnapshotMode {
	case SnapshotInitial:
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		})
		if err != nil {
			return nil, err
		}

		err = h.attachCDCIterator(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("failed to attach cdc iterator: %w", err)
		}

		err = h.attachCopier(ctx, tx.Conn())
		if err != nil {
			return nil, fmt.Errorf("failed to attach copier: %w", err)
		}

		go func() {
			<-h.copy.Done()
			if err := tx.Commit(ctx); err != nil {
				fmt.Printf("failed to commit: %v", err)
			}
			go h.switchToCDC(ctx, conn)
		}()

		return h, nil
	case SnapshotNever:
		err := h.attachCDCIterator(ctx, conn)
		if err != nil {
			return nil, err
		}
		go h.cdc.Listen(ctx)
		return h, nil
	default:
		err := h.attachCDCIterator(ctx, conn)
		if err != nil {
			return nil, err
		}
		return h, nil
	}
}

func (h *Hybrid) Ack(ctx context.Context, pos sdk.Position) error {
	if h.snapshotComplete {
		return h.cdc.Ack(ctx, pos)
	}
	return nil
}

func (h *Hybrid) Next(ctx context.Context) (sdk.Record, error) {
	if !h.snapshotComplete {
		rec, err := h.copy.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrSnapshotComplete) {
				h.snapshotComplete = true
				return h.Next(ctx)
			}
			return sdk.Record{}, fmt.Errorf("copy error: %w", err)
		}
		return rec, nil
	}

	return h.cdc.Next(ctx)
}

func (h *Hybrid) Teardown(ctx context.Context) error {
	if err := h.cdc.Teardown(ctx); err != nil {
		return fmt.Errorf("failed to teardown cdc iterator: %w", err)
	}
	return nil
}

func (h *Hybrid) attachCopier(ctx context.Context, conn *pgx.Conn) error {
	w, err := NewCopyDataWriter(ctx, conn, h.config)
	if err != nil {
		return err
	}
	h.copy = w
	go w.Copy(ctx, conn)
	return nil
}

func (h *Hybrid) attachCDCIterator(ctx context.Context, conn *pgx.Conn) error {
	cdc, err := NewCDCIterator(ctx, conn, h.config)
	if err != nil {
		return fmt.Errorf("failed to create CDC iterator: %w", err)
	}
	h.cdc = cdc
	err = h.cdc.AttachSnapshotSubscription(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to attach snapshot replication slot: %w", err)
	}
	return nil
}

func (h *Hybrid) switchToCDC(ctx context.Context, conn *pgx.Conn) error {

	// TODO: Refactor these two down onto CDC Iterator instead as well.
	err := h.cdc.sub.CreatePublication(ctx, conn.PgConn())
	if err != nil {
		return err
	}

	if err := h.cdc.sub.StartReplication(ctx, conn.PgConn()); err != nil {
		return err
	}

	lctx, cancel := context.WithCancel(ctx)
	h.killswitch = cancel
	go h.cdc.sub.Listen(lctx, conn.PgConn())
	<-h.cdc.sub.Ready()

	return nil
}
