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

		err = h.attachCDCIterator(ctx, tx.Conn())
		if err != nil {
			return nil, fmt.Errorf("failed to attach cdc iterator: %w", err)
		}

		err = h.attachCopier(ctx, tx.Conn())
		if err != nil {
			return nil, fmt.Errorf("failed to attach copier: %w", err)
		}

		go func() {
			if err := h.waitForCopy(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					fmt.Printf("wait for copy received an error: %v", err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				fmt.Printf("failed to commit: %v", err)
			}
			if err := h.switchToCDC(ctx, conn); err != nil {
				if !errors.Is(err, context.Canceled) {
					fmt.Printf("failed to switch to cdc: %v", err)
				}
			}
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
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return h.next(ctx)
	}
}

func (h *Hybrid) next(ctx context.Context) (sdk.Record, error) {
	if !h.snapshotComplete {
		rec, err := h.copy.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrSnapshotComplete) {
				h.snapshotComplete = true
				return h.cdc.Next(ctx)
			}
			return sdk.Record{}, fmt.Errorf("copy error: %w", err)
		}
		return rec, nil
	}

	return h.cdc.Next(ctx)
}

func (h *Hybrid) Teardown(ctx context.Context) error {
	if h.copy != nil {
		if err := h.copy.Teardown(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("failed to teardown copy iterator: %w", err)
		}
	}
	if h.cdc != nil {
		if err := h.cdc.Teardown(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("failed to teardown cdc iterator: %w", err)
		}
	}
	return nil
}

// Wait will wait for CDC or context to signal Done, but not copy's Done.
// Copy's done is ignored to handle transition behavior.
func (h *Hybrid) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.cdc.Done():
		return h.cdc.Err(ctx)
	}
}

func (h *Hybrid) waitForCopy(ctx context.Context) error {
	select {
	case <-h.copy.Done():
		return h.copy.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Hybrid) Done(ctx context.Context) <-chan struct{} {
	<-h.copy.Done()
	return h.cdc.Done()
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
	cdc, err := NewCDCIterator(ctx, h.config)
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
	err := h.cdc.CreatePublication(ctx, conn)
	if err != nil {
		return err
	}
	if err := h.cdc.StartReplication(ctx, conn); err != nil {
		return err
	}
	return nil
}
