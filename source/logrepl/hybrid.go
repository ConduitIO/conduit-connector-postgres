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

	"github.com/conduitio/conduit-connector-postgres/source/logrepl/internal"
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
}

func NewHybridIterator(ctx context.Context, conn *pgx.Conn, cfg Config) (*Hybrid, error) {
	h := &Hybrid{
		config: cfg,
		conn:   conn,
	}

	switch h.config.SnapshotMode {
	case SnapshotInitial:
		records := make(chan sdk.Record)

		handler := NewCDCHandler(
			internal.NewRelationSet(conn.ConnInfo()),
			h.config.KeyColumnName,
			h.config.Columns,
			records,
		).Handle

		sub := internal.NewSubscription(conn.Config().Config, h.config.SlotName,
			h.config.PublicationName, []string{h.config.TableName}, 0, handler)

		h.cdc = &CDCIterator{
			config:  h.config,
			records: records,
			sub:     sub,
		}

		err := sub.CreatePublication(ctx, conn.PgConn())
		if err != nil {
			return nil, err
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, err
		}

		point, err := sub.CreateReplicationSlot(ctx, conn.PgConn())
		if err != nil {
			return nil, err
		}
		fmt.Printf("point: %v\n", point)

		err = h.attachCopier(ctx, tx.Conn())
		if err != nil {
			return nil, fmt.Errorf("failed to attach copier: %w", err)
		}

		go h.copy.Copy(ctx, conn)

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
	if h.copy != nil {
		return nil // copywriter has no ack
	}
	if h.cdc != nil {
		return h.cdc.Ack(ctx, pos)
	}
	return nil
}

func (h *Hybrid) Next(ctx context.Context) (sdk.Record, error) {
	if h.copy != nil {
		rec, err := h.copy.Next(ctx)
		if err != nil {
			if errors.Is(ErrSnapshotComplete, err) {
				h.switchToCDC(ctx)
			}
			return sdk.Record{}, fmt.Errorf("copy failed to return next: %w", err)
		}
		return rec, nil
	}
	return h.cdc.Next(ctx)
}

func (h *Hybrid) Teardown(ctx context.Context) error {
	var err error
	if h.copy != nil {
		return h.copy.Teardown(ctx)
	}
	err = logOrReturnError(ctx, err, h.cdc.Teardown(ctx),
		"failed to teardown cdc iterator")
	return err
}

func (h *Hybrid) switchToCDC(ctx context.Context) error {
	go h.cdc.Listen(ctx)
	return nil
}

func (h *Hybrid) attachCopier(ctx context.Context, conn *pgx.Conn) error {
	w, err := NewCopyDataWriter(ctx, conn, h.config)
	if err != nil {
		return err
	}
	h.copy = w
	return nil
}

func (h *Hybrid) attachCDCIterator(ctx context.Context, conn *pgx.Conn) error {
	cdc, err := NewCDCIterator(ctx, conn, h.config)
	if err != nil {
		return fmt.Errorf("failed to create CDC iterator: %w", err)
	}
	h.cdc = cdc
	return nil
}