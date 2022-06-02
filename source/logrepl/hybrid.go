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
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
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

		err = h.attachSnapshotReplication(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("failed to attach snapshot replication slot: %w", err)
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
			h.switchToCDC(ctx, conn)
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
				return h.cdc.Next(ctx)
			}
			return sdk.Record{}, fmt.Errorf("copy error: %w", err)
		}
		return rec, nil
	}

	return h.cdc.Next(ctx)
}

func (h *Hybrid) Teardown(ctx context.Context) error {
	var err error
	err = logOrReturnError(ctx, err, h.cdc.Teardown(ctx),
		"failed to teardown cdc iterator")
	return err
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
	return nil
}

func (h *Hybrid) attachSnapshotReplication(ctx context.Context, conn *pgx.Conn) error {
	// TODO: refactor this down into the Subscription library.
	point, err := h.createReplicationSlot(ctx, conn)
	if err != nil {
		return err
	}
	lsn, err := pglogrepl.ParseLSN(point)
	if err != nil {
		return err
	}
	h.config.Position = LSNToPosition(lsn)

	records := make(chan sdk.Record)
	handler := NewCDCHandler(
		internal.NewRelationSet(conn.ConnInfo()),
		h.config.KeyColumnName,
		h.config.Columns,
		records,
	).Handle

	sub := internal.NewSubscription(
		conn.Config().Config,
		h.config.SlotName,
		h.config.PublicationName,
		[]string{h.config.TableName},
		lsn,
		handler)

	h.cdc = &CDCIterator{
		config:  h.config,
		records: records,
		sub:     sub,
	}

	return nil
}

func (h *Hybrid) createReplicationSlot(ctx context.Context, conn *pgx.Conn) (string, error) {
	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn.PgConn(),
		h.config.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true, // replication slot is dropped when we disconnect
			SnapshotAction: "USE_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		// If creating the replication slot fails with code 42710, this means
		// the replication slot already exists.
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42710" {
			return "", err
		}
	}
	return result.ConsistentPoint, nil
}

func (h *Hybrid) switchToCDC(ctx context.Context, conn *pgx.Conn) error {
	err := h.cdc.sub.CreatePublication(ctx, conn.PgConn())
	if err != nil {
		return err
	}

	if err := h.cdc.sub.StartReplication(ctx, conn.PgConn()); err != nil {
		return err
	}

	if err := h.cdc.sub.Listen(ctx, conn.PgConn()); err != nil {
		if !errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	return nil
}
