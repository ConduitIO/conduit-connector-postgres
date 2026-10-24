// Copyright © 2024 Meroxa, Inc.
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

package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrMissingSlot = errors.New("replication slot missing")

type ReadReplicationSlotResult struct {
	Name              string
	ConfirmedFlushLSN pglogrepl.LSN
	RestartLSN        pglogrepl.LSN
	// Active is whether a connection is streaming from the slot right now.
	Active bool
}

// ReadReplicationSlot returns state of an existing replication slot.
func ReadReplicationSlot(ctx context.Context, conn *pgxpool.Pool, name string) (ReadReplicationSlotResult, error) {
	var r ReadReplicationSlotResult

	qr := conn.QueryRow(ctx, "SELECT slot_name, confirmed_flush_lsn, restart_lsn, active FROM pg_replication_slots WHERE slot_name=$1", name)
	if err := qr.Scan(&r.Name, &r.ConfirmedFlushLSN, &r.RestartLSN, &r.Active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReadReplicationSlotResult{}, fmt.Errorf("%s: %w", name, ErrMissingSlot)
		}
		return ReadReplicationSlotResult{}, fmt.Errorf("failed to read replication slot %q: %w", name, err)
	}

	return r, nil
}

// SlotHealth is what pg_replication_slots says about whether a slot can still
// be streamed from.
type SlotHealth struct {
	// WALStatus is wal_status (Postgres 13+), "" when the server has no such
	// column or it is NULL. "lost" means the WAL the slot needs is gone.
	WALStatus string
	// InvalidationReason is invalidation_reason (Postgres 17+), "" when unset
	// or not available on this server.
	InvalidationReason string
}

// Invalidated reports whether the slot can no longer be streamed from.
func (h SlotHealth) Invalidated() bool {
	return h.WALStatus == "lost" || h.InvalidationReason != ""
}

// ReadSlotHealth returns the health of an existing slot, or ErrMissingSlot.
// It reads neither restart_lsn nor confirmed_flush_lsn, which are NULL on an
// invalidated slot and would fail a scan into an LSN.
func ReadSlotHealth(ctx context.Context, conn *pgxpool.Pool, name string) (SlotHealth, error) {
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return SlotHealth{}, fmt.Errorf("failed to read the server version: %w", err)
	}
	// Name the columns only on servers that have them: wal_status came with
	// Postgres 13, invalidation_reason with 17.
	walStatus, reason := "NULL::text", "NULL::text"
	if version >= 130000 {
		walStatus = "wal_status"
	}
	if version >= 170000 {
		reason = "invalidation_reason"
	}
	var h SlotHealth
	var ws, ir *string
	err := conn.QueryRow(ctx,
		"SELECT "+walStatus+", "+reason+" FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&ws, &ir)
	if errors.Is(err, pgx.ErrNoRows) {
		return SlotHealth{}, fmt.Errorf("%s: %w", name, ErrMissingSlot)
	}
	if err != nil {
		return SlotHealth{}, fmt.Errorf("failed to read replication slot %q: %w", name, err)
	}
	if ws != nil {
		h.WALStatus = *ws
	}
	if ir != nil {
		h.InvalidationReason = *ir
	}
	return h, nil
}
