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

type ReplicationSlotResult struct {
	Name              string
	ConfirmedFlushLSN pglogrepl.LSN
	RestartLSN        pglogrepl.LSN
}

// ReadReplicationSlot returns state of an existing replication slot.
func ReadReplicationSlot(ctx context.Context, conn *pgxpool.Pool, name string) (ReplicationSlotResult, error) {
	var r ReplicationSlotResult

	qr := conn.QueryRow(ctx, "SELECT slot_name, confirmed_flush_lsn, restart_lsn FROM pg_replication_slots WHERE slot_name=$1", name)
	if err := qr.Scan(&r.Name, &r.ConfirmedFlushLSN, &r.RestartLSN); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReplicationSlotResult{}, fmt.Errorf("%s: %w", name, ErrMissingSlot)
		}
		return ReplicationSlotResult{}, fmt.Errorf("failed to read replication slot %q: %w", name, err)
	}

	return r, nil
}
