// Copyright © 2026 Meroxa, Inc.
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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WALHistory is which WAL history the server is on (#447 B3): the cluster's
// system identifier and its current timeline, from IDENTIFY_SYSTEM, and the
// current WAL position.
type WALHistory struct {
	SystemID string
	Timeline int32
	// CurrentLSN is pg_current_wal_lsn() on a primary, the last replayed
	// position on a standby (IDENTIFY_SYSTEM's xlogpos is the flush position
	// in both cases).
	CurrentLSN pglogrepl.LSN
}

// ReadWALHistory runs IDENTIFY_SYSTEM on a replication connection.
func ReadWALHistory(ctx context.Context, pool *pgxpool.Pool) (WALHistory, error) {
	conn, err := pool.Acquire(cpool.WithReplication(ctx))
	if err != nil {
		return WALHistory{}, fmt.Errorf("could not establish replication connection: %w", err)
	}
	defer conn.Release()
	r, err := pglogrepl.IdentifySystem(ctx, conn.Conn().PgConn())
	if err != nil {
		return WALHistory{}, fmt.Errorf("failed to identify the system: %w", err)
	}
	return WALHistory{SystemID: r.SystemID, Timeline: r.Timeline, CurrentLSN: r.XLogPos}, nil
}

// TimelineSwitchPoint returns where the server's history left timeline
// from, read from the history file of timeline current (TIMELINE_HISTORY):
// the LSN up to which from and current share their WAL. ok is false when
// from is not an ancestor of current.
func TimelineSwitchPoint(ctx context.Context, pool *pgxpool.Pool, current, from int32) (pglogrepl.LSN, bool, error) {
	conn, err := pool.Acquire(cpool.WithReplication(ctx))
	if err != nil {
		return 0, false, fmt.Errorf("could not establish replication connection: %w", err)
	}
	defer conn.Release()
	r, err := pglogrepl.TimelineHistory(ctx, conn.Conn().PgConn(), current)
	if err != nil {
		return 0, false, fmt.Errorf("failed to read the history of timeline %d: %w", current, err)
	}
	return ParseSwitchPoint(r.Content, from)
}

// ParseSwitchPoint finds timeline from in a timeline history file: lines of
// "parentTLI<TAB>switchpoint<TAB>reason", oldest first. The switch point is
// where the history left from for its child.
func ParseSwitchPoint(history []byte, from int32) (pglogrepl.LSN, bool, error) {
	sc := bufio.NewScanner(bytes.NewReader(history))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false, fmt.Errorf("malformed timeline history line %q", line)
		}
		tli, err := strconv.ParseInt(fields[0], 10, 32)
		if err != nil {
			return 0, false, fmt.Errorf("malformed timeline history line %q: %w", line, err)
		}
		if tli != int64(from) {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(fields[1])
		if err != nil {
			return 0, false, fmt.Errorf("malformed timeline history line %q: %w", line, err)
		}
		return lsn, true, nil
	}
	return 0, false, sc.Err()
}

// TxAt is the transaction a slot decodes, or the WAL holds, at a commit LSN
// (#447 re-review S2).
type TxAt struct {
	FinalLSN   pglogrepl.LSN
	Xid        uint32
	CommitTime time.Time
}

// TxCommittingAtErr is a test seam, nil in production (only assigned in
// _test.go files, see TestSeamsOnlyAssignedInTests). When set, TxCommittingAt
// returns its error as if the peek query failed transiently (a timeout, a
// connection drop).
var TxCommittingAtErr func() error

// TxCommittingAt peeks (never consumes) the slot for the transaction that
// commits at lsn (its BEGIN's final LSN). ok is false when the slot decodes
// no transaction committing there. The slot must not be in use, and the role
// needs REPLICATION, as for FindBeacon.
//
// The peek is bounded by LSN, not by a number of changes: upto_lsn lsn+1
// makes Postgres decode up to and including the record that starts at lsn
// and stop there. A change limit stops only at a transaction boundary, so a
// large transaction committing between the slot's confirmed_flush_lsn and
// lsn came back whole each round and the search gave up on a position it
// could check (#447 fresh-context review S1). Only the context bounds it.
func TxCommittingAt(ctx context.Context, pool *pgxpool.Pool, slot, publication string, lsn pglogrepl.LSN) (TxAt, bool, error) {
	if TxCommittingAtErr != nil {
		return TxAt{}, false, TxCommittingAtErr()
	}
	var begins [][]byte
	err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(data) FILTER (WHERE get_byte(data, 0) = 66), '{}')
		FROM pg_logical_slot_peek_binary_changes($1, $2::pg_lsn, NULL,
		     'proto_version', '1', 'publication_names', $3)`,
		slot, (lsn + 1).String(), publication).Scan(&begins)
	if err != nil {
		return TxAt{}, false, fmt.Errorf("failed to peek replication slot %q for the saved position's transaction: %w", slot, err)
	}
	for _, data := range begins {
		msg, err := pglogrepl.Parse(data)
		if err != nil {
			return TxAt{}, false, fmt.Errorf("failed to parse a BEGIN message: %w", err)
		}
		if b, ok := msg.(*pglogrepl.BeginMessage); ok && b.FinalLSN == lsn {
			return TxAt{FinalLSN: b.FinalLSN, Xid: b.Xid, CommitTime: b.CommitTime}, true, nil
		}
	}
	return TxAt{}, false, nil
}
