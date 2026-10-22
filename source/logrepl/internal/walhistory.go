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

// FirstTxFrom peeks (never consumes) the slot for the first transaction it
// decodes whose commit LSN (BEGIN's final LSN) is at or after from. ok is
// false when the slot decodes none up to the end of the WAL. Same limits and
// requirements as FindBeacon.
func FirstTxFrom(ctx context.Context, pool *pgxpool.Pool, slot, publication string, from pglogrepl.LSN) (TxAt, bool, error) {
	maxRows := findBeaconMaxRows
	for limit := min(findBeaconFirstRound, maxRows); ; limit = nextFindBeaconLimit(limit, maxRows) {
		var rows int64
		var begins [][]byte
		err := pool.QueryRow(ctx, `
			SELECT count(*), coalesce(array_agg(data ORDER BY n) FILTER (WHERE get_byte(data, 0) = 66), '{}')
			FROM pg_logical_slot_peek_binary_changes($1, NULL, $2,
			     'proto_version', '1', 'publication_names', $3) WITH ORDINALITY AS t(lsn, xid, data, n)`,
			slot, limit, publication).Scan(&rows, &begins)
		if err != nil {
			return TxAt{}, false, fmt.Errorf("failed to peek replication slot %q for the saved position's transaction: %w", slot, err)
		}
		for _, data := range begins {
			msg, err := pglogrepl.Parse(data)
			if err != nil {
				return TxAt{}, false, fmt.Errorf("failed to parse a BEGIN message: %w", err)
			}
			b, ok := msg.(*pglogrepl.BeginMessage)
			if !ok || b.FinalLSN < from {
				continue
			}
			return TxAt{FinalLSN: b.FinalLSN, Xid: b.Xid, CommitTime: b.CommitTime}, true, nil
		}
		if rows < int64(limit) {
			return TxAt{}, false, nil
		}
		if limit >= maxRows {
			return TxAt{}, false, fmt.Errorf("replication slot %q: %w (read %d changes)", slot, ErrBeaconSearchExhausted, rows)
		}
	}
}
