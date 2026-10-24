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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/matryer/is"
)

// What the idle advance and the beacons' r rely on (reportedPositions,
// Beacons.maybeRequest; #447 security review of 453669f): a keepalive's WAL
// end never passes the commit of a transaction the walsender has not
// finished sending, and reporting it as flushed makes Postgres send that
// transaction again.
//
// The walsender asks for a reply with a keepalive once wal_sender_timeout/2
// passed without one, also in the middle of sending a large transaction.
// Its WAL end is then sentPtr, the end of the last record decoding finished,
// which is the start of the commit record being replayed: the transaction's
// own commit LSN, never past it (observed on 13, 14, 15, 16, 17 and 18). A
// slot whose confirmed_flush_lsn is a transaction's commit LSN decodes that
// transaction again (Postgres skips only commits that start before it). So
// "every transaction committing before the keepalive's WAL end was sent"
// holds, and with every received record acked, reporting it loses nothing.
//
// The test drives that case with a raw client: one large transaction, no
// reply for longer than half of a short wal_sender_timeout, then every
// keepalive inside the transaction is checked, the WAL end of one of them is
// reported as flushed (what the connector does when everything it emitted
// is acked), and the connection dropped without anything else (a crash).
// The slot must decode the whole transaction again.
func TestWalsender_KeepaliveInsideATransactionNeverPassesItsCommit(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(test.Context(t), 3*time.Minute)
	defer cancel()
	pool := test.ConnectPool(ctx, t, test.RepmgrConnString)
	slot, table := beaconSlot(ctx, t, pool)
	const rows = 100000
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (key, column1) SELECT convert_to('t' || g, 'UTF8'), repeat('x', 200) FROM generate_series(1, %d) g`, table, rows))
	is.NoErr(err)

	cfg, err := pgconn.ParseConfig(test.RepmgrConnString)
	is.NoErr(err)
	cfg.RuntimeParams["replication"] = "database"
	cfg.RuntimeParams["wal_sender_timeout"] = "10000" // a reply is asked for after 5 seconds without one
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	is.NoErr(err)
	defer conn.Close(context.Background())
	is.NoErr(pglogrepl.StartReplication(ctx, conn, slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", slot)},
	}))

	// Read nothing for 7 seconds: the walsender fills the socket inside the
	// transaction and asks for a reply.
	time.Sleep(7 * time.Second)
	var (
		inTx      bool
		commit    pglogrepl.LSN
		changes   int
		inside    []pglogrepl.LSN // keepalive WAL ends received inside the transaction
		lastReply = time.Now()
	)
	for deadline := time.Now().Add(2 * time.Minute); len(inside) == 0 || inTx; {
		if time.Now().After(deadline) {
			t.Fatalf("no keepalive inside the transaction after %d of %d changes", changes, rows)
		}
		if time.Since(lastReply) > 6*time.Second {
			// A status update that confirms nothing: keeps the walsender
			// alive, and lets it ask again after 5 seconds.
			is.NoErr(pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: 1, WALFlushPosition: 1}))
			lastReply = time.Now()
		}
		rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := conn.ReceiveMessage(rctx)
		rcancel()
		is.NoErr(err)
		cd, ok := msg.(*pgproto3.CopyData)
		is.True(ok)
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			k, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			is.NoErr(err)
			if inTx {
				inside = append(inside, k.ServerWALEnd)
				if k.ServerWALEnd > commit {
					t.Fatalf("keepalive WAL end %s is past the commit %s of the transaction being sent (%d of %d changes received)",
						k.ServerWALEnd, commit, changes, rows)
				}
			}
		case pglogrepl.XLogDataByteID:
			x, err := pglogrepl.ParseXLogData(cd.Data[1:])
			is.NoErr(err)
			m, err := pglogrepl.Parse(x.WALData)
			is.NoErr(err)
			switch mm := m.(type) {
			case *pglogrepl.BeginMessage:
				inTx, commit, changes = true, mm.FinalLSN, 0
			case *pglogrepl.InsertMessage:
				changes++
			case *pglogrepl.CommitMessage:
				inTx = false
			}
		}
		if len(inside) > 0 && inTx {
			break // report it while the transaction is still being sent
		}
	}
	t.Logf("keepalives inside the transaction (commit %s) after %d of %d changes: %v", commit, changes, rows, inside)
	is.True(changes < rows) // still inside it

	// Report the keepalive's WAL end as flushed, then crash.
	flush := inside[len(inside)-1]
	is.NoErr(pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: flush, WALFlushPosition: flush}))
	// The walsender reads the reply when it next waits for the socket: keep
	// draining (and discarding) the stream until the slot took it.
	for deadline := time.Now().Add(60 * time.Second); confirmedFlush(ctx, t, pool, slot) != flush; {
		is.True(time.Now().Before(deadline))
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := conn.ReceiveMessage(rctx)
		rcancel()
		is.NoErr(err)
	}
	is.NoErr(conn.Close(ctx))
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		s, err := ReadReplicationSlot(ctx, pool, slot)
		is.NoErr(err)
		if !s.Active {
			is.Equal(s.ConfirmedFlushLSN, flush)
			break
		}
		is.True(time.Now().Before(deadline))
	}

	// The slot decodes the whole transaction again.
	var again int
	is.NoErr(pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE get_byte(data, 0) = 73)
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`, slot, slot).Scan(&again))
	t.Logf("after reporting %s and crashing, the slot decodes %d inserts again", flush, again)
	is.Equal(again, rows)
}
