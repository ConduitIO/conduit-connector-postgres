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

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/conduitio/conduit-connector-postgres/source/position"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// Integration tests for the Debezium offset import (#341, slice 2) against a
// real Postgres. They do not run Debezium: a second pgoutput slot stands in
// for Debezium's, and the "offset" is computed from what that slot decodes,
// the way Debezium computes it (lsn_commit = the end of the last commit
// record, lsn = the change's WAL start). Real Debezium offsets are checked
// against the same assumptions by source/dbzoffset/testdata/capture.

// probeMsg is one pgoutput message seen on a peeked slot.
type probeMsg struct {
	typ      byte
	lsn      pglogrepl.LSN // the message's write location
	xid      uint32
	commit   pglogrepl.LSN // 'C': commit record start; 'B': final LSN
	end      pglogrepl.LSN // 'C': commit record end
	commitTs time.Time     // 'B': commit time
}

func peekSlot(ctx context.Context, t *testing.T, conn *pgx.Conn, slot, pub string) []probeMsg {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT lsn, xid::text::bigint, data
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`, slot, pub)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []probeMsg
	for rows.Next() {
		var lsn pglogrepl.LSN
		var xid int64
		var data []byte
		if err := rows.Scan(&lsn, &xid, &data); err != nil {
			t.Fatal(err)
		}
		m := probeMsg{typ: data[0], lsn: lsn, xid: uint32(xid)} //nolint:gosec // xids fit in 32 bits
		msg, err := pglogrepl.Parse(data)
		if err != nil {
			t.Fatal(err)
		}
		switch v := msg.(type) {
		case *pglogrepl.BeginMessage:
			m.commit, m.commitTs = v.FinalLSN, v.CommitTime
		case *pglogrepl.CommitMessage:
			m.commit, m.end = v.CommitLSN, v.TransactionEndLSN
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type importFixture struct {
	ctx       context.Context
	logs      *lockedBuffer
	conn      *pgx.Conn
	table     string
	slot, pub string // Conduit's
	dbzSlot   string // stands in for Debezium's
	c         pglogrepl.LSN
	sysID, db string
}

func newImportFixture(t *testing.T) *importFixture {
	t.Helper()
	is := is.New(t)
	logs := &lockedBuffer{}
	ctx := zerolog.New(logs).Level(zerolog.DebugLevel).WithContext(context.Background())
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	suffix := time.Now().UnixNano() % 100000000
	f := &importFixture{
		ctx: ctx, logs: logs, conn: conn,
		table:   fmt.Sprintf("imp_%d", suffix),
		slot:    fmt.Sprintf("imp_conduit_%d", suffix),
		pub:     fmt.Sprintf("imp_pub_%d", suffix),
		dbzSlot: fmt.Sprintf("imp_dbz_%d", suffix),
	}
	_, err := conn.Exec(ctx, "CREATE TABLE "+f.table+" (id bigint primary key, v text)")
	is.NoErr(err)
	// The CLI's order (B3): publication first, then Conduit's slot (C), and
	// Debezium's slot already exists. Here Debezium's slot is created right
	// after Conduit's, so its confirmed_flush_lsn is at or above C, as the
	// S13 proof requires.
	_, err = conn.Exec(ctx, "CREATE PUBLICATION "+f.pub+" FOR TABLE "+f.table)
	is.NoErr(err)
	is.NoErr(conn.QueryRow(ctx, "SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')", f.slot).Scan(&f.c))
	_, err = conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", f.dbzSlot)
	is.NoErr(err)

	var sysID string
	is.NoErr(conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sysID))
	f.sysID = sysID
	is.NoErr(conn.QueryRow(ctx, "SELECT current_database()").Scan(&f.db))

	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{f.slot, f.dbzSlot} {
			for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
				var exists bool
				_ = conn.QueryRow(bg, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", s).Scan(&exists)
				if !exists {
					break
				}
				if _, err := conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", s); err == nil {
					break
				}
			}
		}
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+f.pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+f.table)
	})
	return f
}

func (f *importFixture) exec(t *testing.T, sqls ...string) {
	t.Helper()
	for _, s := range sqls {
		if _, err := f.conn.Exec(f.ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// setting builds the logrepl.import.debeziumOffset value for offset.
func (f *importFixture) setting(t *testing.T, offset map[string]any, modify func(*dbzoffset.Setting)) string {
	t.Helper()
	raw, err := json.Marshal(offset)
	if err != nil {
		t.Fatal(err)
	}
	s := dbzoffset.Setting{
		Format: dbzoffset.FormatV1, Connector: "inventory-connector",
		Database: f.db, SystemIdentifier: f.sysID, Plugin: dbzoffset.PluginPgoutput,
		DebeziumVersion:  "3.2.4.Final",
		DebeziumSlotName: f.dbzSlot, PublicationName: f.pub,
		Tables: []string{"public." + f.table},
		Identity: dbzoffset.Identity{
			ConduitSlotConsistentPoint: f.c.String(),
			DebeziumSlotFlushAtWait:    f.c.String(),
			ConduitPublication:         f.publicationIdentity(t),
		},
		Offset: raw,
	}
	if modify != nil {
		modify(&s)
	}
	enc, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func (f *importFixture) open(t *testing.T, setting string, pos opencdc.Position) (sdk.Source, error) {
	t.Helper()
	src := Connector.NewSource()
	if err := sdk.Util.ParseConfig(f.ctx, map[string]string{
		"url": test.RepmgrConnString, "tables": f.table, "cdcMode": "logrepl",
		"snapshotMode": "never", "logrepl.slotName": f.slot, "logrepl.publicationName": f.pub,
		"logrepl.autoCleanup": "false", "logrepl.withAvroSchema": "false",
		"logrepl.import.debeziumOffset":  setting,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, src.Config(), Connector.NewSpecification().SourceParams); err != nil {
		t.Fatal(err)
	}
	if err := src.Config().Validate(f.ctx); err != nil {
		return nil, err
	}
	if err := src.Open(f.ctx, pos); err != nil {
		_ = src.Teardown(context.Background())
		return nil, err
	}
	return src, nil
}

func (f *importFixture) slotFlush(t *testing.T, slot string) pglogrepl.LSN {
	t.Helper()
	var l pglogrepl.LSN
	if err := f.conn.QueryRow(f.ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&l); err != nil {
		t.Fatal(err)
	}
	return l
}

// readIDs reads n records and returns their ids. Every record must be a CDC
// create on the fixture's table.
func readIDs(ctx context.Context, t *testing.T, src sdk.Source, n int) ([]string, []opencdc.Record) {
	t.Helper()
	var ids []string
	var recs []opencdc.Record
	readCtx, cancel := context.WithTimeout(ctx, truncateTestWait)
	defer cancel()
	for len(recs) < n {
		got, err := src.ReadN(readCtx, n-len(recs))
		if err != nil {
			t.Fatalf("read %d of %d records: %v", len(recs), n, err)
		}
		for _, r := range got {
			var after map[string]any
			if err := json.Unmarshal(r.Payload.After.Bytes(), &after); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, fmt.Sprint(after["id"]))
			recs = append(recs, r)
		}
	}
	return ids, recs
}

// noMoreRecords asserts nothing else arrives within a short wait.
func noMoreRecords(ctx context.Context, t *testing.T, src sdk.Source) {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	got, err := src.ReadN(readCtx, 10)
	if err == nil && len(got) > 0 {
		t.Fatalf("unexpected extra records: %d", len(got))
	}
}

// workload commits three transactions after C and returns what the stand-in
// Debezium slot saw: the commits in order, and every change (xid, lsn).
// Transaction A: ids 1, 2. B: ids 10..13, one row per statement. C: id 20.
func (f *importFixture) workload(t *testing.T) (commits []probeMsg, changes []probeMsg) {
	t.Helper()
	tb := f.table
	f.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (1,'a'),(2,'a')", tb))
	tx, err := f.conn.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{10, 11, 12, 13} {
		if _, err := tx.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s VALUES (%d,'b')", tb, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (20,'c')", tb))

	for _, m := range peekSlot(f.ctx, t, f.conn, f.dbzSlot, f.pub) {
		switch m.typ {
		case 'C':
			commits = append(commits, m)
		case 'I':
			changes = append(changes, m)
		}
	}
	if len(commits) != 3 || len(changes) != 7 {
		t.Fatalf("stand-in Debezium slot saw %d commits and %d changes, want 3 and 7", len(commits), len(changes))
	}
	return commits, changes
}

// The offset Debezium commits after delivering B's third row: lsn_commit is
// A's commit end, lsn is that row's LSN, txId is B's xid. Conduit must
// deliver B's rows from the third on, and C, and nothing of A.
func TestImport_ResumesMidTransaction(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, changes := f.workload(t)
	a, b := commits[0], commits[1]
	third := changes[4] // A has changes[0..1]; B has changes[2..5]
	is.Equal(third.xid, b.xid)

	var bBegin probeMsg
	for _, m := range peekSlot(f.ctx, t, f.conn, f.dbzSlot, f.pub) {
		if m.typ == 'B' && m.xid == b.xid {
			bBegin = m
		}
	}
	setting := f.setting(t, map[string]any{
		"lsn_proc": uint64(third.lsn), "lsn_commit": uint64(a.end), "lsn": uint64(third.lsn),
		"txId": b.xid, "ts_usec": bBegin.commitTs.UnixMicro(), "messageType": "INSERT",
	}, nil)

	src, err := f.open(t, setting, nil)
	is.NoErr(err)
	ids, recs := readIDs(f.ctx, t, src, 3)
	is.Equal(ids, []string{"12", "13", "20"})
	noMoreRecords(f.ctx, t, src)
	is.True(strings.Contains(f.logs.String(), "debezium offset import: resuming"))

	// D6: before the first ack the slot is held at or below Lc.
	is.True(f.slotFlush(t, f.slot) <= a.end)

	// The records carry their real change keys: 12 is B's third change.
	pos, err := position.ParseSDKPosition(recs[0].Position)
	is.NoErr(err)
	is.Equal(pos.TxCommitLSN, b.commit.String())
	is.Equal(pos.TxSeq, uint64(3))

	for _, r := range recs {
		is.NoErr(src.Ack(f.ctx, r.Position))
	}
	// After the first ack the cap is lifted: once everything is acked the
	// slot moves past Lc (the CLI's durable import-complete signal, S8).
	eventually(t, "the slot to pass lsn_commit after the acks", func() bool {
		return f.slotFlush(t, f.slot) > a.end
	})
	is.NoErr(src.Teardown(context.Background()))
	is.True(strings.Contains(f.logs.String(), "import complete at"))

	// Restart with the position: the setting is ignored, nothing again.
	src, err = f.open(t, setting, recs[len(recs)-1].Position)
	is.NoErr(err)
	noMoreRecords(f.ctx, t, src)
	is.True(strings.Contains(f.logs.String(), "import ignored: pipeline has a position"))
	is.NoErr(src.Teardown(context.Background()))
}

// A crash before the first ack (here: a teardown with nothing acked) leaves
// the position empty. The import is applied again and gives the same
// records: the import is idempotent and the D6 cap kept F <= Lc, so the
// slot check passes the second time.
func TestImport_RestartBeforeFirstAckReappliesTheSamePoint(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(commits[0].end), "lsn": uint64(commits[0].end),
		"txId": commits[0].xid, "messageType": "COMMIT",
	}, nil)

	for run := 0; run < 2; run++ {
		src, err := f.open(t, setting, nil)
		is.NoErr(err)
		ids, _ := readIDs(f.ctx, t, src, 5)
		is.Equal(ids, []string{"10", "11", "12", "13", "20"})
		is.NoErr(src.Teardown(context.Background()))
		is.True(f.slotFlush(t, f.slot) <= commits[0].end)
	}
}

// Each refusal has its code, and none of them moves Conduit's slot.
func TestImport_Refusals(t *testing.T) {
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	lc := commits[0].end
	offset := map[string]any{"lsn_commit": uint64(lc), "lsn": uint64(lc), "messageType": "COMMIT"}

	cases := []struct {
		name    string
		prepare func(t *testing.T) (setting string, undo func())
		want    dbzoffset.Code
	}{
		{"offset from the future", func(t *testing.T) (string, func()) {
			return f.setting(t, map[string]any{"lsn_commit": uint64(1) << 60, "messageType": "COMMIT"}, nil), nil
		}, dbzoffset.CodeOffsetFromFuture},
		{"wrong database", func(t *testing.T) (string, func()) {
			return f.setting(t, offset, func(s *dbzoffset.Setting) { s.Database = "inventory" }), nil
		}, dbzoffset.CodeDatabaseMismatch},
		{"wrong system identifier", func(t *testing.T) (string, func()) {
			return f.setting(t, offset, func(s *dbzoffset.Setting) { s.SystemIdentifier = "1" }), nil
		}, dbzoffset.CodeDatabaseMismatch},
		{"debezium slot missing", func(t *testing.T) (string, func()) {
			return f.setting(t, offset, func(s *dbzoffset.Setting) { s.DebeziumSlotName = "no_such_slot" }), nil
		}, dbzoffset.CodeDatabaseMismatch},
		{"debezium slot never reached C", func(t *testing.T) (string, func()) {
			return f.setting(t, offset, func(s *dbzoffset.Setting) {
				s.Identity.ConduitSlotConsistentPoint = lc.String()
				s.Identity.DebeziumSlotFlushAtWait = lc.String()
			}), nil
		}, dbzoffset.CodeDatabaseMismatch},
		{"table not captured", func(t *testing.T) (string, func()) {
			return f.setting(t, offset, func(s *dbzoffset.Setting) { s.Tables = []string{"public.other"} }), nil
		}, dbzoffset.CodeTablesNotCaptured},
		{"publication does not cover the table", func(t *testing.T) (string, func()) {
			f.exec(t, "ALTER PUBLICATION "+f.pub+" DROP TABLE "+f.table)
			return f.setting(t, offset, nil), func() { f.exec(t, "ALTER PUBLICATION "+f.pub+" ADD TABLE "+f.table) }
		}, dbzoffset.CodePublicationMismatch},
		{"lsn_commit equal to C is allowed", func(t *testing.T) (string, func()) {
			// Lc == C == F: the boundary, allowed (F <= Lc).
			return f.setting(t, map[string]any{"lsn_commit": uint64(f.c), "lsn": uint64(f.c), "messageType": "COMMIT"}, func(s *dbzoffset.Setting) {
				s.Identity.ConduitSlotConsistentPoint = f.c.String()
			}), func() {}
		}, ""},
	}
	before := f.slotFlush(t, f.slot)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			setting, undo := tc.prepare(t)
			if undo != nil {
				defer undo()
			}
			src, err := f.open(t, setting, nil)
			if tc.want == "" {
				// Lc == C == F: allowed (F <= Lc), the boundary case.
				is.NoErr(err)
				is.NoErr(src.Teardown(context.Background()))
				return
			}
			is.True(err != nil)
			is.Equal(dbzoffset.CodeOf(err), tc.want)
			is.Equal(f.slotFlush(t, f.slot), before)
		})
	}
}

// slot_ahead: something consumed Conduit's slot past Debezium's offset.
func TestImport_SlotAheadIsRefused(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(commits[0].end), "lsn": uint64(commits[0].end), "messageType": "COMMIT",
	}, nil)
	f.exec(t, fmt.Sprintf("SELECT pg_replication_slot_advance('%s', '%s')", f.slot, commits[1].end))
	_, err := f.open(t, setting, nil)
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodeSlotAhead)
}

// Import mode never creates a slot.
func TestImport_SlotMissingIsRefusedAndNotCreated(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(commits[0].end), "lsn": uint64(commits[0].end), "messageType": "COMMIT",
	}, nil)
	f.exec(t, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", f.slot))
	_, err := f.open(t, setting, nil)
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodeSlotMissing)
	var exists bool
	is.NoErr(f.conn.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", f.slot).Scan(&exists))
	is.True(!exists)
}

// D6: with nothing to deliver after Lc, the flush gate is open and a
// keepalive would move the slot to the server's WAL end. Until the first ack
// the report is capped at Lc, so a crash before it can re-apply the import.
func TestImport_FlushCapHoldsSlotUntilFirstAck(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	last := commits[len(commits)-1]
	// Debezium delivered everything: the offset is the last commit.
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(last.end), "lsn": uint64(last.end), "txId": last.xid, "messageType": "COMMIT",
	}, nil)
	src, err := f.open(t, setting, nil)
	is.NoErr(err)

	// WAL the pipeline does not capture, so the server's WAL end moves on.
	other := f.table + "_other"
	f.exec(t, "CREATE TABLE "+other+" (id int)")
	defer f.exec(t, "DROP TABLE "+other)
	for i := 0; i < 20; i++ {
		f.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (%d)", other, i))
	}
	var walEnd pglogrepl.LSN
	is.NoErr(f.conn.QueryRow(f.ctx, "SELECT pg_current_wal_lsn()").Scan(&walEnd))
	is.True(walEnd > last.end)

	// Wait past at least one status update (every 10 s).
	eventually(t, "the pending-import warning", func() bool {
		return strings.Count(f.logs.String(), "import pending first ack") >= 1
	})
	// Two more standby status updates after the WAL end moved past Lc.
	f.waitStatusReplies(t, 2)
	noMoreRecords(f.ctx, t, src)
	is.True(f.slotFlush(t, f.slot) <= last.end)
	// The final status update at shutdown is capped too.
	is.NoErr(src.Teardown(context.Background()))
	is.True(f.slotFlush(t, f.slot) <= last.end)
}

// T begins before P commits, so T's first change has an LSN below Lc (P's
// commit end). Debezium's offset after T's second change: lsn_commit is P's
// end, lsn is T's second change. Conduit must skip P whole and T's first
// change (below L, below Lc), and deliver T's second and third changes.
func TestImport_InterleavedTransactionPrefixBelowLc(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	tb := f.table
	tTx, err := f.conn.Begin(f.ctx)
	is.NoErr(err)
	_, err = tTx.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s VALUES (100, 't1')", tb))
	is.NoErr(err)
	other := test.ConnectSimple(f.ctx, t, test.RepmgrConnString)
	_, err = other.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'p')", tb)) // P commits first
	is.NoErr(err)
	_, err = tTx.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s VALUES (101, 't2')", tb))
	is.NoErr(err)
	_, err = tTx.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s VALUES (102, 't3')", tb))
	is.NoErr(err)
	is.NoErr(tTx.Commit(f.ctx))

	var commits, changes []probeMsg
	var tBegin probeMsg
	for _, m := range peekSlot(f.ctx, t, f.conn, f.dbzSlot, f.pub) {
		switch m.typ {
		case 'C':
			commits = append(commits, m)
		case 'I':
			changes = append(changes, m)
		case 'B':
			tBegin = m // the last BEGIN is T's
		}
	}
	is.Equal(len(commits), 2) // P, then T
	is.Equal(len(changes), 4) // P's row, then T's three rows
	p := commits[0]
	t1, t2 := changes[1], changes[2]
	is.True(t1.lsn < p.end) // the interleaving this test is about

	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(p.end), "lsn": uint64(t2.lsn), "txId": t2.xid,
		"ts_usec": tBegin.commitTs.UnixMicro(), "messageType": "INSERT",
	}, nil)
	src, err := f.open(t, setting, nil)
	is.NoErr(err)
	ids, _ := readIDs(f.ctx, t, src, 2)
	is.Equal(ids, []string{"101", "102"})
	noMoreRecords(f.ctx, t, src)
	is.NoErr(src.Teardown(context.Background()))
}

func (f *importFixture) publicationIdentity(t *testing.T) string {
	t.Helper()
	id, err := dbzoffset.PublicationIdentity(f.ctx, f.conn, f.pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitStatusReplies waits until the walsender on Conduit's slot has
// received n more standby status updates (pg_stat_replication.reply_time
// changes n times).
func (f *importFixture) waitStatusReplies(t *testing.T, n int) {
	t.Helper()
	replyTime := func() time.Time {
		var ts *time.Time
		err := f.conn.QueryRow(f.ctx, `SELECT r.reply_time FROM pg_stat_replication r
			JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1`, f.slot).Scan(&ts)
		if err != nil || ts == nil {
			return time.Time{}
		}
		return *ts
	}
	prev := replyTime()
	for i := 0; i < n; i++ {
		eventually(t, fmt.Sprintf("status reply %d of %d", i+1, n), func() bool {
			return replyTime().After(prev)
		})
		prev = replyTime()
	}
}

// B3: a table added to Conduit's publication after the slot was created
// loses its earlier WAL silently. The recorded publication identity no
// longer matches, so the import is refused.
func TestImport_PublicationChangedAfterSlotIsRefused(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	other := f.table + "_late"
	// Debezium captured the late table too, so only the identity can refuse.
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(commits[0].end), "lsn": uint64(commits[0].end), "messageType": "COMMIT",
	}, func(s *dbzoffset.Setting) { s.Tables = append(s.Tables, "public."+other) })
	f.exec(t, "CREATE TABLE "+other+" (id int primary key)", "ALTER PUBLICATION "+f.pub+" ADD TABLE "+other)
	defer f.exec(t, "DROP TABLE "+other)
	_, err := f.open(t, setting, nil)
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodePublicationMismatch)
	is.True(strings.Contains(err.Error(), "changed after Conduit's slot was created"))
}

// The publication streams a table Debezium did not capture.
func TestImport_PublicationTableNotCapturedIsRefused(t *testing.T) {
	is := is.New(t)
	f := newImportFixture(t)
	commits, _ := f.workload(t)
	other := f.table + "_extra"
	f.exec(t, "CREATE TABLE "+other+" (id int primary key)", "ALTER PUBLICATION "+f.pub+" ADD TABLE "+other)
	defer f.exec(t, "DROP TABLE "+other)
	// The identity is recorded after the change, so only the subset check
	// can refuse.
	setting := f.setting(t, map[string]any{
		"lsn_commit": uint64(commits[0].end), "lsn": uint64(commits[0].end), "messageType": "COMMIT",
	}, nil)
	_, err := f.open(t, setting, nil)
	is.Equal(dbzoffset.CodeOf(err), dbzoffset.CodeTablesNotCaptured)
	is.True(strings.Contains(err.Error(), other))
}
