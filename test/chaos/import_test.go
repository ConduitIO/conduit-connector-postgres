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

//go:build conduitchaos

// SIGKILL tests for the Debezium offset import (#341, Tests: "Kill tests").
// A second pgoutput slot stands in for Debezium's; the offset is computed
// from what it decodes. Each test kills the child at one point and checks,
// on recovery: no record lost (invariant 3), no duplicate beyond the
// documented set, the slot never past lsn_commit before the first ack (D6,
// invariant 1), and a restart that either re-applies the same import (empty
// position) or resumes from the version 2 position (invariant 2).

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

type importScenario struct {
	replPool, regPool *pgxpool.Pool
	table, slot, pub  string
	dbzSlot           string
	ledgerPath        string
	c                 pglogrepl.LSN
	ids               []int64 // A: ids[0:2], B: ids[2:6], C: ids[6:7]
	commitEnds        []pglogrepl.LSN
	bXid              uint32
	bThird            pglogrepl.LSN
	bCommitTime       time.Time
	setting           string
}

// importSetup creates the publication, Conduit's slot (C) and the stand-in
// Debezium slot, then commits A (2 rows), B (4 rows, one per statement) and
// C (1 row) after C.
func importSetup(t *testing.T) *importScenario {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	replPool, regPool, table, slot, pub, ledgerPath := b1Setup(t)
	dbzSlot, err := randChaosName()
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = replPool.Exec(context.Background(), "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name=$1", dbzSlot)
	})
	s := &importScenario{replPool: replPool, regPool: regPool, table: table, slot: slot, pub: pub, dbzSlot: dbzSlot, ledgerPath: ledgerPath}

	_, err = replPool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %q FOR TABLE %q", pub, table))
	is.NoErr(err)
	is.NoErr(replPool.QueryRow(ctx, "SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')", slot).Scan(&s.c))
	_, err = replPool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", dbzSlot)
	is.NoErr(err)

	one := func(label string) int64 {
		var id int64
		is.NoErr(regPool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ($1) RETURNING id`, table), label).Scan(&id))
		return id
	}
	// A: one transaction, two rows.
	rows, err := regPool.Query(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('a'), ('a') RETURNING id`, table))
	is.NoErr(err)
	for rows.Next() {
		var id int64
		is.NoErr(rows.Scan(&id))
		s.ids = append(s.ids, id)
	}
	rows.Close()
	// B: one transaction, four statements.
	tx, err := regPool.Begin(ctx)
	is.NoErr(err)
	for i := 0; i < 4; i++ {
		var id int64
		is.NoErr(tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('b') RETURNING id`, table)).Scan(&id))
		s.ids = append(s.ids, id)
	}
	is.NoErr(tx.Commit(ctx))
	s.ids = append(s.ids, one("c"))

	// What the stand-in Debezium slot decoded.
	prows, err := replPool.Query(ctx, `SELECT lsn, xid::text::bigint, data
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`, dbzSlot, pub)
	is.NoErr(err)
	var changes []pglogrepl.LSN
	var changeXids []uint32
	for prows.Next() {
		var lsn pglogrepl.LSN
		var xid int64
		var data []byte
		is.NoErr(prows.Scan(&lsn, &xid, &data))
		msg, err := pglogrepl.Parse(data)
		is.NoErr(err)
		switch m := msg.(type) {
		case *pglogrepl.BeginMessage:
			if len(s.commitEnds) == 1 {
				s.bXid, s.bCommitTime = m.Xid, m.CommitTime
			}
		case *pglogrepl.CommitMessage:
			s.commitEnds = append(s.commitEnds, m.TransactionEndLSN)
		case *pglogrepl.InsertMessage:
			changes = append(changes, lsn)
			changeXids = append(changeXids, uint32(xid)) //nolint:gosec // xids fit in 32 bits
		}
	}
	prows.Close()
	is.Equal(len(s.commitEnds), 3)
	is.Equal(len(changes), 7)
	s.bThird = changes[4]
	is.Equal(changeXids[4], s.bXid)
	return s
}

func (s *importScenario) withOffset(t *testing.T, offset map[string]any) {
	t.Helper()
	ctx := context.Background()
	raw, err := json.Marshal(offset)
	if err != nil {
		t.Fatal(err)
	}
	var db, sysID string
	if err := s.replPool.QueryRow(ctx, "SELECT current_database(), system_identifier::text FROM pg_control_system()").Scan(&db, &sysID); err != nil {
		t.Fatal(err)
	}
	pubID, err := dbzoffset.PublicationIdentity(ctx, s.replPool, s.pub)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := dbzoffset.Setting{
		Format: dbzoffset.FormatV1, Connector: "chaos", Database: db, SystemIdentifier: sysID,
		Plugin: dbzoffset.PluginPgoutput, DebeziumVersion: "3.2.4.Final", DebeziumSlotName: s.dbzSlot, PublicationName: s.pub,
		Tables: []string{"public." + s.table},
		Identity: dbzoffset.Identity{ConduitSlotConsistentPoint: s.c.String(), DebeziumSlotFlushAtWait: s.c.String(),
			ConduitPublication: pubID},
		Offset: raw,
	}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.setting = enc
}

// midB is Debezium's offset after delivering B's third row.
func (s *importScenario) midB() map[string]any {
	return map[string]any{
		"lsn_proc": uint64(s.bThird), "lsn_commit": uint64(s.commitEnds[0]), "lsn": uint64(s.bThird),
		"txId": s.bXid, "ts_usec": s.bCommitTime.UnixMicro(), "messageType": "INSERT",
	}
}

func (s *importScenario) spawn(t *testing.T, run, total int, park string) *childProcess {
	t.Helper()
	env := []string{
		envRealChild + "=" + envValueTrue,
		envURL + "=" + RepmgrConnString,
		envTable + "=" + s.table,
		envSlot + "=" + s.slot,
		envPub + "=" + s.pub,
		envLedger + "=" + s.ledgerPath,
		envTotal + "=" + strconv.Itoa(total),
		envBatchSize + "=3",
		envRun + "=" + strconv.Itoa(run),
		envImport + "=" + s.setting,
	}
	if park != "" {
		env = append(env, "PGCHAOS_PARK="+park)
	}
	return spawnChildWithEnv(t, env)
}

func (s *importScenario) slotFlush(t *testing.T) pglogrepl.LSN {
	t.Helper()
	state, err := ReadSlotState(context.Background(), s.replPool, s.slot)
	if err != nil {
		t.Fatal(err)
	}
	l, err := pglogrepl.ParseLSN(state.ConfirmedFlushLSN)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func idKey(id int64) string { return string(opencdc.StructuredData{"id": id}.Bytes()) }

// ledgerKeys returns the record keys in the ledger, in order.
func ledgerKeys(t *testing.T, path string) []string {
	t.Helper()
	var keys []string
	for _, e := range b1ReadLedger(t, path) {
		keys = append(keys, e.Key)
	}
	return keys
}

func keysOf(ids ...int64) []string {
	var out []string
	for _, id := range ids {
		out = append(out, idKey(id))
	}
	return out
}

// Kill after the first change is delivered but before it is durable or
// acked. The position is still empty, so the restart applies the import
// again: same resume point, the slot still at or below Lc (D6), and the
// whole expected set delivered once.
func TestImport_KillBeforeFirstAck(t *testing.T) {
	is := is.New(t)
	s := importSetup(t)
	s.withOffset(t, s.midB())
	lc := s.commitEnds[0]

	cp := s.spawn(t, 1, 3, fmt.Sprintf("%s:%d", chaospoint.RecordSeen, 1))
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForMarker(t, "PARKED", 60*time.Second)
	is.True(s.slotFlush(t) <= lc)
	cp.sigkill(t)
	is.True(s.slotFlush(t) <= lc)
	is.Equal(len(b1ReadLedger(t, s.ledgerPath)), 0)

	cp2 := s.spawn(t, 2, 3, "")
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	// B's rows from the third on, then C. Nothing of A, nothing of B's
	// first two rows (Debezium delivered them).
	is.Equal(ledgerKeys(t, s.ledgerPath), keysOf(s.ids[4], s.ids[5], s.ids[6]))
}

// Kill after the first ack. The restart resumes from the version 2
// position, not from the import (the setting is ignored once a position
// exists), and delivers the rest exactly once.
func TestImport_KillAfterFirstAck(t *testing.T) {
	is := is.New(t)
	s := importSetup(t)
	s.withOffset(t, s.midB())

	cp := s.spawn(t, 1, 3, fmt.Sprintf("%s:%d", chaospoint.RecordSeen, 2))
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForCount(t, "ACKED ", 1, 60*time.Second)
	cp.waitForMarker(t, "PARKED", 60*time.Second)
	cp.sigkill(t)
	is.Equal(ledgerKeys(t, s.ledgerPath), keysOf(s.ids[4]))

	cp2 := s.spawn(t, 2, 2, "")
	cp2.waitForMarker(t, "RESUME ", 30*time.Second)
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	is.Equal(ledgerKeys(t, s.ledgerPath), keysOf(s.ids[4], s.ids[5], s.ids[6]))
}

// Kill at a standby status update while the D6 cap holds: Debezium
// delivered everything, so nothing is in flight, the gate is open and,
// without the cap, the update would carry the server's WAL end past Lc.
// After the kill the slot is still at or below Lc and the import applies
// again (no slot_ahead), then delivers a row written later.
func TestImport_KillAtStatusUpdateUnderTheCap(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	s := importSetup(t)
	last := s.commitEnds[2]
	s.withOffset(t, map[string]any{"lsn_commit": uint64(last), "lsn": uint64(last), "messageType": "COMMIT"})

	unrelated, err := randChaosName()
	is.NoErr(err)
	_, err = s.regPool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %q (id int)`, unrelated))
	is.NoErr(err)
	_, err = s.regPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q SELECT generate_series(1, 500)`, unrelated))
	is.NoErr(err)

	// The second status update: the first may precede the walsender's
	// first keepalive.
	cp := s.spawn(t, 1, 1, fmt.Sprintf("%s:%d", chaospoint.StandbyStatusUpdate, 2))
	cp.waitForMarker(t, "OPENED", 30*time.Second)
	cp.waitForMarker(t, "PARKED", 90*time.Second)
	is.True(s.slotFlush(t) <= last)
	cp.sigkill(t)
	is.True(s.slotFlush(t) <= last)

	var id int64
	is.NoErr(s.regPool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %q (column1) VALUES ('after') RETURNING id`, s.table)).Scan(&id))
	cp2 := s.spawn(t, 2, 1, "")
	cp2.waitForMarker(t, "DONE", 60*time.Second)
	cp2.waitExit(t, 30*time.Second)
	is.Equal(ledgerKeys(t, s.ledgerPath), keysOf(id))
}
