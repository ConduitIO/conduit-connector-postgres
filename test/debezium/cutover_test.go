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

//go:build debezium

package debezium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	postgres "github.com/conduitio/conduit-connector-postgres"
	"github.com/conduitio/conduit-connector-postgres/source/dbzoffset"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

// cutKind is where in Debezium's stream the cutover stops it.
type cutKind string

const (
	// cutLargeTx stops Debezium while it is emitting the large transaction:
	// the offset is mid-transaction (cut point 2 of the design).
	cutLargeTx cutKind = "large_tx"
	// cutCopy stops Debezium while it is emitting the COPY's rows, which
	// share change LSNs (cut point 4).
	cutCopy cutKind = "copy"
	// cutQuiescent stops Debezium once it has caught up and the workload is
	// paused (cut point 1). The connector runs with a heartbeat, so the last
	// offset is the heartbeat's: a COMMIT.
	cutQuiescent cutKind = "quiescent"
	// cutIdle is cutQuiescent without a heartbeat. Debezium writes no offset
	// for a COMMIT message, so the offset still names the last change of the
	// last transaction, which Debezium fully delivered: a DML offset whose
	// one duplicate is that change.
	cutIdle cutKind = "idle"
)

const (
	seedRows        = 500
	bigRowsDefault  = 40000
	bigUpdates      = 2000
	copyRows        = 60000
	smallBeforeBig  = 150
	smallAfterBig   = 100
	smallTail       = 100
	drainQuiet      = 6 * time.Second
	drainLimit      = 5 * time.Minute
	connectorWait   = 3 * time.Minute
	cutWindow       = 2 * time.Minute
	slotInactiveMax = 5 * time.Minute
)

// TestDebezium_FreshSlotCutover_MidTransaction cuts while Debezium is
// emitting a large transaction, three times. The expected plan case is DML,
// and the duplicates are the offset transaction's changes at the offset's
// LSN.
func TestDebezium_FreshSlotCutover_MidTransaction(t *testing.T) {
	st := requireStack(t)
	for i := 1; i <= 3; i++ {
		t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) {
			runCutover(t, st, cutLargeTx, dbzoffset.CaseDML)
		})
	}
}

// TestDebezium_FreshSlotCutover_DuringCopy cuts while Debezium is emitting
// the COPY, whose rows share change LSNs.
func TestDebezium_FreshSlotCutover_DuringCopy(t *testing.T) {
	st := requireStack(t)
	runCutover(t, st, cutCopy, dbzoffset.CaseDML)
}

// TestDebezium_FreshSlotCutover_Quiescence cuts after Debezium has caught up
// with a paused workload, with a heartbeat: a COMMIT offset, which predicts 0
// duplicates.
func TestDebezium_FreshSlotCutover_Quiescence(t *testing.T) {
	st := requireStack(t)
	runCutover(t, st, cutQuiescent, dbzoffset.CaseCommit)
}

// TestDebezium_FreshSlotCutover_QuiescenceNoHeartbeat is the same cut without
// a heartbeat. The offset is then the last change of the last transaction
// (DML), and the predicted duplicate is that one change.
func TestDebezium_FreshSlotCutover_QuiescenceNoHeartbeat(t *testing.T) {
	st := requireStack(t)
	runCutover(t, st, cutIdle, dbzoffset.CaseDML)
}

// slotInactiveWait is how long to wait for Debezium's slot to go inactive
// after the stop. DBZ_SLOT_INACTIVE_WAIT (a Go duration) overrides it.
func slotInactiveWait() time.Duration {
	if v := os.Getenv("DBZ_SLOT_INACTIVE_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return slotInactiveMax
}

// cutover is the state of one run.
type cutover struct {
	t   *testing.T
	st  *stack
	ctx context.Context

	conn *pgx.Conn // control connection

	cut                                cutKind
	id                                 string
	tables                             [2]string
	connector, prefix                  string
	dbzSlot, dbzPub                    string
	conduitSlot, conduitPub, probeSlot string
	led                                *ledger
	w                                  *workload
	logs                               *lockedBuffer
	c, flushAtWait                     pglogrepl.LSN
	sysID                              string
	cutXid                             uint32
	hitWindow                          bool
	offsetRaw, partitionRaw            json.RawMessage
	off                                dbzoffset.Offset
	plan                               dbzoffset.Plan
	flushAtStop                        pglogrepl.LSN
	bigRows                            int
	setting                            string
	conduitPubID, dbzVersion           string
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func runCutover(t *testing.T, st *stack, cut cutKind, wantCase dbzoffset.Case) {
	t.Helper()
	ctx, cancel := context.WithCancel(zerolog.New(os.Stderr).Level(zerolog.WarnLevel).WithContext(context.Background()))
	t.Cleanup(cancel)

	co := &cutover{t: t, st: st, ctx: ctx, cut: cut, led: newLedger(), logs: &lockedBuffer{}, bigRows: bigRowsDefault}
	if v := os.Getenv("DBZ_BIG_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("DBZ_BIG_ROWS %q is not a positive integer", v)
		}
		co.bigRows = n
	}
	co.ctx = zerolog.New(co.logs).Level(zerolog.DebugLevel).WithContext(ctx)
	co.conn = st.connect(ctx, t)
	t.Cleanup(func() { _ = co.conn.Close(context.Background()) })

	co.setup(cut)
	co.startDebezium()
	co.prepareCutover()
	co.startWorkload()
	co.stopDebezium(cut)
	co.readFinalOffset()
	co.waitWorkload()
	res := co.importAndDiff()
	res.log(t)

	if (cut == cutLargeTx || cut == cutCopy) && !co.hitWindow {
		t.Errorf("cut %s did not stop Debezium inside its transaction (cut xid %d, offset %s): a timing miss of "+
			"the harness, not a finding about the import", cut, co.cutXid, co.offsetRaw)
	}
	if co.plan.Case != wantCase {
		t.Errorf("cut %s landed on case %s, want %s (offset %s): the stop missed its window "+
			"(hit_window=%v); this is a timing miss of the harness, not a finding about the import",
			cut, co.plan.Case, wantCase, co.offsetRaw, co.hitWindow)
	}
}

func (co *cutover) fatalf(format string, args ...any) {
	co.t.Helper()
	co.t.Fatalf(format, args...)
}

func (co *cutover) exec(sql string, args ...any) {
	co.t.Helper()
	if _, err := co.conn.Exec(co.ctx, sql, args...); err != nil {
		co.fatalf("%s: %v", sql, err)
	}
}

// setup creates the tables (primary key, version column, full replica
// identity so deletes carry the version), seeds them, and creates Debezium's
// publication.
func (co *cutover) setup(cut cutKind) {
	co.t.Helper()
	co.id = strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10)
	co.tables = [2]string{"e2e_" + co.id + "_a", "e2e_" + co.id + "_b"}
	co.connector, co.prefix = "dbz"+co.id, "dbz"+co.id
	co.dbzSlot, co.dbzPub = "dbz_slot_"+co.id, "dbz_pub_"+co.id
	co.conduitSlot, co.conduitPub, co.probeSlot = "cdt_slot_"+co.id, "cdt_pub_"+co.id, "probe_slot_"+co.id

	for _, tb := range co.tables {
		co.exec(fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text)", tb))
		co.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tb))
		co.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 0, 'seed' FROM generate_series(1, %d) g", tb, seedRows))
	}
	co.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s", co.dbzPub, co.tables[0], co.tables[1]))
	co.sysID = co.queryString("SELECT system_identifier::text FROM pg_control_system()")

	co.w = newWorkload(co.tables, seedRows, workloadConfig{
		bigRows: co.bigRows, bigUpdates: bigUpdates, copyRows: copyRows,
		smallBefore: smallBeforeBig, smallAfter: smallAfterBig, smallTail: smallTail,
		quiesce: cut != cutLargeTx,
	}, co.led, time.Now().UnixNano())

	co.t.Cleanup(co.cleanup)
	co.t.Cleanup(func() {
		if co.t.Failed() {
			co.diagnose()
		}
	})
}

// diagnose logs what a stuck run needs: the connector's status, the slot, and
// the tail of Connect's log. It runs only after a failure.
func (co *cutover) diagnose() {
	bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if b, err := co.st.rest(bg, http.MethodGet, "/connectors/"+co.connector+"/status", nil); err == nil {
		co.t.Logf("DIAG connector status: %s", b)
	}
	conn, err := pgx.Connect(bg, co.st.pgURL())
	if err == nil {
		defer conn.Close(context.Background())
		var pid *int32
		var active bool
		if conn.QueryRow(bg, "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1", co.dbzSlot).Scan(&active, &pid) == nil {
			co.t.Logf("DIAG Debezium slot active=%v active_pid=%v", active, pid)
		}
	}
	out, _ := co.st.composeLogs(bg, "connect", 400)
	var keep []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "ERROR") || strings.Contains(l, "WARN") || strings.Contains(l, "top") || strings.Contains(l, "Stopp") || strings.Contains(l, "stopp") {
			keep = append(keep, l)
		}
	}
	if len(keep) > 40 {
		keep = keep[len(keep)-40:]
	}
	co.t.Logf("DIAG connect log (filtered):\n%s", strings.Join(keep, "\n"))
}

func (co *cutover) queryString(sql string, args ...any) string {
	co.t.Helper()
	var s string
	if err := co.conn.QueryRow(co.ctx, sql, args...).Scan(&s); err != nil {
		co.fatalf("%s: %v", sql, err)
	}
	return s
}

func (co *cutover) connectorConfig() map[string]string {
	cfg := map[string]string{
		"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname":              "pg",
		"database.port":                  "5432",
		"database.user":                  dbUser,
		"database.password":              dbPass,
		"database.dbname":                dbName,
		"topic.prefix":                   co.prefix,
		"plugin.name":                    co.st.plugin,
		"slot.name":                      co.dbzSlot,
		"publication.name":               co.dbzPub,
		"publication.autocreate.mode":    "disabled",
		"table.include.list":             "public." + co.tables[0] + ",public." + co.tables[1],
		"snapshot.mode":                  "initial",
		"skipped.operations":             "none",
		"tombstones.on.delete":           "false",
		"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
	}
	if co.cut == cutQuiescent {
		cfg["heartbeat.interval.ms"] = "300"
	}
	return cfg
}

// startDebezium starts the connector and waits for the snapshot to finish and
// for streaming to commit its first offset.
func (co *cutover) startDebezium() {
	co.t.Helper()
	if _, err := co.st.rest(co.ctx, http.MethodPut, "/connectors/"+co.connector+"/config", co.connectorConfig()); err != nil {
		co.fatalf("create connector: %v", err)
	}
	// Wait for the snapshot to finish before writing anything: a row committed
	// while it runs may be read by the snapshot (op r) and never streamed (op
	// c), and the ledger would call that loss.
	waitUntil(co.t, connectorWait, 500*time.Millisecond, "Debezium's snapshot to finish", func() (bool, error) {
		o, ok, err := co.st.offset(co.ctx, co.connector)
		if err != nil || !ok {
			return false, fmt.Errorf("no offset yet (err %v)", err)
		}
		var m map[string]any
		if err := json.Unmarshal(o.Offset, &m); err != nil {
			return false, err
		}
		_, inSnapshot := m["snapshot"]
		return !inSnapshot || m["last_snapshot_record"] == true, nil
	})
	// Then a write commits the first streaming offset.
	waitUntil(co.t, connectorWait, 500*time.Millisecond, "Debezium to stream a committed offset", func() (bool, error) {
		if err := co.w.smallTx(co.ctx, co.conn); err != nil {
			co.fatalf("%v", err)
		}
		_, _, err := co.streamingOffset()
		return err == nil, err
	})
}

// streamingOffset returns the connector's committed offset parsed, or an
// error while it has none, is a snapshot offset or has no lsn_commit.
func (co *cutover) streamingOffset() (dbzoffset.Offset, connectorOffset, error) {
	o, ok, err := co.st.offset(co.ctx, co.connector)
	if err != nil {
		return dbzoffset.Offset{}, o, err
	}
	if !ok {
		return dbzoffset.Offset{}, o, fmt.Errorf("no committed offset yet")
	}
	off, err := dbzoffset.ParseOffset(o.Offset)
	return off, o, err
}

// prepareCutover is D3 steps 1 and 2: Conduit's publication, then its slot
// (C), then traffic until Debezium's committed Lc and its slot reach C (S13).
// The probe slot is created first, from Debezium's publication, so it holds
// every transaction Conduit's slot will.
func (co *cutover) prepareCutover() {
	co.t.Helper()
	co.exec("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", co.probeSlot)
	co.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s", co.conduitPub, co.tables[0], co.tables[1]))
	if err := co.conn.QueryRow(co.ctx, "SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')", co.conduitSlot).Scan(&co.c); err != nil {
		co.fatalf("create Conduit's slot: %v", err)
	}
	// As the CLI does: record the publication's identity right after the
	// slot exists (identity.conduitPublication, B3).
	pubID, err := dbzoffset.PublicationIdentity(co.ctx, co.conn, co.conduitPub)
	if err != nil {
		co.fatalf("publication identity: %v", err)
	}
	co.conduitPubID = pubID
	co.dbzVersion = co.debeziumVersion()

	// Two separate transactions at least, then more until both conditions hold.
	for i := 0; i < 2; i++ {
		if err := co.w.smallTx(co.ctx, co.conn); err != nil {
			co.fatalf("%v", err)
		}
	}
	waitUntil(co.t, connectorWait, 300*time.Millisecond, "Debezium's Lc and slot flush to reach C", func() (bool, error) {
		if err := co.w.smallTx(co.ctx, co.conn); err != nil {
			co.fatalf("%v", err)
		}
		off, _, err := co.streamingOffset()
		if err != nil {
			return false, err
		}
		flush, _, exists, err := slotState(co.ctx, co.conn, co.dbzSlot)
		if err != nil || !exists {
			return false, fmt.Errorf("Debezium's slot: exists=%v err=%v", exists, err)
		}
		if off.LSNCommit < co.c || flush < co.c {
			return false, fmt.Errorf("Lc %s, slot flush %s, C %s", off.LSNCommit, flush, co.c)
		}
		co.flushAtWait = flush
		return true, nil
	})
}

func (co *cutover) startWorkload() {
	co.t.Helper()
	wconn, err := pgx.Connect(co.ctx, co.st.pgURL())
	if err != nil {
		co.fatalf("workload connection: %v", err)
	}
	co.t.Cleanup(func() { _ = wconn.Close(context.Background()) })
	co.w.start(co.ctx, wconn)
}

// recv waits for a value with a bound.
func recv[T any](co *cutover, ch <-chan T, what string) T {
	co.t.Helper()
	select {
	case v := <-ch:
		return v
	case <-co.w.done:
		if co.w.err != nil {
			co.fatalf("workload failed before %s: %v", what, co.w.err)
		}
		co.fatalf("workload finished before %s", what)
	case <-time.After(cutWindow):
		co.fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// stopDebezium stops the connector at the cut point with
// PUT /connectors/{name}/stop.
func (co *cutover) stopDebezium(cut cutKind) {
	co.t.Helper()
	switch cut {
	case cutLargeTx:
		co.cutInTransaction(recv(co, co.w.bigXid, "the large transaction's xid"))
	case cutCopy:
		// Debezium emits the large transaction much more slowly than the
		// workload writes it. Hold the COPY back until Debezium has caught up,
		// or the cut would land in the large transaction.
		recv(co, co.w.quiet, "the workload to pause")
		co.waitDebeziumIdle()
		close(co.w.resume)
		co.cutInTransaction(recv(co, co.w.copyXid, "the COPY's xid"))
	case cutQuiescent, cutIdle:
		recv(co, co.w.quiet, "the workload to pause")
		co.waitDebeziumIdle()
		co.stop()
	}
	waitUntil(co.t, slotInactiveWait(), 200*time.Millisecond, "Debezium's slot to go inactive", func() (bool, error) {
		flush, active, exists, err := slotState(co.ctx, co.conn, co.dbzSlot)
		if err != nil || !exists {
			return false, fmt.Errorf("slot exists=%v err=%v", exists, err)
		}
		co.flushAtStop = flush
		return !active, nil
	})
	if cut == cutQuiescent || cut == cutIdle {
		// The workload resumes only now: PUT /stop returns before the task has
		// stopped, and a write in that gap would not be a quiescent cut.
		close(co.w.resume)
	}
}

// cutInTransaction polls the committed offset every few milliseconds and
// stops the connector as soon as the offset belongs to transaction xid, which
// is when Debezium is part-way through emitting it. If the window is missed
// the connector is stopped at the deadline anyway, and the run reports the
// case it got.
func (co *cutover) cutInTransaction(xid uint32) {
	co.t.Helper()
	co.cutXid = xid
	deadline := time.Now().Add(cutWindow)
	for time.Now().Before(deadline) {
		if off, _, err := co.streamingOffset(); err == nil && off.HasTxID && off.TxID == xid && off.MessageType.IsDML() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	co.stop()
}

// waitDebeziumIdle waits until the committed offset's position has not moved
// for 3 s (with a heartbeat, until it is also a COMMIT).
func (co *cutover) waitDebeziumIdle() {
	co.t.Helper()
	var last string
	var since time.Time
	waitUntil(co.t, connectorWait, 250*time.Millisecond, "Debezium to catch up", func() (bool, error) {
		off, _, err := co.streamingOffset()
		if err != nil {
			return false, err
		}
		pos := fmt.Sprintf("%s/%s/%d/%s", off.LSNCommit, off.LSN, off.TxID, off.MessageType)
		if pos != last {
			last, since = pos, time.Now()
			return false, nil
		}
		if co.cut == cutQuiescent && off.MessageType != dbzoffset.MessageCommit {
			return false, fmt.Errorf("waiting for the heartbeat's COMMIT offset, have %s", off.MessageType)
		}
		return time.Since(since) >= 3*time.Second, nil
	})
}

func (co *cutover) stop() {
	co.t.Helper()
	if _, err := co.st.rest(co.ctx, http.MethodPut, "/connectors/"+co.connector+"/stop", nil); err != nil {
		co.fatalf("stop connector: %v", err)
	}
}

// readFinalOffset reads the offset after the slot went inactive (D3 step 4).
func (co *cutover) readFinalOffset() {
	co.t.Helper()
	o, ok, err := co.st.offset(co.ctx, co.connector)
	if err != nil || !ok {
		co.fatalf("final offset: ok=%v err=%v", ok, err)
	}
	co.offsetRaw, co.partitionRaw = o.Offset, o.Partition
	off, err := dbzoffset.ParseOffset(co.offsetRaw)
	if err != nil {
		co.fatalf("final offset %s: %v", co.offsetRaw, err)
	}
	co.off, co.plan = off, dbzoffset.NewPlan(off)
	// The window is hit when the offset the stop left behind is a change of
	// the transaction the cut aimed at.
	co.hitWindow = co.cutXid != 0 && off.HasTxID && off.TxID == co.cutXid && off.MessageType.IsDML()
	if co.off.LSNCommit < co.c {
		co.fatalf("final Lc %s is below C %s: the cutover precondition was lost (offset %s)", co.off.LSNCommit, co.c, co.offsetRaw)
	}
}

func (co *cutover) waitWorkload() {
	co.t.Helper()
	select {
	case <-co.w.done:
		if co.w.err != nil {
			co.fatalf("workload: %v", co.w.err)
		}
	case <-time.After(drainLimit):
		co.fatalf("workload did not finish within %s", drainLimit)
	}
}

// buildSetting builds logrepl.import.debeziumOffset from the real offset.
func (co *cutover) buildSetting() string {
	co.t.Helper()
	s := dbzoffset.Setting{
		Format:           dbzoffset.FormatV1,
		Connector:        co.connector,
		Partition:        co.partitionRaw,
		Database:         dbName,
		SystemIdentifier: co.sysID,
		Plugin:           co.st.plugin,
		DebeziumVersion:  co.dbzVersion,
		DebeziumSlotName: co.dbzSlot,
		PublicationName:  co.dbzPub,
		Tables:           []string{"public." + co.tables[0], "public." + co.tables[1]},
		Identity: dbzoffset.Identity{
			ConduitSlotConsistentPoint: co.c.String(),
			DebeziumSlotFlushAtWait:    co.flushAtWait.String(),
			ConduitPublication:         co.conduitPubID,
		},
		Offset: co.offsetRaw,
	}
	enc, err := s.Encode()
	if err != nil {
		co.fatalf("encode setting: %v", err)
	}
	imp, err := dbzoffset.ParseSetting(enc)
	if err != nil {
		co.fatalf("the connector would refuse the setting: %v\n%s", err, enc)
	}
	if imp.Plan.Case != co.plan.Case {
		co.fatalf("ParseSetting plan case %s differs from NewPlan(offset) %s", imp.Plan.Case, co.plan.Case)
	}
	return enc
}

func (co *cutover) openSource(setting string) sdk.Source {
	co.t.Helper()
	src := postgres.Connector.NewSource()
	err := sdk.Util.ParseConfig(co.ctx, map[string]string{
		"url": co.st.pgURL(), "tables": co.tables[0] + "," + co.tables[1],
		"cdcMode": "logrepl", "snapshotMode": "never",
		"logrepl.slotName": co.conduitSlot, "logrepl.publicationName": co.conduitPub,
		"logrepl.autoCleanup": "false", "logrepl.withAvroSchema": "false",
		"logrepl.import.debeziumOffset":  setting,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, src.Config(), postgres.Connector.NewSpecification().SourceParams)
	if err != nil {
		co.fatalf("parse source config: %v", err)
	}
	if err := src.Config().Validate(co.ctx); err != nil {
		co.fatalf("validate source config: %v", err)
	}
	if err := src.Open(co.ctx, nil); err != nil {
		_ = src.Teardown(context.Background())
		co.fatalf("open source with the import: %v\nsetting: %s", err, setting)
	}
	co.t.Cleanup(func() { _ = src.Teardown(context.Background()) })
	return src
}

// cleanup removes everything the run created in the shared database. The
// compose project's `down -v` removes the rest.
func (co *cutover) cleanup() {
	bg, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, _ = co.st.rest(bg, http.MethodDelete, "/connectors/"+co.connector, nil)
	conn, err := pgx.Connect(bg, co.st.pgURL())
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	for _, slot := range []string{co.conduitSlot, co.probeSlot, co.dbzSlot} {
		for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
			if _, err := conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", slot); err == nil {
				break
			} else if strings.Contains(err.Error(), "does not exist") {
				break
			}
		}
	}
	for _, p := range []string{co.dbzPub, co.conduitPub} {
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+p)
	}
	for _, tb := range co.tables {
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tb)
	}
}

// result is one run's numbers, logged as a RESULT line.
type result struct {
	Debezium, Plugin, Cut, Case string
	Lc, L, C, FlushAtWait       string
	Xid                         uint32
	CutXid                      uint32
	HitWindow                   bool
	MessageType                 string
	Committed, DbzRows          int
	ConduitRows                 int
	DbzSnapshotReads            int
	Observed, Predicted         int
	ObservedNotPredicted        int
	Loss                        int
	ConduitSelfDuplicates       int
	SlotFlushAfterAcks          string
}

func (r result) log(t *testing.T) {
	t.Helper()
	b, _ := json.Marshal(r)
	t.Logf("RESULT %s", b)
}

// importAndDiff opens the source with the import, drains it, and asserts the
// design's guarantees. It returns the run's numbers.
func (co *cutover) importAndDiff() result {
	t := co.t
	t.Helper()
	co.setting = co.buildSetting()
	src := co.openSource(co.setting)

	last := co.w.marker
	out := drainConduit(co.ctx, t, src, last, drainQuiet, drainLimit)

	// Invariant 1 (D6): the slot only moves once records are acked. After the
	// acks it must be past Lc.
	var flushAfter pglogrepl.LSN
	waitUntil(t, time.Minute, 500*time.Millisecond, "Conduit's slot to move past Lc", func() (bool, error) {
		f, _, exists, err := slotState(co.ctx, co.conn, co.conduitSlot)
		if err != nil || !exists {
			return false, fmt.Errorf("slot exists=%v err=%v", exists, err)
		}
		flushAfter = f
		if f <= co.off.LSNCommit {
			return false, fmt.Errorf("flush %s, Lc %s", f, co.off.LSNCommit)
		}
		return true, nil
	})

	lines, err := co.st.readTopics(co.ctx, co.prefix)
	if err != nil {
		co.fatalf("read Debezium's topics: %v", err)
	}
	dbz, snapReads, err := debeziumChanges(lines)
	if err != nil {
		co.fatalf("%v", err)
	}
	predicted := co.predictDuplicates()
	ledger := co.led.snapshot()

	res := result{
		Debezium: co.st.dbzVersion, Plugin: co.st.plugin, Cut: string(co.cut), Case: co.plan.Case.String(),
		Lc: co.off.LSNCommit.String(), L: co.off.LSN.String(), C: co.c.String(), FlushAtWait: co.flushAtWait.String(),
		CutXid: co.cutXid, HitWindow: co.hitWindow, MessageType: string(co.off.MessageType),
		Committed: len(ledger), DbzRows: len(dbz), ConduitRows: out.records, DbzSnapshotReads: snapReads,
		Predicted: len(predicted), SlotFlushAfterAcks: flushAfter.String(),
	}
	if co.off.HasTxID {
		res.Xid = co.off.TxID
	}
	co.assertDiff(&res, ledger, dbz, out.counts, predicted)

	if body := co.logs.String(); strings.Contains(body, "debezium offset import: resuming") {
		for _, l := range strings.Split(body, "\n") {
			if strings.Contains(l, "debezium offset import: resuming") {
				t.Logf("connector: %s", l)
			}
		}
	}
	return res
}

// predictDuplicates computes the S2 duplicate set from the probe slot: for a
// DML offset, the changes of the offset's transaction at the offset's LSN
// (the transaction is the one with xid X, a commit at or past Lc, and
// ts_usec as its commit time); for every other case, the empty set. The set
// is an upper bound: Debezium may have delivered only some of the changes at
// L (a COPY's rows share one LSN).
func (co *cutover) predictDuplicates() map[string]bool {
	co.t.Helper()
	pred := map[string]bool{}
	if co.plan.Case != dbzoffset.CaseDML || co.plan.Partial == nil {
		return pred
	}
	txs, err := probeTransactions(co.ctx, co.conn, co.probeSlot, co.dbzPub)
	if err != nil {
		co.fatalf("%v", err)
	}
	matched := 0
	for _, tx := range txs {
		if !co.plan.Partial.Matches(co.plan, tx.commitLSN, tx.xid, tx.commitTime) {
			continue
		}
		matched++
		for _, ch := range tx.changes {
			if ch.lsn == co.plan.Partial.SkipChangesBelow {
				pred[ch.c.key()] = true
			}
		}
	}
	if matched != 1 {
		co.fatalf("the probe slot has %d transactions matching xid %d (commit >= %s); want exactly 1", matched, co.off.TxID, co.off.LSNCommit)
	}
	return pred
}

// assertDiff checks no loss, the duplicate bound and no duplicates inside
// Conduit's output.
func (co *cutover) assertDiff(res *result, ledger map[string]change, dbz, cdt map[string]int, predicted map[string]bool) {
	t := co.t
	t.Helper()

	var loss, phantom, both, outside, selfDup []string
	for k := range ledger {
		if dbz[k] == 0 && cdt[k] == 0 {
			loss = append(loss, k)
		}
	}
	for k, n := range cdt {
		if _, ok := ledger[k]; !ok {
			phantom = append(phantom, k)
		}
		if n > 1 {
			selfDup = append(selfDup, fmt.Sprintf("%s x%d", k, n))
		}
		if dbz[k] > 0 {
			both = append(both, k)
			if !predicted[k] {
				outside = append(outside, k)
			}
		}
	}
	for k := range dbz {
		if _, ok := ledger[k]; !ok {
			phantom = append(phantom, "debezium:"+k)
		}
	}
	res.Observed, res.ObservedNotPredicted, res.Loss, res.ConduitSelfDuplicates = len(both), len(outside), len(loss), len(selfDup)

	header := fmt.Sprintf("offset=%s plan: case=%s skip_commits_below=%s partial=%+v", co.offsetRaw, co.plan.Case, co.plan.SkipCommitsBelow, co.plan.Partial)
	if len(loss) > 0 {
		t.Errorf("SEV-0 LOSS: %d committed rows are in neither Debezium's topics nor Conduit's output.\n%s\nrows: %s", len(loss), header, sample(loss))
	}
	if len(outside) > 0 {
		t.Errorf("SEV-0 DUPLICATES OUTSIDE THE S2 BOUND: %d rows are in both outputs but not in the predicted set (%d rows).\n%s\nrows: %s\npredicted: %s",
			len(outside), len(predicted), header, sample(outside), sample(keys(predicted)))
	}
	if len(selfDup) > 0 {
		t.Errorf("Conduit delivered %d rows more than once.\n%s\nrows: %s", len(selfDup), header, sample(selfDup))
	}
	if len(phantom) > 0 {
		t.Errorf("rows in an output that the workload never committed (a harness bug or a corruption): %s", sample(phantom))
	}
	if len(both) != len(predicted) {
		t.Logf("observed duplicates %d, predicted bound %d: Debezium delivered a subset of the changes at L", len(both), len(predicted))
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sample(in []string) string {
	sort.Strings(in)
	if len(in) > 20 {
		return strings.Join(in[:20], ", ") + fmt.Sprintf(", ... (%d more)", len(in)-20)
	}
	return strings.Join(in, ", ")
}

// debeziumVersion reads the Postgres connector's version from Kafka Connect,
// as the CLI does (`GET /connector-plugins`).
func (co *cutover) debeziumVersion() string {
	body, err := co.st.rest(co.ctx, http.MethodGet, "/connector-plugins", nil)
	if err != nil {
		co.fatalf("GET /connector-plugins: %v", err)
	}
	var plugins []struct {
		Class   string `json:"class"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &plugins); err != nil {
		co.fatalf("decode /connector-plugins: %v", err)
	}
	for _, p := range plugins {
		if p.Class == "io.debezium.connector.postgresql.PostgresConnector" {
			return p.Version
		}
	}
	co.fatalf("the Debezium Postgres connector is not installed: %s", body)
	return ""
}
