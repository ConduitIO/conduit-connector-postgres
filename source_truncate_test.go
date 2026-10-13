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

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// truncateTestWait bounds every wait in this file. It is deliberately generous:
// the tests wait on conditions, not on a deadline tuned to a quiet machine, so
// a loaded CI runner makes them slower, not flaky.
const truncateTestWait = 2 * time.Minute

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

// truncateWarnings returns the structured TRUNCATE warnings logged so far.
// Parsing the JSON lines (rather than searching the raw text) means the
// assertions are about the warning itself and cannot be satisfied by an
// unrelated log line that happens to mention a table.
func truncateWarnings(t *testing.T, logs *lockedBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "TRUNCATE was NOT propagated") {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("TRUNCATE warning is not a JSON log line: %q: %v", line, err)
		}
		if m["level"] == "warn" {
			out = append(out, m)
		}
	}
	return out
}

// eventually polls cond every 100ms until it holds or truncateTestWait passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(truncateTestWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", truncateTestWait, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// truncateFixture is a CDC-only source over two fresh tables. The slot and
// publication are named after table a and removed on cleanup.
type truncateFixture struct {
	ctx  context.Context
	conn *pgx.Conn
	src  sdk.Source
	logs *lockedBuffer
	a, b string
}

func newTruncateFixture(t *testing.T) *truncateFixture {
	t.Helper()
	is := is.New(t)
	logs := &lockedBuffer{}
	ctx := zerolog.New(logs).WithContext(context.Background())
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	suffix := time.Now().UnixNano() % 100000000
	a, b := fmt.Sprintf("trunc_a_%d", suffix), fmt.Sprintf("trunc_b_%d", suffix)
	for _, tb := range []string{a, b} {
		_, err := conn.Exec(ctx, "CREATE TABLE "+tb+" (id bigint primary key, v text)")
		is.NoErr(err)
	}

	src := Connector.NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RepmgrConnString, "tables": a + "," + b, "cdcMode": "logrepl",
		// CDC only: an initial snapshot would make the time to the first
		// record depend on machine load, which is what made this flaky.
		"snapshotMode":     "never",
		"logrepl.slotName": a, "logrepl.publicationName": a,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, src.Config(), Connector.NewSpecification().SourceParams))

	openCtx, cancel := context.WithCancel(ctx)
	is.NoErr(src.Open(openCtx, nil))

	// Registered after Open so it runs first (LIFO): stop the source, then
	// drop the slot. Teardown returns before the walsender has necessarily
	// exited, and dropping an active slot fails with "is active for PID",
	// so retry until the walsender is gone. Leaving slots behind would
	// exhaust max_replication_slots / max_wal_senders under -count=N.
	t.Cleanup(func() {
		cancel()
		_ = src.Teardown(context.Background())
		bg := context.Background()
		var err error
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if _, err = conn.Exec(bg, "SELECT pg_drop_replication_slot($1)", a); err == nil {
				break
			}
		}
		if err != nil {
			t.Errorf("could not drop replication slot %s: %v", a, err)
		}
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+a)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+a+", "+b)
	})

	return &truncateFixture{ctx: ctx, conn: conn, src: src, logs: logs, a: a, b: b}
}

func (f *truncateFixture) exec(t *testing.T, q string) {
	t.Helper()
	if _, err := f.conn.Exec(f.ctx, q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// read returns records until n have arrived, waiting as long as it takes (up
// to truncateTestWait) rather than for a fixed deadline.
func (f *truncateFixture) read(t *testing.T, n int) []opencdc.Record {
	t.Helper()
	var got []opencdc.Record
	eventually(t, fmt.Sprintf("%d records", n), func() bool {
		rctx, c := context.WithTimeout(f.ctx, time.Second)
		defer c()
		recs, _ := f.src.ReadN(rctx, n-len(got))
		got = append(got, recs...)
		return len(got) >= n
	})
	return got
}

// TestSource_Truncate_WarnsAndKeepsDelivering is the regression test for the
// v0.20.1 part of #344: a multi-table TRUNCATE between two inserts is not
// propagated (no record), but it must be reported with a WARN that names the
// tables and the LSN, and the inserts around it must still be delivered.
func TestSource_Truncate_WarnsAndKeepsDelivering(t *testing.T) {
	is := is.New(t)
	f := newTruncateFixture(t)

	f.exec(t, "INSERT INTO "+f.a+" VALUES (1, 'before')")
	f.exec(t, "TRUNCATE "+f.a+", "+f.b)
	f.exec(t, "INSERT INTO "+f.b+" VALUES (2, 'after')")

	recs := f.read(t, 2)
	is.Equal(len(recs), 2) // both inserts delivered, TRUNCATE emitted nothing
	is.Equal(recs[0].Metadata["opencdc.collection"], f.a)
	is.Equal(recs[1].Metadata["opencdc.collection"], f.b)

	// The warning is logged by the subscription goroutine before the insert
	// after it is delivered, so it is already there.
	warns := truncateWarnings(t, f.logs)
	is.Equal(len(warns), 2) // one per table of the TRUNCATE, not one per statement
	tables := map[string]bool{}
	for _, w := range warns {
		tables[fmt.Sprint(w["table"])] = true
		is.True(strings.Contains(fmt.Sprint(w["lsn"]), "/")) // names the LSN, e.g. 0/16B3748
		is.Equal(w["suppressed"], float64(0))                // first warning for the table
		is.Equal(w["cascade"], false)
	}
	is.Equal(tables, map[string]bool{"public." + f.a: true, "public." + f.b: true})
}

// TestSource_Truncate_ConfirmedFlushAdvances proves the WARN-only handling
// does not pin the slot: after the last acked record, a run of TRUNCATEs (which
// emit nothing) must not stop confirmed_flush_lsn from advancing past them,
// otherwise the slot would retain WAL for as long as the table is only truncated.
func TestSource_Truncate_ConfirmedFlushAdvances(t *testing.T) {
	is := is.New(t)
	f := newTruncateFixture(t)

	f.exec(t, "INSERT INTO "+f.a+" VALUES (1, 'before')")
	recs := f.read(t, 1)
	is.NoErr(f.src.Ack(f.ctx, recs[0].Position))

	for i := 0; i < 5; i++ {
		f.exec(t, "TRUNCATE "+f.a)
	}
	var target string
	is.NoErr(f.conn.QueryRow(f.ctx, "SELECT pg_current_wal_lsn()::text").Scan(&target))

	// The standby status interval is on the order of seconds; the bound is
	// generous so a loaded runner does not turn this into a flake.
	deadline := time.Now().Add(30 * time.Second)
	var advanced bool
	for !advanced && time.Now().Before(deadline) {
		is.NoErr(f.conn.QueryRow(f.ctx,
			"SELECT coalesce(confirmed_flush_lsn >= $1::pg_lsn, false) FROM pg_replication_slots WHERE slot_name = $2",
			target, f.a).Scan(&advanced))
		if !advanced {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if !advanced {
		var got string
		_ = f.conn.QueryRow(f.ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", f.a).Scan(&got)
		t.Fatalf("confirmed_flush_lsn %s did not reach the post-TRUNCATE WAL position %s within 30s", got, target)
	}
	is.Equal(len(truncateWarnings(t, f.logs)), 1) // 5 TRUNCATEs, one warning: rate limited
}
