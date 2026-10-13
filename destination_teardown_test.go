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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-postgres/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"go.uber.org/goleak"
)

// TestDestinationTeardownDuringWrite is the regression test for #354: Teardown used to close the pgx connection while a Write was still using it. The SDK
// calls Teardown without a prior Stop when a pipeline stops on an error, and
// cancels the open context first, so a Write can be in flight.
// It needs -race to see the data race that used to be reported.
func TestDestinationTeardownDuringWrite(t *testing.T) {
	ctx := context.Background()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("teardown_race_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE "+table) }()

	dst := Connector.NewDestination()
	err := sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RegularConnString, "table": table,
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}, dst.Config(), Connector.NewSpecification().DestinationParams)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Open(ctx); err != nil {
		t.Fatal(err)
	}

	writing := make(chan struct{})
	done := make(chan struct{})
	var acked int64 // records Write reported as written
	go func() {
		defer close(done)
		id := int64(0)
		for i := 0; ; i++ {
			recs := make([]opencdc.Record, 100)
			for j := range recs {
				id++
				recs[j] = opencdc.Record{
					Operation: opencdc.OperationCreate,
					Key:       opencdc.StructuredData{"id": id},
					Payload:   opencdc.Change{After: opencdc.StructuredData{"id": id, "name": "x"}},
				}
			}
			if i == 2 {
				close(writing)
			}
			n, err := dst.Write(ctx, recs)
			acked += int64(n)
			if err != nil {
				return // Teardown stopped us
			}
		}
	}()

	<-writing
	if err := dst.Teardown(ctx); err != nil {
		t.Fatal(err)
	}
	<-done

	// Invariant 1: everything Write acked is durable, even though Teardown
	// canceled the Write that was in flight.
	var rows int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows < acked {
		t.Fatalf("Write acked %d records but only %d rows are in the table", acked, rows)
	}

	// Invariant 1: after Teardown a Write must fail rather than report records
	// written to a connection that is gone.
	n, err := dst.Write(ctx, []opencdc.Record{{
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": int64(-1)},
		Payload:   opencdc.Change{After: opencdc.StructuredData{"id": int64(-1), "name": "late"}},
	}})
	if err == nil || n != 0 {
		t.Fatalf("Write after Teardown returned n=%d err=%v, want n=0 and an error", n, err)
	}
}

// newRawDestination opens a Destination without the SDK middleware so the
// tests can reach its internals.
func newRawDestination(ctx context.Context, t *testing.T, table string) *Destination {
	t.Helper()
	d := &Destination{stmtBuilder: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	err := sdk.Util.ParseConfig(ctx, map[string]string{
		"url": test.RegularConnString, "table": table,
	}, d.Config(), Connector.NewSpecification().DestinationParams)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Teardown(context.Background()) })
	return d
}

func newTeardownTable(ctx context.Context, t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	table := fmt.Sprintf("teardown_%d", time.Now().UnixNano()%1000000000)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, name text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE "+table) })
	return table, conn
}

func upsertRecord(id int64, name string) opencdc.Record {
	return opencdc.Record{
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id": id},
		Payload:   opencdc.Change{After: opencdc.StructuredData{"id": id, "name": name}},
	}
}

// TestDestinationTeardownBoundedByContext is the regression test for the
// liveness half of #354: a Write blocked on a row lock used to hold the
// connection mutex, so Teardown(ctx) blocked for as long as the lock was held
// regardless of ctx. Now Teardown cancels the Write (which rolls back, returns
// (0, err) and acks nothing) and returns within its own bound.
func TestDestinationTeardownBoundedByContext(t *testing.T) {
	ctx := context.Background()
	table, conn := newTeardownTable(ctx, t)
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'original')", table)); err != nil {
		t.Fatal(err)
	}
	dst := newRawDestination(ctx, t, table)

	// A second session holds the row lock the Write needs.
	locker := test.ConnectSimple(ctx, t, test.RegularConnString)
	tx, err := locker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET name = 'locked' WHERE id = 1", table)); err != nil {
		t.Fatal(err)
	}

	// Interrupting a Write mid-batch must not leave pgx's context watcher
	// parked (it does when pgx is handed the canceled context).
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	type result struct {
		n   int
		err error
		at  time.Time
	}
	res := make(chan result, 1)
	go func() {
		n, err := dst.Write(ctx, []opencdc.Record{upsertRecord(1, "from-write")})
		res <- result{n, err, time.Now()}
	}()
	// Wait until the Write is actually blocked on the lock.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid() AND query LIKE '%"+table+"%'").Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the Write never blocked on the row lock")
		}
		time.Sleep(20 * time.Millisecond)
	}

	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	start := time.Now()
	tdErr := make(chan error, 1)
	go func() { tdErr <- dst.Teardown(tctx) }()
	select {
	case err := <-tdErr:
		if err != nil {
			t.Fatalf("Teardown: %v", err)
		}
	case <-time.After(6 * time.Second):
		// Release the lock so the blocked goroutines can finish, then fail.
		_ = tx.Rollback(ctx)
		t.Fatal("Teardown still blocked 6s in, with a 3s context: it is not bounded by its context")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Teardown took %s with the row lock still held, want it bounded by its context", took)
	}

	select {
	case r := <-res:
		if r.err == nil || r.n != 0 {
			t.Fatalf("canceled Write returned n=%d err=%v, want n=0 and an error", r.n, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write did not return after Teardown")
	}

	// Not asserted: whether the server still applies the canceled statement
	// once the lock is released. pgx closes the socket on cancellation without
	// a server-side cancel, so the backend may finish the upsert. That is the
	// at-least-once case: the Write reported an error, nothing was acked, the
	// record is redelivered and the upsert is idempotent.
}

// TestDestinationTeardownAfterAckedWrite covers the ordinary case: a Write that
// finished and was acked is durable, and Teardown after it succeeds.
func TestDestinationTeardownAfterAckedWrite(t *testing.T) {
	ctx := context.Background()
	table, conn := newTeardownTable(ctx, t)
	dst := newRawDestination(ctx, t, table)

	n, err := dst.Write(ctx, []opencdc.Record{upsertRecord(1, "a"), upsertRecord(2, "b")})
	if err != nil || n != 2 {
		t.Fatalf("Write returned n=%d err=%v, want 2 and no error", n, err)
	}
	if err := dst.Teardown(ctx); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	var rows int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("got %d rows, want the 2 acked records", rows)
	}
}

// TestDestinationTeardownContextExpires covers the path where the Write does
// not let go in time: Teardown returns its context error without closing the
// connection under the writer, and the writer closes it when it finishes.
func TestDestinationTeardownContextExpires(t *testing.T) {
	ctx := context.Background()
	table, _ := newTeardownTable(ctx, t)
	dst := newRawDestination(ctx, t, table)

	dst.sem() <- struct{}{} // a Write that ignores cancellation
	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err := dst.Teardown(tctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Teardown returned %v, want context.DeadlineExceeded", err)
	}
	if dst.conn == nil {
		t.Fatal("Teardown closed the connection under a running Write")
	}

	// The Write finishes: it is the last user of the connection and closes it.
	dst.endWrite()
	dst.release()
	if dst.conn != nil {
		t.Fatal("the abandoned connection was not closed when the Write finished")
	}
}

// TestDestinationTeardownRacesManyWrites runs concurrent Writes against a
// Teardown under -race. Invariants: once Teardown has begun every new Write
// fails with (0, err); nothing is acked after Teardown returned (Teardown
// returns only when no Write holds the connection); everything acked is
// durable; and nothing panics on a closed or nil connection.
func TestDestinationTeardownRacesManyWrites(t *testing.T) {
	ctx := context.Background()
	table, conn := newTeardownTable(ctx, t)
	dst := newRawDestination(ctx, t, table)
	// No Write, semaphore wait or connection watcher may outlive Teardown.
	// Goroutines of the test's own helpers are excluded.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const writers = 8
	var (
		tdReturned atomic.Bool
		tdBegan    atomic.Bool
		acked      atomic.Int64
		nextID     atomic.Int64
		wg         sync.WaitGroup
		started    = make(chan struct{}, writers)
		failures   = make(chan string, 4*writers)
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			for {
				beganBefore := tdBegan.Load()
				base := nextID.Add(5)
				recs := make([]opencdc.Record, 5)
				for j := range recs {
					recs[j] = upsertRecord(base+int64(j), "x")
				}
				n, err := dst.Write(ctx, recs)
				if n > 0 && tdReturned.Load() {
					failures <- "a Write was acked after Teardown returned"
				}
				if err != nil && n != 0 {
					failures <- fmt.Sprintf("Write returned n=%d with an error", n)
				}
				if beganBefore && tdReturned.Load() && err == nil {
					failures <- "a Write that started after Teardown began succeeded"
				}
				acked.Add(int64(n))
				if err != nil {
					return
				}
			}
		}()
	}
	for w := 0; w < writers; w++ {
		<-started
	}
	time.Sleep(50 * time.Millisecond)
	tdBegan.Store(true)
	if err := dst.Teardown(ctx); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	tdReturned.Store(true)
	wg.Wait()
	close(failures)
	for f := range failures {
		t.Error(f)
	}

	// Every new Write fails fast with (0, err).
	n, err := dst.Write(ctx, []opencdc.Record{upsertRecord(-1, "late")})
	if err == nil || n != 0 {
		t.Fatalf("Write after Teardown returned n=%d err=%v, want n=0 and an error", n, err)
	}
	var rows int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows < acked.Load() {
		t.Fatalf("acked %d records but only %d rows are durable", acked.Load(), rows)
	}
}
