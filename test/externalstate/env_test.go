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

//go:build externalstate

package externalstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	postgres "github.com/conduitio/conduit-connector-postgres"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The stack's superuser and the default host ports, see docker-compose.yml.
const (
	superUser   = "postgres"
	superPass   = "extstate"
	defaultPort = "56150"
	defaultProx = "56151"
	defaultAPI  = "56152"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func pgURL(user, pass string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/postgres?sslmode=disable",
		user, pass, getenv("EXTSTATE_PG_PORT", defaultPort))
}

func proxiedURL(user, pass string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/postgres?sslmode=disable",
		user, pass, getenv("EXTSTATE_PROXY_PORT", defaultProx))
}

// admin connects as the superuser, straight to the server.
func admin(ctx context.Context, t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgURL(superUser, superPass))
	if err != nil {
		// Fail closed: a missing stack is a failure, never a skip.
		t.Fatalf("INFRA: cannot reach the external-state stack (run it with `make test-external-state`): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// scenario names the objects one test owns.
type scenario struct {
	table string // also the slot and the publication name
}

var counter int

func newScenario(ctx context.Context, t *testing.T, conn *pgx.Conn) scenario {
	t.Helper()
	counter++
	name := fmt.Sprintf("es_%d_%d", time.Now().UnixNano()%1_000_000_000, counter)
	if _, err := conn.Exec(ctx, "CREATE TABLE "+name+" (id bigint PRIMARY KEY, v text)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), pgURL(superUser, superPass))
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		dropSlot(c, name)
		_, _ = c.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+name)
		_, _ = c.Exec(context.Background(), "DROP TABLE IF EXISTS "+name)
	})
	return scenario{table: name}
}

// dropSlot drops the slot, ending its walsender first.
func dropSlot(conn *pgx.Conn, slot string) {
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, _ = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active_pid IS NOT NULL", slot)
		if _, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s scenario) insert(ctx context.Context, t *testing.T, conn *pgx.Conn, ids ...int) {
	t.Helper()
	for _, id := range ids {
		if _, err := conn.Exec(ctx, "INSERT INTO "+s.table+" (id, v) VALUES ($1, 'x')", id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
}

// open starts a real *postgres.Source (the connector itself, no engine) in
// CDC-only mode on the scenario's table, slot and publication.
func (s scenario) open(ctx context.Context, url string, pos opencdc.Position, extra map[string]string) (sdk.Source, error) {
	cfg := map[string]string{
		"url": url, "tables": s.table, "cdcMode": "logrepl", "snapshotMode": "never",
		"logrepl.slotName": s.table, "logrepl.publicationName": s.table, "logrepl.withAvroSchema": "false",
		"sdk.schema.extract.key.enabled": "false", "sdk.schema.extract.payload.enabled": "false",
	}
	for k, v := range extra {
		cfg[k] = v
	}
	src := postgres.Connector.NewSource()
	if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), postgres.Connector.NewSpecification().SourceParams); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := src.Open(ctx, pos); err != nil {
		_ = src.Teardown(context.Background())
		return nil, err
	}
	return src, nil
}

func teardown(t *testing.T, src sdk.Source) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Teardown(ctx); err != nil {
		t.Logf("teardown: %v", err)
	}
}

// readResult is what drain observed.
type readResult struct {
	ids     map[int]bool
	lastPos opencdc.Position // position of the last acked record
	err     error            // first read error that is not "nothing yet"
	waited  time.Duration
}

// drain reads and acks until every id in want was seen, a read fails, or the
// deadline passes. A read that returns nothing is not an error.
func drain(ctx context.Context, t *testing.T, src sdk.Source, want []int, d time.Duration) readResult {
	t.Helper()
	res := readResult{ids: map[int]bool{}}
	start := time.Now()
	deadline := start.Add(d)
	for time.Now().Before(deadline) && !all(res.ids, want) {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		recs, err := src.ReadN(rctx, 10)
		cancel()
		if err != nil && !isNothingYet(err) {
			res.err = err
			break
		}
		for _, r := range recs {
			id := recordID(t, r)
			res.ids[id] = true
			res.lastPos = r.Position
			if err := src.Ack(ctx, r.Position); err != nil {
				t.Fatalf("ack %d: %v", id, err)
			}
		}
	}
	res.waited = time.Since(start)
	return res
}

func isNothingYet(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sdk.ErrBackoffRetry)
}

func all(got map[int]bool, want []int) bool {
	for _, w := range want {
		if !got[w] {
			return false
		}
	}
	return true
}

func missing(got map[int]bool, want []int) []int {
	var m []int
	for _, w := range want {
		if !got[w] {
			m = append(m, w)
		}
	}
	return m
}

func recordID(t *testing.T, r opencdc.Record) int {
	t.Helper()
	sd, ok := r.Payload.After.(opencdc.StructuredData)
	if !ok {
		// The connector emits raw JSON when schema extraction is off.
		sd = opencdc.StructuredData{}
		if err := json.Unmarshal(r.Payload.After.Bytes(), &sd); err != nil {
			t.Fatalf("payload %q is not JSON: %v", r.Payload.After.Bytes(), err)
		}
	}
	switch v := sd["id"].(type) {
	case float64:
		return int(v)
	case int64:
		return int(v)
	case int32:
		return int(v)
	case int:
		return v
	default:
		t.Fatalf("id is %T", v)
		return 0
	}
}

// coded reports the machine-matchable code of err: a logrepl.SlotError code
// or a Postgres SQLSTATE. It returns "" for an error with neither, which the
// suite treats as a failure: an uncoded start error is not actionable.
func coded(err error) string {
	if c := logrepl.CodeOf(err); c != "" {
		return string(c)
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "sqlstate:" + pe.Code
	}
	return ""
}

// requireLoudOrDelivered is the suite's single assertion. got is what the
// source delivered, want what was written while it should have been
// delivered; openErr is the error of the start under test, if it failed.
//
// wantCodes pins the refusals that are acceptable for THIS case. A start that
// fails with any other code (or no code) fails the case, and a case that lists
// no code must deliver: "some error" is not an acceptable outcome when the
// engine is expected to recover, and an unrelated error (a timeout, an auth
// failure) must not be able to stand in for the guard under test.
func requireLoudOrDelivered(t *testing.T, what string, openErr error, got map[int]bool, want []int, wantCodes ...string) {
	t.Helper()
	if openErr != nil {
		code := coded(openErr)
		if code == "" {
			t.Fatalf("%s: start failed without a code: %v", what, openErr)
		}
		for _, w := range wantCodes {
			if code == w {
				t.Logf("%s: start refused with %s (loud, acceptable)", what, code)
				return
			}
		}
		if len(wantCodes) == 0 {
			t.Fatalf("%s: start failed with %s (%v), but this case must deliver %v", what, code, openErr, want)
		}
		t.Fatalf("%s: start failed with %s (%v); acceptable refusals for this case are %v", what, code, openErr, wantCodes)
	}
	if m := missing(got, want); len(m) > 0 {
		t.Fatalf("SILENT LOSS (%s): the start succeeded but rows %v were never delivered (delivered %v)", what, m, keys(got))
	}
}

func keys(m map[int]bool) []int {
	var out []int
	for k := range m {
		out = append(out, k)
	}
	return out
}

// toxiproxy talks to the proxy's control API.
func toxiAPI(ctx context.Context, t *testing.T, method, path string, body any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	url := "http://127.0.0.1:" + getenv("EXTSTATE_PROXY_API_PORT", defaultAPI) + path
	req, err := http.NewRequestWithContext(ctx, method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("INFRA: toxiproxy API: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && (method != http.MethodDelete || resp.StatusCode != 404) {
		t.Fatalf("toxiproxy %s %s: %s", method, path, resp.Status)
	}
}

// compose runs docker compose against this suite's project (set by run.sh).
func compose(t *testing.T, args ...string) {
	t.Helper()
	file := os.Getenv("EXTSTATE_COMPOSE_FILE")
	project := getenv("EXTSTATE_PROJECT", "pgextstate")
	if file == "" {
		t.Fatal("INFRA: EXTSTATE_COMPOSE_FILE is not set; run the suite with run.sh")
	}
	full := append([]string{"compose", "-p", project, "-f", file}, args...)
	out, err := exec.CommandContext(context.Background(), "docker", full...).CombinedOutput() //nolint:gosec // test-only; args are this suite's own compose file and project
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(full, " "), err, out)
	}
}

// waitReady waits until the server accepts superuser connections again.
func waitReady(ctx context.Context, t *testing.T) *pgx.Conn {
	t.Helper()
	var last error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		c, err := pgx.Connect(ctx, pgURL(superUser, superPass))
		if err == nil {
			if _, err = c.Exec(ctx, "SELECT 1"); err == nil {
				t.Cleanup(func() { _ = c.Close(context.Background()) })
				return c
			}
			_ = c.Close(ctx)
		}
		last = err
	}
	t.Fatalf("INFRA: server did not come back: %v", last)
	return nil
}

func dockerRun(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.CommandContext(context.Background(), "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func rangeIDs(from, to int) []int {
	var ids []int
	for i := from; i < to; i++ {
		ids = append(ids, i)
	}
	return ids
}
