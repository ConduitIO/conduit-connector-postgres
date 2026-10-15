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
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

const (
	dbName   = "inventory"
	dbUser   = "postgres"
	dbPass   = "postgres"
	httpWait = 15 * time.Second
)

// stack is the handle on the compose stack run.sh brought up.
type stack struct {
	pgAddr      string // host:port on 127.0.0.1
	connectURL  string // Kafka Connect REST
	project     string // compose project name
	composeFile string
	dbzVersion  string
	plugin      string // Debezium's plugin.name
}

// requireStack reads the stack's addresses from the environment and checks
// that Postgres and Kafka Connect answer. Missing infrastructure is an
// "INFRA:" failure, never a skip: a green run must mean the test ran.
func requireStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{
		pgAddr:      os.Getenv("DBZ_PG_ADDR"),
		connectURL:  os.Getenv("DBZ_CONNECT_URL"),
		project:     os.Getenv("DBZ_COMPOSE_PROJECT"),
		composeFile: os.Getenv("DBZ_COMPOSE_FILE"),
		dbzVersion:  os.Getenv("DBZ_VERSION"),
		plugin:      os.Getenv("DBZ_PLUGIN"),
	}
	if s.pgAddr == "" || s.connectURL == "" || s.project == "" || s.composeFile == "" {
		t.Fatal("INFRA: DBZ_PG_ADDR, DBZ_CONNECT_URL, DBZ_COMPOSE_PROJECT and DBZ_COMPOSE_FILE must be set; " +
			"run this test through `make test-debezium` (test/debezium/run.sh), which brings the stack up")
	}
	if s.plugin == "" {
		s.plugin = "pgoutput"
	}
	if s.plugin != "pgoutput" && s.plugin != "decoderbufs" {
		t.Fatalf("INFRA: DBZ_PLUGIN %q is not pgoutput or decoderbufs", s.plugin)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := s.connect(ctx, t)
	defer conn.Close(context.Background())
	var wal string
	if err := conn.QueryRow(ctx, "SELECT current_setting('wal_level')").Scan(&wal); err != nil || wal != "logical" {
		t.Fatalf("INFRA: the stack's Postgres has wal_level %q (%v), want logical", wal, err)
	}
	if _, err := s.rest(ctx, http.MethodGet, "/connectors", nil); err != nil {
		t.Fatalf("INFRA: Kafka Connect REST at %s: %v", s.connectURL, err)
	}
	return s
}

// pgURL is the connection string of the stack's Postgres as seen from the
// host (the superuser, which also has the replication privilege).
func (s *stack) pgURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPass, s.pgAddr, dbName)
}

func (s *stack) connect(ctx context.Context, t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, s.pgURL())
	if err != nil {
		t.Fatalf("INFRA: connect to Postgres at %s: %v", s.pgAddr, err)
	}
	return conn
}

// rest calls the Kafka Connect REST API. A non-2xx status is an error that
// carries the body.
func (s *stack) rest(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, httpWait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s.connectURL+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return out, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(out))
	}
	return out, nil
}

// connectorOffset is one entry of GET /connectors/{name}/offsets.
type connectorOffset struct {
	Partition json.RawMessage `json:"partition"`
	Offset    json.RawMessage `json:"offset"`
}

// offset returns the connector's committed offset, or ok=false when it has
// none yet.
func (s *stack) offset(ctx context.Context, name string) (connectorOffset, bool, error) {
	b, err := s.rest(ctx, http.MethodGet, "/connectors/"+name+"/offsets", nil)
	if err != nil {
		return connectorOffset{}, false, err
	}
	var resp struct {
		Offsets []connectorOffset `json:"offsets"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return connectorOffset{}, false, fmt.Errorf("decode offsets: %w", err)
	}
	if len(resp.Offsets) == 0 || len(resp.Offsets[0].Offset) == 0 || string(resp.Offsets[0].Offset) == "null" {
		return connectorOffset{}, false, nil
	}
	return resp.Offsets[0], true, nil
}

// waitUntil polls fn every interval until it reports true, fails the test on
// an error that fn marks fatal, and fails it at the deadline. Every wait in
// this package goes through it, so every wait has a hard bound.
func waitUntil(t *testing.T, timeout, interval time.Duration, what string, fn func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		ok, err := fn()
		if ok {
			return
		}
		last = err
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s (last error: %v)", timeout, what, last)
		}
		time.Sleep(interval)
	}
}

// slotFlush is a slot's confirmed_flush_lsn and whether a walsender holds it.
func slotState(ctx context.Context, conn *pgx.Conn, slot string) (flush pglogrepl.LSN, active, exists bool, err error) {
	rows, err := conn.Query(ctx,
		"SELECT confirmed_flush_lsn, active FROM pg_replication_slots WHERE slot_name = $1", slot)
	if err != nil {
		return 0, false, false, err
	}
	defer rows.Close()
	if rows.Next() {
		var f *pglogrepl.LSN
		if err := rows.Scan(&f, &active); err != nil {
			return 0, false, false, err
		}
		if f != nil {
			flush = *f
		}
		exists = true
	}
	return flush, active, exists, rows.Err()
}
