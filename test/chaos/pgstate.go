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

package chaos

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RepmgrConnString and RegularConnString mirror test.RepmgrConnString /
// test.RegularConnString but point at the chaos stack's OWN compose
// project — host port 5434, not 5433 (test/docker-compose.chaos.yml) — so
// this suite can never accidentally run against a developer's regular
// `make test` stack (whose max_replication_slots=5 would make the
// preflight check below fail loudly, per test/conf.d/postgresql.conf vs.
// test/conf.d.chaos/postgresql.conf, harness plan §7).
const (
	// #nosec G101 -- fixed, published, local-only credentials for this
	// suite's own docker-compose chaos stack (test/docker-compose.chaos.yml,
	// port 5434), not a real secret.
	RepmgrConnString = "postgres://repmgr:repmgrmeroxa@127.0.0.1:5434/meroxadb?sslmode=disable"
	// #nosec G101 -- see RepmgrConnString above; same local-only chaos stack.
	RegularConnString = "postgres://meroxauser:meroxapass@127.0.0.1:5434/meroxadb?sslmode=disable"
)

// wantMaxReplicationSlots is test/conf.d.chaos/postgresql.conf's
// max_replication_slots value. requireChaosStack asserts the live server
// actually has this setting, so a developer who points this suite at their
// regular 5433 stack by mistake (or forgets to bring the chaos stack up at
// all) gets an immediate, unmistakable INFRA failure instead of a
// mysterious slot-exhaustion error three scenarios later.
const wantMaxReplicationSlots = "20"

// requireChaosStack is the harness's fail-closed preflight (acceptance
// criterion B0.10: missing infra is an INFRA: t.Fatal, never a t.Skip).
// Every scenario in this package calls it first. It does two independent
// checks:
//  1. Can we connect at all? (docker compose stack not running)
//  2. Is max_replication_slots really 20? (wrong-stack case: a developer's
//     regular test/docker-compose.yml stack, still on port 5433... except
//     that would fail step 1 too, since we dial 5434 — this check is the
//     one that catches a MISCONFIGURED chaos stack, e.g. someone edited
//     test/conf.d.chaos/postgresql.conf and forgot to recreate the
//     container, so the running server is stale.)
func requireChaosStack(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := cpool.New(ctx, RepmgrConnString)
	if err != nil {
		t.Fatalf("INFRA: build connection pool for chaos stack (port 5434): %v", err)
	}

	var got string
	err = pool.QueryRow(ctx, "SELECT current_setting('max_replication_slots')").Scan(&got)
	if err != nil {
		pool.Close()
		t.Fatalf("INFRA: chaos stack unreachable on port 5434 - is `make test-chaos` (or "+
			"`docker compose -f test/docker-compose.chaos.yml up --wait`) running? %v", err)
	}

	if got != wantMaxReplicationSlots {
		pool.Close()
		t.Fatalf("INFRA: chaos stack reports max_replication_slots=%s, want %s - this is not "+
			"the chaos stack (test/docker-compose.chaos.yml), or it's stale; refusing to run "+
			"against it (harness plan §7, §10 'shared port silently downgrades the stack')",
			got, wantMaxReplicationSlots)
	}

	t.Cleanup(pool.Close)
	return pool
}

// SlotState is the parent harness's own, richer replication-slot snapshot.
// It is deliberately NOT source/logrepl/internal.ReadReplicationSlot, which
// returns only 3 columns because that's all production code needs -
// widening that production query for test convenience would be exactly the
// kind of test-shaped production code this repo's review standards forbid.
// A second, parent-only query is the honest alternative (harness plan §5),
// built with source/cpool.New per .golangci.yml's forbidigo rule (pgxpool.New*
// is only permitted inside source/cpool itself).
type SlotState struct {
	Name              string
	Active            bool
	ActivePID         *int32
	RestartLSN        string
	ConfirmedFlushLSN string
	WALStatus         string
	SafeWALSize       *int64
	CurrentWALLSN     string
}

// ErrSlotNotFound is returned by ReadSlotState when no replication slot with
// the given name exists.
var ErrSlotNotFound = errors.New("replication slot not found")

// ReadSlotState reads name's full state from pg_replication_slots, plus the
// server's current WAL insert position (pg_current_wal_lsn()) for computing
// how far a subscriber has fallen behind. Every field here is something a
// future kill scenario's PRECONDITION or "wal_status='reserved', therefore
// this is a real gap and not slot invalidation" guard (harness plan §10)
// needs and ReadReplicationSlot doesn't expose.
func ReadSlotState(ctx context.Context, pool *pgxpool.Pool, name string) (SlotState, error) {
	const query = `
		SELECT
			slot_name,
			active,
			active_pid,
			restart_lsn::text,
			confirmed_flush_lsn::text,
			coalesce(wal_status, ''),
			safe_wal_size,
			pg_current_wal_lsn()::text
		FROM pg_replication_slots
		WHERE slot_name = $1`

	var s SlotState
	err := pool.QueryRow(ctx, query, name).Scan(
		&s.Name, &s.Active, &s.ActivePID, &s.RestartLSN, &s.ConfirmedFlushLSN,
		&s.WALStatus, &s.SafeWALSize, &s.CurrentWALLSN,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SlotState{}, fmt.Errorf("%s: %w", name, ErrSlotNotFound)
		}
		return SlotState{}, fmt.Errorf("read slot state %q: %w", name, err)
	}
	return s, nil
}
