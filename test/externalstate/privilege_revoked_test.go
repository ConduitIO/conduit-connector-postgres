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
	"context"
	"testing"
	"time"
)

// Case: privilege revoked mid-run. The role loses REPLICATION while the source
// streams. The running walsender keeps going (the attribute is checked when a
// connection starts), so the break shows at the next connect: the restart must
// fail with a code (SQLSTATE 42501), and once the privilege is back the same
// position must resume and deliver everything written meanwhile, because the
// slot was never lost.
//
// The variant that revokes EXECUTE on pg_logical_emit_message belongs to the
// slot-identity beacons (#447) and is a pending case in cases.txt.
func TestExternalState_ReplicationPrivilegeRevokedMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := admin(ctx, t)
	sc := newScenario(ctx, t, conn)

	const role, pass = "extstate_repl", "extstate_repl_pw" //nolint:gosec // throwaway role of the docker-compose test database
	_, _ = conn.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	if _, err := conn.Exec(ctx, "CREATE ROLE "+role+" LOGIN REPLICATION PASSWORD '"+pass+"'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := admin(context.Background(), t)
		_, _ = c.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = c.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	// The role needs the table, and the connector creates the publication, so
	// the role owns it: that is the least a replicating role needs.
	if _, err := conn.Exec(ctx, "ALTER TABLE "+sc.table+" OWNER TO "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "GRANT CREATE ON DATABASE postgres TO "+role); err != nil {
		t.Fatal(err)
	}
	url := pgURL(role, pass)

	src, err := sc.open(ctx, url, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 1, 2, 3)
	first := drain(ctx, t, src, []int{1, 2, 3}, 30*time.Second)
	if first.err != nil || !all(first.ids, []int{1, 2, 3}) {
		t.Fatalf("setup: delivered %v, err %v", keys(first.ids), first.err)
	}

	if _, err := conn.Exec(ctx, "ALTER ROLE "+role+" NOREPLICATION"); err != nil {
		t.Fatal(err)
	}
	// Force the reconnect the engine would do after any network blip.
	if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active_pid IS NOT NULL", sc.table); err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 4, 5)

	// The running source: fails, or still delivers; never silent, never lies.
	mid := drain(ctx, t, src, []int{4, 5}, 30*time.Second)
	if mid.err == nil && !all(mid.ids, []int{4, 5}) {
		t.Fatalf("SILENT STALL: the walsender was terminated, rows %v were never delivered, and the source reports no error", missing(mid.ids, []int{4, 5}))
	}
	pos := first.lastPos
	if mid.lastPos != nil {
		pos = mid.lastPos
	}
	teardown(t, src)

	// Restart without the privilege: refused with a code.
	if src2, openErr := sc.open(ctx, url, pos, nil); openErr == nil {
		teardown(t, src2)
		t.Fatal("Open succeeded for a role without REPLICATION; the fault was not injected")
	} else if coded(openErr) == "" {
		t.Fatalf("restart without REPLICATION failed without a code: %v", openErr)
	} else {
		t.Logf("restart without REPLICATION refused with %s", coded(openErr))
	}

	// Privilege back: nothing was lost.
	if _, err := conn.Exec(ctx, "ALTER ROLE "+role+" REPLICATION"); err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 6)
	src3, openErr := sc.open(ctx, url, pos, nil)
	got := map[int]bool{}
	for id := range mid.ids {
		got[id] = true
	}
	if openErr == nil {
		defer teardown(t, src3)
		for id := range drain(ctx, t, src3, []int{4, 5, 6}, 45*time.Second).ids {
			got[id] = true
		}
	}
	requireLoudOrDelivered(t, "restart after REPLICATION was granted again", openErr, got, []int{4, 5, 6})
}
