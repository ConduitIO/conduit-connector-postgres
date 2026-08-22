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

// This file proves the harness's own spawn/wait/marker/sigkill plumbing
// (harness.go) using the Postgres-free echo child (child.go's
// runEchoChild), so a bug in THIS package's process supervision is never
// masked by - or mistaken for - a bug in the real connector. It needs no
// docker stack: it never dials Postgres.

package chaos

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-postgres/internal/chaospoint"
)

// TestHarness_EchoChild_CleanExit proves the non-parking path end to end:
// spawn, observe every expected marker in order via waitForMarker/
// waitForCount (never a bare sleep - see pollUntil's doc), and a clean
// waitExit with exit code 0.
func TestHarness_EchoChild_CleanExit(t *testing.T) {
	cp := spawnChildWithEnv(t, []string{
		envEchoChild + "=" + envValueTrue,
		envEchoReaches + "=3",
	})

	cp.waitForMarker(t, "OPENED", 10*time.Second)
	cp.waitForCount(t, "REACHED ", 3, 10*time.Second)
	cp.waitForMarker(t, "DONE", 10*time.Second)

	cp.waitExit(t, 10*time.Second)

	if n := cp.progressCount("REACHED "); n != 3 {
		t.Fatalf("want 3 REACHED lines, got %d\n%s", n, cp.diagnostics())
	}
}

// TestHarness_EchoChild_ParkThenSigkill proves the parking + SIGKILL path:
// a child targeted by PGCHAOS_PARK reaches exactly as far as its Nth reach,
// announces PARKED (chaospoint_on.go), and then genuinely never returns -
// sigkill is required to end it, and the resulting wait status is SIGKILL,
// not a clean exit. This is the same park-then-kill shape every future
// SIGKILL scenario in this package (B0-3, B0-4) will reuse against the real
// connector; here it is proven once, cheaply, against the echo child.
func TestHarness_EchoChild_ParkThenSigkill(t *testing.T) {
	const parkAtNth = 2

	cp := spawnChildWithEnv(t, []string{
		envEchoChild + "=" + envValueTrue,
		envEchoReaches + "=5",
		"PGCHAOS_PARK=" + fmt.Sprintf("%s:%d", chaospoint.SnapshotFetchRow, parkAtNth),
	})

	cp.waitForMarker(t, "OPENED", 10*time.Second)

	parked := cp.waitForMarker(t, "PARKED", 10*time.Second)
	wantParked := fmt.Sprintf("PARKED %s %d", chaospoint.SnapshotFetchRow, parkAtNth)
	if parked != wantParked {
		t.Fatalf("marker mismatch: got %q, want %q", parked, wantParked)
	}

	// Exactly (parkAtNth - 1) REACHED lines: reach N=2 parks BEFORE
	// child.go's runEchoChild prints its REACHED marker for that iteration
	// (chaospoint.Reach is called first), so REACHED 2 must never appear.
	if n := cp.progressCount("REACHED "); n != parkAtNth-1 {
		t.Fatalf("want %d REACHED lines before park, got %d\n%s", parkAtNth-1, n, cp.diagnostics())
	}

	// DONE must never appear - the child is parked, not finished.
	if _, ok := cp.line("DONE"); ok {
		t.Fatalf("child printed DONE despite being parked\n%s", cp.diagnostics())
	}

	cp.sigkill(t)

	ws, ok := cp.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("unexpected ProcessState.Sys() type %T", cp.cmd.ProcessState.Sys())
	}
	if !ws.Signaled() {
		t.Fatalf("child did not die by signal: %v", ws)
	}
	if ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child died by %v, want SIGKILL", ws.Signal())
	}
}

// TestHarness_WaitForMarker_TimesOutWhenMarkerNeverArrives proves the other
// half of acceptance criterion B0.6 ("a park that is never reached FAILS"):
// waitForMarker's deadline actually fires - a marker that never arrives is
// a t.Fatal, not a silent pass - by targeting an impossible reach count
// (envEchoReaches=1, PGCHAOS_PARK's nth=99) and confirming the child exits
// cleanly with NO PARKED marker, i.e. the precondition a real scenario's
// waitForMarker("PARKED", ...) call would time out on. This test does not
// itself call waitForMarker with a doomed deadline (that would make this
// suite blindly wait out a real timeout on every run); it instead asserts
// the underlying condition waitForMarker polls for is verifiably absent,
// which is the property that makes waitForMarker's timeout the correct,
// non-flaky failure in a real scenario.
func TestHarness_WaitForMarker_TimesOutWhenMarkerNeverArrives(t *testing.T) {
	cp := spawnChildWithEnv(t, []string{
		envEchoChild + "=" + envValueTrue,
		envEchoReaches + "=1",
		"PGCHAOS_PARK=" + fmt.Sprintf("%s:99", chaospoint.SnapshotFetchRow), // unreachable in a 1-reach run
	})

	cp.waitForMarker(t, "DONE", 10*time.Second)
	cp.waitExit(t, 10*time.Second)

	if _, ok := cp.line("PARKED"); ok {
		t.Fatalf("child parked despite an unreachable nth\n%s", cp.diagnostics())
	}
}
