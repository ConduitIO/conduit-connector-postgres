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

// Case: snapshot restore. The database volume is restored from a snapshot
// taken earlier, while the pipeline's saved position is from after it. The
// restored server has the slot as it was at the snapshot, and its WAL, on the
// same timeline, grows again from the snapshot's LSN, so new commits reuse LSNs
// the saved position has already passed.
//
// Rows written after the restore are real, current data in the restored
// database. Rows B written between the snapshot and the restore are gone from
// the database itself and are not expected. The assertion is on the rows
// written after the restore: they are delivered, or the start is refused with
// a code.
//
// KNOWN GAP (see cases.txt). When this test was added, the source resumed on
// the restored slot without complaint and did not deliver the post-restore
// rows whose commit LSN is at or below the saved position: with 10 rows
// written after the restore none arrived, and with 60 exactly the first 10
// did not (the number of rows run 2 had delivered before the restore). This
// is silent loss, not a documented limitation, and the test stays red on
// purpose; run.sh requires it to fail and flips the run red once it passes.
func TestExternalState_SnapshotRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := admin(ctx, t)
	sc := newScenario(ctx, t, conn)

	// Run 1: one row delivered and acked, source stopped, slot kept.
	src, err := sc.open(ctx, pgURL(superUser, superPass), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 1)
	r1 := drain(ctx, t, src, []int{1}, 30*time.Second)
	if r1.err != nil || !all(r1.ids, []int{1}) {
		t.Fatalf("setup: delivered %v, err %v", keys(r1.ids), r1.err)
	}
	teardown(t, src)
	_ = conn.Close(ctx)

	// Volume snapshot S: stop the server cleanly, copy the data volume, start.
	project := getenv("EXTSTATE_PROJECT", "pgextstate")
	dataVol, snapVol := project+"_pgdata", project+"_pgsnap"
	t.Cleanup(func() { dockerRun(t, "volume", "rm", "-f", snapVol) })
	compose(t, "stop", "-t", "30", "pg")
	dockerRun(t, "run", "--rm", "-v", dataVol+":/data", "-v", snapVol+":/snap", "docker.io/library/alpine:3.20",
		"sh", "-c", "rm -rf /snap/* /snap/.[!.]* ; cp -a /data/. /snap/")
	compose(t, "start", "pg")
	conn = waitReady(ctx, t)

	// Run 2, after the snapshot: rows B delivered and acked, position saved.
	src, err = sc.open(ctx, pgURL(superUser, superPass), r1.lastPos, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := rangeIDs(100, 110)
	sc.insert(ctx, t, conn, b...)
	r2 := drain(ctx, t, src, b, 45*time.Second)
	if r2.err != nil || !all(r2.ids, b) {
		t.Fatalf("setup: delivered %v, err %v", keys(r2.ids), r2.err)
	}
	teardown(t, src)
	_ = conn.Close(ctx)

	// Restore the volume snapshot: back to the database as it was at S.
	compose(t, "stop", "-t", "30", "pg")
	dockerRun(t, "run", "--rm", "-v", dataVol+":/data", "-v", snapVol+":/snap", "docker.io/library/alpine:3.20",
		"sh", "-c", "find /data -mindepth 1 -delete && cp -a /snap/. /data/")
	compose(t, "start", "pg")
	conn = waitReady(ctx, t)

	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+sc.table+" WHERE id >= 100").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("the restore did not remove rows B (%d left); the fault was not injected", n)
	}

	// Post-restore writes: current data, enough commits to pass the old LSNs.
	c := rangeIDs(200, 210)
	sc.insert(ctx, t, conn, c...)

	src3, openErr := sc.open(ctx, pgURL(superUser, superPass), r2.lastPos, nil)
	var got map[int]bool
	if openErr == nil {
		defer teardown(t, src3)
		// A read that fails loudly counts as a loud refusal (checked against the pinned codes), not as loss.
		d := drain(ctx, t, src3, c, 45*time.Second)
		got, openErr = d.ids, d.err
	}
	requireLoudOrDelivered(t, "resume after a volume snapshot restore", openErr, got, c,
		"postgres.source.slot_wal_gap", "postgres.source.slot_invalidated")
}
