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

// Case: slot dropped mid-run. An operator (or autoCleanup, or an invalidation)
// drops the slot while the source is streaming. The running source must not
// go on as if nothing happened: it either delivers the rows written after the
// drop (impossible without the slot) or fails its read. The restart from the
// last acked position must then be refused with a code, because everything
// between that position and a new slot is gone.
func TestExternalState_SlotDroppedMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := admin(ctx, t)
	sc := newScenario(ctx, t, conn)

	src, err := sc.open(ctx, pgURL(superUser, superPass), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 1, 2, 3)
	first := drain(ctx, t, src, []int{1, 2, 3}, 30*time.Second)
	if first.err != nil || !all(first.ids, []int{1, 2, 3}) {
		t.Fatalf("setup: delivered %v, err %v", keys(first.ids), first.err)
	}

	dropSlot(conn, sc.table)
	sc.insert(ctx, t, conn, 4, 5)

	// Running source: loud failure, never a quiet stall.
	second := drain(ctx, t, src, []int{4, 5}, 45*time.Second)
	switch {
	case second.err != nil:
		t.Logf("running source failed loudly after the slot was dropped: %v", second.err)
	case all(second.ids, []int{4, 5}):
		t.Fatal("rows written after the slot was dropped were delivered; the fault was not injected")
	default:
		t.Fatalf("SILENT STALL: the slot was dropped, rows [4 5] were never delivered, and the source still reports no error after %s", second.waited)
	}
	teardown(t, src)

	// Restart from the last acked position.
	src2, openErr := sc.open(ctx, pgURL(superUser, superPass), first.lastPos, nil)
	var got map[int]bool
	if openErr == nil {
		defer teardown(t, src2)
		sc.insert(ctx, t, conn, 6)
		// A read that fails loudly counts as a loud refusal (checked against the pinned codes), not as loss.
		d := drain(ctx, t, src2, []int{4, 5, 6}, 30*time.Second)
		got, openErr = d.ids, d.err
	}
	requireLoudOrDelivered(t, "restart after the slot was dropped mid-run", openErr, got, []int{4, 5},
		"postgres.source.slot_wal_gap")
}
