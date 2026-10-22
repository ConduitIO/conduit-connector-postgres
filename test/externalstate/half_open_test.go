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

// Case: half-open replication connection. The network between the source and
// the server stops delivering without closing the connection (a dead NAT
// entry, a firewall that drops silently). The server drops its walsender after
// wal_sender_timeout (10s in this stack); the client sees nothing until the
// path comes back. After it does, every row written meanwhile must be
// delivered, either by the same source or by a restart from the last acked
// position; a restart must not be refused without a code; and the source must
// not sit silent forever.
func TestExternalState_HalfOpenReplicationConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := admin(ctx, t)
	sc := newScenario(ctx, t, conn)

	toxiAPI(ctx, t, "DELETE", "/proxies/pg", nil)
	toxiAPI(ctx, t, "POST", "/proxies", map[string]any{"name": "pg", "listen": "0.0.0.0:15432", "upstream": "pg:5432", "enabled": true})
	t.Cleanup(func() { toxiAPI(context.Background(), t, "DELETE", "/proxies/pg", nil) })

	src, err := sc.open(ctx, proxiedURL(superUser, superPass), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.insert(ctx, t, conn, 1, 2, 3)
	first := drain(ctx, t, src, []int{1, 2, 3}, 30*time.Second)
	if first.err != nil || !all(first.ids, []int{1, 2, 3}) {
		t.Fatalf("setup: delivered %v, err %v", keys(first.ids), first.err)
	}

	// Black-hole both directions without closing: timeout 0 holds the data
	// until the toxic is removed and never closes the connection.
	for _, dir := range []string{"upstream", "downstream"} {
		toxiAPI(ctx, t, "POST", "/proxies/pg/toxics", map[string]any{
			"name": "blackhole-" + dir, "type": "timeout", "stream": dir, "toxicity": 1.0,
			"attributes": map[string]any{"timeout": 0},
		})
	}
	sc.insert(ctx, t, conn, 4, 5)

	// While the path is dead nothing may be delivered (the fault is real) and
	// the walsender has to time out on the server.
	during := drain(ctx, t, src, []int{4, 5}, 25*time.Second)
	if len(during.ids) > 0 {
		t.Fatalf("rows %v arrived through a black-holed connection; the fault was not injected", keys(during.ids))
	}
	if during.err != nil {
		t.Logf("source failed while the path was dead: %v", during.err)
	}

	for _, dir := range []string{"upstream", "downstream"} {
		toxiAPI(ctx, t, "DELETE", "/proxies/pg/toxics/blackhole-"+dir, nil)
	}
	sc.insert(ctx, t, conn, 6)

	// After the path heals: delivered by the same source, or it fails loudly.
	after := drain(ctx, t, src, []int{4, 5, 6}, 60*time.Second)
	if after.err == nil && !all(after.ids, []int{4, 5, 6}) {
		t.Fatalf("SILENT STALL: the path healed 60s ago, rows %v were never delivered and the source reports no error", missing(after.ids, []int{4, 5, 6}))
	}
	got := after.ids
	pos := first.lastPos
	if after.lastPos != nil {
		pos = after.lastPos
	}
	teardown(t, src)

	if !all(got, []int{4, 5, 6}) {
		// It failed loudly (or lost the connection): the engine restarts it.
		src2, openErr := sc.open(ctx, pgURL(superUser, superPass), pos, nil)
		if openErr == nil {
			defer teardown(t, src2)
			d := drain(ctx, t, src2, []int{4, 5, 6}, 45*time.Second)
			for id := range d.ids {
				got[id] = true
			}
			openErr = d.err // a read error is reported with its code, not as missing rows
		}
		requireLoudOrDelivered(t, "restart after the half-open connection", openErr, got, []int{4, 5, 6})
		return
	}
	requireLoudOrDelivered(t, "same source after the half-open connection", nil, got, []int{4, 5, 6})
}
