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

package chaospoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// target is a point/count pair parsed from PGCHAOS_PARK, which may carry
// several comma-separated pairs: the first goroutine to reach any target's
// Nth reach parks. targetSet is false when PGCHAOS_PARK is unset or empty,
// in which case Reach never parks — it still counts, so a no-kill smoke run
// can assert liveness witnesses.
//
// Multiple targets let one child park two different goroutines at two
// different points, each provably suspended, before the kill — the B1
// in-run stacked-DDL scenario parks the harness's read-loop goroutine at
// DriftMarkerAppended (marker durable, halt unarmed) and the connector's
// subscription goroutine at DriftVersionSkipped (the second DDL processed
// in-run, no second marker), then kills the fully frozen process.
var (
	parkConfigOnce sync.Once
	targets        []parkTarget
	targetSet      bool
)

type parkTarget struct {
	point string
	nth   uint64
}

// loadParkConfig reads PGCHAOS_PARK exactly once — on the first call to
// Reach in the process — and memoizes it in targets/targetSet for every
// subsequent call. In the harness's real use, that first call happens
// within milliseconds of the child process starting, before it has done
// anything observable, so in practice this is indistinguishable from
// reading it at startup: the park configuration is fixed for the life of
// the process and cannot be raced or re-targeted mid-run by anything the
// connector itself does. (A literal package init() would read the
// environment before any test could ever set it via t.Setenv, which is
// exactly the property chaospoint_on_test.go exercises directly.)
//
// A malformed PGCHAOS_PARK fails loud (stderr + os.Exit) instead of
// silently never parking: a typo in the harness must surface as an
// immediate, obvious child failure, not as a mystifying waitForMarker
// timeout minutes into a CI run.
func loadParkConfig() {
	parkConfigOnce.Do(func() {
		raw, ok := os.LookupEnv("PGCHAOS_PARK")
		if !ok || raw == "" {
			return
		}

		for _, part := range strings.Split(raw, ",") {
			point, nthStr, found := strings.Cut(part, ":")
			if !found || point == "" {
				fmt.Fprintf(os.Stderr, "chaospoint: malformed PGCHAOS_PARK %q, want \"<point>:<nth>[,<point>:<nth>...]\"\n", raw)
				os.Exit(2)
			}

			nth, err := strconv.ParseUint(nthStr, 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "chaospoint: malformed PGCHAOS_PARK nth %q: %v\n", nthStr, err)
				os.Exit(2)
			}

			targets = append(targets, parkTarget{point: point, nth: nth})
		}
		targetSet = true
	})
}

var (
	countersMu sync.Mutex
	counters   = map[string]*uint64{}
)

// Reach records one visit to the named fault-injection point. When
// PGCHAOS_PARK targets this point's Nth reach, Reach announces PARKED on
// stdout as a single write(2) — so the parent's waitForMarker never sees a
// torn partial line — and then blocks the calling goroutine forever.
//
// There is deliberately no way out of the park: the harness's kill signal
// targets the whole process, not this goroutine, so a context or
// cancellation path here would only make the suspension less exact.
//
// The per-name counter is incremented atomically; concurrent callers (e.g.
// the replication status ticker running alongside a parked snapshot fetch)
// cannot corrupt or race each other's counts. Several goroutines can park
// at different targets in one process (see the targets doc); each prints
// its own PARKED line, and the parent waits for each in turn.
func Reach(name string) {
	loadParkConfig()

	n := atomic.AddUint64(counterFor(name), 1)

	if !targetSet {
		return
	}
	for _, target := range targets {
		if name == target.point && n == target.nth {
			msg := fmt.Sprintf("PARKED %s %d\n", name, n)
			_, _ = os.Stdout.WriteString(msg)

			select {} // park forever; only a signal to the process ends this
		}
	}
}

func counterFor(name string) *uint64 {
	countersMu.Lock()
	defer countersMu.Unlock()

	c, ok := counters[name]
	if !ok {
		c = new(uint64)
		counters[name] = c
	}
	return c
}

// Counts returns a snapshot of the per-point reach counters, keyed by point
// name. The harness uses it to assert liveness witnesses — e.g. "the point
// was reached at least 3 times" — rather than trusting a bare pass; a
// missing witness fails the test as inconclusive instead of green.
func Counts() map[string]uint64 {
	countersMu.Lock()
	defer countersMu.Unlock()

	out := make(map[string]uint64, len(counters))
	for name, c := range counters {
		out[name] = atomic.LoadUint64(c)
	}
	return out
}
