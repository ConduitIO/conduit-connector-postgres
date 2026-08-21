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
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

// TestReachParks proves the real, conduitchaos-tagged Reach: given a
// matching PGCHAOS_PARK, it emits the PARKED marker and then never
// returns.
//
// It sets PGCHAOS_PARK via t.Setenv and relies on loadParkConfig's
// sync.Once firing on Reach's first call within this process — this is
// the only test in the package that calls Reach, so that first call is
// this test's. A literal package init() would have read the environment
// before t.Setenv ever ran, which is exactly why chaospoint_on.go reads it
// lazily instead (see loadParkConfig's doc).
//
// Reach is called on its own goroutine, not this test's main goroutine:
// select{} inside Reach blocks forever with no timer anywhere that could
// wake it, and if that goroutine WERE the test's main goroutine, the Go
// runtime's deadlock detector would (correctly) kill the process with
// "all goroutines are asleep" the instant it parked, since nothing else
// in the process would be capable of making progress. Running it on a
// background goroutine and observing it from the main goroutine via a
// timer-bounded select avoids that: the main goroutine stays demonstrably
// "awake" the whole time.
func TestReachParks(t *testing.T) {
	is := is.New(t)

	const (
		point = "chaospoint.unit_test.point"
		nth   = 1
	)
	t.Setenv("PGCHAOS_PARK", fmt.Sprintf("%s:%d", point, nth))

	// Redirect the process's real stdout so the test can observe the exact
	// bytes Reach writes, the same channel the harness's parent process
	// reads the PARKED marker from.
	r, w, err := os.Pipe()
	is.NoErr(err)

	origStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = origStdout })

	markerLines := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "PARKED ") {
				markerLines <- line
				return
			}
		}
	}()

	returned := make(chan struct{})
	go func() {
		Reach(point) // 1st reach == target nth: must park, never return
		close(returned)
	}()

	select {
	case line := <-markerLines:
		is.Equal(line, fmt.Sprintf("PARKED %s %d", point, nth))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the PARKED marker on stdout")
	}

	// The marker is written immediately before Reach enters `select {}`
	// (chaospoint_on.go) — its presence already proves the goroutine is
	// past the point of no return. Confirm it, rather than inferring
	// parking purely from a timeout: `returned` staying open for a bounded
	// window is direct evidence the goroutine has not come back.
	select {
	case <-returned:
		t.Fatal("Reach returned instead of parking")
	case <-time.After(300 * time.Millisecond):
		// still blocked in Reach — expected.
	}

	is.Equal(Counts()[point], uint64(nth))
}

// Note: loadParkConfig's sync.Once fires on the first Reach call anywhere
// in this process (see its doc) and is deliberately not reset between
// tests — that statelessness is the honest reflection of the real child
// process, which calls loadParkConfig exactly once, ever. This file
// therefore keeps a single test that exercises Reach's park behavior, so
// no later test could observe a stale, already-consumed PGCHAOS_PARK.
