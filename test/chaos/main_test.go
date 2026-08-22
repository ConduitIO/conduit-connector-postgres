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
	"os"
	"testing"
)

// TestMain intercepts the "am I a chaos child?" case before any actual Go
// test runs (harness plan §3's re-exec protocol; see harness.go's
// spawnChildWithEnv), then sweeps every pgchaos_ replication slot and
// publication before and after the real test run (reaper.go).
//
// This MUST live in a _test.go file: go test only scans _test.go files for
// TestMain/Test*/Benchmark*/Example* - a TestMain defined in a plain .go
// file compiles into the package fine but is never recognized as the
// special entry point, so the child re-exec protocol would silently never
// fire and every child would just run the full test suite instead of
// routing into runRealChild/runEchoChild.
//
// The post-suite sweep treats a REMAINING pgchaos_ slot or publication as a
// hard failure (forces a non-zero exit even if every individual test
// passed): a leaked slot is exactly the failure mode that makes a LATER
// run's "resume from disk" proofs meaningless (see doc.go and the harness
// plan's "how the harness can lie" table, §10, row 1).
func TestMain(m *testing.M) {
	if isRealChildInvocation() {
		runRealChild() // never returns; always os.Exit's
	}
	if isEchoChildInvocation() {
		runEchoChild() // never returns; always os.Exit's
	}

	ctx := context.Background()
	preSweepBestEffort(ctx)

	code := m.Run()

	if leaked := postSweepOrFail(ctx); leaked && code == 0 {
		code = 1
	}

	os.Exit(code)
}
