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
	"os"
	"strconv"
	"strings"
)

// Environment variables forming the parent<->child re-exec protocol
// (harness plan §3). TestMain (main_test.go) checks envRealChild/envEcho to
// decide whether this process invocation is a test run at all, or one of
// the two child modes below.
const (
	// chaosEnvPrefix namespaces every var this protocol owns, so a child
	// environment can be built by exclusion rather than by listing them all -
	// a new var added below is stripped automatically. Note PGCHAOS_PARK is
	// part of the protocol too but lives in internal/chaospoint, so it is
	// covered by the prefix rather than by a constant here.
	chaosEnvPrefix = "PGCHAOS_"

	// envRealChild, when "1", routes this process invocation into
	// runRealChild (child.go): a real *postgres.Source, driven end to end
	// against the Postgres connection in envURL.
	envRealChild = "PGCHAOS_REAL_CHILD"

	// envEchoChild, when "1", routes this process invocation into
	// runEchoChild (child.go): a trivial, Postgres-free child that only
	// calls chaospoint.Reach a fixed number of times and prints progress
	// markers. It exists so this package can test its OWN spawn/wait/
	// sigkill/marker plumbing (harness_test.go) without a docker
	// dependency, and independently of whether the real Postgres-backed
	// child works at all.
	envEchoChild = "PGCHAOS_ECHO_CHILD"

	// envURL is the Postgres connection string the real child opens
	// (harness plan's RepmgrConnString-equivalent, port 5434).
	envURL = "PGCHAOS_URL"
	// envTable is the source table name.
	envTable = "PGCHAOS_TABLE"
	// envSlot is the replication slot name (crypto/rand, pgchaos_-prefixed;
	// see names.go).
	envSlot = "PGCHAOS_SLOT"
	// envPub is the publication name.
	envPub = "PGCHAOS_PUB"
	// envLedger is the path to the shared ledger file (ledger.go).
	envLedger = "PGCHAOS_LEDGER"
	// envTotal is the number of records the real child reads, ledgers and
	// acks before it tears down and exits 0 on its own - there is no
	// separate "stop" signal from the parent in this slice (no kill
	// scenarios yet); the child is self-terminating once its target count
	// is reached.
	envTotal = "PGCHAOS_TOTAL"
	// envBatchSize is sdk.batch.size, forwarded into the child's config so
	// the SDK's batching middleware (and its read-ahead goroutine) is
	// actually exercised - see doc.go's finding F-1 discussion.
	envBatchSize = "PGCHAOS_BATCH_SIZE"
	// envRun identifies which child invocation this is (1, 2, ...) - stamped
	// onto every ledger entry this run writes (LedgerEntry.Run). Always "1"
	// in this slice; forward-compatible with a later kill+restart scenario.
	envRun = "PGCHAOS_RUN"

	// envEchoReaches is the number of times the echo child calls
	// chaospoint.Reach before exiting.
	envEchoReaches = "PGCHAOS_ECHO_REACHES"

	// envHaltExpected, when "1", tells the child that a terminal schema-drift
	// halt error (postgres.schema_drift.halt, the B1 D5 coded error) is the
	// EXPECTED outcome of this run. Instead of CHILD_FATAL-ing on the read
	// error, the child prints the halt message to stderr, reports HALTED on
	// stdout, and exits 0. Set only on runs whose scenario asserts a halt; a
	// halt error on a run WITHOUT it is a hard failure, so a scenario that
	// forgets to set it fails loudly rather than passing by accident.
	envHaltExpected = "PGCHAOS_HALT_EXPECTED"

	// envParentPID is the OS PID of the process that spawned this
	// invocation via spawnChildWithEnv (harness.go), stamped onto every
	// child's environment as os.Getpid() of the parent at spawn time.
	// isRealChildInvocation/isEchoChildInvocation require this to equal
	// os.Getppid() as observed by THIS process, not merely that the
	// PGCHAOS_REAL_CHILD/PGCHAOS_ECHO_CHILD sentinel is "1" - because a
	// process's real OS parent PID cannot be forged by exporting an env
	// var, whereas the sentinels themselves can be. Without this check, a
	// developer who exports PGCHAOS_ECHO_CHILD=1 by hand to drive a child
	// directly, then reruns `go test -tags conduitchaos ./test/chaos/` in
	// the same shell without unsetting it, gets TestMain routing into
	// runEchoChild - which os.Exit(0)s before a single Go test runs - and
	// `go test` reports that exit status as `ok`, with zero `=== RUN`
	// lines: a silent false green from the one suite whose entire value is
	// that it cannot report green wrongly. A bare shell export can never
	// also happen to equal this process's actual OS parent PID, so the
	// check fails closed in exactly that case.
	envParentPID = "PGCHAOS_PARENT_PID"

	envValueTrue = "1"
)

func isRealChildInvocation() bool {
	return os.Getenv(envRealChild) == envValueTrue && hasRealParent()
}

func isEchoChildInvocation() bool {
	return os.Getenv(envEchoChild) == envValueTrue && hasRealParent()
}

// hasRealParent reports whether envParentPID, as set in THIS process's
// environment, equals this process's actual OS parent PID (os.Getppid()) -
// see envParentPID's doc comment for why that can't be forged by a plain
// shell export.
func hasRealParent() bool {
	return os.Getenv(envParentPID) == strconv.Itoa(os.Getppid())
}

// envWithoutChaosVars returns the parent environment with every PGCHAOS_ var
// removed, so a child's chaos environment is exactly what its spawner passed.
//
// Everything else is forwarded unchanged: the child is a real Go binary that
// still needs PATH, HOME, and the Go toolchain's own variables to run.
func envWithoutChaosVars() []string {
	parent := os.Environ()
	out := make([]string, 0, len(parent))
	for _, kv := range parent {
		if strings.HasPrefix(kv, chaosEnvPrefix) {
			continue
		}
		out = append(out, kv)
	}
	return out
}
