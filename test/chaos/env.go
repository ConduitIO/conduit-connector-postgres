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

import "os"

// Environment variables forming the parent<->child re-exec protocol
// (harness plan §3.3). TestMain (main_test.go) checks envRealChild/envEcho to
// decide whether this process invocation is a test run at all, or one of
// the two child modes below.
const (
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

	envValueTrue = "1"
)

func isRealChildInvocation() bool { return os.Getenv(envRealChild) == envValueTrue }
func isEchoChildInvocation() bool { return os.Getenv(envEchoChild) == envValueTrue }
