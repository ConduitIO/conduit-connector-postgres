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
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-connector-postgres/source/cpool"
	"github.com/jackc/pgx/v5/pgxpool"
)

// preSweepBestEffort is TestMain's (main_test.go) pre-suite half of the
// pgchaos_ sweep: best-effort defensive cleanup against a PRIOR suite run
// that crashed before its own t.Cleanup could drop its slot (e.g. the CI
// runner itself was killed). It does NOT hard-fail the whole binary on a
// connection error: "is the chaos stack up at all" is authoritatively
// answered by requireChaosStack's INFRA: t.Fatal inside an actual test
// (acceptance criterion B0.10), which gives a much more legible failure
// than TestMain aborting before a single test name has even printed.
func preSweepBestEffort(ctx context.Context) {
	pool, err := dialForSweep(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"chaos: pre-suite sweep skipped (could not connect to chaos stack on port 5434; "+
				"this is expected if `make test-chaos` hasn't started it yet - each test's own "+
				"requireChaosStack will report INFRA if it's genuinely missing): %v\n", err)
		return
	}
	defer pool.Close()

	// Same postSweepQueryTimeout bound the post-suite half uses (see its
	// doc comment): without it, this pre-suite half would run the exact
	// same listing/drop queries against the exact same "could be wedged"
	// Postgres on a bare context.Background(), just earlier in TestMain's
	// lifecycle - a hang here has no test-level timeout to save it either,
	// since it happens before m.Run() even starts.
	queryCtx, cancel := context.WithTimeout(ctx, postSweepQueryTimeout)
	defer cancel()

	names, err := listChaosObjects(queryCtx, pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "chaos: pre-suite sweep: list pgchaos_%% objects: %v\n", err)
		return
	}
	if len(names.slots) == 0 && len(names.pubs) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "chaos: pre-suite sweep: reaping %d stale slot(s) and %d stale publication(s) "+
		"left behind by a prior run\n", len(names.slots), len(names.pubs))
	if err := dropChaosObjects(queryCtx, pool, names); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: pre-suite sweep: %v\n", err)
	}
}

// postSweepQueryTimeout bounds the listing/drop queries below. dialForSweep
// only bounds the CONNECT; without a separate deadline on the queries
// themselves, a wedged Postgres (e.g. a hung lock on pg_replication_slots)
// would hang this call forever - and it runs AFTER every test has already
// passed, at the very end of TestMain, so a hang here has no test-level
// timeout to save it. chaos.yml's job-level timeout-minutes is the last
// resort if this one is somehow bypassed.
const postSweepQueryTimeout = 10 * time.Second

// chaosObjectsMayExist records whether this run could have created a
// pgchaos_ object at all. postSweepOrFail reads it to tell "nothing ever ran"
// apart from "things ran and we can no longer check them".
//
// It is set by randChaosName (names.go), NOT by requireChaosStack, and the
// difference matters. Keying on the preflight helper would mean the signal is
// correct only by convention: a future scenario that connects with
// test.ConnectPool or cpool.New directly - entirely plausible for a B0-3
// restart's second run, where the preflight already ran in run 1 - would
// create slots while leaving this false. The sweep would then dial, fail, take
// the nothing-ever-ran branch, and exit 0 with a real leak on disk. That is
// verbatim the round-3 fail-open, reintroduced by the very code that comes
// next.
//
// randChaosName is the choke-point that cannot be bypassed: every chaos slot,
// publication and table name comes from it, so no pgchaos_ object can exist
// without it having been called. requireChaosStack also sets it, which is
// redundant but free and keeps the signal true for a test that connects and
// then fails before naming anything.
//
// Package-level and atomic: TestMain's sweep runs after every test goroutine
// has finished, but the writes happen on test goroutines. Process-global, so
// under -count=N it stays set across iterations - monotonic OR is the correct
// semantics for "could anything have leaked".
var chaosObjectsMayExist atomic.Bool

// postSweepOrFail returns true if it found (and, best-effort, cleaned up) a
// leaked pgchaos_ slot or publication after the suite finished - the signal
// TestMain uses to force a non-zero exit even if every test itself passed.
//
// The rule is "never report 'no leak' unless we actually looked", and it
// takes three cases to say that honestly:
//
//   - The listing query errors on a live connection: we don't know whether a
//     leak exists. Fail.
//   - We cannot reach the server AND no test ever reached it either
//     (chaosObjectsMayExist is false - the docker-free subset, or no chaos
//     never up): nothing ran that could have created a slot, so there is
//     nothing to have leaked. Skip, without failing. This is the case the
//     Ping in dialForSweep exists to identify.
//   - We cannot reach the server BUT a test did reach it earlier: tests
//     created slots against a server we can no longer inspect. Fail.
//
// That third case is the one worth spelling out, because the obvious
// implementation gets it backwards. Treating every dial failure as "skip"
// makes the WORSE condition (server unreachable) quieter than the milder one
// (query failed), and it is reproducible: plant a leaked slot mid-run, stop
// or pause the container before the sweep, and the suite exits 0 with the
// slot still present. Today the window is narrow - main_test.go only
// upgrades a zero exit, so every test must also have passed - but B0-3/B0-4
// run longer and SIGKILL children mid-replication, which is exactly when a
// connection is most likely to fail. That correlates the fail-open with the
// very condition the sweep exists to catch.
//
// dialForSweep's Ping is what makes cases 1 and 2 distinguishable at all:
// pgxpool.NewWithConfig never dials eagerly (MinConns defaults to 0), so
// without an explicit Ping a "connection refused" only surfaced later as a
// Query error from listChaosObjects - indistinguishable from a genuine
// listing failure.
func postSweepOrFail(ctx context.Context) bool {
	pool, err := dialForSweep(ctx)
	if err != nil {
		if chaosObjectsMayExist.Load() {
			fmt.Fprintf(os.Stderr, "chaos: post-suite sweep: this run created chaos object names, but the "+
				"sweep cannot reach the server: %v - treating as a possible leak, since "+
				"pgchaos_ objects may exist on a server we can no longer inspect. Check "+
				"by hand: SELECT slot_name FROM pg_replication_slots WHERE "+
				"starts_with(slot_name, 'pgchaos_');\n", err)
			return true
		}
		fmt.Fprintf(os.Stderr, "chaos: post-suite sweep: could not connect, and this run never "+
			"created a chaos object name (stack never up, or docker-free subset), so "+
			"nothing could have leaked; skipping leak check: %v\n", err)
		return false
	}
	defer pool.Close()

	queryCtx, cancel := context.WithTimeout(ctx, postSweepQueryTimeout)
	defer cancel()

	names, err := listChaosObjects(queryCtx, pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "chaos: post-suite sweep: list pgchaos_%% objects: %v - "+
			"treating as a possible leak, since we can't rule one out\n", err)
		return true
	}
	if len(names.slots) == 0 && len(names.pubs) == 0 {
		return false
	}

	fmt.Fprintf(os.Stderr, "chaos: post-suite sweep: FOUND LEAKED chaos object(s) - a test's own "+
		"t.Cleanup should have dropped these: slots=%v publications=%v\n", names.slots, names.pubs)
	if err := dropChaosObjects(queryCtx, pool, names); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: post-suite sweep: cleanup itself failed: %v\n", err)
	}
	return true
}

// dialForSweep builds a pool AND proves it can actually reach the server.
// cpool.New (pgxpool.NewWithConfig under it) never dials eagerly - MinConns
// defaults to 0 - so it essentially never errors on its own, even when the
// chaos stack is completely down; a "connection refused" only ever used to
// surface later, on the first real Query inside listChaosObjects, which is
// exactly the fail-closed "possible leak" branch below and made "stack down"
// indistinguishable from "stack up but query failed". Ping forces the dial
// here, where a failure can be attributed correctly.
func dialForSweep(ctx context.Context) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := cpool.New(ctx, RepmgrConnString)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

type chaosObjectNames struct {
	slots []string
	pubs  []string
}

func listChaosObjects(ctx context.Context, pool *pgxpool.Pool) (chaosObjectNames, error) {
	var out chaosObjectNames

	// starts_with(), not LIKE $1 with a hand-built '%' pattern: chaosPrefix
	// ("pgchaos_") itself contains an unescaped LIKE wildcard ('_' matches
	// any single character), so "pgchaos_%" as a LIKE pattern also matches
	// e.g. "pgchaosX-anything" for any single character X, not just a
	// literal underscore. starts_with is a plain prefix comparison with no
	// wildcard semantics at all, so it can't silently over- or
	// under-match, and it needs no ESCAPE clause to reason about.
	rows, err := pool.Query(ctx, "SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, $1)", chaosPrefix)
	if err != nil {
		return out, fmt.Errorf("query pg_replication_slots: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return out, fmt.Errorf("scan slot_name: %w", err)
		}
		out.slots = append(out.slots, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("iterate pg_replication_slots: %w", err)
	}

	// See the identical starts_with() note on the slot query above.
	rows, err = pool.Query(ctx, "SELECT pubname FROM pg_publication WHERE starts_with(pubname, $1)", chaosPrefix)
	if err != nil {
		return out, fmt.Errorf("query pg_publication: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return out, fmt.Errorf("scan pubname: %w", err)
		}
		out.pubs = append(out.pubs, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("iterate pg_publication: %w", err)
	}

	return out, nil
}

func dropChaosObjects(ctx context.Context, pool *pgxpool.Pool, names chaosObjectNames) error {
	var errs []error
	for _, s := range names.slots {
		if _, err := pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", s); err != nil {
			errs = append(errs, fmt.Errorf("drop slot %q: %w", s, err))
		}
	}
	for _, p := range names.pubs {
		// #nosec G201 -- p is drawn from pg_publication itself (listChaosObjects), not external input
		if _, err := pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %q", p)); err != nil {
			errs = append(errs, fmt.Errorf("drop publication %q: %w", p, err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%d error(s) reaping chaos objects: %v", len(errs), errs)
}
