# DBZ-3 B0 — process-kill test harness for `conduit-connector-postgres`

Plan document, committed after the fact. No production code changes here. Target repo:
`ConduitIO/conduit-connector-postgres`. Unblocks DBZ-3 acceptance criteria 2, 5, 10 (= v0.20 plan
rows 7.6, 7.5, 7.7) and contributes to 7.8.

Origin: adversarial review of the v0.20 release plan, finding F2 — "ACs 7.5/7.6/7.7 assume a
SIGKILL chaos harness. No such harness exists. Multi-day work was hidden inside three
checkboxes."

## Status

This document was authored to plan B0-1 through B0-5 before any of the code existed. It is
committed here — instead of living only outside the repo — because PRs #324 (B0-1) and #325
(B0-2) cited it by section number, finding ID, and acceptance-criterion ID throughout their code
comments, and an uncommitted plan makes every one of those citations a dead end for the next
reader.

As of this commit:

- **B0-1** (`internal/chaospoint` + its three call sites, PR #324) — shipped, matches §4 below.
- **B0-2** (harness core: spawn/kill/reap plumbing, the ledger, `pgstate.go`, the no-kill smoke
  scenario, PR #325) — shipped. §0's scope line ("mechanism plus the scenarios provable today")
  undersells it slightly: B0-2 delivers the mechanism and exactly one scenario (the no-kill
  smoke test), not yet AC 2 or AC 10.
- **B0-3** (AC 2 / resumable-snapshot SIGKILL scenario) and **B0-4** (AC 10 /
  snapshot→CDC-handoff SIGKILL scenario) — not started. This is where the actual kill scenarios
  land; nothing in the repo today closes AC 2, AC 5, or AC 10.
- **B0-5** (standby-gate observation) — not started, and per §11's own "cheapest honest
  reduction" note, may fold into B2 (heartbeats) instead of shipping standalone.

See "[Where B0-1/B0-2 diverged from this plan](#where-b0-1b0-2-diverged-from-this-plan)" below
for the concrete deltas between what this document specified and what actually shipped — found
during implementation, not predicted here.

## 0. Scope

B0 builds the mechanism plus the scenarios provable today. Not heartbeats (B2), drift policy
(B1), or observability (B3). Delivers: a supervised subprocess killable at a named deterministic
point; a durable delivery ledger as the only downstream; direct slot inspection; two closed
criteria (AC 2, AC 10), each perturbation-proven; one honestly-partial criterion (AC 5); its own
fail-closed CI job.

## 1. Verified starting state

```text
Makefile:test              docker compose up --wait; go test -race ./...; compose down --volumes
test/docker-compose.yml    postgresql-repmgr:17.5.0, host port 5433
test/conf.d/postgresql.conf  wal_level=logical  max_wal_senders=5  max_replication_slots=5
                             log_statement='all'  log_duration=true
grep -rln 'SIGKILL|kill -9|os/exec|syscall' --include=*.go .   -> (empty)
grep -rn 'func TestMain' --include=*.go .                      -> (empty)
docs/                      design-documents/ only (no operations/)
```

Load-bearing code facts: `position.Position{Version,Type,Snapshots,LastLSN,SnapshotLowWatermarkLSN,
SchemaHistory}`; `MetadataSnapshotResumed = "postgres.snapshot.resumed"` (Area 1 step 3 landed, #321);
`handler.buildPosition` carries watermark + schema history forward per record; `subscription.go` holds
`walWritten`/`walFlushed`/`serverWALEnd` with `StatusTimeout` hard-coded to 10s and **no retry** in
`startReplication`; `ReadReplicationSlot` selects only 3 columns; `test.RandomIdentifier` uses a
1000-value suffix; **`source/config.go:62` declares `snapshot.fetchSize` and nothing reads it** (granularity
actually comes from `sdk.batch.size`, default 0); `.goreleaser.yml` and `publish.yml` both build with no
`-tags`, so a `conduitchaos` tag can never reach a released artifact.

## 2. Prior art and where it must differ

Reusable near-verbatim from `conduit/tests/chaos`: `spawnChildWithEnv` (re-exec `os.Args[0]`), `sigkill`,
`waitExit`, `waitForMarker`, the `TestMain` child interception, and `doc.go`'s habit of stating what the
suite does **not** prove.

| Engine chaos suite | Connector chaos suite |
| --- | --- |
| Synthetic in-process upstream, `prune` flag | **Real Postgres**, real WAL, real pruning |
| Badger; no external infra | **Docker compose**, own stack and conf |
| Kills engine `Source` + `Persister` | Kills a process running the **real connector** |
| Opaque monotone positions | Typed `position.Position` with slot LSNs |
| ~1s kill window; wall clock suffices | Kill points are **single statements** — timing cannot work |
| No state outside the process | The replication **slot** survives the crash and is the crux |

The engine suite's own `doc.go` anticipates this: a Postgres replication-slot chaos test "needs its own job".

## 3. Supervision model

**Rejected — exec the built plugin binary.** Driving it needs a v1-protocol client that lives in
`ConduitIO/conduit`. Importing the engine into the connector's test deps inverts the dependency; hand-rolling
a gRPC client puts new code between the SIGKILL and the assertion. Either way the transport joins the crash
path, and you would still have to model the engine's persist-before-ack ordering. Decisive: that ordering is
already covered in the right repo by `conduit/tests/chaos/sigkill_test.go` against `045f283`.

**Rejected — in-process "crash".** Go cannot abandon memory and fds in-process; the pool, the replication
connection and the open snapshot transaction all survive — exactly the state the test claims was destroyed.

**Chosen — re-exec the test binary as a child driver.**

```text
parent (go test)                            child (same binary, PGCHAOS_REAL_CHILD=1)
 spawnChildWithEnv ──────────────────────►  TestMain (main_test.go) intercepts -> runRealChild()
 waitForMarker("PARKED …")                    src.Open(ctx, posFromLedger)
 photograph slot + ledger (diagnostic)        loop: ReadN -> ledger.AppendSync -> src.Ack
 Process.Signal(SIGKILL) ─────────────────►
 Wait(); assert Signaled()==SIGKILL
 poll pg_replication_slots.active == false
 mutate the table (proves run 2 is fresh)
 spawnChildWithEnv ───────────────────────►  run 2 resumes from the SAME ledger file
```

(B0-2 note: the child re-exec protocol shipped with two modes, not one — `PGCHAOS_REAL_CHILD`
routes into `runRealChild`, the real-Postgres driver sketched above, and a second,
Postgres-free `PGCHAOS_ECHO_CHILD` routes into `runEchoChild`, used only by this package's own
`harness_test.go` to prove the spawn/wait/marker/sigkill plumbing independently of whether the
real connector works at all. Both are `test/chaos/child.go`, both intercepted from
`TestMain` in `main_test.go`.)

Child contract, each with an enforcement-site comment: (1) **durable before ack** — append + `fsync`, then
`Ack`, modelling the engine's post-`045f283` ordering, because acking first would make a harness-caused gap
indistinguishable from a connector bug; (2) **single-writer ack loop**; (3) markers on stdout as single
unbuffered writes, logs on stderr.

The ledger is append-only JSON lines, shared across both runs, fsynced per line, CRC-checked; it **is** the
downstream, so it is exactly what a durable destination would hold.

## 4. Injection points

Build-tagged `internal/chaospoint`: untagged production call sites call `chaospoint.Reach(name)`; the default
build's implementation is an empty function the compiler inlines away. Under `conduitchaos`, `Reach` **parks**
(prints `PARKED`, blocks forever) at the Nth reach of a named point, so the process is provably suspended at
the injection point when the signal lands.

```text
source/snapshot/fetch_worker.go   inside the rows.Next() loop, after append   -> SnapshotFetchRow
source/logrepl/combined.go        useCDCIterator(), FIRST statement            -> PreStartSubscriber
source/logrepl/internal/subscription.go  sendStandbyStatusUpdate(), after walFlushed load -> StandbyStatusUpdate
```

| Point | Criterion | Precondition asserted from artifacts before killing |
| --- | --- | --- |
| `snapshot.fetch.row` | AC 2 / 7.6 | ledger non-empty; `Type==TypeSnapshot`; `0 < LastRead < SnapshotEnd`; `wal_status='reserved'` |
| `combined.pre_start_subscriber` | AC 10 / 7.7 | `LastRead==SnapshotEnd` for **every** table; `LastLSN==""` |
| `subscription.standby_status` | AC 5 / 7.5 (partial) | child printed `written=<a> flushed=<b>` with `b < a` |

**Config note, load-bearing.** The switchover precondition is only exactly assertable with the SDK batch
middleware off: with `sdk.batch.size > 0` a `runReadN` goroutine reads ahead independently of the ack loop, so
`useCDCIterator` can be reached with records still buffered unacked. Switchover runs at `sdk.batch.size=0`
(passthrough, no goroutine); mid-fetch keeps batching on, where the buffer is realistic and cannot invalidate
the weaker precondition.

**Why parking.** It makes the kill point exact by construction — no sleeps, no read-count arithmetic, nothing
that degrades on a loaded runner. That is the flake class `conduit/tests/chaos` had to retrofit `persistDelayMS`
to remove (#2534, "observed twice in CI, never reproducible locally"). The parent still sends the signal, so
the killer is external and the parent gets a quiescent window to photograph the world. A park that never fires
is loud: `PARK_MISSED` + non-zero exit + a `waitForMarker` deadline.

What parking does **not** freeze: other goroutines (the 10s status ticker keeps running — so park→kill latency
is asserted <2s and every assertion reads post-kill state), and the parked goroutine holds a `REPEATABLE READ`
transaction and a replication connection.

Rejected alternatives: an untagged env-var injector (ships a "park forever" switch to users); test-only hook
fields threaded through four production structs (visible in `connector.yaml` forever); timing-based kills
(the project's own precedent forbids it); ptrace/delve (unportable, privileged).

## 5. State inspection

Three observers, none of which is the connector: the persisted position (decoded typed, never string-matched);
the slot, via the parent's own `cpool` connection querying `active, active_pid, restart_lsn,
confirmed_flush_lsn, wal_status, safe_wal_size, pg_current_wal_lsn()` — more than `ReadReplicationSlot`
exposes, rather than widening a production function for test reasons; and the ledger.

```text
NO GAP     : Seeded ⊆ keys(L | op="snapshot") ∧ every post-slot write appears in L
DUP BOUND  : duplicated snapshot keys ⊆ { k : k > P_kill.Snapshots[t].LastRead }
             ∧ no CDC record with LSN ≤ P_kill.LastLSN appears twice
RESUMED TAG: every run-2 snapshot line has md_resumed=="true"; no run-1 line does
CARRY-FWD  : every CDC position carries the same non-empty SnapshotLowWatermarkLSN
```

Exact set relations over data the test generated. No thresholds, no tolerances. Every wait polls a watermark
with a failing deadline. **Negative assertions carry liveness witnesses** — the point reached ≥3 times,
`pg_current_wal_lsn()` advanced, `walWritten` past the last acked LSN — and a missing witness fails the test
as inconclusive rather than passing.

## 6. Proving run 2 is a new process

(1) `Wait()` reports `Signaled() && Signal()==SIGKILL`; (2) PIDs differ and `Kill(pid1,0)` returns ESRCH;
(3) run 2 prints `RESUME_FROM <sha256>` which the parent independently recomputes from the ledger;
(4) **the parent mutates the table between runs**, and run 2 must deliver those changes. (4) is the one that
matters — 1–3 prove process identity, 4 proves the state was rebuilt from durable storage.

Not yet built: B0-2 has no run-2 scenario at all (the smoke test is a single, uninterrupted run).
Items 1–4 above are still B0-3/B0-4 work.

## 7. Docker and CI

`test/chaos/` with `doc.go`, `ledger.go`, `ledger_test.go` **untagged** (a directory whose Go files are all
excluded is a build error, and this keeps the analyzer inside `make test`), everything else under
`//go:build conduitchaos`.

Compose override, each line justified: `max_replication_slots=20` / `max_wal_senders=20` (base is 5; a killed
child leaves a slot, and exhaustion surfaces as 53400 which reads like a bug); `max_slot_wal_keep_size=2GB`
(an orphan slot cannot fill the runner disk — and because `wal_status` is asserted `reserved`, if it ever
fires the test fails as INFRA, not as a gap); `log_statement='none'` (base logs every row).

**Separate compose project, host port 5434, separate volume** — otherwise a developer's running `make test`
stack silently serves the chaos suite with `max_replication_slots=5`. Preflight asserts
`current_setting('max_replication_slots')='20'` so the wrong-stack case fails unmistakably.

CI: new `chaos.yml`, `pull_request` with **no path filter**, plus push/nightly/dispatch. No change detector
(the repo is small; a detector adds a fail-closed surface for no saving). Steps include
`go build -tags conduitchaos ./...` and `go vet -tags conduitchaos ./...` — load-bearing, because `make test`
never compiles the tagged package. Fail closed: no `continue-on-error`, no skip on missing docker (preflight
`t.Fatal`s with an `INFRA:` prefix). **No retries** — a flaky chaos test is fixed at cause. `-count=1` on PRs,
`-count=3` nightly. Required context after a one-week green soak.

Wall clock ~2.5–4 min, dominated by the standby-gate scenario's three 10s ticks. B0-2 alone
(no kill scenarios yet) runs in ~18s wall clock from a cold stack (~13s docker up, ~5s tests) —
the 2.5–4 min estimate assumed the standby-gate scenario (B0-5, not yet built).

## 8. Perturbation matrix

| Criterion | Perturbation | Must fail | Must NOT fail |
| --- | --- | --- | --- |
| AC 2 | `NewFetchWorker`: skip a window | NO GAP | — |
| AC 2 | ignore persisted `LastRead`, start at 0 | **DUP BOUND** | NO GAP |
| AC 2 | `SnapshotResumed: false` unconditionally | RESUMED TAG | NO GAP, DUP BOUND |
| AC 2 / 11 | `buildPosition` drops the watermark | CARRY-FWD | NO GAP |
| AC 10 | start CDC at `pg_current_wal_lsn()` instead of `RestartLSN` | NO GAP | — |
| AC 10 | early-return when every table is complete | **nothing — must be GREEN** | this *is* finding F-4 |
| AC 5 | delete the `walFlushed == walWritten` conjunct | slot-advance assertion | liveness witnesses |
| harness | inject a hole and an out-of-bound dup into a ledger fixture | the analyzer's unit tests | — |

Row 2 matters most: a test that only checks "no gap" is passed by a connector that re-snapshots from row 0 on
every restart — i.e. by discarding the entire resumability property. **The duplicate bound is what makes AC 2
an assertion rather than a formality.**

None of these perturbations are wired up yet — the matrix is the B0-3/B0-4 test plan, not a
report of what runs today.

## 9. Acceptance criteria for B0

| # | Criterion | Status |
| --- | --- | --- |
| B0.1 | `make test-chaos` green from a clean checkout | Met (B0-2) |
| B0.2 | Default build unchanged; both tagged and untagged `build`/`vet` green | Met (B0-1/B0-2) |
| B0.3 | No released artifact can contain the injector (`.goreleaser.yml`/`publish.yml` pass no `-tags`) | Met (B0-1) |
| B0.4 | `golangci-lint` green with `build-tags: [conduitchaos]` added | Superseded — see drift note below; met via a separate lint invocation instead |
| B0.5 | *(retired during drafting — no criterion was ever assigned this number; not the same as the B0-5 standby-gate slice in §11, which is unrelated and still pending)* | N/A |
| B0.6 | A park that is never reached FAILS — proven with an impossible `nth` | Met (B0-2, `harness_test.go`) |
| B0.7 | Run-1 termination asserted to be SIGKILL — proven by a clean-exit mutation | Met (B0-2, `harness_test.go`) |
| B0.8 | Run 2's position provably came from disk; between-run writes delivered | Not yet (B0-3/B0-4) |
| B0.9 | `grep -rn "time.Sleep" test/chaos/` matches only inside `pollUntil` | Met (B0-2) |
| B0.10 | No `t.Skip`; missing infra is an `INFRA:` `t.Fatal` | Met (B0-2, `pgstate.go`) |
| B0.11 | Zero `pgchaos_%` slots remain after a full run | Met (B0-2, `reaper.go` + `TestMain`) |
| B0.12 | The analyzer detects an injected gap and an out-of-bound dup, without docker | Met (B0-1/B0-2, `ledger_test.go`) |
| B0.13 | Perturbation evidence in the PR description | N/A until B0-3/B0-4 |
| B0.14 | *(retired during drafting — no criterion was ever assigned this number)* | N/A |
| B0.15 | `doc.go` states what is NOT covered (gRPC transport, engine persister, SDK serving) | Met (B0-2) |
| B0.16 | The PRs do **not** claim AC 5 / 7.5 is closed | Met — B0-2's `doc.go` states the scope explicitly |

## 10. How the harness can lie

| The lie | Why plausible here | Mitigation |
| --- | --- | --- |
| A leaked slot makes run 2 resume stale | `CreateSubscription` **tolerates** an existing slot (42710 → warn and continue) | `crypto/rand` slot names; `TestMain` reaps `pgchaos_%` before and after; per-scenario non-existence assertion |
| Slot exhaustion read as a connector bug | base conf allows 5 | 20 in chaos conf; preflight headroom assertion → `INFRA:` |
| Kill lands after the transaction committed | classic timing chaos | parking; plus a distinct `PRECONDITION:` failure class |
| Slot invalidation read as data loss | this is *the* real mechanism by which a slot loses data, and it looks identical | every gap claim guarded by `wal_status='reserved'`; otherwise `INFRA:` with the slot row printed |
| `55006 slot is active` on restart | the walsender exits asynchronously and `startReplication` has no retry | poll `active=false` before spawning run 2 |
| The ledger creates or hides the gap | it is the only downstream | fsync-before-ack; analyzer unit tests; a no-kill smoke scenario; the perturbation matrix |
| Run 2 silently full-snapshots and still reports "no gap" | **the most dangerous false green** | three guards: `RESUME_FROM` hash, resumed-tag, snapshot dup bound |
| Park never fires, child completes, reads green | a refactor moves the call site | `PARK_MISSED` + non-zero exit + marker deadline + `COUNTS` |

## 11. Sizing

| PR | Contents | Build | Tier |
| --- | --- | --- | --- |
| B0-1 | `internal/chaospoint` + 3 call sites + lint build-tags + CI both-variant build/vet | 1 d | **Tier 1** (edits `source/`, no-op or not) |
| B0-2 | Harness core + compose + make target + workflow + **the no-kill smoke scenario** | 3–4 d | Tier 2, reviewed at Tier-1 depth |
| B0-3 | AC 2 scenario + 4 perturbation proofs | 1.5 d | Tier 2 |
| B0-4 | AC 10 scenario + 2 perturbation proofs | 1 d | Tier 2 |
| B0-5 | Standby-gate observation + ack-withholding knob | 1.5 d | **Tier 1** if `StatusTimeout` becomes injectable |

Build 8–9 days; **calendar 2.5–3 weeks** with serial review. B0-2 is the critical path and harnesses always
bounce once. Cheapest honest reduction: fold B0-5 into B2, where the heartbeat it gates actually lands.
Dropping B0-3 or B0-4 does not work — those are the criteria B0 exists for.

## 12. Adversarial review

| # | Sev | Finding | Resolution |
| --- | --- | --- | --- |
| F-1 | **High** | Only `ParseConfig`+`Open`+`ReadN(ctx,1)` is proven to work outside `sdk.Serve`. `Ack`, the batch middleware's goroutine, and the schema middleware over a long run are unverified | The no-kill smoke scenario ships **in B0-2, before any kill scenario** — if it can't go green the supervision model is wrong on day 3, not day 8 |
| F-2 | **High** | The standby point is the flakiest AND B0 cannot close AC 5 (heartbeats are B2); `StatusTimeout` is a hard-coded 10s | Ship the honest weaker scenario; B0.16 makes not-over-claiming checkable; B0-5 is first to cut |
| F-3 | **High** | **Spec bug in the design doc.** AC 10's "no duplicate" is unachievable with concurrent writes: at the boundary `LastLSN==""` → `startLSN = RestartLSN` (unadvanced) → run 2 replays all WAL since slot creation | Quiescent-snapshot variant asserts "no duplicate" exactly; a second variant asserts the bound. **Report to the design-doc owner as a wording bug, do not absorb silently** |
| F-4 | Medium | The AC-10 test does not pin the empty-cursor short-circuit — removing it is also correct. It pins LSN continuity | Matrix says that perturbation must be GREEN; recorded as a finding, plus a counter assertion so the path is at least witnessed |
| F-5 | Medium | Tagged files mean `make test` never compiles the chaos package | Always-run tagged build/vet in CI; untagged `doc.go`/`ledger.go` |
| F-6 | Medium | The harness models the engine's persist-before-ack ordering and can drift from it (it changed once, `045f283`) | Enforcement-site comment naming the engine file; `doc.go` states it is a model, not the engine |
| F-7 | Medium | Parking holds a `REPEATABLE READ` tx and a replication conn, perturbing the server | Park→kill latency asserted <2s |
| F-8 | Medium | `test.RandomIdentifier` has 1000 values — unsafe under nightly `-count=3` | `crypto/rand` names; the suite never calls it |
| F-9 | Low | `snapshot.fetchSize` is declared and never read; granularity is `sdk.batch.size` | Plan written against the real knob; file the dead config key as its own bug |
| F-10 | Low | A shared port silently downgrades the stack | Separate project, port 5434, preflight assertion |
| F-11 | Low | A precondition could degrade to trivial | `PRECONDITION:` failure class; seed size derived from the park index |
| F-13 | Medium | The batch middleware's read-ahead decouples `ReadN` from acking | Switchover runs at `sdk.batch.size=0` |

Assumed-but-absent: `.golangci.yml` has no `build-tags` (tagged files unlinted today); `docs/operations/`
does not exist (B3 must create it); `ReadReplicationSlot` returns only 3 columns; there is no `TestMain`,
`os/exec` or `syscall` anywhere in the repo — every line of process supervision is new code, which is why
B0-2 is 3–4 days and not one.

## Where B0-1/B0-2 diverged from this plan

Found during implementation, not predicted here:

- **§9's B0.4 approach was superseded.** This plan proposed adding `build-tags: [conduitchaos]`
  to `.golangci.yml`. What actually shipped is a separate `chaos-build` lint invocation in
  `.github/workflows/test.yml` (`golangci-lint run --build-tags conduitchaos`), because
  golangci-lint v2 merges config `build-tags` additively — adding it to the config would have
  made the *default* `golangci-lint run` (the one `make lint` and the main lint job already
  invoke) analyze the tagged variant and silently stop analyzing the untagged one. Verified
  empirically during B0-2: a deliberate `declared and not used` in the untagged file went
  unreported under the config-based approach, while the same bait in the tagged file was caught.
  Both variants are linted today; the mechanism is a second CI step, not a config change.
- **§7's compose sketch didn't account for the base image's naming requirement.** Bitnami's
  `postgresql-repmgr` image requires `REPMGR_NODE_NAME` to match `^.*+-[0-9]+$` and refuses to
  start otherwise. The chaos-stack service is named `pg-chaos-0`, matching the base stack's
  `pg-0` convention (`test/docker-compose.yml`).
- **Not in this plan at all: an Avro encoding bug.** The connector's Avro schema extraction
  emits a non-nullable field type for a nullable Postgres column (root cause: `test/helper.go`'s
  `TestTableAvroSchemaV1` declares `column4` — nullable `numeric(16,3)` in the table DDL — as a
  bare bytes/decimal type, not a nullable union), which breaks encoding the instant a seeded
  `NULL` reaches that column. `child.go` runs the smoke scenario with
  `logrepl.withAvroSchema=false` to route around this rather than debug an unrelated pre-existing
  encoding bug inside a harness PR. Filed as
  [ConduitIO/conduit-connector-postgres#326](https://github.com/ConduitIO/conduit-connector-postgres/issues/326);
  `child.go` links it with a `TODO(#326)`. This matters beyond the harness: `WithAvroSchema`
  defaults to `true` (`source/config.go`), so this is the connector's shipped default failing
  against its own standard test table, and DBZ-3 Area 2 is specifically about schema behavior
  across a restart — B0-3/B0-4 need to make an explicit call on the workaround rather than
  silently inherit it.
- **§2/§3's `ReadReplicationSlot` split, `crypto/rand` naming, and slot/publication
  auto-creation inside `Source.Open`** (not a separate lifecycle hook) all checked out as
  described; no changes needed there.
- **The env var and process names in §3's diagram were illustrative, not literal.** The shipped
  protocol uses `PGCHAOS_REAL_CHILD`/`PGCHAOS_ECHO_CHILD` (not a single `PGCHAOS_CHILD`) and
  `runRealChild`/`runEchoChild` (not a single `runChild`), reflecting the two-child-mode split
  described in §3's B0-2 note above.

## Related

- `internal/chaospoint` and its three call sites — B0-1,
  [PR #324](https://github.com/ConduitIO/conduit-connector-postgres/pull/324).
- Harness core, compose stack, `test-chaos` Make target, `chaos.yml`, the no-kill smoke test —
  B0-2, [PR #325](https://github.com/ConduitIO/conduit-connector-postgres/pull/325).
- [`docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`](20260724-dbz3-postgres-cdc-parity.md)
  — the DBZ-3 design doc this harness proves properties against (Area 1 resumable snapshot, Area
  2 schema evolution).
- [ConduitIO/conduit-connector-postgres#326](https://github.com/ConduitIO/conduit-connector-postgres/issues/326)
  — the Avro nullable-column bug found and worked around while building the B0-2 smoke test.
