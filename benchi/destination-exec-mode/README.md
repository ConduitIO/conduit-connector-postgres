# benchi: destination query exec mode (#367)

Measures what the #367 fix costs: the Postgres destination switching pgx from
its default cached-statement mode to `QueryExecModeDescribeExec`, which
describes each statement before it runs so that parameter types follow
`ALTER COLUMN ... TYPE` instead of a stale cache.

## Method

The method follows Conduit's `benchi/METHODOLOGY.md`:

- **Ground truth, not engine metrics.** The `pre-test` and `post-test` hooks
  count rows in the target table. Throughput is the row difference divided by
  the 60 s test duration. The pre-test hook waits until the pipeline writes,
  then 15 s more, before it counts, so pipeline startup and warmup fall
  outside the window.
- **An A/A control.** `pg-cache-aa` is the base commit built a second time.
  With `-trimpath` the two builds are byte-identical (`binaries.sha256`), so
  the spread between those two arms is run-to-run noise alone: the noise
  floor. An A/B difference smaller than it means nothing.
- **Alternating single runs.** Each run is one benchi invocation with fresh
  infrastructure. Each round runs the three arms in a rotated order, so
  drift during the session lands on every arm. One warmup run per batch size
  is discarded.
- **Five arms.** `pg-cache` (base, cached statements), `pg-cache-aa` (base built again: the A/A control), `pg-describe` (option 1), `pg-optout` (option 2: describe_exec by default, run here with an explicit `default_query_exec_mode=cache_statement` URL, so it should measure like `pg-cache`) and `pg-guarded` (the default since v0.17.0: the default URL selects cache_statement with the per-table schema guard of #433, one extra pipelined statement per table per batch). Rounds rotate the order as a Latin square; five rounds put every arm in every position once.
- **The guarded arm is not measured yet.** The 2026-10-15 run of record had the first four arms only. Until a run with `pg-guarded` is in `results.md`, the cost of the schema guard is unmeasured: the numbers there compare unguarded cached statements with describe_exec.
- **Two batch sizes.** `describe_exec` adds a round trip per distinct
  statement per batch. With `sdk.batch.size: 1` (the default) that is a
  round trip per record, the worst case. With 1000 it is one per thousand
  records.

The pipeline is a generator source (structured `id`, `name`, unlimited rate)
→ a `field.set` processor that makes `.Key` `{"id": ...}` → the standalone
`postgres` destination, which upserts into `bench(id bigint PRIMARY KEY,
name text)`. The connector runs as a standalone plugin in
`ghcr.io/conduitio/conduit:v0.20.2`, so the only difference between the arms
is the connector binary. Postgres 17 keeps its data on tmpfs: the same for
every arm, and no volume is left behind.

No host port is bound. Every container has a fixed `pgbench367-` name, and
`run.sh` refuses to start while a `benchi` network exists, because benchi
removes that network when it exits.

## Running it on EC2

The numbers come from a dedicated instance, not a shared laptop. `aws/` holds
the launcher, after the pattern of Conduit's `benchi/archv2-gate` kit
(ConduitIO/conduit#3067):

```sh
cd benchi/destination-exec-mode/aws
go run . launch -manifest m.json -account <id> -base-sha <sha> -fix-sha <sha> -optout-sha <sha> -guard-sha <sha>
go run . status -manifest m.json          # until status.txt says DONE
go run . fetch -manifest m.json -dir ../results/<run>
go run . cleanup -manifest m.json -delete-bucket
go run . verify -manifest m.json          # every resource NotFound or terminated
```

The launcher checks the account before it creates anything. It tags every
resource with `conduit-bench=<tag>`, puts no credential in user data, and
creates the instance with `InstanceInitiatedShutdownBehavior=terminate`.
The instance schedules its own halt at `-max-minutes` before anything else
and halts when the run ends. The instance clones this repository, checks out
`-guard-sha` (which carries this five-arm kit), and runs `run.sh` with the four
commits.

## Running it locally

```sh
go install github.com/conduitio/benchi/cmd/benchi@v0.4.1   # or set BENCHI
benchi/destination-exec-mode/run.sh 5     # 5 rounds; about 80 minutes
```

`run.sh` builds `bin/main` and `bin/main2` from `BASE_REF` (default `bd44a41`,
main before #367), `bin/describe` from `FIX_REF` (default `HEAD`),
`bin/optout` from `OPTOUT_REF` (default `FIX_REF`) and `bin/guarded` from
`GUARD_REF` (default `HEAD`; any commit from v0.17.0 on). It
writes `runs.csv` (one row per run with the before/after counts), the
binaries' checksums, and each run's benchi output under `results/`. Run it
on an otherwise idle machine; the uptime before and after each run is
recorded so that load can be checked.

## Results

See `results.md`. The file records the machine, the host load, the medians
and the A/A spread, and it states only what the A/A floor allows.
