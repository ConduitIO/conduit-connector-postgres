# benchi: destination query exec mode (#367)

Measures what the #367 fix costs: the Postgres destination switching pgx from
its default cached-statement mode to `QueryExecModeDescribeExec`, which
describes each statement before it runs so that parameter types follow
`ALTER COLUMN ... TYPE` instead of a stale cache.

## Method

The method follows Conduit's `benchi/METHODOLOGY.md`:

- **Ground truth, not engine metrics.** The `pre-test` and `post-test` hooks
  count rows in the target table. Throughput is the row difference divided by
  the 60 s test duration. Pipeline startup falls outside the window.
- **An A/A control.** `pg-cache-aa` is a second, separately built copy of the
  same base commit as `pg-cache`. The spread between those two arms is the
  noise floor, and an A/B difference smaller than it means nothing.
- **Alternating single runs.** Each run is one benchi invocation with fresh
  infrastructure. Each round runs the three arms in a rotated order, so
  drift during the session lands on every arm. One warmup run per batch size
  is discarded.
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

## Running it

```sh
go install github.com/conduitio/benchi/cmd/benchi@v0.4.1   # or set BENCHI
benchi/destination-exec-mode/run.sh 4     # 4 rounds; about 40 minutes
```

`run.sh` builds `bin/main` and `bin/main2` from `BASE_REF` (default `bd44a41`,
main before #367) and `bin/describe` from `FIX_REF` (default `HEAD`). It
writes `runs.csv` (one row per run with the before/after counts), the
binaries' checksums, and each run's benchi output under `results/`. Run it
on an otherwise idle machine; the uptime before and after each run is
recorded so that load can be checked.

## Results

See `results.md`. The file records the machine, the host load, the medians
and the A/A spread, and it states only what the A/A floor allows.
