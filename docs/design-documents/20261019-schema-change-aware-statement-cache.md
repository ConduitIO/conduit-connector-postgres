# Destination: a schema-change-aware statement cache so `cache_statement` can be the default

> Follow-up: `20261019-destination-statement-cache-recovery.md` changes three things described
> here. A batch that cannot be encoded is retried once on a clean cache inside Write (and a
> connection pgx closed is replaced) instead of failing until a redelivery; a fired guard is
> logged at Info, not debug; and the benchi numbers below compare *unguarded* cached statements
> with `describe_exec` — the guarded default has not been measured with benchi yet.

## Summary

The destination's `default_query_exec_mode` defaults to `describe_exec`, which re-describes
every distinct statement once per batch so parameter types follow `ALTER COLUMN ... TYPE`.
That is correct but costs one extra round trip per batch — measured at about −35% throughput
at batch size 1 and −15% at batch size 1000 versus `cache_statement`
(`benchi/destination-exec-mode/results.md`). `cache_statement` recovers that speed but has
conduit-connector-postgres#367: pgx caches the parameter types Postgres inferred when the
statement was first prepared, so after `ALTER COLUMN ... TYPE` the destination keeps sending
parameters typed for the old column, and Postgres converts them by assignment cast without an
error (5/2 into a column that became `integer` is stored as 3).

This document makes the default `cache_statement` — fast — and adds a per-table schema
fingerprint guard that is checked **in-band, in the same write batch**, so the destination
keeps `cache_statement`'s single round trip per batch and stays correct under DDL. On any
column type change the guard aborts the batch's implicit transaction (rolling back the stale
writes), the statement cache is cleared, and the batch is retried with fresh parameter types.

Risk tier: 1 (destination write path). Invariant upheld: 6, schema handling never silently
mangles data.

## Context

pgx `QueryExecMode` for the destination:

| mode | round trips per batch (warm) | behavior under `ALTER COLUMN ... TYPE` |
| --- | --- | --- |
| `cache_statement` (pgx default) | 1 | reuses the cached statement's parameter OIDs; **silent assignment cast** (#367) |
| `cache_describe` | 1 | caches the description; **same silent cast** |
| `describe_exec` (today's connector default) | 2 | re-describes each statement; correct |
| `exec` / `simple_protocol` | 1 | text parameters typed by Go type, not the column; correct but breaks jsonb/arrays/maps the connector sends |

`exec`/`simple_protocol` cannot be the default: the destination relies on the server
describing the parameter type to encode values such as a JSON object (`map[string]any` →
`jsonb`), `[]byte` → `bytea`, and `big.Rat`/`jsonNumber` into the live column type. Text-mode
execution rejects ambiguous Go types and would change the value encoding. So the fast-and-safe
default must keep the extended protocol and invalidate the cached parameter types on schema
change.

The write path batches every record into one `pgx.Batch` (`SendBatch`) and runs it in a
single implicit transaction (one `Sync`). In `cache_statement` mode a warm batch is one round
trip. This is the round trip the fix has to avoid.

## Constraints

- Correct by default: after `ALTER COLUMN ... TYPE`, the very next write must store the value
  type-exactly or fail — never silently coerce, truncate or round (invariant 6).
- Keep `cache_statement`-class speed: the fix must not add a per-batch round trip.
- Backward compatible: every existing `default_query_exec_mode` value keeps its meaning; only
  the value used when the URL omits the parameter changes.
- Correct under concurrent DDL, not only across a restart: an `ALTER` racing a write batch
  must not slip between type-learning and execution.
- No new dependencies; no schema writes (no triggers, no extensions) and no extra privileges.

## Decision

Default the destination to pgx `QueryExecModeCacheStatement`, and add a **schema fingerprint
guard** that rides the write batch:

- For each table written in a batch, the destination remembers a fingerprint — the md5 of the
  table's `(attname, atttypid, atttypmod)` list from `pg_attribute`, resolved through the same
  quoted identifier the INSERT uses. A column type change, a typmod change
  (`varchar(10)`→`varchar(20)`, `numeric(12,2)`→`numeric(12,6)`) and add/drop/rename all change
  it.
- The fingerprint is learned once per table (one query on first write) and stored on the
  destination.
- Every batch then queues, **after** the writes, one guard statement per distinct table:

  ```sql
  SELECT 1 / ((<fingerprint of $1>) IS NOT DISTINCT FROM $2::text)::int
  ```

  `$1` is the quoted table name, `$2` the remembered fingerprint. The boolean "fingerprint
  matches" casts to 1 when unchanged (so the guard returns 1, no error) and to 0 on a change,
  which divides by zero (SQLSTATE 22012). The guard is a cached prepared statement, so it costs
  no extra round trip — it is one more statement in the already-pipelined batch, and the
  server-side work is an indexed `pg_attribute` lookup plus a string aggregate over a handful of
  columns.

- On a mismatch the guard raises `22012`, which aborts the batch's implicit transaction and
  rolls back every write in it (including any that already executed with stale parameter
  types). The destination detects the guard's `22012`, clears the statement cache
  (`DeallocateAll`), forgets the fingerprint, and retries the whole batch. The retry re-learns
  the fingerprint and re-prepares every statement, so the writes go out with parameter types
  matching the live schema.

Why the guard goes **after** the writes, and why that is race-free: a write takes
`RowExclusiveLock` on its table and holds it until the batch commits. `ALTER TABLE` takes
`AccessExclusiveLock`, which conflicts with `RowExclusiveLock`, so an `ALTER` either committed
before the batch began (the writes run with stale types against the new schema, and the guard
sees the new fingerprint and rolls them back) or is blocked until the batch commits (the writes
ran against the old schema, and the next batch's guard sees the change). Either way the stale
writes are rolled back before anything is acknowledged, so no coerced value is ever committed.
The write path's existing invariant — "ack only after `Close` confirms the batch committed" —
is unchanged; on a guard firing, `Close` reports the aborted transaction and the retry acks
only its own successful batch.

`exec`/`simple_protocol` stay as-is (already correct, no cache), and `describe_exec` stays
correct and available for users who want it; the guard only changes behavior when it detects a
schema change.

## Alternatives

| Alternative | Why it lost |
| --- | --- |
| (a) Re-describe every N batches / on a TTL | A window of up to N batches (or the TTL) where an `ALTER` + write still coerces. "Fast unless you ALTER" is exactly what the default must not be. |
| (b) DDL trigger + `pg_notify` invalidation | Needs an event/table trigger created at `Open` (schema write, `CREATE TRIGGER` privilege), fails on read-only replicas, and `LISTEN`/notification delivery on the write connection is racy (a notify can arrive after the stale write). Several failure modes and privileges for a default. |
| (c) Poll `pg_attribute`/`relfilenode` on an interval | Same window as (a); and `relfilenode` does not change on every `ALTER COLUMN ... TYPE`. |
| (d) `cache_describe` + a correctness guard | `cache_describe` still caches parameter OIDs, so it has #367 by itself; the guard is orthogonal and is what actually fixes it. |
| Make `exec` (text parameters) the default | Correct under DDL, but breaks the value types the destination already relies on the server describing (`map`/`[]byte`/json). A much larger, riskier re-encoding of every value. |

## Failure modes

- **Crash mid-`ALTER`**: DDL is transactional; the catalog is old or new, never half-applied.
  On restart the connection and cache are fresh and the fingerprint is re-learned.
- **`ALTER` while a batch is in flight**: serialized by the write's `RowExclusiveLock` against
  `AccessExclusiveLock` (see Decision). The stale batch either rolls back and retries, or runs
  entirely against the old schema.
- **DDL trigger absent / no privileges**: n/a — no trigger, no schema write, no extra privilege
  beyond reading `pg_attribute` (world-readable for the catalog columns used).
- **Re-describe races a concurrent write**: the guard and the writes share one implicit
  transaction and one round trip, so the type-learning (the retry's fresh prepare) and the
  write cannot be separated by a concurrent `ALTER` in the same batch.
- **The guard itself fails** (dropped table, `pg_attribute` read denied): the write fails with
  the same or a clearer error; nothing is acked, at-least-once redelivers.
- **Continuous ALTER churn**: the retry loop is bounded (three attempts); after that the write
  returns an error rather than looping forever.
- **Non-type DDL** (add/drop/rename column) also changes the fingerprint and forces one
  unnecessary rollback-and-retry; rare and harmless.
- **A value that overflows the old cached type** (a column widened, and the very next write holds
  a value that fits the new type but not the old) fails at argument encoding *before* the guard
  statement is sent, so the guard cannot fire that batch. The destination clears the statement
  cache on any failed batch, so the redelivery re-prepares with the live type and succeeds. This
  is at-least-once (one transient error at the engine level), never silent coercion.

## Upgrade / rollback

- No config, protocol, state or serialized-format change. A URL that omits
  `default_query_exec_mode` now selects `cache_statement` (guarded) instead of `describe_exec`;
  explicit `describe_exec`, `cache_statement`, `cache_describe`, `exec` and `simple_protocol`
  all keep their meaning.
- Rollback is reverting this change (the default returns to `describe_exec`); no migration is
  needed, and a running pipeline can be downgraded without a restart taking the wrong branch.

## Observability

- The `Open` warning that "the url selects a query exec mode that caches parameter types" is
  removed: cache modes are now safe.
- A schema change surfaces as one retried batch, not as an error to the pipeline; the retry is
  logged at debug. An error is returned if the retry bound is hit (the schema keeps changing
  under the writes).
- The existing `#367` regression test (write, `ALTER COLUMN ... TYPE`, write a value the new
  type cannot hold exactly) still asserts the value is never silently coerced; a new test
  asserts a widening change is stored type-exactly without a pipeline-visible error.
