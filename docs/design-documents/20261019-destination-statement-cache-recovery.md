# Destination: `describe_exec` by default again, and recovering from a closed connection and lost prepared statements

## Summary

**The default query exec mode returns to `describe_exec`** (the #367 decision). Since v0.17.0
(#433) it was `cache_statement` with a per-table schema guard. The fresh review of this change
found that behind a pooler that shares prepared statements between clients (PgBouncer with
`max_prepared_statements > 0`), the guard is blind: a destination that connects after an
`ALTER COLUMN ... TYPE` learns a fresh fingerprint, so the guard passes, but the pooler hands
it the INSERT another client prepared before the `ALTER`, with the old parameter types. 5/2
into a column that became `integer` was stored as 3, and 5.5 into one that became `float8` as
5 (invariant 6), in every case the reviewer ran. The cached modes stay available, guarded, for
direct connections; Open warns when one is set.

The rest of this document covers the paths around the cache that the first review found:

Follow-up to `20261019-schema-change-aware-statement-cache.md` (#433), from its post-merge
review. The schema guard holds, but two paths around it left the destination failing every
batch until a restart:

1. A batch whose record k > 0 cannot be encoded into a cached parameter type (a column was
   widened and the value overflows the old type) makes pgx close the connection. The
   destination then skipped clearing the statement cache, never replaced the connection, and
   failed the same batch on every redelivery and every later Write.
2. Behind PgBouncer in transaction mode with `max_prepared_statements=0`, batches fail with
   SQLSTATE 26000 (prepared statement does not exist) and 42P05 (already exists). Once a
   server connection holds the schema-fingerprint statement from an earlier client, every
   new destination's first fingerprint read fails with 42P05.

This change makes `describe_exec` the default, replaces a closed connection inside Write, retries the batches that provably did
not commit on an empty statement cache, documents that cached statements are not supported
behind such poolers, and respects a `default_query_exec_mode` set in a service file.

Risk tier: 1 (destination write path). Invariants: 1, 3, 6.

## Context

pgx sends a batch as one pipeline: in a caching mode it first prepares new statements (its
own Sync), then queues Bind/Execute for every statement and sends one Sync. The server runs
everything up to that Sync in one implicit transaction and commits it at the Sync.

When pgx fails to encode the arguments of statement k, statements 0..k-1 are already queued.
pgconn cannot resynchronize a pipeline with unsynced requests, so `Pipeline.Close` closes the
connection (`asyncClose`). The server never saw the Sync, so it rolls the transaction back.
pgx does not invalidate the statement cache on this path. For k = 0 nothing was queued and the
connection stays open; #433 handled that case by clearing the cache (`DeallocateAll`) on any
failed batch, which it skipped on a closed connection.

26000 and 42P05 are raised in Bind/Parse. Either aborts the implicit transaction (or happens in
the prepare step, before anything ran), so nothing of the batch was committed.

## Constraints

- At-least-once (invariant 3): never ack a record that was not committed; a retry may only
  re-send a batch that provably did not commit, or redelivery semantics must already allow it.
- Atomic batches (invariant 1): the batch stays one transaction; nothing is acked on an error.
- Never silent coercion (invariant 6): a retry must re-prepare with the live column types.
- Bounded: no unbounded retry loop, and Teardown can still interrupt a Write.
- No new dependencies, no change to config, protocol or state.

## Decision

### Default mode

A URL (or service file) that does not set `default_query_exec_mode` gets `describe_exec`.
`describe_exec` describes each statement with an unnamed statement on the connection that runs
it and executes it with the described parameter types, so no named statement exists for a
pooler to share, and nothing cached can go stale. `cache_statement` and `cache_describe` are
kept when set explicitly, with the schema guard; Open logs a WARN naming the pooler risk.

The guard's race argument (#433: the writes' `RowExclusiveLock` serializes against the `ALTER`)
assumed the cached statement was prepared on the destination's own connection, under the
fingerprint it remembers. A sharing pooler breaks that assumption: the statement's parameter
types come from another client's prepare, at a time the destination never observed.

### Recovery

Write builds the statements from the records once (`planWrites`) and then makes attempts. Each
attempt first replaces a connection pgx has closed (`ensureConn`: a new connection, so empty
statement and description caches, and forgotten schema fingerprints), then builds the batch
with its guards and runs it. After a failed attempt:

| Failure | In a caching mode | Why it is safe to retry |
| --- | --- | --- |
| Schema guard fired (22012) | clear the cache, retry (as in #433) | the guard aborted the transaction |
| 26000 or 42P05 anywhere (fingerprint read, prepare, execute) | clear the cache, retry | the error aborted the transaction, or came before anything ran |
| pgx failed the batch before its Sync without a server error (encode, or a network error while preparing) | retry once on a clean cache (reconnecting if pgx closed the connection) | the server commits only at the Sync, which was never sent |
| anything else | return the error | the commit outcome may be unknown; the engine's redelivery decides |

All retries share one bound of three per Write. A connection closed by any other error (a
network error mid-batch) is not retried inside Write, because the batch may have committed; the
error goes to the engine as before, and the next Write starts on a new connection instead of
failing on the closed one.

A batch that still cannot be encoded fails with the index of the record that holds the value.
pgx reports the same error for every statement of such a batch, so the index is found on the
error path by describing each distinct statement (unnamed, nothing left on the server) and
encoding each record's arguments against it. Every error that names a record now maps the
statement index back to the record index, so skipped drift markers no longer shift it.

`explicitQueryExecMode` asks `pgconn.ParseConfig`, which keeps every setting it does not know,
`default_query_exec_mode` included, in `RuntimeParams`, wherever it was set: the URL, the
keyword/value form, or a service file selected with `service=` or `PGSERVICE`.

## Alternatives

For the default mode:

| Alternative | Why it lost |
| --- | --- |
| Keep `cache_statement` as the default and document the pooler risk | A default that converts values silently in a common deployment (PgBouncer is the usual pooler for managed Postgres) breaks invariant 6 for people who never read the docs. |
| Put the fingerprint in the SQL text (`sql + " /* fp:<md5> */"`), so a schema change changes the statement name and a pooler cannot hand out a statement prepared under another schema | Promising, and prototyped in a separate draft PR. It needs its own design note (fingerprints learned before the batch is built, one more statement per schema version on the server, interaction with `max_prepared_statements` eviction) and a benchi run before it could make a cached mode the default again. Not for a patch release. |
| Detect a pooler and pick the mode | PgBouncer does not identify itself on the wire; probing needs its admin console. Guessing silently is worse than a safe default. |

For the recovery paths:

| Alternative | Why it lost |
| --- | --- |
| Return the error and let the engine restart the connector | That is what happened: the destination was opened again only on a pipeline restart, and until then every Write failed. A restart also re-runs Open for one bad batch. |
| Retry every failed batch after reconnecting | A network error after the Sync leaves the commit outcome unknown. Retrying is at-least-once, but it hides the failure and, for keyless inserts, adds a duplicate row that a visible error would have let the operator reason about. Only the classes that provably did not commit are retried. |
| Detect transaction-mode poolers and switch to `describe_exec` | PgBouncer does not identify itself on the wire, and a probe (for example `SHOW POOLS`) needs the admin console. Guessing a mode silently is worse than documenting it. |
| Name statements uniquely per connection so 42P05 cannot collide | pgx owns the names (`stmtcache_<sha256>`); changing them means forking pgx's cache. It would not fix 26000 either. |

## Failure modes

- **Throughput**: `describe_exec` adds a round trip per batch. The one benchi run measured 35%
  fewer rows per second at batch size 1 and 16% fewer at 1000 than unguarded cached statements.
  Users on a direct connection can opt back in with `cache_statement`.
- **A user who set `cache_statement` explicitly behind PgBouncer** keeps the risk; Open warns.
- **Reconnect fails** (database down): Write returns the connect error, nothing is acked, the
  closed connection stays in place and the next Write tries again.
- **Teardown during a reconnect**: the connect uses the Write's cancelable context, so
  Teardown interrupts it; Write returns an error and acks nothing.
- **The value really does not fit**: the clean-cache retry fails the same way, and the error
  names the record. The connection pgx closed again is replaced on the next Write.
- **Index lookup fails** (the describe fails, or the schema changed again so every record
  encodes): the original error is returned without an index.
- **Pooler churn**: under PgBouncer with `max_prepared_statements=0` the retries reduce failed
  batches but do not remove them (measured with the review probe: 2 of 2000 batches still
  failed, against all 400 of 400 on main once the server connections held the fingerprint
  statement). `DEALLOCATE ALL` from one client can remove another client's statement on a
  shared server connection, which that client then retries. Not tested: a stale statement of
  the same name with different parameter types on a shared server connection; this is why the
  documentation says cached statements are not supported behind such poolers.
- **Repeated causes**: the bound of three retries per Write ends the loop; the last error is
  returned.

## Upgrade / rollback

No config, protocol, state or serialized-format change. A URL without
`default_query_exec_mode` changes from `cache_statement` (v0.17.0, v0.17.1) to `describe_exec`;
an explicit value keeps its meaning. A `default_query_exec_mode` set in a
service file is now honored (before, cache_statement replaced it); that is the documented
meaning of the setting. Rollback is reverting the change.

## Observability

- Open logs a WARN when the URL or service file selects a cached mode.
- The recovery Info lines below are rate-limited to one per kind per 30 seconds; each carries
  `suppressed_since_last`, and the running totals count every recovery.
- A fired guard is logged at Info with the table and `schema_changes_total`.
- A reconnect is logged at Info with `reconnects_total`; a 26000/42P05 retry at Info with the
  error and `stale_statement_retries_total`, and a hint to use `describe_exec` or `exec`.
- The SDK has no metrics API for connectors, so the counters are in the log lines.
- The drift-metadata warnings are logged once per Write, not once per attempt.
- A missing table fails with `postgres.destination.table_not_found` in every mode (Postgres's
  42P01 in `describe_exec` and `exec`, the fingerprint read in a cached mode); churn past the retry
  bound with `postgres.destination.schema_churn`.
