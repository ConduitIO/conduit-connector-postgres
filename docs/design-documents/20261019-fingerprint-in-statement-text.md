# Destination: restoring cached-mode performance without the pooler risk

Status: **proposal for v0.17.3.** The default stays `describe_exec` (#436) in this change. It
compares two candidates to replace it, guarded `cache_describe` and `cache_statement` with the
fingerprint in the statement text, and recommends one from the benchmark below.

## Summary

In a cached query exec mode the destination appends the target table's schema fingerprint to
every write's SQL text, as a comment: `INSERT ... /* fp:<md5> */`. pgx names a cached statement
after its SQL text (`stmtcache_` plus a hash), and PgBouncer with `max_prepared_statements > 0`
shares prepared statements between clients by that text. So a statement prepared under one
column layout can no longer be handed out under another. The fingerprints are learned before
the batch is built, not after the writes are queued. The in-batch guard from #433 stays.

Risk tier: 1. Invariant: 6.

## Context

#436 found that behind PgBouncer with prepared-statement tracking, a destination that connects
after `ALTER COLUMN ... TYPE` learns a fresh fingerprint, so its guard passes, but it runs an
INSERT another client prepared before the `ALTER`. 5/2 into a column that became integer was
stored as 3. The guard compares the table with what this destination remembers. It cannot see
the parameter types of a statement this destination did not prepare.

If the statement's identity includes the fingerprint, a statement with the old parameter types
has the old fingerprint in its text, and a destination that learned the new fingerprint asks
for a different statement.

## Constraints

- No silent conversion behind any pooler that keys statements by text or name (invariant 6).
- Keep one round trip per warm batch, which is the reason to want a cached mode at all.
- Nothing changes in describe_exec, exec or simple_protocol.

## The two candidates

**Guarded `cache_describe`.** pgx caches the statement description (parameter and result
types) on the client and runs every statement unnamed, with the cached parameter OIDs. No
named prepared statement exists, so a pooler has nothing to share. The schema guard (#433)
protects the description cache on a running connection. After a guard fires, nothing is sent
to the server: pgx invalidates the description of every statement in a failed batch
(`invalidateCachesOnBatchResultsError`), so the retry describes again. Before this change the
destination sent `DEALLOCATE ALL` there, which behind a shared server connection removed other
clients' statements. Where pgx does not invalidate (a batch that was never sent), the
destination drops its caches by reconnecting.

**`cache_statement` with the fingerprint in the statement text.** One round trip per warm batch
(no describe at all), but named statements; it is only as safe as the fingerprint is unique
per column layout. That is what the rest of this section is about.

## Decision (cache_statement candidate)

`buildBatch` in a cached mode:

1. Learns the fingerprint of every table in the batch that it does not know yet (one query per
   new table, as before; before, this ran after the writes were queued, which made no
   difference to I/O).
2. Queues each write as `statementText(sql, fingerprint)`, with one tagged string per distinct
   statement per batch.
3. Queues the guards after the writes, as before.

`TestDestination_Write_PoolerSharedStaleStatement` runs `cache_statement` and
`cache_describe` behind the test PgBouncer and passes. On the #436 head, without the tag,
`cache_statement` fails it: 5/2 is stored as 3, and 5.5 as 5.

## Alternatives

| Alternative | Why it lost or is open |
| --- | --- |
| `describe_exec` by default (#436) | Correct everywhere; costs a round trip per batch (one benchi run: −35% at batch 1, −16% at 1000, against unguarded cached statements). It stays the default unless this proves out. |
| Unique statement names per connection | pgx owns the names; this means forking or wrapping its statement cache. A pooler that shares by text would still share. |
| Detect poolers | PgBouncer does not identify itself on the wire. |

### The pg_attribute row version in the fingerprint

The fingerprint is the md5 of (name, type OID, typmod, **xmin**) per column. Without xmin a
schema that changes and changes back (ABA) returns to its old fingerprint. The review showed
the effect: a client learns fp0 (numeric), the column becomes integer, the client prepares the
fp0-tagged INSERT (so its parameter is int4) and dies without deallocating it, the column goes
back to numeric, and new clients ask for the fp0-tagged statement and store 2.5 as 2.00.
Every ALTER writes a new version of the column's pg_attribute row, so with xmin the
fingerprint after the ABA is one no client has prepared under.
`TestDestination_Write_FingerprintABAKilledPreparer` (CI PgBouncer, one-connection pool) fails
without xmin (stored "2.00") and passes with it.

## Failure modes and open questions

- **ALTER between learning the fingerprint and executing.** The destination prepares the
  statement under the old fingerprint's text, but with the new parameter types, because the
  server describes it after the `ALTER`. The guard fires on this destination (the live
  fingerprint differs from the remembered one) and rolls the batch back. Another client that
  still holds the old fingerprint and gets this statement from the pooler encodes for the old
  types. Its guard fires too, because the live fingerprint is new. Both roll back.
  `TestDestination_Write_FingerprintAlterBetweenLearnAndPrepare` runs the `ALTER` from a dial
  hook at the moment the destination sends the INSERT's Parse, with and without a revert, and
  later clients on the same server connection store exactly. It passes with and without xmin:
  the destination's `DEALLOCATE ALL` after its guard fires removes the poisoned statement. The
  killed-preparer test above is the case where nothing removes it.
- **Fingerprint collisions across type changes.** The fingerprint covers (name, type OID,
  typmod) per column. A change that keeps those but changes encoding (a domain's base type,
  an enum's labels) does not change it. That is the same exposure as #433's guard. **Not
  tested behind a pooler.**
- **Statement count.** Each schema version leaves one more prepared statement per shape on the
  server, until pgx's LRU (512) or PgBouncer's `max_prepared_statements` evicts it. Eviction
  re-prepares; it does not convert values.
- **Cost.** One tagged string per distinct statement per batch, and the fingerprint lookups
  happen before the batch is built. Unmeasured: needs the benchi `pg-guarded` arm run against
  this branch.
- **xmin wraparound.** xmin is a 32-bit transaction id. Two schema versions of one table would
  need the same xmin on every column row to collide, which needs 2^32 transactions between them
  and freezing to keep the raw value; not a practical concern, but not impossible.
- **`cache_describe`** is safe behind poolers by construction (above). The fingerprint check
  covers its own description cache.

## Upgrade / rollback

No config, protocol or state change. The statement text changes only in the cached modes, so a
rolling upgrade re-prepares statements once. Rollback is reverting.

## Observability

Unchanged from #436: WARN at Open in a cached mode, rate-limited Info lines with running
totals for guard fires and recoveries.
