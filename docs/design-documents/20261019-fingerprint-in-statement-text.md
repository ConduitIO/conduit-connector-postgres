# Destination: the schema fingerprint in the statement text (draft, prototype)

Status: **draft.** Prototype only. The default stays `describe_exec` (#436). Before cached
statements could become the default again, this needs review, the open questions below
answered, and a benchi run.

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

## Decision (prototype)

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

## Failure modes and open questions

- **ALTER between learning the fingerprint and executing.** The destination prepares the
  statement under the old fingerprint's text, but with the new parameter types, because the
  server describes it after the `ALTER`. The guard fires on this destination (the live
  fingerprint differs from the remembered one) and rolls the batch back. Another client that
  still holds the old fingerprint and gets this statement from the pooler encodes for the old
  types. Its guard fires too, because the live fingerprint is new. Both roll back. This needs a
  test with two clients behind the pooler and an `ALTER` timed in between. **Not tested.**
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
- **`cache_describe`** also passed the pooler test without the tag on the #436 head: it executes
  with unnamed statements, so there is nothing for a pooler to share. Whether it is safe
  without the tag needs its own argument before anyone relies on it.

## Upgrade / rollback

No config, protocol or state change. The statement text changes only in the cached modes, so a
rolling upgrade re-prepares statements once. Rollback is reverting.

## Observability

Unchanged from #436: WARN at Open in a cached mode, rate-limited Info lines with running
totals for guard fires and recoveries.
