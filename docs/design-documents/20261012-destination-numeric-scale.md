# Destination: let Postgres apply the NUMERIC scale

## Summary

The destination used to read a NUMERIC column's scale from the catalog, cache it for the life
of the process, and round `big.Rat` values to that scale itself. After
`ALTER COLUMN ... TYPE numeric(12,6)` it kept rounding to the old scale of 2, so 1/3 was
stored as `0.33` before and after the change. The lookup also ignored the table's schema.
This document replaces the lookup with sending the exact decimal and letting Postgres apply
the live column definition on assignment.

Risk tier: 1 (record payload values written to the target). Violated invariant: 6, schema
handling never silently mangles data.

## Context

`internal/db_info.go` (v0.14.3 and `main`, identical) did three things wrong:

1. `DbInfo.cache` was keyed by table name and column and never evicted, so a type change was
   invisible until the pipeline restarted.
2. `numericScaleFromDb` queried `information_schema.columns WHERE table_name = $1` with no
   schema condition and took the first row. The INSERT resolves the table through the
   connection's `search_path`, so the scale could come from another schema's table with the
   same name.
3. A NULL `numeric_scale` (unconstrained `numeric`) was mapped to 0, so 5/2 was stored as 3
   and 1/3 as 0.

## Constraints

- Backportable to `release/v0.14.x` without a protocol, config or state change.
- Correct under concurrent DDL, not only across a restart.
- Never round silently: when the value has more scale than the column, the outcome must be
  what Postgres itself does or an error.

## Decision

Format a `big.Rat` as an exact decimal string and pass it as the statement parameter.
Postgres applies the column's typmod on assignment, against the table the statement actually
resolves, at execution time: round half away from zero, or `numeric field overflow` (SQLSTATE
22003) when precision is exceeded. The destination no longer reads, caches or applies a scale.

- A rational whose denominator has only the prime factors 2 and 5 terminates and is sent
  exactly.
- Any other rational (1/3) is truncated toward zero after **1001 fractional digits**. Postgres
  allows a column scale of at most 1000, so the first dropped digit is always beyond every
  rounding boundary (a boundary for scale s has s+1 digits). A non-terminating value never
  equals a boundary, and truncation at more than s+1 digits keeps it on the same side of it,
  so the column's rounding of the truncated string equals rounding the exact value.
  `TestRatToDecimalString_RoundingEquivalence` checks this against an independent oracle.

## Alternatives

| Alternative | Why it lost |
| --- | --- |
| Keep the cache, key it by schema-qualified name, expire on a TTL | Stale for up to the TTL after DDL, which is the bug with a smaller window. |
| Re-read the scale when a write fails | A stale larger scale never fails, it rounds twice (see below). |
| Read `atttypmod` once per batch per table | Correct across restarts and batches, but costs a round trip per table per batch and still has a window between the read and the batch executing, in which a concurrent `ALTER` can land. Closing it needs an explicit transaction and a `LOCK TABLE` before the read. |
| Round client-side with a fresh lookup inside the batch's transaction | Same lock requirement, more moving parts than not rounding at all. |

Stale-scale double rounding: with the old scale 6 cached and the column now `numeric(12,2)`,
0.0049999 was rounded to `0.005000` client-side and then to `0.01` by Postgres. Postgres
rounding the exact value gives `0.00`.

## Failure modes

| Failure | Behavior |
| --- | --- |
| `ALTER COLUMN` commits between batches | Next batch uses the new typmod. No state to invalidate. |
| `ALTER COLUMN` commits while a batch is in flight | `ALTER` takes `ACCESS EXCLUSIVE`, so it waits for the batch's transaction. The batch used one consistent definition. |
| Same table name in two schemas | The statement resolves through `search_path`, so Postgres picks the table the INSERT writes. |
| Value exceeds the column's precision | Postgres raises 22003, `Write` returns the error, the batch is rolled back and not acknowledged (invariant 1). |
| Value has more scale than the column | Postgres rounds half away from zero. This is documented behavior, not a silent coercion by the connector. |
| Unconstrained `numeric` | Stores the exact value, or 1001 digits for a non-terminating rational. Previously rounded to an integer. |
| Non-numeric column | See below. |
| Crash mid-batch | Unchanged: the batch is one implicit transaction. |

## Behavior changes

Removing the lookup removes its "column is not numeric" error. Postgres and pgx now decide:

| Target column | Before | After |
| --- | --- | --- |
| `numeric(p,s)` | rounded client-side to a cached scale | rounded by Postgres to the live scale |
| unconstrained `numeric` | rounded to an integer | exact value stored |
| `integer` | error from the connector | whole values stored; fractional values rejected by Postgres |
| `text` | error from the connector | decimal string stored |
| `float8` | error from the connector | nearest double stored |
| domain over `numeric(p,s)` | worked | works |

## Upgrade and rollback

No config, protocol, position or state change. Rolling back restores the stale-scale
behavior. Pipelines that relied on the connector rejecting a decimal written to a non-numeric
column will now see it accepted for `text` and `float8`.

## Observability

Overflow surfaces as the Postgres error with SQLSTATE 22003 in the destination's write
error. There is no new metric. The cache and its catalog query are gone, which removes one
query per new column per process.
