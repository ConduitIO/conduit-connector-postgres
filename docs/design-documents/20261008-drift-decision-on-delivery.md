# Decide schema drift on the first delivered change (#335)

## Summary

B1 decided schema drift when a Relation message arrived. On a restart inside
a transaction that contains an `ALTER TABLE`, Postgres replays the
pre-ALTER Relation message, the checkpoint already records the post-ALTER
shape, and B1 read the replayed shape as a change made while the connector
was down. It halted, even under `evolve`, and the marker took the place of
the next delivered row, so that row was dropped too (the D5 boundary
disclosure), all for a schema change that never happened.

The drift decision now waits for the first change of the relation that is
actually delivered to the handler, after the resume point has skipped what
the checkpoint covers. It is made against the shape that change is decoded
with. A Relation message only caches the shape.

This amends the B1 design
(`20260829-dbz3-b1-schema-drift-escape-hatch.md`) and composes with the
change-key resume (`20261008-interleaved-tx-resume.md`).

## Problem

Measured on PG 17.5 with the change-key resume (#334):

1. One transaction: `INSERT p1`, `ALTER TABLE t ADD COLUMN extra int`,
   `INSERT p2`, COPY 3 rows, `INSERT p3`.
2. Under `evolve`, run 1 accepts the additive change at p2. Any record from
   p2 on checkpoints the history `[v1, v2]`.
3. Restart from such a checkpoint. Postgres re-sends the transaction from
   its start: `Relation(t, v1)`, p1 (skipped), `Relation(t, v2)`, p2
   (skipped), then the rest.
4. B1 classified `Relation(t, v1)` on arrival. With no in-memory prior and a
   durable last shape of v2, that is `driftAcrossRestart`, which halts under
   every policy. The next delivered row of t emitted a marker.

Under `halt` the same thing happens on the approval restart: run 1 halts at
p2 with a marker whose position records v2; the restart replays v1 and halts
a second time. Halt is the default policy, so every user who restarts in the
middle of a transaction with an ALTER hits this.

## Constraints

- Invariant 6: no data is delivered through a shape the policy did not
  accept. A real revert (B1 AC5) must still halt, and a narrowing change must
  never be accepted silently.
- Invariant 3: deferring the decision must not drop a change.
- B1's acked-gated halt, D4 skip, exactly-one-marker and FM8 contracts stay.
- No position format change.

## Decision

- `handleRelation` caches the shape and marks the relation undecided. It
  decides nothing and commits nothing.
- Each Insert, Update or Delete that reaches the handler first applies the D4
  skip, then, if its relation is undecided, decides drift for the shape it is
  decoded with (`decideDriftOnDelivery`). Changes the resume point skips
  never reach the handler, so a shape that only precedes them is never
  decided. The run that delivered those changes decided it.
- Classification compares the shape with the last durable shape:
  - same hash: `driftNone`;
  - no history: `driftInitial`;
  - this process has seen a Relation message with the durable shape's hash
    (in this run, or replayed on resume): `driftInProcess`, with the exact
    column diff. The hash covers the same column identity the diff compares,
    so any message with that hash gives the same diff;
  - otherwise: `driftAcrossRestart` (hash only, halts under every policy,
    as before).
- On an accepted decision the shape is recorded at the deciding change's LSN,
  so `FirstSeenLSN` is real from the start. The `0/0` backfill stays for
  positions written by older builds.
- On a halting decision the marker is emitted immediately, at the deciding
  change's LSN and change key, and the shape is committed to the history in
  the same step. The staging state between a Relation message and the next
  DML (`driftPending*`, the staging-time history snapshot) is gone: decision
  and emission are now one step, so there is no window to protect.
- FM8 is unchanged in effect: after a marker, every change is skipped before
  any decision, so a second shape is never committed and halts on restart.
  The `DriftVersionSkipped` chaospoint now sits in that skip path.

### Behavior that changes besides the fix

A replayed old shape can now provide the diff base. When a restart resumes
inside a transaction that used the durable shape, and the table changed
while the connector was down, the change used to be either `driftInProcess`
(if a delivered change of the old shape came first) or `driftAcrossRestart`
(if not). It is now `driftInProcess` whenever the old shape was replayed. The
diff is exact, so under `evolve` an additive change is accepted and a
narrowing one halts with the diff. A change made while down with no replay of
the old shape is still `driftAcrossRestart` and halts under every policy.

## Decisions (DeVaris, 2026-10-09)

1. **Replayed shapes are a diff base: accepted.** Under `evolve`, an additive
   change made while the connector was down is accepted when the restart
   replays the old shape, and a narrowing one halts with the exact diff. This
   is the behavior described above. The alternative (use only shapes decided
   in this run) was rejected: it would halt the checkpoint-before-ALTER
   resumes under `evolve`.
2. **The one-time false halt on DBZ-3 version 1 positions: accepted as
   documented.** The last row of the failure-mode table stands. Version 1 is a
   nightly-only format, the halt fails closed, and it happens once per upgrade
   from such a position, only inside a transaction with an ALTER. No
   carve-out.

Follow-up, not part of this change: the approving restart still drops the
boundary row (the D5 disclosure). Stopping that needs the marker to carry the
key one below the deciding change: ConduitIO/conduit-connector-postgres#338.

## Alternatives considered

- **Skip Relation messages whose transaction is at or below the checkpoint.**
  Relation messages carry no change key and arrive with `WALStart` 0, and a
  re-sent transaction mixes skipped and delivered changes. The shape before
  the first delivered change still has to be the one decoded with, so the
  cache must be updated anyway. This only moves the problem.
- **Treat a shape found earlier in the durable history as "no drift".** Would
  fix the replay but accept a real revert to an older shape silently, which
  B1 AC5 forbids (invariant 6).
- **Accept `driftAcrossRestart` under evolve.** Fixes the evolve symptom only;
  the halt-policy approval restart would still halt twice, and an unseen
  change cannot be proven additive.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Restart inside a transaction after an in-transaction ALTER | Replayed old shape is cached, never decided; the first delivered change is decided against the shape it uses: no drift | 6 |
| Real revert after approval | Revert's Relation message is followed by a delivered change, decided against the durable shape: halts (AC5) | 6 |
| Narrowing change, any resume point | Halts under evolve: with a diff when the durable shape was seen, otherwise as across-restart | 6 |
| Several Relation messages before one delivered change | One decision, for the shape the change uses, with the cumulative diff | 6 |
| Relation messages for a table whose changes are all skipped | Never decided, never committed; unrelated records do not carry the shape | 2, 6 |
| Crash after the decision, before the marker is queued (FM3) | Nothing durable claims the shape; restart decides it again and halts | 5 |
| Crash after the marker is durable, before its ack (FM1) | Restart resumes from the marker inside the transaction, replays old shapes, delivers the rest: no second halt (the #335 kill case) | 1, 3 |
| Second DDL while a marker is pending (FM8) | Change skipped by D4 before any decision; shape not committed; halts on restart | 6 |
| Changes skipped by D4 after the marker | Each still advances the subscription's emitted key without producing a record, so the flush gate stays closed after the marker's ack and the slot cannot move past the rest of the transaction. The invariant is documented at the emitted-key assignment (`internal/subscription.go`) and pinned by `TestDrift335_HaltApprovalMidTransaction` | 1, 3 |
| Change for an unknown relation | D4 skip and the undecided check do not need the relation; decoding reports the error as before | 3 |
| Resume from a position with history but no change key (DBZ-3 version 1, nightly builds only) | The legacy resume rule re-delivers the checkpointed transaction's acked prefix. If that prefix used a shape older than the checkpointed one, its first re-delivered change is decided against the newer durable shape and halts as across-restart. Under `halt` the approval restart then meets the ALTER again and halts once more. Only on the first restart after upgrading from such a position, and only when it is inside a transaction with an ALTER. Not reachable from v0.14.2 positions (no history) or from version 2 positions (exact resume) | 6 (fails closed) |

## Upgrade and rollback

- No position format change. Positions written by B1 builds are read as
  before; their `0/0` placeholders are still backfilled.
- Rolling back to a build without this change re-exposes #335: a restart
  inside a transaction with an ALTER halts once more, and the row in the
  marker's place is dropped (the D5 disclosure). A further restart approves
  it.

## Observability

Unchanged: drift is logged at warn with the table and LSN, and the marker
metadata carries the evidence. A replayed shape produces no log line. The FM8
warning now fires when the second shape reaches a delivered change rather
than when its Relation message arrives.

## Tests

- `TestChangeKey_ResendEveryPrefix` (the #333 review test) again includes the
  mid-transaction ALTER, under evolve: checkpoint after every record, restart,
  exact remainder, no marker.
- `TestDrift335_HaltApprovalMidTransaction`: the halt-policy approval restart
  inside the transaction delivers the rest, no second marker.
- `TestDrift335_DriftWhileDownMidTransaction`: a real change while down is
  still acted on (halt halts, evolve accepts additive, evolve halts on a
  drop).
- Handler-level: `Test_Drift335_*` (replay is not drift, revert still halts,
  drift while down with and without replay under both policies, exactly one
  marker).
- Kill harness: `TestB1_335_ApprovalByCrashMidTransaction` (SIGKILL after the
  marker is durable, restart inside the transaction).

## Related

- ConduitIO/conduit-connector-postgres#335, #334, #333, #329, #327.
- `20260829-dbz3-b1-schema-drift-escape-hatch.md` (D1-D8, AC5, FM1-FM8).
- `20261008-interleaved-tx-resume.md` (change key and resume point).
