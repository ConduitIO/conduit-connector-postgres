# The approving restart delivers the row that decided the drift (#338)

## Summary

When a schema-drift halt is approved by restarting, the change that decided
the drift (the "boundary row") is dropped. The marker carries that change's
own key, so the restart's resume point treats the change as already
delivered. B1 accepted this and disclosed it in the halt message (D5);
`20261008-drift-decision-on-delivery.md` repeated the disclosure and filed
this follow-up.

The marker now carries the key one below the deciding change. The unchanged
resume rule then skips exactly what was delivered before the marker and
delivers the deciding change, decoded against the approved shape. No position
field is added and no reading rule changes. The halt message no longer
discloses a dropped record, because none is dropped.

This supersedes the "disclose for v0.20" decision in
`20260829-dbz3-b1-schema-drift-escape-hatch.md` (D5) and the "the boundary DML
is skipped, as before" statement in `20261008-interleaved-tx-resume.md`. The
rest of both documents stands.

## Problem

Measured on PG 17.5 before the change, halt policy:

1. One transaction: `INSERT p1`, `ALTER TABLE t ADD COLUMN extra int`,
   `INSERT p2`, `COPY c1..c3`, `INSERT p3`.
2. Run 1 delivers p1, then the marker in p2's place (D4), then skips the rest.
   The engine acks both and the halt surfaces.
3. The approving restart resumes from the marker, whose key is p2's. Postgres
   re-sends the transaction. p1 and p2 are skipped as delivered. The restart
   delivers `c1 c2 c3 p3`. p2 is never delivered.

A worse variant: the transaction is `ALTER`, `INSERT p2` and nothing else. The
marker is the last emitted change, so its ack satisfies the flush gate
(`acked == emitted`), the slot's `confirmed_flush_lsn` moves to the server's
WAL end, past the commit, and Postgres does not re-send the transaction at
all. Fixing the key alone would not recover p2 here. The gate has to stay
closed too.

The same happens under `evolve` for a narrowing change.

## Constraints

- Invariant 3 (at-least-once): the deciding change must reach the destination.
- Invariant 1: the slot must not move past the deciding change before it is
  delivered and acked, and the halt must still arm only on the marker's ack.
- Invariant 2: no position format change without a versioned migration and an
  upgrade test. Preferred: none.
- Nothing delivered before the marker may be delivered again.
- B1's acked-gated halt, D4 skip, exactly-one-marker and FM8 contracts stay.

## Decision

1. **The marker's position carries `ChangeKey.Predecessor()` of the deciding
   change**, with the deciding change's own LSN:
   - seq > 1: `(commit LSN, seq - 1)`, the previous change of the same
     transaction.
   - seq == 1: `(commit LSN - 1, MaxUint64)`. The resume rule skips every
     transaction with a lower commit LSN and none of this one. Commit LSNs are
     distinct, so no real change sits between this key and `(commit LSN, 1)`.
     A restarted subscription has not seen the previous transaction, so its
     real last key is not available; this one needs no knowledge of it.

   `ResumePoint.Delivered` is unchanged. The result is a `Known()` key, so
   every build with the change-key resume (#331) reads it as an exact resume
   point.

2. **The flush gate is unchanged and now holds.** The marker is acked with the
   predecessor key, while the last emitted key is the deciding change's (or a
   later D4-skipped change's). `acked != emitted`, so the gate stays closed
   until the deciding change itself is delivered and acked after the restart.
   The reported flush position is the last acked record's change LSN, which is
   the deciding change's LSN, below its commit LSN, so Postgres re-sends the
   transaction (the existing argument in `20261008-interleaved-tx-resume.md`).

3. **The halt arms on the marker's position content.** The predecessor key is
   also the key of the record delivered just before the marker whenever the
   deciding change is not the first of its transaction (p1 in the example), so
   an ack's key cannot tell the two apart; arming on `key >= predecessor` would
   let p1's ack surface the halt before the marker was delivered, which is the
   #334 bug again. The handler keeps the marker's parsed position, and
   `maybeArmDriftHalt` arms when the acked position's Type, LastLSN,
   TxCommitLSN, TxSeq and SchemaHistory all equal it. SchemaHistory is what
   separates the marker (it records the new shape) from the previous record
   (older history), including when COPY rows share an LSN. The comparison is
   semantic, not byte-exact: a first version compared bytes, and a re-marshaled
   copy of the marker position (indented, keys reordered) never armed. Nothing
   is emitted after the marker (D4), so no later ack can arm instead, and the
   pipeline waited silently with the slot held. The previous rule (ack key at or
   past the deciding change's key, LSN fallback when keys are unknown) stays as a
   second condition. As a backstop, a marker still unacked after 30 seconds logs
   one warning that names the table and marker LSN and says the position must be
   acked unchanged.
4. **The halt message drops the disclosure sentence** (`haltDisclosure`). The
   error code and the revert-trap sentence are unchanged.

5. **The chaos ledger gives drift markers their own delivery identity**
   (`drift:` plus the position key), because the marker's key is now the
   previous record's key.

## Alternatives considered

- **Relax `ChangeKey.Known()` to accept `(commit, 0)`** (the option named in
  #338). Lost on three counts. `tx_seq` is `omitempty`, so `(commit, 0)`
  serializes exactly as "commit LSN without seq", which the position contract
  defines as the legacy rule; making it mean "exact, nothing delivered" changes
  how every build reads that shape. `Known()` is called on the handler's
  current key, and the key is `(commit, 0)` between a Begin message and the
  first change, so a position built in that window would silently change
  meaning. And builds that already shipped the stricter rule (the v0.14.x
  hotfix) would read the marker as legacy and re-deliver more than needed. The
  predecessor encoding stays inside the key space every reader already
  understands.
- **A new position field, "resume at this key inclusive"** (for example
  `tx_resume_inclusive`). It is the most explicit encoding, but it is a format
  change, so it needs a version and an upgrade test in both directions. It also
  does not fix the gate: the marker would be acked with the deciding change's
  key, equal to the last emitted key, so the gate would open (the variant in
  Problem) and the ack path would need to translate keys as well. Older
  builds ignore the field and silently keep dropping the row. More surface for
  the same result.
- **Re-deliver the boundary row before the marker.** Rejected in B1 D5: a
  new-shape record ahead of the marker un-makes the marker's "approval
  checkpoint" invariant. It still stands.
- **Track the previous transaction's last key for the seq-1 case.** A restarted
  subscription starts mid-stream and has not seen the previous transaction, so
  the key does not exist when it is needed. Falls back to the sentinel anyway.
- **Keep arming on keys and give the marker a distinct ack key.** Would need a
  second key space or a marker flag in the position. The byte comparison is a
  local change to one function and needs neither.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Crash before the marker is durable (FM2, FM3) | Checkpoint is below the deciding change. The restart re-decides the drift and halts again. Unchanged | 3 |
| Crash after the marker is durable, before its ack (FM1) | The restart resumes at the deciding change and delivers it once. No second halt: the marker's position already records the new shape. Kill test | 1, 3 |
| Crash after the marker's ack | Same checkpoint, same result. The slot has not passed the deciding change, so Postgres re-sends the transaction | 1, 3 |
| Crash after the restart delivered the deciding change, before its ack | The checkpoint is still the marker, so the change is delivered again. At-least-once: the one record that was in flight, as for any record | 3 |
| Repeated halts | After approval the shape is in the history, so the deciding change decides nothing. A later DDL produces a later marker at its own predecessor. Reverting the DDL halts as before (AC5) | 6 |
| The record before the marker is acked first | Its key equals the marker's. It does not arm the halt (byte comparison). Only the marker's ack does. Test acks it alone and expects NextN to block | 1 |
| Interleaved transaction at the boundary | Transactions that committed before the deciding change's were emitted earlier and acked ahead of the marker (FIFO), so they are skipped. Those that committed after were skipped by D4 in run 1 and are re-sent in full. Model test over random interleavings, including multi-row inserts and the first change of a transaction | 3, 4 |
| COPY rows sharing one LSN | Keys are `(commit, ordinal)`, so rows are distinct. Marker on row j resumes at row j. Test: deciding change followed by a COPY | 3 |
| Deciding change is the only change of the stream | Gate stays closed (acked predecessor, emitted deciding key). `confirmed_flush_lsn` stays at or below the marker's LSN. Test and kill-harness assertion | 1 |
| Deciding change is seq 1 | Sentinel key. Test | 3 |
| Unknown change key (handler driven without a subscription, tests only) | The marker has no key; the legacy rule re-delivers the whole transaction. A running subscription always stamps keys | 3 |
| Second DDL while the marker is pending (FM8) | Skipped by D4 before any decision, as before | 6 |

## Upgrade and rollback

- **No position format change.** The marker is a version 2 position with a
  `Known()` key. A test pins that its fields are the existing set.
- **Marker written by a build without this change** (nightlies after #337
  only; no release has it): read by the same rule, the deciding change is
  skipped as it always was. That is the loss the earlier halt message
  disclosed. Nothing else is lost or repeated. Upgrade test: the marker is
  rewritten to the old key and the restart delivers everything but the
  boundary row.
- **Marker written by this build, read by an older build** (rollback): the key
  is a plain exact key, so an older build with the #331 resume delivers the
  deciding change too. A build without it (v0.14.2) reads the LSN only and its
  subscription skips `WALStart <= StartLSN`, so it drops the deciding change
  exactly as before (measured: `[b1, MARKER, p3]`); the same loss as a pre-#338
  marker, not a new one.
- **DBZ-3 version 1 positions** (no change key) and **v0.14.3 hotfix
  positions** never carry a marker with this encoding. Their resume rules and
  the #337 failure-mode rows are unchanged.
- **The sentinel `tx_seq` is 18446744073709551615.** Go decodes it as a
  `uint64`. A reader that parses positions as JSON numbers in a double loses
  precision on it (the engine treats positions as opaque bytes). This is the
  only cosmetic cost of the encoding.

## Observability

The halt message loses its last sentence. Release notes state that the
approving restart now delivers the row that triggered the halt, and that a
marker written by an earlier nightly still skips it once. The ledger identity
of markers in the kill harness is `drift:<commit>/<seq>`.

## Tests

- `TestChangeKey_Predecessor`, `TestResumePoint_ResumeAtInclusive` (random
  interleavings with multi-row inserts: resume at change i delivers exactly
  i.., for every i).
- `TestDrift338_Approval` (halt and evolve, mid-transaction with COPY, first
  and only change, first with more after): the boundary is delivered once, in
  order, the stream continues, a third restart delivers nothing twice. Also
  asserts that acking the previous record does not surface the halt and that
  the slot is not past the marker.
- `TestDrift338_CrashBeforeAck`, `TestDrift338_Upgrade`,
  `TestDrift338_PositionFormat`.
- Arming: `Test_MaybeArmDriftHalt_SemanticMarkerMatch` (a re-marshaled marker
  position arms; the previous record's, same key and LSN, does not),
  `TestDrift338_ReformattedMarkerAck` (end to end),
  `Test_DriftMarker_WarnsWhenUnacked`.
- `TestDrift338_InterleavedBoundary`: another transaction open across the
  deciding change, both commit orders; `[a1]` and `[a1, b1]`.
- `TestDrift338_RedeliveredIfUnackedAfterApproval`: the boundary row is
  delivered again, once, if the process stops before its ack.
- `TestChangeKey_PredecessorOfCommitLSN1`: predecessor `{0, Max}` is not
  `Known()`, so the marker carries no key and resumes by the legacy rule.
- Kill harness: `TestB1_AC1_AC2_AC8_HaltAndWedgeRegression` and
  `TestB1_AC3_FM1_ApprovalByCrash` assert the boundary row is delivered once
  by the approving run (`b1AssertBoundaryDeliveredOnce`) and, in the first, that
  `confirmed_flush_lsn` stays at or below the marker's LSN.
  `TestB1_335_ApprovalByCrashMidTransaction` now expects p2, p3, p4.
  `TestB1_338_KillAfterMarkerAck` SIGKILLs the child after the marker's ack
  (new chaospoint `DriftMarkerAcked`), before the halt surfaces and before any
  teardown, then asserts the slot is at or below the marker's LSN and the
  restart delivers the boundary row once.

## Related

- ConduitIO/conduit-connector-postgres#338, #337, #335, #334, #331, #329.
- `20260829-dbz3-b1-schema-drift-escape-hatch.md` (D4, D5, FM1-FM8).
- `20261008-interleaved-tx-resume.md` (change key, resume point, flush gate).
- `20261008-drift-decision-on-delivery.md`.
