# Source: detect a replication slot that was replaced under the same name

## Summary

A pipeline that resumes from a saved position on a replication slot that was dropped and
created again under the same name skips every change between the position and the new
slot's creation, silently (#442, invariant 3). v0.17.3 (#441) refuses a slot that is
missing or invalidated. A replaced slot is neither: it exists, it is healthy, and its
`confirmed_flush_lsn` is past the saved position, which is also what a healthy slot looks
like after the connector advanced it while idle.

Postgres exposes no identity for a slot incarnation (Research, F1 to F4). The connector
therefore writes its own:

- Every position carries a **slot identity** (format version 4, `slot_identity`): a random
  secret key that never reaches the database or a log.
- The connector writes a beacon authenticated with that key (HMAC-SHA256) into the WAL as a logical decoding message in a
  transaction of its own, a **beacon** (`pg_logical_emit_message`, prefix
  `conduit.postgres.slot_identity`).
- It **never advances an idle slot past the last beacon it received back** from the
  stream. Advancing for acked records is unchanged.
- On resume, a slot whose `confirmed_flush_lsn` is at or below the saved position's commit
  LSN is accepted (it loses nothing, whichever slot it is). A slot past it is accepted only
  if it still decodes a beacon with the position's identity, which the connector finds by
  peeking the slot. Otherwise the start is refused with `postgres.source.slot_replaced`.
- The slot is checked again once the stream holds it, which closes the window between the
  check and `START_REPLICATION`.

A recreated slot starts after every beacon the pipeline wrote, so it holds none. A slot the
connector advanced itself always holds one. That is the property the design rests on, and
it holds on every Postgres version that can send messages to the connector (14 to 18,
measured) and across a Postgres 17+ failover with `synchronized_standby_slots` (measured on
17 and 18).

Positions written before v0.17.5 have no identity. The first resume after the upgrade
proves the slot by the position when it can and otherwise takes the slot over with a
`postgres.source.slot_identity_unverified` warning (Decision 6). DeVaris to confirm this
choice; the evidence for it is F7.

Tier 1. Lands on `main` as v0.17.5, on top of v0.17.4 (#445). Roadmap: Phase 0
data-integrity floor (invariant 3).

## Context

How a resume uses the slot (all on `main` at #445):

- The position is written per record. It carries the record's change key (`tx_commit_lsn`,
  `tx_seq`, #331). The engine acks a record only after its position is durable (Conduit
  #2680, "persist position before plugin ack", 2026-07-23).
- The subscription reports the last acked record's LSN as the flush position. When every
  emitted record is acked, it reports the server's WAL end from the last keepalive instead
  (`reportedPositions`, B2). The reported flush position never decreases. Postgres stores
  it as the slot's `confirmed_flush_lsn` and decodes from there after a restart.
- So after a clean stop of an idle pipeline the slot's `confirmed_flush_lsn` is routinely
  past the saved position. Measured on v0.17.3 (F7): `tx_commit_lsn` 0/966A3E0, slot
  `confirmed_flush_lsn` 0/96DE188 after one idle status update.
- #441 refuses a missing slot (`slot_wal_gap`) and an invalidated one (`slot_invalidated`).
  #445 (v0.17.4) warns when `logrepl.allowWALGap` recreates a slot, and documents two cases
  it cannot see: a slot recreated between runs, and one recreated between `checkSlot` and
  `CREATE_REPLICATION_SLOT` (the CREATE fails with a duplicate and no warning fires).

## Research

All measured with throwaway scripts against the official `postgres` images 13.23, 14.24,
15.19, 16.15, 17.11 and 18.6, and against the test stack's bitnami 17.5, unless noted.

**F1. `pg_replication_slots` has no per-incarnation identity.** Columns:

| Version | Columns beyond 13's |
| --- | --- |
| 13 | `slot_name, plugin, slot_type, datoid, database, temporary, active, active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn, wal_status, safe_wal_size` |
| 14, 15 | `two_phase` |
| 16 | `two_phase, conflicting` |
| 17 | `two_phase, inactive_since, conflicting, invalidation_reason, failover, synced` |
| 18 | `two_phase, two_phase_at, inactive_since, conflicting, invalidation_reason, failover, synced` |

None is stable for a slot's lifetime and different for its successor. There is no creation
time or OID. `inactive_since` changes at every release of the slot. `two_phase_at` is set
only for two-phase slots. `failover` and `synced` are flags a recreated slot can share,
and `synced` stays true after a promotion. The slot's on-disk state has no identifier either.

**F2. A healthy slot and a recreated one look the same.** Same scenario on every version:
create a slot, consume three inserts (saved = last change LSN), write unrelated WAL,
`CHECKPOINT`, advance the slot to the WAL end (what the connector's idle advance does),
then drop the slot, insert a row, and create it again.

| Version | Healthy slot after idle advance: `restart_lsn > saved`, `confirmed_flush_lsn > saved` | Recreated slot: same two |
| --- | --- | --- |
| 13 to 18 | true, true | true, true |

`catalog_xmin` advanced on both (PG13: 507 to 509 healthy, 511 recreated). So neither
"`restart_lsn` past the saved position" nor "`confirmed_flush_lsn` past it" separates
them. Both are false positives on a healthy idle slot.

**F3. `pg_stat_replication_slots` (14+) is not an identity.** On 14, a slot dropped and
created again under the same name still showed the old slot's counters (`total_txns` 1,
`total_bytes` 80), and a `pg_stat_reset_replication_slot` was not visible yet: the stats
collector is asynchronous. On 15 to 18 the recreated slot shows zeros and a NULL
`stats_reset`, but the counters are also reset by `pg_stat_reset_replication_slot` and lost
in crash recovery, so "counters went down" is not proof of a recreate either.

**F4. No slot comment.** `COMMENT ON REPLICATION SLOT` is a syntax error on 13 to 18.

**F5. Logical decoding messages are usable from 14, and need a flush.**
`pg_logical_emit_message` is granted to `PUBLIC` on 13 to 18 (`proacl` NULL); 17 added a
`flush` argument to the same function. pgoutput accepts the `messages` option from 14; 13
rejects it (`unrecognized pgoutput option: messages`). A non-transactional message was not
returned by a peek made right after the emission, and was after a 1.5-second wait: a
peek, like the walsender, reads only flushed WAL, and before 17 nothing flushes the
message but the WAL writer. The LSN pgoutput reports for a message is its record's end
(equal to the function's return value). A slot advanced to the WAL insert position read
just before the emission still decodes the message; one advanced to the reported end
does not. Peeking needs `REPLICATION` only (`pg_logical_slot_peek_binary_changes` has no
grant restriction).

**F6. An idle database stays idle after a beacon.** On 17, after one beacon the server
wrote one `xl_running_xacts` record (56 bytes) and then nothing for 60 seconds. A `TRUNCATE`
of an unrelated table is 848 bytes.

**F7. v0.17.3 leaves every cleanly stopped slot past the saved position.** A v0.17.3 run
that acks one record and stops after one idle status update: position
`{"version":3,"type":2,"last_lsn":"0/966A338",...,"tx_commit_lsn":"0/966A3E0","tx_seq":1}`
(kept as `source/position/testdata/v0.17.3-cdc.json`), slot `confirmed_flush_lsn`
0/96DE188. A rule "on upgrade, accept the slot only if `confirmed_flush_lsn` is at or below
the saved position, else fail" refuses this healthy pipeline, and nearly every other one.

**F8. Failover.** A 17 or 18 primary with a `failover = true` slot, a physical standby
with `sync_replication_slots = on` and `hot_standby_feedback = on`, and the standby's
physical slot in the primary's `synchronized_standby_slots`: after the pipeline idled the
slot past its position (bound 0/3041880, `confirmed_flush_lsn` 0/3045150), the standby
synced the slot, was promoted, and the resume there found the beacon (at 0/30451D8) and
delivered the row written after the stop (`TestFailover_SlotIdentitySurvivesPromotion`).
The promoted standby inherits the primary's `synchronized_standby_slots` through the base
backup; it names a slot that does not exist there, and decoding a failover slot then waits
forever (the first run of the test hung in the peek until the test timed out).

## Constraints

- No false positive on a healthy idle slot, a slot the connector advanced itself, or a
  slot carried over by a supported failover. A false positive is an outage whose only
  documented fixes are a re-snapshot or `allowWALGap`.
- No false negative when changes were lost.
- The position format is additive and versioned (`CurrentPositionVersion`); an upgrade test
  from the previous format is required.
- No new privileges beyond the defaults, nothing in the database that exposes row data,
  pipeline configuration or secrets.
- The drift escape hatch (B1, D2) needs the slot to confirm the marker's LSN after its ack.
- An idle database must stay idle (F6).

## Decision

### 1. Identity in the position

`Position.SlotIdentity` (`slot_identity`, omitempty), format version 4. It is set for the
whole run and copied onto every snapshot and CDC position, the drift marker's included
(`CDCHandler.positionAt`, `CombinedIterator`).

The identity is a **secret key**: 256 random bits, hex. It exists only in the positions,
that is in Conduit's position store (and in records' positions wherever records go). It is
never written to the database and never logged; logs carry `slot_identity_fp`, the first 8
hex digits of its SHA-256 (`internal.IdentityFingerprint`).

- A fresh start or a Debezium offset import: a new key.
- A position with an identity, on a slot that proves it: that key, unchanged.
- **Whenever the run accepts a slot that is not the position's** (`allowWALGap` with a
  missing slot it creates, a replaced slot, a slot that was gone at the check and is back,
  or one swapped in before the stream started): **a new key**, stamped into that slot at
  once (`slotIdentityCheck.rotate`). The old key never proves the new slot. Until a
  position with the new key is saved, a restart from the old position finds no beacon of
  its key and is refused again. That is what makes a half-finished attempt harmless: an
  attempt that created the slot (`CREATE_REPLICATION_SLOT` succeeded) and stamped it, then
  failed before anything was saved, leaves a healthy slot that the retry refuses with
  `slot_replaced` (`TestSlotIdentity_HalfFinishedCreate_Refused`, with an injected failure
  after the create). Before this rule the attempt stamped the slot with the position's own
  key and the retry would have passed. With `allowWALGap` still set the retry accepts the
  slot again, with the DATA LOSS warning again.
- A position without one (written before v0.17.5): a new random key too, stamped at once.
  An earlier version of this change derived the key from the slot name and the position's
  resume point, so that a run that stopped before saving anything left a beacon the next
  run could find; that key was guessable by anyone who knows the LSNs, so it was dropped.
  The cost: a run from a legacy position that saves nothing leaves the next run to adopt
  the slot again with the same warning (Decision 6).

### 2. Beacons

`internal.Beacons` reads `pg_current_wal_insert_lsn()` (L0), then writes
`pg_logical_emit_message(true, 'conduit.postgres.slot_identity', 'v=2;n=<nonce>;l=<L0>;mac=<hex>')`
in a transaction of its own with `SET LOCAL synchronous_commit TO on`, where the nonce is
128 random bits and `mac = HMAC-SHA256(key, "conduit.postgres.slot_identity/v2" ‖ slot ‖
nonce ‖ L0)`. It records the pending beacon (exact content, nonce included) before emitting
it; the stream must deliver exactly that content to move the cap.

A beacon proves the slot on resume (`verifyBeacon`) only if its MAC verifies, compared in
constant time (`hmac.Equal`), **and** its `l` is at or after the slot's
`confirmed_flush_lsn`. The second condition stops a replay: a genuine beacon of the old
slot, copied into a replaced slot by someone who can read the WAL, carries an `l` from the
old slot's history, which is before the new slot's creation and so before its
`confirmed_flush_lsn`. A genuine beacon that proves its slot always passes it, because an
idle advance stops at `Cap`, which is that beacon's `l`.

Transactional with a synchronous commit, because a non-transactional message is not
flushed before 17 (F5). The first version of this change emitted it non-transactionally:
a restart within a second missed the beacon and refused an intact slot on 14, 15 and 16
(`TestSlotIdentity_SnapshotPosition_RecreatedRefused` and
`..._NoRecordsThenProvenByBeacon` failed on those versions and passed on 17 and 18 with
`flush`). Postgres decodes a transactional message at its transaction's commit, which is
after the insert position read first.

The stream asks pgoutput for messages (`"messages" 'true'`) when beacons are in use. The
subscription takes every logical decoding message out of the stream before the handler,
so no message, ours or anyone else's, becomes a record or counts in a change key. When the
stream delivers the pending beacon, `Beacons.Cap` becomes the insert position read before
it. The stream delivers only flushed WAL, and with `synchronized_standby_slots` only WAL the
listed standbys confirmed, so a beacon that moved the cap survives a server crash and is
on a failover standby.

### 3. The idle advance stops at the cap

`reportedPositions` gains `idleCap`: when every emitted record is acked, the report goes
to `min(serverWALEnd, idleCap)` instead of `serverWALEnd`. The acked baseline is not
capped (the saved position proves the slot up to there by itself, and D2 needs it).

When an idle advance is held back by the cap and no beacon is on its way, the subscription
writes a new one in the background (`Beacons.maybeRequest`, at a status update), if either
records were acked past the cap, or the server has written at least 512 bytes since the
last received beacon. 512 bytes keeps an idle database idle (F6): our own beacon and the
`xl_running_xacts` record it causes are below it, and with nothing acked since, nothing
else asks for one. A beacon that has not come back after five minutes stops blocking the
next one. So an idle slot reaches the WAL end two status updates (about 20 seconds) after
the last ack, where before it took one.

One consequence: a pipeline that stops within those 20 seconds leaves its slot below the
last acked transaction's commit, and the next start decodes that transaction again. The
change key skips its changes, so nothing is delivered twice; this is what already happens
after a SIGKILL. It changed one chaos scenario's setup: `TestB1_AC7_RestartHaltTruthfulMessage`
needs run 2 to start past the pre-DDL relation message (FM7), which it got from the
teardown's uncapped advance. Its run 1 now waits for the slot to pass the baseline's commit
before it stops (`PGCHAOS_SETTLE_SLOT`); its assertions are unchanged.

**The bound this gives.** After any stop, crash or failover:

`confirmed_flush_lsn <= max(last acked record's LSN, Cap)`, where the last acked record's
LSN is below its transaction's commit LSN, which is at or below the saved position's
`tx_commit_lsn` (acks are FIFO and come after the position is durable), and `Cap` is at or
below the start of a durable beacon. Postgres can only lower `confirmed_flush_lsn` from
there (a crash restores the last persisted value; servers before 14.19, 15.14, 16.10 and
17.6 also let a lower report move it back), which keeps the bound.

### 4. The check on resume

`checkSlotIdentity` runs in `NewCDCIterator` after `checkSlot`, for a saved position and
outside import mode. It reads the slot (`ErrMissingSlot` is left to the existing gap
handling) and decides:

1. `confirmed_flush_lsn <= position.CoveredThrough()` (the `tx_commit_lsn`, or `last_lsn`
   for a position without a change key; nothing for a snapshot position): **covered**.
   Postgres decodes every transaction whose commit is at or after `confirmed_flush_lsn`, so
   nothing after the position is skipped, whichever slot this is.
2. Otherwise, if beacons are supported: peek the slot
   (`pg_logical_slot_peek_binary_changes(slot, NULL, n, ..., 'messages', 'true')`, never
   consuming) for a beacon that verifies, in rounds of 1024, 8192, ... changes. Each round's
   limit is clamped to 2^24 before the query (`nextFindBeaconLimit`), and the search fails
   with `ErrBeaconSearchExhausted` once a round at the clamped limit is full, so it never
   asks for more. A two-minute timeout bounds it. Found: **beacon**.
3. Not found, legacy position: **adopted** (Decision 6).
4. Not found, the position has an identity: **replaced**. Refused with
   `postgres.source.slot_replaced`, or with `allowWALGap` accepted with one DATA LOSS
   warning.
5. Beacons not supported (13, or a standby) and not covered: **unsupported**, the v0.17.4
   behaviour with a `postgres.source.slot_identity_unsupported` warning.

### 5. The window before the stream holds the slot

The check reads the slot before the subscription takes it, and nothing stops a third party
from dropping and recreating it in between. `Subscription.VerifyHeld` reads the slot again
right after `START_REPLICATION` (an active slot cannot be dropped) and before anything is
sent on the stream, like `checkImportSlot`. It refuses a slot whose `confirmed_flush_lsn`
is past what the check accepted, or, for a slot this run created, past what the creating
connection read (that connection holds the slot until the stream starts). A recreated
slot's `confirmed_flush_lsn` is past it, because the slot was created after the check.
This also closes #445's limit: with `allowWALGap`, a slot recreated after the check now
gets the DATA LOSS warning. A slot that was missing at the check and is back when the
subscription opens it is treated the same way.

When to write the run's first beacon (the stamp) matters. A stamp written into a slot that
was swapped in after the check would make that slot pass the next start's check. So a
position with an identity, on a slot this run did not create, is stamped inside
`VerifyHeld`, after the slot was accepted. Its earlier beacons cover a restart until then.
Every other start is stamped in `NewCDCIterator`: a slot this run created is held by its
creating connection, a fresh start has no position to lose, and a legacy position is
adopted either way, so a stamp there changes only the log level of the next start. Stamping
at once matters for a snapshot, which can run for hours before the stream starts. A failed
stamp fails the start with `postgres.source.slot_beacon_failed` and the `GRANT` that fixes
it.

### 6. Positions without an identity: learn on the first resume

Decision: the first resume from a position written before v0.17.5 proves the slot when it
can (covered by the position) and otherwise takes it over with one `postgres.source.slot_identity_unverified` warning, then
stamps it. From then on a replaced slot is refused.

Why not fail closed (accept only `confirmed_flush_lsn <= saved`, else refuse): F7. Every
version before v0.17.5 advances an idle slot to the WAL end, so this refuses nearly every
healthy pipeline on upgrade. Its fixes are a re-snapshot or `allowWALGap`, which skips the
checks for that start. That turns a hypothetical loss into a certain outage whose
workaround is weaker than the check it replaces. What learn-on-first-resume leaves open is
exactly the exposure that exists today: a slot that was replaced before the upgrade. It is
not silent: the warning names the slot and the saved position, and the README tells
operators who suspect a replacement to re-snapshot. **DeVaris to confirm.**

### 7. Error codes

| Code | Kind | Fix text |
| --- | --- | --- |
| `postgres.source.slot_replaced` | refusal (path `logrepl.slotName`) | If you advanced the slot yourself, start once with `allowWALGap`; otherwise re-snapshot, or accept the loss with `logrepl.allowWALGap: true` (DATA LOSS) and remove it after the resume |
| `postgres.source.slot_beacon_failed` | refusal | `GRANT EXECUTE ON FUNCTION pg_logical_emit_message(...) TO <role>` (signature by version) |
| `postgres.source.slot_identity_unverified` | warning | none; re-snapshot if a replacement is suspected |
| `postgres.source.slot_identity_unsupported` | warning | none |
| `postgres.source.slot_beacon_public` | warning | `REVOKE EXECUTE ON FUNCTION pg_logical_emit_message(...) FROM PUBLIC` and `GRANT` to the connector's role (hardening) |

`logrepl.allowWALGap` now also accepts a replaced slot. No new setting.

### Privileges and exposure

`EXECUTE` on `pg_logical_emit_message`, granted to `PUBLIC` by default, and `REPLICATION`,
which the connector already needs. No table, publication change or other object. A beacon
holds a nonce, an LSN and a MAC: no row data, no configuration, no key, nothing that leaks
the pipeline. It is visible to whoever can decode the database's WAL (roles with
`REPLICATION`, `pg_waldump`).

**Threat model.** The check must hold against accidents (a slot dropped and recreated by
hand or by a tool) and against a database role that tries to make a replaced slot pass.

| Attacker | Can they make a replaced slot pass? |
| --- | --- |
| Any role with `CONNECT` (`pg_logical_emit_message` is `PUBLIC`) | No. Without the key no valid MAC can be made (security review of the first version, which put the identity in clear: `TestFindBeacon_RefusesForgedAndCopiedBeacons`, `TestSlotIdentity_ForgedBeacons_StillRefused`). |
| A role that reads the WAL (`REPLICATION`, `pg_waldump`) and replays a genuine beacon | No. The beacon's `l` is below the new slot's `confirmed_flush_lsn` (same tests). |
| Someone with the key: read access to Conduit's position store, the connector's records' positions at the destination, or a log that printed a position | Yes. The key is as sensitive as the position store. The connector never logs it (the one log that printed a raw position on a parse failure now prints its length), and errors never carry it. Positions reaching a destination is Conduit's behaviour, outside this connector. |
| A legacy (pre-v0.17.5) position | Not by forging: there is no key to forge. Such a slot is adopted without proof on the first resume anyway (Decision 6), which is the pre-existing exposure; the first position saved after that carries a random key. |

Hardening: `REVOKE EXECUTE ON FUNCTION pg_logical_emit_message(...) FROM PUBLIC` and
`GRANT EXECUTE ... TO <connector role>`. The connector checks it at start
(`has_function_privilege('public', ...)`, which any role may call) and logs
`postgres.source.slot_beacon_public` when PUBLIC still has it. Beacons stay unforgeable
either way; the revoke keeps other roles from writing messages into the WAL at all.

## Alternatives

- **`restart_lsn` (or the creation `consistent_point`) as a lower bound: a slot whose
  `restart_lsn` is past the saved position was recreated.** Rejected: false positive on
  every healthy idle slot after a checkpoint (F2, all versions).
- **`confirmed_flush_lsn` past the saved position means recreated.** Rejected: the
  connector's own idle advance does exactly that (F2, F7). Bounding it by the saved
  position alone would need the idle advance gone, which brings back WAL retention on idle
  publications (B2's Finding H1) and, with `max_slot_wal_keep_size`, invalidated slots.
- **A column of `pg_replication_slots`, or `catalog_xmin`/`restart_lsn` combined with
  `confirmed_flush_lsn`.** Rejected: no column identifies an incarnation (F1), and the
  LSN/XID shapes of the two slots are the same (F2).
- **`pg_stat_replication_slots`.** Rejected: carried over a recreate on 14, reset by a
  function call and by crash recovery (F3).
- **A slot comment.** Not supported (F4).
- **A connector-owned table** holding (slot name, identity, highest flush position
  reported), updated before each idle advance. It works on 13 too, but it needs `CREATE`
  or a DBA-created table, write access to the source (many CDC roles are read-only), a
  table a `FOR ALL TABLES` publication would stream, and its writes have the same ordering
  problem on a failover standby that the beacon solves by waiting for the stream. Rejected
  for the privileges and the object; the beacon needs neither.
- **Non-transactional beacons.** Rejected after measurement: not flushed before 17 (F5),
  so a quick restart refused an intact slot.
- **Verify in the stream instead of peeking** (start streaming, hold records until a
  beacon or a fresh probe message arrives). Rejected: holds records in memory, entangles
  the receive loop with the check, and the probe itself would be decoded by a swapped
  slot. The peek is isolated, consumes nothing, and runs before anything starts.
- **Cap every flush report at the beacon, acked records included.** Rejected: the drift
  escape hatch needs the slot to confirm the marker's LSN at once (D2), and the saved
  position already proves acked records.
- **Fail closed on legacy positions.** Rejected, Decision 6.
- **Heartbeat records that carry the flush position into the engine's store.** Rejected:
  they reach the destination, and B2 cut heartbeats for the same reason.
- **The identity in clear in the beacon (`v=1;id=<identity>`), the first version of this
  change.** Rejected at security review: any role with `CONNECT` could write a matching
  beacon, since `pg_logical_emit_message` is `PUBLIC`, and the identity was readable from the
  WAL. Replaced by the keyed MAC and the `l` bound.
- **A MAC without the `l` bound.** Rejected: a role that reads the WAL could copy a genuine
  beacon into a replaced slot.
- **Revoking `pg_logical_emit_message` from PUBLIC as the defence.** Rejected as the
  defence (it is a cluster administrator's decision, the connector cannot rely on it, and a
  role with `REPLICATION` or the function granted still writes messages); kept as
  recommended hardening with a startup warning.
- **Keep the position's identity when `allowWALGap` accepts a new slot.** Rejected: an
  attempt that creates and stamps the slot, then fails before saving, would leave a slot
  that proves the old position on retry. A new key per accepted slot.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Slot dropped and recreated between runs | Its `confirmed_flush_lsn` is past the position and it holds no beacon: `slot_replaced` | 3 |
| Recreated after the check, before the stream | `VerifyHeld` refuses it before anything is sent; no stamp is written into it, so a retry is refused too | 3 |
| Recreated between `checkSlot` and our CREATE with `allowWALGap` | CREATE fails with a duplicate, `VerifyHeld` sees the newer slot, one DATA LOSS warning | 3 |
| Another client advances or consumes the slot | Same as a replacement: `slot_replaced` (the skipped changes were not delivered) | 3 |
| Connector killed (SIGKILL) at any point | Reports never passed the cap or the acked records, so a beacon or the position still proves the slot | 1, 3 |
| Server crash right after a beacon | The beacon is committed synchronously, and the cap moves only once the stream delivered it, so it is durable | 3, 5 |
| Server crash moves `confirmed_flush_lsn` back | Lower is always within the bound | 3 |
| Failover with `synchronized_standby_slots` | The beacon that set the cap reached the standby before the stream delivered it: resumes (F8) | 3 |
| Failover without it | The standby can lag behind the cap's beacon: the first start can be refused (fail closed, no loss) | 3 |
| Promoted standby keeps a stale `synchronized_standby_slots` | The peek blocks; after two minutes the start fails with a hint naming the setting (streaming would block too) | 7 |
| Role cannot call `pg_logical_emit_message` | `slot_beacon_failed` at start, with the `GRANT` | 3 |
| Beacon write fails at runtime | Logged; the idle slot is not advanced until one succeeds (WAL retained, no loss) | 1 |
| Beacon never comes back | After five minutes a new one may be written | 1 |
| A role writes a beacon without the key (`PUBLIC` may call `pg_logical_emit_message`) | MAC fails; the replaced slot is refused | 3 |
| A WAL reader copies a genuine beacon into a replaced slot | Its `l` is below the new slot's `confirmed_flush_lsn`; refused | 3 |
| An attempt creates (or takes over) the slot with `allowWALGap`, stamps it, and fails before saving | The slot carries a new key; the retry from the old position is refused (or accepted again with the opt-in and the warning) | 3 |
| The peek's growing rounds | Clamped to 2^24 before the query; exhaustion is decided at the clamped limit | 3 |
| A log or error carrying the identity key | None does: logs show an 8-digit fingerprint of its SHA-256, the raw-position debug log prints only its length | - |
| Stop within about 20s of the last ack | The slot is below that transaction's commit; the next start decodes it again and the change key skips it | 1, 2 |
| Very large backlog in front of the beacon | The peek reads in growing rounds; past 2^24 changes the start fails with an error (never seen: the beacon follows `confirmed_flush_lsn` closely) | 3 |
| Postgres 13, or a standby as the source | No beacons: v0.17.4 behaviour with a warning | 3 (not closed) |
| `pg_upgrade` recreates the slot | Refused; documented: advance and start once with `allowWALGap` | 3 |
| Another consumer decodes messages (Debezium) | Sees beacons; filter by prefix | - |
| Many pipelines on one database | Each slot decodes the others' beacons and ignores them | - |

## Upgrade / rollback

- **Upgrade from v0.17.4 or earlier:** Decision 6. Tested from a v0.17.3 position: adopted
  with one warning, positions become version 4 with a new random key, and a slot
  recreated after that is refused. A v0.17.3 position whose slot is not past it is proven
  without a warning. A first run that writes no record leaves the legacy position, and the
  next run adopts the slot again with the warning (never refuses it).
- **Rollback to v0.17.4 or earlier:** the format is additive; older builds read a version 4
  position and drop `slot_identity` on rewrite (`Test_SlotIdentity_Format`). Their idle
  advance ignores beacons. Coming forward again: if the older build wrote records, the
  positions are legacy again (Decision 6). If it only idled, the position still has an
  identity but the slot is past every beacon, and the start is refused with
  `slot_replaced`; start once with `allowWALGap`. Documented in the README.
- **Shared version history:** version 4 must be reserved in `release/v0.14.x`'s
  `CurrentPositionVersion` comment, as version 3 was (#419).

## Backport to release/v0.14.x

Not as is. v0.14.x has the change key but not the drift escape hatch, and it runs on
Conduit engines before #2680, which ack a record before its position is durable. There the
acked flush report can run ahead of the saved position after a Conduit crash, so the
"covered" rule does not hold and the check would refuse slots after an engine crash. A
backport would cap every flush report at the beacon (possible there, since D2 does not
exist on that branch), and so would rely on the beacon alone. It is a separate change with
its own review; until then v0.14.x keeps #444's missing-slot check and the documented limit.

## Observability

- Every resume from a position logs which proof held, at INFO: `replication slot verified as
  the slot of the saved position` with `slot`, `slot_identity_fp` (fingerprint, never the key), `confirmed_flush_lsn`,
  `saved_position_bound`, `beacon_lsn` and `proof` (`covered by the saved position` or
  `identity beacon found`).
- Warnings carry `code`: `slot_identity_unverified`, `slot_identity_unsupported`,
  `slot_replaced` (with `allowWALGap`).
- Refusals are coded `SlotError`s; `CodeOf` reads them.
- A failing runtime beacon logs `could not write a slot identity beacon`; the symptom is a
  slot whose lag grows while the pipeline is idle.
- Operators can see beacons with the peek in the README's "Delete With a Missing Key"
  section and `'messages', 'true'`.

## Testing

- Integration (`source/logrepl/slot_identity_integration_test.go`): drop and recreate
  between runs (refused, retry refused, `allowWALGap` warns once, the next start without it
  resumes); the connector's own idle advance past the position (no false positive, proof by
  beacon); recreate after the identity check (refused, retry refused); the #445 case with
  `allowWALGap` (warned); snapshot position (intact resumes, recreated refused); upgrade
  from a v0.17.3 position (adopted then enforced; covered; no records then adopted again);
  `pg_logical_emit_message` revoked (`slot_beacon_failed`, in a database of its own); a
  server without beacons (warns, resumes); forged beacons (the identity in the v1 clear
  format, a random MAC) in a replaced slot (still refused, the key in no log or error,
  `slot_beacon_public` logged on a default grant); a half-finished create (an attempt with
  `allowWALGap` creates and stamps the slot, an injected failure stops it before anything is
  saved; the retry without the opt-in is refused, with it it warns again).
- `TestCombinedIterator_ExternallyAdvancedSlot_Refused` replaces
  `TestCombinedIterator_IntactSlot_ResumesAfterIdle`, which advanced the slot with
  `pg_replication_slot_advance` to stand in for the connector's idle advance. That is now a
  different thing: another client's advance is refused, the connector's own is not.
- Internal: the beacon cap in `reportedPositions`; beacon matching, thresholds and
  staleness; `FindBeacon` at the cap and past the commit; `VerifyHeld` against a slot
  swapped in the subscription's own release-and-reacquire window; `verifyBeacon` (wrong
  key, wrong slot, `l` below the bound, tampered nonce or `l`, the v1 format, junk);
  forged and copied beacons refused by `FindBeacon`; the round limit clamp
  (`nextFindBeaconLimit`) and exhaustion at a clamped limit below the first round.
- Property: positions round-trip with the identity (`genPosition`); every CDC and marker
  position carries the run's identity.
- Failover (`test/failover/run.sh`, not part of `make test`): Postgres 17 and 18, promoted
  standby resumes, recreated slot on the new primary refused.
- Version matrix: the integration and internal tests above on 14, 15, 16, 17 (17.5 and
  17.11 in the failover stack) and 18 pass; on 13 the beacon tests skip and the unsupported
  path passes.

## Not verified

- `pg_upgrade` with logical slot migration (17 to 18) is reasoned from the Postgres
  documentation, not run.
- Failover without `synchronized_standby_slots` under standby lag: not reproduced; the
  failover test passes without lag, which proves nothing about the lagging case.
- Managed services (RDS, Cloud SQL, Azure) that may revoke `pg_logical_emit_message` or
  alter keepalives.
- The arch-v2 engine's ack ordering is taken from the code comment in `CDCIterator.Ack`,
  not re-verified for this change.
