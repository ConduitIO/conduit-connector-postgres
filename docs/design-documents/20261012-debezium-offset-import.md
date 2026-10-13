# Resume from a Debezium offset (offset import)

## Summary

A team moving from Debezium Postgres on Kafka Connect to this connector should
not have to re-snapshot. This design lets the connector start from Debezium's
last committed offset, with no loss and with a stated bound on duplicates.

- The connector takes Debezium's raw offset in a new setting,
  `logrepl.import.debeziumOffset`. It applies the setting only when the
  pipeline has no position yet. `conduit migrate kafka-connect` reads the offset
  from Kafka Connect and writes the setting. It also gives a dry run that
  prints the computed resume point.
- **No new position format.** The import becomes a resume point that the
  connector already understands: skip every transaction whose commit LSN is
  below Debezium's `lsn_commit`. One extra in-memory rule skips the part of
  the next transaction that Debezium already delivered. The first acked record
  writes an ordinary version 2 position, and from then on the import plays no
  part.
- **Two slot strategies.** _Fresh_ (the recommended default): create a Conduit
  slot while Debezium is still running, wait until Debezium's committed offset
  passes the new slot's start, then cut over. This works with every Debezium
  decoding plugin and leaves Debezium's slot alone, so rollback is a restart.
  _Reuse_: take over Debezium's own `pgoutput` slot. This needs no second slot,
  but rollback is harder.
- Offsets taken during a Debezium snapshot (initial, blocking or incremental)
  are refused with a coded error. So are offsets that cannot be proven safe.
  The import never guesses.
- **Duplicate bound:** at most the rows that share one change LSN, which is one
  row for an ordinary statement and at most one multi-insert WAL record
  (≤ 1000 rows) for COPY. In some cases the bound is wider, and each case is
  stated below. Nothing is lost relative to what Debezium had committed,
  provided Debezium was stopped gracefully.

This is Tier 1: it decides what a resumed pipeline skips (invariants 1–3).

## Context

### What exists (inventory, 2026-10-12)

- Neither this repo nor `ConduitIO/conduit` contains offset-import code.
  `conduit migrate kafka-connect` has not been built yet: it is the v0.21 lane
  ConduitIO/conduit#3003. This lane is ConduitIO/conduit#3004, under the
  tracking issue #3002. Nothing in `conduitio-labs` touches `connect-offsets`.
- `conduit-kafka-connect-wrapper` runs Debezium jars inside Conduit. It could
  load Debezium's offsets natively, but it is not this connector (see
  Alternatives).
- ConduitIO/conduit-connector-sdk#378 (a position-only checkpoint) is open.
  If it existed, it would let the import persist itself at `Open`. This design
  does not depend on it.
- This connector on `main` (v0.15.0 candidate) already has what the import
  builds on:
  - the change key `(tx_commit_lsn, tx_seq)` and position format version 2
    (`20261008-interleaved-tx-resume.md`);
  - `ResumePoint.Delivered` and `ChangeKey.Predecessor` (#338);
  - the flush gate `reportedPositions` (`20261007-dbz3-b2-heartbeats.md`);
  - deciding drift on delivery and the #339 boundary handling
    (`20261008-drift-decision-on-delivery.md`,
    `20261009-drift-approval-delivers-boundary-row.md`).
- ROADMAP PR #2945 lists offset import under v0.22. The v0.21 plan, approved
  later, moves the Postgres import into v0.21 (#3002). The roadmap text needs
  that edit.

### The Debezium offset

Kafka Connect stores source offsets as `partition → offset` maps.

- **Distributed mode** keeps them in the compacted topic `connect-offsets`.
  That is the default name; the worker's `offset.storage.topic` can rename it,
  and since KIP-618 a connector can set its own `offsets.storage.topic`. The
  record key is the JSON array `["<connector name>", {"server": "<topic.prefix>"}]`
  (`database.server.name` before Debezium 2.0). The value is the JSON offset
  map. The latest record for a key wins, and a null value is a tombstone.
- **REST** (Kafka 3.5+, KIP-875): `GET /connectors/{name}/offsets` returns
  `{"offsets":[{"partition":{...},"offset":{...}}]}`. The same KIP adds
  `PUT /connectors/{name}/stop` and `PATCH .../offsets`.
- **Standalone mode** and Debezium Server write a Java-serialized
  `HashMap<ByteBuffer,ByteBuffer>` file (`FileOffsetBackingStore`). Out of
  scope for v1. Users export the offset through the standalone worker's REST
  API instead (see Non-goals).

The Debezium Postgres offset fields are listed below. They were read from
`PostgresOffsetContext`, `SourceInfo`, `PostgresStreamingChangeEventSource` and
`WalPositionLocator` on Debezium `main`. "Where" says which versions write the
field, and is to be confirmed by the fixture capture in Tests.

| Key | Meaning | Where |
| --- | --- | --- |
| `lsn` | LSN of the last processed message. For a change, its WAL start. For BEGIN, the transaction's first LSN. For COMMIT, the **end** of the commit record. | all |
| `lsn_commit` | `lsn` of the last processed COMMIT, the end of that commit record. Updated only when a COMMIT is processed. This is what Debezium flushes to the slot. | 1.x (late) → 3.x |
| `lsn_proc` | last LSN all of whose events were processed. Debezium uses it as its streaming start, and as `lsn_commit` when that key is absent. | 1.x (late) → 3.x |
| `txId` | xid of the message's transaction (32-bit, carried as a long) | all |
| `ts_usec` | commit time | all |
| `messageType` | `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`/`MESSAGE`/`BEGIN`/`COMMIT`/… | 1.x (late) → 3.x |
| `lsn_events_processed` | how many events at `lsn` in this transaction were processed, including the one this offset belongs to; reset at COMMIT | recent 3.x only |
| `snapshot` | present while a snapshot runs: boolean `true` in older versions, a snapshot-type string in newer ones (the fixtures pin which) | all |
| `last_snapshot_record`, `snapshot_completed` | snapshot finished; Debezium treats either as "snapshot done" | all / 3.x |
| `xmin` | slot xmin, only with `xmin.fetch.interval.ms` | optional |
| `incremental_snapshot_*`, `transaction_*` | incremental-snapshot state; transaction-metadata state | when enabled |

Two facts drive the mapping:

1. **`lsn_commit` is the end of a commit record, not its start.** For a COMMIT
   message, the walsender stamps the XLogData header with `txn->end_lsn`, and
   Debezium records that header value. This connector keys transactions by
   `BeginMessage.FinalLSN`, the **start** of the commit record. Debezium itself
   documents that on PG 17 an `end_lsn` can equal the next transaction's first
   change LSN (debezium/dbz#76). So the two LSN domains must never be compared
   with `<=`.
2. **Debezium flushes `lsn_commit` to the slot** in `commitOffset`. It does so
   after Kafka Connect has committed offsets, using the offset of the last
   record Kafka acknowledged. After a graceful stop, the slot's
   `confirmed_flush_lsn` (F below) is therefore at or below the committed
   `lsn_commit`. After an ungraceful stop it can be above.

### Notation

- **O** is Debezium's committed offset. **Lc** is its `lsn_commit`, **L** its
  `lsn`, **X** its `txId`, **M** its `messageType` and **n** its
  `lsn_events_processed`.
- **P** is the last transaction whose COMMIT Debezium processed, so
  `Lc = P.end_lsn`.
- **T** is the transaction of O's record, when O belongs to a change.
- **F** is the slot's `confirmed_flush_lsn`, and **C** is a fresh slot's
  consistent point.
- A transaction's commit LSN means `FinalLSN`, the start of its commit record,
  as in this connector's change key.

## Goals

1. Start a pipeline from O without a snapshot.
2. **No loss:** every change in the pipeline's tables is delivered either by
   Debezium (it is covered by O) or by Conduit.
3. **Bounded duplicates,** stated per case and checked by the harness against
   the dry run's prediction.
4. **Refuse rather than guess:** every unsafe input fails with a stable code
   and a suggested fix.
5. **Rollback to Debezium** is a documented, tested procedure.
6. **No position format change** and no change to the exact resume rule.

## Non-goals

- Offsets from standalone-mode files or Debezium Server (Java-serialized).
  Users export them through REST. A v0.22 follow-up could add these if anyone
  asks.
- Debezium **output** compatibility, meaning the same envelope and topics. That
  is v0.22 ("Debezium-compatible output mode"). This design only decides where
  the stream resumes.
- MySQL and other connectors (v0.22).
- Importing tables Debezium did not capture. Those need their own snapshot, in
  a separate pipeline.
- Debezium reading from a standby (PG 16+ logical decoding on standby). The
  import refuses when `pg_is_in_recovery()` is true.

## Constraints

- Invariant 1: the slot never moves past an unacked record. The import must
  not loosen the flush gate.
- Invariant 2: no position format change. The import produces no persisted
  state of its own.
- Invariant 3: every rule that skips a change has a proof that Debezium
  delivered it.
- A logical slot has one consumer at a time. A second `START_REPLICATION`
  fails with SQLSTATE 55006 (`replication slot … is active`).
- A slot is bound to one output plugin. Debezium's default `plugin.name` is
  `decoderbufs`, and this connector only speaks `pgoutput`.
- pgoutput reads publications through the historic catalog snapshot of the WAL
  it is decoding. A publication created after a slot's `restart_lsn` can fail
  with `publication "…" does not exist` when that WAL is decoded. So a reused
  slot must use Debezium's publication, and a fresh slot must be created after
  its publication.
- The import is applied only to an **empty** position. It never moves an
  existing position, forward or back.

## Decision

### D1. Where the work lives

| Piece | Repo | Why |
| --- | --- | --- |
| `source/dbzoffset`: parse and validate a Debezium offset, compute the resume point, run the preflight checks against Postgres | this repo, exported package | Postgres semantics belong to the connector. `conduit` already imports this connector as a built-in, so the CLI dry run calls the **same code** the connector runs at `Open`. |
| Import mode in `logrepl` (resume point, flush ceiling, preflight at `Open`) | this repo | data path |
| Reading Kafka Connect (topic, REST, JSON file), the cutover orchestration, `--dry-run`, the rollback export | `conduit`, `migrate kafka-connect` | Kafka Connect knowledge belongs to the CLI. It uses franz-go, which is already a dependency, so nothing new is added. |

`conduit` currently embeds connector v0.14.3. The import needs the v0.15.x bump
that lane #3004 already plans.

### D2. The connector setting

`logrepl.import.debeziumOffset` is a JSON string. It is empty by default. The
CLI writes it, and people do not normally write it by hand:

```json
{
  "format": "debezium-postgres/v1",
  "connector": "inventory-connector",
  "partition": {"server": "dbserver1"},
  "slotName": "debezium",
  "publicationName": "dbz_publication",
  "tables": ["public.orders", "public.customers"],
  "offset": {"lsn_proc": 24023128, "lsn_commit": 24023064, "lsn": 24023128,
             "txId": 555, "ts_usec": 1760000000000000, "messageType": "INSERT"}
}
```

`offset` is Debezium's map, unchanged. LSNs are parsed as `json.Number` into
`uint64`, never through `float64`. `slotName`, `publicationName` and `tables`
are Debezium's settings. They tell the connector whether it is reusing
Debezium's slot (`logrepl.slotName == slotName`) and let it check the table
set.

How the connector treats the setting:

- **Position empty:** apply the import. Validation runs first. With the import
  set, `snapshotMode` must be `never`, or validation fails with
  `postgres.import.snapshot_mode_conflict`, so the "no snapshot" intent is
  explicit. When reusing Debezium's slot, `logrepl.autoCleanup` must be
  `false`, or validation fails with `postgres.import.autocleanup_on_reused_slot`,
  because deleting the pipeline would drop Debezium's slot and publication and
  end any chance of rollback.
- **Position present:** ignore the setting and log one warning per start:
  `import ignored: pipeline has a position; remove logrepl.import.debeziumOffset`.

### D3. Slot strategy

**Fresh (default).** Creating a new slot with the same plugin is how Conduit
replication normally starts. Steps, run by `conduit migrate kafka-connect
--cutover`:

1. While Debezium runs, create the Conduit publication, then the Conduit
   `pgoutput` slot. Record its consistent point C.
2. Wait until Debezium's **committed** offset has `Lc ≥ C`, polling REST or the
   topic. On an idle database, Debezium does not process a commit, so Lc does
   not move. The CLI can prompt one (see Open questions, Q3). Otherwise the
   runbook says to write one row to any captured table.
3. Stop Debezium gracefully (`PUT /connectors/{name}/stop` on Kafka 3.5+, or
   delete the connector on older workers; the CLI saves the connector config
   for rollback). Wait until Debezium's slot is `active = false`.
4. Read the final O and check `Lc ≥ C` again.
5. Start the Conduit pipeline with the import.

Why it is gap-free: a new slot decodes in full every transaction that commits
at or after C. Every transaction that started before its snapshot reached
CONSISTENT has finished by C, which is how slot creation works. Conduit must
deliver T and everything after it, and `T.commit ≥ P.end_lsn = Lc ≥ C`. So the
fresh slot has all of it, whatever plugin Debezium used.

**Reuse (opt-in, `--slot-strategy=reuse`).** Stop Debezium, then point
`logrepl.slotName` and `logrepl.publicationName` at Debezium's slot and
publication. Requires `plugin = 'pgoutput'`. It saves a second slot and a
second WAL retention during the rollback window. The cost is that rollback
needs an offset written back to Debezium (D9).

**Both consumers at once.** With reuse, Postgres refuses the second consumer
(55006). The preflight also checks `active = false` and, through REST, that the
connector is `STOPPED` or absent. `PAUSED` is not enough, because a paused task
can still hold the replication connection. With fresh, two consumers on two
slots is the overlap window. Conduit's slot is not read until cutover, so
nothing is delivered twice during it.

### D4. Mapping O to a resume point

The connector starts the subscription at the slot's position, as it does for a
fresh start (start LSN 0 → `restart_lsn`, and the server decodes from F). It
then replaces the subscription's resume point with the import's.

**Base rule:** skip every transaction whose commit LSN is **strictly below**
Lc. This is the existing exact rule with the key `(Lc − 1, MaxUint64)`, the
same construction as `ChangeKey.Predecessor`. `ResumePoint.Delivered` already
evaluates it. No new reading logic is needed.

**Partial rule (in memory only):** used when M is a DML type (`INSERT`,
`UPDATE`, `DELETE`, `TRUNCATE`) and X is present. For the **first**
transaction streamed with commit LSN ≥ Lc, and only if its `BeginMessage.Xid`
equals X:

- skip its changes whose change LSN is below L;
- if n is present **and** the slot is Debezium's own (reuse, so the same
  publication and the same row filters), also skip the first n changes at
  LSN L;
- deliver everything else.

If the first such transaction's xid is not X, the partial rule is dropped,
with a warning, and that transaction is delivered in full. This can happen
when T touched only tables outside the pipeline. Dropping the rule only adds
duplicates; it never skips anything.

`ResumePoint` gets an optional `Partial{Xid, BeforeLSN, SkipAtLSN}` field,
used only by the subscription goroutine. Changes the partial rule skips still
advance the change key (seq is counted before the skip decision), so the
records Conduit delivers from T carry their real keys. The first ack persists
a normal version 2 position `(T.commit, seq)`, which the exact rule resumes
from. The partial rule exists only until that ack.

The other values of M:

- **M = COMMIT** (a heartbeat or a transaction-metadata END record): P is T,
  so Lc covers T and the base rule skips it.
- **M = BEGIN:** nothing of T was delivered, so T is delivered in full.
- **M = MESSAGE:** Postgres reports a logical message's **end** LSN. To keep
  the reasoning simple, the partial rule is not used and T is delivered in
  full.
- **M absent** (older 1.x): base rule only.

**Offsets without `lsn_commit`** (offsets older than late 1.x, or offsets with
only `lsn`) are refused with `postgres.import.offset_unsupported`. Fix: run
Debezium 1.9 or later until it commits an offset, then retry. The base rule
could use `lsn` with the legacy bound, but no supported Debezium writes such
offsets any more, and an untested rule is not worth having.

### D5. Preflight (the connector at `Open` and the CLI dry run share it)

All checks must pass before replication starts. Each failure has a stable code
(see Error codes).

1. O parses, and Lc is present and non-zero.
2. O is not a snapshot offset. The `snapshot` key is present (boolean or
   string), or `last_snapshot_record`/`snapshot_completed` is true with no
   `lsn_commit`. Also refused: any `incremental_snapshot_*` key that shows a
   snapshot in progress. See D7.
3. Lc ≤ `pg_current_wal_lsn()`, so the offset does not belong to another
   server or to a restored backup. `pg_is_in_recovery()` is false.
4. The slot exists (import mode never creates one: a slot created now would
   start at the current WAL and miss the gap), is on this database, has `plugin = 'pgoutput'`, is not
   `active`, and is not temporary. Its `wal_status` is not `lost` (PG 13+) and
   it has no `invalidation_reason` (PG 17).
5. **F ≤ Lc.** Postgres re-sends exactly the transactions whose commit is at or
   past F, and Conduit needs everything with commit ≥ Lc. If F > Lc, the
   changes in (Lc, F] are not re-sent and O does not cover them. Under reuse
   that means Debezium flushed past its committed offset (an ungraceful stop).
   Under fresh it means the slot was created after O. Either way the import
   refuses (`postgres.import.slot_ahead`). There is no override in v0.21 (Q2).
6. The publication exists and covers every table in `tables`
   (`pg_publication_tables`).
7. The pipeline's tables are a subset of Debezium's `tables`. A table Debezium
   did not capture has no history in O and needs a snapshot. The CLI checks
   this against the Kafka Connect config, and the connector checks it again
   against `tables`.

### D6. The flush gate in import mode

The gate is unchanged except for one addition. **Until the first ack of the
run, the reported flush position is capped at Lc.**

Why this is needed: at start, nothing has been emitted or acked, so the gate
is open and a keepalive could move `confirmed_flush_lsn` to the walsender's
`sentPtr`. That is safe for data, because `sentPtr` is below the commit of any
transaction not yet received (the reasoned assumption in
`20261008-interleaved-tx-resume.md`). But it can carry F above Lc. If the
process is then killed before the first ack, the position is still empty, the
import is re-applied, and check 5 refuses: a crash would turn into a stuck
pipeline. The cap keeps F ≤ Lc until a version 2 position exists, so
re-applying is always allowed and gives the same result.

The cap only ever lowers what is reported, so invariant 1 holds trivially. The
cost: on a database with no captured traffic, the slot stays at Lc until the
first record is acked. That is how Debezium behaves without heartbeats (B2 cut
heartbeats). The warning `import pending first ack; slot held at <Lc>` repeats
every minute, and the runbook covers it.

`reportedPositions` gets a `ceiling LSN` argument, zero meaning none. While
`acked` is unknown, the flush position is `min(flush, ceiling)`. Every earlier
report in this process was capped the same way, so the never-decrease rule
still holds: `reportedFlush` starts at zero in each process, and check 5
guarantees F ≤ Lc at start. A report below F is harmless, because Postgres
never moves `confirmed_flush_lsn` backwards.

### D7. Snapshot state is refused

| Offset shape | Code | Suggested fix |
| --- | --- | --- |
| `snapshot` present, not completed (initial or blocking snapshot running) | `postgres.import.snapshot_in_progress` | Let the Debezium snapshot finish and Debezium commit a streaming offset, then retry. |
| Snapshot completed but no streaming offset yet (`last_snapshot_record`/`snapshot_completed` true, no `lsn_commit`) | `postgres.import.snapshot_in_progress` | Write one row to a captured table so Debezium commits a streaming offset, then retry. |
| `incremental_snapshot_*` shows a snapshot in progress | `postgres.import.incremental_snapshot_in_progress` | Let it finish, or stop it with Debezium's `stop-snapshot` signal, then retry. |

Why snapshot-final offsets are refused: the boundary between Debezium's
snapshot and its stream is Debezium-internal. How a transaction that committed
around the snapshot's LSN was handled depends on the Debezium version and the
snapshot mode. The streaming offset that follows one ordinary write removes the
question.

### D8. Interaction with #339 and drift handling

- **No schema history is imported.** Debezium Postgres keeps none: it relies on
  Relation messages. An imported start therefore looks like a fresh start. The
  first delivered change of each table decides `driftInitial`, so no halt.
- **Replayed Relation messages do not decide drift** (#335). Changes skipped by
  the base or partial rule never reach the handler, so only shapes of
  delivered changes are decided. A DDL inside the delivered window is decided
  `driftInProcess` against the shape cached from the replay, under the
  configured policy, exactly as on any restart.
- **A halt before the first ack** emits the marker with the key one below the
  deciding change (#338). Its ack persists a version 2 position, and the
  approving restart resumes from it, not from the import. If the marker is the
  first record and its ack is the first ack, the D6 cap ends there, and the
  #338 rule that the gate stays closed after a marker still holds.
- **Drift across the cutover window is not detectable.** A DDL applied between
  Debezium's last change and Conduit's first is seen as the initial shape.
  This is the same as any fresh start, and it is documented.

### D9. Rollback (back to Debezium)

| When | Fresh slot | Reuse |
| --- | --- | --- |
| Before Conduit's first ack | Delete the pipeline (`autoCleanup` drops Conduit's slot and publication). Restart the Debezium connector, which resumes from O on its untouched slot. No loss, no duplicates. | Stop Conduit and restart Debezium. The D6 cap kept F ≤ Lc, so Debezium resumes exactly from O. |
| After Conduit has run | Restart Debezium. It resumes from O on its own slot and re-delivers everything since cutover into Kafka: no loss, and duplicates equal to Conduit's whole run. Then drop Conduit's slot. | `conduit migrate kafka-connect rollback` computes a Debezium offset from Conduit's last position and writes it with `PATCH /connectors/{name}/offsets` (Kafka 3.5+). |
| Window | As long as Debezium's slot is kept. It holds WAL the whole time, so set `max_slot_wal_keep_size` and drop the slot after a soak period. | Unbounded, but only supported where the harness proves the written-back offset (Q4). |

The reuse write-back offset, for Conduit's last persisted position
`(Tc, s)` with change LSN Lx:

```text
lsn_commit = Tc − 1   # forces Debezium to treat T' as unfinished
lsn = lsn_proc = Lx
messageType = <op of Lx>
txId = <xid of T'>
```

This depends on `WalPositionLocator`, which has changed in every Debezium
minor version, including the PG 17 false-match fix and the
`lsn_events_processed` skip. So it ships only for Debezium versions where the
rollback test is green. Elsewhere `rollback` refuses
(`migrate.kafka_connect.rollback_unsupported`) and points to the fresh-slot
strategy.

### D10. How the user does it

The import uses both a CLI flag and a connector setting, each for its own job.
The CLI reads Kafka Connect and orchestrates the cutover. The connector setting
is what makes the import repeatable across a crash. A flag alone would leave
nothing for the connector to read when it restarts before its first ack.

```sh
# Plan: translate the connector config and show the computed resume point.
conduit migrate kafka-connect --config dbz-orders.json \
  --import-offset --connect-url http://connect:8083 --dry-run [--peek] --json

# Cut over: fresh slot, wait for Lc ≥ C, stop Debezium, import, write the pipeline.
conduit migrate kafka-connect --config dbz-orders.json \
  --import-offset --cutover --stop-connector --connect-url http://connect:8083

# Without the REST API, read the topic instead (read_committed, all partitions, to the high watermark).
  --offsets-from topic --bootstrap-servers kafka:9092 [--offsets-topic connect-offsets]
# Or read a saved REST export.
  --offsets-from file --offsets-file offsets.json

# Go back.
conduit migrate kafka-connect rollback --pipeline orders --connect-url ...
```

`--stop-connector` is opt-in. Without it, the CLI checks that the connector is
stopped and fails with a fix if it is not.

The `--dry-run --json` output is shaped like this:

```json
{
  "offset": {"source": "rest", "connector": "inventory-connector",
             "partition": {"server": "dbserver1"},
             "lsn_commit": "0/16E8F58", "lsn": "0/16E8F98", "txId": 555,
             "messageType": "INSERT", "lsn_events_processed": null,
             "shape": "debezium>=1.9"},
  "slot": {"name": "debezium", "strategy": "reuse", "plugin": "pgoutput",
           "active": false, "confirmed_flush_lsn": "0/16E8F58",
           "restart_lsn": "0/16E1A20", "wal_status": "reserved",
           "retained_wal_bytes": 512000},
  "resume": {"skip_commits_below": "0/16E8F58",
             "partial": {"xid": 555, "skip_changes_below_lsn": "0/16E8F98",
                         "skip_at_lsn": 0},
             "duplicate_bound": "rows sharing LSN 0/16E8F98 in xid 555 (<= 1000)",
             "peek": {"duplicates_exact": 1, "replay_transactions": 3}},
  "checks": [{"name": "slot_not_ahead", "ok": true}],
  "errors": []
}
```

`--peek` opens the replication stream on the inactive slot and runs the same
`ResumePoint` over it without emitting anything. It sends **no** standby status
update, so the slot does not move. It stops at T's commit or after a timeout,
and reports the exact number of duplicates and the replay size. The harness
asserts the observed duplicates against this number.

### D11. Error codes

Connector codes, raised at validation or at `Open`. Each error says the config
path and how to fix it, following the `postgres.schema_drift.*` style.

| Code | Cause | Fix |
| --- | --- | --- |
| `postgres.import.offset_invalid` | unparseable, missing `lsn_commit` and `lsn`, an LSN that does not fit, or a wrong `format` | Re-export the offset with `migrate kafka-connect --dry-run`. |
| `postgres.import.offset_unsupported` | no `lsn_commit` (an offset older than late 1.x) | Run Debezium ≥ 1.9 until it commits an offset. |
| `postgres.import.snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.incremental_snapshot_in_progress` | see D7 | see D7 |
| `postgres.import.snapshot_mode_conflict` | the import is set with `snapshotMode: initial` | Set `snapshotMode: never`. |
| `postgres.import.autocleanup_on_reused_slot` | reuse with `logrepl.autoCleanup: true` | Set it to `false`. |
| `postgres.import.slot_missing` | the slot does not exist (for example Debezium ran with `slot.drop.on.stop=true`) | If Debezium can run again, use `--slot-strategy fresh`. Otherwise re-snapshot or dual-run (runbook). |
| `postgres.import.slot_plugin` | the slot is not `pgoutput` | Use `--slot-strategy fresh`. |
| `postgres.import.slot_active` | the slot is in use | Stop Debezium (`PUT /connectors/{name}/stop`); pause is not enough. |
| `postgres.import.slot_invalidated` | `wal_status = lost` or `invalidation_reason` is set | The WAL is gone. Re-snapshot. |
| `postgres.import.slot_ahead` | F > Lc | See D5 check 5. Fresh: redo the cutover. Reuse: re-snapshot or dual-run. |
| `postgres.import.slot_database` | the slot is on another database | Fix `url`. |
| `postgres.import.offset_from_future` | Lc > `pg_current_wal_lsn()`, or the server is a standby | Wrong server, or one restored from backup. |
| `postgres.import.publication_mismatch` | the publication is missing or does not cover the tables | Add the tables to the publication, or fix `logrepl.publicationName`. |
| `postgres.import.tables_not_captured` | the pipeline has tables Debezium did not capture | Remove them and add them in a separate pipeline with a snapshot. |

CLI codes (`migrate.kafka_connect.*`, mapped to exit codes per the
deterministic exit code ADR):

- `offset_not_found` – no offset for this connector.
- `ambiguous_partition` – more than one partition.
- `connector_running` – the connector is RUNNING or PAUSED.
- `offsets_unreachable` – neither REST nor the topic could be read.
- `cutover_timeout` – Lc did not reach C in time.
- `rollback_unsupported` – D9.

The CLI also passes the connector's codes through as they are.

## Correctness

### No loss

Kafka Connect commits a source offset only after it has acknowledged the
record the offset belongs to and every earlier record from the task. Before
KAFKA-12226 it did this by flushing the producer before each commit; since
then it tracks acknowledged records in order. Assume also that Debezium was
stopped gracefully. Let c be a change in a pipeline table, in transaction U.

1. **U.commit < Lc.** Commit records do not overlap, so
   `U.commit < Lc = P.end_lsn` gives `U.commit ≤ P.commit`. U is P or a
   transaction before it in commit order, which is stream order. Debezium
   emitted a record for each of U's changes in a captured table before any
   record of T, or before the record that carries O. With in-order commits,
   all of them were acknowledged. **Skipped, delivered by Debezium.**
2. **U = T, xid = X, the partial rule applies, LSN(c) < L.** Within one
   transaction, pgoutput sends changes in WAL order, so c was emitted before
   O's record. **Skipped, delivered by Debezium.** The same holds for the first
   n changes at L under reuse: Debezium counted them on the same stream.
3. **Otherwise** U.commit ≥ Lc ≥ F (check 5), so Postgres re-sends U. Its WAL
   is retained, because a slot keeps what it needs to decode every transaction
   committing after `confirmed_flush_lsn`. Under fresh, `U.commit ≥ Lc ≥ C`
   (D3). Conduit delivers c under its existing invariants. **Delivered by
   Conduit.**
4. **Kill at any point.** Before the first ack, the position is empty. O and
   the setting are unchanged, F ≤ Lc still holds (D6), and the preflight and
   the rule give the same result. After the first ack, the version 2 position
   rules.

Assumptions, each turned into a harness assertion:

- (a) `lsn_commit` is the commit's end LSN.
- (b) `lsn` for a change is that change's WAL start.
- (c) The committed offset covers only acknowledged records, in order (both
  Kafka Connect commit paths above).
- (d) The table set matches.

### Duplicate bound

Conduit re-delivers a change that Debezium also delivered only in these cases:

| Case | Duplicates |
| --- | --- |
| Partial rule, reuse, n present | 0 |
| Partial rule otherwise | the changes of T at LSN L that Debezium delivered: 1 for an ordinary row, ≤ one `heap_multi_insert` record (≤ 1000 rows) for COPY or a multi-row INSERT |
| M = BEGIN or COMMIT | 0 |
| M = MESSAGE, M absent, or xid mismatch | T's prefix that Debezium delivered (bounded by T's size) |
| Debezium stopped ungracefully | the records Debezium produced after O, up to one `offset.flush.interval.ms` of traffic. They are in Kafka but not in O. In this case check 5 usually refuses. |

Every case is exercised by the harness. With `--peek`, the dry run predicts the
exact count, and the harness asserts equality, not just the bound.

## Failure modes

| Failure | Behavior | Invariant |
| --- | --- | --- |
| Debezium still running (reuse) | Preflight `slot_active`. If Debezium is mid-reconnect, Postgres rejects the second consumer (55006). | 3 |
| Debezium stopped ungracefully, F > Lc | `slot_ahead` refusal | 3 |
| Debezium's slot dropped on stop | `slot_missing`; never auto-create a slot in import mode (that would start at the current WAL and lose the gap) | 3 |
| Fresh slot created after O (C > Lc) | `slot_ahead` refusal; the CLI also refuses before stopping Debezium | 3 |
| Slot invalidated (`max_slot_wal_keep_size`) | `slot_invalidated` | 3 |
| Conduit killed before the first ack | Re-import gives the same resume point; the cap keeps F ≤ Lc | 1, 2, 3 |
| Conduit killed after the first ack | Version 2 exact resume | 2, 3 |
| Pipeline reset months later with the setting still present | F ≫ Lc, so `slot_ahead`; the operator removes the setting | 3 |
| Idle database after import | Slot held at Lc until the first ack (D6); warning and runbook | — |
| Snapshot-state offset | Coded refusal (D7) | 3 |
| First transaction ≥ Lc is not X | Partial rule dropped, T delivered in full, warning | 3 |
| Table set mismatch | `tables_not_captured` / `publication_mismatch` | 3 |
| Offset from another server | `offset_from_future`, or an inconsistent slot leads to `slot_ahead`. An offset from another server that is **below** this server's WAL is not detectable from the offset alone. The CLI cross-checks the slot name and database from the Kafka Connect config. | 3 |
| Large LSN in JSON | Parsed as `uint64` through `json.Number`; values that do not fit fail | — |
| Aborted EOS offset writes in `connect-offsets` | Read with `read_committed` | 3 |
| Downgrade to a build without import before the first ack | An older build ignores the unknown setting or rejects it at validation. If it starts, it resumes from F with the legacy rule: duplicates of (F, Lc], no loss, because the slot is present | 3 |

## Upgrade and rollback (of the connector)

- There is no format change. The setting is new and optional. Positions
  written after an import are ordinary version 2 positions.
- Downgrading after the first ack works the same as for any version 2
  position (see the interleaved-tx doc).
- Debezium rollback: D9.

## Observability

- INFO at `Open`: the parsed O, the strategy, F, `restart_lsn`, the computed
  rule, and the duplicate bound.
- INFO at the first ack: `import complete at <key>; remove logrepl.import.debeziumOffset`.
- WARN every minute while the D6 cap holds the slot.
- WARN when the partial rule is dropped.
- Record metadata `postgres.import.possibleDuplicate=true` on the records in
  the duplicate set. With the partial rule, that is T's changes at L. Without
  it, all of T's changes. Sinks that are not idempotent can filter or dedupe
  on it (Q5).
- The connector has no metrics of its own today. Lane #3013 owns connector
  metrics, and an `import_pending` gauge is requested there.

## Tests

### Unit and property

- `FuzzParseDebeziumOffset` with a seed corpus of the fixtures captured from
  every supported Debezium version (`source/dbzoffset/testdata/<version>/*.json`),
  covering streaming, snapshot, snapshot-completed and incremental-snapshot
  offsets.
- A reference model for `ResumePoint` with `Partial`. Random streams with
  interleaved transactions, COPY runs that share an LSN, PG 17 `end_lsn`
  collisions and random O cut points are checked against a model of "what
  Debezium delivered", with loss = 0 and duplicates ≤ the bound. The
  connector has no property framework yet. Adding `pgregory.net/rapid` as a
  test-only dependency is requested here: it is what CLAUDE.md names, and the
  invariants are about sequences.
- `reportedPositions` with a ceiling: the table cases, plus never-decrease.
- Preflight against a real Postgres for each check.

### Integration harness: real Debezium (`test/debezium`)

- **Compose:** Postgres 14, 15, 16 and 17 (`quay.io/debezium/postgres:<v>`,
  which also has decoderbufs); Kafka in KRaft mode; and Kafka Connect with the
  Debezium Postgres connector (`quay.io/debezium/connect`) in versions 2.7
  and the latest 3.x, plus 1.9 on PG 14 for the topic-only path (pre-KIP-875).
- **Matrix:**
  - Pull requests: PG 17 × Debezium 3.x × pgoutput × {fresh, reuse}.
  - Nightly: the full PG × Debezium × {pgoutput: fresh and reuse;
    decoderbufs: fresh} matrix.
  - This shares infrastructure with the migration harness lane (#3010) and
    hands it the Postgres case.
- **Workload:** a generator with a ledger (it reuses `test/chaos/ledger.go`).
  Every row has a unique id and version. The mix:
  - single-row transactions;
  - one large transaction (100k rows);
  - COPY batches;
  - interleaved long-open transactions (T1 begins, many commit, then T1
    commits);
  - updates that change the primary key, and deletes;
  - an `ALTER TABLE ADD COLUMN` mid-stream (drift under `halt` and `evolve`).
- **Cut points** (each one a test):
  1. graceful stop at quiescence;
  2. graceful stop while Debezium is emitting the large transaction (offset
     mid-T, partial rule);
  3. stop during the long-open interleaved T1;
  4. stop during a COPY (repeated with a randomized delay until an offset
     lands at a shared LSN; the test asserts that it did);
  5. offset written by a heartbeat or transaction-metadata END record
     (M = COMMIT);
  6. Kafka Connect worker `kill -9`, which expects either a successful import
     with no loss or a `slot_ahead` refusal, and never loss.
- **Diff:** expected = the ledger. Observed = Debezium's topics (read to the
  end) ∪ Conduit's destination (file destination, JSON Lines). Identity is
  `(table, pk, version, op)`. The test asserts:
  - loss is 0;
  - Conduit ∩ Debezium equals the `--peek` prediction;
  - there are no duplicates inside Conduit's own output beyond the
    connector's at-least-once window.
- **Kill tests** (the `test/chaos` process-kill harness, SIGKILL): kill after
  the preflight but before streaming; after the first change is emitted but
  before its ack (re-import, then assert F ≤ Lc after the kill); after the
  first ack; at `chaospoint.StandbyStatusUpdate` during the cap. Each is
  followed by recovery and the same diff.
- **Rollback tests:** fresh, before and after Conduit has run. Reuse write-back
  on every Debezium version in the matrix. The versions where this is green
  are the supported list for `rollback` under reuse.
- **Refusal tests:** one per error code, each asserting the code and that the
  slot did not move.

## Alternatives considered

1. **Re-snapshot (today's path).** Start a new pipeline with
   `snapshotMode: initial`. It needs no new code, but it takes hours of load on
   large tables and duplicates every row, which is exactly what this lane
   exists to avoid. It remains the fallback for `slot_missing`,
   `slot_invalidated` and `slot_ahead`.
2. **Dual-run cutover with no offset.** Create Conduit's slot, run Conduit
   alongside Debezium with `snapshotMode: never`, then stop Debezium. It needs
   no parsing and no new connector code, only a runbook. There is no loss if
   Conduit's slot exists before Debezium stops. But the duplicates are
   everything committed between C and Debezium's stop, minutes to hours of
   traffic, and two writers feed the sinks at once. It is documented as the
   fallback for unsupported Debezium versions, and rejected as the default
   because its duplicates are unbounded by design.
3. **Snapshot-from-LSN / peek-computed exact position.** Run
   `pg_logical_slot_peek_binary_changes` before starting, find T's commit LSN
   and the seq of L, and write an exact version 2 position, with no runtime
   rule. Conduit has no API to set a connector's position, so it would still
   need a setting. The SQL peek materializes T in full (a tuplestore that
   spills to disk), and it would run again at every start before the first
   ack. The partial rule gets the same result in one pass. Peeking is kept
   only for the dry run, done over the stream with no feedback.
4. **Read Debezium's output topics** to find the last delivered event
   (`source.lsn`, `source.txId`) instead of `connect-offsets`. That is more
   precise, since it includes records produced after the last offset commit.
   But it needs Debezium's converters and Schema Registry, every table topic,
   and an order across partitions. It is rejected for v1. If ungraceful stops
   turn out to matter, it could make the `slot_ahead` case safe.
5. **Keep Debezium through `conduit-kafka-connect-wrapper`.** The wrapper
   loads Debezium's offsets natively, so there is no mapping risk. But it does
   not move the user to this connector, its Postgres coverage is unproven
   (`20260722-debezium-compete-roadmap.md`), and it keeps the JVM. It is the
   right path for databases without a native connector. It is not this one.
6. **Base rule only** (no partial rule). This is less code on the data path,
   and the bound becomes "the prefix of T that Debezium delivered". Stopping
   Debezium during a bulk load would then duplicate up to the whole load. The
   partial rule is about 30 lines and is checked by the property model, so it
   is included. Q1 asks whether to ship it in v0.21.

## Implementation slices

Each slice is its own PR. All are Tier 1 except slice 5.

1. `source/dbzoffset`: the parser, the fixtures, fuzzing, and the
   resume-point computation. No data-path wiring yet.
2. Import mode: the setting, validation, preflight at `Open`, `Resume`
   override with `Partial`, the flush ceiling, and the error codes. Unit and
   property tests, plus chaos kill cases.
3. `test/debezium` harness and CI (PR and nightly matrices).
4. `conduit`: `migrate kafka-connect --import-offset` (REST, topic, file),
   `--dry-run`/`--peek`, `--cutover`, `rollback`. This depends on #3003's
   command skeleton.
5. Docs: the runbook `docs/operations/postgres-debezium-offset-import.md` in
   `conduit` (symptom → diagnosis → fix for every code, plus the slot-held
   warning and the overlap WAL growth), the conduit.io migration page,
   `llms.txt`, and the changelog.

## Open questions for DeVaris

- **Q1.** Ship the partial rule in v0.21, or only the base rule? The
  recommendation is to ship it (see Alternative 6).
- **Q2.** Should `slot_ahead` have an override? The recommendation is no for
  v0.21. The case means O does not cover what the slot discarded. Overriding
  would rely on Debezium having produced records it never committed, and that
  is not provable.
- **Q3.** May the CLI write to the source database during `--cutover`, to move
  an idle Debezium past C? `pg_logical_emit_message(true, …)` writes no table
  rows, but whether it advances `lsn_commit` depends on the PG version (PG 15+
  skips empty transactions) and on Debezium's `messages` option. The
  recommendation is to try it in the harness, and to fall back to the runbook
  ("write one row") where it does not work.
- **Q4.** Reuse rollback: ship the offset write-back only for the Debezium
  versions where the harness is green, or drop reuse from v0.21 and ship fresh
  only? The recommendation is to ship both, with fresh as the default and
  write-back gated per version.
- **Q5.** Add the `postgres.import.possibleDuplicate` metadata key? It would
  be a new public metadata contract.
- **Q6.** Setting name: `logrepl.import.debeziumOffset` (proposed, matching
  the `logrepl.*` prefix) or `cdc.importOffset` (the name in the lane brief)?

## Related

- ConduitIO/conduit#3004 (this lane), #3002 (v0.21 tracking), #3003
  (migrate/kc CLI), #3010 (migration harness), #3013 (connector
  observability).
- ConduitIO/conduit-connector-sdk#378 (position-only checkpoint). If it
  lands, the D6 cap could be replaced by persisting the import at `Open`.
- `20261008-interleaved-tx-resume.md`, `20261007-dbz3-b2-heartbeats.md`,
  `20261008-drift-decision-on-delivery.md`,
  `20261009-drift-approval-delivers-boundary-row.md`,
  `20260724-dbz3-postgres-cdc-parity.md`.
- Debezium sources read for the offset semantics: `PostgresOffsetContext`,
  `PostgresStreamingChangeEventSource` (`processReplicationMessages`,
  `commitOffset`) and `WalPositionLocator` (debezium/debezium `main`,
  2026-10-12).
