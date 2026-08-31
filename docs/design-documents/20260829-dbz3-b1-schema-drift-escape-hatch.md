# DBZ-3 B1 — the schema-drift halt wedge and the checkpoint-before-halt escape hatch

This amendment slots into the DBZ-3 design doc's Area 2 (drift policy, step 3) and
supersedes the hand-waved "restart the pipeline to re-establish the baseline" of
`docs/design-documents/20260724-dbz3-postgres-cdc-parity.md` lines 327-336 with a
mechanism that actually works, verified against the code as it exists on `main`
(HEAD `0dbf995`, after commits 59480f7 / 6fb4dd0 / 845f048 / 77466c0 / 35e45d1 /
e765bc9 / 0dbf995).

## Summary

Drift halts the pipeline at `CDCHandler.handleRelation` (`source/logrepl/handler.go:451-501`).
Under the halt policy the pipeline then stops emitting records, so the position —
the only place schema history is durable (`source/position/position.go:83`) — never
checkpoints the new shape. On restart the connector re-derives the identical drift
and halts again: the wedge. No config change escapes it, because the SDK `Source`
interface has no position-persist API (verified in `conduit-connector-sdk@v0.14.1/source.go`:
`Read/ReadN/Ack/Teardown` are the only data-path methods); the only durable carrier
for the new shape is a record.

This amendment: when drift is detected under any policy whose behavior stops
records, the connector emits one schema-drift marker record that carries the new
schema shape (and the RelationMessage's LSN) forward in the position — a
checkpoint-before-halt. The marker's ack, not the drift sighting, is what surfaces
the halt error to the engine; restart then implicitly approves the drift. For the
dlq policy there is no checkpoint wedge (records flow), but its "acknowledged
drift" state has the same durability gap and rides the same marker.

Three adversarial-review questions (error-code convention, `IsNarrowing` rename
timing, SchemaHistory ack key) are resolved in "Resolved questions" with file:line
evidence. Failure modes and crash windows are analyzed in "Failure-mode analysis";
the mechanism's one genuinely new risk — approval-by-crash — and the
revert-after-approval trap are accepted with mitigations, not hand-waved.

## Status

- DBZ-3 Area 2 step 1 (drift detection, in-memory) — SHIPPED (#322, commit `77466c0`).
- DBZ-3 Area 2 step 2 (durable history) — SHIPPED (#323, commit `35e45d1`).
- DBZ-3 Area 2 step 3 (drift policy: halt / dlq / evolve) — NOT BUILT. This amendment
  is the mechanism the step-3 implementation must follow.
- DBZ-3 B0 kill harness (#324, `e765bc9` chaospoint seam; #325, `0dbf995` harness core)
  — SHIPPED; the SIGKILL acceptance criteria in this amendment run on it.

## Problem

### The wedge, stated against the code

1. Durable schema history lives only in the position payload
   (`source/position/position.go:83`, `SchemaHistory SchemaHistories`), checkpointed
   by the engine whenever a record is acked.
2. `buildPosition` is called from exactly three sites, all on record paths
   (`source/logrepl/handler.go:196`, `:251`, `:285`; the function itself at
   `:359-366`). It returns `LastLSN` plus the *in-memory* base position's
   `SchemaHistory` by reference — nothing except a record makes history durable.
3. Drift halts the pipeline: `handleRelation` (`handler.go:451-501`) records the new
   shape in memory (`RecordSchemaVersion`, `:460`) and (step 3, not yet built)
   stops the iterator.
4. Stopped iterator -> no records -> no position -> the recorded version never
   becomes durable -> on restart the diff machinery re-derives the identical drift
   and halts again, forever.

The wedge is real and reproducible in the existing test suite: the
drift-across-restart test is exactly the wedge in miniature — run 1 records shape
V1 and checkpoints at LSN 150, run 2 sees shape V2 at LSN 200 and classifies it
`driftAcrossRestart` (`source/logrepl/schemahistory_test.go:115-134`).

### Why the fix must be a record, not a config change

The SDK `Source` interface (module cache `conduit-connector-sdk@v0.14.1/source.go`:
`Config` `:45-49`, `Open` `:51-56`, `Read/ReadN` `:58-111`, `Ack` `:113-124`,
`Teardown` `:126-131`, `LifecycleOnCreated/OnUpdated/OnDeleted` `:133-150`) has no
position-persist or checkpoint API. The connector cannot ask the engine to persist
anything out-of-band. A config knob like `logrepl.schemaDrift.approve=true` is the
only non-record escape hatch, and it is rejected in "Alternatives considered":
heavyweight, error-prone, and it inverts the safety decision (approve-by-default).
The record is the carrier the SDK gives us, and it doubles as the operator-visible
evidence trail.

### Scope of the wedge per policy

- halt: wedge, exactly as described above.
- evolve: records flow, so positions advance and the new shape becomes durable —
  no checkpoint wedge. The wedge re-appears for the *narrowing* cases the design
  doc routes to halt ("never for a narrowing change", design doc lines 327-336) —
  those cases are the halt wedge again.
- dlq: records flow to the error path, positions advance — no checkpoint wedge.
  But the design's dlq semantics ("until an operator acknowledges the drift",
  design doc lines 543-574) store *approval state* in the connector, and that state
  has the same durability problem: a restart forgets the approval and re-routes
  everything to the DLQ. The marker carries approval state forward the same way it
  carries the new shape. Note the dlq policy as written is not yet implementable as
  described: the SDK `Source` API has no per-record error return, so "routed to the
  connector's error-return path" needs an engine-side mechanism this amendment does
  not design. The wedge analysis above holds for whatever mechanism step 3 settles
  on, as long as records flow.

## Verified current state (load-bearing facts)

All file:line references are against `main` HEAD `0dbf995` in
`conduit-connector-postgres`. Do not reuse the parent doc's citations; several are
stale (see "Adversarial review" findings F2-F4).

- Relation arm of the message handler: `source/logrepl/handler.go:143-147` calls
  `h.handleRelation(ctx, m, lsn)` and comments at `:145-146` "The returned drift
  kind is not acted on yet ... policy is step 3".
- `handleRelation` classification (`handler.go:451-501`):
  - `driftNone` `:463-467` — shape matches last durable version; comment: Postgres
    re-sends a RelationMessage after a reconnect and when a new subscriber attaches.
  - `driftInitial` `:468-476` — first shape ever seen for this table.
  - `driftInProcess` `:477-484` — the in-memory RelationSet diff is available;
    logs `Bool("narrowing", diff.IsNarrowing())` at `:482`.
  - `driftAcrossRestart` `:485-500` — only the persisted hashes exist; "the
    affected columns cannot be reported".
- Table identity for history is `Namespace + "." + RelationName`
  (`handler.go:412-414`); history itself is `map[string][]SchemaVersion`
  (`source/position/schemahistory.go:68-70`).
- Shape identity: `HashColumnSet` over sorted `(name, dataType, typeModifier)`
  triples (`schemahistory.go:83-94`), deliberately identical to the RelationMessage
  diff identity (`:80-82`). `SchemaVersion` carries `ColumnSetHash` and
  `FirstSeenLSN` (`:60-66`). `RecordSchemaVersion` dedupes same-shape sightings
  (`:131-133`) and prunes oldest-first to `DefaultSchemaHistoryVersions = 10`
  (`:50`, `:143-145`).
- `buildPosition` at `handler.go:359-366`; call sites only `:196`, `:251`, `:285`.
  `basePosition` doc `handler.go:54-67`; `setBasePositionLowWatermark` `:375-377`.
- Position format: `CurrentPositionVersion = 1` (`position.go:54`), additive-only
  contract `:35-53`; `ToSDKPosition` stamps the version on every serialization
  (`:112-121`).
- Subscription plumbing: `handleXLogData` passes `xld.WALStart` as the message LSN,
  skips `WALStart <= StartLSN` (`source/logrepl/internal/subscription.go:254-280`,
  skip at `:260-263`), and records the handler's returned `writtenLSN` as
  `walWritten` when `> 0` (`:270-277`). `Ack` stores `walFlushed` (`:284-287`).
  `sendStandbyStatusUpdate` hard-fails on `walFlushed > walWritten` at
  `subscription.go:402-404` ("walWrite (%s) should be >= walFlush (%s)") and gates
  the "reply with server WAL end" fast path on `walFlushed == walWritten` at `:408`.
- Iterator delivery: `nextRecordsBatchBlocking` (`source/logrepl/cdc.go:202-224`)
  selects among `ctx.Done()`, `sub.Done()`, and `batchesCh` — Go selects randomly
  among ready cases, so a subscription stop can be surfaced before a marker batch
  sitting in the channel. `CDCIterator.Ack` requires `TypeCDC` (`cdc.go:274-295`)
  and a non-zero LSN (`:289-291`).
- Snapshot handoff re-seeds the handler's base position before `StartSubscriber`
  (`source/logrepl/combined.go:303`, `useCDCIterator`; initial seed at `:215`),
  so a marker's `SnapshotLowWatermarkLSN` survives handoffs the same way every
  other CDC position does.
- Engine ordering (from the B0 kill-harness doc's §3, itself citing engine commit
  `045f283` and #2680): the engine persists a record durably *before* acking it,
  and acks arrive to the connector in FIFO order per source. Below a marker's LSN,
  every position was acked in order; above it, nothing was emitted.
- Error conventions today: no error codes anywhere in the connector. Plain
  `fmt.Errorf` (`source/config.go:88`, `source/logrepl/combined.go:71`), sentinel
  errors (`source/snapshot/fetch_worker.go:58-60`), pgerrcode passthrough
  (`source/logrepl/internal/error.go:24-27`). The protocol has no structured
  error-code field (`conduit-connector-protocol@v0.9.5/pconnector`); the SDK's only
  sentinels are `ErrBackoffRetry` and `ErrUnimplemented`
  (`conduit-connector-sdk@v0.14.1/error.go:22,26`). Engine-side, conduit errors are
  stable dot-separated codes (`pkg/foundation/cerrors/conduiterr/conduiterr.go:106-122`,
  e.g. `connector.plugin_not_found`).
- `IsNarrowing` (`source/logrepl/internal/schemadiff.go:97-111`): returns true for
  `ColumnDropped` *or* `ColumnTypeChanged` — any type change, including widening
  `varchar(10)` -> `varchar(20)`. The docstring at `:97-103` says so honestly
  ("removes or retypes"); the name is the misnomer. Test evidence:
  `schemadiff_test.go:72-81` labels "ALTER COLUMN length only (TypeModifier)"
  `wantNarrow: true`. Call sites: `handler.go:482` (log), `handler.go:385`
  (comment), `schemadiff_test.go:122,152`.
- The connector's own destination would treat a keyless `OperationCreate` marker
  as data: `destination.go`'s `handleInsert` on a keyless record "will plainly
  insert the data". The marker therefore carries no payload (see Decision).
- Avro schema and the shape hash move together for some types only:
  `source/schema/avro.go:97` passes `f.TypeModifier` into type extraction, so a
  numeric precision/scale change alters the Avro schema, while a varchar length
  change alters the hash but not the Avro schema ("string" either way). Any
  evolve-path widening decision must be made per-type against the Avro rules, not
  globally.

## Decision

### D1. The escape hatch is a checkpoint-before-halt marker record

On detecting drift that stops the pipeline (halt policy, and the evolve-policy
narrowing cases routed to halt), the connector emits exactly one marker record,
then surfaces the halt error only after that marker is acked. On restart, the
carried schema history makes the new shape the last durable version, the diff
machinery classifies the re-sent RelationMessage as `driftNone` (or
`driftInitial` on a wiped position), and the pipeline resumes. Restart is the
operator's acknowledgment.

Marker record contract:

- Operation `opencdc.OperationCreate`; key `nil`; payload `nil`. Metadata carries
  the evidence (keys follow the `postgres.snapshot.resumed` precedent,
  `source/snapshot/iterator.go:40`):
  - `postgres.schema.drift=true`
  - `postgres.schema.drift.table=<namespace.table>` (the `relationKey`,
    `handler.go:412-414`)
  - `postgres.schema.drift.lsn=<relation message LSN>`
  - `postgres.schema.drift.policy=<halt|dlq|evolve>`
  - `postgres.schema.drift.narrowing=<true|false>` (only when an in-process diff
    is available, i.e. `driftInProcess`)
  - `postgres.schema.drift.diff=<ColumnChange.String()>` (only `driftInProcess`;
    `schemadiff.go:70-83` is the operator-facing diff text)
- The record's position is `buildPosition(relationMessageLSN)` — i.e.
  `SchemaHistory` already contains the new shape (`handleRelation` recorded it at
  `handler.go:460`) and `LastLSN` is the RelationMessage's own LSN. This is the
  invariant-1-safe boundary: pgoutput sends a RelationMessage before any DML that
  references the relation, so below this LSN everything was acked in FIFO order
  and above it nothing was emitted. Acking the marker advances the slot's
  `confirmed_flush_lsn` to exactly the point the connector has seen, and no
  further.

### D2. Handle must return the marker's LSN

`Subscription.handleXLogData` only advances `walWritten` when the handler returns a
`writtenLSN > 0` (`subscription.go:270-277`). A marker path that returns no LSN
would leave `walWritten` at the last data record's LSN, and the next
`sendStandbyStatusUpdate` after the marker's ack would hit the hard
`walFlushed > walWritten` guard (`subscription.go:402-404`) and kill the
subscription. The drift arm therefore returns the RelationMessage's LSN when it
emits a marker. (Also required for the status-update "reply with server WAL end"
gate at `:408` to behave.)

### D3. The halt error is acked-gated, not sighting-gated

`nextRecordsBatchBlocking` (`cdc.go:202-224`) can surface `sub.Done()` before a
marker batch that is already in `batchesCh`; a halt delivered on the sighting
alone could be acknowledged before the marker record is ever acked, silently
dropping the checkpoint and leaving the wedge intact. The halt state machine:

1. `handleRelation` detects drift that stops records: record the version in memory
   (`handler.go:460`), build the marker, hand it to the iterator's batch channel,
   set a `driftHalted` flag in the handler, and return the RelationMessage LSN
   (D2).
2. `nextRecordsBatchBlocking` keeps serving batches (the marker, then nothing).
3. `CDCIterator.Ack` on the marker position (TypeCDC, non-zero LSN — `cdc.go:274-295`,
   `:289-291`) is what arms the halt: from that point `NextN` returns the drift
   error instead of records.
4. After arming, the iterator serves no further records; DML for the drifted
   table is skipped, not emitted (D4).

The engine persists the marker before acking it (engine `045f283`), so acked-gated
halt is proof the checkpoint is durable — the exact property the wedge was
missing. The marker may be acked on a later pipeline run after a crash (see FM1);
that is correct, not a leak.

### D4. Between marker and halt, skip — never emit

Once the marker is emitted, DML for the drifted relation must be dropped, not
converted. The alternative — emitting old-shape projections — would write data
through a schema the connector no longer believes in, i.e. silent mangling under
invariant 6. Skipping is safe for resumption: every position up to and including
the marker's LSN is at or below the first new-shape DML, so a restart resumes
before any new-shape record; the skipped DML is re-read and skipped again, and
the operator's restart-approval admits it.

### D5. Halt is terminal and coded, with the revert trap in the message

The drift error is a stable, machine-readable string; it is not
`ErrBackoffRetry` (no retry — see FM5). Style: engine dot-codes, embedded at the
message start, since the protocol carries no structured code field:

```
postgres.schema_drift.halt: table "public.users": column "age" added (type 23);
observed at LSN 0/16B3748. Restart this pipeline to approve the change, or revert
the DDL — a restart after reverting halts once more before resuming (the approved
shape is still the last checkpointed one). The first record under the new schema
shape is not delivered (dropped at the wire on restart — the marker emits at the
boundary DML's LSN); verify it manually.
```

For `driftAcrossRestart` the column diff is unrecoverable (`handler.go:485-500`);
the message names the table and the hash transition plus `FirstSeenLSN` of the
last durable shape (`schemahistory.go:63-65`) and tells the operator to compare
against their DDL history — the parent doc's promise of a precise diff holds only
for `driftInProcess`, and the doc must not overpromise (finding F7).

The final sentence is the Blocker-2 disclosure: the first DML under the new
shape is not delivered. In-process the trigger DML is skipped by D4 (the marker
emits at its LSN, then nothing); across a restart the boundary DML is dropped at
the wire — the subscription resumes exactly at the marker's LSN, and the resume
guard skips the transaction starting there (`subscription.go:258-262`). The
operator approves by restarting and must verify that record manually.
**Decision (2026-08-30): disclose for v0.20.** The alternative — no-drop,
re-delivering the boundary record before the marker — would put a new-shape
record ahead of the marker in the resume stream and un-make the marker's
"approval checkpoint" invariant; it is filed as a follow-up rather than shipped.

### D6. Approval state for dlq rides the same marker

Whatever the step-3 dlq mechanism turns out to be, its "drift acknowledged" state
persists via a marker carrying the ack metadata, so a restart does not re-route an
already-approved drift to the DLQ. The dlq acknowledgement endpoint from the
parent doc (`20260724-dbz3-postgres-cdc-parity.md`, observability section, "an
acknowledgment endpoint") reduces to the restart for halt; for dlq the endpoint
itself is the approval and the marker is its durability carrier.

### D7. No position format bump

The marker reuses existing fields only (`LastLSN`, `SchemaHistory`,
`SnapshotLowWatermarkLSN`), all additive and omitempty (`position.go:56-84`,
`:47-53`). `CurrentPositionVersion` stays 1 (`position.go:54`). A legacy connector
reading a position written after a marker degrades gracefully: it sees a newer
`LastLSN` and history it ignores. The upgrade path is unchanged.

### D8. No locking change

`handleRelation` runs on the single subscription goroutine (`Subscription.listen`
-> `handleXLogData` -> `Handler`, `subscription.go:182-228`, `:254-280`) and is the
only writer of the base position (`handler.go:445-450`). The marker is built and
queued on that same goroutine; `buildPosition` reads on the record path and
mutates nothing. No new synchronization. The `chaospoint` seam (`combined.go:275`,
`subscription.go:410-419`) is the test injection point for FM1-FM3.

## Resolved questions

### Q1. What error-code convention do the connector's existing errors follow?

None — the connector has no error-code pattern today. Precedents, all verified:
`fmt.Errorf("invalid url: %w")` in `source/config.go:88`; `errors.Join` of
`fmt.Errorf("missing key for table %q", ...)` in `source/logrepl/combined.go:71-75`;
package-level sentinels in `source/snapshot/fetch_worker.go:58-60`; pgerrcode
passthrough in `source/logrepl/internal/error.go:24-27`. The protocol
(`conduit-connector-protocol@v0.9.5/pconnector`) has no structured code field, and
the SDK exports only `ErrBackoffRetry` / `ErrUnimplemented`. So B1 introduces the
connector's first coded error: a stable dot-separated token embedded at the start
of the message string, engine-style (`conduiterr.go:106-122`), terminal (never
`ErrBackoffRetry`), exactly as D5 specifies. Decision: adopt D5's
`postgres.schema_drift.halt` token with the D5 message template, and extend the
convention to the `driftInProcess`/`driftAcrossRestart` variants via the same
prefix.

### Q2. `IsNarrowing` — is the name wrong or the semantics?

Both, in different directions. The name is wrong: any type change, including
widening, is "narrowing" (`schemadiff.go:104-111`; `schemadiff_test.go:72-81`
labels a varchar(10)->varchar(20) length change `wantNarrow: true`). The semantics
are deliberately conservative — "any drop or retype blocks evolve" — and the
docstring owns it (`schemadiff.go:97-103`). But the parent design doc's evolve
intent ("compatible type widening per the existing Avro schema-compatibility
rules", design doc lines 331-333) requires a narrower judgement than "any type
change": per `source/schema/avro.go:97`, TypeModifier feeds Avro extraction, so a
numeric precision change alters the Avro schema while a varchar length change does
not — "compatible" must be decided per-type against the Avro rules, and
`IsNarrowing`'s blanket block would reject widenings that evolve could accept.
Decision, with timing: rename `IsNarrowing` to `IsIncompatible` (or
`BlocksEvolve`) in the prep PR, before B1 — a mechanical no-op at call sites
`handler.go:482`, `handler.go:385`, `schemadiff_test.go:122,152`; ship the semantic
refinement (Avro-compat judgement) with B1's evolve implementation, where it gets
test coverage.

### Q3. How is SchemaHistory keyed, and does the escape hatch need LSN in the ack key?

Keyed table-only: `SchemaHistories` is `map[string][]SchemaVersion` keyed by
`namespace.table` (`schemahistory.go:68-70`; key construction at `handler.go:412-414`),
with `FirstSeenLSN` per version recording *when* a shape appeared
(`schemahistory.go:60-66`). Do not add LSN to the key. The history is deliberately
LSN-agnostic because pgoutput re-sends a RelationMessage at a *new* LSN on
reconnect with no DDL at all — a test proves same-shape-new-LSN is `driftNone`
(`source/logrepl/schemahistory_test.go:138-155`). LSN-keyed history would classify
every reconnect as drift. The LSN that matters is the marker's own `LastLSN` = the
RelationMessage's LSN (D1), which is the gap-free resume boundary; it lives in the
position, not in the history key. Decision: keep table-only keys; the escape hatch
adds nothing to the key and nothing to the format (D7).

## Failure-mode analysis

- **FM1 — Approval-by-crash window.** SIGKILL lands between the engine persisting
  the marker and the engine observing the connector's ack (or between the marker
  entering the batch channel and the ack). Restart resumes at or below the marker
  LSN, re-derives the drift, and emits the marker again (dedupe: same-shape
  sighting is a no-op per `schemahistory.go:131-133`; the second marker may still
  be emitted). Net effect: the drift is approved without the operator ever seeing
  the halt. Accepted, with two mitigations: (a) the marker metadata
  (`postgres.schema.drift.*`) makes the approval observable — an operator
  inspecting the pipeline sees the marker record and its position; (b) the engine
  never acks before durable persist, so the crash cannot approve *more* than one
  marker's worth of history, and DML past the marker LSN is still re-read on
  restart. This is a genuine new risk the parent doc's "restart" phrasing hid; it
  is the price of "restart is the acknowledgment" and it is bounded.
- **FM2 — Crash between marker persist and halt delivery (the same window, halt
  side).** Worst case the operator sees one marker and a stop that looks like a
  normal crash. Restart resumes cleanly (the drift is now the last durable shape).
  Diagnostic: the marker's metadata + `FirstSeenLSN` in the position explain what
  was approved and when. Covered by SIGKILL tests (Acceptance).
- **FM3 — Crash between position-write and halt, at the handler.** The position
  that carries the new shape is written only when the engine persists the marker
  record; there is no separate position-write step, so invariants 2 and 5 (atomic,
  crash-safe positions) are untouched — the marker is a record like any other and
  inherits the engine's persist-before-ack path. The `handleRelation`-only window
  (version recorded in memory, marker not yet emitted) is benign: restart forgets
  the in-memory version and re-derives the same drift, which is exactly the status
  quo ante of the wedge — no worse, no approval.
- **FM4 — Revert after approval: two halt cycles.** Operator restarts (approval)
  and then reverts the DDL. The approved new shape is still the last checkpointed
  version; the revert's RelationMessage diffs against it and halts again — one
  extra halt cycle, then restart approves the reverted shape and the pipeline
  resumes. Not a loop: each halt is a distinct, user-initiated approval. Must be
  in the halt message (D5) so operators do not mistake it for the wedge
  returning.
- **FM5 — Retry loops.** The drift error must not be `ErrBackoffRetry`: the SDK
  retries it (`conduit-connector-sdk@v0.14.1/error.go:22`), and a retried halt
  would re-arm against a position that has not moved, cycling forever while the
  marker sits acked. Terminal error, exactly once.
- **FM6 — Marker delivered to a destination.** A keyless, payload-less
  `OperationCreate` is still a record; the connector's own destination "will
  plainly insert the data" (`destination.go` `handleInsert`) — with no payload,
  behavior is empty-row insert or error depending on the destination. This is an
  acceptance-test decision, not assumed: the test pins what happens to the marker
  at a real destination (expected: an explicit, documented no-op/error — never a
  silent wrong row). For non-data destinations (e.g. a stream sink) the marker
  arrives as an explicit, filterable event, which is the point.
- **FM7 — `driftAcrossRestart` halt without a diff.** The message cannot name
  columns (`handler.go:485-500`); it names the table, both hashes, and
  `FirstSeenLSN`. Overpromising the parent doc's diff is finding F7; the message
  template (D5) is honest.
- **FM8 — Stacked DDLs between sighting and halt.** The subscription keeps
  streaming between the marker's emission and its ack (D3 step 2). A second DDL on
  the same table within that window is classified as drift but is not committed to
  the history — the FM8 guard skips before any commit, so the shape cannot leak
  into an unrelated record's position and dedupe the drift away on a restart
  (re-review should-fix) — and only the marker's version is checkpointed. On
  restart the newer shape diff shows
  `driftAcrossRestart` against the checkpointed one — the pipeline halts again for
  the second DDL. Correct behavior (no silent admission of the second DDL); the
  halt message's LSN correlation (`FirstSeenLSN`) tells the operator what
  happened. An alternative — emit one marker per sighting — is rejected: it turns
  a burst of DDL into a burst of approvals, and the wedge is only about the *first*
  durable checkpoint.
- **FM9 — Marker under the snapshot->CDC handoff.** The marker is a CDC-mode
  record; positions carry `SnapshotLowWatermarkLSN` through the re-seed
  (`combined.go:303`), so a marker acked around the handoff is coherent. A marker
  *before* the handoff cannot exist — drift detection only runs once CDC handles
  RelationMessages, after the handoff.
- **FM10 — Slot pruning.** Acking the marker advances `confirmed_flush_lsn` to
  the RelationMessage LSN, below which everything was acked in FIFO order — the
  invariant-1-safe boundary from D1. Postgres may prune WAL up to it; nothing
  below it is ever re-read after approval, by construction.
- **FM11 — Marker record ordering.** The marker is a single record in a FIFO
  batch channel on the subscription goroutine; it cannot overtake a prior record's
  position, and nothing after it is emitted (D4). Ordering guarantees per
  source-partition (invariant 4) are unchanged: the marker is itself a
  per-partition record with a monotonic LSN.
- **FM12 — Bounded history still bounded.** The marker carries at most one new
  version per table (dedupe, `schemahistory.go:131-133`; prune to 10,
  `:143-145`). Marker bursts cannot grow the position payload unboundedly.

## Alternatives considered

- **A. The wedge as-is ("halt; operator re-seeds")**: restart halts forever; the
  operator's only exits are revert-the-DDL (which the connector re-detects after
  restart — the wedge again, since the reverted shape is still what the connector
  believes in... only a position wipe + re-snapshot escapes) or manual position
  surgery. No operator-friendly approval path exists today. Rejected; the wedge is
  the bug being fixed.
- **B. Config knob (`schemaDrift.approve=true`)**: escapes the wedge without a
  record, but (a) requires a restart-with-config to apply and another to revoke —
  the restart the marker achieves in one step is now two plus a config edit;
  (b) it is approve-by-default while set, so a DDL that lands while the knob is
  on is silently admitted — the exact silent-mangling failure invariant 6 forbids;
  (c) it leaves no evidence trail in the data. Rejected.
- **C. Emit new-shape records into a side "drift" stream**: two streams from one
  source, new positions for the side stream — the SDK `Source` model has no
  second-stream primitive, and the engine's DLQ story for sources is not built
  (see Problem/scope). Rejected for now; revisit if the dlq policy needs it.
- **D. Operator API ack endpoint for halt (parent doc's phrasing)**: the parent
  doc's "acknowledgment endpoint" assumes a running, queryable connector — but a
  halted connector's API is exactly what B1 does not want to keep alive, and the
  endpoint would still need a durable carrier (the marker) to survive restarts.
  The marker makes the restart itself the ack (D1, D6); the endpoint remains
  necessary only for dlq.
- **E. Emit marker as a tombstone/other operation**: `OperationDelete` with no
  key is worse than `OperationCreate` with no payload — destinations and
  processors have delete-specific semantics (schema-aware deletes) that would
  guess about a key that does not exist. `OperationCreate` + nil payload is the
  most inert shape the record model offers.

## Upgrade / rollback

- **Upgrade:** additive-only position fields; `CurrentPositionVersion` unchanged
  (D7). Existing positions carry no history; first RelationMessage is
  `driftInitial`, no marker, no halt — identical to today's behavior. Pipelines
  running a pre-B1 build that halt mid-drift are unaffected; B1's first run after
  upgrade emits the marker and resumes. No config migration.
- **Rollback:** a position written post-marker (with history for the new shape)
  read by a pre-B1 build: unknown fields ignored (`position.go:47-53`), the build
  sees only `LastLSN`, resumes at the marker LSN — the drift is already admitted
  at the WAL level regardless of build. The marker record itself is inert at the
  destination only if the operator's destination handles payload-less
  `OperationCreate` as tested (FM6); rolling the connector back does not retract
  an approved drift — expected: restart *is* approval.
- **Deprecation:** none. The marker metadata keys are new; no existing metadata
  is changed.

## Acceptance criteria (run on the B0 harness, `chaospoint` seam)

1. Halt-on-drift, no crash: pipeline halts with the D5 terminal error, exactly one
   marker emitted, marker position = RelationMessage LSN, history durable
   (restart resumes, `driftNone`).
2. Wedge regression: restart after (1) does NOT halt (the wedge is fixed).
3. SIGKILL in each of the FM1-FM3 windows (after marker persist before ack; after
   marker in channel before ack; after in-memory record before marker emission):
   restart resumes, no duplicate markers that grow history, no loss (invariants
   1-3). The FM1 window is asserted to be *observable* (marker metadata present),
   not prevented.
4. Ack-gated halt: a halt is never surfaced before its marker is acked; a
   `sub.Done()` race against `batchesCh` (`cdc.go:202-224`) does not drop the
   marker — deterministic test via the `chaospoint` seam.
5. Revert-after-approval: exactly two halt cycles, then clean resume (FM4), with
   the revert-trap sentence present in both halt messages.
6. Marker at the connector's own destination: pinned behavior, no silent wrong
   row (FM6); withAvroSchema=false explicitly (routes around the open Avro bug
   #326 noted in the B0 doc) and with the drift test table configured in
   `TableKeys` so the marker never trips key validation (`combined.go:69-73`).
7. `driftAcrossRestart` halt message contains table, hash transition, and
   `FirstSeenLSN`; never fabricates a column diff (FM7).
8. Marker position survives the snapshot->CDC handoff (FM9): scenario runs
   snapshot-then-drift and asserts `SnapshotLowWatermarkLSN` is present on the
   marker's position.
9. Stacked-DDL window (FM8): second DDL before marker ack produces exactly one
   marker and a second halt on restart for the second shape — no silent admission.
10. Race detector clean on the marker path (single subscription goroutine, D8).

## Adversarial review

Findings from the pre-commit adversarial pass (fresh-context review of the wedge
framing and of the parent doc's citations):

- **F1 — "The wedge blocks all three policies" is wrong.** dlq and evolve flow
  records, so positions advance and history checkpoints. The wedge is a property
  of halt (and of evolve's narrowing cases, which route to halt). Corrected in
  Problem/scope. The dlq policy does have a durability gap — approval state — and
  D6 covers it.
- **F2 — "No operator recovery path exists" is overbroad.** Revert-the-DDL and
  position-wipe + re-snapshot both exist; what is missing is a *cheap* approval
  path. Corrected in Alternatives A.
- **F3 — Stale citation, parent doc line 110** ("always before the next DML
  referencing that relation", cited to `handler.go:126-128`): the relation arm is
  at `handler.go:143-147`; `:126-128` is inside `flush()`. Corrected here; the
  ordering claim itself holds (pgoutput protocol).
- **F4 — Stale citation, parent doc lines 111-113** ("`RelationSet.Add`
  unconditionally overwrites", cited to `relationset.go:41-43`): `Add` is
  test-only; production shape updates go through `RelationSet.Update`
  (`source/logrepl/internal/relationset.go:198-207`). The diff claim in the parent
  doc survives; the citation does not.
- **F5 — Stale citation, parent doc lines 357-359** (`buildPosition` cited to
  `handler.go:312-318`): it is at `handler.go:359-366` today. Corrected here.
- **F6 — Marker-drop race.** The parent doc's implicit "halt right after
  detecting" cannot work: `nextRecordsBatchBlocking` selects randomly among ready
  cases (`cdc.go:202-224`), so the halt can be surfaced before the marker is
  acked. D3 (ack-gated halt) is the fix.
- **F7 — `driftAcrossRestart` cannot name columns** (`handler.go:485-500`); the
  parent doc's halt-message diff promise holds only for `driftInProcess`. D5's
  template is honest; the parent doc's wording should be softened on merge.
- **F8 — The draft's "no config change escapes it" premise** is true only of the
  naive halt (no marker, no knob). With the escape hatch, a config knob *would*
  be a config escape; rejected anyway (Alternatives B). The premise survives as a
  description of the naive design's failure, not as a constraint on the fix.
- **F9 — The draft file this amendment replaces did not exist** at the assigned
  scratchpad path (session directory was cleaned up); this document is the first
  written form of the amendment, authored after the verification pass rather than
  corrected against it.

## Related

- Parent design: `docs/design-documents/20260724-dbz3-postgres-cdc-parity.md`
  (Area 2 drift policy; halt/evolve/dlq decisions at lines 543-574; "restart to
  re-establish baseline" at lines 327-336).
- B0 plan: `docs/design-documents/20260821-dbz3-b0-kill-harness.md` (chaospoint
  seam §4; SIGKILL matrix §8; engine persist-before-ack §3).
- Implemented foundations: #318 versioned position (59480f7), #319 watermark +
  handoff re-seed (6fb4dd0), #321 resumed-snapshot degraded mode (845f048),
  #322 drift detection (77466c0), #323 durable history (35e45d1), #324 chaospoint
  seam (e765bc9), #325 harness core (0dbf995).
- Precedent for metadata keys: `postgres.snapshot.resumed`
  (`source/snapshot/iterator.go:40`).
- Ack/durability invariants: conduit engine `045f283` (persist-before-ack) and
  #2680 (connector-level FIFO ack sequence), as documented in the B0 doc §3.
