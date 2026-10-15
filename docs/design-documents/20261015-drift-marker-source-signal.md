# Recognise drift markers by a source-only signal

## Summary

The destination skips and acks schema-drift markers (FM6 in
`20260829-dbz3-b1-schema-drift-escape-hatch.md`). On `main` it recognises a
marker by the metadata key `postgres.schema.drift=true` alone. Any record that
carries the key, a real row included, is acked and never written: silent loss
(invariant 3) and an ack without a durable write (invariant 1). The key is
ordinary metadata, so a processor, another source or a pass-through hop can
set it. #358 narrowed the check to the marker's shape. This design adds the
signal DeVaris asked for on 2026-10-15: **a marker is skipped only when its
position says the Postgres source produced it as a marker.**

Tier 1: it changes what the destination acks without writing, and it adds a
field to the source's position.

## Context

- The marker is `NewRecordCreate(position, metadata, nil, nil)`: no key, no
  payload. Its position is a CDC position whose change key is one below the
  change that decided the drift (#338).
- Conduit refuses a processor that changes a record's position
  (`pkg/lifecycle/stream/processor.go`, `handleSingleRecord`: "processor
  changed position ... not allowed"). In a Conduit pipeline the destination
  therefore sees exactly the position bytes the source connector produced.
- A record that reached the destination through another system (Kafka, a
  file) carries that system's position, never one this source wrote.

## Decision

1. The source stamps the marker's position with
   `schema_drift_marker: "<namespace.table>"` (position field
   `SchemaDriftMarker`). No other position carries it.
2. The destination skips and acks a record only when **all** of these hold:
   - metadata `postgres.schema.drift=true`;
   - the #358 shape: `OperationCreate`, empty key, empty `Payload.Before` and
     `Payload.After`;
   - the record's position parses as this connector's CDC position whose
     `schema_drift_marker` equals the metadata's `postgres.schema.drift.table`.
3. A record with the metadata and any key, payload or other operation is a
   real record: it is written, with one aggregated WARN per batch (#358).
4. A record with the metadata and the marker's shape **but no verified
   position** fails the batch with
   `postgres.destination.drift_marker_unverified`, nothing acked. It cannot be
   written correctly (an empty `DEFAULT VALUES` row is a wrong row) and it
   cannot be proved to be a marker. The fix text tells the operator to drop
   markers before the hop that loses their position (a `filter` processor on
   `postgres.schema.drift`), or to remove the metadata if it was set by
   mistake.

## Alternatives

- **Metadata only (`main`).** Rejected: loss and spoofing.
- **Metadata plus shape (#358).** A record with no key and no payload carries
  no row data, so skipping it loses nothing. But it trusts metadata, which is
  exactly what was asked not to be the proof, and it silently swallows a
  malformed upstream record instead of surfacing it.
- **A keyed MAC (HMAC over the position) with a shared secret.** Unforgeable,
  but source and destination are separate connector instances with no shared
  secret, often in different pipelines. It needs key distribution and
  rotation for a no-op record. Rejected as disproportionate: the position is
  already source-only inside a Conduit pipeline.
- **Write unverified markers as rows.** Rejected: FM6, an empty wrong row.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| Real record carrying the metadata | Written; WARN. No loss |
| Spoofed empty record carrying the metadata, no marker position | Batch fails with `drift_marker_unverified`; nothing acked; loud |
| Genuine marker forwarded through Kafka to a Postgres destination | Same refusal; operator filters markers upstream. Previously skipped |
| Genuine marker in a Postgres-to-Postgres pipeline | Skipped and acked, as before |
| A position-rewriting engine (arch-v2 allows a processor to rewrite `Record.Position`) | A processor with that power can already break acks; it could also forge the field. The record still has no data, so nothing is lost |
| Marker from a source build without the field (older connector upstream) | Refused, loudly. Upgrade the source, or filter markers |

## Upgrade and rollback

- The position format gains one additive, `omitempty` field, under the
  existing contract (`position.go`: additive only, keyed on presence).
  `CurrentPositionVersion` becomes 3. Older builds ignore the field; the
  source never carries it forward (only the marker's own position has it).
  A marker position with the field, read back as a resume position (the
  approving restart), resumes exactly as before. Tests pin both.
- Behaviour change for destinations fed by markers whose position was lost
  on the way: they now halt instead of skipping. Release note.
- Rollback: revert. Positions written with the field are read by the
  previous build, which ignores it.

## Observability

- Debug for each verified marker skipped; WARN per batch for real records
  with the metadata; the coded error for an unverified marker.
