# Debezium goldens

Inputs of `TestSource_DebeziumGolden` (`source_debezium_golden_test.go`).

- `sql/01-schema.sql`, `sql/02-stream.sql`: the golden workload, copied unchanged
  from ConduitIO/conduit `tests/debezium/sql` at commit `de14091d`.
- `golden/<version>/<converter>/events.jsonl`: the streaming events of the
  captures of real Debezium 2.7.4.Final and 3.7.0.Final in ConduitIO/conduit
  `tests/debezium/testdata/golden` at the same commit, for the `json-numeric`
  and `json-txmeta` converters. Each line is a line of the capture, unchanged.
  Snapshot events (`op: r`), the transaction topic and the message topic are
  left out, because this version of the source does not produce them.

- `sql/03-temporal.sql` and `golden/<version>/<converter>/temporal.jsonl`:
  infinite, BC and five-digit-year temporal values, which the conduit workload
  does not have. Captured on 2026-10-15 with the same pinned images
  (2.7.4: `sha256:5a62d0fb…1d04`, 3.7.0: `sha256:65a33b21…dd6e`) by running
  `tests/debezium/capture.sh` at conduit `de14091d` with `03-temporal.sql`'s
  table added to `01-schema.sql` and its inserts placed before the sentinel
  insert of `02-stream.sql`. The rest of that capture was identical to the
  committed goldens (op, key, before, after, transaction orders), so these
  lines extend them. They belong upstream in the conduit goldens.

To refresh, re-capture in the conduit repository (`tests/debezium/capture.sh`)
and filter the new `events.jsonl` files the same way.
