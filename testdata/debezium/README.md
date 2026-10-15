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

To refresh, re-capture in the conduit repository (`tests/debezium/capture.sh`)
and filter the new `events.jsonl` files the same way.
