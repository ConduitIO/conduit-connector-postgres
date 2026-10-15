Synthetic fixtures, written by hand from the `getOffset()` of Debezium's
`PostgresOffsetContext` (3.x `main`, read 2026-10-12): the key names, which
keys appear in which state, and the value types. They are not captured from
a running Debezium. Real captures go in `testdata/<debezium-version>-<plugin>/`
and are produced by `testdata/capture/capture.sh`, which also checks the
design's LSN assumptions (a) and (b) against a probe slot.
