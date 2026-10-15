# Results

## 2026-10-15, local, stopped: no claim

Machine: Apple Silicon laptop, Docker Desktop (linux/arm64 VM), shared with
other agent lanes. Binaries: `bd44a41` (cache, twice) and `8e12206`
(describe).

The run was stopped in round 2. The host load went from about 11 to 38 when
another lane's test run started on the same machine, which contaminates every
run from then on (see conduit's `benchi/METHODOLOGY.md`: never benchmark next
to other work). Only batch size 1 was reached. Batch size 1000 was not run.

Rows written in the 60 s window (rows after minus rows before):

| run | arm | rows | rows/s | load before → after |
| --- | --- | --- | --- | --- |
| warmup | cache | 633,773 | 10,563 | 5.5 → 5.2 |
| r1 | cache | 640,916 | 10,682 | 5.2 → 11.2 |
| r1 | describe | 293,551 | 4,893 | 11.2 → 13.4 |
| r1 | cache-aa | 574,083 | 9,568 | 13.4 → 11.1 |
| r2 | describe | 310,943 | 5,182 | 11.1 → 11.4 |
| r2 | cache-aa | 197,955 | (excluded) | 11.4 → 38.4 |

The last row is excluded. Its pre-test count was 0, so the pipeline had not
started writing when the window opened, and the load reached 38 during the
run.

What this supports: nothing past a direction. With one usable A/A pair (10,682
vs 9,568, about 11% apart) the noise floor is not known. The two describe runs
are less than half of the cache runs at batch size 1, which is the expected
direction: one extra round trip per record. That is not a measured cost, and
the batch-1000 cost is unknown. A clean run needs an idle machine, or the
dedicated benchmark host (conduit#3014).
