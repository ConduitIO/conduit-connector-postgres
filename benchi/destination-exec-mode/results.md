# Results

## 2026-10-15, EC2 c7i.2xlarge: the run of record

| | |
| --- | --- |
| Instance | c7i.2xlarge, on demand, dedicated to this run, us-west-1a (8 vCPU, Intel Xeon Platinum 8488C) |
| Software | Amazon Linux 2023 (`ami-0bc4193249c3c9538`), Docker 25.0.16, Compose 2.39.4, Go 1.26.9, benchi v0.4.1, Conduit v0.20.2, Postgres 17 (data on tmpfs) |
| Commits | base `bd44a41` (cache and cache-aa), option 1 `589a259` (describe), option 2 `44a6097` (opt-out; same code as `95e843e`) |
| Binaries | `binaries.sha256`: `main` and `main2` are byte-identical |
| Method | 4 rounds × 4 arms per batch size, Latin-square order, 1 warmup discarded, a 60 s window per run (measured 60.6 s) after the pipeline writes and 15 s more |
| Host load | 1-minute load median 3.5, max 4.1, over the whole run (`loadavg.log`); nothing else ran on the instance |
| Teardown | `teardown-verify.txt`: every resource NotFound or terminated, nothing else carries the run's tag |
| Cost | about 54 minutes at $0.4452/h on demand, about $0.40 (EBS and S3 under a cent) |

Raw data: `runs/20261015-ec2-c7i.2xlarge/` (`runs.csv` row counts, `rates.csv` rows per second, `environment.txt`).

Rows per second, the median of 4 runs (min to max):

| Arm | batch size 1 | vs cache | batch size 1000 | vs cache |
| --- | --- | --- | --- | --- |
| cache (base) | 8,413 (8,314 to 8,447) | | 18,591 (18,559 to 18,626) | |
| cache-aa (base again) | 8,368 (8,352 to 8,423) | −0.5% | 18,534 (17,701 to 18,577) | −0.3% |
| describe (option 1) | 5,437 (5,401 to 5,479) | **−35.4%** | 15,699 (15,657 to 16,268) | **−15.6%** |
| opt-out (option 2, `cache_statement` URL) | 8,389 (8,373 to 8,413) | −0.3% | 18,089 (17,654 to 18,545) | −2.7% |

Per-round A/A deltas (cache-aa against cache in the same round): batch 1:
−0.9%, +1.3%, −0.9%, −0.3%; batch 1000: −4.9%, −0.3%, −0.3%, −0.1%. The
coefficient of variation is at most 0.7% at batch 1 and 2.8% at batch 1000.

What this supports:

- **describe_exec costs throughput** beyond the noise floor and beyond the 10%
  regression gate at both batch sizes: about a third at batch size 1, the
  default, and about a sixth at 1000. The ranges do not overlap the cache arms.
- **The opt-out recovers it.** Option 2 with an explicit `cache_statement` URL
  is within the A/A spread of the base at both batch sizes (−0.3% and −2.7%,
  against an A/A run that was 4.9% off once).
- Only one pipeline shape was measured (generator, then one keyed upsert per
  record into a two-column table, on one machine). Other shapes and tables
  were not measured.

## 2026-10-15, local laptop, stopped: no claim

A first local run was stopped in round 2 when the shared host's load went
from about 11 to 38. Its numbers are kept in the git history of this file;
they are not used.
