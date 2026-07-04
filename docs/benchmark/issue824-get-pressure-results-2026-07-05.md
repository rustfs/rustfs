# Issue 824 GET Pressure Validation Results

Date: 2026-07-05

## Scope

This run validates the current branch `houseme/get-object-performance-hardening`
against the closest local historical GET metrics gate baseline.

Historical baseline:

- Path: `docs/benchmark/rustfs-target-bench/get-metrics-gate-baseline/warp/median_summary.csv`
- Git head: `92c50156a3cbbdfa454be1d14062098708851f51`
- Parameters: `10MiB`, warp, concurrency `32`, duration `10s`, rounds `3`
- Median throughput: `5734882344.960000` B/s
- Median req/s: `546.920000`
- Median latency: `48.700000` ms

Current run:

- Path: `target/bench/issue824-get-metrics-gate-current-20260705`
- Git head recorded by script: `55ad8df1c2f574178669052093aa185fae8185ca`
- Branch/worktree contained staged and unstaged issue-824 changes.
- Parameters: `10MiB`, warp, concurrency `32`, duration `10s`, rounds `3`
- RustFS binary: `target/release/rustfs`
- Observability export: disabled
- Scanner: disabled
- Command:

```bash
scripts/run_get_metrics_gate_smoke.sh \
  --skip-build \
  --rustfs-bin target/release/rustfs \
  --size 10MiB \
  --concurrency 32 \
  --duration 10s \
  --rounds 3 \
  --round-cooldown-secs 1 \
  --baseline-csv docs/benchmark/rustfs-target-bench/get-metrics-gate-baseline/warp/median_summary.csv \
  --out-dir target/bench/issue824-get-metrics-gate-current-20260705 \
  --data-root /private/tmp/issue824-get-metrics-gate-current-20260705
```

## Raw Rounds

| round | throughput | req/s | avg latency | p90 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 6505.75 MiB/s | 650.57 | 47.9 ms | 73.0 ms | 103.9 ms |
| 2 | 5631.35 MiB/s | 563.13 | 52.1 ms | 80.1 ms | 106.3 ms |
| 3 | 5830.30 MiB/s | 583.03 | 52.9 ms | 72.2 ms | 92.7 ms |

Corrected median from raw round data:

- Median throughput: `6113512652.800000` B/s (`5830.30 MiB/s`)
- Median req/s: `583.030000`
- Median latency: `52.100000` ms
- Median p90: `73.000000` ms
- Median p99: `103.900000` ms

## Baseline Comparison

| metric | baseline | current | delta |
| --- | ---: | ---: | ---: |
| throughput B/s | 5734882344.960000 | 6113512652.800000 | +6.60% |
| req/s | 546.920000 | 583.030000 | +6.60% |
| avg latency ms | 48.700000 | 52.100000 | +6.98% |

Interpretation:

- No throughput regression was observed in this local release-binary smoke.
- Latency moved higher by 6.98% against the selected baseline. This is not a
  blocking regression by itself because host state, cooldown, and branch state
  differ, but it should be watched in the PR benchmark matrix.
- The run validates that the current changes do not obviously collapse the
  default 10MiB/C32 GET hot path.

## Artifact Caveat

The script completed with exit code `0`, but its generated
`warp/median_summary.csv` and `warp/baseline_compare.csv` contain `N/A` values.
The terminal summary and `warp/round_results.csv` preserved the actual round
values. The corrected medians above are computed from those raw round logs.

Follow-up: fix `scripts/run_object_batch_bench_enhanced.sh` CSV parsing/report
generation so the persisted median and baseline comparison match the terminal
summary.
