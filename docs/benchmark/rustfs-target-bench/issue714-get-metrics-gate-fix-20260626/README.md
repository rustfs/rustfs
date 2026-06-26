# Issue 714 GET Metrics Gate Fix Benchmark

## Context

This directory archives the 10MiB GET A/B benchmark data captured on 2026-06-26 for the GET regression follow-up.

Benchmark shape:

1. Tool: `warp`
2. Object size: `10MiB`
3. Concurrency: `32`
4. Rounds: `3`
5. Server: local single-node RustFS test instance from `scripts/run_get_metrics_gate_smoke.sh`

## Run Directories

| Directory | Stage | Commit / Scope | Baseline |
|---|---|---|---|
| `get-metrics-gate-baseline` | Unfixed baseline | main-derived code before the local GET metrics gate fix | N/A |
| `get-metrics-gate-patched` | P0 | `0deda9690 perf(get): gate detailed GET metrics` | `get-metrics-gate-baseline` |
| `get-metrics-gate-p1` | P1 | `d59ce2ab9 perf(get): reduce seek buffering at concurrency` | `get-metrics-gate-patched` |
| `get-metrics-gate-stream-handoff` | P1 streaming | `8582fea48 perf(get): streamline reader response handoff` | `get-metrics-gate-p1` |

## Median Summary

| Stage | Median req/s | Median latency | Median throughput |
|---|---:|---:|---:|
| Unfixed baseline | `546.92` | `48.7ms` | `5734882344.96 B/s` |
| P0 metrics gate | `531.21` | `50.6ms` | `5570151055.36 B/s` |
| P1 concurrency-aware seek threshold | `532.63` | `48.5ms` | `5585019863.04 B/s` |
| P1 streaming handoff | `611.48` | `46.7ms` | `6411801067.52 B/s` |

## Key Comparisons

1. P0 vs unfixed baseline: `-2.87%` median req/s, `+3.90%` median latency.
2. P1 vs P0: `+0.27%` median req/s, `-4.15%` median latency.
3. P1 streaming handoff vs P1: `+14.80%` median req/s, `-3.71%` median latency.
4. P1 streaming handoff vs unfixed baseline: `+11.80%` median req/s, `-4.11%` median latency.

## Conclusion

The focused 10MiB/c32 benchmark did not validate the buffered seek-support aggregation experiment as a profitable path for this workload. After the concurrency-aware threshold change, 10MiB/c32 is already on the streaming path, so the stronger result came from reducing streaming response handoff overhead:

1. Replace the hot GET streaming response chain `ReaderStream -> bytes_stream -> StreamingBlob::wrap`.
2. Use a local `ByteStream` wrapper around `ReaderStream` that preserves exact remaining length and final chunk truncation.
3. Build the response with `StreamingBlob::new`, avoiding the extra async stream adapter.

CSV summaries, round results, and run manifests are preserved under each run directory.
