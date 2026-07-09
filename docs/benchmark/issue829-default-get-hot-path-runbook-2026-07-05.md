# Issue 829 Default GET Hot Path Benchmark and Attribution Runbook

Date: 2026-07-05

## Scope

This runbook defines the evidence package for proving the current default GET
hot path. It intentionally separates throughput evidence from path attribution:

- Metrics-off runs are throughput evidence.
- Metrics-on diagnostic runs are path attribution evidence.
- Codec and direct-memory experiments are A/B evidence, not default-path proof
  unless their enabling environment is recorded.

The current review baseline is:

- Ordinary non-inline GETs are expected to use ECStore metadata/cache lookup and
  the legacy duplex reader path.
- Metadata early-stop is not expected for normal `read_data` GETs.
- Direct-memory, codec streaming, and metadata early-stop are opt-in or gated.
- Diagnostic metrics add overhead and must not be used as pure throughput proof.
- Metadata cache is topology-sensitive: distributed erasure currently bypasses
  the metadata GET cache, so distributed baselines should assume full metadata
  fanout unless a later change records otherwise.
- Rollout percentage is not the primary safety gate for codec streaming or
  metadata early-stop in the current defaults; record the base enable flag and
  compatibility switches before interpreting any percentage value.
- Codec-streaming cost accounting must include both shard-to-output-buffer and
  output-buffer-to-client copies, plus the `+ Sync` reader wrapper lock cost.

## Artifact Contract

Every run directory must keep enough information to reproduce or reject the
result:

- `git_head` or `git rev-parse HEAD`
- branch name and dirty-tree status
- full script command and arguments
- RustFS binary path and build mode
- object size, object class, bucket, concurrency, duration, rounds
- metric mode: metrics-off throughput or metrics-on attribution
- environment affecting GET path selection
- `manifest.env` and/or `environment.txt`
- `warp/median_summary.csv` and `warp/round_results.csv` for throughput runs
- `service_metrics_summary.csv`, `service_metrics_stage_distribution.csv`, or
  raw before/after Prometheus snapshots for attribution runs

Do not compare a metrics-on run against a metrics-off run as a throughput win or
loss. The only valid cross-mode claim is attribution.

## Baseline Dimensions

Object sizes:

- `1KiB`
- `4KiB`
- `10KiB`
- `64KiB`
- `100KiB`
- `128KiB`
- `1MiB`
- `10MiB`
- `128MiB`

Object classes:

- default inline candidate
- forced non-inline candidate
- plain single-part
- multipart
- range request
- versioned bucket
- checksum-mode request
- CORS request

Run the small default-path proof first. Expand into large, range, multipart, and
versioned dimensions only after the small matrix has stable artifacts.

## Step 0: Script Syntax Gate

```bash
for s in \
  scripts/run_get_codec_streaming_smoke.sh \
  scripts/run_get_metrics_gate_smoke.sh \
  scripts/run_object_batch_bench_enhanced.sh \
  scripts/run_object_data_cache_bench.sh
do
  bash -n "$s"
done
```

## Step 1: Metrics-Off Throughput Smoke

Use this run only for throughput and latency. Observability export and detailed
GET stage attribution are disabled by the script.

```bash
scripts/run_get_metrics_gate_smoke.sh \
  --skip-build \
  --size 1KiB \
  --concurrency 32 \
  --duration 10s \
  --rounds 3 \
  --out-dir target/bench/get-default-metrics-off-1kib-c32
```

Required checks:

- `target/bench/get-default-metrics-off-1kib-c32/manifest.env` records
  observability export as disabled.
- `warp/median_summary.csv` and `warp/round_results.csv` exist.
- No statement about reader path is made from this run alone.

## Step 2: Metrics-On Default Path Attribution

Use this run to prove the default reader path. It may be slower than the
metrics-off smoke and that is expected.

```bash
scripts/run_get_codec_streaming_smoke.sh \
  --mode legacy \
  --sizes 1KiB,4KiB,10MiB \
  --concurrency 32 \
  --duration 20s \
  --rounds 3 \
  --diagnostic-metrics \
  --diagnostic-metrics-filter-regex 'rustfs[._]get_object[._](reader_path|stage_duration|reader_setup|disk_permit|request|reader)' \
  --out-dir target/bench/get-default-attribution-legacy
```

Required checks:

- `manifest.env` records `mode=legacy` and diagnostic metrics enabled.
- `service_metrics_summary.csv` exists.
- The default path proof reports the delta for
  `rustfs_io_get_object_reader_path_total{path="legacy_duplex"}`.
- If `legacy_duplex` is not positive, report the observed path instead of
  forcing the expected conclusion.

## Step 3: Small-Object Matrix

Run the small sizes with metrics-off first:

```bash
for size in 1KiB 4KiB 10KiB 64KiB 100KiB 128KiB 1MiB; do
  scripts/run_get_metrics_gate_smoke.sh \
    --skip-build \
    --size "$size" \
    --concurrency 32 \
    --duration 10s \
    --rounds 3 \
    --out-dir "target/bench/get-default-metrics-off-${size}-c32"
done
```

Then run attribution only for representative sizes:

```bash
scripts/run_get_codec_streaming_smoke.sh \
  --mode legacy \
  --sizes 1KiB,10KiB,128KiB,1MiB \
  --concurrency 32 \
  --duration 20s \
  --rounds 3 \
  --diagnostic-metrics \
  --diagnostic-metrics-filter-regex 'rustfs[._]get_object[._](reader_path|stage_duration|reader_setup|disk_permit|request|reader)' \
  --out-dir target/bench/get-default-small-attribution
```

## Step 4: Large Sequential and Range Matrix

Use the dedicated GT1G helper for very large sequential/ranged GETs when the
target machine has enough local disk and time:

```bash
scripts/run_gt1g_get_http_matrix.sh \
  --skip-build \
  --out-dir target/bench/get-default-large-http-matrix
```

For a shorter release-gate matrix:

```bash
scripts/run_get_codec_streaming_smoke.sh \
  --mode legacy \
  --sizes 10MiB,128MiB \
  --concurrency 16 \
  --duration 20s \
  --rounds 3 \
  --out-dir target/bench/get-default-large-metrics-off
```

Add `--diagnostic-metrics` only in a separate attribution run.

## Step 5: Codec Streaming A/B Matrix

Codec streaming is not default-path proof. Use it only after the default legacy
path has been proven.

```bash
scripts/run_get_codec_streaming_smoke.sh \
  --mode both \
  --profile-order reverse \
  --codec-engine rustfs \
  --sizes 1MiB,10MiB,128MiB \
  --concurrency 16 \
  --duration 20s \
  --rounds 3 \
  --diagnostic-metrics \
  --out-dir target/bench/get-codec-ab-attribution
```

Required checks:

- Legacy profile has a positive `legacy_duplex` path delta.
- Codec profile has a positive `codec_streaming` path delta only when codec
  settings are enabled.
- Compatibility summaries pass before any performance conclusion is accepted.

## Step 6: Admission and Permit Fallback Stress

Use this to validate workload admission visibility under pressure:

```bash
RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=1 \
RUSTFS_OBJECT_DISK_PERMIT_WAIT_TIMEOUT=1 \
scripts/run_get_codec_streaming_smoke.sh \
  --mode legacy \
  --sizes 10MiB \
  --concurrency 64 \
  --duration 20s \
  --rounds 3 \
  --diagnostic-metrics \
  --diagnostic-metrics-filter-regex 'rustfs[._]get_object[._]disk_permit[._]bypass|rustfs_io_disk_permit|rustfs_io_queue_|rustfs_io_get_object_' \
  --out-dir target/bench/get-permit-admission
```

Required checks:

- `rustfs.get_object.disk_permit.bypass.total` or its exported equivalent is
  captured when fallback happens.
- Queue/admission metrics are present in the attribution snapshot.
- Slow-client behavior does not deadlock the benchmark.

## Reporting Template

Use this structure in the final issue or PR comment:

```text
Commit:
Branch:
Dirty tree:
Host:
RustFS binary:
Metric mode:
Command:
Out dir:

Throughput evidence:
- median req/s:
- median latency:
- p90:
- p99:

Attribution evidence:
- reader_path legacy_duplex delta:
- reader_path codec_streaming delta:
- top stage durations:
- disk permit fallback delta:

Conclusion:
- Default path:
- Compatibility:
- Follow-up:
```

## Acceptance Checklist

- Metrics-off throughput and metrics-on attribution are separate directories.
- Every artifact directory records commit, env, object layout, and arguments.
- Default path hit rate is explicitly reported from diagnostic metrics.
- Any default-path claim cites the metric delta used to prove it.
- Codec/direct-memory/metadata-early-stop claims include their enabling env.
