# Object I/O (GET/PUT) tuning A/B matrix runbook

> Scope: **parameter tuning** — measuring the effect of changing one
> `RUSTFS_*` runtime knob at a time, against a fixed binary. This is
> deliberately different from the code-change A/B gate in
> [`hotpath-warp-ab-runbook.md`](hotpath-warp-ab-runbook.md) and the formal
> ABBA validation in
> [`hotpath-warp-abba-runbook.md`](hotpath-warp-abba-runbook.md), which compare
> a baseline binary against a candidate binary.
>
> When a knob change turns out to need a code change, use those two runbooks
> for the code-level validation and come back here for the knob-level sweep.

## 1. What this runbook answers

For each tuning knob it answers three questions:

1. Which stage is actually slow — `set_disk_encode`, `set_disk_rename`,
   `metadata_fanout`, `bitrot_verify`, etc.?
2. Is the knob the real bottleneck, or is the stage slow for another reason?
3. Does widening/loosening the knob buy throughput without an unacceptable
   memory (RSS) or tail-latency regression?

The core discipline is **one variable per A/B cell**. Never change two knobs in
the same cell, or the result is unexplainable.

## 2. Prerequisites

- Linux bench host (or an ansible-managed cluster); a laptop smoke run is too
  noisy to decide anything.
- `warp` on `PATH` (or pass `--warp-bin` to the driver).
- The observability metrics runtime **enabled**. The stage histograms below are
  not emitted when `RUSTFS_OBS_METRICS_EXPORT_ENABLED=false` or the runtime is
  otherwise off — see
  [`hotpath-warp-ab-runbook.md`](hotpath-warp-ab-runbook.md) for the no-log /
  no-monitor baseline env.
- A warm, disposable data set. Recreate the bucket per run; do not bench against
  production data.

Load driver and gate are reused, not reimplemented:

- `scripts/run_object_batch_bench_enhanced.sh` — warp driver with rounds,
  median aggregation, `baseline_compare.csv`, and Prometheus service-metric
  capture.
- `scripts/hotpath_warp_ab_gate.sh` — relative budget gate over the deltas.
- `scripts/run_hotpath_warp_ab.sh` — optional orchestrator when a knob needs
  the full baseline-vs-candidate treatment (e.g. two different defaults).

## 3. Fixed test conditions (lock before you start)

Record these per run; a result without them is not reproducible:

```text
nodes, disks_per_node, total_disks, cpu_per_node, mem_per_node,
network, erasure_set_drive_count, endpoint_mode (direct|lb),
rustfs_commit_sha, warp --version, durability mode
```

Workload matrix (the same shapes the hotpath gate uses, expanded for the
stage-breakdown object sizes):

| Workload | mode | sizes |
| --- | --- | --- |
| small-fixed | put / get | 4KiB, 100KiB |
| ec-boundary | put / get | 1MiB, 4MiB |
| large-stream | put / get | 10MiB, 16MiB, 32MiB |
| mixed | mixed | 256KiB |

Concurrency ladder: `8, 16, 32, 64` (add `96, 128` on a bigger rig). Duration
`120s`, `--rounds >= 3`, cooldown `>= 30s`.

Isolate background noise before the sweep: scanner deep-verify, heal,
replication, lifecycle transition, periodic capacity refresh — record whether
each is on rather than silently assuming it is off.

## 4. Measurement stack

The code already instruments every stage below. Drive each A/B cell with these
histograms (names verified against `crates/io-metrics/src/lib.rs`):

- PUT stages: `rustfs_s3_put_object_stage_duration_ms{stage=...}` — compute
  P50/P95/P99 per stage. Stages: `app_bucket_validate`, `app_sse_config_lookup`,
  `app_object_lock_config_lookup`, `app_put_opts_build`, `app_prelookup`,
  `ingress_prepare`, `app_encryption_prepare`, `app_replication_decision`,
  `app_store_put`, `app_post_store_bookkeeping`, `app_capacity_update`,
  `set_disk_writer_setup`, `set_disk_encode`, `set_disk_rename`,
  `set_disk_old_data_cleanup`.
- GET stages: `rustfs_io_get_object_stage_duration_seconds{path=..., stage=...}` —
  the `path` label separates the read paths: `legacy_duplex`, `codec_streaming`,
  `direct_memory`, `body_cache`, `inline_direct`, `internal_meta`,
  `remote_transition`, `set_disk`, `empty`. Stages: `metadata`,
  `metadata_cache_lookup`, `metadata_fanout`, `metadata_resolve`, `object_info`,
  `path_decision`, `quorum_reached`, `range`, `reader_setup`,
  `stripe_read`, `stripe_read_first_shard`, `stripe_read_quorum`, `decode`,
  `reconstruct`, `emit`, `fill`, `output_poll`, `output_lock_wait`,
  `bitrot_verify`, `first_byte`, `full_body`, `response_handoff`,
  `lock_acquire`.
- EC memory pressure: `rustfs_ec_encode_inflight_bytes_current` and the
  allocator reclaim gauge; plus node RSS and CPU.

Host telemetry (collect alongside every cell):

```bash
pidstat -durh 5 > telemetry/pidstat.txt &
mpstat 5 > telemetry/mpstat.txt &
iostat -xz 5 > telemetry/iostat.txt &
```

## 5. Tuning knob catalog

Defaults are verified against `crates/config/src/constants/object.rs` and
`crates/ecstore/src/erasure/coding/encode.rs`.

### 5.1 PUT

| Knob | Default | Controls | Validating stage | Risk if widened |
| --- | --- | --- | --- | --- |
| `RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES` | 32MiB | EC encode producer/consumer memory budget (blocks queued between encode and shard write) | `set_disk_encode` P95 + `rustfs_ec_encode_inflight_bytes_current` | RSS growth under high concurrency |
| `RUSTFS_OBJECT_IO_BUFFER_SIZE` | 128KiB | Streaming read-in / write-out block size | `ingress_prepare`, `set_disk_encode` | Larger buffers = fewer polls, more resident memory |
| `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE` | 4MiB | duplex pipe capacity (shared, but PUT path uses it less than GET) | `set_disk_encode` feed smoothness | Memory per in-flight request |
| `RUSTFS_DURABILITY_MODE` / `RUSTFS_DRIVE_SYNC_ENABLE` | mode-dependent | per-shard fsync/sync discipline on commit | `set_disk_rename` P99 | Weakening it changes the durability contract — treat as a deliberate tradeoff, not a free win |
| `RUSTFS_RUNTIME_WORKER_THREADS` / `RUSTFS_RUNTIME_MAX_BLOCKING_THREADS` | Tokio defaults | async workers + `spawn_blocking` pool feeding per-block encode | `set_disk_encode` P95 + mpstat | Oversubscription |

### 5.2 GET

| Knob | Default | Controls | Validating stage | Risk if enabled |
| --- | --- | --- | --- | --- |
| `RUSTFS_GET_CODEC_STREAMING_ROLLOUT` | `off` | switches the read path from `legacy_duplex` to the pull-based `ErasureDecodeReader` (`codec_streaming`) | compare `path="legacy_duplex"` vs `path="codec_streaming"` for `decode`/`emit`/`output_lock_wait`/`stripe_read` | behavioral change to the read path; rollout is `off` by default for a reason |
| `RUSTFS_GET_CODEC_STREAMING_ENGINE` | `legacy` | `legacy` vs `rustfs` decode engine under the streaming reader | `reconstruct`/`decode` per `path` | engine swap on a correctness-critical path |
| `RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE` | `false` | multipart objects on the streaming reader | same, multipart cells | wider format coverage |
| `RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE` (+ `_MAX_SIZE`, `_FIRST_READER_SETUP`) | `false` / 512KiB | prefer data-shard readers before parity | `stripe_read_first_shard`/`stripe_read_quorum` | shard-selection order change |
| `RUSTFS_OBJECT_GET_SKIP_BITROT_VERIFY` | `false` | skip per-shard HighwayHash verify | `bitrot_verify` | **do not default on** — measures the theoretical ceiling only |
| `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE` | 4MiB | legacy GET in-process pipe capacity | `output_lock_wait`/`output_poll` | memory per in-flight GET |
| `RUSTFS_GET_SEEK_BUFFER_ENABLE` | `false` | in-memory seek buffer for small GET | `first_byte` | experimental, startup-latched — see [`get-path-experimental-switches.md`](get-path-experimental-switches.md) |
| `RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE` | `false` | adds `response_handoff` attribution (metrics only) | `response_handoff` | small per-request bookkeeping cost |

## 6. A/B matrix

Run each row as an independent cell. Baseline is the shipped default; candidate
is one knob moved. Everything else (topology, sizes, concurrency, rounds,
durability) stays fixed.

### 6.1 PUT

| # | Knob | Baseline | Candidates | Signal metric | Judgement |
| --- | --- | --- | --- | --- | --- |
| P1 | encode in-flight | 32MiB | 48MiB, 64MiB, 96MiB | `set_disk_encode` P95 + throughput + RSS | throughput up AND `set_disk_encode` down AND RSS tolerable → budget was the binding constraint |
| P2 | object I/O buffer | 128KiB | 256KiB, 512KiB, 1MiB | `ingress_prepare` + `set_disk_encode` | encode smooths with bounded RSS → upstream feed was too small |
| P3 | duplex buffer | 4MiB | 8MiB, 16MiB | `set_disk_encode` feed variance | lower priority than P1/P2 |
| P4 | blocking threads | default | 512, 768, 1024 | `set_disk_encode` P95 + mpstat | per-block `spawn_blocking` is scheduler-bound if P95 falls |
| P5 | durability | current mode | weaker/stronger mode | `set_disk_rename` P99 | only as a **deliberate durability tradeoff**, never a silent default change |
| P6 | set drive count | current | other valid widths | all PUT stages | highest cost — only after P1–P5, and only on a rebuildable topology |

### 6.2 GET

| # | Knob | Baseline | Candidates | Signal metric | Judgement |
| --- | --- | --- | --- | --- | --- |
| G1 | codec streaming rollout | `off` | `on` (pct ramp 10/50/100) | `path="legacy_duplex"` vs `path="codec_streaming"` for `decode`/`emit`/`output_lock_wait`/`stripe_read` + throughput | streaming beats duplex on `decode`+`output_lock_wait` and byte-for-byte output matches → candidate for default |
| G2 | codec streaming engine | `legacy` | `rustfs` | `reconstruct`/`decode` per `path` | engine swap is neutral-or-better on CPU with identical bytes |
| G3 | multipart streaming | `false` | `true` | multipart GET cells | only after G1 is stable on single-part |
| G4 | data-blocks-first | `false` | `true` | `stripe_read_first_shard`/`stripe_read_quorum` | fewer shard reads without a correctness regression |
| G5 | duplex buffer | 4MiB | 8MiB, 16MiB | `output_lock_wait`/`output_poll` (legacy path) | only if still on `legacy_duplex` |
| G6 | skip bitrot verify | `false` | `true` | `bitrot_verify` | **ceiling measurement only**; do not carry into production |

## 7. Execution sequence

1. Freeze the conditions in §3 and record the provenance block.
2. Run the baseline cell (all defaults) and capture stage histograms + host
   telemetry.
3. Pick the **one** most-likely knob from the analysis. For large-object PUT
   that is almost always `set_disk_encode` → P1; for GET it is G1 (the
   `legacy_duplex` → `codec_streaming` switch).
4. Sweep that knob's candidate column one value at a time, same workload.
5. Read the decision table in §8; if the stage did not move, the knob is not
   the bottleneck — stop widening it and pick the next stage.

Driver invocation for one PUT cell:

```bash
scripts/run_object_batch_bench_enhanced.sh \
  --tool warp --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" --secret-key "$RUSTFS_SECRET_KEY" \
  --bucket rustfs-put-tuning --warp-mode put \
  --sizes 16MiB,32MiB --concurrency 32 --duration 120s --rounds 3 \
  --out-dir target/bench/put-tuning-p1-64mib
```

## 8. Interpretation / decision table

| Stage high | Most likely cause | Next action |
| --- | --- | --- |
| `set_disk_encode` | per-block EC encode scheduling + in-flight budget | P1 → P4 → P2, in that order |
| `set_disk_rename` | commit tail (rename fan-out / RPC / fsync) | P5 (durability) and cluster tail analysis, not encode |
| `set_disk_writer_setup` | per-disk `BitrotWriter` + temp-file create | disk/filesystem metadata; per-disk fan-out cost |
| `set_disk_old_data_cleanup` | overwrite / versioned-object directory delete | confirm overwrite-vs-new-write; defer cleanup further |
| `metadata_fanout` / `metadata_resolve` | cross-disk `xl.meta` read + quorum | metadata cache hit rate; small-object fixed cost |
| `bitrot_verify` | HighwayHash verify on the read path | G6 ceiling only; do not default on |
| `output_lock_wait` / `output_poll` | legacy duplex backpressure | G1 (move off duplex) or G5 |
| `stripe_read*` | shard concurrency / selection | G4 shard-selection, disk/network tail |

## 9. Guardrails

- **Never weaken correctness for throughput**: read/write quorum, bitrot verify,
  `xl.meta` validation, and durability (`RUSTFS_DURABILITY_MODE`) are integrity
  contracts, not knobs. P5 and G6 are ceiling measurements and must be labelled
  as such; do not carry their values into production without an explicit
  durability/correctness decision.
- **One variable per cell.** A cell that changes two knobs is thrown away.
- **Memory is part of the result.** A throughput win with unbounded RSS growth
  is a regression; record RSS and the EC in-flight gauge for every PUT cell.
- **Startup-latched knobs** (`RUSTFS_GET_SEEK_BUFFER_ENABLE`,
  `RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE`, and the codec-streaming
  switches) require a process restart to change — see
  [`get-path-experimental-switches.md`](get-path-experimental-switches.md).
- **Archive the raw data.** Keep the `baseline_compare.csv`, `median_summary.csv`,
  stage histograms, and host telemetry per cell; the conclusion must trace back
  to them. Do not commit benchmark result snapshots to the repo — record them in
  the issue tracker.
