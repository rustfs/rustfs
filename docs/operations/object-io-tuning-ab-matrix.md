# Object I/O (GET/PUT) tuning A/B matrix runbook

**Use this when:** you want to measure the effect of changing one `RUSTFS_*` runtime knob at a time against a fixed binary, or you need the catalog of GET/PUT tuning knobs, their defaults, and the stage metric that validates each.
**Source of truth:** `crates/io-metrics/src/lib.rs` (stage histogram names and stage tokens); `crates/config/src/constants/object.rs` and `crates/ecstore/src/erasure/coding/encode.rs` (knob defaults); `crates/ecstore/src/set_disk/mod.rs` (codec-streaming rollout and engine defaults); `rustfs/src/app/object/get.rs` (GET experimental switches).

Scope: **parameter tuning** with a fixed binary. The code-change A/B gate that compares a baseline binary against a candidate binary is [`hotpath-warp-ab-runbook.md`](hotpath-warp-ab-runbook.md); when a knob change turns out to need a code change, validate it there and come back here for the knob-level sweep.

## 1. Questions this runbook answers

1. Which stage is actually slow — `set_disk_encode`, `set_disk_rename`, `metadata_fanout`, `bitrot_verify`, ...?
2. Is the knob the real bottleneck, or is the stage slow for another reason?
3. Does widening the knob buy throughput without an unacceptable memory (RSS) or tail-latency regression?

For a main-effect comparison, vary one factor per A/B cell. The fsync worker experiment below also includes a declared two-factor combination to measure interaction after both individual factors have been compared.

## 2. Prerequisites

Host, `warp`, and the no-log / no-monitor baseline environment are as in [`hotpath-warp-ab-runbook.md`](hotpath-warp-ab-runbook.md). Two additions: the observability metrics runtime must be **enabled** (the stage histograms are not emitted when `RUSTFS_OBS_METRICS_EXPORT_ENABLED=false`), and the data set must be disposable — recreate the bucket per run.

Load driver and gate are reused, not reimplemented:

| Script | Role |
| --- | --- |
| `scripts/run_object_batch_bench_enhanced.sh` | warp driver with rounds, median aggregation, `baseline_compare.csv`, Prometheus service-metric capture |
| `scripts/hotpath_warp_ab_gate.sh` | relative budget gate over the deltas |
| `scripts/run_hotpath_warp_ab.sh` | orchestrator when a knob needs the full baseline-vs-candidate treatment (e.g. two different defaults) |

## 3. Fixed test conditions (lock before you start)

Record these per run; a result without them is not reproducible:

```text
nodes, disks_per_node, total_disks, cpu_per_node, mem_per_node,
network, erasure_set_drive_count, endpoint_mode (direct|lb),
rustfs_commit_sha, warp --version, durability mode
```

| Workload | mode | sizes |
| --- | --- | --- |
| small-fixed | put / get | 4KiB, 100KiB |
| ec-boundary | put / get | 1MiB, 4MiB |
| large-stream | put / get | 10MiB, 16MiB, 32MiB |
| mixed | mixed | 256KiB |

Concurrency ladder `8, 16, 32, 64` (add `96, 128` on a bigger rig); duration `120s`, `--rounds >= 3`, cooldown `>= 30s`. Record whether scanner deep-verify, heal, replication, lifecycle transition, and periodic capacity refresh are on rather than assuming they are off.

## 4. Measurement stack

Drive each cell with the stage histograms emitted by `crates/io-metrics/src/lib.rs`; the stage and path label tokens are defined there and are not repeated here.

| Metric | Labels | Use |
| --- | --- | --- |
| `rustfs_s3_put_object_stage_duration_ms` | `stage` | P50/P95/P99 per PUT stage (`app_*`, `ingress_prepare`, `set_disk_*`, `multipart_*`) |
| `rustfs_io_get_object_stage_duration_seconds` | `path`, `stage` | Per GET stage, split by read path (`legacy_duplex`, `codec_streaming`, ...) |
| `rustfs_ec_encode_inflight_bytes_current` | — | EC encode memory pressure; pair with node RSS and CPU |

Host telemetry, collected alongside every cell:

```bash
pidstat -durh 5 > telemetry/pidstat.txt &
mpstat 5 > telemetry/mpstat.txt &
iostat -xz 5 > telemetry/iostat.txt &
```

## 5. Tuning knob catalog

### 5.1 PUT

| Knob | Default | Controls | Validating stage | Risk if widened |
| --- | --- | --- | --- | --- |
| `RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES` | 32MiB (`crates/ecstore/src/erasure/coding/encode.rs`) | EC encode producer/consumer memory budget (blocks queued between encode and shard write) | `set_disk_encode` P95 + `rustfs_ec_encode_inflight_bytes_current` | RSS growth under high concurrency |
| `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST` | `true` | Streaming encode reads into an owned, EC-sized buffer, avoiding the input-to-encoder block copy. Set `false` to select the previous streaming Vec ingest path; batched encode always uses owned buffers. Cached at first use. | `erasure_encode_cpu`, PUT throughput and RSS | No disk-format change; queue budget does not bound total process memory |
| `RUSTFS_OBJECT_IO_BUFFER_SIZE` | 128KiB (`crates/config/src/constants/object.rs`) | Streaming read-in / write-out block size | `ingress_prepare`, `set_disk_encode` | Larger buffers = fewer polls, more resident memory |
| `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE` | 4MiB (`crates/config/src/constants/object.rs`) | Duplex pipe capacity (shared; PUT uses it less than GET) | `set_disk_encode` feed smoothness | Memory per in-flight request |
| `RUSTFS_DURABILITY_MODE` / `RUSTFS_DRIVE_SYNC_ENABLE` | mode-dependent | Per-shard fsync/sync discipline on commit | `set_disk_rename` P99 | Weakening it changes the durability contract — a deliberate tradeoff, never a free win |
| `RUSTFS_RUNTIME_WORKER_THREADS` / `RUSTFS_RUNTIME_MAX_BLOCKING_THREADS` | Tokio defaults | Async workers + `spawn_blocking` pool feeding per-block encode | `set_disk_encode` P95 + mpstat | Oversubscription |

#### Fsync worker idle policy experiment

The disk fsync queue exposes two independent experimental controls for measuring idle CPU cost. Both are cached at first queue creation and require restarting every participating server when changed. The default spin budget is 256 iterations per empty-queue episode, with multiple idle workers allowed.

| Variable | Default | Values and effect |
| --- | --- | --- |
| `RUSTFS_EXPERIMENTAL_FSYNC_WORKER_IDLE_SPIN_ITERS` | `256` | Spin budget per empty-queue episode, clamped to `0..=1024`. `0` immediately rechecks retirement under the queue mutex. Invalid values use the default. Set `1024` to restore the longer spin budget. |
| `RUSTFS_EXPERIMENTAL_FSYNC_WORKER_KEEP_MULTIPLE_IDLE` | `true` | `false` retires an empty fsync worker when another idle/reserved worker already exists. Invalid values use the default. |

These controls affect the foreground fsync lane's idle policy. Its maximum execution concurrency remains eight workers per disk, and the namespace and isolated cleanup lanes retain their own policy. The queue's internal idle count includes worker reservations awaiting execution, so measure actual worker and spin activity through the diagnostics below.

Use one fixed binary and the four cells below to distinguish the two factors and their interaction. Keep effective bucket durability at `strict`, and record inline admission, versioning, EC layout, CPU quota, storage mapping, background activity and build identity for each run. A historical release comparison also requires fixing that release's source and build inputs.

| Cell | Spin iterations | Keep multiple idle workers |
| --- | --- | --- |
| Current default | 256 | true |
| Longer spin | 1024 | true |
| Single idle reservation | 256 | false |
| Longer spin, single idle reservation | 1024 | false |

Before A1, record the acceptable baseline drift, minimum gain above measurement noise, and p99, RSS and error limits. Verify A/A stability, then compare each candidate with the default through A1/B1/B2/A2. Include sustained fresh 100 KiB PUT and short burst/idle/burst traffic. Track the dev-to-1.0.5 change and the beta.10 gap as separate outcomes.

Collect detailed diagnostics in dedicated rounds with `RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED=true` and metrics export enabled. Include `app_put_commit_owner_submit_to_start`, the `set_disk_rename_remote_client_*` preparation stages, `disk_prepared_metadata_fdatasync` for inline `xl.meta`, `disk_fsync_worker_dispatch_wait`, `disk_fsync_worker_execute`, destination and ancestor directory sync stages, and actual worker/spin activity. The app owner timer includes submission and runtime scheduling before the outer spawned commit task starts. The lower `put_object_commit_submit_to_closure_enter` timer covers a different ECStore owner, so the two timers must not be added as one wait. Remote-client preparation is split into JSON compatibility encoding, msgpack encoding, client acquisition, and canonical request-body/digest preparation. Completed errors are included in those four sub-stage samples, while cancellation can drop an in-flight timer. Request construction and metric publication remain in the preparation total outside the four sub-stages, so their histograms can have different sample counts and must not be added or subtracted to reconstruct an unmeasured residual. Operation execution timers cover the job closure; idle-spin diagnostics cover the loop between jobs. Heavy syscall tracing and on/off-CPU profiling belong in these diagnostic rounds. Use the same lightweight observation settings for all acceptance rounds.

| Metric | Meaning |
| --- | --- |
| `rustfs_disk_fsync_workers_current` / `rustfs_disk_fsync_workers_peak` | Actual started fsync workers across the process, including workers executing or waiting inside a job and workers spinning between jobs. Queued worker reservations are excluded. |
| `rustfs_disk_fsync_spinning_workers_current` / `rustfs_disk_fsync_spinning_workers_peak` | Workers in the measured idle-spin phase across all disk fsync lanes. A zero spin budget bypasses this phase. |
| `rustfs_disk_fsync_idle_spin_episodes` / `rustfs_disk_fsync_idle_spin_iterations` | Completed measured spin episodes and actual `spin_loop` calls, aggregated once per episode. |
| `rustfs_disk_fsync_idle_spin_duration_ms` | Elapsed time in the spin loop, including any descheduling. Interpret alongside on-CPU profiles. |

These metrics cover the disk queue's Fsync lane. Direct fsync dispatches and cleanup work on the Isolated lane require their own stage observations. Peak gauges are process-lifetime high-water marks for workers whose diagnostics started while the detailed PUT gate was enabled, including startup and warmup. A retiring closure and its replacement can briefly overlap in this count while execution concurrency remains bounded. Restart each server per leg to establish a fresh lifetime. Guards finish accounting on normal exit and panic, including when the gate is disabled while a measured worker remains alive. Diagnostics use atomic accounting and cached metric handles outside the disk queue mutex; the recorder may acquire its own publication locks. Hold this observation overhead consistent within diagnostic comparisons.

### 5.2 GET

| Knob | Default | Controls | Validating stage | Risk if enabled |
| --- | --- | --- | --- | --- |
| `RUSTFS_GET_CODEC_STREAMING_ROLLOUT` | `off` (`crates/ecstore/src/set_disk/mod.rs`) | Switches the read path from `legacy_duplex` to the pull-based `ErasureDecodeReader` (`codec_streaming`) | `path="legacy_duplex"` vs `path="codec_streaming"` for `decode`/`emit`/`output_lock_wait`/`stripe_read` | Behavioral change to the read path |
| `RUSTFS_GET_CODEC_STREAMING_ENGINE` | `legacy` | `legacy` vs `rustfs` decode engine under the streaming reader | `reconstruct`/`decode` per `path` | Engine swap on a correctness-critical path |
| `RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE` | `false` | Multipart objects on the streaming reader | Same, multipart cells | Wider format coverage |
| `RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE` (+ `_MAX_SIZE`, `_FIRST_READER_SETUP`) | `false` / 512KiB | Prefer data-shard readers before parity | `stripe_read_first_shard`/`stripe_read_quorum` | Shard-selection order change |
| `RUSTFS_OBJECT_GET_SKIP_BITROT_VERIFY` | `false` | Skip per-shard HighwayHash verify | `bitrot_verify` | **Never default on** — measures the theoretical ceiling only |
| `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE` | 4MiB | Legacy GET in-process pipe capacity | `output_lock_wait`/`output_poll` | Memory per in-flight GET |
| `RUSTFS_GET_SEEK_BUFFER_ENABLE` [^latched] | `false` | Serves small GETs through an in-memory seek buffer (seek support without re-reading the object); gates `should_buffer_get_object_in_memory_with_threshold` | `first_byte` | Experimental; read by `scripts/run_get_1mib_abba_stage_metrics.sh` |
| `RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE` [^latched] | `false` | Attributes output-handoff timing to the `response_handoff` stage (metrics only) | `response_handoff` | Small per-request bookkeeping cost; set by `scripts/run_get_codec_streaming_smoke.sh` and `scripts/test_get_1mib_abba_stage_metrics.sh` |

[^latched]: Both switches are startup-latched `OnceLock` booleans in `rustfs/src/app/object/get.rs` (`ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE` / `is_get_seek_buffer_enabled`, `ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE` / `is_get_output_handoff_attribution_enabled`), read once via `get_env_bool(.., false)`; changing a value requires a process restart. They exist to support A/B runs, are referenced only by the harness scripts named above, and are kept deliberately — do not remove either switch or the seek-buffer path as dead code. Leave both unset in production. The codec-streaming switches above are startup-latched as well.

## 6. A/B matrix

Run each row as an independent cell. Baseline is the shipped default; candidate is one knob moved. Everything else (topology, sizes, concurrency, rounds, durability) stays fixed.

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

## 7. Execution and interpretation

1. Freeze the conditions in §3 and record the provenance block.
2. Run the baseline cell (all defaults) and capture stage histograms plus host telemetry.
3. Pick the **one** most-likely knob from the decision table below (large-object PUT is almost always `set_disk_encode` → P1; GET is G1), sweep its candidate column one value at a time on the same workload, and stop widening as soon as the stage stops moving — the knob is then not the bottleneck.
4. Keep `baseline_compare.csv`, `median_summary.csv`, stage histograms, and host telemetry per cell; the conclusion must trace back to them. Record results in the issue tracker, not the repo.

Driver invocation for one PUT cell:

```bash
scripts/run_object_batch_bench_enhanced.sh \
  --tool warp --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" --secret-key "$RUSTFS_SECRET_KEY" \
  --bucket rustfs-put-tuning --warp-mode put \
  --sizes 16MiB,32MiB --concurrency 32 --duration 120s --rounds 3 \
  --out-dir target/bench/put-tuning-p1-64mib
```

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

Read/write quorum, bitrot verify, `xl.meta` validation, and durability are integrity contracts, not knobs: P5 and G6 are ceiling measurements and must be labelled as such. A throughput win with unbounded RSS growth is a regression — RSS and the EC in-flight gauge are part of every PUT result.
