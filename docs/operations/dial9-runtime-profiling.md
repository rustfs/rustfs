# dial9 Tokio Runtime Profiling

`dial9-tokio-telemetry` records Tokio runtime-level events — poll start/end,
worker park/unpark, task spawn/terminate, and optionally async backtraces of
stalled tasks — into binary trace segments.

It answers questions that Prometheus metrics and `tracing` spans cannot:

- Which task held a worker for 40 ms without yielding?
- Are workers parking because there is no work, or because the queue is starved?

On a plain warp workload with no injected fault it recorded single polls of
418–625 ms: real worker stalls that nothing else in the obs stack would surface.

## What it does not see

**A drive stall is invisible to dial9.** RustFS performs disk I/O on the blocking
pool (`spawn_blocking`) and through io_uring, never on an async worker, so a slow
drive does not lengthen any poll. Injecting 200 ms of latency on one of four
drives cut throughput by 64% and left the poll-duration distribution unchanged
(polls ≥ 5 ms: 49 → 56; p999: 2.67 ms → 2.75 ms). Enabling dial9's CPU and sched
profilers does not help either: sched events are captured per-worker only, and
the CPU profiler samples on-CPU, while a stalled drive is an off-CPU wait.

For drive stalls use the `rustfs_io_*` metrics and the drive-stall budget. dial9
answers a different question: which task held a worker, and for how long.

**It cannot tell you where a task was stuck.** That would need a task dump, and
dial9 only captures those for futures spawned through `dial9_tokio_telemetry::spawn`.
RustFS spawns with `tokio::spawn` throughout, so no task dump is ever recorded and
no configuration exposes one. Tracked as D9-16 in rustfs/backlog#1157.

## This is a profiler, not telemetry

Three properties follow from that, and all three matter operationally.

**The stock binary cannot run it.** dial9 hooks Tokio's unstable runtime API, which
requires `--cfg tokio_unstable`. Release binaries are built without it, so they neither
depend on Tokio's non-semver surface nor pay its cost. Setting
`RUSTFS_RUNTIME_DIAL9_ENABLED=true` on a stock binary logs a warning and records nothing.

**Traces are written continuously and evicted.** The retained budget is
`MAX_FILE_SIZE × ROTATION_COUNT` (1 GiB by default). Once exceeded, the *oldest*
segments are deleted. Measured on a single-node 4-drive cluster under warp mixed
(66 MiB/s, 110 obj/s, 32 concurrent): **13023 events/s, 0.16 MiB/s**, so the
default budget wraps after roughly **108 minutes**. Scale that by your own event
rate — it tracks poll rate, not object throughput — and prefer short, targeted runs.

**There is no runtime toggle.** Telemetry is installed when the Tokio runtime is
constructed, so enabling or disabling it requires a process restart.

## Building

```bash
make build-profiling
```

That is `cargo build --release --bin rustfs --features dial9` with
`RUSTFLAGS="--cfg tokio_unstable"`. There are no other telemetry features: task
dumps and S3 upload are both unavailable, for the reasons given above and below.

`crates/obs/build.rs` fails the build if the `dial9` feature is enabled without
`--cfg tokio_unstable`. This is deliberate: an environment `RUSTFLAGS` *replaces*
the value from `.cargo/config.toml` rather than appending to it, so the flag used
to disappear silently whenever anything else set `RUSTFLAGS`.

For CPU profiling with usable stacks, add `-C force-frame-pointers=yes`.

## Running an investigation

```bash
export RUSTFS_RUNTIME_DIAL9_ENABLED=true
export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-telemetry-investigation
export RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT=3      # 300 MiB total, ~short window
```

Reproduce the fault, then stop the process **gracefully**. The final buffered
events are flushed when the telemetry guard drops during shutdown; a `SIGKILL`
loses them, and they are usually the interesting ones.

Segments land in `$OUTPUT_DIR/$FILE_PREFIX.N.bin`. Analyse them with upstream's
`TRACE_ANALYSIS_GUIDE.md`.

Turn `RUSTFS_RUNTIME_DIAL9_ENABLED` back off when you are done.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `RUSTFS_RUNTIME_DIAL9_ENABLED` | `false` | Needs a `dial9` build to take effect |
| `RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR` | `/var/log/rustfs/telemetry` | Must be writable |
| `RUSTFS_RUNTIME_DIAL9_FILE_PREFIX` | `rustfs-tokio` | |
| `RUSTFS_RUNTIME_DIAL9_MAX_FILE_SIZE` | `104857600` | Bytes per segment |
| `RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT` | `10` | Total budget = size × count |
| `RUSTFS_RUNTIME_DIAL9_S3_BUCKET` | unset | **Not honoured**, see below |
| `RUSTFS_RUNTIME_DIAL9_S3_PREFIX` | unset | **Not honoured**, see below |

Variables whose build feature is absent are ignored, with a warning naming the
feature to rebuild with.

### S3 upload is unavailable

dial9's `worker-s3` feature depends on `aws-sdk-s3-transfer-manager` 0.1.3 (its
latest release), which pins `aws-smithy-http-client` onto `hyper-rustls` 0.24 and
`rustls-webpki` 0.101.7. That webpki carries RUSTSEC-2026-0098, -0099 and -0104,
and the repository's `cargo deny` gate rejects it.

Cargo's feature unification can add features but cannot drop a transitive
dependency, so this cannot be worked around downstream — it needs an upstream
release. RustFS therefore builds no S3 uploader at all. Setting the two variables
above logs a warning and changes nothing; retrieve trace segments from
`OUTPUT_DIR` directly. Tracked as D9-14 in rustfs/backlog#1157 and reported
upstream as [dial9-rs/dial9#659](https://github.com/dial9-rs/dial9/issues/659).

## Metrics

| Metric | Meaning |
|---|---|
| `rustfs_dial9_supported` | Binary was compiled with telemetry support |
| `rustfs_dial9_configured` | Operator asked for telemetry via the environment |
| `rustfs_dial9_active_sessions` | A session is actually recording (0 or 1) |
| `rustfs_dial9_disk_usage_bytes` | Trace segment bytes on disk, refreshed every 60 s |
| `rustfs_dial9_errors_total` | Telemetry setup failures |

The first three are separate on purpose, because they disagree in exactly the
cases you need to diagnose:

- `configured=1, supported=0` — wrong binary. Rebuild with `make build-profiling`.
- `configured=1, supported=1, active_sessions=0` — telemetry failed to start
  (usually an unwritable `OUTPUT_DIR`) and the process fell back to a standard
  runtime. Check `rustfs_dial9_errors_total` and the startup logs.

The session metrics are not exported at all unless telemetry is running. A counter
pinned at zero reads as "nothing happened", which is worse than a missing series.

### Known gap: writer death is not directly observable

dial9's `RotatingWriter` stops accepting writes (its internal `Finished` state) if
the output directory is removed underneath it or a segment cannot be sealed —
a real risk in containers where `/var/log` gets cleaned. Upstream exposes no way
to observe this: `TelemetryGuard::is_enabled()` reports how the session was *built*,
not whether it is still writing.

There is therefore no `writer_healthy` gauge, because it could only ever be
hard-coded to `1`. Instead, watch `rustfs_dial9_disk_usage_bytes`: a session with
`active_sessions=1` whose disk usage has stopped growing has most likely hit this
state, and needs a restart.

Reported upstream as [dial9-rs/dial9#658](https://github.com/dial9-rs/dial9/issues/658).
