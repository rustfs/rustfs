# dial9 Tokio Runtime Profiling

`dial9-tokio-telemetry` records Tokio runtime-level events — poll start/end,
worker park/unpark, task spawn/terminate, and optionally async backtraces of
stalled tasks — into binary trace segments.

It answers questions that Prometheus metrics and `tracing` spans cannot:

- A request took 200 ms. Was a worker thread blocked, or was it waiting on I/O?
- Which task held a worker for 40 ms without yielding?
- Are workers parking because there is no work, or because the queue is starved?

These are the failure modes behind drive stalls, `io_uring`/`O_DIRECT` regressions,
and object-data-cache fill contention. They are invisible to the rest of the obs stack.

## This is a profiler, not telemetry

Three properties follow from that, and all three matter operationally.

**The stock binary cannot run it.** dial9 hooks Tokio's unstable runtime API, which
requires `--cfg tokio_unstable`. Release binaries are built without it, so they neither
depend on Tokio's non-semver surface nor pay its cost. Setting
`RUSTFS_RUNTIME_DIAL9_ENABLED=true` on a stock binary logs a warning and records nothing.

**Traces are written continuously and evicted.** The retained budget is
`MAX_FILE_SIZE × ROTATION_COUNT` (1 GB by default). Once exceeded, the *oldest*
segments are deleted. Under a high poll rate that budget can wrap in minutes,
which means the incident you are chasing may already have been overwritten.
Size the budget against the window you need, and prefer short, targeted runs.

**There is no runtime toggle.** Telemetry is installed when the Tokio runtime is
constructed, so enabling or disabling it requires a process restart.

## Building

```bash
make build-profiling
```

That is `cargo build --release --bin rustfs --features dial9` with
`RUSTFLAGS="--cfg tokio_unstable"`. Two optional features layer on top:

```bash
# Upload sealed segments to S3 (pulls in the AWS SDK)
DIAL9_FEATURES="dial9-s3" make build-profiling

# Async backtraces of stalled tasks (Linux x86_64/aarch64)
DIAL9_RUSTFLAGS="--cfg tokio_unstable --cfg tokio_taskdump" \
  DIAL9_FEATURES="dial9-taskdump" make build-profiling
```

`crates/obs/build.rs` fails the build if the `dial9` feature is enabled without
`--cfg tokio_unstable`. This is deliberate: an environment `RUSTFLAGS` *replaces*
the value from `.cargo/config.toml` rather than appending to it, so the flag used
to disappear silently whenever anything else set `RUSTFLAGS`.

For CPU profiling with usable stacks, add `-C force-frame-pointers=yes`.

## Running an investigation

```bash
export RUSTFS_RUNTIME_DIAL9_ENABLED=true
export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-telemetry-investigation
export RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT=3      # 300 MB total, ~short window
export RUSTFS_RUNTIME_DIAL9_TASK_DUMP_ENABLED=true # if built with dial9-taskdump
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
| `RUSTFS_RUNTIME_DIAL9_TASK_DUMP_ENABLED` | `false` | Needs `dial9-taskdump` |
| `RUSTFS_RUNTIME_DIAL9_TASK_DUMP_IDLE_THRESHOLD_MS` | `10` | Mean idle for Poisson sampling |
| `RUSTFS_RUNTIME_DIAL9_S3_BUCKET` | unset | Needs `dial9-s3` |
| `RUSTFS_RUNTIME_DIAL9_S3_PREFIX` | unset | Needs `dial9-s3` |

Variables whose build feature is absent are ignored, with a warning naming the
feature to rebuild with.

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
