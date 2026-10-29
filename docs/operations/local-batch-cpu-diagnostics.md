# Sampled local batch CPU diagnostics

**Use this when:** separating sampled local EC batch allocation/initialization
and filesystem wall time from worker CPU time in a default-off diagnostic build.

This diagnostic build separates allocation/initialization, metadata, open and
positioned-read elapsed time from Linux thread CPU time. It preserves the
single blocking dispatch, buffer initialization, offsets, integrity checks and
error handling. It does not implement a rustix optimization or qualify
observation overhead.

Build separately from any frozen performance binary:

```sh
RUSTUP_TOOLCHAIN=stable cargo build --release --locked -p rustfs --bin rustfs \
  --features diagnostic-local-batch
```

The feature is absent from default builds. Even this diagnostic build records
nothing until both the sample rate and the target filter are enabled:

```sh
export RUSTFS_DIAGNOSTIC_LOCAL_BATCH_SAMPLE_EVERY=64
export RUST_LOG=warn,rustfs_local_batch_diagnostic=trace
```

The rate is an integer in `1..=65536`. Missing, zero or invalid values disable
capture. Rate 64 selects one in every 64 submitted batches; rate 1 is intended
only for small correctness probes. Restart to change the rate. Disabled or
unselected batches create no timing records and read no diagnostic clocks.
While recording is enabled, each external S3 request retains a bounded shared
ID, including requests whose batches are not selected. This is diagnostic work
and must be accounted for before any observation-cost claim.

Business fields contain a process-local batch ID, the bounded server-owned canonical
request ID when propagated, worker PID/TID, shard index, sizes and timing.
The RustFS feature also enables a matching JSON formatter policy: explicit
root events on this fixed diagnostic target omit current/ancestor spans and
parent-derived request ID aliases. No additional spans are allocated on the
sampled I/O path. There are no variable-ID metric labels. Missing request
context is reported as `request_context=missing`, never treated as a successful
join.
The standard logger may attach source-location and thread metadata. Its JSON
format flattens business fields; readers must distinguish these fields from
formatter metadata and reject inherited request spans.

Queue wait is elapsed time across the async/blocking boundary and has no
thread CPU delta. Execution and per-shard stages run synchronously on one
worker thread. CPU clock failure or unsupported platforms omit `cpu_ns` and
report `cpu_clock_status=unavailable`; this is different from an available
zero-duration measurement. Clock and logging work perturb diagnostic samples.
Elapsed and CPU are measured separately and must not be assumed equal or
subtracted without accounting for measurement overhead. Stage durations can
overlap and per-shard samples differ from per-batch samples.
When production batch-stage metrics are enabled, these timings also include
their recording work. Keep that state in the provenance; they are not bare
system-call durations.

`shard_result` reports requested and actually read bytes, including partial
reads on error. Unattempted stages are absent; a zero-length shard has no read
sample. Captured correlation and dispatch stay with a blocking worker even
after its caller is cancelled.

Keep the diagnostic binary, source revision, feature, rate, filters and raw
events distinct from baseline or acceptance artifacts. Do not log credentials,
object/disk paths, request bodies or arbitrary client-supplied identifiers to add
correlation. Request-level client association must use the server's canonical
response ID. Diagnostic results locate hypotheses; they do not regrade failed
A/A or ABBA runs.
