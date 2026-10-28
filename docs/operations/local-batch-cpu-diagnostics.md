# Sampled local batch CPU diagnostics

**Use this when:** separating sampled local EC batch allocation/initialization
and filesystem wall time from worker CPU time, or associating GET verification,
copy and response stages within one request in a default-off diagnostic build.

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

## Request-coherent GET stages

`diagnostic-get-pipeline` is a separate default-off build feature. It includes
`diagnostic-local-batch` and adds one request-level sampling decision at the
external S3 GET boundary. Use this mode to associate verification, copies,
reconstruction, producer emit and response-body polls with the same canonical
response ID. List and rejected GET requests can have no storage stages; never
classify them as complete object-read samples solely from their method.

```sh
RUSTUP_TOOLCHAIN=stable cargo build --release --locked -p rustfs --bin rustfs \
  --features diagnostic-get-pipeline
export RUSTFS_DIAGNOSTIC_LOCAL_BATCH_SAMPLE_EVERY=0
export RUSTFS_DIAGNOSTIC_GET_STAGE_SAMPLE_EVERY=64
export RUST_LOG=warn,rustfs_local_batch_diagnostic=trace
```

The request rate uses the same `1..=65536` bounds and restart requirement.
When it is nonzero, selected requests select all their batch records; the
per-batch interval does not independently filter that cohort. Only selected
requests retain the shared diagnostic ID in request mode; the original batch
mode retains its existing all-request ID behavior. When the request
rate is missing/invalid/zero, the original batch mode remains available. The
rate and target filter must both be enabled to select a request. There are no
request-stage symbols or calls without the new feature; synchronous probe
macros discard diagnostic arguments in that build.

A selected request retains its diagnostic context through service polling,
the legacy producer, response-body polling and selected blocking work. Its
last owner emits one `get_pipeline_diagnostic` summary on the same isolated
JSON target. `request_sequence` advances for each eligible canonical GET and
`sample_every` records the configured interval; selected sequences are multiples
of that interval. Emission can be reordered by producer drain, so sort sequence
values and check gaps rather than assuming log order is request order. `stage_totals_json` is a JSON-encoded array in the fixed order
`verify,copy,reconstruct,emit,response_poll`. Every entry contains `calls`,
`bytes`, `wall_ns`, `cpu_ns`, `cpu_unavailable`, `failures`, `cancelled`,
`first_tid`, `last_tid` and `thread_changes`. The TID fields describe
synchronous observations; changes compare consecutive completed observations,
not scheduler migrations. An async emit has no thread CPU/TID attribution.
The summary's `emitter_tid` identifies its final emitter, not the worker for
every stage.
Counters saturate with `overflowed=true`; a poisoned accumulator is explicitly
marked. Neither case is usable for timing attribution.

`verify` measures synchronous bitrot hash/comparison; `copy` measures the
verified reader-to-output copies, not every copy in the service. Reconstruction
measures the legacy duplex synchronous decode call, excluding awaits for integrity proof
preparation. `emit` brackets each async duplex `write_all` and never reads a
thread CPU clock. Its bytes count only fully completed writes; a failed or
cancelled write may already have transferred an unknown partial prefix.
Reconstruction does not report byte accounting in this version. The codec-streaming reconstruction path and copies outside these reader hooks
are not covered by these reconstruction/copy totals. `response_poll` measures
one synchronous inner body poll,
including Pending polls, and counts bytes returned by data frames. It does
not measure transport/network progress while the body is not polled. Other
CPU unavailability is explicit; available zero and unavailable are distinct.
Selected requests execute additional clock reads and accumulator updates.
That work can itself move selected requests into a slow cohort; selection
does not make them an unperturbed random control. Treat stage totals as
diagnostic observations and qualify instrumentation cost separately.
Stage scopes can overlap, including copies performed within a response poll.
They must not be added to form a request critical path.

`result=body_complete` means the server body reached EOF or a suppressed-body
completion. It is not a client acknowledgement. Body errors (including unsuccessful HTTP status), service errors
and incomplete drops use `body_error`, `service_error` and `cancelled`.
Completion is idempotent; if cancellation leaves a producer/blocking worker
alive, its token retains the summary until the work drains. Thus
`lifetime_wall_ns` can extend beyond body completion and is not an HTTP latency.
`response_ready_ns`, `first_data_poll_end_ns` and `body_end_ns` are monotonic
offsets from the captured request start, not network timestamps. The owner
marks an unfinished request/body cancelled when dropped; the summary can
remain deferred while producer work drains. Missing boundaries remain absent.

The process admits at most 4096 selected request summaries. It reserves a final
summary event before accepting a request, and at most 131072 events in request
mode, budgeting 8192 bytes per fixed-schema event (1 GiB of reserved payload
budget). Request IDs are bounded ASCII and all other owned fields are fixed
enums, counters or bounded numeric JSON. The logger can add its standard
metadata and can write to more than one sink; this budget does not limit other
logging or exporter output. Batch-event exhaustion marks `truncated=true` in
the reserved summary. Admission exhaustion stops selecting requests. Reject
truncated records and runs that reach an admission/event cap as evidence for a
complete cohort; do not silently treat missing records as zeros.

Keep the new feature, both rates, limits, source, binary and client in separate
provenance. Unit/contract checks are diagnostic correctness evidence only.
New Linux workloads still need a separately registered protocol and resource
reservation. This mode neither selects an I/O optimization nor qualifies
baseline repeatability or observation overhead.
