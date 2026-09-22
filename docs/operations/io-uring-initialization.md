# io_uring Backend Initialization

**Use this when:** diagnosing local-disk startup or reconnect delays with the opt-in io_uring read backend, or reviewing cancellation and capacity ownership during backend construction.
**Source of truth:** `crates/ecstore/src/disk/local.rs` (`UringBackend::try_new`, `build_local_io_backend`, `LocalDisk::new`) and `crates/ecstore/src/disk/uring_probe.rs` (`run`, `MAX_CONCURRENT_PROBES`, `ProbeResult`).

## Initialization boundary

The io_uring backend remains opt-in and Linux-only. Disabled and non-Linux
paths construct the standard backend without submitting a driver probe.
Previously cached environment restrictions also bypass a new probe.

Enabled, uncached initialization acquires a process-wide semaphore before
submitting the synchronous driver probe/start operation to Tokio's blocking
pool. Its private concurrency bound is `MAX_CONCURRENT_PROBES`; it does not
change the configured shard count or per-ring queue depth. No blocking task is
created for a caller still waiting for admission.

Only the root-independent driver operation crosses this boundary. Root-related
backend construction, the negative probe cache and statistics exporter remain
with the async caller. A local disk's I/O root may refer to a mount-lease file
descriptor: do not detach path-based work from that descriptor's ownership.

## Cancellation and failure

Canceling before admission prevents that caller's probe from being scheduled.
After submission, the blocking work owns the permit. Dropping its async waiter
does not stop a running syscall and does not release capacity for a replacement
probe before the work finishes.

A completed result that the caller has not consumed also owns its permit.
Abandoning that result requests blocking-pool cleanup through the current Tokio
handle; capacity is retained through that cleanup. Outside a runtime, cleanup
is synchronous. During runtime shutdown the pool can reject new work and destroy
its captured result on the calling thread even when a handle is present. This
matches the backend's existing teardown boundary, not an unconditional promise
of asynchronous destruction or a deadline for a hung kernel operation.

Normal probe errors retain their existing classification: expected environment
restrictions can be negatively cached, and unexpected failures fall back
without permanently disabling later probes for the path. Admission and task
join errors preserve their error sources but are not OS restriction errnos.

## Scope of the limit

Initialization admission is not a limit on all live drivers, disk count, io-wq
threads, retained read results or process-wide read-buffer memory. Successful
construction releases its initialization capacity once the caller takes the
result. Long-lived and retiring backend budgets require separate accounting.

A slow probe can still delay readiness for its disk. Offloading protects the
async worker from performing that synchronous probe; it does not accelerate
the filesystem, change read/write semantics, or establish throughput gains.
Use [runtime profiling](dial9-runtime-profiling.md) for worker evidence and
separate backend metrics for I/O or initialization waits.
