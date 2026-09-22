# io_uring Backend Initialization

**Use this when:** diagnosing local-disk startup or reconnect delays with the opt-in io_uring read backend, or reviewing cancellation and capacity ownership during backend construction.
**Source of truth:** `crates/ecstore/src/disk/local.rs` (`UringBackend::try_new`, `build_local_io_backend`, `LocalDisk::new`), `crates/ecstore/src/disk/uring_probe.rs` (`run`, `MAX_CONCURRENT_PROBES`, `ProbeResult`) and `crates/ecstore/src/disk/uring_driver_budget.rs` (`BudgetedDriver`).

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

## Optional process-wide driver thread budget

`RUSTFS_IO_URING_MAX_DRIVER_THREADS` limits the sum of reserved shard-driver
threads across disks in one RustFS process. It is separate from initialization
concurrency and from the per-disk `RUSTFS_IO_URING_SHARDS` setting.

- Unset or `0`: no driver-thread budget, preserving the existing behavior.
- A positive decimal integer within the semaphore's supported capacity: each
  initialization reserves its configured shard count before scheduling a probe.
- Empty, malformed, non-Unicode or out-of-range input: reject io_uring admission
  and use the standard backend, with one configuration warning. Invalid input
  never silently selects unlimited admission; raw configuration text is not logged.

The configuration is read once on first use. Restart the process to change it.
The io_uring enable switch remains required; this setting alone does not enable
the backend or affect the non-Linux path.

Reservation is nonblocking. If the full shard count cannot be acquired, that
construction falls back to std instead of waiting for a live driver to retire.
The shard count is not silently reduced. This avoids making disk readiness
depend on indefinitely occupied steady-state slots. Startup order can determine
which disks obtain the available slots; there is no equal-share or priority
guarantee. The first shortage emits a warning, and budget rejection does not
enter the permanent unsupported-disk cache.

The reservation follows the probe and, on success, the driver itself. It is
retained through cancellation, completed-but-unconsumed results, extra strong
statistics references and retiring-driver cleanup. Dropping the backend alone
does not release it: the driver must finish destruction first. A failed probe
or canceled admission releases only its own reservation. A later construction
may acquire returned capacity; an already-selected std backend is not migrated
automatically.

For example, a budget of `8` with `4` shards per disk permits at most two such
driver reservations, including pending or retiring instances. This is a capacity
example, not a tuning recommendation or measured performance result.

## Scope of the limits

Initialization admission ends when the caller takes the completed result. The
optional driver-thread reservation instead follows driver lifetime. Neither
limits disk count, Tokio blocking-pool threads, io-wq workers, retained read
results, leaked kernel-visible buffers or process-wide read-buffer memory.
Std fallback retains its existing resource behavior; a driver thread budget
does not become a whole-process thread or memory bound through that fallback.

A slow probe can still delay readiness for its disk. Offloading protects the
async worker from performing that synchronous probe; it does not accelerate
the filesystem, change read/write semantics, or establish throughput gains.
Use [runtime profiling](dial9-runtime-profiling.md) for worker evidence and
separate backend metrics for I/O or initialization waits.
