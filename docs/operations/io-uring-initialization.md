# io_uring Backend Initialization

**Use this when:** diagnosing local-disk startup or reconnect delays with the opt-in io_uring read backend, or reviewing cancellation and capacity ownership during backend construction.
**Source of truth:** `crates/ecstore/src/disk/local.rs` (`UringBackend::try_new`, `build_local_io_backend`, `LocalDisk::new`), `crates/ecstore/src/disk/uring_probe.rs` (`run`, `MAX_CONCURRENT_PROBES`, `ProbeResult`), `crates/ecstore/src/disk/uring_driver_budget.rs` (`BudgetedDriver`) and `crates/ecstore/src/disk/uring_read_budget.rs` (`ReadBudgetConfig`, `DriverReadBudget`).

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

## Optional shared driver read budget

Configure both of these strict decimal byte counts to opt into a shared pool:

| Setting | Meaning |
| --- | --- |
| `RUSTFS_IO_URING_READ_BUDGET_TOTAL_BYTES` | Total whole-driver reservation capacity for participating drivers in this process |
| `RUSTFS_IO_URING_READ_BUDGET_DRIVER_BYTES` | One driver's in-flight read-buffer quota, shared by all of that driver's shards |

Both unset preserves the existing count-only driver constructor. When enabled,
both values must be positive, the driver quota must not exceed the total, and
the driver quota must fit `tokio::sync::Semaphore::MAX_PERMITS`. Total capacity
uses `usize` and is not narrowed to the driver's per-read `u32` acquisition size.
Zero, a missing half of the pair, empty/non-Unicode values, signs, whitespace,
overflow and out-of-range values disable io_uring construction with one safe
configuration warning. Raw values are not logged. As with the driver-thread
setting, configuration is read once on first enabled construction; restart is
required to change it.

The process retains one pool across backend retirement and disk reconstruction.
Inside the existing bounded blocking probe, the library reserves the complete
driver quota before creating rings. Idle drivers keep the full block; they do
not borrow unused capacity from another admitted driver. Pool exhaustion returns
`WouldBlock` without a kernel errno. Construction selects StdBackend for now,
returns its tentative driver-thread reservation, and does not negatively cache
the root. A later reconstruction can retry; an existing std backend does not
automatically switch back when capacity becomes available. Native OS setup
errors retain their existing classification, including an OS `EAGAIN` rather
than a pool-generated `WouldBlock`.

The library retains the reservation through driver ownership, deferred handles,
queued requests and pending kernel reads. An exporter Arc can delay retirement.
Clean final ownership release refunds the block; a bounded-drain leak retains
the **entire** driver quota, even for a small leaked read. Thread-slot return and
read-quota return therefore need not coincide. Closing one driver does not close
the shared pool or another driver's admission. The application must not create
a replacement pool to bypass retained reservations.

### Operation size, fallback and returned results

An optional process-wide returned-result reservation can be enabled with
`RUSTFS_IO_URING_READ_RESULT_BUDGET_BYTES`. The value is a strict positive
decimal byte count. A read reserves its requested result length before either
the io_uring path or its std fallback starts; exhaustion returns `WouldBlock`
to the caller and is never routed through the fallback. Successful results use
`Bytes::from_owner` to retain the receipt through every returned `Bytes` clone,
so a caller-held result remains charged until its final owner is dropped. The
pool is shared by all io_uring backends in the process and survives disk
reconstruction.

This pool covers only participating io_uring drivers' in-flight read-buffer
allocations. The result pool covers the requested output and the std fallback
result; it does not cover metadata, probe buffers, allocator overhead, rings or
io-wq resources. It is not an RSS measurement and does not yet account for
allocator slack or every physical direct-I/O padding byte. A retained result
remains readable after its driver retires while its result receipt keeps the
process result budget charged.

This change retains the existing 128 MiB logical operation cap. If a configured
driver quota is smaller, an individual larger operation returns `InvalidInput`
before driver submission and the existing read path falls back to std. That
error does not latch io_uring off. Direct I/O can exceed the quota even when
logical length fits: its charge is the aligned enclosing range plus alignment
allocation padding. No automatic shrinking or new chunk algorithm is introduced.

Future integration with configurable logical chunks must size each operation
against the physical direct-I/O charge, using the actual alignment and offset;
setting the logical chunk size equal to the quota is not sufficient. Neither
logical chunking nor std fallback extends this pool to retained/assembled results.
The result pool is a separate owner domain from the driver pool. Budget direct
physical padding, assembled intermediate buffers, and RSS sampling separately
before claiming an end-to-end physical-memory limit.

### Dependency and verification boundary

The Linux dependency temporarily pins the merged `rustfs/uring` commit
`c92fc4016502e12e6ba03483dcb2a1c3bf2e0a94`, which provides `SharedReadBudget`.
Published registry version 0.2.2 does not contain that API. The exact Git revision
and lockfile make this source choice explicit; replace both with a containing
registry release after verifying the same contracts. No diagnostics,
fault-injection or Tokio shutdown-adapter feature is enabled by this pin.

Portable tests exercise the strict configuration pair. Native backend tests
cover shared admission, temporary denial without negative-cache pollution,
thread-slot rollback, exporter-held retirement, same-root retry, independent
peer reads, and small-quota fallback with retained result bytes. A native
padding case first proves O_DIRECT with fixed 4096-byte alignment, then
verifies that logical 4096-byte reads exceed a 4096-byte quota once allocation
padding is charged, while std fallback remains byte-exact and does not latch
io_uring off. The library's own fault-injection suite verifies whole-quota leak retention; the application
tests do not inject a stuck kernel read. Compilation or restricted-host skips
alone do not prove native execution or a memory/performance improvement.

## Scope of the limits

Initialization admission ends when the caller takes the completed result. The
optional driver-thread reservation instead follows driver lifetime. Neither
the driver pool nor the result pool limits disk count, Tokio blocking-pool
threads, io-wq workers, assembled intermediate buffers, leaked kernel-visible
buffers or RSS. The shared read pool bounds only participating driver quotas;
the result pool bounds requested returned-result ownership, including std
fallback results. Std fallback allocation slack and physical direct-I/O padding
remain outside these logical byte pools; a driver thread budget does not become
a whole-process thread or memory bound through fallback.

A slow probe can still delay readiness for its disk. Offloading protects the
async worker from performing that synchronous probe; it does not accelerate
the filesystem, change read/write semantics, or establish throughput gains.
Use [runtime profiling](dial9-runtime-profiling.md) for worker evidence and
separate backend metrics for I/O or initialization waits.
