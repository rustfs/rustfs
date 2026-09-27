# PUT and disk cleanup isolation

Cleanup can use process-wide admission budgets and a dedicated Tokio runtime.
This is opt-in. It preserves cleanup receipts, eligible-data moves to trash,
PUT temporary workspace cleanup, and periodic physical trash removal. It does
not disable healing or skip cleanup.

## Configuration

| Variable | Default | Meaning when isolation is enabled |
| --- | --- | --- |
| `RUSTFS_CLEANUP_ISOLATE_ENABLE` | `false` | Enable admission and execution isolation on this node. |
| `RUSTFS_PUT_RENAME_TAIL_CLEANUP_MAX_PENDING` | `1024` | Maximum admitted PUT ownership groups, including surviving commit and cleanup owners. |
| `RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_WORKERS` | `2` | Maximum active PUT cleanup continuations. Also controls existing deferred queue workers; does not determine OS thread count. |
| `RUSTFS_CLEANUP_ASYNC_THREADS` | up to `2` | Dedicated async worker threads, limited by available parallelism and an explicit CPU mask by default. Independent of logical job concurrency. |
| `RUSTFS_CLEANUP_DISK_MAX_PENDING` | `16 × disk workers` (`64`) | Maximum admitted local disk cleanup operations, including queued and running operations. Shared by local requests, incoming RPC cleanup and periodic GC steps. |
| `RUSTFS_CLEANUP_DISK_WORKERS` | blocking threads (`4`) | Maximum active disk cleanup operations within that budget. |
| `RUSTFS_CLEANUP_GC_WORKERS` | `1` | Maximum periodic disk scans producing GC steps. |
| `RUSTFS_CLEANUP_BLOCKING_THREADS` | `4` | Maximum additional blocking threads on the cleanup runtime. |
| `RUSTFS_CLEANUP_CPUS` | `1-2` on Linux; unbound elsewhere | CPU affinity for cleanup async and blocking threads. Override with a CPU list such as `4-5,8`, or `none` to disable affinity while keeping isolation. |
| `RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_ENABLE` | `false` | Use the existing bounded deferred queue before coordinator execution. Isolation also works without this queue. |
| `RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_QUEUE_CAPACITY` | `1024` | Capacity of that existing queue; this is not the total admission budget. |

Counts must be positive. PUT workers must not exceed PUT pending capacity;
GC workers must not exceed disk workers, which must not exceed disk pending
capacity. Async and blocking thread counts must not exceed 1024. CPU IDs must be supported by the affinity implementation and belong
to the initiating thread's allowed CPU set. Non-Linux hosts reject explicit CPU
masks. Configuration is read once at first use, including disk cleanup or GC;
changes require a restart. Invalid configuration rejects new cleanup admission.
PUT checks configuration before staging data.

Isolation remains disabled by default. When enabling it on Linux, the default
mask requires logical CPU IDs 1 and 2 to be available inside the process/container
CPU set. On smaller or restricted CPU sets, configure permitted CPU IDs or `none`
before enabling isolation. An unavailable default mask is an error; it does not
silently select different CPUs. Configure each node separately.

Do not combine isolation with cleanup counterfactuals
`RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP`,
`RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER`, or
`RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP`. These combinations
are rejected because they deliberately suppress cleanup or stop queue progress.

## Ownership and execution

PUT attempts admission before staging. Full admission returns `SlowDown`, so
clients should retry with backoff. Admission does not wait: COPY readers and
data-movement callers can already hold locks needed by admitted PUTs. No body
is ingested or tmp data staged by a rejected PUT.

The request, detached commit owner, and cleanup continuations share one
reservation. An early ACK or cancelled waiter cannot release it while an owner
still needs cleanup. After the existing rename-convergence and guard-release
handoff, deferred workers, queue overflow and direct continuations all acquire
the same coordinator execution semaphore. Queue overflow cannot bypass that
limit. Awaited failure cleanup and no-tail tmp cleanup use the same budget.
Full-tail receipt persistence remains awaited at its existing handoff point;
old-data reclamation remains after the existing guard release. Incomplete
rollback still preserves staging for recovery.

Each local disk operation has separate pending and active permits. The wrapper
below RPC authentication uses this gate for low-level `delete`, `delete_data_dir`
and old-data cleanup receipt `write_all`. Local PUT fan-out and incoming peer
RPCs therefore share the same node-local disk budget. Receipt classification
uses the internal receipt filename; ordinary writes retain their current path.
RPC schemas and authentication are unchanged. Enable isolation on every node
to isolate peer execution; settings and CPU masks are not transmitted by RPC.

Coordinator and receiver permits are deliberately distinct: a coordinator can
wait for peer RPCs without occupying the slots those peers need to service disk
cleanup. Full disk admission returns an I/O `WouldBlock` error through the
existing disk error path. A mutation carrying an external publication or
namespace owner also refuses a full active budget instead of waiting while it
holds a lock. Existing cleanup error handling and receipt recovery still apply.

Ordinary snapshot-lease release stays on the caller and only updates the in-memory
registry. It does not allocate a cleanup task or wait for the cleanup scheduler.
Invalid cleanup configuration also must not strand a reader token: release still
updates the registry, while any deferred reclamation reports the initialization
error and keeps both its intent and data. It does not fall back to unbudgeted I/O.
If the last release triggers deferred old-data
reclamation, that reclamation tries the same pending and active disk permits.
Only this case dispatches an owned task to the cleanup runtime.
Saturation leaves the token released and the cleanup intent/receipt available
for retry; it does not strand a live reader token or bypass the budget. Quota
fence release keeps its original receiver lifecycle and does not acquire disk
execution permits, since it can wait for mutations that already own them.

An admitted request waits for disk execution capacity in the caller's existing
future. Cancellation or timeout discards this unstarted operation and releases
pending capacity; it cannot later start deleting a reused path. After execution
capacity is acquired, the worker also checks whether its result receiver closed
before its first poll. A job cancelled while waiting for the runtime to schedule
it is discarded when the worker polls it, releasing its permits without starting
I/O. Once the worker starts the operation, the owned job retains its permits and
disk reference until completion. Cancelling or timing out its waiter does not
free disk capacity while that started job still runs.
This also retains the disk's mount ownership during detached execution. Cleanup
waiter timeouts include queue delay and do not by themselves mark a disk faulty;
the existing active disk-health probe retains its timeout policy.

Periodic trash and stale-tmp scans have a separate scan limit. Each scan acquires
the shared disk budget to open its directory and then for each entry, releasing
it between entries. The steps borrow the scan's iterator and paths and run in
the same owned task, without a spawn/join or path clones per entry. A scan cannot occupy a disk execution slot while waiting
for another scan slot. GC waits for admission without holding object locks;
request admission is bounded without creating waiting producer tasks. A single
trash subtree is still removed as one operation, so a very large subtree can
hold a slot for its entire removal. Queued scans retain the original mount
handle even when the corresponding disk object is dropped.
Missed periodic ticks are skipped so a slow scan does not cause a burst of
catch-up scans.

Cleanup runs on `rustfs-cleanup` threads. Nested Tokio tasks, Tokio filesystem
work and ordinary `spawn_blocking` use that runtime. Explicit fsync dispatch
also stays on its blocking pool. File/directory group-commit keys separate
cleanup and foreground producers, preventing batches from moving into the
other class's runtime. Fsync durability settings remain unchanged.

The blocking limit bounds simultaneous blocking execution, not Tokio's internal
queue or kernel I/O. Disk admission bounds selected operations, not bytes,
syscall count or device IOPS. Existing synchronous path probes and disk-full
delete fallbacks can still run on the dedicated async workers, under the disk
operation budget. All classes still share the filesystem, journal,
page cache and devices with foreground work.

If CPU binding fails at thread startup, new admissions fail and the failure is
logged; already-admitted cleanup can finish. Affinity does not reserve exclusive
cores, move foreground threads elsewhere, change cgroups, or control kernel
workers and interrupt affinity.

## Scope and recovery

The limits are per process, not cluster-wide quotas. This is not a durable work
queue. Existing cleanup receipts and tmp/trash recovery retain their role after
process exit. The option does not bound all storage mutations: metadata/version
DELETE transactions, heal/scanner coordination and
multipart orchestration retain their scheduling. Calls from those paths that
reach the selected disk wrapper entries do share the disk budget; internal
LocalDisk operations that bypass those entries do not.

## Measurement and tuning

On Linux, `cargo test -p rustfs-ecstore --test cleanup_isolation_test` exercises
the public disk facade, actual thread affinity, receipt/delete execution and
periodic physical trash removal with temporary disks. It does not benchmark
throughput.

With PUT stage metrics enabled, observe:

- `rustfs_cleanup_admitted_puts` and `rustfs_cleanup_admission_rejected_total`;
- `rustfs_cleanup_execution_wait_seconds`;
- `rustfs_cleanup_disk_pending`, `rustfs_cleanup_disk_active`, and
  `rustfs_cleanup_disk_admission_rejected_total`;
- `rustfs_cleanup_disk_execution_wait_seconds` and `rustfs_cleanup_gc_active`;
- existing deferred queue depth, age, overflow, completion and service time.

Compare isolation disabled, budget/pool isolation without CPU affinity, and
isolation with affinity. Keep durability, object sizes, overwrite ratio,
concurrency and cluster placement fixed. Measure PUT/GET throughput and p95/p99
latency, rejections, queue age, receipt/trash growth, per-disk I/O latency, CPU
usage and context switches. Include a long steady-state interval covering
physical GC, and verify backlog drains afterward.

Start with small worker and blocking-thread limits, then increase only when
cleanup cannot keep up without harming foreground latency. Too few workers or
CPUs can increase rejections and lower PUT throughput. Larger queues do not
increase sustainable device throughput. Affinity helps only when CPU scheduling
contention matters to the measured workload; no throughput gain is guaranteed.

The defaults protect a node; they are not a claim of optimal production throughput.
The default disk queue permits sixteen service waves, rather than 256 waves with
1024 pending operations and four workers. For illustration, at a uniform 100 ms
per operation, these bounds correspond to roughly 1.6 s versus 25.6 s to drain a
full queue with no new arrivals. Real service times, stalls and disk fan-out vary;
there is no queue latency guarantee. The ordinary disk waiter timeout defaults to
30 s and does not cancel already-admitted work.

Size pending capacity for measured arrival bursts and local/peer fan-out, not
only for PUT request concurrency. For example, two coordinator jobs each touching
16 local disks can submit 32 disk operations before incoming peer requests and
GC are counted. Sixty-four slots need not absorb every distributed burst. Increase
the explicit limit when measured rejection rates require it, while keeping queue
age comfortably below waiter deadlines. On many-disk nodes, measure whether four
active operations underutilize devices before increasing disk/blocking workers.
The 1024 PUT admission limit includes uploading requests and detached owners;
it is an object-count bound, not a memory or byte budget. The older deferred queue
remains optional and disabled by default; its overflow path cannot bypass the
new coordinator or disk limits. No unbounded foreground fallback is introduced.
