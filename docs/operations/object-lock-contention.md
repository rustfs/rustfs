# Object lock contention diagnostics

**Use this when:** an S3 operation returns `ServiceUnavailable` near the namespace lock acquisition deadline, including multipart session creation or abort, or an object GET overlaps PUT or Heal on the same key.

## Client-visible acquisition timeouts

The canonical `ApiError` conversion in `rustfs/src/error.rs` retains HTTP 503 and the S3 `ServiceUnavailable` code for typed lock acquisition timeouts, and adds `Retry-After: 1`. The header is a minimum retry delay, not a receipt that the competing owner has released its lock. Use bounded exponential backoff with jitter and keep the client's overall request budget.

S3 data-plane catalog publication-lock timeouts use the same conversion, typed lock-failure counter, and generic public message; catalog resource and peer details remain in the internal error source. Catalog failures represented only as strings do not inherit lock retry advice.

When a typed cause is available, the same conversion adds an `x-rustfs-error-cause` response header without changing the S3 code or public message:

| Header value | Meaning |
| --- | --- |
| `namespace_lock_timeout` | A typed namespace lock acquisition timeout. Also carries the existing `Retry-After: 1` advice. |
| `namespace_lock_unavailable` | A different typed lock failure or namespace fence refusal. This includes lock quorum loss and capability/fence refusals; it does not prove a particular peer or disk failed. |
| `storage_write_quorum` | A typed storage write-quorum failure. This is separate from namespace locking and does not receive lock-timeout retry advice. |

The header is diagnostic, not a retry-safety guarantee. An absent header means the failure was not classified, not that locking or storage is healthy. The classifier uses bounded typed error sources, never elapsed time or error text, and exports no resource names, owners, peer addresses, or quorum counts. It applies only to `ServiceUnavailable` responses; other S3 codes keep their own semantics.

The `Retry-After` advice does not change the lock acquisition budget or shorten a competing writer's commit tail. It is not added to unrelated disk timeouts, quorum failures, or errors that merely contain timeout text. A 503 near five seconds is insufficient to attribute the request to a namespace lock: disk metadata operations also have a five-second default budget. Correlate the typed lock-failure counters and acquisition events below before changing contention policy.

## Multipart session failures near five seconds

`CreateMultipartUpload` and `AbortMultipartUpload` acquire bucket lifecycle and metadata validation locks. Abort also takes an exclusive upload lock, which must wait for active part commits. A healthy disk inventory does not establish lock availability or the quorum result of an individual metadata operation.

The namespace lock acquisition budget is `RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT`, in seconds, with a default of **5**. Set it before process startup; it is cached. `RUSTFS_DRIVE_TIMEOUT_PROFILE=high_latency` controls drive operation budgets and does not change this namespace lock budget. A longer lock budget can permit a longer wait, but does not remove contention or identify the competing owner.

Ordinary PUT and multipart operations share successful bucket-incarnation validation while their owned lifecycle read guards overlap continuously. This avoids a redundant metadata transaction lock and sidecar quorum read on subsequent covered requests. The owning guard registers before asynchronous validation, so cancellation removes its registration; UploadPart's owned commit retains both the actual lock and its registration until the tail drains. Last-owner release and observed lock loss invalidate the memo. Cold validations remain concurrent, and failures retain their original typed errors without being cached or queued for sequential retries.

Object Lock snapshots validate through their own transaction guard even when the memo is warm. Operations borrowing only an outer lifecycle fence retain their authoritative validation path. Disabled locking always validates from disk, because a no-op guard provides no continuous exclusion proof. These rules preserve generation checks across cancellation and bucket recreation; they do not establish the production trigger of an individual five-second response.

Session creation writes metadata through `write_unique_file_info` and the metadata RPC; abort uses metadata checks and quorum deletion. These paths do not use the five-second PUT-file capability probe in `internode_data_transport`. Raising that probe deadline does not change session metadata or lock deadlines.

Capture the response headers, request ID, operation, and the `rustfs_s3_lock_failures_total` counter by `op`, `resource_class`, `mode`, and `reason` during a bounded reproduction. Combine them with acquisition events where available; these events do not yet cover every lifecycle or metadata lock. Do not infer the failing phase from timing alone. Retain HTTP 503 and `ServiceUnavailable` for lock/write-quorum availability failures rather than recoding them as `InternalError` or assuming every failure is request-rate overload. Avoid automatic server-side create retries after uncertain writes: a retry creates a new upload ID and can leave another incomplete session.

## Capture one acquisition and its competing owners

Enable the existing diagnostic switch before starting the process and add the diagnostic target to the deployment's log filter:

```bash
RUSTFS_OBJECT_LOCK_DIAG_ENABLE=true
RUST_LOG=info,rustfs_ecstore::object_lock_diag=trace
```

Preserve any other required deployment filter directives. The switch is cached on first use in each process; restart with the desired setting to change it. Disable it and remove the added trace directive after collecting the bounded reproduction window.

The `object_namespace_lock` event covers store GET/read and write acquisition, set-level read and PUT commit acquisition, and the object Heal lock phases. Existing slow acquisition and hold warnings remain available. These events do not provide a complete inventory of every namespace lock in the process.

| Field or state | Interpretation |
| --- | --- |
| `lock_attempt_id` | Unique identity for one acquisition and its guard lifetime; unlike the set's `owner`, it distinguishes concurrent requests. |
| `op`, `mode`, `bucket`, `object`, `namespace`, `owner` | Operation and logical lock context. Local namespace labels may share a lock manager; different labels do not prove independent locking. |
| `requested_version_id` | Caller-selected version when available, including store GET and Heal. Absence means it was not supplied to that acquisition helper, not proof that the request selected the latest version. Namespace exclusion still uses the object key without a VersionId. |
| `acquiring` | The caller is about to request the lock. This event alone does not prove that it queued or acquired ownership. |
| `acquired` | Acquisition returned a live guard. `acquire_ms` measures the acquisition interval. |
| `failed` | Acquisition failed before storage/S3 error mapping. `failure` is a bounded error class; `timeout_ms` is the configured acquisition budget. Failure is emitted at error level when diagnostics are enabled. |
| `cancelled` | The acquisition future was dropped before an acquisition result was observed. It is not a timeout verdict. |
| `guard_dropped` | The acquisition's owning guard was dropped; `hold_ms` measures its observed lifetime. Distributed release can still be in flight, so this event is not a release-quorum receipt. |
| `disabled` | The lock backend returned a disabled guard; no exclusion interval is claimed. |

The diagnostic retains the parent tracing span available at acquisition. When an HTTP request span carries `request_id`, guard events retain that parent even if the guard moves into a background PUT commit tail. Heal events retain the available task span. Missing parent fields must not be reconstructed from adjacent log lines.

Use `read_repair_data`, `read_repair_commit`, and `heal_object` to distinguish shared read-repair reconstruction, its exclusive revalidation/rename phase, and ordinary exclusive object healing. A PUT quorum ACK can precede the end of its rename tail and lock lifetime.

## Distinguish the waiting chains

An explicit historical VersionId does not select a separate namespace lock. Two useful candidate chains are:

1. PUT owns the exclusive object lock while a slow rename tail drains; a historical GET exhausts its acquisition budget.
2. Read-repair owns the shared data-phase lock, a PUT writer queues, and the local lock's writer preference prevents later GET readers from entering. The GET can time out before PUT ever acquires the lock.

For each failed request, bind the original HTTP request ID, exact key/VersionId, binary revision/hash, process generation, and monotonic SDK interval to the acquisition events. Follow each candidate owner's complete acquisition/guard interval; also retain PUT rename timing, admission/permit metrics, and actual device `io.max` settings. A nearby slow PUT or an `acquiring` event is not sufficient proof of the blocking owner. Missing events leave attribution incomplete.

## Separate slow-I/O stress from functional overlap

A process cgroup write cap also applies to foreground PUT writes on the capped device. Keep that case as explicit slow-I/O stress. For functional Heal/foreground validation, remove the foreground process cap and prove the original Heal root actually overlaps the reader's SDK interval for the target key/version and process generation. Root `running` before or after the call, fixed sleeps, and final successful reads do not substitute for that witness.

Retain every original GET result and latency without implicit retries. Reconcile each acknowledged PUT by its exact VersionId, bytes, and required metadata. Report availability separately from version/data correctness; a final successful reconciliation does not erase an earlier 503. A no-Admin control can still run GET-triggered read-repair.

The deterministic tests `object_lock_diagnostics_report_historical_get_blocked_by_put_commit` and `object_lock_diagnostics_distinguish_read_repair_from_queued_put` in the ecstore store/object and set-disk Heal modules exercise these lock chains and retain historical version bodies. They validate attribution instrumentation, not a measured SLO or a real uncapped Heal-overlap acceptance result. Do not extend product lock timeouts, bypass namespace exclusion, or suppress failures to make that acceptance pass.
