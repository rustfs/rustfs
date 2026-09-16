# Object lock contention diagnostics

**Use this when:** an object GET returns `ServiceUnavailable` near the namespace lock acquisition deadline while PUT or Heal is active on the same key.

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
