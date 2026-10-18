# Table Maintenance Planning and Periodic Queueing

**Use this when:** planning table maintenance, enabling automatic job admission, or recovering a stalled queue.

The server can periodically **queue** maintenance jobs for explicitly selected table buckets. It does not run workers, delete objects, expire snapshots, or compact data automatically. Workers remain an explicit operator action. Both object-backed and durable-strong catalogs use the existing maintenance configuration and job format.

## Read-only Metadata Maintenance Planning

Both backings accept authenticated planning requests at:

```text
POST /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata
```

For example:

```json
{
  "retain-recent-metadata-files": 2,
  "snapshot-expiration": {
    "min-snapshots-to-keep": 1,
    "max-snapshot-age-ms": 86400000
  },
  "compaction": {
    "target-file-size-bytes": 536870912,
    "small-file-threshold-bytes": 67108864,
    "min-input-files": 2,
    "max-rewrite-bytes-per-job": 1073741824
  }
}
```

The optional snapshot-expiration and compaction fields request plans, not commits. An empty JSON object requests ordinary metadata planning. A nonzero retention value overrides the effective maintenance configuration for this request; omission or zero uses the effective configured retention.

In durable-strong mode, successful planning returns HTTP 200 with a completed `DRY_RUN` job and persists its named, current, and latest reports with audit events. Read-only means that table data, metadata pointers, and catalog generations are unchanged; the request still writes maintenance report sidecars and requires the table-scoped `admin:RunTableMaintenance` permission. Inspect the returned plan and any manual-review state before taking further action.

This direct endpoint rejects `delete=true`, `commit-snapshot-expiration=true`, or `commit-compaction=true` with HTTP 400 in durable-strong mode, without publishing a job or changing table objects. The existing object-backed mutation behavior is unchanged. These restrictions do not change the separate explicit maintenance worker or quarantine APIs.

A queued, running, or failed job, active quarantine, or inconsistent current/latest aliases or their named report returns HTTP 409 instead of replacing the existing job or discarding its retry budget. Inspect that state before retrying or taking explicit worker/recovery action. A failed publication that saved only a completed named dry-run report leaves harmless history: no alias advertises a running claim, and replanning is safe because there were no table-object effects. Planning revalidates table identity, metadata pointer, configuration, and the prior report before publication; concurrent changes also conflict. It does not introduce automatic workers, snapshot-expiration commits, or compaction commits on durable-strong backing.

## Enablement

The standalone server reads these variables at startup; the embedded server does not start this scheduler.

| Variable | Default | Accepted value |
|---|---|---|
| `RUSTFS_TABLE_MAINTENANCE_SCHEDULER_ENABLED` | `false` | `true` or `false`; invalid values fail startup |
| `RUSTFS_TABLE_MAINTENANCE_SCHEDULER_BUCKETS` | None | Required when enabled: comma-separated, 1–32 distinct valid bucket names |
| `RUSTFS_TABLE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS` | `300` | Integer from 30 to 86400 |

Example:

```sh
export RUSTFS_TABLE_MAINTENANCE_SCHEDULER_ENABLED=true
export RUSTFS_TABLE_MAINTENANCE_SCHEDULER_BUCKETS=analytics-tables
export RUSTFS_TABLE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS=300
```

The allowlist does not create or enable table buckets. Each bucket must already have an active catalog. The effective bucket default or table override must set `background-enabled=true` and `worker-paused=false`. Deletion remains disabled by default; queueing honors the existing `delete-enabled` setting but never executes the queued operation.

## Admission And Limits

- The first pass is delayed by the configured interval plus 0–10% jitter. Each completed pass waits again; missed ticks do not produce catch-up bursts.
- Each process runs one owned pass at a time, visiting at most eight distinct tables and 64 discovery steps. Bucket turns rotate and namespace/table cursors persist between passes. Restart resumes discovery from the beginning; persisted jobs still prevent duplicate admission.
- A 30-second admission deadline stops starting further store operations. An in-flight store call drains without cancellation or overlap. This is **not** a hard wall-clock, memory, or per-table object-I/O limit: existing planning and history inspection costs still depend on the table.
- The scheduler only admits a first job or a new job after a successful previous job and the configured cooldown. It does not reset failed retry budgets, renew abandoned queues, recover expired workers, or clear quarantine.
- Before publishing, the store rechecks table identity, enabled/unpaused configuration, the observed current job, and complete current/latest report publication under its existing locks. A concurrent manual run, rename, or identity change conflicts rather than overwriting that state.

Use the same interval on enabled nodes. The existing distributed catalog locks coordinate claims; this is not a cluster-wide throughput quota. A busy or corrupt table is reported as a degraded pass and does not remove other buckets from discovery.

## Inspection And Recovery

Inspect the existing endpoint:

```text
GET /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler
```

Use its current job, quarantine status, and recommended actions. An eligible queue can be consumed through the authenticated `POST .../maintenance/worker/run` endpoint. If a job failed, a queue/worker lease expired, or a previous manual invocation left a Disabled/Paused control report, resolve the underlying condition and explicitly invoke `POST .../maintenance/scheduler/run` or the recommended worker/quarantine operation. Automatic admission deliberately leaves these states unchanged.

A partial current/latest publication requires recovery before periodic admission; repeated ticks do not repair or discard it. A conditional worker replay can repair the narrowly defined terminal-publication case below. Review retained reports for other cases; do not delete catalog sidecars directly.

## Conditional Worker Invocation

An operator can bind a worker invocation to the job and attempt observed through the scheduler or job endpoint:

```text
POST /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run
```

```json
{
  "worker-id": "maintenance-worker-a",
  "expected-job-id": "observed-job-id",
  "expected-attempt": 0
}
```

Send both expected fields or neither. Missing one, explicit `null`, invalid types, and unknown fields are rejected. Omitting both retains the existing manual worker behavior; an empty JSON object `{}` still uses the default worker ID. Authentication and the table-scoped `admin:RunTableMaintenance` permission are required in either mode.

The conditional mode consumes an unexpired `QUEUED` job at attempt zero, or retries a `FAILED` job after its persisted `next-retry-after` when its persisted attempt budget allows. It keeps the same job ID, operation, original candidate lists, cleanup watermark, and total attempt budget. The first attempt is allowed even when that budget is zero; zero disables retries. Subsequent failed attempts calculate their delay from the current valid backoff configuration without replacing the saved budget.

Stale job/attempt, catalog identity or pointer/generation changes, expired worker/queue leases, active quarantine, and changed recorded maintenance policy conflict without claiming another attempt. The recorded policy includes retention, quarantine settings, and configuration source; deletion must still be enabled for a delete job. The job format does not store a complete historical configuration snapshot. This endpoint does not promise detection of changes to fields that were never recorded.

If the response is lost, repeat the same worker ID, expected job ID, and expected attempt. When that invocation already claimed the current attempt, the endpoint returns its persisted report without rerunning candidates or consuming another attempt. Inspect `job.status`: a returned `RUNNING` or `FAILED` report is not a successful maintenance outcome. To retry a completed failed attempt after its backoff, explicitly use its new observed attempt number.

All named/current/latest reports must agree before new work starts. If a completed report was persisted but final alias publication was interrupted, the same invocation can reconcile aliases only when the reports prove the same job, attempt, lease, immutable plan, and audit transition. Incomplete claims, conflicting histories, and expired `RUNNING` jobs fail closed for operator inspection; the endpoint does not take over or cancel an abandoned worker. Catalog commits remain protected by the existing publication locks during execution and reconciliation.

To pause admission for a table, set `worker-paused=true` in its effective maintenance configuration. To stop the process-wide scheduler, disable the startup flag and restart. Shutdown stops new admissions and waits for owned queueing work; if completion cannot be confirmed within the normal shutdown grace, it retains unclean-shutdown markers and logs `drain_unconfirmed`. A deadline is not proof that a mutation was cancelled.

This scheduler does not claim automatic worker retry, cross-node worker takeover, bounded history retention, or a fully managed maintenance service.
