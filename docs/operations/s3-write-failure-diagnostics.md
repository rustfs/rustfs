# S3 write failure diagnostics

**Use this when:** distinguishing occasional write failures from a node-wide outage, or interpreting cached storage inventory during an internode failure.

## Measure the failure ratio

`rustfs_s3_http_requests_total` counts external S3 HTTP outcomes independently of the configured log level. Its bounded labels are `method`, `op`, and `outcome`; the exporter target identifies the node. Bucket names, object keys, request IDs, and error text are not metric labels.

The counter increments exactly once when response headers are produced, the service returns an error without a response (`service_error`), or its pending future is dropped (`cancelled`). Outcomes `1xx` through `5xx` classify HTTP responses; `unknown` is reserved for a response outside those classes. A successful response header is not proof that a streamed response body reached the client. Body-stream errors remain separate streaming diagnostics.

The `op` label uses the existing S3 operation names, such as `s3:PutObject`. A request rejected before operation dispatch has `op="unknown"`, while retaining its HTTP method. Include these requests when measuring a node outage. Do not infer `PutObject` from `PUT` alone: bucket and multipart operations also use that method. Admin, console, health, RPC, STS, and enabled non-S3 protocol routes are excluded.

For example, with the usual Prometheus `instance` target label, compare the per-node PUT-method HTTP 5xx ratio:

```promql
sum by (instance) (rate(rustfs_s3_http_requests_total{method="PUT",outcome="5xx"}[5m]))
/
sum by (instance) (rate(rustfs_s3_http_requests_total{method="PUT",outcome=~"[1-5]xx"}[5m]))
```

Inspect `service_error` and `cancelled` separately; neither implies a received HTTP status. A zero denominator or absent series means no observed traffic, not proof of health. Use `rate` or reset-aware deltas because counters restart with the process. The older `rustfs_s3_operations_total` counter measures handler entries and excludes pre-dispatch rejections; it is not this HTTP denominator.

The authenticated admin metrics endpoint exposes the same counters through the optional `http` field in `aggregated` and `by_host`. Request `/rustfs/admin/v3/metrics?types=512&by-host=true&n=1` on each node for HTTP-only data. The default type selection also includes HTTP outcomes. This endpoint remains an NDJSON stream and does not become a cluster-wide peer fanout. `http.requests` contains `method`, `operation`, `outcome`, and `total`; `http.collected` timestamps collection. Compare consecutive samples from the same host. Concurrent snapshots are not atomic across series.

A missing `http` field means an older or non-reporting node, not zero failures. Old map-encoded RPC readers ignore the additive field; new readers accept older snapshots. In mixed-version deployments, check reporting coverage before aggregating a fleet-wide ratio.

## Interpret failed storage probes

Storage inventory includes an `observations` entry for each probed node. The aggregator owns this provenance even when a peer runs an older version.

| Field | Meaning |
| --- | --- |
| `endpoint` | Node whose local inventory was queried. |
| `status` | `succeeded`, `failed`, or `unknown`; this is the probe result, not physical drive health. |
| `cached` | Historical inventory was reused for this response. |
| `last_success_unix_millis` | Wall-clock time of the last successful observation, when known. |
| `snapshot_age_seconds` | Monotonic elapsed age since that observation, when known. |
| `error_code` | Bounded storage error classification for a failed probe, without raw error text. |

After a failed probe, inventory younger than 60 seconds may retain drive identity and capacity, but returned drive `state` and `runtime_state` become `unknown` immediately. Capacity is marked as a `snapshot`; its age advances when the original observation age was known. Expired or absent inventory is synthesized from topology, with capacity observation source `missing`. Repeated polling does not extend this age budget. A successful probe replaces the historical snapshot and clears the failure streak.

An admin RPC timeout or authentication error is not proof of failed physical disks. It also cannot supply fresh evidence of healthy disks. Consequently, cluster health reports can become unready on the first failed probe when remaining known-online drives cannot demonstrate the existing quorum. The quorum thresholds, S3 admission gate, drive-health tracker, and metadata recovery algorithm are unchanged. Consult independent drive and transport diagnostics before replacing a disk. Legacy snapshots without observations have unknown provenance.

The probe round timeout is configured independently; see [Admin peer probe timeout](admin-peer-probe-timeout.md).

## Correlate bounded diagnostics

Normal operation does not require success logs at WARN. Request counters remain available with WARN logging, while runtime readiness diagnostics distinguish `pool_meta_write_blocked`, `pool_metadata_check_timeout`, and insufficient storage quorum. Do not clear a metadata write fence merely to make readiness green.

Query the authenticated `/rustfs/admin/v4/cluster/snapshot` endpoint on the affected node. `snapshot.pool_meta_write_gate` describes that node's metadata writer, not a fleet-wide aggregate. Its existing booleans are preserved; `state` and optional block details extend the same bounded, read-only gate inspection. This starts no recovery, disk reads, or additional RPCs. Runtime readiness and the gate are inspected separately, so a whole cluster snapshot is not atomic across sections.

| `state` | Meaning |
| --- | --- |
| `writable` | No metadata write block was observed. Other dependencies may still make the node unready. |
| `blocked` | A metadata write block was observed; `reason`, `phase`, and `sinceUnixSecs` describe its typed failure context. |
| `check_timeout` | The existing 100 ms inspection budget expired. Readiness remains false, but a write block is not asserted. |
| `unavailable` | The object store or a recognized metadata observation was unavailable; no block details are invented. |

For `blocked`, `reason` reuses the metadata failure classification, `phase` identifies the operation stage that caused the block (not live recovery-worker progress), and `sinceUnixSecs` is the original block time in Unix seconds. Polling does not reset that time. Recovery replaces the observation with `writable` and removes the previous block details. An unavailable store reports `writesReady=false` with `state="unavailable"`. Older responses may omit `state` and block details; missing fields alone do not prove a healthy writer. Operation text, raw replica errors, disk paths, and credentials are excluded. See [Pool metadata recovery](pool-metadata-recovery.md) for recovery and escalation boundaries.

PUT storage failures retain their typed source chain internally and emit bounded S3/storage error codes, I/O kinds, and RPC status codes alongside the existing request ID, bucket, and key. Raw nested error strings and RPC metadata are not logged by this diagnostic. A repeated PUT diagnostic is limited to one event per five seconds; HTTP server-error logs are limited per status code over the same interval for accounted S3 traffic. `suppressed_errors` reports suppressed events at the next emitted event; use the HTTP counter, not log-line counts, to measure failures. HTTP server-error URI diagnostics omit query strings, including presigned credentials.

Typed pool metadata failures additionally carry `pool_metadata_reason`, `pool_metadata_phase`, and `pool_metadata_since_unix_secs` in the PUT diagnostic. These describe the request's failure context: `read_unavailable` before write dispatch is retryable and does not by itself imply a latched write block. Use the current admin snapshot to distinguish that case from a persistent block. Public S3 errors remain sanitized `503 ServiceUnavailable` responses.

Storage inventory emits a WARN event on the first failed probe and an INFO event on recovery, using `event="storage_info_probe"`. A recovery event confirms the RPC succeeded, not that every reported disk is healthy. Bucket metadata load/retry errors include the bucket and a bounded error code, so one failing bucket can be identified without dumping its metadata.

No new environment variable, admin authorization action, or recovery command is required.
