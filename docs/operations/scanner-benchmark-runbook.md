# Scanner Benchmark Runbook

**Use this when:** you need reproducible before/after evidence that a scanner pacing or cycle change reduces background pressure without stalling lifecycle, replication, heal, or bitrot progress, or you are assembling evidence for a scanner-behavior PR.

**Source of truth:** `scripts/run_scanner_validation_harness.sh` (collection, `scanner-summary.csv` columns), `scripts/run_object_batch_bench.sh` (workload), and [Scanner Runtime Controls](scanner-runtime-controls.md) for the meaning of every status field and configuration key.

## Scope

This runbook verifies that scanner pacing and cycle controls reduce background pressure while preserving maintenance progress. It covers mostly idle single-node deployments with many small objects, multi-disk or erasure-set nodes, distributed clusters where scanner pressure mixes with lifecycle, replication, heal, or bitrot queues, and backlog investigations for any of those subsystems.

It does not prove full MinIO parity, site-replication correctness, or replaced-disk heal correctness. Those flows need dedicated distributed tests because their failure modes are not limited to scanner pacing.

## Safety

Run the workload only in a disposable test environment. The commands below can create many buckets and objects and overwrite runtime scanner settings. Record the current scanner and heal configuration before changing anything:

```bash
mkdir -p artifacts
mc admin config get ALIAS scanner > artifacts/scanner-config.before.txt
mc admin config get ALIAS heal > artifacts/heal-config.before.txt
```

The `scanner` and `heal` subsystems are served by `GetConfigKVHandler` (`rustfs/src/admin/handlers/config_admin.rs`, route `/v3/get-config-kv`); this was confirmed by code inspection, not by running `mc` against a live deployment. Replace `ALIAS`, endpoint, and credentials with values for the test deployment. Do not paste production credentials into saved artifacts.

## Required Tools

| Tool | Purpose |
|---|---|
| `mc` or a compatible admin client | Config snapshots and changes. |
| `awscurl` or another SigV4-capable HTTP client | `/v3/scanner/status` and admin metrics. |
| `jq` | Status extraction. |
| `pidstat`, `mpstat`, `iostat`, `top`, or equivalent | Host telemetry. |
| `warp`, `s3bench`, or `scripts/run_object_batch_bench.sh` | Workload generation. |

## Test Matrix

### Formal Scanner/Heal ABBA

The `--abba` mode runs five independent scenario cells: `cold-hot`, `fresh-hot`,
`multi-hot-new`, `running-heal`, and `mrf-replay`. Each scenario runs at least
three A1/B1/B2/A2 groups for both baseline/candidate with background work on,
and candidate-only background off/on. A measured release leg lasts at least 7200
seconds; the minimum matrix contains 120 legs (240 hours before setup/oracles).
The hotpath warp ABBA rig (`scripts/run_hotpath_warp_abba.sh`) supplies the pattern for immutable build
provenance and failure propagation, but its short Warp workload is not this
scanner gate. No scheduled workflow starts this matrix automatically.

```bash
scripts/run_scanner_validation_harness.sh --abba \
  --manifest scanner-abba.json --adapter /path/to/isolated-deployment-adapter \
  --out-dir /path/to/new-artifacts --data-root /path/to/new-test-data
```

Both roots must be new and non-overlapping. Every leg receives a unique data
directory. The runner checks disk capacity before each leg, never removes data,
and stops the adapter after success or failure. Retain raw artifacts and inspect
task ownership before removing any test data. The operator must reserve the
target machines and map the assigned directory to separate data paths on every
node; the runner cannot prove remote isolation from local path names.

The manifest has the following JSON contract (all fields are required):

| Field | Value |
|---|---|
| `schema`, `evidence` | `1`, and `measured` or `synthetic`. |
| `rounds`, `duration_seconds`, `min_free_bytes` | 3..10 groups, 7200..86400 seconds for measured release runs, and the independently estimated free-space reservation in bytes. Synthetic runs may use 1 second. |
| `baseline`, `candidate` | Each contains executable `binary`, full 40-character `revision`, and verified `sha256`. The runner rehashes binaries before every leg. |
| `fixed` | `config_sha256`, `dataset_sha256`, `release_flags`, `durability`, `disk_type`, `cache_state`, `load_command`, `resource_isolation`, `topology` (`EC8+4`), and positive `offered_load_ops`. Hashes use 64 lowercase hexadecimal characters. |
| `release_evidence` | Required for `measured` runs. It declares the 3x4 EC8+4 topology, multi-pool/multi-set coverage, per-node metrics endpoints, same-window distributed sampling, process restart and crash-restart fault modes, mixed-version reader/writer/rollback participation, and allocation/flamegraph/RSS/save-frequency profile artifact requirements. Synthetic runs do not require it and cannot approve release evidence. |
| `oracles` | A map with all five scenario names. Each value contains positive integer `objects`, `versions`, `bytes`, and `sha256` of the independently prepared canonical object/version/content manifest. |
| `expected_healed_objects` | A map with all five scenario names and independently seeded repair counts. Running-heal and MRF-replay require a positive count. |

Record exact build flags and effective durability settings, not just defaults.
Use deterministic workload seeds so every isolated leg has the same expected
object/version/content result. Fix the foreground arrival rate (offered load),
cache preparation procedure, configuration, and hardware across every leg.
Do not include credentials in the manifest, adapter output, or saved commands;
the collector reads `RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` from its environment.
The adapter must echo the measured run's `release_evidence` object in every
measurement response. A mismatch fails the cell because it means the deployment,
mixed-version set, crash mode, or profiler contract no longer matches the
operator-reviewed manifest. This echo checks consistency with the declared plan;
it does not prove that observations share a source window or that the reported
client percentiles are exact. Until those original observations are independently
verified, measured results are marked unverified and cannot produce a PASS or a
release descriptor. The summary command's legacy descriptor option rejects the
request until a producer can verify each field against its original window.

The existing live collector persists the declared load command, scanner/heal
configuration snapshots, and adapter/collector output. It is not a verified
sanitized capture path; use it only after confirming those inputs and outputs do
not contain credentials, private configuration files, or sensitive commands.

#### Deployment Adapter Contract

The runner invokes an executable as `adapter ACTION request.json response.json`
with no shell evaluation. Actions are separate processes: `prepare`, `measure`,
`oracle`, and `stop`. Every action must return zero and write a JSON object of
at most 1 MiB. Logs are kept separately and require an operator-managed disk
quota. Missing output, timeout, nonzero exit, unknown/missing metrics, zero
samples, and request errors fail the run. Adapters must terminate their own
children on failure and `stop` must be idempotent even after partial preparation.
The runner keeps its session leader unreaped while stopping a failed command
or collector: it sends TERM, allows the existing ten-second grace period, then
kills the remaining process group before reaping. This prevents a parent exit
from hiding live descendants or allowing the group ID to be reused before its
last signal. A successful `prepare` preserves adapter-owned services until
`stop`; services that leave the command's process group remain the adapter's
cleanup responsibility.

The request contains the fixed manifest fields, selected build, scenario, round,
leg, comparison (`build` or `background`), background mode (`on` or `off`),
duration, unique `data_dir`, expected object oracle, and expected repair count.
Adapter responsibilities:

1. `prepare` deploys the selected binary into an authorized isolated topology,
   checks actual binary/config/durability, initializes deterministic scenario
   data and the requested cache state, and returns `{"ready": true}`. For measured
   runs it also returns `collector` with exactly `alias`, `endpoint`, and
   comma-separated `metrics_endpoints`; the runner starts the existing scanner
   collector at 60-second cadence while `measure` runs.
2. `measure` maintains the fixed offered load for the entire requested duration.
   `cold-hot` retains cold buckets while mutating a hot bucket; `fresh-hot`
   creates a bucket after scanner startup; `multi-hot-new` combines several hot
   buckets with a newly created bucket; `running-heal` applies foreground load
   during active repair; `mrf-replay` replays independently seeded durable repair
   work. Capture same-window status for bucket-freshness issue #7108. Actual
   fault injection and dataset generation belong to the reviewed adapter.
3. `oracle` independently enumerates all objects and versions, reads and checks
   their complete bytes, and verifies repairs. Return `complete: true`, integer
   `errors: 0`, and `actual` matching the manifest's expected oracle. Never copy
   expected values into a measured oracle or infer completion from empty queues.
4. `stop` stops task-owned workload/server processes and returns `stopped: true`.
   Preserve data and artifacts for diagnosis. An adapter may restore previous
   settings but must not delete arbitrary paths or stop unrelated deployments.

#### Linux service control

`scripts/scanner_service.py` supplies node-local `start`, `status`, `resize`
and `stop` operations for an adapter on Linux with Python 3.11+, systemd 255+
and cgroup v2. Run it as the benchmark user with noninteractive sudo access.
It is a service primitive, not a complete deployment or measurement adapter.
The caller still owns topology, data isolation, readiness, load, fault actions,
the independent oracle and metric provenance.

Before starting any node, retain its immutable JSON plan on the controller and
node. Use a new UUID for each node/service and a private, user-owned directory
(mode `0700`). Provision a verified executable and a separate environment file
(mode `0600`) inside it. Keep credentials out of the plan, arguments and archived
artifacts. The plan has exactly these fields; replace the example digest with
the actual binary SHA-256:

```json
{
  "run_id": "20718acd-9e81-4697-a0b5-8ac3207f6aef",
  "root": "/srv/rustfs-bench/run",
  "binary": "/srv/rustfs-bench/run/rustfs",
  "binary_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "environment_file": "/srv/rustfs-bench/run/private.env",
  "args": ["/data/isolated-benchmark"],
  "runtime_seconds": 10800,
  "cpu_percent": 200,
  "memory_bytes": 4294967296
}
```

```bash
python3 scripts/scanner_service.py start node-plan.json
python3 scripts/scanner_service.py status node-plan.json
python3 scripts/scanner_service.py resize node-plan.json --cpu-percent 100 --memory-bytes 3221225472
python3 scripts/scanner_service.py stop node-plan.json
```

The plan determines the transient unit's name and ownership identity. Recovery
uses that same plan after SSH loss, including when the start response was lost.
An existing service is never restarted by `start`; reconcile with `status` or
`stop`. Stop every planned node after partial preparation, including nodes that
never started. Stop is idempotent and does not delete binaries, data or evidence;
retain the private root and plan until cleanup completes. File locking serializes
controllers, and operations reject a unit with a different plan, user or root.
Inputs must remain immutable while starting; this is not a sandbox against a
compromised benchmark user or privileged host administrator.

The finite `runtime_seconds` limit is enforced by systemd independently of SSH.
Choose it to cover preparation, the entire required measurement and the oracle;
expiration fails the cell and must not shorten its required duration. The unit
does not restart automatically. Shutdown targets the whole cgroup, allows 30
seconds for termination, then kills remaining processes, including descendants
that changed sessions. Resize reports the kernel CPU/memory limits and rejects
a changed PID or mismatched limits. Applying limits alone does not demonstrate
RustFS adaptation or pass the resource hot-shrink gate.

#### Running-process evidence

After readiness and before each measurement, run `scripts/scanner_process_receipt.py`
as the same benchmark user. Supply the immutable service plan, a JSON object with
the expected non-secret environment, and one `--data-root` per dedicated local
data directory. The environment must exactly match the script's supported
configuration keys present in the process; an unknown `RUSTFS_` key fails closed.
Credential variables are excluded. Keep both input files private and archive
only the resulting receipt, never the environment file or raw `/proc` contents.

```bash
python3 scripts/scanner_process_receipt.py node-plan.json public-environment.json --data-root /data/isolated-benchmark
```

The receipt binds the boot ID, PID/start ticks, actual `/proc/PID/exe` SHA-256 and
inode, expected arguments/configuration hashes, service identity, observed kernel
CPU/memory limits, and directory device/inode/capacity. It rejects changes seen
between its two process observations or replacement of the executable or data
directories. Configuration values and raw process arguments are not emitted.
The observations span a reported interval, not an atomic snapshot; they do not
protect against a compromised benchmark user or administrator. Resource limits
are observations, including legitimate hot-shrink changes, and must be checked
against the workload plan by the adapter.

Environment bytes do not establish which configuration the server accepted.
Listed directories are caller-selected, not proof of the server's open volumes,
device isolation or SSD/HDD class. Those claims, the binary's source/build
provenance and the independent data oracle still need separate evidence. The
receipt explicitly leaves effective configuration, directory-to-server binding
and storage acceptance unverified. A failed observation produces no receipt;
retain the failure and reconcile the service before continuing.

The `measure` response echoes the observed `evidence`, `fixed`, `build`,
`data_dir`, and `background`, plus `sample_count` (1..3600), `elapsed_seconds`,
and `metrics`. All metrics must be finite nonnegative numbers: `p95_ms`, `p99_ms`,
`throughput_ops`, `rss_bytes`, `cpu_seconds`, `iops`, `rpc_count`,
`cache_clone_bytes`, `encode_bytes`, `save_bytes`, `oldest_age_seconds`,
`walk_objects`, `cold_walk_objects`, `healed_objects`, `errors`, and `requests`.
The adapter also reports the measurement-window delta of
`rustfs_heal_mainline_throttle_total{source="admin",result="delayed"}` as
`heal_mainline_throttle_delayed`; a cumulative process-lifetime value is not a
valid input.

The clone, encode, and save byte fields are also deltas from the same window.
Requests, throughput, and p99 must be positive; errors must be zero. Repair
counts must match the manifest when background work is on. Keep underlying
request samples, counter reset checks, profiler captures, and per-node telemetry
in the cell artifact directory; aggregate values alone do not establish their
measurement provenance. Missing production instrumentation is a pending gate,
not permission to report a fabricated zero.

For measured cells, retain the original schedule, start, and completion ledgers
and their references in `client_request_samples`. The runner reconciles request
identities, counts, timestamps, errors, and file hashes before calculating
nearest-rank percentiles from the recorded worker-start-to-completion samples.
The current adapter timestamps worker start before its ledger flush, so those
samples also include local logging and do not establish SDK-call latency.
Missing HTTP status does not prove zero 503s. The current ledger format does not
prove that the offered load reached the service or that the client and node
windows coincide, so a measured release result remains unavailable until those
boundaries are verified.
The four ABBA legs cover different windows: their per-leg percentiles are
reported separately, not renamed as a combined p95 or p99. Ledger reduction
uses a size-limited scratch database and samples free space; those checks do not
reserve disk space or prevent concurrent writers from crossing the floor.

The live telemetry collector starts its HTTP captures concurrently and records
their actual transport intervals. These receipts explicitly report
`synchronized_source_window=false`: concurrent collection does not establish
that the servers measured the same source window. The adapter must separately
prove alignment with the client window before claiming same-window acceptance.

For usage-cache saves, `encode_bytes` uses the window delta of
`rustfs_scanner_cache_serialized_bytes_total`. `save_bytes` uses the sum of
`rustfs_scanner_cache_save_payload_bytes_total` deltas for `cache="main"` and
`cache="backup"`.
The latter counts each full encoded payload submitted to the save interface
after publication admission, including retries, failed writes, and timeouts.
A rejected admission, skipped backup, or read-after-error reconciliation adds
no save bytes. These are submitted application bytes: a failed attempt may
consume only part of its body, and lower-layer retries, erasure parity, metadata,
and physical disk writes are outside this counter. Keep the same definition on
both builds; retain disk counters separately. Neither counter measures cache
clone allocations, and an absent series is not a measured zero.

`walk_objects` can use the aligned window delta of
`rustfs_scanner_object_metadata_visits_total`, summed across the declared
`bucket` and `disk` identities. Each visit starts after scanner I/O admission,
immediately before reading an `xl.meta` entry. Failed or cancelled reads still
count; payload entries, cached summaries, and paths skipped before admission do
not. Re-reading an object counts again, including on another drive. This measures
scanner metadata visits, not unique S3 objects, versions, successful repairs, or
physical disk operations. The counter handle is registered once per bucket/drive
walker and reused without per-object label allocation; recording still has a cost.

For a workload with distinct hot and cold buckets, `cold_walk_objects` selects
only the cold bucket identities declared in the independent workload manifest.
Do not infer coldness from the observed counts or multiply bucket-walk counts by
cached object totals. This metric cannot separate hot and cold prefixes within
one bucket; that attribution remains unverified. Warm up the required series,
retain node/drive identities, reject resets or missing window boundaries, and
use the same visit definition on both builds. Bucket labels follow the existing
bucket/drive scanner metrics and have bucket-scale cardinality; no key or version
is a label. An absent or dropped series is not zero.

On Linux with a readable cgroup v2 hierarchy, the existing memory observation
worker also samples the process's current cgroup and effective CPU affinity.
`rustfs_background_resource_cpu_millis`, `memory_limit_bytes`, and
`memory_headroom_bytes` (all with the `rustfs_background_resource_` prefix)
reflect visible ancestor limits, sibling memory use, and host available memory.
Ancestors hidden by the mount/cgroup namespace are not observable; these values
are not a reservation or proof of every enclosing resource limit. The
startup-only container gauges retain their existing meaning. These new gauges
are observations; they do not resize scanner/heal workers or prove hot-shrink
acceptance.

Require `rustfs_background_resource_sample_available = 1` and a fresh
`rustfs_background_resource_observed_timestamp_seconds`. Sampling follows
`RUSTFS_MEMORY_OBSERVABILITY_INTERVAL_SECS` (15 seconds by default), not a
one-second control loop. A failed sample clears availability and leaves the
last successful timestamp and values unchanged. Missing/invalid limits, invisible
membership, or oversized files must not be interpreted as host capacity or zero
pressure. Other platforms and cgroup v1 have no sample from this observer.

The `cpu_usage_microseconds`, `cpu_throttled_microseconds`, `cpu_stall_microseconds`,
`memory_stall_microseconds`, and `io_stall_microseconds` gauges, under the same
prefix, expose cumulative leaf-cgroup counters. Each requires its own
`rustfs_background_resource_counter_available{resource=...}` flag (`cpu_usage`,
`cpu_throttled`, `cpu_stall`, `memory_stall`, or `io_stall`). They do not summarize
ancestor throttling. `identity_high` and `identity_low` preserve the cgroup
generation hash in two exact 32-bit gauge values; a change invalidates counter
differences. Floating-point counter exports cannot establish exact large-integer
deltas. These series are not an atomic cross-instrument snapshot: retain the raw
sample times and do not manufacture a common acceptance window.

Each comparison records a `w22` section with clone, encode, and save bytes per
walked object, clone/encode and save/encode byte ratios, and candidate changes.
These are traffic amplification indicators, not allocation attribution or an
fsync profile. The `running-heal` build comparison also records a `w10`
section. `status=observed` requires sampled high foreground pressure, at least
one admin pacing delay in the same window, and an improvement in either
foreground p99 or throughput. `no_measured_benefit` means pacing ran but neither
foreground metric improved; `pending` means the run did not prove that pacing
engaged; `inconclusive` means ABBA repeatability failed. Baseline and candidate
delay counts are both retained so an operator can reject unrelated or
process-lifetime counter contamination. Correct repair oracles and the existing
regression limits still apply in every case.

The `running-heal` build comparison also records a `w11` section for the bounded
retry-window evidence. `status=observed` requires same-window healthy-page
latency improvement, reduced heal lock-wait p99, bounded candidate RSS growth,
and a candidate attempt-cost value. `rss_regression` means latency and lock-wait
improved but RSS exceeded the allowed growth limit; `no_measured_benefit` means
attempt-cost evidence exists without the full W11 benefit; `pending` means the
attempt-cost evidence needed for the comparison is missing.

For P2, `measure.convergence` contains booleans `writes_stopped`,
`last_mutation_observed`, `first_complete_publication`; numeric
`last_mutation_time`, `last_mutation_observed_time`, `writes_stopped_time`, `window_start`, `window_end`,
`budget_available_seconds`, `walk_objects`, and `full_walk_objects`. Times use
one monotonic clock. The window starts after writes stop and the final mutation
is observed, and ends at the first complete publication. The reference is an
independent full walk of the same static namespace. Record available budget
seconds to interpret elapsed time. During continuing writes, omit this proof
and report useful-work ratio and justified invalidation/re-scan work separately;
the runner reports P2 pending and does not impose a fixed cumulative walk bound.

The nightly heal workflow clones **`rustfs/auto-testing`** separately and invokes
`auto-testing/rustfs_heal_test.sh`; that script is not a local `scripts/test`
entry point. If an adapter uses it, record and verify the external checkout's
owner and full commit before use. The current workflow clones the default branch,
so its contents must not be attributed to a RustFS source SHA.

#### Independent version oracle

`scripts/scanner_version_oracle.py` supplies the read-only S3 oracle portion of
an adapter. Stop workload mutations first. Prepare an inventory independently
from deterministic payloads and acknowledged PUT/DELETE receipts; do not derive
the expected keys, bytes or hashes from the listing being checked. Its JSON
contains `buckets` (the complete owned bucket scope, including empty buckets)
and `versions` with these fields per retained version:

| Field | Meaning |
|---|---|
| `bucket`, `key` | Owned bucket and exact object key. |
| `generation` | Stable logical generation from the workload plan, such as `1`, `2`, or `delete-3`. Unique per key. |
| `version_id` | Actual server ID from the write receipt, including literal `null` for an unversioned/null version. Unique per key. |
| `is_latest`, `delete_marker` | Boolean expected final state. Exactly one latest entry per key. |
| `bytes`, `sha256` | Independently computed full payload length and SHA-256. Delete markers use `0` and JSON `null`. |

The comparable canonical digest sorts by bucket, key and logical generation,
then hashes compact, key-sorted UTF-8 JSON of those records without `version_id`.
Server-assigned IDs differ across isolated legs. They are still checked exactly
against every endpoint's listing and version GET, and bound in a separate
`version_bindings_sha256` receipt. Removing an overwritten null version or a
permanently deleted version from the expected inventory must follow the actual
planned mutation and its receipt, not an observed omission.

```bash
python3 scripts/scanner_version_oracle.py expected-inventory.json \
  --endpoint node-a=http://192.0.2.10:9000 \
  --endpoint node-b=http://192.0.2.11:9000 \
  --output new-oracle-evidence.json
```

The CLI needs boto3, reads credentials through the AWS credential provider chain,
uses path-style SigV4 requests and disables SDK retries. It never writes S3 data.
For Python workload seeding with pooled S3 connections, use botocore 1.43.77 or
newer: older versions can retain a zero-length PUT's response state and misparse
the next PUT, causing a read timeout even with valid server response framing.
See the [upstream fix](https://github.com/boto/botocore/pull/3784).
Record the exact SDK and transitive dependency versions, package hashes and
effective checksum settings, and use the same client environment for both legs.
Retain failures from older clients; do not hide them with retries or a larger
timeout. A client fix does not establish that server-side 503s are resolved.

Every endpoint gets independently paginated version and current-object listings,
full streamed GETs for every data version and every current live object, and
explicit/current delete-marker checks. A hidden old version, extra key, wrong
latest flag, corrupt/truncated body, unexpected error or changing namespace
fails the oracle. Expected delete-marker GET responses follow the
[S3 GET contract](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/get_object.html);
a 503 is never treated as a deleted object.

The evidence file retains observed version records and per-endpoint summaries.
`actual.objects` counts current live keys, `actual.versions` includes data
versions and delete markers, and `actual.bytes` sums all retained data-version
bytes once. The stdout summary can feed the adapter's `oracle` response after
checking the exit code; retain the full evidence file separately. Output must
be a new file, and failure never produces a passing response. Workers and queued
GETs are bounded (`--workers`, default 8, maximum 64); each body is read in 1 MiB
chunks. This checks S3-visible content and versions, not every on-disk erasure
fragment, and does not replace the seeded repair or performance gates.

#### In-Process Foreground Windows

When scanner or heal is enabled, a one-second process-local sampler tracks external S3 requests from the outer service call through response-body completion, stream failure or cancellation. It runs independently of the metrics export switch and stops with background services. Requests and their existing body buffers are not copied or moved to another task. The start boundary is `Service::call`; connection establishment and Tower readiness waits before that call are outside this window. A successful body observation means the server produced its final frame, not that the client received or acknowledged it. HEAD and bodyless HTTP statuses finish at response headers. Admin, health, console, STS and internal RPC traffic are excluded by the existing external S3 route classifier.

Windows preserve request starts, in-flight requests and completed, failed and cancelled counts in four fixed classes: `read`, `write`, `metadata` and `other`. Cancellation is a censored observation and contributes no completed-latency sample. P99 is an inclusive nanosecond interval over non-cancelled completions, including errors, and is unavailable below 100 samples; 100 samples permit rank calculation but do not establish statistical confidence. The overflow bucket has no upper bound. The sampler brackets each atomic window cut with two monotonic clock readings. A writer delayed across a cut delays publication without changing those readings. Consumers must check validity, age and cut uncertainty; a missing or low-sample window does not prove that foreground work is idle.

With metrics export enabled, `rustfs_background_foreground_window_*` gauges expose validity, cut uncertainty, duration, starts, in-flight work and per-class counts and percentile bounds. Check the class and percentile availability gauges before reading retained numeric values. Exported floating-point gauges are diagnostic; the in-process counters retain integer precision. These local windows neither align different nodes nor provide the exact client-side percentile required by the SSD ABBA gate. They supply observations, without enabling an adaptive admission policy or completing performance acceptance.

#### Histogram Window Evidence

Use `scripts/scanner_otlp_window.py` to subtract two cumulative explicit-histogram or monotonic integer-counter snapshots before deriving window evidence. It reads OTLP JSON, or binary OTLP with `--format protobuf` when `opentelemetry-proto` is installed in the active Python environment. Pin and record that dependency in the adapter environment. Keep each original export and its source-node association; output includes the SHA-256 of both inputs.

```bash
python3 scripts/scanner_otlp_window.py \
  --before before.json --after after.json \
  --metric rustfs_heal_admission_start_duration_seconds \
  --start-ns "$WINDOW_START_NS" --end-ns "$WINDOW_END_NS" \
  --out histogram-window.json
```

The command requires matching resource, scope, unit, labels, bucket boundaries and cumulative start time at both boundaries. It rejects missing series, resets, decreasing bucket counters, no-recorded-value flags and duplicate series. Warm up every required label combination before taking the first snapshot; an absent series is not an implicit zero. The optional window arguments must both be present and match every selected point's collection timestamps exactly. Without them, `window_checked` is false and each series retains its actual interval; this is diagnostic output, not same-window release evidence. Independently timed exports from different nodes must not be presented as synchronized samples.

The p95/p99 outputs are bucket intervals, in the metric's original unit, with an exclusive lower and inclusive upper bound. A null endpoint is unbounded; a null percentile means no observations. Historical maxima and minima cannot be subtracted to obtain window maxima or minima. The tool does not interpolate, average per-node percentiles, or turn interval endpoints into the scalar percentiles required by the ABBA gate. Obtain aligned observations with sufficient resolution before reporting those scalars. It also does not change the meaning of the underlying metric: `rustfs_heal_admission_lock_phase_seconds` includes lock acquisition waits and is not an exact lock-hold measurement.

Integer counters use the same `--metric` selection and identity/window checks. Their rows have `kind: "sum"` and an exact integer `delta`; histogram rows have `kind: "histogram"`. Counters must be cumulative, monotonic, and nonnegative OTLP `asInt` values within signed int64 range. Floating-point sums, resets, decreases, type changes and absent values are rejected. A present unchanged counter produces a measured zero delta; missing observations never do. No summation across nodes or labels is implicit: retain those identities when aggregating a counter such as `rustfs_scanner_cache_serialized_bytes_total` across a proven common window.

#### Heal Object Lock Wait

`rustfs_heal_object_lock_wait_seconds` measures namespace acquisition in the set-local heal paths. Timing starts when the acquisition future is first polled, after namespace construction and existing diagnostic setup, and stops when acquisition returns or the pending future is dropped. It excludes repair work and lock hold time. Observation returns the original guard and error; it does not change release points, timeouts or retries.

The fixed labels are `op` (`heal_object`, `read_repair_data`, `read_repair_commit`, `heal_object_dir`, `check_abandoned_parts`), `mode` (`read`, `write`) and `result` (`acquired`, `failed`, `cancelled`, `disabled`). The ordinary outer heal lock and explicit-version helper both use `heal_object`; these count actual acquisitions, not logical repair tasks. `no_lock` and errors constructing a namespace produce no acquisition sample. No bucket, key, version or owner is a label, and the metric does not require object-lock diagnostic logging.

Keep acquired and failed completed-attempt distributions, cancellation counts, and disabled-lock observations distinguishable in W11 evidence. A cancelled sample is elapsed wait before cancellation, not a completed acquisition; a disabled guard does not establish real contention. Do not discard failed waits to improve a percentile, infer task-attempt counts from acquisitions, or substitute admission mutex timing for this metric. Window differences and histogram resolution still need independent verification; this instrument alone does not supply exact scalar percentiles or complete SSD acceptance.

#### Evidence Gates

`report.json` records each group's verdict and the raw responses remain in their
cell directories. Candidate/build p99 regression must be at most 5% and
throughput loss at most 3%; candidate background on/off limits are 10% and 5%.
P1 requires cold-hot walk reduction of at least the baseline cold-walk share
times 80%, rather than a fixed 80% reduction for every workload. P2 requires
candidate post-stop work at most 1.2 times the independent full-walk reference.
Missing candidate convergence proof yields `inconclusive`. A2/A1 or B2/B1 p99
or throughput drift above 5% also yields `inconclusive`, with exit code 3.
Correctness errors and non-noisy performance regressions exit 1. Every group
must pass; a favorable median cannot hide a failing group.

Synthetic success is explicitly `synthetic_validated`, with `performance:
pending`. It validates orchestration and gate logic only. It proves no runtime,
distributed, crash, mixed-version, or performance behavior and cannot close the
performance acceptance gate. Run the fake-adapter self-tests with:

```bash
scripts/test_scanner_validation_harness.sh
```

For CI summaries, PR evidence tables, and operator handoff, collapse the raw
matrix into a quiet one-line verdict plus durable JSON/Markdown artifacts:

```bash
scripts/summarize_scanner_heal_perf.py \
  --abba-dir /path/to/new-artifacts \
  --cache-cost-log /path/to/cache-cost-profile.log \
  --json-out /path/to/new-artifacts/perf-summary.json \
  --markdown-out /path/to/new-artifacts/perf-summary.md
```

The command prints only `PASS scanner_heal_perf ...` for measured passing ABBA
evidence, otherwise `FAIL scanner_heal_perf ...`. The JSON and Markdown outputs
carry the key p99/throughput/P1/P2/cache-cost fields and artifact provenance
hashes; raw per-cell logs remain in the original artifact tree for audit.
Failed or interrupted ABBA reports that contain only `status`, `performance`,
`completed_cells`, and `error` also summarize as `FAIL`; they do not become
performance evidence, and a missing comparison matrix is accepted only for a
non-passing report.
Measured passing reports must also retain the W10/W11 foreground-pressure,
heal-lock-wait, and heal-attempt-cost fields emitted by the ABBA evaluator. If
those fields are removed, empty, malformed, or length-mismatched, the quiet
summary fails closed instead of treating the report as performance evidence. For
`running-heal` build comparisons, the summary additionally requires the emitted
W11 section to be `observed` and to retain the RSS-growth, lock-wait,
healthy-page-latency, and candidate attempt-cost values.

They cover the complete 120-cell schedule, data isolation, missing builds and
oracles, zero samples/requests, swallowed request errors, offered-load drift,
incomplete repairs, missing metrics, noise, and P1/P2/p99 regressions. A real
deployment adapter and actual ABBA artifacts remain required before any measured
performance or release claim.

Collect at least two runs on the same RustFS commit and the same workload. Keep hardware, commit, object count, object size, bucket count, scanner-enabled state, and foreground workload constant between runs.

| Run | Purpose | Example scanner settings |
|---|---|---|
| Baseline | Observe current behavior without additional pacing changes. | Existing config. |
| Pacing override | Measure whether cooperative scanner sleeps reduce pressure. | `scanner.delay="30"` and `scanner.max_wait="15"`. |
| Duration budget (when one cycle is too long) | Bound wall-clock time per cycle. | `scanner.cycle_max_duration="1800"`. |
| Object budget | Bound objects processed per cycle. | `scanner.cycle_max_objects="1000000"`. |
| Directory budget | Bound directories entered per cycle. | `scanner.cycle_max_directories="100000"`. |
| Disk I/O budget (single slow disk) | Cap scanner metadata I/O per local disk regardless of idle time; compare foreground p99 and scan completion against the media default. | `scanner.disk_io_budget_iops="50"` and `scanner.disk_io_budget_bytes_per_sec="8388608"`; `off` removes the cap for an A/B leg. |

## Deployment Matrix

Use the smallest deployment that reproduces the symptom. The single-node, single-disk run is the cheap, repeatable baseline; it is not sufficient for PRs that claim to improve distributed queue behavior, replication repair, or heal/bitrot admission.

| Deployment | What it validates | Minimum evidence | Workload shape |
|---|---|---|---|
| Single-node, single-disk | Small-object scanner pressure, pacing, cycle interval, basic progress. | Scanner status time series plus host CPU and disk telemetry. | One node, one data disk, several buckets, at least 100,000 small objects, scanner enabled, no sustained foreground workload during observation. |
| Single-node, multi-disk or erasure set | Set and disk scan concurrency, cycle budgets, checkpoint movement, usage cache persistence, active path age. | Scanner status time series, per-disk host telemetry, before/after data usage freshness. | Same as above across all disks. |
| Distributed cluster | Lifecycle transition queues, bucket replication repair admission, scanner-originated heal and bitrot admission, queue/backlog pressure under cross-node work. | Scanner status time series from the cluster, host telemetry from each node, subsystem-specific queued/skipped/missed counters. | Same structure plus the relevant subsystem condition (lifecycle rules, a replication target, a heal/bitrot scenario); keep status and telemetry cadence identical to the baseline. |

Generate object traffic with the repository script if `warp` or `s3bench` is installed; repeat with new buckets or prefixes if one run cannot create enough objects, and record the final object count:

```bash
scripts/run_object_batch_bench.sh \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --bucket scanner-bench \
  --auto-new-bucket \
  --concurrency 64 \
  --duration 10m \
  --sizes 1KiB,4KiB,16KiB \
  --warp-mode put \
  --out-dir artifacts/object-load
```

## Status Collection

Capture scanner status before the workload, after the workload finishes, and throughout the idle observation window. The validation harness does this repeatably and writes scanner/heal config snapshots, scanner status samples, background heal status samples, host telemetry when available, run metadata, `scanner-summary.csv`, and `scanner-validation-report.md`:

```bash
export RUSTFS_ACCESS_KEY="<admin-access-key>"
export RUSTFS_SECRET_KEY="<admin-secret-key>"

scripts/run_scanner_validation_harness.sh \
  --alias ALIAS \
  --endpoint http://127.0.0.1:9000 \
  --deployment single-disk \
  --workload-label small-object-idle \
  --samples 30 \
  --interval-secs 60 \
  --out-dir artifacts/scanner-validation
```

For per-node distributed evidence pass `--metrics-endpoints` (comma-separated). Each sample then stores `/v3/scanner/status`, one `/v3/background-heal/status` response per listed endpoint, and one by-host admin metrics response per listed endpoint; without it, background-heal status is captured only from `--endpoint`:

```bash
scripts/run_scanner_validation_harness.sh \
  --alias ALIAS \
  --endpoint http://node-a:9000 \
  --deployment distributed \
  --workload-label lifecycle-replication-heal-backlog \
  --metrics-endpoints http://node-a:9000,http://node-b:9000,http://node-c:9000,http://node-d:9000 \
  --samples 30 \
  --interval-secs 60 \
  --out-dir artifacts/scanner-validation-distributed
```

For ad hoc per-node snapshots outside the harness window, use the by-host `awscurl` loop in [Reading Distributed Metrics](scanner-runtime-controls.md#reading-distributed-metrics); the metrics endpoint reports only the node that handles the request.

Host collectors are optional. Check `host-telemetry-status.tsv` before using their output: each started collector records its exit status, and a failure emits a warning while scanner status collection remains available. A missing collector row is not a zero measurement. Each run resets its host telemetry artifacts, including when collection is skipped or the interval is zero. If a scanner status request fails, the harness stops and waits for its started collectors while preserving the request failure exit code; collector statuses are recorded when the output file remains writable. On macOS the harness uses `iostat -d -w <interval> -c <samples>`; Linux retains `iostat -xz <interval> <samples>`. These are host/device aggregates, not RustFS process I/O or filesystem syscall counts. The initial iostat report may cover time since boot; do not treat it as an aligned workload interval. `fs_usage` still requires the appropriate macOS privileges. Without it, report filesystem-event counts as unavailable and keep CPU/progress observations separate from production-burst attribution.

### Bucket metrics freshness validation

Use the harness around a post-start bucket creation workload to cover the timing where scanner startup sees no buckets, a bucket is created afterwards, and the first metrics collection must not confuse a cold usage cache with real zero usage:

1. Start RustFS from an empty data path.
2. Start the harness before creating buckets.
3. Create a bucket, upload objects, and keep the harness running until at least one usage save is observed.
4. Compare `scanner-summary.csv` with `/rustfs/admin/v3/realtime?types=1&n=1` bucket metrics.

Expected evidence: dirty usage is marked, `life_time_scan_cycle` or `life_time_scan_bucket_drive` advances, `life_time_scan_object` advances for object workloads, and `life_time_save_usage` plus `usage_last_save_result=success` appear before non-zero bucket usage metrics are accepted as fresh.

### Manual status sampling

Single snapshot:

```bash
awscurl \
  --service s3 \
  --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" \
  --secret_key "$RUSTFS_SECRET_KEY" \
  --request GET \
  'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' \
  | jq . > "artifacts/scanner-status.$(date -u +%Y%m%dT%H%M%SZ).json"
```

Time series (stop after the planned observation window):

```bash
mkdir -p artifacts/status
while sleep 60; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  awscurl \
    --service s3 \
    --region us-east-1 \
    --access_key "$RUSTFS_ACCESS_KEY" \
    --secret_key "$RUSTFS_SECRET_KEY" \
    --request GET \
    'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' \
    | jq . > "artifacts/status/scanner-status.${ts}.json"
done
```

## Host Telemetry

Collect host metrics over the same window as scanner status. If `pidstat` is unavailable, use `top`, `ps`, or the platform monitoring system, but record the sampling interval and window in the report.

```bash
pidstat -p "$(pidof rustfs)" 60 > artifacts/pidstat.txt
iostat -xz 60 > artifacts/iostat.txt
mpstat 60 > artifacts/mpstat.txt
```

## Runtime Tuning Examples

Persistent scanner config values use seconds for time fields; use numeric strings, not duration suffixes. The canonical persistent bitrot cadence belongs to the `heal` subsystem.

```bash
mc admin config set ALIAS scanner delay="30" max_wait="15"
mc admin config set ALIAS scanner cycle="3600"
mc admin config set ALIAS scanner cycle_max_duration="1800"
mc admin config set ALIAS scanner cycle_max_objects="1000000"
mc admin config set ALIAS scanner cycle_max_directories="100000"
mc admin config set ALIAS heal bitrot_cycle="2592000"
```

Environment variables take precedence over persisted config and should be recorded separately:

```bash
RUSTFS_SCANNER_DELAY=30
RUSTFS_SCANNER_MAX_WAIT_SECS=15
RUSTFS_SCANNER_CYCLE=3600
RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS=1800
RUSTFS_SCANNER_CYCLE_MAX_OBJECTS=1000000
RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES=100000
RUSTFS_SCANNER_BITROT_CYCLE_SECS=2592000
```

After each config change, read scanner status and confirm the effective value and `source` under `runtime_config`.

## Observation Window

Use the same window for each run:

1. Generate or verify the object namespace.
2. Wait until foreground workload is idle.
3. Save scanner and heal config.
4. Save one scanner status snapshot.
5. Collect scanner status and host telemetry for at least 30 minutes, or for one complete scanner cycle when practical.
6. Save one final scanner status snapshot.

Longer windows are better for cycle interval comparisons. Short windows are acceptable for quick pressure checks only if the conclusion avoids changing defaults.

## Fields To Compare

Field semantics are defined in [Scanner Runtime Controls](scanner-runtime-controls.md); the decision fields for a before/after comparison are:

| Field | Decision it supports |
|---|---|
| `runtime_config.*.value` and `runtime_config.*.source` | The tested settings actually took effect. |
| `metrics.pacing_pressure.primary_pressure`, `last_cycle_total_pause_ratio` | Where pressure comes from and how much of the cycle was cooperative pause. |
| `metrics.maintenance_control.primary_control`, `metrics.maintenance_control.sources` | Whether a maintenance source is blocked, deferred, active, or only pacing-limited. |
| `metrics.current_cycle_objects_scanned`, `metrics.current_cycle_directories_scanned` | Scan progress continues. |
| `metrics.last_cycle_result`, `last_cycle_partial_reason`, `last_cycle_partial_source` | Whether the previous cycle completed, which budget stopped it, and which source consumed it. |
| `metrics.source_work`, `metrics.current_cycle_source_work`, `metrics.last_cycle_source_work` | `missed` growth per source is a downstream admission problem, not pacing. |
| `metrics.replication_repair` (and current/last-cycle variants) | Repair kind, `scanner_role`, and `execution_owner` for replication backlog runs. |
| `metrics.lifecycle_expiry.{current_queued,current_active,queue_missed,scanner_missed}` | Expiry backlog and admission failures. |
| `metrics.lifecycle_transition.{scanner_missed,queue_full,compensation_pending,failed}` | Transition backlog, queue pressure, and worker failures. |
| `metrics.usage_freshness.*`, `metrics.current_cycle_usage_saves`, `metrics.last_cycle_usage_saves` | Bucket metrics freshness; `last_usage_save_result` must be `success`. |
| `metrics.life_time_ops.{scan_cycle,scan_bucket_drive,scan_object,save_usage}` | Cycles, bucket-drive scans, object scans, and `DataUsageInfo` saves actually happened after the workload. |
| `metrics.scan_checkpoint`, `metrics.oldest_active_path_age_seconds` | Partial cycles preserve resume context; stuck paths. |

Do not use a single CPU spike as the conclusion; compare average and p95 CPU over the same observation window.

For heal or bitrot pressure investigations, also capture `/v3/background-heal/status` from every distributed endpoint and compare `healOperations.queueLength`, `activeTasks`, `queuedBySource`, `activeBySource`, `queuedByPriority`, and `activeByPriority` (see [Reading Heal Operations](scanner-runtime-controls.md#reading-heal-operations)).

### `scanner-summary.csv` columns

In distributed runs the heal columns are aggregated from the background-heal snapshots captured across `--metrics-endpoints`.

| Column | Meaning |
|---|---|
| `heal_queue_length` | Total queued heal requests at the same timestamp as the scanner status sample. |
| `heal_active_tasks` | Total running heal tasks. |
| `heal_scanner_queued` | Scanner-submitted heal or bitrot work waiting in the queue. |
| `heal_admin_queued` | Manual/admin heal work waiting in the queue. |
| `heal_auto_heal_queued` | Auto-heal work waiting in the queue, typically from disk/set recovery paths. |
| `current_cycle_usage_saves` | Usage saves during the current cycle. |
| `last_cycle_usage_saves` | Usage saves from the last finished or partial cycle. |
| `usage_dirty_pending_buckets` | Dirty buckets still waiting for scanner refresh. |
| `usage_last_cycle_dirty_buckets` | Dirty buckets selected by the last cycle. |
| `usage_last_cycle_cleared_dirty_buckets` | Dirty bucket marks cleared by the last cycle. |
| `usage_last_save_result` | Last `DataUsageInfo` save result. |
| `usage_last_save_unix_secs` | Last `DataUsageInfo` save timestamp. |
| `life_time_scan_cycle` | Total scanner cycles observed by the node. |
| `life_time_scan_bucket_drive` | Total bucket-drive scans completed by the node. |
| `life_time_scan_object` | Total object scan operations observed by the node. |
| `life_time_save_usage` | Total usage save operations observed by the node. |

## Interpreting Results

A useful tuning result has all of these properties:

- average or p95 scanner-related CPU and disk pressure decreases;
- `current_cycle_objects_scanned` or `current_cycle_directories_scanned` continues to advance;
- `source_work.missed` does not grow unexpectedly for lifecycle, replication, heal, or bitrot;
- `last_cycle_result` is either `success` or a partial result with a clear budget reason and checkpoint;
- data usage freshness remains acceptable for the tested deployment.

Treat these as failure signals:

| Signal | Reading |
|---|---|
| CPU drops only because the scanner stops making progress | Not a tuning win. |
| `primary_pressure` stays at `queued_scans` while queues grow | Concurrency, not pacing, is the constraint. |
| `last_cycle_partial_reason` repeats forever with no checkpoint movement | Budget too small or checkpoint not advancing. |
| Lifecycle expiry `queue_missed`, `scanner_missed`, `current_queued`, or `current_active` grows during a run meant to reduce expiry backlog | Downstream expiry pressure. |
| Lifecycle transition `scanner_missed`, `queue_full`, `compensation_pending`, or `failed` grows during a run meant to reduce backlog | Downstream transition pressure. |
| Bucket metrics show zero usage after post-start uploads while dirty usage remains pending and `life_time_save_usage` does not advance | Usage freshness regression. |
| `bucket_replication` missed work with `scanner_role=repair_admission` grows while replication worker queues or target failures also grow | Downstream replication pressure, not only scanner pacing. |
| `site_replication` `active_resync` grows and is read as scanner-owned repair execution | Misreading: `scanner_role=boundary_signal` and `execution_owner=site_replication_runtime` mean active resync remains owned by the site replication runtime. |
| Heal or bitrot work moves from `queued` to `missed` after a scanner pacing change | Heal admission regression. |

## PR Evidence Checklist

For scanner behavior PRs, include when available:

- RustFS commit SHA and branch.
- Deployment shape: node count, disk count, disk type, CPU count, memory, object count.
- Workload command or script and benchmark artifact path.
- Scanner and heal config before and after tuning.
- Observation window and sample interval.
- Scanner status snapshots or time series.
- Host CPU and disk telemetry.
- Usage freshness fields from `scanner-summary.csv` when validating bucket metrics timing.
- A short conclusion that separates pressure reduction from scanner progress.
- `scanner-validation-report.md` from the harness when using the scripted collection path.
