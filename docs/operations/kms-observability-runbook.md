# KMS observability runbook

This runbook covers the KMS metrics, the Grafana dashboard that visualizes them, and the response procedure for each Prometheus alert shipped in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`. It is the `runbook_url` target for those alerts. For what each KMS backend protects and how Vault authentication behaves, see the [KMS backend security properties](kms-backend-security.md) and the [Vault KMS authentication runbook](vault-kms-authentication.md).

## Metric reference

Across every family below, label values come from bounded enums or fixed call-site tokens — key identifiers, key material, ciphertext, paths, and tokens never appear in metric labels, and any change that would add such a label is a regression.

### Backend operation metrics

All six are emitted at the single operation-policy choke point (`crates/kms/src/policy.rs`) that every instrumented KMS backend call flows through.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_backend_operations_total` | counter | `backend`, `operation`, `op_class`, `outcome` | Operations executed under the operation policy, counted once per terminal outcome |
| `rustfs_kms_backend_attempt_failures_total` | counter | `backend`, `operation`, `error_class` | Individual failed attempts, including attempts the retry policy later absorbed |
| `rustfs_kms_backend_operation_duration_seconds` | histogram | `backend`, `operation`, `outcome` | Wall-clock duration of a whole operation, including retries and backoff sleeps |
| `rustfs_kms_backend_operation_attempts` | histogram | `backend`, `operation`, `outcome` | Number of attempts one operation used before completing |
| `rustfs_kms_backend_in_flight` | gauge | `backend`, `scope` | External backend attempts currently in flight after admission |
| `rustfs_kms_backend_circuit_open` | gauge | `backend`, `scope` | Open or half-open circuits; `0` means closed |

Label values:

- `backend`: the backend that served the call — `vault-kv2`, `vault-transit`, `aws`, and `vault-restore` for the calls a restore makes against a Vault bundle's trust root. Operation names are shared across backends (every one of them has a `decrypt`), so without this label a Vault Transit latency regression and an AWS one land in the same series. Vault credential logins and renewals report their backend's own name and are told apart by the operation, not by a separate `backend` value; the `scope` label that distinguishes them appears only on the two gauges. Local and Static serve from process memory and never enter the operation policy, so they emit no `backend` series at all.
- `outcome`: `success`, `fatal` (a non-retryable failure ended the operation on first observation), `budget_exhausted` (the attempt budget ran out on retryable failures), `deadline_exceeded` (the operation deadline ran out before another attempt could complete), `backpressure_timeout` (the deadline elapsed before capacity admission completed), `backpressure_rejected` (active capacity and the bounded queue were full or unavailable), `circuit_open` (a retryable failure opened the breaker or an open breaker rejected the operation), `cancelled` (shutdown or caller cancellation).
- `op_class`: `read_idempotent` (safe to retry), `mutating_non_idempotent` (never replayed — a retryable failure terminates after a single attempt because the server may have processed the request), `auth` (login and token renewal).
- `error_class`: `retryable_conn` (connection-level failure: dial, TLS, broken connection), `retryable_status` (retryable backend status, e.g. Vault 5xx or a sealed Vault's 503), `attempt_timeout` (the per-attempt timeout cut the attempt off; retried like a connection failure because the server may still have processed the request), `fatal` (non-retryable: authentication, permissions, malformed request, missing key or version).
- `operation`: static per-call-site names, e.g. `vault_kv2_read_key_version`, `vault_kv2_cas_write_key`, `vault_transit_encrypt`, `vault_transit_decrypt`, `vault_login`, `vault_token_renew`.

Admission sharing follows two different boundaries. Total active backend capacity is shared by backend identity and capped at 64; ordinary operations are limited to 63 so login and renewal always retain one reserved slot without exceeding the total cap. Each backend configuration generation owns fresh bounded queues and circuit breakers for its policy scopes, so a failed reconfiguration candidate cannot inherit or mutate the running generation's admission state.

Instrumentation boundary: the Local and Static backends do not flow through the choke point and emit no operation metrics; bringing them under the same instrumentation is tracked separately (rustfs/backlog#1569). Absence of these six series on a cluster using those backends is expected, not an outage. The families below sit above the backend layer and are emitted regardless.

### Key metadata cache metrics

Emitted by the manager-level key metadata cache (`crates/kms/src/cache.rs`), which every backend shares. Publication is gated by the cache's `enable_metrics` setting, which defaults to on and which no configure-request field sets today, so in practice these are always published. The counters behind the admin status API are maintained either way, so the switch could never blind `kms service-status`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_metadata_cache_lookups_total` | counter | `result` | Key metadata lookups, by `hit` or `miss` |
| `rustfs_kms_metadata_cache_evictions_total` | counter | `cause` | Entries dropped from the cache, by removal cause |
| `rustfs_kms_metadata_cache_entries` | gauge | — | Entries the cache currently holds |

`cause` is `expired` (TTL), `size` (capacity), `explicit` (invalidated by a key lifecycle operation), or `replaced` (overwritten by a newer value). Only `expired` and `size` are true evictions — a sustained `explicit`/`replaced` rate is lifecycle traffic, not cache pressure.

The entry gauge is republished from every write path and from lookups that miss, because TTL expiry drops entries without any write taking place; a cache that goes completely idle can therefore hold a stale value until the next lookup. Note also that this cache only serves key metadata reads such as `describe_key` — encrypt, decrypt and data key generation never consult it, so a low hit ratio is not a data-path problem.

### Key lifecycle metrics

Published by the background deletion worker (`crates/kms/src/deletion_worker.rs`) at the end of each sweep, derived from the pages the sweep already walks, so observing the lifecycle costs no extra backend call. The worker only runs on backends whose capabilities include `schedule_deletion`, so a deployment on a backend without it emits none of these.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_pending_deletion_keys` | gauge | — | Keys scheduled for deletion whose deadline has not passed |
| `rustfs_kms_deletion_tombstone_keys` | gauge | — | Keys left tombstoned by an interrupted removal, still awaiting the sweep |
| `rustfs_kms_oldest_key_rotation_age_seconds` | gauge | — | Seconds since the least recently rotated usable key was rotated, counting from creation for keys with no recorded rotation; `0` when there are none |
| `rustfs_kms_max_key_wrap_operations` | gauge | — | Largest reserved wrap-operation count across usable keys; published only by backends that count wraps (Vault KV2 today) |
| `rustfs_kms_deletion_sweep_keys_total` | counter | `outcome` | Keys the sweep acted on, by outcome: `removed`, `blocked`, `skipped`, `failed`, `unreadable` |

`outcome` is `removed`, `blocked` (live configuration — the default key, or a reference reported by the injected checker — still points at the key, so the sweep refuses to remove it), `skipped` (pending but not yet due, or the state changed between inspection and removal), `failed` (the removal attempt failed and is retried next sweep), or `unreadable` (the backend listed a key record this build cannot describe — a record written by a newer build, or damaged material). Every series is emitted at zero from the first sweep on, so a `rate()` over it is defined immediately.

A non-zero `unreadable` rate does not stop the sweep — the expired keys it *can* read are still destroyed — but it does suppress the lifecycle gauges for that round, because a census taken over a partially readable key set would quietly undercount. Sustained `unreadable` therefore shows up as gauges that stop advancing; investigate the named key ids from the sweep's log line before trusting a rotation-age or pending-deletion reading again.

Total damage looks different, and it is worth knowing which you are seeing. When *no* key in a complete listing is readable, the backend fails the listing outright rather than returning an empty page (see the key listing contract in the admin contract page), so the sweep never gets a page to count: it reports `outcome="failed"` with the listing error in its `warn!` line and names no key ids. So `failed` climbing while `unreadable` stays at zero and the gauges freeze means the whole key set is unreadable on this node — a mixed-version node, or a credential that cannot open any record — not that individual removals are failing.

The gauges are republished only by a sweep that saw the whole key set; a sweep that could not finish listing leaves the previous, complete values standing rather than understating them. Keys already on their way out are excluded from the rotation-age and wrap gauges, so neither stays pinned high by a key that will never be rotated — or wrap — again.

`rustfs_kms_max_key_wrap_operations` exists because AES-256-GCM caps one key at 2^32 encryptions under random nonces (NIST SP 800-38D), and the KV2 backend wraps every DEK locally with the key's current material — so wraps track encrypted-object writes and the bound is real. The value is a reservation-based approximation that by design *overestimates*: nodes reserve wrap budget from the key record in blocks of one million and count individual wraps in memory only, so a crash discards unused budget, never a counted wrap. Alert on it approaching 2^32 and rotate the key — rotation installs fresh material and resets the counter. Two ways it can understate, both bounded and logged: a node whose reservation writes keep failing continues wrapping under a warn (`Vault KMS wrap budget reservation failed`), and an old build rewriting the key record during a mixed-version window drops the field (see the [mixed-version notes](kms-backend-security.md#mixed-version-clusters-during-a-rolling-upgrade)). Backends that do not wrap locally with rotatable material publish nothing here: Transit and AWS wrap inside the KMS, and Local/Static cannot rotate, so a counter would be an alarm with no remediation.

The rotation age comes from whatever the backend reports as the last rotation, and backends only report a rotation they recorded themselves. Today only the Vault KV2 backend persists that timestamp — it is stamped in the same check-and-set write that commits the rotation (`crates/kms/src/backends/vault.rs`), so it exists if and only if the rotation did. Vault Transit and AWS KMS record no rotation timestamp at all: their key listings always report the rotation time as absent, so on those backends every key ages from creation permanently, the gauge measures key age rather than rotation age, and rotating does not reset it. A KV2 key rotated before the timestamp existed likewise ages from creation until its next rotation stamps the record. In every case the gauge overstates rather than invents — it can report an already-rotated key as overdue, never a stale key as fresh — so an alert on it fires early rather than late. Backends that cannot rotate at all (Local, Static) age every key from creation by construction.

### Vault credential metrics

Published by the Vault credential provider (`crates/kms/src/backends/vault_credentials.rs`), so they exist only on Vault-backed backends. Both are label-less: there is exactly one credential generation to describe, and the Vault address, mount, auth path and token are all off limits as label values.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_vault_token_ttl_seconds` | gauge | — | Seconds left before the active Vault token expires; `0` once it has |
| `rustfs_kms_vault_credentials_fail_closed` | gauge | — | `1` while the provider refuses to hand out its token because it is inside the fail-closed safety window, `0` otherwise |

The renewal loop republishes both on a 10-second cadence while it waits, generating no extra Vault traffic, so a scrape landing between refresh cycles never reads a TTL frozen at the last refresh. `rustfs_kms_vault_credentials_fail_closed` at `1` is the metric form of the fail-closed window described in the [Vault KMS authentication runbook](vault-kms-authentication.md): while it is set, Vault-backed operations fail rather than run on a credential that may already be invalid.

### Synthetic probe metrics

Published by the background probe worker (`crates/kms/src/probe.rs`), which generates a data key under a reserved probe key, decrypts it, and compares the material. It runs every `RUSTFS_KMS_PROBE_INTERVAL_SECS` seconds (default 60, raised to a floor of 5, `0` disables the probe entirely), and the status it publishes is what KMS readiness reads.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_probe_rounds_total` | counter | `result` | Probe rounds completed, by `success`, `failure` or `unsupported` |
| `rustfs_kms_probe_failures_total` | counter | `failure_kind` | Failed rounds, by the round-trip stage that failed |
| `rustfs_kms_probe_duration_seconds` | histogram | `result` | Wall-clock duration of one probe round |
| `rustfs_kms_probe_last_success_timestamp_seconds` | gauge | — | Unix timestamp of the most recent successful round |
| `rustfs_kms_probe_consecutive_failures` | gauge | — | Rounds that have failed since the last success |

`failure_kind` is `key_provisioning` (the probe key could not be described or created), `generate`, `decrypt`, or `mismatch` — the last means both calls answered but the material did not survive the round trip, which is as serious as an outage and is reported as loudly.

`unsupported` means the backend cannot host the probe key. It is deliberately counted as its own result and never as a failure, and the worker stops after recording it, so failure-counter alerts stay silent on such deployments; the AWS KMS backend is the case in practice, because it refuses a caller-named create. Note also that `rustfs_kms_probe_last_success_timestamp_seconds` only ever moves forward on a success, so while the probe fails its age keeps growing — alert on that age, not on the presence of a failure counter.

Export path: the `metrics` facade feeds the OTel recorder in `crates/obs`, which exports over OTLP to the collector scraped by Prometheus. Histograms therefore appear in Prometheus as `_bucket`/`_sum`/`_count` series. None of these metrics carry the RustFS `server` label used by the node observability dashboard — distinguish nodes through your scrape topology (`job`/`instance` or promoted OTel resource attributes such as `service_instance_id`).

## Dashboard

Import `deploy/observability/grafana/rustfs-kms-observability.json` into Grafana and select a Prometheus data source that scrapes RustFS metrics. The dashboard has two variables: `datasource` (Prometheus data source) and `operation` (multi-select over the `operation` label). In the docker-compose observability stack (`.docker/observability/`), dashboards are provisioned from a directory (`grafana/provisioning/dashboards/dashboard.yml` points at `/etc/grafana/dashboards`), so no per-file registration is needed there.

The shipped dashboard covers the backend operation metrics only. Its "Planned Panels (TODO)" text panel still describes the cache, lifecycle, Vault credential and probe families as not landed — that panel is stale: the emitting code is merged and the metric names, types and label values are in [Metric reference](#metric-reference) above. Until real panels replace it, query those families ad hoc; nothing in the shipped dashboard or alert rules reads them. See [Coverage gaps](#coverage-gaps).

## Alert rules

The rules live in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`. The docker-compose Prometheus loads `/etc/prometheus/rules/*.yml`, so the `.yml` extension is load-bearing. Validate edits with `promtool check rules rustfs-kms-alerts.yml`.

Every threshold in that file is a conservative default chosen without a production baseline; see [Threshold calibration](#threshold-calibration) before treating a firing alert as an SLO breach or a quiet one as health.

## Alert response procedures

### KmsBackendFatalErrors

Meaning: attempts are failing with `error_class="fatal"` — failures the policy never retries. Each one is a KMS backend call that failed permanently (authentication, permissions, malformed request, or a missing key/version), so callers are seeing errors right now. This is the highest-signal KMS alert: fatal failures do not appear as background noise in a healthy system.

Investigation:

1. Break the rate down by operation: `sum by (operation) (rate(rustfs_kms_backend_attempt_failures_total{error_class="fatal"}[5m]))`.
2. If the failing operations are `vault_login` or `vault_token_renew` (`op_class="auth"`), the Vault credentials are invalid or expired. Follow the [Vault KMS authentication runbook](vault-kms-authentication.md) — note that credential refresh is fail-closed, so a broken credential eventually takes down all Vault-backed operations, not just auth. Look for the `Vault token renewal failed; falling back to a fresh login` and `Vault credential refresh failed; retrying until the credentials recover` warnings in the RustFS logs; `rustfs_kms_vault_credentials_fail_closed` at `1`, or `rustfs_kms_vault_token_ttl_seconds` at or near `0`, confirms that state without reading logs.
3. If the failing operations are `vault_kv2_*` or `vault_transit_*`, check for Vault permission denials: compare the token's policy against the minimal policy in [KMS backend security properties](kms-backend-security.md) (a policy that drifted or was re-scoped produces 403s that classify as fatal), and check the Vault audit log for the corresponding denied requests.
4. A fatal `KeyVersionNotFound` on decrypt-path operations means a DEK envelope references a key version whose record is missing. Decryption deliberately fails closed with no fallback — see the rotation retention preconditions in [KMS backend security properties](kms-backend-security.md) and verify nobody destroyed version records under the key subtree.
5. Confirm blast radius with the outcome view: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome="fatal"}[5m]))`.

Related signals: the "Attempt Failure Rate by Error Class" and "Backend Operation Rate by Outcome" dashboard panels; Vault server audit and server logs; S3-level 5xx on encrypted buckets.

### KmsBackendHighErrorRate

Meaning: more than 5% of KMS operations are terminating without success (`fatal`, `budget_exhausted`, `deadline_exceeded`, `backpressure_timeout`, `backpressure_rejected`, or `circuit_open`; `cancelled` is excluded because shutdown windows legitimately produce it). A traffic guard suppresses the alert below ~0.02 ops/s so a single failure on a near-idle cluster does not page.

Investigation:

1. Break the failures down by outcome: `sum by (outcome) (rate(rustfs_kms_backend_operations_total{outcome!~"success|cancelled"}[5m]))`.
2. If `fatal` dominates, follow [KmsBackendFatalErrors](#kmsbackendfatalerrors).
3. If `budget_exhausted` or `deadline_exceeded` dominates, follow [KmsBackendRetryBudgetExhausted](#kmsbackendretrybudgetexhausted) — the backend is unavailable or too slow for longer than the retry policy can bridge.
4. If `backpressure_timeout` or `backpressure_rejected` dominates, compare `rustfs_kms_backend_in_flight` by `backend` and `scope`; total active capacity is shared by backend identity, one slot is reserved for credential refresh, and each configuration generation has fresh scope-local bounded queues.
5. If `circuit_open` dominates, follow [KmsBackendCircuitOpen](#kmsbackendcircuitopen).
6. Correlate with client impact: encrypted-object PUT/GET failures and S3 error rates on buckets with encryption configured.

Related signals: the "Non-Success Outcome Ratio" dashboard panel; the KMS-related warnings listed under the other alerts in this runbook.

### KmsBackendP99LatencyHigh

Meaning: the p99 wall-clock duration of KMS operations is sustained above 2s. The histogram includes retries and backoff sleeps, so a high p99 with a healthy p50 usually means a slow retry tail (a subset of calls failing and being retried), not a uniform slowdown.

Investigation:

1. Compare p50 and p99 on the "Operation Duration p50 / p99" panel. Flat p50 with elevated p99 points at retries; both elevated points at the backend or the network path being uniformly slow.
2. Split by backend and operation with `histogram_quantile(0.99, sum by (le, backend, operation) (rate(rustfs_kms_backend_operation_duration_seconds_bucket[5m])))` to see whether one backend call or all of them regressed.
3. Check the attempts histogram: an average meaningfully above 1 confirms the latency is retry-driven; follow [KmsBackendAttemptFailureSpike](#kmsbackendattemptfailurespike) for the failure classes.
4. If latency is not retry-driven, check the network path to Vault (TLS handshakes, DNS, proxies) and Vault's own telemetry (storage backend latency, load).
5. Remember that this latency sits inside S3 request latency for encrypted objects: sustained p99 near the operation deadline will start converting into `deadline_exceeded` outcomes.

Related signals: the "Operation Duration p99 by Operation" and "Operation Attempts Distribution" panels; `KMS backend attempt failed with a retryable error; backing off before retry` warnings (fields: `operation`, `attempt`, `error_class`, `backoff`).

### KmsBackendAttemptFailureSpike

Meaning: individual attempts are failing at a sustained rate across all error classes. The retry policy may still be absorbing these — operations can keep succeeding while this alert fires — but the system is burning retry budget and running degraded, and a small further degradation will surface to callers.

Investigation:

1. Break the rate down by class: `sum by (error_class) (rate(rustfs_kms_backend_attempt_failures_total[5m]))`.
2. `retryable_conn`: network-level failures — check connectivity, TLS, DNS, and whether Vault is down or restarting.
3. `retryable_status`: the backend answered with a retryable error — check Vault health and seal status (a sealed Vault returns 503, which lands here), and Vault-side rate limiting.
4. `attempt_timeout`: attempts are being cut off by the per-attempt timeout — either the backend is slow (correlate with [KmsBackendP99LatencyHigh](#kmsbackendp99latencyhigh)) or the configured attempt timeout is too tight for the deployment's network path.
5. `fatal`: follow [KmsBackendFatalErrors](#kmsbackendfatalerrors).
6. Grep RustFS logs for `KMS backend attempt failed with a retryable error; backing off before retry` — the structured fields (`operation`, `attempt`, `error_class`, `backoff`) identify which call sites are cycling.

Related signals: the "Attempt Failure Rate by Error Class" panel; the attempts histogram average rising above 1.

### KmsBackendRetryBudgetExhausted

Meaning: operations are terminating as `budget_exhausted` or `deadline_exceeded` — every individual failure was retryable, but the backend stayed unhealthy for longer than the retry policy could bridge, so callers received hard failures.

Investigation:

1. Identify the failing operations: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome=~"budget_exhausted|deadline_exceeded"}[5m]))`.
2. Establish how long the underlying failure has persisted from the attempt-failure rate history; follow [KmsBackendAttemptFailureSpike](#kmsbackendattemptfailurespike) for the class-specific diagnosis.
3. Note the by-design case: `mutating_non_idempotent` operations (e.g. `vault_kv2_cas_write_key`, `vault_transit_create_key`) are never replayed, so a single retryable failure terminates them as `budget_exhausted` after one attempt. A spike confined to mutating operations means write-path failures, not an exhausted retry loop.
4. `deadline_exceeded` clustering with duration p99 near the operation deadline means the budget is being spent on slow attempts rather than fast failures — treat as a latency problem first.
5. Confirm client impact and, if the backend outage is confirmed external (Vault down), coordinate recovery there; RustFS will resume without intervention once the backend recovers.

Related signals: the "Backend Operation Rate by Outcome" panel; retry-backoff warnings in RustFS logs; Vault availability monitoring.

### KmsBackendCircuitOpen

Meaning: `rustfs_kms_backend_circuit_open` has remained above `0` for a `backend` and `scope` for one minute. This direct gauge alert does not depend on operation traffic: it remains visible when the circuit is open and rejecting calls, and while the single half-open recovery probe is running.

Investigation:

1. Identify the affected scope with `rustfs_kms_backend_circuit_open > 0`.
2. Break recent rejections down by operation: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome="circuit_open"}[5m]))`.
3. Check `sum by (error_class) (rate(rustfs_kms_backend_attempt_failures_total{error_class=~"retryable_conn|retryable_status|attempt_timeout"}[5m]))` to distinguish transport failures, retryable backend responses such as a sealed Vault, and attempt timeouts. An attempt timeout counts toward the breaker as a retryable connection failure.
4. After the open interval, the next eligible operation is the only half-open probe. A success or non-retryable failure closes the circuit; a retryable failure reopens it. A non-retryable probe still fails as `fatal`, so follow [KmsBackendFatalErrors](#kmsbackendfatalerrors) even after the circuit gauge clears. Do not restart RustFS just to clear the state.
5. Remember the sharing boundary: each configuration generation has fresh scope-local breaker and queue state, while total active capacity is shared by backend identity with one slot reserved for credential refresh. Check other scopes for capacity pressure even when their circuits remain closed.

Related signals: `circuit_open`, `backpressure_timeout`, and `backpressure_rejected` on the "Backend Operation Rate by Outcome" panel; `rustfs_kms_backend_in_flight`; Vault availability and seal status.

### KmsKeyRotationOverdue

Meaning: `rustfs_kms_oldest_key_rotation_age_seconds` — seconds since the least recently rotated usable key was rotated, counting from creation for keys with no recorded rotation — has been above 400 days for an hour. This is a compliance and hygiene signal, not an outage: encryption and decryption continue unchanged, and nothing in RustFS acts on the verdict. But the longer master key material stays in service the larger the blast radius of its compromise, and on backends where RustFS wraps DEKs locally (Local, Static, Vault KV2) the AES-GCM random-nonce invocation ceiling (NIST SP 800-38D: at most 2^32 wraps under one key) is consumed by every encrypted object write and only ever resets through rotation.

Investigation:

1. Find which keys are due. The gauge deliberately names no key — a per-key label would carry key identifiers into the metric stream — so read the per-key verdict from the listing: `GET /rustfs/admin/v3/kms/keys` carries `rotation_due` and `rotation_due_reason` (`age`, `never_rotated`, `wraps`, or `unsupported`) per key, computed against `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` and `RUSTFS_KMS_ROTATION_MAX_WRAPS`. A `wraps` reason means the key's material has wrapped more data keys than the configured budget — the AES-GCM random-nonce ceiling rather than an age policy, so it is not satisfied by relaxing the age threshold. The verdict appears only on the listing, not on single-key describe. If `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` is unset, set it to your policy's rotation period so the per-key verdict and this alert agree on what "overdue" means.
2. If the reason is `unsupported`, the backend cannot rotate at all (Local, Static). There is no key-level response; the decision is a backend migration, and the wrap ceiling above is the reason it cannot be deferred forever. See the [rotation drivers and scheduling matrix](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend).
3. On a backend that can rotate, act per the driver matrix: on **Vault KV2**, check why your external rotation scheduler did not run (or set one up — RustFS deliberately ships none) and satisfy the [pre-rotation checklist](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend) before rotating, above all the [upgrade-ordering hard constraint](kms-backend-security.md#upgrade-before-first-rotation-hard-constraint) — never respond to this alert by rotating in the middle of a rolling upgrade. On **Vault Transit**, check `auto_rotate_period` on the key in Vault. On **AWS KMS**, check the key's automatic rotation status in AWS — and do not schedule rotation through the RustFS endpoint, which maps to quota-limited `RotateKeyOnDemand`.
4. Know the gauge's blind spot on Transit and AWS before chasing a rotation that already happened: only KV2 persists a rotation timestamp, so Transit and AWS keys age from creation permanently and this alert will not clear after a rotation there. Confirm the real cadence at the owning system — the Transit key's version history in Vault, or the key's rotation status in AWS — and treat a confirmed-healthy cadence as a known overstatement of this gauge rather than an overdue key.
5. If a KV2 key was genuinely rotated and the gauge stays high, remember the gauge is republished only by a sweep that saw the whole key set: check `rustfs_kms_deletion_sweep_keys_total` for `unreadable` or `failed` outcomes freezing the lifecycle gauges (see [Key lifecycle metrics](#key-lifecycle-metrics)), and that the deletion worker is running at all — it only runs on backends with the `schedule_deletion` capability, which is also why the Static backend never emits this series.

Related signals: `rotation_due` / `rotation_due_reason` on the key listing; `rustfs_kms_deletion_sweep_keys_total{outcome=~"unreadable|failed"}` (a frozen gauge is stale, not healthy); the [rotation drivers and scheduling matrix](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend) and pre-rotation checklist in the backend security properties document.

## Threshold calibration

Every numeric traffic or latency threshold in `rustfs-kms-alerts.yml` (5% error ratio, 2s p99, 0.5/s attempt failures, 0.05/s budget exhaustion) is a conservative default chosen without a production baseline, biased toward not paging on healthy-but-busy systems. Before relying on these alerts for paging: run the workload in staging for at least a week, record the steady-state values of the expressions above, then tighten thresholds to sit clearly above observed peaks. `KmsBackendCircuitOpen` is different: its gauge is direct state, and the one-minute hold only suppresses a circuit that recovers immediately. `KmsKeyRotationOverdue` is different in the other direction: its 400-day threshold is a policy default (sitting above a common one-year rotation period), not a traffic default — calibrate it against the rotation period your compliance policy requires and against `RUSTFS_KMS_ROTATION_MAX_AGE_SECS`, not against a staging baseline. Once a stable baseline exists, consider converting `KmsBackendAttemptFailureSpike` to a baseline-relative form (`offset 1d` ratio, see `.docker/observability/prometheus-rules/rustfs-get-optimization-alerts.yaml` for the pattern). Formal SLO targets for KMS operations are deliberately out of scope until that baseline exists (rustfs/backlog#1584).

## Coverage gaps

The four metric families designed under rustfs/backlog#1584 — key-cache effectiveness, key lifecycle, Vault credentials, synthetic probe — have all landed and are documented in [Metric reference](#metric-reference). What is still missing:

- **No dashboard panels for those four families, and an alert rule for only one of them.** The key lifecycle family has one rule — [`KmsKeyRotationOverdue`](#kmskeyrotationoverdue) on the rotation-age gauge — while the cache, Vault credential, and probe families are emitted but neither visualized nor alerted on, so they surface only in ad-hoc queries. Building against them is safe now: the names and label values above are what the code emits.
- **The Local and Static backends emit no operation metrics**, because they do not flow through the operation-policy choke point; bringing them under the same instrumentation is tracked separately (rustfs/backlog#1569). Their cache metrics are emitted normally.
- **No formal SLO targets**, deliberately, until a production baseline exists — see [Threshold calibration](#threshold-calibration).

When a panel or alert rule for one of the landed families is added, replace the corresponding TODO bullet in the dashboard's "Planned Panels" text panel and update this section.

## Related documents

- [KMS backend security properties](kms-backend-security.md) — backend trust boundaries, minimal Vault policies, rotation retention preconditions.
- [Vault KMS authentication runbook](vault-kms-authentication.md) — credential sources, refresh behavior, and the fail-closed window.
- `deploy/observability/README.md` — dashboard import notes for all RustFS dashboards.
