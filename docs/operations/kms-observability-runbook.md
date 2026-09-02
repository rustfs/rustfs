# KMS observability runbook

**Use this when:** a `Kms*` Prometheus alert fires (this file is their `runbook_url` target), you are building dashboards or alerts on KMS metrics, or KMS reports not-configured after a restart.
**Source of truth:** `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml` (alert names, thresholds); `crates/kms/src/policy.rs` (backend operation metrics), `crates/kms/src/cache.rs`, `crates/kms/src/deletion_worker.rs`, `crates/kms/src/backends/vault_credentials.rs`, `crates/kms/src/probe.rs`; dashboard `deploy/observability/grafana/rustfs-kms-observability.json`.

For what each backend protects and how Vault authentication behaves, see [KMS backend security properties](kms-backend-security.md) and the [Vault KMS authentication runbook](vault-kms-authentication.md).

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

| Label | Values |
| --- | --- |
| `backend` | `vault-kv2`, `vault-transit`, `aws`, and `vault-restore` (calls a restore makes against a Vault bundle's trust root). Operation names are shared across backends (each has a `decrypt`), so this label separates a Transit latency regression from an AWS one. Vault credential logins and renewals report their backend's name and are told apart by `operation`; the `scope` label appears only on the two gauges. Local and Static serve from process memory, never enter the operation policy, and emit no `backend` series |
| `outcome` | `success`; `fatal` (non-retryable failure on first observation); `budget_exhausted` (attempt budget ran out on retryable failures); `deadline_exceeded` (operation deadline ran out before another attempt could complete); `backpressure_timeout` (deadline elapsed before capacity admission); `backpressure_rejected` (active capacity and the bounded queue were full or unavailable); `circuit_open` (a retryable failure opened the breaker, or an open breaker rejected the operation); `cancelled` (shutdown or caller cancellation) |
| `op_class` | `read_idempotent` (safe to retry); `mutating_non_idempotent` (never replayed — a retryable failure terminates after one attempt because the server may have processed the request); `auth` (login and token renewal) |
| `error_class` | `retryable_conn` (dial, TLS, broken connection); `retryable_status` (retryable backend status, e.g. Vault 5xx or a sealed Vault's 503); `attempt_timeout` (per-attempt timeout; retried like a connection failure); `fatal` (authentication, permissions, malformed request, missing key or version) |
| `operation` | Static per-call-site names, e.g. `vault_kv2_read_key_version`, `vault_kv2_cas_write_key`, `vault_transit_encrypt`, `vault_transit_decrypt`, `vault_login`, `vault_token_renew` |

Admission sharing follows two boundaries. Total active backend capacity is shared by backend identity and capped at `DEFAULT_MAX_CONCURRENT_OPERATIONS`; ordinary operations may use that minus `RESERVED_CREDENTIAL_OPERATIONS`, so login and renewal always retain a reserved slot. Each backend configuration generation owns fresh bounded queues and circuit breakers for its policy scopes, so a failed reconfiguration candidate cannot inherit or mutate the running generation's admission state.

Instrumentation boundary: the Local and Static backends do not flow through the choke point and emit no operation metrics. Absence of these six series on a cluster using those backends is expected, not an outage. The families below sit above the backend layer and are emitted regardless.

### Key metadata cache metrics

Emitted by the manager-level key metadata cache (`crates/kms/src/cache.rs`), which every backend shares. Publication is gated by the cache's `enable_metrics` setting, which defaults to on and which no configure-request field sets, so in practice these are always published. The counters behind the admin status API are maintained either way.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_metadata_cache_lookups_total` | counter | `result` | Key metadata lookups, by `hit` or `miss` |
| `rustfs_kms_metadata_cache_evictions_total` | counter | `cause` | Entries dropped from the cache, by removal cause |
| `rustfs_kms_metadata_cache_entries` | gauge | — | Entries the cache currently holds |

| `cause` | Meaning |
| --- | --- |
| `expired` | TTL — a true eviction |
| `size` | Capacity — a true eviction |
| `explicit` | Invalidated by a key lifecycle operation; a sustained rate is lifecycle traffic, not cache pressure |
| `replaced` | Overwritten by a newer value; same reading as `explicit` |

The entry gauge is republished from every write path and from lookups that miss, because TTL expiry drops entries without any write taking place; a cache that goes completely idle can hold a stale value until the next lookup. This cache only serves key metadata reads such as `describe_key` — encrypt, decrypt and data key generation never consult it, so a low hit ratio is not a data-path problem.

### Key lifecycle metrics

Published by the background deletion worker (`crates/kms/src/deletion_worker.rs`) at the end of each sweep, derived from the pages the sweep already walks. The worker only runs on backends whose capabilities include `schedule_deletion`, so a deployment on a backend without it emits none of these.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_pending_deletion_keys` | gauge | — | Keys scheduled for deletion whose deadline has not passed |
| `rustfs_kms_deletion_tombstone_keys` | gauge | — | Keys left tombstoned by an interrupted removal, still awaiting the sweep |
| `rustfs_kms_oldest_key_rotation_age_seconds` | gauge | — | Seconds since the least recently rotated usable key was rotated, counting from creation for keys with no recorded rotation; `0` when there are none |
| `rustfs_kms_max_key_wrap_operations` | gauge | — | Largest reserved wrap-operation count across usable keys; published only by backends that count wraps (Vault KV2) |
| `rustfs_kms_deletion_sweep_keys_total` | counter | `outcome` | Keys the sweep acted on, by outcome |

| `outcome` | Meaning |
| --- | --- |
| `removed` | Material destroyed |
| `blocked` | Live configuration (the default key, or a reference reported by the injected checker) still points at the key; the sweep refuses to remove it |
| `skipped` | Pending but not yet due, or the state changed between inspection and removal |
| `failed` | The removal attempt failed; retried next sweep. Also reported, with no key ids, when the listing itself failed |
| `unreadable` | The backend listed a key record this build cannot describe — written by a newer build, or damaged material |

Every series is emitted at zero from the first sweep on, so a `rate()` over it is defined immediately. A non-zero `unreadable` rate does not stop the sweep, but it suppresses the lifecycle gauges for that round, because a census over a partially readable key set would undercount; sustained `unreadable` therefore shows up as gauges that stop advancing — investigate the key ids named in the sweep's log line before trusting a rotation-age or pending-deletion reading again. When *no* key in a complete listing is readable, the backend fails the listing outright (see the [key listing contract](kms-admin-contract.md#key-listing-contract)), so the sweep reports `outcome="failed"` with the listing error and names no key ids: `failed` climbing while `unreadable` stays at zero and the gauges freeze means the whole key set is unreadable on this node — a mixed-version node, or a credential that cannot open any record. Gauges are republished only by a sweep that saw the whole key set; keys already on their way out are excluded from the rotation-age and wrap gauges.

`rustfs_kms_max_key_wrap_operations` tracks the AES-GCM wrap ceiling described under [Rotation drivers and scheduling, per backend](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend). The value is a reservation-based approximation that by design *overestimates*: nodes reserve wrap budget from the key record in blocks of one million and count individual wraps in memory only, so a crash discards unused budget, never a counted wrap. Alert on it approaching 2^32 and rotate the key. It can understate in two bounded, logged cases: a node whose reservation writes keep failing continues wrapping under the warn `Vault KMS wrap budget reservation failed`, and an old build rewriting the key record during a mixed-version window drops the field (see the [mixed-version notes](kms-backend-security.md#mixed-version-clusters-during-a-rolling-upgrade)). Transit and AWS wrap inside the KMS and Local/Static cannot rotate, so none of them publish this series.

The rotation age comes from whatever the backend reports as the last rotation, and only Vault KV2 persists that timestamp — stamped in the same check-and-set write that commits the rotation (`crates/kms/src/backends/vault.rs`). Vault Transit and AWS KMS record none, so on those backends every key ages from creation permanently, the gauge measures key age rather than rotation age, and rotating does not reset it. A KV2 key rotated before the timestamp existed likewise ages from creation until its next rotation. In every case the gauge overstates rather than invents, so an alert on it fires early rather than late.

### Vault credential metrics

Published by the Vault credential provider (`crates/kms/src/backends/vault_credentials.rs`), so they exist only on Vault-backed backends. Both are label-less: there is exactly one credential generation to describe, and the Vault address, mount, auth path and token are off limits as label values.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_vault_token_ttl_seconds` | gauge | — | Seconds left before the active Vault token expires; `0` once it has |
| `rustfs_kms_vault_credentials_fail_closed` | gauge | — | `1` while the provider refuses to hand out its token because it is inside the fail-closed safety window, `0` otherwise |

The renewal loop republishes both on a 10-second cadence while it waits, generating no extra Vault traffic. `rustfs_kms_vault_credentials_fail_closed` at `1` is the metric form of the fail-closed window described in the [Vault KMS authentication runbook](vault-kms-authentication.md): while it is set, Vault-backed operations fail rather than run on a credential that may already be invalid.

### Synthetic probe metrics

Published by the background probe worker (`crates/kms/src/probe.rs`), which generates a data key under a reserved probe key, decrypts it, and compares the material. It runs every `RUSTFS_KMS_PROBE_INTERVAL_SECS` seconds (default `DEFAULT_PROBE_INTERVAL`, raised to a floor of `MIN_PROBE_INTERVAL`, `0` disables the probe), and the status it publishes is what KMS readiness reads.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_probe_rounds_total` | counter | `result` | Probe rounds completed, by `success`, `failure` or `unsupported` |
| `rustfs_kms_probe_failures_total` | counter | `failure_kind` | Failed rounds, by the round-trip stage that failed |
| `rustfs_kms_probe_duration_seconds` | histogram | `result` | Wall-clock duration of one probe round |
| `rustfs_kms_probe_last_success_timestamp_seconds` | gauge | — | Unix timestamp of the most recent successful round |
| `rustfs_kms_probe_consecutive_failures` | gauge | — | Rounds that have failed since the last success |

| `failure_kind` | Meaning |
| --- | --- |
| `key_provisioning` | The probe key could not be described or created |
| `generate` | Data key generation failed |
| `decrypt` | Decryption failed |
| `mismatch` | Both calls answered but the material did not survive the round trip — as serious as an outage |

`unsupported` means the backend cannot host the probe key (the AWS KMS backend, which refuses a caller-named create). It is counted as its own result, never as a failure, and the worker stops after recording it, so failure-counter alerts stay silent on such deployments. `rustfs_kms_probe_last_success_timestamp_seconds` only moves forward on a success, so while the probe fails its age keeps growing — alert on that age, not on the presence of a failure counter.

Export path: the `metrics` facade feeds the OTel recorder in `crates/obs`, which exports over OTLP to the collector scraped by Prometheus. Histograms therefore appear in Prometheus as `_bucket`/`_sum`/`_count` series. None of these metrics carry the RustFS `server` label used by the node observability dashboard — distinguish nodes through your scrape topology (`job`/`instance` or promoted OTel resource attributes such as `service_instance_id`).

## Dashboard

Import `deploy/observability/grafana/rustfs-kms-observability.json` into Grafana and select a Prometheus data source that scrapes RustFS metrics. The dashboard has two variables: `datasource` and `operation` (multi-select over the `operation` label). In the docker-compose observability stack (`.docker/observability/`), dashboards are provisioned from a directory (`grafana/provisioning/dashboards/dashboard.yml` points at `/etc/grafana/dashboards`), so no per-file registration is needed.

The shipped dashboard covers the backend operation metrics only. Its "Planned Panels (TODO)" text panel is stale: the cache, lifecycle, Vault credential and probe families are emitted and documented in [Metric reference](#metric-reference) above. Until panels replace it, query those families ad hoc. See [Coverage gaps](#coverage-gaps).

## Alert rules

The rules live in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`. The docker-compose Prometheus loads `/etc/prometheus/rules/*.yml`, so the `.yml` extension is load-bearing. Validate edits with `promtool check rules rustfs-kms-alerts.yml`.

Every threshold in that file is a conservative default chosen without a production baseline; see [Threshold calibration](#threshold-calibration) before treating a firing alert as an SLO breach or a quiet one as health.

## Alert response procedures

### KmsBackendFatalErrors

Meaning: attempts are failing with `error_class="fatal"` — failures the policy never retries (authentication, permissions, malformed request, or a missing key/version), so callers are seeing errors right now. This is the highest-signal KMS alert: fatal failures do not appear as background noise in a healthy system.

1. Break the rate down by operation: `sum by (operation) (rate(rustfs_kms_backend_attempt_failures_total{error_class="fatal"}[5m]))`.
2. If the failing operations are `vault_login` or `vault_token_renew` (`op_class="auth"`), the Vault credentials are invalid or expired. Follow the [Vault KMS authentication runbook](vault-kms-authentication.md) — credential refresh is fail-closed, so a broken credential eventually takes down all Vault-backed operations. Look for the `Vault token renewal failed; falling back to a fresh login` and `Vault credential refresh failed; retrying until the credentials recover` warnings; `rustfs_kms_vault_credentials_fail_closed` at `1`, or `rustfs_kms_vault_token_ttl_seconds` at or near `0`, confirms that state without reading logs.
3. If the failing operations are `vault_kv2_*` or `vault_transit_*`, check for Vault permission denials: compare the token's policy against the [minimal policy](kms-backend-security.md#minimal-vault-policy-for-the-kv2-backend) (a re-scoped policy produces 403s that classify as fatal), and check the Vault audit log for the denied requests.
4. A fatal `KeyVersionNotFound` on decrypt-path operations means a DEK envelope references a key version whose record is missing. Decryption deliberately fails closed with no fallback — see the [retention and destruction preconditions](kms-backend-security.md#retention-and-destruction-preconditions) and verify nobody destroyed version records under the key subtree.
5. Confirm blast radius with the outcome view: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome="fatal"}[5m]))`.

Related signals: the "Attempt Failure Rate by Error Class" and "Backend Operation Rate by Outcome" dashboard panels; Vault server audit and server logs; S3-level 5xx on encrypted buckets.

### KmsBackendHighErrorRate

Meaning: more than 5% of KMS operations are terminating without success (`fatal`, `budget_exhausted`, `deadline_exceeded`, `backpressure_timeout`, `backpressure_rejected`, or `circuit_open`; `cancelled` is excluded because shutdown windows legitimately produce it). A traffic guard suppresses the alert below ~0.02 ops/s so a single failure on a near-idle cluster does not page.

1. Break the failures down by outcome: `sum by (outcome) (rate(rustfs_kms_backend_operations_total{outcome!~"success|cancelled"}[5m]))`.
2. If `fatal` dominates, follow [KmsBackendFatalErrors](#kmsbackendfatalerrors).
3. If `budget_exhausted` or `deadline_exceeded` dominates, follow [KmsBackendRetryBudgetExhausted](#kmsbackendretrybudgetexhausted).
4. If `backpressure_timeout` or `backpressure_rejected` dominates, compare `rustfs_kms_backend_in_flight` by `backend` and `scope`; total active capacity is shared by backend identity with one slot reserved for credential refresh, and each configuration generation has fresh scope-local bounded queues.
5. If `circuit_open` dominates, follow [KmsBackendCircuitOpen](#kmsbackendcircuitopen).
6. Correlate with client impact: encrypted-object PUT/GET failures and S3 error rates on buckets with encryption configured.

Related signals: the "Non-Success Outcome Ratio" dashboard panel; the KMS-related warnings listed under the other alerts.

### KmsBackendP99LatencyHigh

Meaning: the p99 wall-clock duration of KMS operations is sustained above 2s. The histogram includes retries and backoff sleeps, so a high p99 with a healthy p50 usually means a slow retry tail, not a uniform slowdown.

1. Compare p50 and p99 on the "Operation Duration p50 / p99" panel. Flat p50 with elevated p99 points at retries; both elevated points at the backend or network path being uniformly slow.
2. Split by backend and operation: `histogram_quantile(0.99, sum by (le, backend, operation) (rate(rustfs_kms_backend_operation_duration_seconds_bucket[5m])))`.
3. Check the attempts histogram: an average meaningfully above 1 confirms retry-driven latency; follow [KmsBackendAttemptFailureSpike](#kmsbackendattemptfailurespike) for the failure classes.
4. If not retry-driven, check the network path to Vault (TLS handshakes, DNS, proxies) and Vault's own telemetry (storage backend latency, load).
5. This latency sits inside S3 request latency for encrypted objects: sustained p99 near the operation deadline starts converting into `deadline_exceeded` outcomes.

Related signals: the "Operation Duration p99 by Operation" and "Operation Attempts Distribution" panels; `KMS backend attempt failed with a retryable error; backing off before retry` warnings (fields: `operation`, `attempt`, `error_class`, `backoff`).

### KmsBackendAttemptFailureSpike

Meaning: individual attempts are failing at a sustained rate across all error classes. The retry policy may still be absorbing them — operations can keep succeeding while this fires — but the system is burning retry budget and a small further degradation will surface to callers.

1. Break the rate down by class: `sum by (error_class) (rate(rustfs_kms_backend_attempt_failures_total[5m]))`.
2. `retryable_conn`: network-level failures — check connectivity, TLS, DNS, and whether Vault is down or restarting.
3. `retryable_status`: the backend answered with a retryable error — check Vault health and seal status (a sealed Vault returns 503), and Vault-side rate limiting.
4. `attempt_timeout`: attempts are cut off by the per-attempt timeout — either the backend is slow (correlate with [KmsBackendP99LatencyHigh](#kmsbackendp99latencyhigh)) or the configured attempt timeout is too tight for the network path.
5. `fatal`: follow [KmsBackendFatalErrors](#kmsbackendfatalerrors).
6. Grep RustFS logs for `KMS backend attempt failed with a retryable error; backing off before retry` — the structured fields (`operation`, `attempt`, `error_class`, `backoff`) identify which call sites are cycling.

Related signals: the "Attempt Failure Rate by Error Class" panel; the attempts histogram average rising above 1.

### KmsBackendRetryBudgetExhausted

Meaning: operations are terminating as `budget_exhausted` or `deadline_exceeded` — every individual failure was retryable, but the backend stayed unhealthy for longer than the retry policy could bridge, so callers received hard failures.

1. Identify the failing operations: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome=~"budget_exhausted|deadline_exceeded"}[5m]))`.
2. Establish how long the underlying failure has persisted from the attempt-failure rate history; follow [KmsBackendAttemptFailureSpike](#kmsbackendattemptfailurespike) for the class-specific diagnosis.
3. By-design case: `mutating_non_idempotent` operations (e.g. `vault_kv2_cas_write_key`, `vault_transit_create_key`) are never replayed, so a single retryable failure terminates them as `budget_exhausted` after one attempt. A spike confined to mutating operations means write-path failures, not an exhausted retry loop.
4. `deadline_exceeded` clustering with duration p99 near the operation deadline means the budget is spent on slow attempts rather than fast failures — treat as a latency problem first.
5. Confirm client impact; if the backend outage is external (Vault down), coordinate recovery there — RustFS resumes without intervention once the backend recovers.

Related signals: the "Backend Operation Rate by Outcome" panel; retry-backoff warnings; Vault availability monitoring.

### KmsBackendCircuitOpen

Meaning: `rustfs_kms_backend_circuit_open` has remained above `0` for a `backend` and `scope` for one minute. This direct gauge alert does not depend on operation traffic: it stays visible while the circuit rejects calls and while the single half-open recovery probe runs.

1. Identify the affected scope with `rustfs_kms_backend_circuit_open > 0`.
2. Break recent rejections down by operation: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome="circuit_open"}[5m]))`.
3. Check `sum by (error_class) (rate(rustfs_kms_backend_attempt_failures_total{error_class=~"retryable_conn|retryable_status|attempt_timeout"}[5m]))` to distinguish transport failures, retryable backend responses such as a sealed Vault, and attempt timeouts. An attempt timeout counts toward the breaker as a retryable connection failure.
4. After the open interval, the next eligible operation is the only half-open probe. A success or non-retryable failure closes the circuit; a retryable failure reopens it. A non-retryable probe still fails as `fatal`, so follow [KmsBackendFatalErrors](#kmsbackendfatalerrors) even after the gauge clears. Do not restart RustFS just to clear the state.
5. Each configuration generation has fresh scope-local breaker and queue state, while total active capacity is shared by backend identity with one slot reserved for credential refresh. Check other scopes for capacity pressure even when their circuits remain closed.

Related signals: `circuit_open`, `backpressure_timeout`, and `backpressure_rejected` on the "Backend Operation Rate by Outcome" panel; `rustfs_kms_backend_in_flight`; Vault availability and seal status.

### KmsKeyRotationOverdue

Meaning: `rustfs_kms_oldest_key_rotation_age_seconds` — seconds since the least recently rotated usable key was rotated, counting from creation for keys with no recorded rotation — has been above 400 days for an hour. This is a compliance and hygiene signal, not an outage: encryption and decryption continue unchanged, and nothing in RustFS acts on the verdict. The reasons rotation matters (blast radius, and the AES-GCM wrap ceiling on backends where RustFS wraps DEKs locally) are stated once in [Rotation drivers and scheduling, per backend](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend).

1. Find which keys are due. The gauge deliberately names no key, so read the per-key verdict from the listing: `GET /rustfs/admin/v3/kms/keys` carries `rotation_due` and `rotation_due_reason` (`age`, `never_rotated`, `wraps`, or `unsupported`) per key, computed against `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` and `RUSTFS_KMS_ROTATION_MAX_WRAPS` — see [Rotation readiness](kms-backend-security.md#rotation-readiness-reported-never-acted-on). A `wraps` reason is not satisfied by relaxing the age threshold. If `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` is unset, set it to your policy's rotation period so the per-key verdict and this alert agree.
2. If the reason is `unsupported`, the backend cannot rotate at all (Local, Static); the only response is a backend migration.
3. On a backend that can rotate, act per the driver matrix in [Rotation drivers and scheduling, per backend](kms-backend-security.md#rotation-drivers-and-scheduling-per-backend) — external scheduler for Vault KV2, `auto_rotate_period` for Vault Transit, AWS automatic rotation for AWS KMS — and satisfy its pre-rotation checklist first, above all the [upgrade-ordering hard constraint](kms-backend-security.md#upgrade-before-first-rotation-hard-constraint). Never respond to this alert by rotating in the middle of a rolling upgrade.
4. Know the gauge's blind spot on Transit and AWS: only KV2 persists a rotation timestamp, so Transit and AWS keys age from creation permanently and this alert will not clear after a rotation there. Confirm the real cadence at the owning system (the Transit key's version history, or the key's rotation status in AWS) and treat a confirmed-healthy cadence as a known overstatement of this gauge.
5. If a KV2 key was genuinely rotated and the gauge stays high, remember the gauge is republished only by a sweep that saw the whole key set: check `rustfs_kms_deletion_sweep_keys_total` for `unreadable` or `failed` outcomes freezing the lifecycle gauges (see [Key lifecycle metrics](#key-lifecycle-metrics)), and that the deletion worker is running at all — it only runs on backends with the `schedule_deletion` capability, which is also why the Static backend never emits this series.

Related signals: `rotation_due` / `rotation_due_reason` on the key listing; `rustfs_kms_deletion_sweep_keys_total{outcome=~"unreadable|failed"}` (a frozen gauge is stale, not healthy).

## Startup persisted-configuration load

KMS configured through the admin API is persisted to cluster storage and restored on every startup. The load result is visible in two places; check both before concluding that KMS "was never configured":

| Startup log `event="kms_persisted_config_lookup"` (`target: rustfs::init`) | `GET /rustfs/admin/v3/kms/service-status` | Meaning and action |
| --- | --- | --- |
| `state="found"` | configured | Persisted configuration loaded and applied |
| `state="not_found"` | `"NotConfigured"` | Nothing persisted; configuring from scratch is the correct response |
| `state="load_failed"` | `Error("Failed to load persisted KMS configuration: ...")` or `Error("Failed to apply persisted KMS configuration: ...")` | A configuration exists but reading, unsealing, or decoding it failed. Do not resubmit a full configuration; use reload |

To recover from `load_failed` — or from any state where the server runs but its in-memory KMS lags the persisted configuration — call `POST /rustfs/admin/v3/kms/reload` (`kms:ServiceControl`). It re-reads the persisted configuration from cluster storage and reconfigures the service without resubmitting secrets, then broadcasts the reload to peer nodes. If reload keeps failing, check cluster storage health first (the read needs quorum), then `RUSTFS_KMS_CONFIG_SECRET`: an unseal error means the secret is missing or differs from the one that sealed the persisted copy — it must be identical on every node.

A separate event, `kms_config_load_skipped` with `reason="storage_uninitialized"`, comes from the ambient loader used by the peer-reload RPC path; seeing it outside a peer reload indicates a request arrived before storage initialization finished.

## Threshold calibration

Every numeric traffic or latency threshold in `rustfs-kms-alerts.yml` (5% error ratio, 2s p99, 0.5/s attempt failures, 0.05/s budget exhaustion) is a conservative default chosen without a production baseline, biased toward not paging on healthy-but-busy systems. Before relying on these alerts for paging: run the workload in staging for at least a week, record the steady-state values of the expressions above, then tighten thresholds to sit clearly above observed peaks. `KmsBackendCircuitOpen` is different: its gauge is direct state, and the one-minute hold only suppresses a circuit that recovers immediately. `KmsKeyRotationOverdue` is different in the other direction: its 400-day threshold is a policy default sitting above a common one-year rotation period — calibrate it against the rotation period your compliance policy requires and against `RUSTFS_KMS_ROTATION_MAX_AGE_SECS`, not against a staging baseline. Once a stable baseline exists, consider converting `KmsBackendAttemptFailureSpike` to a baseline-relative form (`offset 1d` ratio; see `.docker/observability/prometheus-rules/rustfs-get-optimization-alerts.yaml` for the pattern). Formal SLO targets for KMS operations are deliberately out of scope until that baseline exists.

## Coverage gaps

- The cache, Vault credential, and probe metric families have no dashboard panels and no alert rules; the lifecycle family has only [`KmsKeyRotationOverdue`](#kmskeyrotationoverdue). Building against them is safe: the names and label values above are what the code emits.
- The Local and Static backends emit no operation metrics, because they do not flow through the operation-policy choke point. Their cache metrics are emitted normally.
- No formal SLO targets until a production baseline exists — see [Threshold calibration](#threshold-calibration).

## Related documents

- [KMS backend security properties](kms-backend-security.md) — backend trust boundaries, minimal Vault policies, rotation drivers and retention preconditions.
- [Vault KMS authentication runbook](vault-kms-authentication.md) — credential sources, refresh behavior, and the fail-closed window.
- [KMS admin API contract](kms-admin-contract.md) — endpoint actions and the key listing contract.
- `deploy/observability/README.md` — dashboard import notes for all RustFS dashboards.
