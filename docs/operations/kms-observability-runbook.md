# KMS observability runbook

This runbook covers the KMS backend operation metrics, the Grafana dashboard that visualizes them, and the response procedure for each Prometheus alert shipped in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`. It is the `runbook_url` target for those alerts. For what each KMS backend protects and how Vault authentication behaves, see the [KMS backend security properties](kms-backend-security.md) and the [Vault KMS authentication runbook](vault-kms-authentication.md).

## Metric reference

All four metrics are emitted at the single operation-policy choke point (`crates/kms/src/policy.rs`) that every instrumented KMS backend call flows through. Label values are exclusively static enum strings — key identifiers, key material, ciphertext, paths, and tokens never appear in metric labels, and any change that would add such a label is a regression.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_kms_backend_operations_total` | counter | `operation`, `op_class`, `outcome` | Operations executed under the operation policy, counted once per terminal outcome |
| `rustfs_kms_backend_attempt_failures_total` | counter | `operation`, `error_class` | Individual failed attempts, including attempts the retry policy later absorbed |
| `rustfs_kms_backend_operation_duration_seconds` | histogram | `operation`, `outcome` | Wall-clock duration of a whole operation, including retries and backoff sleeps |
| `rustfs_kms_backend_operation_attempts` | histogram | `operation`, `outcome` | Number of attempts one operation used before completing |

Label values:

- `outcome`: `success`, `fatal` (a non-retryable failure ended the operation on first observation), `budget_exhausted` (the attempt budget ran out on retryable failures), `deadline_exceeded` (the operation deadline ran out before another attempt could complete), `cancelled` (shutdown or caller cancellation).
- `op_class`: `read_idempotent` (safe to retry), `mutating_non_idempotent` (never replayed — a retryable failure terminates after a single attempt because the server may have processed the request), `auth` (login and token renewal).
- `error_class`: `retryable_conn` (connection-level failure: dial, TLS, broken connection), `retryable_status` (retryable backend status, e.g. Vault 5xx or a sealed Vault's 503), `attempt_timeout` (the per-attempt timeout cut the attempt off; retried like a connection failure because the server may still have processed the request), `fatal` (non-retryable: authentication, permissions, malformed request, missing key or version).
- `operation`: static per-call-site names, e.g. `vault_kv2_read_key_version`, `vault_kv2_cas_write_key`, `vault_transit_encrypt`, `vault_transit_decrypt`, `vault_login`, `vault_token_renew`.

Export path: the `metrics` facade feeds the OTel recorder in `crates/obs`, which exports over OTLP to the collector scraped by Prometheus. Histograms therefore appear in Prometheus as `_bucket`/`_sum`/`_count` series. These metrics do not carry the RustFS `server` label used by the node observability dashboard — distinguish nodes through your scrape topology (`job`/`instance` or promoted OTel resource attributes such as `service_instance_id`).

Instrumentation boundary: the Local and Static backends do not flow through the choke point and emit no operation metrics; bringing them under the same instrumentation is tracked separately (rustfs/backlog#1569). Absence of KMS series on a cluster using those backends is expected, not an outage.

## Dashboard

Import `deploy/observability/grafana/rustfs-kms-observability.json` into Grafana and select a Prometheus data source that scrapes RustFS metrics. The dashboard has two variables: `datasource` (Prometheus data source) and `operation` (multi-select over the `operation` label). In the docker-compose observability stack (`.docker/observability/`), dashboards are provisioned from a directory (`grafana/provisioning/dashboards/dashboard.yml` points at `/etc/grafana/dashboards`), so no per-file registration is needed there.

The dashboard's "Planned Panels (TODO)" text panel lists metric families designed under rustfs/backlog#1584 but not yet landed (key-cache effectiveness, lifecycle gauges, token TTL, synthetic probe). Do not add panels or alerts for those names until the emitting code is merged; keep that panel and the [Coverage gaps](#coverage-gaps-and-planned-metrics) section below in sync as they land.

## Alert rules

The rules live in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`. The docker-compose Prometheus loads `/etc/prometheus/rules/*.yml`, so the `.yml` extension is load-bearing. Validate edits with `promtool check rules rustfs-kms-alerts.yml`.

Every threshold in that file is a conservative default chosen without a production baseline; see [Threshold calibration](#threshold-calibration) before treating a firing alert as an SLO breach or a quiet one as health.

## Alert response procedures

### KmsBackendFatalErrors

Meaning: attempts are failing with `error_class="fatal"` — failures the policy never retries. Each one is a KMS backend call that failed permanently (authentication, permissions, malformed request, or a missing key/version), so callers are seeing errors right now. This is the highest-signal KMS alert: fatal failures do not appear as background noise in a healthy system.

Investigation:

1. Break the rate down by operation: `sum by (operation) (rate(rustfs_kms_backend_attempt_failures_total{error_class="fatal"}[5m]))`.
2. If the failing operations are `vault_login` or `vault_token_renew` (`op_class="auth"`), the Vault credentials are invalid or expired. Follow the [Vault KMS authentication runbook](vault-kms-authentication.md) — note that credential refresh is fail-closed, so a broken credential eventually takes down all Vault-backed operations, not just auth. Look for the `Vault token renewal failed; falling back to a fresh login` and `Vault credential refresh failed; retrying until the credentials recover` warnings in the RustFS logs.
3. If the failing operations are `vault_kv2_*` or `vault_transit_*`, check for Vault permission denials: compare the token's policy against the minimal policy in [KMS backend security properties](kms-backend-security.md) (a policy that drifted or was re-scoped produces 403s that classify as fatal), and check the Vault audit log for the corresponding denied requests.
4. A fatal `KeyVersionNotFound` on decrypt-path operations means a DEK envelope references a key version whose record is missing. Decryption deliberately fails closed with no fallback — see the rotation retention preconditions in [KMS backend security properties](kms-backend-security.md) and verify nobody destroyed version records under the key subtree.
5. Confirm blast radius with the outcome view: `sum by (operation) (rate(rustfs_kms_backend_operations_total{outcome="fatal"}[5m]))`.

Related signals: the "Attempt Failure Rate by Error Class" and "Backend Operation Rate by Outcome" dashboard panels; Vault server audit and server logs; S3-level 5xx on encrypted buckets.

### KmsBackendHighErrorRate

Meaning: more than 5% of KMS operations are terminating without success (`fatal`, `budget_exhausted`, or `deadline_exceeded`; `cancelled` is excluded because shutdown windows legitimately produce it). A traffic guard suppresses the alert below ~0.02 ops/s so a single failure on a near-idle cluster does not page.

Investigation:

1. Break the failures down by outcome: `sum by (outcome) (rate(rustfs_kms_backend_operations_total{outcome!~"success|cancelled"}[5m]))`.
2. If `fatal` dominates, follow [KmsBackendFatalErrors](#kmsbackendfatalerrors).
3. If `budget_exhausted` or `deadline_exceeded` dominates, follow [KmsBackendRetryBudgetExhausted](#kmsbackendretrybudgetexhausted) — the backend is unavailable or too slow for longer than the retry policy can bridge.
4. Correlate with client impact: encrypted-object PUT/GET failures and S3 error rates on buckets with encryption configured.

Related signals: the "Non-Success Outcome Ratio" dashboard panel; the KMS-related warnings listed under the other alerts in this runbook.

### KmsBackendP99LatencyHigh

Meaning: the p99 wall-clock duration of KMS operations is sustained above 2s. The histogram includes retries and backoff sleeps, so a high p99 with a healthy p50 usually means a slow retry tail (a subset of calls failing and being retried), not a uniform slowdown.

Investigation:

1. Compare p50 and p99 on the "Operation Duration p50 / p99" panel. Flat p50 with elevated p99 points at retries; both elevated points at the backend or the network path being uniformly slow.
2. Split by operation with `histogram_quantile(0.99, sum by (le, operation) (rate(rustfs_kms_backend_operation_duration_seconds_bucket[5m])))` to see whether one backend call or all of them regressed.
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

## Threshold calibration

Every numeric threshold in `rustfs-kms-alerts.yml` (5% error ratio, 2s p99, 0.5/s attempt failures, 0.05/s budget exhaustion) is a conservative default chosen without a production baseline, biased toward not paging on healthy-but-busy systems. Before relying on these alerts for paging: run the workload in staging for at least a week, record the steady-state values of the expressions above, then tighten thresholds to sit clearly above observed peaks. Once a stable baseline exists, consider converting `KmsBackendAttemptFailureSpike` to a baseline-relative form (`offset 1d` ratio, see `.docker/observability/prometheus-rules/rustfs-get-optimization-alerts.yaml` for the pattern). Formal SLO targets for KMS operations are deliberately out of scope until that baseline exists (rustfs/backlog#1584).

## Coverage gaps and planned metrics

The following families are designed under rustfs/backlog#1584 but land in separate changes; they are intentionally absent from the dashboard and alert rules, and referencing their names before the emitting code merges would produce permanently-empty panels and never-firing alerts:

- Key-cache effectiveness: hit/miss counters, entry gauge, eviction counter (replacing the former hardcoded-zero miss statistic).
- Key lifecycle: pending-deletion/tombstone gauges from the deletion-worker sweep and an aggregate rotation-age gauge.
- Vault credentials: token TTL remaining and fail-closed state gauges (today only the log warnings quoted above exist).
- Synthetic backend probe: probe outcome and failure-class metrics feeding the readiness cache.

When one of these lands, add its panels, extend the alert rules, replace the corresponding TODO bullet in the dashboard's "Planned Panels" text panel, and update this section.

## Related documents

- [KMS backend security properties](kms-backend-security.md) — backend trust boundaries, minimal Vault policies, rotation retention preconditions.
- [Vault KMS authentication runbook](vault-kms-authentication.md) — credential sources, refresh behavior, and the fail-closed window.
- `deploy/observability/README.md` — dashboard import notes for all RustFS dashboards.
