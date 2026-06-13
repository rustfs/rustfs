# RustFS Logging Governance Reference

## Workspace Scope Map

Use `Cargo.toml` `[workspace].members` as the source of truth for crate membership. When doing a broad logging sweep, classify crates by operational role so logs stay consistent within each role.

### Core Server And Request Handling

- `rustfs`
  - Role: top-level server, startup, auth, admin wiring, S3 request handling.
  - Logging focus: startup lifecycle, config summaries, authn/authz failures, protocol entrypoints, degraded subsystems.
- `crates/protocols`
  - Role: protocol integrations such as FTP, SFTP, WebDAV, and related server-side protocol layers.
  - Logging focus: listener lifecycle, per-protocol enablement/disablement, request bridge failures.
- `crates/madmin`
  - Role: admin API contracts and management interfaces.
  - Logging focus: admin action boundaries, validation failures, compatibility warnings.
- `crates/trusted-proxies`
  - Role: forwarded IP trust, proxy chain validation, cloud metadata sources.
  - Logging focus: direct/trusted/fallback decisions, degraded metadata fetches, aggregated config summaries.
- `crates/keystone`
  - Role: Keystone auth integration.
  - Logging focus: integration enablement, upstream auth failures, config safety without credential leakage.

### Storage, Healing, And Data Plane

- `crates/ecstore`
  - Role: erasure-coded storage implementation and peer/store initialization.
  - Logging focus: disk/peer lifecycle, storage fallback, object I/O failures, avoid per-object noise.
- `crates/heal`
  - Role: healing orchestration and repair workflows.
  - Logging focus: scheduler lifecycle, repair decisions, backlog or skipped work summaries, avoid repetitive task spam at `info`.
- `crates/scanner`
  - Role: data integrity scanning and health monitoring.
  - Logging focus: scan lifecycle, compaction/deep-heal transitions, lag/backlog, noisy folder iteration should stay at `debug/trace`.
- `crates/object-capacity`
  - Role: capacity scan and refresh core.
  - Logging focus: refresh lifecycle, degraded capacity sources, aggregate stats rather than per-object chatter.
- `crates/filemeta`
  - Role: file metadata parsing and helpers.
  - Logging focus: parse failures, schema/format mismatch, avoid dumping raw metadata payloads.
- `crates/storage-api`
  - Role: storage contracts and shared data plane interfaces.
  - Logging focus: contract mismatch and boundary diagnostics, usually low-volume.
- `crates/checksums`
  - Role: checksum helpers and validation.
  - Logging focus: integrity failures and compatibility mismatches, not per-chunk success logs.
- `crates/zip`
  - Role: ZIP handling and compression helpers.
  - Logging focus: parse/extract failures, archive path safety issues, avoid verbose file-by-file success logs.

### Security, Identity, And Policy

- `crates/iam`
  - Role: identity and access management.
  - Logging focus: authz decision boundaries, imported payload safety, do not leak principals, secrets, or claims.
- `crates/policy`
  - Role: policy modeling and evaluation.
  - Logging focus: deny/allow decision context, parser/validation failures, no raw secret-bearing request dumps.
- `crates/credentials`
  - Role: credential handling.
  - Logging focus: never log secrets or tokens; only safe identifiers and redacted states.
- `crates/kms`
  - Role: key management service integration.
  - Logging focus: init/health/fallback, key-source availability, never log key material.
- `crates/crypto`
  - Role: cryptographic helpers and security primitives.
  - Logging focus: only algorithm or mode state, not plaintext, ciphertext, or secret-derived material.
- `crates/security-governance`
  - Role: security governance contracts.
  - Logging focus: policy/state transitions and enforcement diagnostics.
- `crates/signer`
  - Role: request signing helpers.
  - Logging focus: signature validation failures without expected-signature leakage.

### Notifications, Audit, And Targets

- `crates/notify`
  - Role: notification dispatch, runtime facade, notifier implementations.
  - Logging focus: target lifecycle, dispatch summaries, stream lag/backpressure, avoid per-event success spam.
- `crates/audit`
  - Role: audit target fan-out and audit pipeline management.
  - Logging focus: pipeline lifecycle, target availability, batch dispatch summaries, avoid noisy "started successfully" prose.
- `crates/targets`
  - Role: target-specific configuration and utilities used by fan-out style systems.
  - Logging focus: target selection, config validation, per-target degraded state.
- `crates/s3-types`
  - Role: S3 event and type definitions.
  - Logging focus: usually minimal; keep logging at integration boundaries rather than low-level type crates.
- `crates/s3-ops`
  - Role: S3 operation definitions and mapping.
  - Logging focus: mapping/contract failures, unsupported combinations, not normal-path request spam.

### Concurrency, Locking, And Runtime Foundations

- `crates/concurrency`
  - Role: timeout, locking, backpressure, and I/O scheduling facade.
  - Logging focus: lifecycle transitions and degraded states, not high-frequency worker/permit churn at `info`.
- `crates/lock`
  - Role: distributed locking implementation.
  - Logging focus: lock lifecycle, contention anomalies, lock ordering or timeout diagnostics.
- `crates/tls-runtime`
  - Role: shared TLS runtime foundation.
  - Logging focus: certificate lifecycle, reload/fallback, validation failures without sensitive dumps.
- `crates/obs`
  - Role: observability helpers.
  - Logging focus: this crate shapes other crates' telemetry conventions; avoid recursive or redundant summaries.
- `crates/io-core`
  - Role: zero-copy I/O core primitives.
  - Logging focus: keep very sparse; prefer metrics unless failures are actionable.
- `crates/io-metrics`
  - Role: I/O metrics collection.
  - Logging focus: typically minimal; metrics should carry the hot-path signal.
- `crates/rio`
  - Role: Rust I/O utility layer.
  - Logging focus: compatibility or runtime boundary failures, not fast-path internals.
- `crates/rio-v2`
  - Role: next-generation I/O compatibility layer.
  - Logging focus: migration/feature-mode differences and degraded fallback between I/O paths.
- `crates/utils`
  - Role: shared helpers.
  - Logging focus: usually avoid direct logging in generic helpers unless the helper is itself an operational boundary.
- `crates/common`
  - Role: shared data structures and helpers.
  - Logging focus: same principle as `utils`; prefer callers to log context-rich events.
- `crates/config`
  - Role: configuration management.
  - Logging focus: config source, fallback, validation, and summary aggregation; avoid dumping merged configs.
- `crates/data-usage`
  - Role: shared data usage models and algorithms.
  - Logging focus: refresh lifecycle, summary stats, and degraded reads.

### Schema, Contracts, And API Support

- `crates/protos`
  - Role: protobuf definitions.
  - Logging focus: usually none inside the crate; emit logs at decode/use boundaries.
- `crates/extension-schema`
  - Role: extension schema contracts.
  - Logging focus: schema validation and compatibility mismatches.
- `crates/s3select-api`
  - Role: S3 Select API interfaces.
  - Logging focus: request validation and unsupported feature boundaries.
- `crates/s3select-query`
  - Role: S3 Select query engine.
  - Logging focus: query parse/planning/execution failures, avoid row-level spam.
- `crates/protocols`
  - Role: non-S3 protocol support.
  - Logging focus: see core server section; keep per-request verbosity below `info`.

### Testing And Non-Production Crates

- `crates/e2e_test`
  - Role: end-to-end tests.
  - Logging focus: test clarity matters more than production governance, but avoid copying test-only logging style into production crates.

## Current Guardrail Coverage Map

`scripts/check_logging_guardrails.sh` currently enforces retired patterns in these high-signal areas:

- `rustfs/src/main.rs`
- `rustfs/src/startup_iam.rs`
- `rustfs/src/auth.rs`
- `rustfs/src/protocols/client.rs`
- `crates/audit/src/pipeline.rs`
- `crates/audit/src/system.rs`
- `crates/audit/src/global.rs`
- `crates/notify/src/config_manager.rs`
- `crates/notify/src/runtime_facade.rs`
- `crates/notify/src/notifier.rs`
- `crates/ecstore/src/store/peer.rs`
- `crates/ecstore/src/store/init.rs`
- `crates/ecstore/src/tier/tier.rs`
- `crates/concurrency/src/workers.rs`
- `crates/concurrency/src/manager.rs`
- `crates/concurrency/src/lock.rs`
- `crates/concurrency/src/deadlock.rs`
- `crates/trusted-proxies/src/global.rs`
- `crates/trusted-proxies/src/config/loader.rs`
- `crates/trusted-proxies/src/proxy/metrics.rs`
- `crates/trusted-proxies/src/proxy/validator.rs`
- `crates/trusted-proxies/src/proxy/chain.rs`
- `crates/trusted-proxies/src/middleware/service.rs`
- `crates/trusted-proxies/src/cloud/detector.rs`
- `crates/trusted-proxies/src/cloud/ranges.rs`
- `crates/trusted-proxies/src/cloud/metadata/aws.rs`
- `crates/trusted-proxies/src/cloud/metadata/azure.rs`
- `crates/trusted-proxies/src/cloud/metadata/gcp.rs`

When expanding coverage, prefer crates with:

- repeated sentence-style lifecycle logs
- high-frequency success-path `info!`
- startup/config checklist banners
- security-sensitive fallback wording
- external fetch/retry/fallback flows

That typically means the next broad candidates are `rustfs`, `crates/notify`, `crates/audit`, `crates/targets`, `crates/heal`, and `crates/scanner`.

## Event Model

Prefer this structure when the fields are available:

- `event`
- `component`
- `subsystem`
- `state` or `result`
- stable context fields such as:
  - `enabled`
  - `implementation`
  - `validation_mode`
  - `peer_ip`
  - `client_ip`
  - `proxy_hops`
  - `duration_ms`
  - `fallback`
  - `reason`
  - `range_count`
  - `hold_time_ms`
  - `available_slots`
  - `total_slots`
  - `permits_in_use`

## Level Policy

- `error`: the operation fails and callers or security guarantees are affected.
- `warn`: a degraded path, fallback, suspicious request, or operator-actionable config issue occurs.
- `info`: a low-frequency lifecycle or mode transition occurs.
- `debug`: useful diagnostics exist but normal operators do not need them all the time.
- `trace`: hot-path and repetitive success-path details occur.

## Preferred Patterns

- Use a short message label:
  - `"trusted proxy validation failed"`
  - `"concurrency manager state changed"`
  - `"trusted proxy cloud metadata loaded"`
- Put key meaning into fields, not only the message text.
- Aggregate config or metrics summaries into one log event.

## Retired Patterns

These should usually be removed or replaced:

- Sentence-style lifecycle logs:
  - `info!("Concurrency manager stopped")`
  - `info!("Trusted Proxies module initialized")`
- Checklist or banner logs:
  - `info!("=== Application Configuration ===")`
  - `info!("Available metrics:")`
- Hot-path noise:
  - `info!("worker take, {}", *available)`
  - `debug!("Proxy validation successful in {:?}", duration)`
- Legacy fallback prose:
  - `"Request from private network but not trusted: ..."`
  - `"Cloud metadata fetching is disabled"`

## Guardrail Update Checklist

When extending `scripts/check_logging_guardrails.sh`:

1. Add the touched files to `checked_files`.
2. Add only legacy patterns that have been intentionally retired.
3. Keep patterns literal and grep-friendly.
4. Run the guardrail script after changes.
5. Avoid adding patterns for logs that are still valid elsewhere in the repo.

## Validation Checklist

For logging-only changes:

```bash
cargo fmt --all --check
./scripts/check_logging_guardrails.sh
cargo check -p <affected-crate>
cargo test -p <affected-crate>
```

For broader Rust changes:

```bash
./scripts/check_unsafe_code_allowances.sh
./scripts/check_architecture_migration_rules.sh
cargo clippy -p <affected-crates> --all-targets -- -D warnings
```
