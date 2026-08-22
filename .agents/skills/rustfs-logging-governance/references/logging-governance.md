# Logging Audit and Migration Reference

Read this reference only for a broad logging audit, an event-model migration,
or a change to `scripts/check_logging_guardrails.sh`. Use `Cargo.toml` for the
current workspace/crate list instead of maintaining one here.

## Audit by Operational Role

- Server/protocol/admin: lifecycle, authorization failures, request boundaries,
  and degraded subsystems; avoid normal request success at `info`.
- Storage/heal/scanner/capacity: integrity failures and aggregate lifecycle;
  avoid per-object, per-shard, and folder iteration noise.
- IAM/policy/credentials/KMS/crypto: safe identifiers and enforcement results;
  never emit secrets, claims, payloads, or expected authenticators.
- Notify/audit/targets: target lifecycle and batch/backpressure summaries; avoid
  per-event success logs.
- Locking/concurrency/I/O foundations: contention anomalies and state changes;
  prefer metrics for high-frequency worker/permit signals.
- Shared type/schema crates: log at the operational caller boundary unless the
  crate itself owns the failure context.

## Event Shape

Prefer stable fields in this order when available:

1. `event`
2. `component`
3. `subsystem`
4. `state` or `result`
5. stable context such as mode, duration, reason, counts, safe identifiers, or
   capacity/permit values
6. short message label

Reuse the module's constants and neighboring field names. Do not create aliases
for the same concept.

## Patterns to Retire

- sentence-style lifecycle announcements;
- startup banners and checklist lines;
- repetitive success logs at `info`/`debug`;
- raw inventories when an aggregate count is sufficient;
- fallback prose with values embedded in the message;
- `?value`/`Debug` output for credential-bearing or attacker-controlled data;
- logging a parse input when the malformed input may itself be a secret.

## Guardrail Changes

When expanding `scripts/check_logging_guardrails.sh`:

1. Add only files/patterns intentionally migrated in the same change.
2. Keep patterns concrete and grep-friendly.
3. Do not encode a style that remains valid elsewhere as a global ban.
4. Run the guardrail script and the root validation tier.
5. Treat the script as a floor; manually verify level, field shape, and privacy.

Useful search seeds for the changed surface:

```bash
rg -n 'error!|warn!|info!|debug!|trace!|#\[instrument' <changed-paths>
rg -n '\?[^,)]|secret|token|credential|authorization|merged_config' <changed-paths>
```
