## Related Issues

N/A

## Summary of Changes

This PR hardens the site replication control plane in small, reviewable commits. The work starts with guardrails identified during the MinIO comparison and multi-review pass: credential redaction, stricter diagnostic authorization, remove cleanup, and clearer status diagnostics.

### Implementation Log

1. `docs: plan site replication hardening`
   - Purpose: establish the execution plan and PR draft before code changes.
   - Reason: the site replication follow-up touches security, compatibility, operations, and tests, so reviewers need a durable record of each step and why it exists.
2. `fix: redact site replication target secrets`
   - Purpose: prevent bucket replication target credentials from leaking through admin listing, bucket metadata export, and debug formatting.
   - Reason: site replication stores the `site-replicator-0` secret in bucket targets today; keeping the internal storage format intact while redacting external surfaces reduces immediate blast radius without changing replication target loading.
3. `fix: require operation access for replication diagnostics`
   - Purpose: require operation-level admin authorization for site replication diagnostic POST endpoints and clamp netperf duration.
   - Reason: `devnull` and `netperf` read request bodies and exercise diagnostic work, so granting them through read-only site replication info permission was broader than intended.

## Verification

Baseline started from `origin/main` at `758677da`.

- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs-ecstore bucket_target --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all`

## Impact

The planned implementation is intended to reduce credential exposure, require stronger permission for diagnostic write-like endpoints, clean removed site replication targets, and make replication status more actionable. Compatibility-sensitive behavior will be called out per commit in the implementation log.

The credential redaction step changes admin/export visibility of bucket target secrets. Stored target configuration is not migrated or reformatted by this PR step.

The diagnostic authorization step intentionally changes required permission for `POST /site-replication/devnull` and `POST /site-replication/netperf` from `SiteReplicationInfoAction` to `SiteReplicationOperationAction`.

## Additional Notes

This PR intentionally keeps durable outbox/bootstrap/heal work as follow-up architecture unless the initial hardening steps require small supporting hooks.
