## Related Issues

N/A

## Summary of Changes

This PR hardens the site replication control plane in small, reviewable commits. The work starts with guardrails identified during the MinIO comparison and multi-review pass: credential redaction, stricter diagnostic authorization, remove cleanup, and clearer status diagnostics.

### Implementation Log

1. `docs: plan site replication hardening`
   - Purpose: establish the execution plan and PR draft before code changes.
   - Reason: the site replication follow-up touches security, compatibility, operations, and tests, so reviewers need a durable record of each step and why it exists.

## Verification

Baseline started from `origin/main` at `758677da`.

- Passed: `cargo test -p rustfs route_policy --lib`

## Impact

The planned implementation is intended to reduce credential exposure, require stronger permission for diagnostic write-like endpoints, clean removed site replication targets, and make replication status more actionable. Compatibility-sensitive behavior will be called out per commit in the implementation log.

## Additional Notes

This PR intentionally keeps durable outbox/bootstrap/heal work as follow-up architecture unless the initial hardening steps require small supporting hooks.
