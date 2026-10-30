# Audit Entries for Authorization Denials

**Use this when:** you consume the audit stream and need to know which authorization denials appear in it, how many, and how to tune the limit.
**Source of truth:** `rustfs/src/storage/denial_audit.rs` (`DenialAuditor`, `DenialRateLimiter`), `rustfs/src/storage/access.rs` (`DenialContext`), `crates/config/src/constants/env.rs` (`ENV_AUDIT_DENIAL_RATE`, `ENV_AUDIT_DENIAL_ANONYMOUS_RATE`).

With audit enabled (`RUSTFS_AUDIT_ENABLE=true`), a request refused by the authorization layer produces an audit entry in addition to the `s3_authorization_denied` log event. The S3 response is unchanged: a bare `Access Denied`.

## Entry shape

| Field | Value |
| --- | --- |
| `event` | `audit:S3:Operation` |
| `api.name` | the refused action, for example `s3:PutObject` |
| `api.bucket`, `api.object` | the request's bucket and object (the object is omitted for a bulk entry) |
| `api.status`, `api.statusCode` | `failure`, `403` |
| `accessKey` | the caller's access key, absent for an anonymous caller |
| `error` | `Access Denied` |
| `tags.denialReason` | the internal reason, for example `iam_explicit_deny`, `bucket_policy_explicit_deny`, `no_policy_allows_action` |
| `tags.deniedKeys` | bulk entries only: how many keys of a DeleteObjects request were refused for the action in `api.name` |
| `tags.suppressedDenials` | present when denials were dropped by the rate limit since the previous entry |

## What is not audited

- Probe denials that callers treat as an expected filter outcome (the per-bucket ListBuckets fallback and per-object metadata permission collection).
- A DeleteObjects request audits its refused keys as one entry per denied action (for example `s3:DeleteObject`, `s3:GetObject`, `s3:BypassGovernanceRetention`) with `tags.deniedKeys`, not one entry per key.
- Denials that are later overridden, such as an implicit IAM deny followed by a bucket policy allow.

## Rate limit

A caller can cause denials at will, so the number of entries is bounded by a process-wide token bucket per caller class. The burst equals the per-second rate.

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUSTFS_AUDIT_DENIAL_RATE` | `50` | Entries per second for authenticated callers. `0` turns denial entries off for them. |
| `RUSTFS_AUDIT_DENIAL_ANONYMOUS_RATE` | `5` | Entries per second for anonymous callers. `0` turns denial entries off for them. |

Denials dropped by the limit are counted per caller class, and the count is reported in `tags.suppressedDenials` on the next entry of the same class that is let through. The limit is per process, not per node group.

Trade-off: all authenticated callers share one bucket. A valid, low-privilege account that causes more denials per second than `RUSTFS_AUDIT_DENIAL_RATE` can use up the budget and crowd out other principals' denial entries during that time. Its own drops are still counted in `tags.suppressedDenials`, so the loss is visible, but the other principals' denials in that window are not recorded. A per-principal sub-limit would remove this and is a follow-up, not part of this change.
