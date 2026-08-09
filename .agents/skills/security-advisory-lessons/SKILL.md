---
name: security-advisory-lessons
description: Apply RustFS security lessons distilled from repository GitHub Security Advisories. Use when making or reviewing RustFS code changes, doing security checks, handling PR review for auth/authz, IAM, storage, RPC, logging, CORS, console/browser, encryption, policy, or endpoint changes, and when deciding which security regression tests are required.
---

# RustFS Security Advisory Lessons

Use this skill as a RustFS-specific security lens before changing or approving code. For the distilled advisory lessons and review patterns, read [advisory-patterns.md](references/advisory-patterns.md).

When currentness matters, fetch the live advisory inventory instead of relying on this skill as a status mirror:

```bash
gh api repos/rustfs/rustfs/security-advisories --paginate \
  --jq '.[] | {ghsa_id,state,severity,summary,updated_at}'
```

Fetch full advisory details only when the live summary suggests a new or changed lesson:

```bash
gh api repos/rustfs/rustfs/security-advisories/<GHSA_ID>
```

For the full pattern map, read [advisory-patterns.md](references/advisory-patterns.md).

## Workflow

### 1. Scope the change
- Identify touched routes, protocol frontends, handlers, storage paths, credentials, logs, browser surfaces, CI/release code, and policy checks.
- Treat these paths as security-sensitive by default: `rustfs/src/admin/`, `rustfs/src/storage/`, `rustfs/src/auth.rs`, `rustfs/src/server/layer.rs`, `crates/iam/`, `crates/policy/`, `crates/credentials/`, `crates/ecstore/src/rpc/`, `crates/protocols/`, `crates/rio/`, OIDC/STS federation code, and console preview/auth code.

### 2. Map to advisory classes
- Read [advisory-patterns.md](references/advisory-patterns.md) for matching GHSA lessons.
- Do not rely on advisory titles alone. Confirm whether the issue is authentication, authorization, input validation, storage invariant, browser isolation, logging, or operational hardening.

### 3. Verify fail-closed behavior
- Check that unauthenticated, wrong-permission, cross-user, cross-bucket, malformed-input, and default-config cases fail explicitly.
- Prefer exact action/permission checks over broad helper calls or inferred ownership.
- Confirm lower storage/RPC layers do not bypass checks done in upper layers.

### 4. Require regression evidence
- For behavior changes, add focused negative tests that reproduce the advisory class.
- For sensitive fixes, include tests for the bypass form, not only the happy path.
- If a test is impractical, explain the residual risk and provide a manual verification command.

### 5. Report clearly
- Lead with concrete findings and file/line evidence.
- Separate proven vulnerabilities from hardening risks.
- Avoid exaggerating unauthenticated impact when the code actually rejects unauthenticated requests but allows a low-privileged authenticated bypass.

## Advisory-Derived Guardrails

### Auth and admin authorization
- Every admin or diagnostic route needs an explicit authn and authz story. Route registration, router whitelist, and handler-level authorization must agree.
- Match the admin action to the operation exactly. Copy-paste action constants are a known RustFS vulnerability class.
- Avoid authentication-only helpers for state-changing admin APIs; use `validate_admin_request` or the established equivalent with the right `AdminAction`.
- Read-only admin APIs such as metrics, server info, and diagnostics still require admin authorization; checking only that credentials exist is not enough.
- Replication admin reads can expose remote target credentials; list/get target endpoints require replication/admin authorization and must not return secrets to low-privilege callers.
- Do not assume admin-action `Resource` scoping constrains blast radius unless the policy engine actually enforces resources for that action.

### IAM and service accounts
- Treat imported IAM payload fields as attacker-controlled: `parent`, `claims`, `accessKey`, `secretKey`, status, policy names, and groups.
- For service account create/update/import, prove parent ownership or root/admin authority before writing credentials or claims; an action permission alone must not allow choosing root or another user as `target_user`.
- Treat IAM export packages as credential disclosure surfaces; never include plaintext user or service-account secret keys unless the caller is allowed to recover those secrets and the export format is intentionally sealed.
- Do not let `deny_only` or "no explicit deny" become an allow decision that skips required allow checks.
- Test cross-user list/update/import flows with wrong, correct, self, parent, and root identities.

### STS, OIDC, and federation flows
- Every STS endpoint must have an explicit authentication story: SigV4 where required, OIDC token verification for web identity, and role/session policy validation before issuing credentials.
- For web identity, the JWT is the credential; exemption from SigV4 is not itself an authentication bypass. Treat pre-verification claims only as untrusted routing hints, bound token size, normalize public failures, rate-limit discovery, and issue credentials only after signature, issuer, audience, and expiration checks.
- JWT session tokens must be signed and verified by a trusted issuer/key path, not by service-account-controlled material or a reused root secret.
- JWT verification must enforce required claims and expiration for every bearer token path; "allow missing exp" is never acceptable for user-presented credentials.
- Public OIDC bootstrap and callback routes must treat `Host`, `X-Forwarded-Proto`, redirect targets, `state`, and callback parameters as untrusted; credential-bearing redirects require a configured, allowlisted origin.
- OIDC discovery and validation URLs are SSRF sinks. Resolve and classify hostnames at connection time, reject rebinding to loopback/private/link-local ranges, and do not rely on literal string checks.

### IAM policy conditions and plugins
- Treat request headers as attacker-controlled even after SigV4; callers sign their own spoofed headers. Do not merge them into server-derived condition keys such as identity, groups, version ID, signature version, JWT, or LDAP claims.
- Keep the condition-key namespace explicit. Reserved server-derived keys must reject or ignore colliding headers, while intentional request-header keys such as `s3:x-amz-*` remain available.
- Quantified IAM condition tests need partially overlapping multi-value sets. Fully contained and fully disjoint sets cannot distinguish `ForAllValues` from `ForAnyValue` bugs.
- External policy plugins must receive the same security context as built-in policy evaluation. If OPA or another plugin depends on existing object tags, load and pass `ExistingObjectTag/*` before the plugin decision.

### S3 object actions, copy, multipart, and presigned POST
- Version-aware object requests need version-aware actions. Explicit `versionId` reads and copy sources must authorize `s3:GetObjectVersion`, not only `s3:GetObject`.
- Multipart copy must enforce source `GetObject` and destination `PutObject` semantics equivalent to `CopyObject`, including copy-source and policy conditions.
- Do not let `CreateMultipartUpload`, `UploadPartCopy`, `CompleteMultipartUpload`, or `AbortMultipartUpload` return success without authorization.
- Fallbacks from version actions to non-version actions must still pass the same public-access-block, anonymous-deny, and post-authorization gates as a direct allow.
- Presigned POST policies are server-side contracts. Enforce `content-length-range`, key prefix, exact metadata/content-type, and all signed policy conditions.

### Protocol frontends and IAM parity
- FTP/FTPS, SFTP, gateway, and other protocol drivers must enforce IAM per operation before calling storage backends; authentication to a protocol listener is not authorization.
- Match protocol commands to the same S3 actions as HTTP, such as `RETR` to `GetObject`, `SIZE`/`MDTM` to `HeadObject`, `MKD` to `CreateBucket`, and bucket probes to `ListBucket` or `HeadBucket`.
- Review every handler in a protocol driver, not only the changed handler, because RustFS advisories show mixed guarded and unguarded siblings in the same driver.
- Regression tests for protocol frontends should deny the shared authorization hook and prove the backend is not reached for the denied command.
- Compare protocol secrets in constant time, normalize invalid-user and invalid-secret failures where practical, and add rate limiting before exposing password-style protocol endpoints.

### Paths, object keys, and filesystem access
- Never join untrusted bucket/object/RPC path strings onto filesystem roots without normalization and boundary checks.
- Reject or safely handle `..`, absolute paths, URL-encoded traversal, platform separators, empty components, and paths that canonicalize outside the intended root.
- Validate both S3 object-key paths and internode/RPC disk paths; storage helpers can bypass S3 authorization if they trust already-parsed paths.
- Archive auto-extract paths are object keys too. Validate tar/zip entry names before IAM checks and before storage writes, and prove cleaned paths cannot cross bucket or prefix boundaries.

### Secrets, default credentials, and crypto
- Do not ship hard-coded shared tokens, HMAC secrets, private keys, or production test keys.
- Defaults for root credentials and internode/RPC auth must fail closed for network-reachable deployments or generate per-install random secrets; warnings alone are not a security boundary.
- Keep cryptographic roles separated: root S3 credentials, RPC HMAC keys, and STS/JWT signing keys must not be reused or deterministically derived from each other.
- License or token validation must use signatures with embedded public/verifying keys only; do not use private-key decryption as authenticity.
- Plan key rotation and key IDs when removing exposed keys.

### Logging and debug output
- Logs must never include access keys beyond safe identifiers, secret keys, session tokens, JWT claims, HMAC secrets, expected signatures, license secrets, or raw response bodies containing credentials.
- Treat `Debug` implementations, `?value` tracing, merged config dumps, and dependency-level HTTP body logging as leak surfaces.
- Error and panic messages are log content: they propagate through `?` and get printed by `error!`/startup logging far from where they were constructed. Never interpolate a raw config or credential value into an error string.
- A value that fails secret-format parsing is usually the secret itself (e.g. a bare base64 key missing its `<name>:` prefix), so a parse-failure hint must name the env var or file and the expected format, never echo the input. Redacting `Debug` impls does not cover this channel.
- Add log-capture tests or targeted unit tests for redaction wrappers when changing credential structs or response bodies.

### RPC, parsing, and panic safety
- Treat all RPC payload bytes as attacker-controlled. Replace `unwrap`, `expect`, and panic-prone deserialization with typed errors.
- Malformed request tests should cover empty bytes, truncated MessagePack/protobuf, invalid enum values, stale timestamps, and invalid signatures.
- RPC authentication must be independently strong; do not depend on S3 admin credentials unless the fallback is explicit and safe.
- RPC signatures must bind the exact generated gRPC method path, timestamp, and request method. Service-prefix signatures must not authorize a different concrete NodeService call.

### Browser, CORS, and console surfaces
- Do not reflect arbitrary `Origin` while also allowing credentials. Default CORS should be no CORS unless explicitly configured.
- Do not render user-controlled object content in a same-origin iframe with console credentials available to JavaScript.
- Prefer origin separation for object preview/download, `nosniff`, CSP, strict content-type handling, and avoiding durable credentials in `localStorage`.
- Preview safety must be based on trusted content type and sandboxing, not object names or extensions such as `.pdf`.
- Console license/version-like metadata endpoints should expose only coarse public data unless authenticated, especially subject names and expiration timestamps.

### Profiling, debug, and health endpoints
- Profiling and debug endpoints are not health checks. They require admin auth, opt-in enablement, rate limiting, and safe responses.
- Do not return absolute filesystem paths or other deployment layout in unauthenticated or low-privilege responses.
- Ensure health endpoint allowlists cannot accidentally include expensive diagnostics.

### Trusted proxy and network identity
- Only honor `X-Forwarded-For` or `X-Real-IP` when the request came from a configured trusted proxy.
- Apply the same trusted-proxy rule to scheme and host derivation; direct clients must not control security-sensitive redirects through `Host`, `X-Forwarded-Host`, or `X-Forwarded-Proto`.
- Direct clients must use the socket peer address for `aws:SourceIp` and policy condition evaluation.
- Add tests for direct spoofed headers and trusted-proxy headers.

### SSE and storage invariants
- Encryption metadata is not proof that bytes were encrypted on disk.
- When touching reader/writer wrappers such as hashing, encryption, compression, or warp readers, verify wrapper order and inspect stored bytes in regression tests.
- Avoid helper shortcuts that unwrap nested readers and accidentally bypass encryption or integrity layers.

### Object Lock and retention invariants
- Object Lock state must fail closed when bucket metadata is unreadable, fabricated, or unparsable. Only a confirmed absence of Object Lock configuration may permit unprotected deletes or writes.
- Do not collapse metadata read faults, missing persisted metadata, parse failures, and genuinely absent Object Lock config into one "not configured" result.
- Retention enforcement must cover foreground deletes, batch deletes, force-delete helpers, default-retention materialization on PUT, lifecycle expiry, scanner sweeps, and all-versions expiry.

## Review Prompts

Use these prompts while reviewing a diff:

- Could a low-privileged authenticated user reach this path with the wrong action, parent, bucket, or source object?
- Does a non-HTTP protocol path call the same authorization boundary as the S3 API before touching storage?
- Does a public/default/empty config change security behavior from fail-closed to fail-open?
- Is any attacker-controlled value later used as a path, policy condition, credential identity, log field, URL, Origin, or response body?
- Does this response contain stored replication, remote target, or service credentials that need redaction or stricter authorization?
- Does any error constructor or `format!` interpolate a variable that can hold secret material, including a config parse error that echoes the raw input?
- Does an IAM export/import path expose or trust plaintext credential secrets beyond the caller's intended authority?
- Can this STS/OIDC path issue credentials without SigV4, trusted issuer validation, allowlisted redirects, or trusted-proxy host/scheme handling?
- Can a service-account or STS token omit `exp`, forge `sessionPolicy`, or use a principal-controlled key as signing authority?
- Does this outbound validation path resolve attacker-supplied hostnames and reject private, loopback, link-local, and rebound addresses at the actual connection boundary?
- Is an archive entry, object key, or policy resource normalized differently between authorization and storage?
- Is the same operation implemented in multiple paths, such as `CopyObject` vs `UploadPartCopy`, and do all paths enforce the same security contract?
- Does an explicit object version, fallback action, or plugin authorization path pass through the same action and post-authorization gates as the direct S3 path?
- Can a caller-controlled header populate a condition key that should be derived only by the server?
- Do condition tests include partially overlapping multi-value inputs for quantified operators?
- Does unreadable bucket metadata make Object Lock or retention enforcement fail closed rather than disappear?
- Does a preview or browser-surface fix preserve the original security invariant when adding alternate viewers or file-type detection?
- Does the test prove the exploit form is denied, or only that the intended form still works?
