---
name: security-advisory-lessons
description: Apply RustFS security lessons distilled from repository GitHub Security Advisories. Use when making or reviewing RustFS code changes, doing security checks, handling PR review for auth/authz, IAM, storage, RPC, logging, CORS, console/browser, encryption, policy, or endpoint changes, and when deciding which security regression tests are required.
---

# RustFS Security Advisory Lessons

Use this skill as a RustFS-specific security lens before changing or approving code. It is based on the repository advisory snapshot fetched on 2026-05-05: 23 total advisories across triage, published, and closed states.

When currentness matters, refresh the advisory inventory first:

```bash
gh api repos/rustfs/rustfs/security-advisories --paginate \
  --jq '.[] | {ghsa_id,state,severity,summary,updated_at,html_url}'
```

For the full pattern map, read [advisory-patterns.md](references/advisory-patterns.md).

## Workflow

### 1. Scope the change
- Identify touched routes, handlers, storage paths, credentials, logs, browser surfaces, CI/release code, and policy checks.
- Treat these paths as security-sensitive by default: `rustfs/src/admin/`, `rustfs/src/storage/`, `rustfs/src/auth.rs`, `rustfs/src/server/layer.rs`, `crates/iam/`, `crates/policy/`, `crates/credentials/`, `crates/ecstore/src/rpc/`, `crates/rio/`, and console preview/auth code.

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
- Do not assume admin-action `Resource` scoping constrains blast radius unless the policy engine actually enforces resources for that action.

### IAM and service accounts
- Treat imported IAM payload fields as attacker-controlled: `parent`, `claims`, `accessKey`, `secretKey`, status, policy names, and groups.
- For service account create/update/import, prove parent ownership or root/admin authority before writing credentials or claims.
- Do not let `deny_only` or "no explicit deny" become an allow decision that skips required allow checks.
- Test cross-user list/update/import flows with wrong, correct, self, parent, and root identities.

### S3 copy, multipart, and presigned POST
- Multipart copy must enforce source `GetObject` and destination `PutObject` semantics equivalent to `CopyObject`, including copy-source and policy conditions.
- Do not let `CreateMultipartUpload`, `UploadPartCopy`, `CompleteMultipartUpload`, or `AbortMultipartUpload` return success without authorization.
- Presigned POST policies are server-side contracts. Enforce `content-length-range`, key prefix, exact metadata/content-type, and all signed policy conditions.

### Paths, object keys, and filesystem access
- Never join untrusted bucket/object/RPC path strings onto filesystem roots without normalization and boundary checks.
- Reject or safely handle `..`, absolute paths, URL-encoded traversal, platform separators, empty components, and paths that canonicalize outside the intended root.
- Validate both S3 object-key paths and internode/RPC disk paths; storage helpers can bypass S3 authorization if they trust already-parsed paths.

### Secrets, default credentials, and crypto
- Do not ship hard-coded shared tokens, HMAC secrets, private keys, or production test keys.
- Defaults for internode/RPC auth must fail closed for network-reachable deployments or require explicit opt-in with loud warnings.
- License or token validation must use signatures with embedded public/verifying keys only; do not use private-key decryption as authenticity.
- Plan key rotation and key IDs when removing exposed keys.

### Logging and debug output
- Logs must never include access keys beyond safe identifiers, secret keys, session tokens, JWT claims, HMAC secrets, expected signatures, license secrets, or raw response bodies containing credentials.
- Treat `Debug` implementations, `?value` tracing, merged config dumps, and dependency-level HTTP body logging as leak surfaces.
- Add log-capture tests or targeted unit tests for redaction wrappers when changing credential structs or response bodies.

### RPC, parsing, and panic safety
- Treat all RPC payload bytes as attacker-controlled. Replace `unwrap`, `expect`, and panic-prone deserialization with typed errors.
- Malformed request tests should cover empty bytes, truncated MessagePack/protobuf, invalid enum values, stale timestamps, and invalid signatures.
- RPC authentication must be independently strong; do not depend on S3 admin credentials unless the fallback is explicit and safe.

### Browser, CORS, and console surfaces
- Do not reflect arbitrary `Origin` while also allowing credentials. Default CORS should be no CORS unless explicitly configured.
- Do not render user-controlled object content in a same-origin iframe with console credentials available to JavaScript.
- Prefer origin separation for object preview/download, `nosniff`, CSP, strict content-type handling, and avoiding durable credentials in `localStorage`.
- License/version-like metadata endpoints should expose only coarse public data unless authenticated.

### Profiling, debug, and health endpoints
- Profiling and debug endpoints are not health checks. They require admin auth, opt-in enablement, rate limiting, and safe responses.
- Do not return absolute filesystem paths or other deployment layout in unauthenticated or low-privilege responses.
- Ensure health endpoint allowlists cannot accidentally include expensive diagnostics.

### Trusted proxy and network identity
- Only honor `X-Forwarded-For` or `X-Real-IP` when the request came from a configured trusted proxy.
- Direct clients must use the socket peer address for `aws:SourceIp` and policy condition evaluation.
- Add tests for direct spoofed headers and trusted-proxy headers.

### SSE and storage invariants
- Encryption metadata is not proof that bytes were encrypted on disk.
- When touching reader/writer wrappers such as hashing, encryption, compression, or warp readers, verify wrapper order and inspect stored bytes in regression tests.
- Avoid helper shortcuts that unwrap nested readers and accidentally bypass encryption or integrity layers.

## Review Prompts

Use these prompts while reviewing a diff:

- Could a low-privileged authenticated user reach this path with the wrong action, parent, bucket, or source object?
- Does a public/default/empty config change security behavior from fail-closed to fail-open?
- Is any attacker-controlled value later used as a path, policy condition, credential identity, log field, URL, Origin, or response body?
- Is the same operation implemented in multiple paths, such as `CopyObject` vs `UploadPartCopy`, and do all paths enforce the same security contract?
- Does the test prove the exploit form is denied, or only that the intended form still works?
