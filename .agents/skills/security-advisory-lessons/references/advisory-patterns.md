# RustFS Advisory Pattern Map

This file is a lesson map, not an advisory inventory mirror. It keeps durable security patterns distilled from RustFS GitHub Security Advisories.

When current advisory state, severity, URLs, or full text matters, fetch it live:

```bash
gh api repos/rustfs/rustfs/security-advisories --paginate \
  --jq '.[] | {ghsa_id,state,severity,summary,updated_at}'
gh api repos/rustfs/rustfs/security-advisories/<GHSA_ID>
```

Update this file only when an advisory adds or changes a reusable lesson, affected surface, validation pattern, or regression-test expectation. Do not update it for state-only, URL-only, count-only, or timestamp-only changes.

## Pattern Index

### Admin authorization and route exposure

- `GHSA-pfcq-4gjr-6gjm`: notification target endpoints accepted authenticated users but skipped admin authorization. Lesson: distinguish authn from authz; admin target CRUD must call the operation-specific admin authorization path.
- `GHSA-mm2q-qcmx-gw4w`: `ListServiceAccount` used `UpdateServiceAccountAdminAction`, while update lacked target ownership checks. Lesson: exact action constants and ownership checks are both required; information disclosure can chain into secret rotation and takeover.
- `GHSA-vcwh-pff9-64cc`: `ImportIam` checked `ExportIAMAction` for an import/write operation. Lesson: every admin handler must authorize the action it actually performs.
- `GHSA-jqmc-mg33-v45g` and `GHSA-8784-9m7f-c6p6`: `/profile/cpu` and `/profile/memory` were whitelisted from auth and allowed expensive diagnostics plus path disclosure. Lesson: profiling/debug endpoints need admin auth, opt-in, rate limits, and non-sensitive responses.
- `GHSA-f5cv-v44x-2xgf`: `/rustfs/admin/v3/metrics` accepted any authenticated IAM user and skipped admin authorization. Lesson: read-only metrics and diagnostic admin endpoints still require an operation-specific admin action check.
- `GHSA-796f-j7xp-hwf4`: `/rustfs/admin/v3/list-remote-targets` checked only that credentials existed and leaked replication target credentials. Lesson: replication target reads are privileged admin operations, and stored remote credentials require strict authz plus response redaction review.
- `GHSA-xp32-gxq2-3v52`: console license metadata endpoint was public and exposed subject and expiration fields. Lesson: management metadata endpoints should require admin auth or return only coarse public status.

### IAM import, service accounts, and privilege boundaries

- `GHSA-566f-q62r-wcr8`: `ImportIam` accepted attacker-controlled service account `parent`, `claims`, `accessKey`, and `secretKey`, enabling persistent backdoor accounts under root. Lesson: imported IAM payloads are untrusted data and must be validated against privilege boundaries.
- `GHSA-xgr5-qc6w-vcg9`: `deny_only=true` skipped allow checks and let restricted service accounts mint unrestricted children. Lesson: deny-only logic must never become implicit allow for privilege creation.
- `GHSA-mm2q-qcmx-gw4w`: leaked service account access keys plus update-without-ownership formed an escalation chain. Lesson: service-account identifiers are security-sensitive because update APIs consume them.

### S3 copy, multipart, and upload policy validation

- `GHSA-mx42-j6wv-px98`: `UploadPartCopy` missed source authorization and allowed cross-bucket object exfiltration. Lesson: multipart copy must enforce the same source and destination contract as `CopyObject`.
- `GHSA-wfxj-ph3v-7mjf`: `UploadPartCopy` checked source and destination independently but missed destination copy-source policy constraints. Lesson: source read and destination write checks are not sufficient when policy constrains allowed copy sources.
- `GHSA-w5fh-f8xh-5x3p`: presigned POST accepted uploads without enforcing signed policy conditions. Lesson: parse and enforce all POST policy constraints server-side, including size, key prefix, and content type.

### Protocol frontends and IAM parity

- `GHSA-3g29-xff2-92vp`: FTP `RETR` and `SIZE`/`MDTM` read paths authenticated the user but skipped IAM before calling storage. Lesson: non-HTTP protocol frontends must enforce the same per-operation authorization as the S3 API before backend access.
- `GHSA-g3vq-vv42-f647`: FTPS `MKD` called `create_bucket` without checking `s3:CreateBucket`. Lesson: protocol command handlers need action-specific checks even when sibling handlers already authorize correctly.

### Filesystem paths and object key traversal

- `GHSA-pq29-69jg-9mxc`: RPC `read_file_stream` joined untrusted paths under a volume directory without canonical boundary checks. Lesson: `PathBuf::join` plus length checks are not path security.
- `GHSA-8r6f-hmq2-28rg`: object keys containing traversal sequences bypassed bucket/object authorization when mapped to filesystem paths. Lesson: reject traversal at object-key parsing and verify final storage paths remain under the expected bucket/key root.
- `GHSA-f4vq-9ffr-m8m3`: Snowball auto-extract accepted archive entries such as `../victim-bucket/object`, authorized the raw attacker-bucket path, then storage path cleaning crossed bucket boundaries. Lesson: archive entries become object keys and need traversal rejection plus consistent authz/storage normalization before writes.

### Secrets, defaults, and cryptographic misuse

- `GHSA-h956-rh7x-ppgj`: gRPC used the hard-coded token `rustfs rpc` on both client and server. Lesson: source-visible shared tokens are authentication bypasses.
- `GHSA-r5qv-rc46-hv8q`: internode RPC HMAC secret fell back to the public default `rustfsadmin`. Lesson: RPC/internode auth must fail closed instead of silently using public defaults.
- `GHSA-923g-jp7v-f97f`: license verification embedded a production RSA private key and used private-key decryption as authenticity. Lesson: ship verifying/public keys only and use real signature verification.

### Sensitive logging and debug output

- `GHSA-r54g-49rx-98cr`: STS credentials were logged at info level. Lesson: generated credentials must never be logged in plaintext.
- `GHSA-8cm2-h255-v749`: debug logs leaked session tokens, secret keys, JWT claims, and raw STS response bodies. Lesson: redaction must cover custom `Debug` implementations and dependency response-body logging.
- `GHSA-333v-68xh-8mmq`: invalid RPC signature logging included the shared HMAC secret and expected signature. Lesson: error paths often leak secrets; never log raw secrets or derived authenticators.

### RPC input validation and panic safety

- `GHSA-gw2x-q739-qhcr`: malformed gRPC `GetMetrics` payloads reached `unwrap()` on deserialization and caused remote DoS. Lesson: every network/RPC deserialization failure returns an error, not a panic.
- `GHSA-h956-rh7x-ppgj` and `GHSA-r5qv-rc46-hv8q`: weak RPC auth increased reachability of otherwise internal handlers. Lesson: panic bugs become more severe when internode auth is weak or defaulted.
- `GHSA-c667-rgrv-99vj`: NodeService authentication signed the service prefix instead of the concrete generated method path, so valid metadata for one RPC could be replayed to another method during the timestamp window. Lesson: RPC HMAC payloads must bind exact gRPC method path, HTTP method surrogate, timestamp, and secret.

### Browser, CORS, and console isolation

- `GHSA-v9fg-3cr2-277j`: object preview rendered attacker-controlled HTML in a same-origin iframe, exposing console credentials stored in `localStorage`. Lesson: user content must be origin-isolated from the console and protected with `nosniff`, CSP, and strict content-type handling.
- `GHSA-7gcx-wg4x-q9x6`: an incomplete preview fix reintroduced extension-based PDF detection and bypassed the sandboxed fallback for attacker-controlled content. Lesson: browser-surface fixes need regression tests for alternate viewers and file-type branches, and preview trust must come from validated content type plus sandboxing rather than object names.
- `GHSA-x5xv-223c-8vm7`: default CORS reflected arbitrary origins with credentials. Lesson: never combine reflected origins with `Access-Control-Allow-Credentials: true`; default should be fail-closed.

### Trusted proxy and source IP conditions

- `GHSA-fc6g-2gcp-2qrq`: `aws:SourceIp` trusted client-supplied `X-Forwarded-For` or `X-Real-IP`. Lesson: forwarded IP headers are valid only behind configured trusted proxies; direct clients use socket peer IP.

### SSE and on-disk storage invariants

- `GHSA-xrrf-67jm-3c2r`: SSE metadata reported encryption while reader composition bypassed `EncryptReader` and stored plaintext. Lesson: test actual bytes on disk and wrapper order, not only API metadata.

### Serde deserialization and input validation

- No `#[serde(deny_unknown_fields)]` found across the entire codebase. Lesson: all structs deserialized from untrusted input (S3 API XML/JSON, lifecycle rules, bucket policies, replication configs) should have `#[serde(deny_unknown_fields)]` to reject malformed or adversarial payloads.
- `#[serde(default)]` on security-critical fields silently accepts missing values as zero/empty. Lesson: when a field has security implications (retention days, permissions, limits), validate the deserialized value explicitly rather than relying on defaults.
- Integer fields deserialized from user input and cast with `as` (e.g., `i32 as u32`) can wrap negative values to large positives. Lesson: validate ranges before casting; use `try_into()` or clamp.
- XML config typos (e.g., `"NoncurentDays"` instead of `"NoncurrentDays"`) are silently accepted when `deny_unknown_fields` is absent. Lesson: strict deserialization prevents silent misconfiguration that could cause data loss or unexpected retention behavior.

## Useful Search Seeds

Use these targeted searches when a diff touches security-sensitive code:

```bash
rg -n "validate_admin_request|check_permissions|AdminAction::|deny_only|is_allowed" rustfs crates
rg -n "authorize_operation|FtpsDriver|SftpDriver|RETR|MKD|SIZE|MDTM|CreateBucket|GetObject|HeadObject" crates/protocols rustfs
rg -n "UploadPartCopy|upload_part_copy|CompleteMultipart|PostObject|content-length-range|starts-with" rustfs crates
rg -n "normalize_extract_entry_key|Snowball|auto-extract|PathBuf::join|canonicalize|\\.\\.|x-forwarded-for|x-real-ip|SourceIp" rustfs crates
rg -n "DEFAULT_SECRET|DEFAULT_ACCESS|TEST_PRIVATE_KEY|rustfs rpc|RUSTFS_RPC_SECRET" rustfs crates
rg -n "TONIC_RPC_PREFIX|verify_rpc_signature|check_auth|NodeServiceServer|x-rustfs-signature" rustfs crates
rg -n "debug!|trace!|info!|error!|\\?resp|\\?merged_config|session_token|secret_key" rustfs crates
rg -n "HashReader|EncryptReader|SSE|server-side encryption|Access-Control-Allow-Credentials|Origin" rustfs crates
rg -n "deny_unknown_fields|serde.default|as u32|as usize|as i32" rustfs crates
```

## Minimum Regression Test Expectations

- Authz fixes: include unauthenticated, valid low-privilege, wrong-action, correct-action, owner, non-owner, and root/admin cases as applicable.
- Protocol frontend authz fixes: include denied `RETR`, `SIZE`/`MDTM`, `MKD`, bucket probe, and sibling allowed-operation cases, and assert denied paths do not reach the storage backend.
- IAM fixes: include import/update/list service-account cases with attacker-controlled parent, claims, access key, secret key, and policy.
- Copy/upload fixes: include cross-bucket, cross-user, source-denied, destination-denied, copy-source-condition, and multipart completion cases.
- Path fixes: include encoded traversal, absolute path, nested traversal, archive entries with `..`, valid object keys that resemble traversal text but should be rejected, and canonical bucket/prefix boundary checks.
- Logging fixes: assert redacted output for structs and response bodies that may contain credentials.
- RPC auth fixes: include captured metadata replay across two concrete methods, stale timestamps, wrong path, wrong method surrogate, wrong secret, and valid same-method calls.
- Browser/CORS fixes: assert no credentials on reflected/default origins, correct behavior for explicit allowlists, and no same-origin script execution for previewed object content.
- SSE fixes: inspect stored bytes and verify API metadata, read-back behavior, and on-disk ciphertext together.
