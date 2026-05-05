# RustFS Advisory Pattern Map

Snapshot source: `gh api repos/rustfs/rustfs/security-advisories --paginate` on 2026-05-05. It included 23 advisories: 8 triage, 13 published, and 2 closed.

Refresh this file when new advisories appear or when an advisory changes state materially.

## Pattern Index

### Admin authorization and route exposure

- `GHSA-pfcq-4gjr-6gjm` published: notification target endpoints accepted authenticated users but skipped admin authorization. Lesson: distinguish authn from authz; admin target CRUD must call the operation-specific admin authorization path.
- `GHSA-mm2q-qcmx-gw4w` published: `ListServiceAccount` used `UpdateServiceAccountAdminAction`, while update lacked target ownership checks. Lesson: exact action constants and ownership checks are both required; information disclosure can chain into secret rotation and takeover.
- `GHSA-vcwh-pff9-64cc` published: `ImportIam` checked `ExportIAMAction` for an import/write operation. Lesson: every admin handler must authorize the action it actually performs.
- `GHSA-jqmc-mg33-v45g` triage and `GHSA-8784-9m7f-c6p6` triage: `/profile/cpu` and `/profile/memory` were whitelisted from auth and allowed expensive diagnostics plus path disclosure. Lesson: profiling/debug endpoints need admin auth, opt-in, rate limits, and non-sensitive responses.
- `GHSA-x5xv-223c-8vm7` triage: console license metadata endpoint was public. Lesson: public metadata endpoints should be coarse or authenticated.

### IAM import, service accounts, and privilege boundaries

- `GHSA-566f-q62r-wcr8` triage: `ImportIam` accepted attacker-controlled service account `parent`, `claims`, `accessKey`, and `secretKey`, enabling persistent backdoor accounts under root. Lesson: imported IAM payloads are untrusted data and must be validated against privilege boundaries.
- `GHSA-xgr5-qc6w-vcg9` published: `deny_only=true` skipped allow checks and let restricted service accounts mint unrestricted children. Lesson: deny-only logic must never become implicit allow for privilege creation.
- `GHSA-mm2q-qcmx-gw4w` published: leaked service account access keys plus update-without-ownership formed an escalation chain. Lesson: service-account identifiers are security-sensitive because update APIs consume them.

### S3 copy, multipart, and upload policy validation

- `GHSA-mx42-j6wv-px98` published: `UploadPartCopy` missed source authorization and allowed cross-bucket object exfiltration. Lesson: multipart copy must enforce the same source and destination contract as `CopyObject`.
- `GHSA-wfxj-ph3v-7mjf` triage: `UploadPartCopy` checked source and destination independently but missed destination copy-source policy constraints. Lesson: source read and destination write checks are not sufficient when policy constrains allowed copy sources.
- `GHSA-w5fh-f8xh-5x3p` published: presigned POST accepted uploads without enforcing signed policy conditions. Lesson: parse and enforce all POST policy constraints server-side, including size, key prefix, and content type.

### Filesystem paths and object key traversal

- `GHSA-pq29-69jg-9mxc` published: RPC `read_file_stream` joined untrusted paths under a volume directory without canonical boundary checks. Lesson: `PathBuf::join` plus length checks are not path security.
- `GHSA-8r6f-hmq2-28rg` closed: object keys containing traversal sequences bypassed bucket/object authorization when mapped to filesystem paths. Lesson: reject traversal at object-key parsing and verify final storage paths remain under the expected bucket/key root.

### Secrets, defaults, and cryptographic misuse

- `GHSA-h956-rh7x-ppgj` published: gRPC used the hard-coded token `rustfs rpc` on both client and server. Lesson: source-visible shared tokens are authentication bypasses.
- `GHSA-r5qv-rc46-hv8q` triage: internode RPC HMAC secret fell back to the public default `rustfsadmin`. Lesson: RPC/internode auth must fail closed instead of silently using public defaults.
- `GHSA-923g-jp7v-f97f` triage: license verification embedded a production RSA private key and used private-key decryption as authenticity. Lesson: ship verifying/public keys only and use real signature verification.

### Sensitive logging and debug output

- `GHSA-r54g-49rx-98cr` published: STS credentials were logged at info level. Lesson: generated credentials must never be logged in plaintext.
- `GHSA-8cm2-h255-v749` triage: debug logs leaked session tokens, secret keys, JWT claims, and raw STS response bodies. Lesson: redaction must cover custom `Debug` implementations and dependency response-body logging.
- `GHSA-333v-68xh-8mmq` published: invalid RPC signature logging included the shared HMAC secret and expected signature. Lesson: error paths often leak secrets; never log raw secrets or derived authenticators.

### RPC input validation and panic safety

- `GHSA-gw2x-q739-qhcr` published: malformed gRPC `GetMetrics` payloads reached `unwrap()` on deserialization and caused remote DoS. Lesson: every network/RPC deserialization failure returns an error, not a panic.
- `GHSA-h956-rh7x-ppgj` published and `GHSA-r5qv-rc46-hv8q` triage: weak RPC auth increased reachability of otherwise internal handlers. Lesson: panic bugs become more severe when internode auth is weak or defaulted.

### Browser, CORS, and console isolation

- `GHSA-v9fg-3cr2-277j` published: object preview rendered attacker-controlled HTML in a same-origin iframe, exposing console credentials stored in `localStorage`. Lesson: user content must be origin-isolated from the console and protected with `nosniff`, CSP, and strict content-type handling.
- `GHSA-x5xv-223c-8vm7` triage: default CORS reflected arbitrary origins with credentials. Lesson: never combine reflected origins with `Access-Control-Allow-Credentials: true`; default should be fail-closed.

### Trusted proxy and source IP conditions

- `GHSA-fc6g-2gcp-2qrq` published: `aws:SourceIp` trusted client-supplied `X-Forwarded-For` or `X-Real-IP`. Lesson: forwarded IP headers are valid only behind configured trusted proxies; direct clients use socket peer IP.

### SSE and on-disk storage invariants

- `GHSA-xrrf-67jm-3c2r` closed: SSE metadata reported encryption while reader composition bypassed `EncryptReader` and stored plaintext. Lesson: test actual bytes on disk and wrapper order, not only API metadata.

## Useful Search Seeds

Use these targeted searches when a diff touches security-sensitive code:

```bash
rg -n "validate_admin_request|check_permissions|AdminAction::|deny_only|is_allowed" rustfs crates
rg -n "UploadPartCopy|upload_part_copy|CompleteMultipart|PostObject|content-length-range|starts-with" rustfs crates
rg -n "PathBuf::join|canonicalize|\\.\\.|x-forwarded-for|x-real-ip|SourceIp" rustfs crates
rg -n "DEFAULT_SECRET|DEFAULT_ACCESS|TEST_PRIVATE_KEY|rustfs rpc|RUSTFS_RPC_SECRET" rustfs crates
rg -n "debug!|trace!|info!|error!|\\?resp|\\?merged_config|session_token|secret_key" rustfs crates
rg -n "HashReader|EncryptReader|SSE|server-side encryption|Access-Control-Allow-Credentials|Origin" rustfs crates
```

## Minimum Regression Test Expectations

- Authz fixes: include unauthenticated, valid low-privilege, wrong-action, correct-action, owner, non-owner, and root/admin cases as applicable.
- IAM fixes: include import/update/list service-account cases with attacker-controlled parent, claims, access key, secret key, and policy.
- Copy/upload fixes: include cross-bucket, cross-user, source-denied, destination-denied, copy-source-condition, and multipart completion cases.
- Path fixes: include encoded traversal, absolute path, nested traversal, valid object keys that resemble traversal text but should be rejected, and canonical boundary checks.
- Logging fixes: assert redacted output for structs and response bodies that may contain credentials.
- Browser/CORS fixes: assert no credentials on reflected/default origins, correct behavior for explicit allowlists, and no same-origin script execution for previewed object content.
- SSE fixes: inspect stored bytes and verify API metadata, read-back behavior, and on-disk ciphertext together.
