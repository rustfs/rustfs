# Security Lens

Use `security-advisory-lessons` only for a dedicated advisory/security audit.
For an ordinary matched diff, attack these boundaries:

- Admin routes: route registration, whitelist, handler authn, and the exact
  `AdminAction` must agree. Read-only diagnostics still require admin authz.
- IAM/service accounts: treat parent, claims, keys, groups, status, and policy
  names as attacker-controlled; prove ownership/root authority before writes.
- Protocol frontends: every changed/sibling command authorizes the matching S3
  action before reaching storage.
- Secrets/signatures: use constant-time comparison, normalize public failures,
  keep RPC/root/STS keys independent, and fail closed when secrets are absent.
- RPC: bind signatures to the exact method/path and timestamp; reject replay,
  stale, malformed, truncated, and invalid-enum payloads without panic.
- Paths/object/archive entries: reject traversal, absolute/platform escapes,
  and normalization differences between authz and storage.
- Copy/multipart/presigned POST: enforce source, destination, version-aware
  actions, copy-source conditions, and every signed policy condition.
- Logging/errors: never expose credentials, tokens, expected signatures, raw
  secret-bearing input, or merged configs—including via `Debug` and parse errors.
- Untrusted serde: reject unknown fields where compatible and validate
  security-critical defaults/ranges before numeric conversion.
- SSE/browser/CORS/trusted proxy: inspect stored ciphertext and wrapper order;
  isolate user content; never reflect credentialed arbitrary origins or trust
  forwarded identity from direct clients.
- Object Lock: unreadable/fabricated/unparsable metadata fails closed across
  foreground, lifecycle, scanner, and force-delete paths.

Security findings distinguish unauthenticated compromise from a
low-privileged authenticated bypass.
