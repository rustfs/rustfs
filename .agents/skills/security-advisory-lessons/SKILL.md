---
name: security-advisory-lessons
description: Perform a dedicated RustFS security/advisory review for authn/authz, IAM, RPC trust, paths, secrets, browser isolation, encryption, Object Lock, or other security boundaries. Use only when the user requests a security/advisory review or an adversarial review explicitly escalates to the full advisory map; do not auto-load solely because code touches a sensitive path.
---

# RustFS Security Advisory Lessons

Use this skill as the deep security lens. For a normal adversarial review with a
matched security surface, the concise security reference under
`adversarial-validation` is sufficient.

## Workflow

1. Freeze the exact diff/head and identify the changed trust boundaries.
2. Read [advisory-patterns.md](references/advisory-patterns.md), then apply only
   the matching sections. Useful headings are
   auth/admin, IAM/STS/OIDC, policy/plugins, S3/copy/multipart, protocols, paths,
   secrets/logging/RPC, browser/CORS/proxy, SSE, Object Lock, and serde.
3. Trace unauthenticated, low-privilege, wrong-action/owner/bucket, malformed,
   and default-config cases. Security decisions must fail closed.
4. Require a focused negative regression test for the bypass/exploit form, not
   only the intended success path. State residual risk when a test is impractical.
5. Report proven vulnerabilities separately from defense-in-depth hardening.

When advisory currentness matters, fetch the live inventory instead of treating
the reference as a status mirror:

```bash
gh api repos/rustfs/rustfs/security-advisories --paginate \
  --jq '.[] | {ghsa_id,state,severity,summary,updated_at}'
```

Fetch an individual advisory only when the live summary indicates a new or
changed lesson.

## Finding Standard

Each finding includes severity, `file:line`, attacker prerequisites, concrete
input/path, impact, smallest safe fix, and a regression check. Do not exaggerate
unauthenticated impact when the actual issue requires authenticated low privilege.
