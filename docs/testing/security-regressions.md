# Security Advisory Regression Tests

Every fixed RustFS GitHub Security Advisory (GHSA) should map to at least one
named, discoverable regression test. The convention is: name the test (or a
helper / doc comment on the exact assertion) after the advisory so that

```bash
rg -i "ghsa|3p3x|r5qv"
```

finds the guard for any advisory, and a future fix of a still-open advisory is
forced to update its pinned test (red -> green).

> This is the lightweight inventory. sec-14 (backlog#1151) formalizes the
> written admission policy in `AGENTS.md`; keep this file as the map.

## Advisory -> test mapping

| Advisory | Class | Fix PR | Named regression tests | Layer |
| --- | --- | --- | --- | --- |
| [GHSA-3p3x-734c-h5vx](https://github.com/rustfs/rustfs/security/advisories/GHSA-3p3x-734c-h5vx) | Constant-time secret comparison on WebDAV/FTPS password login | rustfs/rustfs#4403 | `assert_ftps_ghsa_3p3x_wrong_credentials_rejected` (`crates/e2e_test/src/protocols/ftps_core.rs`); `GHSA-3p3x` auth-failure block in `test_webdav_core_operations` (`crates/e2e_test/src/protocols/webdav_core.rs`) | e2e (protocols suite) |
| [GHSA-r5qv-rc46-hv8q](https://github.com/rustfs/rustfs/security/advisories/GHSA-r5qv-rc46-hv8q) | Internode RPC authentication must fail closed | rustfs/rustfs#4402 | `ghsa_r5qv_resolve_shared_secret_rejects_default_fallback`, `ghsa_r5qv_verify_rpc_signature_fails_closed_on_missing_or_invalid_auth` (`crates/ecstore/src/cluster/rpc/http_auth.rs`) | unit |
| [GHSA-m77q-r63m-pj89](https://github.com/rustfs/rustfs/security/advisories/GHSA-m77q-r63m-pj89) | STS JWTs signed with shared root secret (intentionally unfixed) | n/a | `test_created_sts_credentials_authorize_with_session_token_claims` (`crates/iam/src/sys.rs`) — pins current behavior; sec-7 adds GHSA naming | unit |

## Where these run (CI-execution map)

Every security regression must land where CI actually runs it — a named test in
an unexecuted suite is theater. The suites split across three execution paths by
topology:

- **Unit tests** — `ghsa_r5qv_*` (`crates/ecstore`) and the GHSA-m77q STS
  pinning (`crates/iam`) run automatically in the default CI pass
  (`cargo nextest run --profile ci --all --exclude e2e_test`) — no special
  wiring. This is the CI-executed regression for the RPC fail-closed (r5qv) and
  STS-signing (m77q) advisories.
- **S3-API negative-auth e2e (e2e-smoke, PR-gated)** — the attacker-facing S3
  auth-rejection suites run on every PR via the `e2e-smoke` nextest profile
  (`.config/nextest.toml`), which each spawns its own server on a random port
  and is parallel-safe:
  - `negative_sigv4_test` — tampered/wrong-key/skewed header SigV4 (sec-1)
  - `presigned_negative_test` — expired/tampered/wrong-key presigned URLs (sec-2)
  - `admin_auth_test` — non-admin denial + root-credential lifecycle (sec-4)

  A count-floor guard (`scripts/check_security_smoke_count.sh`, floor in
  `.config/security-smoke-floor.txt`) runs in the `e2e-tests` CI job and fails if
  a rename drops any of these out of the smoke filter (infra-12 mechanism). This
  is the sec-5 wiring: those merged suites were dead weight until listed here.
- **Protocol e2e (WebDAV/FTPS constant-time login, GHSA-3p3x)** — lives in the
  `e2e_test` protocols suite (`test_protocol_core_suite`), which binds **fixed
  ports**, needs the `ftps,webdav` build features, and is `#[serial]`. Those
  three properties make it **structurally incompatible** with the random-port,
  default-feature, parallel `e2e-smoke` profile: it cannot be a filterset change,
  so sec-5 (scoped to a filterset change, global ruling G5) does not wire it.
  It runs manually today:

  ```bash
  RUSTFS_BUILD_FEATURES=ftps,webdav cargo test --package e2e_test \
    test_protocol_core_suite -- --test-threads=1 --nocapture
  ```

  **Open gap:** GHSA-3p3x's *e2e* layer has no PR-gated CI execution. A protocols
  e2e CI lane is a ci-domain concern (a new profile/job, not a filterset change);
  it is deliberately out of sec-5's boundary and left as a follow-up.

## Adding a new advisory guard

1. Reproduce the advisory's bypass form as a focused negative test.
2. Name the test (or the helper/assertion) `ghsa_<id>_*`, or attach a
   `GHSA-<id>` doc comment with the advisory URL and fix PR.
3. Add a row to the table above.
4. Land it where CI runs it (see the map above): a unit guard in the default CI
   pass; an S3-API negative-auth e2e in the `e2e-smoke` filter (add the module to
   `.config/nextest.toml` and bump `.config/security-smoke-floor.txt`); a
   fixed-port protocol e2e in the protocols suite (still manual — see the open
   gap above).
