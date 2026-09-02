# Security Advisory Regression Tests

**Use this when:** fixing or reviewing a GHSA, or checking that an advisory's guard actually executes in CI.
**Source of truth:** the test files named below (`rg -i ghsa` finds them); `.config/nextest.toml` for lane membership; `.github/workflows/ci.yml` and `.github/workflows/e2e-replication-nightly.yml` for execution.

Every fixed RustFS GitHub Security Advisory maps to at least one named regression test. Name the test (or the helper / doc comment on the exact assertion) after the advisory so `rg -i "ghsa|<id>"` finds the guard, and a future fix of a still-open advisory is forced to update its pinned test (red -> green).

## Advisory -> test map

| Advisory | Class | Fix | Named regression tests | Layer |
| --- | --- | --- | --- | --- |
| [GHSA-3p3x-734c-h5vx](https://github.com/rustfs/rustfs/security/advisories/GHSA-3p3x-734c-h5vx) | Constant-time secret comparison on WebDAV/FTPS password login | rustfs/rustfs#4403 | `assert_ftps_ghsa_3p3x_wrong_credentials_rejected` (`crates/e2e_test/src/protocols/ftps_core.rs`); `GHSA-3p3x` auth-failure block in `test_webdav_core_operations` (`crates/e2e_test/src/protocols/webdav_core.rs`) | e2e (`e2e-protocols`) |
| [GHSA-r5qv-rc46-hv8q](https://github.com/rustfs/rustfs/security/advisories/GHSA-r5qv-rc46-hv8q) | Internode RPC authentication must fail closed | rustfs/rustfs#4402 | `ghsa_r5qv_resolve_shared_secret_rejects_default_fallback`, `ghsa_r5qv_verify_rpc_signature_fails_closed_on_missing_or_invalid_auth` (`crates/ecstore/src/cluster/rpc/http_auth.rs`) | unit |
| [GHSA-m77q-r63m-pj89](https://github.com/rustfs/rustfs/security/advisories/GHSA-m77q-r63m-pj89) | STS JWTs signed with the shared root secret (intentionally unfixed) | n/a; tests pin the by-design behaviour and must flip red -> green when m77q is fixed | `test_ghsa_m77q_sts_session_token_signed_with_root_secret`, `test_created_sts_credentials_authorize_with_session_token_claims` (`crates/iam/src/sys.rs`); `token_signing_key` doc (`crates/iam/src/root_credentials.rs`) | unit |
| [GHSA-5354-r3w2-34m8](https://github.com/rustfs/rustfs/security/advisories/GHSA-5354-r3w2-34m8) | Service-account parent must stay within caller scope; a non-owner with `CreateServiceAccountAdminAction` could parent a service account to root | rustfs/rustfs#5141 | `ghsa_5354_non_owner_service_account_parent_confined_to_scope`, `ghsa_5354_scope_guard_matches_owner_or_self_scope_for_derived_credentials`, and the `add_service_account_parent_within_scope` invariant they pin (`rustfs/src/admin/handlers/service_account.rs`) | unit |
| [GHSA-3ppv-fx5m-m749](https://github.com/rustfs/rustfs/security/advisories/GHSA-3ppv-fx5m-m749) | Versioned reads (`get_object`, CopyObject source, UploadPartCopy source) authorize against `s3:GetObjectVersion`, not `s3:GetObject` | rustfs/rustfs#5142 | `ghsa_3ppv_versioned_read_selects_get_object_version_action` and the `versioned_read_action` helper it pins (`rustfs/src/storage/access.rs`) | unit |
| [GHSA-v9cp-qfw9-9pfp](https://github.com/rustfs/rustfs/security/advisories/GHSA-v9cp-qfw9-9pfp) | `ForAllValues:`/`ForAnyValue:` negated string operators applied negation to the aggregate instead of the per-value predicate | fixed, GHSA private-fork merge | `ghsa_v9cp_for_all_values_not_equals_partial_overlap`, `ghsa_v9cp_for_any_value_not_equals_partial_overlap` and the absent-key/positive-quantifier cases beside them (`crates/policy/tests/quantified_negation.rs`); the value set must partially overlap the policy set, since contained or disjoint sets cannot tell the quantifiers apart | crate test |
| [GHSA-6r96-hmgc-726c](https://github.com/rustfs/rustfs/security/advisories/GHSA-6r96-hmgc-726c) | Request headers must not populate server-derived IAM condition keys (`userid`, `groups`, `jwt:`/`ldap:` claims) | fixed, GHSA private-fork merge | `ghsa_6r96_identity_condition_keys_ignore_spoofed_headers`, `ghsa_6r96_claim_condition_keys_ignore_spoofed_headers`, and `test_request_headers_still_reach_conditions`, which keeps the reserved set from growing too broad (`rustfs/src/auth.rs`) | unit |
| [GHSA-x298-9x87-fvjq](https://github.com/rustfs/rustfs/security/advisories/GHSA-x298-9x87-fvjq) | Anonymous ListObjectVersions -> `s3:ListBucket` fallback must reach the same public-access gates as a direct grant | fixed, GHSA private-fork merge | `ghsa_x298_anonymous_list_object_versions_denied_when_restrict_public_buckets_enabled` (`crates/e2e_test/src/anonymous_access_test.rs`); asserts 200 before the public-access block is applied so it proves the gate, not a broken fallback | e2e (`e2e-smoke`) |
| [GHSA-g3vq-vv42-f647](https://github.com/rustfs/rustfs/security/advisories/GHSA-g3vq-vv42-f647) | FTPS `MKD` must clear the `s3:CreateBucket` authorization boundary before reaching the backend | fixed, GHSA private-fork merge | `ghsa_g3vq_mkd_denied_before_reaching_backend` (`crates/protocols/src/ftps/driver.rs`); primes `create_bucket` to succeed so the assertion distinguishes "denied at authorization" from "backend refused" | unit (`ftps` feature) |

## Where these run

| Layer | Command | Lane | Guard |
| --- | --- | --- | --- |
| Unit and crate tests (`ghsa_r5qv_*`, the m77q pins, `ghsa_5354_*`, `ghsa_3ppv_*`, `ghsa_6r96_*`, `ghsa_v9cp_*`, `ghsa_g3vq_*`) | `cargo nextest run --profile ci --all --exclude e2e_test` | every PR, `Test and Lint` (required) | none needed; the workspace pass runs every unit and crate test |
| S3-API negative-auth e2e (`negative_sigv4_test`, `presigned_negative_test`, `admin_auth_test`) | `cargo nextest run --profile e2e-smoke -p e2e_test` | every PR, `End-to-End Tests` (report-only) | `scripts/check_security_smoke_count.sh` with the floor in `.config/security-smoke-floor.txt`, run in the `e2e-tests` job; fails when a rename drops one of these modules out of the smoke filter |
| Other S3 e2e guards (`anonymous_access_test`) | `cargo nextest run --profile e2e-smoke -p e2e_test` | every PR, `End-to-End Tests` (report-only) | `scripts/check_test_wiring.py --check-profile e2e-smoke` digest |
| Protocol e2e (`protocols::test_protocol_core_suite`, GHSA-3p3x) | `RUSTFS_BUILD_FEATURES=ftps,webdav,sftp cargo nextest run -j 1 --profile e2e-protocols -p e2e_test` | nightly, `e2e-replication-nightly.yml` job `protocols-nightly`; not PR-gated | `scripts/check_test_wiring.py --check-profile e2e-protocols` digest |

Notes:

- `test_protocol_core_suite` is a single `#[tokio::test]` (not `#[serial]`) that binds fixed ports and needs the `ftps,webdav` build features; the nightly job serializes it with `-j 1`. It cannot join the random-port, default-feature `e2e-smoke` profile. Targeted local run: `crates/e2e_test/src/protocols/README.md`.
- `ghsa_g3vq_*` sits behind the `ftps` feature of `rustfs-protocols`, which is off by default for that crate alone. It still runs in the workspace pass because the `rustfs` crate defaults to `["ftps", "webdav"]` and cargo unifies features across the build; `cargo test -p rustfs-protocols` on its own skips it, so pass `--features ftps`. The `protocol-features` matrix in `ci.yml` covers only `swift` and `sftp` for the same reason.

## Adding a new advisory guard

1. Reproduce the advisory's bypass form as a focused negative test.
2. Name the test (or the helper/assertion) `ghsa_<id>_*`, or attach a `GHSA-<id>` doc comment with the advisory URL.
3. Add a row to the table above.
4. Land it where it runs (table above): a unit guard needs nothing extra; an S3 e2e guard joins the `e2e-smoke` filter in `.config/nextest.toml` with its digest updated, and a negative-auth module also bumps `.config/security-smoke-floor.txt`; a fixed-port protocol guard goes into the protocols suite (nightly lane).
