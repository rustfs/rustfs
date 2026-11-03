# Cryptographic compliance positioning

**Use this when:** writing README, CHANGELOG, release notes, marketing, RFP, or security-questionnaire text that touches FIPS, or reasoning about the `rustfs` / `rustfs-tls-runtime` / `rustfs-crypto` `fips` features and algorithm deprecation.
**Source of truth:** `scripts/check_fips_wording.sh` (the enforced guard); the `fips` features in `rustfs/Cargo.toml` and the affected library manifests; `crates/tls-runtime/src/provider.rs` (provider policy); `crates/crypto/Cargo.toml` (`fips` feature); `crates/crypto/src/encdec/id.rs` (`ID` algorithm bytes).

This document records where RustFS stands on cryptographic module validation and what may and may not be said about it, so the question is answered once, from the code. For where master key material lives per backend, see [KMS backend security properties](kms-backend-security.md).

## Status: not FIPS 140-3 validated

**RustFS is not FIPS 140-3 (or 140-2) validated as a product, and in the default build no component it links runs as a validated cryptographic module.** There is no CMVP certificate covering RustFS itself, or the libraries it uses in the default shipped configuration. An opt-in `--features fips` build flag on `rustfs` enables the `aws-lc-fips-sys` backend in the applicable dependency graph; it does not establish that every TLS, X.509, or JWT operation uses an approved service. Object data-path and local KMS operations remain on unvalidated RustCrypto software implementations. Validation is a property of a specific module build, its documented boundary, and a certificate — none of which RustFS as a whole has or pursues.

### What the process actually links

| Layer | Where | Implementation | Primitives | Validated module |
| --- | --- | --- | --- | --- |
| TLS using the process-wide rustls provider | Provider installed or checked by `install_default_crypto_provider` in `rustfs/src/startup_runtime_hooks.rs` | `rustls` with the process-wide provider (AWS-LC is installed when none exists) | TLS 1.2/1.3 suites, `prefer-post-quantum` hybrid key exchange (restricted to FIPS-approved suites and groups when `--features fips` is enabled) | No in the default build (`aws-lc-sys`); opt-in `--features fips` enables `aws-lc-fips-sys` and checks the installed provider at startup; module validation depends on the deployment |
| Internode RPC HMAC authentication | `crates/ecstore/src/cluster/rpc/http_auth.rs` via `rustfs_utils::crypto::HmacSha256` | RustCrypto `hmac` + `sha2` in the default build; `aws-lc-rs` when `fips` is enabled | HMAC-SHA256 (`x-rustfs-signature`, `x-rustfs-rpc-signature-v2`, `x-rustfs-rpc-signature-v3`) | No in the default build; opt-in `--features fips` enables `rustfs-utils/fips` (`aws-lc-fips-sys`) |
| Object data path AEAD (SSE) | `crates/kms/src/encryption/ciphers.rs`, `crates/rio/src/encrypt_reader.rs`, `crates/rio-v2/src/encrypt_reader.rs` | RustCrypto `aes-gcm`, `chacha20poly1305` | AES-256-GCM, ChaCha20-Poly1305 | No |
| DEK wrapping | `crates/kms/src/encryption/dek.rs` | RustCrypto `aes-gcm` | AES-256-GCM | No |
| Local KMS backend master key | `crates/kms/src/backends/local.rs` | RustCrypto `argon2`, `aes-gcm` | Argon2id KDF, AES-256-GCM | No |
| Config and IAM blobs at rest | `crates/crypto/src/encdec/` (`rustfs-crypto`) | RustCrypto `pbkdf2`/`argon2`, `aes-gcm`, `chacha20poly1305`, `sha2` | see [the `rustfs-crypto` `fips` feature](#the-rustfs-crypto-fips-feature-what-it-actually-does) | No |
| JWT signing and verification | `jsonwebtoken` with the `aws_lc_rs` feature (`crates/crypto`, `crates/iam`, `crates/policy`) | AWS-LC through `aws-lc-rs` | Non-FIPS `aws-lc-sys` in the standalone default build; the FIPS backend can be enabled by Cargo feature unification | No in the standalone default build; linking the FIPS backend does not establish validation of each operation |

Two consequences are commonly assumed the other way:

- **AWS-LC being present does not imply FIPS in the default build.** `aws-lc-rs` has a FIPS variant (`aws-lc-fips-sys`); default builds do not enable it. Building `rustfs` with `--features fips` enables `rustls/fips`, `tokio-rustls/fips`, and `rustfs-tls-runtime/fips`, which enables `aws-lc-fips-sys` and installs or checks the process-wide rustls provider (see [below](#the-rustfs-and-rustfs-tls-runtime-fips-feature)).
- **The object encryption data path uses RustCrypto.** When RustFS encrypts object data, it uses RustCrypto software implementations, even when `rustfs` is built with `--features fips`. Enabling the TLS FIPS provider does not change the object encryption data path; see route 1 below.

## Terminology red lines for external material

These rules apply to the README, CHANGELOG, release notes, marketing pages, sales decks, RFP responses, and security questionnaires. Claiming validation RustFS does not have is a false statement of fact with regulatory and contractual consequences.

### Never use

- "FIPS validated", "FIPS certified", "FIPS 140-2/140-3 compliant", "FIPS compliant"
- "FIPS mode", "runs in FIPS mode", "FIPS-enabled"
- "NIST certified", "NIST approved", "CMVP certificate", any certificate number
- "meets FIPS requirements", "satisfies FIPS", or any phrasing a reader would reasonably read as validation
- The internal Cargo feature name `fips` as a product capability. It is a build-time algorithm selector (see below).

### Permitted, with the qualifier attached

- **"FIPS-preferred algorithms"** — only when accompanied, in the same paragraph or table cell, by an explicit non-validation statement. Defined meaning: *the default algorithm selection is restricted to algorithms on the FIPS 140-3 approved list, implemented by software that has not been validated as a cryptographic module.*
- Naming specific primitives factually ("AES-256-GCM", "ChaCha20-Poly1305", "PBKDF2-HMAC-SHA256") is always fine; algorithm names carry no validation claim.

Boilerplate when the topic cannot be avoided:

> RustFS supports AES-256-GCM and ChaCha20-Poly1305 for object encryption. AES-256-GCM can be a FIPS-approved algorithm when used under the applicable conditions; ChaCha20-Poly1305 is not FIPS-approved. These implementations are not FIPS 140-3 validated cryptographic modules and RustFS makes no FIPS validation claim.

### Guard

`scripts/check_fips_wording.sh` greps `README.md` and `CHANGELOG.md` for the banned phrases above, and separately rejects any wording under `crates/kms` that describes the Vault KV2 backend as wrapping key material through Vault's Transit engine (`KmsBackend::VaultKv2` stores RustFS-wrapped material in KV v2 and never calls Transit; use `VaultTransit` when cryptographic isolation is the requirement). This document is intentionally outside the scan: it needs the terminology to define the policy.

## The `rustfs` and `rustfs-tls-runtime` `fips` feature

`rustfs/Cargo.toml` and the TLS runtime, protocols, targets, S3 client, and ECStore library manifests define opt-in `fips` features (disabled by default) for the `aws-lc-fips-sys` / `rustls` path. The RustFS feature propagates the policy to these libraries; when enabling a protocol library independently, also enable the required protocol feature.

Cargo's `--all-features` also enables `fips`, including for these individual libraries. Such builds inherit the FIPS backend's target and native-tool requirements; select supported feature combinations deliberately rather than assuming every all-features build is supported.

```bash
cargo build --release --locked -p rustfs --bin rustfs --features fips
```

- **What it enables:** Activates `rustls/fips` and `tokio-rustls/fips`, which pull in `aws-lc-rs/fips` (`aws-lc-fips-sys`) and `rustls-webpki/aws-lc-rs-fips`. Cargo unifies features for the same dependency version in a build graph; consumers of that `aws-lc-rs` instance use its FIPS backend, but this does not establish approval for every operation or cover independent dependency versions.
- **Runtime provider installation:** `rustfs-tls-runtime` installs or reuses the process-wide provider and checks the actual provider in FIPS builds. RustFS performs this check before observability initialization in standalone and embedded startup. FTPS, WebDAV, transition clients, and relevant notification TLS clients also invoke the shared policy before using the process-wide provider. An incompatible host provider causes a configuration error. The check does not inspect clients that use their own provider or a different TLS implementation.
- **Build requirements:** The FIPS backend requires a C/C++ toolchain, CMake, and Go; targets without suitable pre-generated bindings also require bindgen and Clang/`libclang`. Check the locked dependency version and target platform for their exact requirements.
- **What it does not change:** It does not alter the object data-path AEAD (`crates/kms`, `crates/rio`, `crates/rio-v2`) or `rustfs-crypto` config/IAM blob encryption, which continue to use pure-Rust `RustCrypto` crates.

The RustFS server TLS configuration uses the process-wide rustls provider. The Kafka notification client and remote replication SDK explicitly select AWS-LC, so their providers are independent of the process default; for the locked rustls version, the unified `rustls/fips` feature restricts that provider's suites and groups. ECStore's insecure replication-client path still uses the process default: an application using ECStore independently must directly depend on `rustfs-tls-runtime` with its `fips` feature and call `rustfs_tls_runtime::ensure_crypto_provider()` before constructing that client when it requires the FIPS policy. Skipping TLS certificate verification does not authenticate the remote target, regardless of provider selection. Feature unification applies only to the same dependency version in a build graph; independent builds and third-party TLS stacks require separate verification. No complete outbound TLS or platform validation matrix is asserted here.

NATS can upgrade a connection to TLS based on the server's response, so the targets library checks the process provider before connecting in a FIPS build, including for a plaintext-looking NATS URL. Webhook targets construct a TLS-capable HTTP client even for an HTTP endpoint and also check the provider when built with `fips`. An incompatible host provider therefore prevents these initializations even if a particular connection would not use TLS.

A provider check or linked FIPS backend does not establish CMVP validation for a deployment or approval of every operation. Verify the specific module version, certificate status, security policy, build, operating environment, and approved services against the [CMVP validated modules list](https://csrc.nist.gov/projects/cryptographic-module-validation-program/validated-modules). The feature is opt-in; before enabling it, test client algorithms, certificates, TLS versions, rolling upgrades, and rollback in the target environment.

## The `rustfs-crypto` `fips` feature: what it actually does

`crates/crypto/Cargo.toml` declares `default = ["crypto", "fips"]`, so the feature is on in every normal build. Its entire effect is **which algorithm the write path selects**; the implementation is RustCrypto either way.

| `fips` | Algorithm ID written | KDF | AEAD |
| --- | --- | --- | --- |
| enabled (default) | `ID::Pbkdf2AESGCM` (`0x02`) | PBKDF2-HMAC-SHA256, 8192 iterations | AES-256-GCM |
| disabled | `ID::Argon2idAESGCM` (`0x00`) or `ID::Argon2idChaCHa20Poly1305` (`0x01`), chosen at runtime by CPU AES support | Argon2id (64 MiB, t=1, p=4) | AES-256-GCM or ChaCha20-Poly1305 |

Selection sites are `crates/crypto/src/encdec/encrypt.rs` and `crates/crypto/src/encdec/stream_io.rs`; identifiers and KDF parameters live in `crates/crypto/src/encdec/id.rs`.

- **It affects writes only.** The decrypt path accepts all three identifiers unconditionally, and every ciphertext carries its identifier byte, so toggling the feature never orphans existing data.
- **It does not select a different implementation.** Both branches call RustCrypto; the feature cannot move RustFS toward or away from validation.
- **It is a trade-off, not an upgrade.** PBKDF2-HMAC-SHA256 at 8192 iterations is a work factor well below current password-hashing guidance, whereas the non-FIPS branch uses memory-hard Argon2id. Against an offline attack on the passphrase of a stolen config or IAM blob, the default branch is the weaker of the two.

**Known naming debt.** The feature name `fips` states a compliance property the feature does not provide, and `rustfs-crypto` is published. The intended fix is to introduce `fips-preferred-algs` as the real name, keep `fips` as a deprecated alias for one release cycle, and revisit the PBKDF2 iteration count at the same time; none of this has been done, and no ciphertext format changes when it is.

## Routes to a stronger position, and what each costs

| Route | Scope | What it buys | Verdict |
| --- | --- | --- | --- |
| 1. Adopt the `aws-lc-rs` FIPS variant (`aws-lc-fips-sys`) across the data path | The opt-in `-p rustfs --features fips` flag enables the FIPS backend for the relevant dependency graph; individual TLS, X.509, and JWT services still require verification. Extending `aws-lc-fips-sys` to the data path would require re-implementing every AEAD/KDF call in `crates/kms/src/encryption/`, `crates/rio*/src/encrypt_reader.rs`, `crates/kms/src/backends/local.rs`, and `crates/crypto/src/encdec/` against `aws-lc-rs` primitives; Argon2id (no FIPS status) and ChaCha20-Poly1305 would need read-only compatibility stories. Build needs a C toolchain, CMake, Go, and target-dependent bindings tooling, on a narrower target set — the static musl release already hits AWS-LC's `getentropy` abort on kernels older than 3.17, and the FIPS variant constrains the matrix strictly harder | A claim that particular operations use a validated module requires verified module and deployment boundaries, certificate status, approved-mode operation, and service coverage; it does not establish product-level validation | Data-path rewrite remains heavy and re-opens platform-support constraints; the FIPS backend is opt-in via `--features fips` |
| 2. Let an externally validated KMS carry key operations | Mostly built: the `VaultTransit` backend never lets key-encryption key material leave Vault, so master key generation, wrapping, unwrapping, and rotation happen inside whatever module Vault's seal/HSM is validated against. Remaining work is configuration guidance and a supported-deployment statement | A defensible partial answer to "where do keys live and who validated that". The object data path stays RustCrypto; TLS backend selection remains a separate decision: "key management operations are performed by an externally validated module; the object data path is not validated" | Nearest partial step, no code rewrite. Point customers here when the requirement is key custody rather than a certificate covering the storage layer |
| 3. Make no validation claim (current default) | This document plus the grep guard | Nothing further | The current decision. FIPS 140-3 validation is not a roadmap target; adjacent items (PKCS#11, KMIP, BYOK, signing keys) are deferred for lack of demand and because HSM-dependent paths cannot be exercised in CI |

## Algorithm disablement and migration policy

Retiring an algorithm from a storage system is a data migration with a code change at each end. This section fixes the sequence so that no deprecation removes a decrypt path while data still depends on it.

### Every persisted artifact is self-describing

- `rustfs-crypto` blobs carry the `ID` byte (`crates/crypto/src/encdec/id.rs`) immediately after the salt.
- KMS ciphers are selected from the recorded `EncryptionAlgorithm` (`crates/kms/src/types.rs`).
- DEK envelopes record which master key version wrapped them in `DataKeyEnvelope::master_key_version` (`crates/kms/src/encryption/dek.rs`).

For any stored object it is therefore decidable, from the object alone, which algorithm and key version it needs.

### Deprecation classes

1. **Write-disabled, read-supported.** New writes select a replacement; existing data decrypts unchanged. The only cheap, reversible step.
2. **Read-deprecated.** Reads still work but are counted and warned on, so the remaining population is measurable.
3. **Read-removed.** The decrypt path is deleted. Permitted only once the remaining population is provably zero.

### Sequencing rules

- Never advance to read-removed on the strength of an argument that data "should have been" migrated. Removal requires evidence that nothing references the algorithm, not an elapsed-time policy.
- A change to default algorithm selection is a compatibility event: it changes what new nodes write, which matters in a mixed-version cluster. Record it in the release notes and the crate's feature documentation, and check it against the [mixed-version constraints](kms-backend-security.md#mixed-version-clusters-during-a-rolling-upgrade).
- Roll out write-disablement before the corresponding read change, and let the cluster fully converge in between.

### Known gap

The bulk rekey sweep (`POST /rustfs/admin/v3/kms/keys/rekey`, see [`kms-bulk-rekey-contract.md`](../architecture/kms-bulk-rekey-contract.md)) migrates stored DEK envelopes off superseded **master key versions** without touching object bodies. It does not re-encrypt object data, so migrating off a data-encryption **algorithm** has no supported path — treat every algorithm that has ever been written as permanently read-required, and confine algorithm deprecation to step 1.
