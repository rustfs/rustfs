# Cryptographic compliance positioning

**Use this when:** writing README, CHANGELOG, release notes, marketing, RFP, or security-questionnaire text that touches FIPS, or reasoning about the `rustfs-crypto` `fips` feature and algorithm deprecation.
**Source of truth:** `scripts/check_fips_wording.sh` (the enforced guard); `crates/crypto/Cargo.toml` (`fips` feature); `crates/crypto/src/encdec/id.rs` (`ID` algorithm bytes); `rustfs/src/startup_runtime_hooks.rs` (`install_default_crypto_provider`).

This document records where RustFS stands on cryptographic module validation and what may and may not be said about it, so the question is answered once, from the code. For where master key material lives per backend, see [KMS backend security properties](kms-backend-security.md).

## Status: not FIPS 140-3 validated

**RustFS is not FIPS 140-3 (or 140-2) validated, and no component it links runs as a validated cryptographic module.** There is no CMVP certificate covering RustFS or the libraries it uses in the shipped configuration. This is a deliberate position and not a statement about algorithm strength: the algorithms in use are standard, well-reviewed AEADs. Validation is a property of a specific module build, its documented boundary, and a certificate — none of which RustFS has or pursues.

### What the process actually links

| Layer | Where | Implementation | Primitives | Validated module |
| --- | --- | --- | --- | --- |
| TLS (S3 server, internode, outbound clients) | Process-wide default provider installed by `install_default_crypto_provider` in `rustfs/src/startup_runtime_hooks.rs` | `rustls` with the `aws-lc-rs` provider | TLS 1.2/1.3 suites, `prefer-post-quantum` hybrid key exchange | No — the ordinary `aws-lc-rs` build, not the `aws-lc-fips-sys`-backed FIPS variant |
| Object data path AEAD (SSE) | `crates/kms/src/encryption/ciphers.rs`, `crates/rio/src/encrypt_reader.rs`, `crates/rio-v2/src/encrypt_reader.rs` | RustCrypto `aes-gcm`, `chacha20poly1305` | AES-256-GCM, ChaCha20-Poly1305 | No |
| DEK wrapping | `crates/kms/src/encryption/dek.rs` | RustCrypto `aes-gcm` | AES-256-GCM | No |
| Local KMS backend master key | `crates/kms/src/backends/local.rs` | RustCrypto `argon2`, `aes-gcm` | Argon2id KDF, AES-256-GCM | No |
| Config and IAM blobs at rest | `crates/crypto/src/encdec/` (`rustfs-crypto`) | RustCrypto `pbkdf2`/`argon2`, `aes-gcm`, `chacha20poly1305`, `sha2` | see [the `fips` feature](#the-rustfs-crypto-fips-feature-what-it-actually-does) | No |
| JWT signing and verification | `jsonwebtoken` with the `aws_lc_rs` feature (`crates/crypto`, `crates/iam`, `crates/policy`) | AWS-LC through `aws-lc-rs` | Non-FIPS build | No |

Two consequences are commonly assumed the other way:

- **AWS-LC being present does not imply FIPS.** `aws-lc-rs` has a FIPS variant; the workspace does not enable it anywhere.
- **The data path never touches AWS-LC.** Every byte of object plaintext is encrypted by RustCrypto software implementations. Swapping the TLS provider would not change that; see route 1 below.

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

> RustFS encrypts object data with AES-256-GCM and supports ChaCha20-Poly1305. These are FIPS-approved algorithms, but the implementations are not FIPS 140-3 validated cryptographic modules and RustFS makes no FIPS validation claim.

### Guard

`scripts/check_fips_wording.sh` greps `README.md` and `CHANGELOG.md` for the banned phrases above, and separately rejects any wording under `crates/kms` that describes the Vault KV2 backend as wrapping key material through Vault's Transit engine (`KmsBackend::VaultKv2` stores RustFS-wrapped material in KV v2 and never calls Transit; use `VaultTransit` when cryptographic isolation is the requirement). This document is intentionally outside the scan: it needs the terminology to define the policy.

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
| 1. Adopt the `aws-lc-rs` FIPS variant (`aws-lc-fips-sys`) | The TLS provider swap is the small part. Every AEAD call in the data path (`crates/kms/src/encryption/`, `crates/rio*/src/encrypt_reader.rs`, `crates/kms/src/backends/local.rs`, `crates/crypto/src/encdec/`) would be re-implemented against `aws-lc-rs` primitives; Argon2id (no FIPS status) and ChaCha20-Poly1305 would need read-only compatibility stories. Build needs CMake, a C toolchain, and Go, on a narrower target set — the static musl release already hits AWS-LC's `getentropy` abort on kernels older than 3.17, and the FIPS variant constrains the matrix strictly harder | The accurate claim becomes "cryptographic operations are performed by a FIPS 140-3 validated module", not "RustFS is FIPS validated"; a product-level claim additionally needs a documented boundary, approved-mode enforcement, self-tests, zeroization, and entropy documentation | Heavy; re-opens an open platform-support problem. Justified only by a named customer or regulatory commitment |
| 2. Let an externally validated KMS carry key operations | Mostly built: the `VaultTransit` backend never lets key-encryption key material leave Vault, so master key generation, wrapping, unwrapping, and rotation happen inside whatever module Vault's seal/HSM is validated against. Remaining work is configuration guidance and a supported-deployment statement | A defensible partial answer to "where do keys live and who validated that". The object data path stays RustCrypto and TLS stays non-FIPS AWS-LC: "key management operations are performed by an externally validated module; the object data path is not validated" | Nearest partial step, no code rewrite. Point customers here when the requirement is key custody rather than a certificate covering the storage layer |
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
