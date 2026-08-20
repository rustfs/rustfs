# Cryptographic compliance positioning

This document records where RustFS stands on cryptographic module validation, what may and may not be said about it in external material, and what each possible route to a stronger position would actually cost. It exists so that the question is answered once, from the code, instead of being re-litigated from assumptions about crate names and feature flags.

For where master key material lives per backend and how rotation retention works, see [KMS backend security properties](kms-backend-security.md).

## Status: not FIPS 140-3 validated

**RustFS is not FIPS 140-3 (or 140-2) validated, and no component it links is running as a validated cryptographic module.** There is no CMVP certificate covering RustFS or the libraries it uses in the shipped configuration.

This is a deliberate position, not an oversight. It is also not a statement about algorithm strength: the algorithms in use are standard, well-reviewed AEADs. Validation is a property of a specific module build, its documented boundary, and a certificate — none of which RustFS has or currently pursues.

### What the process actually links

The table below is the audited inventory as of this document's writing. "Validated module" asks only whether the code performing the operation is a FIPS-validated cryptographic module; the answer is uniformly no.

| Layer | Where | Implementation | Primitives | Validated module |
| --- | --- | --- | --- | --- |
| TLS (S3 server, internode, outbound clients) | Process-wide default provider installed by `install_default_crypto_provider` in `rustfs/src/startup_runtime_hooks.rs` | `rustls` with the `aws-lc-rs` provider | TLS 1.2/1.3 suites, `prefer-post-quantum` hybrid key exchange | No — this is the ordinary `aws-lc-rs` build, not the `aws-lc-fips-sys`-backed FIPS variant |
| Object data path AEAD (SSE) | `crates/kms/src/encryption/ciphers.rs`, `crates/rio/src/encrypt_reader.rs`, `crates/rio-v2/src/encrypt_reader.rs` | RustCrypto `aes-gcm`, `chacha20poly1305` | AES-256-GCM, ChaCha20-Poly1305 | No |
| DEK wrapping | `crates/kms/src/encryption/dek.rs` | RustCrypto `aes-gcm` | AES-256-GCM | No |
| Local KMS backend master key | `crates/kms/src/backends/local.rs` | RustCrypto `argon2`, `aes-gcm` | Argon2id KDF, AES-256-GCM | No |
| Config and IAM blobs at rest | `crates/crypto/src/encdec/` (`rustfs-crypto`) | RustCrypto `pbkdf2`/`argon2`, `aes-gcm`, `chacha20poly1305`, `sha2` | see [the `fips` feature](#the-rustfs-crypto-fips-feature-what-it-actually-does) | No |
| JWT signing and verification | `jsonwebtoken` with the `aws_lc_rs` feature (`crates/crypto`, `crates/iam`, `crates/policy`) | AWS-LC through `aws-lc-rs` | Non-FIPS build | No |

Two consequences follow directly from the table and are worth stating explicitly, because both are commonly assumed the other way:

- **AWS-LC being present does not imply FIPS.** `aws-lc-rs` has a FIPS variant; the workspace does not enable it. Every `aws-lc-rs` dependency in the workspace is the default, non-FIPS build.
- **The data path never touches AWS-LC.** Every byte of object plaintext is encrypted by RustCrypto software implementations. Swapping the TLS provider would not change that; see [route 1](#route-1-adopt-the-aws-lc-rs-fips-variant) for what would.

## Terminology red lines for external material

These rules apply to the README, CHANGELOG, release notes, marketing pages, sales decks, RFP responses, and security questionnaires. Claiming validation RustFS does not have is a false statement of fact with regulatory and contractual consequences, not a marketing overreach.

### Never use

- "FIPS validated", "FIPS certified", "FIPS 140-2/140-3 compliant", "FIPS compliant"
- "FIPS mode", "runs in FIPS mode", "FIPS-enabled"
- "NIST certified", "NIST approved", "CMVP certificate", any certificate number
- "meets FIPS requirements", "satisfies FIPS", or any phrasing a reader would reasonably read as validation
- The internal Cargo feature name `fips` as a product capability. It is a build-time algorithm selector (see below), and surfacing it as a feature name invites exactly the misreading this section exists to prevent.

### Permitted, with the qualifier attached

- **"FIPS-preferred algorithms"** — permitted only when accompanied, in the same paragraph or table cell, by an explicit non-validation statement. The defined meaning is: *the default algorithm selection is restricted to algorithms on the FIPS 140-3 approved list, implemented by software that has not been validated as a cryptographic module.*
- Naming specific primitives factually ("AES-256-GCM", "ChaCha20-Poly1305", "PBKDF2-HMAC-SHA256") is always fine. Algorithm names carry no validation claim.

Suggested boilerplate when the topic cannot be avoided:

> RustFS encrypts object data with AES-256-GCM and supports ChaCha20-Poly1305. These are FIPS-approved algorithms, but the implementations are not FIPS 140-3 validated cryptographic modules and RustFS makes no FIPS validation claim.

### Guard

`README.md` and `CHANGELOG.md` currently contain no FIPS-related wording; `scripts/check_fips_wording.sh` is the grep guard for that public baseline. Any future occurrence of the banned strings in either file should be treated as a defect and either removed or brought under the qualifier rule above. This document intentionally contains the terminology needed to define the policy and is not part of that narrow outward-material scan.

The same script carries a second block for the adjacent over-claim: no file under `crates/kms` may describe the Vault KV2 backend as wrapping key material through Vault's Transit engine. `KmsBackend::VaultKv2` stores RustFS-wrapped key material in Vault's KV v2 engine and never calls Transit, so that wording would tell an operator their key material is cryptographically isolated inside Vault when it is not. Use the `VaultTransit` backend when that isolation is the requirement.

## The `rustfs-crypto` `fips` feature: what it actually does

`crates/crypto/Cargo.toml` declares `default = ["crypto", "fips"]`, so the feature is on in every normal build. Its entire effect is **which algorithm the write path selects**; the implementation is RustCrypto either way.

| `fips` | Algorithm ID written | KDF | AEAD |
| --- | --- | --- | --- |
| enabled (default) | `ID::Pbkdf2AESGCM` (`0x02`) | PBKDF2-HMAC-SHA256, 8192 iterations | AES-256-GCM |
| disabled | `ID::Argon2idAESGCM` (`0x00`) or `ID::Argon2idChaCHa20Poly1305` (`0x01`), chosen at runtime by CPU AES support | Argon2id (64 MiB, t=1, p=4) | AES-256-GCM or ChaCha20-Poly1305 |

The selection sites are `crates/crypto/src/encdec/encrypt.rs` and `crates/crypto/src/encdec/stream_io.rs`; the algorithm identifiers and their KDF parameters live in `crates/crypto/src/encdec/id.rs`.

Three properties matter for anyone reasoning about this feature:

- **It affects writes only.** The decrypt path in `crates/crypto/src/encdec/id.rs` accepts all three identifiers unconditionally, and every ciphertext carries its identifier byte. Toggling the feature therefore never orphans existing data in either direction.
- **It does not select a different implementation.** Both branches call RustCrypto. There is no validated module on either side of the switch, so the feature cannot move RustFS toward or away from validation.
- **It is a trade-off, not an upgrade.** The FIPS-preferred branch uses PBKDF2-HMAC-SHA256 at 8192 iterations, a work factor well below current password-hashing guidance, whereas the non-FIPS branch uses memory-hard Argon2id. Against an attacker who has obtained an encrypted config or IAM blob and is attacking the passphrase offline, the default branch is the weaker of the two. Enabling the feature buys approved-algorithm alignment, not more resistance.

### Rename recommendation

The name `fips` states a compliance property the feature does not provide, and `rustfs-crypto` is published, so the name is visible to downstream consumers. Recommended direction:

1. Introduce `fips-preferred-algs` as the real feature name, carrying the current behavior.
2. Redefine `fips = ["fips-preferred-algs"]` so existing consumers keep building, and mark it deprecated in the crate documentation with a pointer to this document.
3. Drop the `fips` alias after one release cycle.
4. While renaming, raise the PBKDF2 iteration count or document the trade-off above at the feature definition, so the choice is explicit rather than inherited.

This is a naming and documentation change only; no ciphertext format changes, because the identifier bytes stay as they are.

## Routes to a stronger position, and what each costs

### Route 1: adopt the `aws-lc-rs` FIPS variant

Switch the whole process to `aws-lc-rs`'s FIPS build (backed by `aws-lc-fips-sys`) so cryptographic operations run inside a validated module boundary.

**Scope.** The TLS provider swap is the small part — one feature flag plus the provider install sites. The substantial work is the data path: every AEAD call in `crates/kms/src/encryption/ciphers.rs`, `crates/kms/src/encryption/dek.rs`, `crates/rio/src/encrypt_reader.rs`, `crates/rio-v2/src/encrypt_reader.rs`, `crates/kms/src/backends/local.rs`, and `crates/crypto/src/encdec/` would have to be re-implemented against `aws-lc-rs` primitives. Anything the validated module does not expose has to be dropped or moved out of the boundary: Argon2id has no FIPS status, so the Local backend's KDF and the non-FIPS branch of `rustfs-crypto` would need a compatibility story (read-only support for existing records, PBKDF2 for new ones), and ChaCha20-Poly1305 would become non-approved for new writes.

**Build and platform cost.** `aws-lc-fips-sys` builds a pinned, validated source release and needs CMake, a C toolchain, and Go at build time; it supports a narrower target set than the ordinary crate. The platform matrix cost of plain AWS-LC is already documented and non-hypothetical: rustfs/backlog#883 records that the static musl release build compiles AWS-LC's `getentropy` entropy backend, which aborts on Linux kernels older than 3.17 (the Synology class of device), and that upstream considers this by design with no plan to fix it. The FIPS variant constrains the buildable matrix strictly harder than that, and pins upgrades to whatever the certified source revision allows.

**What it would and would not buy.** Linking the validated module makes the accurate claim "cryptographic operations are performed by a FIPS 140-3 validated module", not "RustFS is FIPS validated". A product-level claim additionally requires a documented module boundary, approved-mode enforcement, power-on self-tests, key zeroization, and entropy-source documentation, plus the operational procedures to keep them true across releases.

**Verdict.** Heavy, and it re-opens a platform-support question that is already an open problem. Justified only by a concrete customer or regulatory commitment that names FIPS as a requirement.

### Route 2: let an externally validated KMS carry key operations

Keep RustFS as-is and place key management inside someone else's validated boundary: the Vault Transit backend against a Vault deployment whose seal/HSM is validated, or an equivalent managed KMS.

**Scope.** Mostly already built. The Transit backend (`VaultTransit`) never lets key-encryption key material leave Vault; RustFS only ever holds Transit ciphertext. What remains is configuration guidance, a supported-deployment statement, and the operational documentation that says which parts of the system are covered.

**What it buys.** Master key generation, wrapping, unwrapping, and rotation happen inside the external module. That is a real, defensible partial answer to "where do keys live and who validated that": it covers the key operations, which is often the part an auditor actually asks about.

**What it does not buy.** The object data path is untouched. DEKs are used for bulk AEAD by RustCrypto inside the RustFS process, and TLS still runs the non-FIPS AWS-LC build. The honest formulation is "key management operations are performed by an externally validated module; the object data path is not validated".

**Verdict.** The nearest partial step, with no code rewrite and no platform-matrix risk. This is the route to point customers at when the requirement is about key custody rather than about a certificate covering the storage layer.

### Route 3: make no validation claim (current default)

Document the position, hold the terminology line, and revisit only when a requirement with a name attached shows up.

**Cost.** This document plus the grep guard. Nothing else.

**Verdict.** The current decision. FIPS 140-3 validation is explicitly not a roadmap target, and adjacent items (PKCS#11, KMIP, BYOK, signing keys) are deferred for lack of demand and because HSM-dependent paths cannot be exercised in CI.

## Algorithm disablement and migration policy

Retiring an algorithm from a storage system is not a code change; it is a data migration with a code change at each end. This section fixes the sequence so that no future deprecation removes a decrypt path while data still depends on it.

### Every persisted artifact is self-describing

The precondition for safe migration already holds: nothing relies on a global "current algorithm" setting to be decodable.

- `rustfs-crypto` blobs carry the `ID` byte (`crates/crypto/src/encdec/id.rs`) immediately after the salt.
- KMS ciphers are selected from the recorded `EncryptionAlgorithm` (`crates/kms/src/types.rs`).
- DEK envelopes record which master key version wrapped them in `DataKeyEnvelope::master_key_version` (`crates/kms/src/encryption/dek.rs`).

So for any stored object it is decidable, from the object alone, which algorithm and which key version it needs.

### Deprecation classes

Retirement moves an algorithm through these states, never skipping one:

1. **Write-disabled, read-supported.** New writes select a replacement; existing data decrypts unchanged. This is the only step that is cheap and reversible.
2. **Read-deprecated.** Reads still work but are counted and warned on, so the remaining population is measurable.
3. **Read-removed.** The decrypt path is deleted. Permitted only once the remaining population is provably zero.

### Sequencing rules

- Never advance to read-removed on the strength of an argument that data "should have been" migrated. Removal requires evidence that nothing references the algorithm, not an elapsed-time policy.
- A change to default algorithm selection is a compatibility event: it changes what new nodes write, which matters in a mixed-version cluster. Record it in the release notes and in the relevant crate's feature documentation, and check it against the [mixed-version constraints](kms-backend-security.md#mixed-version-clusters-during-a-rolling-upgrade).
- Roll out write-disablement before the corresponding read change, and let the cluster fully converge in between. A build that cannot read what a peer is still writing is the failure mode to avoid.

### Known gap

Step 3 is currently unreachable for object data. There is no object rewrap or re-encryption capability, so there is no supported way to migrate already-written objects off an algorithm or off a master key version — the same gap that forces the rotation retention rule in [KMS backend security properties](kms-backend-security.md#retention-and-destruction-preconditions). Until a rewrap capability exists, treat every algorithm that has ever been written as permanently read-required, and confine deprecation to step 1.
