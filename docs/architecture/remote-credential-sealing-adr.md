# Remote Credential Sealing ADR

**Use this when:** you add, read, or persist a stored remote credential — a replication target, a remote tier, or an on-demand migration source — or you need the sealed-envelope format, the mixed-version rules, or the reason this is worth doing in one deployment and not in another.
**Source of truth:** the three stores that hold remote credentials — `BUCKET_TARGETS_FILE` and `BUCKET_ON_DEMAND_MIGRATION_CONFIG` in `crates/ecstore/src/bucket/metadata.rs`, `TIER_CONFIG_FILE` in `crates/ecstore/src/services/tier/tier.rs` — the shared envelope in `crates/ecstore/src/bucket/sealed_credentials.rs`, the consumers `crates/ecstore/src/bucket/bucket_target_sys.rs`, `crates/ecstore/src/services/tier/tier_config.rs` and `crates/ecstore/src/bucket/on_demand_migration/config.rs`, and the backend properties in [../operations/kms-backend-security.md](../operations/kms-backend-security.md).

## Recommendation

Seal the credentials, ship the write side off by default, and claim a security benefit only for deployments running the Vault Transit or AWS KMS backend — everywhere else recommend full-disk encryption and the fail-closed parse fix below, which cost no code and cover strictly more.

## Whether encryption buys anything here

This decides the whole question, so it comes before the design. Sealing converts "read the drives" into "read the drives **and** hold an authenticated path to the key". How much that is worth depends entirely on the KMS backend, and [../operations/kms-backend-security.md](../operations/kms-backend-security.md) is explicit about the difference.

| Backend | Where the key that unwraps these credentials lives | What sealing is worth |
|---|---|---|
| Vault Transit, AWS KMS | Inside Vault or AWS; only ciphertext ever leaves | Real. An offline copy of `.rustfs.sys` is inert. Each unwrap is a live authenticated call that is logged, rate-limitable and revocable, and revoking the node's identity retroactively protects every copy already taken |
| Vault KV2 | In Vault KV v2, Base64-encoded, not wrapped | Thin. The referenced document states that KV read access is equivalent to holding the master keys, so the boundary is the Vault ACL on the key prefix — worth something only when that ACL is genuinely narrower than access to the drives, and worth nothing against anyone holding both |
| Local, Static | In `key_dir` on the node's own filesystem, or in the process environment | Close to nothing. Whoever reads the drives on a node usually reads the host too. The only gap it covers is media taken away from the host — the same gap full-disk encryption covers better |

Two honest limits hold on every backend. Sealing is **not** a defense against code execution on a node: the sealer runs in-process on every node that has to build a remote client, so an attacker at that level asks it to unseal and gets the plaintext. And it is not a defense against an authorized admin, because an admin who can rewrite a target can point it at a remote they control instead of reading the old secret.

What it does remove is the media-level read: a decommissioned or RMA'd drive, a drive-level backup or volume snapshot, a host path exposed by a bad mount, a copy of a drive taken for support. That threat is real, and it is the only one this design addresses.

## Decision

Remote credentials are sealed **per field, into an added field, behind one shared seam**, and ECStore reaches KMS through an installed hook rather than a crate dependency.

1. **One seam, three consumers.** `BucketTargetSys`, `TierConfigMgr` and `OnDemandMigrationSys` seal and unseal through `crates/ecstore/src/bucket/sealed_credentials.rs`. No consumer talks to KMS, and no consumer defines its own ciphertext layout.
2. **Only secret material is sealed,** and the enumeration comes from the redaction code, not from a pair of field names — see [Which fields are sealed](#which-fields-are-sealed). Endpoint, region, ARN, bucket, prefixes, path style, TLS flags and the custom CA bundle stay in clear text: they are needed for validation, listing and support diagnosis, and none of them is a secret.
3. **Sealed material lives in an added field, and the plaintext field is emptied rather than removed.** A reader that does not understand the sealed field must see a credential that is *present and empty*, so it takes a missing-credential path rather than a parse failure. Removing the field instead is what turns this design into an outage; [Compatibility](#compatibility-per-store-because-the-three-differ) explains why.
4. **Unsealing happens at client construction, not at parse time.** `build_remote_s3_client` in `crates/ecstore/src/bucket/remote_s3_client.rs` is the only place that needs plaintext, so admin reads, listings, validation and status paths never call KMS — and a KMS outage never changes which targets or tiers *exist*.

## What is stored today, and where

Two of the three are not files at all. `bucket-targets.json` and `on-demand-migration.json` are named sub-configurations inside one msgpack blob per bucket, and only the tier configuration is its own object.

| Store | Reached as | Actually persisted at | Written by | Container |
|---|---|---|---|---|
| Replication and ILM targets | `BUCKET_TARGETS_FILE` | `BucketMetadata::bucket_targets_config_json`, msgpack field `BucketTargetsConfigJSON` | `BucketMetadata::update_config`, then `BucketMetadata::save_with_store`; `crates/ecstore/src/bucket/metadata_sys.rs` serializes the update under a transaction lock | `{BUCKET_META_PREFIX}/{bucket}/{BUCKET_METADATA_FILE}` in `RUSTFS_META_BUCKET` (`crates/ecstore/src/disk/mod.rs`) |
| On-demand migration source | `BUCKET_ON_DEMAND_MIGRATION_CONFIG` | `BucketMetadata::on_demand_migration_config_json`, msgpack field `OnDemandMigrationConfigJSON` | same path; `update_config` additionally refuses a blob this build cannot parse | same blob as above |
| Remote tiers | `TIER_CONFIG_FILE` | its own object, a four-byte `TIER_CONFIG_FORMAT` / `TIER_CONFIG_VERSION` header followed by an `rmp_serde` payload of `ExternalTierConfigMgr` | `TierConfigMgr` through `encode_external_tiering_config_blob`, under `tier_config_lock_path` | `tier_config_path` under `CONFIG_PREFIX` in `RUSTFS_META_BUCKET` |

The consequence of the first two sharing a blob is that any change to how that blob parses has a blast radius covering policy, lifecycle, versioning, object lock and everything else in `BucketMetadata` — not just credentials.

## The at-rest boundary as it stands

Three things hold the line today, and all three keep working whether or not sealing ships.

- **The reserved bucket.** `RUSTFS_META_BUCKET` is `.rustfs.sys`; `is_reserved_or_invalid_bucket` keeps it off the S3 surface, and the admin inspect archive in `rustfs/src/admin/handlers/inspect_archive.rs` runs its request through a strict bucket-name check that a dot-prefixed reserved name does not pass.
- **Admin authorization** on every route that can read or write one of the three configurations.
- **Redaction on every read path.** `BucketTarget::redacted_credentials` and the `Debug` for `Credentials` in `crates/ecstore/src/bucket/target/bucket_target.rs`, used by the remote-target listing in `rustfs/src/admin/handlers/replication.rs` and by the bucket-metadata export in `rustfs/src/admin/handlers/bucket_meta.rs`; `TierConfig::redacted` in `crates/ecstore/src/services/tier/tier_config.rs`, which is also what that type's `Clone` and `Debug` do; and `SourceCredentials::redacted` in `crates/ecstore/src/bucket/on_demand_migration/config.rs`, used by `rustfs/src/admin/handlers/on_demand_migration.rs`.

So no API returns a stored secret. The bytes are reachable by reading the drives, and that is the boundary sealing is proposed to move.

## Which fields are sealed

The authoritative list of what this codebase treats as secret is the redaction functions above, and it is wider than `secret_key` plus `session_token`.

| Store | Sealed | Left in clear text although redacted |
|---|---|---|
| Targets | `Credentials::secret_key`, `Credentials::session_token` | — |
| On-demand migration | `SourceCredentials::secret_key`, `SourceCredentials::session_token` | — |
| Tiers | `secret_key` on each of the nine S3-family backends in `crates/ecstore/src/services/tier/tier_config.rs`, `TierAzure::sp_auth.client_secret`, and `TierGCS::creds` | `TierS3::aws_role_web_identity_token_file`, which is a path rather than a secret |

`TierGCS::creds` carries a whole service-account key and is the largest single secret of the three stores; a design that sealed only fields literally named `secret_key` would leave it in clear text. `aws_role_web_identity_token_file` points at a file outside `.rustfs.sys`, so sealing it would protect nothing — and a tier configured that way stores no long-lived secret at all, which is the cheapest mitigation available and should be preferred where the remote supports it.

## Envelope format

`SealedCredential` in `crates/ecstore/src/bucket/sealed_credentials.rs`: envelope version, KMS key id, optional KMS key version, algorithm label, and the ciphertext produced by the sealer. It is stored base64 in the two JSON stores and as bytes alongside the tier payload. `SEALED_CREDENTIAL_VERSION` is checked by `SealedCredential::check_version` *before* the sealer is consulted, so an envelope from a newer build is refused here rather than inside a backend.

The encryption context binds each ciphertext to the record that owns it. `SealScope` renders store kind, owner (bucket name, tier name or target ARN) and field name into the context, so a ciphertext copied into another bucket, another tier or another field fails to decrypt instead of silently authorizing a different remote. Those context keys are part of the on-disk contract: changing one makes every existing ciphertext undecryptable.

The envelope deliberately does **not** carry its own scope. A scope read out of the stored bytes would be attacker-controlled, and checking a ciphertext against a context it supplied itself proves nothing. The scope is always re-derived from where the ciphertext was found, which is also a constraint on any rewrap job — see [Rotation](#rotation).

## Why a hook instead of a dependency

`crates/ecstore/Cargo.toml` has no `rustfs-kms` dependency, and adding one would invert the crate layering described in [crate-boundaries.md](crate-boundaries.md). The established shape is an `OnceLock` hook that ECStore defines and the binary installs at startup, as `EVENT_DISPATCH_HOOK` in `crates/ecstore/src/services/event_notification.rs` and `ON_DEMAND_MIGRATION_CONFIG_HOOK` in `crates/ecstore/src/bucket/on_demand_migration/config.rs` already do. `install_credential_sealer` follows it, and the binary supplies an implementation backed by `crates/kms/src/service_manager.rs`.

## Compatibility, per store, because the three differ

The generic matrix is short: a plaintext record reads unchanged on any node; a sealed record reads on a new node with a sealer installed; a sealed record on a new node without one is a typed error and never a default. Everything difficult is in what an **old** node does, and the three stores behave differently enough that a single answer would be wrong.

| Store | Old node meets an added sealed field | Old node meets an emptied plaintext field | Verdict |
|---|---|---|---|
| Targets | Ignored. `BucketTarget` and `Credentials` do not use `deny_unknown_fields` | `Credentials` has no struct-level `serde(default)`, so a **missing** `secretKey` is a hard parse error for the whole document — but an **empty** one parses | Safe only if the plaintext field is emptied rather than removed |
| On-demand migration | **Rejected.** `OnDemandMigrationConfig`, `SourceConfig` and `SourceCredentials` all carry `deny_unknown_fields`, so the whole configuration becomes unreadable, and `BucketMetadata::update_config` also refuses to persist it | Parses | Needs a reader-first release before any node writes the field |
| Tiers | The payload is compact `rmp_serde`, which encodes structs positionally; an added field is an arity change a reader built for the previous struct cannot skip. `decode_external_tiering_config_blob` also rejects any `TIER_CONFIG_VERSION` it does not know | Parses | The sealed value must not be added to any struct inside the existing payload |

Two of those rows are load-bearing enough to spell out.

**Targets.** `BucketMetadata::parse_all_configs` responds to an unparseable `bucket-targets.json` by logging `bucket_metadata_parse_failed` and setting `bucket_target_config` to `BucketTargets::default()` — an empty target list. So on an old node a record whose `secretKey` was removed does not fail per target: **every target in that bucket disappears, replication stops, and no caller sees an error.** The raw bytes survive in the blob, so it is recoverable, but the silence is the hazard. Emptying the field instead of removing it avoids triggering it, and the substitution itself should be replaced by a retained parse failure before any of this ships — see [Prerequisites](#prerequisites-in-this-order).

An emptied `secretKey` is not yet a clean local failure either. `build_remote_s3_client` raises `RemoteS3ClientError::MissingCredentials` only when the whole credentials object is absent, and `remote_sdk_credentials` passes an empty secret to the SDK, so today an emptied field signs a request that the remote rejects. That is loud rather than silent, and therefore acceptable as a floor, but the reader-first release should turn an empty access key or secret key into the same typed local error so the failure is attributable to this node instead of to the remote.

**Tiers.** A format change to `tier-config.bin` takes out every tier at once, and tiers are not only a write-path concern: an object already transitioned to a tier cannot be read without that tier's configuration, so the failure reaches GETs of data that has been there for months. The sealed values therefore belong in a companion object under the same prefix, covered by the same `tier_config_lock_path`, keyed by tier name and field name, leaving `tier-config.bin` byte-shaped exactly as it is with an empty `SecretKey`. Putting the envelope *into* `SecretKey` was considered and rejected: an old node would sign requests with the ciphertext, producing remote 403s and ciphertext in signature-related logs, instead of taking its missing-credential path. Confirm the exact decode behaviour against the encode/decode tests in `crates/ecstore/src/services/tier/tier.rs` before writing a byte of the new layout, and do not bump `TIER_CONFIG_VERSION` until every node in the supported upgrade range reads it.

**Downgrade** is the same event as "old node reads new bytes", with one addition: a node that has been downgraded keeps writing the old shape, so a configuration re-submitted through it loses the sealed field and returns to plaintext. That is a security regression, not a correctness one, and it is silent — which is another reason the write side is gated rather than defaulted on.

## KMS unavailable: read time versus write time

These two are not symmetric, and conflating them is how this design would cause an outage.

**At write time** the answer is easy: sealing fails, the admin write is refused with the typed error, and nothing is persisted. A configuration is never stored with the secret dropped, and never stored in clear text after the operator asked for sealing. The cost is that configuration cannot be changed while the KMS is down, which is acceptable and visible.

**At read time** the rule is that a credential which cannot be unsealed makes a remote *unusable*, never *absent*.

- Because unsealing happens at `build_remote_s3_client`, a KMS outage does not change which targets or tiers exist. Listings, status and admin reads keep returning them; each attempt to use one fails with a typed, retryable error that names the KMS as the cause.
- Startup must not treat "cannot unseal" as "no such tier". A tier whose credential is unavailable stays present in `TierConfigMgr`, so a GET of an object transitioned to it fails with a retryable error rather than presenting as missing data, and nothing re-drives a transition elsewhere. The same holds for a replication target: it stays configured and reports why it is not working.
- **A write must refuse to rewrite a configuration it could not fully read.** This is the sharpest edge in the whole design. If a partially-unreadable configuration can be re-serialized from a partially-populated in-memory view, then a KMS outage plus one unrelated admin edit persists the configuration with the unreadable records dropped — and that is the only mechanism by which a target or tier really would disappear for good. Today's code does not have this hazard, because both stores keep raw bytes or fail the whole decode; any per-record sealed handling that skips undecodable records would introduce it.

## Rotation

The envelope records the key id and, when the backend reports one, the key version. Re-wrapping is the KMS side's job, follows [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md) and rustfs/backlog#1637 and #1642, and nothing here rotates, re-wraps or expires a key on its own. Two properties make that division workable, and both are constraints on the rewrap job rather than on this design.

- A rewrap must reproduce the encryption context, and the envelope does not carry it. The job must therefore reach a ciphertext **through its store** — enumerate targets, tiers and migration sources and derive the `SealScope` from the record's own position — rather than by scanning for envelope-shaped bytes.
- `key_version` is absent for backends that report none. An absent version means "not known to be current", never "current"; a rewrap sweep must be able to act on it, and a completed sweep is evidence about scanned sources only, exactly as the referenced contract already says about key deletion.

## Fail-closed rules

- A missing sealer, a malformed envelope, an unknown envelope version, a failed decrypt or an encryption-context mismatch is a typed error, per the root `AGENTS.md` rule that a required value returns a typed error when it is absent or corrupt. `SealedCredentialError` has no variant that degrades to a default, an empty credential, or the raw ciphertext.
- A seal failure fails the admin write.
- Redaction is unchanged and independent: admin responses keep returning `REDACTED`, and `Debug` implementations keep hiding secret fields whether or not the stored form is sealed. `SealedCredential`'s own `Debug` prints the key id and a byte count, not the ciphertext.
- Logs may carry the key id and the envelope version. They never carry ciphertext, plaintext, or an encryption-context value.
- A sealed value never enters an equality or fingerprint comparison. `tier_config_fingerprint` hashes a tier configuration to decide whether an edit is a no-op, and `OnDemandMigrationConfig` derives `PartialEq`; a fresh nonce per seal would make every write look like a change and churn the tier driver cache. Compare plaintext configurations, then seal.

## Alternatives considered

**Do not encrypt; harden the existing boundary instead.** This is the strongest alternative, not a foil. Its parts: keep `.rustfs.sys` off every request surface, which already holds; make an unparseable `bucket-targets.json` fail closed instead of becoming an empty list, which is a bug fix worth doing regardless; prefer keyless credentials where the remote supports them, as `TierS3::aws_role_web_identity_token_file` already allows; and encrypt the drives, which removes the media threat completely, covers all three stores plus every other secret in `.rustfs.sys`, and costs no code. Against the media threat, full-disk encryption strictly dominates application-level sealing. Sealing wins only where the KMS is Transit or AWS **and** the operator wants each unwrap to be individually authenticated, logged and revocable — which is exactly the scope this ADR claims and no more.

**Encrypt the whole blob, as MinIO does for its tier configuration.** Rejected. `tier-config.bin`'s header is what tells a reader the format, and a whole-blob ciphertext makes every tier unreadable whenever the KMS is unreachable; for the bucket metadata blob it would take policy, lifecycle, versioning and object lock down with the credential. Per-field sealing keeps the blast radius at one credential.

**Keep the credential in the KMS and store only a reference.** Rejected. It makes the KMS the durability authority for configuration, adds a second lifecycle with its own orphans when a bucket or tier is deleted, and none of the supported backends is a general secret store — the backends documented in [../operations/kms-backend-security.md](../operations/kms-backend-security.md) manage keys, not arbitrary secrets.

**Deterministic encryption so ciphertext is stable across writes.** Rejected. It weakens the encryption to make ciphertext comparable, and the thing that wanted comparable bytes — configuration-change detection — is correctly solved by comparing plaintext configurations before sealing.

**Seal inside `TierConfig` rather than at the persistence boundary.** Rejected. That type's `Clone` is `redacted()`, so cloning drops secrets, and `tier_config_fingerprint` hashes the type; a nondeterministic sealed field inside it would be both lossy and churn-inducing.

**Encrypt with a node-local key instead of the KMS.** Rejected. The key would sit on the same host as the data, so it removes nothing the reserved path does not already remove, and it creates key material that nothing rotates.

## Prerequisites, in this order

1. Make an unparseable `bucket-targets.json` fail closed in `BucketMetadata::parse_all_configs` instead of substituting `BucketTargets::default()`. This is independently correct and it is what keeps a later mistake from being silent.
2. Make an empty access key or secret key a typed `RemoteS3ClientError` in `remote_sdk_credentials`, so an emptied plaintext field fails on this node rather than as a signature rejection at the remote.
3. Ship a reader-first release: every store tolerates the sealed field and the emptied plaintext field, and nothing writes either. For on-demand migration this means relaxing `deny_unknown_fields` for exactly that field name; for tiers it means reading the companion object when present.
4. Only then enable writing, gated on KMS being configured and on a module switch in `rustfs/src/module_switches.rs` that defaults off in the release introducing it. Operators turn it on once every node reads the format. Existing plaintext records convert by re-submitting the configuration through its admin API; this work ships no in-place migration sweep.

Steps 1 through 3 each introduce a compatibility path that needs a `RUSTFS_COMPAT_TODO` marker and a matching entry in [compat-cleanup-register.md](compat-cleanup-register.md) when the code lands. This document adds neither, because the guard matches markers and register entries in both directions and an entry without a marker fails it.

## Non-goals

Sealing the server configuration, IAM credentials or object data keys; changing which principals may read a configuration; migrating key material between KMS backends; and any at-rest protection when KMS is not configured — without KMS the stored form stays plaintext and the boundary described above is unchanged.
