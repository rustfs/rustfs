# Remote Credential Sealing ADR

**Use this when:** you add, read, or persist a stored remote credential — a replication target, a remote tier, or an on-demand migration source — or you need the sealed-envelope format, its fail-closed rules, and the mixed-version compatibility matrix.
**Source of truth:** the three stores that hold remote credentials today — `BUCKET_TARGETS_FILE` and `BUCKET_ON_DEMAND_MIGRATION_CONFIG` in `crates/ecstore/src/bucket/metadata.rs`, and `TIER_CONFIG_FILE` in `crates/ecstore/src/services/tier/tier.rs` — plus the consumers `crates/ecstore/src/bucket/bucket_target_sys.rs`, `crates/ecstore/src/services/tier/tier.rs`, and `crates/ecstore/src/bucket/on_demand_migration/config.rs`.

## Decision

Remote credentials are sealed **per field, into an added field, behind one shared seam**, and ECStore reaches KMS through an installed hook rather than a crate dependency.

1. **One seam, three consumers.** `BucketTargetSys`, `TierConfigMgr`, and `OnDemandMigrationSys` seal and unseal through a single ECStore-owned envelope type. No consumer talks to KMS, and no consumer defines its own ciphertext layout.
2. **Only secret material is sealed.** `secret_key` and `session_token` are sealed. Endpoint, region, ARN, bucket, prefixes, path style, TLS flags, and the custom CA bundle stay in clear text: they are needed for validation, listing, and support diagnosis, and none of them is a secret.
3. **Sealed material lives in an added field, never in place of the plaintext field.** A record carries either the plaintext field or the sealed field. A reader that does not understand the sealed field therefore finds the credential *absent* rather than finding a ciphertext string it would sign requests with.
4. **Unsealing happens at client construction, not at parse time.** `build_remote_s3_client` in `crates/ecstore/src/bucket/remote_s3_client.rs` is the single point that needs plaintext, so admin reads, listings, validation, and status paths never call KMS.

## Envelope format

A versioned, self-describing record: envelope version, KMS key id, KMS key version, algorithm, nonce, and ciphertext. It is stored base64 in the two JSON stores and as raw bytes inside the msgpack payload of the tier blob; the tier blob's own `TIER_CONFIG_FORMAT` / `TIER_CONFIG_VERSION` header constants are unchanged, because the envelope carries its own version.

The KMS encryption context binds each ciphertext to the record that owns it — store kind, owning bucket or tier name, and field name — so a ciphertext copied into another bucket, another tier, or another field fails to decrypt instead of silently authorizing a different remote.

## Why a hook instead of a dependency

`crates/ecstore/Cargo.toml` has no `rustfs-kms` dependency, and adding one would invert the crate layering. The established shape is an `OnceLock` hook that ECStore defines and the binary installs at startup: `EVENT_DISPATCH_HOOK` in `crates/ecstore/src/services/event_notification.rs`, installed by `install_ecstore_event_dispatch_hook` in `rustfs/src/server/event.rs`, and `ON_DEMAND_MIGRATION_CONFIG_HOOK` in `crates/ecstore/src/bucket/on_demand_migration/config.rs`. Sealing uses the same shape, with the binary supplying an implementation backed by `get_global_kms_service_manager` in `crates/kms/src/service_manager.rs`.

## Compatibility matrix

| Stored form | Reader | Behavior |
|---|---|---|
| Plaintext (today's format) | Old node | Unchanged. |
| Plaintext | New node | Read as plaintext, no KMS call. Carries a `RUSTFS_COMPAT_TODO` marker per [compat-cleanup-register.md](compat-cleanup-register.md). |
| Sealed | New node, hook installed | Unsealed at client construction. |
| Sealed | New node, no hook or decrypt failure | Typed error; the target, tier, or source is unusable and reports why. Never a default, an empty credential, or the ciphertext bytes. |
| Sealed | Old node | The credential field is absent, so the old node fails closed on its existing "missing credentials" path. This is the migration hazard the rollout gate exists for. |

## Rollout gate

Sealing is written only when KMS is configured **and** a module switch in `rustfs/src/module_switches.rs` is on, defaulting off in the release that introduces it. Reading sealed records is always supported; writing them is what waits. Operators enable the switch after every node in the cluster can read the format, and existing plaintext records are sealed by re-submitting the configuration through its admin API — this task ships no in-place migration sweep.

## Rotation

The envelope records the key id and key version it was wrapped under. Re-wrapping is the KMS side's job and follows [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md); nothing in this design rotates, re-wraps, or expires a key on its own.

## Fail-closed rules

- A missing hook, a malformed envelope, an unknown envelope version, a failed decrypt, or an encryption-context mismatch is a typed error, per the AGENTS.md rule that required values return a typed error when absent or corrupt.
- A seal failure fails the admin write. A configuration is never persisted with the secret dropped or left in clear text after the operator asked for sealing.
- Redaction is unchanged and independent: admin responses keep returning `REDACTED`, and `Debug` implementations keep hiding secret fields whether or not the stored form is sealed.
- Logs may carry the key id and envelope version. They never carry ciphertext, plaintext, or the encryption context's secret-adjacent values.

## Non-goals

Sealing the server config, IAM credentials, or object data keys; changing which principals may read a configuration; key material migration between KMS backends; and any at-rest protection when KMS is not configured — without KMS the stored form stays plaintext and the existing trust boundary (reserved bucket paths plus admin authorization) is unchanged.
