// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vault-based KMS backend implementation using vaultrs

use crate::backends::vault_credentials::{
    CredentialTaskHandle, VaultClientHandle, VaultConnectionSettings, VaultCredentialPolicy, VaultCredentialProvider,
    token_source_for,
};
use crate::backends::{
    BackendCapabilities, ExpiredKeyRemoval, KmsBackend, ListedKeyFailure, StateGatedOperation, UnreadableKeys,
    classify_listed_key_failure, empty_key_page, ensure_key_state_permits, ensure_key_status_permits,
    ensure_rewrap_context_matches, ensure_tag_keys_are_mutable, list_keys_page_size, paginate_keys, started_at_the_first_key,
};
use crate::config::{KmsConfig, VaultConfig};
use crate::encryption::{AesDekCrypto, DataKeyEnvelope, DekCrypto, generate_key_material};
use crate::error::{KmsError, Result};
use crate::persisted_observability::{BoundedUnknownFieldName, UnknownFieldSummary};
use crate::policy::{self, AttemptError, OpClass, RetryPolicy};
use crate::types::*;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use jiff::Zoned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use vaultrs::{api::kv2::requests::SetSecretRequestOptions, error::ClientError, kv2};
use zeroize::Zeroize as _;

/// Vault KMS client implementation
pub struct VaultKmsClient {
    credentials: Arc<VaultCredentialProvider>,
    /// Mount path for the KV engine (typically "kv" or "secret")
    kv_mount: String,
    /// Path prefix for storing keys
    key_path_prefix: String,
    /// DEK encryption implementation
    dek_crypto: AesDekCrypto,
    /// Budgets wrapping every outbound Vault call (see `crate::policy`).
    retry: RetryPolicy,
    /// Cancellation point for the operation executor: aborts in-flight
    /// attempts and backoff sleeps. Owned by the client and currently never
    /// triggered — shutdown drops the whole client — but kept as the single
    /// hook a future lifecycle owner can cancel through.
    cancel: CancellationToken,
    /// Per-key in-process remainder of the persisted wrap-budget reservation
    /// (see [`VaultKmsClient::consume_wrap_budget`]). Grows with the master
    /// keys this node wraps under and is never pruned; that set is small by
    /// construction. The outer lock is only ever held to look up or insert the
    /// per-key entry, never across an await.
    wrap_budgets: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<WrapBudget>>>>,
}

/// Key data stored in Vault
///
/// `Deserialize` is hand-written so fields the current build does not know
/// are counted and warned about instead of vanishing silently — this record
/// is compatibility-bound in both directions (older and newer builds read
/// each other's writes), so `deny_unknown_fields` is not an option.
#[derive(Debug, Clone, Serialize)]
struct VaultKeyData {
    /// Key algorithm
    algorithm: String,
    /// Key usage type
    usage: KeyUsage,
    /// Key creation timestamp
    created_at: Zoned,
    /// Key status
    status: KeyStatus,
    /// Key version
    version: u32,
    /// Key description
    description: Option<String>,
    /// Key metadata
    metadata: HashMap<String, String>,
    /// Key tags
    tags: HashMap<String, String>,
    /// Scheduled deletion deadline; absent on records written before deadline
    /// persistence landed, so it must stay optional for backward compatibility.
    #[serde(default)]
    deletion_date: Option<Zoned>,
    /// When the key's material last became current through a rotation.
    ///
    /// Written by [`VaultKmsClient::rotate_key`] as part of the same
    /// check-and-set that switches the current version, so it can only be set on
    /// a rotation that actually committed. `None` means "no rotation time on
    /// record", which covers two cases that are deliberately not distinguished
    /// here: a key that was never rotated, and a key rotated by a build that
    /// predates this field. Nothing is back-filled — inventing a timestamp for
    /// the second case would report a rotation that this node never observed.
    /// See [`crate::deletion_worker`] for why collapsing the two is safe for the
    /// rotation-age gauge.
    #[serde(default)]
    rotated_at: Option<Zoned>,
    /// Encrypted key material (base64 encoded)
    encrypted_key_material: String,
    /// Version that pre-versioning envelopes (no `master_key_version`) resolve to.
    ///
    /// Recorded once, at the key's first rotation, when the then-current material is
    /// frozen as an immutable version record. `None` means the key has never been
    /// rotated, so legacy envelopes keep resolving to the current version — exactly
    /// the pre-versioning behavior. Optional so records written by older builds keep
    /// deserializing.
    #[serde(default)]
    baseline_version: Option<u32>,
    /// Wrap operations reserved against this key's *current* master key
    /// material, in blocks of [`WRAP_BUDGET_BLOCK`].
    ///
    /// AES-256-GCM caps one key at 2^32 encryptions under random 96-bit nonces
    /// (NIST SP 800-38D), and this backend wraps every DEK locally with the
    /// current material, so this approximates how much of that bound the
    /// cluster has consumed. Nodes reserve whole blocks up front and count
    /// individual wraps in process memory only, so the persisted value can run
    /// ahead of the wraps actually performed but — on builds that know the
    /// field — never behind: a crash discards unused in-memory budget, never a
    /// counted wrap. Two documented ways the value can still understate: wraps
    /// performed while a reservation write kept failing (logged at warn, and
    /// re-covered by the next reservation that lands), and an old build
    /// rewriting this record on any lifecycle write, which drops the field it
    /// does not know and regresses the count to zero.
    ///
    /// Reset to 0 by [`VaultKmsClient::rotate_key`]'s pointer-switch commit:
    /// the GCM bound is per key material, and rotation installs fresh material.
    #[serde(default)]
    wrap_budget_reserved: u64,
}

/// Wrap operations reserved from the key record per reservation write.
///
/// Large enough that the once-per-block CAS write disappears against a million
/// data-path wraps, small enough that the crash-time overestimate (at most one
/// discarded block per node) stays negligible against the 2^32 bound.
const WRAP_BUDGET_BLOCK: u64 = 1_000_000;

/// In-process remainder of one key's persisted wrap-budget reservation.
#[derive(Debug, Default)]
struct WrapBudget {
    /// Master key version the grant was taken against, only ever moved
    /// forward. A *newer* version about to wrap means the key rotated: fresh
    /// material has a fresh nonce budget and a zeroed persisted counter, so
    /// the stale grant (and any stale debt — the old material never wraps
    /// again) is discarded. An *older* one is a wrap whose snapshot lost a
    /// race with a rotation and is simply counted against the current grant.
    version: u32,
    /// Wraps still covered by the last block grant.
    available: u64,
    /// Budget granted in memory while reservation writes were failing — wraps
    /// the persisted counter does not cover yet. Added onto the next
    /// successful reservation so the persisted count catches back up.
    unpersisted: u64,
}

impl UnknownFieldSummary {
    fn record_for_vault_kv2_key(&self) {
        let Some((field, field_name_truncated, field_count)) = self.record("vault-kv2-key") else {
            return;
        };

        static RECORDS_WITH_UNKNOWN_FIELDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let observed_records = RECORDS_WITH_UNKNOWN_FIELDS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        if observed_records.is_power_of_two() {
            tracing::warn!(
                field = ?field,
                field_name_truncated,
                field_count,
                observed_records,
                "Vault KV2 key record contains unknown fields"
            );
        }
    }
}

impl<'de> Deserialize<'de> for VaultKeyData {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, IgnoredAny, MapAccess, Visitor};
        use std::fmt;

        enum Field {
            Algorithm,
            Usage,
            CreatedAt,
            Status,
            Version,
            Description,
            Metadata,
            Tags,
            DeletionDate,
            RotatedAt,
            EncryptedKeyMaterial,
            BaselineVersion,
            WrapBudgetReserved,
            Unknown(BoundedUnknownFieldName),
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a Vault KV2 key record field name")
                    }

                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(match value {
                            "algorithm" => Field::Algorithm,
                            "usage" => Field::Usage,
                            "created_at" => Field::CreatedAt,
                            "status" => Field::Status,
                            "version" => Field::Version,
                            "description" => Field::Description,
                            "metadata" => Field::Metadata,
                            "tags" => Field::Tags,
                            "deletion_date" => Field::DeletionDate,
                            "rotated_at" => Field::RotatedAt,
                            "encrypted_key_material" => Field::EncryptedKeyMaterial,
                            "baseline_version" => Field::BaselineVersion,
                            "wrap_budget_reserved" => Field::WrapBudgetReserved,
                            _ => Field::Unknown(BoundedUnknownFieldName::new(value)),
                        })
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct VaultKeyDataVisitor;

        impl<'de> Visitor<'de> for VaultKeyDataVisitor {
            type Value = VaultKeyData;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Vault KV2 key record")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                macro_rules! read_field {
                    ($slot:ident, $name:literal) => {{
                        if $slot.is_some() {
                            return Err(de::Error::duplicate_field($name));
                        }
                        $slot = Some(map.next_value()?);
                    }};
                }

                let mut algorithm = None;
                let mut usage = None;
                let mut created_at = None;
                let mut status = None;
                let mut version = None;
                let mut description = None;
                let mut metadata = None;
                let mut tags = None;
                let mut deletion_date = None;
                let mut rotated_at = None;
                let mut encrypted_key_material = None;
                let mut baseline_version = None;
                let mut wrap_budget_reserved = None;
                let mut unknown_fields = UnknownFieldSummary::default();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Algorithm => read_field!(algorithm, "algorithm"),
                        Field::Usage => read_field!(usage, "usage"),
                        Field::CreatedAt => read_field!(created_at, "created_at"),
                        Field::Status => read_field!(status, "status"),
                        Field::Version => read_field!(version, "version"),
                        Field::Description => read_field!(description, "description"),
                        Field::Metadata => read_field!(metadata, "metadata"),
                        Field::Tags => read_field!(tags, "tags"),
                        Field::DeletionDate => read_field!(deletion_date, "deletion_date"),
                        Field::RotatedAt => read_field!(rotated_at, "rotated_at"),
                        Field::EncryptedKeyMaterial => read_field!(encrypted_key_material, "encrypted_key_material"),
                        Field::BaselineVersion => read_field!(baseline_version, "baseline_version"),
                        Field::WrapBudgetReserved => read_field!(wrap_budget_reserved, "wrap_budget_reserved"),
                        Field::Unknown(field) => {
                            let _: IgnoredAny = map.next_value()?;
                            unknown_fields.observe(field);
                        }
                    }
                }

                let key_data = VaultKeyData {
                    algorithm: algorithm.ok_or_else(|| de::Error::missing_field("algorithm"))?,
                    usage: usage.ok_or_else(|| de::Error::missing_field("usage"))?,
                    created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?,
                    status: status.ok_or_else(|| de::Error::missing_field("status"))?,
                    version: version.ok_or_else(|| de::Error::missing_field("version"))?,
                    description: description.unwrap_or(None),
                    metadata: metadata.ok_or_else(|| de::Error::missing_field("metadata"))?,
                    tags: tags.ok_or_else(|| de::Error::missing_field("tags"))?,
                    deletion_date: deletion_date.unwrap_or(None),
                    rotated_at: rotated_at.unwrap_or(None),
                    encrypted_key_material: encrypted_key_material
                        .ok_or_else(|| de::Error::missing_field("encrypted_key_material"))?,
                    baseline_version: baseline_version.unwrap_or(None),
                    // Absent on records written before wrap accounting existed, and
                    // on records an older build rewrote; zero restarts the
                    // reservation rather than blocking a wrap.
                    wrap_budget_reserved: wrap_budget_reserved.unwrap_or(0),
                };
                unknown_fields.record_for_vault_kv2_key();
                Ok(key_data)
            }
        }

        const FIELDS: &[&str] = &[
            "algorithm",
            "usage",
            "created_at",
            "status",
            "version",
            "description",
            "metadata",
            "tags",
            "deletion_date",
            "rotated_at",
            "encrypted_key_material",
            "baseline_version",
            "wrap_budget_reserved",
        ];
        deserializer.deserialize_struct("VaultKeyData", FIELDS, VaultKeyDataVisitor)
    }
}

/// Immutable per-version master key material record stored under
/// `{prefix}/{key_id}/versions/{N}`.
///
/// Version records are created with a KV2 check-and-set of 0 (create-only) and are
/// never rewritten, so every master key version that ever wrapped a DEK stays
/// readable after rotation. The top-level `{prefix}/{key_id}` record keeps a copy of
/// the current material as a fast path and so binaries that predate versioned
/// storage can still read never-rotated keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VaultKeyVersionRecord {
    /// Master key version this record holds material for
    version: u32,
    /// Encrypted key material (base64 encoded)
    encrypted_key_material: String,
    /// When this version's material was created
    created_at: Zoned,
}

/// Sub-path (under each key path) reserved for immutable version records.
const KEY_VERSIONS_SUBPATH: &str = "versions";

/// Upper bound on read-modify-write attempts for a check-and-set lifecycle
/// mutation. A lost race means live contention on the key; each retry re-reads
/// and re-validates, and a small bound keeps a pathologically contended key
/// from spinning while still absorbing ordinary interleavings.
const LIFECYCLE_CAS_ATTEMPTS: u32 = 3;

/// Typed error for a lifecycle write that lost its check-and-set race (and, in
/// the retry loop, kept losing it up to the attempt bound).
fn concurrent_modification(key_id: &str) -> KmsError {
    KmsError::invalid_operation(format!("Concurrent modification of key {key_id} detected, retry the operation"))
}

/// Decision returned by a [`VaultKmsClient::update_key_data_with_cas`]
/// mutation closure.
enum CasMutation<T> {
    /// Persist the mutated record with a check-and-set write, then yield the
    /// value.
    Write(T),
    /// The freshly observed state already settles the operation; yield the
    /// value without writing.
    Skip(T),
}

/// Drop KV2 directory entries from a key listing.
///
/// Once a key has version records, listing the key prefix returns both the key
/// record itself ("my-key") and a directory entry for its version sub-path
/// ("my-key/"); only the former is a key.
fn filter_key_directory_entries(keys: Vec<String>) -> Vec<String> {
    keys.into_iter().filter(|key| !key.ends_with('/')).collect()
}

/// Resolve which master key version wrapped an envelope.
///
/// `Some` versions are honored verbatim: if the record for that version is missing
/// the lookup must fail closed with [`KmsError::KeyVersionNotFound`], never fall
/// back to the current material. `None` (a pre-versioning envelope) resolves to the
/// key's baseline version — the deterministic version whose material was current
/// before the first rotation froze it — and, for keys that were never rotated and
/// thus have no baseline, to the current version, which matches pre-versioning
/// behavior exactly.
fn resolve_envelope_master_key_version(
    envelope_version: Option<u32>,
    baseline_version: Option<u32>,
    current_version: u32,
) -> u32 {
    envelope_version.or(baseline_version).unwrap_or(current_version)
}

/// Whether a KV2 write failed its check-and-set precondition.
fn is_cas_conflict(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::APIError { code: 400, errors } if errors.iter().any(|message| message.contains("check-and-set"))
    )
}

/// Whether a Vault LIST failed with the 404 that means "the path was routed,
/// and there is nothing under it".
///
/// Vault answers a LIST of a path holding no entries with a 404 whose `errors`
/// array is empty. A path with no mount behind it answers with the same status
/// but carries a "no handler for route" message, so the empty `errors` array is
/// what separates "reachable but empty" from "nothing mounted there".
/// `ClientError`'s `Display` renders both as a bare "(status code 404)", so the
/// distinction survives only on the typed error.
///
/// This separates a routed path from an unrouted one, not a correct mount from
/// a wrong one. Two configurations still read as empty: a `kv_mount` pointing at
/// a KV v1 engine, which routes the KV2 metadata path and finds nothing under
/// it, and (on OSS Vault) a `namespace` that does not exist. Telling those apart
/// needs a `sys/mounts` read the KMS token is not required to be allowed to
/// make, so no LIST-based probe can catch them.
fn is_empty_vault_list(error: &ClientError) -> bool {
    matches!(error, ClientError::APIError { code: 404, errors } if errors.is_empty())
}

/// Message for a KV2 listing failure, naming the mount it was made against.
///
/// `ClientError`'s `Display` carries only the status code — never the `errors`
/// array, and for a body it could not parse not even that — so on its own it
/// reaches the operator as an unexplained failure against an unnamed mount.
/// Vault's own message says which route found no handler, so it rides along.
/// What Vault reported is repeated rather than diagnosed: a message-bearing 404
/// also covers a mount of the wrong type and, on Vault Enterprise, a mount
/// filtered out of this namespace or replica.
///
/// `listed` names what was being listed (`"keys"`, `"key version records"`) and
/// only reaches error text, never a metric label.
fn describe_kv2_list_failure(kv_mount: &str, listed: &str, error: &ClientError) -> String {
    match error {
        ClientError::APIError { code: 404, errors } => {
            format!("Failed to list {listed} in Vault kv_mount '{kv_mount}': {}", errors.join("; "))
        }
        other => format!("Failed to list {listed} in Vault kv_mount '{kv_mount}': {other}"),
    }
}

/// Map a KV2 record read failure onto the typed error surface.
///
/// The three record-level outcomes are told apart from a backend outcome here,
/// at the only place that knows a per-key record was being read: a 404 or a wrap
/// failure is a key that is not there, an unparseable body is a record that is
/// there and cannot be interpreted, and an empty `data` field is a record whose
/// contents are gone. Folding the last two into a generic backend error made a
/// damaged record indistinguishable from a Vault outage, which is exactly the
/// distinction the fail-closed material rules and the listing contract turn on.
///
/// `record` names what was being read (`"key record"`, `"transit key
/// metadata"`) and only reaches error text, never a metric label. Shared with
/// the Transit backend, whose per-key metadata records live in KV2 too and
/// otherwise had no way to be reported as damaged rather than as an outage.
pub(super) fn map_key_record_read_error(key_id: &str, record: &str, error: ClientError) -> KmsError {
    match error {
        ClientError::ResponseWrapError | ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
        // Only the position and category of the parse failure are reported.
        // serde's own message embeds the offending scalar ("invalid type:
        // string \"…\", expected u32"), which for a key record is a value out
        // of the stored material, and this message reaches both a log line and
        // an admin HTTP body.
        ClientError::JsonParseError { source } => KmsError::material_corrupt(
            key_id,
            format!(
                "stored {record} does not deserialize ({:?} at line {}, column {})",
                source.classify(),
                source.line(),
                source.column()
            ),
        ),
        ClientError::ResponseDataEmptyError => KmsError::material_missing(key_id),
        error => KmsError::backend_error(format!("Failed to read {record} from Vault: {error}")),
    }
}

/// Decode and validate the stored master key material of a [`VaultKeyData`] record.
///
/// This is the single read-side gate for KV2 key material: missing or undecodable
/// material must fail closed with a typed error and must never be regenerated or
/// written back (regenerating would orphan every DEK wrapped by the original key).
/// Kept synchronous and free of Vault I/O so the poison matrix is unit-testable
/// without a live Vault.
fn decode_stored_key_material(key_id: &str, encrypted_material: &str) -> Result<Vec<u8>> {
    if encrypted_material.is_empty() {
        return Err(KmsError::material_missing(key_id));
    }

    // Mirrors `decrypt_key_material`: stored material is currently base64 without an
    // additional encryption layer.
    let key_material = general_purpose::STANDARD
        .decode(encrypted_material)
        .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key material is not valid base64: {e}")))?;

    // Key material must be exactly 32 bytes for AES-256.
    if key_material.len() != 32 {
        return Err(KmsError::material_corrupt(
            key_id,
            format!("stored key material has invalid length ({} bytes, expected 32)", key_material.len()),
        ));
    }

    Ok(key_material)
}

impl VaultKmsClient {
    /// Create a new Vault KMS client
    ///
    /// `kms_config` supplies the per-attempt timeout that caps every HTTP
    /// request issued through this client, plus the retry and fail-closed
    /// budgets for credential refresh.
    pub async fn new(config: VaultConfig, kms_config: &KmsConfig) -> Result<Self> {
        let settings = VaultConnectionSettings {
            address: config.address.clone(),
            namespace: config.namespace.clone(),
            attempt_timeout: kms_config.effective_timeout(),
            skip_tls_verify: config.tls.as_ref().is_some_and(|tls| tls.skip_verify),
        };
        let source = token_source_for(&config.auth_method, &settings)?;
        let policy = VaultCredentialPolicy::from_kms_config(
            kms_config,
            &config.auth_method,
            "vault-kv2",
            &config.address,
            config.namespace.as_deref(),
        );
        let credentials = Arc::new(VaultCredentialProvider::new(settings, source, policy).await?);

        info!(address = %config.address, "Vault KMS backend connected");

        Ok(Self {
            credentials,
            kv_mount: config.kv_mount.clone(),
            key_path_prefix: config.key_path_prefix.clone(),
            dek_crypto: AesDekCrypto::new(),
            retry: RetryPolicy::for_backend(kms_config, "vault-kv2", &config.address, config.namespace.as_deref(), "operations"),
            cancel: CancellationToken::new(),
            wrap_budgets: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Count one DEK wrap against the key's persisted wrap budget.
    ///
    /// `key_version` is the master key version whose material is about to
    /// wrap, from the same record snapshot the wrap itself uses. Budget is
    /// taken from an in-process block; only when the block is exhausted (or
    /// the key rotated under it) is a new block of [`WRAP_BUDGET_BLOCK`]
    /// reserved by a check-and-set update of the key record — never a write
    /// per wrap, so the data path pays one extra Vault round trip per million
    /// wraps, not per object.
    ///
    /// Reserve-then-consume on purpose: the reservation lands before the wrap
    /// it covers, so a crash can only ever discard reserved-but-unused budget
    /// — the persisted count overestimates, never undercounts. The counter is
    /// advisory observability, not a quota: a reservation that cannot be
    /// persisted is logged and the wrap proceeds on an in-memory grant carried
    /// as `unpersisted` debt, which the next successful reservation adds on
    /// top of its own block. That fail-open grant is also what bounds the
    /// warn to at most one per block of wraps. Infallible by design — no
    /// Vault hiccup here may fail a PUT.
    ///
    /// Concurrent wraps of the same key briefly queue on the per-key lock
    /// while the once-per-block reservation is in flight instead of each
    /// issuing their own.
    async fn consume_wrap_budget(&self, key_id: &str, key_version: u32) {
        let budget = {
            let mut budgets = self.wrap_budgets.lock().expect("wrap budget map lock poisoned");
            match budgets.get(key_id) {
                // Fast path spares the per-wrap key allocation `entry` needs.
                Some(budget) => Arc::clone(budget),
                None => Arc::clone(budgets.entry(key_id.to_string()).or_default()),
            }
        };
        let mut budget = budget.lock().await;
        if key_version > budget.version {
            *budget = WrapBudget {
                version: key_version,
                ..WrapBudget::default()
            };
        }
        // `key_version < budget.version` is a wrap whose record snapshot lost a
        // race with a rotation. It is counted against the current grant rather
        // than resetting to the old version: the newer grant is persisted on
        // the post-rotation record, so the count stays an overestimate, and a
        // burst of in-flight stale wraps cannot ping-pong the version tag into
        // one reservation write each.
        if budget.available == 0 {
            let requested = WRAP_BUDGET_BLOCK.saturating_add(budget.unpersisted);
            let reserved = self
                .update_key_data_with_cas(key_id, |key_data| {
                    key_data.wrap_budget_reserved = key_data.wrap_budget_reserved.saturating_add(requested);
                    Ok(CasMutation::Write(()))
                })
                .await;
            match reserved {
                Ok(_) => {
                    budget.available = WRAP_BUDGET_BLOCK;
                    budget.unpersisted = 0;
                }
                Err(error) => {
                    budget.available = WRAP_BUDGET_BLOCK;
                    budget.unpersisted = requested;
                    warn!(key_id, requested, %error, "Vault KMS wrap budget reservation failed; wraps continue uncounted");
                }
            }
        }
        budget.available -= 1;
    }

    /// Snapshot the authenticated Vault client for a single request.
    ///
    /// Every Vault call takes its own snapshot so a credential rotation
    /// applies to subsequent calls without interrupting in-flight ones. Fails
    /// closed when the credentials could not be refreshed in time.
    fn vault(&self) -> Result<Arc<VaultClientHandle>> {
        self.credentials.current()
    }

    /// Run one Vault call under the operation policy.
    ///
    /// The closure performs a single classified attempt and takes a fresh
    /// credential snapshot per attempt, so a retry after a credential rotation
    /// uses the new token.
    async fn run<T, F, Fut>(&self, operation: &'static str, class: OpClass, attempt: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, AttemptError>>,
    {
        policy::execute(operation, class, &self.retry, &self.cancel, attempt).await
    }

    /// Get the full path for a key in Vault
    fn key_path(&self, key_id: &str) -> String {
        format!("{}/{}", self.key_path_prefix, key_id)
    }

    /// Get the path of the immutable record holding one version's material
    fn key_version_path(&self, key_id: &str, version: u32) -> String {
        format!("{}/{}/{}/{}", self.key_path_prefix, key_id, KEY_VERSIONS_SUBPATH, version)
    }

    /// Get the directory path holding a key's version records
    fn key_versions_dir(&self, key_id: &str) -> String {
        format!("{}/{}/{}", self.key_path_prefix, key_id, KEY_VERSIONS_SUBPATH)
    }

    /// Encode key material for KV2 storage.
    ///
    /// This is plain Base64 encoding, not encryption: the KV2 backend stores master key
    /// material as-is and relies on Vault ACLs plus KV2 at-rest encryption for
    /// confidentiality. Any identity with KV read access to the key path can recover the
    /// plaintext master key.
    async fn encrypt_key_material(&self, key_material: &[u8]) -> Result<String> {
        Ok(general_purpose::STANDARD.encode(key_material))
    }

    /// Read the immutable material record of one key version.
    ///
    /// A missing record fails closed with [`KmsError::KeyVersionNotFound`]; falling
    /// back to the current material would decrypt with the wrong key at best and
    /// mask a tampered envelope version at worst.
    async fn get_key_version_record(&self, key_id: &str, version: u32) -> Result<VaultKeyVersionRecord> {
        let path = self.key_version_path(key_id, version);

        let path = path.as_str();
        let record: VaultKeyVersionRecord = self
            .run("vault_kv2_read_key_version", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                kv2::read(&vault.client, &self.kv_mount, path).await.map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| match e {
                        ClientError::ResponseWrapError | ClientError::APIError { code: 404, .. } => {
                            KmsError::key_version_not_found(key_id, version)
                        }
                        e => KmsError::backend_error(format!("Failed to read key version record from Vault: {e}")),
                    })
                })
            })
            .await?;

        if record.version != version {
            return Err(KmsError::material_corrupt(
                key_id,
                format!("version record at {path} claims version {} instead of {version}", record.version),
            ));
        }

        Ok(record)
    }

    /// Load master key material for a specific key version.
    ///
    /// The top-level record is the authoritative copy for the current version (a
    /// never-rotated key has no version records at all); any other version must have
    /// an immutable version record.
    async fn get_key_material_for_version(&self, key_id: &str, key_data: &VaultKeyData, version: u32) -> Result<Vec<u8>> {
        let encrypted_material = if version == key_data.version {
            key_data.encrypted_key_material.clone()
        } else {
            let record = self.get_key_version_record(key_id, version).await?;
            if version > key_data.version {
                // The requested version has an immutable record, yet the
                // current pointer sits below it. Material for a version is
                // only requested once an envelope references it, and
                // envelopes are only stamped after the pointer switch
                // committed — so the pointer must have regressed (a lost
                // update rolled back a committed rotation). Fail closed:
                // serving in this state would keep new encryptions on the
                // rolled-back material. A version with no record at all still
                // fails as KeyVersionNotFound above.
                return Err(KmsError::internal_error(format!(
                    "current version {} of key {key_id} is behind existing version record {version}; refusing to use an inconsistent key record",
                    key_data.version
                )));
            }
            record.encrypted_key_material
        };

        decode_stored_key_material(key_id, &encrypted_material).inspect_err(|error| {
            warn!(key_id, version, %error, "Vault KMS key material failed validation");
        })
    }

    /// Read the key record together with the KV2 secret version holding it, so a
    /// later write can be check-and-set against exactly this snapshot.
    async fn get_key_data_versioned(&self, key_id: &str) -> Result<(u32, VaultKeyData)> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        let metadata = self
            .run("vault_kv2_read_key_metadata", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                kv2::read_metadata(&vault.client, &self.kv_mount, path).await.map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| match e {
                        ClientError::ResponseWrapError | ClientError::APIError { code: 404, .. } => {
                            KmsError::key_not_found(key_id)
                        }
                        e => KmsError::backend_error(format!("Failed to read key metadata from Vault: {e}")),
                    })
                })
            })
            .await?;
        let cas = u32::try_from(metadata.current_version)
            .map_err(|_| KmsError::backend_error(format!("KV2 secret version for key {key_id} exceeds u32")))?;

        // Read the exact secret version from the metadata to keep the (cas, data)
        // pair consistent even if another writer lands in between.
        let secret_version = metadata.current_version;
        let key_data: VaultKeyData = self
            .run("vault_kv2_read_key_at_version", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                kv2::read_version(&vault.client, &self.kv_mount, path, secret_version)
                    .await
                    .map_err(|e| AttemptError::from_vaultrs(e, |e| map_key_record_read_error(key_id, "key record", e)))
            })
            .await?;

        Ok((cas, key_data))
    }

    /// Check-and-set write of the key record, reporting a lost race as
    /// `Ok(None)`.
    ///
    /// `cas` must match the KV2 secret version currently holding the record.
    /// On success returns the secret version created by this write so a caller
    /// can chain further check-and-set writes.
    async fn try_cas_store_key_data(&self, key_id: &str, key_data: &VaultKeyData, cas: u32) -> Result<Option<u32>> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        // Single attempt: replaying a lost-response write would double-apply
        // the mutation, and a CAS conflict is a normal concurrency signal the
        // caller resolves by re-reading, never by resending the same write.
        let written = self
            .run("vault_kv2_cas_write_key", OpClass::MutatingNonIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match kv2::set_with_options(&vault.client, &self.kv_mount, path, key_data, SetSecretRequestOptions { cas }).await
                {
                    Ok(written) => Ok(Some(written)),
                    Err(e) if is_cas_conflict(&e) => Ok(None),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to store key in Vault: {e}"))
                    })),
                }
            })
            .await?;

        written
            .map(|written| {
                u32::try_from(written.version)
                    .map_err(|_| KmsError::backend_error(format!("KV2 secret version for key {key_id} exceeds u32")))
            })
            .transpose()
    }

    /// Check-and-set write of the key record, surfacing a lost race as the
    /// typed concurrent-modification error.
    async fn cas_store_key_data(&self, key_id: &str, key_data: &VaultKeyData, cas: u32) -> Result<u32> {
        self.try_cas_store_key_data(key_id, key_data, cas)
            .await?
            .ok_or_else(|| concurrent_modification(key_id))
    }

    /// Apply a lifecycle mutation to the key record as a check-and-set
    /// read-modify-write loop.
    ///
    /// Every attempt re-reads the record pinned to its current KV2 secret
    /// version, re-derives the mutation from that fresh snapshot — `mutate`
    /// must re-run its state gate, so a transition that lost a race against
    /// e.g. a rotation or a cancellation is re-validated against the committed
    /// state instead of being replayed — and writes back check-and-set against
    /// exactly the version it read. A conflict means another writer committed
    /// in between; after [`LIFECYCLE_CAS_ATTEMPTS`] lost races the typed
    /// conflict error is surfaced to the caller.
    ///
    /// This loop does not bypass the operation policy's single-attempt rule
    /// for `MutatingNonIdempotent` writes: each individual write is still sent
    /// at most once and never replayed on a lost response. Only the whole
    /// read-gate-mutate-write cycle repeats, and every repeat is derived from
    /// newly observed state, so the two layers compose instead of conflicting.
    async fn update_key_data_with_cas<T, F>(&self, key_id: &str, mut mutate: F) -> Result<(VaultKeyData, T)>
    where
        F: FnMut(&mut VaultKeyData) -> Result<CasMutation<T>>,
    {
        for attempt in 1..=LIFECYCLE_CAS_ATTEMPTS {
            let (cas, mut key_data) = self.get_key_data_versioned(key_id).await?;
            match mutate(&mut key_data)? {
                CasMutation::Skip(value) => return Ok((key_data, value)),
                CasMutation::Write(value) => {
                    if self.try_cas_store_key_data(key_id, &key_data, cas).await?.is_some() {
                        return Ok((key_data, value));
                    }
                    debug!(key_id, attempt, "Vault KV2 lifecycle write lost a check-and-set race; re-reading");
                }
            }
        }

        Err(concurrent_modification(key_id))
    }

    /// Create-only write of an immutable version record (KV2 check-and-set of 0).
    ///
    /// Returns `Ok(true)` when this call created the record and `Ok(false)` when a
    /// record already exists at that version; the caller decides whether the
    /// existing record is acceptable. The record is never overwritten.
    async fn try_create_key_version_record(&self, key_id: &str, record: &VaultKeyVersionRecord) -> Result<bool> {
        let path = self.key_version_path(key_id, record.version);
        let path = path.as_str();

        // Single attempt: the create-only CAS makes a duplicate replay fail
        // with a conflict, which the caller resolves by reading the record
        // back, so retrying here would only mask that recovery path.
        self.run("vault_kv2_create_key_version", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::set_with_options(&vault.client, &self.kv_mount, path, record, SetSecretRequestOptions { cas: 0 }).await {
                Ok(_) => Ok(true),
                Err(e) if is_cas_conflict(&e) => Ok(false),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Failed to store key version record in Vault: {e}"))
                })),
            }
        })
        .await
    }

    /// Create-only write of the top-level key record (KV2 check-and-set of 0).
    ///
    /// Returns `Ok(true)` when this call created the record and `Ok(false)`
    /// when a record already exists — i.e. a concurrent create committed
    /// first. An existing record is never overwritten.
    async fn try_create_key_data(&self, key_id: &str, key_data: &VaultKeyData) -> Result<bool> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        // Single attempt: the create-only CAS makes a duplicate replay fail
        // with a conflict, which create_key reports as the key already
        // existing, so retrying here would only mask that signal.
        self.run("vault_kv2_create_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::set_with_options(&vault.client, &self.kv_mount, path, key_data, SetSecretRequestOptions { cas: 0 }).await {
                Ok(_) => Ok(true),
                Err(e) if is_cas_conflict(&e) => Ok(false),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Failed to store key in Vault: {e}"))
                })),
            }
        })
        .await
    }

    /// Blind, last-writer-wins overwrite of the key record.
    ///
    /// Test-only: production writes go through the create-only or
    /// check-and-set paths so concurrent writers cannot silently clobber each
    /// other. Kept for tests that need to inject corrupted or downgraded
    /// records.
    #[cfg(test)]
    async fn store_key_data(&self, key_id: &str, key_data: &VaultKeyData) -> Result<()> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        self.run("vault_kv2_write_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            kv2::set(&vault.client, &self.kv_mount, path, key_data)
                .await
                .map(|_| ())
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| KmsError::backend_error(format!("Failed to store key in Vault: {e}")))
                })
        })
        .await?;

        debug!("Stored key {} in Vault at path {}", key_id, path);
        Ok(())
    }

    async fn store_key_metadata(&self, key_id: &str, request: &CreateKeyRequest) -> Result<()> {
        debug!("Storing key metadata for {}, input tags: {:?}", key_id, request.tags);

        // Read-modify-write under check-and-set: only the request-driven
        // fields change, everything else — most importantly the key material,
        // version and status — is carried over from the freshly read record,
        // so a rotation or state transition landing in between is preserved
        // instead of clobbered.
        self.update_key_data_with_cas(key_id, |key_data| {
            // A key that was just created must already carry material; an empty value means
            // the create flow failed to persist it. Fail closed instead of minting replacement
            // material: silently generating a new key here would mask the broken create and
            // orphan any DEK already wrapped by a different copy of this key.
            if key_data.encrypted_key_material.is_empty() {
                warn!(key_id, "Vault KMS key metadata missing encrypted key material");
                return Err(KmsError::material_missing(key_id));
            }

            key_data.usage = request.key_usage.clone();
            key_data.description = request.description.clone();
            key_data.tags = request.tags.clone();
            Ok(CasMutation::Write(()))
        })
        .await?;
        Ok(())
    }

    /// Retrieve key data from Vault
    async fn get_key_data(&self, key_id: &str) -> Result<VaultKeyData> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        let secret: VaultKeyData = self
            .run("vault_kv2_read_key", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                kv2::read(&vault.client, &self.kv_mount, path)
                    .await
                    .map_err(|e| AttemptError::from_vaultrs(e, |e| map_key_record_read_error(key_id, "key record", e)))
            })
            .await?;

        debug!("Retrieved key {} from Vault, tags: {:?}", key_id, secret.tags);
        Ok(secret)
    }

    /// List all keys stored in Vault
    async fn list_vault_keys(&self) -> Result<Vec<String>> {
        // List keys under the prefix; `None` means the prefix does not exist
        // yet (no keys were ever created).
        let keys = self
            .run("vault_kv2_list_keys", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match kv2::list(&vault.client, &self.kv_mount, &self.key_path_prefix).await {
                    Ok(keys) => Ok(Some(keys)),
                    Err(ClientError::ResponseWrapError) => Ok(None),
                    // The prefix holds nothing until the first key is created,
                    // which is where every deployment starts. A 404 that
                    // carries a Vault message instead means the request found
                    // no mount to answer it, and that is a configuration
                    // failure, not an empty listing.
                    Err(error) if is_empty_vault_list(&error) => Ok(None),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(describe_kv2_list_failure(&self.kv_mount, "keys", &e))
                    })),
                }
            })
            .await?;

        match keys {
            Some(keys) => {
                let keys = filter_key_directory_entries(keys);
                debug!("Found {} keys in Vault", keys.len());
                Ok(keys)
            }
            None => {
                debug!("Key path doesn't exist in Vault (404), returning empty list");
                Ok(Vec::new())
            }
        }
    }

    /// List the names of a key's immutable version records.
    ///
    /// `None` means the versions directory does not exist — the key was never
    /// rotated and has no version records. A 404 that carries a Vault message is
    /// not that: `delete_key` purges the version records this returns before it
    /// removes the key itself, so an unrouted path read as "no versions" would
    /// turn the purge into a no-op and leave master key material behind.
    async fn list_key_version_records(&self, key_id: &str) -> Result<Option<Vec<String>>> {
        let versions_dir = self.key_versions_dir(key_id);
        let versions_dir = versions_dir.as_str();
        self.run("vault_kv2_list_key_versions", OpClass::ReadIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::list(&vault.client, &self.kv_mount, versions_dir).await {
                Ok(versions) => Ok(Some(versions)),
                Err(ClientError::ResponseWrapError) => Ok(None),
                Err(error) if is_empty_vault_list(&error) => Ok(None),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(describe_kv2_list_failure(&self.kv_mount, "key version records", &e))
                })),
            }
        })
        .await
    }

    /// The version numbers of a key's immutable version records.
    ///
    /// Entries that are not version numbers are ignored: the rotation protocol
    /// only ever creates numeric records under the versions directory, so
    /// anything else is not part of the history this reasons about.
    async fn recorded_version_numbers(&self, key_id: &str) -> Result<Vec<u32>> {
        Ok(self
            .list_key_version_records(key_id)
            .await?
            .unwrap_or_default()
            .iter()
            .filter_map(|entry| entry.trim_end_matches('/').parse::<u32>().ok())
            .collect())
    }

    /// Fail closed when the version history contradicts the key record.
    ///
    /// Both contradictions below mean the record no longer describes the history
    /// the rotation protocol produced, and rotating on top of either would make
    /// the inconsistency permanent.
    ///
    /// * **Version records without a baseline.** The first rotation freezes the
    ///   then-current material as a version record and pins `baseline_version` to
    ///   it in the same commit, so records can only exist without a baseline if
    ///   the baseline was dropped afterwards. A node older than versioned rotation
    ///   does exactly that: it does not know the field, and every lifecycle write
    ///   rewrites the whole record, so one enable/disable/tag from such a node
    ///   erases it. Rotating here would freeze a *new* baseline at the current
    ///   version and permanently resolve every pre-versioning envelope to material
    ///   that never wrapped it — the point of no return this guard blocks.
    /// * **A record more than one version above current.** A record exactly one
    ///   above current is the footprint of an interrupted rotation (material
    ///   persisted, pointer switch never committed) and is recovered by the next
    ///   rotation's adopt path. Anything further ahead cannot come from the
    ///   rotation protocol: it means the top-level record regressed (for example a
    ///   historical lost update rolled back committed rotations), and extending the
    ///   history from the rolled-back state would re-mint version numbers that
    ///   already have immutable records.
    async fn ensure_version_history_consistent(&self, key_id: &str, key_data: &VaultKeyData) -> Result<()> {
        let recorded = self.recorded_version_numbers(key_id).await?;

        if key_data.baseline_version.is_none()
            && let Some(oldest_recorded) = recorded.iter().min().copied()
        {
            warn!(
                key_id,
                oldest_recorded, "Vault KMS key has version records but no baseline version; refusing to rotate"
            );
            return Err(KmsError::baseline_version_lost(key_id, oldest_recorded));
        }

        let current_version = key_data.version;
        match recorded.iter().max().copied() {
            Some(max_recorded) if max_recorded > current_version.saturating_add(1) => Err(KmsError::internal_error(format!(
                "current version {current_version} of key {key_id} is behind existing version record {max_recorded}; refusing to extend an inconsistent version history"
            ))),
            _ => Ok(()),
        }
    }

    /// Physically delete a key from Vault storage
    async fn delete_key(&self, key_id: &str) -> Result<()> {
        let path = self.key_path(key_id);
        let path = path.as_str();

        // Purge immutable version records first: if any purge fails, the top-level
        // record still exists and the deletion can be retried. The reverse order
        // would leave orphaned master key material in Vault after the key vanished.
        let versions_dir = self.key_versions_dir(key_id);
        let versions_dir = versions_dir.as_str();
        // `None` means no version records exist (the key was never rotated).
        let versions = self.list_key_version_records(key_id).await?;
        for version in versions.unwrap_or_default() {
            let version_path = format!("{versions_dir}/{version}");
            let version_path = version_path.as_str();
            self.run("vault_kv2_delete_key_version", OpClass::MutatingNonIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match kv2::delete_metadata(&vault.client, &self.kv_mount, version_path).await {
                    // A version record that is already gone is a completed
                    // delete (e.g. this deletion is being re-run after a lost
                    // response), not a failure.
                    Ok(_) | Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(()),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to delete key version record from Vault: {e}"))
                    })),
                }
            })
            .await?;
        }

        // For this specific key path, we can safely delete the metadata
        // since each key has its own unique path under the prefix
        self.run("vault_kv2_delete_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            kv2::delete_metadata(&vault.client, &self.kv_mount, path).await.map_err(|e| {
                AttemptError::from_vaultrs(e, |e| match e {
                    ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                    e => KmsError::backend_error(format!("Failed to delete key metadata from Vault: {e}")),
                })
            })
        })
        .await?;

        debug!("Permanently deleted key {} metadata from Vault at path {}", key_id, path);
        Ok(())
    }
}

impl VaultKmsClient {
    pub(crate) async fn generate_data_key(
        &self,
        request: &GenerateKeyRequest,
        _context: Option<&OperationContext>,
    ) -> Result<DataKeyInfo> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        let key_data = self.get_key_data(&request.master_key_id).await?;
        ensure_key_status_permits(&request.master_key_id, &key_data.status, StateGatedOperation::GenerateDataKey)?;

        // Generate random data key material using the existing method
        let plaintext_key = generate_key_material(&request.key_spec)?;

        // Encrypt the data key with the current master key material. Single read of
        // the key record: the material we wrap with and the version we stamp into
        // the envelope must come from the same snapshot, or a concurrent rotation
        // could stamp a version that never wrapped this DEK.
        let key_data = self.get_key_data(&request.master_key_id).await?;
        let key_material =
            decode_stored_key_material(&request.master_key_id, &key_data.encrypted_key_material).inspect_err(|error| {
                warn!(key_id = %request.master_key_id, %error, "Vault KMS key material failed validation");
            })?;
        self.consume_wrap_budget(&request.master_key_id, key_data.version).await;
        let (encrypted_key, nonce) = self.dek_crypto.encrypt(&key_material, &plaintext_key).await?;

        // Create data key envelope with master key version for rotation support
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            master_key_version: Some(key_data.version),
        };

        // Serialize the envelope as the ciphertext
        let ciphertext = serde_json::to_vec(&envelope)?;

        let data_key = DataKeyInfo::new(envelope.key_id, 1, Some(plaintext_key), ciphertext, request.key_spec.clone());

        debug!(key_id = %request.master_key_id, "Vault KMS data key generated");
        Ok(data_key)
    }

    pub(crate) async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Single read of the key record: the material we wrap with and the
        // version stamped into the envelope must come from the same snapshot
        // (see generate_data_key).
        let key_data = self.get_key_data(&request.key_id).await?;
        ensure_key_status_permits(&request.key_id, &key_data.status, StateGatedOperation::Encrypt)?;
        let key_material = decode_stored_key_material(&request.key_id, &key_data.encrypted_key_material)
            .inspect_err(|error| warn!(key_id = %request.key_id, %error, "Vault KMS key material failed validation"))?;
        self.consume_wrap_budget(&request.key_id, key_data.version).await;
        let (encrypted_key, nonce) = self.dek_crypto.encrypt(&key_material, &request.plaintext).await?;

        // Wrap the ciphertext in the same authenticated envelope that
        // generate_data_key emits, so decrypt() round-trips it and resolves
        // the wrapping master key version after rotations.
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.key_id.clone(),
            key_spec: "AES_256".to_string(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            master_key_version: Some(key_data.version),
        };
        let ciphertext = serde_json::to_vec(&envelope)?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: key_data.version,
            algorithm: key_data.algorithm,
        })
    }

    /// Open a data-key envelope, returning the plaintext and the master key
    /// that wrapped it.
    pub(crate) async fn decrypt(
        &self,
        request: &DecryptRequest,
        _context: Option<&OperationContext>,
    ) -> Result<(Vec<u8>, String)> {
        debug!("Decrypting data");

        // Parse the data key envelope from ciphertext
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;

        // NOTE: this comparison is an authorization check, not a cryptographic
        // binding. `DekCrypto` seals only the plaintext, so `encryption_context`
        // rides in the envelope unauthenticated: anyone able to rewrite the
        // stored envelope can rewrite this field and present a matching context.
        // The Static and Vault Transit backends do bind it (as AEAD AAD and as
        // the Transit KDF context respectively); closing the gap here needs a
        // versioned envelope, since existing ciphertext was sealed without AAD.
        // Verify encryption context matches
        // Check that all keys in envelope.encryption_context are present in request.encryption_context
        // and their values match. This ensures the context used for decryption matches what was used for encryption.
        for (key, expected_value) in &envelope.encryption_context {
            if let Some(actual_value) = request.encryption_context.get(key) {
                if actual_value != expected_value {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
            } else {
                // If request.encryption_context is empty, allow decryption (backward compatibility)
                // Otherwise, require all envelope context keys to be present
                if !request.encryption_context.is_empty() {
                    return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
                }
            }
        }

        // Decrypt the data key with the master key version that wrapped it
        let key_data = self.get_key_data(&envelope.master_key_id).await?;
        let version =
            resolve_envelope_master_key_version(envelope.master_key_version, key_data.baseline_version, key_data.version);
        let key_material = self
            .get_key_material_for_version(&envelope.master_key_id, &key_data, version)
            .await?;
        let plaintext = match self
            .dek_crypto
            .decrypt(&key_material, &envelope.encrypted_key, &envelope.nonce)
            .await
        {
            Ok(plaintext) => plaintext,
            Err(error) => {
                return Err(self
                    .explain_unwrap_failure(&envelope.master_key_id, &key_data, envelope.master_key_version, error)
                    .await);
            }
        };

        debug!("Vault KMS data decrypted");
        Ok((plaintext, envelope.master_key_id))
    }

    /// Report which master key version wraps an envelope, and whether that is
    /// the key's current version.
    ///
    /// Reads the key record only; it never unwraps anything, so it works on keys
    /// whose state forbids new cryptographic use — which is exactly the
    /// population a retirement inventory has to cover.
    pub(crate) async fn describe_data_key_wrapping(
        &self,
        request: &DescribeDataKeyWrappingRequest,
    ) -> Result<DescribeDataKeyWrappingResponse> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;
        ensure_rewrap_context_matches(&envelope.encryption_context, &request.encryption_context)?;

        let key_data = self.get_key_data(&envelope.master_key_id).await?;
        let current_version = key_data.version;

        Ok(DescribeDataKeyWrappingResponse {
            key_id: envelope.master_key_id,
            key_version: Some(resolve_envelope_master_key_version(
                envelope.master_key_version,
                key_data.baseline_version,
                current_version,
            )),
            current_key_version: Some(current_version),
            // Deliberately not `key_version == current_version`. A
            // pre-versioning envelope resolves to the current version while
            // saying nothing, and `rewrap_data_key` rewrites exactly those to
            // stamp the version — so reporting them as current here would leave
            // the sweep and the scan permanently disagreeing.
            is_current: envelope.master_key_version == Some(current_version),
        })
    }

    /// Re-wrap an existing envelope with the key's current master key version.
    ///
    /// The plaintext data key is unwrapped with the version that actually
    /// wrapped it and immediately re-wrapped with the current material. It is
    /// zeroized before this returns, never persisted, never logged and never
    /// handed to the caller: the whole point of rewrap is that the data key is
    /// re-protected without anyone above this layer holding it.
    ///
    /// Everything except the wrapping is carried over verbatim — the data key's
    /// own id, its spec, its encryption context and its creation time — so the
    /// result is the same data key under new wrapping. That keeps the DEK's
    /// recorded age honest and makes the operation invisible to every consumer
    /// of the envelope other than the version stamp.
    ///
    /// A pre-versioning envelope (no `master_key_version`) is rewrapped even
    /// when it resolves to the current version and its bytes would be unwrapped
    /// with the very material they are about to be re-wrapped with. That is not
    /// wasted work: the version stamp is the only evidence a retirement scan can
    /// read, and an envelope that does not state its version can never be
    /// counted as migrated.
    pub(crate) async fn rewrap_data_key(&self, request: &RewrapDataKeyRequest) -> Result<RewrapDataKeyResponse> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;
        ensure_rewrap_context_matches(&envelope.encryption_context, &request.encryption_context)?;

        // Single read of the key record: the version that unwraps, the material
        // that re-wraps and the version stamped into the result must all come
        // from one snapshot. Reading them separately would let a concurrent
        // rotation produce an envelope stamped with a version whose material
        // never wrapped it — an envelope that then fails to decrypt forever.
        let key_data = self.get_key_data(&envelope.master_key_id).await?;
        ensure_key_status_permits(&envelope.master_key_id, &key_data.status, StateGatedOperation::Encrypt)?;
        let current_version = key_data.version;

        if envelope.master_key_version == Some(current_version) {
            // Already on the current version and saying so. Hand the input back
            // untouched rather than producing an equivalent envelope with a
            // fresh nonce: a re-run of a sweep must converge to zero writes, and
            // the storage layer keys its write decision off these bytes.
            return Ok(RewrapDataKeyResponse {
                ciphertext: request.ciphertext.clone(),
                key_id: envelope.master_key_id,
                source_key_version: Some(current_version),
                destination_key_version: Some(current_version),
                rewrapped: false,
            });
        }

        // The re-wrap below encrypts with the current material under a fresh
        // random nonce — one wrap off the budget, counted before any plaintext
        // exists so the accounting never extends the plaintext's lifetime.
        self.consume_wrap_budget(&envelope.master_key_id, current_version).await;

        let source_version =
            resolve_envelope_master_key_version(envelope.master_key_version, key_data.baseline_version, current_version);
        // Both materials are resolved before anything is unwrapped, so no
        // fallible step sits between the plaintext data key coming into
        // existence and the zeroize that removes it again.
        let source_material = self
            .get_key_material_for_version(&envelope.master_key_id, &key_data, source_version)
            .await?;
        let destination_material = decode_stored_key_material(&envelope.master_key_id, &key_data.encrypted_key_material)
            .inspect_err(|error| warn!(key_id = %envelope.master_key_id, %error, "Vault KMS key material failed validation"))?;

        let mut plaintext_key = match self
            .dek_crypto
            .decrypt(&source_material, &envelope.encrypted_key, &envelope.nonce)
            .await
        {
            Ok(plaintext) => plaintext,
            Err(error) => {
                return Err(self
                    .explain_unwrap_failure(&envelope.master_key_id, &key_data, envelope.master_key_version, error)
                    .await);
            }
        };
        let rewrapped = self.dek_crypto.encrypt(&destination_material, &plaintext_key).await;
        plaintext_key.zeroize();
        let (encrypted_key, nonce) = rewrapped?;

        let rewrapped_envelope = DataKeyEnvelope {
            key_id: envelope.key_id,
            master_key_id: envelope.master_key_id,
            key_spec: envelope.key_spec,
            encrypted_key,
            nonce,
            encryption_context: envelope.encryption_context,
            created_at: envelope.created_at,
            master_key_version: Some(current_version),
        };
        let ciphertext = serde_json::to_vec(&rewrapped_envelope)?;

        debug!(key_id = %rewrapped_envelope.master_key_id, source_version, current_version, "Vault KMS data key rewrapped");
        Ok(RewrapDataKeyResponse {
            ciphertext,
            key_id: rewrapped_envelope.master_key_id,
            source_key_version: Some(source_version),
            destination_key_version: Some(current_version),
            rewrapped: true,
        })
    }

    /// Re-report a failure to unwrap a data key as the lost-baseline diagnosis
    /// when that is what the key record shows.
    ///
    /// Only a pre-versioning envelope (no `master_key_version`) read against a key
    /// with no `baseline_version` can qualify: every other envelope was unwrapped
    /// with the version it or the baseline named. Such an envelope resolves to the
    /// *current* version, which is correct for a key that was never rotated and
    /// wrong for one whose baseline was erased — and the immutable version records
    /// tell those two apart.
    ///
    /// Deliberately runs after the unwrap fails, never before it. A version-less
    /// envelope written by an old node *after* a rotation is genuinely wrapped with
    /// the current material and still decrypts, so refusing up front on the same
    /// evidence would break reads that work today. Nothing is risked by trying
    /// first: the wrapping is AES-256-GCM, so a wrong master key version cannot
    /// yield plaintext, only this authentication failure. The extra listing is
    /// therefore paid once per already-failing read instead of on every read of
    /// pre-versioning data.
    async fn explain_unwrap_failure(
        &self,
        key_id: &str,
        key_data: &VaultKeyData,
        envelope_version: Option<u32>,
        error: KmsError,
    ) -> KmsError {
        if envelope_version.is_some() || key_data.baseline_version.is_some() {
            return error;
        }

        match self.recorded_version_numbers(key_id).await {
            Ok(recorded) => match recorded.iter().min().copied() {
                Some(oldest_recorded) => {
                    warn!(
                        key_id,
                        oldest_recorded,
                        "Vault KMS pre-versioning data key failed to unwrap and the key has version records but no baseline version"
                    );
                    KmsError::baseline_version_lost(key_id, oldest_recorded)
                }
                // No version records: the key was never rotated by a versioning
                // build, so the current version really is the right one and the
                // failure has some other cause.
                None => error,
            },
            Err(list_error) => {
                warn!(key_id, %list_error, "Vault KMS could not list key version records while diagnosing a failed unwrap");
                error
            }
        }
    }

    pub(crate) async fn create_key(
        &self,
        key_id: &str,
        algorithm: &str,
        _context: Option<&OperationContext>,
    ) -> Result<MasterKeyInfo> {
        debug!("Creating master key: {} with algorithm: {}", key_id, algorithm);

        // Existence pre-check with read-confirm recovery: a create whose
        // response was lost gets retried by callers, and used to be
        // misreported as KeyAlreadyExists. If the stored key is exactly what
        // this create would have produced (same algorithm, active, usable
        // material), report the stored key as the create result. Anything
        // else keeps failing: create never adopts a key it would not have
        // produced. A failed pre-check read must fail the create rather than
        // fall through to a blind overwrite of a possibly existing key.
        match self.get_key_data(key_id).await {
            Ok(existing) => {
                return if existing.algorithm == algorithm
                    && existing.status == KeyStatus::Active
                    && decode_stored_key_material(key_id, &existing.encrypted_key_material).is_ok()
                {
                    info!(
                        key_id,
                        "Vault KMS create found an identical active key; treating it as a recovered create"
                    );
                    Ok(MasterKeyInfo {
                        key_id: key_id.to_string(),
                        version: existing.version,
                        algorithm: existing.algorithm,
                        usage: existing.usage,
                        status: existing.status,
                        description: existing.description,
                        metadata: existing.metadata,
                        created_at: existing.created_at,
                        rotated_at: existing.rotated_at,
                        created_by: None,
                        deletion_date: existing.deletion_date,
                    })
                } else {
                    Err(KmsError::key_already_exists(key_id))
                };
            }
            Err(KmsError::KeyNotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        // Generate key material
        let key_material = generate_key_material(algorithm)?;
        let encrypted_material = self.encrypt_key_material(&key_material).await?;

        // Create key data
        let key_data = VaultKeyData {
            algorithm: algorithm.to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            deletion_date: None,
            rotated_at: None,
            encrypted_key_material: encrypted_material,
            baseline_version: None,
            wrap_budget_reserved: 0,
        };

        // Create-only write: the not-found pre-check above is only advisory —
        // another node can create the same key in between — so the write
        // itself must refuse to overwrite. Exactly one of two concurrent
        // creates commits; the loser reports the key as already existing
        // instead of adopting material it did not persist.
        if !self.try_create_key_data(key_id, &key_data).await? {
            return Err(KmsError::key_already_exists(key_id));
        }

        let master_key = MasterKeyInfo {
            key_id: key_id.to_string(),
            version: key_data.version,
            algorithm: key_data.algorithm.clone(),
            usage: key_data.usage,
            status: key_data.status,
            description: None, // This method doesn't receive description parameter
            metadata: key_data.metadata.clone(),
            created_at: key_data.created_at,
            rotated_at: None,
            created_by: None,
            deletion_date: None,
        };

        debug!(key_id, "Vault KMS master key created");
        Ok(master_key)
    }

    pub(crate) async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let key_data = self.get_key_data(key_id).await?;

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            description: key_data.description,
            algorithm: key_data.algorithm,
            usage: key_data.usage,
            status: key_data.status,
            version: key_data.version,
            metadata: key_data.metadata,
            tags: key_data.tags,
            created_at: key_data.created_at,
            rotated_at: key_data.rotated_at,
            created_by: None,
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: Some(key_data.wrap_budget_reserved),
        })
    }

    pub(crate) async fn list_keys(
        &self,
        request: &ListKeysRequest,
        _context: Option<&OperationContext>,
    ) -> Result<ListKeysResponse> {
        debug!("Listing keys with limit: {:?}", request.limit);

        // A caller asking for no keys is answered without reaching Vault.
        if list_keys_page_size(request.limit).is_none() {
            return Ok(empty_key_page());
        }

        let mut all_keys = self.list_vault_keys().await?;
        // Vault's own LIST ordering is not part of its contract, so the sort is
        // what makes the marker a stable cursor across calls.
        all_keys.sort_unstable();
        let page = paginate_keys(&all_keys, request, String::as_str);

        let mut key_infos = Vec::with_capacity(page.items.len());
        let mut unreadable = UnreadableKeys::default();
        for key_id in page.items {
            let key_info = match self.describe_key(key_id, None).await {
                Ok(key_info) => {
                    unreadable.saw_readable();
                    key_info
                }
                Err(error) => match classify_listed_key_failure(&error) {
                    Some(ListedKeyFailure::Vanished) => {
                        debug!(key_id, "skipping key removed while listing");
                        continue;
                    }
                    Some(ListedKeyFailure::Unreadable) => {
                        warn!(key_id, %error, "listing a KV2 key record this build cannot describe");
                        unreadable.record(key_id, error);
                        continue;
                    }
                    None => return Err(error),
                },
            };
            if request
                .status_filter
                .as_ref()
                .is_some_and(|status| status != &key_info.status)
            {
                continue;
            }
            if request.usage_filter.as_ref().is_some_and(|usage| usage != &key_info.usage) {
                continue;
            }
            key_infos.push(key_info);
        }

        Ok(ListKeysResponse {
            keys: key_infos,
            next_marker: page.next_marker,
            truncated: page.truncated,
            unreadable_key_ids: unreadable.into_reported_ids(!page.truncated && started_at_the_first_key(request))?,
        })
    }

    pub(crate) async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        self.update_key_data_with_cas(key_id, |key_data| {
            ensure_key_status_permits(key_id, &key_data.status, StateGatedOperation::Enable)?;
            key_data.status = KeyStatus::Active;
            Ok(CasMutation::Write(()))
        })
        .await?;

        debug!(key_id, "Vault KMS key enabled");
        Ok(())
    }

    pub(crate) async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        self.update_key_data_with_cas(key_id, |key_data| {
            ensure_key_status_permits(key_id, &key_data.status, StateGatedOperation::Disable)?;
            key_data.status = KeyStatus::Disabled;
            Ok(CasMutation::Write(()))
        })
        .await?;

        debug!(key_id, "Vault KMS key disabled");
        Ok(())
    }

    /// Replace the key's description; `None` clears it.
    ///
    /// The write goes through the check-and-set read-modify-write loop, so a
    /// rotation or state transition landing in between is carried over instead
    /// of clobbered. A description that already matches is not rewritten.
    pub(crate) async fn update_key_description(&self, key_id: &str, description: Option<&str>) -> Result<()> {
        self.update_key_data_with_cas(key_id, |key_data| {
            if key_data.description.as_deref() == description {
                return Ok(CasMutation::Skip(()));
            }
            key_data.description = description.map(str::to_string);
            Ok(CasMutation::Write(()))
        })
        .await?;

        debug!(key_id, "Vault KMS key description updated");
        Ok(())
    }

    /// Add or overwrite tags, leaving every other tag untouched.
    pub(crate) async fn tag_key(&self, key_id: &str, tags: &HashMap<String, String>) -> Result<()> {
        ensure_tag_keys_are_mutable(tags.keys().map(String::as_str))?;

        self.update_key_data_with_cas(key_id, |key_data| {
            let mut changed = false;
            for (tag_key, value) in tags {
                changed |= key_data.tags.insert(tag_key.clone(), value.clone()).as_ref() != Some(value);
            }
            Ok(if changed {
                CasMutation::Write(())
            } else {
                CasMutation::Skip(())
            })
        })
        .await?;

        debug!(key_id, "Vault KMS key tags updated");
        Ok(())
    }

    /// Remove tags; tags that are not set are ignored.
    pub(crate) async fn untag_key(&self, key_id: &str, tag_keys: &[String]) -> Result<()> {
        ensure_tag_keys_are_mutable(tag_keys.iter().map(String::as_str))?;

        self.update_key_data_with_cas(key_id, |key_data| {
            let mut changed = false;
            for tag_key in tag_keys {
                changed |= key_data.tags.remove(tag_key).is_some();
            }
            Ok(if changed {
                CasMutation::Write(())
            } else {
                CasMutation::Skip(())
            })
        })
        .await?;

        debug!(key_id, "Vault KMS key tags removed");
        Ok(())
    }

    /// Rotate the master key while keeping every historical version decryptable.
    ///
    /// Commit protocol (all writes check-and-set, in this order):
    /// 1. First rotation only: freeze the current material as an immutable version
    ///    record and persist `baseline_version` so pre-versioning envelopes resolve
    ///    to it deterministically.
    /// 2. Persist the next version's material as an immutable version record
    ///    (create-only) before anything references it.
    /// 3. Switch the current pointer: bump `version` and mirror the new material
    ///    into the top-level record in a single check-and-set write.
    ///
    /// If any step fails the current pointer is untouched, so a failed, cancelled,
    /// or interrupted rotation never exposes half-committed material. Concurrent
    /// rotations are serialized by the check-and-set writes: at most one caller
    /// commits each version and the losers fail without side effects on current.
    ///
    /// The whole protocol is refused up front, before any write, when the persisted
    /// version history contradicts the key record — see
    /// [`Self::ensure_version_history_consistent`].
    pub(crate) async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        debug!("Rotating master key: {}", key_id);

        let (mut cas, mut key_data) = self.get_key_data_versioned(key_id).await?;
        ensure_key_status_permits(key_id, &key_data.status, StateGatedOperation::Rotate)?;

        // The material about to be frozen must be decodable: freezing poisoned
        // material would give legacy envelopes a permanently broken baseline. This
        // surfaces the same typed Material* errors as the read path.
        decode_stored_key_material(key_id, &key_data.encrypted_key_material)
            .inspect_err(|error| warn!(key_id, %error, "Vault KMS key material failed validation"))?;

        // The version history must still agree with the key record: a history
        // that extends past what this rotation would commit means the current
        // pointer regressed, and a history without a baseline means the baseline
        // was erased after the fact. Both fail closed before any write.
        self.ensure_version_history_consistent(key_id, &key_data).await?;

        // Step 1: freeze the baseline on first rotation.
        if key_data.baseline_version.is_none() {
            let baseline = VaultKeyVersionRecord {
                version: key_data.version,
                encrypted_key_material: key_data.encrypted_key_material.clone(),
                created_at: key_data.created_at.clone(),
            };
            if !self.try_create_key_version_record(key_id, &baseline).await? {
                // Either a previous rotation attempt crashed between freezing the
                // baseline and recording it in metadata, or a concurrent rotation
                // got here first. Both are benign only if the existing record holds
                // exactly the material being frozen; anything else means the
                // version history is inconsistent and rotation must not proceed.
                let existing = self.get_key_version_record(key_id, key_data.version).await?;
                if existing.encrypted_key_material != key_data.encrypted_key_material {
                    return Err(KmsError::internal_error(format!(
                        "version record {} of key {key_id} does not match the current key material; refusing to rotate",
                        key_data.version
                    )));
                }
            }
            key_data.baseline_version = Some(key_data.version);
            cas = self.cas_store_key_data(key_id, &key_data, cas).await?;
        }

        // Step 2: durably persist the next version's material before it can become
        // current.
        let new_version = key_data
            .version
            .checked_add(1)
            .ok_or_else(|| KmsError::internal_error(format!("key {key_id} exhausted the version space")))?;
        let generated = generate_key_material(&key_data.algorithm)?;
        let mut new_material = self.encrypt_key_material(&generated).await?;
        let record = VaultKeyVersionRecord {
            version: new_version,
            encrypted_key_material: new_material.clone(),
            created_at: Zoned::now(),
        };
        if !self.try_create_key_version_record(key_id, &record).await? {
            // A record for the next version already exists: an interrupted rotation
            // persisted it and stopped before switching the current pointer, or a
            // concurrent rotation just created it. Adopt the persisted material —
            // it is immutable, fully durable, and has never been current — instead
            // of failing the create-only write forever. The check-and-set switch
            // below still lets at most one caller commit this version.
            let existing = self.get_key_version_record(key_id, new_version).await?;
            decode_stored_key_material(key_id, &existing.encrypted_key_material)?;
            new_material = existing.encrypted_key_material;
        }

        // Step 3: switch the current pointer. The top-level copy of the material is
        // the fast path for new encryptions and must always match `version`.
        //
        // The rotation timestamp rides along on this same write: it marks the
        // moment the new material became current, and persisting it here means it
        // commits if and only if the rotation does. A rotation that fails after
        // freezing the version record leaves the key unrotated and unstamped, so
        // the recorded time never runs ahead of the current version.
        key_data.version = new_version;
        key_data.encrypted_key_material = new_material;
        key_data.rotated_at = Some(Zoned::now());
        // Fresh material, fresh AES-GCM nonce budget: the wrap ceiling is per
        // key material version, so the counter restarts with the same commit
        // that makes the new material current.
        key_data.wrap_budget_reserved = 0;
        self.cas_store_key_data(key_id, &key_data, cas).await?;

        info!(key_id, version = new_version, "Vault KMS master key rotated");

        Ok(MasterKeyInfo {
            key_id: key_id.to_string(),
            version: new_version,
            algorithm: key_data.algorithm.clone(),
            usage: key_data.usage.clone(),
            status: key_data.status,
            description: key_data.description.clone(),
            metadata: key_data.metadata.clone(),
            created_at: key_data.created_at.clone(),
            // The persisted value, not a fresh `now()`: what the caller is told
            // must be what a later describe of the same key reports.
            rotated_at: key_data.rotated_at.clone(),
            created_by: None,
            deletion_date: key_data.deletion_date.clone(),
        })
    }

    pub(crate) async fn health_check(&self) -> Result<()> {
        debug!("Performing Vault health check");

        // `list_vault_keys` already reports the empty-prefix 404 as an empty
        // listing, which is the state every deployment starts in. Anything that
        // reaches here is a real failure and must fail the check that gates
        // startup — including the 404 from a `kv_mount` with no engine behind
        // it, which no listing can be served from.
        match self.list_vault_keys().await {
            Ok(_) => {
                debug!("Vault health check passed - successfully listed keys");
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "Vault KMS health check failed");
                Err(e)
            }
        }
    }
}

/// VaultKmsBackend wraps VaultKmsClient and implements the KmsBackend trait
pub struct VaultKmsBackend {
    client: VaultKmsClient,
}

impl VaultKmsBackend {
    /// Create a new VaultKmsBackend
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate()?;

        let vault_config = match &config.backend_config {
            crate::config::BackendConfig::VaultKv2(vault_config) => (**vault_config).clone(),
            crate::config::BackendConfig::Local(_)
            | crate::config::BackendConfig::VaultTransit(_)
            | crate::config::BackendConfig::Static(_)
            | crate::config::BackendConfig::Aws(_) => {
                return Err(KmsError::configuration_error("Expected Vault KV2 backend configuration"));
            }
        };

        let client = VaultKmsClient::new(vault_config, &config).await?;
        Ok(Self { client })
    }

    /// Spawn the background credential renewal task for this backend, if its
    /// auth method issues lease-bound tokens. The caller owns the returned
    /// handle; dropping it cancels the task.
    pub(crate) fn spawn_credential_renewal(&self) -> Option<CredentialTaskHandle> {
        self.client.credentials.spawn_renewal_task()
    }

    /// Mark a key `PendingDeletion` with the given deadline, under
    /// check-and-set with per-attempt re-validation.
    ///
    /// The state gate re-runs on every attempt, so a transition that lost a
    /// race (for example against a concurrent schedule or rotation) is
    /// re-validated against the committed state. Refuses to write back a
    /// record whose key material is missing: persisting it would cement the
    /// empty-material state under a fresh document version — a damaged key
    /// must go through an explicit repair operation, not a lifecycle update.
    async fn mark_key_pending_deletion(&self, key_id: &str, deletion_date: &Zoned) -> Result<()> {
        self.client
            .update_key_data_with_cas(key_id, |key_data| {
                ensure_key_status_permits(key_id, &key_data.status, StateGatedOperation::ScheduleDeletion)?;
                if key_data.encrypted_key_material.is_empty() {
                    return Err(KmsError::material_missing(key_id));
                }
                key_data.status = KeyStatus::PendingDeletion;
                key_data.deletion_date = Some(deletion_date.clone());
                Ok(CasMutation::Write(()))
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl KmsBackend for VaultKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Create key in Vault transit engine
        let _master_key = self.client.create_key(&key_id, "AES_256", None).await?;

        // Also store key metadata in KV store with tags
        self.client.store_key_metadata(&key_id, &request).await?;

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: request.tags,
        };

        Ok(CreateKeyResponse {
            key_id,
            key_metadata: metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let encrypt_request = crate::types::EncryptRequest {
            key_id: request.key_id.clone(),
            plaintext: request.plaintext,
            encryption_context: request.encryption_context,
            grant_tokens: request.grant_tokens,
        };

        let response = self.client.encrypt(&encrypt_request, None).await?;

        Ok(EncryptResponse {
            ciphertext: response.ciphertext,
            key_id: response.key_id,
            key_version: response.key_version,
            algorithm: response.algorithm,
        })
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let (plaintext, key_id) = self.client.decrypt(&request, None).await?;

        Ok(DecryptResponse {
            plaintext,
            key_id,
            encryption_algorithm: Some("AES-256-GCM".to_string()),
        })
    }

    async fn rewrap_data_key(&self, request: RewrapDataKeyRequest) -> Result<RewrapDataKeyResponse> {
        self.client.rewrap_data_key(&request).await
    }

    async fn describe_data_key_wrapping(
        &self,
        request: DescribeDataKeyWrappingRequest,
    ) -> Result<DescribeDataKeyWrappingResponse> {
        self.client.describe_data_key_wrapping(&request).await
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let generate_request = GenerateKeyRequest {
            master_key_id: request.key_id.clone(),
            key_spec: request.key_spec.as_str().to_string(),
            key_length: Some(request.key_spec.key_size() as u32),
            encryption_context: request.encryption_context,
            grant_tokens: Vec::new(),
        };

        let mut data_key = self.client.generate_data_key(&generate_request, None).await?;

        // Fields are taken, not destructured or cloned: `DataKeyInfo` has a
        // `Drop` impl, and a clone would leave a second un-zeroized plaintext
        // DEK on the heap.
        let plaintext_key = data_key
            .plaintext
            .take()
            .ok_or_else(|| KmsError::internal_error("Generated data key is missing plaintext"))?;
        Ok(GenerateDataKeyResponse {
            key_id: request.key_id,
            plaintext_key,
            ciphertext_blob: std::mem::take(&mut data_key.ciphertext),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        let key_info = self.client.describe_key(&request.key_id, None).await?;

        // Also get key metadata from KV store to retrieve tags
        let key_data = self.client.get_key_data(&request.key_id).await?;

        let metadata = KeyMetadata {
            key_id: key_info.key_id,
            key_state: match key_info.status {
                KeyStatus::Active => KeyState::Enabled,
                KeyStatus::Disabled => KeyState::Disabled,
                KeyStatus::PendingDeletion => KeyState::PendingDeletion,
                KeyStatus::Deleted => KeyState::Unavailable,
            },
            key_usage: key_info.usage,
            description: key_info.description,
            creation_date: key_info.created_at,
            deletion_date: key_data.deletion_date.clone(),
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: key_data.tags,
        };

        Ok(DescribeKeyResponse { key_metadata: metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        let response = self.client.list_keys(&request, None).await?;
        Ok(response)
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        // For Vault backend, we'll mark keys for deletion but not physically delete them
        // This allows for recovery during the pending window
        let key_id = &request.key_id;

        // First, check if the key exists and get its metadata
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {key_id} not found")));
            }
        };

        let deletion_date = if request.force_immediate.unwrap_or(false) {
            // Check if key is already in PendingDeletion state (or a tombstone
            // left by a crashed removal, which may simply be completed)
            if key_metadata.key_state == KeyState::PendingDeletion || key_metadata.key_state == KeyState::Unavailable {
                // Tombstone first: mark the record Deleted before removing it,
                // so a crash between the two steps leaves a key that is already
                // unusable and whose removal can simply be re-run. Written
                // check-and-set with re-validation: a concurrent cancellation
                // that commits first wins and fails this removal instead of
                // being overwritten.
                self.client
                    .update_key_data_with_cas(key_id, |key_data| match key_data.status {
                        KeyStatus::Deleted => Ok(CasMutation::Skip(())),
                        KeyStatus::PendingDeletion => {
                            key_data.status = KeyStatus::Deleted;
                            Ok(CasMutation::Write(()))
                        }
                        KeyStatus::Active | KeyStatus::Disabled => {
                            Err(KmsError::invalid_key_state(format!("Key {key_id} is no longer pending deletion")))
                        }
                    })
                    .await?;
                // Force immediate deletion: physically delete the key from Vault storage
                self.client.delete_key(key_id).await?;

                // Return empty deletion_date to indicate key was permanently deleted
                None
            } else {
                // For non-pending keys, mark as PendingDeletion
                let marked_at = Zoned::now();
                self.mark_key_pending_deletion(key_id, &marked_at).await?;
                key_metadata.key_state = KeyState::PendingDeletion;
                key_metadata.deletion_date = Some(marked_at);

                None
            }
        } else {
            // Schedule for deletion (default 30 days)
            ensure_key_state_permits(key_id, &key_metadata.key_state, StateGatedOperation::ScheduleDeletion)?;

            // Defensive: KmsManager::delete_key is the enforcement point for the
            // waiting window and rejects out-of-range requests before any
            // backend runs. This repeats the bound for callers holding a backend
            // handle directly (tests, maintenance tasks).
            let days = request.pending_window_in_days.unwrap_or(DEFAULT_PENDING_DELETION_WINDOW_DAYS);
            if !(MIN_PENDING_DELETION_WINDOW_DAYS..=MAX_PENDING_DELETION_WINDOW_DAYS).contains(&days) {
                return Err(crate::error::KmsError::invalid_parameter(format!(
                    "pending_window_in_days must be between {MIN_PENDING_DELETION_WINDOW_DAYS} and {MAX_PENDING_DELETION_WINDOW_DAYS}"
                )));
            }

            let deletion_date = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            self.mark_key_pending_deletion(key_id, &deletion_date).await?;
            key_metadata.key_state = KeyState::PendingDeletion;
            key_metadata.deletion_date = Some(deletion_date.clone());

            Some(deletion_date.to_string())
        };

        Ok(DeleteKeyResponse {
            key_id: key_id.clone(),
            deletion_date,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = &request.key_id;

        // Check if the key exists and is pending deletion
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {key_id} not found")));
            }
        };

        if key_metadata.key_state != KeyState::PendingDeletion {
            return Err(crate::error::KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
        }

        // Persist the reset state back to Vault. Without this the key stays PendingDeletion in
        // storage and would still be reaped, so we must fail the request if the write fails
        // rather than report a false success. Check-and-set with per-attempt
        // re-validation: once the deletion sweep has tombstoned the record, the
        // cancellation must fail instead of resurrecting a key whose material
        // is about to be (or already is) destroyed.
        self.client
            .update_key_data_with_cas(key_id, |key_data| match key_data.status {
                KeyStatus::PendingDeletion => {
                    if key_data.encrypted_key_material.is_empty() {
                        return Err(KmsError::material_missing(key_id));
                    }
                    key_data.status = KeyStatus::Active;
                    key_data.deletion_date = None;
                    Ok(CasMutation::Write(()))
                }
                KeyStatus::Active | KeyStatus::Disabled | KeyStatus::Deleted => {
                    Err(crate::error::KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")))
                }
            })
            .await?;

        // Cancel the deletion by resetting the state
        key_metadata.key_state = KeyState::Enabled;
        key_metadata.deletion_date = None;

        Ok(CancelKeyDeletionResponse {
            key_id: key_id.clone(),
            key_metadata,
        })
    }

    async fn enable_key(&self, key_id: &str) -> Result<()> {
        self.client.enable_key(key_id, None).await
    }

    async fn disable_key(&self, key_id: &str) -> Result<()> {
        self.client.disable_key(key_id, None).await
    }

    async fn rotate_key(&self, key_id: &str) -> Result<()> {
        self.client.rotate_key(key_id, None).await.map(|_| ())
    }

    async fn update_key_description(&self, key_id: &str, description: Option<&str>) -> Result<()> {
        self.client.update_key_description(key_id, description).await
    }

    async fn tag_key(&self, key_id: &str, tags: &HashMap<String, String>) -> Result<()> {
        self.client.tag_key(key_id, tags).await
    }

    async fn untag_key(&self, key_id: &str, tag_keys: &[String]) -> Result<()> {
        self.client.untag_key(key_id, tag_keys).await
    }

    async fn health_check(&self) -> Result<bool> {
        self.client.health_check().await.map(|_| true)
    }

    fn capabilities(&self) -> BackendCapabilities {
        // Rotation freezes the outgoing material as an immutable version
        // record before switching the current pointer, and envelopes resolve
        // their wrapping version on decrypt, so every historical version
        // stays decryptable after a rotation. Those same immutable records are
        // what lets an envelope be unwrapped with the version that wrapped it
        // and re-wrapped onto the current one.
        BackendCapabilities::minimal()
            .with_rotate(true)
            .with_enable_disable(true)
            .with_schedule_deletion(true)
            .with_versioning(true)
            .with_physical_delete(true)
            .with_update_key_metadata(true)
            .with_rewrap(true)
    }

    async fn remove_expired_key(&self, key_id: &str, now: &Zoned) -> Result<ExpiredKeyRemoval> {
        // Tombstone under check-and-set with per-attempt re-validation: a
        // cancellation landing between the read and the write makes the write
        // conflict, and the re-read then observes the cancelled state and
        // reports StateChanged instead of overwriting it.
        let settled = self
            .client
            .update_key_data_with_cas(key_id, |key_data| match key_data.status {
                // Tombstone left by a crashed removal: complete it.
                KeyStatus::Deleted => Ok(CasMutation::Skip(None)),
                KeyStatus::PendingDeletion => match &key_data.deletion_date {
                    Some(deadline) if deadline <= now => {
                        // Tombstone first: mark the record Deleted before
                        // removing it, so a crash between the two steps leaves
                        // a key that is already unusable and whose removal can
                        // simply be re-run.
                        key_data.status = KeyStatus::Deleted;
                        Ok(CasMutation::Write(None))
                    }
                    // Not yet due, or a legacy record without a persisted
                    // deadline — never auto-remove those.
                    _ => Ok(CasMutation::Skip(Some(ExpiredKeyRemoval::NotExpired))),
                },
                KeyStatus::Active | KeyStatus::Disabled => Ok(CasMutation::Skip(Some(ExpiredKeyRemoval::StateChanged))),
            })
            .await;
        match settled {
            Ok((_, Some(outcome))) => return Ok(outcome),
            Ok((_, None)) => {}
            Err(KmsError::KeyNotFound { .. }) => return Ok(ExpiredKeyRemoval::Removed),
            Err(error) => return Err(error),
        }

        match self.client.delete_key(key_id).await {
            Ok(()) | Err(KmsError::KeyNotFound { .. }) => {
                debug!(key_id, "Vault KV2 expired key removed");
                Ok(ExpiredKeyRemoval::Removed)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::scripted_vault::{ScriptedResponse, ScriptedVault};
    use crate::config::{VaultAuthMethod, VaultConfig};

    const SCRIPTED_RETRY_ATTEMPTS: u32 = 3;

    /// Vault + KMS config pair pointing at a scripted loopback Vault.
    fn scripted_configs(address: &str) -> (VaultConfig, KmsConfig) {
        let vault_config = VaultConfig {
            address: address.to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "scripted-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        };
        let kms_config = KmsConfig {
            timeout: Duration::from_secs(5),
            retry_attempts: SCRIPTED_RETRY_ATTEMPTS,
            ..KmsConfig::default()
        };
        (vault_config, kms_config)
    }

    async fn scripted_client(responses: Vec<ScriptedResponse>) -> (ScriptedVault, VaultKmsClient) {
        let vault = ScriptedVault::serve(responses).await;
        let (vault_config, kms_config) = scripted_configs(&vault.address);
        let client = VaultKmsClient::new(vault_config, &kms_config)
            .await
            .expect("scripted Vault client");
        (vault, client)
    }

    async fn scripted_kv2_client(key_data: &VaultKeyData) -> (ScriptedVault, VaultKmsClient) {
        let vault = ScriptedVault::serve_kv2(
            "rustfs/kms/keys/wired-key",
            serde_json::to_value(key_data).expect("serialize scripted KV2 key"),
        )
        .await;
        let (vault_config, kms_config) = scripted_configs(&vault.address);
        let client = VaultKmsClient::new(vault_config, &kms_config)
            .await
            .expect("scripted Vault client");
        (vault, client)
    }

    fn healthy_key_data() -> VaultKeyData {
        VaultKeyData {
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            deletion_date: None,
            rotated_at: None,
            encrypted_key_material: general_purpose::STANDARD.encode([0x42u8; 32]),
            baseline_version: None,
            wrap_budget_reserved: 0,
        }
    }

    /// The 404 a Vault LIST answers with when no mount is routed at the path.
    /// The message names the route, exactly as Vault writes it — so a test that
    /// wants to prove the mount name was interpolated into an error cannot look
    /// for the bare mount name, which this payload already contains.
    fn missing_mount_404() -> ScriptedResponse {
        ScriptedResponse::error(404, "no handler for route \"secret/metadata/rustfs/kms/keys/\". route entry not found.")
    }

    /// KV2 read payload (the `data` field of the Vault envelope) for a key record.
    fn kv2_read_data(key_data: &VaultKeyData) -> serde_json::Value {
        serde_json::json!({
            "data": serde_json::to_value(key_data).expect("serialize key data"),
            "metadata": {
                "created_time": "2026-01-01T00:00:00Z",
                "deletion_time": "",
                "custom_metadata": null,
                "destroyed": false,
                "version": 1,
            },
        })
    }

    /// A caller asking for no keys gets an empty page, and the page arithmetic
    /// never reaches for the element before an empty page. The scripted key
    /// listing stays unused: a request for zero keys has nothing to ask Vault.
    #[tokio::test]
    async fn zero_limit_list_returns_an_empty_page_without_calling_vault() {
        let (vault, client) =
            scripted_client(vec![ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a", "key-b"] }))]).await;

        let response = client
            .list_keys(
                &ListKeysRequest {
                    limit: Some(0),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("a zero-limit list must succeed");

        assert!(response.keys.is_empty());
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());
        assert!(
            vault.requests().is_empty(),
            "a request for no keys must not reach Vault: {:?}",
            vault.requests()
        );
    }

    /// A KV2 record that does not deserialize is a property of that one key, so
    /// the listing names it and keeps going. Dropping it silently — which is
    /// what this backend used to do for every describe failure — answered "these
    /// are your keys" with a set that omitted one, and the deletion sweep took
    /// its census over that partial set.
    #[tokio::test]
    async fn wired_list_reports_an_undeserializable_record_and_keeps_the_rest() {
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a", "key-b"] })),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // `algorithm` is typed as a string; a number makes the record
            // undecodable exactly as a newer or damaged writer would.
            ScriptedResponse::ok(serde_json::json!({
                "data": { "algorithm": 42 },
                "metadata": { "created_time": "2026-01-01T00:00:00Z", "deletion_time": "", "custom_metadata": null, "destroyed": false, "version": 1 },
            })),
        ])
        .await;

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("one undecodable record must not fail the whole listing");

        assert_eq!(response.keys.len(), 1, "the readable key must still be listed");
        assert_eq!(response.unreadable_key_ids, vec!["key-b".to_string()]);
    }

    /// The counterpart: an error that says nothing about a specific key must not
    /// be reported as a damaged key. A Vault outage that listed itself as mass
    /// key corruption would send an operator hunting for a data-loss event that
    /// never happened, and would let the sweep proceed on a key set it could not
    /// actually read.
    #[tokio::test]
    async fn wired_list_fails_when_the_backend_itself_is_unavailable() {
        let mut responses = vec![
            ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a", "key-b"] })),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
        ];
        for _ in 0..SCRIPTED_RETRY_ATTEMPTS {
            responses.push(ScriptedResponse::error(503, "temporarily unavailable"));
        }
        let (_vault, client) = scripted_client(responses).await;

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("an unreachable backend must fail the listing, not shrink it");
        assert!(
            matches!(error, KmsError::BackendError { .. }),
            "a transient backend failure must not be reported as a damaged record: {error:?}"
        );
    }

    /// A 404 whose `errors` array is empty is Vault reporting an empty prefix,
    /// which is where every deployment starts: no key has been created yet, so
    /// the health check that gates KMS startup must pass. Failing it would keep
    /// a first-ever deployment from ever starting.
    #[tokio::test]
    async fn health_check_passes_on_an_empty_kv2_prefix() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::empty_list_404()]).await;

        client
            .health_check()
            .await
            .expect("a mounted KV2 engine with no keys yet must pass the health check");

        assert_eq!(
            vault.requests(),
            vec!["LIST /v1/secret/metadata/rustfs/kms/keys".to_string()],
            "the check must list the configured mount and prefix, once"
        );
    }

    /// The same status with a Vault message behind it means nothing is routed at
    /// `kv_mount`. That must fail the health check: passing it let a KMS whose
    /// configured mount does not exist report itself healthy at startup and then
    /// answer every listing with "no keys".
    #[tokio::test]
    async fn health_check_fails_when_the_kv2_mount_is_missing() {
        let (_vault, client) = scripted_client(vec![missing_mount_404()]).await;

        let error = client
            .health_check()
            .await
            .expect_err("a missing KV2 mount must fail the health check");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
        let message = error.to_string();
        // Not a bare `contains("secret")`: the scripted route text carries the
        // mount name too, so only the composed phrase proves it was interpolated.
        assert!(
            message.contains("kv_mount 'secret'"),
            "the failure must name the mount it was made against: {message}"
        );
        assert!(
            message.contains("no handler for route"),
            "the failure must carry Vault's own explanation: {message}"
        );
    }

    /// A 404 whose body is not a Vault error at all — a reverse proxy's own page,
    /// say — cannot be read as an empty prefix, and must still say which mount
    /// failed. `vaultrs` only builds an `APIError` from a body it could parse, so
    /// this arrives as a different variant and takes the fallback message.
    #[tokio::test]
    async fn list_keys_fails_closed_on_a_404_whose_body_is_not_a_vault_error() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::Http {
            status: 404,
            body: "<html><body>404 Not Found</body></html>".to_string(),
        }])
        .await;

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("a 404 that is not a Vault error must not read as an empty listing");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
        assert!(
            error.to_string().contains("kv_mount 'secret'"),
            "an unparseable failure must still name the mount: {error}"
        );
    }

    /// The empty-prefix 404 on the listing path is an empty result set, not a
    /// backend failure.
    #[tokio::test]
    async fn list_keys_returns_an_empty_page_on_an_empty_kv2_prefix() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::empty_list_404()]).await;

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("an empty KV2 prefix must list as empty, not fail");
        assert!(response.keys.is_empty(), "got {:?}", response.keys);
        assert!(!response.truncated, "an empty listing has nothing left to page through");
        assert_eq!(response.next_marker, None);
    }

    /// A missing mount must not read as "you have no keys": that answer is
    /// indistinguishable from a KMS whose keys are all gone, and the deletion
    /// sweep takes its census over exactly this listing.
    #[tokio::test]
    async fn list_keys_fails_when_the_kv2_mount_is_missing() {
        let (_vault, client) = scripted_client(vec![missing_mount_404()]).await;

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("a missing KV2 mount must fail the listing, not empty it");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
        assert!(
            error.to_string().contains("kv_mount 'secret'"),
            "the failure must name the mount it was made against: {error}"
        );
    }

    /// The version-record listing takes the same discriminator, and for a
    /// sharper reason: `delete_key` purges the records it returns before
    /// removing the key, so an unrouted path read as "no versions" would skip
    /// the purge and leave master key material in Vault.
    #[tokio::test]
    async fn key_version_records_fail_when_the_kv2_mount_is_missing() {
        let (_vault, client) = scripted_client(vec![missing_mount_404()]).await;

        let error = client
            .list_key_version_records("wired-key")
            .await
            .expect_err("a missing KV2 mount must not read as 'this key was never rotated'");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
    }

    /// A key that was never rotated has no versions directory, and Vault answers
    /// that with the empty-list 404 — still "no version records", not a failure.
    #[tokio::test]
    async fn key_version_records_are_absent_for_a_never_rotated_key() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::empty_list_404()]).await;

        let versions = client
            .list_key_version_records("wired-key")
            .await
            .expect("a key with no versions directory must list as absent, not fail");
        assert_eq!(versions, None);
    }

    /// A record whose body is present but not a key record is corrupt material,
    /// not a backend problem — and the reported message carries only where the
    /// parse failed, never the values it tripped over.
    #[tokio::test]
    async fn wired_read_reports_an_uninterpretable_record_as_corrupt_material() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(serde_json::json!({
            "data": null,
            "metadata": { "created_time": "2026-01-01T00:00:00Z", "deletion_time": "", "custom_metadata": null, "destroyed": false, "version": 1 },
        }))])
        .await;

        let error = client
            .describe_key("wired-key", None)
            .await
            .expect_err("an uninterpretable key record must fail closed");
        assert!(matches!(&error, KmsError::MaterialCorrupt { .. }), "got {error:?}");
    }

    /// A `200` whose envelope carries no `data` at all is material that is gone
    /// — a distinct outcome from a record that cannot be parsed, and from Vault
    /// being unreachable.
    #[tokio::test]
    async fn wired_read_reports_an_absent_record_body_as_missing_material() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::Http {
            status: 200,
            body: serde_json::json!({
                "request_id": "scripted",
                "lease_id": "",
                "lease_duration": 0,
                "renewable": false,
            })
            .to_string(),
        }])
        .await;

        let error = client
            .describe_key("wired-key", None)
            .await
            .expect_err("a record with no body must fail closed");
        assert!(matches!(&error, KmsError::MaterialMissing { .. }), "got {error:?}");
    }

    /// serde names the offending scalar in its own message ("invalid type:
    /// string \"…\", expected u32"). For a key record that scalar comes out of
    /// the stored material, and this message reaches both a log line and an
    /// admin HTTP body, so only the position and category may survive.
    #[tokio::test]
    async fn wired_read_does_not_echo_record_values_into_the_error() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(serde_json::json!({
            "data": { "version": "sensitive-value-must-not-leak" },
            "metadata": { "created_time": "2026-01-01T00:00:00Z", "deletion_time": "", "custom_metadata": null, "destroyed": false, "version": 1 },
        }))])
        .await;

        let error = client
            .describe_key("wired-key", None)
            .await
            .expect_err("a type-mismatched key record must fail closed");
        assert!(matches!(&error, KmsError::MaterialCorrupt { .. }), "got {error:?}");
        assert!(
            !error.to_string().contains("sensitive-value-must-not-leak"),
            "the error echoed a stored record value: {error}"
        );
    }

    #[tokio::test]
    async fn wired_read_retries_transient_status_then_succeeds() {
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::error(503, "temporarily unavailable"),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
        ])
        .await;

        let key_data = client
            .get_key_data("wired-key")
            .await
            .expect("read must retry past a transient 503");
        assert_eq!(key_data.algorithm, "AES_256");

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "one failed attempt plus one retry: {requests:?}");
        assert!(
            requests
                .iter()
                .all(|line| line == "GET /v1/secret/data/rustfs/kms/keys/wired-key"),
            "both attempts must hit the same read endpoint: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_read_retries_closed_connections_within_budget() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::close(), ScriptedResponse::close()]).await;

        let error = client
            .get_key_data("wired-key")
            .await
            .expect_err("closed connections must exhaust the retry budget");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        let expected_requests = usize::try_from(SCRIPTED_RETRY_ATTEMPTS).expect("retry attempts must fit usize");
        assert_eq!(
            requests.len(),
            expected_requests,
            "all budgeted retry attempts must reach Vault: {requests:?}"
        );
        assert!(
            requests
                .iter()
                .all(|line| line == "GET /v1/secret/data/rustfs/kms/keys/wired-key"),
            "all attempts must hit the same read endpoint: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_read_does_not_retry_permission_errors() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::error(403, "permission denied")]).await;

        client
            .get_key_data("wired-key")
            .await
            .expect_err("a 403 must fail the read outright");

        let requests = vault.requests();
        assert_eq!(requests.len(), 1, "fatal statuses must not be retried: {requests:?}");
    }

    #[tokio::test]
    async fn wired_write_is_never_retried_on_transient_status() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::error(503, "sealed")]).await;

        let error = client
            .cas_store_key_data("wired-key", &healthy_key_data(), 1)
            .await
            .expect_err("the scripted 503 must fail the write");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec!["POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string()],
            "a non-idempotent write must run exactly once even on a retryable status"
        );
    }

    #[tokio::test]
    async fn wired_cas_conflict_is_surfaced_without_retry() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::error(
            400,
            "check-and-set parameter did not match the current version",
        )])
        .await;

        let error = client
            .cas_store_key_data("wired-key", &healthy_key_data(), 7)
            .await
            .expect_err("the scripted CAS conflict must fail the write");
        assert!(
            matches!(error, KmsError::InvalidOperation { .. }),
            "a CAS conflict is a concurrency signal, not a backend failure: {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 1, "a CAS conflict must never be retried: {requests:?}");
    }

    #[tokio::test]
    async fn wired_create_key_read_confirms_identical_existing_key() {
        // The stored key is exactly what create_key("wired-key", "AES_256")
        // would have produced, so a retried create whose first response was
        // lost recovers by reading it back instead of failing.
        let (vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&healthy_key_data()))]).await;

        let recovered = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect("an identical active key must read-confirm as a recovered create");
        assert_eq!(recovered.version, 1);
        assert_eq!(recovered.algorithm, "AES_256");

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec!["GET /v1/secret/data/rustfs/kms/keys/wired-key".to_string()],
            "a recovered create must not write anything"
        );
    }

    #[tokio::test]
    async fn wired_create_key_still_fails_on_mismatched_existing_key() {
        let mut disabled = healthy_key_data();
        disabled.status = KeyStatus::Disabled;
        let (vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&disabled))]).await;

        let error = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect_err("a non-active existing key must keep failing the create");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 1, "the mismatch must be decided from the single read: {requests:?}");
    }

    /// Poison matrix for the read-side material gate. Every corruption class must fail
    /// closed with its typed error; reintroducing any "self-heal" (regenerate on empty or
    /// undecodable material) turns one of these expected errors into an Ok and fails the
    /// test. Offline on purpose: `decode_stored_key_material` has no Vault I/O.
    #[test]
    fn decode_stored_key_material_fails_closed_on_poisoned_values() {
        // Empty material means the record lost its key, not that a new one may be minted.
        assert!(matches!(
            decode_stored_key_material("poisoned", ""),
            Err(KmsError::MaterialMissing { key_id }) if key_id == "poisoned"
        ));

        // Invalid base64.
        assert!(matches!(
            decode_stored_key_material("poisoned", "!!!not-base64!!!"),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Truncated material: valid base64 of fewer than 32 bytes.
        let truncated = general_purpose::STANDARD.encode([0x42u8; 16]);
        assert!(matches!(
            decode_stored_key_material("poisoned", &truncated),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Oversized material: valid base64 of more than 32 bytes.
        let oversized = general_purpose::STANDARD.encode([0x42u8; 33]);
        assert!(matches!(
            decode_stored_key_material("poisoned", &oversized),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Well-formed material still decodes.
        let valid = general_purpose::STANDARD.encode([0x42u8; 32]);
        assert_eq!(
            decode_stored_key_material("healthy", &valid).expect("valid material must decode"),
            vec![0x42u8; 32]
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance
    async fn test_vault_client_integration() {
        let config = VaultConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-only-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        };

        let client = VaultKmsClient::new(config, &KmsConfig::default())
            .await
            .expect("Failed to create Vault client");

        // Test key operations
        let key_id = "test-key-vault";
        let master_key = client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");
        assert_eq!(master_key.key_id, key_id);
        assert_eq!(master_key.algorithm, "AES_256");

        // Test key description
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id);

        // Test data key generation
        let data_key_request = GenerateKeyRequest {
            master_key_id: key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        };

        let data_key = client
            .generate_data_key(&data_key_request, None)
            .await
            .expect("Failed to generate data key");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        // Test health check
        client.health_check().await.expect("Health check failed");
    }

    fn integration_vault_config() -> VaultConfig {
        VaultConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-only-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        }
    }

    /// The scripted tests assert what this backend does with each of Vault's two
    /// 404 shapes; this one asserts that Vault still produces the shape they
    /// assume. An empty prefix must arrive as a 404 the client reads as an empty
    /// listing — if a Vault release ever answered it differently, every scripted
    /// test would stay green while a first-ever deployment stopped starting.
    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn live_health_check_passes_on_an_empty_kv2_prefix() {
        let config = VaultConfig {
            key_path_prefix: format!("rustfs/kms/empty-probe/{}", uuid::Uuid::new_v4()),
            ..integration_vault_config()
        };
        let client = VaultKmsClient::new(config, &KmsConfig::default()).await.expect("client");

        client
            .health_check()
            .await
            .expect("a prefix nothing was ever written to must pass the health check");
    }

    /// The other direction, against the same real Vault: a mount that does not
    /// exist must fail the check that gates startup, and say which mount.
    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn live_health_check_fails_when_the_kv2_mount_is_missing() {
        let config = VaultConfig {
            kv_mount: "rustfs-kms-definitely-not-mounted".to_string(),
            ..integration_vault_config()
        };
        let client = VaultKmsClient::new(config, &KmsConfig::default()).await.expect("client");

        let error = client
            .health_check()
            .await
            .expect_err("a kv_mount with no engine behind it must fail the health check");
        assert!(
            error.to_string().contains("rustfs-kms-definitely-not-mounted"),
            "the failure must name the mount it was made against: {error}"
        );
    }

    #[tokio::test]
    async fn test_key_version_paths_stay_under_the_key() {
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        assert_eq!(client.key_path("my-key"), "rustfs/kms/keys/my-key");
        assert_eq!(client.key_versions_dir("my-key"), "rustfs/kms/keys/my-key/versions");
        assert_eq!(client.key_version_path("my-key", 3), "rustfs/kms/keys/my-key/versions/3");
    }

    #[test]
    fn test_filter_key_directory_entries_drops_version_dirs() {
        // Listing the key prefix returns "my-key/" as a directory entry once
        // my-key has version records; only real key records may be listed.
        let listed = vec!["alpha".to_string(), "alpha/".to_string(), "beta".to_string()];
        assert_eq!(filter_key_directory_entries(listed), vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn test_resolve_envelope_master_key_version_rules() {
        // An explicit envelope version is honored verbatim, even when it differs
        // from both the baseline and the current version: whether material exists
        // for it is decided by the versioned lookup, never by falling back.
        assert_eq!(resolve_envelope_master_key_version(Some(2), Some(1), 5), 2);
        assert_eq!(resolve_envelope_master_key_version(Some(9), Some(1), 5), 9);

        // A pre-versioning envelope resolves to the frozen baseline, not to
        // whatever version happens to be current.
        assert_eq!(resolve_envelope_master_key_version(None, Some(1), 5), 1);

        // Never-rotated keys have no baseline; the current version is the only
        // material that ever existed, matching pre-versioning behavior.
        assert_eq!(resolve_envelope_master_key_version(None, None, 1), 1);
    }

    #[test]
    fn test_vault_key_data_without_baseline_version_deserializes() {
        // Key records written before versioned storage have no baseline_version
        // field and must keep deserializing with None.
        let key_data = VaultKeyData {
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            encrypted_key_material: general_purpose::STANDARD.encode([0x42u8; 32]),
            baseline_version: Some(1),
            deletion_date: None,
            rotated_at: None,
            wrap_budget_reserved: 0,
        };

        let mut value = serde_json::to_value(&key_data).expect("serialize key data");
        value
            .as_object_mut()
            .expect("key data serializes to an object")
            .remove("baseline_version");

        let legacy: VaultKeyData = serde_json::from_value(value).expect("legacy record must deserialize");
        assert_eq!(legacy.baseline_version, None);
        assert_eq!(legacy.version, 1);
    }

    /// Every declared `VaultKeyData` field must survive a serialize/deserialize
    /// round trip through the hand-written `Deserialize`.
    ///
    /// The hand-written impl lists its fields three times (the `Field` enum, the
    /// match arms, the struct literal), so a field added to the struct alone
    /// compiles on its own branch and only breaks once both branches merge —
    /// which is exactly how `wrap_budget_reserved` briefly broke the build.
    /// Asserting against the serialized key set makes the deserializer's
    /// coverage a test failure rather than a merge-order accident.
    #[test]
    fn vault_key_data_deserializer_covers_every_serialized_field() {
        let mut key_data = healthy_key_data();
        key_data.wrap_budget_reserved = 7_000_000;
        key_data.baseline_version = Some(2);
        key_data.rotated_at = Some(Zoned::now());
        key_data.deletion_date = Some(Zoned::now());
        key_data.description = Some("described".to_string());

        let value = serde_json::to_value(&key_data).expect("serialize key data");
        let serialized_fields: Vec<String> = value
            .as_object()
            .expect("key data serializes to an object")
            .keys()
            .cloned()
            .collect();

        // Every serialized field must be a known field: an unknown one would be
        // counted by the unknown-field observer instead of being read back.
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let restored: VaultKeyData =
            metrics::with_local_recorder(&recorder, || serde_json::from_value(value).expect("round trip"));
        assert_eq!(
            crate::test_support::unknown_field_metric(&recorder, "vault-kv2-key"),
            0,
            "a serialized field was not recognized by the deserializer; fields: {serialized_fields:?}"
        );

        // And every value must survive, not just parse.
        assert_eq!(restored.wrap_budget_reserved, key_data.wrap_budget_reserved);
        assert_eq!(restored.baseline_version, key_data.baseline_version);
        assert_eq!(restored.version, key_data.version);
        assert_eq!(restored.status, key_data.status);
        assert_eq!(restored.description, key_data.description);
        assert_eq!(restored.encrypted_key_material, key_data.encrypted_key_material);
        assert!(restored.rotated_at.is_some());
        assert!(restored.deletion_date.is_some());
    }

    #[test]
    fn vault_key_data_unknown_fields_remain_readable_and_are_observed() {
        // A record written by a newer build carries fields this build does not
        // know. It must stay readable — and the drop must be visible, not
        // silent (rustfs/backlog#1641). Only the field name may be logged; the
        // value can sit next to key material.
        let mut value = serde_json::to_value(healthy_key_data()).expect("serialize key data");
        let object = value.as_object_mut().expect("key data serializes to an object");
        object.insert("field_from_the_future".to_string(), serde_json::json!("field value must not be logged"));

        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let parsed: VaultKeyData = metrics::with_local_recorder(&recorder, || {
            tracing::dispatcher::with_default(&dispatch, || {
                serde_json::from_value(value).expect("unknown fields must remain readable")
            })
        });
        assert_eq!(parsed.algorithm, healthy_key_data().algorithm);
        assert_eq!(crate::test_support::unknown_field_metric(&recorder, "vault-kv2-key"), 1);

        let output = logs.output();
        assert!(output.contains("Vault KV2 key record contains unknown fields"), "got: {output}");
        assert!(output.contains("field_from_the_future"));
        assert!(!output.contains("field value must not be logged"));
    }

    #[test]
    fn test_is_cas_conflict_only_matches_cas_failures() {
        let cas = ClientError::APIError {
            code: 400,
            errors: vec!["check-and-set parameter did not match the current version".to_string()],
        };
        assert!(is_cas_conflict(&cas));

        let other_400 = ClientError::APIError {
            code: 400,
            errors: vec!["invalid request".to_string()],
        };
        assert!(!is_cas_conflict(&other_400));

        let not_found = ClientError::APIError {
            code: 404,
            errors: Vec::new(),
        };
        assert!(!is_cas_conflict(&not_found));
    }

    /// The whole discriminator: same status, opposite meanings, told apart by
    /// whether Vault attached a message.
    #[test]
    fn test_is_empty_vault_list_only_matches_the_empty_list_404() {
        let empty_prefix = ClientError::APIError {
            code: 404,
            errors: Vec::new(),
        };
        assert!(is_empty_vault_list(&empty_prefix));

        let missing_mount = ClientError::APIError {
            code: 404,
            errors: vec!["no handler for route \"secret/metadata/rustfs/kms/keys/\". route entry not found.".to_string()],
        };
        assert!(!is_empty_vault_list(&missing_mount));

        // The mount name is deliberately one that cannot appear in the route
        // text, so the assertion below can only pass by interpolation.
        let message = describe_kv2_list_failure("kv-not-the-route", "keys", &missing_mount);
        assert!(
            message.contains("no handler for route"),
            "the reported failure must carry Vault's own explanation, which its Display drops: {message}"
        );
        assert!(
            message.contains("kv_mount 'kv-not-the-route'"),
            "the reported failure must name the mount it was made against: {message}"
        );

        // Only a 404 means "not there"; every other status is an outcome of its
        // own and must never be read as an empty listing.
        for code in [400u16, 403, 500, 503] {
            let other = ClientError::APIError {
                code,
                errors: Vec::new(),
            };
            assert!(!is_empty_vault_list(&other), "status {code}");
        }
    }

    fn integration_generate_request(key_id: &str) -> GenerateKeyRequest {
        GenerateKeyRequest {
            master_key_id: key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        }
    }

    fn integration_decrypt_request(ciphertext: Vec<u8>) -> DecryptRequest {
        DecryptRequest {
            ciphertext,
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        }
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_decrypt_after_rotate() {
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        let key_id = format!("rotate-retain-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");
        let request = integration_generate_request(&key_id);

        let dk_v1 = client.generate_data_key(&request, None).await.expect("generate under v1");
        let env_v1: DataKeyEnvelope = serde_json::from_slice(&dk_v1.ciphertext).expect("parse v1 envelope");
        assert_eq!(env_v1.master_key_version, Some(1));

        let rotated = client.rotate_key(&key_id, None).await.expect("rotate to v2");
        assert_eq!(rotated.version, 2);
        let dk_v2 = client.generate_data_key(&request, None).await.expect("generate under v2");
        let env_v2: DataKeyEnvelope = serde_json::from_slice(&dk_v2.ciphertext).expect("parse v2 envelope");
        assert_eq!(env_v2.master_key_version, Some(2), "new envelopes must carry the latest version");

        let rotated = client.rotate_key(&key_id, None).await.expect("rotate to v3");
        assert_eq!(rotated.version, 3);
        let dk_v3 = client.generate_data_key(&request, None).await.expect("generate under v3");
        let env_v3: DataKeyEnvelope = serde_json::from_slice(&dk_v3.ciphertext).expect("parse v3 envelope");
        assert_eq!(env_v3.master_key_version, Some(3));

        // A mixed batch of envelopes from every historical version must decrypt.
        for (data_key, label) in [(&dk_v1, "v1"), (&dk_v3, "v3"), (&dk_v2, "v2"), (&dk_v1, "v1 again")] {
            let (plaintext, _opened_by) = client
                .decrypt(&integration_decrypt_request(data_key.ciphertext.clone()), None)
                .await
                .unwrap_or_else(|error| panic!("envelope wrapped under {label} must stay decryptable: {error}"));
            assert_eq!(Some(plaintext), data_key.plaintext, "{label} plaintext must round-trip");
        }
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_rotate_does_not_orphan_legacy_envelopes() {
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        let key_id = format!("rotate-legacy-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        // Simulate an envelope written by a pre-versioning build: same wrapped DEK,
        // but without the master_key_version field.
        let data_key = client
            .generate_data_key(&integration_generate_request(&key_id), None)
            .await
            .expect("generate");
        let mut envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
        envelope
            .as_object_mut()
            .expect("envelope is an object")
            .remove("master_key_version");
        let legacy_ciphertext = serde_json::to_vec(&envelope).expect("serialize legacy envelope");

        client.rotate_key(&key_id, None).await.expect("rotate to v2");
        client.rotate_key(&key_id, None).await.expect("rotate to v3");

        // The baseline rule must route the legacy envelope to the frozen version 1
        // material even though the current version has moved on.
        let (plaintext, _opened_by) = client
            .decrypt(&integration_decrypt_request(legacy_ciphertext), None)
            .await
            .expect("legacy envelope must stay decryptable after rotation");
        assert_eq!(Some(plaintext), data_key.plaintext);

        let key_data = client.get_key_data(&key_id).await.expect("read");
        assert_eq!(key_data.baseline_version, Some(1), "first rotation must pin the baseline");
        assert_eq!(key_data.version, 3);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_envelope_version_tampering_fails_closed() {
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        let key_id = format!("rotate-tamper-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");
        let data_key = client
            .generate_data_key(&integration_generate_request(&key_id), None)
            .await
            .expect("generate");
        client.rotate_key(&key_id, None).await.expect("rotate");

        // Point the envelope at a version that has no material record.
        let mut envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
        envelope
            .as_object_mut()
            .expect("envelope is an object")
            .insert("master_key_version".to_string(), serde_json::json!(999));
        let tampered = serde_json::to_vec(&envelope).expect("serialize tampered envelope");

        let error = client
            .decrypt(&integration_decrypt_request(tampered), None)
            .await
            .expect_err("nonexistent version must fail closed, not fall back to current");
        assert!(
            matches!(error, KmsError::KeyVersionNotFound { version: 999, key_id: ref error_key_id } if *error_key_id == key_id),
            "expected KeyVersionNotFound for version 999, got {error:?}"
        );

        // The untampered envelope still decrypts through its recorded version.
        let (plaintext, _opened_by) = client
            .decrypt(&integration_decrypt_request(data_key.ciphertext.clone()), None)
            .await
            .expect("untampered envelope must still decrypt");
        assert_eq!(Some(plaintext), data_key.plaintext);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_concurrent_rotate_versions_monotonic() {
        use std::sync::Arc;

        let client = Arc::new(
            VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
                .await
                .expect("client"),
        );

        let key_id = format!("rotate-concurrent-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        let attempts = 4;
        let tasks: Vec<_> = (0..attempts)
            .map(|_| {
                let client = Arc::clone(&client);
                let key_id = key_id.clone();
                tokio::spawn(async move { client.rotate_key(&key_id, None).await })
            })
            .collect();

        let mut successes = 0u32;
        for task in tasks {
            // Losing a check-and-set race is an expected error; committing is not
            // required, but every commit must account for exactly one version bump.
            if task.await.expect("join rotate task").is_ok() {
                successes += 1;
            }
        }
        assert!(successes >= 1, "at least one rotation must commit");

        let key_data = client.get_key_data(&key_id).await.expect("read");
        assert_eq!(
            key_data.version,
            1 + successes,
            "each successful rotation must commit exactly one new version"
        );
        assert_eq!(key_data.baseline_version, Some(1));

        // Every version has an immutable record with unique material, and the
        // top-level fast-path copy matches the current version's record.
        let mut materials = std::collections::HashSet::new();
        for version in 1..=key_data.version {
            let record = client
                .get_key_version_record(&key_id, version)
                .await
                .unwrap_or_else(|error| panic!("version {version} must have a record: {error}"));
            assert_eq!(record.version, version);
            assert!(materials.insert(record.encrypted_key_material), "version materials must be unique");
        }
        let current_record = client
            .get_key_version_record(&key_id, key_data.version)
            .await
            .expect("current version record");
        assert_eq!(current_record.encrypted_key_material, key_data.encrypted_key_material);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_corrupted_key_material_does_not_regenerate() {
        // Regression: get_key_material previously "self-healed" a decrypt/length failure by
        // minting a fresh random master key and overwriting the stored value — destroying the
        // original key and making every DEK wrapped by it permanently undecryptable.
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        let key_id = format!("corrupt-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        // Corrupt the stored material to an invalid base64 string.
        let mut key_data = client.get_key_data(&key_id).await.expect("read");
        key_data.encrypted_key_material = "!!!not-base64!!!".to_string();
        client.store_key_data(&key_id, &key_data).await.expect("store corrupt");

        // Reading the material must now ERROR, not silently regenerate + overwrite.
        let poisoned = client.get_key_data(&key_id).await.expect("read poisoned");
        let error = client
            .get_key_material_for_version(&key_id, &poisoned, poisoned.version)
            .await
            .expect_err("corrupted key material must yield an error, not a fresh key");
        assert!(
            matches!(error, KmsError::MaterialCorrupt { .. }),
            "expected MaterialCorrupt, got {error:?}"
        );

        // And the stored (corrupted) material must be UNCHANGED.
        let after = client.get_key_data(&key_id).await.expect("reread");
        assert_eq!(
            after.encrypted_key_material, "!!!not-base64!!!",
            "the material read path must not overwrite stored master key material on failure"
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_empty_key_material_does_not_regenerate() {
        // Regression: get_key_material previously treated empty stored material as a
        // bootstrap case and silently generated + persisted a fresh master key on the
        // read path. Empty material must instead fail closed as MaterialMissing and
        // leave the stored record untouched.
        let client = VaultKmsClient::new(integration_vault_config(), &KmsConfig::default())
            .await
            .expect("client");

        let key_id = format!("empty-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        let mut key_data = client.get_key_data(&key_id).await.expect("read");
        key_data.encrypted_key_material = String::new();
        client.store_key_data(&key_id, &key_data).await.expect("store empty");

        let poisoned = client.get_key_data(&key_id).await.expect("read poisoned");
        let error = client
            .get_key_material_for_version(&key_id, &poisoned, poisoned.version)
            .await
            .expect_err("empty key material must yield an error, not a fresh key");
        assert!(
            matches!(error, KmsError::MaterialMissing { .. }),
            "expected MaterialMissing, got {error:?}"
        );

        // The stored record must still hold the empty value: no regeneration, no write.
        let after = client.get_key_data(&key_id).await.expect("reread");
        assert!(
            after.encrypted_key_material.is_empty(),
            "the material read path must not backfill missing master key material"
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_cancel_key_deletion_persists_state() {
        use crate::config::{BackendConfig, KmsConfig};
        use crate::types::{CancelKeyDeletionRequest, CreateKeyRequest, DeleteKeyRequest, KeyStatus, KeyUsage};

        // A dev Vault speaks plain HTTP, which validate() refuses unless
        // development mode is declared on the config itself — the env override
        // is applied by the config loaders, not by Default::default().
        let kms_config = KmsConfig {
            backend_config: BackendConfig::VaultKv2(Box::new(integration_vault_config())),
            ..Default::default()
        }
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(kms_config).await.expect("backend");

        let key_id = format!("cancel-persist-{}", uuid::Uuid::new_v4());
        backend
            .create_key(CreateKeyRequest {
                key_name: Some(key_id.clone()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("create");

        backend
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: Some(false),
                confirm_key_id: None,
            })
            .await
            .expect("schedule delete");

        backend
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await
            .expect("cancel");

        // Re-read the PERSISTED state from Vault. Before the fix, storage still held
        // PendingDeletion because cancel only mutated the response, never wrote back.
        let persisted = backend.client.get_key_data(&key_id).await.expect("reread");
        assert_eq!(
            persisted.status,
            KeyStatus::Active,
            "cancel_key_deletion must persist Active status to Vault, not only mutate the response"
        );
    }

    /// The persisted KV2 record round-trips its deletion deadline, and records
    /// written before the field existed keep deserializing (as None). A revert
    /// of deadline persistence turns this test red.
    #[test]
    fn vault_key_data_deletion_date_round_trips_and_stays_backward_compatible() {
        let deadline = Zoned::now() + Duration::from_secs(7 * 86400);
        let key_data = VaultKeyData {
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::PendingDeletion,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            deletion_date: Some(deadline.clone()),
            rotated_at: None,
            encrypted_key_material: "material".to_string(),
            baseline_version: None,
            wrap_budget_reserved: 0,
        };

        let mut value = serde_json::to_value(&key_data).expect("serialize");
        let restored: VaultKeyData = serde_json::from_value(value.clone()).expect("round trip");
        assert_eq!(
            restored.deletion_date.as_ref().map(Zoned::timestamp),
            Some(deadline.timestamp()),
            "deletion deadline must survive the KV2 round trip"
        );

        value
            .as_object_mut()
            .expect("record must be a JSON object")
            .remove("deletion_date")
            .expect("current records must carry the field");
        let legacy: VaultKeyData = serde_json::from_value(value).expect("legacy record must deserialize");
        assert!(legacy.deletion_date.is_none());
    }

    /// KV2 write acknowledgement (`SecretVersionMetadata`) for `kv2::set`.
    fn kv2_write_ack() -> serde_json::Value {
        serde_json::json!({
            "created_time": "2026-01-01T00:00:00Z",
            "custom_metadata": null,
            "deletion_time": "",
            "destroyed": false,
            "version": 2,
        })
    }

    #[tokio::test]
    async fn wired_kv2_encrypt_round_trips_through_decrypt() {
        // One key-record read plus the first wrap's budget reservation for the
        // encrypt, one read for the decrypt.
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
        ])
        .await;
        let context = HashMap::from([("bucket".to_string(), "kv2".to_string())]);

        let encrypted = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"kv2-direct-encrypt".to_vec(),
                    encryption_context: context.clone(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("encrypt must produce an envelope");

        // The ciphertext is a real KMS envelope wrapping AEAD output that
        // decrypt() can open, not an XOR of the plaintext with the master key
        // material.
        let envelope: DataKeyEnvelope = serde_json::from_slice(&encrypted.ciphertext).expect("envelope must parse");
        assert_eq!(envelope.master_key_id, "wired-key");
        assert_eq!(envelope.master_key_version, Some(1));

        let (decrypted, opened_by) = client
            .decrypt(
                &DecryptRequest {
                    ciphertext: encrypted.ciphertext.clone(),
                    encryption_context: context,
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("decrypt must round-trip the envelope");
        assert_eq!(decrypted, b"kv2-direct-encrypt".to_vec());
        assert_eq!(opened_by, "wired-key", "decrypt must report the master key that opened the envelope");

        // A different object context must not decrypt (checked before any
        // Vault read, so no scripted response is consumed).
        let error = client
            .decrypt(
                &DecryptRequest {
                    ciphertext: encrypted.ciphertext,
                    encryption_context: HashMap::from([("bucket".to_string(), "other".to_string())]),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("a different context must not decrypt");
        assert!(matches!(error, KmsError::ContextMismatch { .. }), "got {error:?}");
    }

    /// KV2 secret-metadata read payload (`kv2::read_metadata`) pinning the
    /// current secret version used as the rotation check-and-set base.
    fn kv2_metadata_read_data(current_version: u64) -> serde_json::Value {
        serde_json::json!({
            "cas_required": false,
            "created_time": "2026-01-01T00:00:00Z",
            "current_version": current_version,
            "delete_version_after": "0s",
            "max_versions": 0,
            "oldest_version": 0,
            "updated_time": "2026-01-01T00:00:00Z",
            "custom_metadata": null,
            "versions": {},
        })
    }

    #[tokio::test]
    async fn wired_kv2_rotate_rejected_while_disabled() {
        let mut key_data = healthy_key_data();
        key_data.status = KeyStatus::Disabled;
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
        ])
        .await;

        let error = client
            .rotate_key("wired-key", None)
            .await
            .expect_err("rotation of a disabled key must be rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(
            requests.len(),
            2,
            "the state gate must reject after the versioned read, before any write: {requests:?}"
        );
        assert!(requests.iter().all(|line| line.starts_with("GET ")), "{requests:?}");
    }

    #[tokio::test]
    async fn wired_backend_lifecycle_overrides_reach_the_client() {
        let mut disabled = healthy_key_data();
        disabled.status = KeyStatus::Disabled;
        let vault = ScriptedVault::serve(vec![
            // disable: versioned read of the Active record, persist it Disabled.
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_write_ack()),
            // enable: versioned read of the Disabled record, persist it Active.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(kv2_read_data(&disabled)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        backend
            .disable_key("wired-key")
            .await
            .expect("KmsBackend::disable_key must persist through the client");
        backend
            .enable_key("wired-key")
            .await
            .expect("KmsBackend::enable_key must persist through the client");

        let requests = vault.requests();
        assert_eq!(
            requests.len(),
            6,
            "each transition is one versioned read (metadata + data) plus one write: {requests:?}"
        );
        assert!(
            requests[0].starts_with("GET /v1/secret/metadata/") && requests[3].starts_with("GET /v1/secret/metadata/"),
            "{requests:?}"
        );
        assert!(
            requests[1].starts_with("GET /v1/secret/data/") && requests[4].starts_with("GET /v1/secret/data/"),
            "{requests:?}"
        );
        assert!(requests[2].starts_with("POST ") && requests[5].starts_with("POST "), "{requests:?}");

        // Both lifecycle writes must carry a check-and-set precondition pinned
        // to the KV2 secret version they read.
        let bodies = vault.request_bodies();
        for (index, cas) in [(2usize, 1u64), (5, 2)] {
            let body: serde_json::Value = serde_json::from_str(&bodies[index]).expect("lifecycle write body must be JSON");
            assert_eq!(
                body["options"]["cas"],
                serde_json::json!(cas),
                "write {index} must be check-and-set: {body}"
            );
        }
    }

    /// Parse a captured KV2 write body (`{"data": ..., "options": {"cas": N}}`).
    fn parse_write_body(body: &str) -> serde_json::Value {
        serde_json::from_str(body).expect("KV2 write body must be JSON")
    }

    /// KV2 read payload for an immutable version record.
    fn kv2_read_version_record_data(record: &VaultKeyVersionRecord) -> serde_json::Value {
        serde_json::json!({
            "data": serde_json::to_value(record).expect("serialize version record"),
            "metadata": {
                "created_time": "2026-01-01T00:00:00Z",
                "deletion_time": "",
                "custom_metadata": null,
                "destroyed": false,
                "version": 1,
            },
        })
    }

    const CAS_CONFLICT_MESSAGE: &str = "check-and-set parameter did not match the current version";

    /// Base64 material distinct from `healthy_key_data`'s, standing in for the
    /// material a concurrent rotation committed.
    fn rotated_material() -> String {
        general_purpose::STANDARD.encode([0x43u8; 32])
    }

    /// The issue's lost-update scenario: node A disables a key while node B's
    /// rotation commits in between. The blind write this replaces would have
    /// written A's stale snapshot back — rolling the key from version 2 to
    /// version 1 and resurrecting the pre-rotation material, which is exactly
    /// what the final-write assertions below reject. Under check-and-set the
    /// stale write conflicts, A re-reads, re-passes the state gate against the
    /// rotated record, and persists only the status change on top of it.
    #[tokio::test]
    async fn wired_disable_interleaved_with_rotate_preserves_committed_rotation() {
        let pre_rotate = healthy_key_data();
        let mut rotated = healthy_key_data();
        rotated.version = 2;
        rotated.baseline_version = Some(1);
        rotated.encrypted_key_material = rotated_material();

        let (vault, client) = scripted_client(vec![
            // Attempt 1: versioned read observes the pre-rotation record...
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&pre_rotate)),
            // ...but the concurrent rotation committed KV2 versions 2 and 3 in
            // between, so the check-and-set write loses.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // Attempt 2: the re-read observes the rotated record and the write
            // pinned to it succeeds.
            ScriptedResponse::ok(kv2_metadata_read_data(3)),
            ScriptedResponse::ok(kv2_read_data(&rotated)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        client
            .disable_key("wired-key", None)
            .await
            .expect("the disable must retry past the lost race and commit");

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=1".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=3".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
            ],
            "a conflict must trigger exactly one full re-read before the retry write"
        );

        let bodies = vault.request_bodies();
        let first_write = parse_write_body(&bodies[2]);
        assert_eq!(first_write["options"]["cas"], serde_json::json!(1), "{first_write}");

        // The committed write must be the *rotated* snapshot with only the
        // status changed. A blind write would have persisted version 1 and the
        // pre-rotation material here.
        let committed = parse_write_body(&bodies[5]);
        assert_eq!(committed["options"]["cas"], serde_json::json!(3), "{committed}");
        assert_eq!(committed["data"]["status"], serde_json::json!("Disabled"), "{committed}");
        assert_eq!(
            committed["data"]["version"],
            serde_json::json!(2),
            "the rotation's version bump must survive: {committed}"
        );
        assert_eq!(committed["data"]["baseline_version"], serde_json::json!(1), "{committed}");
        assert_eq!(
            committed["data"]["encrypted_key_material"],
            serde_json::json!(rotated_material()),
            "the rotation's material must survive the disable: {committed}"
        );
    }

    #[tokio::test]
    async fn wired_create_key_write_is_create_only() {
        let (vault, client) = scripted_client(vec![
            // Existence pre-check: not found.
            ScriptedResponse::error(404, "not found"),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let created = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect("create against an absent key must succeed");
        assert_eq!(created.version, 1);

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
            ]
        );

        // The write must be create-only (check-and-set of 0). A blind
        // overwrite — the pre-CAS behavior — carries no options at all.
        let body = parse_write_body(&vault.request_bodies()[1]);
        assert_eq!(body["options"]["cas"], serde_json::json!(0), "create must be create-only: {body}");
    }

    /// Concurrent same-name create: both nodes pass the not-found pre-check,
    /// exactly one create-only write commits, and the loser reports
    /// KeyAlreadyExists instead of overwriting the winner's material (which
    /// would permanently orphan every DEK the winner already wrapped).
    #[tokio::test]
    async fn wired_concurrent_create_loser_reports_key_already_exists() {
        let (vault, client) = scripted_client(vec![
            // Existence pre-check: not found (the racing create has not
            // committed yet).
            ScriptedResponse::error(404, "not found"),
            // The create-only write loses: the racing create committed first.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
        ])
        .await;

        let error = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect_err("the losing create must fail");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "the loser must not retry or fall back to a blind write: {requests:?}");
    }

    /// Deletion sweep racing a cancellation: the sweep's tombstone write loses
    /// its check-and-set race, the re-read observes the cancelled (Active)
    /// record, and the sweep reports StateChanged without deleting anything.
    /// The blind tombstone this replaces would have overwritten the committed
    /// cancellation and destroyed the key.
    #[tokio::test]
    async fn wired_expired_key_sweep_yields_to_concurrent_cancellation() {
        let now = Zoned::now() + Duration::from_secs(3600);
        let mut pending = healthy_key_data();
        pending.status = KeyStatus::PendingDeletion;
        pending.deletion_date = Some(Zoned::now());

        let vault = ScriptedVault::serve(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&pending)),
            // The cancellation commits between the read and the tombstone.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            // The re-read observes the cancelled (Active again) record.
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        let outcome = backend
            .remove_expired_key("wired-key", &now)
            .await
            .expect("the sweep must settle by observing the cancelled state");
        assert_eq!(outcome, ExpiredKeyRemoval::StateChanged);

        let requests = vault.requests();
        assert_eq!(requests.len(), 5, "{requests:?}");
        assert!(
            !requests.iter().any(|line| line.starts_with("DELETE ")),
            "a sweep that lost to a cancellation must not delete anything: {requests:?}"
        );
        // The one write attempt was the check-and-set tombstone.
        let body = parse_write_body(&vault.request_bodies()[2]);
        assert_eq!(body["options"]["cas"], serde_json::json!(1), "{body}");
        assert_eq!(body["data"]["status"], serde_json::json!("Deleted"), "{body}");
    }

    /// The other half of the cancel × sweep interleaving: once the sweep has
    /// tombstoned the record, a cancellation re-validates against the fresh
    /// state and fails instead of resurrecting a key whose material is about
    /// to be destroyed.
    #[tokio::test]
    async fn wired_cancel_deletion_after_sweep_tombstone_fails_closed() {
        let mut pending = healthy_key_data();
        pending.status = KeyStatus::PendingDeletion;
        pending.deletion_date = Some(Zoned::now());
        let mut tombstoned = healthy_key_data();
        tombstoned.status = KeyStatus::Deleted;

        let vault = ScriptedVault::serve(vec![
            // describe_key still observes the pre-sweep PendingDeletion state
            // (one read for the key info, one for the stored metadata).
            ScriptedResponse::ok(kv2_read_data(&pending)),
            ScriptedResponse::ok(kv2_read_data(&pending)),
            // The check-and-set update re-reads and observes the tombstone.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(kv2_read_data(&tombstoned)),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        let error = backend
            .cancel_key_deletion(CancelKeyDeletionRequest {
                key_id: "wired-key".to_string(),
            })
            .await
            .expect_err("cancelling after the sweep tombstoned the key must fail");
        assert!(
            matches!(&error, KmsError::InvalidOperation { message } if message.contains("not pending deletion")),
            "got {error:?}"
        );

        let requests = vault.requests();
        assert!(
            !requests.iter().any(|line| line.starts_with("POST ")),
            "a cancellation that lost to the sweep must not write anything: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_schedule_deletion_retries_after_cas_conflict() {
        let vault = ScriptedVault::serve(vec![
            // describe_key: key info plus stored metadata.
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // Attempt 1 loses its check-and-set race.
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // Attempt 2: the re-read re-passes the state gate and commits.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        let response = backend
            .delete_key(DeleteKeyRequest {
                key_id: "wired-key".to_string(),
                pending_window_in_days: Some(7),
                force_immediate: Some(false),
                confirm_key_id: None,
            })
            .await
            .expect("the schedule must retry past the lost race and commit");
        assert!(response.deletion_date.is_some());

        let requests = vault.requests();
        assert_eq!(requests.len(), 8, "{requests:?}");
        let committed = parse_write_body(&vault.request_bodies()[7]);
        assert_eq!(committed["options"]["cas"], serde_json::json!(2), "{committed}");
        assert_eq!(committed["data"]["status"], serde_json::json!("PendingDeletion"), "{committed}");
        assert!(
            !committed["data"]["deletion_date"].is_null(),
            "the deadline must be persisted: {committed}"
        );
    }

    /// KmsManager::delete_key is the enforcement point for the waiting window;
    /// this pins the backend's defensive copy of the same bound, which is all
    /// that stands between a direct backend caller and a one-day window.
    #[tokio::test]
    async fn wired_schedule_deletion_refuses_a_window_outside_the_supported_range() {
        for days in [MIN_PENDING_DELETION_WINDOW_DAYS - 1, MAX_PENDING_DELETION_WINDOW_DAYS + 1] {
            let vault = ScriptedVault::serve(vec![
                // describe_key: key info plus stored metadata.
                ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
                ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ])
            .await;
            let config = KmsConfig::vault(
                url::Url::parse(&vault.address).expect("scripted vault address should parse"),
                "scripted-token".to_string(),
            )
            .with_insecure_development_defaults();
            let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

            let result = backend
                .delete_key(DeleteKeyRequest {
                    key_id: "wired-key".to_string(),
                    pending_window_in_days: Some(days),
                    ..Default::default()
                })
                .await;
            assert!(
                matches!(result, Err(KmsError::InvalidOperation { .. })),
                "a {days}-day window must be refused, got {result:?}"
            );

            let requests = vault.requests();
            assert!(
                !requests.iter().any(|line| line.starts_with("POST ")),
                "a refused window must not write anything: {requests:?}"
            );
        }
    }

    /// Conflict semantics are re-read *and* re-gate: when the re-read after a
    /// lost race shows the key was concurrently scheduled for deletion, the
    /// state gate rejects the retry instead of blindly re-applying it.
    #[tokio::test]
    async fn wired_schedule_deletion_regates_after_conflict() {
        let mut already_pending = healthy_key_data();
        already_pending.status = KeyStatus::PendingDeletion;
        already_pending.deletion_date = Some(Zoned::now() + Duration::from_secs(7 * 86400));

        let vault = ScriptedVault::serve(vec![
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // A concurrent schedule committed first.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(kv2_read_data(&already_pending)),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        let error = backend
            .delete_key(DeleteKeyRequest {
                key_id: "wired-key".to_string(),
                pending_window_in_days: Some(7),
                force_immediate: Some(false),
                confirm_key_id: None,
            })
            .await
            .expect_err("the retry must re-run the state gate against the fresh record");
        assert!(
            matches!(&error, KmsError::InvalidOperation { message } if message.contains("pending deletion")),
            "got {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 7, "{requests:?}");
        assert_eq!(
            requests.iter().filter(|line| line.starts_with("POST ")).count(),
            1,
            "the rejected retry must not write again: {requests:?}"
        );
    }

    /// The read-modify-write loop is bounded: persistent contention surfaces
    /// the typed conflict error after `LIFECYCLE_CAS_ATTEMPTS` full
    /// read-gate-write cycles instead of spinning or falling back to a blind
    /// write.
    #[tokio::test]
    async fn wired_lifecycle_cas_retries_are_bounded() {
        let mut responses = Vec::new();
        for secret_version in 1..=LIFECYCLE_CAS_ATTEMPTS as u64 {
            responses.push(ScriptedResponse::ok(kv2_metadata_read_data(secret_version)));
            responses.push(ScriptedResponse::ok(kv2_read_data(&healthy_key_data())));
            responses.push(ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE));
        }
        let (vault, client) = scripted_client(responses).await;

        let error = client
            .disable_key("wired-key", None)
            .await
            .expect_err("persistent contention must surface the typed conflict error");
        assert!(
            matches!(&error, KmsError::InvalidOperation { message } if message.contains("Concurrent modification")),
            "got {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 3 * LIFECYCLE_CAS_ATTEMPTS as usize, "{requests:?}");
        assert_eq!(
            requests.iter().filter(|line| line.starts_with("POST ")).count(),
            LIFECYCLE_CAS_ATTEMPTS as usize,
            "every attempt must be a fresh read-gate-write cycle: {requests:?}"
        );
    }

    /// Concurrent rotations use KV2 create-only records and a CAS pointer
    /// switch. The stateful scripted Vault applies those preconditions to real
    /// HTTP requests, so this test proves committed versions are unique and
    /// contiguous instead of only checking that several calls returned.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wired_concurrent_kv2_rotations_commit_unique_monotonic_versions() {
        use std::collections::HashSet;
        use std::sync::Arc;

        const ATTEMPTS: usize = 8;
        let mut key_data = healthy_key_data();
        key_data.baseline_version = Some(1);
        let (vault, client) = scripted_kv2_client(&key_data).await;
        let client = Arc::new(client);
        let barrier = Arc::new(tokio::sync::Barrier::new(ATTEMPTS));
        let tasks: Vec<_> = (0..ATTEMPTS)
            .map(|_| {
                let client = Arc::clone(&client);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    client.rotate_key("wired-key", None).await
                })
            })
            .collect();

        let mut committed_versions = Vec::new();
        let mut errors = Vec::new();
        for task in tasks {
            match task.await.expect("join concurrent rotation task") {
                Ok(result) => committed_versions.push(result.version),
                Err(error) => errors.push(error),
            }
        }

        assert!(
            !committed_versions.is_empty(),
            "at least one concurrent rotation must commit; errors: {errors:?}"
        );
        assert!(
            errors.iter().all(
                |error| matches!(error, KmsError::InvalidOperation { message } if message.contains("Concurrent modification"))
            ),
            "a lost CAS race must be the only expected failure: {errors:?}"
        );

        let mut sorted_versions = committed_versions.clone();
        sorted_versions.sort_unstable();
        let unique_versions: HashSet<_> = sorted_versions.iter().copied().collect();
        assert_eq!(
            unique_versions.len(),
            sorted_versions.len(),
            "concurrent rotations must never return a version twice: {committed_versions:?}"
        );

        let successful_rotations = u32::try_from(sorted_versions.len()).expect("test attempts fit u32");
        let current_version = 1u32 + successful_rotations;
        assert_eq!(
            sorted_versions,
            (2..=current_version).collect::<Vec<_>>(),
            "committed versions must form one monotonic sequence: {sorted_versions:?}"
        );

        let snapshot = vault.kv2_snapshot().expect("stateful KV2 snapshot");
        let persisted_version = snapshot.current_data["version"]
            .as_u64()
            .and_then(|version| u32::try_from(version).ok())
            .expect("current KV2 record must carry a u32 key version");
        assert_eq!(persisted_version, current_version);
        assert!(
            snapshot.current_secret_version >= u64::from(current_version),
            "the KV2 secret version must advance with each committed pointer switch"
        );
        assert_eq!(
            snapshot.version_records.keys().copied().collect::<Vec<_>>(),
            (1..=current_version).collect::<Vec<_>>(),
            "every committed KMS version must have exactly one immutable record"
        );

        let mut materials = HashSet::new();
        for (version, record) in &snapshot.version_records {
            let material = record["encrypted_key_material"]
                .as_str()
                .expect("version record must carry encrypted key material");
            assert!(materials.insert(material), "version {version} reuses another version's material");
        }
        assert_eq!(
            snapshot.current_data["encrypted_key_material"],
            snapshot
                .version_records
                .get(&current_version)
                .expect("current version record")["encrypted_key_material"],
            "the top-level fast path must match the current immutable version record"
        );
    }

    /// The tags write-back after a create is a check-and-set read-modify-write
    /// that carries the key material over from the freshly read record.
    #[tokio::test]
    async fn wired_create_key_tags_writeback_is_check_and_set() {
        let vault = ScriptedVault::serve(vec![
            // create_key: existence pre-check misses, create-only write lands.
            ScriptedResponse::error(404, "not found"),
            ScriptedResponse::ok(kv2_write_ack()),
            // store_key_metadata: versioned read plus check-and-set write.
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;
        let config = KmsConfig::vault(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");

        let response = backend
            .create_key(CreateKeyRequest {
                key_name: Some("wired-key".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                tags: HashMap::from([("team".to_string(), "storage".to_string())]),
                ..Default::default()
            })
            .await
            .expect("create with tags must succeed");
        assert_eq!(response.key_id, "wired-key");

        let bodies = vault.request_bodies();
        let create = parse_write_body(&bodies[1]);
        assert_eq!(create["options"]["cas"], serde_json::json!(0), "{create}");

        let writeback = parse_write_body(&bodies[4]);
        assert_eq!(writeback["options"]["cas"], serde_json::json!(1), "{writeback}");
        assert_eq!(writeback["data"]["tags"]["team"], serde_json::json!("storage"), "{writeback}");
        assert_eq!(
            writeback["data"]["encrypted_key_material"],
            serde_json::json!(healthy_key_data().encrypted_key_material),
            "the write-back must preserve the material of the freshly read record: {writeback}"
        );
    }

    /// Tag updates are check-and-set read-modify-writes over the live record,
    /// never blind overwrites: they preserve the material and the tags they did
    /// not address.
    #[tokio::test]
    async fn wired_tag_key_writeback_is_check_and_set() {
        let mut key_data = healthy_key_data();
        key_data.tags = HashMap::from([("name".to_string(), "wired-key".to_string())]);
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        client
            .tag_key("wired-key", &HashMap::from([("team".to_string(), "storage".to_string())]))
            .await
            .expect("tagging must succeed");

        let bodies = vault.request_bodies();
        let writeback = parse_write_body(&bodies[2]);
        assert_eq!(writeback["options"]["cas"], serde_json::json!(1), "{writeback}");
        assert_eq!(writeback["data"]["tags"]["team"], serde_json::json!("storage"), "{writeback}");
        assert_eq!(
            writeback["data"]["tags"]["name"],
            serde_json::json!("wired-key"),
            "a tag update must not drop tags it did not address: {writeback}"
        );
        assert_eq!(
            writeback["data"]["encrypted_key_material"],
            serde_json::json!(healthy_key_data().encrypted_key_material),
            "the write-back must preserve the material of the freshly read record: {writeback}"
        );
    }

    /// Rejecting the identity tag happens before any Vault call, so a rejected
    /// request cannot leave a partial write behind.
    #[tokio::test]
    async fn wired_identity_tag_update_is_rejected_before_any_vault_call() {
        let (vault, client) = scripted_client(Vec::new()).await;

        for result in [
            client
                .tag_key("wired-key", &HashMap::from([("name".to_string(), "other".to_string())]))
                .await,
            client.untag_key("wired-key", &["name".to_string()]).await,
        ] {
            let error = result.expect_err("the identity tag must not be writable");
            assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        }
        assert!(
            vault.request_bodies().is_empty(),
            "a rejected metadata update must not reach Vault: {:?}",
            vault.request_bodies()
        );
    }

    /// A version record above the current pointer means the top-level record
    /// regressed (a lost update rolled back a committed rotation). Resolving
    /// material through such a record must fail closed instead of quietly
    /// serving it while new encryptions keep using the rolled-back material.
    #[tokio::test]
    async fn wired_decrypt_fails_closed_when_current_version_regressed() {
        let material_v2 = [0x43u8; 32];
        let record_v2 = VaultKeyVersionRecord {
            version: 2,
            encrypted_key_material: general_purpose::STANDARD.encode(material_v2),
            created_at: Zoned::now(),
        };
        // A well-formed envelope wrapped under version 2 — under a reverted
        // guard this decrypt would *succeed*, which is exactly the masked
        // rollback this test pins down.
        let (encrypted_key, nonce) = AesDekCrypto::new()
            .encrypt(&material_v2, b"dek-plaintext")
            .await
            .expect("wrap test DEK");
        let envelope = DataKeyEnvelope {
            key_id: "dek".to_string(),
            master_key_id: "wired-key".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key,
            nonce,
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: Some(2),
        };
        let ciphertext = serde_json::to_vec(&envelope).expect("serialize envelope");

        let (vault, client) = scripted_client(vec![
            // Top-level record: current version rolled back to 1.
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // ...yet the immutable record for version 2 exists.
            ScriptedResponse::ok(kv2_read_version_record_data(&record_v2)),
        ])
        .await;

        let error = client
            .decrypt(
                &DecryptRequest {
                    ciphertext,
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("a version record above the current pointer must fail the decrypt");
        assert!(
            matches!(&error, KmsError::InternalError { message } if message.contains("behind existing version record")),
            "got {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "the inconsistency must be decided from the two reads: {requests:?}");
    }

    /// Rotation refuses to extend a version history whose records already
    /// reach more than one step past the current pointer: that state cannot
    /// come from the rotation protocol and re-minting those version numbers
    /// would collide with immutable records.
    #[tokio::test]
    async fn wired_rotate_fails_closed_when_version_history_regressed() {
        // The baseline is intact, so this isolates the monotonicity guard from
        // the lost-baseline guard that also inspects the version listing.
        let mut key_data = healthy_key_data();
        key_data.baseline_version = Some(1);
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(4)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            // Version records reach 3 while the current pointer says 1.
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2", "3"] })),
        ])
        .await;

        let error = client
            .rotate_key("wired-key", None)
            .await
            .expect_err("a regressed version history must fail the rotation");
        assert!(
            matches!(&error, KmsError::InternalError { message } if message.contains("refusing to extend")),
            "got {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 3, "{requests:?}");
        assert!(
            !requests.iter().any(|line| line.starts_with("POST ")),
            "nothing may be written on a regressed history: {requests:?}"
        );
    }

    /// A record exactly one past the current pointer is the footprint of an
    /// interrupted rotation; the next rotation must adopt its persisted
    /// material (the monotonicity guard must not misread it as a regression).
    #[tokio::test]
    async fn wired_rotate_adopts_interrupted_rotation_record() {
        let mut key_data = healthy_key_data();
        key_data.baseline_version = Some(1);
        let adopted_material = rotated_material();
        let record_v2 = VaultKeyVersionRecord {
            version: 2,
            encrypted_key_material: adopted_material.clone(),
            created_at: Zoned::now(),
        };

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(5)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            // The interrupted rotation left a record for version 2.
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2"] })),
            // The create-only write for version 2 conflicts...
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // ...so the rotation reads the persisted record back and adopts it.
            ScriptedResponse::ok(kv2_read_version_record_data(&record_v2)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let rotated = client
            .rotate_key("wired-key", None)
            .await
            .expect("an interrupted rotation must be recoverable");
        assert_eq!(rotated.version, 2);

        // The pointer switch must commit the adopted (persisted) material, not
        // freshly generated material that no record holds.
        let committed = parse_write_body(&vault.request_bodies()[5]);
        assert_eq!(committed["options"]["cas"], serde_json::json!(5), "{committed}");
        assert_eq!(committed["data"]["version"], serde_json::json!(2), "{committed}");
        assert_eq!(
            committed["data"]["encrypted_key_material"],
            serde_json::json!(adopted_material),
            "{committed}"
        );
    }

    /// The mixed-version corruption path: a node older than versioned rotation
    /// performed a lifecycle write, which rewrites the whole key record and
    /// silently drops the `baseline_version` it does not know. Version records
    /// without a baseline can only mean that, so the next rotation must refuse
    /// instead of freezing a fresh baseline at the current version — which would
    /// resolve every pre-versioning envelope to material that never wrapped it.
    #[tokio::test]
    async fn wired_rotate_refuses_when_baseline_was_erased() {
        let mut key_data = healthy_key_data();
        key_data.version = 2;
        key_data.baseline_version = None;

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(3)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            // The key was rotated once, so records for versions 1 and 2 exist —
            // the baseline the first rotation pinned is gone from the record.
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2"] })),
        ])
        .await;

        let error = client
            .rotate_key("wired-key", None)
            .await
            .expect_err("a key whose baseline was erased must not rotate");
        assert!(
            matches!(
                &error,
                KmsError::BaselineVersionLost { key_id, oldest_version: 1 } if key_id == "wired-key"
            ),
            "the refusal must name the baseline to restore: {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=3".to_string(),
                "LIST /v1/secret/metadata/rustfs/kms/keys/wired-key/versions".to_string(),
            ],
            "the refusal must be decided from reads alone"
        );
        assert!(
            !requests.iter().any(|line| line.starts_with("POST ")),
            "nothing may be written once the baseline is known lost: {requests:?}"
        );
    }

    /// A key whose baseline is intact keeps rotating: the guard must key off the
    /// contradiction, not off the presence of version records.
    #[tokio::test]
    async fn wired_rotate_with_intact_baseline_still_commits() {
        let mut key_data = healthy_key_data();
        key_data.version = 2;
        key_data.baseline_version = Some(1);

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(3)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2"] })),
            // Version 3's material record, then the pointer switch.
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let rotated = client
            .rotate_key("wired-key", None)
            .await
            .expect("a key with an intact baseline must still rotate");
        assert_eq!(rotated.version, 3);

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=3".to_string(),
                "LIST /v1/secret/metadata/rustfs/kms/keys/wired-key/versions".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key/versions/3".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
            ],
            "an intact baseline skips the freeze step and commits the usual two writes"
        );

        let bodies = vault.request_bodies();
        let record = parse_write_body(&bodies[3]);
        assert_eq!(
            record["options"]["cas"],
            serde_json::json!(0),
            "version records are create-only: {record}"
        );
        let committed = parse_write_body(&bodies[4]);
        assert_eq!(committed["options"]["cas"], serde_json::json!(3), "{committed}");
        assert_eq!(committed["data"]["version"], serde_json::json!(3), "{committed}");
        assert_eq!(
            committed["data"]["baseline_version"],
            serde_json::json!(1),
            "the existing baseline must be carried over untouched: {committed}"
        );
    }

    /// A never-rotated key legitimately has no baseline and no version records,
    /// so its first rotation must still freeze one. The guard must not read this
    /// state as an erased baseline.
    #[tokio::test]
    async fn wired_first_rotate_of_never_rotated_key_still_freezes_baseline() {
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(7)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // The versions directory does not exist yet: Vault reports that as a
            // 404 with an empty `errors` array.
            ScriptedResponse::empty_list_404(),
            // Freeze version 1, persist the baseline, create version 2, switch.
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let rotated = client
            .rotate_key("wired-key", None)
            .await
            .expect("the first rotation of a never-rotated key must commit");
        assert_eq!(rotated.version, 2);

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=7".to_string(),
                "LIST /v1/secret/metadata/rustfs/kms/keys/wired-key/versions".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key/versions/1".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key/versions/2".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
            ],
            "{requests:?}"
        );

        let bodies = vault.request_bodies();
        let frozen = parse_write_body(&bodies[3]);
        assert_eq!(
            frozen["data"]["encrypted_key_material"],
            serde_json::json!(healthy_key_data().encrypted_key_material),
            "the baseline record must freeze the pre-rotation material: {frozen}"
        );
        let baseline_commit = parse_write_body(&bodies[4]);
        assert_eq!(
            baseline_commit["data"]["baseline_version"],
            serde_json::json!(1),
            "the first rotation must pin the baseline: {baseline_commit}"
        );
    }

    /// The rotation-age gauge ages a key from `rotated_at`, falling back to
    /// `created_at`. A rotation that commits without recording its time makes a
    /// key rotated many times read exactly like one that was never rotated, so
    /// the commit must carry the timestamp and a later describe must report it.
    /// Reverting either half turns this test red.
    #[tokio::test]
    async fn wired_rotate_persists_rotation_time_and_describe_reports_it() {
        let created_at = Zoned::now() - Duration::from_secs(365 * 86400);
        let mut key_data = healthy_key_data();
        key_data.created_at = created_at.clone();
        key_data.version = 2;
        key_data.baseline_version = Some(1);
        key_data.description = Some("payload key".to_string());
        key_data.metadata.insert("owner".to_string(), "platform".to_string());
        key_data.tags.insert("env".to_string(), "prod".to_string());

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(3)),
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2"] })),
            // Version 3's material record, then the pointer switch.
            ScriptedResponse::ok(kv2_write_ack()),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let rotated = client.rotate_key("wired-key", None).await.expect("rotate a healthy key");
        let reported = rotated.rotated_at.clone().expect("a committed rotation must report its time");

        let bodies = vault.request_bodies();
        let committed = parse_write_body(&bodies[4]);
        let persisted: VaultKeyData =
            serde_json::from_value(committed["data"].clone()).expect("the committed record must deserialize");
        assert_eq!(
            persisted.rotated_at.as_ref().map(Zoned::timestamp),
            Some(reported.timestamp()),
            "the rotation must persist the time it reports: {committed}"
        );

        // The rest of the record rides through the read-modify-write untouched;
        // a rotation that dropped any of it would corrupt the key.
        assert_eq!(persisted.version, 3, "{committed}");
        assert_eq!(persisted.created_at.timestamp(), created_at.timestamp(), "{committed}");
        assert_eq!(persisted.baseline_version, Some(1), "{committed}");
        assert_eq!(persisted.description.as_deref(), Some("payload key"), "{committed}");
        assert_eq!(persisted.metadata.get("owner").map(String::as_str), Some("platform"), "{committed}");
        assert_eq!(persisted.tags.get("env").map(String::as_str), Some("prod"), "{committed}");

        // Describing the committed record must report the rotation, not the
        // creation a year earlier that the gauge would otherwise fall back to.
        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&persisted))]).await;
        let described = client
            .describe_key("wired-key", None)
            .await
            .expect("describe the rotated key");
        assert_eq!(
            described.rotated_at.as_ref().map(Zoned::timestamp),
            Some(reported.timestamp()),
            "describe must report the persisted rotation time"
        );
        assert_ne!(
            described.rotated_at.as_ref().map(Zoned::timestamp),
            Some(created_at.timestamp()),
            "a rotated key must not be aged from its creation"
        );
    }

    /// A record written before rotation timestamps were persisted carries no
    /// rotation time. Reporting one anyway — the current time, the read time —
    /// would tell the rotation-age gauge the key was just rotated and silence a
    /// genuinely overdue key, so the absence has to travel as `None`.
    #[tokio::test]
    async fn wired_describe_key_invents_no_rotation_time_for_legacy_records() {
        let mut key_data = healthy_key_data();
        key_data.created_at = Zoned::now() - Duration::from_secs(365 * 86400);
        key_data.version = 4;
        key_data.baseline_version = Some(1);

        let mut record = kv2_read_data(&key_data);
        record["data"]
            .as_object_mut()
            .expect("key record must be a JSON object")
            .remove("rotated_at")
            .expect("current records must carry the field");

        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(record)]).await;
        let described = client
            .describe_key("wired-key", None)
            .await
            .expect("a record without the field must still describe");
        assert!(
            described.rotated_at.is_none(),
            "an unstamped record must not be reported as freshly rotated, got {:?}",
            described.rotated_at
        );
        assert_eq!(
            described.created_at.timestamp(),
            key_data.created_at.timestamp(),
            "the rest of the legacy record must survive the read"
        );
        assert_eq!(described.version, 4);
    }

    /// The persisted KV2 record round-trips its rotation time, and records
    /// written before the field existed keep deserializing (as None) with the
    /// rest of their contents intact.
    #[test]
    fn vault_key_data_rotated_at_round_trips_and_stays_backward_compatible() {
        let rotated_at = Zoned::now();
        let mut key_data = healthy_key_data();
        key_data.rotated_at = Some(rotated_at.clone());
        key_data.baseline_version = Some(1);
        key_data.version = 2;
        key_data.tags.insert("env".to_string(), "prod".to_string());

        let mut value = serde_json::to_value(&key_data).expect("serialize");
        let restored: VaultKeyData = serde_json::from_value(value.clone()).expect("round trip");
        assert_eq!(
            restored.rotated_at.as_ref().map(Zoned::timestamp),
            Some(rotated_at.timestamp()),
            "the rotation time must survive the KV2 round trip"
        );

        value
            .as_object_mut()
            .expect("record must be a JSON object")
            .remove("rotated_at")
            .expect("current records must carry the field");
        let legacy: VaultKeyData = serde_json::from_value(value).expect("legacy record must deserialize");
        assert!(legacy.rotated_at.is_none());
        assert_eq!(legacy.version, 2);
        assert_eq!(legacy.baseline_version, Some(1));
        assert_eq!(legacy.tags.get("env").map(String::as_str), Some("prod"));
    }

    /// Reading a pre-versioning envelope against a key whose baseline was erased
    /// resolves to the current version, whose material never wrapped it. The
    /// unwrap therefore fails (AES-GCM cannot yield plaintext under the wrong
    /// key); the failure must name the erased baseline instead of surfacing an
    /// undiagnosable authentication error.
    #[tokio::test]
    async fn wired_decrypt_reports_erased_baseline_for_pre_versioning_envelope() {
        let baseline_material = [0x41u8; 32];
        let (encrypted_key, nonce) = AesDekCrypto::new()
            .encrypt(&baseline_material, b"dek-plaintext")
            .await
            .expect("wrap test DEK under the baseline material");
        // A pre-versioning envelope: no master_key_version field.
        let envelope = DataKeyEnvelope {
            key_id: "dek".to_string(),
            master_key_id: "wired-key".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key,
            nonce,
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope).expect("serialize envelope");

        // The key has been rotated (current material differs from the baseline's)
        // and its baseline pointer was erased by an older node.
        let mut key_data = healthy_key_data();
        key_data.version = 2;
        key_data.baseline_version = None;
        key_data.encrypted_key_material = rotated_material();

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_read_data(&key_data)),
            ScriptedResponse::ok(serde_json::json!({ "keys": ["1", "2"] })),
        ])
        .await;

        let error = client
            .decrypt(
                &DecryptRequest {
                    ciphertext,
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("the wrong master key version cannot unwrap the data key");
        assert!(
            matches!(
                &error,
                KmsError::BaselineVersionLost { key_id, oldest_version: 1 } if key_id == "wired-key"
            ),
            "the failure must point at the erased baseline: {error:?}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "the diagnosis costs one listing after the failure: {requests:?}");
        assert!(requests[1].contains("/versions"), "{requests:?}");
    }

    /// Without version records the key was never rotated by a versioning build,
    /// so the current version really is the right one for a pre-versioning
    /// envelope and an unwrap failure has some other cause. Misreporting it as a
    /// lost baseline would send operators after a baseline that never existed.
    #[tokio::test]
    async fn wired_decrypt_keeps_original_error_when_key_was_never_rotated() {
        let (encrypted_key, nonce) = AesDekCrypto::new()
            .encrypt(&[0x41u8; 32], b"dek-plaintext")
            .await
            .expect("wrap test DEK");
        let envelope = DataKeyEnvelope {
            key_id: "dek".to_string(),
            master_key_id: "wired-key".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key,
            nonce,
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope).expect("serialize envelope");

        let (vault, client) = scripted_client(vec![
            // The key record holds different material than the envelope was
            // wrapped with, but has no version history at all.
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::error(404, "not found"),
        ])
        .await;

        let error = client
            .decrypt(
                &DecryptRequest {
                    ciphertext,
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("the mismatched material must still fail the unwrap");
        assert!(
            matches!(error, KmsError::CryptographicError { .. }),
            "a key with no version records must keep its original failure: {error:?}"
        );
        assert_eq!(vault.requests().len(), 2);
    }

    /// The common upgrade shape — pre-versioning envelopes against a key that was
    /// never rotated — must keep decrypting with exactly one Vault read. The
    /// diagnosis above may not add a listing to reads that succeed.
    #[tokio::test]
    async fn wired_decrypt_of_pre_versioning_envelope_adds_no_request() {
        let key_data = healthy_key_data();
        let key_material = general_purpose::STANDARD
            .decode(&key_data.encrypted_key_material)
            .expect("decode fixture material");
        let (encrypted_key, nonce) = AesDekCrypto::new()
            .encrypt(&key_material, b"dek-plaintext")
            .await
            .expect("wrap test DEK under the current material");
        let envelope = DataKeyEnvelope {
            key_id: "dek".to_string(),
            master_key_id: "wired-key".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key,
            nonce,
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope).expect("serialize envelope");

        let (vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&key_data))]).await;

        let (plaintext, _opened_by) = client
            .decrypt(
                &DecryptRequest {
                    ciphertext,
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("a pre-versioning envelope on a never-rotated key must decrypt");
        assert_eq!(plaintext, b"dek-plaintext".to_vec());
        assert_eq!(
            vault.requests(),
            vec!["GET /v1/secret/data/rustfs/kms/keys/wired-key".to_string()],
            "a successful read must not pay for the lost-baseline diagnosis"
        );
    }

    /// The Vault-side state of one KV2 key: the top-level record plus the
    /// immutable version records rotations froze.
    ///
    /// The scripted responder serves canned responses, so a multi-operation
    /// scenario has to carry the state between operations itself. Rotations
    /// fold what they *wrote* back into this state (see [`Self::apply_writes`]),
    /// which is what makes the rotation regressions below real: the material a
    /// later decrypt resolves is the material the rotation persisted, not a
    /// fixture the test invented.
    struct KeyState {
        key_data: VaultKeyData,
        version_records: Vec<VaultKeyVersionRecord>,
    }

    impl KeyState {
        /// A never-rotated key: no version records exist yet.
        fn new(key_data: VaultKeyData) -> Self {
            Self {
                key_data,
                version_records: Vec::new(),
            }
        }

        fn version_record(&self, version: u32) -> &VaultKeyVersionRecord {
            self.version_records
                .iter()
                .find(|record| record.version == version)
                .unwrap_or_else(|| panic!("no version record was frozen for version {version}"))
        }

        /// The versions-directory listing; a key with no records has no
        /// directory at all.
        fn versions_listing(&self) -> ScriptedResponse {
            if self.version_records.is_empty() {
                // Vault answers a LIST of a path holding nothing with a 404
                // carrying an empty `errors` array — not a message-bearing one,
                // which would mean the path was never routed at all.
                return ScriptedResponse::empty_list_404();
            }
            let keys: Vec<String> = self.version_records.iter().map(|record| record.version.to_string()).collect();
            ScriptedResponse::ok(serde_json::json!({ "keys": keys }))
        }

        /// Fold the writes an operation made into the state, so the next
        /// operation reads exactly what Vault would now hold.
        fn apply_writes(&mut self, requests: &[String], bodies: &[String]) {
            for (line, body) in requests.iter().zip(bodies) {
                let Some(path) = line.strip_prefix("POST ") else {
                    continue;
                };
                let data = parse_write_body(body)["data"].clone();
                if path.contains("/versions/") {
                    let record: VaultKeyVersionRecord = serde_json::from_value(data).expect("version record write body");
                    self.version_records.retain(|existing| existing.version != record.version);
                    self.version_records.push(record);
                } else {
                    self.key_data = serde_json::from_value(data).expect("key record write body");
                }
            }
        }
    }

    /// Encrypt against a scripted Vault serving `state`. The fresh client's
    /// first wrap also reserves its budget block, so that exchange is scripted
    /// alongside the key-record read.
    async fn encrypt_scripted(state: &KeyState, plaintext: &[u8]) -> EncryptResponse {
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_read_data(&state.key_data)),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&state.key_data)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;
        client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: plaintext.to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("encrypt must produce an envelope")
    }

    /// Rotate against a scripted Vault seeded with `state`, and return the state
    /// Vault holds afterwards.
    ///
    /// The first rotation freezes the baseline before creating the next version
    /// (four writes); later rotations skip that step (two writes).
    async fn rotate_scripted(state: &KeyState) -> KeyState {
        let writes = if state.key_data.baseline_version.is_none() { 4 } else { 2 };
        let mut responses = vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&state.key_data)),
            state.versions_listing(),
        ];
        responses.extend((0..writes).map(|_| ScriptedResponse::ok(kv2_write_ack())));
        let (vault, client) = scripted_client(responses).await;

        let rotated = client.rotate_key("wired-key", None).await.expect("rotation must commit");
        assert_eq!(rotated.version, state.key_data.version + 1, "a rotation must advance the version");

        let mut next = KeyState {
            key_data: state.key_data.clone(),
            version_records: state.version_records.clone(),
        };
        next.apply_writes(&vault.requests(), &vault.request_bodies());
        next
    }

    /// Decrypt against a scripted Vault serving `state`, scripting the
    /// version-record read the envelope's own version calls for. Returns the
    /// plaintext together with the requests the decrypt made.
    async fn decrypt_scripted(state: &KeyState, ciphertext: &[u8]) -> (Vec<u8>, Vec<String>) {
        let envelope: DataKeyEnvelope = serde_json::from_slice(ciphertext).expect("envelope must parse");
        let mut responses = vec![ScriptedResponse::ok(kv2_read_data(&state.key_data))];
        if let Some(version) = envelope.master_key_version
            && version != state.key_data.version
        {
            responses.push(ScriptedResponse::ok(kv2_read_version_record_data(state.version_record(version))));
        }
        let (vault, client) = scripted_client(responses).await;

        let (plaintext, _opened_by) = client
            .decrypt(
                &DecryptRequest {
                    ciphertext: ciphertext.to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("the envelope must decrypt against the rotated key");
        (plaintext, vault.requests())
    }

    /// The forward half of the rotation contract: data written before a rotation
    /// stays readable after it, with no live Vault involved.
    ///
    /// The negative half (a regressed pointer must fail closed) is covered by
    /// `wired_decrypt_fails_closed_when_current_version_regressed`; this pins the
    /// path that must keep working, which the fail-closed guards could otherwise
    /// tighten into a rotation that orphans every existing object.
    #[tokio::test]
    async fn wired_kv2_envelope_from_before_rotation_still_decrypts() {
        const PLAINTEXT: &[u8] = b"written-before-the-rotation";
        let state_v1 = KeyState::new(healthy_key_data());

        let encrypted_v1 = encrypt_scripted(&state_v1, PLAINTEXT).await;
        let envelope_v1: DataKeyEnvelope = serde_json::from_slice(&encrypted_v1.ciphertext).expect("envelope must parse");
        assert_eq!(envelope_v1.master_key_version, Some(1));

        let state_v2 = rotate_scripted(&state_v1).await;
        assert_eq!(state_v2.key_data.version, 2);
        assert_eq!(state_v2.key_data.baseline_version, Some(1));
        assert_ne!(
            state_v2.key_data.encrypted_key_material, state_v1.key_data.encrypted_key_material,
            "the rotation must have replaced the current material, or the decrypt below proves nothing"
        );

        let (plaintext, requests) = decrypt_scripted(&state_v2, &encrypted_v1.ciphertext).await;
        assert_eq!(
            plaintext, PLAINTEXT,
            "the pre-rotation envelope must yield its original plaintext, not merely avoid an error"
        );
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key/versions/1".to_string(),
            ],
            "the old envelope must resolve through the immutable v1 record"
        );

        // The rotation is not cosmetic: new writes go to the rotated version and
        // still round-trip, so both generations are live at once.
        let encrypted_v2 = encrypt_scripted(&state_v2, b"written-after-the-rotation").await;
        let envelope_v2: DataKeyEnvelope = serde_json::from_slice(&encrypted_v2.ciphertext).expect("envelope must parse");
        assert_eq!(envelope_v2.master_key_version, Some(2), "new envelopes must carry the rotated version");
        assert_eq!(encrypted_v2.key_version, 2);
        let (plaintext_v2, requests_v2) = decrypt_scripted(&state_v2, &encrypted_v2.ciphertext).await;
        assert_eq!(plaintext_v2, b"written-after-the-rotation".to_vec());
        assert_eq!(
            requests_v2.len(),
            1,
            "an envelope on the current version must not read a version record: {requests_v2:?}"
        );
    }

    /// Old envelopes must survive more than one generation: the baseline is
    /// frozen once and every intermediate version keeps its own record, so both
    /// a pre-rotation envelope and one written between the two rotations still
    /// decrypt after the second.
    #[tokio::test]
    async fn wired_kv2_envelopes_survive_consecutive_rotations() {
        let state_v1 = KeyState::new(healthy_key_data());
        let encrypted_v1 = encrypt_scripted(&state_v1, b"generation-1").await;

        let state_v2 = rotate_scripted(&state_v1).await;
        let encrypted_v2 = encrypt_scripted(&state_v2, b"generation-2").await;
        assert_eq!(encrypted_v2.key_version, 2);

        let state_v3 = rotate_scripted(&state_v2).await;
        assert_eq!(state_v3.key_data.version, 3);
        assert_eq!(
            state_v3.key_data.baseline_version,
            Some(1),
            "the baseline is frozen once and carried through later rotations"
        );
        let mut recorded: Vec<u32> = state_v3.version_records.iter().map(|record| record.version).collect();
        recorded.sort_unstable();
        assert_eq!(recorded, vec![1, 2, 3], "every version that ever wrapped a DEK must keep a record");

        for (ciphertext, expected, version) in [
            (&encrypted_v1.ciphertext, b"generation-1".as_slice(), 1u32),
            (&encrypted_v2.ciphertext, b"generation-2".as_slice(), 2),
        ] {
            let (plaintext, requests) = decrypt_scripted(&state_v3, ciphertext).await;
            assert_eq!(
                plaintext, expected,
                "an envelope from version {version} must survive two rotations intact"
            );
            assert!(
                requests[1].ends_with(&format!("/versions/{version}")),
                "the decrypt must resolve the version that wrapped it: {requests:?}"
            );
        }
    }

    /// Rewrap against a scripted Vault serving `state`, scripting the key
    /// record, the fresh client's first-wrap budget reservation, and every
    /// version record the state holds, so the implementation — not the harness
    /// — decides which of them it needs (a no-op rewrap consumes neither the
    /// reservation nor a version record). Returns the response together with
    /// the requests the rewrap made.
    async fn rewrap_scripted(state: &KeyState, ciphertext: &[u8]) -> (RewrapDataKeyResponse, Vec<String>) {
        let mut responses = vec![
            ScriptedResponse::ok(kv2_read_data(&state.key_data)),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&state.key_data)),
            ScriptedResponse::ok(kv2_write_ack()),
        ];
        responses.extend(
            state
                .version_records
                .iter()
                .map(|record| ScriptedResponse::ok(kv2_read_version_record_data(record))),
        );
        let (vault, client) = scripted_client(responses).await;

        let response = client
            .rewrap_data_key(&RewrapDataKeyRequest {
                ciphertext: ciphertext.to_vec(),
                encryption_context: HashMap::new(),
            })
            .await
            .expect("rewrap must produce an envelope on the current version");
        (response, vault.requests())
    }

    /// Describe the wrapping of `ciphertext` against a scripted Vault serving
    /// `state`.
    async fn describe_wrapping_scripted(state: &KeyState, ciphertext: &[u8]) -> DescribeDataKeyWrappingResponse {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&state.key_data))]).await;
        client
            .describe_data_key_wrapping(&DescribeDataKeyWrappingRequest {
                ciphertext: ciphertext.to_vec(),
                encryption_context: HashMap::new(),
            })
            .await
            .expect("describing an envelope's wrapping must succeed")
    }

    /// The whole point of the primitive: an envelope wrapped by a superseded
    /// master key version comes back wrapped by the current one, carrying the
    /// same data key.
    ///
    /// Two independent things pin that the *old* material did the unwrapping.
    /// The frozen version-1 record is read — an implementation that reached for
    /// the current material would never ask for it — and the wrapping is
    /// AES-256-GCM, so unwrapping with the wrong material cannot yield the
    /// original data key at all, only an authentication failure. The closing
    /// decrypt then shows the result is genuinely bound to version 2: it
    /// resolves on the key record alone, with no version record in sight.
    #[tokio::test]
    async fn wired_kv2_rewrap_moves_an_old_envelope_onto_the_current_version() {
        const PLAINTEXT: &[u8] = b"data-key-material-that-must-survive";

        let state_v1 = KeyState::new(healthy_key_data());
        let encrypted_v1 = encrypt_scripted(&state_v1, PLAINTEXT).await;
        let state_v2 = rotate_scripted(&state_v1).await;
        assert_ne!(
            state_v2.key_data.encrypted_key_material, state_v1.key_data.encrypted_key_material,
            "the rotation must have replaced the current material, or this test proves nothing"
        );

        let before = describe_wrapping_scripted(&state_v2, &encrypted_v1.ciphertext).await;
        assert_eq!(before.key_version, Some(1));
        assert_eq!(before.current_key_version, Some(2));
        assert!(!before.is_current, "a version-1 envelope on a version-2 key is not current");

        let (response, requests) = rewrap_scripted(&state_v2, &encrypted_v1.ciphertext).await;
        assert!(response.rewrapped);
        assert_eq!(response.source_key_version, Some(1));
        assert_eq!(response.destination_key_version, Some(2));
        assert_eq!(
            requests,
            vec![
                "GET /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                // The fresh client's first wrap reserves its budget block...
                "GET /v1/secret/metadata/rustfs/kms/keys/wired-key".to_string(),
                "GET /v1/secret/data/rustfs/kms/keys/wired-key?version=1".to_string(),
                "POST /v1/secret/data/rustfs/kms/keys/wired-key".to_string(),
                // ...and the unwrap must resolve the frozen version-1 material.
                "GET /v1/secret/data/rustfs/kms/keys/wired-key/versions/1".to_string(),
            ],
            "the unwrap must resolve the frozen version-1 material: {requests:?}"
        );

        let original: DataKeyEnvelope = serde_json::from_slice(&encrypted_v1.ciphertext).expect("envelope must parse");
        let rewrapped: DataKeyEnvelope = serde_json::from_slice(&response.ciphertext).expect("rewrapped envelope must parse");
        assert_eq!(rewrapped.master_key_version, Some(2), "the result must name the current version");
        assert_ne!(rewrapped.encrypted_key, original.encrypted_key, "the wrapping must actually change");
        // Everything but the wrapping is carried over, so the data key keeps its
        // identity and its recorded age.
        assert_eq!(rewrapped.key_id, original.key_id);
        assert_eq!(rewrapped.key_spec, original.key_spec);
        assert_eq!(rewrapped.encryption_context, original.encryption_context);
        assert_eq!(rewrapped.created_at, original.created_at);

        let (plaintext, decrypt_requests) = decrypt_scripted(&state_v2, &response.ciphertext).await;
        assert_eq!(
            plaintext, PLAINTEXT,
            "the rewrapped envelope must yield the original data key byte for byte"
        );
        assert_eq!(
            decrypt_requests.len(),
            1,
            "the rewrapped envelope must resolve on the current record alone: {decrypt_requests:?}"
        );

        let after = describe_wrapping_scripted(&state_v2, &response.ciphertext).await;
        assert!(after.is_current, "the scan must agree the envelope no longer needs rewrapping");
    }

    /// Re-running a sweep must converge. An envelope already on the current
    /// version comes back byte for byte with nothing to persist, rather than as
    /// an equivalent envelope with a fresh nonce that would make every pass
    /// rewrite every object's metadata forever.
    #[tokio::test]
    async fn wired_kv2_rewrap_of_a_current_envelope_is_a_no_op() {
        let state_v1 = KeyState::new(healthy_key_data());
        let state_v2 = rotate_scripted(&state_v1).await;
        let encrypted_v2 = encrypt_scripted(&state_v2, b"written-after-the-rotation").await;

        let described = describe_wrapping_scripted(&state_v2, &encrypted_v2.ciphertext).await;
        assert!(described.is_current);

        let (response, requests) = rewrap_scripted(&state_v2, &encrypted_v2.ciphertext).await;
        assert!(!response.rewrapped, "an already-current envelope has nothing to rewrap");
        assert_eq!(
            response.ciphertext, encrypted_v2.ciphertext,
            "a no-op rewrap must hand the input back unchanged"
        );
        assert_eq!(response.source_key_version, Some(2));
        assert_eq!(response.destination_key_version, Some(2));
        assert_eq!(requests.len(), 1, "a no-op must not reach for any version record: {requests:?}");

        // Idempotence in the literal sense: feeding the result back in changes
        // nothing again.
        let (again, _) = rewrap_scripted(&state_v2, &response.ciphertext).await;
        assert!(!again.rewrapped);
        assert_eq!(again.ciphertext, encrypted_v2.ciphertext);
    }

    /// A pre-versioning envelope carries no version at all, so it can never
    /// satisfy a retirement scan however current its material happens to be.
    /// Rewrap therefore rewrites it to stamp the version — including on a
    /// never-rotated key, where the material it is unwrapped with and the
    /// material it is re-wrapped with are the same bytes.
    #[tokio::test]
    async fn wired_kv2_rewrap_stamps_a_pre_versioning_envelope() {
        const PLAINTEXT: &[u8] = b"written-before-versioning-existed";

        let state_v1 = KeyState::new(healthy_key_data());
        let encrypted_v1 = encrypt_scripted(&state_v1, PLAINTEXT).await;
        let legacy = strip_master_key_version(&encrypted_v1.ciphertext);
        let legacy_envelope: DataKeyEnvelope = serde_json::from_slice(&legacy).expect("legacy envelope must parse");
        assert_eq!(legacy_envelope.master_key_version, None);

        // Never rotated: the resolved version is already the current one, and
        // the envelope is still rewritten purely to record it.
        let described = describe_wrapping_scripted(&state_v1, &legacy).await;
        assert_eq!(described.key_version, Some(1));
        assert_eq!(described.current_key_version, Some(1));
        assert!(
            !described.is_current,
            "an envelope that does not state its version can never count as migrated"
        );

        let (response, requests) = rewrap_scripted(&state_v1, &legacy).await;
        assert!(response.rewrapped);
        assert_eq!(response.source_key_version, Some(1));
        assert_eq!(response.destination_key_version, Some(1));
        assert!(
            !requests.iter().any(|line| line.contains("/versions/")),
            "a never-rotated key has no version record to read: {requests:?}"
        );
        let stamped: DataKeyEnvelope = serde_json::from_slice(&response.ciphertext).expect("stamped envelope must parse");
        assert_eq!(stamped.master_key_version, Some(1));
        let (plaintext, _) = decrypt_scripted(&state_v1, &response.ciphertext).await;
        assert_eq!(plaintext, PLAINTEXT);

        // After a rotation the same legacy envelope resolves through the frozen
        // baseline instead, and lands on the rotated version.
        let state_v2 = rotate_scripted(&state_v1).await;
        assert_eq!(state_v2.key_data.baseline_version, Some(1));
        let (rotated_response, rotated_requests) = rewrap_scripted(&state_v2, &legacy).await;
        assert_eq!(rotated_response.source_key_version, Some(1), "the baseline is what wrapped it");
        assert_eq!(rotated_response.destination_key_version, Some(2));
        assert!(
            rotated_requests.iter().any(|line| line.ends_with("/versions/1")),
            "the unwrap must resolve the baseline material: {rotated_requests:?}"
        );
        let (rotated_plaintext, _) = decrypt_scripted(&state_v2, &rotated_response.ciphertext).await;
        assert_eq!(rotated_plaintext, PLAINTEXT);
    }

    /// Drop the `master_key_version` field to produce the envelope shape a
    /// pre-versioning build wrote.
    fn strip_master_key_version(ciphertext: &[u8]) -> Vec<u8> {
        let mut value: serde_json::Value = serde_json::from_slice(ciphertext).expect("envelope must parse");
        value
            .as_object_mut()
            .expect("envelope is a JSON object")
            .remove("master_key_version");
        serde_json::to_vec(&value).expect("serialize legacy envelope")
    }

    /// The encryption context binds an envelope to one object. A caller that
    /// cannot reproduce it is refused before any Vault read, so rewrap cannot be
    /// used to launder an envelope onto a fresh wrapping.
    #[tokio::test]
    async fn wired_kv2_rewrap_rejects_a_tampered_encryption_context() {
        let context = HashMap::from([("bucket".to_string(), "photos/cat.jpg".to_string())]);
        // The scripted exchange covers the encrypt (key read plus the first
        // wrap's budget reservation) and nothing else: the refused calls below
        // must not add a single request.
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        let encrypted = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"bound-to-one-object".to_vec(),
                    encryption_context: context.clone(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("encrypt must produce an envelope");

        let error = client
            .rewrap_data_key(&RewrapDataKeyRequest {
                ciphertext: encrypted.ciphertext.clone(),
                encryption_context: HashMap::from([("bucket".to_string(), "photos/other.jpg".to_string())]),
            })
            .await
            .expect_err("a context that does not match the envelope must be refused");
        assert!(matches!(error, KmsError::ContextMismatch { .. }), "got {error:?}");

        let error = client
            .describe_data_key_wrapping(&DescribeDataKeyWrappingRequest {
                ciphertext: encrypted.ciphertext,
                encryption_context: HashMap::from([("bucket".to_string(), "photos/other.jpg".to_string())]),
            })
            .await
            .expect_err("the read-only accessor must apply the same guard");
        assert!(matches!(error, KmsError::ContextMismatch { .. }), "got {error:?}");

        assert_eq!(
            vault.requests().len(),
            4,
            "the context guard must run before any Vault read — every request must belong to the encrypt: {:?}",
            vault.requests()
        );
    }

    /// The wrap counter is block-reserved, never written per wrap: N wraps
    /// with N < [`WRAP_BUDGET_BLOCK`] perform exactly one check-and-set
    /// reservation write, and the persisted value is the full block — an
    /// overestimate of the wraps actually performed, which is the direction a
    /// crash must leave it in (the unused in-memory remainder dies with the
    /// process, counted wraps never do). A revert to per-wrap persistence
    /// fails the write count; a revert to not persisting at all fails the
    /// stored value.
    #[tokio::test]
    async fn wired_generate_data_key_reserves_wrap_budget_in_blocks() {
        let (vault, client) = scripted_kv2_client(&healthy_key_data()).await;
        let request = integration_generate_request("wired-key");

        const WRAPS: u64 = 3;
        for _ in 0..WRAPS {
            client
                .generate_data_key(&request, None)
                .await
                .expect("wraps within the reserved block must succeed");
        }

        let requests = vault.requests();
        assert_eq!(
            requests.iter().filter(|line| line.starts_with("POST ")).count(),
            1,
            "{WRAPS} wraps inside one block must reserve exactly once: {requests:?}"
        );

        // "Crash": drop the client and its in-memory remainder, then read what
        // Vault durably holds.
        drop(client);
        let snapshot = vault.kv2_snapshot().expect("stateful KV2 snapshot");
        let reserved = snapshot.current_data["wrap_budget_reserved"]
            .as_u64()
            .expect("the key record must carry the reserved wrap budget");
        assert_eq!(reserved, WRAP_BUDGET_BLOCK, "the reservation persists the whole block up front");
        assert!(reserved >= WRAPS, "the persisted count must never understate the wraps performed");
    }

    /// Two nodes reserving against the same record must accumulate, not
    /// clobber: the loser of the check-and-set race re-reads the record the
    /// winner committed and adds its block on top, ending at two blocks. A
    /// blind write here would silently erase the peer's reservation and
    /// undercount its million wraps.
    #[tokio::test]
    async fn wired_wrap_budget_reservation_adds_on_top_after_losing_a_cas_race() {
        let mut peer_reserved = healthy_key_data();
        peer_reserved.wrap_budget_reserved = WRAP_BUDGET_BLOCK;

        let (vault, client) = scripted_client(vec![
            // The encrypt reads the key record...
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // ...and its first wrap reserves: attempt 1 observes no
            // reservation yet...
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // ...but the peer's reservation committed in between, so the
            // check-and-set write loses.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // Attempt 2 re-reads the record the peer committed and lands.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(kv2_read_data(&peer_reserved)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"counted-once".to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("losing the reservation race must not fail the wrap");

        let bodies = vault.request_bodies();
        let lost = parse_write_body(&bodies[3]);
        assert_eq!(lost["options"]["cas"], serde_json::json!(1), "{lost}");
        assert_eq!(lost["data"]["wrap_budget_reserved"], serde_json::json!(WRAP_BUDGET_BLOCK), "{lost}");
        let committed = parse_write_body(&bodies[6]);
        assert_eq!(committed["options"]["cas"], serde_json::json!(2), "{committed}");
        assert_eq!(
            committed["data"]["wrap_budget_reserved"],
            serde_json::json!(2 * WRAP_BUDGET_BLOCK),
            "the retry must add its block on top of the peer's, not overwrite it: {committed}"
        );
    }

    /// The counter is advisory observability, not a quota: a reservation that
    /// cannot be persisted is warned about and the wrap proceeds. Turning the
    /// scripted 5xx into a failed `generate_data_key` — a Vault hiccup failing
    /// a PUT — is exactly the regression this test pins down.
    #[tokio::test]
    async fn wired_wrap_proceeds_and_warns_when_budget_reservation_fails() {
        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (vault, client) = scripted_client(vec![
            // generate_data_key: the state-gate read and the wrap snapshot.
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // The reservation's versioned read succeeds...
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(kv2_read_data(&healthy_key_data())),
            // ...and its write fails.
            ScriptedResponse::error(503, "sealed"),
        ])
        .await;

        let data_key = client
            .generate_data_key(&integration_generate_request("wired-key"), None)
            .await
            .expect("a failed budget reservation must never fail the wrap");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        let requests = vault.requests();
        assert_eq!(requests.len(), 5, "{requests:?}");
        assert_eq!(
            requests.iter().filter(|line| line.starts_with("POST ")).count(),
            1,
            "the failed non-idempotent reservation write must not be replayed: {requests:?}"
        );

        let output = logs.output();
        assert!(output.contains("WARN"), "the failure must be visible to operators: {output}");
        assert!(
            output.contains("Vault KMS wrap budget reservation failed"),
            "the warn must name the degraded counter: {output}"
        );
        assert!(
            !output.contains(&healthy_key_data().encrypted_key_material),
            "the warn must not echo stored key material: {output}"
        );
    }

    /// The AES-GCM wrap bound is per key material version, so the rotation's
    /// pointer-switch commit — and only that commit — resets the persisted
    /// counter. The baseline-pin write before it must still carry the
    /// pre-rotation count (the old material is still current there), and the
    /// immutable version records never carry the field at all.
    #[tokio::test]
    async fn wired_rotate_resets_wrap_budget_with_the_pointer_switch() {
        let mut key_data = healthy_key_data();
        key_data.wrap_budget_reserved = 123_456;
        let (vault, client) = scripted_kv2_client(&key_data).await;

        client.rotate_key("wired-key", None).await.expect("rotation must commit");

        let snapshot = vault.kv2_snapshot().expect("stateful KV2 snapshot");
        assert_eq!(snapshot.current_data["version"], serde_json::json!(2));
        assert_eq!(
            snapshot.current_data["wrap_budget_reserved"],
            serde_json::json!(0),
            "fresh material must start with a fresh nonce budget"
        );
        assert!(
            snapshot
                .version_records
                .get(&1)
                .expect("the first rotation must freeze the version-1 record")
                .get("wrap_budget_reserved")
                .is_none(),
            "version records carry material, never the wrap counter"
        );

        // First rotation writes: freeze v1, pin the baseline, create v2,
        // switch the pointer. Only the last one resets the counter.
        let bodies = vault.request_bodies();
        let baseline_pin = parse_write_body(&bodies[4]);
        assert_eq!(
            baseline_pin["data"]["wrap_budget_reserved"],
            serde_json::json!(123_456),
            "the pre-switch write still describes the old material: {baseline_pin}"
        );
        let switch = parse_write_body(&bodies[6]);
        assert_eq!(switch["data"]["version"], serde_json::json!(2), "{switch}");
        assert_eq!(switch["data"]["wrap_budget_reserved"], serde_json::json!(0), "{switch}");
    }

    /// The in-memory block is tied to the material version it was reserved
    /// against: a wrap after a rotation must not spend the stale grant (whose
    /// persisted counter the rotation just reset) but reserve a fresh block on
    /// the new version's record.
    #[tokio::test]
    async fn wired_wrap_after_rotation_reserves_a_fresh_block() {
        let (vault, client) = scripted_kv2_client(&healthy_key_data()).await;
        let request = integration_generate_request("wired-key");

        client.generate_data_key(&request, None).await.expect("wrap under v1");
        client.rotate_key("wired-key", None).await.expect("rotate to v2");
        client.generate_data_key(&request, None).await.expect("wrap under v2");

        let snapshot = vault.kv2_snapshot().expect("stateful KV2 snapshot");
        assert_eq!(snapshot.current_data["version"], serde_json::json!(2));
        assert_eq!(
            snapshot.current_data["wrap_budget_reserved"],
            serde_json::json!(WRAP_BUDGET_BLOCK),
            "a wrap under fresh material must reserve anew instead of spending the stale grant"
        );
    }

    /// `describe_key` reports the persisted counter — the deletion worker's
    /// census reads it from exactly this surface to publish the aggregate
    /// wrap gauge.
    #[tokio::test]
    async fn wired_describe_key_reports_wrap_budget_for_the_census() {
        let mut key_data = healthy_key_data();
        key_data.wrap_budget_reserved = 42;
        let (_vault, client) = scripted_client(vec![ScriptedResponse::ok(kv2_read_data(&key_data))]).await;

        let described = client.describe_key("wired-key", None).await.expect("describe must succeed");
        assert_eq!(described.wrap_budget_reserved, Some(42));
    }

    /// The persisted KV2 record round-trips its wrap counter, and a record
    /// without the field — written by an older build, or rewritten by one,
    /// which is the documented mixed-version regression — reads back as zero.
    #[test]
    fn vault_key_data_wrap_budget_round_trips_and_defaults_to_zero() {
        let mut key_data = healthy_key_data();
        key_data.wrap_budget_reserved = 42;

        let mut value = serde_json::to_value(&key_data).expect("serialize");
        let restored: VaultKeyData = serde_json::from_value(value.clone()).expect("round trip");
        assert_eq!(restored.wrap_budget_reserved, 42);

        value
            .as_object_mut()
            .expect("record must be a JSON object")
            .remove("wrap_budget_reserved")
            .expect("current records must carry the field");
        let legacy: VaultKeyData = serde_json::from_value(value).expect("legacy record must deserialize");
        assert_eq!(legacy.wrap_budget_reserved, 0);
    }
}
