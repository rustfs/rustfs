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

//! Vault Transit-based KMS backend.

use crate::backends::vault::map_key_record_read_error;
use crate::backends::vault_credentials::{
    CredentialTaskHandle, VaultClientHandle, VaultConnectionSettings, VaultCredentialPolicy, VaultCredentialProvider,
    token_source_for,
};
use crate::backends::{
    BackendCapabilities, ExpiredKeyRemoval, KmsBackend, ListedKeyFailure, StateGatedOperation, UnreadableKeys,
    classify_listed_key_failure, empty_key_page, ensure_key_state_permits, ensure_rewrap_context_matches,
    ensure_tag_keys_are_mutable, list_keys_page_size, paginate_keys, started_at_the_first_key,
};
use crate::config::{KmsConfig, VaultTransitConfig};
use crate::encryption::{DataKeyEnvelope, generate_key_material};
use crate::error::{KmsError, Result};
use crate::persisted_observability::{BoundedUnknownFieldName, UnknownFieldSummary};
use crate::policy::{self, AttemptError, OpClass, RetryPolicy};
use crate::types::*;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jiff::Zoned;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use vaultrs::{
    api::kv2::requests::SetSecretRequestOptions,
    api::transit::{
        KeyType,
        requests::{
            CreateKeyRequestBuilder, DecryptDataRequestBuilder, EncryptDataRequestBuilder, UpdateKeyConfigurationRequestBuilder,
        },
        responses::ReadKeyData,
    },
    error::ClientError,
    kv2,
    transit::{data, key},
};

/// Attempt budget for metadata read-modify-write cycles: every check-and-set
/// conflict triggers a fresh read plus state-gate re-validation, never a blind
/// replay of the stale snapshot.
const METADATA_CAS_ATTEMPTS: usize = 3;

/// TTL bound on cached metadata records. This caps how long one node can keep
/// acting on lifecycle state another node has since changed (disable,
/// schedule-deletion): the divergence window is one TTL instead of "until
/// process restart".
///
/// Deliberately fixed rather than derived from `CacheConfig`: this cache gates
/// cryptographic operations through `ensure_key_state_allows`, so its staleness
/// window must not follow a knob an operator turns to tune the manager-level
/// describe cache. It happens to equal `config::DEFAULT_CACHE_TTL` today, but
/// that is a coincidence rather than a contract, and binding the two would let
/// a later change to the operator-facing default silently widen this window.
const METADATA_CACHE_TTL: Duration = Duration::from_secs(300);

/// Capacity bound on the metadata cache so an unbounded key namespace cannot
/// grow process memory without limit.
const METADATA_CACHE_CAPACITY: u64 = 1024;

/// Read the key version out of a Transit ciphertext's `vault:vN:` prefix.
///
/// Transit ciphertext self-describes the version that wrapped it, which is why
/// [`DataKeyEnvelope::master_key_version`] stays `None` on this backend. The
/// prefix is therefore the only place a rewrap can learn whether it changed
/// anything. `None` means the ciphertext is not in a shape this backend
/// produced, and callers must treat the version as unknown rather than assume
/// one.
fn transit_ciphertext_version(ciphertext: &str) -> Option<u32> {
    let (version, _) = ciphertext.strip_prefix("vault:v")?.split_once(':')?;
    version.parse().ok()
}

/// Whether a KV2 write failed its check-and-set precondition.
///
/// Mirrors the helper of the same name in `vault.rs`; the two backends keep
/// separate copies because they share no private module.
fn is_cas_conflict(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::APIError { code: 400, errors } if errors.iter().any(|message| message.contains("check-and-set"))
    )
}

/// Whether a transit LIST failed with the 404 Vault uses for "mounted, but no
/// keys yet".
///
/// Vault answers a LIST on a mounted transit engine that holds no keys with a
/// 404 whose `errors` array is empty — the mount routed and answered the
/// request, so the engine is reachable. A 404 for a path with no mount behind
/// it instead carries a "no handler for route" message, so the empty `errors`
/// array is what separates "engine reachable but empty" from "engine missing".
///
/// An empty non-transit engine (e.g. KV v1) at the configured path answers
/// with byte-identical 404s, so this probe cannot detect that misconfiguration
/// — no LIST-based probe can. The data path still fails hard on the first real
/// transit operation against such a mount.
fn is_empty_transit_list(error: &ClientError) -> bool {
    matches!(error, ClientError::APIError { code: 404, errors } if errors.is_empty())
}

#[derive(Debug, Clone)]
struct TransitKeyMetadata {
    key_usage: KeyUsage,
    description: Option<String>,
    tags: HashMap<String, String>,
    key_state: KeyState,
    created_at: Zoned,
    deletion_date: Option<Zoned>,
    origin: String,
    created_by: Option<String>,
    current_version: u32,
}

/// Serializable version of TransitKeyMetadata for KV v2 persistence.
///
/// `Deserialize` is hand-written so fields the current build does not know
/// are counted and warned about instead of vanishing silently — this record
/// is compatibility-bound in both directions (older and newer builds read
/// each other's writes), so `deny_unknown_fields` is not an option.
#[derive(Debug, Clone, Serialize)]
struct TransitKeyMetadataPersisted {
    key_usage: KeyUsage,
    description: Option<String>,
    tags: HashMap<String, String>,
    key_state: KeyState,
    created_at: Zoned,
    deletion_date: Option<Zoned>,
    origin: String,
    created_by: Option<String>,
    current_version: u32,
}

impl UnknownFieldSummary {
    fn record_for_transit_key_metadata(&self) {
        let Some((field, field_name_truncated, field_count)) = self.record("vault-transit-key-metadata") else {
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
                "Vault Transit key metadata record contains unknown fields"
            );
        }
    }
}

impl<'de> Deserialize<'de> for TransitKeyMetadataPersisted {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, IgnoredAny, MapAccess, Visitor};
        use std::fmt;

        enum Field {
            KeyUsage,
            Description,
            Tags,
            KeyState,
            CreatedAt,
            DeletionDate,
            Origin,
            CreatedBy,
            CurrentVersion,
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
                        formatter.write_str("a Vault Transit key metadata field name")
                    }

                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(match value {
                            "key_usage" => Field::KeyUsage,
                            "description" => Field::Description,
                            "tags" => Field::Tags,
                            "key_state" => Field::KeyState,
                            "created_at" => Field::CreatedAt,
                            "deletion_date" => Field::DeletionDate,
                            "origin" => Field::Origin,
                            "created_by" => Field::CreatedBy,
                            "current_version" => Field::CurrentVersion,
                            _ => Field::Unknown(BoundedUnknownFieldName::new(value)),
                        })
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct TransitKeyMetadataPersistedVisitor;

        impl<'de> Visitor<'de> for TransitKeyMetadataPersistedVisitor {
            type Value = TransitKeyMetadataPersisted;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Vault Transit key metadata record")
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

                let mut key_usage = None;
                let mut description = None;
                let mut tags = None;
                let mut key_state = None;
                let mut created_at = None;
                let mut deletion_date = None;
                let mut origin = None;
                let mut created_by = None;
                let mut current_version = None;
                let mut unknown_fields = UnknownFieldSummary::default();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::KeyUsage => read_field!(key_usage, "key_usage"),
                        Field::Description => read_field!(description, "description"),
                        Field::Tags => read_field!(tags, "tags"),
                        Field::KeyState => read_field!(key_state, "key_state"),
                        Field::CreatedAt => read_field!(created_at, "created_at"),
                        Field::DeletionDate => read_field!(deletion_date, "deletion_date"),
                        Field::Origin => read_field!(origin, "origin"),
                        Field::CreatedBy => read_field!(created_by, "created_by"),
                        Field::CurrentVersion => read_field!(current_version, "current_version"),
                        Field::Unknown(field) => {
                            let _: IgnoredAny = map.next_value()?;
                            unknown_fields.observe(field);
                        }
                    }
                }

                let metadata = TransitKeyMetadataPersisted {
                    key_usage: key_usage.ok_or_else(|| de::Error::missing_field("key_usage"))?,
                    description: description.unwrap_or(None),
                    tags: tags.ok_or_else(|| de::Error::missing_field("tags"))?,
                    key_state: key_state.ok_or_else(|| de::Error::missing_field("key_state"))?,
                    created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?,
                    deletion_date: deletion_date.unwrap_or(None),
                    origin: origin.ok_or_else(|| de::Error::missing_field("origin"))?,
                    created_by: created_by.unwrap_or(None),
                    current_version: current_version.ok_or_else(|| de::Error::missing_field("current_version"))?,
                };
                unknown_fields.record_for_transit_key_metadata();
                Ok(metadata)
            }
        }

        const FIELDS: &[&str] = &[
            "key_usage",
            "description",
            "tags",
            "key_state",
            "created_at",
            "deletion_date",
            "origin",
            "created_by",
            "current_version",
        ];
        deserializer.deserialize_struct("TransitKeyMetadataPersisted", FIELDS, TransitKeyMetadataPersistedVisitor)
    }
}

impl TransitKeyMetadata {
    fn from_create_request(request: &CreateKeyRequest) -> Self {
        Self {
            key_usage: request.key_usage.clone(),
            description: request.description.clone(),
            tags: request.tags.clone(),
            key_state: KeyState::Enabled,
            created_at: Zoned::now(),
            deletion_date: None,
            origin: request.origin.clone().unwrap_or_else(|| "VAULT_TRANSIT".to_string()),
            created_by: None,
            current_version: 1,
        }
    }

    // Fallback record for transit keys created before metadata persistence
    // existed (rustfs#4256 / rustfs#4262): those keys have no KV record at
    // all, and failing closed on the missing record would brick every one of
    // them, so the record defaults to Enabled to match their pre-persistence
    // behavior. The historical fail-open around it (rustfs/backlog#808,
    // rustfs/backlog#1571: any metadata read failure yielded a usable Enabled
    // key) is resolved for rustfs/backlog#1581: `get_key_metadata` only serves
    // this record after durably persisting it with a create-only
    // check-and-set, and any read or persist failure on that path fails
    // closed.
    fn synthesized() -> Self {
        Self {
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            tags: HashMap::new(),
            key_state: KeyState::Enabled,
            created_at: Zoned::now(),
            deletion_date: None,
            origin: "VAULT_TRANSIT".to_string(),
            created_by: None,
            current_version: 1,
        }
    }
}

impl From<TransitKeyMetadata> for TransitKeyMetadataPersisted {
    fn from(m: TransitKeyMetadata) -> Self {
        Self {
            key_usage: m.key_usage,
            description: m.description,
            tags: m.tags,
            key_state: m.key_state,
            created_at: m.created_at,
            deletion_date: m.deletion_date,
            origin: m.origin,
            created_by: m.created_by,
            current_version: m.current_version,
        }
    }
}

impl From<TransitKeyMetadataPersisted> for TransitKeyMetadata {
    fn from(m: TransitKeyMetadataPersisted) -> Self {
        Self {
            key_usage: m.key_usage,
            description: m.description,
            tags: m.tags,
            key_state: m.key_state,
            created_at: m.created_at,
            deletion_date: m.deletion_date,
            origin: m.origin,
            created_by: m.created_by,
            current_version: m.current_version,
        }
    }
}

pub struct VaultTransitKmsClient {
    credentials: Arc<VaultCredentialProvider>,
    config: VaultTransitConfig,
    /// KV v2 mount path for persisting transit key metadata
    metadata_kv_mount: String,
    /// Path prefix under metadata_kv_mount for storing transit key metadata records
    metadata_key_prefix: String,
    /// Process-local metadata cache, TTL- and capacity-bounded (see
    /// [`METADATA_CACHE_TTL`]): a lifecycle change made by another node
    /// becomes visible here within one TTL window at the latest.
    metadata_cache: Cache<String, TransitKeyMetadata>,
    /// Budgets wrapping every outbound Vault call (see `crate::policy`).
    retry: RetryPolicy,
    /// Cancellation point for the operation executor: aborts in-flight
    /// attempts and backoff sleeps. Owned by the client and currently never
    /// triggered — shutdown drops the whole client — but kept as the single
    /// hook a future lifecycle owner can cancel through.
    cancel: CancellationToken,
}

impl VaultTransitKmsClient {
    /// Create a new Vault Transit KMS client
    ///
    /// `kms_config` supplies the per-attempt timeout that caps every HTTP
    /// request issued through this client, plus the retry and fail-closed
    /// budgets for credential refresh.
    pub async fn new(config: VaultTransitConfig, kms_config: &KmsConfig) -> Result<Self> {
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
            "vault-transit",
            &config.address,
            config.namespace.as_deref(),
        );
        let credentials = Arc::new(VaultCredentialProvider::new(settings, source, policy).await?);
        let retry =
            RetryPolicy::for_backend(kms_config, "vault-transit", &config.address, config.namespace.as_deref(), "operations");

        Ok(Self {
            credentials,
            metadata_kv_mount: config.metadata_kv_mount.clone(),
            metadata_key_prefix: config.metadata_key_prefix.clone(),
            config,
            metadata_cache: Cache::builder()
                .max_capacity(METADATA_CACHE_CAPACITY)
                .time_to_live(METADATA_CACHE_TTL)
                .build(),
            retry,
            cancel: CancellationToken::new(),
        })
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

    fn canonicalize_context(encryption_context: &HashMap<String, String>) -> Result<Option<String>> {
        if encryption_context.is_empty() {
            return Ok(None);
        }

        let ordered: BTreeMap<_, _> = encryption_context
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let serialized = serde_json::to_vec(&ordered)?;
        Ok(Some(BASE64.encode(serialized)))
    }

    fn map_vault_error(key_id: &str, error: vaultrs::error::ClientError, operation: &str) -> KmsError {
        match error {
            vaultrs::error::ClientError::ResponseWrapError => KmsError::key_not_found(key_id),
            vaultrs::error::ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
            other => KmsError::backend_error(format!("Vault Transit {operation} failed for key {key_id}: {other}")),
        }
    }

    async fn read_transit_key(&self, key_id: &str) -> Result<vaultrs::api::transit::responses::ReadKeyResponse> {
        self.run("vault_transit_read_key", OpClass::ReadIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            key::read(&vault.client, &self.config.mount_path, key_id)
                .await
                .map_err(|e| AttemptError::from_vaultrs(e, |e| Self::map_vault_error(key_id, e, "read")))
        })
        .await
    }

    async fn create_transit_key(&self, key_id: &str) -> Result<()> {
        // Single attempt: create carries external side effects and the caller
        // owns the read-confirm recovery for lost responses.
        self.run("vault_transit_create_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            let mut builder = CreateKeyRequestBuilder::default();
            builder.key_type(KeyType::Aes256Gcm96);
            key::create(&vault.client, &self.config.mount_path, key_id, Some(&mut builder))
                .await
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to create Vault Transit key {key_id}: {e}"))
                    })
                })
        })
        .await
    }

    async fn transit_encrypt(
        &self,
        key_id: &str,
        plaintext: &[u8],
        encryption_context: &HashMap<String, String>,
    ) -> Result<String> {
        let plaintext_b64 = BASE64.encode(plaintext);
        let plaintext_b64 = plaintext_b64.as_str();
        let aad = Self::canonicalize_context(encryption_context)?;
        let aad = aad.as_deref();

        let response = self
            .run("vault_transit_encrypt", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                let mut builder = EncryptDataRequestBuilder::default();
                if let Some(aad) = aad {
                    builder.associated_data(aad);
                }
                data::encrypt(&vault.client, &self.config.mount_path, key_id, plaintext_b64, Some(&mut builder))
                    .await
                    .map_err(|e| AttemptError::from_vaultrs(e, |e| Self::map_vault_error(key_id, e, "encrypt")))
            })
            .await?;

        Ok(response.ciphertext)
    }

    async fn transit_decrypt(
        &self,
        key_id: &str,
        ciphertext: &str,
        encryption_context: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let aad = Self::canonicalize_context(encryption_context)?;
        let aad = aad.as_deref();

        let response = self
            .run("vault_transit_decrypt", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                let mut builder = DecryptDataRequestBuilder::default();
                if let Some(aad) = aad {
                    builder.associated_data(aad);
                }
                data::decrypt(&vault.client, &self.config.mount_path, key_id, ciphertext, Some(&mut builder))
                    .await
                    .map_err(|e| AttemptError::from_vaultrs(e, |e| Self::map_vault_error(key_id, e, "decrypt")))
            })
            .await?;

        BASE64
            .decode(response.plaintext)
            .map_err(|e| KmsError::cryptographic_error("base64_decode", e.to_string()))
    }

    /// Re-encrypt a Transit ciphertext under the key's latest version without
    /// the plaintext ever leaving Vault.
    ///
    /// Classified as an idempotent read because it is one: Vault mutates
    /// nothing, and a replayed attempt only produces another ciphertext of the
    /// same data key under the same version.
    async fn transit_rewrap(&self, key_id: &str, ciphertext: &str) -> Result<String> {
        let response = self
            .run("vault_transit_rewrap", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                data::rewrap(&vault.client, &self.config.mount_path, key_id, ciphertext, None)
                    .await
                    .map_err(|e| AttemptError::from_vaultrs(e, |e| Self::map_vault_error(key_id, e, "rewrap")))
            })
            .await?;

        Ok(response.ciphertext)
    }

    /// Vault's own newest version of a transit key.
    ///
    /// `transit/keys/:name` reports retained versions as a version-number to
    /// creation-time map rather than as a single "latest" field, so the newest
    /// version is the largest entry in it.
    ///
    /// Read from Vault rather than taken from the RustFS metadata record's
    /// `current_version` counter: that counter only advances when a rotation
    /// goes through this process, while `transit/rewrap` always targets Vault's
    /// notion of latest. The scan that decides whether a rewrap is still needed
    /// and the rewrap that acts on it must answer to the same authority, or an
    /// operator-side `vault write -f transit/keys/x/rotate` would leave the two
    /// permanently disagreeing.
    async fn latest_transit_key_version(&self, key_id: &str) -> Result<Option<u32>> {
        let response = self.read_transit_key(key_id).await?;
        let latest = match &response.keys {
            ReadKeyData::Symmetric(versions) => versions.keys().filter_map(|version| version.parse::<u32>().ok()).max(),
            ReadKeyData::Asymmetric(versions) => versions.keys().filter_map(|version| version.parse::<u32>().ok()).max(),
        };
        Ok(latest)
    }

    fn metadata_key_path(&self, key_id: &str) -> String {
        format!("{}/{}", self.metadata_key_prefix, key_id)
    }

    async fn read_metadata_from_kv(&self, key_id: &str) -> Result<Option<TransitKeyMetadata>> {
        let path = self.metadata_key_path(key_id);
        let path = path.as_str();
        self.run("vault_transit_read_metadata", OpClass::ReadIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::read::<TransitKeyMetadataPersisted>(&vault.client, &self.metadata_kv_mount, path).await {
                Ok(persisted) => Ok(Some(persisted.into())),
                Err(vaultrs::error::ClientError::ResponseWrapError)
                | Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => Ok(None),
                // A metadata record that is present but undecodable is a
                // property of this one key, so it is reported as such rather
                // than as a backend outage: otherwise a single record written
                // by a newer build fails every listing on the node, and with it
                // every scheduled deletion.
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    map_key_record_read_error(key_id, "transit key metadata", e)
                })),
            }
        })
        .await
    }

    /// Read the persisted metadata record together with the KV2 secret version
    /// holding it, so a later write can be check-and-set against exactly this
    /// snapshot. `None` means no record exists (a pre-persistence key).
    async fn read_metadata_from_kv_versioned(&self, key_id: &str) -> Result<Option<(u32, TransitKeyMetadata)>> {
        let path = self.metadata_key_path(key_id);
        let path = path.as_str();

        let kv_metadata = self
            .run("vault_transit_read_metadata_version", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match kv2::read_metadata(&vault.client, &self.metadata_kv_mount, path).await {
                    Ok(metadata) => Ok(Some(metadata)),
                    Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(None),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to read transit key metadata version from Vault KV: {e}"))
                    })),
                }
            })
            .await?;
        let Some(kv_metadata) = kv_metadata else {
            return Ok(None);
        };
        let cas = u32::try_from(kv_metadata.current_version)
            .map_err(|_| KmsError::backend_error(format!("KV2 secret version for transit key {key_id} metadata exceeds u32")))?;

        // Read the exact secret version named by the metadata so the
        // (cas, record) pair stays consistent even if another writer lands in
        // between the two reads.
        let secret_version = kv_metadata.current_version;
        let record: Option<TransitKeyMetadataPersisted> = self
            .run("vault_transit_read_metadata_at_version", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match kv2::read_version(&vault.client, &self.metadata_kv_mount, path, secret_version).await {
                    Ok(persisted) => Ok(Some(persisted)),
                    Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(None),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to read transit key metadata from Vault KV: {e}"))
                    })),
                }
            })
            .await?;

        Ok(record.map(|persisted| (cas, persisted.into())))
    }

    /// Check-and-set write of the metadata record.
    ///
    /// `cas` must match the KV2 secret version currently holding the record
    /// (0 = create-only). Returns `Ok(false)` when the precondition failed — a
    /// concurrent writer landed first — so the caller re-reads instead of
    /// clobbering. Single attempt: replaying a lost-response write would
    /// double-apply the mutation, and a CAS conflict is a normal concurrency
    /// signal, not a backend failure.
    async fn cas_write_metadata_to_kv(&self, key_id: &str, metadata: &TransitKeyMetadata, cas: u32) -> Result<bool> {
        let path = self.metadata_key_path(key_id);
        let path = path.as_str();
        let persisted: TransitKeyMetadataPersisted = metadata.clone().into();
        let persisted = &persisted;
        self.run("vault_transit_cas_write_metadata", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::set_with_options(&vault.client, &self.metadata_kv_mount, path, persisted, SetSecretRequestOptions { cas })
                .await
            {
                Ok(_) => Ok(true),
                Err(e) if is_cas_conflict(&e) => Ok(false),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Failed to write transit key metadata to Vault KV: {e}"))
                })),
            }
        })
        .await
    }

    /// The error surfaced when a metadata read-modify-write exhausts its
    /// [`METADATA_CAS_ATTEMPTS`] budget without winning a check-and-set write.
    fn metadata_cas_conflict(key_id: &str) -> KmsError {
        KmsError::invalid_operation(format!(
            "Concurrent modification of transit key {key_id} metadata detected; retry the operation"
        ))
    }

    async fn delete_metadata_from_kv(&self, key_id: &str) -> Result<()> {
        let path = self.metadata_key_path(key_id);
        let path = path.as_str();
        self.run("vault_transit_delete_metadata", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match kv2::delete_metadata(&vault.client, &self.metadata_kv_mount, path).await {
                // Metadata that is already gone is a completed delete.
                Ok(_)
                | Err(vaultrs::error::ClientError::ResponseWrapError)
                | Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => Ok(()),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Failed to delete transit key metadata from Vault KV: {e}"))
                })),
            }
        })
        .await
    }

    /// Flip `deletion_allowed` on the transit key so it can be deleted.
    async fn allow_transit_key_deletion(&self, key_id: &str) -> Result<()> {
        self.run("vault_transit_allow_deletion", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            let mut builder = UpdateKeyConfigurationRequestBuilder::default();
            builder.deletion_allowed(true);
            key::update(&vault.client, &self.config.mount_path, key_id, Some(&mut builder))
                .await
                .map(|_| ())
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to allow deletion of Vault Transit key {key_id}: {e}"))
                    })
                })
        })
        .await
    }

    /// Physically delete the transit key material in Vault.
    async fn delete_transit_key(&self, key_id: &str) -> Result<()> {
        self.run("vault_transit_delete_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            key::delete(&vault.client, &self.config.mount_path, key_id)
                .await
                .map(|_| ())
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to delete Vault Transit key {key_id}: {e}"))
                    })
                })
        })
        .await
    }

    async fn get_key_metadata(&self, key_id: &str) -> Result<TransitKeyMetadata> {
        // Check in-memory cache first (TTL-bounded, so a stale entry can only
        // survive one TTL window).
        if let Some(metadata) = self.metadata_cache.get(key_id).await {
            return Ok(metadata);
        }

        for _ in 0..METADATA_CAS_ATTEMPTS {
            // On cache miss, try reading from the persistent KV store.
            if let Some(persisted) = self.read_metadata_from_kv(key_id).await? {
                self.metadata_cache.insert(key_id.to_string(), persisted.clone()).await;
                return Ok(persisted);
            }

            // Deliberate exemption from the "read paths never write" rule (rustfs#4256 /
            // rustfs#4262): transit keys created before metadata persistence existed have no
            // KV record at all, so failing closed here would brick every pre-existing transit
            // key. The synthesised record only describes metadata — key material lives solely
            // inside Vault's transit engine and is never generated or written by this path.
            //
            // Verify the transit key actually exists in Vault before synthesising.
            self.read_transit_key(key_id).await?;
            let metadata = TransitKeyMetadata::synthesized();
            // Fail closed on the persist (rustfs/backlog#1581): the synthesised record is
            // only served once it is durable, so every node gates on the same stored
            // state; a failed KV write must fail the read instead of minting a usable
            // Enabled record out of thin air. The create-only check-and-set keeps two
            // nodes from fabricating divergent records — losing that race loops back to
            // re-read the winner's record.
            if self.cas_write_metadata_to_kv(key_id, &metadata, 0).await? {
                self.metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
                return Ok(metadata);
            }
        }
        Err(Self::metadata_cas_conflict(key_id))
    }

    /// Create-only write of the metadata record (check-and-set of 0).
    ///
    /// Returns `Ok(false)` when a record already exists — a concurrent creator
    /// won the race — and never overwrites it; the caller reconciles by
    /// reading the stored record back.
    async fn create_key_metadata(&self, key_id: &str, metadata: &TransitKeyMetadata) -> Result<bool> {
        if self.cas_write_metadata_to_kv(key_id, metadata, 0).await? {
            self.metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Read-modify-write of the persisted metadata record under KV2
    /// check-and-set.
    ///
    /// Every attempt re-reads the authoritative record, re-runs `apply` —
    /// which owns state-gate validation — against that fresh snapshot, and
    /// writes back with the snapshot's KV2 secret version as the check-and-set
    /// precondition, so a concurrent writer is never clobbered blind. Losing
    /// the race drops the (now stale) cache entry and retries with a fresh
    /// read; exhausting the budget surfaces the conflict to the caller.
    async fn mutate_key_metadata<F>(&self, key_id: &str, mut apply: F) -> Result<TransitKeyMetadata>
    where
        F: FnMut(&mut TransitKeyMetadata) -> Result<()>,
    {
        for _ in 0..METADATA_CAS_ATTEMPTS {
            let (cas, mut metadata) = match self.read_metadata_from_kv_versioned(key_id).await? {
                Some(snapshot) => snapshot,
                None => {
                    // Pre-persistence key without a KV record (see
                    // get_key_metadata): mutate the synthesised record and
                    // create it with a create-only check-and-set so two nodes
                    // cannot fabricate divergent records.
                    self.read_transit_key(key_id).await?;
                    (0, TransitKeyMetadata::synthesized())
                }
            };
            apply(&mut metadata)?;
            if self.cas_write_metadata_to_kv(key_id, &metadata, cas).await? {
                self.metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
                return Ok(metadata);
            }
            self.metadata_cache.invalidate(key_id).await;
        }
        Err(Self::metadata_cas_conflict(key_id))
    }

    /// Drop the cached metadata record when a transit data-path call failed in
    /// a way that signals the cached lifecycle state diverged from Vault (the
    /// key is gone server-side), so the next state gate re-reads the
    /// authoritative record instead of trusting the stale entry until its TTL.
    async fn invalidate_metadata_on_state_error(&self, key_id: &str, error: &KmsError) {
        if matches!(error, KmsError::KeyNotFound { .. }) {
            self.metadata_cache.invalidate(key_id).await;
        }
    }

    async fn delete_key_metadata(&self, key_id: &str) -> Result<()> {
        self.delete_metadata_from_kv(key_id).await?;
        self.metadata_cache.invalidate(key_id).await;
        Ok(())
    }

    async fn key_info(&self, key_id: &str) -> Result<KeyInfo> {
        self.read_transit_key(key_id).await?;
        let metadata = self.get_key_metadata(key_id).await?;

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            description: metadata.description.clone(),
            algorithm: "AES_256".to_string(),
            usage: metadata.key_usage.clone(),
            status: match metadata.key_state {
                KeyState::Enabled => KeyStatus::Active,
                KeyState::Disabled => KeyStatus::Disabled,
                KeyState::PendingDeletion => KeyStatus::PendingDeletion,
                KeyState::PendingImport | KeyState::Unavailable => KeyStatus::Deleted,
            },
            version: metadata.current_version,
            metadata: metadata.tags.clone(),
            tags: metadata.tags,
            created_at: metadata.created_at,
            rotated_at: None,
            created_by: metadata.created_by,
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: None,
        })
    }

    async fn key_metadata_response(&self, key_id: &str) -> Result<KeyMetadata> {
        self.read_transit_key(key_id).await?;
        let metadata = self.get_key_metadata(key_id).await?;

        Ok(KeyMetadata {
            key_id: key_id.to_string(),
            key_state: metadata.key_state,
            key_usage: metadata.key_usage,
            description: metadata.description,
            creation_date: metadata.created_at,
            deletion_date: metadata.deletion_date,
            origin: metadata.origin,
            key_manager: "VAULT_TRANSIT".to_string(),
            tags: metadata.tags,
        })
    }

    async fn ensure_key_state_allows(&self, key_id: &str, operation: StateGatedOperation) -> Result<TransitKeyMetadata> {
        let metadata = self.get_key_metadata(key_id).await?;
        ensure_key_state_permits(key_id, &metadata.key_state, operation)?;
        Ok(metadata)
    }
}

impl VaultTransitKmsClient {
    pub(crate) async fn generate_data_key(
        &self,
        request: &GenerateKeyRequest,
        _context: Option<&OperationContext>,
    ) -> Result<DataKeyInfo> {
        self.ensure_key_state_allows(&request.master_key_id, StateGatedOperation::GenerateDataKey)
            .await?;

        let plaintext_key = generate_key_material(&request.key_spec)?;
        let encrypted_key = match self
            .transit_encrypt(&request.master_key_id, &plaintext_key, &request.encryption_context)
            .await
        {
            Ok(encrypted_key) => encrypted_key,
            Err(error) => {
                self.invalidate_metadata_on_state_error(&request.master_key_id, &error).await;
                return Err(error);
            }
        };

        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key: encrypted_key.into_bytes(),
            nonce: Vec::new(),
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            // Transit ciphertext already self-describes its key version
            // ("vault:vN:..."), so the envelope never carries one.
            master_key_version: None,
        };

        let ciphertext = serde_json::to_vec(&envelope)?;
        Ok(DataKeyInfo::new(
            envelope.key_id,
            1,
            Some(plaintext_key),
            ciphertext,
            request.key_spec.clone(),
        ))
    }

    pub(crate) async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        let metadata = self
            .ensure_key_state_allows(&request.key_id, StateGatedOperation::Encrypt)
            .await?;
        let encrypted = match self
            .transit_encrypt(&request.key_id, &request.plaintext, &request.encryption_context)
            .await
        {
            Ok(ciphertext) => ciphertext,
            Err(error) => {
                self.invalidate_metadata_on_state_error(&request.key_id, &error).await;
                return Err(error);
            }
        };

        // The ciphertext must be the same envelope `decrypt` parses — it is what
        // carries the key id and the bound context. Returning the bare Transit
        // string made every `encrypt` result permanently unopenable.
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.key_id.clone(),
            key_spec: "AES_256".to_string(),
            encrypted_key: encrypted.into_bytes(),
            nonce: Vec::new(),
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            // Transit ciphertext already self-describes its key version
            // ("vault:vN:..."), so the envelope never carries one.
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope)?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: metadata.current_version,
            algorithm: "vault-transit".to_string(),
        })
    }

    /// Open a data-key envelope, returning the plaintext and the master key
    /// that wrapped it.
    pub(crate) async fn decrypt(
        &self,
        request: &DecryptRequest,
        _context: Option<&OperationContext>,
    ) -> Result<(Vec<u8>, String)> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;

        for (key, expected_value) in &envelope.encryption_context {
            if let Some(actual_value) = request.encryption_context.get(key) {
                if actual_value != expected_value {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
            } else if !request.encryption_context.is_empty() {
                return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
            }
        }

        let encrypted_key = std::str::from_utf8(&envelope.encrypted_key)
            .map_err(|e| KmsError::cryptographic_error("utf8", format!("Invalid Transit ciphertext: {e}")))?;
        match self
            .transit_decrypt(&envelope.master_key_id, encrypted_key, &envelope.encryption_context)
            .await
        {
            Ok(plaintext) => Ok((plaintext, envelope.master_key_id)),
            Err(error) => {
                self.invalidate_metadata_on_state_error(&envelope.master_key_id, &error).await;
                Err(error)
            }
        }
    }

    /// Report which transit key version wraps an envelope, and whether that is
    /// the key's latest version.
    ///
    /// Reads only key metadata, never the ciphertext's contents, so it answers
    /// for AAD-bound envelopes that [`Self::rewrap_data_key`] has to refuse — an
    /// inventory must be able to count exactly the envelopes that are stuck.
    pub(crate) async fn describe_data_key_wrapping(
        &self,
        request: &DescribeDataKeyWrappingRequest,
    ) -> Result<DescribeDataKeyWrappingResponse> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;
        ensure_rewrap_context_matches(&envelope.encryption_context, &request.encryption_context)?;

        let source_ciphertext = std::str::from_utf8(&envelope.encrypted_key)
            .map_err(|e| KmsError::cryptographic_error("utf8", format!("Invalid Transit ciphertext: {e}")))?;
        let key_version = transit_ciphertext_version(source_ciphertext);
        let current_key_version = self.latest_transit_key_version(&envelope.master_key_id).await?;

        Ok(DescribeDataKeyWrappingResponse {
            key_id: envelope.master_key_id,
            key_version,
            current_key_version,
            // An unreadable prefix on either side means the version is unknown,
            // and unknown must never read as "already current" — that is the
            // answer that lets an operator destroy a version still in use.
            is_current: key_version.is_some() && key_version == current_key_version,
        })
    }

    /// Re-wrap an existing envelope onto the transit key's latest version using
    /// Vault's native rewrap endpoint.
    ///
    /// The data key is never decrypted into this process: Vault re-encrypts the
    /// ciphertext internally and hands back only the new ciphertext, so no
    /// `transit/decrypt` is issued and no plaintext data key exists here to
    /// leak, log or persist.
    ///
    /// # Envelopes bound to an encryption context cannot be rewrapped
    ///
    /// This backend binds the encryption context into the wrapping as AEAD
    /// associated data ([`Self::transit_encrypt`]), and Vault's `transit/rewrap`
    /// endpoint accepts no `associated_data` parameter — the only way to move
    /// such a ciphertext onto a newer version is `transit/decrypt` followed by
    /// `transit/encrypt`, which materializes the plaintext data key inside
    /// RustFS. That trade is refused here rather than made silently: it would
    /// hand back a valid envelope while dropping the very property that makes a
    /// backend-side rewrap worth having. Every object-level envelope carries a
    /// bucket/object context, so in practice this rejects them all until the
    /// context binding or the endpoint changes.
    ///
    /// The context guard still runs first, so a caller that cannot reproduce the
    /// envelope's context is told that rather than being told about the AAD
    /// limitation of an envelope it has no claim on.
    pub(crate) async fn rewrap_data_key(&self, request: &RewrapDataKeyRequest) -> Result<RewrapDataKeyResponse> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;
        ensure_rewrap_context_matches(&envelope.encryption_context, &request.encryption_context)?;
        self.ensure_key_state_allows(&envelope.master_key_id, StateGatedOperation::Encrypt)
            .await?;

        if !envelope.encryption_context.is_empty() {
            return Err(KmsError::rewrap_would_expose_plaintext(
                &envelope.master_key_id,
                "the envelope binds its encryption context as AEAD associated data, which Vault Transit's rewrap endpoint \
                 cannot carry; rewrapping it would require decrypting the data key inside RustFS",
            ));
        }

        let source_ciphertext = std::str::from_utf8(&envelope.encrypted_key)
            .map_err(|e| KmsError::cryptographic_error("utf8", format!("Invalid Transit ciphertext: {e}")))?;
        let source_key_version = transit_ciphertext_version(source_ciphertext);

        let rewrapped_ciphertext = match self.transit_rewrap(&envelope.master_key_id, source_ciphertext).await {
            Ok(ciphertext) => ciphertext,
            Err(error) => {
                self.invalidate_metadata_on_state_error(&envelope.master_key_id, &error).await;
                return Err(error);
            }
        };
        let destination_key_version = transit_ciphertext_version(&rewrapped_ciphertext);

        // Vault re-encrypts unconditionally, so an already-current ciphertext
        // comes back changed but no newer. Report that as "nothing to persist"
        // and hand the input back untouched, or a repeated sweep would rewrite
        // every object's metadata on every pass forever.
        if source_key_version.is_some() && source_key_version == destination_key_version {
            return Ok(RewrapDataKeyResponse {
                ciphertext: request.ciphertext.clone(),
                key_id: envelope.master_key_id,
                source_key_version,
                destination_key_version,
                rewrapped: false,
            });
        }

        let rewrapped_envelope = DataKeyEnvelope {
            key_id: envelope.key_id,
            master_key_id: envelope.master_key_id,
            key_spec: envelope.key_spec,
            encrypted_key: rewrapped_ciphertext.into_bytes(),
            nonce: envelope.nonce,
            encryption_context: envelope.encryption_context,
            created_at: envelope.created_at,
            // Transit ciphertext still self-describes its version, so the
            // envelope field stays absent exactly as generate_data_key leaves it.
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&rewrapped_envelope)?;

        Ok(RewrapDataKeyResponse {
            ciphertext,
            key_id: rewrapped_envelope.master_key_id,
            source_key_version,
            destination_key_version,
            rewrapped: true,
        })
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn create_key(
        &self,
        key_id: &str,
        algorithm: &str,
        _context: Option<&OperationContext>,
    ) -> Result<MasterKeyInfo> {
        if algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(algorithm));
        }

        // Existence pre-check with read-confirm recovery: a create whose
        // response was lost gets retried by callers, and used to be
        // misreported as KeyAlreadyExists. Transit keys are always AES-256,
        // so an existing enabled key of the default usage is exactly what
        // this create would have produced; report it as the create result.
        // Anything else keeps failing. A failed pre-check read must fail the
        // create rather than fall through to re-creating over an unknown key.
        //
        // Two passes: losing the create-only metadata check-and-set race loops
        // back here so the pre-check read-confirms the winning record.
        for _ in 0..2 {
            match self.read_transit_key(key_id).await {
                Ok(_) => {
                    let existing = self.get_key_metadata(key_id).await?;
                    return if existing.key_state == KeyState::Enabled && existing.key_usage == KeyUsage::EncryptDecrypt {
                        info!(
                            key_id,
                            "Vault Transit create found an identical enabled key; treating it as a recovered create"
                        );
                        Ok(MasterKeyInfo {
                            key_id: key_id.to_string(),
                            version: existing.current_version,
                            algorithm: algorithm.to_string(),
                            usage: existing.key_usage,
                            status: KeyStatus::Active,
                            description: existing.description,
                            metadata: existing.tags.clone(),
                            created_at: existing.created_at,
                            rotated_at: None,
                            created_by: existing.created_by,
                            deletion_date: None,
                        })
                    } else {
                        Err(KmsError::key_already_exists(key_id))
                    };
                }
                Err(KmsError::KeyNotFound { .. }) => {}
                Err(error) => return Err(error),
            }

            self.create_transit_key(key_id).await?;

            let metadata = TransitKeyMetadata {
                created_by: Some("vault-transit".to_string()),
                ..TransitKeyMetadata::from_create_request(&CreateKeyRequest {
                    key_name: Some(key_id.to_string()),
                    ..Default::default()
                })
            };
            if self.create_key_metadata(key_id, &metadata).await? {
                return Ok(MasterKeyInfo {
                    key_id: key_id.to_string(),
                    version: metadata.current_version,
                    algorithm: algorithm.to_string(),
                    usage: metadata.key_usage,
                    status: KeyStatus::Active,
                    description: metadata.description,
                    metadata: metadata.tags,
                    created_at: metadata.created_at,
                    rotated_at: None,
                    created_by: metadata.created_by,
                    deletion_date: None,
                });
            }
            // A concurrent creator persisted metadata first; make sure the
            // pre-check reads their record, not a stale cache entry.
            self.metadata_cache.invalidate(key_id).await;
        }
        Err(KmsError::key_already_exists(key_id))
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        self.key_info(key_id).await
    }

    pub(crate) async fn list_keys(
        &self,
        request: &ListKeysRequest,
        _context: Option<&OperationContext>,
    ) -> Result<ListKeysResponse> {
        // A caller asking for no keys is answered without reaching Vault.
        if list_keys_page_size(request.limit).is_none() {
            return Ok(empty_key_page());
        }

        let mut all_keys = self
            .run("vault_transit_list_keys", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                match key::list(&vault.client, &self.config.mount_path).await {
                    Ok(response) => Ok(response.keys),
                    // An empty transit engine answers LIST with a bare 404;
                    // that is an empty listing, not a backend failure.
                    Err(error) if is_empty_transit_list(&error) => Ok(Vec::new()),
                    Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to list Vault Transit keys: {e}"))
                    })),
                }
            })
            .await?;
        // Vault's own LIST ordering is not part of its contract, so the sort is
        // what makes the marker a stable cursor across calls.
        all_keys.sort_unstable();
        let page = paginate_keys(&all_keys, request, String::as_str);

        // Reading metadata only for the page keeps a list bounded by the
        // requested limit instead of by the size of the transit mount.
        let mut keys = Vec::with_capacity(page.items.len());
        let mut unreadable = UnreadableKeys::default();
        for key_id in page.items {
            let key_info = match self.key_info(key_id).await {
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
                        warn!(key_id, %error, "listing a transit key this build cannot describe");
                        unreadable.record(key_id, error);
                        continue;
                    }
                    None => return Err(error),
                },
            };
            let usage_matches = request.usage_filter.as_ref().is_none_or(|usage| usage == &key_info.usage);
            let status_matches = request.status_filter.as_ref().is_none_or(|status| status == &key_info.status);
            if usage_matches && status_matches {
                keys.push(key_info);
            }
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: page.next_marker,
            truncated: page.truncated,
            unreadable_key_ids: unreadable.into_reported_ids(!page.truncated && started_at_the_first_key(request))?,
        })
    }

    pub(crate) async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        // A pending deletion must be reverted through cancel_key_deletion, not
        // silently by enabling, so the gate rejects PendingDeletion here. The
        // gate runs inside the check-and-set loop against every fresh snapshot.
        self.mutate_key_metadata(key_id, |metadata| {
            ensure_key_state_permits(key_id, &metadata.key_state, StateGatedOperation::Enable)?;
            metadata.key_state = KeyState::Enabled;
            metadata.deletion_date = None;
            Ok(())
        })
        .await
        .map(|_| ())
    }

    pub(crate) async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        self.mutate_key_metadata(key_id, |metadata| {
            ensure_key_state_permits(key_id, &metadata.key_state, StateGatedOperation::Disable)?;
            metadata.key_state = KeyState::Disabled;
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// Replace the key's description; `None` clears it.
    ///
    /// Metadata edits carry no state gate: they neither use nor invalidate key
    /// material, so they stay available for whatever lifecycle state the key
    /// is in.
    pub(crate) async fn update_key_description(&self, key_id: &str, description: Option<&str>) -> Result<()> {
        self.mutate_key_metadata(key_id, |metadata| {
            metadata.description = description.map(str::to_string);
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// Add or overwrite tags, leaving every other tag untouched.
    pub(crate) async fn tag_key(&self, key_id: &str, tags: &HashMap<String, String>) -> Result<()> {
        ensure_tag_keys_are_mutable(tags.keys().map(String::as_str))?;
        self.mutate_key_metadata(key_id, |metadata| {
            metadata
                .tags
                .extend(tags.iter().map(|(key, value)| (key.clone(), value.clone())));
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// Remove tags; tags that are not set are ignored.
    pub(crate) async fn untag_key(&self, key_id: &str, tag_keys: &[String]) -> Result<()> {
        ensure_tag_keys_are_mutable(tag_keys.iter().map(String::as_str))?;
        self.mutate_key_metadata(key_id, |metadata| {
            for tag_key in tag_keys {
                metadata.tags.remove(tag_key);
            }
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// Test-only lifecycle driver: the product path goes through [`KmsBackend`].
    #[cfg(test)]
    pub(crate) async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        let deletion_date = Zoned::now() + Duration::from_secs(pending_window_days as u64 * 86400);
        self.mutate_key_metadata(key_id, |metadata| {
            ensure_key_state_permits(key_id, &metadata.key_state, StateGatedOperation::ScheduleDeletion)?;
            metadata.key_state = KeyState::PendingDeletion;
            metadata.deletion_date = Some(deletion_date.clone());
            Ok(())
        })
        .await
        .map(|_| ())
    }

    pub(crate) async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        self.ensure_key_state_allows(key_id, StateGatedOperation::Rotate).await?;

        // Single attempt, never retried: replaying a rotate whose response was
        // lost would advance the key version once more per replay.
        self.run("vault_transit_rotate_key", OpClass::MutatingNonIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            key::rotate(&vault.client, &self.config.mount_path, key_id)
                .await
                .map(|_| ())
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| {
                        KmsError::backend_error(format!("Failed to rotate Vault Transit key {key_id}: {e}"))
                    })
                })
        })
        .await?;

        let metadata = self
            .mutate_key_metadata(key_id, |metadata| {
                // The transit rotation above has already happened; recording
                // the version bump must not be blocked by a concurrent
                // lifecycle transition, so no state gate here.
                metadata.current_version += 1;
                Ok(())
            })
            .await?;

        Ok(MasterKeyInfo {
            key_id: key_id.to_string(),
            version: metadata.current_version,
            algorithm: "AES_256".to_string(),
            usage: metadata.key_usage,
            status: KeyStatus::Active,
            description: metadata.description,
            metadata: metadata.tags,
            created_at: metadata.created_at,
            rotated_at: Some(Zoned::now()),
            created_by: metadata.created_by,
            deletion_date: None,
        })
    }

    pub(crate) async fn health_check(&self) -> Result<()> {
        self.run("vault_transit_health_check", OpClass::ReadIdempotent, move || async move {
            let vault = self.vault().map_err(AttemptError::fatal)?;
            match key::list(&vault.client, &self.config.mount_path).await {
                Ok(_) => Ok(()),
                // A brand-new transit mount holds no keys until something
                // creates one, and this check gates startup before the service
                // creates its own probe key — treating "empty" as unhealthy
                // would keep a first-ever deployment from ever starting.
                Err(error) if is_empty_transit_list(&error) => Ok(()),
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Vault Transit health check failed: {e}"))
                })),
            }
        })
        .await
    }
}

#[cfg(test)]
impl VaultTransitKmsClient {
    /// Rebuild the metadata cache with test-controlled bounds so TTL and
    /// capacity behavior can be exercised without real sleeps.
    fn rebuild_metadata_cache_for_tests(&mut self, capacity: u64, ttl: Duration) {
        self.metadata_cache = Cache::builder().max_capacity(capacity).time_to_live(ttl).build();
    }
}

pub struct VaultTransitKmsBackend {
    client: VaultTransitKmsClient,
}

impl VaultTransitKmsBackend {
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate()?;

        let vault_config = match &config.backend_config {
            crate::config::BackendConfig::VaultTransit(vault_config) => (**vault_config).clone(),
            crate::config::BackendConfig::VaultKv2(vault_config) => VaultTransitConfig {
                address: vault_config.address.clone(),
                auth_method: vault_config.auth_method.clone(),
                namespace: vault_config.namespace.clone(),
                mount_path: vault_config.mount_path.clone(),
                metadata_kv_mount: vault_config.kv_mount.clone(),
                metadata_key_prefix: vault_config.key_path_prefix.clone(),
                tls: vault_config.tls.clone(),
            },
            crate::config::BackendConfig::Local(_)
            | crate::config::BackendConfig::Static(_)
            | crate::config::BackendConfig::Aws(_) => {
                return Err(KmsError::configuration_error("Expected Vault Transit backend configuration"));
            }
        };

        let client = VaultTransitKmsClient::new(vault_config, &config).await?;
        Ok(Self { client })
    }

    /// Spawn the background credential renewal task for this backend, if its
    /// auth method issues lease-bound tokens. The caller owns the returned
    /// handle; dropping it cancels the task.
    pub(crate) fn spawn_credential_renewal(&self) -> Option<CredentialTaskHandle> {
        self.client.credentials.spawn_renewal_task()
    }
}

#[async_trait]
impl KmsBackend for VaultTransitKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Existence pre-check with read-confirm recovery: a create whose
        // response was lost gets retried by callers, and used to be
        // misreported as KeyAlreadyExists. If the stored record is exactly
        // what this request would have written, report it as the create
        // result; any divergence keeps failing so a create can never adopt or
        // reshape a key it would not have produced.
        //
        // Two passes: losing the create-only metadata check-and-set race loops
        // back here so the pre-check read-confirms the winning record.
        for _ in 0..2 {
            match self.client.read_transit_key(&key_id).await {
                Ok(_) => {
                    let existing = self.client.get_key_metadata(&key_id).await?;
                    let requested = TransitKeyMetadata::from_create_request(&request);
                    return if existing.key_state == KeyState::Enabled
                        && existing.key_usage == requested.key_usage
                        && existing.description == requested.description
                        && existing.tags == requested.tags
                    {
                        info!(
                            key_id,
                            "Vault Transit create found an identical enabled key; treating it as a recovered create"
                        );
                        Ok(CreateKeyResponse {
                            key_id: key_id.clone(),
                            key_metadata: KeyMetadata {
                                key_id,
                                key_state: existing.key_state,
                                key_usage: existing.key_usage,
                                description: existing.description,
                                creation_date: existing.created_at,
                                deletion_date: existing.deletion_date,
                                origin: existing.origin,
                                key_manager: "VAULT_TRANSIT".to_string(),
                                tags: existing.tags,
                            },
                        })
                    } else {
                        Err(KmsError::key_already_exists(&key_id))
                    };
                }
                Err(KmsError::KeyNotFound { .. }) => {}
                Err(error) => return Err(error),
            }

            self.client.create_transit_key(&key_id).await?;
            let metadata = TransitKeyMetadata::from_create_request(&request);
            if self.client.create_key_metadata(&key_id, &metadata).await? {
                return Ok(CreateKeyResponse {
                    key_id: key_id.clone(),
                    key_metadata: KeyMetadata {
                        key_id,
                        key_state: metadata.key_state,
                        key_usage: metadata.key_usage,
                        description: metadata.description,
                        creation_date: metadata.created_at,
                        deletion_date: metadata.deletion_date,
                        origin: metadata.origin,
                        key_manager: "VAULT_TRANSIT".to_string(),
                        tags: metadata.tags,
                    },
                });
            }
            // A concurrent creator persisted metadata first; make sure the
            // pre-check reads their record, not a stale cache entry.
            self.client.metadata_cache.invalidate(&key_id).await;
        }
        Err(KmsError::key_already_exists(&key_id))
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        self.client.encrypt(&request, None).await
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let (plaintext, key_id) = self.client.decrypt(&request, None).await?;
        Ok(DecryptResponse {
            plaintext,
            key_id,
            encryption_algorithm: Some("vault-transit".to_string()),
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
        Ok(DescribeKeyResponse {
            key_metadata: self.client.key_metadata_response(&request.key_id).await?,
        })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        self.client.list_keys(&request, None).await
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        let key_id = request.key_id;
        let mut key_metadata = self.client.key_metadata_response(&key_id).await?;

        let deletion_date = if request.force_immediate.unwrap_or(false) {
            if key_metadata.key_state == KeyState::PendingDeletion {
                if !self.client.read_transit_key(&key_id).await?.deletion_allowed {
                    self.client.allow_transit_key_deletion(&key_id).await?;
                }
                self.client.delete_transit_key(&key_id).await?;
                self.client.delete_key_metadata(&key_id).await?;
                None
            } else {
                let now = Zoned::now();
                self.client
                    .mutate_key_metadata(&key_id, |metadata| {
                        metadata.key_state = KeyState::PendingDeletion;
                        metadata.deletion_date = Some(now.clone());
                        Ok(())
                    })
                    .await?;
                key_metadata = self.client.key_metadata_response(&key_id).await?;
                None
            }
        } else {
            ensure_key_state_permits(&key_id, &key_metadata.key_state, StateGatedOperation::ScheduleDeletion)?;

            // Defensive: KmsManager::delete_key is the enforcement point for the
            // waiting window and rejects out-of-range requests before any
            // backend runs. This repeats the bound for callers holding a backend
            // handle directly (tests, maintenance tasks).
            let days = request.pending_window_in_days.unwrap_or(DEFAULT_PENDING_DELETION_WINDOW_DAYS);
            if !(MIN_PENDING_DELETION_WINDOW_DAYS..=MAX_PENDING_DELETION_WINDOW_DAYS).contains(&days) {
                return Err(KmsError::invalid_parameter(format!(
                    "pending_window_in_days must be between {MIN_PENDING_DELETION_WINDOW_DAYS} and {MAX_PENDING_DELETION_WINDOW_DAYS}"
                )));
            }

            let scheduled = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            self.client
                .mutate_key_metadata(&key_id, |metadata| {
                    // Re-run the gate against every fresh snapshot: the check
                    // above used a possibly cached record.
                    ensure_key_state_permits(&key_id, &metadata.key_state, StateGatedOperation::ScheduleDeletion)?;
                    metadata.key_state = KeyState::PendingDeletion;
                    metadata.deletion_date = Some(scheduled.clone());
                    Ok(())
                })
                .await?;
            key_metadata = self.client.key_metadata_response(&key_id).await?;
            Some(scheduled.to_string())
        };

        Ok(DeleteKeyResponse {
            key_id,
            deletion_date,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = request.key_id.as_str();
        self.client
            .mutate_key_metadata(key_id, |metadata| {
                // Re-checked against every fresh snapshot: a concurrent sweep
                // that tombstoned the key must fail this cancel, not be
                // overwritten blind.
                if metadata.key_state != KeyState::PendingDeletion {
                    return Err(KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
                }
                metadata.key_state = KeyState::Enabled;
                metadata.deletion_date = None;
                Ok(())
            })
            .await?;

        Ok(CancelKeyDeletionResponse {
            key_id: request.key_id.clone(),
            key_metadata: self.client.key_metadata_response(&request.key_id).await?,
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
        // Vault Transit natively supports version-retaining rotation, keeps
        // prior versions addressable for decryption, and allows physical
        // deletion once a key is pending deletion. Rewrap is advertised because
        // the endpoint exists and works; envelopes whose encryption context is
        // bound as associated data are still refused per envelope (see
        // `VaultTransitKmsClient::rewrap_data_key`), which is a property of the
        // envelope rather than of the backend.
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
        // The transit key's existence anchors "already removed": once it is
        // gone only stale scheduling metadata can remain, so clean that up.
        match self.client.read_transit_key(key_id).await {
            Ok(_) => {}
            Err(KmsError::KeyNotFound { .. }) => {
                self.client.delete_key_metadata(key_id).await?;
                return Ok(ExpiredKeyRemoval::Removed);
            }
            Err(error) => return Err(error),
        }

        // Tombstone under check-and-set: every attempt re-reads the record and
        // re-validates state and due-ness, so a cancel_key_deletion racing the
        // sweep either lands before the tombstone (the re-read sees Enabled
        // and the sweep backs off) or after it (the cancel's own
        // check-and-set write fails).
        let mut tombstoned = false;
        for _ in 0..METADATA_CAS_ATTEMPTS {
            let Some((cas, mut metadata)) = self.client.read_metadata_from_kv_versioned(key_id).await? else {
                // No persisted lifecycle record (pre-persistence key): the
                // worker never destroys material whose scheduling state was
                // never recorded.
                return Ok(ExpiredKeyRemoval::StateChanged);
            };
            match metadata.key_state {
                // Tombstone left by a crashed removal: complete it.
                KeyState::Unavailable => {
                    tombstoned = true;
                }
                KeyState::PendingDeletion => {
                    match &metadata.deletion_date {
                        Some(deadline) if deadline <= now => {}
                        // Not yet due, or no persisted deadline — never auto-remove.
                        _ => return Ok(ExpiredKeyRemoval::NotExpired),
                    }
                    // Tombstone first: an Unavailable record is rejected by every
                    // state gate, and a crashed removal can simply be re-run.
                    metadata.key_state = KeyState::Unavailable;
                    if self.client.cas_write_metadata_to_kv(key_id, &metadata, cas).await? {
                        self.client.metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
                        tombstoned = true;
                    } else {
                        // Lost the check-and-set race — most likely a
                        // concurrent cancel; re-read and re-decide.
                        self.client.metadata_cache.invalidate(key_id).await;
                        continue;
                    }
                }
                KeyState::Enabled | KeyState::Disabled | KeyState::PendingImport => {
                    return Ok(ExpiredKeyRemoval::StateChanged);
                }
            }
            break;
        }
        if !tombstoned {
            return Err(VaultTransitKmsClient::metadata_cas_conflict(key_id));
        }

        if !self.client.read_transit_key(key_id).await?.deletion_allowed {
            self.client.allow_transit_key_deletion(key_id).await?;
        }
        self.client.delete_transit_key(key_id).await?;
        self.client.delete_key_metadata(key_id).await?;
        Ok(ExpiredKeyRemoval::Removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::scripted_vault::{ScriptedResponse, ScriptedVault};
    use crate::config::{
        DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT, VaultAuthMethod, VaultTransitConfig,
    };
    use crate::types::KeyStatus;
    use vaultrs::api::transit::responses::{ReadKeyData, ReadKeyResponse};

    async fn scripted_client(responses: Vec<ScriptedResponse>) -> (ScriptedVault, VaultTransitKmsClient) {
        let vault = ScriptedVault::serve(responses).await;
        let config = VaultTransitConfig {
            address: vault.address.clone(),
            ..test_vault_transit_config()
        };
        let kms_config = KmsConfig {
            timeout: Duration::from_secs(5),
            retry_attempts: 3,
            ..KmsConfig::default()
        };
        let client = VaultTransitKmsClient::new(config, &kms_config)
            .await
            .expect("scripted Vault Transit client");
        (vault, client)
    }

    /// KV2 read payload for a persisted transit metadata record.
    fn metadata_read_data(metadata: &TransitKeyMetadata) -> serde_json::Value {
        let persisted: TransitKeyMetadataPersisted = metadata.clone().into();
        serde_json::json!({
            "data": serde_json::to_value(&persisted).expect("serialize transit metadata"),
            "metadata": {
                "created_time": "2026-01-01T00:00:00Z",
                "deletion_time": "",
                "custom_metadata": null,
                "destroyed": false,
                "version": 1,
            },
        })
    }

    /// Transit read-key payload for an existing symmetric key.
    fn transit_key_read_data(key_id: &str) -> serde_json::Value {
        let response = ReadKeyResponse {
            key_type: KeyType::Aes256Gcm96,
            deletion_allowed: false,
            derived: false,
            exportable: false,
            allow_plaintext_backup: false,
            keys: ReadKeyData::Symmetric(HashMap::from([("1".to_string(), 1_700_000_000_u64)])),
            min_decryption_version: 1,
            min_encryption_version: 0,
            name: key_id.to_string(),
            supports_encryption: true,
            supports_decryption: true,
            supports_derivation: false,
            supports_signing: false,
            imported: Some(false),
        };
        serde_json::to_value(&response).expect("serialize transit key read response")
    }

    /// A listing must not silently shrink when the backend is the problem.
    ///
    /// Before the per-key classification this path used `?`, so any describe
    /// failure failed the page; the risk introduced by classifying is the
    /// opposite one — quietly dropping a key on an error that says nothing
    /// about it. A transit mount that stops answering must still fail loudly.
    #[tokio::test]
    async fn list_fails_when_a_transit_key_read_is_unavailable() {
        let mut responses = vec![ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a"] }))];
        for _ in 0..3 {
            responses.push(ScriptedResponse::error(503, "temporarily unavailable"));
        }
        let (_vault, client) = scripted_client(responses).await;

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("an unreachable transit mount must fail the listing, not empty it");
        assert!(
            matches!(error, KmsError::BackendError { .. }),
            "a transient backend failure must not be reported as a damaged key: {error:?}"
        );
    }

    /// A transit key whose persisted metadata record cannot be decoded is
    /// reported per key, not as a backend outage.
    ///
    /// The metadata record lives in KV2 exactly like a KV2 key record, so it has
    /// the same failure mode: without this classification one record written by
    /// a newer build fails every listing on the node, and the deletion sweep —
    /// which aborts on a listing error — stops destroying every other expired
    /// key for as long as that record is there.
    #[tokio::test]
    async fn list_reports_an_undecodable_metadata_record_per_key() {
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a", "key-b"] })),
            ScriptedResponse::ok(transit_key_read_data("key-a")),
            ScriptedResponse::ok(metadata_read_data(&TransitKeyMetadata::from_create_request(
                &CreateKeyRequest::default(),
            ))),
            ScriptedResponse::ok(transit_key_read_data("key-b")),
            // `key_usage` is an enum; a number cannot be decoded into it.
            ScriptedResponse::ok(serde_json::json!({
                "data": { "key_usage": 42 },
                "metadata": { "created_time": "2026-01-01T00:00:00Z", "deletion_time": "", "custom_metadata": null, "destroyed": false, "version": 1 },
            })),
        ])
        .await;

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("one undecodable metadata record must not fail the whole listing");
        assert_eq!(response.keys.len(), 1, "the readable key must still be listed");
        assert_eq!(response.unreadable_key_ids, vec!["key-b".to_string()]);
    }

    /// A key destroyed between the listing and the read is dropped, and the
    /// listing still succeeds — the cursor comes from the identifier list, so
    /// it advances past the gap on its own.
    #[tokio::test]
    async fn list_drops_a_key_that_vanished_between_the_scan_and_the_read() {
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(serde_json::json!({ "keys": ["key-a"] })),
            ScriptedResponse::error(404, "no handler for route"),
        ])
        .await;

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("a key removed mid-listing must not fail the page");
        assert!(response.keys.is_empty());
        assert!(
            response.unreadable_key_ids.is_empty(),
            "a concurrent deletion is not damage: {:?}",
            response.unreadable_key_ids
        );
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

    #[tokio::test]
    async fn wired_transit_encrypt_retries_transient_status() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::error(429, "throttled"),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
        ])
        .await;

        let response = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"plaintext".to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("encrypt must retry past a transient 429");
        let envelope: DataKeyEnvelope = serde_json::from_slice(&response.ciphertext).expect("encrypt must return an envelope");
        assert_eq!(envelope.encrypted_key, b"vault:v1:scripted".to_vec());
        assert_eq!(envelope.master_key_id, "wired-key");

        let requests = vault.requests();
        assert_eq!(requests.len(), 3, "metadata read plus two encrypt attempts: {requests:?}");
        assert_eq!(requests[1], "POST /v1/transit/encrypt/wired-key");
        assert_eq!(requests[2], "POST /v1/transit/encrypt/wired-key");
    }

    #[tokio::test]
    async fn wired_transit_rotate_is_never_retried() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::error(503, "standby"),
        ])
        .await;

        let error = client
            .rotate_key("wired-key", None)
            .await
            .expect_err("the scripted 503 must fail the rotation");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "metadata read plus exactly one rotate attempt: {requests:?}");
        assert_eq!(
            requests[1], "POST /v1/transit/keys/wired-key/rotate",
            "a rotation must never be replayed: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_transit_create_read_confirms_identical_existing_key() {
        // The stored key and metadata are exactly what this create would have
        // produced, so a retried create whose first response was lost recovers
        // by reading them back instead of failing.
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            ScriptedResponse::ok(metadata_read_data(&metadata)),
        ])
        .await;

        let recovered = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect("an identical enabled key must read-confirm as a recovered create");
        assert_eq!(recovered.status, KeyStatus::Active);

        let requests = vault.requests();
        assert_eq!(requests.len(), 2, "read-confirm must be decided from reads alone: {requests:?}");
        assert!(
            requests.iter().all(|line| line.starts_with("GET ")),
            "a recovered create must not write anything: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_transit_create_still_fails_on_mismatched_existing_key() {
        let mut metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        metadata.key_state = KeyState::Disabled;
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            ScriptedResponse::ok(metadata_read_data(&metadata)),
        ])
        .await;

        let error = client
            .create_key("wired-key", "AES_256", None)
            .await
            .expect_err("a non-enabled existing key must keep failing the create");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }), "got {error:?}");

        let requests = vault.requests();
        assert!(
            requests.iter().all(|line| line.starts_with("GET ")),
            "the rejected create must not write anything: {requests:?}"
        );
    }

    /// Regression test for the first-boot chicken-and-egg on a fresh transit
    /// mount (rustfs/backlog#1774).
    ///
    /// Vault answers a LIST on a mounted-but-empty transit engine with a 404
    /// carrying an empty `errors` array. The health check gates startup before
    /// the service creates its probe key, so this 404 must count as healthy —
    /// failing it means a first-ever deployment on a fresh mount can never
    /// start until an operator creates some transit key out-of-band.
    #[tokio::test]
    async fn health_check_passes_on_an_empty_transit_engine() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::Http {
            status: 404,
            body: serde_json::json!({ "errors": [] }).to_string(),
        }])
        .await;

        client
            .health_check()
            .await
            .expect("an empty transit engine is reachable and must pass the health check");

        let requests = vault.requests();
        assert_eq!(
            requests,
            vec!["LIST /v1/transit/keys".to_string()],
            "the empty-list 404 must be accepted on the first attempt, not retried"
        );
    }

    /// A 404 whose body says "no handler for route" means no transit engine is
    /// mounted at the configured path at all; that must keep failing the
    /// health check instead of riding the empty-engine allowance.
    #[tokio::test]
    async fn health_check_fails_when_the_transit_mount_is_missing() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::error(
            404,
            "no handler for route \"transit/keys\". route entry not found.",
        )])
        .await;

        let error = client
            .health_check()
            .await
            .expect_err("a missing transit mount must fail the health check");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
    }

    /// The empty-engine allowance is scoped to 404 alone: any other status
    /// whose body happens to carry an empty `errors` array (an intermediary
    /// answering for Vault, for instance) must keep failing the health check.
    #[tokio::test]
    async fn health_check_fails_on_a_non_404_error_with_an_empty_errors_body() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::Http {
            status: 403,
            body: serde_json::json!({ "errors": [] }).to_string(),
        }])
        .await;

        let error = client
            .health_check()
            .await
            .expect_err("only a 404 may ride the empty-engine allowance");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
    }

    /// The listing's own copy of the discriminator must not widen into "every
    /// LIST failure is an empty listing" — a missing mount still fails loudly.
    #[tokio::test]
    async fn list_fails_when_the_transit_mount_is_missing() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::error(
            404,
            "no handler for route \"transit/keys\". route entry not found.",
        )])
        .await;

        let error = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect_err("a missing transit mount must fail the listing, not empty it");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");
    }

    /// The same empty-engine 404 on the listing path is an empty result set,
    /// not a backend failure.
    #[tokio::test]
    async fn list_keys_returns_an_empty_page_on_an_empty_transit_engine() {
        let (_vault, client) = scripted_client(vec![ScriptedResponse::Http {
            status: 404,
            body: serde_json::json!({ "errors": [] }).to_string(),
        }])
        .await;

        let response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("an empty transit engine must list as empty, not fail");
        assert!(response.keys.is_empty(), "got {:?}", response.keys);
        assert!(!response.truncated, "an empty listing has nothing left to page through");
        assert_eq!(response.next_marker, None);
    }

    fn test_vault_transit_config() -> VaultTransitConfig {
        VaultTransitConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "dev-token".to_string()),
            },
            namespace: None,
            mount_path: "transit".to_string(),
            metadata_kv_mount: DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT.to_string(),
            metadata_key_prefix: DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX.to_string(),
            tls: None,
        }
    }

    /// Regression test for rustfs/backlog#808.
    ///
    /// VaultTransit stores key metadata (state, tags, etc.) ONLY in an in-memory
    /// `metadata_cache`. On a cache miss — including after any server restart —
    /// `get_key_metadata()` synthesises a fresh record with `key_state: Enabled`.
    /// This means a disabled/deleted key silently revives as Enabled after restart.
    #[tokio::test]
    #[ignore] // Requires a running Vault instance with transit engine enabled
    async fn test_transit_key_state_lost_after_restart_simulation() {
        let config = test_vault_transit_config();

        // --- First "process": create a key and disable it ---
        let client1 = VaultTransitKmsClient::new(config.clone(), &KmsConfig::default())
            .await
            .expect("Failed to create VaultTransit client");

        let key_id = format!("regression-808-{}", uuid::Uuid::new_v4());

        // Create key → Enabled
        let created = client1.create_key(&key_id, "AES_256", None).await.expect("create_key");
        assert_eq!(created.status, KeyStatus::Active, "newly created key must be Active");

        let info = client1
            .describe_key(&key_id, None)
            .await
            .expect("describe_key before disable");
        assert_eq!(info.status, KeyStatus::Active, "key must be Active before disable");

        // Disable the key
        client1.disable_key(&key_id, None).await.expect("disable_key");

        let info_after_disable = client1.describe_key(&key_id, None).await.expect("describe_key after disable");
        assert_eq!(info_after_disable.status, KeyStatus::Disabled, "key must be Disabled after disable_key");

        // --- Simulate restart: create a brand new client with empty cache ---
        let client2 = VaultTransitKmsClient::new(config, &KmsConfig::default())
            .await
            .expect("Failed to create second VaultTransit client (restart simulation)");

        // After "restart", the key must remain Disabled because KV-persisted metadata
        // survives across client recreation.
        let info_after_restart = client2
            .describe_key(&key_id, None)
            .await
            .expect("describe_key after restart simulation");

        assert_eq!(
            info_after_restart.status,
            KeyStatus::Disabled,
            "after restart, a disabled key must remain Disabled"
        );

        // Cleanup: schedule the key for deletion so Vault state is clean for the next run.
        let _ = client2.schedule_key_deletion(&key_id, 7, None).await;
    }

    /// Regression test for rustfs/backlog#808.
    ///
    /// PendingDeletion must be persisted outside the process-local metadata cache.
    /// Otherwise, a restart would synthesize Enabled metadata and allow new key use.
    #[tokio::test]
    #[ignore] // Requires a running Vault instance with transit engine enabled
    async fn test_transit_pending_deletion_survives_restart_simulation() {
        let config = test_vault_transit_config();

        let client1 = VaultTransitKmsClient::new(config.clone(), &KmsConfig::default())
            .await
            .expect("Failed to create VaultTransit client");

        let key_id = format!("regression-808-pending-{}", uuid::Uuid::new_v4());

        let created = client1.create_key(&key_id, "AES_256", None).await.expect("create_key");
        assert_eq!(created.status, KeyStatus::Active, "newly created key must be Active");

        client1
            .schedule_key_deletion(&key_id, 7, None)
            .await
            .expect("schedule_key_deletion");

        let info_after_schedule = client1
            .describe_key(&key_id, None)
            .await
            .expect("describe_key after schedule_key_deletion");
        assert_eq!(
            info_after_schedule.status,
            KeyStatus::PendingDeletion,
            "key must be PendingDeletion after schedule_key_deletion"
        );

        let client2 = VaultTransitKmsClient::new(config, &KmsConfig::default())
            .await
            .expect("Failed to create second VaultTransit client (restart simulation)");

        let info_after_restart = client2
            .describe_key(&key_id, None)
            .await
            .expect("describe_key after restart simulation");

        assert_eq!(
            info_after_restart.status,
            KeyStatus::PendingDeletion,
            "after restart, a pending-deletion key must remain PendingDeletion"
        );

        let generate_result = client2
            .generate_data_key(
                &GenerateKeyRequest {
                    master_key_id: key_id,
                    key_spec: "AES_256".to_string(),
                    key_length: Some(32),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await;
        assert!(
            generate_result.is_err(),
            "after restart, a pending-deletion key must not be usable for new data keys"
        );
    }

    /// Contract regression for rustfs/backlog#1565.
    ///
    /// Transit rotation is delegated entirely to Vault's own key versioning: the
    /// ciphertext self-describes the wrapping version ("vault:vN:..."), so historical
    /// ciphertext must keep decrypting after rotation without any RustFS-side
    /// version bookkeeping in the envelope.
    #[tokio::test]
    #[ignore] // Requires a running Vault instance with transit engine enabled
    async fn test_transit_old_ciphertext_decrypts_after_rotate() {
        let client = VaultTransitKmsClient::new(test_vault_transit_config(), &KmsConfig::default())
            .await
            .expect("Failed to create VaultTransit client");

        let key_id = format!("regression-1565-rotate-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create_key");

        let request = GenerateKeyRequest {
            master_key_id: key_id.clone(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        let dk_v1 = client.generate_data_key(&request, None).await.expect("generate under v1");
        let env_v1: DataKeyEnvelope = serde_json::from_slice(&dk_v1.ciphertext).expect("parse v1 envelope");
        assert!(
            env_v1.encrypted_key.starts_with(b"vault:v1:"),
            "first-version Transit ciphertext must carry the vault:v1: prefix"
        );
        assert_eq!(
            env_v1.master_key_version, None,
            "Transit envelopes must not carry a RustFS-side master key version"
        );

        let rotated = client.rotate_key(&key_id, None).await.expect("rotate_key");
        assert_eq!(rotated.version, 2, "rotation must advance the Transit key version");

        let dk_v2 = client.generate_data_key(&request, None).await.expect("generate under v2");
        let env_v2: DataKeyEnvelope = serde_json::from_slice(&dk_v2.ciphertext).expect("parse v2 envelope");
        assert!(
            env_v2.encrypted_key.starts_with(b"vault:v2:"),
            "post-rotation Transit ciphertext must carry the vault:v2: prefix"
        );

        // Historical ciphertext keeps decrypting per Vault's version semantics,
        // interleaved with post-rotation ciphertext.
        for (data_key, label) in [(&dk_v1, "v1"), (&dk_v2, "v2"), (&dk_v1, "v1 again")] {
            let (plaintext, _opened_by) = client
                .decrypt(
                    &DecryptRequest {
                        ciphertext: data_key.ciphertext.clone(),
                        encryption_context: HashMap::new(),
                        grant_tokens: Vec::new(),
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("{label} ciphertext must stay decryptable after rotation: {error}"));
            assert_eq!(Some(plaintext), data_key.plaintext, "{label} plaintext must round-trip");
        }

        // Cleanup so repeated runs against the same Vault do not accumulate keys.
        let _ = client.schedule_key_deletion(&key_id, 7, None).await;
    }

    /// The persistence fallback for pre-metadata keys deliberately fabricates
    /// an Enabled record (rustfs#4256 / rustfs#4262): those keys were usable
    /// before metadata persistence existed and must stay usable once the
    /// record is durably persisted. The old fail-open this test used to pin —
    /// a failed metadata read or persist still yielded a usable Enabled key —
    /// was flipped to fail closed for rustfs/backlog#1581; that side is
    /// covered by `wired_encrypt_fails_closed_when_the_metadata_read_fails`
    /// and `wired_synthesized_metadata_is_not_served_when_the_persist_fails`.
    #[test]
    fn synthesized_metadata_defaults_to_enabled() {
        let metadata = TransitKeyMetadata::synthesized();
        assert_eq!(metadata.key_state, KeyState::Enabled);
        assert!(metadata.deletion_date.is_none());
    }

    #[test]
    fn transit_key_metadata_unknown_fields_remain_readable_and_are_observed() {
        // A record written by a newer build carries fields this build does not
        // know. It must stay readable — and the drop must be visible, not
        // silent (rustfs/backlog#1641). Only the field name may be logged.
        let persisted: TransitKeyMetadataPersisted = TransitKeyMetadata::synthesized().into();
        let mut value = serde_json::to_value(&persisted).expect("serialize metadata record");
        let object = value.as_object_mut().expect("metadata record serializes to an object");
        object.insert("field_from_the_future".to_string(), serde_json::json!("field value must not be logged"));

        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let parsed: TransitKeyMetadataPersisted = metrics::with_local_recorder(&recorder, || {
            tracing::dispatcher::with_default(&dispatch, || {
                serde_json::from_value(value).expect("unknown fields must remain readable")
            })
        });
        assert_eq!(parsed.key_state, KeyState::Enabled);
        assert_eq!(crate::test_support::unknown_field_metric(&recorder, "vault-transit-key-metadata"), 1);

        let output = logs.output();
        assert!(
            output.contains("Vault Transit key metadata record contains unknown fields"),
            "got: {output}"
        );
        assert!(output.contains("field_from_the_future"));
        assert!(!output.contains("field value must not be logged"));
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
    async fn wired_backend_lifecycle_overrides_reach_the_client() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let mut disabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        disabled.key_state = KeyState::Disabled;
        let vault = ScriptedVault::serve(vec![
            // disable: versioned read (secret metadata + pinned version), then
            // the check-and-set write persisting Disabled.
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(kv2_write_ack()),
            // enable: another versioned read against the Disabled record, then
            // the check-and-set write persisting Enabled.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(metadata_read_data(&disabled)),
            ScriptedResponse::ok(kv2_write_ack()),
            // rotate: the state gate hits the metadata cache; the single
            // rotate attempt fails and must not be retried.
            ScriptedResponse::error(503, "standby"),
        ])
        .await;
        let config = KmsConfig::vault_transit(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultTransitKmsBackend::new(config)
            .await
            .expect("vault transit backend should build");

        backend
            .disable_key("wired-key")
            .await
            .expect("KmsBackend::disable_key must persist through the client");
        backend
            .enable_key("wired-key")
            .await
            .expect("KmsBackend::enable_key must persist through the client");
        let error = backend
            .rotate_key("wired-key")
            .await
            .expect_err("the scripted 503 must fail the rotation");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 7, "two versioned read+write cycles plus one rotate attempt: {requests:?}");
        assert_eq!(requests[6], "POST /v1/transit/keys/wired-key/rotate", "{requests:?}");
    }

    /// KmsManager::delete_key is the enforcement point for the waiting window;
    /// this pins the backend's defensive copy of the same bound, which is all
    /// that stands between a direct backend caller and a one-day window.
    #[tokio::test]
    async fn wired_backend_delete_refuses_a_window_outside_the_supported_range() {
        for days in [MIN_PENDING_DELETION_WINDOW_DAYS - 1, MAX_PENDING_DELETION_WINDOW_DAYS + 1] {
            let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
            let vault = ScriptedVault::serve(vec![
                // The state gate reads the transit key, then its metadata record.
                ScriptedResponse::ok(transit_key_read_data("wired-key")),
                ScriptedResponse::ok(metadata_read_data(&metadata)),
            ])
            .await;
            let config = KmsConfig::vault_transit(
                url::Url::parse(&vault.address).expect("scripted vault address should parse"),
                "scripted-token".to_string(),
            )
            .with_insecure_development_defaults();
            let backend = VaultTransitKmsBackend::new(config)
                .await
                .expect("vault transit backend should build");

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

    /// KV2 secret-metadata read payload (`kv2::read_metadata`) pinning the
    /// current secret version used as the check-and-set base.
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

    const CAS_CONFLICT_MESSAGE: &str = "check-and-set parameter did not match the current version";

    const METADATA_PATH: &str = "/v1/secret/data/rustfs/kms/transit-metadata/wired-key";
    const METADATA_VERSION_PATH: &str = "/v1/secret/metadata/rustfs/kms/transit-metadata/wired-key";

    #[tokio::test]
    async fn wired_disable_retries_past_a_cas_conflict_with_a_fresh_read() {
        let enabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(metadata_read_data(&enabled)),
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // The conflict must trigger a fresh versioned read, then the
            // write is check-and-set against the new snapshot.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(metadata_read_data(&enabled)),
            ScriptedResponse::ok(kv2_write_ack()),
        ])
        .await;

        client
            .disable_key("wired-key", None)
            .await
            .expect("a single check-and-set conflict must be absorbed by a re-read");

        let requests = vault.requests();
        assert_eq!(requests.len(), 6, "two read+read+write cycles: {requests:?}");
        assert_eq!(requests[0], format!("GET {METADATA_VERSION_PATH}"));
        assert_eq!(requests[1], format!("GET {METADATA_PATH}?version=1"));
        assert_eq!(requests[2], format!("POST {METADATA_PATH}"));
        assert_eq!(requests[3], format!("GET {METADATA_VERSION_PATH}"), "conflict must re-read: {requests:?}");
        assert_eq!(requests[4], format!("GET {METADATA_PATH}?version=2"));
        assert_eq!(requests[5], format!("POST {METADATA_PATH}"));
    }

    #[tokio::test]
    async fn wired_disable_cas_conflict_budget_is_bounded() {
        let enabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let mut responses = Vec::new();
        for cycle in 0..3u64 {
            responses.push(ScriptedResponse::ok(kv2_metadata_read_data(cycle + 1)));
            responses.push(ScriptedResponse::ok(metadata_read_data(&enabled)));
            responses.push(ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE));
        }
        let (vault, client) = scripted_client(responses).await;

        let error = client
            .disable_key("wired-key", None)
            .await
            .expect_err("exhausting the check-and-set budget must surface the conflict");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(
            error.to_string().contains("Concurrent modification"),
            "the error must name the conflict: {error}"
        );

        let requests = vault.requests();
        assert_eq!(requests.len(), 9, "exactly three read+read+write cycles, no blind replays: {requests:?}");
    }

    #[tokio::test]
    async fn wired_cas_conflict_reread_revalidates_the_state_gate() {
        let enabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let mut pending = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        pending.key_state = KeyState::PendingDeletion;
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(metadata_read_data(&enabled)),
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // The concurrent writer scheduled the key for deletion; the
            // re-read must re-run the state gate and reject the disable.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(metadata_read_data(&pending)),
        ])
        .await;

        let error = client
            .disable_key("wired-key", None)
            .await
            .expect_err("the re-read state gate must reject a pending-deletion key");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(error.to_string().contains("pending deletion"), "got {error}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 5, "the gate rejection must not issue another write: {requests:?}");
        assert!(requests[4].starts_with("GET "), "{requests:?}");
    }

    #[tokio::test]
    async fn wired_encrypt_key_not_found_invalidates_the_cached_metadata() {
        let enabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let mut disabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        disabled.key_state = KeyState::Disabled;
        let (vault, client) = scripted_client(vec![
            // First encrypt: gate reads Enabled and caches it, then the
            // transit call reports the key gone server-side.
            ScriptedResponse::ok(metadata_read_data(&enabled)),
            ScriptedResponse::error(404, "encryption key not found"),
            // Second encrypt: the state error must have dropped the cache
            // entry, so the gate re-reads and sees the Disabled record.
            ScriptedResponse::ok(metadata_read_data(&disabled)),
        ])
        .await;

        let request = EncryptRequest {
            key_id: "wired-key".to_string(),
            plaintext: b"plaintext".to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };
        let error = client
            .encrypt(&request, None)
            .await
            .expect_err("the scripted 404 must fail the encrypt");
        assert!(matches!(error, KmsError::KeyNotFound { .. }), "got {error:?}");

        let error = client
            .encrypt(&request, None)
            .await
            .expect_err("the re-read Disabled record must reject the encrypt");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(
            requests.len(),
            3,
            "the second gate must re-read instead of trusting the stale Enabled entry, \
             and must not reach the encrypt endpoint: {requests:?}"
        );
        assert_eq!(requests[2], format!("GET {METADATA_PATH}"), "{requests:?}");
    }

    #[tokio::test]
    async fn wired_metadata_cache_ttl_expiry_forces_a_fresh_read() {
        let enabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let mut disabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        disabled.key_state = KeyState::Disabled;
        let (vault, mut client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&enabled)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
            // Post-expiry gate read observes the disable another node
            // persisted in the meantime.
            ScriptedResponse::ok(metadata_read_data(&disabled)),
        ])
        .await;
        // A 1ns TTL expires between any two awaits, standing in for the real
        // 300s bound without a wall-clock sleep.
        client.rebuild_metadata_cache_for_tests(METADATA_CACHE_CAPACITY, Duration::from_nanos(1));

        let request = EncryptRequest {
            key_id: "wired-key".to_string(),
            plaintext: b"plaintext".to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };
        client
            .encrypt(&request, None)
            .await
            .expect("the first encrypt must pass the Enabled gate");

        let error = client
            .encrypt(&request, None)
            .await
            .expect_err("after TTL expiry the gate must see the remote disable");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 3, "the expired entry must force a fresh KV read: {requests:?}");
        assert_eq!(requests[2], format!("GET {METADATA_PATH}"), "{requests:?}");
    }

    #[tokio::test]
    async fn metadata_cache_capacity_is_bounded() {
        let records: Vec<_> = (0..3)
            .map(|_| {
                ScriptedResponse::ok(metadata_read_data(&TransitKeyMetadata::from_create_request(&CreateKeyRequest::default())))
            })
            .collect();
        let (_vault, mut client) = scripted_client(records).await;
        client.rebuild_metadata_cache_for_tests(2, METADATA_CACHE_TTL);

        for key_id in ["key-a", "key-b", "key-c"] {
            client
                .get_key_metadata(key_id)
                .await
                .expect("each scripted metadata read must succeed");
        }

        client.metadata_cache.run_pending_tasks().await;
        assert!(
            client.metadata_cache.entry_count() <= 2,
            "the cache must not hold more entries than its capacity, got {}",
            client.metadata_cache.entry_count()
        );
    }

    #[tokio::test]
    async fn wired_encrypt_fails_closed_when_the_metadata_read_fails() {
        let (vault, client) = scripted_client(vec![ScriptedResponse::error(403, "permission denied")]).await;

        let error = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"plaintext".to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("a failed metadata read must fail the encrypt, not synthesize Enabled");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 1, "the gate failure must never reach the encrypt endpoint: {requests:?}");
    }

    #[tokio::test]
    async fn wired_synthesized_metadata_is_not_served_when_the_persist_fails() {
        // Regression for the rustfs/backlog#1581 fail-open flip: a missing
        // metadata record used to synthesize a usable Enabled record even when
        // persisting it failed, letting encrypt proceed on state no other node
        // could observe. The persist failure must now fail the read.
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::error(404, "no value found"),
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            ScriptedResponse::error(500, "kv write failed"),
        ])
        .await;

        let error = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"plaintext".to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("an unpersisted synthesized record must never gate an encrypt open");
        assert!(matches!(error, KmsError::BackendError { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 3, "read, existence check, failed persist — and no encrypt: {requests:?}");
        assert_eq!(requests[2], format!("POST {METADATA_PATH}"), "{requests:?}");
    }

    #[tokio::test]
    async fn wired_synthesized_metadata_create_race_adopts_the_winning_record() {
        let mut disabled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        disabled.key_state = KeyState::Disabled;
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::error(404, "no value found"),
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            // Another node persisted a record first; the create-only
            // check-and-set loses and the re-read adopts the winner.
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            ScriptedResponse::ok(metadata_read_data(&disabled)),
        ])
        .await;

        let error = client
            .encrypt(
                &EncryptRequest {
                    key_id: "wired-key".to_string(),
                    plaintext: b"plaintext".to_vec(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect_err("the winner's Disabled record must gate the encrypt, not the loser's Enabled one");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        let requests = vault.requests();
        assert_eq!(requests.len(), 4, "the lost create race must re-read, never overwrite: {requests:?}");
        assert_eq!(requests[3], format!("GET {METADATA_PATH}"), "{requests:?}");
    }

    #[tokio::test]
    async fn wired_backend_create_loses_the_metadata_create_race_and_read_confirms() {
        let winner = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let vault = ScriptedVault::serve(vec![
            // Pre-check: the transit key does not exist yet.
            ScriptedResponse::error(404, "not found"),
            // Transit create succeeds, but a concurrent creator persists the
            // metadata record first.
            ScriptedResponse::ok(serde_json::json!({})),
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // Second pass: the pre-check now read-confirms the winner.
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            ScriptedResponse::ok(metadata_read_data(&winner)),
        ])
        .await;
        let config = KmsConfig::vault_transit(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultTransitKmsBackend::new(config)
            .await
            .expect("vault transit backend should build");

        let response = backend
            .create_key(CreateKeyRequest {
                key_name: Some("wired-key".to_string()),
                ..Default::default()
            })
            .await
            .expect("losing the metadata create race to an identical record must recover the create");
        assert_eq!(response.key_metadata.key_state, KeyState::Enabled);

        let requests = vault.requests();
        assert_eq!(requests.len(), 5, "one lost create pass plus one read-confirm pass: {requests:?}");
        assert!(
            requests[3].starts_with("GET ") && requests[4].starts_with("GET "),
            "the recovery pass must be reads only: {requests:?}"
        );
    }

    #[tokio::test]
    async fn wired_expired_sweep_backs_off_when_cancel_wins_the_cas_race() {
        let mut pending = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        pending.key_state = KeyState::PendingDeletion;
        pending.deletion_date = Some(Zoned::now());
        let cancelled = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let vault = ScriptedVault::serve(vec![
            // The transit key still exists.
            ScriptedResponse::ok(transit_key_read_data("wired-key")),
            // Versioned read finds a due pending-deletion record, but the
            // tombstone write loses the check-and-set race to a cancel.
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(metadata_read_data(&pending)),
            ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE),
            // The re-read sees the cancelled (Enabled) record: back off.
            ScriptedResponse::ok(kv2_metadata_read_data(2)),
            ScriptedResponse::ok(metadata_read_data(&cancelled)),
        ])
        .await;
        let config = KmsConfig::vault_transit(
            url::Url::parse(&vault.address).expect("scripted vault address should parse"),
            "scripted-token".to_string(),
        )
        .with_insecure_development_defaults();
        let backend = VaultTransitKmsBackend::new(config)
            .await
            .expect("vault transit backend should build");

        let now = Zoned::now() + Duration::from_secs(3600);
        let outcome = backend
            .remove_expired_key("wired-key", &now)
            .await
            .expect("losing the tombstone race to a cancel must back off cleanly");
        assert_eq!(outcome, ExpiredKeyRemoval::StateChanged);

        let requests = vault.requests();
        assert_eq!(requests.len(), 6, "no delete may follow a lost tombstone race: {requests:?}");
        assert!(
            !requests
                .iter()
                .any(|line| line.contains("/transit/keys/wired-key/config") || line.starts_with("DELETE ")),
            "the sweep must not touch the transit key after backing off: {requests:?}"
        );
    }

    /// The forward half of the rotation contract on the Transit path: a data key
    /// generated before a rotation must still be decryptable after it.
    ///
    /// Transit key material never leaves Vault, so an offline responder cannot
    /// prove the cryptographic round trip — `test_transit_old_ciphertext_decrypts_after_rotate`
    /// keeps that against a live Vault. What this pins is the wiring the client
    /// owns and could regress on its own: the historical `vault:v1:` ciphertext
    /// is forwarded to Vault byte for byte after the rotation bumped the
    /// recorded version, and nothing on the decrypt path gates on that version.
    #[tokio::test]
    async fn wired_transit_pre_rotation_data_key_is_decrypted_unchanged() {
        const CIPHERTEXT_V1: &str = "vault:v1:scripted-pre-rotation";
        const RECOVERED_DEK: [u8; 32] = [0x37u8; 32];
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            // generate_data_key: state gate reads the metadata record, then the
            // transit encrypt returns first-version ciphertext.
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": CIPHERTEXT_V1 })),
            // rotate: the state gate hits the metadata cache, the rotation
            // commits, and the versioned read+write records the version bump.
            ScriptedResponse::ok(serde_json::json!({})),
            ScriptedResponse::ok(kv2_metadata_read_data(1)),
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(kv2_write_ack()),
            // decrypt of the pre-rotation envelope; Vault owns the transit
            // crypto, so the recovered material is the responder's to hand back.
            ScriptedResponse::ok(serde_json::json!({ "plaintext": BASE64.encode(RECOVERED_DEK) })),
        ])
        .await;

        let data_key = client
            .generate_data_key(
                &GenerateKeyRequest {
                    master_key_id: "wired-key".to_string(),
                    key_spec: "AES_256".to_string(),
                    key_length: Some(32),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("generate_data_key must produce an envelope");
        let envelope: DataKeyEnvelope = serde_json::from_slice(&data_key.ciphertext).expect("envelope must parse");
        assert_eq!(envelope.encrypted_key, CIPHERTEXT_V1.as_bytes());

        let rotated = client.rotate_key("wired-key", None).await.expect("rotation must commit");
        assert_eq!(rotated.version, 2, "the rotation must record the version bump");

        let (plaintext, opened_by) = client
            .decrypt(
                &DecryptRequest {
                    ciphertext: data_key.ciphertext.clone(),
                    encryption_context: HashMap::new(),
                    grant_tokens: Vec::new(),
                },
                None,
            )
            .await
            .expect("a pre-rotation data key must stay decryptable");
        assert_eq!(
            plaintext,
            RECOVERED_DEK.to_vec(),
            "the decrypt must hand back the recovered material, not merely avoid an error"
        );
        assert_eq!(opened_by, "wired-key", "decrypt must report the master key that opened the envelope");

        let requests = vault.requests();
        assert_eq!(requests.len(), 7, "{requests:?}");
        assert_eq!(requests[6], "POST /v1/transit/decrypt/wired-key", "{requests:?}");

        // The version the rotation recorded, and the ciphertext the decrypt sent:
        // the client must forward the historical version verbatim instead of
        // re-stamping it to (or pinning the request at) the current one.
        let bodies = vault.request_bodies();
        let recorded: serde_json::Value = serde_json::from_str(&bodies[5]).expect("metadata write body must be JSON");
        assert_eq!(recorded["data"]["current_version"], serde_json::json!(2), "{recorded}");
        let decrypt_body: serde_json::Value = serde_json::from_str(&bodies[6]).expect("decrypt body must be JSON");
        assert_eq!(
            decrypt_body["ciphertext"],
            serde_json::json!(CIPHERTEXT_V1),
            "the pre-rotation ciphertext must reach Vault unchanged: {decrypt_body}"
        );
    }

    /// Transit read-key payload for a key that has been rotated up to `latest`.
    fn transit_key_read_data_up_to(key_id: &str, latest: u32) -> serde_json::Value {
        let mut response: serde_json::Value = transit_key_read_data(key_id);
        let keys: serde_json::Map<String, serde_json::Value> = (1..=latest)
            .map(|version| (version.to_string(), serde_json::json!(1_700_000_000_u64 + u64::from(version))))
            .collect();
        response["keys"] = serde_json::Value::Object(keys);
        response
    }

    fn wired_key_request(context: HashMap<String, String>) -> GenerateKeyRequest {
        GenerateKeyRequest {
            master_key_id: "wired-key".to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: context,
            grant_tokens: Vec::new(),
        }
    }

    #[test]
    fn transit_ciphertext_version_reads_only_a_well_formed_prefix() {
        assert_eq!(transit_ciphertext_version("vault:v1:abc"), Some(1));
        assert_eq!(transit_ciphertext_version("vault:v27:abc"), Some(27));
        // Anything else leaves the version unknown rather than guessing one; an
        // invented version is what would let a still-referenced key version be
        // reported as retired.
        assert_eq!(transit_ciphertext_version("vault:v:abc"), None);
        assert_eq!(transit_ciphertext_version("vault:vx:abc"), None);
        assert_eq!(transit_ciphertext_version("vault:v1"), None);
        assert_eq!(transit_ciphertext_version("v1:abc"), None);
        assert_eq!(transit_ciphertext_version(""), None);
    }

    /// The property that justifies having a Transit-specific rewrap at all: the
    /// data key is re-encrypted by Vault, so no `transit/decrypt` is issued and
    /// no plaintext data key ever exists inside this process.
    #[tokio::test]
    async fn wired_transit_rewrap_uses_the_native_endpoint_and_never_decrypts() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            // generate_data_key: metadata state gate, then the transit encrypt.
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
            // rewrap: the state gate hits the metadata cache, so the only call
            // is the native rewrap.
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v3:rewrapped" })),
        ])
        .await;

        let data_key = client
            .generate_data_key(&wired_key_request(HashMap::new()), None)
            .await
            .expect("generate_data_key must produce an envelope");
        let original: DataKeyEnvelope = serde_json::from_slice(&data_key.ciphertext).expect("envelope must parse");

        let response = client
            .rewrap_data_key(&RewrapDataKeyRequest {
                ciphertext: data_key.ciphertext.clone(),
                encryption_context: HashMap::new(),
            })
            .await
            .expect("rewrap must move the ciphertext onto the latest version");

        assert!(response.rewrapped);
        assert_eq!(response.source_key_version, Some(1));
        assert_eq!(response.destination_key_version, Some(3));

        let rewrapped: DataKeyEnvelope = serde_json::from_slice(&response.ciphertext).expect("rewrapped envelope must parse");
        assert_eq!(rewrapped.encrypted_key, b"vault:v3:rewrapped".to_vec());
        assert_eq!(
            rewrapped.master_key_version, None,
            "Transit ciphertext self-describes its version, so the envelope field must stay absent"
        );
        assert_eq!(rewrapped.key_id, original.key_id);
        assert_eq!(rewrapped.created_at, original.created_at);
        assert_eq!(rewrapped.encryption_context, original.encryption_context);

        let requests = vault.requests();
        assert!(
            requests.contains(&"POST /v1/transit/rewrap/wired-key".to_string()),
            "the native rewrap endpoint must be used: {requests:?}"
        );
        assert!(
            !requests.iter().any(|request| request.contains("/transit/decrypt/")),
            "no decrypt may be issued: the plaintext data key must never enter this process: {requests:?}"
        );

        // The ciphertext Vault was asked to rewrap is the one that was stored,
        // byte for byte.
        let bodies = vault.request_bodies();
        let rewrap_index = requests
            .iter()
            .position(|request| request == "POST /v1/transit/rewrap/wired-key")
            .expect("the rewrap request must be recorded");
        let body: serde_json::Value = serde_json::from_str(&bodies[rewrap_index]).expect("rewrap body must be JSON");
        assert_eq!(body["ciphertext"], serde_json::json!("vault:v1:scripted"), "{body}");
    }

    /// Vault re-encrypts unconditionally, so an already-latest ciphertext comes
    /// back different but no newer. That must report as "nothing to persist", or
    /// every sweep pass would rewrite every object's metadata forever.
    #[tokio::test]
    async fn wired_transit_rewrap_of_a_current_ciphertext_is_a_no_op() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (_vault, client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
            // Same version back, different bytes.
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:re-encrypted" })),
        ])
        .await;

        let data_key = client
            .generate_data_key(&wired_key_request(HashMap::new()), None)
            .await
            .expect("generate_data_key must produce an envelope");

        let response = client
            .rewrap_data_key(&RewrapDataKeyRequest {
                ciphertext: data_key.ciphertext.clone(),
                encryption_context: HashMap::new(),
            })
            .await
            .expect("rewrap must succeed");

        assert!(!response.rewrapped, "a ciphertext already on the latest version is a no-op");
        assert_eq!(
            response.ciphertext, data_key.ciphertext,
            "a no-op must hand the stored envelope back unchanged, not Vault's fresh re-encryption"
        );
        assert_eq!(response.source_key_version, Some(1));
        assert_eq!(response.destination_key_version, Some(1));
    }

    /// Vault's `transit/rewrap` endpoint takes no `associated_data` parameter,
    /// and this backend binds the encryption context as exactly that. The only
    /// remaining route would decrypt the data key inside RustFS, so the request
    /// is refused rather than silently downgraded — and refused without any call
    /// to Vault at all.
    #[tokio::test]
    async fn wired_transit_rewrap_refuses_an_aad_bound_envelope() {
        let context = HashMap::from([("bucket".to_string(), "photos/cat.jpg".to_string())]);
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
            // Only the read-only accessor below is allowed to consume this.
            ScriptedResponse::ok(transit_key_read_data_up_to("wired-key", 2)),
        ])
        .await;

        let data_key = client
            .generate_data_key(&wired_key_request(context.clone()), None)
            .await
            .expect("generate_data_key must produce an envelope");

        let error = client
            .rewrap_data_key(&RewrapDataKeyRequest {
                ciphertext: data_key.ciphertext.clone(),
                encryption_context: context.clone(),
            })
            .await
            .expect_err("an AAD-bound envelope must not be rewrapped by decrypting it here");
        assert!(
            matches!(&error, KmsError::RewrapWouldExposePlaintext { key_id, .. } if key_id == "wired-key"),
            "got {error:?}"
        );

        // The stuck envelope must still be countable, or an inventory could not
        // report how much of the key version is unmigratable.
        let described = client
            .describe_data_key_wrapping(&DescribeDataKeyWrappingRequest {
                ciphertext: data_key.ciphertext.clone(),
                encryption_context: context,
            })
            .await
            .expect("describing the wrapping must work even when rewrapping it cannot");
        assert_eq!(described.key_version, Some(1));
        assert_eq!(described.current_key_version, Some(2));
        assert!(!described.is_current);

        let requests = vault.requests();
        assert!(
            !requests.iter().any(|request| request.contains("/transit/rewrap/")),
            "the refusal must happen before any rewrap call: {requests:?}"
        );
        assert!(
            !requests.iter().any(|request| request.contains("/transit/decrypt/")),
            "and above all before any decrypt: {requests:?}"
        );
    }

    /// The current version comes from Vault's own key record rather than from
    /// the RustFS metadata counter, which only advances on rotations this
    /// process performed.
    #[tokio::test]
    async fn wired_transit_describe_wrapping_reads_vaults_latest_version() {
        let metadata = TransitKeyMetadata::from_create_request(&CreateKeyRequest::default());
        assert_eq!(metadata.current_version, 1, "the RustFS counter still says version 1");

        let (vault, client) = scripted_client(vec![
            ScriptedResponse::ok(metadata_read_data(&metadata)),
            ScriptedResponse::ok(serde_json::json!({ "ciphertext": "vault:v1:scripted" })),
            // Vault has been rotated behind RustFS's back.
            ScriptedResponse::ok(transit_key_read_data_up_to("wired-key", 4)),
        ])
        .await;

        let data_key = client
            .generate_data_key(&wired_key_request(HashMap::new()), None)
            .await
            .expect("generate_data_key must produce an envelope");

        let described = client
            .describe_data_key_wrapping(&DescribeDataKeyWrappingRequest {
                ciphertext: data_key.ciphertext.clone(),
                encryption_context: HashMap::new(),
            })
            .await
            .expect("describing the wrapping must succeed");

        assert_eq!(described.key_id, "wired-key");
        assert_eq!(described.key_version, Some(1));
        assert_eq!(
            described.current_key_version,
            Some(4),
            "the latest version must come from Vault, not from the RustFS metadata counter"
        );
        assert!(!described.is_current);

        let requests = vault.requests();
        assert!(
            requests.contains(&"GET /v1/transit/keys/wired-key".to_string()),
            "the latest version must be read from the transit key record: {requests:?}"
        );
    }
}
