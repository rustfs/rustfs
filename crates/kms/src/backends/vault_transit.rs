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

use crate::backends::vault_credentials::{
    CredentialTaskHandle, VaultClientHandle, VaultConnectionSettings, VaultCredentialPolicy, VaultCredentialProvider,
    token_source_for,
};
use crate::backends::{
    BackendCapabilities, ExpiredKeyRemoval, KmsBackend, StateGatedOperation, ensure_key_state_permits,
    ensure_tag_keys_are_mutable,
};
use crate::config::{KmsConfig, VaultTransitConfig};
use crate::encryption::{DataKeyEnvelope, generate_key_material};
use crate::error::{KmsError, Result};
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
use tracing::info;
use vaultrs::{
    api::kv2::requests::SetSecretRequestOptions,
    api::transit::{
        KeyType,
        requests::{
            CreateKeyRequestBuilder, DecryptDataRequestBuilder, EncryptDataRequestBuilder, UpdateKeyConfigurationRequestBuilder,
        },
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
/// process restart". Matches the manager-level `KmsCache` TTL.
const METADATA_CACHE_TTL: Duration = Duration::from_secs(300);

/// Capacity bound on the metadata cache so an unbounded key namespace cannot
/// grow process memory without limit.
const METADATA_CACHE_CAPACITY: u64 = 1024;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        };
        let source = token_source_for(&config.auth_method, &settings)?;
        let policy = VaultCredentialPolicy::from_kms_config(kms_config, &config.auth_method);
        let credentials = Arc::new(VaultCredentialProvider::new(settings, source, policy).await?);

        Ok(Self {
            credentials,
            metadata_kv_mount: config.metadata_kv_mount.clone(),
            metadata_key_prefix: config.metadata_key_prefix.clone(),
            config,
            metadata_cache: Cache::builder()
                .max_capacity(METADATA_CACHE_CAPACITY)
                .time_to_live(METADATA_CACHE_TTL)
                .build(),
            retry: RetryPolicy::from_config(kms_config),
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
                Err(e) => Err(AttemptError::from_vaultrs(e, |e| {
                    KmsError::backend_error(format!("Failed to read transit key metadata from Vault KV: {e}"))
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
        let ciphertext = match self
            .transit_encrypt(&request.key_id, &request.plaintext, &request.encryption_context)
            .await
        {
            Ok(ciphertext) => ciphertext,
            Err(error) => {
                self.invalidate_metadata_on_state_error(&request.key_id, &error).await;
                return Err(error);
            }
        };

        Ok(EncryptResponse {
            ciphertext: ciphertext.into_bytes(),
            key_id: request.key_id.clone(),
            key_version: metadata.current_version,
            algorithm: "vault-transit".to_string(),
        })
    }

    pub(crate) async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
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
            Ok(plaintext) => Ok(plaintext),
            Err(error) => {
                self.invalidate_metadata_on_state_error(&envelope.master_key_id, &error).await;
                Err(error)
            }
        }
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
        let all_keys = self
            .run("vault_transit_list_keys", OpClass::ReadIdempotent, move || async move {
                let vault = self.vault().map_err(AttemptError::fatal)?;
                key::list(&vault.client, &self.config.mount_path).await.map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| KmsError::backend_error(format!("Failed to list Vault Transit keys: {e}")))
                })
            })
            .await?
            .keys;

        let mut filtered = Vec::new();
        for key_id in all_keys {
            let key_info = self.key_info(&key_id).await?;
            let usage_matches = request.usage_filter.as_ref().is_none_or(|usage| usage == &key_info.usage);
            let status_matches = request.status_filter.as_ref().is_none_or(|status| status == &key_info.status);
            if usage_matches && status_matches {
                filtered.push(key_info);
            }
        }

        let start_idx = request
            .marker
            .as_ref()
            .and_then(|marker| filtered.iter().position(|info| &info.key_id == marker))
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let limit = request.limit.unwrap_or(100) as usize;
        let end_idx = std::cmp::min(start_idx + limit, filtered.len());
        let keys = filtered[start_idx..end_idx].to_vec();
        let next_marker = if end_idx < filtered.len() {
            Some(filtered[end_idx - 1].key_id.clone())
        } else {
            None
        };

        Ok(ListKeysResponse {
            keys,
            next_marker,
            truncated: end_idx < filtered.len(),
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
            key::list(&vault.client, &self.config.mount_path)
                .await
                .map(|_| ())
                .map_err(|e| {
                    AttemptError::from_vaultrs(e, |e| KmsError::backend_error(format!("Vault Transit health check failed: {e}")))
                })
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
            crate::config::BackendConfig::Local(_) | crate::config::BackendConfig::Static(_) => {
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
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)?;
        let plaintext = self.client.decrypt(&request, None).await?;
        Ok(DecryptResponse {
            plaintext,
            key_id: envelope.master_key_id,
            encryption_algorithm: Some("vault-transit".to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let generate_request = GenerateKeyRequest {
            master_key_id: request.key_id.clone(),
            key_spec: request.key_spec.as_str().to_string(),
            key_length: Some(request.key_spec.key_size() as u32),
            encryption_context: request.encryption_context,
            grant_tokens: Vec::new(),
        };

        let data_key = self.client.generate_data_key(&generate_request, None).await?;
        let plaintext_key = data_key.plaintext.clone().unwrap_or_default();
        let ciphertext_blob = data_key.ciphertext.clone();
        Ok(GenerateDataKeyResponse {
            key_id: request.key_id,
            plaintext_key,
            ciphertext_blob,
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

            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(KmsError::invalid_parameter("pending_window_in_days must be between 7 and 30"));
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
        // deletion once a key is pending deletion.
        BackendCapabilities::minimal()
            .with_rotate(true)
            .with_enable_disable(true)
            .with_schedule_deletion(true)
            .with_versioning(true)
            .with_physical_delete(true)
            .with_update_key_metadata(true)
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
        assert_eq!(response.ciphertext, b"vault:v1:scripted".to_vec());

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
            let plaintext = client
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
}
