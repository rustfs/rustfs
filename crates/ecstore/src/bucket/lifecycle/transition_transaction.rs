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

use std::{future::Future, sync::Arc, time::Duration};

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::bucket::lifecycle::config_boundary;
use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use crate::bucket::lifecycle::tier_sweeper::delete_object_from_remote_tier_idempotent_with_manager_and_identity;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::{list::ListOperations as _, object::ObjectOperations as _};
use crate::store::ECStore;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY: &str = "lifecycle_transition_transaction_recovery";
pub const DEFAULT_TRANSITION_TRANSACTION_RECOVERY_LIMIT: usize = 1_000;
const TRANSITION_TRANSACTION_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const TRANSITION_TRANSACTION_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);
pub const TRANSITION_TRANSACTION_SCHEMA: &str = "rustfs-transition-transaction-v1";
pub const TRANSITION_TRANSACTION_PREFIX: &str = "ilm/transition-transactions";
pub const TRANSITION_TRANSACTION_RECORD_PREFIX: &str = "ilm/transition-transactions/records";
pub const MAX_TRANSITION_TRANSACTION_SIZE: usize = 64 * 1024;

pub type Result<T> = std::result::Result<T, TransitionTransactionError>;

#[derive(Debug, thiserror::Error)]
pub enum TransitionTransactionError {
    #[error("transition transaction already exists")]
    AlreadyExists,
    #[error("transition transaction is not found")]
    NotFound,
    #[error("transition transaction is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("transition transaction schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("transition transaction checksum mismatch")]
    ChecksumMismatch,
    #[error("transition transaction owner or revision fence is stale")]
    Fenced,
    #[error("invalid transition transaction state change from {from:?} to {to:?}")]
    InvalidStateChange {
        from: TransitionTransactionState,
        to: TransitionTransactionState,
    },
    #[error("transition transaction json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionTransactionState {
    UploadStarted,
    UploadOutcomeUnknown,
    AbortedNoRemote,
    Uploaded,
    LocalCommitStarted,
    Committed,
    CleanupPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionRemoteVersionKind {
    Unknown,
    Unversioned,
    Versioned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionRemoteVersion {
    pub kind: TransitionRemoteVersionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl TransitionRemoteVersion {
    pub fn known_from_put_response(version_id: impl Into<String>) -> Self {
        let version_id = version_id.into();
        if version_id.is_empty() || Uuid::parse_str(&version_id).is_ok_and(|parsed| parsed.is_nil()) {
            Self::unversioned()
        } else {
            Self::versioned(version_id)
        }
    }

    pub fn unknown() -> Self {
        Self {
            kind: TransitionRemoteVersionKind::Unknown,
            version_id: None,
        }
    }

    pub fn unversioned() -> Self {
        Self {
            kind: TransitionRemoteVersionKind::Unversioned,
            version_id: None,
        }
    }

    pub fn versioned(version_id: impl Into<String>) -> Self {
        Self {
            kind: TransitionRemoteVersionKind::Versioned,
            version_id: Some(version_id.into()),
        }
    }

    pub fn is_unknown(&self) -> bool {
        self.kind == TransitionRemoteVersionKind::Unknown
    }

    pub fn tier_delete_version_id(&self) -> Option<&str> {
        match self.kind {
            TransitionRemoteVersionKind::Unknown | TransitionRemoteVersionKind::Unversioned => None,
            TransitionRemoteVersionKind::Versioned => self.version_id.as_deref(),
        }
    }

    fn validate(&self) -> Result<()> {
        match (&self.kind, self.version_id.as_deref()) {
            (TransitionRemoteVersionKind::Unknown | TransitionRemoteVersionKind::Unversioned, None) => Ok(()),
            (TransitionRemoteVersionKind::Unknown | TransitionRemoteVersionKind::Unversioned, Some(_)) => Err(
                TransitionTransactionError::Corrupt("remote version must be absent for non-versioned state"),
            ),
            (TransitionRemoteVersionKind::Versioned, Some(version_id)) => {
                if version_id.is_empty() {
                    return Err(TransitionTransactionError::Corrupt("versioned remote version is empty"));
                }
                if Uuid::parse_str(version_id).is_ok_and(|parsed| parsed.is_nil()) {
                    return Err(TransitionTransactionError::Corrupt("versioned remote version is nil uuid"));
                }
                Ok(())
            }
            (TransitionRemoteVersionKind::Versioned, None) => {
                Err(TransitionTransactionError::Corrupt("versioned remote version is missing"))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionSourceIdentity {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<Uuid>,
    pub data_dir: Uuid,
    pub mod_time_unix_nanos: i64,
    pub size: i64,
    pub etag: String,
    pub version_mode: TransitionSourceVersionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionSourceVersionMode {
    Unversioned,
    Versioned,
    VersionSuspended,
}

impl TransitionSourceIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() {
            return Err(TransitionTransactionError::Corrupt("source bucket is empty"));
        }
        if self.object.is_empty() {
            return Err(TransitionTransactionError::Corrupt("source object is empty"));
        }
        if self.data_dir.is_nil() {
            return Err(TransitionTransactionError::Corrupt("source data_dir is nil"));
        }
        if self.version_id.is_some_and(|version_id| version_id.is_nil()) {
            return Err(TransitionTransactionError::Corrupt("source version_id is nil"));
        }
        if self.version_mode == TransitionSourceVersionMode::Versioned && self.version_id.is_none() {
            return Err(TransitionTransactionError::Corrupt("versioned source is missing version_id"));
        }
        if matches!(
            self.version_mode,
            TransitionSourceVersionMode::Unversioned | TransitionSourceVersionMode::VersionSuspended
        ) && self.version_id.is_some()
        {
            return Err(TransitionTransactionError::Corrupt("non-versioned source mode must not carry version_id"));
        }
        if self.size < 0 {
            return Err(TransitionTransactionError::Corrupt("source size is negative"));
        }
        if self.etag.is_empty() {
            return Err(TransitionTransactionError::Corrupt("source etag is empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionTransactionFence {
    pub revision: u64,
    pub owner_epoch: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionTransaction {
    pub deployment_id: Uuid,
    pub transaction_id: Uuid,
    pub revision: u64,
    pub owner_epoch: Uuid,
    pub write_id: Uuid,
    pub source: TransitionSourceIdentity,
    pub tier_name: String,
    pub backend_fingerprint: [u8; 32],
    pub remote_object: String,
    pub remote_version: TransitionRemoteVersion,
    pub state: TransitionTransactionState,
    pub not_after_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTransactionInit {
    pub deployment_id: Uuid,
    pub transaction_id: Uuid,
    pub owner_epoch: Uuid,
    pub write_id: Uuid,
    pub source: TransitionSourceIdentity,
    pub tier_name: String,
    pub backend_fingerprint: [u8; 32],
    pub not_after_unix_nanos: i64,
}

impl TransitionTransaction {
    pub fn new(init: TransitionTransactionInit) -> Result<Self> {
        let remote_object =
            canonical_transition_remote_object(init.deployment_id, &init.source.bucket, init.transaction_id, init.write_id)?;
        let transaction = Self {
            deployment_id: init.deployment_id,
            transaction_id: init.transaction_id,
            revision: 1,
            owner_epoch: init.owner_epoch,
            write_id: init.write_id,
            source: init.source,
            tier_name: init.tier_name,
            backend_fingerprint: init.backend_fingerprint,
            remote_object,
            remote_version: TransitionRemoteVersion::unknown(),
            state: TransitionTransactionState::UploadStarted,
            not_after_unix_nanos: init.not_after_unix_nanos,
        };
        transaction.validate()?;
        Ok(transaction)
    }

    pub fn fence(&self) -> TransitionTransactionFence {
        TransitionTransactionFence {
            revision: self.revision,
            owner_epoch: self.owner_epoch,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.deployment_id.is_nil() {
            return Err(TransitionTransactionError::Corrupt("deployment_id is nil"));
        }
        if self.transaction_id.is_nil() {
            return Err(TransitionTransactionError::Corrupt("transaction_id is nil"));
        }
        if self.revision == 0 {
            return Err(TransitionTransactionError::Corrupt("revision is zero"));
        }
        if self.owner_epoch.is_nil() {
            return Err(TransitionTransactionError::Corrupt("owner_epoch is nil"));
        }
        if self.write_id.is_nil() {
            return Err(TransitionTransactionError::Corrupt("write_id is nil"));
        }
        self.source.validate()?;
        if self.tier_name.is_empty() {
            return Err(TransitionTransactionError::Corrupt("tier name is empty"));
        }
        if self.remote_object
            != canonical_transition_remote_object(self.deployment_id, &self.source.bucket, self.transaction_id, self.write_id)?
        {
            return Err(TransitionTransactionError::Corrupt("remote object is not canonical"));
        }
        self.remote_version.validate()?;
        if state_requires_known_remote_version(self.state) && self.remote_version.is_unknown() {
            return Err(TransitionTransactionError::Corrupt("state requires known remote version"));
        }
        if matches!(
            self.state,
            TransitionTransactionState::UploadStarted
                | TransitionTransactionState::UploadOutcomeUnknown
                | TransitionTransactionState::AbortedNoRemote
        ) && !self.remote_version.is_unknown()
        {
            return Err(TransitionTransactionError::Corrupt(
                "pre-upload-completion state must not carry a known remote version",
            ));
        }
        Ok(())
    }

    pub fn advance(
        &mut self,
        fence: TransitionTransactionFence,
        next: TransitionTransactionState,
        remote_version: Option<TransitionRemoteVersion>,
    ) -> Result<TransitionTransactionFence> {
        self.check_fence(fence)?;
        if !state_change_allowed(self.state, next) {
            return Err(TransitionTransactionError::InvalidStateChange {
                from: self.state,
                to: next,
            });
        }

        match next {
            TransitionTransactionState::Uploaded => {
                let remote_version =
                    remote_version.ok_or(TransitionTransactionError::Corrupt("uploaded state requires remote version"))?;
                if remote_version.is_unknown() {
                    return Err(TransitionTransactionError::Corrupt("uploaded state requires known remote version"));
                }
                self.remote_version = remote_version;
            }
            TransitionTransactionState::AbortedNoRemote => {
                if remote_version.is_some() {
                    return Err(TransitionTransactionError::Corrupt(
                        "aborted no remote state must not carry remote version",
                    ));
                }
                self.remote_version = TransitionRemoteVersion::unknown();
            }
            TransitionTransactionState::UploadOutcomeUnknown => {
                if remote_version.is_some() {
                    return Err(TransitionTransactionError::Corrupt(
                        "unknown upload outcome state must not carry remote version",
                    ));
                }
                self.remote_version = TransitionRemoteVersion::unknown();
            }
            TransitionTransactionState::LocalCommitStarted | TransitionTransactionState::Committed => {
                if let Some(remote_version) = remote_version
                    && remote_version != self.remote_version
                {
                    return Err(TransitionTransactionError::Corrupt("remote version changed after upload"));
                }
                if self.remote_version.is_unknown() {
                    return Err(TransitionTransactionError::Corrupt("local commit requires known remote version"));
                }
            }
            TransitionTransactionState::CleanupPending | TransitionTransactionState::UploadStarted => {
                return Err(TransitionTransactionError::InvalidStateChange {
                    from: self.state,
                    to: next,
                });
            }
        }

        self.state = next;
        self.bump_revision()?;
        self.validate()?;
        Ok(self.fence())
    }

    pub fn mark_cleanup_pending(
        &mut self,
        fence: TransitionTransactionFence,
        proof: TransitionCleanupProof,
    ) -> Result<TransitionTransactionFence> {
        self.check_fence(fence)?;
        self.validate_cleanup_proof(&proof)?;

        match (&self.state, &proof.decision) {
            (TransitionTransactionState::Uploaded, TransitionCleanupDecision::UploadAbortedBeforeLocalCommit)
            | (
                TransitionTransactionState::UploadOutcomeUnknown,
                TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation,
            ) => {}
            (
                TransitionTransactionState::LocalCommitStarted,
                TransitionCleanupDecision::SourceReconciledUnchanged { observed_source },
            ) if observed_source == &self.source => {}
            _ => {
                return Err(TransitionTransactionError::InvalidStateChange {
                    from: self.state,
                    to: TransitionTransactionState::CleanupPending,
                });
            }
        }

        self.state = TransitionTransactionState::CleanupPending;
        self.remote_version = proof.remote_version;
        self.bump_revision()?;
        self.validate()?;
        Ok(self.fence())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let transaction_bytes = serde_json::to_vec(self)?;
        let content_sha256 = hex_sha256(&transaction_bytes, ToOwned::to_owned);
        let persisted = PersistedTransitionTransaction {
            schema: TRANSITION_TRANSACTION_SCHEMA.to_string(),
            content_sha256,
            transaction: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_TRANSITION_TRANSACTION_SIZE {
            return Err(TransitionTransactionError::Corrupt("encoded transaction exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_transaction_id: Uuid, data: &[u8]) -> Result<Self> {
        if data.len() > MAX_TRANSITION_TRANSACTION_SIZE {
            return Err(TransitionTransactionError::Corrupt("encoded transaction exceeds maximum size"));
        }
        let persisted: PersistedTransitionTransaction = serde_json::from_slice(data)?;
        if persisted.schema != TRANSITION_TRANSACTION_SCHEMA {
            return Err(TransitionTransactionError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(TransitionTransactionError::Corrupt("content checksum is not a sha256 checksum"));
        }
        let transaction_bytes = serde_json::to_vec(&persisted.transaction)?;
        let actual_checksum = hex_sha256(&transaction_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(TransitionTransactionError::ChecksumMismatch);
        }
        if persisted.transaction.transaction_id != expected_transaction_id {
            return Err(TransitionTransactionError::Corrupt("transaction_id does not match record key"));
        }
        persisted.transaction.validate()?;
        Ok(persisted.transaction)
    }

    fn check_fence(&self, fence: TransitionTransactionFence) -> Result<()> {
        if self.revision != fence.revision || self.owner_epoch != fence.owner_epoch {
            return Err(TransitionTransactionError::Fenced);
        }
        Ok(())
    }

    fn validate_cleanup_proof(&self, proof: &TransitionCleanupProof) -> Result<()> {
        if proof.transaction_id != self.transaction_id
            || proof.write_id != self.write_id
            || proof.remote_object != self.remote_object
            || proof.backend_fingerprint != self.backend_fingerprint
        {
            return Err(TransitionTransactionError::Fenced);
        }
        proof.remote_version.validate()?;
        if proof.remote_version.is_unknown() {
            return Err(TransitionTransactionError::Corrupt("cleanup proof requires known remote version"));
        }
        if state_requires_known_remote_version(self.state) && proof.remote_version != self.remote_version {
            return Err(TransitionTransactionError::Fenced);
        }
        Ok(())
    }

    fn bump_revision(&mut self) -> Result<()> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(TransitionTransactionError::Corrupt("revision overflow"))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionCleanupProof {
    pub transaction_id: Uuid,
    pub write_id: Uuid,
    pub remote_object: String,
    pub remote_version: TransitionRemoteVersion,
    pub backend_fingerprint: [u8; 32],
    pub decision: TransitionCleanupDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransitionCleanupDecision {
    UploadAbortedBeforeLocalCommit,
    RemoteVersionRecoveredAfterCancellation,
    SourceReconciledUnchanged { observed_source: TransitionSourceIdentity },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedTransitionTransaction {
    schema: String,
    content_sha256: String,
    transaction: TransitionTransaction,
}

pub fn canonical_transition_remote_object(
    deployment_id: Uuid,
    source_bucket: &str,
    transaction_id: Uuid,
    write_id: Uuid,
) -> Result<String> {
    if deployment_id.is_nil() {
        return Err(TransitionTransactionError::Corrupt("deployment_id is nil"));
    }
    if source_bucket.is_empty() {
        return Err(TransitionTransactionError::Corrupt("source bucket is empty"));
    }
    if transaction_id.is_nil() {
        return Err(TransitionTransactionError::Corrupt("transaction_id is nil"));
    }
    if write_id.is_nil() {
        return Err(TransitionTransactionError::Corrupt("write_id is nil"));
    }

    let bucket_scope = format!("{deployment_id}:{source_bucket}");
    let bucket_hash = hex_sha256(bucket_scope.as_bytes(), ToOwned::to_owned);
    let transaction_key = transaction_id.simple().to_string();
    let write_key = write_id.simple().to_string();
    Ok(format!(
        "{}/{}/{}/{}/{}/{}",
        TRANSITION_TRANSACTION_PREFIX,
        &bucket_hash[..16],
        &transaction_key[..2],
        &transaction_key[2..4],
        transaction_key,
        write_key
    ))
}

pub fn transition_transaction_record_object_name(transaction_id: Uuid) -> Result<String> {
    if transaction_id.is_nil() {
        return Err(TransitionTransactionError::Corrupt("transaction_id is nil"));
    }
    let transaction_key = transaction_id.simple().to_string();
    Ok(format!(
        "{}/{}/{}/{}.json",
        TRANSITION_TRANSACTION_RECORD_PREFIX,
        &transaction_key[..2],
        &transaction_key[2..4],
        transaction_key
    ))
}

pub(crate) async fn save_transition_transaction_record(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<()> {
    let object =
        transition_transaction_record_object_name(transaction.transaction_id).map_err(transition_transaction_store_error)?;
    let data = transaction.encode().map_err(transition_transaction_store_error)?;
    config_boundary::save_config(api, &object, data).await
}

pub(crate) async fn load_transition_transaction_record(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> EcstoreResult<TransitionTransaction> {
    let object = transition_transaction_record_object_name(transaction_id).map_err(transition_transaction_store_error)?;
    let data = config_boundary::read_config(api, &object).await?;
    TransitionTransaction::decode(transaction_id, &data).map_err(transition_transaction_store_error)
}

pub(crate) async fn delete_transition_transaction_record(api: Arc<ECStore>, transaction_id: Uuid) -> EcstoreResult<()> {
    let object = transition_transaction_record_object_name(transaction_id).map_err(transition_transaction_store_error)?;
    match config_boundary::delete_config(api, &object).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(err) => Err(err),
    }
}

fn transition_transaction_store_error(err: TransitionTransactionError) -> Error {
    Error::other(err)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTransactionRecoveryStats {
    pub scanned: usize,
    pub recovered: usize,
    pub retained: usize,
    pub failed: usize,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionTransactionRecoveryOutcome {
    RemoteCandidateDeleted,
    RecordDeleted,
    Retained,
}

fn transition_transaction_id_from_record_object_name(object: &str) -> Result<Uuid> {
    let prefix = format!("{TRANSITION_TRANSACTION_RECORD_PREFIX}/");
    let suffix = object
        .strip_prefix(&prefix)
        .ok_or(TransitionTransactionError::Corrupt("transaction record path has wrong prefix"))?;
    let file_name = suffix
        .rsplit('/')
        .next()
        .ok_or(TransitionTransactionError::Corrupt("transaction record path is incomplete"))?;
    let transaction_key = file_name
        .strip_suffix(".json")
        .ok_or(TransitionTransactionError::Corrupt("transaction record path has wrong suffix"))?;
    if transaction_key.len() != 32 || !transaction_key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TransitionTransactionError::Corrupt("transaction record path has invalid transaction id"));
    }
    Uuid::parse_str(transaction_key).map_err(|_| TransitionTransactionError::Corrupt("transaction record path has invalid uuid"))
}

pub async fn process_transition_transaction_record(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    transaction.validate().map_err(transition_transaction_store_error)?;
    match transaction.state {
        TransitionTransactionState::Uploaded => {
            delete_transition_remote_candidate(api.clone(), transaction).await?;
            delete_transition_transaction_record(api, transaction.transaction_id).await?;
            Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted)
        }
        TransitionTransactionState::CleanupPending => match local_commit_matches_transaction(api.clone(), transaction).await {
            Ok(true) => {
                delete_transition_transaction_record(api, transaction.transaction_id).await?;
                Ok(TransitionTransactionRecoveryOutcome::RecordDeleted)
            }
            Ok(false) => {
                delete_transition_remote_candidate(api.clone(), transaction).await?;
                delete_transition_transaction_record(api, transaction.transaction_id).await?;
                Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted)
            }
            Err(err) if transition_source_is_missing(&err) => {
                delete_transition_remote_candidate(api.clone(), transaction).await?;
                delete_transition_transaction_record(api, transaction.transaction_id).await?;
                Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted)
            }
            Err(err) => Err(err),
        },
        TransitionTransactionState::LocalCommitStarted => {
            match local_commit_matches_transaction(api.clone(), transaction).await {
                Ok(true) => {
                    delete_transition_transaction_record(api, transaction.transaction_id).await?;
                    Ok(TransitionTransactionRecoveryOutcome::RecordDeleted)
                }
                Ok(false) => Ok(TransitionTransactionRecoveryOutcome::Retained),
                Err(err) if transition_source_is_missing(&err) => Ok(TransitionTransactionRecoveryOutcome::Retained),
                Err(err) => Err(err),
            }
        }
        TransitionTransactionState::AbortedNoRemote | TransitionTransactionState::Committed => {
            delete_transition_transaction_record(api, transaction.transaction_id).await?;
            Ok(TransitionTransactionRecoveryOutcome::RecordDeleted)
        }
        TransitionTransactionState::UploadStarted | TransitionTransactionState::UploadOutcomeUnknown => {
            Ok(TransitionTransactionRecoveryOutcome::Retained)
        }
    }
}

fn transition_source_is_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::FileNotFound
            | Error::FileVersionNotFound
            | Error::ObjectNotFound(_, _)
            | Error::VersionNotFound(_, _, _)
            | Error::BucketNotFound(_)
    )
}

async fn local_commit_matches_transaction(api: Arc<ECStore>, transaction: &TransitionTransaction) -> EcstoreResult<bool> {
    let opts = ObjectOptions {
        version_id: transaction.source.version_id.map(|version_id| version_id.to_string()),
        versioned: transaction.source.version_mode == TransitionSourceVersionMode::Versioned,
        version_suspended: transaction.source.version_mode == TransitionSourceVersionMode::VersionSuspended,
        metadata_cache_safe: false,
        ..Default::default()
    };
    let object = api
        .get_object_info(&transaction.source.bucket, &transaction.source.object, &opts)
        .await?;
    let transitioned = &object.transitioned_object;
    Ok(transitioned.status == TRANSITION_COMPLETE
        && transitioned.name == transaction.remote_object
        && transitioned.tier == transaction.tier_name
        && transitioned.version_id == transaction.remote_version.tier_delete_version_id().unwrap_or_default())
}

async fn delete_transition_remote_candidate(api: Arc<ECStore>, transaction: &TransitionTransaction) -> EcstoreResult<()> {
    let version_id = transaction.remote_version.tier_delete_version_id().unwrap_or_default();
    let version_id_exact = transaction.remote_version.kind == TransitionRemoteVersionKind::Versioned;
    delete_object_from_remote_tier_idempotent_with_manager_and_identity(
        &transaction.remote_object,
        version_id,
        &transaction.tier_name,
        transaction.backend_fingerprint,
        &api.tier_config_mgr(),
        version_id_exact,
    )
    .await
    .map(|_| ())
    .map_err(Error::other)
}

pub async fn recover_transition_transaction_records(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> EcstoreResult<TransitionTransactionRecoveryStats> {
    if limit == 0 {
        return Err(Error::other("transition transaction recovery limit must be greater than zero"));
    }

    let list_limit = i32::try_from(limit).map_or(i32::MAX, |value| value);
    let list = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            TRANSITION_TRANSACTION_RECORD_PREFIX,
            marker.clone(),
            None,
            list_limit,
            false,
            None,
            false,
        )
        .await?;

    let mut stats = TransitionTransactionRecoveryStats {
        scanned: 0,
        recovered: 0,
        retained: 0,
        failed: 0,
        next_marker: list.next_continuation_token,
        truncated: list.is_truncated,
    };

    for object in list.objects {
        stats.scanned += 1;
        let transaction_id = match transition_transaction_id_from_record_object_name(&object.name) {
            Ok(transaction_id) => transaction_id,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    transaction_record = %object.name,
                    error = ?err,
                    "Failed to derive transition transaction id from record path"
                );
                continue;
            }
        };
        let transaction = match load_transition_transaction_record(api.clone(), transaction_id).await {
            Ok(transaction) => transaction,
            Err(Error::ConfigNotFound) => continue,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    transaction_record = %object.name,
                    transaction_id = %transaction_id,
                    error = ?err,
                    "Failed to load transition transaction record"
                );
                continue;
            }
        };

        match process_transition_transaction_record(api.clone(), &transaction).await {
            Ok(
                TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted
                | TransitionTransactionRecoveryOutcome::RecordDeleted,
            ) => {
                stats.recovered += 1;
            }
            Ok(TransitionTransactionRecoveryOutcome::Retained) => {
                stats.retained += 1;
                debug!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    transaction_record = %object.name,
                    transaction_id = %transaction.transaction_id,
                    state = ?transaction.state,
                    "Transition transaction recovery retained record for a later reconcile pass"
                );
            }
            Err(err) => {
                stats.failed += 1;
                debug!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    transaction_record = %object.name,
                    transaction_id = %transaction.transaction_id,
                    state = ?transaction.state,
                    error = ?err,
                    "Transition transaction recovery will retry later"
                );
            }
        }
    }

    Ok(stats)
}

pub async fn run_transition_transaction_recovery_loop(api: Arc<ECStore>, cancel_token: CancellationToken) {
    let mut interval = tokio::time::interval(TRANSITION_TRANSACTION_RECOVERY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut marker: Option<String> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {},
        }

        let recovery =
            recover_transition_transaction_records(api.clone(), DEFAULT_TRANSITION_TRANSACTION_RECOVERY_LIMIT, marker.clone());
        let Some(result) =
            await_transition_transaction_recovery(&cancel_token, TRANSITION_TRANSACTION_RECOVERY_TIMEOUT, recovery).await
        else {
            return;
        };
        match result {
            Ok(stats) => {
                marker = stats.next_marker;
                debug!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    scanned = stats.scanned,
                    recovered = stats.recovered,
                    retained = stats.retained,
                    failed = stats.failed,
                    truncated = stats.truncated,
                    next_marker = ?marker,
                    "Recovered transition transaction records"
                );
            }
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    next_marker = ?marker,
                    error = ?err,
                    "Failed to recover transition transaction records"
                );
            }
        }
    }
}

async fn await_transition_transaction_recovery<T, F>(
    cancel_token: &CancellationToken,
    timeout: Duration,
    recovery: F,
) -> Option<EcstoreResult<T>>
where
    F: Future<Output = EcstoreResult<T>>,
{
    tokio::select! {
        _ = cancel_token.cancelled() => None,
        result = tokio::time::timeout(timeout, recovery) => Some(match result {
            Ok(result) => result,
            Err(_) => Err(Error::other(format!(
                "transition transaction recovery timed out after {} seconds",
                timeout.as_secs()
            ))),
        }),
    }
}

fn state_change_allowed(from: TransitionTransactionState, to: TransitionTransactionState) -> bool {
    matches!(
        (from, to),
        (TransitionTransactionState::UploadStarted, TransitionTransactionState::Uploaded)
            | (
                TransitionTransactionState::UploadStarted,
                TransitionTransactionState::UploadOutcomeUnknown
            )
            | (TransitionTransactionState::UploadStarted, TransitionTransactionState::AbortedNoRemote)
            | (TransitionTransactionState::UploadOutcomeUnknown, TransitionTransactionState::Uploaded)
            | (TransitionTransactionState::Uploaded, TransitionTransactionState::LocalCommitStarted)
            | (TransitionTransactionState::LocalCommitStarted, TransitionTransactionState::Committed)
    )
}

fn state_requires_known_remote_version(state: TransitionTransactionState) -> bool {
    matches!(
        state,
        TransitionTransactionState::Uploaded
            | TransitionTransactionState::LocalCommitStarted
            | TransitionTransactionState::Committed
            | TransitionTransactionState::CleanupPending
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const BACKEND_FINGERPRINT: [u8; 32] = [7; 32];

    #[derive(Default)]
    struct MemoryTransactionStore {
        records: HashMap<Uuid, Vec<u8>>,
    }

    impl MemoryTransactionStore {
        fn create(&mut self, transaction: &TransitionTransaction) -> Result<()> {
            if self.records.contains_key(&transaction.transaction_id) {
                return Err(TransitionTransactionError::AlreadyExists);
            }
            self.records.insert(transaction.transaction_id, transaction.encode()?);
            Ok(())
        }

        fn load(&self, transaction_id: Uuid) -> Result<TransitionTransaction> {
            let data = self
                .records
                .get(&transaction_id)
                .ok_or(TransitionTransactionError::NotFound)?;
            TransitionTransaction::decode(transaction_id, data)
        }

        fn compare_save(
            &mut self,
            transaction_id: Uuid,
            fence: TransitionTransactionFence,
            next: TransitionTransactionState,
        ) -> Result<TransitionTransactionFence> {
            let mut transaction = self.load(transaction_id)?;
            let remote_version = (next == TransitionTransactionState::Uploaded)
                .then(|| TransitionRemoteVersion::versioned(Uuid::new_v4().to_string()));
            let next_fence = transaction.advance(fence, next, remote_version)?;
            self.records.insert(transaction_id, transaction.encode()?);
            Ok(next_fence)
        }

        fn compare_delete(&mut self, transaction_id: Uuid, fence: TransitionTransactionFence) -> Result<()> {
            let transaction = self.load(transaction_id)?;
            if transaction.fence() != fence {
                return Err(TransitionTransactionError::Fenced);
            }
            self.records.remove(&transaction_id);
            Ok(())
        }
    }

    fn source_identity(version_mode: TransitionSourceVersionMode) -> TransitionSourceIdentity {
        TransitionSourceIdentity {
            bucket: "source-bucket".to_string(),
            object: "object/key".to_string(),
            version_id: (version_mode == TransitionSourceVersionMode::Versioned).then(Uuid::new_v4),
            data_dir: Uuid::new_v4(),
            mod_time_unix_nanos: 1_770_000_000_000_000_000,
            size: 42,
            etag: "etag".to_string(),
            version_mode,
        }
    }

    fn new_transaction() -> TransitionTransaction {
        TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: Uuid::new_v4(),
            transaction_id: Uuid::new_v4(),
            owner_epoch: Uuid::new_v4(),
            write_id: Uuid::new_v4(),
            source: source_identity(TransitionSourceVersionMode::Versioned),
            tier_name: "warm-tier".to_string(),
            backend_fingerprint: BACKEND_FINGERPRINT,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("valid transaction should be created")
    }

    fn upload(transaction: &mut TransitionTransaction) -> TransitionTransactionFence {
        let fence = transaction.fence();
        transaction
            .advance(
                fence,
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(Uuid::new_v4().to_string())),
            )
            .expect("upload state change should succeed")
    }

    fn cleanup_proof(transaction: &TransitionTransaction, decision: TransitionCleanupDecision) -> TransitionCleanupProof {
        TransitionCleanupProof {
            transaction_id: transaction.transaction_id,
            write_id: transaction.write_id,
            remote_object: transaction.remote_object.clone(),
            remote_version: transaction.remote_version.clone(),
            backend_fingerprint: transaction.backend_fingerprint,
            decision,
        }
    }

    #[test]
    fn remote_version_distinguishes_unknown_unversioned_and_versioned() {
        assert_eq!(TransitionRemoteVersion::known_from_put_response("").tier_delete_version_id(), None);
        assert_eq!(
            TransitionRemoteVersion::known_from_put_response(Uuid::nil().to_string()).tier_delete_version_id(),
            None
        );

        let version_id = Uuid::new_v4().to_string();
        assert_eq!(
            TransitionRemoteVersion::known_from_put_response(version_id.clone()).tier_delete_version_id(),
            Some(version_id.as_str())
        );
        assert!(TransitionRemoteVersion::unknown().is_unknown());
    }

    #[test]
    fn transaction_round_trip_rejects_schema_checksum_unknown_fields_and_wrong_key() {
        let transaction = new_transaction();
        let encoded = transaction.encode().expect("transaction should encode");
        let decoded = TransitionTransaction::decode(transaction.transaction_id, &encoded).expect("transaction should decode");
        assert_eq!(decoded, transaction);

        let mut wrong_schema: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded transaction should be json");
        wrong_schema["schema"] = serde_json::Value::String("rustfs-transition-transaction-v2".to_string());
        let wrong_schema_bytes = serde_json::to_vec(&wrong_schema).expect("wrong schema transaction should encode");
        assert!(matches!(
            TransitionTransaction::decode(transaction.transaction_id, &wrong_schema_bytes),
            Err(TransitionTransactionError::UnsupportedSchema(_))
        ));

        let mut bad_checksum: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded transaction should be json");
        bad_checksum["content_sha256"] =
            serde_json::Value::String("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string());
        let bad_checksum_bytes = serde_json::to_vec(&bad_checksum).expect("bad checksum transaction should encode");
        assert!(matches!(
            TransitionTransaction::decode(transaction.transaction_id, &bad_checksum_bytes),
            Err(TransitionTransactionError::ChecksumMismatch)
        ));

        let mut unknown_field: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded transaction should be json");
        unknown_field["unexpected"] = serde_json::Value::Bool(true);
        let unknown_field_bytes = serde_json::to_vec(&unknown_field).expect("unknown field transaction should encode");
        assert!(matches!(
            TransitionTransaction::decode(transaction.transaction_id, &unknown_field_bytes),
            Err(TransitionTransactionError::Json(_))
        ));

        let mut nested_unknown_field: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded transaction should be json");
        nested_unknown_field["transaction"]["remote_version"]["unexpected"] = serde_json::Value::Bool(true);
        let nested_unknown_field_bytes =
            serde_json::to_vec(&nested_unknown_field).expect("nested unknown field transaction should encode");
        assert!(matches!(
            TransitionTransaction::decode(transaction.transaction_id, &nested_unknown_field_bytes),
            Err(TransitionTransactionError::Json(_))
        ));

        assert!(matches!(
            TransitionTransaction::decode(Uuid::new_v4(), &encoded),
            Err(TransitionTransactionError::Corrupt("transaction_id does not match record key"))
        ));
    }

    #[test]
    fn invalid_source_and_identity_boundaries_fail_closed() {
        assert!(matches!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::nil(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source: source_identity(TransitionSourceVersionMode::Versioned),
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: BACKEND_FINGERPRINT,
                not_after_unix_nanos: 1,
            }),
            Err(TransitionTransactionError::Corrupt("deployment_id is nil"))
        ));

        let mut source = source_identity(TransitionSourceVersionMode::Unversioned);
        source.version_id = Some(Uuid::nil());
        assert!(matches!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::new_v4(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source,
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: BACKEND_FINGERPRINT,
                not_after_unix_nanos: 1,
            }),
            Err(TransitionTransactionError::Corrupt("source version_id is nil"))
        ));

        let mut source = source_identity(TransitionSourceVersionMode::VersionSuspended);
        source.version_id = Some(Uuid::new_v4());
        assert!(matches!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::new_v4(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source,
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: BACKEND_FINGERPRINT,
                not_after_unix_nanos: 1,
            }),
            Err(TransitionTransactionError::Corrupt("non-versioned source mode must not carry version_id"))
        ));

        assert!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::new_v4(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source: source_identity(TransitionSourceVersionMode::VersionSuspended),
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: BACKEND_FINGERPRINT,
                not_after_unix_nanos: 1,
            })
            .is_ok()
        );
    }

    #[test]
    fn state_machine_requires_known_remote_version_and_rejects_commit_skip() {
        let mut transaction = new_transaction();
        let fence = transaction.fence();
        assert!(matches!(
            transaction.advance(fence, TransitionTransactionState::Committed, None),
            Err(TransitionTransactionError::InvalidStateChange {
                from: TransitionTransactionState::UploadStarted,
                to: TransitionTransactionState::Committed,
            })
        ));

        assert!(matches!(
            transaction.advance(fence, TransitionTransactionState::Uploaded, Some(TransitionRemoteVersion::unknown())),
            Err(TransitionTransactionError::Corrupt("uploaded state requires known remote version"))
        ));

        let uploaded_fence = upload(&mut transaction);
        let local_commit_fence = transaction
            .advance(uploaded_fence, TransitionTransactionState::LocalCommitStarted, None)
            .expect("local commit start should succeed");
        let committed_fence = transaction
            .advance(local_commit_fence, TransitionTransactionState::Committed, None)
            .expect("commit should succeed");
        assert_eq!(committed_fence.revision, 4);
        assert_eq!(transaction.state, TransitionTransactionState::Committed);
    }

    #[test]
    fn cleanup_pending_requires_exact_proof_and_state_specific_decision() {
        let mut transaction = new_transaction();
        let uploaded_fence = upload(&mut transaction);

        let mut wrong_remote = cleanup_proof(&transaction, TransitionCleanupDecision::UploadAbortedBeforeLocalCommit);
        wrong_remote.remote_object.push_str("-different");
        assert!(matches!(
            transaction.mark_cleanup_pending(uploaded_fence, wrong_remote),
            Err(TransitionTransactionError::Fenced)
        ));

        let proof = cleanup_proof(&transaction, TransitionCleanupDecision::UploadAbortedBeforeLocalCommit);
        let cleanup_fence = transaction
            .mark_cleanup_pending(uploaded_fence, proof)
            .expect("uploaded transaction should enter cleanup pending");
        assert_eq!(cleanup_fence.revision, 3);
        assert_eq!(transaction.state, TransitionTransactionState::CleanupPending);

        let mut local_commit_transaction = new_transaction();
        let uploaded_fence = upload(&mut local_commit_transaction);
        let local_commit_fence = local_commit_transaction
            .advance(uploaded_fence, TransitionTransactionState::LocalCommitStarted, None)
            .expect("local commit start should succeed");
        let mut changed_source = local_commit_transaction.source.clone();
        changed_source.etag = "different-etag".to_string();
        let proof = cleanup_proof(
            &local_commit_transaction,
            TransitionCleanupDecision::SourceReconciledUnchanged {
                observed_source: changed_source,
            },
        );
        assert!(matches!(
            local_commit_transaction.mark_cleanup_pending(local_commit_fence, proof),
            Err(TransitionTransactionError::InvalidStateChange {
                from: TransitionTransactionState::LocalCommitStarted,
                to: TransitionTransactionState::CleanupPending,
            })
        ));
    }

    #[test]
    fn upload_outcome_unknown_can_only_cleanup_after_version_recovery() {
        let mut transaction = new_transaction();
        let unknown_fence = transaction
            .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
            .expect("upload outcome unknown should be recorded");

        let mut proof = cleanup_proof(&transaction, TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation);
        proof.remote_version = TransitionRemoteVersion::unknown();
        assert!(matches!(
            transaction.mark_cleanup_pending(unknown_fence, proof),
            Err(TransitionTransactionError::Corrupt("cleanup proof requires known remote version"))
        ));

        let proof = TransitionCleanupProof {
            remote_version: TransitionRemoteVersion::versioned(Uuid::new_v4().to_string()),
            decision: TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation,
            ..cleanup_proof(&transaction, TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation)
        };
        let cleanup_fence = transaction
            .mark_cleanup_pending(unknown_fence, proof)
            .expect("recovered remote version should allow cleanup pending");
        assert_eq!(cleanup_fence.revision, 3);
        assert_eq!(transaction.state, TransitionTransactionState::CleanupPending);
    }

    #[test]
    fn stale_fence_blocks_second_actor_and_compare_delete() {
        let mut store = MemoryTransactionStore::default();
        let transaction = new_transaction();
        let transaction_id = transaction.transaction_id;
        let first_actor_fence = transaction.fence();
        let second_actor_fence = transaction.fence();
        store.create(&transaction).expect("initial create should succeed");

        let uploaded_fence = store
            .compare_save(transaction_id, first_actor_fence, TransitionTransactionState::Uploaded)
            .expect("first actor should advance upload");
        assert!(matches!(
            store.compare_save(transaction_id, second_actor_fence, TransitionTransactionState::LocalCommitStarted),
            Err(TransitionTransactionError::Fenced)
        ));

        let local_commit_fence = store
            .compare_save(transaction_id, uploaded_fence, TransitionTransactionState::LocalCommitStarted)
            .expect("fresh fence should advance local commit");
        assert!(matches!(
            store.compare_delete(transaction_id, uploaded_fence),
            Err(TransitionTransactionError::Fenced)
        ));
        store
            .compare_delete(transaction_id, local_commit_fence)
            .expect("fresh fence should delete transaction");
        assert!(matches!(store.load(transaction_id), Err(TransitionTransactionError::NotFound)));
    }

    #[test]
    fn decode_rejects_oversized_record_before_parsing() {
        let transaction = new_transaction();
        let oversized = vec![b' '; MAX_TRANSITION_TRANSACTION_SIZE + 1];
        assert!(matches!(
            TransitionTransaction::decode(transaction.transaction_id, &oversized),
            Err(TransitionTransactionError::Corrupt("encoded transaction exceeds maximum size"))
        ));
    }

    #[test]
    fn record_object_name_is_stable_and_sanitized() {
        let transaction_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").expect("test uuid should parse");

        let object = transition_transaction_record_object_name(transaction_id).expect("record object should build");

        assert_eq!(object, "ilm/transition-transactions/records/aa/aa/aaaaaaaabbbbccccddddeeeeeeeeeeee.json");
        let file_name = object.rsplit('/').next().expect("record object should have file name");
        assert!(!file_name.contains('-'));
        assert!(matches!(
            transition_transaction_record_object_name(Uuid::nil()),
            Err(TransitionTransactionError::Corrupt("transaction_id is nil"))
        ));
    }
}
