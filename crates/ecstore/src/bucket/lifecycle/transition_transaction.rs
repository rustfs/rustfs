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
use crate::bucket::lifecycle::durable_namespace::TRANSITION_TRANSACTION_NAMESPACE;
use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use crate::bucket::lifecycle::recovery_control::{
    IlmRecoveryClassification, IlmRecoveryControl, IlmRecoveryControlIdentity, IlmRecoveryErrorCode, IlmRecoveryProtocol,
    ObservedIlmRecoveryControl, load_recovery_control, observe_recovery_source, recovery_control_record_object_name,
    save_recovery_control_if_absent, save_recovery_control_if_current,
};
use crate::bucket::lifecycle::tier_sweeper::{
    delete_confirmed_transition_candidate_exact_with_lease_idempotent,
    delete_object_from_remote_tier_idempotent_with_manager_and_identity,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::services::tier::{tier::TierConfigMgr, warm_backend::TransitionCandidateProbe};
use crate::storage_api_contracts::{
    list::ListOperations as _,
    namespace::NamespaceLocking as _,
    object::{HTTPPreconditions, ObjectOperations as _},
};
use crate::store::ECStore;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_TRANSITION_TRANSACTION_RECOVERY: &str = "lifecycle_transition_transaction_recovery";
pub const DEFAULT_TRANSITION_TRANSACTION_RECOVERY_LIMIT: usize = 1_000;
const TRANSITION_TRANSACTION_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const TRANSITION_TRANSACTION_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);
const TRANSITION_RECOVERY_CONTROL_LEASE_NANOS: i64 = 15 * 60 * 1_000_000_000;
pub const TRANSITION_TRANSACTION_SCHEMA: &str = "rustfs-transition-transaction-v1";
pub const TRANSITION_TRANSACTION_PREFIX: &str = "ilm/transition-transactions";
pub const TRANSITION_TRANSACTION_RECORD_PREFIX: &str = TRANSITION_TRANSACTION_NAMESPACE.prefix;
pub const MAX_TRANSITION_TRANSACTION_SIZE: usize = 64 * 1024;

pub type Result<T> = std::result::Result<T, TransitionTransactionError>;

#[derive(Debug, thiserror::Error)]
pub enum TransitionTransactionError {
    #[error("transition transaction already exists")]
    #[allow(
        dead_code,
        reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
    )]
    AlreadyExists,
    #[error("transition transaction is not found")]
    #[allow(
        dead_code,
        reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
    )]
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
        if version_id.is_empty() {
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
        if self.not_after_unix_nanos <= 0 {
            return Err(TransitionTransactionError::Corrupt("ownership deadline is not positive"));
        }
        self.source.validate()?;
        if self.tier_name.is_empty() {
            return Err(TransitionTransactionError::Corrupt("tier name is empty"));
        }
        if self.backend_fingerprint == [0; 32] {
            return Err(TransitionTransactionError::Corrupt("backend fingerprint is empty"));
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

    fn has_same_immutable_identity(&self, other: &Self) -> bool {
        self.deployment_id == other.deployment_id
            && self.transaction_id == other.transaction_id
            && self.owner_epoch == other.owner_epoch
            && self.write_id == other.write_id
            && self.source == other.source
            && self.tier_name == other.tier_name
            && self.backend_fingerprint == other.backend_fingerprint
            && self.remote_object == other.remote_object
            && self.not_after_unix_nanos == other.not_after_unix_nanos
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
    config_boundary::save_config_with_opts(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    // Box::pin: the durable-receipt state machine is large and sits on the
    // already-deep transition worker poll chain; keeping it inline overflows
    // the default 2 MiB tokio worker stack in debug builds.
    Box::pin(api.record_durable_ilm_decommission_progress(&object, &data)).await
}

pub(crate) async fn save_transition_transaction_record_if_current(
    api: Arc<ECStore>,
    expected: &TransitionTransaction,
    next: &TransitionTransaction,
) -> EcstoreResult<()> {
    let object = transition_transaction_record_object_name(next.transaction_id).map_err(transition_transaction_store_error)?;
    let revision_is_next = expected.revision.checked_add(1) == Some(next.revision);
    let state_is_next = state_change_allowed(expected.state, next.state)
        || matches!(
            (expected.state, next.state),
            (
                TransitionTransactionState::Uploaded
                    | TransitionTransactionState::UploadOutcomeUnknown
                    | TransitionTransactionState::LocalCommitStarted,
                TransitionTransactionState::CleanupPending
            )
        );
    let remote_version_is_monotonic = expected.remote_version.is_unknown() || expected.remote_version == next.remote_version;
    if !expected.has_same_immutable_identity(next) || !revision_is_next || !state_is_next || !remote_version_is_monotonic {
        return Err(Error::PreconditionFailed);
    }
    let (current, etag) = load_transition_transaction_record_with_etag(api.clone(), expected.transaction_id).await?;
    if &current != expected {
        return Err(Error::PreconditionFailed);
    }
    let data = next.encode().map_err(transition_transaction_store_error)?;
    config_boundary::save_config_with_opts(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(etag),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    // Box::pin: see save_transition_transaction_record.
    Box::pin(api.record_durable_ilm_decommission_progress(&object, &data)).await
}

pub(crate) async fn load_transition_transaction_record(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> EcstoreResult<TransitionTransaction> {
    load_transition_transaction_record_with_etag(api, transaction_id)
        .await
        .map(|(transaction, _)| transaction)
}

async fn load_transition_transaction_record_with_etag(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> EcstoreResult<(TransitionTransaction, String)> {
    let object = transition_transaction_record_object_name(transaction_id).map_err(transition_transaction_store_error)?;
    let (data, object_info) = config_boundary::read_config_with_metadata(api, &object, &ObjectOptions::default()).await?;
    let etag = object_info
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("transition transaction record is missing an ETag"))?;
    let transaction = TransitionTransaction::decode(transaction_id, &data).map_err(transition_transaction_store_error)?;
    Ok((transaction, etag))
}

pub(crate) async fn delete_transition_transaction_record(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<()> {
    let object =
        transition_transaction_record_object_name(transaction.transaction_id).map_err(transition_transaction_store_error)?;
    let (current, etag) = match load_transition_transaction_record_with_etag(api.clone(), transaction.transaction_id).await {
        Ok(record) => record,
        Err(Error::ConfigNotFound) => return Ok(()),
        Err(err) => return Err(err),
    };
    if &current != transaction {
        return Err(Error::PreconditionFailed);
    }
    let data = current.encode().map_err(transition_transaction_store_error)?;
    // Box::pin: see save_transition_transaction_record.
    Box::pin(api.record_durable_ilm_decommission_terminal(&object, &data)).await?;
    match config_boundary::delete_config_if_match(api, &object, &etag).await {
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
    RetainedAmbiguous(IlmRecoveryErrorCode),
    OperatorRequired(IlmRecoveryErrorCode),
}

#[cfg(all(test, feature = "test-util"))]
#[derive(Default)]
struct TransitionRecoveryClaimBarrierState {
    transaction_id: Uuid,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) struct TransitionRecoveryClaimBarrier {
    state: Arc<TransitionRecoveryClaimBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
static TRANSITION_RECOVERY_CLAIM_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionRecoveryClaimBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
impl TransitionRecoveryClaimBarrier {
    pub(crate) fn install(transaction_id: Uuid) -> Self {
        let state = Arc::new(TransitionRecoveryClaimBarrierState {
            transaction_id,
            ..Default::default()
        });
        let mut slot = TRANSITION_RECOVERY_CLAIM_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition recovery claim barrier mutex should not poison");
        assert!(
            slot.is_none(),
            "transition recovery claim barrier must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition recovery should reach the cleanup claim CAS");
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionRecoveryClaimBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_RECOVERY_CLAIM_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition recovery claim barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn pause_before_transition_recovery_claim(transaction_id: Uuid) {
    let barrier = TRANSITION_RECOVERY_CLAIM_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition recovery claim barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.transaction_id == transaction_id)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(all(test, feature = "test-util"))]
#[derive(Default)]
struct TransitionRecoveryTerminalBarrierState {
    transaction_id: Uuid,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) struct TransitionRecoveryTerminalBarrier {
    state: Arc<TransitionRecoveryTerminalBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
static TRANSITION_RECOVERY_TERMINAL_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionRecoveryTerminalBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
impl TransitionRecoveryTerminalBarrier {
    pub(crate) fn install(transaction_id: Uuid) -> Self {
        let state = Arc::new(TransitionRecoveryTerminalBarrierState {
            transaction_id,
            ..Default::default()
        });
        let mut slot = TRANSITION_RECOVERY_TERMINAL_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition recovery terminal barrier mutex should not poison");
        assert!(
            slot.is_none(),
            "transition recovery terminal barrier must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition recovery should persist terminal control before source cleanup");
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionRecoveryTerminalBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_RECOVERY_TERMINAL_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition recovery terminal barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn pause_after_transition_recovery_terminal(transaction_id: Uuid) {
    let barrier = TRANSITION_RECOVERY_TERMINAL_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition recovery terminal barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.transaction_id == transaction_id)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionOperatorProbe {
    Missing,
    UnversionedPresent,
    VersionedPresent(String),
    Ambiguous,
    Unsupported,
}

impl From<TransitionCandidateProbe> for TransitionOperatorProbe {
    fn from(value: TransitionCandidateProbe) -> Self {
        match value {
            TransitionCandidateProbe::Missing => Self::Missing,
            TransitionCandidateProbe::UnversionedPresent => Self::UnversionedPresent,
            TransitionCandidateProbe::VersionedPresent(version_id) => Self::VersionedPresent(version_id),
            TransitionCandidateProbe::Ambiguous => Self::Ambiguous,
            TransitionCandidateProbe::Unsupported => Self::Unsupported,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionOperatorStatus {
    pub transaction_id: Uuid,
    pub state: TransitionTransactionState,
    pub tier_name: String,
    pub remote_object: String,
    pub not_after_unix_nanos: i64,
    pub probe: TransitionOperatorProbe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionOperatorDeleteResult {
    pub status: TransitionOperatorStatus,
    pub journal_observed_after_delete: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TransitionOperatorError {
    #[error("transition transaction was not found")]
    NotFound,
    #[error("transition transaction is still inside its active ownership window")]
    NotExpired,
    #[error("transition transaction state is not eligible for operator reconciliation: {0:?}")]
    InvalidState(TransitionTransactionState),
    #[error("an exact non-empty remote version is required")]
    RemoteVersionRequired,
    #[error("remote candidate is not proven missing: {0:?}")]
    CandidateNotMissing(TransitionOperatorProbe),
    #[error("remote candidate version does not match requested exact version: expected {expected}, observed {actual:?}")]
    CandidateVersionMismatch {
        expected: String,
        actual: TransitionOperatorProbe,
    },
    #[error("transition recovery control is stale")]
    StaleRecoveryControl,
    #[error("transition recovery control is not eligible for operator retry")]
    RetryNotAllowed,
    #[error("transition transaction store failed: {0}")]
    Store(#[source] Error),
    #[error("remote tier reconciliation failed: {0}")]
    Remote(#[source] std::io::Error),
}

type TransitionOperatorResult<T> = std::result::Result<T, TransitionOperatorError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionRecoveryRetryStatus {
    pub control_id: String,
    pub transaction_id: Uuid,
    pub state: TransitionTransactionState,
    pub classification: IlmRecoveryClassification,
    pub control_revision: u64,
    pub attempt_count: u64,
    pub consecutive_failure_count: u32,
    pub last_error_code: IlmRecoveryErrorCode,
    pub source_generation_sha256: String,
    pub copy_set_sha256: String,
    pub retry_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_not_ready_reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionRecoveryRetryResult {
    pub control_id: String,
    pub transaction_id: Uuid,
    pub previous_revision: u64,
    pub revision: u64,
    pub classification: IlmRecoveryClassification,
    pub attempt_count: u64,
    pub source_generation_sha256: String,
}

struct TransitionRecoveryRetryContext {
    observed: ObservedIlmRecoveryControl,
    transaction: TransitionTransaction,
    source_generation_sha256: String,
}

fn transition_recovery_retry_readiness(control: &IlmRecoveryControl) -> (bool, Option<&'static str>) {
    if control.owner.is_some() {
        return (false, Some("attempt_owned"));
    }
    match control.classification {
        IlmRecoveryClassification::RetainedAmbiguous | IlmRecoveryClassification::OperatorRequired => (true, None),
        IlmRecoveryClassification::Retrying => (false, Some("already_retrying")),
        IlmRecoveryClassification::Corrupt => (false, Some("source_corrupt")),
        IlmRecoveryClassification::Abandoned => (false, Some("source_abandoned")),
        IlmRecoveryClassification::Terminal => (false, Some("source_terminal")),
    }
}

async fn load_transition_recovery_retry_context(
    api: Arc<ECStore>,
    control_id: &str,
) -> TransitionOperatorResult<TransitionRecoveryRetryContext> {
    let observed = match load_recovery_control(api.clone(), IlmRecoveryProtocol::TransitionTransaction, control_id).await {
        Ok(observed) => observed,
        Err(Error::ConfigNotFound) => return Err(TransitionOperatorError::NotFound),
        Err(err) => return Err(TransitionOperatorError::Store(err)),
    };
    let transaction_id = Uuid::parse_str(&observed.control.identity.stable_operation_identity)
        .ok()
        .filter(|transaction_id| !transaction_id.is_nil())
        .ok_or(TransitionOperatorError::StaleRecoveryControl)?;
    let canonical_path = transition_transaction_record_object_name(transaction_id)
        .map_err(|err| TransitionOperatorError::Store(Error::other(err)))?;
    if observed.control.identity.canonical_source_path != canonical_path
        || observed.control.identity.record_class != "transition_transaction_v1"
    {
        return Err(TransitionOperatorError::StaleRecoveryControl);
    }
    let transaction = match load_transition_transaction_record(api.clone(), transaction_id).await {
        Ok(transaction) => transaction,
        Err(Error::ConfigNotFound) => return Err(TransitionOperatorError::NotFound),
        Err(err) => return Err(TransitionOperatorError::Store(err)),
    };
    let source = observe_recovery_source(api, &canonical_path, TRANSITION_TRANSACTION_SCHEMA)
        .await
        .map_err(TransitionOperatorError::Store)?;
    let exact_source = source.is_consistent()
        && source.generation == observed.control.observed_source_generation
        && source
            .canonical_data
            .as_deref()
            .is_some_and(|data| TransitionTransaction::decode(transaction_id, data).is_ok_and(|decoded| decoded == transaction));
    if !exact_source {
        return Err(TransitionOperatorError::StaleRecoveryControl);
    }
    let generation = serde_json::to_vec(&observed.control.observed_source_generation)
        .map_err(|err| TransitionOperatorError::Store(Error::other(err)))?;
    Ok(TransitionRecoveryRetryContext {
        observed,
        transaction,
        source_generation_sha256: hex_sha256(&generation, ToOwned::to_owned),
    })
}

pub async fn inspect_transition_recovery_retry_for_operator(
    api: Arc<ECStore>,
    control_id: &str,
) -> TransitionOperatorResult<TransitionRecoveryRetryStatus> {
    let context = load_transition_recovery_retry_context(api, control_id).await?;
    let (retry_ready, retry_not_ready_reason) = transition_recovery_retry_readiness(&context.observed.control);
    Ok(TransitionRecoveryRetryStatus {
        control_id: control_id.to_string(),
        transaction_id: context.transaction.transaction_id,
        state: context.transaction.state,
        classification: context.observed.control.classification,
        control_revision: context.observed.control.revision,
        attempt_count: context.observed.control.attempt_count,
        consecutive_failure_count: context.observed.control.consecutive_failure_count,
        last_error_code: context.observed.control.last_error_code,
        source_generation_sha256: context.source_generation_sha256,
        copy_set_sha256: context.observed.control.observed_source_generation.copy_set_sha256.clone(),
        retry_ready,
        retry_not_ready_reason,
    })
}

pub async fn retry_transition_recovery_for_operator(
    api: Arc<ECStore>,
    control_id: &str,
    expected_control_revision: u64,
    expected_source_generation_sha256: &str,
) -> TransitionOperatorResult<TransitionRecoveryRetryResult> {
    let control_object = recovery_control_record_object_name(IlmRecoveryProtocol::TransitionTransaction, control_id)
        .map_err(|err| TransitionOperatorError::Store(Error::other(err)))?;
    let retry_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &format!("{control_object}.recovery-lock"))
        .await
        .map_err(TransitionOperatorError::Store)?;
    let retry_guard = retry_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await
        .map_err(|err| TransitionOperatorError::Store(Error::other(err)))?;
    let context = load_transition_recovery_retry_context(api.clone(), control_id).await?;
    let (retry_ready, _) = transition_recovery_retry_readiness(&context.observed.control);
    if !retry_ready {
        return Err(TransitionOperatorError::RetryNotAllowed);
    }
    if retry_guard.is_lock_lost()
        || expected_control_revision == 0
        || context.observed.control.revision != expected_control_revision
        || context.source_generation_sha256 != expected_source_generation_sha256
    {
        return Err(TransitionOperatorError::StaleRecoveryControl);
    }
    let previous_revision = context.observed.control.revision;
    let mut next = context.observed.control.clone();
    next.retry_for_operator(&context.observed.control.observed_source_generation)
        .map_err(|_| TransitionOperatorError::RetryNotAllowed)?;
    if retry_guard.is_lock_lost() {
        return Err(TransitionOperatorError::StaleRecoveryControl);
    }
    save_recovery_control_if_current(api.clone(), &context.observed, &next)
        .await
        .map_err(|err| match err {
            Error::PreconditionFailed => TransitionOperatorError::StaleRecoveryControl,
            err => TransitionOperatorError::Store(err),
        })?;
    let persisted = load_recovery_control(api, IlmRecoveryProtocol::TransitionTransaction, control_id)
        .await
        .map_err(TransitionOperatorError::Store)?;
    if retry_guard.is_lock_lost() || persisted.control != next {
        return Err(TransitionOperatorError::StaleRecoveryControl);
    }
    Ok(TransitionRecoveryRetryResult {
        control_id: control_id.to_string(),
        transaction_id: context.transaction.transaction_id,
        previous_revision,
        revision: persisted.control.revision,
        classification: persisted.control.classification,
        attempt_count: persisted.control.attempt_count,
        source_generation_sha256: context.source_generation_sha256,
    })
}

fn validate_operator_reconcile_transaction(
    transaction: &TransitionTransaction,
    now_unix_nanos: i128,
) -> TransitionOperatorResult<()> {
    transaction
        .validate()
        .map_err(|err| TransitionOperatorError::Store(Error::other(err)))?;
    if transaction.state != TransitionTransactionState::UploadOutcomeUnknown {
        return Err(TransitionOperatorError::InvalidState(transaction.state));
    }
    if now_unix_nanos < i128::from(transaction.not_after_unix_nanos) {
        return Err(TransitionOperatorError::NotExpired);
    }
    Ok(())
}

async fn load_operator_reconcile_transaction(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> TransitionOperatorResult<TransitionTransaction> {
    match load_transition_transaction_record(api, transaction_id).await {
        Ok(transaction) => Ok(transaction),
        Err(Error::ConfigNotFound) => Err(TransitionOperatorError::NotFound),
        Err(err) => Err(TransitionOperatorError::Store(err)),
    }
}

async fn operator_probe_transition_candidate(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> TransitionOperatorResult<TransitionOperatorProbe> {
    let lease = TierConfigMgr::acquire_operation_lease_for_backend_identity(
        &api.tier_config_mgr(),
        &transaction.tier_name,
        transaction.backend_fingerprint,
    )
    .await
    .map_err(|err| TransitionOperatorError::Remote(std::io::Error::other(err)))?;
    lease
        .probe_transition_candidate_for(&transaction.remote_object, transaction.transaction_id)
        .await
        .map(TransitionOperatorProbe::from)
        .map_err(TransitionOperatorError::Remote)
}

pub async fn inspect_transition_transaction_for_operator(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> TransitionOperatorResult<TransitionOperatorStatus> {
    let transaction = load_operator_reconcile_transaction(api.clone(), transaction_id).await?;
    validate_operator_reconcile_transaction(&transaction, time::OffsetDateTime::now_utc().unix_timestamp_nanos())?;
    let probe = operator_probe_transition_candidate(api, &transaction).await?;
    Ok(TransitionOperatorStatus {
        transaction_id,
        state: transaction.state,
        tier_name: transaction.tier_name,
        remote_object: transaction.remote_object,
        not_after_unix_nanos: transaction.not_after_unix_nanos,
        probe,
    })
}

pub async fn delete_transition_candidate_for_operator(
    api: Arc<ECStore>,
    transaction_id: Uuid,
    remote_version_id: &str,
) -> TransitionOperatorResult<TransitionOperatorDeleteResult> {
    if remote_version_id.is_empty() {
        return Err(TransitionOperatorError::RemoteVersionRequired);
    }
    let transaction = load_operator_reconcile_transaction(api.clone(), transaction_id).await?;
    validate_operator_reconcile_transaction(&transaction, time::OffsetDateTime::now_utc().unix_timestamp_nanos())?;
    let lease = TierConfigMgr::acquire_operation_lease_for_backend_identity(
        &api.tier_config_mgr(),
        &transaction.tier_name,
        transaction.backend_fingerprint,
    )
    .await
    .map_err(|err| TransitionOperatorError::Remote(std::io::Error::other(err)))?;
    lease
        .validate_remote_version_id(remote_version_id)
        .map_err(TransitionOperatorError::Remote)?;
    let before_delete_probe = lease
        .probe_transition_candidate_for(&transaction.remote_object, transaction.transaction_id)
        .await
        .map(TransitionOperatorProbe::from)
        .map_err(TransitionOperatorError::Remote)?;
    if !matches!(&before_delete_probe, TransitionOperatorProbe::VersionedPresent(version_id) if version_id == remote_version_id) {
        return Err(TransitionOperatorError::CandidateVersionMismatch {
            expected: remote_version_id.to_string(),
            actual: before_delete_probe,
        });
    }
    delete_confirmed_transition_candidate_exact_with_lease_idempotent(&transaction.remote_object, remote_version_id, &lease)
        .await
        .map_err(TransitionOperatorError::Remote)?;
    let probe = operator_probe_transition_candidate(api.clone(), &transaction).await?;
    let journal_observed_after_delete = match load_transition_transaction_record(api, transaction_id).await {
        Ok(_) => true,
        Err(Error::ConfigNotFound) => false,
        Err(err) => return Err(TransitionOperatorError::Store(err)),
    };
    Ok(TransitionOperatorDeleteResult {
        status: TransitionOperatorStatus {
            transaction_id,
            state: transaction.state,
            tier_name: transaction.tier_name,
            remote_object: transaction.remote_object,
            not_after_unix_nanos: transaction.not_after_unix_nanos,
            probe,
        },
        journal_observed_after_delete,
    })
}

pub async fn finalize_missing_transition_transaction_for_operator(
    api: Arc<ECStore>,
    transaction_id: Uuid,
) -> TransitionOperatorResult<()> {
    let transaction = load_operator_reconcile_transaction(api.clone(), transaction_id).await?;
    validate_operator_reconcile_transaction(&transaction, time::OffsetDateTime::now_utc().unix_timestamp_nanos())?;
    let probe = operator_probe_transition_candidate(api.clone(), &transaction).await?;
    if probe != TransitionOperatorProbe::Missing {
        return Err(TransitionOperatorError::CandidateNotMissing(probe));
    }
    delete_transition_transaction_record(api, &transaction)
        .await
        .map_err(TransitionOperatorError::Store)
}

pub(crate) fn decode_transition_transaction_record(object: &str, data: &[u8]) -> Result<TransitionTransaction> {
    let transaction_id = transition_transaction_id_from_record_object_name(object)?;
    TransitionTransaction::decode(transaction_id, data)
}

fn transition_transaction_id_from_record_object_name(object: &str) -> Result<Uuid> {
    let prefix = format!("{TRANSITION_TRANSACTION_RECORD_PREFIX}/");
    let suffix = object
        .strip_prefix(&prefix)
        .ok_or(TransitionTransactionError::Corrupt("transaction record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let shard_a = parts
        .next()
        .ok_or(TransitionTransactionError::Corrupt("transaction record path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or(TransitionTransactionError::Corrupt("transaction record path is incomplete"))?;
    let file_name = parts
        .next()
        .ok_or(TransitionTransactionError::Corrupt("transaction record path is incomplete"))?;
    if parts.next().is_some() {
        return Err(TransitionTransactionError::Corrupt("transaction record path is not canonical"));
    }
    let transaction_key = file_name
        .strip_suffix(".json")
        .ok_or(TransitionTransactionError::Corrupt("transaction record path has wrong suffix"))?;
    if transaction_key.len() != 32
        || !transaction_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || shard_a != &transaction_key[..2]
        || shard_b != &transaction_key[2..4]
    {
        return Err(TransitionTransactionError::Corrupt("transaction record path has invalid transaction id"));
    }
    Uuid::parse_str(transaction_key)
        .ok()
        .filter(|transaction_id| !transaction_id.is_nil())
        .ok_or(TransitionTransactionError::Corrupt("transaction record path has invalid uuid"))
}

pub async fn process_transition_transaction_record(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    transaction.validate().map_err(transition_transaction_store_error)?;
    // Box the expanded recovery state machine so callers on Tokio's default
    // worker stack do not inline its full future into an already-deep scan.
    Box::pin(process_transition_transaction_record_at(
        api,
        transaction,
        time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
    ))
    .await
}

async fn process_transition_transaction_record_at(
    api: Arc<ECStore>,
    observed: &TransitionTransaction,
    now_unix_nanos: i128,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    let record_name =
        transition_transaction_record_object_name(observed.transaction_id).map_err(transition_transaction_store_error)?;
    let now_unix_nanos =
        i64::try_from(now_unix_nanos).map_err(|_| Error::other("transition transaction recovery timestamp does not fit i64"))?;
    let recovery_control_identity = transition_recovery_control_identity(observed, &record_name);
    let recovery_control_id = recovery_control_identity
        .source_operation_digest()
        .map_err(|err| Error::other(err.to_string()))?;
    let control_record_name =
        recovery_control_record_object_name(IlmRecoveryProtocol::TransitionTransaction, &recovery_control_id)
            .map_err(|err| Error::other(err.to_string()))?;
    let control_lock = if transition_state_needs_recovery_control(observed, now_unix_nanos) {
        Some(
            api.new_ns_lock(RUSTFS_META_BUCKET, &format!("{control_record_name}.recovery-lock"))
                .await?,
        )
    } else {
        None
    };
    let _control_guard = match &control_lock {
        Some(lock) => Some(lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout()).await?),
        None => None,
    };
    // The synthetic key avoids nesting the recovery lock with the config
    // object's own I/O lock. Holding it across the bounded source proof and
    // remote DELETE elects one destructive recovery worker across nodes.
    let recovery_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &format!("{record_name}.recovery-lock"))
        .await?;
    let _recovery_guard = recovery_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let current = match load_transition_transaction_record(api.clone(), observed.transaction_id).await {
        Ok(current) => current,
        Err(Error::ConfigNotFound) => return Ok(TransitionTransactionRecoveryOutcome::RecordDeleted),
        Err(err) => return Err(err),
    };
    if &current != observed {
        return Ok(TransitionTransactionRecoveryOutcome::Retained);
    }

    let mut recovery_control = if transition_state_needs_recovery_control(&current, now_unix_nanos) {
        if cleanup_terminal_transition_recovery_control(
            api.clone(),
            &current,
            &record_name,
            &recovery_control_identity,
            &recovery_control_id,
        )
        .await?
        {
            return Ok(TransitionTransactionRecoveryOutcome::RecordDeleted);
        }
        match claim_transition_recovery_control(
            api.clone(),
            &current,
            &record_name,
            recovery_control_identity,
            &recovery_control_id,
            now_unix_nanos,
        )
        .await?
        {
            Some(control) => Some(control),
            None => return Ok(TransitionTransactionRecoveryOutcome::Retained),
        }
    } else {
        None
    };

    let recovery = match current.state {
        TransitionTransactionState::Uploaded => {
            if transition_transaction_ownership_is_active(&current, i128::from(now_unix_nanos)) {
                Ok(TransitionTransactionRecoveryOutcome::Retained)
            } else {
                let mut cleanup = current.clone();
                cleanup
                    .mark_cleanup_pending(
                        current.fence(),
                        TransitionCleanupProof {
                            transaction_id: current.transaction_id,
                            write_id: current.write_id,
                            remote_object: current.remote_object.clone(),
                            remote_version: current.remote_version.clone(),
                            backend_fingerprint: current.backend_fingerprint,
                            decision: TransitionCleanupDecision::UploadAbortedBeforeLocalCommit,
                        },
                    )
                    .map_err(transition_transaction_store_error)?;
                #[cfg(all(test, feature = "test-util"))]
                pause_before_transition_recovery_claim(current.transaction_id).await;
                match save_transition_transaction_record_if_current(api.clone(), &current, &cleanup).await {
                    Ok(()) => recover_cleanup_pending(api.clone(), &cleanup).await,
                    Err(Error::PreconditionFailed) | Err(Error::ConfigNotFound) => {
                        Ok(TransitionTransactionRecoveryOutcome::Retained)
                    }
                    Err(err) => Err(err),
                }
            }
        }
        TransitionTransactionState::CleanupPending => recover_cleanup_pending(api.clone(), &current).await,
        TransitionTransactionState::LocalCommitStarted => match local_commit_matches_transaction(api.clone(), &current).await {
            Ok(true) => Ok(TransitionTransactionRecoveryOutcome::RecordDeleted),
            Ok(false) => Ok(TransitionTransactionRecoveryOutcome::OperatorRequired(
                IlmRecoveryErrorCode::LocalCommitAmbiguous,
            )),
            Err(err) if transition_source_is_missing(&err) => Ok(TransitionTransactionRecoveryOutcome::OperatorRequired(
                IlmRecoveryErrorCode::LocalCommitAmbiguous,
            )),
            Err(err) => Err(err),
        },
        TransitionTransactionState::AbortedNoRemote | TransitionTransactionState::Committed => {
            Ok(TransitionTransactionRecoveryOutcome::RecordDeleted)
        }
        TransitionTransactionState::UploadOutcomeUnknown => {
            if transition_transaction_ownership_is_active(&current, i128::from(now_unix_nanos)) {
                Ok(TransitionTransactionRecoveryOutcome::Retained)
            } else {
                recover_unknown_upload_outcome(api.clone(), &current).await
            }
        }
        TransitionTransactionState::UploadStarted => {
            if transition_transaction_ownership_is_active(&current, i128::from(now_unix_nanos)) {
                Ok(TransitionTransactionRecoveryOutcome::Retained)
            } else {
                Ok(TransitionTransactionRecoveryOutcome::RetainedAmbiguous(
                    IlmRecoveryErrorCode::RemoteVersionUnknown,
                ))
            }
        }
    };

    if let Some(mut control) = recovery_control.take() {
        let source_to_delete = if matches!(
            recovery,
            Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted
                | TransitionTransactionRecoveryOutcome::RecordDeleted)
        ) {
            let refreshed =
                refresh_transition_recovery_control_source(api.clone(), control, &record_name, current.transaction_id).await?;
            control = refreshed.0;
            refreshed.1
        } else {
            None
        };
        persist_transition_recovery_result(api.clone(), control, &recovery, now_unix_nanos).await?;
        if let Some(source) = source_to_delete {
            #[cfg(all(test, feature = "test-util"))]
            pause_after_transition_recovery_terminal(source.transaction_id).await;
            delete_transition_transaction_record(api, &source).await?;
        }
    } else if matches!(
        recovery,
        Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted | TransitionTransactionRecoveryOutcome::RecordDeleted)
    ) {
        delete_transition_transaction_record(api, &current).await?;
    }
    recovery
}

fn transition_recovery_control_identity(transaction: &TransitionTransaction, record_name: &str) -> IlmRecoveryControlIdentity {
    IlmRecoveryControlIdentity {
        protocol: IlmRecoveryProtocol::TransitionTransaction,
        canonical_source_path: record_name.to_string(),
        stable_operation_identity: transaction.transaction_id.to_string(),
        record_class: "transition_transaction_v1".to_string(),
    }
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) fn transition_recovery_control_id(transaction: &TransitionTransaction) -> Result<String> {
    let record_name = transition_transaction_record_object_name(transaction.transaction_id)?;
    transition_recovery_control_identity(transaction, &record_name)
        .source_operation_digest()
        .map_err(|_| TransitionTransactionError::Corrupt("transition recovery control identity is invalid"))
}

fn transition_state_needs_recovery_control(transaction: &TransitionTransaction, now_unix_nanos: i64) -> bool {
    now_unix_nanos >= transaction.not_after_unix_nanos
        && !matches!(
            transaction.state,
            TransitionTransactionState::AbortedNoRemote | TransitionTransactionState::Committed
        )
}

async fn cleanup_terminal_transition_recovery_control(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
    record_name: &str,
    identity: &IlmRecoveryControlIdentity,
    control_id: &str,
) -> EcstoreResult<bool> {
    let observed = match load_recovery_control(api.clone(), IlmRecoveryProtocol::TransitionTransaction, control_id).await {
        Ok(observed) => observed,
        Err(Error::ConfigNotFound) => return Ok(false),
        Err(err) => return Err(err),
    };
    if observed.control.classification != IlmRecoveryClassification::Terminal {
        return Ok(false);
    }
    let source = observe_recovery_source(api.clone(), record_name, TRANSITION_TRANSACTION_SCHEMA).await?;
    let exact_source = source.is_consistent()
        && source.generation == observed.control.observed_source_generation
        && source.canonical_data.as_deref().is_some_and(|data| {
            TransitionTransaction::decode(transaction.transaction_id, data).is_ok_and(|decoded| decoded == *transaction)
        });
    if observed.control.identity != *identity || !exact_source {
        return Ok(false);
    }
    delete_transition_transaction_record(api, transaction).await?;
    Ok(true)
}

async fn claim_transition_recovery_control(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
    record_name: &str,
    identity: IlmRecoveryControlIdentity,
    control_id: &str,
    now_unix_nanos: i64,
) -> EcstoreResult<Option<ObservedIlmRecoveryControl>> {
    let existing = match load_recovery_control(api.clone(), IlmRecoveryProtocol::TransitionTransaction, control_id).await {
        Ok(control) => Some(control),
        Err(Error::ConfigNotFound) => None,
        Err(err) => return Err(err),
    };
    if let Some(observed) = existing.as_ref() {
        if observed.control.identity != identity {
            return Ok(None);
        }
        if observed
            .control
            .owner
            .as_ref()
            .is_some_and(|owner| owner.lease_expires_at_unix_nanos <= now_unix_nanos)
        {
            let mut expired = observed.control.clone();
            expired
                .record_expired_attempt(now_unix_nanos)
                .map_err(|err| Error::other(err.to_string()))?;
            save_recovery_control_if_current(api, observed, &expired).await?;
            return Ok(None);
        }
        if !observed.control.should_attempt_at(now_unix_nanos) {
            return Ok(None);
        }
    }

    let source = match observe_recovery_source(api.clone(), record_name, TRANSITION_TRANSACTION_SCHEMA).await {
        Ok(source) => source,
        Err(err) => {
            if let Some(observed) = existing {
                persist_transition_recovery_source_failure(api, observed, now_unix_nanos).await?;
                return Ok(None);
            }
            return Err(err);
        }
    };
    let source_matches = source.is_consistent()
        && source.canonical_data.as_deref().is_some_and(|data| {
            TransitionTransaction::decode(transaction.transaction_id, data).is_ok_and(|observed| observed == *transaction)
        });
    let source_error = if source_matches {
        IlmRecoveryErrorCode::None
    } else if source.canonical_data.is_some() {
        IlmRecoveryErrorCode::SourceGenerationChanged
    } else {
        IlmRecoveryErrorCode::SourceDivergent
    };

    let mut observed = match existing {
        Some(control) => control,
        None => {
            let candidate = IlmRecoveryControl::new(
                identity.clone(),
                source.generation.clone(),
                if source_matches {
                    IlmRecoveryClassification::Retrying
                } else {
                    IlmRecoveryClassification::Corrupt
                },
                now_unix_nanos,
                source_error,
            )
            .map_err(|err| Error::other(err.to_string()))?;
            match save_recovery_control_if_absent(api.clone(), &candidate).await {
                Ok(()) | Err(Error::PreconditionFailed) => {}
                Err(err) => return Err(err),
            }
            load_recovery_control(api.clone(), IlmRecoveryProtocol::TransitionTransaction, control_id).await?
        }
    };
    if observed.control.identity != identity || !observed.control.should_attempt_at(now_unix_nanos) {
        return Ok(None);
    }

    let mut claimed = observed.control.clone();
    claimed
        .claim_for_source_generation(
            api.id.to_string(),
            Uuid::new_v4(),
            now_unix_nanos,
            TRANSITION_RECOVERY_CONTROL_LEASE_NANOS,
            source.generation,
        )
        .map_err(|err| Error::other(err.to_string()))?;
    save_recovery_control_if_current(api.clone(), &observed, &claimed).await?;
    observed = load_recovery_control(api.clone(), IlmRecoveryProtocol::TransitionTransaction, control_id).await?;
    if observed.control != claimed {
        return Err(Error::PreconditionFailed);
    }
    if !source_matches {
        let mut corrupt = observed.control.clone();
        corrupt
            .finish_attempt(IlmRecoveryClassification::Corrupt, source_error)
            .map_err(|err| Error::other(err.to_string()))?;
        save_recovery_control_if_current(api, &observed, &corrupt).await?;
        return Ok(None);
    }
    Ok(Some(observed))
}

async fn persist_transition_recovery_source_failure(
    api: Arc<ECStore>,
    observed: ObservedIlmRecoveryControl,
    now_unix_nanos: i64,
) -> EcstoreResult<()> {
    let mut claimed = observed.control.clone();
    claimed
        .claim(
            api.id.to_string(),
            Uuid::new_v4(),
            now_unix_nanos,
            TRANSITION_RECOVERY_CONTROL_LEASE_NANOS,
        )
        .map_err(|err| Error::other(err.to_string()))?;
    save_recovery_control_if_current(api.clone(), &observed, &claimed).await?;
    let claimed = load_recovery_control(
        api.clone(),
        IlmRecoveryProtocol::TransitionTransaction,
        &claimed
            .identity
            .source_operation_digest()
            .map_err(|err| Error::other(err.to_string()))?,
    )
    .await?;
    let mut failed = claimed.control.clone();
    failed
        .record_retryable_failure(now_unix_nanos, IlmRecoveryErrorCode::SourceUnavailable)
        .map_err(|err| Error::other(err.to_string()))?;
    save_recovery_control_if_current(api, &claimed, &failed).await
}

async fn refresh_transition_recovery_control_source(
    api: Arc<ECStore>,
    mut observed: ObservedIlmRecoveryControl,
    record_name: &str,
    transaction_id: Uuid,
) -> EcstoreResult<(ObservedIlmRecoveryControl, Option<TransitionTransaction>)> {
    let transaction = match load_transition_transaction_record(api.clone(), transaction_id).await {
        Ok(transaction) => transaction,
        Err(Error::ConfigNotFound) => return Ok((observed, None)),
        Err(err) => return Err(err),
    };
    let source = observe_recovery_source(api.clone(), record_name, TRANSITION_TRANSACTION_SCHEMA).await?;
    let exact_source = source.is_consistent()
        && source
            .canonical_data
            .as_deref()
            .is_some_and(|data| TransitionTransaction::decode(transaction_id, data).is_ok_and(|decoded| decoded == transaction));
    if !exact_source {
        return Err(Error::PreconditionFailed);
    }
    if observed.control.observed_source_generation != source.generation {
        let mut refreshed = observed.control.clone();
        refreshed
            .refresh_owned_source_generation(source.generation)
            .map_err(|err| Error::other(err.to_string()))?;
        save_recovery_control_if_current(api.clone(), &observed, &refreshed).await?;
        observed = load_recovery_control(
            api,
            IlmRecoveryProtocol::TransitionTransaction,
            &refreshed
                .identity
                .source_operation_digest()
                .map_err(|err| Error::other(err.to_string()))?,
        )
        .await?;
        if observed.control != refreshed {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok((observed, Some(transaction)))
}

async fn persist_transition_recovery_result(
    api: Arc<ECStore>,
    observed: ObservedIlmRecoveryControl,
    recovery: &EcstoreResult<TransitionTransactionRecoveryOutcome>,
    now_unix_nanos: i64,
) -> EcstoreResult<()> {
    let mut next = observed.control.clone();
    match recovery {
        Ok(
            TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted | TransitionTransactionRecoveryOutcome::RecordDeleted,
        ) => next
            .finish_attempt(IlmRecoveryClassification::Terminal, IlmRecoveryErrorCode::None)
            .map_err(|err| Error::other(err.to_string()))?,
        Ok(TransitionTransactionRecoveryOutcome::Retained) => next
            .record_retryable_failure(now_unix_nanos, IlmRecoveryErrorCode::SourceGenerationChanged)
            .map_err(|err| Error::other(err.to_string()))?,
        Ok(TransitionTransactionRecoveryOutcome::RetainedAmbiguous(code)) => next
            .finish_attempt(IlmRecoveryClassification::RetainedAmbiguous, *code)
            .map_err(|err| Error::other(err.to_string()))?,
        Ok(TransitionTransactionRecoveryOutcome::OperatorRequired(code)) => next
            .finish_attempt(IlmRecoveryClassification::OperatorRequired, *code)
            .map_err(|err| Error::other(err.to_string()))?,
        Err(err) => next
            .record_retryable_failure(now_unix_nanos, transition_recovery_error_code(err))
            .map_err(|err| Error::other(err.to_string()))?,
    }
    save_recovery_control_if_current(api, &observed, &next).await
}

fn transition_recovery_error_code(err: &Error) -> IlmRecoveryErrorCode {
    match err {
        Error::PreconditionFailed => IlmRecoveryErrorCode::CasConflict,
        Error::ConfigNotFound
        | Error::FileNotFound
        | Error::FileVersionNotFound
        | Error::ObjectNotFound(_, _)
        | Error::VersionNotFound(_, _, _)
        | Error::BucketNotFound(_) => IlmRecoveryErrorCode::SourceUnavailable,
        Error::SlowDown => IlmRecoveryErrorCode::BackendThrottled,
        _ => IlmRecoveryErrorCode::Unknown,
    }
}

fn transition_transaction_ownership_is_active(transaction: &TransitionTransaction, now_unix_nanos: i128) -> bool {
    now_unix_nanos < i128::from(transaction.not_after_unix_nanos)
}

async fn recover_cleanup_pending(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    match local_commit_matches_transaction(api.clone(), transaction).await {
        Ok(true) => Ok(TransitionTransactionRecoveryOutcome::RecordDeleted),
        Ok(false) => delete_unreferenced_transition_candidate(api, transaction).await,
        Err(err) if transition_source_is_missing(&err) => delete_unreferenced_transition_candidate(api, transaction).await,
        Err(err) => Err(err),
    }
}

async fn delete_unreferenced_transition_candidate(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    let current = match load_transition_transaction_record(api.clone(), transaction.transaction_id).await {
        Ok(current) => current,
        Err(Error::ConfigNotFound) => return Ok(TransitionTransactionRecoveryOutcome::RecordDeleted),
        Err(err) => return Err(err),
    };
    if &current != transaction || current.state != TransitionTransactionState::CleanupPending {
        return Ok(TransitionTransactionRecoveryOutcome::Retained);
    }
    delete_transition_remote_candidate(api.clone(), &current).await?;
    Ok(TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted)
}

async fn recover_unknown_upload_outcome(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    let lease = TierConfigMgr::acquire_operation_lease_for_backend_identity(
        &api.tier_config_mgr(),
        &transaction.tier_name,
        transaction.backend_fingerprint,
    )
    .await
    .map_err(Error::other)?;

    match lease
        .probe_transition_candidate_for(&transaction.remote_object, transaction.transaction_id)
        .await
        .map_err(Error::other)?
    {
        TransitionCandidateProbe::Missing => Ok(TransitionTransactionRecoveryOutcome::RecordDeleted),
        TransitionCandidateProbe::UnversionedPresent => {
            cleanup_recovered_unknown_upload_candidate(api, transaction, TransitionRemoteVersion::unversioned()).await
        }
        TransitionCandidateProbe::VersionedPresent(version_id)
            if Uuid::parse_str(&version_id).is_ok_and(|version_id| version_id.is_nil()) =>
        {
            Ok(TransitionTransactionRecoveryOutcome::RetainedAmbiguous(
                IlmRecoveryErrorCode::RemoteVersionUnknown,
            ))
        }
        TransitionCandidateProbe::VersionedPresent(version_id) => {
            cleanup_recovered_unknown_upload_candidate(api, transaction, TransitionRemoteVersion::versioned(version_id)).await
        }
        TransitionCandidateProbe::Ambiguous => Ok(TransitionTransactionRecoveryOutcome::RetainedAmbiguous(
            IlmRecoveryErrorCode::RemoteProbeAmbiguous,
        )),
        TransitionCandidateProbe::Unsupported => Ok(TransitionTransactionRecoveryOutcome::RetainedAmbiguous(
            IlmRecoveryErrorCode::RemoteProbeUnsupported,
        )),
    }
}

async fn cleanup_recovered_unknown_upload_candidate(
    api: Arc<ECStore>,
    transaction: &TransitionTransaction,
    remote_version: TransitionRemoteVersion,
) -> EcstoreResult<TransitionTransactionRecoveryOutcome> {
    let mut cleanup = transaction.clone();
    cleanup
        .mark_cleanup_pending(
            transaction.fence(),
            TransitionCleanupProof {
                transaction_id: transaction.transaction_id,
                write_id: transaction.write_id,
                remote_object: transaction.remote_object.clone(),
                remote_version,
                backend_fingerprint: transaction.backend_fingerprint,
                decision: TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation,
            },
        )
        .map_err(transition_transaction_store_error)?;
    match save_transition_transaction_record_if_current(api.clone(), transaction, &cleanup).await {
        Ok(()) => recover_cleanup_pending(api, &cleanup).await,
        Err(Error::PreconditionFailed) | Err(Error::ConfigNotFound) => Ok(TransitionTransactionRecoveryOutcome::Retained),
        Err(err) => Err(err),
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
    let opts = transition_source_lookup_options(transaction);
    let object = api
        .get_object_info(&transaction.source.bucket, &transaction.source.object, &opts)
        .await?;
    let transitioned = &object.transitioned_object;
    Ok(local_object_matches_transition_source(&object, &transaction.source)
        && transitioned.status == TRANSITION_COMPLETE
        && transitioned.name == transaction.remote_object
        && transitioned.tier == transaction.tier_name
        && transitioned.version_id == transaction.remote_version.tier_delete_version_id().unwrap_or_default())
}

fn local_object_matches_transition_source(object: &ObjectInfo, source: &TransitionSourceIdentity) -> bool {
    let observed_version_id = object.version_id.filter(|version_id| !version_id.is_nil());
    let observed_mod_time = object
        .mod_time
        .and_then(|mod_time| i64::try_from(mod_time.unix_timestamp_nanos()).ok());
    object.bucket == source.bucket
        && object.name == source.object
        && observed_version_id == source.version_id
        && object.data_dir == Some(source.data_dir)
        && observed_mod_time == Some(source.mod_time_unix_nanos)
        && object.size == source.size
        && object.etag.as_deref() == Some(source.etag.as_str())
}

fn transition_source_lookup_options(transaction: &TransitionTransaction) -> ObjectOptions {
    ObjectOptions {
        version_id: match transaction.source.version_mode {
            TransitionSourceVersionMode::Versioned => transaction.source.version_id.map(|version_id| version_id.to_string()),
            // Both modes identify the stored null version. Query it explicitly
            // so a later versioning change cannot redirect the proof to a new latest version.
            TransitionSourceVersionMode::Unversioned | TransitionSourceVersionMode::VersionSuspended => {
                Some(Uuid::nil().to_string())
            }
        },
        versioned: transaction.source.version_mode == TransitionSourceVersionMode::Versioned,
        version_suspended: transaction.source.version_mode == TransitionSourceVersionMode::VersionSuspended,
        metadata_cache_safe: false,
        ..Default::default()
    }
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
    recover_transition_transaction_records_with_now(api, limit, marker, None).await
}

#[cfg(feature = "test-util")]
pub async fn recover_transition_transaction_records_at(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
    now_unix_nanos: i128,
) -> EcstoreResult<TransitionTransactionRecoveryStats> {
    recover_transition_transaction_records_with_now(api, limit, marker, Some(now_unix_nanos)).await
}

async fn recover_transition_transaction_records_with_now(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
    now_unix_nanos: Option<i128>,
) -> EcstoreResult<TransitionTransactionRecoveryStats> {
    if limit == 0 {
        return Err(Error::other("transition transaction recovery limit must be greater than zero"));
    }

    let list_limit = i32::try_from(limit).unwrap_or(i32::MAX);
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
    if list.is_truncated && list.next_continuation_token.is_none() {
        return Err(Error::other(
            "transition transaction recovery returned a truncated page without a continuation marker",
        ));
    }

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

        let recovery = match now_unix_nanos {
            Some(now_unix_nanos) => {
                Box::pin(process_transition_transaction_record_at(api.clone(), &transaction, now_unix_nanos)).await
            }
            None => process_transition_transaction_record(api.clone(), &transaction).await,
        };
        match recovery {
            Ok(
                TransitionTransactionRecoveryOutcome::RemoteCandidateDeleted
                | TransitionTransactionRecoveryOutcome::RecordDeleted,
            ) => {
                stats.recovered += 1;
            }
            Ok(
                TransitionTransactionRecoveryOutcome::Retained
                | TransitionTransactionRecoveryOutcome::RetainedAmbiguous(_)
                | TransitionTransactionRecoveryOutcome::OperatorRequired(_),
            ) => {
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    const BACKEND_FINGERPRINT: [u8; 32] = [7; 32];

    struct RecoveryAttemptDropGuard(Arc<AtomicBool>);

    impl Drop for RecoveryAttemptDropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn pending_recovery_attempt(started: Arc<tokio::sync::Notify>, dropped: Arc<AtomicBool>) -> EcstoreResult<()> {
        let _drop_guard = RecoveryAttemptDropGuard(dropped);
        started.notify_one();
        std::future::pending().await
    }

    #[tokio::test(start_paused = true)]
    async fn transition_recovery_timeout_and_cancellation_drop_inflight_attempts() {
        let timeout_started = Arc::new(tokio::sync::Notify::new());
        let timeout_dropped = Arc::new(AtomicBool::new(false));
        let timeout_task = tokio::spawn({
            let started = Arc::clone(&timeout_started);
            let dropped = Arc::clone(&timeout_dropped);
            async move {
                await_transition_transaction_recovery(
                    &CancellationToken::new(),
                    TRANSITION_TRANSACTION_RECOVERY_TIMEOUT,
                    pending_recovery_attempt(started, dropped),
                )
                .await
            }
        });
        timeout_started.notified().await;
        tokio::time::advance(TRANSITION_TRANSACTION_RECOVERY_TIMEOUT).await;
        let timed_out = timeout_task.await.expect("timeout wrapper task should join");
        assert!(matches!(timed_out, Some(Err(_))), "outer timeout should fail the recovery pass");
        assert!(timeout_dropped.load(Ordering::SeqCst), "outer timeout must drop its in-flight attempt");

        let cancel_token = CancellationToken::new();
        let cancel_started = Arc::new(tokio::sync::Notify::new());
        let cancel_dropped = Arc::new(AtomicBool::new(false));
        let cancel_task = tokio::spawn({
            let cancel_token = cancel_token.clone();
            let started = Arc::clone(&cancel_started);
            let dropped = Arc::clone(&cancel_dropped);
            async move {
                await_transition_transaction_recovery(
                    &cancel_token,
                    TRANSITION_TRANSACTION_RECOVERY_TIMEOUT,
                    pending_recovery_attempt(started, dropped),
                )
                .await
            }
        });
        cancel_started.notified().await;
        cancel_token.cancel();
        let cancelled = cancel_task.await.expect("cancellation wrapper task should join");
        assert!(cancelled.is_none(), "outer cancellation should stop the recovery loop");
        assert!(
            cancel_dropped.load(Ordering::SeqCst),
            "outer cancellation must drop its in-flight attempt"
        );
    }

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

    #[test]
    fn operator_reconcile_requires_expired_unknown_upload_outcome() {
        let mut transaction = new_transaction();
        let active_deadline = transaction.not_after_unix_nanos;

        assert!(matches!(
            validate_operator_reconcile_transaction(&transaction, i128::from(active_deadline) + 1),
            Err(TransitionOperatorError::InvalidState(TransitionTransactionState::UploadStarted))
        ));

        transaction
            .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
            .expect("unknown upload outcome should be recorded");
        assert!(matches!(
            validate_operator_reconcile_transaction(&transaction, i128::from(active_deadline) - 1),
            Err(TransitionOperatorError::NotExpired)
        ));
        validate_operator_reconcile_transaction(&transaction, i128::from(active_deadline))
            .expect("expired unknown upload outcome should be eligible");
    }

    #[test]
    fn transition_ownership_window_expires_at_not_after() {
        let transaction = new_transaction();
        let deadline = i128::from(transaction.not_after_unix_nanos);

        assert!(transition_transaction_ownership_is_active(&transaction, deadline - 1));
        assert!(!transition_transaction_ownership_is_active(&transaction, deadline));
    }

    #[test]
    fn null_transition_source_lookup_targets_the_exact_version_shape() {
        for mode in [
            TransitionSourceVersionMode::Unversioned,
            TransitionSourceVersionMode::VersionSuspended,
        ] {
            let mut transaction = new_transaction();
            transaction.source.version_id = None;
            transaction.source.version_mode = mode;

            let opts = transition_source_lookup_options(&transaction);

            assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
            assert!(!opts.versioned);
            assert_eq!(opts.version_suspended, mode == TransitionSourceVersionMode::VersionSuspended);
            assert!(!opts.metadata_cache_safe);
        }
    }

    #[test]
    fn local_commit_proof_requires_the_complete_source_identity() {
        let source = source_identity(TransitionSourceVersionMode::Versioned);
        let exact = ObjectInfo {
            bucket: source.bucket.clone(),
            name: source.object.clone(),
            version_id: source.version_id,
            data_dir: Some(source.data_dir),
            mod_time: Some(
                time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(source.mod_time_unix_nanos))
                    .expect("source timestamp should be valid"),
            ),
            size: source.size,
            etag: Some(source.etag.clone()),
            ..Default::default()
        };
        assert!(local_object_matches_transition_source(&exact, &source));

        let mut changed = exact.clone();
        changed.version_id = Some(Uuid::new_v4());
        assert!(!local_object_matches_transition_source(&changed, &source));
        changed = exact.clone();
        changed.data_dir = Some(Uuid::new_v4());
        assert!(!local_object_matches_transition_source(&changed, &source));
        changed = exact.clone();
        changed.mod_time = changed.mod_time.map(|value| value + Duration::from_nanos(1));
        assert!(!local_object_matches_transition_source(&changed, &source));
        changed = exact.clone();
        changed.size += 1;
        assert!(!local_object_matches_transition_source(&changed, &source));
        changed = exact;
        changed.etag = Some("different-etag".to_string());
        assert!(!local_object_matches_transition_source(&changed, &source));
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
        let nil_version = Uuid::nil().to_string();
        let invalid_nil = TransitionRemoteVersion::known_from_put_response(nil_version.clone());
        assert_eq!(
            invalid_nil.tier_delete_version_id(),
            Some(nil_version.as_str()),
            "a non-empty version must never be downgraded to an unversioned DELETE"
        );
        assert!(matches!(
            invalid_nil.validate(),
            Err(TransitionTransactionError::Corrupt("versioned remote version is nil uuid"))
        ));

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

        assert!(matches!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::new_v4(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source: source_identity(TransitionSourceVersionMode::Unversioned),
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: [0; 32],
                not_after_unix_nanos: 1,
            }),
            Err(TransitionTransactionError::Corrupt("backend fingerprint is empty"))
        ));

        assert!(matches!(
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: Uuid::new_v4(),
                transaction_id: Uuid::new_v4(),
                owner_epoch: Uuid::new_v4(),
                write_id: Uuid::new_v4(),
                source: source_identity(TransitionSourceVersionMode::Unversioned),
                tier_name: "warm-tier".to_string(),
                backend_fingerprint: BACKEND_FINGERPRINT,
                not_after_unix_nanos: 0,
            }),
            Err(TransitionTransactionError::Corrupt("ownership deadline is not positive"))
        ));
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
        assert_eq!(
            transition_transaction_id_from_record_object_name(&object).expect("canonical record path should parse"),
            transaction_id
        );
        for malformed in [
            object.to_ascii_uppercase(),
            object.replace("/aa/aa/", "/ff/aa/"),
            object.replace("/aa/aa/", "/aa/aa/extra/"),
        ] {
            assert!(matches!(
                transition_transaction_id_from_record_object_name(&malformed),
                Err(TransitionTransactionError::Corrupt(_))
            ));
        }
    }
}
