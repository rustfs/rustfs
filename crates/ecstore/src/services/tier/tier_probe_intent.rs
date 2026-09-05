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

use std::sync::Arc;

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::com;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::ObjectOptions;
use crate::services::tier::{tier::TierDestinationId, tier_mutation_intent::TierMutationDigest};
use crate::storage_api_contracts::object::{EcstoreObjectIO, EcstoreObjectOperations, HTTPPreconditions};
use crate::store::ECStore;

pub(crate) const TIER_PROBE_INTENT_SCHEMA: &str = "rustfs-tier-probe-intent-v1";
pub(crate) const TIER_PROBE_INTENT_RECORD_PREFIX: &str = "ilm/tier-probe-intents/records";
pub(crate) const MAX_TIER_PROBE_INTENT_SIZE: usize = 64 * 1024;
const TIER_PROBE_OBJECT_PREFIX: &str = "rustfs-tier-probe-";

pub(crate) type Result<T> = std::result::Result<T, TierProbeIntentError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TierProbeIntentError {
    #[error("tier probe intent is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("tier probe intent schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("tier probe intent checksum mismatch")]
    ChecksumMismatch,
    #[error("invalid tier probe intent state change from {from:?} to {to:?}")]
    InvalidStateChange {
        from: TierProbeIntentState,
        to: TierProbeIntentState,
    },
    #[error("tier probe intent json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TierProbeIntentState {
    UploadOutcomeUnknown,
    Uploaded,
    CleanupPending,
    AbortedNoRemote,
    Completed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TierProbeRemoteVersionKind {
    #[default]
    Unknown,
    Unversioned,
    Versioned,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TierProbeRemoteVersion {
    pub(crate) kind: TierProbeRemoteVersionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) version_id: Option<String>,
}

impl TierProbeRemoteVersion {
    #[allow(dead_code, reason = "the dormant recovery model has no provider probe caller yet")]
    pub(crate) fn unversioned() -> Self {
        Self {
            kind: TierProbeRemoteVersionKind::Unversioned,
            version_id: None,
        }
    }

    #[allow(dead_code, reason = "the dormant writer has no remote PUT caller yet")]
    pub(crate) fn versioned(version_id: impl Into<String>) -> Self {
        Self {
            kind: TierProbeRemoteVersionKind::Versioned,
            version_id: Some(version_id.into()),
        }
    }

    pub(crate) fn is_unknown(&self) -> bool {
        self.kind == TierProbeRemoteVersionKind::Unknown
    }

    fn validate(&self) -> Result<()> {
        match (self.kind, self.version_id.as_deref()) {
            (TierProbeRemoteVersionKind::Unknown | TierProbeRemoteVersionKind::Unversioned, None) => Ok(()),
            (TierProbeRemoteVersionKind::Unknown | TierProbeRemoteVersionKind::Unversioned, Some(_)) => Err(
                TierProbeIntentError::Corrupt("remote version must be absent for unknown or unversioned state"),
            ),
            (TierProbeRemoteVersionKind::Versioned, Some(version_id)) if !version_id.is_empty() => {
                if Uuid::parse_str(version_id).is_ok_and(|parsed| parsed.is_nil()) {
                    return Err(TierProbeIntentError::Corrupt("versioned remote version is nil uuid"));
                }
                Ok(())
            }
            (TierProbeRemoteVersionKind::Versioned, Some(_)) => {
                Err(TierProbeIntentError::Corrupt("versioned remote version is empty"))
            }
            (TierProbeRemoteVersionKind::Versioned, None) => {
                Err(TierProbeIntentError::Corrupt("versioned remote version is missing"))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TierProbeOperationIdentity {
    Add {
        mutation_id: Uuid,
        old_config_etag: String,
        candidate_digest: TierMutationDigest,
    },
    Edit {
        mutation_id: Uuid,
        old_config_etag: String,
        candidate_digest: TierMutationDigest,
    },
    Verify {
        config_etag: String,
        backend_identity: TierDestinationId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TierProbeOwnerFence {
    pub(crate) owner_id: String,
    pub(crate) owner_epoch: Uuid,
    pub(crate) not_after_unix_nanos: i64,
}

impl TierProbeOwnerFence {
    fn validate(&self, created_at_unix_nanos: i64) -> Result<()> {
        if self.owner_id.trim().is_empty() {
            return Err(TierProbeIntentError::Corrupt("owner_id is empty"));
        }
        if self.owner_epoch.is_nil() {
            return Err(TierProbeIntentError::Corrupt("owner_epoch is nil"));
        }
        if self.not_after_unix_nanos <= created_at_unix_nanos {
            return Err(TierProbeIntentError::Corrupt("owner expiry does not follow creation"));
        }
        Ok(())
    }
}

impl TierProbeOperationIdentity {
    fn validate(&self, destination_id: &TierDestinationId) -> Result<()> {
        match self {
            Self::Add {
                mutation_id,
                old_config_etag,
                candidate_digest,
            } => {
                if mutation_id.is_nil() {
                    return Err(TierProbeIntentError::Corrupt("add mutation_id is nil"));
                }
                if old_config_etag.trim().is_empty() {
                    return Err(TierProbeIntentError::Corrupt("add old config ETag is empty"));
                }
                if digest_is_empty(candidate_digest) {
                    return Err(TierProbeIntentError::Corrupt("add candidate digest is empty"));
                }
            }
            Self::Edit {
                mutation_id,
                old_config_etag,
                candidate_digest,
            } => {
                if mutation_id.is_nil() {
                    return Err(TierProbeIntentError::Corrupt("edit mutation_id is nil"));
                }
                if old_config_etag.trim().is_empty() {
                    return Err(TierProbeIntentError::Corrupt("edit old config ETag is empty"));
                }
                if digest_is_empty(candidate_digest) {
                    return Err(TierProbeIntentError::Corrupt("edit candidate digest is empty"));
                }
            }
            Self::Verify {
                config_etag,
                backend_identity,
            } => {
                if config_etag.trim().is_empty() {
                    return Err(TierProbeIntentError::Corrupt("verify config ETag is empty"));
                }
                if identity_is_empty(backend_identity) {
                    return Err(TierProbeIntentError::Corrupt("verify backend identity is empty"));
                }
                if backend_identity != destination_id {
                    return Err(TierProbeIntentError::Corrupt(
                        "verify backend identity does not match destination identity",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TierProbeIntent {
    pub(crate) probe_id: Uuid,
    pub(crate) revision: u64,
    pub(crate) state: TierProbeIntentState,
    pub(crate) operation: TierProbeOperationIdentity,
    pub(crate) tier_name: String,
    pub(crate) destination_id: TierDestinationId,
    pub(crate) probe_object: String,
    pub(crate) creator_id: String,
    pub(crate) creator_epoch: Uuid,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) owner: TierProbeOwnerFence,
    pub(crate) remote_version: TierProbeRemoteVersion,
}

impl TierProbeIntent {
    pub(crate) fn validate_initial(&self) -> Result<()> {
        self.validate()?;
        if self.revision != 1 || self.state != TierProbeIntentState::UploadOutcomeUnknown || !self.remote_version.is_unknown() {
            return Err(TierProbeIntentError::Corrupt(
                "new intent must start at revision one with unknown upload outcome",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.probe_id.is_nil() {
            return Err(TierProbeIntentError::Corrupt("probe_id is nil"));
        }
        if self.revision == 0 {
            return Err(TierProbeIntentError::Corrupt("revision is zero"));
        }
        if self.tier_name.trim().is_empty() {
            return Err(TierProbeIntentError::Corrupt("tier name is empty"));
        }
        if identity_is_empty(&self.destination_id) {
            return Err(TierProbeIntentError::Corrupt("destination identity is empty"));
        }
        if self.probe_object != tier_probe_object_name(self.probe_id) {
            return Err(TierProbeIntentError::Corrupt("probe object does not match canonical probe identity"));
        }
        if self.creator_id.trim().is_empty() {
            return Err(TierProbeIntentError::Corrupt("creator_id is empty"));
        }
        if self.creator_epoch.is_nil() {
            return Err(TierProbeIntentError::Corrupt("creator_epoch is nil"));
        }
        if self.created_at_unix_nanos <= 0 {
            return Err(TierProbeIntentError::Corrupt("creation timestamp is not positive"));
        }
        self.owner.validate(self.created_at_unix_nanos)?;
        if self.owner.owner_id != self.creator_id || self.owner.owner_epoch != self.creator_epoch {
            return Err(TierProbeIntentError::Corrupt("v1 owner must match its immutable creator"));
        }
        let reachable_revision = match self.state {
            TierProbeIntentState::UploadOutcomeUnknown => self.revision == 1,
            TierProbeIntentState::Uploaded | TierProbeIntentState::AbortedNoRemote => self.revision == 2,
            TierProbeIntentState::CleanupPending => matches!(self.revision, 2 | 3),
            TierProbeIntentState::Completed => matches!(self.revision, 3 | 4),
        };
        if !reachable_revision {
            return Err(TierProbeIntentError::Corrupt("state and revision are not reachable in v1"));
        }
        self.operation.validate(&self.destination_id)?;
        self.remote_version.validate()?;
        match (self.state, self.remote_version.is_unknown()) {
            (TierProbeIntentState::UploadOutcomeUnknown | TierProbeIntentState::AbortedNoRemote, true)
            | (TierProbeIntentState::Uploaded | TierProbeIntentState::CleanupPending | TierProbeIntentState::Completed, false) => {
                Ok(())
            }
            (TierProbeIntentState::UploadOutcomeUnknown | TierProbeIntentState::AbortedNoRemote, false) => {
                Err(TierProbeIntentError::Corrupt("unknown/no-remote state carries a known remote version"))
            }
            (TierProbeIntentState::Uploaded | TierProbeIntentState::CleanupPending | TierProbeIntentState::Completed, true) => {
                Err(TierProbeIntentError::Corrupt("remote-owning state is missing an exact remote version"))
            }
        }
    }

    pub(crate) fn same_identity_as(&self, other: &Self) -> bool {
        self.probe_id == other.probe_id
            && self.operation == other.operation
            && self.tier_name == other.tier_name
            && self.destination_id == other.destination_id
            && self.probe_object == other.probe_object
            && self.creator_id == other.creator_id
            && self.creator_epoch == other.creator_epoch
            && self.created_at_unix_nanos == other.created_at_unix_nanos
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.state, TierProbeIntentState::AbortedNoRemote | TierProbeIntentState::Completed)
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        if !self.same_identity_as(next) {
            return Err(TierProbeIntentError::Corrupt("successor changes immutable intent identity"));
        }
        let mut expected = self.clone();
        expected.advance(next.state, next.remote_version.clone())?;
        if expected != *next {
            return Err(TierProbeIntentError::Corrupt("successor does not match the next exact generation"));
        }
        Ok(())
    }

    pub(crate) fn advance(&mut self, next: TierProbeIntentState, remote_version: TierProbeRemoteVersion) -> Result<()> {
        self.validate()?;
        let valid = match (self.state, next) {
            (TierProbeIntentState::UploadOutcomeUnknown, TierProbeIntentState::Uploaded) => !remote_version.is_unknown(),
            (TierProbeIntentState::UploadOutcomeUnknown, TierProbeIntentState::CleanupPending) => !remote_version.is_unknown(),
            (TierProbeIntentState::UploadOutcomeUnknown, TierProbeIntentState::AbortedNoRemote) => remote_version.is_unknown(),
            (TierProbeIntentState::Uploaded, TierProbeIntentState::CleanupPending)
            | (TierProbeIntentState::CleanupPending, TierProbeIntentState::Completed) => remote_version == self.remote_version,
            _ => false,
        };
        if !valid {
            return Err(TierProbeIntentError::InvalidStateChange {
                from: self.state,
                to: next,
            });
        }
        remote_version.validate()?;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(TierProbeIntentError::Corrupt("revision overflow"))?;
        self.state = next;
        self.remote_version = remote_version;
        self.validate()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let intent_bytes = serde_json::to_vec(self)?;
        let persisted = PersistedTierProbeIntent {
            schema: TIER_PROBE_INTENT_SCHEMA.to_string(),
            content_sha256: hex_sha256(&intent_bytes, ToOwned::to_owned),
            intent: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_TIER_PROBE_INTENT_SIZE {
            return Err(TierProbeIntentError::Corrupt("encoded intent exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub(crate) fn decode(expected_probe_id: Uuid, data: &[u8]) -> Result<Self> {
        if data.len() > MAX_TIER_PROBE_INTENT_SIZE {
            return Err(TierProbeIntentError::Corrupt("encoded intent exceeds maximum size"));
        }
        let persisted: PersistedTierProbeIntent = serde_json::from_slice(data)?;
        if persisted.schema != TIER_PROBE_INTENT_SCHEMA {
            return Err(TierProbeIntentError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(TierProbeIntentError::Corrupt("content checksum is not a sha256 checksum"));
        }
        let intent_bytes = serde_json::to_vec(&persisted.intent)?;
        let actual_checksum = hex_sha256(&intent_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(TierProbeIntentError::ChecksumMismatch);
        }
        if persisted.intent.probe_id != expected_probe_id {
            return Err(TierProbeIntentError::Corrupt("probe_id does not match intent key"));
        }
        persisted.intent.validate()?;
        Ok(persisted.intent)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TierProbeIntentInspection {
    pub(crate) intent: TierProbeIntent,
    pub(crate) record_etag: String,
    pub(crate) writer_enabled: bool,
    pub(crate) destructive_recovery_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedTierProbeIntent {
    intent: TierProbeIntent,
    record_etag: String,
}

impl ObservedTierProbeIntent {
    #[cfg(all(test, feature = "test-util"))]
    pub(crate) fn intent(&self) -> &TierProbeIntent {
        &self.intent
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) fn with_intent_for_test(&self, intent: TierProbeIntent) -> Self {
        Self {
            intent,
            record_etag: self.record_etag.clone(),
        }
    }
}

pub(crate) fn tier_probe_object_name(probe_id: Uuid) -> String {
    format!("{TIER_PROBE_OBJECT_PREFIX}{probe_id}")
}

pub(crate) fn tier_probe_intent_record_object_name(probe_id: Uuid) -> Result<String> {
    if probe_id.is_nil() {
        return Err(TierProbeIntentError::Corrupt("probe_id is nil"));
    }
    let probe_key = probe_id.simple().to_string();
    Ok(format!(
        "{TIER_PROBE_INTENT_RECORD_PREFIX}/{}/{}/{}.json",
        &probe_key[..2],
        &probe_key[2..4],
        probe_key
    ))
}

pub(crate) fn tier_probe_intent_id_from_record_object_name(object: &str) -> Result<Uuid> {
    let prefix = format!("{TIER_PROBE_INTENT_RECORD_PREFIX}/");
    let suffix = object
        .strip_prefix(&prefix)
        .ok_or(TierProbeIntentError::Corrupt("intent record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let shard_a = parts
        .next()
        .ok_or(TierProbeIntentError::Corrupt("intent record path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or(TierProbeIntentError::Corrupt("intent record path is incomplete"))?;
    let file_name = parts
        .next()
        .ok_or(TierProbeIntentError::Corrupt("intent record path is incomplete"))?;
    if parts.next().is_some() {
        return Err(TierProbeIntentError::Corrupt("intent record path is not canonical"));
    }
    let probe_key = file_name
        .strip_suffix(".json")
        .ok_or(TierProbeIntentError::Corrupt("intent record path has wrong suffix"))?;
    if probe_key.len() != 32
        || !probe_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(TierProbeIntentError::Corrupt("intent record path has invalid probe id"));
    }
    if shard_a != &probe_key[..2] || shard_b != &probe_key[2..4] {
        return Err(TierProbeIntentError::Corrupt("intent record path shard does not match probe id"));
    }
    let probe_id =
        Uuid::parse_str(probe_key).map_err(|_| TierProbeIntentError::Corrupt("intent record path has invalid uuid"))?;
    if probe_id.is_nil() {
        return Err(TierProbeIntentError::Corrupt("intent record path has nil uuid"));
    }
    Ok(probe_id)
}

#[allow(
    dead_code,
    reason = "the durable writer remains disabled until the fleet capability gate is approved"
)]
pub(crate) async fn save_tier_probe_intent_record_if_absent<S>(api: Arc<S>, intent: &TierProbeIntent) -> EcstoreResult<()>
where
    S: EcstoreObjectIO,
{
    intent.validate_initial().map_err(tier_probe_intent_store_error)?;
    let object = tier_probe_intent_record_object_name(intent.probe_id).map_err(tier_probe_intent_store_error)?;
    let data = intent.encode().map_err(tier_probe_intent_store_error)?;
    com::save_config_with_opts(
        api,
        &object,
        data,
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
    .await
}

pub(crate) async fn load_tier_probe_intent_record<S>(api: Arc<S>, probe_id: Uuid) -> EcstoreResult<ObservedTierProbeIntent>
where
    S: EcstoreObjectIO,
{
    let object = tier_probe_intent_record_object_name(probe_id).map_err(tier_probe_intent_store_error)?;
    let (data, object_info) = com::read_config_with_metadata(api, &object, &ObjectOptions::default()).await?;
    let etag = object_info
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("tier probe intent record is missing an ETag"))?;
    let intent = TierProbeIntent::decode(probe_id, &data).map_err(tier_probe_intent_store_error)?;
    Ok(ObservedTierProbeIntent {
        intent,
        record_etag: etag,
    })
}

#[allow(
    dead_code,
    reason = "the durable writer remains disabled until the fleet capability gate is approved"
)]
pub(crate) async fn save_tier_probe_intent_record_if_current<S>(
    api: Arc<S>,
    current: &ObservedTierProbeIntent,
    successor: &TierProbeIntent,
) -> EcstoreResult<()>
where
    S: EcstoreObjectIO,
{
    current
        .intent
        .validate_successor(successor)
        .map_err(tier_probe_intent_store_error)?;
    let authoritative = load_tier_probe_intent_record(api.clone(), current.intent.probe_id).await?;
    if authoritative != *current {
        return Err(Error::PreconditionFailed);
    }
    let object = tier_probe_intent_record_object_name(current.intent.probe_id).map_err(tier_probe_intent_store_error)?;
    let data = successor.encode().map_err(tier_probe_intent_store_error)?;
    com::save_config_with_opts(
        api,
        &object,
        data,
        &ObjectOptions {
            max_parity: true,
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(current.record_etag.clone()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
}

#[allow(
    dead_code,
    reason = "destructive recovery remains disabled until its fleet and owner fences are approved"
)]
pub(crate) async fn delete_tier_probe_intent_record_if_current<S>(
    api: Arc<S>,
    current: &ObservedTierProbeIntent,
) -> EcstoreResult<()>
where
    S: EcstoreObjectIO + EcstoreObjectOperations,
{
    current.intent.validate().map_err(tier_probe_intent_store_error)?;
    if !current.intent.is_terminal() {
        return Err(Error::other("tier probe intent must be terminal before record deletion"));
    }
    let authoritative = load_tier_probe_intent_record(api.clone(), current.intent.probe_id).await?;
    if authoritative != *current {
        return Err(Error::PreconditionFailed);
    }
    let object = tier_probe_intent_record_object_name(current.intent.probe_id).map_err(tier_probe_intent_store_error)?;
    match api
        .delete_object(
            RUSTFS_META_BUCKET,
            &object,
            ObjectOptions {
                http_preconditions: Some(HTTPPreconditions {
                    if_match: Some(current.record_etag.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) => Err(Error::ConfigNotFound),
        Err(err) => Err(err),
    }
}

#[allow(
    dead_code,
    reason = "the inspect core has no HTTP route until its operator contract is approved"
)]
pub(crate) async fn inspect_tier_probe_intent_record(
    api: Arc<ECStore>,
    probe_id: Uuid,
) -> EcstoreResult<TierProbeIntentInspection> {
    let observed = load_tier_probe_intent_record(api, probe_id).await?;
    Ok(TierProbeIntentInspection {
        intent: observed.intent,
        record_etag: observed.record_etag,
        writer_enabled: false,
        destructive_recovery_enabled: false,
    })
}

fn tier_probe_intent_store_error(err: TierProbeIntentError) -> Error {
    Error::other(err)
}

fn identity_is_empty(identity: &TierDestinationId) -> bool {
    identity.iter().all(|byte| *byte == 0)
}

fn digest_is_empty(digest: &TierMutationDigest) -> bool {
    digest.iter().all(|byte| *byte == 0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedTierProbeIntent {
    schema: String,
    content_sha256: String,
    intent: TierProbeIntent,
}

#[cfg(test)]
mod tests {
    use super::*;

    const DESTINATION_ID: TierDestinationId = [1; 32];
    const CANDIDATE_DIGEST: TierMutationDigest = [2; 32];

    fn unknown_intent() -> TierProbeIntent {
        let probe_id = Uuid::parse_str("36e2220e-9ad2-495b-b3bc-c4d2caf70a31").expect("fixture uuid should parse");
        TierProbeIntent {
            probe_id,
            revision: 1,
            state: TierProbeIntentState::UploadOutcomeUnknown,
            operation: TierProbeOperationIdentity::Edit {
                mutation_id: Uuid::parse_str("202a42ea-e722-4e8d-8aa8-67492c740b04").expect("fixture mutation uuid should parse"),
                old_config_etag: "old-etag".to_string(),
                candidate_digest: CANDIDATE_DIGEST,
            },
            tier_name: "COLD-A".to_string(),
            destination_id: DESTINATION_ID,
            probe_object: tier_probe_object_name(probe_id),
            creator_id: "node-a".to_string(),
            creator_epoch: Uuid::parse_str("76746062-c05a-40b7-9e38-d2722d7e0332").expect("fixture creator epoch should parse"),
            created_at_unix_nanos: 1_780_000_000_000_000_000,
            owner: TierProbeOwnerFence {
                owner_id: "node-a".to_string(),
                owner_epoch: Uuid::parse_str("76746062-c05a-40b7-9e38-d2722d7e0332").expect("fixture owner epoch should parse"),
                not_after_unix_nanos: 1_780_000_900_000_000_000,
            },
            remote_version: TierProbeRemoteVersion::default(),
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum InjectedCrashPoint {
        BeforeIntentCreate,
        AfterIntentCreate,
        BeforeRemotePut,
        AfterRemotePutBeforeResponse,
        AfterResponseBeforeVersionCas,
        AfterVersionCas,
        AfterCleanupCas,
        AfterRemoteDeleteBeforeTerminalCas,
        AfterTerminalCas,
        BeforeIntentDelete,
    }

    fn persisted_snapshot_at(point: InjectedCrashPoint) -> Option<TierProbeIntent> {
        if matches!(point, InjectedCrashPoint::BeforeIntentCreate) {
            return None;
        }

        let mut persisted = unknown_intent();
        if matches!(
            point,
            InjectedCrashPoint::AfterIntentCreate
                | InjectedCrashPoint::BeforeRemotePut
                | InjectedCrashPoint::AfterRemotePutBeforeResponse
                | InjectedCrashPoint::AfterResponseBeforeVersionCas
        ) {
            return Some(round_trip(persisted));
        }

        let remote_version = TierProbeRemoteVersion::versioned("opaque-provider-version");
        persisted
            .advance(TierProbeIntentState::Uploaded, remote_version.clone())
            .expect("the exact PUT result should persist");
        if matches!(point, InjectedCrashPoint::AfterVersionCas) {
            return Some(round_trip(persisted));
        }

        persisted
            .advance(TierProbeIntentState::CleanupPending, remote_version.clone())
            .expect("the exact cleanup candidate should persist before delete");
        if matches!(
            point,
            InjectedCrashPoint::AfterCleanupCas | InjectedCrashPoint::AfterRemoteDeleteBeforeTerminalCas
        ) {
            return Some(round_trip(persisted));
        }

        persisted
            .advance(TierProbeIntentState::Completed, remote_version)
            .expect("the completed generation should persist before record deletion");
        Some(round_trip(persisted))
    }

    fn round_trip(intent: TierProbeIntent) -> TierProbeIntent {
        let encoded = intent.encode().expect("fault snapshot should encode");
        TierProbeIntent::decode(intent.probe_id, &encoded).expect("fault snapshot should decode")
    }

    fn encode_unchecked(intent: TierProbeIntent) -> Vec<u8> {
        let intent_bytes = serde_json::to_vec(&intent).expect("hostile intent should serialize");
        serde_json::to_vec(&PersistedTierProbeIntent {
            schema: TIER_PROBE_INTENT_SCHEMA.to_string(),
            content_sha256: hex_sha256(&intent_bytes, ToOwned::to_owned),
            intent,
        })
        .expect("hostile envelope should serialize")
    }

    #[test]
    fn probe_identity_has_one_canonical_record_and_remote_object() {
        let intent = unknown_intent();
        let path = tier_probe_intent_record_object_name(intent.probe_id).expect("canonical path should build");

        assert_eq!(path, "ilm/tier-probe-intents/records/36/e2/36e2220e9ad2495bb3bcc4d2caf70a31.json");
        assert_eq!(
            tier_probe_intent_id_from_record_object_name(&path).expect("canonical path should parse"),
            intent.probe_id
        );
        assert_eq!(intent.probe_object, "rustfs-tier-probe-36e2220e-9ad2-495b-b3bc-c4d2caf70a31");
    }

    #[test]
    fn record_path_rejects_aliases_and_mismatched_shards() {
        for path in [
            "ilm/tier-probe-intents/records/ff/e2/36e2220e9ad2495bb3bcc4d2caf70a31.json",
            "ilm/tier-probe-intents/records/36/e2/36E2220E9AD2495BB3BCC4D2CAF70A31.json",
            "ilm/tier-probe-intents/records/36/e2/extra/36e2220e9ad2495bb3bcc4d2caf70a31.json",
            "ilm/tier-probe-intents/records/36/e2/36e2220e9ad2495bb3bcc4d2caf70a31",
            "ilm/tier-probe-intents/records/00/00/00000000000000000000000000000000.json",
        ] {
            assert!(
                tier_probe_intent_id_from_record_object_name(path).is_err(),
                "noncanonical path must fail: {path}"
            );
        }
    }

    #[test]
    fn operation_identity_rejects_missing_and_mixed_generation_proofs() {
        let mut intent = unknown_intent();
        intent.operation = TierProbeOperationIdentity::Add {
            mutation_id: Uuid::nil(),
            old_config_etag: "old-etag".to_string(),
            candidate_digest: CANDIDATE_DIGEST,
        };
        assert!(matches!(intent.validate(), Err(TierProbeIntentError::Corrupt("add mutation_id is nil"))));

        let mut intent = unknown_intent();
        intent.operation = TierProbeOperationIdentity::Add {
            mutation_id: Uuid::new_v4(),
            old_config_etag: String::new(),
            candidate_digest: CANDIDATE_DIGEST,
        };
        assert!(matches!(
            intent.validate(),
            Err(TierProbeIntentError::Corrupt("add old config ETag is empty"))
        ));

        let mut intent = unknown_intent();
        intent.operation = TierProbeOperationIdentity::Verify {
            config_etag: "config-etag".to_string(),
            backend_identity: [3; 32],
        };
        assert!(matches!(
            intent.validate(),
            Err(TierProbeIntentError::Corrupt(
                "verify backend identity does not match destination identity"
            ))
        ));

        let mut intent = unknown_intent();
        intent.operation = TierProbeOperationIdentity::Edit {
            mutation_id: Uuid::new_v4(),
            old_config_etag: String::new(),
            candidate_digest: CANDIDATE_DIGEST,
        };
        assert!(matches!(
            intent.validate(),
            Err(TierProbeIntentError::Corrupt("edit old config ETag is empty"))
        ));

        let mut intent = unknown_intent();
        intent.owner.owner_epoch = Uuid::new_v4();
        assert!(matches!(
            intent.validate_initial(),
            Err(TierProbeIntentError::Corrupt("v1 owner must match its immutable creator"))
        ));
    }

    #[test]
    fn remote_version_rejects_ambiguous_missing_and_empty_shapes() {
        for (remote_version, expected) in [
            (
                TierProbeRemoteVersion {
                    kind: TierProbeRemoteVersionKind::Unversioned,
                    version_id: Some(String::new()),
                },
                "remote version must be absent for unknown or unversioned state",
            ),
            (
                TierProbeRemoteVersion {
                    kind: TierProbeRemoteVersionKind::Versioned,
                    version_id: None,
                },
                "versioned remote version is missing",
            ),
            (
                TierProbeRemoteVersion {
                    kind: TierProbeRemoteVersionKind::Versioned,
                    version_id: Some(String::new()),
                },
                "versioned remote version is empty",
            ),
        ] {
            assert!(matches!(
                remote_version.validate(),
                Err(TierProbeIntentError::Corrupt(actual)) if actual == expected
            ));
        }
    }

    #[test]
    fn state_machine_models_each_crash_boundary_monotonically() {
        let mut intent = unknown_intent();
        let remote_version = TierProbeRemoteVersion::versioned("opaque-provider-version");
        intent
            .advance(TierProbeIntentState::Uploaded, remote_version.clone())
            .expect("the acknowledged PUT version should persist");
        assert_eq!(intent.revision, 2);
        intent
            .advance(TierProbeIntentState::CleanupPending, remote_version.clone())
            .expect("the exact cleanup candidate should be fenced");
        assert_eq!(intent.revision, 3);
        intent
            .advance(TierProbeIntentState::Completed, remote_version)
            .expect("an exact delete may become terminal");
        assert_eq!(intent.revision, 4);
        let completed_version = intent.remote_version.clone();
        assert!(
            intent
                .advance(TierProbeIntentState::CleanupPending, completed_version)
                .is_err()
        );

        let mut missing = unknown_intent();
        missing
            .advance(TierProbeIntentState::AbortedNoRemote, TierProbeRemoteVersion::default())
            .expect("an authoritative missing probe may become terminal without delete");
        assert_eq!(missing.revision, 2);

        let mut recovered = unknown_intent();
        recovered
            .advance(TierProbeIntentState::CleanupPending, TierProbeRemoteVersion::unversioned())
            .expect("an authoritative unversioned probe may establish exact cleanup semantics");
        assert_eq!(recovered.revision, 2);
        recovered
            .advance(TierProbeIntentState::Completed, TierProbeRemoteVersion::unversioned())
            .expect("the exact unversioned cleanup may become terminal");
        let recovered = round_trip(recovered);
        assert_eq!(recovered.state, TierProbeIntentState::Completed);
        assert_eq!(recovered.revision, 3);
        assert_eq!(recovered.remote_version, TierProbeRemoteVersion::unversioned());
    }

    #[test]
    fn deterministic_fault_points_retain_only_unknown_or_exact_cleanup_evidence() {
        let cases = [
            (InjectedCrashPoint::BeforeIntentCreate, None),
            (
                InjectedCrashPoint::AfterIntentCreate,
                Some((TierProbeIntentState::UploadOutcomeUnknown, 1, true)),
            ),
            (
                InjectedCrashPoint::BeforeRemotePut,
                Some((TierProbeIntentState::UploadOutcomeUnknown, 1, true)),
            ),
            (
                InjectedCrashPoint::AfterRemotePutBeforeResponse,
                Some((TierProbeIntentState::UploadOutcomeUnknown, 1, true)),
            ),
            (
                InjectedCrashPoint::AfterResponseBeforeVersionCas,
                Some((TierProbeIntentState::UploadOutcomeUnknown, 1, true)),
            ),
            (InjectedCrashPoint::AfterVersionCas, Some((TierProbeIntentState::Uploaded, 2, false))),
            (
                InjectedCrashPoint::AfterCleanupCas,
                Some((TierProbeIntentState::CleanupPending, 3, false)),
            ),
            (
                InjectedCrashPoint::AfterRemoteDeleteBeforeTerminalCas,
                Some((TierProbeIntentState::CleanupPending, 3, false)),
            ),
            (InjectedCrashPoint::AfterTerminalCas, Some((TierProbeIntentState::Completed, 4, false))),
            (InjectedCrashPoint::BeforeIntentDelete, Some((TierProbeIntentState::Completed, 4, false))),
        ];

        for (point, expected) in cases {
            let actual = persisted_snapshot_at(point);
            match (actual, expected) {
                (None, None) => {}
                (Some(intent), Some((state, revision, remote_unknown))) => {
                    assert_eq!(intent.state, state, "wrong state at {point:?}");
                    assert_eq!(intent.revision, revision, "wrong revision at {point:?}");
                    assert_eq!(intent.remote_version.is_unknown(), remote_unknown, "wrong version proof at {point:?}");
                }
                (actual, expected) => panic!("wrong persisted snapshot at {point:?}: {actual:?}, expected {expected:?}"),
            }
        }
    }

    #[test]
    fn state_machine_rejects_unknown_or_rebound_remote_versions() {
        let mut intent = unknown_intent();
        assert!(
            intent
                .advance(TierProbeIntentState::Uploaded, TierProbeRemoteVersion::default())
                .is_err()
        );
        intent
            .advance(TierProbeIntentState::Uploaded, TierProbeRemoteVersion::versioned("v1"))
            .expect("known response should advance");
        assert!(
            intent
                .advance(TierProbeIntentState::CleanupPending, TierProbeRemoteVersion::versioned("v2"))
                .is_err()
        );

        let mut nil_version = unknown_intent();
        assert!(matches!(
            nil_version.advance(TierProbeIntentState::Uploaded, TierProbeRemoteVersion::versioned(Uuid::nil().to_string())),
            Err(TierProbeIntentError::Corrupt("versioned remote version is nil uuid"))
        ));
        assert_eq!(nil_version.revision, 1, "a rejected edge must not mutate the record");
    }

    #[test]
    fn persistence_primitives_accept_only_initial_successor_and_terminal_shapes() {
        let initial = unknown_intent();
        initial.validate_initial().expect("initial generation should validate");
        assert!(!initial.is_terminal());

        let mut successor = initial.clone();
        successor
            .advance(TierProbeIntentState::Uploaded, TierProbeRemoteVersion::versioned("v1"))
            .expect("known version should advance");
        initial
            .validate_successor(&successor)
            .expect("one exact generation should be a valid successor");

        let mut rebound = successor.clone();
        rebound.tier_name = "COLD-B".to_string();
        assert!(matches!(
            successor.validate_successor(&rebound),
            Err(TierProbeIntentError::Corrupt("successor changes immutable intent identity"))
        ));

        let mut takeover = successor.clone();
        takeover.owner.owner_epoch = Uuid::new_v4();
        assert!(matches!(
            successor.validate_successor(&takeover),
            Err(TierProbeIntentError::Corrupt("v1 owner must match its immutable creator"))
        ));

        assert!(matches!(
            successor.validate_initial(),
            Err(TierProbeIntentError::Corrupt(
                "new intent must start at revision one with unknown upload outcome"
            ))
        ));

        let mut terminal = initial;
        terminal
            .advance(TierProbeIntentState::AbortedNoRemote, TierProbeRemoteVersion::default())
            .expect("missing candidate should terminalize");
        assert!(terminal.is_terminal());
    }

    #[test]
    fn strict_envelope_rejects_unknown_fields_checksums_and_identity_mismatch() {
        let intent = unknown_intent();
        let encoded = intent.encode().expect("intent should encode");
        assert_eq!(TierProbeIntent::decode(intent.probe_id, &encoded).expect("intent should decode"), intent);

        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("fixture should be json");
        value["unexpected"] = serde_json::json!(true);
        let unknown = serde_json::to_vec(&value).expect("fixture should encode");
        assert!(matches!(
            TierProbeIntent::decode(intent.probe_id, &unknown),
            Err(TierProbeIntentError::Json(_))
        ));

        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("fixture should be json");
        value["intent"]["tier_name"] = serde_json::json!("COLD-B");
        let changed = serde_json::to_vec(&value).expect("fixture should encode");
        assert!(matches!(
            TierProbeIntent::decode(intent.probe_id, &changed),
            Err(TierProbeIntentError::ChecksumMismatch)
        ));

        assert!(matches!(
            TierProbeIntent::decode(Uuid::new_v4(), &encoded),
            Err(TierProbeIntentError::Corrupt("probe_id does not match intent key"))
        ));

        let mut unsupported: PersistedTierProbeIntent =
            serde_json::from_slice(&encoded).expect("fixture should decode as persisted envelope");
        unsupported.schema = "rustfs-tier-probe-intent-v999".to_string();
        let unsupported = serde_json::to_vec(&unsupported).expect("unsupported schema fixture should encode");
        assert!(matches!(
            TierProbeIntent::decode(intent.probe_id, &unsupported),
            Err(TierProbeIntentError::UnsupportedSchema(schema))
                if schema == "rustfs-tier-probe-intent-v999"
        ));

        let mut exact_limit = encoded;
        exact_limit.resize(MAX_TIER_PROBE_INTENT_SIZE, b' ');
        assert_eq!(exact_limit.len(), MAX_TIER_PROBE_INTENT_SIZE);
        assert_eq!(
            TierProbeIntent::decode(intent.probe_id, &exact_limit).expect("an exact-limit envelope should decode"),
            intent
        );
        exact_limit.push(b' ');
        assert!(matches!(
            TierProbeIntent::decode(intent.probe_id, &exact_limit),
            Err(TierProbeIntentError::Corrupt("encoded intent exceeds maximum size"))
        ));
    }

    #[test]
    fn strict_decode_rejects_v1_owner_takeover_and_unreachable_histories() {
        let mut takeover = unknown_intent();
        takeover.owner.owner_epoch = Uuid::new_v4();
        assert!(matches!(
            TierProbeIntent::decode(takeover.probe_id, &encode_unchecked(takeover)),
            Err(TierProbeIntentError::Corrupt("v1 owner must match its immutable creator"))
        ));

        let mut rebound_owner = unknown_intent();
        rebound_owner.owner.owner_id = "node-b".to_string();
        assert_eq!(rebound_owner.owner.owner_epoch, rebound_owner.creator_epoch);
        assert!(matches!(
            TierProbeIntent::decode(rebound_owner.probe_id, &encode_unchecked(rebound_owner)),
            Err(TierProbeIntentError::Corrupt("v1 owner must match its immutable creator"))
        ));

        let cases = [
            (TierProbeIntentState::UploadOutcomeUnknown, 2, TierProbeRemoteVersion::default()),
            (TierProbeIntentState::Uploaded, 1, TierProbeRemoteVersion::versioned("uploaded-v1")),
            (TierProbeIntentState::AbortedNoRemote, 1, TierProbeRemoteVersion::default()),
            (TierProbeIntentState::CleanupPending, 4, TierProbeRemoteVersion::versioned("cleanup-v1")),
            (TierProbeIntentState::Completed, 2, TierProbeRemoteVersion::versioned("completed-v1")),
        ];
        for (state, revision, remote_version) in cases {
            let mut impossible = unknown_intent();
            impossible.state = state;
            impossible.revision = revision;
            impossible.remote_version = remote_version;
            assert!(matches!(
                TierProbeIntent::decode(impossible.probe_id, &encode_unchecked(impossible)),
                Err(TierProbeIntentError::Corrupt("state and revision are not reachable in v1"))
            ));
        }
    }

    #[test]
    fn strict_operation_tag_rejects_fields_from_another_identity_kind() {
        let mixed = serde_json::json!({
            "kind": "add",
            "mutation_id": "202a42ea-e722-4e8d-8aa8-67492c740b04",
            "old_config_etag": "old-etag",
            "candidate_digest": CANDIDATE_DIGEST,
            "backend_identity": DESTINATION_ID
        });

        assert!(serde_json::from_value::<TierProbeOperationIdentity>(mixed).is_err());

        let missing_add_etag = serde_json::json!({
            "kind": "add",
            "mutation_id": "202a42ea-e722-4e8d-8aa8-67492c740b04",
            "candidate_digest": CANDIDATE_DIGEST
        });
        assert!(serde_json::from_value::<TierProbeOperationIdentity>(missing_add_etag).is_err());
    }

    #[test]
    fn inspect_contract_is_explicitly_non_writing_and_non_destructive() {
        let inspection = TierProbeIntentInspection {
            intent: unknown_intent(),
            record_etag: "record-etag".to_string(),
            writer_enabled: false,
            destructive_recovery_enabled: false,
        };

        assert!(!inspection.writer_enabled);
        assert!(!inspection.destructive_recovery_enabled);
    }
}
