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
#[cfg(all(test, feature = "test-util"))]
use std::sync::atomic::{AtomicU8, Ordering};

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use super::config_boundary;
use super::recovery_control::{
    IlmRecoveryClassification, IlmRecoveryControl, IlmRecoveryProtocol, IlmRecoverySourceCopy, IlmRecoverySourceGeneration,
    MAX_ILM_RECOVERY_CONTROL_SIZE, load_recovery_control, observe_recovery_source, observe_recovery_source_no_lock,
    recovery_control_record_object_name,
};
use super::recovery_export::{IlmRecoveryExport, IlmRecoveryExportObservation, load_recovery_export, recovery_export_id};
use super::tier_delete_journal::{
    TIER_DELETE_JOURNAL_V1_RECOVERY_SCHEMA, TIER_DELETE_JOURNAL_V2_RECOVERY_SCHEMA, validate_legacy_tier_delete_recovery_path,
    validate_legacy_tier_delete_recovery_source,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::{ObjectOptions, WriteCompletion};
use crate::services::notification_sys::{
    IlmRecoveryExportFleetProofToken, acquire_ilm_recovery_export_fleet_proof, ilm_recovery_export_fleet_proof_matches,
    ilm_recovery_export_member_epochs_sha256, ilm_recovery_export_topology_generation,
};
use crate::storage_api_contracts::{
    namespace::NamespaceLocking as _,
    object::{HTTPPreconditions, ObjectOperations as _},
};
use crate::store::ECStore;

const ILM_RECOVERY_DISPOSITION_SCHEMA_V1: &str = "rustfs-ilm-recovery-disposition-v1";
pub const ILM_RECOVERY_DISPOSITION_SCHEMA: &str = "rustfs-ilm-recovery-disposition-v2";
pub const ILM_RECOVERY_DISPOSITION_PREFIX: &str = "ilm/recovery-dispositions";
pub const MAX_ILM_RECOVERY_DISPOSITION_SIZE: usize = 16 * 1024;
const DISPOSITION_RETENTION_NANOS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;
const DISPOSITION_OWNER_LEASE_NANOS: i64 = 5 * 60 * 1_000_000_000;
const MAX_LEGACY_TIER_DELETE_SOURCE_SIZE: usize = 64 * 1024;
const OWNER_FENCE_CHECKPOINT_DOMAIN: &[u8] = b"rustfs-ilm-recovery-disposition-owner-fence-v1";
const LEGACY_V1_UNKNOWN_ABANDONED_CONTROL_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[cfg(all(test, feature = "test-util"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryDispositionCrashStage {
    AfterLocalDelete = 1,
    AfterControlAbandon = 2,
}

#[cfg(all(test, feature = "test-util"))]
static RECOVERY_DISPOSITION_CRASH_STAGE: AtomicU8 = AtomicU8::new(0);

#[cfg(all(test, feature = "test-util"))]
pub(crate) fn inject_recovery_disposition_crash_once(stage: RecoveryDispositionCrashStage) {
    RECOVERY_DISPOSITION_CRASH_STAGE.store(stage as u8, Ordering::SeqCst);
}

#[cfg(all(test, feature = "test-util"))]
fn maybe_inject_recovery_disposition_crash(stage: RecoveryDispositionCrashStage) -> EcstoreResult<()> {
    if RECOVERY_DISPOSITION_CRASH_STAGE
        .compare_exchange(stage as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let message = match stage {
            RecoveryDispositionCrashStage::AfterLocalDelete => "injected ILM recovery disposition crash after local delete",
            RecoveryDispositionCrashStage::AfterControlAbandon => {
                "injected ILM recovery disposition crash after control abandonment"
            }
        };
        return Err(Error::other(message));
    }
    Ok(())
}

pub type Result<T> = std::result::Result<T, IlmRecoveryDispositionError>;

#[derive(Debug, thiserror::Error)]
pub enum IlmRecoveryDispositionError {
    #[error("ILM recovery disposition is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("ILM recovery disposition schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("ILM recovery disposition checksum mismatch")]
    ChecksumMismatch,
    #[error("ILM recovery disposition successor is invalid: {0}")]
    InvalidSuccessor(&'static str),
    #[error("ILM recovery disposition v1 requires safe migration: {0}")]
    LegacyV1MigrationRequired(&'static str),
    #[error("ILM recovery disposition json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryDispositionAction {
    AbandonRemoteCleanup,
}

impl IlmRecoveryDispositionAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AbandonRemoteCleanup => "abandon_remote_cleanup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryDispositionReasonCode {
    LegacyRemoteCleanupAbandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryDispositionState {
    Prepared,
    Applying,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryDispositionIdentity {
    pub disposition_id: String,
    pub protocol: IlmRecoveryProtocol,
    pub action: IlmRecoveryDispositionAction,
    pub export_id: String,
    pub export_content_sha256: String,
    pub control_id: String,
    pub control_etag: String,
    pub control_revision: u64,
    pub abandoned_control_sha256: String,
    pub canonical_source_path: String,
    pub source_generation: IlmRecoverySourceGeneration,
    pub admitted_topology_generation: String,
    pub admitted_member_epochs_sha256: String,
    pub actor_sha256: String,
    pub reason_code: IlmRecoveryDispositionReasonCode,
    pub confirmed_at_unix_nanos: i64,
}

impl IlmRecoveryDispositionIdentity {
    fn validate(&self) -> Result<()> {
        validate_sha256(&self.disposition_id, "disposition ID is invalid")?;
        validate_sha256(&self.export_id, "export ID is invalid")?;
        validate_sha256(&self.export_content_sha256, "export content checksum is invalid")?;
        validate_sha256(&self.control_id, "control ID is invalid")?;
        validate_sha256(&self.abandoned_control_sha256, "abandoned control content checksum is invalid")?;
        validate_sha256(&self.admitted_topology_generation, "admitted topology generation is invalid")?;
        validate_sha256(&self.admitted_member_epochs_sha256, "admitted member epoch digest is invalid")?;
        validate_sha256(&self.actor_sha256, "actor digest is invalid")?;
        if self.protocol != IlmRecoveryProtocol::TierDeleteJournal {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "only legacy tier-delete journals support disposition",
            ));
        }
        if self.control_etag.trim().is_empty() || self.control_revision == 0 {
            return Err(IlmRecoveryDispositionError::Corrupt("control generation is invalid"));
        }
        validate_legacy_tier_delete_recovery_path(&self.canonical_source_path)
            .map_err(|_| IlmRecoveryDispositionError::Corrupt("legacy source path is not canonical"))?;
        self.source_generation
            .validate()
            .map_err(|_| IlmRecoveryDispositionError::Corrupt("source generation is invalid"))?;
        if !matches!(
            self.source_generation.source_schema.as_str(),
            TIER_DELETE_JOURNAL_V1_RECOVERY_SCHEMA | TIER_DELETE_JOURNAL_V2_RECOVERY_SCHEMA
        ) {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "only legacy v1/v2 journal generations support disposition",
            ));
        }
        if self.source_generation.copies.iter().any(|copy| {
            copy.canonical_path != self.canonical_source_path
                || copy.etag != self.source_generation.source_etag
                || copy.content_sha256 != self.source_generation.content_sha256
                || copy.encoded_len == 0
                || copy.encoded_len > 64 * 1024
        }) {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "copy manifest does not describe one exact source generation",
            ));
        }
        if recovery_disposition_id(&self.export_id, self.action)? != self.disposition_id {
            return Err(IlmRecoveryDispositionError::Corrupt("disposition ID does not match export and action"));
        }
        if recovery_export_id(&self.control_id, &self.source_generation)
            .map_err(|_| IlmRecoveryDispositionError::Corrupt("export identity is invalid"))?
            != self.export_id
        {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "export ID does not match control and source generation",
            ));
        }
        if self.confirmed_at_unix_nanos <= 0 {
            return Err(IlmRecoveryDispositionError::Corrupt("confirmation timestamp is not positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryDispositionOwnerLease {
    pub owner_id: String,
    pub owner_epoch: Uuid,
    pub lease_acquired_at_unix_nanos: i64,
    pub lease_expires_at_unix_nanos: i64,
    pub topology_generation: String,
    pub member_epochs_sha256: String,
}

impl IlmRecoveryDispositionOwnerLease {
    fn validate(&self) -> Result<()> {
        if self.owner_id.trim().is_empty() || self.owner_epoch.is_nil() {
            return Err(IlmRecoveryDispositionError::Corrupt("owner fence is invalid"));
        }
        if self.lease_acquired_at_unix_nanos <= 0 || self.lease_expires_at_unix_nanos <= self.lease_acquired_at_unix_nanos {
            return Err(IlmRecoveryDispositionError::Corrupt("owner lease interval is invalid"));
        }
        validate_sha256(&self.topology_generation, "owner topology generation is invalid")?;
        validate_sha256(&self.member_epochs_sha256, "owner member epoch digest is invalid")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryDisposition {
    pub identity: IlmRecoveryDispositionIdentity,
    pub created_at_unix_nanos: i64,
    pub retain_until_unix_nanos: i64,
    pub revision: u64,
    pub state: IlmRecoveryDispositionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<IlmRecoveryDispositionOwnerLease>,
    pub confirmed_absent: Vec<String>,
    #[serde(skip)]
    legacy_v1_migration_required: bool,
}

impl IlmRecoveryDisposition {
    pub fn new(identity: IlmRecoveryDispositionIdentity, created_at_unix_nanos: i64) -> Result<Self> {
        let retain_until_unix_nanos = created_at_unix_nanos
            .checked_add(DISPOSITION_RETENTION_NANOS)
            .ok_or(IlmRecoveryDispositionError::Corrupt("retention timestamp overflow"))?;
        let disposition = Self {
            identity,
            created_at_unix_nanos,
            retain_until_unix_nanos,
            revision: 1,
            state: IlmRecoveryDispositionState::Prepared,
            owner: None,
            confirmed_absent: Vec::new(),
            legacy_v1_migration_required: false,
        };
        disposition.validate()?;
        Ok(disposition)
    }

    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.created_at_unix_nanos <= 0
            || self.created_at_unix_nanos < self.identity.confirmed_at_unix_nanos
            || self.retain_until_unix_nanos < self.created_at_unix_nanos.saturating_add(DISPOSITION_RETENTION_NANOS)
        {
            return Err(IlmRecoveryDispositionError::Corrupt("disposition retention is invalid"));
        }
        if self.revision == 0 {
            return Err(IlmRecoveryDispositionError::Corrupt("revision is zero"));
        }
        if let Some(owner) = &self.owner {
            owner.validate()?;
        }
        if !is_strictly_sorted_unique(&self.confirmed_absent) {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "confirmed-absent entries are not in unique canonical order",
            ));
        }
        let manifest_authorities: Vec<&str> = self
            .identity
            .source_generation
            .copies
            .iter()
            .map(|copy| copy.authority.as_str())
            .collect();
        if self
            .confirmed_absent
            .iter()
            .any(|authority| manifest_authorities.binary_search(&authority.as_str()).is_err())
        {
            return Err(IlmRecoveryDispositionError::Corrupt(
                "confirmed-absent entry is not in the immutable copy manifest",
            ));
        }
        match self.state {
            IlmRecoveryDispositionState::Prepared if !self.confirmed_absent.is_empty() => {
                Err(IlmRecoveryDispositionError::Corrupt("prepared disposition carries absence progress"))
            }
            IlmRecoveryDispositionState::Applying if self.owner.is_none() => {
                Err(IlmRecoveryDispositionError::Corrupt("applying disposition has no owner lease"))
            }
            IlmRecoveryDispositionState::Completed
                if self.owner.is_some() || self.confirmed_absent.len() != manifest_authorities.len() =>
            {
                Err(IlmRecoveryDispositionError::Corrupt(
                    "completed disposition does not cover its entire manifest",
                ))
            }
            _ => Ok(()),
        }
    }

    pub fn claim(&mut self, owner: IlmRecoveryDispositionOwnerLease) -> Result<()> {
        if self.state == IlmRecoveryDispositionState::Completed || self.owner.is_some() {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "only an unowned nonterminal disposition can be claimed",
            ));
        }
        owner.validate()?;
        self.bump_revision()?;
        self.owner = Some(owner);
        self.validate()
    }

    pub fn take_over(&mut self, owner: IlmRecoveryDispositionOwnerLease) -> Result<()> {
        let current = self
            .owner
            .as_ref()
            .ok_or(IlmRecoveryDispositionError::InvalidSuccessor("takeover requires an existing owner"))?;
        if self.state == IlmRecoveryDispositionState::Completed
            || owner.owner_epoch == current.owner_epoch
            || owner.lease_acquired_at_unix_nanos < current.lease_expires_at_unix_nanos
        {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor("takeover did not fence an expired owner"));
        }
        owner.validate()?;
        self.bump_revision()?;
        self.owner = Some(owner);
        self.validate()
    }

    pub fn begin_applying(&mut self) -> Result<()> {
        if self.state != IlmRecoveryDispositionState::Prepared || self.owner.is_none() {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "applying requires an owned prepared disposition",
            ));
        }
        self.bump_revision()?;
        self.state = IlmRecoveryDispositionState::Applying;
        self.validate()
    }

    pub fn confirm_absent(&mut self, authority: impl Into<String>) -> Result<()> {
        if self.state != IlmRecoveryDispositionState::Applying || self.owner.is_none() {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "absence progress requires an owned applying disposition",
            ));
        }
        let authority = authority.into();
        if self
            .identity
            .source_generation
            .copies
            .binary_search_by(|copy| copy.authority.cmp(&authority))
            .is_err()
        {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "absence progress does not name a manifest entry",
            ));
        }
        match self.confirmed_absent.binary_search(&authority) {
            Ok(_) => return Err(IlmRecoveryDispositionError::InvalidSuccessor("absence progress is already recorded")),
            Err(index) => self.confirmed_absent.insert(index, authority),
        }
        self.bump_revision()?;
        self.validate()
    }

    pub fn complete(&mut self) -> Result<()> {
        if self.state != IlmRecoveryDispositionState::Applying
            || self.confirmed_absent.len() != self.identity.source_generation.copies.len()
        {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "completion requires absence proof for the entire manifest",
            ));
        }
        self.bump_revision()?;
        self.state = IlmRecoveryDispositionState::Completed;
        self.owner = None;
        self.validate()
    }

    pub fn validate_successor(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        if self.identity != next.identity
            || self.created_at_unix_nanos != next.created_at_unix_nanos
            || self.retain_until_unix_nanos != next.retain_until_unix_nanos
        {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor("immutable identity changed"));
        }
        if self.revision.checked_add(1) != Some(next.revision) {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor("revision did not advance by one"));
        }
        if !is_sorted_subset(&self.confirmed_absent, &next.confirmed_absent) {
            return Err(IlmRecoveryDispositionError::InvalidSuccessor("confirmed-absent progress regressed"));
        }

        match (self.state, next.state) {
            (IlmRecoveryDispositionState::Prepared, IlmRecoveryDispositionState::Prepared)
            | (IlmRecoveryDispositionState::Applying, IlmRecoveryDispositionState::Applying) => {
                self.validate_same_state_successor(next)
            }
            (IlmRecoveryDispositionState::Prepared, IlmRecoveryDispositionState::Applying)
                if self.owner.is_some() && self.owner == next.owner && self.confirmed_absent == next.confirmed_absent =>
            {
                Ok(())
            }
            (IlmRecoveryDispositionState::Applying, IlmRecoveryDispositionState::Completed)
                if self.owner.is_some()
                    && next.owner.is_none()
                    && self.confirmed_absent == next.confirmed_absent
                    && self.confirmed_absent.len() == self.identity.source_generation.copies.len() =>
            {
                Ok(())
            }
            _ => Err(IlmRecoveryDispositionError::InvalidSuccessor("state transition is not allowed")),
        }
    }

    fn is_same_or_later_generation_of(&self, previous: &Self) -> bool {
        if self.identity != previous.identity
            || self.created_at_unix_nanos != previous.created_at_unix_nanos
            || self.retain_until_unix_nanos != previous.retain_until_unix_nanos
            || self.revision < previous.revision
            || !is_sorted_subset(&previous.confirmed_absent, &self.confirmed_absent)
        {
            return false;
        }
        if self.revision == previous.revision {
            return self == previous;
        }
        let Some(revision_distance) = self.revision.checked_sub(previous.revision) else {
            return false;
        };
        let previous_progress = previous.confirmed_absent.len();
        let current_progress = self.confirmed_absent.len();
        let remaining = self.identity.source_generation.copies.len().saturating_sub(previous_progress);
        let owner_change_is_fenced = || match (&previous.owner, &self.owner) {
            (Some(previous_owner), Some(current_owner)) if previous_owner != current_owner => {
                current_owner.lease_acquired_at_unix_nanos >= previous_owner.lease_expires_at_unix_nanos
            }
            _ => true,
        };
        let minimum_distance = match (previous.state, self.state) {
            (IlmRecoveryDispositionState::Prepared, IlmRecoveryDispositionState::Prepared) => {
                if self.owner.is_some()
                    && (previous.owner.is_none() || (previous.owner != self.owner && owner_change_is_fenced()))
                {
                    1
                } else {
                    return false;
                }
            }
            (IlmRecoveryDispositionState::Prepared, IlmRecoveryDispositionState::Applying) => {
                let owner_steps = if previous.owner.is_none() {
                    2
                } else if previous.owner == self.owner {
                    1
                } else if self.owner.is_some() && owner_change_is_fenced() {
                    2
                } else {
                    return false;
                };
                owner_steps + current_progress as u64
            }
            (IlmRecoveryDispositionState::Prepared, IlmRecoveryDispositionState::Completed) => {
                u64::from(previous.owner.is_none()) + 2 + self.identity.source_generation.copies.len() as u64
            }
            (IlmRecoveryDispositionState::Applying, IlmRecoveryDispositionState::Applying) => {
                let progress_steps = current_progress.saturating_sub(previous_progress) as u64;
                if previous.owner == self.owner && progress_steps > 0 {
                    progress_steps
                } else if previous.owner != self.owner && self.owner.is_some() && owner_change_is_fenced() {
                    1 + progress_steps
                } else {
                    return false;
                }
            }
            (IlmRecoveryDispositionState::Applying, IlmRecoveryDispositionState::Completed) => remaining as u64 + 1,
            _ => return false,
        };
        revision_distance >= minimum_distance
    }

    fn validate_same_state_successor(&self, next: &Self) -> Result<()> {
        match (&self.owner, &next.owner) {
            (None, Some(_)) if self.confirmed_absent == next.confirmed_absent => Ok(()),
            (Some(current), Some(candidate)) if current == candidate => {
                if self.state == IlmRecoveryDispositionState::Prepared && self.confirmed_absent != next.confirmed_absent {
                    Err(IlmRecoveryDispositionError::InvalidSuccessor(
                        "prepared disposition advanced absence progress",
                    ))
                } else if self.state == IlmRecoveryDispositionState::Applying
                    && self.confirmed_absent.len().checked_add(1) == Some(next.confirmed_absent.len())
                {
                    Ok(())
                } else {
                    Err(IlmRecoveryDispositionError::InvalidSuccessor(
                        "absence progress did not advance by one copy",
                    ))
                }
            }
            (Some(current), Some(candidate))
                if self.confirmed_absent == next.confirmed_absent
                    && candidate.owner_epoch != current.owner_epoch
                    && candidate.lease_acquired_at_unix_nanos >= current.lease_expires_at_unix_nanos =>
            {
                Ok(())
            }
            _ => Err(IlmRecoveryDispositionError::InvalidSuccessor(
                "same-state successor changed owner and progress together",
            )),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        if self.requires_legacy_v1_migration() {
            return Err(IlmRecoveryDispositionError::LegacyV1MigrationRequired(
                "legacy records cannot be emitted as v2 before their control successor is bound",
            ));
        }
        let disposition_bytes = serde_json::to_vec(self)?;
        let persisted = PersistedIlmRecoveryDisposition {
            schema: ILM_RECOVERY_DISPOSITION_SCHEMA.to_string(),
            content_sha256: hex_sha256(&disposition_bytes, ToOwned::to_owned),
            disposition: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_ILM_RECOVERY_DISPOSITION_SIZE {
            return Err(IlmRecoveryDispositionError::Corrupt("encoded disposition exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_disposition_id: &str, data: &[u8]) -> Result<Self> {
        validate_sha256(expected_disposition_id, "disposition ID is invalid")?;
        if data.len() > MAX_ILM_RECOVERY_DISPOSITION_SIZE {
            return Err(IlmRecoveryDispositionError::Corrupt("encoded disposition exceeds maximum size"));
        }
        let schema: PersistedIlmRecoveryDispositionSchema = serde_json::from_slice(data)?;
        let disposition = match schema.schema.as_str() {
            ILM_RECOVERY_DISPOSITION_SCHEMA => {
                let persisted: PersistedIlmRecoveryDisposition = serde_json::from_slice(data)?;
                if persisted.schema != ILM_RECOVERY_DISPOSITION_SCHEMA {
                    return Err(IlmRecoveryDispositionError::UnsupportedSchema(persisted.schema));
                }
                validate_sha256(&persisted.content_sha256, "content checksum is invalid")?;
                let disposition_bytes = serde_json::to_vec(&persisted.disposition)?;
                if hex_sha256(&disposition_bytes, ToOwned::to_owned) != persisted.content_sha256 {
                    return Err(IlmRecoveryDispositionError::ChecksumMismatch);
                }
                persisted.disposition
            }
            ILM_RECOVERY_DISPOSITION_SCHEMA_V1 => {
                let persisted: PersistedIlmRecoveryDispositionV1 = serde_json::from_slice(data)?;
                if persisted.schema != ILM_RECOVERY_DISPOSITION_SCHEMA_V1 {
                    return Err(IlmRecoveryDispositionError::UnsupportedSchema(persisted.schema));
                }
                validate_sha256(&persisted.content_sha256, "content checksum is invalid")?;
                let disposition_bytes = serde_json::to_vec(&persisted.disposition)?;
                if hex_sha256(&disposition_bytes, ToOwned::to_owned) != persisted.content_sha256 {
                    return Err(IlmRecoveryDispositionError::ChecksumMismatch);
                }
                persisted.disposition.into_current()
            }
            _ => return Err(IlmRecoveryDispositionError::UnsupportedSchema(schema.schema)),
        };
        disposition.validate()?;
        if disposition.identity.disposition_id != expected_disposition_id {
            return Err(IlmRecoveryDispositionError::Corrupt("disposition ID does not match record key"));
        }
        Ok(disposition)
    }

    fn requires_legacy_v1_migration(&self) -> bool {
        self.legacy_v1_migration_required
    }

    fn bump_revision(&mut self) -> Result<()> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(IlmRecoveryDispositionError::Corrupt("revision overflow"))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct PersistedIlmRecoveryDispositionSchema {
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryDispositionIdentityV1 {
    disposition_id: String,
    protocol: IlmRecoveryProtocol,
    action: IlmRecoveryDispositionAction,
    export_id: String,
    export_content_sha256: String,
    control_id: String,
    control_etag: String,
    control_revision: u64,
    canonical_source_path: String,
    source_generation: IlmRecoverySourceGeneration,
    admitted_topology_generation: String,
    admitted_member_epochs_sha256: String,
    actor_sha256: String,
    reason_code: IlmRecoveryDispositionReasonCode,
    confirmed_at_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryDispositionV1 {
    identity: IlmRecoveryDispositionIdentityV1,
    created_at_unix_nanos: i64,
    retain_until_unix_nanos: i64,
    revision: u64,
    state: IlmRecoveryDispositionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<IlmRecoveryDispositionOwnerLease>,
    confirmed_absent: Vec<String>,
}

impl IlmRecoveryDispositionV1 {
    fn into_current(self) -> IlmRecoveryDisposition {
        let retain_until_unix_nanos = if self.state == IlmRecoveryDispositionState::Completed {
            // A completed v1 record cannot prove the exact control successor.
            // Keep it quarantined and auditable instead of allowing retention GC.
            i64::MAX
        } else {
            self.retain_until_unix_nanos
        };
        IlmRecoveryDisposition {
            identity: IlmRecoveryDispositionIdentity {
                disposition_id: self.identity.disposition_id,
                protocol: self.identity.protocol,
                action: self.identity.action,
                export_id: self.identity.export_id,
                export_content_sha256: self.identity.export_content_sha256,
                control_id: self.identity.control_id,
                control_etag: self.identity.control_etag,
                control_revision: self.identity.control_revision,
                abandoned_control_sha256: LEGACY_V1_UNKNOWN_ABANDONED_CONTROL_SHA256.to_string(),
                canonical_source_path: self.identity.canonical_source_path,
                source_generation: self.identity.source_generation,
                admitted_topology_generation: self.identity.admitted_topology_generation,
                admitted_member_epochs_sha256: self.identity.admitted_member_epochs_sha256,
                actor_sha256: self.identity.actor_sha256,
                reason_code: self.identity.reason_code,
                confirmed_at_unix_nanos: self.identity.confirmed_at_unix_nanos,
            },
            created_at_unix_nanos: self.created_at_unix_nanos,
            retain_until_unix_nanos,
            revision: self.revision,
            state: self.state,
            owner: self.owner,
            confirmed_absent: self.confirmed_absent,
            legacy_v1_migration_required: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIlmRecoveryDispositionV1 {
    schema: String,
    content_sha256: String,
    disposition: IlmRecoveryDispositionV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIlmRecoveryDisposition {
    schema: String,
    content_sha256: String,
    disposition: IlmRecoveryDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedIlmRecoveryDisposition {
    pub disposition: IlmRecoveryDisposition,
    pub etag: String,
    pub content_sha256: String,
    pub encoded: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedIlmRecoveryDisposition {
    pub observed: ObservedIlmRecoveryDisposition,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IlmRecoveryDispositionDryRun {
    pub disposition_id: String,
    pub export_id: String,
    pub export_content_sha256: String,
    pub source_generation_sha256: String,
    pub copy_set_sha256: String,
    pub source_copy_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryDispositionExecutionOutcome {
    AcceptedForRecovery,
    Completed,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IlmRecoveryDispositionExecution {
    pub disposition_id: String,
    pub state: IlmRecoveryDispositionState,
    pub outcome: IlmRecoveryDispositionExecutionOutcome,
    pub confirmed_absent_copy_count: usize,
    pub source_copy_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedIlmRecoveryDispositionCheckpoint {
    pub(crate) disposition_id: String,
    pub(crate) content_sha256: String,
    pub(crate) identity_sha256: String,
    pub(crate) copy_manifest_sha256: String,
    pub(crate) copy_manifest_count: usize,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) revision: u64,
    pub(crate) state: IlmRecoveryDispositionState,
    pub(crate) owner_fence_sha256: Option<String>,
    pub(crate) owner_lease_acquired_at_unix_nanos: Option<i64>,
    pub(crate) owner_lease_expires_at_unix_nanos: Option<i64>,
    pub(crate) confirmed_absent_sha256: Vec<String>,
    pub(crate) retain_until_unix_nanos: i64,
}

pub fn recovery_disposition_id(export_id: &str, action: IlmRecoveryDispositionAction) -> Result<String> {
    validate_sha256(export_id, "export ID is invalid")?;
    Ok(length_delimited_digest(&[export_id.as_bytes(), action.as_str().as_bytes()]))
}

pub fn recovery_disposition_record_object_name(protocol: IlmRecoveryProtocol, disposition_id: &str) -> Result<String> {
    validate_sha256(disposition_id, "disposition ID is invalid")?;
    if protocol != IlmRecoveryProtocol::TierDeleteJournal {
        return Err(IlmRecoveryDispositionError::Corrupt("disposition protocol is unsupported"));
    }
    Ok(format!(
        "{}/{}/{}/{}/{}.json",
        ILM_RECOVERY_DISPOSITION_PREFIX,
        protocol.as_str(),
        &disposition_id[..2],
        &disposition_id[2..4],
        disposition_id
    ))
}

pub fn recovery_disposition_id_from_record_object_name(object: &str) -> Result<(IlmRecoveryProtocol, String)> {
    let suffix = object
        .strip_prefix(ILM_RECOVERY_DISPOSITION_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or(IlmRecoveryDispositionError::Corrupt("disposition record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let protocol = match parts.next() {
        Some("tier_delete_journal") => IlmRecoveryProtocol::TierDeleteJournal,
        _ => {
            return Err(IlmRecoveryDispositionError::Corrupt("disposition record protocol is invalid"));
        }
    };
    let shard_a = parts
        .next()
        .ok_or(IlmRecoveryDispositionError::Corrupt("disposition record path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or(IlmRecoveryDispositionError::Corrupt("disposition record path is incomplete"))?;
    let disposition_id = parts
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .ok_or(IlmRecoveryDispositionError::Corrupt("disposition record suffix is invalid"))?;
    if parts.next().is_some() {
        return Err(IlmRecoveryDispositionError::Corrupt("disposition record path is not canonical"));
    }
    validate_sha256(disposition_id, "disposition ID is invalid")?;
    if shard_a != &disposition_id[..2] || shard_b != &disposition_id[2..4] {
        return Err(IlmRecoveryDispositionError::Corrupt(
            "disposition record shard does not match disposition ID",
        ));
    }
    Ok((protocol, disposition_id.to_string()))
}

pub(crate) fn decode_recovery_disposition_checkpoint(
    path: &str,
    data: &[u8],
) -> EcstoreResult<DecodedIlmRecoveryDispositionCheckpoint> {
    let (protocol, disposition_id) = recovery_disposition_id_from_record_object_name(path).map_err(disposition_store_error)?;
    let disposition = IlmRecoveryDisposition::decode(&disposition_id, data).map_err(disposition_store_error)?;
    let canonical = recovery_disposition_record_object_name(protocol, &disposition_id).map_err(disposition_store_error)?;
    if canonical != path || disposition.identity.protocol != protocol {
        return Err(Error::other("ILM recovery disposition path is not canonical"));
    }
    let legacy_v1 = if disposition.requires_legacy_v1_migration() {
        Some(
            serde_json::from_slice::<PersistedIlmRecoveryDispositionV1>(data)
                .map_err(IlmRecoveryDispositionError::from)
                .map_err(disposition_store_error)?,
        )
    } else {
        None
    };
    let identity_sha256 = match legacy_v1.as_ref() {
        Some(persisted) => checkpoint_hash(&persisted.disposition.identity)?,
        None => checkpoint_hash(&disposition.identity)?,
    };
    let owner_fence_sha256 = disposition
        .owner
        .as_ref()
        .map(|owner| checkpoint_domain_hash(OWNER_FENCE_CHECKPOINT_DOMAIN, owner))
        .transpose()?;
    let mut confirmed_absent_sha256 = disposition
        .confirmed_absent
        .iter()
        .map(|authority| hex_sha256(authority.as_bytes(), ToOwned::to_owned))
        .collect::<Vec<_>>();
    confirmed_absent_sha256.sort_unstable();
    Ok(DecodedIlmRecoveryDispositionCheckpoint {
        disposition_id,
        content_sha256: hex_sha256(data, ToOwned::to_owned),
        identity_sha256,
        copy_manifest_sha256: disposition.identity.source_generation.copy_set_sha256.clone(),
        copy_manifest_count: disposition.identity.source_generation.copies.len(),
        created_at_unix_nanos: disposition.created_at_unix_nanos,
        revision: disposition.revision,
        state: disposition.state,
        owner_fence_sha256,
        owner_lease_acquired_at_unix_nanos: disposition.owner.as_ref().map(|owner| owner.lease_acquired_at_unix_nanos),
        owner_lease_expires_at_unix_nanos: disposition.owner.as_ref().map(|owner| owner.lease_expires_at_unix_nanos),
        confirmed_absent_sha256,
        retain_until_unix_nanos: legacy_v1.as_ref().map_or(disposition.retain_until_unix_nanos, |persisted| {
            persisted.disposition.retain_until_unix_nanos
        }),
    })
}

pub(crate) async fn create_recovery_disposition_if_absent(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
) -> EcstoreResult<CreatedIlmRecoveryDisposition> {
    if disposition.revision != 1
        || disposition.state != IlmRecoveryDispositionState::Prepared
        || disposition.owner.is_some()
        || !disposition.confirmed_absent.is_empty()
    {
        return Err(Error::PreconditionFailed);
    }
    let object = recovery_disposition_record_object_name(disposition.identity.protocol, &disposition.identity.disposition_id)
        .map_err(disposition_store_error)?;
    match load_recovery_disposition(api.clone(), disposition.identity.protocol, &disposition.identity.disposition_id).await {
        Ok(observed) if observed.disposition.is_same_or_later_generation_of(disposition) => {
            record_disposition_decommission_checkpoint(api.as_ref(), &object, &observed).await?;
            return Ok(CreatedIlmRecoveryDisposition {
                observed,
                replayed: true,
            });
        }
        Ok(_) => return Err(Error::PreconditionFailed),
        Err(err) if disposition_is_missing(&err) => {}
        Err(err) => return Err(err),
    }
    let control_object = recovery_control_record_object_name(disposition.identity.protocol, &disposition.identity.control_id)
        .map_err(|err| Error::other(err.to_string()))?;
    let control_lock = api.new_ns_lock(RUSTFS_META_BUCKET, &control_object).await?;
    let control_guard = control_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let source_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &disposition.identity.canonical_source_path)
        .await?;
    let source_guard = source_lock.get_read_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
    let locks_current = || !control_guard.is_lock_lost() && !source_guard.is_lock_lost();
    validate_disposition_sources(api.clone(), disposition, true).await?;
    let current_source = observe_recovery_source_no_lock(
        api.clone(),
        &disposition.identity.canonical_source_path,
        &disposition.identity.source_generation.source_schema,
    )
    .await?;
    if current_source.canonical_data.is_none()
        || current_source.generation != disposition.identity.source_generation
        || !locks_current()
    {
        return Err(Error::PreconditionFailed);
    }
    let encoded = disposition.encode().map_err(disposition_store_error)?;
    let mut write_options = ObjectOptions {
        max_parity: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    write_options.add_namespace_lock_guard(&control_guard);
    write_options.add_namespace_lock_guard(&source_guard);
    if !locks_current() {
        return Err(Error::PreconditionFailed);
    }
    let write_result = config_boundary::save_config_with_opts(api.clone(), &object, encoded.clone(), &write_options).await;
    match load_recovery_disposition(api.clone(), disposition.identity.protocol, &disposition.identity.disposition_id).await {
        Ok(observed) if observed.disposition.is_same_or_later_generation_of(disposition) => {
            record_disposition_decommission_checkpoint(api.as_ref(), &object, &observed).await?;
            if !locks_current() {
                return Err(Error::PreconditionFailed);
            }
            let replayed = write_result.is_err() || observed.encoded != encoded;
            Ok(CreatedIlmRecoveryDisposition { observed, replayed })
        }
        Ok(_) => Err(Error::PreconditionFailed),
        Err(read_err) => Err(write_result.err().unwrap_or(read_err)),
    }
}

async fn validate_disposition_sources(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
    no_lock: bool,
) -> EcstoreResult<()> {
    let stored_export = load_recovery_export(api.clone(), &disposition.identity.export_id).await?;
    if stored_export.content_sha256 != disposition.identity.export_content_sha256 {
        return Err(Error::PreconditionFailed);
    }
    let export = IlmRecoveryExport::decode(&stored_export.export_id, &stored_export.encoded)?;
    let observed_control = if no_lock {
        load_recovery_control_no_lock(api, disposition).await?
    } else {
        load_recovery_control(api, disposition.identity.protocol, &disposition.identity.control_id).await?
    };
    if export.control_id != disposition.identity.control_id
        || export.protocol != disposition.identity.protocol
        || export.control_etag != disposition.identity.control_etag
        || export.control_revision != disposition.identity.control_revision
        || export.classification != IlmRecoveryClassification::RetainedAmbiguous
        || export.topology_generation != disposition.identity.admitted_topology_generation
        || export.member_epochs_sha256 != disposition.identity.admitted_member_epochs_sha256
        || export.canonical_source_path != disposition.identity.canonical_source_path
        || export.source_generation != disposition.identity.source_generation
        || observed_control.etag != disposition.identity.control_etag
        || observed_control.control.revision != disposition.identity.control_revision
        || observed_control.control.classification != IlmRecoveryClassification::RetainedAmbiguous
        || observed_control.control.identity.canonical_source_path != disposition.identity.canonical_source_path
        || observed_control.control.observed_source_generation != disposition.identity.source_generation
        || expected_abandoned_control_sha256(&observed_control.control, &disposition.identity.source_generation)?
            != disposition.identity.abandoned_control_sha256
    {
        return Err(Error::PreconditionFailed);
    }
    Ok(())
}

pub async fn dry_run_recovery_disposition(
    api: Arc<ECStore>,
    observation: &IlmRecoveryExportObservation,
    export_id: &str,
    export_content_sha256: &str,
    actor_sha256: &str,
    confirmed_at_unix_nanos: i64,
) -> EcstoreResult<IlmRecoveryDispositionDryRun> {
    let (candidate, proof) = prepare_recovery_disposition(
        api.clone(),
        observation,
        export_id,
        export_content_sha256,
        actor_sha256,
        confirmed_at_unix_nanos,
    )
    .await?;
    match load_recovery_disposition(api.clone(), candidate.identity.protocol, &candidate.identity.disposition_id).await {
        Ok(existing) => {
            validate_existing_request_binding(&existing.disposition, &candidate.identity)?;
            if existing.disposition.requires_legacy_v1_migration() {
                return Err(legacy_v1_migration_error(
                    "v1 records require an explicit migration before disposition execution",
                ));
            }
            if existing.disposition.state != IlmRecoveryDispositionState::Completed {
                validate_live_disposition_inputs(api, &existing.disposition, &proof, false).await?;
            }
        }
        Err(err) if disposition_is_missing(&err) => {
            validate_disposition_sources(api.clone(), &candidate, false).await?;
            let observed_source = observe_recovery_source(
                api,
                &candidate.identity.canonical_source_path,
                &candidate.identity.source_generation.source_schema,
            )
            .await?;
            if observed_source.canonical_data.is_none() || observed_source.generation != candidate.identity.source_generation {
                return Err(Error::PreconditionFailed);
            }
        }
        Err(err) => return Err(err),
    }
    if !ilm_recovery_export_fleet_proof_matches(&proof).await {
        return Err(Error::PreconditionFailed);
    }
    Ok(IlmRecoveryDispositionDryRun {
        disposition_id: candidate.identity.disposition_id,
        export_id: export_id.to_string(),
        export_content_sha256: export_content_sha256.to_string(),
        source_generation_sha256: checkpoint_hash(&observation.source_generation)?,
        copy_set_sha256: observation.source_generation.copy_set_sha256.clone(),
        source_copy_count: observation.source_generation.copies.len(),
    })
}

pub async fn execute_recovery_disposition(
    api: Arc<ECStore>,
    observation: &IlmRecoveryExportObservation,
    export_id: &str,
    export_content_sha256: &str,
    actor_sha256: &str,
    confirmed_at_unix_nanos: i64,
) -> EcstoreResult<IlmRecoveryDispositionExecution> {
    let (candidate, proof) = prepare_recovery_disposition(
        api.clone(),
        observation,
        export_id,
        export_content_sha256,
        actor_sha256,
        confirmed_at_unix_nanos,
    )
    .await?;
    let disposition_id = candidate.identity.disposition_id.clone();
    match load_recovery_disposition(api.clone(), candidate.identity.protocol, &disposition_id).await {
        Ok(existing) => {
            validate_existing_request_binding(&existing.disposition, &candidate.identity)?;
            if existing.disposition.requires_legacy_v1_migration() {
                return Err(legacy_v1_migration_error(
                    "v1 records require an explicit migration before disposition execution",
                ));
            }
            if existing.disposition.state == IlmRecoveryDispositionState::Completed {
                return Ok(disposition_execution(
                    &existing.disposition,
                    IlmRecoveryDispositionExecutionOutcome::Replayed,
                ));
            }
            validate_live_disposition_inputs(api.clone(), &existing.disposition, &proof, false).await?;
        }
        Err(err) if disposition_is_missing(&err) => {
            validate_disposition_sources(api.clone(), &candidate, false).await?;
            let created = super::recovery_disposition_runtime::create_recovery_disposition_with_admission(
                api.clone(),
                &candidate,
                confirmed_at_unix_nanos,
            )
            .await?;
            validate_existing_request_binding(&created.observed.disposition, &candidate.identity)?;
        }
        Err(err) => return Err(err),
    }
    if !ilm_recovery_export_fleet_proof_matches(&proof).await {
        return Err(Error::PreconditionFailed);
    }
    let _execution_permit = super::recovery_disposition_runtime::acquire_recovery_disposition_execution_permit().await?;
    resume_recovery_disposition(api, observation.protocol, &disposition_id, confirmed_at_unix_nanos).await
}

pub(crate) async fn resume_recovery_disposition(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    disposition_id: &str,
    now_unix_nanos: i64,
) -> EcstoreResult<IlmRecoveryDispositionExecution> {
    if now_unix_nanos <= 0 {
        return Err(Error::PreconditionFailed);
    }
    let initial = load_recovery_disposition(api.clone(), protocol, disposition_id).await?;
    if initial.disposition.requires_legacy_v1_migration() {
        return Err(legacy_v1_migration_error(
            "v1 records require an explicit migration before disposition execution",
        ));
    }
    if initial.disposition.state == IlmRecoveryDispositionState::Completed {
        return Ok(disposition_execution(
            &initial.disposition,
            IlmRecoveryDispositionExecutionOutcome::Replayed,
        ));
    }
    let proof = acquire_matching_disposition_proof(&initial.disposition).await?;
    let control_object = recovery_control_record_object_name(protocol, &initial.disposition.identity.control_id)
        .map_err(|_| Error::PreconditionFailed)?;
    // Lock order is control record, then legacy source. Disposition revisions
    // use ETag CAS and never acquire either namespace lock internally.
    let control_lock = api.new_ns_lock(RUSTFS_META_BUCKET, &control_object).await?;
    let control_guard = control_lock
        .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let source_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &initial.disposition.identity.canonical_source_path)
        .await?;
    let source_guard = source_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let fences_current = || !control_guard.is_lock_lost() && !source_guard.is_lock_lost();
    if !fences_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
        return Err(Error::PreconditionFailed);
    }

    let mut current = load_recovery_disposition(api.clone(), protocol, disposition_id).await?;
    if current.disposition.requires_legacy_v1_migration() {
        return Err(legacy_v1_migration_error(
            "v1 records require an explicit migration before disposition execution",
        ));
    }
    if current.disposition.state == IlmRecoveryDispositionState::Completed {
        return Ok(disposition_execution(
            &current.disposition,
            IlmRecoveryDispositionExecutionOutcome::Replayed,
        ));
    }
    validate_live_disposition_inputs(api.clone(), &current.disposition, &proof, true).await?;
    if current.disposition.owner.as_ref().is_some_and(|owner| {
        owner.topology_generation != current.disposition.identity.admitted_topology_generation
            || owner.member_epochs_sha256 != current.disposition.identity.admitted_member_epochs_sha256
    }) {
        return Err(Error::PreconditionFailed);
    }

    let owner = match current.disposition.owner.as_ref() {
        Some(owner) if owner.owner_id == api.id.to_string() && owner.lease_expires_at_unix_nanos > now_unix_nanos => {
            owner.clone()
        }
        Some(owner) if owner.lease_expires_at_unix_nanos <= now_unix_nanos => {
            let replacement = new_disposition_owner(&api, &proof, now_unix_nanos)?;
            let mut next = current.disposition.clone();
            next.take_over(replacement.clone()).map_err(disposition_store_error)?;
            current = save_recovery_disposition_step_fenced(api.clone(), &current, &next, &control_guard, &source_guard, &proof)
                .await?;
            replacement
        }
        Some(_) => {
            return Ok(disposition_execution(
                &current.disposition,
                IlmRecoveryDispositionExecutionOutcome::AcceptedForRecovery,
            ));
        }
        None => {
            let owner = new_disposition_owner(&api, &proof, now_unix_nanos)?;
            let mut next = current.disposition.clone();
            next.claim(owner.clone()).map_err(disposition_store_error)?;
            current = save_recovery_disposition_step_fenced(api.clone(), &current, &next, &control_guard, &source_guard, &proof)
                .await?;
            owner
        }
    };
    if current.disposition.owner.as_ref() != Some(&owner) {
        return Ok(disposition_execution(
            &current.disposition,
            IlmRecoveryDispositionExecutionOutcome::AcceptedForRecovery,
        ));
    }
    if current.disposition.state == IlmRecoveryDispositionState::Prepared {
        let mut next = current.disposition.clone();
        next.begin_applying().map_err(disposition_store_error)?;
        current =
            save_recovery_disposition_step_fenced(api.clone(), &current, &next, &control_guard, &source_guard, &proof).await?;
    }

    for copy in current.disposition.identity.source_generation.copies.clone() {
        if current.disposition.confirmed_absent.binary_search(&copy.authority).is_ok() {
            continue;
        }
        ensure_owned_execution_fence(&current.disposition, &owner, wall_clock_unix_nanos()?)?;
        if !fences_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
            return Err(Error::PreconditionFailed);
        }
        delete_exact_local_recovery_copy(
            api.clone(),
            &copy,
            &current.disposition.identity.source_generation.source_schema,
            &control_guard,
            &source_guard,
        )
        .await?;
        #[cfg(all(test, feature = "test-util"))]
        maybe_inject_recovery_disposition_crash(RecoveryDispositionCrashStage::AfterLocalDelete)?;
        if !fences_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
            return Err(Error::PreconditionFailed);
        }
        ensure_owned_execution_fence(&current.disposition, &owner, wall_clock_unix_nanos()?)?;
        let mut next = current.disposition.clone();
        next.confirm_absent(copy.authority).map_err(disposition_store_error)?;
        current =
            save_recovery_disposition_step_fenced(api.clone(), &current, &next, &control_guard, &source_guard, &proof).await?;
        if current.disposition.owner.as_ref() != Some(&owner) {
            return Ok(disposition_execution(
                &current.disposition,
                IlmRecoveryDispositionExecutionOutcome::AcceptedForRecovery,
            ));
        }
        ensure_owned_execution_fence(&current.disposition, &owner, wall_clock_unix_nanos()?)?;
    }
    if current.disposition.state != IlmRecoveryDispositionState::Completed {
        ensure_owned_execution_fence(&current.disposition, &owner, wall_clock_unix_nanos()?)?;
        abandon_recovery_control_fenced(api.clone(), &current.disposition, &control_guard, &source_guard, &proof).await?;
        #[cfg(all(test, feature = "test-util"))]
        maybe_inject_recovery_disposition_crash(RecoveryDispositionCrashStage::AfterControlAbandon)?;
        ensure_owned_execution_fence(&current.disposition, &owner, wall_clock_unix_nanos()?)?;
        let mut next = current.disposition.clone();
        next.complete().map_err(disposition_store_error)?;
        current = save_recovery_disposition_step_fenced(api, &current, &next, &control_guard, &source_guard, &proof).await?;
    }
    let outcome = if current.disposition.state == IlmRecoveryDispositionState::Completed {
        IlmRecoveryDispositionExecutionOutcome::Completed
    } else {
        IlmRecoveryDispositionExecutionOutcome::AcceptedForRecovery
    };
    Ok(disposition_execution(&current.disposition, outcome))
}

async fn prepare_recovery_disposition(
    api: Arc<ECStore>,
    observation: &IlmRecoveryExportObservation,
    export_id: &str,
    export_content_sha256: &str,
    actor_sha256: &str,
    confirmed_at_unix_nanos: i64,
) -> EcstoreResult<(IlmRecoveryDisposition, IlmRecoveryExportFleetProofToken)> {
    validate_sha256(export_id, "export ID is invalid").map_err(disposition_store_error)?;
    validate_sha256(export_content_sha256, "export content checksum is invalid").map_err(disposition_store_error)?;
    validate_sha256(actor_sha256, "actor digest is invalid").map_err(disposition_store_error)?;
    if confirmed_at_unix_nanos <= 0 {
        return Err(Error::PreconditionFailed);
    }
    let proof = acquire_ilm_recovery_export_fleet_proof()
        .await
        .ok_or(Error::PreconditionFailed)?;
    if ilm_recovery_export_topology_generation(&proof) != observation.topology_generation
        || ilm_recovery_export_member_epochs_sha256(&proof) != observation.member_epochs_sha256
        || !ilm_recovery_export_fleet_proof_matches(&proof).await
    {
        return Err(Error::PreconditionFailed);
    }
    let stored_export = load_recovery_export(api.clone(), export_id).await?;
    if stored_export.content_sha256 != export_content_sha256 {
        return Err(Error::PreconditionFailed);
    }
    let export = IlmRecoveryExport::decode(export_id, &stored_export.encoded)?;
    if !export_matches_observation(&export, observation) {
        return Err(Error::PreconditionFailed);
    }
    let observed_control = load_recovery_control(api, observation.protocol, &observation.control_id).await?;
    let retained_predecessor_is_exact = observed_control.etag == observation.control_etag
        && observed_control.control.revision == observation.control_revision
        && observed_control.control.classification == IlmRecoveryClassification::RetainedAmbiguous
        && observed_control.control.observed_source_generation == observation.source_generation;
    let abandoned_successor_is_exact = observation.control_revision.checked_add(1) == Some(observed_control.control.revision)
        && observed_control.control.classification == IlmRecoveryClassification::Abandoned
        && observed_control.control.owner.is_none()
        && observed_control.control.identity.protocol == observation.protocol
        && observed_control.control.identity.canonical_source_path == observation.canonical_source_path
        && observed_control.control.observed_source_generation == observation.source_generation;
    let abandoned_control_sha256 = if retained_predecessor_is_exact {
        expected_abandoned_control_sha256(&observed_control.control, &observation.source_generation)?
    } else if abandoned_successor_is_exact {
        let encoded = observed_control.control.encode().map_err(|_| Error::PreconditionFailed)?;
        hex_sha256(&encoded, ToOwned::to_owned)
    } else {
        return Err(Error::PreconditionFailed);
    };
    let action = IlmRecoveryDispositionAction::AbandonRemoteCleanup;
    let identity = IlmRecoveryDispositionIdentity {
        disposition_id: recovery_disposition_id(export_id, action).map_err(disposition_store_error)?,
        protocol: observation.protocol,
        action,
        export_id: export_id.to_string(),
        export_content_sha256: export_content_sha256.to_string(),
        control_id: observation.control_id.clone(),
        control_etag: observation.control_etag.clone(),
        control_revision: observation.control_revision,
        abandoned_control_sha256,
        canonical_source_path: observation.canonical_source_path.clone(),
        source_generation: observation.source_generation.clone(),
        admitted_topology_generation: observation.topology_generation.clone(),
        admitted_member_epochs_sha256: observation.member_epochs_sha256.clone(),
        actor_sha256: actor_sha256.to_string(),
        reason_code: IlmRecoveryDispositionReasonCode::LegacyRemoteCleanupAbandoned,
        confirmed_at_unix_nanos,
    };
    Ok((
        IlmRecoveryDisposition::new(identity, confirmed_at_unix_nanos).map_err(disposition_store_error)?,
        proof,
    ))
}

fn export_matches_observation(export: &IlmRecoveryExport, observation: &IlmRecoveryExportObservation) -> bool {
    export.control_id == observation.control_id
        && export.protocol == observation.protocol
        && export.control_etag == observation.control_etag
        && export.control_revision == observation.control_revision
        && export.classification == observation.classification
        && export.canonical_source_path == observation.canonical_source_path
        && export.source_generation == observation.source_generation
        && export.topology_generation == observation.topology_generation
        && export.member_epochs_sha256 == observation.member_epochs_sha256
}

fn validate_existing_request_binding(
    disposition: &IlmRecoveryDisposition,
    expected_identity: &IlmRecoveryDispositionIdentity,
) -> EcstoreResult<()> {
    let mut expected_identity = expected_identity.clone();
    expected_identity.confirmed_at_unix_nanos = disposition.identity.confirmed_at_unix_nanos;
    if disposition.requires_legacy_v1_migration() {
        expected_identity.abandoned_control_sha256 = disposition.identity.abandoned_control_sha256.clone();
    }
    if disposition.identity != expected_identity {
        return Err(Error::PreconditionFailed);
    }
    Ok(())
}

fn expected_abandoned_control_sha256(
    control: &IlmRecoveryControl,
    source_generation: &IlmRecoverySourceGeneration,
) -> EcstoreResult<String> {
    let mut abandoned = control.clone();
    abandoned
        .abandon_for_operator(source_generation)
        .map_err(|_| Error::PreconditionFailed)?;
    control
        .validate_successor(&abandoned)
        .map_err(|_| Error::PreconditionFailed)?;
    let encoded = abandoned.encode().map_err(|_| Error::PreconditionFailed)?;
    Ok(hex_sha256(&encoded, ToOwned::to_owned))
}

fn legacy_v1_migration_error(message: &'static str) -> Error {
    Error::other(IlmRecoveryDispositionError::LegacyV1MigrationRequired(message))
}

async fn acquire_matching_disposition_proof(
    disposition: &IlmRecoveryDisposition,
) -> EcstoreResult<IlmRecoveryExportFleetProofToken> {
    let proof = acquire_ilm_recovery_export_fleet_proof()
        .await
        .ok_or(Error::PreconditionFailed)?;
    if ilm_recovery_export_topology_generation(&proof) != disposition.identity.admitted_topology_generation
        || ilm_recovery_export_member_epochs_sha256(&proof) != disposition.identity.admitted_member_epochs_sha256
        || !ilm_recovery_export_fleet_proof_matches(&proof).await
    {
        return Err(Error::PreconditionFailed);
    }
    Ok(proof)
}

async fn validate_live_disposition_inputs(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
    proof: &IlmRecoveryExportFleetProofToken,
    no_lock: bool,
) -> EcstoreResult<()> {
    if ilm_recovery_export_topology_generation(proof) != disposition.identity.admitted_topology_generation
        || ilm_recovery_export_member_epochs_sha256(proof) != disposition.identity.admitted_member_epochs_sha256
        || !ilm_recovery_export_fleet_proof_matches(proof).await
    {
        return Err(Error::PreconditionFailed);
    }
    let stored_export = load_recovery_export(api.clone(), &disposition.identity.export_id).await?;
    if stored_export.content_sha256 != disposition.identity.export_content_sha256 {
        return Err(Error::PreconditionFailed);
    }
    let export = IlmRecoveryExport::decode(&stored_export.export_id, &stored_export.encoded)?;
    if export.control_id != disposition.identity.control_id
        || export.protocol != disposition.identity.protocol
        || export.control_etag != disposition.identity.control_etag
        || export.control_revision != disposition.identity.control_revision
        || export.classification != IlmRecoveryClassification::RetainedAmbiguous
        || export.canonical_source_path != disposition.identity.canonical_source_path
        || export.source_generation != disposition.identity.source_generation
        || export.topology_generation != disposition.identity.admitted_topology_generation
        || export.member_epochs_sha256 != disposition.identity.admitted_member_epochs_sha256
    {
        return Err(Error::PreconditionFailed);
    }
    let observed_control = if no_lock {
        load_recovery_control_no_lock(api.clone(), disposition).await?
    } else {
        load_recovery_control(api.clone(), disposition.identity.protocol, &disposition.identity.control_id).await?
    };
    let retained_generation_is_exact = observed_control.etag == disposition.identity.control_etag
        && observed_control.control.revision == disposition.identity.control_revision
        && observed_control.control.classification == IlmRecoveryClassification::RetainedAmbiguous
        && observed_control.control.identity.canonical_source_path == disposition.identity.canonical_source_path
        && observed_control.control.observed_source_generation == disposition.identity.source_generation;
    let abandonment_is_durable = disposition.state == IlmRecoveryDispositionState::Applying
        && disposition.confirmed_absent.len() == disposition.identity.source_generation.copies.len()
        && control_is_abandoned_successor(disposition, &observed_control.control);
    if !retained_generation_is_exact && !abandonment_is_durable {
        return Err(Error::PreconditionFailed);
    }
    for copy in &disposition.identity.source_generation.copies {
        let observed = read_local_recovery_copy(api.clone(), copy, no_lock).await?;
        if disposition.confirmed_absent.binary_search(&copy.authority).is_ok() {
            if observed.is_some() {
                return Err(Error::PreconditionFailed);
            }
        } else {
            match observed.as_ref() {
                Some((data, etag))
                    if etag == &copy.etag
                        && u64::try_from(data.len()).ok() == Some(copy.encoded_len)
                        && hex_sha256(data, ToOwned::to_owned) == copy.content_sha256 =>
                {
                    validate_legacy_tier_delete_recovery_source(
                        &copy.canonical_path,
                        &disposition.identity.source_generation.source_schema,
                        data,
                    )?;
                }
                None if disposition.state == IlmRecoveryDispositionState::Applying => {
                    // Applying is durable authorization for local-only removal.
                    // A crash may happen after delete but before its CAS progress
                    // revision, so restart may confirm that absence here.
                }
                _ => return Err(Error::PreconditionFailed),
            }
        }
    }
    if !ilm_recovery_export_fleet_proof_matches(proof).await {
        return Err(Error::PreconditionFailed);
    }
    Ok(())
}

async fn load_recovery_control_no_lock(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
) -> EcstoreResult<super::recovery_control::ObservedIlmRecoveryControl> {
    let object = recovery_control_record_object_name(disposition.identity.protocol, &disposition.identity.control_id)
        .map_err(|_| Error::PreconditionFailed)?;
    let (encoded, metadata) = config_boundary::read_config_limited_preserve_empty_with_metadata(
        api,
        &object,
        &ObjectOptions {
            no_lock: true,
            ..Default::default()
        },
        MAX_ILM_RECOVERY_CONTROL_SIZE,
    )
    .await?;
    let etag = metadata
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or(Error::PreconditionFailed)?;
    let control =
        IlmRecoveryControl::decode(&disposition.identity.control_id, &encoded).map_err(|_| Error::PreconditionFailed)?;
    Ok(super::recovery_control::ObservedIlmRecoveryControl { control, etag })
}

fn control_is_abandoned_successor(disposition: &IlmRecoveryDisposition, control: &IlmRecoveryControl) -> bool {
    disposition.identity.control_revision.checked_add(1) == Some(control.revision)
        && control.classification == IlmRecoveryClassification::Abandoned
        && control.owner.is_none()
        && control.identity.protocol == disposition.identity.protocol
        && control.identity.canonical_source_path == disposition.identity.canonical_source_path
        && control.observed_source_generation == disposition.identity.source_generation
        && control
            .encode()
            .ok()
            .is_some_and(|encoded| hex_sha256(&encoded, ToOwned::to_owned) == disposition.identity.abandoned_control_sha256)
}

async fn abandon_recovery_control_fenced(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
    control_guard: &rustfs_lock::NamespaceLockGuard,
    source_guard: &rustfs_lock::NamespaceLockGuard,
    proof: &IlmRecoveryExportFleetProofToken,
) -> EcstoreResult<()> {
    if disposition.state != IlmRecoveryDispositionState::Applying
        || disposition.confirmed_absent.len() != disposition.identity.source_generation.copies.len()
    {
        return Err(Error::PreconditionFailed);
    }
    let current = load_recovery_control_no_lock(api.clone(), disposition).await?;
    let object = recovery_control_record_object_name(disposition.identity.protocol, &disposition.identity.control_id)
        .map_err(|_| Error::PreconditionFailed)?;
    if control_is_abandoned_successor(disposition, &current.control) {
        if control_guard.is_lock_lost() || source_guard.is_lock_lost() || !ilm_recovery_export_fleet_proof_matches(proof).await {
            return Err(Error::PreconditionFailed);
        }
        let encoded = current.control.encode().map_err(|_| Error::PreconditionFailed)?;
        api.record_durable_ilm_decommission_terminal(&object, &encoded).await?;
        if control_guard.is_lock_lost() || source_guard.is_lock_lost() {
            return Err(Error::PreconditionFailed);
        }
        return Ok(());
    }
    if current.etag != disposition.identity.control_etag
        || current.control.revision != disposition.identity.control_revision
        || current.control.classification != IlmRecoveryClassification::RetainedAmbiguous
        || current.control.identity.protocol != disposition.identity.protocol
        || current.control.identity.canonical_source_path != disposition.identity.canonical_source_path
        || current.control.observed_source_generation != disposition.identity.source_generation
    {
        return Err(Error::PreconditionFailed);
    }
    let mut next = current.control.clone();
    next.abandon_for_operator(&disposition.identity.source_generation)
        .map_err(|_| Error::PreconditionFailed)?;
    current
        .control
        .validate_successor(&next)
        .map_err(|_| Error::PreconditionFailed)?;
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() || !ilm_recovery_export_fleet_proof_matches(proof).await {
        return Err(Error::PreconditionFailed);
    }
    let encoded = next.encode().map_err(|_| Error::PreconditionFailed)?;
    let mut options = ObjectOptions {
        max_parity: true,
        no_lock: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(HTTPPreconditions {
            if_match: Some(current.etag),
            ..Default::default()
        }),
        ..Default::default()
    };
    options.add_namespace_lock_guard(control_guard);
    options.add_namespace_lock_guard(source_guard);
    let write_result = config_boundary::save_config_with_opts(api.clone(), &object, encoded, &options).await;
    let observed = match load_recovery_control_no_lock(api.clone(), disposition).await {
        Ok(observed) if control_is_abandoned_successor(disposition, &observed.control) => observed,
        Ok(_) => return Err(Error::PreconditionFailed),
        Err(read_err) => return Err(write_result.err().unwrap_or(read_err)),
    };
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() || !ilm_recovery_export_fleet_proof_matches(proof).await {
        return Err(Error::PreconditionFailed);
    }
    let observed_encoded = observed.control.encode().map_err(|_| Error::PreconditionFailed)?;
    api.record_durable_ilm_decommission_terminal(&object, &observed_encoded)
        .await?;
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    Ok(())
}

async fn read_local_recovery_copy(
    api: Arc<ECStore>,
    copy: &IlmRecoverySourceCopy,
    no_lock: bool,
) -> EcstoreResult<Option<(Vec<u8>, String)>> {
    let set = recovery_copy_set(&api, &copy.authority)?;
    match config_boundary::read_config_limited_preserve_empty_with_metadata(
        set,
        &copy.canonical_path,
        &ObjectOptions {
            no_lock,
            ..Default::default()
        },
        MAX_LEGACY_TIER_DELETE_SOURCE_SIZE,
    )
    .await
    {
        Ok((data, metadata)) => {
            let etag = metadata
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .ok_or(Error::PreconditionFailed)?;
            Ok(Some((data, etag)))
        }
        Err(err) if disposition_is_missing(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

fn recovery_copy_set(api: &ECStore, authority: &str) -> EcstoreResult<Arc<crate::set_disk::SetDisks>> {
    let (pool_index, set_index) = parse_recovery_copy_authority(authority)?;
    api.all_set_disks()
        .into_iter()
        .find(|set| set.pool_index == pool_index && set.set_index == set_index)
        .ok_or(Error::PreconditionFailed)
}

fn parse_recovery_copy_authority(authority: &str) -> EcstoreResult<(usize, usize)> {
    let Some((pool, set)) = authority.strip_prefix("pool-").and_then(|value| value.split_once("/set-")) else {
        return Err(Error::PreconditionFailed);
    };
    let pool_index = pool.parse::<usize>().map_err(|_| Error::PreconditionFailed)?;
    let set_index = set.parse::<usize>().map_err(|_| Error::PreconditionFailed)?;
    if format!("pool-{pool_index}/set-{set_index}") != authority {
        return Err(Error::PreconditionFailed);
    }
    Ok((pool_index, set_index))
}

async fn delete_exact_local_recovery_copy(
    api: Arc<ECStore>,
    copy: &IlmRecoverySourceCopy,
    source_schema: &str,
    control_guard: &rustfs_lock::NamespaceLockGuard,
    source_guard: &rustfs_lock::NamespaceLockGuard,
) -> EcstoreResult<()> {
    let set = recovery_copy_set(&api, &copy.authority)?;
    let Some((data, etag)) = read_local_recovery_copy(api.clone(), copy, true).await? else {
        return Ok(());
    };
    if etag != copy.etag
        || u64::try_from(data.len()).ok() != Some(copy.encoded_len)
        || hex_sha256(&data, ToOwned::to_owned) != copy.content_sha256
    {
        return Err(Error::PreconditionFailed);
    }
    validate_legacy_tier_delete_recovery_source(&copy.canonical_path, source_schema, &data)?;
    let mut options = ObjectOptions {
        no_lock: true,
        http_preconditions: Some(HTTPPreconditions {
            if_match: Some(copy.etag.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };
    options.add_namespace_lock_guard(control_guard);
    options.add_namespace_lock_guard(source_guard);
    let delete = set.delete_object(RUSTFS_META_BUCKET, &copy.canonical_path, options).await;
    match read_local_recovery_copy(api, copy, true).await {
        Ok(None) => Ok(()),
        Ok(Some((observed, observed_etag))) => {
            if observed_etag != copy.etag || observed != data {
                Err(Error::PreconditionFailed)
            } else {
                Err(delete.err().unwrap_or(Error::PreconditionFailed))
            }
        }
        Err(read_err) => Err(delete.err().unwrap_or(read_err)),
    }
}

fn new_disposition_owner(
    api: &ECStore,
    proof: &IlmRecoveryExportFleetProofToken,
    now_unix_nanos: i64,
) -> EcstoreResult<IlmRecoveryDispositionOwnerLease> {
    let lease_expires_at_unix_nanos = now_unix_nanos
        .checked_add(DISPOSITION_OWNER_LEASE_NANOS)
        .ok_or(Error::PreconditionFailed)?;
    Ok(IlmRecoveryDispositionOwnerLease {
        owner_id: api.id.to_string(),
        owner_epoch: Uuid::new_v4(),
        lease_acquired_at_unix_nanos: now_unix_nanos,
        lease_expires_at_unix_nanos,
        topology_generation: ilm_recovery_export_topology_generation(proof),
        member_epochs_sha256: ilm_recovery_export_member_epochs_sha256(proof),
    })
}

fn ensure_owned_execution_fence(
    disposition: &IlmRecoveryDisposition,
    owner: &IlmRecoveryDispositionOwnerLease,
    now_unix_nanos: i64,
) -> EcstoreResult<()> {
    if disposition.state != IlmRecoveryDispositionState::Applying
        || disposition.owner.as_ref() != Some(owner)
        || owner.lease_expires_at_unix_nanos <= now_unix_nanos
    {
        return Err(Error::PreconditionFailed);
    }
    Ok(())
}

async fn save_recovery_disposition_step_fenced(
    api: Arc<ECStore>,
    current: &ObservedIlmRecoveryDisposition,
    next: &IlmRecoveryDisposition,
    control_guard: &rustfs_lock::NamespaceLockGuard,
    source_guard: &rustfs_lock::NamespaceLockGuard,
    proof: &IlmRecoveryExportFleetProofToken,
) -> EcstoreResult<ObservedIlmRecoveryDisposition> {
    current
        .disposition
        .validate_successor(next)
        .map_err(disposition_store_error)?;
    let protocol = current.disposition.identity.protocol;
    let disposition_id = &current.disposition.identity.disposition_id;
    let object = recovery_disposition_record_object_name(protocol, disposition_id).map_err(disposition_store_error)?;
    let authoritative = load_recovery_disposition(api.clone(), protocol, disposition_id).await?;
    if authoritative != *current {
        if authoritative.disposition.is_same_or_later_generation_of(next) {
            if control_guard.is_lock_lost()
                || source_guard.is_lock_lost()
                || !ilm_recovery_export_fleet_proof_matches(proof).await
            {
                return Err(Error::PreconditionFailed);
            }
            record_disposition_decommission_checkpoint(api.as_ref(), &object, &authoritative).await?;
            if control_guard.is_lock_lost() || source_guard.is_lock_lost() {
                return Err(Error::PreconditionFailed);
            }
            return Ok(authoritative);
        }
        return Err(Error::PreconditionFailed);
    }
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() || !ilm_recovery_export_fleet_proof_matches(proof).await {
        return Err(Error::PreconditionFailed);
    }
    let encoded = next.encode().map_err(disposition_store_error)?;
    let mut options = ObjectOptions {
        max_parity: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(HTTPPreconditions {
            if_match: Some(current.etag.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };
    options.add_namespace_lock_guard(control_guard);
    options.add_namespace_lock_guard(source_guard);
    let write_result = config_boundary::save_config_with_opts(api.clone(), &object, encoded, &options).await;
    let observed = match load_recovery_disposition(api.clone(), protocol, disposition_id).await {
        Ok(observed) if observed.disposition.is_same_or_later_generation_of(next) => observed,
        Ok(_) => return Err(Error::PreconditionFailed),
        Err(read_err) => return Err(write_result.err().unwrap_or(read_err)),
    };
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() || !ilm_recovery_export_fleet_proof_matches(proof).await {
        return Err(Error::PreconditionFailed);
    }
    record_disposition_decommission_checkpoint(api.as_ref(), &object, &observed).await?;
    if control_guard.is_lock_lost() || source_guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    Ok(observed)
}

fn disposition_execution(
    disposition: &IlmRecoveryDisposition,
    outcome: IlmRecoveryDispositionExecutionOutcome,
) -> IlmRecoveryDispositionExecution {
    IlmRecoveryDispositionExecution {
        disposition_id: disposition.identity.disposition_id.clone(),
        state: disposition.state,
        outcome,
        confirmed_absent_copy_count: disposition.confirmed_absent.len(),
        source_copy_count: disposition.identity.source_generation.copies.len(),
    }
}

fn wall_clock_unix_nanos() -> EcstoreResult<i64> {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).map_err(|_| Error::PreconditionFailed)
}

pub(crate) async fn load_recovery_disposition(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    disposition_id: &str,
) -> EcstoreResult<ObservedIlmRecoveryDisposition> {
    load_recovery_disposition_with_options(api, protocol, disposition_id, false).await
}

pub(crate) async fn load_recovery_disposition_no_lock(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    disposition_id: &str,
) -> EcstoreResult<ObservedIlmRecoveryDisposition> {
    load_recovery_disposition_with_options(api, protocol, disposition_id, true).await
}

async fn load_recovery_disposition_with_options(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    disposition_id: &str,
    no_lock: bool,
) -> EcstoreResult<ObservedIlmRecoveryDisposition> {
    let object = recovery_disposition_record_object_name(protocol, disposition_id).map_err(disposition_store_error)?;
    let (encoded, metadata) = config_boundary::read_config_limited_preserve_empty_with_metadata(
        api,
        &object,
        &ObjectOptions {
            no_lock,
            ..Default::default()
        },
        MAX_ILM_RECOVERY_DISPOSITION_SIZE,
    )
    .await?;
    let etag = metadata
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("ILM recovery disposition is missing an ETag"))?;
    let disposition = IlmRecoveryDisposition::decode(disposition_id, &encoded).map_err(disposition_store_error)?;
    if disposition.identity.protocol != protocol {
        return Err(Error::other("ILM recovery disposition protocol does not match record path"));
    }
    Ok(ObservedIlmRecoveryDisposition {
        disposition,
        etag,
        content_sha256: hex_sha256(&encoded, ToOwned::to_owned),
        encoded,
    })
}

async fn record_disposition_decommission_checkpoint(
    api: &ECStore,
    object: &str,
    observed: &ObservedIlmRecoveryDisposition,
) -> EcstoreResult<()> {
    if observed.disposition.state == IlmRecoveryDispositionState::Completed {
        api.record_durable_ilm_decommission_terminal(object, &observed.encoded).await
    } else {
        api.record_durable_ilm_decommission_progress(object, &observed.encoded).await
    }
}

fn validate_sha256(value: &str, message: &'static str) -> Result<()> {
    if !is_sha256_checksum(value)
        || value
            .bytes()
            .any(|byte| byte.is_ascii_hexdigit() && byte.is_ascii_uppercase())
    {
        return Err(IlmRecoveryDispositionError::Corrupt(message));
    }
    Ok(())
}

fn is_strictly_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn is_sorted_subset(previous: &[String], next: &[String]) -> bool {
    previous.iter().all(|value| next.binary_search(value).is_ok())
}

fn length_delimited_digest(parts: &[&[u8]]) -> String {
    let mut encoded = Vec::new();
    for part in parts {
        encoded.extend_from_slice(&(part.len() as u64).to_be_bytes());
        encoded.extend_from_slice(part);
    }
    hex_sha256(&encoded, ToOwned::to_owned)
}

fn checkpoint_hash<T: Serialize>(value: &T) -> EcstoreResult<String> {
    let encoded = serde_json::to_vec(value).map_err(Error::other)?;
    Ok(hex_sha256(&encoded, ToOwned::to_owned))
}

fn checkpoint_domain_hash<T: Serialize>(domain: &[u8], value: &T) -> EcstoreResult<String> {
    let encoded = serde_json::to_vec(value).map_err(Error::other)?;
    Ok(length_delimited_digest(&[domain, &encoded]))
}

fn disposition_store_error(err: IlmRecoveryDispositionError) -> Error {
    Error::other(err)
}

fn disposition_is_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(value: &[u8]) -> String {
        hex_sha256(value, ToOwned::to_owned)
    }

    fn sample_disposition() -> IlmRecoveryDisposition {
        let source_path = format!("ilm/tier-delete-journal/{}.json", digest(b"journal-identity"));
        let content_sha256 = digest(b"legacy-source");
        let source_generation = IlmRecoverySourceGeneration::new(
            "rustfs-tier-delete-journal-v2",
            "source-etag",
            content_sha256.clone(),
            vec![
                IlmRecoverySourceCopy {
                    authority: "pool-1/set-0".to_string(),
                    canonical_path: source_path.clone(),
                    etag: "source-etag".to_string(),
                    encoded_len: 13,
                    content_sha256: content_sha256.clone(),
                },
                IlmRecoverySourceCopy {
                    authority: "pool-0/set-0".to_string(),
                    canonical_path: source_path.clone(),
                    etag: "source-etag".to_string(),
                    encoded_len: 13,
                    content_sha256,
                },
            ],
        )
        .expect("source generation should be valid");
        let control_id = digest(b"control");
        let export_id = recovery_export_id(&control_id, &source_generation).expect("export ID should be valid");
        let action = IlmRecoveryDispositionAction::AbandonRemoteCleanup;
        let identity = IlmRecoveryDispositionIdentity {
            disposition_id: recovery_disposition_id(&export_id, action).expect("ID should be valid"),
            protocol: IlmRecoveryProtocol::TierDeleteJournal,
            action,
            export_id,
            export_content_sha256: digest(b"export-envelope"),
            control_id,
            control_etag: "control-etag".to_string(),
            control_revision: 1,
            abandoned_control_sha256: digest(b"abandoned-control"),
            canonical_source_path: source_path,
            source_generation,
            admitted_topology_generation: digest(b"topology"),
            admitted_member_epochs_sha256: digest(b"member-epochs"),
            actor_sha256: digest(b"actor"),
            reason_code: IlmRecoveryDispositionReasonCode::LegacyRemoteCleanupAbandoned,
            confirmed_at_unix_nanos: 1_000_000_000,
        };
        IlmRecoveryDisposition::new(identity, 1_000_000_001).expect("disposition should be valid")
    }

    fn encode_v1_fixture(disposition: &IlmRecoveryDisposition) -> Vec<u8> {
        let legacy = IlmRecoveryDispositionV1 {
            identity: IlmRecoveryDispositionIdentityV1 {
                disposition_id: disposition.identity.disposition_id.clone(),
                protocol: disposition.identity.protocol,
                action: disposition.identity.action,
                export_id: disposition.identity.export_id.clone(),
                export_content_sha256: disposition.identity.export_content_sha256.clone(),
                control_id: disposition.identity.control_id.clone(),
                control_etag: disposition.identity.control_etag.clone(),
                control_revision: disposition.identity.control_revision,
                canonical_source_path: disposition.identity.canonical_source_path.clone(),
                source_generation: disposition.identity.source_generation.clone(),
                admitted_topology_generation: disposition.identity.admitted_topology_generation.clone(),
                admitted_member_epochs_sha256: disposition.identity.admitted_member_epochs_sha256.clone(),
                actor_sha256: disposition.identity.actor_sha256.clone(),
                reason_code: disposition.identity.reason_code,
                confirmed_at_unix_nanos: disposition.identity.confirmed_at_unix_nanos,
            },
            created_at_unix_nanos: disposition.created_at_unix_nanos,
            retain_until_unix_nanos: disposition.retain_until_unix_nanos,
            revision: disposition.revision,
            state: disposition.state,
            owner: disposition.owner.clone(),
            confirmed_absent: disposition.confirmed_absent.clone(),
        };
        let disposition_bytes = serde_json::to_vec(&legacy).expect("v1 disposition should encode");
        serde_json::to_vec(&PersistedIlmRecoveryDispositionV1 {
            schema: ILM_RECOVERY_DISPOSITION_SCHEMA_V1.to_string(),
            content_sha256: digest(&disposition_bytes),
            disposition: legacy,
        })
        .expect("v1 envelope should encode")
    }

    fn owner(epoch: u128, acquired: i64) -> IlmRecoveryDispositionOwnerLease {
        IlmRecoveryDispositionOwnerLease {
            owner_id: "node-a".to_string(),
            owner_epoch: Uuid::from_u128(epoch),
            lease_acquired_at_unix_nanos: acquired,
            lease_expires_at_unix_nanos: acquired + 1_000,
            topology_generation: digest(b"topology-now"),
            member_epochs_sha256: digest(b"member-epochs-now"),
        }
    }

    #[test]
    fn disposition_id_and_path_are_canonical_and_deterministic() {
        let disposition = sample_disposition();
        let id = &disposition.identity.disposition_id;
        assert_eq!(
            recovery_disposition_id(&disposition.identity.export_id, IlmRecoveryDispositionAction::AbandonRemoteCleanup)
                .expect("ID should be valid"),
            *id
        );
        let path =
            recovery_disposition_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, id).expect("path should be valid");
        assert_eq!(
            recovery_disposition_id_from_record_object_name(&path).expect("path should parse"),
            (IlmRecoveryProtocol::TierDeleteJournal, id.clone())
        );
        let wrong_shard = path.replacen(&format!("/{}/", &id[..2]), "/ff/", 1);
        assert!(recovery_disposition_id_from_record_object_name(&wrong_shard).is_err());
    }

    #[test]
    fn checksum_envelope_round_trips_and_rejects_tampering() {
        let disposition = sample_disposition();
        let encoded = disposition.encode().expect("disposition should encode");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap()["schema"],
            ILM_RECOVERY_DISPOSITION_SCHEMA
        );
        assert_eq!(
            IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &encoded).expect("disposition should decode"),
            disposition
        );

        let mut envelope: serde_json::Value = serde_json::from_slice(&encoded).expect("fixture should be json");
        envelope["disposition"]["identity"]["control_etag"] = serde_json::Value::String("tampered".to_string());
        let tampered = serde_json::to_vec(&envelope).expect("fixture should encode");
        assert!(matches!(
            IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &tampered),
            Err(IlmRecoveryDispositionError::ChecksumMismatch)
        ));

        envelope["unknown"] = serde_json::Value::Bool(true);
        let unknown = serde_json::to_vec(&envelope).expect("fixture should encode");
        assert!(matches!(
            IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &unknown),
            Err(IlmRecoveryDispositionError::Json(_))
        ));

        for source_path in [
            "ilm/tier-delete-journal/legacy.json",
            "ilm/tier-delete-journal/../legacy.json",
        ] {
            let mut invalid = disposition.clone();
            invalid.identity.canonical_source_path = source_path.to_string();
            for copy in &mut invalid.identity.source_generation.copies {
                copy.canonical_path = source_path.to_string();
            }
            invalid.identity.source_generation = IlmRecoverySourceGeneration::new(
                invalid.identity.source_generation.source_schema.clone(),
                invalid.identity.source_generation.source_etag.clone(),
                invalid.identity.source_generation.content_sha256.clone(),
                invalid.identity.source_generation.copies.clone(),
            )
            .expect("copy manifest should be internally consistent");
            assert!(invalid.validate().is_err(), "noncanonical source path must fail closed");
        }

        let mut rebound = disposition;
        rebound.identity.control_id = digest(b"different-control");
        assert!(rebound.validate().is_err(), "export ID must bind the exact control and source generation");
    }

    #[test]
    fn v1_envelope_decodes_strictly_and_requires_safe_migration() {
        let disposition = sample_disposition();
        let encoded = encode_v1_fixture(&disposition);
        let decoded = IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &encoded)
            .expect("valid v1 records should remain distinguishable from corruption");
        assert!(decoded.requires_legacy_v1_migration());
        assert!(matches!(decoded.encode(), Err(IlmRecoveryDispositionError::LegacyV1MigrationRequired(_))));
        let path =
            recovery_disposition_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, &disposition.identity.disposition_id)
                .unwrap();
        let checkpoint = decode_recovery_disposition_checkpoint(&path, &encoded).unwrap();
        let persisted: PersistedIlmRecoveryDispositionV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(checkpoint.identity_sha256, checkpoint_hash(&persisted.disposition.identity).unwrap());
        assert_eq!(checkpoint.retain_until_unix_nanos, persisted.disposition.retain_until_unix_nanos);

        let mut tampered: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        tampered["disposition"]["identity"]["actor_sha256"] = serde_json::Value::String(digest(b"other-actor"));
        assert!(matches!(
            IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &serde_json::to_vec(&tampered).unwrap()),
            Err(IlmRecoveryDispositionError::ChecksumMismatch)
        ));

        let mut unknown: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        unknown["disposition"]["identity"]["unknown"] = serde_json::Value::Bool(true);
        assert!(matches!(
            IlmRecoveryDisposition::decode(&disposition.identity.disposition_id, &serde_json::to_vec(&unknown).unwrap()),
            Err(IlmRecoveryDispositionError::Json(_))
        ));

        let mut completed = disposition;
        completed.claim(owner(1, 2_000_000_000)).unwrap();
        completed.begin_applying().unwrap();
        completed.confirm_absent("pool-0/set-0").unwrap();
        completed.confirm_absent("pool-1/set-0").unwrap();
        completed.complete().unwrap();
        let completed = IlmRecoveryDisposition::decode(&completed.identity.disposition_id, &encode_v1_fixture(&completed))
            .expect("completed v1 record should decode into fail-closed quarantine");
        assert!(completed.requires_legacy_v1_migration());
        assert_eq!(completed.state, IlmRecoveryDispositionState::Completed);
        assert_eq!(completed.retain_until_unix_nanos, i64::MAX);
    }

    #[test]
    fn state_and_absence_progress_successors_are_strict() {
        let prepared = sample_disposition();
        let mut claimed = prepared.clone();
        claimed.claim(owner(1, 2_000_000_000)).expect("claim should succeed");
        prepared.validate_successor(&claimed).expect("claim should be a successor");

        let mut applying = claimed.clone();
        applying.begin_applying().expect("begin applying should succeed");
        claimed.validate_successor(&applying).expect("applying should follow a claim");

        let mut one_absent = applying.clone();
        one_absent
            .confirm_absent("pool-0/set-0")
            .expect("manifest entry should be recorded");
        applying
            .validate_successor(&one_absent)
            .expect("absence should advance monotonically");

        let mut batched_absence = applying.clone();
        batched_absence.revision += 1;
        batched_absence.confirmed_absent = vec!["pool-0/set-0".to_string(), "pool-1/set-0".to_string()];
        assert!(
            applying.validate_successor(&batched_absence).is_err(),
            "each durable progress revision must confirm exactly one copy"
        );

        let mut skipped_progress = one_absent.clone();
        skipped_progress.revision += 1;
        skipped_progress.state = IlmRecoveryDispositionState::Completed;
        skipped_progress.owner = None;
        skipped_progress.confirmed_absent.push("pool-1/set-0".to_string());
        assert!(one_absent.validate_successor(&skipped_progress).is_err());

        let mut all_absent = one_absent.clone();
        all_absent
            .confirm_absent("pool-1/set-0")
            .expect("manifest entry should be recorded");
        let mut completed = all_absent.clone();
        completed.complete().expect("full absence should complete");
        all_absent
            .validate_successor(&completed)
            .expect("completed should follow full absence");
        assert!(completed.validate_successor(&completed).is_err());

        let mut regressed = all_absent.clone();
        regressed.revision += 1;
        regressed.confirmed_absent.remove(0);
        assert!(one_absent.validate_successor(&regressed).is_err());
    }

    #[test]
    fn advanced_replay_requires_a_reachable_generation() {
        let prepared = sample_disposition();

        let mut skipped_to_applying = prepared.clone();
        skipped_to_applying.revision = 2;
        skipped_to_applying.state = IlmRecoveryDispositionState::Applying;
        skipped_to_applying.owner = Some(owner(1, 2_000_000_000));
        skipped_to_applying.confirmed_absent.push("pool-0/set-0".to_string());
        assert!(skipped_to_applying.validate().is_ok());
        assert!(!skipped_to_applying.is_same_or_later_generation_of(&prepared));

        let mut skipped_to_completed = prepared.clone();
        skipped_to_completed.revision = 2;
        skipped_to_completed.state = IlmRecoveryDispositionState::Completed;
        skipped_to_completed.confirmed_absent = vec!["pool-0/set-0".to_string(), "pool-1/set-0".to_string()];
        assert!(skipped_to_completed.validate().is_ok());
        assert!(!skipped_to_completed.is_same_or_later_generation_of(&prepared));

        let mut claimed = prepared.clone();
        claimed.claim(owner(1, 2_000_000_000)).unwrap();
        let mut applying = claimed.clone();
        applying.begin_applying().unwrap();
        applying.confirm_absent("pool-0/set-0").unwrap();
        applying.confirm_absent("pool-1/set-0").unwrap();
        let mut completed = applying.clone();
        completed.complete().unwrap();
        assert!(completed.is_same_or_later_generation_of(&prepared));

        let mut same_revision_conflict = prepared.clone();
        same_revision_conflict.identity.actor_sha256 = digest(b"different-actor");
        assert!(!same_revision_conflict.is_same_or_later_generation_of(&prepared));
    }

    #[test]
    fn owner_takeover_requires_expiry_and_cannot_advance_progress() {
        let mut applying = sample_disposition();
        applying.claim(owner(1, 2_000_000_000)).expect("claim should succeed");
        applying.begin_applying().expect("begin applying should succeed");

        let mut early = applying.clone();
        assert!(early.take_over(owner(2, 2_000_000_999)).is_err());

        let mut taken_over = applying.clone();
        taken_over
            .take_over(owner(2, 2_000_001_000))
            .expect("expired owner should be replaceable");
        applying
            .validate_successor(&taken_over)
            .expect("takeover should be a valid successor");

        let mut changed_both = taken_over.clone();
        changed_both.revision += 1;
        changed_both.owner = Some(owner(3, 2_000_002_000));
        changed_both.confirmed_absent.push("pool-0/set-0".to_string());
        assert!(taken_over.validate_successor(&changed_both).is_err());
    }

    #[test]
    fn manifest_and_confirmed_absent_must_remain_canonical() {
        let mut disposition = sample_disposition();
        disposition.identity.source_generation.copies.swap(0, 1);
        assert!(disposition.validate().is_err());

        let mut disposition = sample_disposition();
        disposition.state = IlmRecoveryDispositionState::Applying;
        disposition.owner = Some(owner(1, 2_000_000_000));
        disposition.confirmed_absent = vec!["pool-9/set-9".to_string()];
        assert!(disposition.validate().is_err());

        let mut disposition = sample_disposition();
        disposition.state = IlmRecoveryDispositionState::Completed;
        disposition.confirmed_absent = vec!["pool-0/set-0".to_string()];
        assert!(disposition.validate().is_err());
    }

    #[test]
    fn recovery_copy_authority_parser_is_strict() {
        assert_eq!(
            parse_recovery_copy_authority("pool-0/set-17").expect("canonical authority should parse"),
            (0, 17)
        );
        for invalid in [
            "pool-00/set-17",
            "pool-0/set-017",
            "pool-0/set-17/extra",
            "pool-x/set-1",
            "pool-1/set-x",
            "set-0/pool-1",
            "",
        ] {
            assert!(parse_recovery_copy_authority(invalid).is_err(), "authority must fail closed: {invalid}");
        }
    }

    #[test]
    fn execution_fence_requires_exact_live_owner() {
        let mut disposition = sample_disposition();
        let active_owner = owner(1, 2_000_000_000);
        disposition.claim(active_owner.clone()).expect("owner claim should succeed");
        disposition.begin_applying().expect("applying transition should succeed");
        assert!(ensure_owned_execution_fence(&disposition, &active_owner, 2_000_000_999).is_ok());
        assert!(ensure_owned_execution_fence(&disposition, &active_owner, 2_000_001_000).is_err());
        assert!(ensure_owned_execution_fence(&disposition, &owner(2, 2_000_000_000), 2_000_000_999).is_err());
    }

    #[test]
    fn source_copy_size_is_bounded_before_destructive_execution() {
        let mut disposition = sample_disposition();
        disposition.identity.source_generation.copies[0].encoded_len = 64 * 1024 + 1;
        assert!(disposition.validate().is_err());
    }

    #[test]
    fn abandoned_control_is_a_valid_crash_recovery_bridge_to_completion() {
        use crate::bucket::lifecycle::recovery_control::{IlmRecoveryControlIdentity, IlmRecoveryErrorCode};

        let mut disposition = sample_disposition();
        disposition
            .claim(owner(1, 2_000_000_000))
            .expect("owner claim should succeed");
        disposition.begin_applying().expect("applying transition should succeed");
        disposition
            .confirm_absent("pool-0/set-0")
            .expect("first absence should be recorded");
        disposition
            .confirm_absent("pool-1/set-0")
            .expect("second absence should be recorded");

        let mut control = IlmRecoveryControl::new(
            IlmRecoveryControlIdentity {
                protocol: IlmRecoveryProtocol::TierDeleteJournal,
                canonical_source_path: disposition.identity.canonical_source_path.clone(),
                stable_operation_identity: "legacy-operation".to_string(),
                record_class: "tier_delete_journal_v2".to_string(),
            },
            disposition.identity.source_generation.clone(),
            IlmRecoveryClassification::RetainedAmbiguous,
            1_000_000_000,
            IlmRecoveryErrorCode::OperatorDispositionRequired,
        )
        .expect("retained control should build");
        assert_eq!(control.revision, disposition.identity.control_revision);
        control
            .abandon_for_operator(&disposition.identity.source_generation)
            .expect("exact source generation should be abandoned");
        disposition.identity.abandoned_control_sha256 =
            hex_sha256(&control.encode().expect("abandoned control should encode"), ToOwned::to_owned);
        assert!(control_is_abandoned_successor(&disposition, &control));

        let mut completed = disposition.clone();
        completed.complete().expect("completed transition should succeed");
        disposition
            .validate_successor(&completed)
            .expect("completed disposition should be a valid successor");
    }

    #[test]
    fn completed_replay_binding_requires_the_original_actor() {
        let disposition = sample_disposition();
        assert!(validate_existing_request_binding(&disposition, &disposition.identity).is_ok());

        let mut later_retry = disposition.identity.clone();
        later_retry.confirmed_at_unix_nanos += 1;
        assert!(
            validate_existing_request_binding(&disposition, &later_retry).is_ok(),
            "server request time must not make a stable replay identity diverge"
        );

        let mut wrong_identity = disposition.identity.clone();
        wrong_identity.actor_sha256 = digest(b"different-actor");
        assert!(validate_existing_request_binding(&disposition, &wrong_identity).is_err());

        let mut wrong_identity = disposition.identity.clone();
        wrong_identity.abandoned_control_sha256 = digest(b"different-abandoned-control");
        assert!(validate_existing_request_binding(&disposition, &wrong_identity).is_err());

        let mut wrong_identity = disposition.identity.clone();
        wrong_identity.admitted_member_epochs_sha256 = digest(b"different-fleet");
        assert!(validate_existing_request_binding(&disposition, &wrong_identity).is_err());
    }

    #[test]
    fn encoded_disposition_is_bounded_to_sixteen_kibibytes() {
        let mut disposition = sample_disposition();
        disposition.identity.control_etag = "x".repeat(MAX_ILM_RECOVERY_DISPOSITION_SIZE);
        assert!(matches!(
            disposition.encode(),
            Err(IlmRecoveryDispositionError::Corrupt("encoded disposition exceeds maximum size"))
        ));
        assert!(matches!(
            IlmRecoveryDisposition::decode(
                &sample_disposition().identity.disposition_id,
                &vec![b'x'; MAX_ILM_RECOVERY_DISPOSITION_SIZE + 1]
            ),
            Err(IlmRecoveryDispositionError::Corrupt("encoded disposition exceeds maximum size"))
        ));
    }

    #[test]
    fn durable_checkpoint_hashes_identity_owner_manifest_and_absence_set() {
        let prepared = sample_disposition();
        let path =
            recovery_disposition_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, &prepared.identity.disposition_id)
                .expect("path should be valid");
        let prepared_encoded = prepared.encode().expect("prepared disposition should encode");
        let prepared_checkpoint =
            decode_recovery_disposition_checkpoint(&path, &prepared_encoded).expect("prepared checkpoint should decode");
        assert_eq!(prepared_checkpoint.state, IlmRecoveryDispositionState::Prepared);
        assert_eq!(prepared_checkpoint.owner_fence_sha256, None);

        let mut disposition = prepared;
        disposition.claim(owner(1, 2_000_000_000)).expect("claim should succeed");
        disposition.begin_applying().expect("begin applying should succeed");
        disposition
            .confirm_absent("pool-0/set-0")
            .expect("absence should be recorded");
        let encoded = disposition.encode().expect("disposition should encode");
        let checkpoint = decode_recovery_disposition_checkpoint(&path, &encoded).expect("checkpoint should decode");
        assert_eq!(checkpoint.disposition_id, disposition.identity.disposition_id);
        assert_eq!(checkpoint.content_sha256, digest(&encoded));
        assert_eq!(checkpoint.copy_manifest_sha256, disposition.identity.source_generation.copy_set_sha256);
        assert_eq!(checkpoint.copy_manifest_count, 2);
        assert_eq!(checkpoint.revision, disposition.revision);
        assert_eq!(checkpoint.state, IlmRecoveryDispositionState::Applying);
        assert_eq!(
            checkpoint.owner_fence_sha256,
            Some(
                checkpoint_domain_hash(
                    OWNER_FENCE_CHECKPOINT_DOMAIN,
                    disposition.owner.as_ref().expect("applying disposition should have an owner")
                )
                .expect("owner fence should hash")
            )
        );
        assert_eq!(checkpoint.confirmed_absent_sha256, vec![digest(b"pool-0/set-0")]);
        assert_eq!(checkpoint.retain_until_unix_nanos, disposition.retain_until_unix_nanos);
    }
}
