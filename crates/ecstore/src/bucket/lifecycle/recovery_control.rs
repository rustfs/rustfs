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

use super::config_boundary;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::{list::ListOperations as _, object::HTTPPreconditions};
use crate::store::ECStore;

pub const ILM_RECOVERY_CONTROL_SCHEMA: &str = "rustfs-ilm-recovery-control-v1";
pub const ILM_RECOVERY_CONTROL_PREFIX: &str = "ilm/recovery-controls";
pub const MAX_ILM_RECOVERY_CONTROL_SIZE: usize = 16 * 1024;
pub const MAX_RECOVERY_ATTEMPTS: u32 = 32;
const MAX_RECOVERY_AGE_NANOS: i64 = 7 * 24 * 60 * 60 * 1_000_000_000;
const MIN_RETRY_DELAY_NANOS: i64 = 60 * 1_000_000_000;
const MAX_RETRY_DELAY_NANOS: i64 = 60 * 60 * 1_000_000_000;

pub type Result<T> = std::result::Result<T, IlmRecoveryControlError>;

#[derive(Debug, thiserror::Error)]
pub enum IlmRecoveryControlError {
    #[error("ILM recovery control is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("ILM recovery control schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("ILM recovery control checksum mismatch")]
    ChecksumMismatch,
    #[error("ILM recovery control successor is invalid: {0}")]
    InvalidSuccessor(&'static str),
    #[error("ILM recovery control json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryProtocol {
    TransitionTransaction,
    TierDeleteJournal,
    TierDeleteManifest,
}

impl IlmRecoveryProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TransitionTransaction => "transition_transaction",
            Self::TierDeleteJournal => "tier_delete_journal",
            Self::TierDeleteManifest => "tier_delete_manifest",
        }
    }

    pub const fn all() -> [Self; 3] {
        [Self::TransitionTransaction, Self::TierDeleteJournal, Self::TierDeleteManifest]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryClassification {
    Retrying,
    RetainedAmbiguous,
    Corrupt,
    OperatorRequired,
    Abandoned,
    Terminal,
}

impl IlmRecoveryClassification {
    pub fn permits_automatic_attempt(self) -> bool {
        self == Self::Retrying
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IlmRecoveryErrorCode {
    None,
    SourceUnavailable,
    SourceDivergent,
    SourceCorrupt,
    SourceGenerationChanged,
    BackendUnavailable,
    BackendTimeout,
    BackendThrottled,
    BackendServerError,
    AttemptLeaseExpired,
    RemoteVersionUnknown,
    RemoteProbeAmbiguous,
    RemoteProbeUnsupported,
    LocalCommitAmbiguous,
    CasConflict,
    CleanupFailed,
    OperatorDispositionRequired,
    UnsupportedSchema,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoverySourceCopy {
    pub authority: String,
    pub canonical_path: String,
    pub etag: String,
    pub encoded_len: u64,
    pub content_sha256: String,
}

impl IlmRecoverySourceCopy {
    fn validate(&self) -> Result<()> {
        if self.authority.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("source copy authority is empty"));
        }
        validate_canonical_source_path(&self.canonical_path)?;
        if self.etag.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("source copy ETag is empty"));
        }
        if self.encoded_len == 0 {
            return Err(IlmRecoveryControlError::Corrupt("source copy encoded length is zero"));
        }
        validate_sha256(&self.content_sha256, "source copy content checksum is invalid")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoverySourceGeneration {
    pub source_schema: String,
    pub source_etag: String,
    pub content_sha256: String,
    pub copy_set_sha256: String,
    pub copies: Vec<IlmRecoverySourceCopy>,
}

impl IlmRecoverySourceGeneration {
    pub fn new(
        source_schema: impl Into<String>,
        source_etag: impl Into<String>,
        content_sha256: impl Into<String>,
        mut copies: Vec<IlmRecoverySourceCopy>,
    ) -> Result<Self> {
        copies.sort_by(|left, right| (&left.authority, &left.canonical_path).cmp(&(&right.authority, &right.canonical_path)));
        let copy_set_sha256 = copy_set_digest(&copies)?;
        let generation = Self {
            source_schema: source_schema.into(),
            source_etag: source_etag.into(),
            content_sha256: content_sha256.into(),
            copy_set_sha256,
            copies,
        };
        generation.validate()?;
        Ok(generation)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.source_schema.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("source schema is empty"));
        }
        if self.source_etag.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("source ETag is empty"));
        }
        validate_sha256(&self.content_sha256, "source content checksum is invalid")?;
        validate_sha256(&self.copy_set_sha256, "source copy-set checksum is invalid")?;
        if self.copies.is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("source copy set is empty"));
        }
        for copy in &self.copies {
            copy.validate()?;
        }
        if !self
            .copies
            .windows(2)
            .all(|pair| (&pair[0].authority, &pair[0].canonical_path) < (&pair[1].authority, &pair[1].canonical_path))
        {
            return Err(IlmRecoveryControlError::Corrupt("source copies are not in unique canonical order"));
        }
        if copy_set_digest(&self.copies)? != self.copy_set_sha256 {
            return Err(IlmRecoveryControlError::Corrupt("source copy-set checksum does not match copies"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryControlIdentity {
    pub protocol: IlmRecoveryProtocol,
    pub canonical_source_path: String,
    pub stable_operation_identity: String,
    pub record_class: String,
}

impl IlmRecoveryControlIdentity {
    pub fn source_operation_digest(&self) -> Result<String> {
        self.validate()?;
        Ok(length_delimited_digest(&[
            self.protocol.as_str().as_bytes(),
            self.canonical_source_path.as_bytes(),
            self.stable_operation_identity.as_bytes(),
        ]))
    }

    fn validate(&self) -> Result<()> {
        validate_canonical_source_path(&self.canonical_source_path)?;
        if self.stable_operation_identity.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("stable operation identity is empty"));
        }
        if self.record_class.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("record class is empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryOwnerLease {
    pub owner_id: String,
    pub owner_epoch: Uuid,
    pub lease_acquired_at_unix_nanos: i64,
    pub lease_expires_at_unix_nanos: i64,
}

impl IlmRecoveryOwnerLease {
    fn validate(&self) -> Result<()> {
        if self.owner_id.trim().is_empty() {
            return Err(IlmRecoveryControlError::Corrupt("owner id is empty"));
        }
        if self.owner_epoch.is_nil() {
            return Err(IlmRecoveryControlError::Corrupt("owner epoch is nil"));
        }
        if self.lease_acquired_at_unix_nanos <= 0 {
            return Err(IlmRecoveryControlError::Corrupt("lease acquisition timestamp is not positive"));
        }
        if self.lease_expires_at_unix_nanos <= self.lease_acquired_at_unix_nanos {
            return Err(IlmRecoveryControlError::Corrupt("lease expiry does not follow acquisition"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryControl {
    pub identity: IlmRecoveryControlIdentity,
    pub first_seen_at_unix_nanos: i64,
    pub observed_source_generation: IlmRecoverySourceGeneration,
    pub revision: u64,
    pub classification: IlmRecoveryClassification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<IlmRecoveryOwnerLease>,
    pub attempt_count: u64,
    pub consecutive_failure_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure_at_unix_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at_unix_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at_unix_nanos: Option<i64>,
    pub last_error_code: IlmRecoveryErrorCode,
}

impl IlmRecoveryControl {
    pub fn new(
        identity: IlmRecoveryControlIdentity,
        observed_source_generation: IlmRecoverySourceGeneration,
        classification: IlmRecoveryClassification,
        now_unix_nanos: i64,
        last_error_code: IlmRecoveryErrorCode,
    ) -> Result<Self> {
        let control = Self {
            identity,
            first_seen_at_unix_nanos: now_unix_nanos,
            observed_source_generation,
            revision: 1,
            classification,
            owner: None,
            attempt_count: 0,
            consecutive_failure_count: 0,
            first_failure_at_unix_nanos: None,
            last_failure_at_unix_nanos: None,
            next_attempt_at_unix_nanos: None,
            last_error_code,
        };
        control.validate()?;
        Ok(control)
    }

    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        self.observed_source_generation.validate()?;
        if self.first_seen_at_unix_nanos <= 0 {
            return Err(IlmRecoveryControlError::Corrupt("first-seen timestamp is not positive"));
        }
        if self.revision == 0 {
            return Err(IlmRecoveryControlError::Corrupt("revision is zero"));
        }
        if let Some(owner) = &self.owner {
            owner.validate()?;
            if !self.classification.permits_automatic_attempt() {
                return Err(IlmRecoveryControlError::Corrupt("non-retrying control carries an owner lease"));
            }
        }
        match (
            self.consecutive_failure_count,
            self.first_failure_at_unix_nanos,
            self.last_failure_at_unix_nanos,
            self.next_attempt_at_unix_nanos,
        ) {
            (0, None, None, None) => {}
            (0, Some(first), Some(last), None) if first > 0 && last >= first => {}
            (0, _, _, _) => return Err(IlmRecoveryControlError::Corrupt("zero failures carry inconsistent history")),
            (_, Some(first), Some(last), next) if first > 0 && last >= first => {
                if self.classification == IlmRecoveryClassification::Retrying && next.is_none_or(|next| next <= last) {
                    return Err(IlmRecoveryControlError::Corrupt("retrying control has no future retry timestamp"));
                }
            }
            _ => return Err(IlmRecoveryControlError::Corrupt("failure counters and timestamps are inconsistent")),
        }
        if u64::from(self.consecutive_failure_count) > self.attempt_count {
            return Err(IlmRecoveryControlError::Corrupt("consecutive failures exceed lifetime attempts"));
        }
        if self.classification == IlmRecoveryClassification::Retrying
            && self.last_error_code == IlmRecoveryErrorCode::None
            && self.consecutive_failure_count > 0
        {
            return Err(IlmRecoveryControlError::Corrupt("failed retry has no bounded error code"));
        }
        Ok(())
    }

    pub fn should_attempt_at(&self, now_unix_nanos: i64) -> bool {
        self.classification.permits_automatic_attempt()
            && self
                .owner
                .as_ref()
                .is_none_or(|owner| owner.lease_expires_at_unix_nanos <= now_unix_nanos)
            && self.next_attempt_at_unix_nanos.is_none_or(|next| next <= now_unix_nanos)
    }

    pub fn claim(
        &mut self,
        owner_id: impl Into<String>,
        owner_epoch: Uuid,
        now_unix_nanos: i64,
        lease_duration_nanos: i64,
    ) -> Result<()> {
        self.claim_for_source_generation(
            owner_id,
            owner_epoch,
            now_unix_nanos,
            lease_duration_nanos,
            self.observed_source_generation.clone(),
        )
    }

    pub fn claim_for_source_generation(
        &mut self,
        owner_id: impl Into<String>,
        owner_epoch: Uuid,
        now_unix_nanos: i64,
        lease_duration_nanos: i64,
        observed_source_generation: IlmRecoverySourceGeneration,
    ) -> Result<()> {
        if !self.should_attempt_at(now_unix_nanos) {
            return Err(IlmRecoveryControlError::InvalidSuccessor("control is not ready for an attempt"));
        }
        let lease_expires_at_unix_nanos = now_unix_nanos
            .checked_add(lease_duration_nanos)
            .ok_or(IlmRecoveryControlError::Corrupt("lease timestamp overflow"))?;
        self.bump_revision()?;
        self.observed_source_generation = observed_source_generation;
        self.owner = Some(IlmRecoveryOwnerLease {
            owner_id: owner_id.into(),
            owner_epoch,
            lease_acquired_at_unix_nanos: now_unix_nanos,
            lease_expires_at_unix_nanos,
        });
        self.attempt_count = self
            .attempt_count
            .checked_add(1)
            .ok_or(IlmRecoveryControlError::Corrupt("attempt count overflow"))?;
        self.validate()
    }

    pub fn record_retryable_failure(&mut self, now_unix_nanos: i64, code: IlmRecoveryErrorCode) -> Result<()> {
        if code == IlmRecoveryErrorCode::None {
            return Err(IlmRecoveryControlError::InvalidSuccessor("retryable failure requires an error code"));
        }
        let attempt_started_at = self
            .owner
            .as_ref()
            .ok_or(IlmRecoveryControlError::InvalidSuccessor("retryable failure requires an owner lease"))?
            .lease_acquired_at_unix_nanos;
        if now_unix_nanos < attempt_started_at {
            return Err(IlmRecoveryControlError::InvalidSuccessor("retryable failure predates its owner claim"));
        }
        let failures = self
            .consecutive_failure_count
            .checked_add(1)
            .ok_or(IlmRecoveryControlError::Corrupt("failure count overflow"))?;
        let first_failure_at = self.first_failure_at_unix_nanos.unwrap_or(attempt_started_at);
        let age = now_unix_nanos.saturating_sub(first_failure_at);
        self.bump_revision()?;
        self.owner = None;
        self.consecutive_failure_count = failures;
        self.first_failure_at_unix_nanos = Some(first_failure_at);
        self.last_failure_at_unix_nanos = Some(now_unix_nanos);
        self.last_error_code = code;
        if failures >= MAX_RECOVERY_ATTEMPTS
            || self.attempt_count >= u64::from(MAX_RECOVERY_ATTEMPTS)
            || age >= MAX_RECOVERY_AGE_NANOS
        {
            self.classification = IlmRecoveryClassification::OperatorRequired;
            self.next_attempt_at_unix_nanos = None;
        } else {
            self.classification = IlmRecoveryClassification::Retrying;
            self.next_attempt_at_unix_nanos = Some(
                now_unix_nanos
                    .checked_add(retry_delay_nanos(
                        &self.observed_source_generation.copy_set_sha256,
                        self.attempt_count,
                        failures,
                    ))
                    .ok_or(IlmRecoveryControlError::Corrupt("retry timestamp overflow"))?,
            );
        }
        self.validate()
    }

    pub fn record_expired_attempt(&mut self, now_unix_nanos: i64) -> Result<()> {
        let lease_expires_at = self
            .owner
            .as_ref()
            .ok_or(IlmRecoveryControlError::InvalidSuccessor("expired attempt requires an owner lease"))?
            .lease_expires_at_unix_nanos;
        if lease_expires_at > now_unix_nanos {
            return Err(IlmRecoveryControlError::InvalidSuccessor("attempt owner lease is still active"));
        }
        self.record_retryable_failure(now_unix_nanos, IlmRecoveryErrorCode::AttemptLeaseExpired)
    }

    pub fn refresh_owned_source_generation(&mut self, observed_source_generation: IlmRecoverySourceGeneration) -> Result<()> {
        if self.owner.is_none() || self.classification != IlmRecoveryClassification::Retrying {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "source generation refresh requires a retrying owner",
            ));
        }
        self.bump_revision()?;
        self.observed_source_generation = observed_source_generation;
        self.validate()
    }

    pub fn finish_attempt(&mut self, classification: IlmRecoveryClassification, code: IlmRecoveryErrorCode) -> Result<()> {
        if self.owner.is_none() {
            return Err(IlmRecoveryControlError::InvalidSuccessor("finishing an attempt requires an owner lease"));
        }
        if classification == IlmRecoveryClassification::Retrying {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "successful attempt result cannot remain retrying",
            ));
        }
        self.bump_revision()?;
        self.owner = None;
        self.classification = classification;
        self.consecutive_failure_count = 0;
        self.next_attempt_at_unix_nanos = None;
        self.last_error_code = code;
        self.validate()
    }

    pub fn abandon_for_operator(&mut self, expected_source_generation: &IlmRecoverySourceGeneration) -> Result<()> {
        if self.owner.is_some()
            || self.classification != IlmRecoveryClassification::RetainedAmbiguous
            || &self.observed_source_generation != expected_source_generation
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "operator abandonment requires the exact ownerless retained source generation",
            ));
        }
        self.bump_revision()?;
        self.classification = IlmRecoveryClassification::Abandoned;
        self.validate()
    }

    pub fn retry_for_operator(&mut self, expected_source_generation: &IlmRecoverySourceGeneration) -> Result<()> {
        if self.owner.is_some()
            || !matches!(
                self.classification,
                IlmRecoveryClassification::RetainedAmbiguous | IlmRecoveryClassification::OperatorRequired
            )
            || self.attempt_count == u64::MAX
            || &self.observed_source_generation != expected_source_generation
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "operator retry requires the exact ownerless retained source generation",
            ));
        }
        self.bump_revision()?;
        self.classification = IlmRecoveryClassification::Retrying;
        self.consecutive_failure_count = 0;
        self.next_attempt_at_unix_nanos = None;
        self.validate()
    }

    pub fn validate_successor(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        if self.identity != next.identity || self.first_seen_at_unix_nanos != next.first_seen_at_unix_nanos {
            return Err(IlmRecoveryControlError::InvalidSuccessor("immutable identity changed"));
        }
        if self.revision.checked_add(1) != Some(next.revision) {
            return Err(IlmRecoveryControlError::InvalidSuccessor("revision did not advance by one"));
        }
        match (&self.owner, &next.owner) {
            (Some(current_owner), Some(next_owner)) if current_owner == next_owner => {
                self.validate_source_refresh_successor(next)
            }
            (_, Some(_)) => self.validate_claim_successor(next),
            (Some(_), None)
                if self
                    .consecutive_failure_count
                    .checked_add(1)
                    .is_some_and(|failures| next.consecutive_failure_count == failures) =>
            {
                self.validate_failure_successor(next)
            }
            (Some(_), None) => self.validate_finish_successor(next),
            (None, None)
                if self.classification == IlmRecoveryClassification::RetainedAmbiguous
                    && next.classification == IlmRecoveryClassification::Abandoned =>
            {
                self.validate_operator_abandon_successor(next)
            }
            (None, None)
                if matches!(
                    self.classification,
                    IlmRecoveryClassification::RetainedAmbiguous | IlmRecoveryClassification::OperatorRequired
                ) && next.classification == IlmRecoveryClassification::Retrying =>
            {
                self.validate_operator_retry_successor(next)
            }
            (None, None) => Err(IlmRecoveryControlError::InvalidSuccessor(
                "ownerless control cannot advance without a claim",
            )),
        }
    }

    fn validate_operator_abandon_successor(&self, next: &Self) -> Result<()> {
        if next.observed_source_generation != self.observed_source_generation
            || next.attempt_count != self.attempt_count
            || next.consecutive_failure_count != self.consecutive_failure_count
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos
            || next.last_failure_at_unix_nanos != self.last_failure_at_unix_nanos
            || next.next_attempt_at_unix_nanos != self.next_attempt_at_unix_nanos
            || next.last_error_code != self.last_error_code
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "operator abandonment changed recovery history or source generation",
            ));
        }
        Ok(())
    }

    fn validate_operator_retry_successor(&self, next: &Self) -> Result<()> {
        if next.observed_source_generation != self.observed_source_generation
            || next.attempt_count != self.attempt_count
            || next.consecutive_failure_count != 0
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos
            || next.last_failure_at_unix_nanos != self.last_failure_at_unix_nanos
            || next.next_attempt_at_unix_nanos.is_some()
            || next.last_error_code != self.last_error_code
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "operator retry changed recovery history or source generation",
            ));
        }
        Ok(())
    }

    fn validate_claim_successor(&self, next: &Self) -> Result<()> {
        if self.classification != IlmRecoveryClassification::Retrying
            || next.classification != IlmRecoveryClassification::Retrying
            || self
                .attempt_count
                .checked_add(1)
                .is_none_or(|attempts| next.attempt_count != attempts)
            || next.consecutive_failure_count != self.consecutive_failure_count
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos
            || next.last_failure_at_unix_nanos != self.last_failure_at_unix_nanos
            || next.next_attempt_at_unix_nanos != self.next_attempt_at_unix_nanos
            || next.last_error_code != self.last_error_code
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor("claim changed non-owner recovery state"));
        }
        Ok(())
    }

    fn validate_source_refresh_successor(&self, next: &Self) -> Result<()> {
        if self.classification != IlmRecoveryClassification::Retrying
            || next.classification != IlmRecoveryClassification::Retrying
            || next.attempt_count != self.attempt_count
            || next.consecutive_failure_count != self.consecutive_failure_count
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos
            || next.last_failure_at_unix_nanos != self.last_failure_at_unix_nanos
            || next.next_attempt_at_unix_nanos != self.next_attempt_at_unix_nanos
            || next.last_error_code != self.last_error_code
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "source generation refresh changed non-source recovery state",
            ));
        }
        Ok(())
    }

    fn validate_failure_successor(&self, next: &Self) -> Result<()> {
        let owner = self
            .owner
            .as_ref()
            .ok_or(IlmRecoveryControlError::InvalidSuccessor("retry failure has no owner claim"))?;
        if next.observed_source_generation != self.observed_source_generation
            || next.attempt_count != self.attempt_count
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos.or(Some(owner.lease_acquired_at_unix_nanos))
            || next.last_error_code == IlmRecoveryErrorCode::None
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor("retry failure changed immutable attempt state"));
        }
        let last_failure = next
            .last_failure_at_unix_nanos
            .ok_or(IlmRecoveryControlError::InvalidSuccessor("retry failure has no timestamp"))?;
        let first_failure = next
            .first_failure_at_unix_nanos
            .ok_or(IlmRecoveryControlError::InvalidSuccessor("retry failure has no first timestamp"))?;
        if self
            .owner
            .as_ref()
            .is_none_or(|owner| last_failure < owner.lease_acquired_at_unix_nanos)
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor("retry failure predates its owner claim"));
        }
        let exhausted = next.consecutive_failure_count >= MAX_RECOVERY_ATTEMPTS
            || next.attempt_count >= u64::from(MAX_RECOVERY_ATTEMPTS)
            || last_failure.saturating_sub(first_failure) >= MAX_RECOVERY_AGE_NANOS;
        let expected_next = if exhausted {
            None
        } else {
            Some(
                last_failure
                    .checked_add(retry_delay_nanos(
                        &next.observed_source_generation.copy_set_sha256,
                        next.attempt_count,
                        next.consecutive_failure_count,
                    ))
                    .ok_or(IlmRecoveryControlError::InvalidSuccessor("retry timestamp overflowed"))?,
            )
        };
        if next.classification
            != if exhausted {
                IlmRecoveryClassification::OperatorRequired
            } else {
                IlmRecoveryClassification::Retrying
            }
            || next.next_attempt_at_unix_nanos != expected_next
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "retry failure has an invalid terminal or backoff state",
            ));
        }
        Ok(())
    }

    fn validate_finish_successor(&self, next: &Self) -> Result<()> {
        if next.observed_source_generation != self.observed_source_generation
            || next.attempt_count != self.attempt_count
            || next.classification == IlmRecoveryClassification::Retrying
            || next.consecutive_failure_count != 0
            || next.first_failure_at_unix_nanos != self.first_failure_at_unix_nanos
            || next.last_failure_at_unix_nanos != self.last_failure_at_unix_nanos
            || next.next_attempt_at_unix_nanos.is_some()
        {
            return Err(IlmRecoveryControlError::InvalidSuccessor(
                "finished attempt changed immutable recovery state",
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let control_bytes = serde_json::to_vec(self)?;
        let persisted = PersistedIlmRecoveryControl {
            schema: ILM_RECOVERY_CONTROL_SCHEMA.to_string(),
            content_sha256: hex_sha256(&control_bytes, ToOwned::to_owned),
            control: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_ILM_RECOVERY_CONTROL_SIZE {
            return Err(IlmRecoveryControlError::Corrupt("encoded control exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_control_id: &str, data: &[u8]) -> Result<Self> {
        validate_sha256(expected_control_id, "control id is invalid")?;
        if data.len() > MAX_ILM_RECOVERY_CONTROL_SIZE {
            return Err(IlmRecoveryControlError::Corrupt("encoded control exceeds maximum size"));
        }
        let persisted: PersistedIlmRecoveryControl = serde_json::from_slice(data)?;
        if persisted.schema != ILM_RECOVERY_CONTROL_SCHEMA {
            return Err(IlmRecoveryControlError::UnsupportedSchema(persisted.schema));
        }
        validate_sha256(&persisted.content_sha256, "content checksum is invalid")?;
        let control_bytes = serde_json::to_vec(&persisted.control)?;
        if hex_sha256(&control_bytes, ToOwned::to_owned) != persisted.content_sha256 {
            return Err(IlmRecoveryControlError::ChecksumMismatch);
        }
        if persisted.control.identity.source_operation_digest()? != expected_control_id {
            return Err(IlmRecoveryControlError::Corrupt("control id does not match record key"));
        }
        persisted.control.validate()?;
        Ok(persisted.control)
    }

    fn bump_revision(&mut self) -> Result<()> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(IlmRecoveryControlError::Corrupt("revision overflow"))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedIlmRecoveryControl {
    pub control: IlmRecoveryControl,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedIlmRecoverySource {
    pub generation: IlmRecoverySourceGeneration,
    pub canonical_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IlmRecoveryControlView {
    pub control_id: String,
    pub protocol: IlmRecoveryProtocol,
    pub classification: IlmRecoveryClassification,
    pub schema: &'static str,
    pub revision: u64,
    pub attempt_count: u64,
    pub consecutive_failure_count: u32,
    pub first_seen_at_unix_nanos: i64,
    pub first_failure_at_unix_nanos: Option<i64>,
    pub last_failure_at_unix_nanos: Option<i64>,
    pub next_attempt_at_unix_nanos: Option<i64>,
    pub last_error_code: IlmRecoveryErrorCode,
    pub source_schema: String,
    pub source_generation_sha256: String,
    pub copy_set_sha256: String,
    pub source_copy_count: usize,
}

impl IlmRecoveryControlView {
    fn from_control(control_id: String, control: &IlmRecoveryControl) -> EcstoreResult<Self> {
        let generation = serde_json::to_vec(&control.observed_source_generation).map_err(Error::other)?;
        Ok(Self {
            control_id,
            protocol: control.identity.protocol,
            classification: control.classification,
            schema: ILM_RECOVERY_CONTROL_SCHEMA,
            revision: control.revision,
            attempt_count: control.attempt_count,
            consecutive_failure_count: control.consecutive_failure_count,
            first_seen_at_unix_nanos: control.first_seen_at_unix_nanos,
            first_failure_at_unix_nanos: control.first_failure_at_unix_nanos,
            last_failure_at_unix_nanos: control.last_failure_at_unix_nanos,
            next_attempt_at_unix_nanos: control.next_attempt_at_unix_nanos,
            last_error_code: control.last_error_code,
            source_schema: control.observed_source_generation.source_schema.clone(),
            source_generation_sha256: hex_sha256(&generation, ToOwned::to_owned),
            copy_set_sha256: control.observed_source_generation.copy_set_sha256.clone(),
            source_copy_count: control.observed_source_generation.copies.len(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IlmRecoveryControlPage {
    pub records: Vec<IlmRecoveryControlView>,
    pub next_marker: Option<String>,
    pub truncated: bool,
    pub incomplete: bool,
}

impl ObservedIlmRecoverySource {
    pub fn is_consistent(&self) -> bool {
        self.canonical_data.is_some()
            && self.generation.copies.iter().all(|copy| {
                copy.etag == self.generation.source_etag
                    && copy.content_sha256 == self.generation.content_sha256
                    && copy.encoded_len
                        == self
                            .canonical_data
                            .as_ref()
                            .map_or(0, |data| u64::try_from(data.len()).unwrap_or(u64::MAX))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIlmRecoveryControl {
    schema: String,
    content_sha256: String,
    control: IlmRecoveryControl,
}

pub fn recovery_control_record_object_name(protocol: IlmRecoveryProtocol, control_id: &str) -> Result<String> {
    validate_sha256(control_id, "control id is invalid")?;
    Ok(format!(
        "{}/{}/{}/{}/{}.json",
        ILM_RECOVERY_CONTROL_PREFIX,
        protocol.as_str(),
        &control_id[..2],
        &control_id[2..4],
        control_id
    ))
}

pub fn recovery_control_id_from_record_object_name(object: &str) -> Result<(IlmRecoveryProtocol, String)> {
    let suffix = object
        .strip_prefix(ILM_RECOVERY_CONTROL_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or(IlmRecoveryControlError::Corrupt("control record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let protocol = match parts.next() {
        Some("transition_transaction") => IlmRecoveryProtocol::TransitionTransaction,
        Some("tier_delete_journal") => IlmRecoveryProtocol::TierDeleteJournal,
        Some("tier_delete_manifest") => IlmRecoveryProtocol::TierDeleteManifest,
        _ => return Err(IlmRecoveryControlError::Corrupt("control record protocol is invalid")),
    };
    let shard_a = parts
        .next()
        .ok_or(IlmRecoveryControlError::Corrupt("control record path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or(IlmRecoveryControlError::Corrupt("control record path is incomplete"))?;
    let control_id = parts
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .ok_or(IlmRecoveryControlError::Corrupt("control record suffix is invalid"))?;
    if parts.next().is_some() {
        return Err(IlmRecoveryControlError::Corrupt("control record path is not canonical"));
    }
    validate_sha256(control_id, "control id is invalid")?;
    if shard_a != &control_id[..2] || shard_b != &control_id[2..4] {
        return Err(IlmRecoveryControlError::Corrupt("control record shard does not match control id"));
    }
    Ok((protocol, control_id.to_string()))
}

pub async fn save_recovery_control_if_absent(api: Arc<ECStore>, control: &IlmRecoveryControl) -> EcstoreResult<()> {
    let control_id = control
        .identity
        .source_operation_digest()
        .map_err(recovery_control_store_error)?;
    let object =
        recovery_control_record_object_name(control.identity.protocol, &control_id).map_err(recovery_control_store_error)?;
    let data = control.encode().map_err(recovery_control_store_error)?;
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
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

pub async fn observe_recovery_source(
    api: Arc<ECStore>,
    canonical_path: &str,
    source_schema: &str,
) -> EcstoreResult<ObservedIlmRecoverySource> {
    observe_recovery_source_with_options(api, canonical_path, source_schema, false).await
}

pub(crate) async fn observe_recovery_source_no_lock(
    api: Arc<ECStore>,
    canonical_path: &str,
    source_schema: &str,
) -> EcstoreResult<ObservedIlmRecoverySource> {
    observe_recovery_source_with_options(api, canonical_path, source_schema, true).await
}

async fn observe_recovery_source_with_options(
    api: Arc<ECStore>,
    canonical_path: &str,
    source_schema: &str,
    no_lock: bool,
) -> EcstoreResult<ObservedIlmRecoverySource> {
    validate_canonical_source_path(canonical_path).map_err(recovery_control_store_error)?;
    if source_schema.trim().is_empty() {
        return Err(Error::other("ILM recovery source schema is empty"));
    }

    let mut copies = Vec::new();
    let mut observations = Vec::new();
    for set in api.all_set_disks() {
        let authority = format!("pool-{}/set-{}", set.pool_index, set.set_index);
        match config_boundary::read_config_with_metadata(
            set,
            canonical_path,
            &ObjectOptions {
                no_lock,
                ..Default::default()
            },
        )
        .await
        {
            Ok((data, metadata)) => {
                let etag = metadata
                    .etag
                    .filter(|etag| !etag.trim().is_empty())
                    .ok_or_else(|| Error::other("ILM recovery source copy is missing an ETag"))?;
                let encoded_len =
                    u64::try_from(data.len()).map_err(|_| Error::other("ILM recovery source copy length does not fit u64"))?;
                let content_sha256 = hex_sha256(&data, ToOwned::to_owned);
                copies.push(IlmRecoverySourceCopy {
                    authority,
                    canonical_path: canonical_path.to_string(),
                    etag: etag.clone(),
                    encoded_len,
                    content_sha256: content_sha256.clone(),
                });
                observations.push((etag, content_sha256, data));
            }
            Err(err) if recovery_source_is_missing(&err) => {}
            Err(err) => return Err(err),
        }
    }
    let Some((source_etag, content_sha256, first_data)) = observations.first().cloned() else {
        return Err(Error::ConfigNotFound);
    };
    let consistent = observations
        .iter()
        .all(|(etag, digest, data)| etag == &source_etag && digest == &content_sha256 && data == &first_data);
    let generation = IlmRecoverySourceGeneration::new(source_schema, source_etag, content_sha256, copies)
        .map_err(recovery_control_store_error)?;
    Ok(ObservedIlmRecoverySource {
        generation,
        canonical_data: consistent.then_some(first_data),
    })
}

pub async fn load_recovery_control(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    control_id: &str,
) -> EcstoreResult<ObservedIlmRecoveryControl> {
    let object = recovery_control_record_object_name(protocol, control_id).map_err(recovery_control_store_error)?;
    let (data, metadata) = config_boundary::read_config_with_metadata(api, &object, &ObjectOptions::default()).await?;
    let etag = metadata
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("ILM recovery control is missing an ETag"))?;
    let control = IlmRecoveryControl::decode(control_id, &data).map_err(recovery_control_store_error)?;
    if control.identity.protocol != protocol {
        return Err(Error::other("ILM recovery control protocol does not match record path"));
    }
    Ok(ObservedIlmRecoveryControl { control, etag })
}

pub async fn inspect_recovery_control(api: Arc<ECStore>, control_id: &str) -> EcstoreResult<IlmRecoveryControlView> {
    validate_sha256(control_id, "control id is invalid").map_err(recovery_control_store_error)?;
    for protocol in IlmRecoveryProtocol::all() {
        match load_recovery_control(api.clone(), protocol, control_id).await {
            Ok(observed) => return IlmRecoveryControlView::from_control(control_id.to_string(), &observed.control),
            Err(Error::ConfigNotFound) => {}
            Err(err) => return Err(err),
        }
    }
    Err(Error::ConfigNotFound)
}

pub async fn list_recovery_controls(
    api: Arc<ECStore>,
    protocol: IlmRecoveryProtocol,
    classification: Option<IlmRecoveryClassification>,
    limit: usize,
    marker: Option<String>,
) -> EcstoreResult<IlmRecoveryControlPage> {
    if !(1..=1_000).contains(&limit) {
        return Err(Error::other("ILM recovery control list limit must be between 1 and 1000"));
    }
    let prefix = format!("{}/{}/", ILM_RECOVERY_CONTROL_PREFIX, protocol.as_str());
    let page = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            &prefix,
            marker,
            None,
            i32::try_from(limit).unwrap_or(1_000),
            false,
            None,
            false,
        )
        .await?;
    if page.is_truncated && page.next_continuation_token.is_none() {
        return Err(Error::other(
            "ILM recovery control list returned a truncated page without a continuation marker",
        ));
    }

    let mut records = Vec::new();
    let mut incomplete = false;
    for object in page.objects {
        let parsed = recovery_control_id_from_record_object_name(&object.name);
        let (path_protocol, control_id) = match parsed {
            Ok(parsed) if parsed.0 == protocol => parsed,
            Ok(_) | Err(_) => {
                incomplete = true;
                continue;
            }
        };
        match load_recovery_control(api.clone(), path_protocol, &control_id).await {
            Ok(observed) if classification.is_none_or(|filter| observed.control.classification == filter) => {
                records.push(IlmRecoveryControlView::from_control(control_id, &observed.control)?);
            }
            Ok(_) => {}
            Err(Error::ConfigNotFound) => {}
            Err(_) => incomplete = true,
        }
    }

    Ok(IlmRecoveryControlPage {
        records,
        next_marker: page.next_continuation_token,
        truncated: page.is_truncated,
        incomplete,
    })
}

pub async fn save_recovery_control_if_current(
    api: Arc<ECStore>,
    current: &ObservedIlmRecoveryControl,
    next: &IlmRecoveryControl,
) -> EcstoreResult<()> {
    current
        .control
        .validate_successor(next)
        .map_err(recovery_control_store_error)?;
    let control_id = current
        .control
        .identity
        .source_operation_digest()
        .map_err(recovery_control_store_error)?;
    let authoritative = load_recovery_control(api.clone(), current.control.identity.protocol, &control_id).await?;
    if &authoritative != current {
        return Err(Error::PreconditionFailed);
    }
    let object = recovery_control_record_object_name(current.control.identity.protocol, &control_id)
        .map_err(recovery_control_store_error)?;
    let data = next.encode().map_err(recovery_control_store_error)?;
    config_boundary::save_config_with_opts(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(current.etag.clone()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

fn retry_delay_nanos(copy_set_sha256: &str, attempt_count: u64, consecutive_failure_count: u32) -> i64 {
    let exponent = consecutive_failure_count.saturating_sub(1).min(6);
    let base = MIN_RETRY_DELAY_NANOS
        .saturating_mul(1_i64 << exponent)
        .min(MAX_RETRY_DELAY_NANOS);
    let seed = length_delimited_digest(&[copy_set_sha256.as_bytes(), &attempt_count.to_be_bytes()]);
    let jitter_bucket = u8::from_str_radix(&seed[..2], 16).unwrap_or(0) % 21;
    base.saturating_mul(i64::from(80 + jitter_bucket)) / 100
}

fn copy_set_digest(copies: &[IlmRecoverySourceCopy]) -> Result<String> {
    let encoded = serde_json::to_vec(copies)?;
    Ok(hex_sha256(&encoded, ToOwned::to_owned))
}

fn length_delimited_digest(parts: &[&[u8]]) -> String {
    let mut encoded = Vec::new();
    for part in parts {
        encoded.extend_from_slice(&(part.len() as u64).to_be_bytes());
        encoded.extend_from_slice(part);
    }
    hex_sha256(&encoded, ToOwned::to_owned)
}

fn validate_canonical_source_path(path: &str) -> Result<()> {
    if path.is_empty() || path.starts_with('/') || path.ends_with('/') || path.split('/').any(|part| part.is_empty()) {
        return Err(IlmRecoveryControlError::Corrupt("canonical source path is invalid"));
    }
    Ok(())
}

fn validate_sha256(value: &str, message: &'static str) -> Result<()> {
    if !is_sha256_checksum(value)
        || value
            .bytes()
            .any(|byte| byte.is_ascii_hexdigit() && byte.is_ascii_uppercase())
    {
        return Err(IlmRecoveryControlError::Corrupt(message));
    }
    Ok(())
}

fn recovery_control_store_error(err: IlmRecoveryControlError) -> Error {
    Error::other(err)
}

fn recovery_source_is_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE_PATH: &str = "ilm/transition-transactions/records/12/34/1234567890abcdef1234567890abcdef.json";

    fn generation() -> IlmRecoverySourceGeneration {
        let content_sha256 = hex_sha256(b"source", ToOwned::to_owned);
        IlmRecoverySourceGeneration::new(
            "rustfs-transition-transaction-v1",
            "etag-a",
            content_sha256.clone(),
            vec![
                IlmRecoverySourceCopy {
                    authority: "pool-1/set-0".to_string(),
                    canonical_path: SOURCE_PATH.to_string(),
                    etag: "etag-b".to_string(),
                    encoded_len: 6,
                    content_sha256: content_sha256.clone(),
                },
                IlmRecoverySourceCopy {
                    authority: "pool-0/set-1".to_string(),
                    canonical_path: SOURCE_PATH.to_string(),
                    etag: "etag-a".to_string(),
                    encoded_len: 6,
                    content_sha256,
                },
            ],
        )
        .expect("source generation should build")
    }

    fn control() -> IlmRecoveryControl {
        IlmRecoveryControl::new(
            IlmRecoveryControlIdentity {
                protocol: IlmRecoveryProtocol::TransitionTransaction,
                canonical_source_path: SOURCE_PATH.to_string(),
                stable_operation_identity: "12345678-90ab-cdef-1234-567890abcdef".to_string(),
                record_class: "transition_transaction_v1".to_string(),
            },
            generation(),
            IlmRecoveryClassification::Retrying,
            1_000_000_000,
            IlmRecoveryErrorCode::None,
        )
        .expect("control should build")
    }

    #[test]
    fn recovery_control_round_trip_and_canonical_path() {
        let control = control();
        let control_id = control.identity.source_operation_digest().expect("control id should derive");
        let path =
            recovery_control_record_object_name(control.identity.protocol, &control_id).expect("control path should build");
        assert_eq!(
            recovery_control_id_from_record_object_name(&path).expect("control path should parse"),
            (control.identity.protocol, control_id.clone())
        );
        let encoded = control.encode().expect("control should encode");
        assert_eq!(IlmRecoveryControl::decode(&control_id, &encoded).expect("control should decode"), control);
    }

    #[test]
    fn recovery_control_rejects_noncanonical_copy_set_and_tampering() {
        let mut noncanonical = control();
        noncanonical.observed_source_generation.copies.swap(0, 1);
        assert!(matches!(noncanonical.validate(), Err(IlmRecoveryControlError::Corrupt(_))));

        let control = control();
        let control_id = control.identity.source_operation_digest().expect("control id should derive");
        let mut persisted: serde_json::Value =
            serde_json::from_slice(&control.encode().expect("control should encode")).expect("encoded control should be json");
        persisted["control"]["attempt_count"] = serde_json::json!(9);
        let tampered = serde_json::to_vec(&persisted).expect("tampered json should encode");
        assert!(matches!(
            IlmRecoveryControl::decode(&control_id, &tampered),
            Err(IlmRecoveryControlError::ChecksumMismatch)
        ));
    }

    #[test]
    fn recovery_control_persists_deterministic_bounded_backoff() {
        let mut first = control();
        let mut second = first.clone();
        for control in [&mut first, &mut second] {
            control
                .claim("node-a", Uuid::new_v4(), 2_000_000_000, 300_000_000_000)
                .expect("attempt should claim");
            control
                .record_retryable_failure(3_000_000_000, IlmRecoveryErrorCode::BackendTimeout)
                .expect("failure should schedule retry");
        }
        assert_eq!(first.next_attempt_at_unix_nanos, second.next_attempt_at_unix_nanos);
        let delay = first.next_attempt_at_unix_nanos.expect("retry time") - 3_000_000_000;
        assert!((48_000_000_000..=60_000_000_000).contains(&delay));
        assert!(!first.should_attempt_at(first.next_attempt_at_unix_nanos.expect("retry time") - 1));
        assert!(first.should_attempt_at(first.next_attempt_at_unix_nanos.expect("retry time")));
    }

    #[test]
    fn recovery_control_stops_after_bounded_failures() {
        let mut control = control();
        let mut now = 2_000_000_000;
        for _ in 0..MAX_RECOVERY_ATTEMPTS {
            let ready = control.next_attempt_at_unix_nanos.unwrap_or(now);
            now = now.max(ready);
            control
                .claim("node-a", Uuid::new_v4(), now, 300_000_000_000)
                .expect("attempt should claim");
            control
                .record_retryable_failure(now + 1, IlmRecoveryErrorCode::BackendTimeout)
                .expect("failure should persist");
            now += 2;
        }
        assert_eq!(control.classification, IlmRecoveryClassification::OperatorRequired);
        assert_eq!(control.attempt_count, u64::from(MAX_RECOVERY_ATTEMPTS));
        assert_eq!(control.next_attempt_at_unix_nanos, None);
    }

    #[test]
    fn recovery_control_expired_timeout_and_cancellation_attempts_stop_at_bounds() {
        let mut active = control();
        active
            .claim("node-a", Uuid::new_v4(), 2_000_000_000, 2)
            .expect("active attempt should claim");
        assert!(matches!(
            active.record_expired_attempt(2_000_000_001),
            Err(IlmRecoveryControlError::InvalidSuccessor("attempt owner lease is still active"))
        ));

        let mut bounded = control();
        let mut now = 2_000_000_000;
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            now = now.max(bounded.next_attempt_at_unix_nanos.unwrap_or(now));
            bounded
                .claim("node-a", Uuid::new_v4(), now, 1)
                .expect("timeout or cancellation attempt should claim");
            let abandonment = if attempt % 2 == 0 { "cancellation" } else { "timeout" };
            bounded
                .record_expired_attempt(now + 1)
                .unwrap_or_else(|err| panic!("expired {abandonment} attempt should consume its budget: {err}"));
            if attempt < MAX_RECOVERY_ATTEMPTS {
                assert_eq!(bounded.classification, IlmRecoveryClassification::Retrying);
            }
            now += 2;
        }

        assert_eq!(bounded.classification, IlmRecoveryClassification::OperatorRequired);
        assert_eq!(bounded.attempt_count, u64::from(MAX_RECOVERY_ATTEMPTS));
        assert_eq!(bounded.consecutive_failure_count, MAX_RECOVERY_ATTEMPTS);
        assert_eq!(bounded.last_error_code, IlmRecoveryErrorCode::AttemptLeaseExpired);
        assert_eq!(bounded.next_attempt_at_unix_nanos, None);

        let mut younger = control();
        younger
            .claim("node-a", Uuid::new_v4(), now, 1)
            .expect("younger attempt should claim");
        younger
            .record_expired_attempt(now + MAX_RECOVERY_AGE_NANOS - 1)
            .expect("younger expired attempt should be recorded");
        assert_eq!(younger.classification, IlmRecoveryClassification::Retrying);

        let mut aged = control();
        aged.claim("node-a", Uuid::new_v4(), now, 1)
            .expect("aged attempt should claim");
        aged.record_expired_attempt(now + MAX_RECOVERY_AGE_NANOS)
            .expect("seven-day expired attempt should be recorded");
        assert_eq!(aged.classification, IlmRecoveryClassification::OperatorRequired);
        assert_eq!(aged.consecutive_failure_count, 1);
    }

    #[test]
    fn recovery_control_successor_preserves_lineage_and_generation() {
        let current = control();
        let mut next = current.clone();
        let mut advanced_generation = generation();
        advanced_generation.source_schema = "rustfs-transition-transaction-v2".to_string();
        next.claim_for_source_generation("node-a", Uuid::new_v4(), 2_000_000_000, 300_000_000_000, advanced_generation)
            .expect("attempt should claim");
        current
            .validate_successor(&next)
            .expect("a claim may adopt a newly proven source generation");

        let mut changed = next.clone();
        let mut refreshed_generation = changed.observed_source_generation.clone();
        refreshed_generation.source_schema = "rustfs-transition-transaction-v3".to_string();
        changed
            .refresh_owned_source_generation(refreshed_generation)
            .expect("the current owner may refresh a newly proven source generation");
        next.validate_successor(&changed)
            .expect("owned source refresh should be a legal successor");

        let mut invalid = changed.clone();
        invalid.revision += 1;
        invalid.attempt_count += 1;
        assert!(matches!(
            changed.validate_successor(&invalid),
            Err(IlmRecoveryControlError::InvalidSuccessor(_))
        ));
    }

    #[test]
    fn operator_abandonment_is_an_exact_ownerless_retained_successor() {
        let mut retained = IlmRecoveryControl::new(
            control().identity,
            generation(),
            IlmRecoveryClassification::RetainedAmbiguous,
            1_000_000_000,
            IlmRecoveryErrorCode::OperatorDispositionRequired,
        )
        .expect("retained control should build");
        let previous = retained.clone();
        retained
            .abandon_for_operator(&previous.observed_source_generation)
            .expect("exact retained generation should be abandonable");
        previous
            .validate_successor(&retained)
            .expect("operator abandonment should be a valid successor");
        assert_eq!(retained.classification, IlmRecoveryClassification::Abandoned);
        assert_eq!(retained.revision, previous.revision + 1);

        let mut wrong_generation = previous.clone();
        let mut generation = previous.observed_source_generation.clone();
        generation.source_etag = "different".to_string();
        assert!(wrong_generation.abandon_for_operator(&generation).is_err());

        let mut mutated_history = retained.clone();
        mutated_history.attempt_count += 1;
        assert!(previous.validate_successor(&mutated_history).is_err());
    }

    #[test]
    fn operator_retry_rearms_exact_retained_generation_without_resetting_history() {
        for classification in [
            IlmRecoveryClassification::RetainedAmbiguous,
            IlmRecoveryClassification::OperatorRequired,
        ] {
            let mut retained = control();
            retained
                .claim("node-a", Uuid::new_v4(), 2_000_000_000, 1)
                .expect("attempt should claim");
            retained
                .record_retryable_failure(2_000_000_001, IlmRecoveryErrorCode::BackendTimeout)
                .expect("failure should persist");
            retained.classification = classification;
            retained.next_attempt_at_unix_nanos = None;
            if classification == IlmRecoveryClassification::OperatorRequired {
                retained.attempt_count = u64::from(MAX_RECOVERY_ATTEMPTS);
                retained.consecutive_failure_count = MAX_RECOVERY_ATTEMPTS;
            }
            retained.validate().expect("retained control should remain valid");

            let previous = retained.clone();
            retained
                .retry_for_operator(&previous.observed_source_generation)
                .expect("exact retained generation should be retryable");
            previous
                .validate_successor(&retained)
                .expect("operator retry should be a valid successor");
            assert_eq!(retained.classification, IlmRecoveryClassification::Retrying);
            assert_eq!(retained.revision, previous.revision + 1);
            assert_eq!(retained.attempt_count, previous.attempt_count);
            assert_eq!(retained.first_failure_at_unix_nanos, previous.first_failure_at_unix_nanos);
            assert_eq!(retained.last_failure_at_unix_nanos, previous.last_failure_at_unix_nanos);
            assert_eq!(retained.last_error_code, previous.last_error_code);
            assert_eq!(retained.consecutive_failure_count, 0);
            assert_eq!(retained.next_attempt_at_unix_nanos, None);
            assert!(retained.should_attempt_at(2_000_000_002));

            if classification == IlmRecoveryClassification::OperatorRequired {
                retained
                    .claim("node-b", Uuid::new_v4(), 2_000_000_002, 1)
                    .expect("operator retry should authorize one new bounded attempt");
                retained
                    .record_retryable_failure(2_000_000_003, IlmRecoveryErrorCode::BackendTimeout)
                    .expect("the bounded attempt failure should persist");
                assert_eq!(retained.classification, IlmRecoveryClassification::OperatorRequired);
                assert_eq!(retained.attempt_count, u64::from(MAX_RECOVERY_ATTEMPTS) + 1);
                assert_eq!(retained.consecutive_failure_count, 1);
            }

            let mut wrong_generation = previous;
            let mut changed_generation = wrong_generation.observed_source_generation.clone();
            changed_generation.source_etag = "changed".to_string();
            assert!(wrong_generation.retry_for_operator(&changed_generation).is_err());
        }

        let mut exhausted = control();
        exhausted.classification = IlmRecoveryClassification::OperatorRequired;
        exhausted.attempt_count = u64::MAX;
        let generation = exhausted.observed_source_generation.clone();
        assert!(exhausted.retry_for_operator(&generation).is_err());
    }

    #[test]
    fn recovery_control_view_redacts_source_and_owner_details() {
        let mut control = control();
        control
            .claim("secret-node-id", Uuid::new_v4(), 2_000_000_000, 300_000_000_000)
            .expect("attempt should claim");
        let control_id = control.identity.source_operation_digest().expect("control id should derive");
        let view = IlmRecoveryControlView::from_control(control_id, &control).expect("view should build");
        let encoded = serde_json::to_string(&view).expect("view should encode");

        for secret in [
            SOURCE_PATH,
            "etag-a",
            "etag-b",
            "secret-node-id",
            "12345678-90ab-cdef-1234-567890abcdef",
        ] {
            assert!(!encoded.contains(secret), "redacted view leaked {secret}");
        }
        assert!(encoded.contains(ILM_RECOVERY_CONTROL_SCHEMA));
        assert!(encoded.contains("source_generation_sha256"));
    }
}
