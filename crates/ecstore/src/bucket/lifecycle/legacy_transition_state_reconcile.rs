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

//! Exact, generation-conditional reconciliation of legacy transition metadata.
//! Repairs are monotonic and never use metadata rollback or remote deletion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::bucket::utils::check_bucket_and_object_names;
use crate::disk::{DiskAPI as _, TransitionStateReconcileCondition, UpdateMetadataOpts};
use crate::object_api::ObjectOptions;
use crate::services::notification_sys::{
    LegacyTransitionStateReconcileFleetProofToken, acquire_cross_pool_fence_fleet_proof,
    acquire_legacy_transition_state_reconcile_fleet_proof, acquire_remote_version_state_fleet_proof,
    cross_pool_fence_fleet_proof_matches, cross_pool_fence_topology_generation,
    legacy_transition_state_reconcile_fleet_proof_current, legacy_transition_state_reconcile_fleet_proof_matches,
    remote_version_state_fleet_proof_matches,
};
use crate::services::tier::tier::{TierConfigMgr, TierOperationLease, tier_destination_id_from_metadata};
use crate::services::tier::warm_backend::LegacyTransitionStateProbe;
use crate::set_disk::read_legacy_transition_state_metadata_copies;
use crate::store::ECStore;

use rustfs_filemeta::{FileInfo, FileMeta, TRANSITION_COMPLETE, TransitionVersionState};
use rustfs_utils::http::metadata_compat::{
    SUFFIX_TRANSITION_TIER_DESTINATION_ID, SUFFIX_TRANSITIONED_VERSION_ID, SUFFIX_TRANSITIONED_VERSION_STATE,
    strip_internal_prefix_preserving_case,
};

const LIVE_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateReconcileSelector {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
}

impl LegacyTransitionStateReconcileSelector {
    fn canonicalize(mut self) -> Result<(Self, Option<Uuid>), LegacyTransitionStateReconcileError> {
        check_bucket_and_object_names(&self.bucket, &self.object)
            .map_err(|err| LegacyTransitionStateReconcileError::InvalidSelector(err.to_string()))?;
        if self.version_id.is_empty() {
            return Err(LegacyTransitionStateReconcileError::InvalidSelector(
                "versionId is required; use the literal null for an unversioned object".to_string(),
            ));
        }
        if self.version_id == "null" {
            return Ok((self, None));
        }
        let version_id = Uuid::parse_str(&self.version_id).map_err(|_| {
            LegacyTransitionStateReconcileError::InvalidSelector(
                "versionId must be a non-nil UUID or the literal null".to_string(),
            )
        })?;
        if version_id.is_nil() {
            return Err(LegacyTransitionStateReconcileError::InvalidSelector(
                "a nil UUID is not a valid selector; use the literal null".to_string(),
            ));
        }
        self.version_id = version_id.to_string();
        Ok((self, Some(version_id)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateMetadataAlias {
    pub key: String,
    /// Lowercase hexadecimal preserves empty and non-UTF-8 values exactly.
    pub value_hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateSource {
    pub bucket_incarnation: String,
    /// Absent when the fleet has not supplied a current topology proof.
    pub topology_generation: Option<String>,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub data_dir: String,
    pub modification_time_unix_nanos: i128,
    pub size: i64,
    pub etag: String,
    pub transition_status: String,
    pub tier: String,
    pub remote_object: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateCopyRepresentation {
    pub disk_index: usize,
    pub metadata_digest: String,
    pub unchanged_metadata_digest: String,
    pub state_aliases: Vec<LegacyTransitionStateMetadataAlias>,
    pub version_aliases: Vec<LegacyTransitionStateMetadataAlias>,
    pub destination_aliases: Vec<LegacyTransitionStateMetadataAlias>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateSetRepresentation {
    pub pool_index: usize,
    pub set_index: usize,
    pub total_copies: usize,
    pub available_copies: usize,
    pub copies: Vec<LegacyTransitionStateCopyRepresentation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateTarget {
    pub state: TransitionVersionState,
    pub remote_version: Option<String>,
    pub destination_id: String,
    pub tier_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateReconcileRequest {
    pub confirm: bool,
    pub selector: LegacyTransitionStateReconcileSelector,
    pub source: LegacyTransitionStateSource,
    pub original_sets: Vec<LegacyTransitionStateSetRepresentation>,
    pub target: LegacyTransitionStateTarget,
    pub reconciliation_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LegacyTransitionStateReconcileOutcome {
    ReadyToMigrate,
    Migrated,
    RetainedAmbiguous,
    Corrupt,
    BackendUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateReconcileReadiness {
    pub fleet_ready: bool,
    pub topology_ready: bool,
    pub tier_generation_ready: bool,
    pub metadata_quorum_ready: bool,
    pub post_ready: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTransitionStateReconcileResponse {
    pub outcome: LegacyTransitionStateReconcileOutcome,
    pub reason_code: String,
    pub reason: String,
    pub retryable: bool,
    pub changed: bool,
    /// A failed RPC may have committed even when its response/readback is lost.
    #[serde(default)]
    pub changes_indeterminate: bool,
    pub selector: LegacyTransitionStateReconcileSelector,
    pub source: Option<LegacyTransitionStateSource>,
    pub original_sets: Vec<LegacyTransitionStateSetRepresentation>,
    pub target: Option<LegacyTransitionStateTarget>,
    pub reconciliation_digest: Option<String>,
    pub readiness: LegacyTransitionStateReconcileReadiness,
}

#[derive(Debug, thiserror::Error)]
pub enum LegacyTransitionStateReconcileError {
    #[error("invalid legacy transition state selector: {0}")]
    InvalidSelector(String),
    #[error("invalid legacy transition state reconciliation request: {0}")]
    InvalidRequest(String),
    #[error("legacy transition state reconciliation expected tuple is stale: {0}")]
    StaleExpectedTuple(String),
    #[error("legacy transition state metadata is corrupt: {0}")]
    Corrupt(String),
    #[error("legacy transition state backend is unavailable: {0}")]
    BackendUnavailable(String),
    #[error("legacy transition state write fence is unavailable: {0}")]
    WriteFenceUnavailable(String),
}

struct InspectedCopy {
    file_info: FileInfo,
    representation: LegacyTransitionStateCopyRepresentation,
}

/// Ownership accompanies a local publication into its blocking executor.
/// Remote disks reconstruct fleet/backend ownership from current local state;
/// serialized options never carry an authority supplied by another process.
pub(crate) struct TransitionStateReconcileAuthority {
    // Declaration order releases physical locks, tier, bucket, then fleet.
    objects: Vec<crate::store::ObjectLockDiagGuard>,
    tier: TierOperationLease,
    bucket: Option<rustfs_lock::NamespaceLockGuard>,
    fleet: LegacyTransitionStateReconcileFleetProofToken,
}

impl std::fmt::Debug for TransitionStateReconcileAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransitionStateReconcileAuthority")
            .finish_non_exhaustive()
    }
}

impl TransitionStateReconcileAuthority {
    pub(crate) fn is_current(&self) -> bool {
        legacy_transition_state_reconcile_fleet_proof_current(&self.fleet)
            && self.tier.is_current_generation()
            && self.bucket.as_ref().is_none_or(|bucket| !bucket.is_lock_lost())
            && self.objects.iter().all(|guard| !guard.is_lock_lost())
    }

    pub(crate) async fn for_disk(condition: &TransitionStateReconcileCondition) -> crate::disk::error::Result<Arc<Self>> {
        use crate::disk::error::Error as DiskError;
        if let Some(authority) = &condition.authority {
            if authority.is_current() {
                return Ok(Arc::clone(authority));
            }
            return Err(DiskError::OutdatedXLMeta);
        }
        let fleet = acquire_legacy_transition_state_reconcile_fleet_proof()
            .await
            .ok_or(DiskError::OutdatedXLMeta)?;
        let topology = acquire_cross_pool_fence_fleet_proof().ok_or(DiskError::OutdatedXLMeta)?;
        if cross_pool_fence_topology_generation(&topology) != condition.topology_generation {
            return Err(DiskError::OutdatedXLMeta);
        }
        let tier = TierConfigMgr::acquire_operation_lease(&crate::runtime::sources::global_tier_config_mgr(), &condition.tier)
            .await
            .map_err(|_| DiskError::OutdatedXLMeta)?;
        if rustfs_utils::crypto::hex(tier.backend_identity()) != condition.target.destination_id {
            return Err(DiskError::OutdatedXLMeta);
        }
        Ok(Arc::new(Self {
            fleet,
            tier,
            objects: Vec::new(),
            bucket: None,
        }))
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    rustfs_utils::crypto::hex(Sha256::digest(bytes))
}

fn metadata_aliases(metadata: &HashMap<String, Vec<u8>>, suffix: &str) -> Vec<LegacyTransitionStateMetadataAlias> {
    let mut aliases = metadata
        .iter()
        .filter(|(key, _)| internal_suffix_matches(key, suffix))
        .map(|(key, value)| LegacyTransitionStateMetadataAlias {
            key: key.clone(),
            value_hex: rustfs_utils::crypto::hex(value),
        })
        .collect::<Vec<_>>();
    aliases.sort_by(|left, right| left.key.cmp(&right.key));
    aliases
}

fn internal_suffix_matches(key: &str, suffix: &str) -> bool {
    strip_internal_prefix_preserving_case(key).is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix))
}

fn inspect_copy(
    raw: &[u8],
    disk_index: usize,
    bucket: &str,
    object: &str,
    version_id: Option<Uuid>,
) -> Result<Option<InspectedCopy>, LegacyTransitionStateReconcileError> {
    let metadata = FileMeta::load(raw)
        .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(format!("xl.meta decode failed: {err}")))?;
    let (_, version) = match metadata.find_version(version_id) {
        Ok(version) => version,
        Err(rustfs_filemeta::Error::FileVersionNotFound) => return Ok(None),
        Err(err) => return Err(LegacyTransitionStateReconcileError::Corrupt(err.to_string())),
    };
    let object_metadata = version.object.as_ref().ok_or_else(|| {
        LegacyTransitionStateReconcileError::Corrupt("the selected local version is not an object version".to_string())
    })?;
    validate_remote_version_bytes(&object_metadata.meta_sys)?;
    let file_info = version
        .into_fileinfo(bucket, object, true)
        .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?;
    file_info
        .validate_for_metadata_read()
        .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?;
    Ok(Some(InspectedCopy {
        file_info,
        representation: LegacyTransitionStateCopyRepresentation {
            disk_index,
            metadata_digest: digest_hex(raw),
            unchanged_metadata_digest: digest_hex(
                &metadata
                    .transition_reconcile_generation(version_id)
                    .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?,
            ),
            state_aliases: metadata_aliases(&object_metadata.meta_sys, SUFFIX_TRANSITIONED_VERSION_STATE),
            version_aliases: metadata_aliases(&object_metadata.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID),
            destination_aliases: metadata_aliases(&object_metadata.meta_sys, SUFFIX_TRANSITION_TIER_DESTINATION_ID),
        },
    }))
}

fn validate_remote_version_bytes(metadata: &HashMap<String, Vec<u8>>) -> Result<(), LegacyTransitionStateReconcileError> {
    for (key, value) in metadata
        .iter()
        .filter(|(key, _)| internal_suffix_matches(key, SUFFIX_TRANSITIONED_VERSION_ID))
    {
        if value.is_empty() {
            continue;
        }
        if let Ok(version_id) = Uuid::from_slice(value) {
            if !version_id.is_nil() {
                continue;
            }
            return Err(LegacyTransitionStateReconcileError::Corrupt(format!(
                "legacy remote version alias {key} contains a nil UUID"
            )));
        }
        let value = std::str::from_utf8(value).map_err(|_| {
            LegacyTransitionStateReconcileError::Corrupt(format!(
                "legacy remote version alias {key} is not valid UTF-8 or a raw UUID"
            ))
        })?;
        if value.len() > 1024
            || value.chars().any(char::is_control)
            || Uuid::parse_str(value).is_ok_and(|version_id| version_id.is_nil())
        {
            return Err(LegacyTransitionStateReconcileError::Corrupt(format!(
                "legacy remote version alias {key} contains an invalid identifier"
            )));
        }
    }
    Ok(())
}

fn source_from_file_info(
    selector: &LegacyTransitionStateReconcileSelector,
    bucket_incarnation: Uuid,
    file_info: &FileInfo,
) -> Result<LegacyTransitionStateSource, LegacyTransitionStateReconcileError> {
    let data_dir = file_info.data_dir.ok_or_else(|| {
        LegacyTransitionStateReconcileError::Corrupt("the selected transition source is missing its data directory".to_string())
    })?;
    let modification_time = file_info.mod_time.ok_or_else(|| {
        LegacyTransitionStateReconcileError::Corrupt(
            "the selected transition source is missing its modification time".to_string(),
        )
    })?;
    let etag = crate::object_api::object_api_utils::get_raw_etag(&file_info.metadata);
    if etag.is_empty() {
        return Err(LegacyTransitionStateReconcileError::Corrupt(
            "the selected transition source is missing its ETag".to_string(),
        ));
    }
    Ok(LegacyTransitionStateSource {
        bucket_incarnation: bucket_incarnation.to_string(),
        topology_generation: None,
        bucket: selector.bucket.clone(),
        object: selector.object.clone(),
        version_id: selector.version_id.clone(),
        data_dir: data_dir.to_string(),
        modification_time_unix_nanos: modification_time.unix_timestamp_nanos(),
        size: file_info.size,
        etag,
        transition_status: file_info.transition_status.clone(),
        tier: file_info.transition_tier.clone(),
        remote_object: file_info.transitioned_objname.clone(),
    })
}

fn response_digest(
    source: &LegacyTransitionStateSource,
    sets: &[LegacyTransitionStateSetRepresentation],
    target: &LegacyTransitionStateTarget,
) -> Result<String, LegacyTransitionStateReconcileError> {
    let encoded = serde_json::to_vec(&(source, sets, target))
        .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?;
    Ok(digest_hex(&encoded))
}

impl ECStore {
    pub async fn inspect_legacy_transition_state(
        &self,
        selector: LegacyTransitionStateReconcileSelector,
    ) -> Result<LegacyTransitionStateReconcileResponse, LegacyTransitionStateReconcileError> {
        let (canonical_selector, _) = selector.clone().canonicalize()?;
        // Keep the multi-pool inspection off the request stack and erase its
        // concrete future so admin callers do not repeat its Send proof.
        let inspection: futures::future::BoxFuture<
            '_,
            Result<LegacyTransitionStateReconcileResponse, LegacyTransitionStateReconcileError>,
        > = Box::pin(self.inspect_legacy_transition_state_inner(canonical_selector.clone(), false, None));
        match inspection.await {
            Ok(response) => Ok(response),
            Err(err) => Ok(error_response(canonical_selector, err)),
        }
    }

    async fn inspect_legacy_transition_state_inner(
        &self,
        selector: LegacyTransitionStateReconcileSelector,
        write_locked: bool,
        bound_lease: Option<&TierOperationLease>,
    ) -> Result<LegacyTransitionStateReconcileResponse, LegacyTransitionStateReconcileError> {
        let (selector, local_version_id) = selector.canonicalize()?;
        let remote_fleet_proof = acquire_remote_version_state_fleet_proof();
        let topology_proof = acquire_cross_pool_fence_fleet_proof();
        // Snapshot lock order: bucket lifecycle READ, then the fixed and
        // physical object READ domains in pool/set order. Release these locks
        // before acquiring the tier lease or waiting on remote I/O.
        let bucket_guard = if write_locked {
            None
        } else {
            Some(
                self.acquire_bucket_lifecycle_read_lock(&selector.bucket)
                    .await
                    .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?,
            )
        };
        let bucket_incarnation = self
            .bucket_incarnation_id_from_disk(&selector.bucket)
            .await
            .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?;
        let encoded_object = rustfs_utils::path::encode_dir_object(&selector.object);
        let mut lock_options = ObjectOptions::default();
        let object_guards = if write_locked {
            Vec::new()
        } else {
            self.acquire_all_physical_object_read_locks(
                "legacy_transition_state_reconcile_inspect",
                &selector.bucket,
                &encoded_object,
                &mut lock_options,
            )
            .await
            .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?
        };
        let fleet_ready = acquire_legacy_transition_state_reconcile_fleet_proof().await.is_some();
        let mut topology_ready = topology_proof.is_some();
        let mut source = None;
        let mut original_sets = Vec::new();
        let mut canonical_file_info: Option<FileInfo> = None;
        let mut observed_file_infos = Vec::new();
        let mut observed_state_alias_presence = Vec::new();

        for set in self.all_set_disks() {
            let raw_copies = read_legacy_transition_state_metadata_copies(&set, &selector.bucket, &selector.object)
                .await
                .map_err(|err| match err {
                    crate::disk::error::Error::FileCorrupt => {
                        LegacyTransitionStateReconcileError::Corrupt("an xl.meta copy is corrupt".to_string())
                    }
                    _ => LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()),
                })?;
            let total_copies = raw_copies.len();
            let available_copies = raw_copies.iter().filter(|copy| copy.is_some()).count();
            let mut representations = Vec::new();
            let mut set_file_infos = Vec::new();
            for (disk_index, raw) in raw_copies.into_iter().enumerate() {
                let Some(raw) = raw else { continue };
                let Some(inspected) = inspect_copy(&raw, disk_index, &selector.bucket, &selector.object, local_version_id)?
                else {
                    continue;
                };
                if let Some(expected) = &canonical_file_info {
                    if !matches_immutable_transition_source(expected, &inspected.file_info) {
                        return Err(LegacyTransitionStateReconcileError::Corrupt(
                            "authoritative copies disagree on the immutable transition source tuple".to_string(),
                        ));
                    }
                } else {
                    source = Some(source_from_file_info(&selector, bucket_incarnation, &inspected.file_info)?);
                    canonical_file_info = Some(inspected.file_info.clone());
                }
                observed_state_alias_presence.push(!inspected.representation.state_aliases.is_empty());
                observed_file_infos.push(inspected.file_info.clone());
                set_file_infos.push(inspected.file_info.clone());
                representations.push(inspected.representation);
            }
            if !representations.is_empty() {
                if representations.len() != available_copies {
                    return Err(LegacyTransitionStateReconcileError::BackendUnavailable(
                        "the selected version is missing from an existing metadata copy".to_string(),
                    ));
                }
                let required = required_reconcile_copy_quorum(&set_file_infos, set.default_write_quorum());
                if representations.len() < required {
                    return Err(LegacyTransitionStateReconcileError::BackendUnavailable(format!(
                        "pool {} set {} has only {} selected-version copies; {required} are required",
                        set.pool_index,
                        set.set_index,
                        representations.len()
                    )));
                }
                original_sets.push(LegacyTransitionStateSetRepresentation {
                    pool_index: set.pool_index,
                    set_index: set.set_index,
                    total_copies,
                    available_copies,
                    copies: representations,
                });
            }
        }
        if object_guards.iter().any(|guard| guard.is_lock_lost())
            || bucket_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
        {
            return Err(LegacyTransitionStateReconcileError::BackendUnavailable(
                "a local metadata snapshot lock was lost before inspection completed".to_string(),
            ));
        }
        drop(object_guards);
        drop(bucket_guard);

        let Some(mut source) = source else {
            return Err(LegacyTransitionStateReconcileError::Corrupt(
                "the selected local object version was not found in any pool or set".to_string(),
            ));
        };
        source.topology_generation = topology_proof.as_ref().map(cross_pool_fence_topology_generation);
        let file_info = canonical_file_info.ok_or_else(|| {
            LegacyTransitionStateReconcileError::Corrupt(
                "the selected source tuple disappeared while its metadata was being inspected".to_string(),
            )
        })?;
        if file_info.transition_status != TRANSITION_COMPLETE
            || file_info.transition_tier.is_empty()
            || file_info.transitioned_objname.is_empty()
        {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::Corrupt,
                reason_code: "partial_transition_tuple".to_string(),
                reason: "the selected object does not contain a complete transition source tuple".to_string(),
                retryable: false,
                changed: false,
                changes_indeterminate: false,
                selector,
                source: Some(source),
                original_sets,
                target: None,
                reconciliation_digest: None,
                readiness: readiness(fleet_ready, topology_ready, false, true, false),
            });
        }
        let has_explicit_unknown =
            observed_file_infos
                .iter()
                .zip(&observed_state_alias_presence)
                .any(|(observed, state_present)| {
                    observed.transition_version_state == TransitionVersionState::Unknown && *state_present
                });
        if has_explicit_unknown {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::Corrupt,
                reason_code: "explicit_unknown_state".to_string(),
                reason: "an explicit unknown transition state is not legacy absence".to_string(),
                retryable: false,
                changed: false,
                changes_indeterminate: false,
                selector,
                source: Some(source),
                original_sets,
                target: None,
                reconciliation_digest: None,
                readiness: readiness(fleet_ready, topology_ready, false, true, false),
            });
        }

        let mut persisted_destination = None;
        let mut persisted_remote_version: Option<String> = None;
        for observed in &observed_file_infos {
            let destination = tier_destination_id_from_metadata(&observed.metadata)
                .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?;
            if let Some(destination) = destination {
                if persisted_destination.is_some_and(|expected| expected != destination) {
                    return Err(LegacyTransitionStateReconcileError::Corrupt(
                        "authoritative copies contain conflicting tier destination identities".to_string(),
                    ));
                }
                persisted_destination = Some(destination);
            }
            // A converged or partially migrated exact tuple still identifies
            // its historical version, even when other remote versions exist.
            if let Some(version) = observed.transition_version.as_deref() {
                if persisted_remote_version
                    .as_deref()
                    .is_some_and(|expected| expected != version)
                {
                    return Err(LegacyTransitionStateReconcileError::Corrupt(
                        "authoritative copies contain conflicting nonempty remote versions".to_string(),
                    ));
                }
                persisted_remote_version = Some(version.to_string());
            }
        }
        let tier_manager = self.tier_config_mgr();
        let owned_lease = if bound_lease.is_none() {
            Some(
                match persisted_destination {
                    Some(destination) => {
                        TierConfigMgr::acquire_operation_lease_for_backend_identity(
                            &tier_manager,
                            &file_info.transition_tier,
                            destination,
                        )
                        .await
                    }
                    None => TierConfigMgr::acquire_operation_lease(&tier_manager, &file_info.transition_tier).await,
                }
                .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?,
            )
        } else {
            None
        };
        let lease = bound_lease.or(owned_lease.as_ref()).ok_or_else(|| {
            LegacyTransitionStateReconcileError::BackendUnavailable("tier generation lease is unavailable".to_string())
        })?;
        if persisted_destination.is_some_and(|identity| identity != lease.backend_identity()) {
            return Err(LegacyTransitionStateReconcileError::Corrupt(
                "persisted tier destination differs from the leased backend".to_string(),
            ));
        }
        let tier_generation = lease.generation();
        let destination_id = rustfs_utils::crypto::hex(lease.backend_identity());
        let probe = tokio::time::timeout(
            LIVE_PROBE_TIMEOUT,
            lease.probe_legacy_transition_state(&file_info.transitioned_objname, persisted_remote_version.as_deref()),
        )
        .await
        .map_err(|_| LegacyTransitionStateReconcileError::BackendUnavailable("live tier probe timed out".to_string()))?
        .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?;
        let remote_state_fleet_current = remote_fleet_proof
            .as_ref()
            .is_some_and(remote_version_state_fleet_proof_matches);
        topology_ready = topology_proof.as_ref().is_some_and(cross_pool_fence_fleet_proof_matches) && remote_state_fleet_current;
        if !lease.is_current_generation()
            || self
                .bucket_incarnation_id_from_disk(&selector.bucket)
                .await
                .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?
                != bucket_incarnation
        {
            return Err(LegacyTransitionStateReconcileError::BackendUnavailable(
                "the tier generation or bucket incarnation changed during the live probe".to_string(),
            ));
        }
        let target = match probe {
            LegacyTransitionStateProbe::UnversionedPresent => Some(LegacyTransitionStateTarget {
                state: TransitionVersionState::KnownDisabled,
                remote_version: None,
                destination_id,
                tier_generation,
            }),
            LegacyTransitionStateProbe::SuspendedNullPresent => Some(LegacyTransitionStateTarget {
                state: TransitionVersionState::SuspendedNull,
                remote_version: Some("null".to_string()),
                destination_id,
                tier_generation,
            }),
            LegacyTransitionStateProbe::VersionedPresent(version) => {
                if version.is_empty() || version == "null" || Uuid::parse_str(&version).is_ok_and(|id| id.is_nil()) {
                    return Err(LegacyTransitionStateReconcileError::BackendUnavailable(
                        "the live tier probe returned an invalid exact version identifier".to_string(),
                    ));
                }
                rustfs_s3_client::provider_versions::validate_remote_version_id(&version)
                    .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?;
                lease
                    .validate_remote_version_id(&version)
                    .map_err(|err| LegacyTransitionStateReconcileError::Corrupt(err.to_string()))?;
                Some(LegacyTransitionStateTarget {
                    state: TransitionVersionState::Exact,
                    remote_version: Some(version),
                    destination_id,
                    tier_generation,
                })
            }
            LegacyTransitionStateProbe::Missing
            | LegacyTransitionStateProbe::Ambiguous
            | LegacyTransitionStateProbe::Unsupported => None,
        };
        let Some(target) = target else {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::RetainedAmbiguous,
                reason_code: "live_probe_ambiguous".to_string(),
                reason: "the live backend probe did not prove exactly one remote version model".to_string(),
                retryable: true,
                changed: false,
                changes_indeterminate: false,
                selector,
                source: Some(source),
                original_sets,
                target: None,
                reconciliation_digest: None,
                readiness: readiness(fleet_ready, topology_ready, true, true, false),
            });
        };

        let already_explicit = file_info.transition_version_state == target.state
            && observed_file_infos.iter().all(|observed| {
                observed.transition_version_state == target.state
                    && observed.transition_version == target.remote_version
                    && tier_destination_id_from_metadata(&observed.metadata).ok().flatten() == Some(lease.backend_identity())
            });
        let legacy_remote_versions_match = observed_file_infos
            .iter()
            .zip(&observed_state_alias_presence)
            .filter(|(observed, state_present)| {
                observed.transition_version_state == TransitionVersionState::Unknown && !**state_present
            })
            .all(|(observed, _)| legacy_remote_version_matches_target(observed, &target));
        if !legacy_remote_versions_match
            || persisted_remote_version
                .as_deref()
                .is_some_and(|persisted| target.remote_version.as_deref() != Some(persisted))
        {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::RetainedAmbiguous,
                reason_code: "legacy_remote_version_changed".to_string(),
                reason: "the live backend candidate does not match the persisted nonempty legacy remote version".to_string(),
                retryable: true,
                changed: false,
                changes_indeterminate: false,
                selector,
                source: Some(source),
                original_sets,
                target: None,
                reconciliation_digest: None,
                readiness: readiness(fleet_ready, topology_ready, true, true, false),
            });
        }
        let allowed_retry_subset =
            observed_file_infos
                .iter()
                .zip(&observed_state_alias_presence)
                .all(|(observed, state_present)| {
                    let destination = tier_destination_id_from_metadata(&observed.metadata).ok();
                    let explicit_target = observed.transition_version_state == target.state
                        && observed.transition_version == target.remote_version
                        && matches!(destination, Some(Some(value)) if value == lease.backend_identity());
                    let missing_destination_matches = match destination {
                        Some(None) => true,
                        Some(Some(value)) => value == lease.backend_identity(),
                        None => false,
                    };
                    let missing_state = observed.transition_version_state == TransitionVersionState::Unknown
                        && !*state_present
                        && missing_destination_matches;
                    explicit_target || missing_state
                });
        if !allowed_retry_subset {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::Corrupt,
                reason_code: "conflicting_retry_subset".to_string(),
                reason: "authoritative copies are outside the allowed original-missing plus exact-target retry subset"
                    .to_string(),
                retryable: false,
                changed: false,
                changes_indeterminate: false,
                selector,
                source: Some(source),
                original_sets,
                target: Some(target),
                reconciliation_digest: None,
                readiness: readiness(fleet_ready, topology_ready, true, true, false),
            });
        }
        let digest = response_digest(&source, &original_sets, &target)?;
        let post_ready = !already_explicit && fleet_ready && topology_ready;
        Ok(LegacyTransitionStateReconcileResponse {
            outcome: if already_explicit {
                LegacyTransitionStateReconcileOutcome::Migrated
            } else {
                LegacyTransitionStateReconcileOutcome::ReadyToMigrate
            },
            reason_code: if already_explicit {
                "already_converged".to_string()
            } else if post_ready {
                "ready_to_migrate".to_string()
            } else {
                "fleet_write_capability_unavailable".to_string()
            },
            reason: if already_explicit {
                "all inspected copies already contain the proven explicit transition state".to_string()
            } else if post_ready {
                "the live backend probe proved a single target tuple".to_string()
            } else {
                "the target tuple is proven, but the required fleet write capability is not available".to_string()
            },
            retryable: !already_explicit,
            changed: false,
            changes_indeterminate: false,
            selector,
            source: Some(source),
            original_sets,
            target: Some(target),
            reconciliation_digest: Some(digest),
            readiness: readiness(fleet_ready, topology_ready, true, true, post_ready),
        })
    }

    pub async fn reconcile_legacy_transition_state(
        &self,
        request: LegacyTransitionStateReconcileRequest,
    ) -> Result<LegacyTransitionStateReconcileResponse, LegacyTransitionStateReconcileError> {
        if !request.confirm {
            return Err(LegacyTransitionStateReconcileError::InvalidRequest("confirm must be true".to_string()));
        }
        let (selector, _) = request.selector.clone().canonicalize()?;
        if selector != request.selector {
            return Err(LegacyTransitionStateReconcileError::InvalidRequest(
                "selector UUID must use its canonical representation".to_string(),
            ));
        }
        let expected_digest = response_digest(&request.source, &request.original_sets, &request.target)?;
        if expected_digest != request.reconciliation_digest {
            return Ok(LegacyTransitionStateReconcileResponse {
                outcome: LegacyTransitionStateReconcileOutcome::Corrupt,
                reason_code: "stale_expected_tuple".to_string(),
                reason: "reconciliation digest does not match the supplied source, sets, and target".to_string(),
                retryable: false,
                changed: false,
                changes_indeterminate: false,
                selector: request.selector,
                source: Some(request.source),
                original_sets: request.original_sets,
                target: Some(request.target),
                reconciliation_digest: Some(request.reconciliation_digest),
                readiness: unavailable_write_readiness(),
            });
        }
        let mut changed = false;
        let mut indeterminate = false;
        let result = Box::pin(self.apply_legacy_transition_state(&request, &mut changed, &mut indeterminate)).await;
        let mut response = match result {
            Ok(response) => response,
            Err(err) => error_response(request.selector.clone(), err),
        };
        response.changed = changed;
        response.changes_indeterminate = indeterminate;
        Ok(response)
    }

    async fn apply_legacy_transition_state(
        &self,
        request: &LegacyTransitionStateReconcileRequest,
        changed: &mut bool,
        indeterminate: &mut bool,
    ) -> Result<LegacyTransitionStateReconcileResponse, LegacyTransitionStateReconcileError> {
        let unavailable = |message: &str| LegacyTransitionStateReconcileError::WriteFenceUnavailable(message.to_string());
        let fleet = acquire_legacy_transition_state_reconcile_fleet_proof()
            .await
            .ok_or_else(|| unavailable("every metadata writer must support conditional transition reconciliation"))?;
        // Admission -> bucket lifecycle WRITE -> exact tier generation ->
        // all physical object WRITE domains -> disk metadata mutation domain.
        let bucket = self
            .acquire_bucket_lifecycle_write_lock(&request.selector.bucket)
            .await
            .map_err(|err| unavailable(&err.to_string()))?;
        let tier = TierConfigMgr::acquire_operation_lease(&self.tier_config_mgr(), &request.source.tier)
            .await
            .map_err(|err| unavailable(&err.to_string()))?;
        if tier.generation() != request.target.tier_generation
            || rustfs_utils::crypto::hex(tier.backend_identity()) != request.target.destination_id
        {
            return Err(LegacyTransitionStateReconcileError::StaleExpectedTuple(
                "tier generation or destination changed".to_string(),
            ));
        }
        let object = rustfs_utils::path::encode_dir_object(&request.selector.object);
        let objects = self
            .acquire_all_physical_object_write_locks("legacy_transition_state_reconcile", &request.selector.bucket, &object)
            .await
            .map_err(|err| unavailable(&err.to_string()))?;
        let authority = Arc::new(TransitionStateReconcileAuthority {
            fleet,
            tier,
            objects,
            bucket: Some(bucket),
        });
        let mut current = self
            .inspect_legacy_transition_state_inner(request.selector.clone(), true, Some(&authority.tier))
            .await?;
        if !matches!(
            current.outcome,
            LegacyTransitionStateReconcileOutcome::ReadyToMigrate | LegacyTransitionStateReconcileOutcome::Migrated
        ) {
            return Ok(current);
        }
        validate_reconcile_snapshot(request, current.source.as_ref(), &current.original_sets, current.target.as_ref())?;
        if !current.readiness.fleet_ready || !current.readiness.topology_ready {
            return Err(unavailable("the current fleet/topology snapshot cannot authorize metadata writes"));
        }
        let topology = request
            .source
            .topology_generation
            .as_ref()
            .ok_or_else(|| unavailable("topology proof is absent"))?;
        let (_, version_id) = request.selector.clone().canonicalize()?;
        // Visit every observed copy, including an already committed retry
        // subset. The second pass is a zero-write barrier in the same disk
        // mutation domain; a delayed first attempt can only be idempotent.
        for verify_only in [false, true] {
            for set in self.all_set_disks() {
                let Some(original_set) = request
                    .original_sets
                    .iter()
                    .find(|item| item.pool_index == set.pool_index && item.set_index == set.set_index)
                else {
                    continue;
                };
                let disks = set.disk_inventory().await;
                for original in &original_set.copies {
                    if !legacy_transition_state_reconcile_fleet_proof_matches(&authority.fleet).await
                        || self
                            .bucket_incarnation_id_from_disk(&request.selector.bucket)
                            .await
                            .map_err(|err| unavailable(&err.to_string()))?
                            .to_string()
                            != request.source.bucket_incarnation
                        || !authority.is_current()
                    {
                        return Err(unavailable("fleet, tier, bucket, or object fence changed before metadata publication"));
                    }
                    let disk = disks
                        .get(original.disk_index)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| unavailable("an authoritative disk is unavailable"))?;
                    let opts = UpdateMetadataOpts {
                        transition_reconcile: Some(Box::new(TransitionStateReconcileCondition {
                            expected_metadata_digest: original.metadata_digest.clone(),
                            unchanged_metadata_digest: original.unchanged_metadata_digest.clone(),
                            target: rustfs_filemeta::TransitionStateReconcileTarget {
                                state: request.target.state,
                                remote_version: request.target.remote_version.clone(),
                                destination_id: request.target.destination_id.clone(),
                            },
                            tier: request.source.tier.clone(),
                            topology_generation: topology.clone(),
                            verify_only,
                            authority: Some(Arc::clone(&authority)),
                        })),
                        ..Default::default()
                    };
                    let already_target = current
                        .original_sets
                        .iter()
                        .find(|item| item.pool_index == set.pool_index && item.set_index == set.set_index)
                        .and_then(|item| item.copies.iter().find(|copy| copy.disk_index == original.disk_index))
                        .is_some_and(|copy| representation_matches_target(copy, &request.target));
                    // Empty metadata makes a server which ignores the new
                    // conditional option reject the legacy update operation.
                    let result = disk
                        .update_metadata(
                            &request.selector.bucket,
                            &object,
                            FileInfo {
                                version_id,
                                ..Default::default()
                            },
                            &opts,
                        )
                        .await;
                    set.invalidate_get_object_metadata_cache(&request.selector.bucket, &object)
                        .await;
                    match result {
                        Ok(()) => {
                            *changed |= !verify_only && !already_target;
                        }
                        Err(err) => {
                            *indeterminate |= !verify_only && !already_target;
                            return Err(match err {
                                crate::disk::error::Error::OutdatedXLMeta => {
                                    unavailable("a disk generation, rollback, or publication fence changed; inspect or retry")
                                }
                                crate::disk::error::Error::FileCorrupt => LegacyTransitionStateReconcileError::Corrupt(
                                    "a disk rejected conflicting transition metadata".to_string(),
                                ),
                                _ => LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()),
                            });
                        }
                    }
                }
            }
        }
        let (source, copies) = self.read_reconciled_transition_snapshot(&request.selector).await?;
        validate_reconcile_snapshot(request, Some(&source), &copies, Some(&request.target))?;
        if copies
            .iter()
            .flat_map(|set| &set.copies)
            .any(|copy| !representation_matches_target(copy, &request.target))
        {
            return Err(unavailable("strong readback did not prove convergence of every authoritative copy"));
        }
        if !legacy_transition_state_reconcile_fleet_proof_matches(&authority.fleet).await || !authority.is_current() {
            return Err(unavailable("publication authority changed before final readback completed"));
        }
        current.outcome = LegacyTransitionStateReconcileOutcome::Migrated;
        current.reason_code = if *changed { "migrated" } else { "already_converged" }.to_string();
        current.reason = "every authoritative metadata copy contains the proven state and destination".to_string();
        current.retryable = false;
        current.reconciliation_digest = Some(response_digest(&source, &copies, &request.target)?);
        current.source = Some(source);
        current.original_sets = copies;
        current.readiness = readiness(true, true, true, true, false);
        Ok(current)
    }

    async fn read_reconciled_transition_snapshot(
        &self,
        selector: &LegacyTransitionStateReconcileSelector,
    ) -> Result<(LegacyTransitionStateSource, Vec<LegacyTransitionStateSetRepresentation>), LegacyTransitionStateReconcileError>
    {
        let (_, version_id) = selector.clone().canonicalize()?;
        let incarnation = self
            .bucket_incarnation_id_from_disk(&selector.bucket)
            .await
            .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?;
        let topology = acquire_cross_pool_fence_fleet_proof()
            .ok_or_else(|| LegacyTransitionStateReconcileError::WriteFenceUnavailable("topology proof expired".to_string()))?;
        let mut source = None;
        let mut sets = Vec::new();
        for set in self.all_set_disks() {
            let raw = read_legacy_transition_state_metadata_copies(&set, &selector.bucket, &selector.object)
                .await
                .map_err(|err| LegacyTransitionStateReconcileError::BackendUnavailable(err.to_string()))?;
            let total_copies = raw.len();
            let available_copies = raw.iter().flatten().count();
            let mut copies = Vec::new();
            for (index, raw) in raw.into_iter().enumerate() {
                let Some(raw) = raw else { continue };
                let Some(copy) = inspect_copy(&raw, index, &selector.bucket, &selector.object, version_id)? else {
                    continue;
                };
                let mut observed_source = source_from_file_info(selector, incarnation, &copy.file_info)?;
                observed_source.topology_generation = Some(cross_pool_fence_topology_generation(&topology));
                if source.as_ref().is_some_and(|source| source != &observed_source) {
                    return Err(LegacyTransitionStateReconcileError::StaleExpectedTuple(
                        "immutable source changed during readback".to_string(),
                    ));
                }
                source = Some(observed_source);
                copies.push(copy.representation);
            }
            if !copies.is_empty() {
                sets.push(LegacyTransitionStateSetRepresentation {
                    pool_index: set.pool_index,
                    set_index: set.set_index,
                    total_copies,
                    available_copies,
                    copies,
                });
            }
        }
        Ok((
            source.ok_or_else(|| LegacyTransitionStateReconcileError::StaleExpectedTuple("source disappeared".to_string()))?,
            sets,
        ))
    }
}

fn representation_matches_target(copy: &LegacyTransitionStateCopyRepresentation, target: &LegacyTransitionStateTarget) -> bool {
    let aliases_equal = |aliases: &[LegacyTransitionStateMetadataAlias], value: &str| {
        !aliases.is_empty()
            && aliases
                .iter()
                .all(|alias| alias.value_hex == rustfs_utils::crypto::hex(value.as_bytes()))
    };
    aliases_equal(&copy.state_aliases, target.state.as_str())
        && aliases_equal(&copy.destination_aliases, &target.destination_id)
        && match &target.remote_version {
            Some(version) => aliases_equal(&copy.version_aliases, version),
            None => copy.version_aliases.iter().all(|alias| alias.value_hex.is_empty()),
        }
}

fn validate_reconcile_snapshot(
    request: &LegacyTransitionStateReconcileRequest,
    source: Option<&LegacyTransitionStateSource>,
    sets: &[LegacyTransitionStateSetRepresentation],
    target: Option<&LegacyTransitionStateTarget>,
) -> Result<(), LegacyTransitionStateReconcileError> {
    let matches = source == Some(&request.source)
        && target == Some(&request.target)
        && !sets.is_empty()
        && sets.len() == request.original_sets.len()
        && sets.iter().zip(&request.original_sets).all(|(current, original)| {
            current.pool_index == original.pool_index
                && current.set_index == original.set_index
                && current.total_copies == original.total_copies
                && current.available_copies == original.available_copies
                && current.copies.len() == original.copies.len()
                && current.copies.iter().zip(&original.copies).all(|(current, original)| {
                    current.disk_index == original.disk_index
                        && current.unchanged_metadata_digest == original.unchanged_metadata_digest
                        && (current == original || representation_matches_target(current, &request.target))
                })
        });
    if !matches {
        return Err(LegacyTransitionStateReconcileError::StaleExpectedTuple(
            "source, ownership, target, or unrelated metadata differs from the inspected generation".to_string(),
        ));
    }
    Ok(())
}

fn legacy_remote_version_matches_target(file_info: &FileInfo, target: &LegacyTransitionStateTarget) -> bool {
    match (&file_info.transition_version, &target.remote_version) {
        (None, _) => true,
        (Some(persisted), Some(proven)) => persisted == proven,
        (Some(_), None) => false,
    }
}

fn unavailable_write_readiness() -> LegacyTransitionStateReconcileReadiness {
    LegacyTransitionStateReconcileReadiness {
        fleet_ready: false,
        topology_ready: false,
        tier_generation_ready: false,
        metadata_quorum_ready: false,
        post_ready: false,
    }
}

fn readiness(
    fleet_ready: bool,
    topology_ready: bool,
    tier_generation_ready: bool,
    metadata_quorum_ready: bool,
    post_ready: bool,
) -> LegacyTransitionStateReconcileReadiness {
    LegacyTransitionStateReconcileReadiness {
        fleet_ready,
        topology_ready,
        tier_generation_ready,
        metadata_quorum_ready,
        post_ready,
    }
}

fn error_response(
    selector: LegacyTransitionStateReconcileSelector,
    err: LegacyTransitionStateReconcileError,
) -> LegacyTransitionStateReconcileResponse {
    let (outcome, reason_code, retryable) = match &err {
        LegacyTransitionStateReconcileError::Corrupt(_) | LegacyTransitionStateReconcileError::StaleExpectedTuple(_) => {
            (LegacyTransitionStateReconcileOutcome::Corrupt, "corrupt", false)
        }
        LegacyTransitionStateReconcileError::BackendUnavailable(_) => {
            (LegacyTransitionStateReconcileOutcome::BackendUnavailable, "backend_unavailable", true)
        }
        LegacyTransitionStateReconcileError::WriteFenceUnavailable(_) => {
            (LegacyTransitionStateReconcileOutcome::BackendUnavailable, "write_fence_unavailable", true)
        }
        LegacyTransitionStateReconcileError::InvalidSelector(_) | LegacyTransitionStateReconcileError::InvalidRequest(_) => {
            (LegacyTransitionStateReconcileOutcome::Corrupt, "invalid_request", false)
        }
    };
    LegacyTransitionStateReconcileResponse {
        outcome,
        reason_code: reason_code.to_string(),
        reason: err.to_string(),
        retryable,
        changed: false,
        changes_indeterminate: false,
        selector,
        source: None,
        original_sets: Vec::new(),
        target: None,
        reconciliation_digest: None,
        readiness: unavailable_write_readiness(),
    }
}

fn required_reconcile_copy_quorum(file_infos: &[FileInfo], default_write_quorum: usize) -> usize {
    file_infos
        .iter()
        .map(|info| info.write_quorum(default_write_quorum))
        .max()
        .unwrap_or(default_write_quorum)
        .max(default_write_quorum)
}

fn matches_immutable_transition_source(expected: &FileInfo, observed: &FileInfo) -> bool {
    expected.version_id == observed.version_id
        && expected.data_dir == observed.data_dir
        && expected.mod_time == observed.mod_time
        && expected.size == observed.size
        && crate::object_api::object_api_utils::get_raw_etag(&expected.metadata)
            == crate::object_api::object_api_utils::get_raw_etag(&observed.metadata)
        && expected.transition_status == observed.transition_status
        && expected.transition_tier == observed.transition_tier
        && expected.transitioned_objname == observed.transitioned_objname
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_minio_and_legacy_rustfs_envelopes_are_inspected_without_reencoding() {
        for bytes in [
            rustfs_filemeta::test_data::create_minio_small_object_xlmeta().expect("pinned MinIO inline object"),
            rustfs_filemeta::test_data::create_minio_large_object_xlmeta().expect("pinned MinIO external data object"),
            rustfs_filemeta::test_data::create_minio_versioned_object_xlmeta().expect("pinned MinIO versioned object"),
            rustfs_filemeta::test_data::create_issue_2265_legacy_meta_v2_object_xlmeta().expect("pinned legacy RustFS object"),
        ] {
            let original = bytes.clone();
            let metadata = FileMeta::load(&bytes).expect("load pinned metadata envelope");
            for version in &metadata.versions {
                let inspected = inspect_copy(&bytes, 0, "bucket", "object", version.header.version_id);
                if version.header.version_type == rustfs_filemeta::VersionType::Delete {
                    assert!(
                        matches!(inspected, Err(LegacyTransitionStateReconcileError::Corrupt(_))),
                        "a pinned delete marker is not a transition-state repair candidate"
                    );
                    continue;
                }
                let inspected = inspected.expect("inspect pinned version").expect("pinned version is present");
                assert_eq!(inspected.file_info.transition_version_state, TransitionVersionState::Unknown);
                assert!(inspected.representation.state_aliases.is_empty());
                assert_eq!(inspected.representation.metadata_digest, digest_hex(&original));
            }
            assert_eq!(bytes, original, "foreign metadata must never be round-tripped by inspection");
        }
    }

    fn legacy_metadata_fixture(version_bytes: Option<&[u8]>, state: Option<&[u8]>) -> Vec<u8> {
        let mut metadata = FileMeta::new();
        metadata
            .add_version(FileInfo {
                version_id: Some(Uuid::from_u128(1)),
                data_dir: Some(Uuid::from_u128(2)),
                mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture modification time")),
                size: 7,
                parts: vec![rustfs_filemeta::ObjectPartInfo {
                    number: 1,
                    size: 7,
                    actual_size: 7,
                    ..Default::default()
                }],
                erasure: rustfs_filemeta::ErasureInfo {
                    algorithm: "ReedSolomon".to_string(),
                    data_blocks: 2,
                    parity_blocks: 2,
                    block_size: 1_048_576,
                    index: 1,
                    distribution: vec![1, 2, 3, 4],
                    ..Default::default()
                },
                metadata: HashMap::from([("etag".to_string(), "legacy-etag".to_string())]),
                ..Default::default()
            })
            .expect("encode source object fixture");
        let (index, mut version) = metadata.find_version(Some(Uuid::from_u128(1))).expect("fixture version");
        let object = version.object.as_mut().expect("fixture object metadata");
        for (suffix, value) in [
            ("transition-status", b"complete".as_slice()),
            ("transition-tier", b"WARM".as_slice()),
            ("transitioned-object", b"archive/object".as_slice()),
        ] {
            object.meta_sys.insert(format!("x-minio-internal-{suffix}"), value.to_vec());
        }
        if let Some(value) = version_bytes {
            object
                .meta_sys
                .insert(format!("x-minio-internal-{SUFFIX_TRANSITIONED_VERSION_ID}"), value.to_vec());
        }
        if let Some(value) = state {
            object
                .meta_sys
                .insert(format!("x-minio-internal-{SUFFIX_TRANSITIONED_VERSION_STATE}"), value.to_vec());
        }
        metadata.versions[index].header = version.header();
        metadata.versions[index].meta = version.marshal_msg().expect("encode legacy object version");
        metadata.marshal_msg().expect("encode legacy xl.meta")
    }

    #[test]
    fn raw_minio_and_rustfs_fixtures_preserve_legacy_provenance_without_backfill() {
        for raw_version in [
            None,
            Some(b"".as_slice()),
            Some(b"opaque-version".as_slice()),
            Some(Uuid::from_u128(9).as_bytes().as_slice()),
        ] {
            let bytes = legacy_metadata_fixture(raw_version, None);
            let before = bytes.clone();
            let inspected = inspect_copy(&bytes, 0, "bucket", "object", Some(Uuid::from_u128(1)))
                .expect("legacy metadata should decode")
                .expect("legacy version should exist");
            assert_eq!(inspected.file_info.transition_version_state, TransitionVersionState::Unknown);
            assert!(inspected.representation.state_aliases.is_empty());
            assert_eq!(inspected.representation.version_aliases.len(), usize::from(raw_version.is_some()));
            assert_eq!(inspected.representation.metadata_digest, digest_hex(&before));
            assert_eq!(bytes, before, "inspection must not rewrite legacy bytes");
        }
    }

    #[test]
    fn corrupt_legacy_bytes_cannot_become_an_unversioned_candidate() {
        for raw_version in [
            b"bad\nversion".as_slice(),
            b"\xff\xfe".as_slice(),
            Uuid::nil().as_bytes().as_slice(),
        ] {
            let bytes = legacy_metadata_fixture(Some(raw_version), None);
            assert!(matches!(
                inspect_copy(&bytes, 0, "bucket", "object", Some(Uuid::from_u128(1))),
                Err(LegacyTransitionStateReconcileError::Corrupt(_))
            ));
        }
        let bytes = legacy_metadata_fixture(Some(b""), Some(b"unknown"));
        let inspected = inspect_copy(&bytes, 0, "bucket", "object", Some(Uuid::from_u128(1)))
            .expect("explicit unknown is structurally decodable")
            .expect("fixture version");
        assert_eq!(inspected.file_info.transition_version_state, TransitionVersionState::Unknown);
        assert!(
            !inspected.representation.state_aliases.is_empty(),
            "explicit unknown must remain distinct from legacy absence"
        );
    }

    #[test]
    fn selector_requires_explicit_non_nil_local_version_identity() {
        for version_id in ["", "00000000-0000-0000-0000-000000000000", "not-a-version"] {
            let selector = LegacyTransitionStateReconcileSelector {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: version_id.to_string(),
            };
            assert!(matches!(
                selector.canonicalize(),
                Err(LegacyTransitionStateReconcileError::InvalidSelector(_))
            ));
        }
        assert!(
            LegacyTransitionStateReconcileSelector {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: "null".to_string(),
            }
            .canonicalize()
            .is_ok()
        );
    }

    #[test]
    fn digest_binds_source_sets_and_target() {
        let source = LegacyTransitionStateSource {
            bucket_incarnation: Uuid::from_u128(1).to_string(),
            topology_generation: Some("topology-a".to_string()),
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: "null".to_string(),
            data_dir: Uuid::from_u128(2).to_string(),
            modification_time_unix_nanos: 1,
            size: 1,
            etag: "etag".to_string(),
            transition_status: TRANSITION_COMPLETE.to_string(),
            tier: "WARM".to_string(),
            remote_object: "remote/object".to_string(),
        };
        let sets = vec![LegacyTransitionStateSetRepresentation {
            pool_index: 0,
            set_index: 0,
            total_copies: 1,
            available_copies: 1,
            copies: vec![],
        }];
        let target = LegacyTransitionStateTarget {
            state: TransitionVersionState::KnownDisabled,
            remote_version: None,
            destination_id: "00".repeat(32),
            tier_generation: 1,
        };
        let digest = response_digest(&source, &sets, &target).expect("valid source digest");
        let mut changed = source.clone();
        changed.remote_object.push_str("-changed");
        assert_ne!(digest, response_digest(&changed, &sets, &target).expect("changed source digest"));
    }

    #[test]
    fn nonempty_legacy_remote_version_must_match_live_candidate() {
        let mut file_info = FileInfo {
            transition_version: Some("remote-version-a".to_string()),
            ..Default::default()
        };
        let mut target = LegacyTransitionStateTarget {
            state: TransitionVersionState::Exact,
            remote_version: Some("remote-version-b".to_string()),
            destination_id: "00".repeat(32),
            tier_generation: 1,
        };
        assert!(!legacy_remote_version_matches_target(&file_info, &target));
        target.remote_version = Some("remote-version-a".to_string());
        assert!(legacy_remote_version_matches_target(&file_info, &target));
        file_info.transition_version = None;
        assert!(legacy_remote_version_matches_target(&file_info, &target));
    }

    #[test]
    fn missing_state_accepts_only_empty_utf8_or_non_nil_raw_uuid_version_values() {
        let key = format!("x-minio-internal-{SUFFIX_TRANSITIONED_VERSION_ID}");
        let mut metadata = HashMap::from([(key.clone(), Vec::new())]);
        validate_remote_version_bytes(&metadata).expect("empty MinIO value is preserved legacy provenance");

        metadata.insert(key.clone(), Uuid::from_u128(1).as_bytes().to_vec());
        validate_remote_version_bytes(&metadata).expect("non-nil historical RustFS UUID is valid provenance");

        metadata.insert(key.clone(), Uuid::nil().as_bytes().to_vec());
        assert!(matches!(
            validate_remote_version_bytes(&metadata),
            Err(LegacyTransitionStateReconcileError::Corrupt(_))
        ));

        metadata.insert(key, vec![0xff, 0xfe]);
        assert!(matches!(
            validate_remote_version_bytes(&metadata),
            Err(LegacyTransitionStateReconcileError::Corrupt(_))
        ));
    }

    #[test]
    fn raw_copy_quorum_cannot_be_lowered_by_embedded_erasure_geometry() {
        let undersized = FileInfo {
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 1,
                parity_blocks: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let balanced = FileInfo {
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 4,
                parity_blocks: 4,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(required_reconcile_copy_quorum(&[undersized], 5), 5);
        assert_eq!(required_reconcile_copy_quorum(&[balanced], 4), 5);
        assert_eq!(required_reconcile_copy_quorum(&[], 5), 5);

        let old_pool = FileInfo {
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 8,
                parity_blocks: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let new_pool = FileInfo {
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 2,
                parity_blocks: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(required_reconcile_copy_quorum(&[old_pool], 7), 8);
        assert_eq!(required_reconcile_copy_quorum(&[new_pool], 3), 3);
    }
}
