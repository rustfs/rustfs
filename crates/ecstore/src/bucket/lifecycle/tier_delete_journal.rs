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

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bucket::lifecycle::config_boundary;
use crate::bucket::lifecycle::durable_namespace::{
    TIER_DELETE_JOURNAL_NAMESPACE, TIER_DELETE_JOURNAL_V6_NAMESPACE, validate_durable_ilm_record,
};
use crate::bucket::lifecycle::runtime_boundary;
use crate::bucket::lifecycle::tier_sweeper::{
    Jentry, TierDeleteDispatchBinding, TierDeleteJournalState, TierDeleteSourceIdentity,
    delete_confirmed_transition_candidate_exact_with_lease_idempotent, delete_object_from_remote_tier_with_lease_idempotent,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result, is_err_strict_volume_not_found};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::services::notification_sys::{
    TierDeleteJournalFleetProofToken, acquire_tier_delete_journal_fleet_proof, tier_delete_journal_fleet_proof_matches,
    tier_delete_journal_topology_generation,
};
use crate::services::tier::tier::{TierConfigMgr, tier_destination_id_from_metadata};
use crate::storage_api_contracts::{
    list::ListOperations as _,
    namespace::NamespaceLocking as _,
    object::{DeletedObject, HTTPPreconditions, ObjectIO, ObjectOperations, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::ECStore;
use rustfs_filemeta::FileInfo;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_TIER_DELETE_JOURNAL: &str = "lifecycle_tier_delete_journal";

pub const DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT: usize = 1_000;
const TIER_DELETE_JOURNAL_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const TIER_DELETE_JOURNAL_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);
const TIER_DELETE_REMOTE_DEADLINE: Duration = Duration::from_secs(30);
const TIER_DELETE_JOURNAL_VERSION: u8 = 2;
const TIER_DELETE_JOURNAL_EXACT_VERSION: u8 = 3;
const TIER_DELETE_JOURNAL_STATE_VERSION: u8 = 4;
const TIER_DELETE_JOURNAL_TRANSACTION_VERSION: u8 = 5;
// v6 is the first transaction journal whose Committed state is allowed to be
// the sole remote-cleanup owner. Readers capped at v5 reject this version and
// therefore cannot bypass the all-pool live-source proof during a rolling
// upgrade or downgrade.
// RUSTFS_COMPAT_TODO(backlog-2097-tier-delete-journal-v6): retain v1-v5 readers and the v6 downgrade fence for safe rollback. Remove after every supported rollback release preserves v6 sole-owner journals without remote deletion.
const TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION: u8 = 6;
/// Common list prefix covering both the byte-compatible v1-v5 namespace and
/// the operation-scoped v6 namespace.
pub(crate) const TIER_DELETE_JOURNAL_PREFIX: &str = "ilm/tier-delete-journal";
pub(crate) const TIER_DELETE_JOURNAL_LEGACY_PREFIX: &str = TIER_DELETE_JOURNAL_NAMESPACE.prefix;
pub(crate) const TIER_DELETE_JOURNAL_V6_PREFIX: &str = TIER_DELETE_JOURNAL_V6_NAMESPACE.prefix;
pub(crate) const TIER_DELETE_DISPATCH_MANIFEST_PREFIX: &str = "ilm/tier-delete-dispatch-manifests/";
const TIER_DELETE_DISPATCH_MANIFEST_VERSION: u8 = 1;
pub(crate) const MAX_TIER_DELETE_DISPATCH_MANIFEST_SIZE: usize = 32 * 1024 * 1024;
const MAX_TIER_DELETE_DISPATCH_JOURNALS: usize = 200_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TierDeleteDispatchManifestState {
    Preparing,
    DispatchAuthorized,
    Aborting,
    Aborted,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TierDeleteDispatchManifest {
    version: u8,
    operation_id: uuid::Uuid,
    bucket: String,
    bucket_incarnation: uuid::Uuid,
    prefix: String,
    journal_names: Vec<String>,
    journal_set_sha256: String,
    journal_count: u64,
    topology_generation: String,
    state: TierDeleteDispatchManifestState,
}

impl TierDeleteDispatchManifest {
    fn validate(&self, object_name: &str) -> Result<()> {
        if self.version != TIER_DELETE_DISPATCH_MANIFEST_VERSION
            || self.operation_id.is_nil()
            || self.bucket.is_empty()
            || self.bucket_incarnation.is_nil()
            || self.journal_names.len() > MAX_TIER_DELETE_DISPATCH_JOURNALS
            || self.journal_count != self.journal_names.len() as u64
            || self.topology_generation.len() != 64
            || !self.topology_generation.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::other("tier delete dispatch manifest is invalid"));
        }
        if !self.journal_names.windows(2).all(|pair| pair[0] < pair[1])
            || self.journal_names.iter().any(|name| {
                let expected_prefix = format!("{TIER_DELETE_JOURNAL_V6_PREFIX}{}/", self.operation_id.simple());
                !name.starts_with(&expected_prefix) || !name.ends_with(".json")
            })
            || tier_delete_dispatch_journal_set_digest(&self.journal_names) != self.journal_set_sha256
            || tier_delete_dispatch_manifest_object_name(&self.bucket, self.bucket_incarnation, &self.prefix) != object_name
        {
            return Err(Error::other("tier delete dispatch manifest binding is invalid"));
        }
        Ok(())
    }
}

fn tier_delete_dispatch_journal_set_digest(names: &[String]) -> String {
    let mut hasher = Sha256::new();
    for name in names {
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
    }
    rustfs_utils::crypto::hex(hasher.finalize().as_slice())
}

fn tier_delete_dispatch_manifest_object_name(bucket: &str, incarnation: uuid::Uuid, prefix: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bucket.as_bytes());
    hasher.update([0]);
    hasher.update(incarnation.as_bytes());
    hasher.update([0]);
    hasher.update(prefix.as_bytes());
    format!(
        "{TIER_DELETE_DISPATCH_MANIFEST_PREFIX}{}.json",
        rustfs_utils::crypto::hex(hasher.finalize().as_slice())
    )
}

fn tier_delete_dispatch_operation_lock_name(manifest_object: &str) -> String {
    format!("{manifest_object}.operation-lock")
}

fn ensure_dispatch_lock_current(
    bucket_fence: &crate::object_api::NamespaceLockFence,
    operation_guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<()> {
    if bucket_fence.is_lock_lost() || operation_guard.is_lock_lost() {
        return Err(Error::other("tier delete dispatch namespace fence was lost"));
    }
    Ok(())
}

fn dispatch_write_fences_current(
    bucket_fence: &crate::object_api::NamespaceLockFence,
    operation_guard: &rustfs_lock::NamespaceLockGuard,
    fleet_proof: &TierDeleteJournalFleetProofToken,
    topology_generation: &str,
) -> bool {
    !bucket_fence.is_lock_lost()
        && !operation_guard.is_lock_lost()
        && tier_delete_journal_fleet_proof_matches(fleet_proof)
        && tier_delete_journal_topology_generation(fleet_proof) == topology_generation
}

fn encode_tier_delete_dispatch_manifest(manifest: &TierDeleteDispatchManifest) -> Result<Vec<u8>> {
    let data = serde_json::to_vec(manifest)
        .map_err(|err| Error::other_with_context("encode tier delete dispatch manifest failed", err))?;
    if data.len() > MAX_TIER_DELETE_DISPATCH_MANIFEST_SIZE {
        return Err(Error::other("tier delete dispatch manifest is too large"));
    }
    Ok(data)
}

fn decode_tier_delete_dispatch_manifest(data: &[u8], object_name: &str) -> Result<TierDeleteDispatchManifest> {
    if data.len() > MAX_TIER_DELETE_DISPATCH_MANIFEST_SIZE {
        return Err(Error::other("tier delete dispatch manifest is too large"));
    }
    let manifest: TierDeleteDispatchManifest = serde_json::from_slice(data)
        .map_err(|err| Error::other_with_context("decode tier delete dispatch manifest failed", err))?;
    manifest.validate(object_name)?;
    Ok(manifest)
}

pub(crate) fn validate_tier_delete_dispatch_manifest_record(
    object_name: &str,
    data: &[u8],
) -> Result<(uuid::Uuid, String, TierDeleteDispatchManifestState)> {
    let manifest = decode_tier_delete_dispatch_manifest(data, object_name)?;
    let identity = serde_json::to_vec(&(
        manifest.version,
        manifest.operation_id,
        &manifest.bucket,
        manifest.bucket_incarnation,
        &manifest.prefix,
        &manifest.journal_names,
        &manifest.journal_set_sha256,
        manifest.journal_count,
        &manifest.topology_generation,
    ))
    .map_err(Error::other)?;
    Ok((
        manifest.operation_id,
        rustfs_utils::crypto::hex_sha256(&identity, ToOwned::to_owned),
        manifest.state,
    ))
}

#[cfg(test)]
pub(crate) fn test_tier_delete_dispatch_manifest_record(
    operation_id: uuid::Uuid,
    state: TierDeleteDispatchManifestState,
) -> (String, Vec<u8>) {
    let bucket = "durable-manifest-bucket";
    let bucket_incarnation = uuid::Uuid::from_u128(1);
    let prefix = "archive/";
    let object_name = tier_delete_dispatch_manifest_object_name(bucket, bucket_incarnation, prefix);
    let manifest = TierDeleteDispatchManifest {
        version: TIER_DELETE_DISPATCH_MANIFEST_VERSION,
        operation_id,
        bucket: bucket.to_string(),
        bucket_incarnation,
        prefix: prefix.to_string(),
        journal_names: Vec::new(),
        journal_set_sha256: tier_delete_dispatch_journal_set_digest(&[]),
        journal_count: 0,
        topology_generation: "a".repeat(64),
        state,
    };
    let data = encode_tier_delete_dispatch_manifest(&manifest).expect("test dispatch manifest should encode");
    (object_name, data)
}

struct DispatchedJournalPermit {
    manifest: TierDeleteDispatchManifest,
    authorized_etag: String,
    entries: Vec<Jentry>,
    fleet_proof: TierDeleteJournalFleetProofToken,
}

struct TierDeleteDispatchAuthorizationInner {
    manifest_object: String,
    operation_id: uuid::Uuid,
    bucket: String,
    bucket_incarnation: uuid::Uuid,
    prefix: String,
    journal_set_sha256: String,
    journal_names: BTreeSet<String>,
    topology_generation: String,
    fleet_proof: TierDeleteJournalFleetProofToken,
    mutation_started: AtomicBool,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct TierDeleteDispatchAuthorization(Arc<TierDeleteDispatchAuthorizationInner>);

impl std::fmt::Debug for TierDeleteDispatchAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TierDeleteDispatchAuthorization")
            .field("operation_id", &self.0.operation_id)
            .field("journal_count", &self.0.journal_names.len())
            .field("mutation_started", &self.mutation_started())
            .finish()
    }
}

impl TierDeleteDispatchAuthorization {
    pub(crate) fn ensure_current(&self, bucket: &str, incarnation: uuid::Uuid, prefix: &str) -> Result<()> {
        if self.0.bucket != bucket
            || self.0.bucket_incarnation != incarnation
            || self.0.prefix != prefix
            || !tier_delete_journal_fleet_proof_matches(&self.0.fleet_proof)
            || tier_delete_journal_topology_generation(&self.0.fleet_proof) != self.0.topology_generation
        {
            return Err(Error::other(
                "tier delete dispatch authorization is stale or belongs to another operation",
            ));
        }
        Ok(())
    }

    pub(crate) fn mark_mutation_started(&self, bucket: &str, incarnation: uuid::Uuid, prefix: &str) -> Result<()> {
        self.ensure_current(bucket, incarnation, prefix)?;
        self.0.mutation_started.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn mutation_started(&self) -> bool {
        self.0.mutation_started.load(Ordering::Acquire)
    }

    pub(crate) fn authorized_journal_name(&self, entry: &Jentry) -> Result<String> {
        let source = entry
            .source
            .as_ref()
            .filter(|source| source.has_stable_identity())
            .ok_or_else(|| Error::other("tier delete dispatch candidate has no stable source identity"))?;
        if source.bucket != self.0.bucket || !source.object.starts_with(&self.0.prefix) || !entry.can_replace_tier_free_version()
        {
            return Err(Error::other("tier delete dispatch candidate is outside its authorized source scope"));
        }
        let mut bound = entry.clone();
        bound.persisted_version = 0;
        bound.state = TierDeleteJournalState::Prepared;
        bound.dispatch = Some(TierDeleteDispatchBinding {
            operation_id: self.0.operation_id,
            manifest_object: self.0.manifest_object.clone(),
            journal_set_sha256: self.0.journal_set_sha256.clone(),
        });
        let name = tier_delete_journal_object_name(&bound);
        if !self.0.journal_names.contains(&name) {
            return Err(Error::other("tier delete dispatch does not authorize this exact cleanup identity"));
        }
        Ok(name)
    }
}

pub(crate) struct PreparedTierDeleteDispatch {
    permit: Option<DispatchedJournalPermit>,
}

pub(crate) struct ActiveTierDeleteDispatch {
    manifest: TierDeleteDispatchManifest,
    authorized_etag: String,
    entries: Vec<Jentry>,
    authorization: TierDeleteDispatchAuthorization,
}

impl PreparedTierDeleteDispatch {
    pub(crate) fn consume(mut self, bucket: &str, incarnation: uuid::Uuid, prefix: &str) -> Result<ActiveTierDeleteDispatch> {
        let permit = self
            .permit
            .take()
            .ok_or_else(|| Error::other("tier delete dispatch permit was already consumed"))?;
        let manifest_object = tier_delete_dispatch_manifest_object_name(bucket, incarnation, prefix);
        if permit.manifest.state != TierDeleteDispatchManifestState::DispatchAuthorized
            || permit.manifest.bucket != bucket
            || permit.manifest.bucket_incarnation != incarnation
            || permit.manifest.prefix != prefix
            || permit.entries.iter().map(tier_delete_journal_object_name).collect::<Vec<_>>() != permit.manifest.journal_names
            || !tier_delete_journal_fleet_proof_matches(&permit.fleet_proof)
            || tier_delete_journal_topology_generation(&permit.fleet_proof) != permit.manifest.topology_generation
        {
            return Err(Error::other("tier delete dispatch permit validation failed"));
        }
        let authorization = TierDeleteDispatchAuthorization(Arc::new(TierDeleteDispatchAuthorizationInner {
            manifest_object,
            operation_id: permit.manifest.operation_id,
            bucket: permit.manifest.bucket.clone(),
            bucket_incarnation: permit.manifest.bucket_incarnation,
            prefix: permit.manifest.prefix.clone(),
            journal_set_sha256: permit.manifest.journal_set_sha256.clone(),
            journal_names: permit.manifest.journal_names.iter().cloned().collect(),
            topology_generation: permit.manifest.topology_generation.clone(),
            fleet_proof: permit.fleet_proof,
            mutation_started: AtomicBool::new(false),
        }));
        Ok(ActiveTierDeleteDispatch {
            manifest: permit.manifest,
            authorized_etag: permit.authorized_etag,
            entries: permit.entries,
            authorization,
        })
    }
}

impl ActiveTierDeleteDispatch {
    pub(crate) fn authorization(&self) -> TierDeleteDispatchAuthorization {
        self.authorization.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PersistedTierDeleteJournalEntry {
    version: u8,
    obj_name: String,
    version_id: String,
    tier_name: String,
    #[serde(default)]
    backend_identity: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_id_exact: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_state: Option<rustfs_filemeta::TransitionVersionState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state: Option<TierDeleteJournalState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<TierDeleteSourceIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatch: Option<TierDeleteDispatchBinding>,
}

impl PersistedTierDeleteJournalEntry {
    fn from_jentry(je: &Jentry) -> Result<Self> {
        validate_version_state(je.version_state, &je.version_id, je.version_id_exact)?;
        let legacy_unknown = je.version_state == rustfs_filemeta::TransitionVersionState::Unknown;
        let version = if je.dispatch.is_some() {
            if je.backend_identity.is_none() {
                return Err(Error::other("tier delete transaction is missing its backend identity"));
            }
            if !je.source.as_ref().is_some_and(TierDeleteSourceIdentity::has_stable_identity) {
                return Err(Error::other("tier delete v6 transaction is missing its stable source identity"));
            }
            TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION
        } else if je.persisted_version == TIER_DELETE_JOURNAL_TRANSACTION_VERSION {
            // A decoded v5 may be inspected or checkpointed by compatibility
            // tooling, but new online transactions are never emitted as v5.
            TIER_DELETE_JOURNAL_TRANSACTION_VERSION
        } else if je.source.is_some() || matches!(je.state, TierDeleteJournalState::Prepared | TierDeleteJournalState::Dispatched)
        {
            return Err(Error::other("new sole-owner tier delete journal requires a dispatch manifest binding"));
        } else if legacy_unknown {
            if je.backend_identity.is_some() {
                TIER_DELETE_JOURNAL_VERSION
            } else {
                1
            }
        } else {
            if je.backend_identity.is_none() {
                return Err(Error::other("new tier delete journal entry is missing its backend identity"));
            }
            TIER_DELETE_JOURNAL_STATE_VERSION
        };
        Ok(Self {
            version,
            obj_name: je.obj_name.clone(),
            version_id: je.version_id.clone(),
            tier_name: je.tier_name.clone(),
            backend_identity: je.backend_identity,
            version_id_exact: je.version_id_exact.then_some(true),
            version_state: (!legacy_unknown).then_some(je.version_state),
            state: (version == TIER_DELETE_JOURNAL_TRANSACTION_VERSION || version == TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION)
                .then_some(je.state),
            source: (version == TIER_DELETE_JOURNAL_TRANSACTION_VERSION || version == TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION)
                .then(|| je.source.clone())
                .flatten(),
            dispatch: (version == TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION)
                .then(|| je.dispatch.clone())
                .flatten(),
        })
    }

    fn into_jentry(self) -> Result<Jentry> {
        // Empty `version_id` is a legal sentinel for objects transitioned to an
        // unversioned remote tier (see CLAUDE.md: a tier version of `None`/`""`
        // means the tier bucket is unversioned, so the remote delete is issued
        // without a versionId). Only reject entries missing the object or tier
        // name, which are always populated for a TRANSITION_COMPLETE object.
        if self.obj_name.is_empty() || self.tier_name.is_empty() {
            return Err(Error::other("tier delete journal entry is incomplete"));
        }
        if self.version != TIER_DELETE_JOURNAL_EXACT_VERSION
            && self.version != TIER_DELETE_JOURNAL_STATE_VERSION
            && self.version != TIER_DELETE_JOURNAL_TRANSACTION_VERSION
            && self.version != TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION
            && self.version_id_exact.unwrap_or(false)
        {
            return Err(Error::other(
                "legacy tier delete journal entry has an unsupported exact version constraint",
            ));
        }
        let persisted_version = self.version;
        let (backend_identity, version_id_exact, version_state, state, source, dispatch) = match self.version {
            1 => (
                None,
                false,
                rustfs_filemeta::TransitionVersionState::Unknown,
                TierDeleteJournalState::Committed,
                None,
                None,
            ),
            TIER_DELETE_JOURNAL_VERSION => (
                Some(
                    self.backend_identity
                        .ok_or_else(|| Error::other("tier delete journal v2 entry is missing its backend identity"))?,
                ),
                false,
                rustfs_filemeta::TransitionVersionState::Unknown,
                TierDeleteJournalState::Committed,
                None,
                None,
            ),
            TIER_DELETE_JOURNAL_EXACT_VERSION => {
                if self.version_id.is_empty() || self.version_id_exact != Some(true) {
                    return Err(Error::other("tier delete journal v3 entry is missing its exact version constraint"));
                }
                (
                    Some(
                        self.backend_identity
                            .ok_or_else(|| Error::other("tier delete journal v3 entry is missing its backend identity"))?,
                    ),
                    true,
                    rustfs_filemeta::TransitionVersionState::Exact,
                    TierDeleteJournalState::Committed,
                    None,
                    None,
                )
            }
            TIER_DELETE_JOURNAL_STATE_VERSION => {
                let state = self
                    .version_state
                    .ok_or_else(|| Error::other("tier delete journal v4 entry is missing its version state"))?;
                let exact = self.version_id_exact.unwrap_or(false);
                validate_version_state(state, &self.version_id, exact)?;
                (
                    Some(
                        self.backend_identity
                            .ok_or_else(|| Error::other("tier delete journal v4 entry is missing its backend identity"))?,
                    ),
                    exact,
                    state,
                    TierDeleteJournalState::Committed,
                    None,
                    None,
                )
            }
            TIER_DELETE_JOURNAL_TRANSACTION_VERSION | TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION => {
                let version = self.version;
                let state = self.state.ok_or_else(|| {
                    Error::other_with_context("tier delete journal entry is missing its state", format!("journal v{version}"))
                })?;
                let source = self.source.ok_or_else(|| {
                    Error::other_with_context(
                        "tier delete journal entry is missing its source identity",
                        format!("journal v{version}"),
                    )
                })?;
                let exact = self.version_id_exact.unwrap_or(false);
                let version_state = self.version_state.ok_or_else(|| {
                    Error::other_with_context(
                        "tier delete journal entry is missing its version state",
                        format!("journal v{version}"),
                    )
                })?;
                validate_version_state(version_state, &self.version_id, exact)?;
                let dispatch = if version == TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION {
                    Some(
                        self.dispatch
                            .ok_or_else(|| Error::other("tier delete journal v6 entry is missing its dispatch binding"))?,
                    )
                } else {
                    if self.dispatch.is_some() {
                        return Err(Error::other("tier delete journal v5 entry has an unsupported dispatch binding"));
                    }
                    None
                };
                (
                    Some(self.backend_identity.ok_or_else(|| {
                        Error::other_with_context(
                            "tier delete journal entry is missing its backend identity",
                            format!("journal v{version}"),
                        )
                    })?),
                    exact,
                    version_state,
                    state,
                    Some(source),
                    dispatch,
                )
            }
            version => return Err(Error::other(format!("unsupported tier delete journal version {version}"))),
        };
        Ok(Jentry {
            persisted_version,
            obj_name: self.obj_name,
            version_id: self.version_id,
            tier_name: self.tier_name,
            backend_identity,
            version_id_exact,
            version_state,
            state,
            source,
            dispatch,
        })
    }
}

fn validate_version_state(
    state: rustfs_filemeta::TransitionVersionState,
    version_id: &str,
    version_id_exact: bool,
) -> Result<()> {
    use rustfs_filemeta::TransitionVersionState::{Exact, KnownDisabled, SuspendedNull, Unknown};

    let valid = match state {
        Unknown => !version_id_exact,
        KnownDisabled => version_id.is_empty() && !version_id_exact,
        SuspendedNull => version_id == "null" && version_id_exact,
        Exact => !version_id.is_empty() && version_id != "null" && version_id_exact,
    };
    if !valid {
        return Err(Error::other("tier delete journal version state conflicts with its version id"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierDeleteJournalRecoveryStats {
    pub scanned: usize,
    pub deleted: usize,
    pub failed: usize,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierDeleteDispatchManifestRecoveryStats {
    pub scanned: usize,
    pub advanced: usize,
    pub deleted: usize,
    pub retained: usize,
    pub failed: usize,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

pub(crate) fn tier_delete_journal_object_name(je: &Jentry) -> String {
    let digest = tier_delete_journal_identity_digest(je);
    if let Some(dispatch) = &je.dispatch {
        return format!(
            "{TIER_DELETE_JOURNAL_V6_PREFIX}{}/{}.json",
            dispatch.operation_id.simple(),
            rustfs_utils::crypto::hex(digest.as_ref())
        );
    }
    format!("{TIER_DELETE_JOURNAL_LEGACY_PREFIX}{}.json", rustfs_utils::crypto::hex(digest.as_ref()))
}

fn tier_delete_journal_v6_object_name(je: &Jentry, operation_id: uuid::Uuid) -> String {
    let digest = tier_delete_journal_identity_digest(je);
    format!(
        "{TIER_DELETE_JOURNAL_V6_PREFIX}{}/{}.json",
        operation_id.simple(),
        rustfs_utils::crypto::hex(digest.as_ref())
    )
}

fn tier_delete_journal_identity_digest(je: &Jentry) -> impl AsRef<[u8]> {
    let mut hasher = Sha256::new();
    hasher.update(je.tier_name.as_bytes());
    hasher.update([0]);
    hasher.update(je.obj_name.as_bytes());
    hasher.update([0]);
    hasher.update(je.version_id.as_bytes());
    if let Some(backend_identity) = je.backend_identity {
        hasher.update([0]);
        hasher.update(backend_identity);
    }
    if je.version_id_exact {
        hasher.update([0]);
        hasher.update(b"exact-version-id");
    }
    if let Some(source) = &je.source {
        hasher.update([0]);
        hasher.update(source.bucket.as_bytes());
        hasher.update([0]);
        hasher.update(source.object.as_bytes());
        hasher.update([0]);
        hasher.update(source.version_id.as_deref().unwrap_or_default().as_bytes());
        hasher.update([0]);
        hasher.update(source.data_dir.as_deref().unwrap_or_default().as_bytes());
        hasher.update([0]);
        hasher.update(source.etag.as_deref().unwrap_or_default().as_bytes());
        hasher.update([0]);
        hasher.update(source.mod_time.as_deref().unwrap_or_default().as_bytes());
    }
    hasher.finalize()
}

pub(crate) fn decode_tier_delete_journal_entry(data: &[u8]) -> Result<Jentry> {
    let persisted: PersistedTierDeleteJournalEntry =
        serde_json::from_slice(data).map_err(|err| Error::other(format!("decode tier delete journal failed: {err}")))?;
    persisted.into_jentry()
}

pub(crate) fn encode_tier_delete_journal_entry(je: &Jentry) -> Result<Vec<u8>> {
    serde_json::to_vec(&PersistedTierDeleteJournalEntry::from_jentry(je)?)
        .map_err(|err| Error::other(format!("encode tier delete journal failed: {err}")))
}

async fn read_tier_delete_dispatch_manifest(
    api: Arc<ECStore>,
    object_name: &str,
) -> Result<Option<(TierDeleteDispatchManifest, String)>> {
    match config_boundary::read_config_with_metadata(api, object_name, &ObjectOptions::default()).await {
        Ok((data, metadata)) => {
            let etag = metadata
                .etag
                .ok_or_else(|| Error::other("tier delete dispatch manifest has no entity tag"))?;
            Ok(Some((decode_tier_delete_dispatch_manifest(&data, object_name)?, etag)))
        }
        Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

async fn read_tier_delete_journal_with_etag(api: Arc<ECStore>, name: &str) -> Result<Option<(Jentry, String)>> {
    match config_boundary::read_config_with_metadata(api, name, &ObjectOptions::default()).await {
        Ok((data, metadata)) => {
            let etag = metadata
                .etag
                .ok_or_else(|| Error::other("tier delete journal has no entity tag"))?;
            Ok(Some((decode_tier_delete_journal_entry(&data)?, etag)))
        }
        Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn save_config_if_none(api: Arc<ECStore>, name: &str, data: Vec<u8>) -> Result<()> {
    save_config_if_none_fenced(api, name, data, &|| true).await
}

fn ensure_durable_write_fence(fences_current: &impl Fn() -> bool, edge: &str) -> Result<()> {
    if fences_current() {
        Ok(())
    } else {
        Err(Error::other_with_context("durable ILM write fence changed", edge.to_owned()))
    }
}

async fn save_config_if_none_fenced(
    api: Arc<ECStore>,
    name: &str,
    data: Vec<u8>,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    ensure_durable_write_fence(fences_current, "before If-None-Match save")?;
    let write = config_boundary::save_config_with_opts(
        api.clone(),
        name,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    ensure_durable_write_fence(fences_current, "during If-None-Match save")?;
    match write {
        Ok(()) => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
        Err(Error::PreconditionFailed) => {
            let observed = config_boundary::read_config(api.clone(), name).await;
            ensure_durable_write_fence(fences_current, "during If-None-Match confirmation read")?;
            match observed {
                Ok(observed) if observed == data => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
                _ => Err(Error::PreconditionFailed),
            }
        }
        Err(write_err) => {
            let observed = config_boundary::read_config(api.clone(), name).await;
            ensure_durable_write_fence(fences_current, "during ambiguous If-None-Match confirmation read")?;
            match observed {
                Ok(observed) if observed == data => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
                _ => Err(write_err),
            }
        }
    }
}

async fn save_config_if_match_fenced(
    api: Arc<ECStore>,
    name: &str,
    data: Vec<u8>,
    etag: &str,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    ensure_durable_write_fence(fences_current, "before If-Match save")?;
    let write = config_boundary::save_config_with_opts(
        api.clone(),
        name,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(etag.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    ensure_durable_write_fence(fences_current, "during If-Match save")?;
    match write {
        Ok(()) => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
        Err(Error::PreconditionFailed) => {
            let observed = config_boundary::read_config(api.clone(), name).await;
            ensure_durable_write_fence(fences_current, "during If-Match confirmation read")?;
            match observed {
                Ok(observed) if observed == data => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
                _ => Err(Error::PreconditionFailed),
            }
        }
        Err(write_err) => {
            let observed = config_boundary::read_config(api.clone(), name).await;
            ensure_durable_write_fence(fences_current, "during ambiguous If-Match confirmation read")?;
            match observed {
                Ok(observed) if observed == data => record_durable_config_progress_fenced(api, name, &data, fences_current).await,
                _ => Err(write_err),
            }
        }
    }
}

async fn record_durable_config_progress_fenced(
    api: Arc<ECStore>,
    name: &str,
    data: &[u8],
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    // Keep this receipt update retryable independently from the config write:
    // both save helpers recognize an already-persisted byte-identical target
    // and repair a crash between the quorum write and receipt advancement.
    ensure_durable_write_fence(fences_current, "before decommission progress receipt")?;
    let result = Box::pin(api.record_durable_ilm_decommission_progress(name, data)).await;
    ensure_durable_write_fence(fences_current, "during decommission progress receipt")?;
    result
}

async fn record_tier_delete_journal_progress_fenced(
    api: Arc<ECStore>,
    name: &str,
    entry: &Jentry,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    record_durable_config_progress_fenced(api, name, &encode_tier_delete_journal_entry(entry)?, fences_current).await
}

async fn record_tier_delete_dispatch_manifest_progress_fenced(
    api: Arc<ECStore>,
    name: &str,
    manifest: &TierDeleteDispatchManifest,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    record_durable_config_progress_fenced(api, name, &encode_tier_delete_dispatch_manifest(manifest)?, fences_current).await
}

async fn delete_durable_config_if_match(
    api: Arc<ECStore>,
    name: &str,
    current_data: &[u8],
    current_etag: &str,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    if !fences_current() {
        return Err(Error::other("durable ILM terminal cleanup fence changed before terminal receipt"));
    }
    // Terminal receipt is the durable authorization to remove every copy a
    // pool decommission may have staged. It must precede the first delete.
    let target_pool_indices = Box::pin(api.record_durable_ilm_decommission_terminal_target_pools(name, current_data)).await?;
    if !fences_current() {
        return Err(Error::other("durable ILM terminal cleanup fence changed after terminal receipt"));
    }
    let terminal_record = validate_durable_ilm_record(name, current_data)?;
    if let Some(target_pool_indices) = target_pool_indices {
        for target_pool_idx in target_pool_indices {
            if !fences_current() {
                return Err(Error::other("durable ILM terminal cleanup fence changed before target cleanup"));
            }
            match config_boundary::delete_config(api.pools[target_pool_idx].clone(), name).await {
                Ok(()) | Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => {}
                Err(err) => return Err(err),
            }
            if !fences_current() {
                return Err(Error::other("durable ILM terminal cleanup fence changed during target cleanup"));
            }
        }
        // The source pool remains the decommission owner's durable checkpoint
        // until its verified cleanup consumes the terminal receipt. A global
        // delete here would erase that source after only proving target-copy
        // cleanup, defeating restart recovery.
        return Ok(());
    }

    // A logical config key may be visible from more than one pool while a
    // decommission is active. Delete one byte-identical ETag generation at a
    // time and strongly re-read after every attempt; never delete a successor
    // operation that reuses a manifest path.
    let mut etag = current_etag.to_string();
    for _ in 0..api.pools.len().saturating_add(3) {
        if !fences_current() {
            return Err(Error::other("durable ILM terminal cleanup fence changed before ETag deletion"));
        }
        let delete = config_boundary::delete_config_if_match(api.clone(), name, &etag).await;
        let observed = match config_boundary::read_config_with_metadata(api.clone(), name, &ObjectOptions::default()).await {
            Ok(observed) => Some(observed),
            Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => None,
            Err(err) => return Err(err),
        };
        if !fences_current() {
            return Err(Error::other("durable ILM terminal cleanup fence changed during ETag deletion"));
        }
        let Some((observed_data, metadata)) = observed else {
            return Ok(());
        };
        let observed_etag = metadata
            .etag
            .filter(|etag| !etag.trim().is_empty())
            .ok_or_else(|| Error::other("durable ILM record has no entity tag during terminal cleanup"))?;
        if observed_data != current_data {
            let observed_record = validate_durable_ilm_record(name, &observed_data)?;
            let same_terminal_history = observed_record.namespace == terminal_record.namespace
                && observed_record.id_kind == terminal_record.id_kind
                && observed_record.id == terminal_record.id
                && observed_record
                    .checkpoint
                    .is_predecessor_of_terminal(&terminal_record.checkpoint);
            if !same_terminal_history
                || !matches!(
                    &delete,
                    Ok(()) | Err(Error::ConfigNotFound) | Err(Error::FileNotFound) | Err(Error::PreconditionFailed)
                )
            {
                return Err(Error::PreconditionFailed);
            }
            // The exact terminal ETag was removed and exposed an older
            // generation of the same immutable operation. The operation lock
            // (manifest) or operation-scoped v6 path (journal) excludes ABA,
            // so purge the remaining historical versions before confirming.
            if !fences_current() {
                return Err(Error::other("durable ILM terminal cleanup fence changed before history purge"));
            }
            match config_boundary::delete_config(api.clone(), name).await {
                Ok(()) | Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => {
                    if !fences_current() {
                        return Err(Error::other("durable ILM terminal cleanup fence changed during history purge"));
                    }
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        match delete {
            Ok(()) | Err(Error::ConfigNotFound) | Err(Error::FileNotFound) | Err(Error::PreconditionFailed) => {
                etag = observed_etag;
            }
            Err(_) if observed_etag != etag => etag = observed_etag,
            Err(err) => return Err(err),
        }
    }
    Err(Error::other("durable ILM terminal cleanup retained too many visible pool generations"))
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn install_test_tier_delete_dispatch_fixture(
    api: Arc<ECStore>,
    bucket: &str,
    bucket_incarnation: uuid::Uuid,
    prefix: &str,
    entries: Vec<(Jentry, Option<TierDeleteJournalState>)>,
    manifest_state: TierDeleteDispatchManifestState,
) -> Result<(String, Vec<Jentry>)> {
    let fleet_proof =
        acquire_tier_delete_journal_fleet_proof().ok_or_else(|| Error::other("test dispatch fixture requires a fleet proof"))?;
    let topology_generation = tier_delete_journal_topology_generation(&fleet_proof);
    let manifest_name = tier_delete_dispatch_manifest_object_name(bucket, bucket_incarnation, prefix);
    let operation_id = uuid::Uuid::new_v4();
    let mut states_by_name = std::collections::BTreeMap::new();
    let raw_entries = entries
        .into_iter()
        .map(|(entry, state)| {
            let name = tier_delete_journal_v6_object_name(&entry, operation_id);
            if states_by_name.insert(name, state).is_some() {
                return Err(Error::other("test dispatch fixture contains a duplicate journal name"));
            }
            Ok(entry)
        })
        .collect::<Result<Vec<_>>>()?;
    let journal_names = states_by_name.keys().cloned().collect::<Vec<_>>();
    let manifest = TierDeleteDispatchManifest {
        version: TIER_DELETE_DISPATCH_MANIFEST_VERSION,
        operation_id,
        bucket: bucket.to_string(),
        bucket_incarnation,
        prefix: prefix.to_string(),
        journal_set_sha256: tier_delete_dispatch_journal_set_digest(&journal_names),
        journal_count: journal_names.len() as u64,
        journal_names,
        topology_generation,
        state: manifest_state,
    };
    let mut bound = bind_dispatch_entries(raw_entries, manifest.operation_id, &manifest_name, &manifest.journal_set_sha256)?;
    save_config_if_none(api.clone(), &manifest_name, encode_tier_delete_dispatch_manifest(&manifest)?).await?;
    for entry in &mut bound {
        let name = tier_delete_journal_object_name(entry);
        let Some(state) = states_by_name
            .remove(&name)
            .ok_or_else(|| Error::other("test dispatch fixture state is missing"))?
        else {
            continue;
        };
        entry.state = state;
        save_config_if_none(api.clone(), &name, encode_tier_delete_journal_entry(entry)?).await?;
    }
    Ok((manifest_name, bound))
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn test_tier_delete_dispatch_manifest_state(
    api: Arc<ECStore>,
    manifest_name: &str,
) -> Result<Option<TierDeleteDispatchManifestState>> {
    Ok(read_tier_delete_dispatch_manifest(api, manifest_name)
        .await?
        .map(|(manifest, _)| manifest.state))
}

fn bind_dispatch_entries(
    entries: Vec<Jentry>,
    operation_id: uuid::Uuid,
    manifest_object: &str,
    journal_set_sha256: &str,
) -> Result<Vec<Jentry>> {
    let mut entries = entries;
    entries.sort_by_key(|entry| tier_delete_journal_v6_object_name(entry, operation_id));
    let mut bound = Vec::with_capacity(entries.len());
    let mut last_name: Option<String> = None;
    for mut entry in entries {
        if !entry.can_replace_tier_free_version() {
            return Err(Error::other(
                "tier delete dispatch contains a transitioned source without a stable exact cleanup identity",
            ));
        }
        let name = tier_delete_journal_v6_object_name(&entry, operation_id);
        entry.persisted_version = 0;
        entry.state = TierDeleteJournalState::Prepared;
        entry.dispatch = Some(TierDeleteDispatchBinding {
            operation_id,
            manifest_object: manifest_object.to_string(),
            journal_set_sha256: journal_set_sha256.to_string(),
        });
        debug_assert_eq!(tier_delete_journal_object_name(&entry), name);
        if last_name.as_ref() == Some(&name) {
            let previous = bound
                .last()
                .ok_or_else(|| Error::other("tier delete dispatch duplicate accounting failed"))?;
            if !same_tier_delete_journal_identity(previous, &entry) {
                return Err(Error::other("tier delete dispatch contains conflicting duplicate journal names"));
            }
            continue;
        }
        last_name = Some(name);
        bound.push(entry);
    }
    Ok(bound)
}

fn tier_delete_dispatch_desired_names(entries: &[Jentry], operation_id: uuid::Uuid) -> Result<Vec<String>> {
    if entries.len() > MAX_TIER_DELETE_DISPATCH_JOURNALS {
        return Err(Error::other("tier delete dispatch contains too many journals"));
    }
    let mut names = Vec::with_capacity(entries.len());
    for entry in entries {
        if !entry.can_replace_tier_free_version() {
            return Err(Error::other(
                "tier delete dispatch contains a transitioned source without a stable exact cleanup identity",
            ));
        }
        names.push(tier_delete_journal_v6_object_name(entry, operation_id));
    }
    names.sort();
    names.dedup();
    Ok(names)
}

fn validate_bound_journal(manifest: &TierDeleteDispatchManifest, name: &str, entry: &Jentry) -> Result<()> {
    if entry.persisted_version != TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION
        || tier_delete_journal_object_name(entry) != name
        || entry.dispatch.as_ref()
            != Some(&TierDeleteDispatchBinding {
                operation_id: manifest.operation_id,
                manifest_object: tier_delete_dispatch_manifest_object_name(
                    &manifest.bucket,
                    manifest.bucket_incarnation,
                    &manifest.prefix,
                ),
                journal_set_sha256: manifest.journal_set_sha256.clone(),
            })
    {
        return Err(Error::other("tier delete journal does not match its dispatch manifest"));
    }
    Ok(())
}

async fn load_complete_dispatch_journal_set(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    allowed_states: &[TierDeleteJournalState],
    fences_current: &impl Fn() -> bool,
) -> Result<Vec<Jentry>> {
    let mut entries = Vec::with_capacity(manifest.journal_names.len());
    for name in &manifest.journal_names {
        let (entry, _) = read_tier_delete_journal_with_etag(api.clone(), name)
            .await?
            .ok_or_else(|| Error::other("tier delete dispatch manifest references a missing journal"))?;
        validate_bound_journal(manifest, name, &entry)?;
        if !allowed_states.contains(&entry.state) {
            return Err(Error::other("tier delete dispatch journal has an invalid state"));
        }
        record_tier_delete_journal_progress_fenced(api.clone(), name, &entry, fences_current).await?;
        entries.push(entry);
    }
    Ok(entries)
}

async fn persist_prepared_dispatch_journal(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    entry: &Jentry,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    let name = tier_delete_journal_object_name(entry);
    for _ in 0..4 {
        if let Some((current, _)) = read_tier_delete_journal_with_etag(api.clone(), &name).await? {
            validate_bound_journal(manifest, &name, &current)?;
            if !same_tier_delete_journal_identity(&current, entry) {
                return Err(Error::other("tier delete journal key is occupied by another cleanup identity"));
            }
            record_tier_delete_journal_progress_fenced(api.clone(), &name, &current, fences_current).await?;
            return match current.state {
                TierDeleteJournalState::Prepared | TierDeleteJournalState::Dispatched => Ok(()),
                TierDeleteJournalState::Committed => {
                    Err(Error::other("a completed tier delete journal cannot be rebound to a preparing operation"))
                }
            };
        }
        match save_config_if_none_fenced(api.clone(), &name, encode_tier_delete_journal_entry(entry)?, fences_current).await {
            Ok(()) | Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::other("tier delete journal changed repeatedly during prepare"))
}

async fn dispatch_prepared_journal(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    expected: &Jentry,
    fences_current: &impl Fn() -> bool,
) -> Result<Jentry> {
    let name = tier_delete_journal_object_name(expected);
    for _ in 0..4 {
        let (mut current, etag) = read_tier_delete_journal_with_etag(api.clone(), &name)
            .await?
            .ok_or_else(|| Error::other("prepared tier delete journal disappeared before dispatch"))?;
        validate_bound_journal(manifest, &name, &current)?;
        if !same_tier_delete_journal_identity(&current, expected) {
            return Err(Error::other("prepared tier delete journal changed identity before dispatch"));
        }
        match current.state {
            TierDeleteJournalState::Dispatched => {
                record_tier_delete_journal_progress_fenced(api.clone(), &name, &current, fences_current).await?;
                return Ok(current);
            }
            TierDeleteJournalState::Committed => {
                record_tier_delete_journal_progress_fenced(api.clone(), &name, &current, fences_current).await?;
                return Err(Error::other("prepared tier delete journal was committed before dispatch authorization"));
            }
            TierDeleteJournalState::Prepared => {}
        }
        current.state = TierDeleteJournalState::Dispatched;
        match save_config_if_match_fenced(api.clone(), &name, encode_tier_delete_journal_entry(&current)?, &etag, fences_current)
            .await
        {
            Ok(()) | Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::other("tier delete journal changed repeatedly during dispatch"))
}

async fn validate_staged_dispatch_journal_set(api: Arc<ECStore>, manifest: &TierDeleteDispatchManifest) -> Result<()> {
    for name in &manifest.journal_names {
        let Some((entry, _)) = read_tier_delete_journal_with_etag(api.clone(), name).await? else {
            continue;
        };
        validate_bound_journal(manifest, name, &entry)?;
        if !matches!(entry.state, TierDeleteJournalState::Prepared | TierDeleteJournalState::Dispatched) {
            return Err(Error::other("an uncommitted tier delete dispatch contains a committed journal"));
        }
    }
    Ok(())
}

async fn delete_staged_dispatch_journal_set<F>(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    fences_current: &F,
) -> Result<()>
where
    F: Fn() -> bool,
{
    // Validate the complete immutable set before deleting its first member so
    // a corrupt binding or impossible Committed state quarantines the whole
    // operation rather than producing a partial rollback.
    validate_staged_dispatch_journal_set(api.clone(), manifest).await?;
    for name in &manifest.journal_names {
        if !fences_current() {
            return Err(Error::other("tier delete dispatch rollback fence changed"));
        }
        let Some((entry, etag)) = read_tier_delete_journal_with_etag(api.clone(), name).await? else {
            continue;
        };
        validate_bound_journal(manifest, name, &entry)?;
        if !matches!(entry.state, TierDeleteJournalState::Prepared | TierDeleteJournalState::Dispatched) {
            return Err(Error::other("an uncommitted tier delete dispatch contains a committed journal"));
        }
        if !fences_current() {
            return Err(Error::other("tier delete dispatch rollback fence changed"));
        }
        let data = encode_tier_delete_journal_entry(&entry)?;
        match delete_durable_config_if_match(api.clone(), name, &data, &etag, fences_current).await {
            Ok(()) | Err(Error::ConfigNotFound) => {}
            Err(Error::PreconditionFailed) => {
                return Err(Error::other("tier delete dispatch journal changed during rollback"));
            }
            Err(err) => return Err(err),
        }
    }
    for name in &manifest.journal_names {
        if !fences_current() {
            return Err(Error::other("tier delete dispatch rollback fence changed"));
        }
        if read_tier_delete_journal_with_etag(api.clone(), name).await?.is_some() {
            return Err(Error::other("tier delete dispatch rollback retained a journal"));
        }
    }
    Ok(())
}

async fn seal_and_rollback_preparing_dispatch(
    api: Arc<ECStore>,
    expected: &TierDeleteDispatchManifest,
    fleet_proof: &TierDeleteJournalFleetProofToken,
    bucket_fence: &crate::object_api::NamespaceLockFence,
    operation_guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<()> {
    let manifest_name =
        tier_delete_dispatch_manifest_object_name(&expected.bucket, expected.bucket_incarnation, &expected.prefix);
    let fences_current =
        || dispatch_write_fences_current(bucket_fence, operation_guard, fleet_proof, &expected.topology_generation);
    if !fences_current() {
        return Err(Error::other("tier delete dispatch rollback fence changed"));
    }
    let Some((mut current, etag)) = read_tier_delete_dispatch_manifest(api.clone(), &manifest_name).await? else {
        return Ok(());
    };
    if current.operation_id != expected.operation_id
        || current.journal_set_sha256 != expected.journal_set_sha256
        || current.state != TierDeleteDispatchManifestState::Preparing
    {
        return Err(Error::other(
            "tier delete dispatch cannot be rolled back after authorization or identity change",
        ));
    }
    current.state = TierDeleteDispatchManifestState::Aborting;
    if !fences_current() {
        return Err(Error::other("tier delete dispatch rollback fence changed"));
    }
    save_config_if_match_fenced(
        api.clone(),
        &manifest_name,
        encode_tier_delete_dispatch_manifest(&current)?,
        &etag,
        &fences_current,
    )
    .await?;
    let (mut sealed, sealed_etag) = read_tier_delete_dispatch_manifest(api.clone(), &manifest_name)
        .await?
        .ok_or_else(|| Error::other("sealed tier delete dispatch manifest disappeared"))?;
    if sealed.operation_id != expected.operation_id || sealed.state != TierDeleteDispatchManifestState::Aborting {
        return Err(Error::other("tier delete dispatch rollback seal changed"));
    }
    delete_staged_dispatch_journal_set(api.clone(), &sealed, &fences_current).await?;
    sealed.state = TierDeleteDispatchManifestState::Aborted;
    if !fences_current() {
        return Err(Error::other("tier delete dispatch rollback fence changed"));
    }
    save_config_if_match_fenced(
        api,
        &manifest_name,
        encode_tier_delete_dispatch_manifest(&sealed)?,
        &sealed_etag,
        &fences_current,
    )
    .await
}

async fn authorized_dispatch_permit(
    api: Arc<ECStore>,
    manifest_name: &str,
    fleet_proof: TierDeleteJournalFleetProofToken,
    bucket_fence: &crate::object_api::NamespaceLockFence,
    operation_guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<PreparedTierDeleteDispatch> {
    let (manifest, authorized_etag) = read_tier_delete_dispatch_manifest(api.clone(), manifest_name)
        .await?
        .ok_or_else(|| Error::other("authorized tier delete dispatch manifest disappeared"))?;
    if manifest.state != TierDeleteDispatchManifestState::DispatchAuthorized
        || !tier_delete_journal_fleet_proof_matches(&fleet_proof)
        || tier_delete_journal_topology_generation(&fleet_proof) != manifest.topology_generation
    {
        return Err(Error::other("tier delete dispatch authorization is stale or unconfirmed"));
    }
    let entries = {
        let fences_current =
            || dispatch_write_fences_current(bucket_fence, operation_guard, &fleet_proof, &manifest.topology_generation);
        record_tier_delete_dispatch_manifest_progress_fenced(api.clone(), manifest_name, &manifest, &fences_current).await?;
        load_complete_dispatch_journal_set(
            api,
            &manifest,
            &[TierDeleteJournalState::Dispatched, TierDeleteJournalState::Committed],
            &fences_current,
        )
        .await?
    };
    Ok(PreparedTierDeleteDispatch {
        permit: Some(DispatchedJournalPermit {
            manifest,
            authorized_etag,
            entries,
            fleet_proof,
        }),
    })
}

pub(crate) async fn prepare_tier_delete_dispatch(
    api: Arc<ECStore>,
    bucket: &str,
    bucket_incarnation: uuid::Uuid,
    prefix: &str,
    entries: Vec<Jentry>,
    fleet_proof: TierDeleteJournalFleetProofToken,
    bucket_fence: &crate::object_api::NamespaceLockFence,
) -> Result<PreparedTierDeleteDispatch> {
    if bucket_incarnation.is_nil() || bucket_fence.is_lock_lost() || !tier_delete_journal_fleet_proof_matches(&fleet_proof) {
        return Err(Error::other("tier delete journal v6 fleet capability is unavailable"));
    }
    let topology_generation = tier_delete_journal_topology_generation(&fleet_proof);
    let manifest_name = tier_delete_dispatch_manifest_object_name(bucket, bucket_incarnation, prefix);
    let operation_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &tier_delete_dispatch_operation_lock_name(&manifest_name))
        .await?;
    let operation_guard = operation_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;

    let (mut manifest, manifest_etag) = loop {
        ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
        match read_tier_delete_dispatch_manifest(api.clone(), &manifest_name).await? {
            Some((existing, etag)) => {
                if existing.bucket != bucket
                    || existing.bucket_incarnation != bucket_incarnation
                    || existing.prefix != prefix
                    || existing.topology_generation != topology_generation
                {
                    return Err(Error::other("an incompatible tier delete dispatch operation already owns this prefix"));
                }
                {
                    let fences_current = || {
                        dispatch_write_fences_current(bucket_fence, &operation_guard, &fleet_proof, &existing.topology_generation)
                    };
                    record_tier_delete_dispatch_manifest_progress_fenced(api.clone(), &manifest_name, &existing, &fences_current)
                        .await?;
                }
                let desired_names = tier_delete_dispatch_desired_names(&entries, existing.operation_id)?;
                let desired_digest = tier_delete_dispatch_journal_set_digest(&desired_names);
                match existing.state {
                    TierDeleteDispatchManifestState::Preparing => {
                        if existing.journal_names != desired_names || existing.journal_set_sha256 != desired_digest {
                            return Err(Error::other("preparing tier delete dispatch no longer matches the complete source set"));
                        }
                        break (existing, etag);
                    }
                    TierDeleteDispatchManifestState::DispatchAuthorized => {
                        if desired_names
                            .iter()
                            .any(|name| !existing.journal_names.binary_search(name).is_ok())
                        {
                            return Err(Error::other(
                                "authorized tier delete dispatch does not cover a newly discovered transitioned source",
                            ));
                        }
                        ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
                        return authorized_dispatch_permit(api, &manifest_name, fleet_proof, bucket_fence, &operation_guard)
                            .await;
                    }
                    TierDeleteDispatchManifestState::Completed => {
                        return Err(Error::other("a completed tier delete dispatch is still awaiting durable cleanup"));
                    }
                    TierDeleteDispatchManifestState::Aborting => {
                        return Err(Error::other("a tier delete dispatch rollback is still in progress"));
                    }
                    TierDeleteDispatchManifestState::Aborted => {
                        for name in &existing.journal_names {
                            if read_tier_delete_journal_with_etag(api.clone(), name).await?.is_some() {
                                return Err(Error::other("an aborted tier delete dispatch still owns journal records"));
                            }
                        }
                        let data = encode_tier_delete_dispatch_manifest(&existing)?;
                        let fences_current = || {
                            !bucket_fence.is_lock_lost()
                                && !operation_guard.is_lock_lost()
                                && tier_delete_journal_fleet_proof_matches(&fleet_proof)
                                && tier_delete_journal_topology_generation(&fleet_proof) == existing.topology_generation
                        };
                        match delete_durable_config_if_match(api.clone(), &manifest_name, &data, &etag, &fences_current).await {
                            Ok(()) | Err(Error::ConfigNotFound) | Err(Error::PreconditionFailed) => continue,
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
            None => {
                let operation_id = uuid::Uuid::new_v4();
                let desired_names = tier_delete_dispatch_desired_names(&entries, operation_id)?;
                let desired_digest = tier_delete_dispatch_journal_set_digest(&desired_names);
                let new_manifest = TierDeleteDispatchManifest {
                    version: TIER_DELETE_DISPATCH_MANIFEST_VERSION,
                    operation_id,
                    bucket: bucket.to_string(),
                    bucket_incarnation,
                    prefix: prefix.to_string(),
                    journal_names: desired_names.clone(),
                    journal_set_sha256: desired_digest.clone(),
                    journal_count: desired_names.len() as u64,
                    topology_generation: topology_generation.clone(),
                    state: TierDeleteDispatchManifestState::Preparing,
                };
                let fences_current =
                    || dispatch_write_fences_current(bucket_fence, &operation_guard, &fleet_proof, &topology_generation);
                match save_config_if_none_fenced(
                    api.clone(),
                    &manifest_name,
                    encode_tier_delete_dispatch_manifest(&new_manifest)?,
                    &fences_current,
                )
                .await
                {
                    Ok(()) | Err(Error::PreconditionFailed) => continue,
                    Err(err) => return Err(err),
                }
            }
        }
    };

    let bound = bind_dispatch_entries(entries, manifest.operation_id, &manifest_name, &manifest.journal_set_sha256)?;
    let attempt = async {
        let fences_current =
            || dispatch_write_fences_current(bucket_fence, &operation_guard, &fleet_proof, &manifest.topology_generation);
        for entry in &bound {
            ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
            persist_prepared_dispatch_journal(api.clone(), &manifest, entry, &fences_current).await?;
        }
        for entry in &bound {
            ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
            dispatch_prepared_journal(api.clone(), &manifest, entry, &fences_current).await?;
        }
        ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
        if !tier_delete_journal_fleet_proof_matches(&fleet_proof)
            || tier_delete_journal_topology_generation(&fleet_proof) != manifest.topology_generation
        {
            return Err(Error::other("tier delete dispatch fleet proof changed before authorization"));
        }
        manifest.state = TierDeleteDispatchManifestState::DispatchAuthorized;
        let authorized_data = encode_tier_delete_dispatch_manifest(&manifest)?;
        match save_config_if_match_fenced(api.clone(), &manifest_name, authorized_data, &manifest_etag, &fences_current).await {
            Ok(()) => {}
            Err(Error::PreconditionFailed) => {
                return Err(Error::other("tier delete dispatch manifest changed before authorization"));
            }
            Err(cas_err) => {
                // The write may have reached quorum even if the client saw a
                // timeout. Only a strong read confirming Authorized permits
                // mutation; every other outcome is retained for recovery.
                match read_tier_delete_dispatch_manifest(api.clone(), &manifest_name).await {
                    Ok(Some((observed, _)))
                        if observed.operation_id == manifest.operation_id
                            && observed.state == TierDeleteDispatchManifestState::DispatchAuthorized => {}
                    _ => return Err(cas_err),
                }
            }
        }
        Ok(())
    }
    .await;

    if let Err(err) = attempt {
        let authorized = read_tier_delete_dispatch_manifest(api.clone(), &manifest_name)
            .await
            .ok()
            .flatten()
            .is_some_and(|(current, _)| {
                current.operation_id == manifest.operation_id
                    && current.state == TierDeleteDispatchManifestState::DispatchAuthorized
            });
        if !authorized
            && let Err(rollback_err) =
                seal_and_rollback_preparing_dispatch(api.clone(), &manifest, &fleet_proof, bucket_fence, &operation_guard).await
        {
            warn!(
                event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                operation_id = %manifest.operation_id,
                error = ?rollback_err,
                "Tier delete dispatch preparation failed and its rollback remains durable for recovery"
            );
        }
        return Err(err);
    }

    // Re-read the exact Authorized manifest and every bound journal before
    // constructing the private one-shot permit.
    ensure_dispatch_lock_current(bucket_fence, &operation_guard)?;
    authorized_dispatch_permit(api, &manifest_name, fleet_proof, bucket_fence, &operation_guard).await
}

async fn commit_dispatched_journal(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    expected: &Jentry,
    authorization: &TierDeleteDispatchAuthorization,
    fences_current: &impl Fn() -> bool,
) -> Result<Jentry> {
    let name = tier_delete_journal_object_name(expected);
    for _ in 0..4 {
        authorization.ensure_current(&manifest.bucket, manifest.bucket_incarnation, &manifest.prefix)?;
        let (mut current, etag) = read_tier_delete_journal_with_etag(api.clone(), &name)
            .await?
            .ok_or_else(|| Error::other("dispatched tier delete journal disappeared before commit"))?;
        validate_bound_journal(manifest, &name, &current)?;
        if !same_tier_delete_journal_identity(&current, expected) {
            return Err(Error::other("dispatched tier delete journal changed identity before commit"));
        }
        match current.state {
            TierDeleteJournalState::Committed => {
                record_tier_delete_journal_progress_fenced(api.clone(), &name, &current, fences_current).await?;
                authorization.ensure_current(&manifest.bucket, manifest.bucket_incarnation, &manifest.prefix)?;
                return Ok(current);
            }
            TierDeleteJournalState::Prepared => {
                return Err(Error::other("prepared tier delete journal was never dispatch-authorized"));
            }
            TierDeleteJournalState::Dispatched => {}
        }
        current.state = TierDeleteJournalState::Committed;
        authorization.ensure_current(&manifest.bucket, manifest.bucket_incarnation, &manifest.prefix)?;
        let result =
            save_config_if_match_fenced(api.clone(), &name, encode_tier_delete_journal_entry(&current)?, &etag, fences_current)
                .await;
        authorization.ensure_current(&manifest.bucket, manifest.bucket_incarnation, &manifest.prefix)?;
        match result {
            Ok(()) | Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::other("tier delete journal changed repeatedly during commit"))
}

pub(crate) async fn complete_tier_delete_dispatch(
    api: Arc<ECStore>,
    active: &ActiveTierDeleteDispatch,
    bucket_fence: &crate::object_api::NamespaceLockFence,
) -> Result<()> {
    active
        .authorization
        .ensure_current(&active.manifest.bucket, active.manifest.bucket_incarnation, &active.manifest.prefix)?;
    if !active.authorization.mutation_started() {
        return Err(Error::other("tier delete dispatch cannot commit before its local mutation starts"));
    }
    let manifest_name = tier_delete_dispatch_manifest_object_name(
        &active.manifest.bucket,
        active.manifest.bucket_incarnation,
        &active.manifest.prefix,
    );
    let operation_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &tier_delete_dispatch_operation_lock_name(&manifest_name))
        .await?;
    let operation_guard = operation_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let fences_current = || {
        !bucket_fence.is_lock_lost()
            && !operation_guard.is_lock_lost()
            && active
                .authorization
                .ensure_current(&active.manifest.bucket, active.manifest.bucket_incarnation, &active.manifest.prefix)
                .is_ok()
    };
    ensure_durable_write_fence(&fences_current, "before tier delete dispatch completion")?;
    let mut committed_entries = Vec::with_capacity(active.entries.len());
    for entry in &active.entries {
        active.authorization.ensure_current(
            &active.manifest.bucket,
            active.manifest.bucket_incarnation,
            &active.manifest.prefix,
        )?;
        committed_entries
            .push(commit_dispatched_journal(api.clone(), &active.manifest, entry, &active.authorization, &fences_current).await?);
    }

    active
        .authorization
        .ensure_current(&active.manifest.bucket, active.manifest.bucket_incarnation, &active.manifest.prefix)?;
    let (mut current, etag) = read_tier_delete_dispatch_manifest(api.clone(), &manifest_name)
        .await?
        .ok_or_else(|| Error::other("authorized tier delete dispatch manifest disappeared before completion"))?;
    if current.operation_id != active.manifest.operation_id
        || current.journal_set_sha256 != active.manifest.journal_set_sha256
        || current.state != TierDeleteDispatchManifestState::DispatchAuthorized
        || (etag != active.authorized_etag && current != active.manifest)
    {
        return Err(Error::other("tier delete dispatch manifest changed before completion"));
    }
    current.state = TierDeleteDispatchManifestState::Completed;
    active
        .authorization
        .ensure_current(&active.manifest.bucket, active.manifest.bucket_incarnation, &active.manifest.prefix)?;
    let completion = save_config_if_match_fenced(
        api.clone(),
        &manifest_name,
        encode_tier_delete_dispatch_manifest(&current)?,
        &etag,
        &fences_current,
    )
    .await;
    active
        .authorization
        .ensure_current(&active.manifest.bucket, active.manifest.bucket_incarnation, &active.manifest.prefix)?;
    completion?;

    for entry in committed_entries {
        if let Err(err) = enqueue_committed_tier_delete_journal_entry(&entry).await {
            debug!(
                event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                operation_id = %current.operation_id,
                error = ?err,
                "Committed tier delete dispatch will be picked up by periodic recovery"
            );
        }
    }

    Ok(())
}

pub fn record_tier_delete_journal_backend_identity(
    je: &mut Jentry,
    metadata: &std::collections::HashMap<String, String>,
) -> std::io::Result<()> {
    if let Some(identity) = tier_destination_id_from_metadata(metadata)? {
        je.backend_identity = Some(identity);
    }
    Ok(())
}

pub async fn persist_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    let data = encode_tier_delete_journal_entry(je).map_err(std::io::Error::other)?;
    if je.state == TierDeleteJournalState::Prepared && je.source.is_some() {
        let name = tier_delete_journal_object_name(je);
        for _ in 0..3 {
            match config_boundary::save_config_with_opts(
                api.clone(),
                &name,
                data.clone(),
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(Error::PreconditionFailed) => {
                    let current = match config_boundary::read_config(api.clone(), &name).await {
                        Ok(current) => decode_tier_delete_journal_entry(&current).map_err(std::io::Error::other)?,
                        Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => continue,
                        Err(err) => return Err(std::io::Error::other(err)),
                    };
                    if !same_tier_delete_journal_identity(&current, je) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "tier delete journal key is occupied by a different transaction",
                        ));
                    }
                    // A replayed Prepare is idempotent. In particular, never
                    // regress a concurrently committed transaction back to
                    // Prepared.
                    return Ok(());
                }
                Err(err) => return Err(std::io::Error::other(err)),
            }
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal changed repeatedly during prepare",
        ));
    }
    config_boundary::save_config(api, &tier_delete_journal_object_name(je), data)
        .await
        .map_err(std::io::Error::other)
}

#[allow(
    dead_code,
    reason = "legacy v1-v5 journal writer retained for rollback/source compatibility"
)]
pub async fn commit_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    let name = tier_delete_journal_object_name(je);
    let mut committed = je.clone();
    committed.state = TierDeleteJournalState::Committed;
    let data = encode_tier_delete_journal_entry(&committed).map_err(std::io::Error::other)?;

    for _ in 0..3 {
        let (current, etag) =
            match config_boundary::read_config_with_metadata(api.clone(), &name, &ObjectOptions::default()).await {
                Ok((current, metadata)) => (
                    Some(decode_tier_delete_journal_entry(&current).map_err(std::io::Error::other)?),
                    metadata.etag,
                ),
                Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => (None, None),
                Err(err) => return Err(std::io::Error::other(err)),
            };

        if let Some(current) = current {
            if !same_tier_delete_journal_identity(&current, je) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "tier delete journal changed identity before commit",
                ));
            }
            if current.state == TierDeleteJournalState::Committed {
                return Ok(());
            }
            let etag = etag.ok_or_else(|| std::io::Error::other("prepared tier delete journal has no entity tag"))?;
            match config_boundary::save_config_with_opts(
                api.clone(),
                &name,
                data.clone(),
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(HTTPPreconditions {
                        if_match: Some(etag),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(Error::PreconditionFailed) | Err(Error::ConfigNotFound) => continue,
                Err(err) => return Err(std::io::Error::other(err)),
            }
        } else {
            // Another participant may have aborted the shared Prepared key
            // while this participant committed locally. Recreate only this
            // exact committed identity; its all-pool live-source gate still
            // prevents an early remote deletion.
            match config_boundary::save_config_with_opts(
                api.clone(),
                &name,
                data.clone(),
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(Error::PreconditionFailed) => continue,
                Err(err) => return Err(std::io::Error::other(err)),
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "tier delete journal changed repeatedly during commit",
    ))
}

fn same_tier_delete_journal_identity(left: &Jentry, right: &Jentry) -> bool {
    left.obj_name == right.obj_name
        && left.version_id == right.version_id
        && left.tier_name == right.tier_name
        && left.backend_identity == right.backend_identity
        && left.version_id_exact == right.version_id_exact
        && left.version_state == right.version_state
        && left.source == right.source
        && left.dispatch == right.dispatch
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn abort_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    remove_tier_delete_journal_entry(api, je).await
}

#[allow(
    dead_code,
    reason = "legacy v1-v5 prepared-journal cleanup retained for rollback/source compatibility"
)]
pub async fn abort_prepared_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    let name = tier_delete_journal_object_name(je);
    let (data, metadata) = match config_boundary::read_config_with_metadata(api.clone(), &name, &ObjectOptions::default()).await {
        Ok(result) => result,
        Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => return Ok(()),
        Err(err) => return Err(std::io::Error::other(err)),
    };
    let current = decode_tier_delete_journal_entry(&data).map_err(std::io::Error::other)?;
    if !same_tier_delete_journal_identity(&current, je) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "prepared tier delete journal changed identity before abort",
        ));
    }
    if current.state != TierDeleteJournalState::Prepared {
        return Ok(());
    }
    let etag = metadata
        .etag
        .ok_or_else(|| std::io::Error::other("prepared tier delete journal has no entity tag"))?;
    match config_boundary::delete_config_if_match(api, &name, &etag).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(Error::PreconditionFailed) => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "prepared tier delete journal changed before abort",
        )),
        Err(err) => Err(std::io::Error::other(err)),
    }
}

pub(crate) async fn enqueue_committed_tier_delete_journal_entry(je: &Jentry) -> std::io::Result<()> {
    let expiry_state = runtime_boundary::expiry_state_handle();
    expiry_state.write().await.enqueue_tier_journal_entry(je)
}

pub async fn remove_tier_delete_journal_entry<S>(api: Arc<S>, je: &Jentry) -> std::io::Result<()>
where
    S: ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    match config_boundary::delete_config(api, &tier_delete_journal_object_name(je)).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(err) => Err(std::io::Error::other(err)),
    }
}

pub async fn process_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    let journal_name = tier_delete_journal_object_name(je);
    // The lock lives at a synthetic namespace key so config I/O on the journal
    // object can still acquire its normal object lock. It serializes the
    // strong-read -> remote DELETE -> ETag-delete transaction across recovery
    // workers and nodes, avoiding avoidable duplicate backend calls.
    let recovery_lock_name = format!("{journal_name}.recovery-lock");
    let recovery_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &recovery_lock_name)
        .await
        .map_err(std::io::Error::other)?;
    let _recovery_guard = recovery_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await
        .map_err(std::io::Error::other)?;
    let (current, journal_etag) = read_tier_delete_journal_with_etag(api.clone(), &journal_name)
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "tier delete journal disappeared"))?;
    if !same_tier_delete_journal_identity(&current, je) {
        metrics::counter!(
            "rustfs_ilm_tier_delete_journal_quarantined_total",
            "reason" => "identity_mismatch"
        )
        .increment(1);
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "mismatched tier delete journal is quarantined",
        ));
    }
    match current.persisted_version {
        1 | TIER_DELETE_JOURNAL_VERSION => {
            metrics::counter!(
                "rustfs_ilm_tier_delete_journal_quarantined_total",
                "reason" => "legacy_unknown_version_state"
            )
            .increment(1);
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "v1/v2 tier delete journal with unknown remote version state is quarantined",
            ));
        }
        TIER_DELETE_JOURNAL_EXACT_VERSION | TIER_DELETE_JOURNAL_STATE_VERSION => {
            return process_committed_v3_v4_journal(api, current, journal_etag).await;
        }
        TIER_DELETE_JOURNAL_TRANSACTION_VERSION => {
            return process_v5_journal(api, current, journal_etag).await;
        }
        TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION => {}
        _ => {
            metrics::counter!(
                "rustfs_ilm_tier_delete_journal_quarantined_total",
                "reason" => "unsupported_version"
            )
            .increment(1);
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "unsupported tier delete journal version is quarantined",
            ));
        }
    }
    let manifest = load_manifest_for_journal(api.clone(), &journal_name, &current)
        .await
        .map_err(std::io::Error::other)?;
    match current.state {
        TierDeleteJournalState::Prepared => reconcile_prepared_v6_journal(api, current, journal_etag, manifest).await,
        TierDeleteJournalState::Dispatched => reconcile_dispatched_v6_journal(api, current, journal_etag, manifest).await,
        TierDeleteJournalState::Committed => process_committed_v6_journal(api, current, journal_etag, manifest).await,
    }
}

fn object_info_references_tier_delete(info: &ObjectInfo, je: &Jentry) -> std::io::Result<bool> {
    if info.transitioned_object.status != rustfs_filemeta::TRANSITION_COMPLETE
        || info.transitioned_object.name != je.obj_name
        || info.transitioned_object.tier != je.tier_name
    {
        return Ok(false);
    }
    let source_backend_identity = tier_destination_id_from_metadata(&info.user_defined)?;
    if source_backend_identity.is_some() && source_backend_identity != je.backend_identity {
        return Ok(false);
    }
    if !je.version_id_exact {
        return Ok(true);
    }
    Ok(match info.transition_version_state {
        rustfs_filemeta::TransitionVersionState::Unknown => true,
        rustfs_filemeta::TransitionVersionState::KnownDisabled => false,
        rustfs_filemeta::TransitionVersionState::SuspendedNull | rustfs_filemeta::TransitionVersionState::Exact => {
            info.transitioned_object.version_id == je.version_id
        }
    })
}

async fn tier_delete_has_durable_source_or_free_version(
    api: &ECStore,
    source: &TierDeleteSourceIdentity,
    je: &Jentry,
) -> std::io::Result<(bool, Vec<crate::store::ObjectLockDiagGuard>)> {
    let lock_object = rustfs_utils::path::encode_dir_object(&source.object);
    let mut lock_opts = ObjectOptions::default();
    let read_guards = api
        .acquire_all_physical_object_read_locks("tier_delete_journal_recovery", &source.bucket, &lock_object, &mut lock_opts)
        .await
        .map_err(std::io::Error::other)?;
    if api.ctx.lock_manager().is_disabled() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal recovery requires namespace locking",
        ));
    }
    let mut has_durable_owner = false;
    for pool in &api.pools {
        for set in &pool.disk_set {
            let versions = match set.load_file_info_versions_exact(&source.bucket, &source.object).await {
                Ok(Some(versions)) => versions,
                Ok(None) => continue,
                // A quorum-confirmed missing physical volume proves absence in
                // this set. Every other read/parse/quorum outcome is Unknown.
                Err(err) if is_err_strict_volume_not_found(&err) => continue,
                Err(err) => return Err(std::io::Error::other(err)),
            };
            for version in versions.versions.iter().chain(versions.free_versions.iter()) {
                let info = ObjectInfo::from_file_info(version, &source.bucket, &source.object, source.versioned);
                if object_info_references_tier_delete(&info, je)? {
                    has_durable_owner = true;
                    break;
                }
            }
            if has_durable_owner {
                break;
            }
        }
        if has_durable_owner {
            break;
        }
    }
    if read_guards.iter().any(crate::store::ObjectLockDiagGuard::is_lock_lost) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal recovery object read lock was lost",
        ));
    }
    Ok((has_durable_owner, read_guards))
}

async fn delete_remote_tier_journal_target_with_lease(
    journal: &Jentry,
    lease: &crate::services::tier::tier::TierOperationLease,
) -> std::io::Result<()> {
    let delete = async {
        if journal.version_id_exact {
            delete_confirmed_transition_candidate_exact_with_lease_idempotent(&journal.obj_name, &journal.version_id, lease).await
        } else {
            delete_object_from_remote_tier_with_lease_idempotent(&journal.obj_name, &journal.version_id, lease, false).await
        }
    };
    tokio::time::timeout(TIER_DELETE_REMOTE_DEADLINE, delete)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "remote tier delete exceeded its deadline"))??;
    Ok(())
}

async fn process_committed_v3_v4_journal(api: Arc<ECStore>, current: Jentry, etag: String) -> std::io::Result<()> {
    if current.state != TierDeleteJournalState::Committed
        || current.version_state == rustfs_filemeta::TransitionVersionState::Unknown
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "v3/v4 tier delete journal has no committed exact remote-version state",
        ));
    }
    let backend_identity = current
        .backend_identity
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v3/v4 journal has no backend identity"))?;
    let lease =
        TierConfigMgr::acquire_operation_lease_for_backend_identity(&api.tier_config_mgr(), &current.tier_name, backend_identity)
            .await
            .map_err(std::io::Error::other)?;
    delete_remote_tier_journal_target_with_lease(&current, &lease).await?;
    if !lease.is_current_generation() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "legacy tier generation changed after remote delete; journal retained",
        ));
    }

    let path = tier_delete_journal_object_name(&current);
    let (observed, observed_etag) = read_tier_delete_journal_with_etag(api.clone(), &path)
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "legacy journal disappeared"))?;
    if observed_etag != etag
        || observed.state != TierDeleteJournalState::Committed
        || !same_tier_delete_journal_identity(&observed, &current)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "legacy tier delete journal changed during remote deletion",
        ));
    }
    let observed_data = encode_tier_delete_journal_entry(&observed).map_err(std::io::Error::other)?;
    let fences_current = || lease.is_current_generation();
    delete_durable_config_if_match(api, &path, &observed_data, &observed_etag, &fences_current)
        .await
        .map_err(std::io::Error::other)
}

async fn process_v5_journal(api: Arc<ECStore>, mut current: Jentry, mut etag: String) -> std::io::Result<()> {
    let source = current
        .source
        .as_ref()
        .filter(|source| source.has_stable_identity())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v5 journal has no stable source identity"))?;
    let bucket_guard = api
        .acquire_bucket_lifecycle_read_lock(&source.bucket)
        .await
        .map_err(std::io::Error::other)?;
    let backend_identity = current
        .backend_identity
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v5 journal has no backend identity"))?;
    let lease =
        TierConfigMgr::acquire_operation_lease_for_backend_identity(&api.tier_config_mgr(), &current.tier_name, backend_identity)
            .await
            .map_err(std::io::Error::other)?;
    let (present, guards) = tier_delete_has_durable_source_or_free_version(&api, source, &current).await?;
    let fences_current =
        || !bucket_guard.is_lock_lost() && guards.iter().all(|guard| !guard.is_lock_lost()) && lease.is_current_generation();
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "v5 tier delete recovery fence changed",
        ));
    }

    match current.state {
        TierDeleteJournalState::Prepared if present => {
            let path = tier_delete_journal_object_name(&current);
            let result = config_boundary::delete_config_if_match(api, &path, &etag).await;
            if !fences_current() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "v5 prepared journal fence changed during abort",
                ));
            }
            return match result {
                Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
                Err(Error::PreconditionFailed) => Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "v5 prepared journal changed before abort",
                )),
                Err(err) => Err(std::io::Error::other(err)),
            };
        }
        TierDeleteJournalState::Dispatched | TierDeleteJournalState::Committed if present => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "v5 tier delete journal retains a durable source or free-version owner",
            ));
        }
        TierDeleteJournalState::Prepared | TierDeleteJournalState::Dispatched => {
            current.state = TierDeleteJournalState::Committed;
            let path = tier_delete_journal_object_name(&current);
            save_config_if_match_fenced(
                api.clone(),
                &path,
                encode_tier_delete_journal_entry(&current).map_err(std::io::Error::other)?,
                &etag,
                &fences_current,
            )
            .await
            .map_err(std::io::Error::other)?;
            let (observed, observed_etag) = read_tier_delete_journal_with_etag(api.clone(), &path)
                .await
                .map_err(std::io::Error::other)?
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "committed v5 journal disappeared"))?;
            if observed.state != TierDeleteJournalState::Committed || !same_tier_delete_journal_identity(&observed, &current) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "v5 journal changed after committed CAS",
                ));
            }
            current = observed;
            etag = observed_etag;
        }
        TierDeleteJournalState::Committed => {}
    }

    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "v5 tier delete fence changed before remote deletion",
        ));
    }
    delete_remote_tier_journal_target_with_lease(&current, &lease).await?;
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "v5 tier delete fence changed after remote deletion; journal retained",
        ));
    }
    let path = tier_delete_journal_object_name(&current);
    let (observed, observed_etag) = read_tier_delete_journal_with_etag(api.clone(), &path)
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "committed v5 journal disappeared"))?;
    if observed_etag != etag
        || observed.state != TierDeleteJournalState::Committed
        || !same_tier_delete_journal_identity(&observed, &current)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "v5 tier delete journal changed during remote deletion",
        ));
    }
    let observed_data = encode_tier_delete_journal_entry(&observed).map_err(std::io::Error::other)?;
    delete_durable_config_if_match(api, &path, &observed_data, &observed_etag, &fences_current)
        .await
        .map_err(std::io::Error::other)
}

async fn load_manifest_for_journal(
    api: Arc<ECStore>,
    journal_name: &str,
    journal: &Jentry,
) -> Result<TierDeleteDispatchManifest> {
    let binding = journal
        .dispatch
        .as_ref()
        .ok_or_else(|| Error::other("tier delete journal v6 is missing its dispatch binding"))?;
    let (manifest, _) = read_tier_delete_dispatch_manifest(api, &binding.manifest_object)
        .await?
        .ok_or_else(|| Error::other("tier delete dispatch manifest is missing"))?;
    validate_bound_journal(&manifest, journal_name, journal)?;
    if manifest.journal_names.binary_search(&journal_name.to_string()).is_err() {
        return Err(Error::other("tier delete dispatch manifest does not contain its journal"));
    }
    Ok(manifest)
}

fn acquire_matching_manifest_fleet_proof(
    manifest: &TierDeleteDispatchManifest,
) -> std::io::Result<TierDeleteJournalFleetProofToken> {
    let proof = acquire_tier_delete_journal_fleet_proof().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::WouldBlock, "tier delete journal v6 fleet capability is unavailable")
    })?;
    if tier_delete_journal_topology_generation(&proof) != manifest.topology_generation {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal topology generation changed",
        ));
    }
    Ok(proof)
}

async fn reconcile_prepared_v6_journal(
    _api: Arc<ECStore>,
    _current: Jentry,
    _etag: String,
    manifest: TierDeleteDispatchManifest,
) -> std::io::Result<()> {
    // A Prepared journal is negative authorization evidence. Only the
    // manifest coordinator, while holding the bucket WRITE lock, operation
    // lock, and matching fleet proof, may seal rollback and remove the full
    // immutable journal set. A single-journal worker must never punch a hole
    // in that set even when it observes Aborting/Aborted.
    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        format!("prepared tier delete journal is owned by the {:?} manifest coordinator", manifest.state),
    ))
}

async fn reconcile_dispatched_v6_journal(
    api: Arc<ECStore>,
    current: Jentry,
    etag: String,
    manifest: TierDeleteDispatchManifest,
) -> std::io::Result<()> {
    if !matches!(
        manifest.state,
        TierDeleteDispatchManifestState::DispatchAuthorized | TierDeleteDispatchManifestState::Completed
    ) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "dispatched tier delete journal has no authorized manifest",
        ));
    }
    let fleet_proof = acquire_matching_manifest_fleet_proof(&manifest)?;
    let source = current
        .source
        .as_ref()
        .filter(|source| source.has_stable_identity())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v6 journal has no stable source identity"))?;
    let bucket_guard = api
        .acquire_bucket_lifecycle_read_lock(&source.bucket)
        .await
        .map_err(std::io::Error::other)?;
    let backend_identity = current
        .backend_identity
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v6 journal has no backend identity"))?;
    let lease =
        TierConfigMgr::acquire_operation_lease_for_backend_identity(&api.tier_config_mgr(), &current.tier_name, backend_identity)
            .await
            .map_err(std::io::Error::other)?;
    let (present, guards) = tier_delete_has_durable_source_or_free_version(&api, source, &current).await?;
    let fences_current = || journal_remote_fences_current(&bucket_guard, &guards, &lease, &fleet_proof, &manifest);
    if present || !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "dispatched tier delete journal still has a source, free-version, or invalid fence",
        ));
    }
    record_tier_delete_journal_progress_fenced(
        api.clone(),
        &tier_delete_journal_object_name(&current),
        &current,
        &fences_current,
    )
    .await
    .map_err(std::io::Error::other)?;
    let mut committed = current.clone();
    committed.state = TierDeleteJournalState::Committed;
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "dispatched tier delete journal fence changed before commit",
        ));
    }
    let commit = save_config_if_match_fenced(
        api.clone(),
        &tier_delete_journal_object_name(&committed),
        encode_tier_delete_journal_entry(&committed).map_err(std::io::Error::other)?,
        &etag,
        &fences_current,
    )
    .await;
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "dispatched tier delete journal fence changed during commit; durable state retained",
        ));
    }
    commit.map_err(std::io::Error::other)?;
    drop(guards);
    drop(lease);
    drop(bucket_guard);
    // The journal CAS is complete, but manifest completion belongs exclusively
    // to the coordinator with its operation lock and bucket WRITE fence.
    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "committed tier delete journal waits for manifest coordinator completion",
    ))
}

fn journal_remote_fences_current(
    bucket_guard: &rustfs_lock::NamespaceLockGuard,
    guards: &[crate::store::ObjectLockDiagGuard],
    lease: &crate::services::tier::tier::TierOperationLease,
    fleet_proof: &TierDeleteJournalFleetProofToken,
    manifest: &TierDeleteDispatchManifest,
) -> bool {
    !bucket_guard.is_lock_lost()
        && guards.iter().all(|guard| !guard.is_lock_lost())
        && lease.is_current_generation()
        && tier_delete_journal_fleet_proof_matches(fleet_proof)
        && tier_delete_journal_topology_generation(fleet_proof) == manifest.topology_generation
}

async fn process_committed_v6_journal(
    api: Arc<ECStore>,
    current: Jentry,
    etag: String,
    manifest: TierDeleteDispatchManifest,
) -> std::io::Result<()> {
    if manifest.state != TierDeleteDispatchManifestState::Completed {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "committed tier delete journal waits for its completed manifest",
        ));
    }
    let fleet_proof = acquire_matching_manifest_fleet_proof(&manifest)?;
    let source = current
        .source
        .as_ref()
        .filter(|source| source.has_stable_identity())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v6 journal has no stable source identity"))?;
    let bucket_guard = api
        .acquire_bucket_lifecycle_read_lock(&source.bucket)
        .await
        .map_err(std::io::Error::other)?;
    let backend_identity = current
        .backend_identity
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "v6 journal has no backend identity"))?;
    let lease =
        TierConfigMgr::acquire_operation_lease_for_backend_identity(&api.tier_config_mgr(), &current.tier_name, backend_identity)
            .await
            .map_err(std::io::Error::other)?;
    let (present, guards) = tier_delete_has_durable_source_or_free_version(&api, source, &current).await?;
    let fences_current = || journal_remote_fences_current(&bucket_guard, &guards, &lease, &fleet_proof, &manifest);
    if present || !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "committed tier delete journal has a durable source owner or invalid fence",
        ));
    }
    record_tier_delete_journal_progress_fenced(
        api.clone(),
        &tier_delete_journal_object_name(&current),
        &current,
        &fences_current,
    )
    .await
    .map_err(std::io::Error::other)?;

    let remote_delete = async {
        if current.version_id_exact {
            delete_confirmed_transition_candidate_exact_with_lease_idempotent(&current.obj_name, &current.version_id, &lease)
                .await
        } else {
            delete_object_from_remote_tier_with_lease_idempotent(&current.obj_name, &current.version_id, &lease, false).await
        }
    };
    tokio::time::timeout(TIER_DELETE_REMOTE_DEADLINE, remote_delete)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "remote tier delete exceeded its deadline"))??;
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete fence changed after remote deletion; durable journal retained",
        ));
    }
    let path = tier_delete_journal_object_name(&current);
    let (observed, observed_etag) = read_tier_delete_journal_with_etag(api.clone(), &path)
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "committed journal disappeared"))?;
    if observed_etag != etag
        || observed.state != TierDeleteJournalState::Committed
        || !same_tier_delete_journal_identity(&observed, &current)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "committed tier delete journal changed during remote deletion",
        ));
    }
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete fence changed before committed journal cleanup; durable journal retained",
        ));
    }
    let observed_data = encode_tier_delete_journal_entry(&observed).map_err(std::io::Error::other)?;
    match delete_durable_config_if_match(api.clone(), &path, &observed_data, &observed_etag, &fences_current).await {
        Ok(()) | Err(Error::ConfigNotFound) => {}
        Err(err) => return Err(std::io::Error::other(err)),
    }
    if !fences_current() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete fence changed during committed journal cleanup",
        ));
    }
    drop(guards);
    drop(lease);
    drop(bucket_guard);
    // Completed manifest GC belongs to the manifest coordinator so a stale
    // journal worker cannot erase the last durable operation record.
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TierDeleteDispatchManifestRecoveryOutcome {
    Advanced,
    Deleted,
    Retained,
}

fn manifest_recovery_fences_current(
    bucket_guard: &rustfs_lock::NamespaceLockGuard,
    operation_guard: &rustfs_lock::NamespaceLockGuard,
    fleet_proof: &TierDeleteJournalFleetProofToken,
    manifest: &TierDeleteDispatchManifest,
) -> bool {
    !bucket_guard.is_lock_lost()
        && !operation_guard.is_lock_lost()
        && tier_delete_journal_fleet_proof_matches(fleet_proof)
        && tier_delete_journal_topology_generation(fleet_proof) == manifest.topology_generation
}

async fn cas_tier_delete_dispatch_manifest_state(
    api: Arc<ECStore>,
    manifest_name: &str,
    expected: &TierDeleteDispatchManifest,
    expected_etag: &str,
    next_state: TierDeleteDispatchManifestState,
    fences_current: &impl Fn() -> bool,
) -> Result<(TierDeleteDispatchManifest, String)> {
    if !fences_current() {
        return Err(Error::other(
            "tier delete dispatch manifest recovery fence changed before state transition",
        ));
    }
    let mut next = expected.clone();
    next.state = next_state;
    let write = save_config_if_match_fenced(
        api.clone(),
        manifest_name,
        encode_tier_delete_dispatch_manifest(&next)?,
        expected_etag,
        fences_current,
    )
    .await;
    let observed = read_tier_delete_dispatch_manifest(api, manifest_name).await?;
    if !fences_current() {
        return Err(Error::other(
            "tier delete dispatch manifest recovery fence changed during state transition",
        ));
    }
    match observed {
        Some((observed, etag)) if observed == next => Ok((observed, etag)),
        _ => match write {
            Ok(()) | Err(Error::PreconditionFailed) => {
                Err(Error::other("tier delete dispatch manifest changed during state transition"))
            }
            Err(err) => Err(err),
        },
    }
}

async fn delete_tier_delete_dispatch_manifest_if_match_confirmed(
    api: Arc<ECStore>,
    manifest_name: &str,
    manifest: &TierDeleteDispatchManifest,
    etag: &str,
    fences_current: &impl Fn() -> bool,
) -> Result<()> {
    if !fences_current() {
        return Err(Error::other("tier delete dispatch manifest recovery fence changed before deletion"));
    }
    let data = encode_tier_delete_dispatch_manifest(manifest)?;
    let delete = delete_durable_config_if_match(api.clone(), manifest_name, &data, etag, fences_current).await;
    let observed = read_tier_delete_dispatch_manifest(api, manifest_name).await?;
    if !fences_current() {
        return Err(Error::other("tier delete dispatch manifest recovery fence changed during deletion"));
    }
    match observed {
        None => Ok(()),
        Some((observed, _)) => Err(Error::other_with_context(
            "tier delete dispatch manifest changed during deletion",
            format!("observed {:?}, delete result {:?}", observed.state, delete.as_ref().err()),
        )),
    }
}

async fn authorized_dispatch_all_committed(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    fences_current: &impl Fn() -> bool,
) -> Result<bool> {
    for name in &manifest.journal_names {
        let (entry, _) = read_tier_delete_journal_with_etag(api.clone(), name)
            .await?
            .ok_or_else(|| Error::other("authorized tier delete dispatch manifest references a missing journal"))?;
        validate_bound_journal(manifest, name, &entry)?;
        record_tier_delete_journal_progress_fenced(api.clone(), name, &entry, fences_current).await?;
        match entry.state {
            TierDeleteJournalState::Committed => {}
            TierDeleteJournalState::Dispatched => return Ok(false),
            TierDeleteJournalState::Prepared => {
                return Err(Error::other(
                    "authorized tier delete dispatch contains a journal that was never dispatched",
                ));
            }
        }
    }
    Ok(true)
}

async fn completed_dispatch_has_present_journal(
    api: Arc<ECStore>,
    manifest: &TierDeleteDispatchManifest,
    fences_current: &impl Fn() -> bool,
) -> Result<bool> {
    for name in &manifest.journal_names {
        let Some((entry, _)) = read_tier_delete_journal_with_etag(api.clone(), name).await? else {
            continue;
        };
        validate_bound_journal(manifest, name, &entry)?;
        record_tier_delete_journal_progress_fenced(api.clone(), name, &entry, fences_current).await?;
        if entry.state != TierDeleteJournalState::Committed {
            return Err(Error::other("completed tier delete dispatch contains an uncommitted journal"));
        }
        return Ok(true);
    }
    Ok(false)
}

async fn process_tier_delete_dispatch_manifest(
    api: Arc<ECStore>,
    manifest_name: &str,
    observed_before_lock: &TierDeleteDispatchManifest,
) -> Result<TierDeleteDispatchManifestRecoveryOutcome> {
    // A stale/missing capability may inspect records but must never mutate or
    // delete v6 evidence. The same proof is revalidated at every write edge.
    let fleet_proof = acquire_matching_manifest_fleet_proof(observed_before_lock).map_err(Error::other)?;
    let bucket_guard = api.acquire_bucket_lifecycle_write_lock(&observed_before_lock.bucket).await?;
    let operation_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &tier_delete_dispatch_operation_lock_name(manifest_name))
        .await?;
    let operation_guard = operation_lock
        .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let (mut current, mut etag) = read_tier_delete_dispatch_manifest(api.clone(), manifest_name)
        .await?
        .ok_or_else(|| Error::ConfigNotFound)?;
    if current.bucket != observed_before_lock.bucket
        || !manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current)
    {
        return Err(Error::other("tier delete dispatch manifest recovery fence changed"));
    }
    {
        let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
        record_tier_delete_dispatch_manifest_progress_fenced(api.clone(), manifest_name, &current, &fences_current).await?;
    }

    if current.state == TierDeleteDispatchManifestState::Preparing {
        let next = {
            let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
            cas_tier_delete_dispatch_manifest_state(
                api.clone(),
                manifest_name,
                &current,
                &etag,
                TierDeleteDispatchManifestState::Aborting,
                &fences_current,
            )
            .await?
        };
        (current, etag) = next;
    }

    if matches!(
        current.state,
        TierDeleteDispatchManifestState::Aborting | TierDeleteDispatchManifestState::Aborted
    ) {
        {
            let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
            delete_staged_dispatch_journal_set(api.clone(), &current, &fences_current).await?;
        }
        if current.state == TierDeleteDispatchManifestState::Aborting {
            let next = {
                let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
                cas_tier_delete_dispatch_manifest_state(
                    api.clone(),
                    manifest_name,
                    &current,
                    &etag,
                    TierDeleteDispatchManifestState::Aborted,
                    &fences_current,
                )
                .await?
            };
            (current, etag) = next;
        }
        if !manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current) {
            return Err(Error::other("tier delete dispatch manifest recovery fence changed"));
        }
        validate_staged_dispatch_journal_set(api.clone(), &current).await?;
        let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
        delete_tier_delete_dispatch_manifest_if_match_confirmed(api, manifest_name, &current, &etag, &fences_current).await?;
        return Ok(TierDeleteDispatchManifestRecoveryOutcome::Deleted);
    }

    if current.state == TierDeleteDispatchManifestState::DispatchAuthorized {
        let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
        if !authorized_dispatch_all_committed(api.clone(), &current, &fences_current).await? {
            return Ok(TierDeleteDispatchManifestRecoveryOutcome::Retained);
        }
        let next = {
            let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
            cas_tier_delete_dispatch_manifest_state(
                api.clone(),
                manifest_name,
                &current,
                &etag,
                TierDeleteDispatchManifestState::Completed,
                &fences_current,
            )
            .await?
        };
        (current, etag) = next;
        if !current.journal_names.is_empty() {
            return Ok(TierDeleteDispatchManifestRecoveryOutcome::Advanced);
        }
    }

    if current.state != TierDeleteDispatchManifestState::Completed {
        return Err(Error::other("tier delete dispatch manifest has an unsupported recovery state"));
    }
    let fences_current = || manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current);
    if completed_dispatch_has_present_journal(api.clone(), &current, &fences_current).await? {
        return Ok(TierDeleteDispatchManifestRecoveryOutcome::Retained);
    }
    if !manifest_recovery_fences_current(&bucket_guard, &operation_guard, &fleet_proof, &current) {
        return Err(Error::other("tier delete dispatch manifest recovery fence changed"));
    }
    delete_tier_delete_dispatch_manifest_if_match_confirmed(api, manifest_name, &current, &etag, &fences_current).await?;
    Ok(TierDeleteDispatchManifestRecoveryOutcome::Deleted)
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn recover_test_tier_delete_dispatch_manifest(api: Arc<ECStore>, manifest_name: &str) -> Result<()> {
    let (manifest, _) = read_tier_delete_dispatch_manifest(api.clone(), manifest_name)
        .await?
        .ok_or(Error::ConfigNotFound)?;
    process_tier_delete_dispatch_manifest(api, manifest_name, &manifest)
        .await
        .map(|_| ())
}

pub async fn recover_tier_delete_dispatch_manifests(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> Result<TierDeleteDispatchManifestRecoveryStats> {
    if limit == 0 {
        return Err(Error::other("tier delete dispatch manifest recovery limit must be greater than zero"));
    }
    let list = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
            marker,
            None,
            i32::try_from(limit).unwrap_or(i32::MAX),
            false,
            None,
            false,
        )
        .await?;
    let mut stats = TierDeleteDispatchManifestRecoveryStats {
        scanned: 0,
        advanced: 0,
        deleted: 0,
        retained: 0,
        failed: 0,
        next_marker: list.next_continuation_token,
        truncated: list.is_truncated,
    };
    for object in list.objects {
        stats.scanned += 1;
        let data = match config_boundary::read_config(api.clone(), &object.name).await {
            Ok(data) => data,
            Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => continue,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    manifest_object = %object.name,
                    error = ?err,
                    "Failed to read tier delete dispatch manifest"
                );
                continue;
            }
        };
        let manifest = match decode_tier_delete_dispatch_manifest(&data, &object.name) {
            Ok(manifest) => manifest,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    manifest_object = %object.name,
                    error = ?err,
                    "Invalid tier delete dispatch manifest is quarantined"
                );
                continue;
            }
        };
        match process_tier_delete_dispatch_manifest(api.clone(), &object.name, &manifest).await {
            Ok(TierDeleteDispatchManifestRecoveryOutcome::Advanced) => stats.advanced += 1,
            Ok(TierDeleteDispatchManifestRecoveryOutcome::Deleted) => stats.deleted += 1,
            Ok(TierDeleteDispatchManifestRecoveryOutcome::Retained) => stats.retained += 1,
            Err(Error::ConfigNotFound) => {}
            Err(err) => {
                stats.failed += 1;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    manifest_object = %object.name,
                    operation_id = %manifest.operation_id,
                    state = ?manifest.state,
                    error = ?err,
                    "Tier delete dispatch manifest recovery will retry later"
                );
            }
        }
    }
    Ok(stats)
}

pub async fn recover_tier_delete_journal_entries(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> Result<TierDeleteJournalRecoveryStats> {
    if limit == 0 {
        return Err(Error::other("tier delete journal recovery limit must be greater than zero"));
    }

    let list = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            TIER_DELETE_JOURNAL_PREFIX,
            marker.clone(),
            None,
            i32::try_from(limit).unwrap_or(i32::MAX),
            false,
            None,
            false,
        )
        .await?;

    let mut stats = TierDeleteJournalRecoveryStats {
        scanned: 0,
        deleted: 0,
        failed: 0,
        next_marker: list.next_continuation_token,
        truncated: list.is_truncated,
    };

    for object in list.objects {
        stats.scanned += 1;
        let data = match config_boundary::read_config(api.clone(), &object.name).await {
            Ok(data) => data,
            Err(Error::ConfigNotFound) => continue,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    error = ?err,
                    "Failed to read tier delete journal entry"
                );
                continue;
            }
        };

        let je = match decode_tier_delete_journal_entry(&data) {
            Ok(je) => je,
            Err(err) => {
                stats.failed += 1;
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    error = ?err,
                    "Failed to decode tier delete journal entry"
                );
                continue;
            }
        };

        if tier_delete_journal_object_name(&je) != object.name {
            stats.failed += 1;
            warn!(
                event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                journal_object = %object.name,
                "Tier delete journal content does not match its object name and will be retained"
            );
            continue;
        }

        if je.backend_identity.is_none() {
            stats.failed += 1;
            warn!(
                event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                journal_object = %object.name,
                remote_object = %je.obj_name,
                remote_version_id = %je.version_id,
                tier = %je.tier_name,
                "Legacy tier delete journal entry has no durable backend identity and will be retained"
            );
            continue;
        }

        match process_tier_delete_journal_entry(api.clone(), &je).await {
            Ok(()) => stats.deleted += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Another recovery worker may have completed the same journal
                // after this page was listed but before we acquired its
                // recovery lock. The durable obligation has converged.
            }
            Err(err) => {
                stats.failed += 1;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    journal_object = %object.name,
                    remote_object = %je.obj_name,
                    remote_version_id = %je.version_id,
                    tier = %je.tier_name,
                    error = ?err,
                    "Tier delete journal recovery will retry later"
                );
            }
        }
    }

    Ok(stats)
}

pub async fn run_tier_delete_journal_recovery_loop(api: Arc<ECStore>, cancel_token: CancellationToken) {
    let mut interval = tokio::time::interval(TIER_DELETE_JOURNAL_RECOVERY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut marker: Option<String> = None;
    let mut manifest_marker: Option<String> = None;

    loop {
        #[cfg(test)]
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {},
            _ = api.ctx.wait_for_tier_delete_journal_recovery() => {},
        }
        #[cfg(not(test))]
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {},
        }

        let manifest_recovery = recover_tier_delete_dispatch_manifests(
            api.clone(),
            DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT,
            manifest_marker.clone(),
        );
        let Some(manifest_result) =
            await_tier_delete_journal_recovery(&cancel_token, TIER_DELETE_JOURNAL_RECOVERY_TIMEOUT, manifest_recovery).await
        else {
            return;
        };
        match manifest_result {
            Ok(stats) => {
                manifest_marker = stats.next_marker;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    scanned = stats.scanned,
                    advanced = stats.advanced,
                    deleted = stats.deleted,
                    retained = stats.retained,
                    failed = stats.failed,
                    truncated = stats.truncated,
                    next_marker = ?manifest_marker,
                    "Reconciled tier delete dispatch manifests"
                );
            }
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    next_marker = ?manifest_marker,
                    error = ?err,
                    "Failed to recover tier delete dispatch manifests"
                );
            }
        }

        let recovery =
            recover_tier_delete_journal_entries(api.clone(), DEFAULT_TIER_DELETE_JOURNAL_RECOVERY_LIMIT, marker.clone());
        let Some(result) =
            await_tier_delete_journal_recovery(&cancel_token, TIER_DELETE_JOURNAL_RECOVERY_TIMEOUT, recovery).await
        else {
            return;
        };
        match result {
            Ok(stats) => {
                marker = stats.next_marker;
                debug!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    scanned = stats.scanned,
                    deleted = stats.deleted,
                    failed = stats.failed,
                    truncated = stats.truncated,
                    next_marker = ?marker,
                    "Recovered tier delete journal tasks"
                );
            }
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_TIER_DELETE_JOURNAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    next_marker = ?marker,
                    error = ?err,
                    "Failed to recover tier delete journal tasks"
                );
            }
        }
    }
}

async fn await_tier_delete_journal_recovery<T, F>(
    cancel_token: &CancellationToken,
    timeout: Duration,
    recovery: F,
) -> Option<Result<T>>
where
    F: Future<Output = Result<T>>,
{
    tokio::select! {
        _ = cancel_token.cancelled() => None,
        result = tokio::time::timeout(timeout, recovery) => Some(match result {
            Ok(result) => result,
            Err(_) => Err(Error::other(format!(
                "tier delete journal recovery timed out after {} seconds",
                timeout.as_secs()
            ))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TIER_DELETE_JOURNAL_EXACT_VERSION, TIER_DELETE_JOURNAL_LEGACY_PREFIX, TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION,
        TIER_DELETE_JOURNAL_STATE_VERSION, TIER_DELETE_JOURNAL_TRANSACTION_VERSION, TIER_DELETE_JOURNAL_V6_PREFIX,
        await_tier_delete_journal_recovery, decode_tier_delete_journal_entry, encode_tier_delete_journal_entry,
        object_info_references_tier_delete, record_tier_delete_journal_backend_identity, tier_delete_journal_object_name,
    };
    use crate::bucket::lifecycle::tier_sweeper::{
        Jentry, TierDeleteDispatchBinding, TierDeleteJournalState, TierDeleteSourceIdentity,
    };
    use crate::error::Result;
    use crate::object_api::ObjectInfo;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn journal_entry() -> Jentry {
        Jentry {
            persisted_version: 0,
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([7; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        }
    }

    fn bound_v6_journal_entry(state: TierDeleteJournalState) -> Jentry {
        let operation_id = uuid::Uuid::new_v4();
        Jentry {
            persisted_version: 0,
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([7; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state,
            source: Some(TierDeleteSourceIdentity {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: Some("version".to_string()),
                versioned: true,
                version_suspended: false,
                data_dir: Some("data-dir".to_string()),
                etag: Some("etag".to_string()),
                mod_time: Some("mod-time".to_string()),
            }),
            dispatch: Some(TierDeleteDispatchBinding {
                operation_id,
                manifest_object: "ilm/tier-delete-dispatch-manifests/manifest.json".to_string(),
                journal_set_sha256: "7".repeat(64),
            }),
        }
    }

    #[test]
    fn tier_delete_journal_roundtrips_entry() {
        let je = journal_entry();

        let encoded = encode_tier_delete_journal_entry(&je).expect("journal entry should encode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("journal entry should decode");

        assert_eq!(decoded.obj_name, je.obj_name);
        assert_eq!(decoded.version_id, je.version_id);
        assert_eq!(decoded.tier_name, je.tier_name);
        assert_eq!(decoded.backend_identity, je.backend_identity);
        assert_eq!(decoded.version_id_exact, je.version_id_exact);
        assert_eq!(decoded.version_state, je.version_state);
    }

    #[test]
    fn tier_delete_journal_object_name_binds_persisted_content() {
        let original = journal_entry();
        let original_name = tier_delete_journal_object_name(&original);
        let mut replaced = original;
        replaced.obj_name = "remote/replaced".to_string();

        assert_ne!(tier_delete_journal_object_name(&replaced), original_name);
    }

    #[test]
    fn tier_delete_v6_dispatch_roundtrips_bound_source_identity() {
        let je = bound_v6_journal_entry(TierDeleteJournalState::Prepared);

        let encoded = encode_tier_delete_journal_entry(&je).expect("prepared transaction should encode");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("transaction should be JSON");
        assert_eq!(value["version"], serde_json::json!(TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION));
        assert_eq!(value["state"], serde_json::json!("Prepared"));
        assert!(value["source"].is_object());

        let decoded = decode_tier_delete_journal_entry(&encoded).expect("prepared transaction should decode");
        assert_eq!(decoded.persisted_version, TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION);
        assert_eq!(decoded.state, TierDeleteJournalState::Prepared);
        assert_eq!(decoded.source, je.source);
        assert_eq!(decoded.dispatch, je.dispatch);
    }

    #[test]
    fn v6_operation_path_isolated_from_v5_and_successor_operations() {
        let first = bound_v6_journal_entry(TierDeleteJournalState::Prepared);
        let first_path = tier_delete_journal_object_name(&first);
        let first_operation = first.dispatch.as_ref().expect("v6 entry should be bound").operation_id;
        assert!(first_path.starts_with(&format!("{TIER_DELETE_JOURNAL_V6_PREFIX}{}/", first_operation.simple())));

        let mut legacy = first.clone();
        legacy.persisted_version = TIER_DELETE_JOURNAL_TRANSACTION_VERSION;
        legacy.dispatch = None;
        let legacy_path = tier_delete_journal_object_name(&legacy);
        assert!(legacy_path.starts_with(TIER_DELETE_JOURNAL_LEGACY_PREFIX));
        assert_ne!(legacy_path, first_path, "an old v5 writer must not address a v6 record");

        let mut successor = first;
        successor
            .dispatch
            .as_mut()
            .expect("successor should remain bound")
            .operation_id = uuid::Uuid::new_v4();
        let successor_path = tier_delete_journal_object_name(&successor);
        assert_ne!(
            successor_path, first_path,
            "a retry after terminal rollback must not reuse the previous operation receipt key"
        );
    }

    #[test]
    fn sole_owner_transaction_is_rejected_by_previous_v5_reader_and_v5_remains_readable() {
        let je = bound_v6_journal_entry(TierDeleteJournalState::Prepared);

        let encoded = encode_tier_delete_journal_entry(&je).expect("sole-owner transaction should encode");
        let mut persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("transaction should be JSON");
        let written_version = persisted["version"].as_u64().expect("journal version should be numeric");
        assert_eq!(written_version, u64::from(TIER_DELETE_JOURNAL_SOLE_OWNER_VERSION));
        assert!(
            written_version > 5,
            "the previous v5 reader must reject sole-owner records before remote cleanup"
        );
        let previous: super::PersistedTierDeleteJournalEntry =
            serde_json::from_slice(&encoded).expect("the previous reader should parse the shared JSON shape");
        let previous_decode = if previous.version > super::TIER_DELETE_JOURNAL_TRANSACTION_VERSION {
            Err(format!("unsupported tier delete journal version {}", previous.version))
        } else {
            previous.into_jentry().map_err(|err| err.to_string())
        };
        assert_eq!(
            previous_decode.expect_err("the previous v5 decoder must fail closed on v6"),
            "unsupported tier delete journal version 6"
        );

        persisted["version"] = serde_json::json!(5);
        persisted
            .as_object_mut()
            .expect("journal must be an object")
            .remove("dispatch");
        let legacy_v5 = serde_json::to_vec(&persisted).expect("v5 compatibility fixture should encode");
        let decoded = decode_tier_delete_journal_entry(&legacy_v5).expect("the new reader must retain v5 compatibility");
        assert_eq!(decoded.persisted_version, TIER_DELETE_JOURNAL_TRANSACTION_VERSION);
        assert_eq!(decoded.state, TierDeleteJournalState::Prepared);
        assert_eq!(decoded.source, je.source);
        let checkpointed = encode_tier_delete_journal_entry(&decoded).expect("compatibility tooling may checkpoint v5");
        let checkpointed: serde_json::Value =
            serde_json::from_slice(&checkpointed).expect("checkpointed transaction should be JSON");
        assert_eq!(checkpointed["version"], serde_json::json!(TIER_DELETE_JOURNAL_TRANSACTION_VERSION));
        assert!(checkpointed.get("dispatch").is_none());
    }

    #[test]
    fn prepared_recovery_blocks_any_live_reference_to_the_remote_version() {
        let je = journal_entry();
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(je.backend_identity.expect("test journal should bind a backend")),
        );
        let mut info = ObjectInfo {
            user_defined: std::sync::Arc::new(metadata),
            transitioned_object: crate::storage_api_contracts::lifecycle::TransitionedObject {
                name: je.obj_name.clone(),
                version_id: je.version_id.clone(),
                tier: je.tier_name.clone(),
                status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };

        assert!(object_info_references_tier_delete(&info, &je).expect("matching reference should be valid"));
        info.transitioned_object.version_id = "other-version".to_string();
        assert!(!object_info_references_tier_delete(&info, &je).expect("different exact version should be valid"));
        info.transition_version_state = rustfs_filemeta::TransitionVersionState::Unknown;
        assert!(
            object_info_references_tier_delete(&info, &je).expect("legacy unknown reference should fail closed"),
            "an unknown live source may still reference the journaled remote version"
        );
    }

    #[test]
    fn tier_delete_journal_roundtrips_exact_put_response_constraint() {
        let mut exact = journal_entry();
        exact.version_id = uuid::Uuid::nil().to_string();
        exact.version_id_exact = true;
        let mut normalized = exact.clone();
        normalized.version_id_exact = false;

        let encoded = encode_tier_delete_journal_entry(&exact).expect("exact journal entry should encode");
        let persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("exact journal JSON should decode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("exact journal entry should decode");

        assert_eq!(persisted["version"], TIER_DELETE_JOURNAL_STATE_VERSION);
        assert_eq!(persisted["version_id_exact"], true);
        assert!(decoded.version_id_exact);
        assert_ne!(tier_delete_journal_object_name(&exact), tier_delete_journal_object_name(&normalized));
    }

    #[test]
    fn tier_delete_journal_rejects_invalid_exact_version_constraints() {
        let identity = vec![7_u8; 32];
        let invalid = [
            serde_json::json!({
                "version": 1,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": 2,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": true,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": false,
            }),
            serde_json::json!({
                "version": TIER_DELETE_JOURNAL_EXACT_VERSION,
                "obj_name": "remote/object",
                "version_id": "exact-version",
                "tier_name": "WARM",
                "version_id_exact": true,
            }),
        ];

        for persisted in invalid {
            let encoded = serde_json::to_vec(&persisted).expect("invalid journal fixture should encode");
            decode_tier_delete_journal_entry(&encoded).expect_err("invalid exact journal constraint must fail closed");
        }
    }

    #[test]
    fn tier_delete_journal_rejects_conflicting_v4_version_states() {
        let identity = vec![7_u8; 32];
        let invalid = [
            ("known-disabled", "unexpected", false),
            ("suspended-null", "", true),
            ("suspended-null", "null", false),
            ("exact", "", true),
            ("exact", "null", true),
            ("exact", "version", false),
            ("unknown", "version", true),
        ];

        for (state, version_id, exact) in invalid {
            let persisted = serde_json::json!({
                "version": TIER_DELETE_JOURNAL_STATE_VERSION,
                "obj_name": "remote/object",
                "version_id": version_id,
                "tier_name": "WARM",
                "backend_identity": identity,
                "version_id_exact": exact.then_some(true),
                "version_state": state,
            });
            let encoded = serde_json::to_vec(&persisted).expect("invalid journal fixture should encode");
            decode_tier_delete_journal_entry(&encoded).expect_err("conflicting v4 version state must fail closed");
        }
    }

    #[test]
    fn legacy_journals_decode_with_unknown_version_state() {
        let v1 = br#"{"version":1,"obj_name":"remote/object","version_id":"opaque","tier_name":"WARM"}"#;
        let v2 = br#"{"version":2,"obj_name":"remote/object","version_id":"opaque","tier_name":"WARM","backend_identity":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}"#;

        for payload in [v1.as_slice(), v2.as_slice()] {
            let decoded = decode_tier_delete_journal_entry(payload).expect("legacy journal should decode");
            assert_eq!(decoded.version_state, rustfs_filemeta::TransitionVersionState::Unknown);
            assert!(!decoded.version_id_exact);
        }
    }

    #[test]
    fn tier_delete_journal_path_is_stable_and_sanitized() {
        let je = journal_entry();

        let first = tier_delete_journal_object_name(&je);
        let second = tier_delete_journal_object_name(&je);

        assert_eq!(first, second);
        assert!(first.starts_with("ilm/tier-delete-journal/"));
        assert!(first.ends_with(".json"));
        assert!(!first.contains("remote/object"));
    }

    #[test]
    fn tier_delete_journal_paths_separate_legacy_and_backend_identities() {
        let mut legacy = journal_entry();
        legacy.backend_identity = None;
        legacy.version_id_exact = false;
        legacy.version_state = rustfs_filemeta::TransitionVersionState::Unknown;
        let mut backend_a = journal_entry();
        backend_a.backend_identity = Some([1; 32]);
        let mut backend_b = journal_entry();
        backend_b.backend_identity = Some([2; 32]);

        assert_eq!(
            tier_delete_journal_object_name(&legacy),
            "ilm/tier-delete-journal/5ba6a7eb6338412b771613a6845a42ae5b8e26b5d201323eb01b38c5b42ff300.json"
        );
        assert_ne!(tier_delete_journal_object_name(&legacy), tier_delete_journal_object_name(&backend_a));
        assert_ne!(tier_delete_journal_object_name(&backend_a), tier_delete_journal_object_name(&backend_b));
    }

    #[test]
    fn tier_delete_journal_v2_requires_backend_identity() {
        let payload = br#"{"version":2,"obj_name":"remote/object","version_id":"v1","tier_name":"WARM"}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("v2 entry without identity must fail closed");

        assert!(err.to_string().contains("backend identity"));
    }

    #[test]
    fn tier_delete_journal_uses_persisted_transition_destination_identity() {
        let mut je = journal_entry();
        je.backend_identity = None;
        let identity = [9_u8; 32];
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity),
        );

        record_tier_delete_journal_backend_identity(&mut je, &metadata).expect("persisted transition identity should decode");
        let encoded = encode_tier_delete_journal_entry(&je).expect("identity-bound journal should encode");
        let decoded = decode_tier_delete_journal_entry(&encoded).expect("identity-bound journal should decode");

        assert_eq!(decoded.backend_identity, Some(identity));
    }

    #[test]
    fn tier_delete_journal_without_transition_identity_stays_legacy() {
        let mut je = journal_entry();
        je.backend_identity = None;
        je.version_id_exact = false;
        je.version_state = rustfs_filemeta::TransitionVersionState::Unknown;

        let encoded = encode_tier_delete_journal_entry(&je).expect("legacy journal should remain encodable");
        let persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("journal JSON should decode");

        assert_eq!(persisted["version"], 1);
        assert!(persisted["backend_identity"].is_null());
    }

    #[test]
    fn tier_delete_journal_rejects_incomplete_entry() {
        let payload = br#"{"version":1,"obj_name":"","version_id":"v1","tier_name":"WARM"}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("incomplete journal entry should be rejected");

        assert!(err.to_string().contains("incomplete"));
    }

    #[test]
    fn tier_delete_journal_recovers_unversioned_tier_entry() {
        // A remote tier that is unversioned records an empty `version_id`. Such a
        // WAL entry must decode successfully so recovery can drive a versionless
        // remote delete, otherwise the remote object is orphaned and the journal
        // file leaks forever.
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":"","tier_name":"WARM"}"#;

        let decoded = decode_tier_delete_journal_entry(payload).expect("unversioned tier entry should decode");

        assert_eq!(decoded.obj_name, "remote/object");
        assert!(decoded.version_id.is_empty());
        assert_eq!(decoded.tier_name, "WARM");
        assert_eq!(decoded.backend_identity, None);
    }

    #[test]
    fn tier_delete_journal_rejects_missing_tier_name() {
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":"v1","tier_name":""}"#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("entry missing tier name should be rejected");

        assert!(err.to_string().contains("incomplete"));
    }

    #[test]
    fn tier_delete_journal_rejects_truncated_payload() {
        // A partially written journal file fails at JSON deserialization, so
        // relaxing the empty-version_id check does not admit truncated records.
        let payload = br#"{"version":1,"obj_name":"remote/object","version_id":""#;

        let err = decode_tier_delete_journal_entry(payload).expect_err("truncated journal payload should be rejected");

        assert!(err.to_string().contains("decode tier delete journal failed"));
    }

    #[tokio::test]
    async fn tier_delete_journal_recovery_has_a_hard_outer_timeout() {
        let result = await_tier_delete_journal_recovery(
            &CancellationToken::new(),
            Duration::from_millis(10),
            std::future::pending::<Result<()>>(),
        )
        .await
        .expect("an elapsed timeout should return a recovery error")
        .expect_err("a permanently pending recovery must time out");

        assert!(result.to_string().contains("recovery timed out"), "{result}");
    }

    #[tokio::test]
    async fn tier_delete_journal_recovery_drops_in_flight_work_on_shutdown() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result =
            await_tier_delete_journal_recovery(&cancel, Duration::from_secs(30), std::future::pending::<Result<()>>()).await;

        assert!(result.is_none(), "shutdown must cancel the in-flight recovery future");
    }
}
