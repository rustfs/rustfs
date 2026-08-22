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

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bucket::lifecycle::config_boundary;
use crate::bucket::lifecycle::durable_namespace::TIER_DELETE_JOURNAL_NAMESPACE;
use crate::bucket::lifecycle::runtime_boundary;
use crate::bucket::lifecycle::tier_sweeper::{
    Jentry, TierDeleteJournalState, TierDeleteSourceIdentity,
    delete_confirmed_transition_candidate_exact_with_manager_and_identity,
    delete_object_from_remote_tier_idempotent_with_manager_and_identity,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::services::tier::tier::tier_destination_id_from_metadata;
use crate::storage_api_contracts::{
    list::ListOperations as _,
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
const TIER_DELETE_JOURNAL_VERSION: u8 = 2;
const TIER_DELETE_JOURNAL_EXACT_VERSION: u8 = 3;
const TIER_DELETE_JOURNAL_STATE_VERSION: u8 = 4;
const TIER_DELETE_JOURNAL_TRANSACTION_VERSION: u8 = 5;
pub(crate) const TIER_DELETE_JOURNAL_PREFIX: &str = TIER_DELETE_JOURNAL_NAMESPACE.prefix;

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
}

impl PersistedTierDeleteJournalEntry {
    fn from_jentry(je: &Jentry) -> Result<Self> {
        validate_version_state(je.version_state, &je.version_id, je.version_id_exact)?;
        let legacy_unknown = je.version_state == rustfs_filemeta::TransitionVersionState::Unknown;
        let version = if je.source.is_some() || je.state == TierDeleteJournalState::Prepared {
            if je.backend_identity.is_none() {
                return Err(Error::other("tier delete transaction is missing its backend identity"));
            }
            TIER_DELETE_JOURNAL_TRANSACTION_VERSION
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
            state: (version == TIER_DELETE_JOURNAL_TRANSACTION_VERSION).then_some(je.state),
            source: (version == TIER_DELETE_JOURNAL_TRANSACTION_VERSION)
                .then(|| je.source.clone())
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
            && self.version_id_exact.unwrap_or(false)
        {
            return Err(Error::other(
                "legacy tier delete journal entry has an unsupported exact version constraint",
            ));
        }
        let (backend_identity, version_id_exact, version_state, state, source) = match self.version {
            1 => (
                None,
                false,
                rustfs_filemeta::TransitionVersionState::Unknown,
                TierDeleteJournalState::Committed,
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
                )
            }
            TIER_DELETE_JOURNAL_TRANSACTION_VERSION => {
                let state = self
                    .state
                    .ok_or_else(|| Error::other("tier delete journal v5 entry is missing its state"))?;
                let source = self
                    .source
                    .ok_or_else(|| Error::other("tier delete journal v5 entry is missing its source identity"))?;
                let exact = self.version_id_exact.unwrap_or(false);
                let version_state = self
                    .version_state
                    .ok_or_else(|| Error::other("tier delete journal v5 entry is missing its version state"))?;
                validate_version_state(version_state, &self.version_id, exact)?;
                (
                    Some(
                        self.backend_identity
                            .ok_or_else(|| Error::other("tier delete journal v5 entry is missing its backend identity"))?,
                    ),
                    exact,
                    version_state,
                    state,
                    Some(source),
                )
            }
            version => return Err(Error::other(format!("unsupported tier delete journal version {version}"))),
        };
        Ok(Jentry {
            obj_name: self.obj_name,
            version_id: self.version_id,
            tier_name: self.tier_name,
            backend_identity,
            version_id_exact,
            version_state,
            state,
            source,
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

pub(crate) fn tier_delete_journal_object_name(je: &Jentry) -> String {
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
    format!(
        "{TIER_DELETE_JOURNAL_PREFIX}{}.json",
        rustfs_utils::crypto::hex(hasher.finalize().as_slice())
    )
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
    config_boundary::save_config(api, &tier_delete_journal_object_name(je), data)
        .await
        .map_err(std::io::Error::other)
}

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
    let mut committed = je.clone();
    committed.state = TierDeleteJournalState::Committed;
    persist_tier_delete_journal_entry(api, &committed).await
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

pub async fn abort_prepared_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    let name = tier_delete_journal_object_name(je);
    let (data, metadata) = match config_boundary::read_config_with_metadata(api.clone(), &name, &ObjectOptions::default()).await {
        Ok(result) => result,
        Err(Error::ConfigNotFound) | Err(Error::FileNotFound) => return Ok(()),
        Err(err) => return Err(std::io::Error::other(err)),
    };
    let current = decode_tier_delete_journal_entry(&data).map_err(std::io::Error::other)?;
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
    if je.state == TierDeleteJournalState::Prepared {
        return reconcile_prepared_tier_delete_journal_entry(api, je).await;
    }
    process_committed_tier_delete_journal_entry(api, je).await
}

async fn process_committed_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    if je.version_state == rustfs_filemeta::TransitionVersionState::Unknown {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "tier delete journal remote version state is unknown",
        ));
    }
    let backend_identity = je
        .backend_identity
        .ok_or_else(|| std::io::Error::other("legacy tier delete journal has no durable backend identity"))?;
    if je.version_id_exact {
        delete_confirmed_transition_candidate_exact_with_manager_and_identity(
            &je.obj_name,
            &je.version_id,
            &je.tier_name,
            backend_identity,
            &api.tier_config_mgr(),
        )
        .await?;
    } else {
        delete_object_from_remote_tier_idempotent_with_manager_and_identity(
            &je.obj_name,
            &je.version_id,
            &je.tier_name,
            backend_identity,
            &api.tier_config_mgr(),
            false,
        )
        .await?;
    }
    let path = tier_delete_journal_object_name(je);
    let data = encode_tier_delete_journal_entry(je).map_err(std::io::Error::other)?;
    let target_pool_indices = api
        .record_durable_ilm_decommission_terminal_target_pools(&path, &data)
        .await
        .map_err(std::io::Error::other)?;
    if let Some(target_pool_indices) = target_pool_indices {
        for target_pool_idx in target_pool_indices {
            match config_boundary::delete_config(api.pools[target_pool_idx].clone(), &path).await {
                Ok(()) | Err(Error::ConfigNotFound) => {}
                Err(err) => return Err(std::io::Error::other(err)),
            }
        }
        return Ok(());
    }
    remove_tier_delete_journal_entry(api, je).await
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

async fn prepared_tier_delete_has_live_source(
    api: &ECStore,
    source: &TierDeleteSourceIdentity,
    je: &Jentry,
) -> std::io::Result<(bool, Vec<crate::store::ObjectLockDiagGuard>)> {
    let lock_object = rustfs_utils::path::encode_dir_object(&source.object);
    let mut lock_opts = ObjectOptions::default();
    let read_guards = api
        .acquire_all_object_read_locks("tier_delete_journal_recovery", &source.bucket, &lock_object, &mut lock_opts)
        .await
        .map_err(std::io::Error::other)?;
    if api.ctx.lock_manager().is_disabled() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal recovery requires namespace locking",
        ));
    }
    let mut has_live_source = false;
    for pool in &api.pools {
        let set = pool.get_disks_by_key(&lock_object);
        let Some(versions) = set
            .load_file_info_versions_exact(&source.bucket, &source.object)
            .await
            .map_err(std::io::Error::other)?
        else {
            continue;
        };
        for version in versions.versions.iter().filter(|version| !version.tier_free_version()) {
            let info = ObjectInfo::from_file_info(version, &source.bucket, &source.object, source.versioned);
            if object_info_references_tier_delete(&info, je)? {
                has_live_source = true;
                break;
            }
        }
        if has_live_source {
            break;
        }
    }
    if read_guards.iter().any(crate::store::ObjectLockDiagGuard::is_lock_lost) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "tier delete journal recovery object read lock was lost",
        ));
    }
    Ok((has_live_source, read_guards))
}

async fn reconcile_prepared_tier_delete_journal_entry(api: Arc<ECStore>, je: &Jentry) -> std::io::Result<()> {
    let journal_name = tier_delete_journal_object_name(je);
    let (data, metadata) = config_boundary::read_config_with_metadata(api.clone(), &journal_name, &ObjectOptions::default())
        .await
        .map_err(std::io::Error::other)?;
    let current = decode_tier_delete_journal_entry(&data).map_err(std::io::Error::other)?;
    if tier_delete_journal_object_name(&current) != journal_name {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "prepared tier delete journal content does not match its object name",
        ));
    }
    if current.state != TierDeleteJournalState::Prepared {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "prepared tier delete journal changed before reconciliation",
        ));
    }
    let Some(etag) = metadata.etag else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "prepared tier delete journal has no entity tag",
        ));
    };
    let source = current
        .source
        .as_ref()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "prepared tier delete journal has no source"))?;
    match prepared_tier_delete_has_live_source(&api, source, &current).await {
        Ok((true, read_guards)) => {
            if read_guards.iter().any(crate::store::ObjectLockDiagGuard::is_lock_lost) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "tier delete journal recovery object read lock was lost before abort",
                ));
            }
            let result = config_boundary::delete_config_if_match(api, &tier_delete_journal_object_name(&current), &etag).await;
            drop(read_guards);
            match result {
                Ok(()) => Ok(()),
                Err(Error::PreconditionFailed) => Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "prepared tier delete journal changed before abort",
                )),
                Err(err) => Err(std::io::Error::other(err)),
            }
        }
        Ok((false, read_guards)) if source.has_stable_identity() => {
            if read_guards.iter().any(crate::store::ObjectLockDiagGuard::is_lock_lost) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "tier delete journal recovery object read lock was lost before commit",
                ));
            }
            let mut commit_opts = ObjectOptions::default();
            for signal in read_guards
                .iter()
                .filter_map(crate::store::ObjectLockDiagGuard::lock_lost_signal)
            {
                commit_opts.add_namespace_lock_lost_signal(signal);
            }
            let committed =
                commit_prepared_tier_delete_journal_entry_if_current(api.clone(), current, etag, &commit_opts).await?;
            // Keep namespace locks only through the journal CAS. Remote-tier IO
            // must not block writers for the object during recovery.
            drop(read_guards);
            process_committed_tier_delete_journal_entry(api, &committed).await
        }
        Ok((false, _read_guards)) => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "prepared tier delete journal source identity is not sufficient to confirm deletion",
        )),
        Err(err) => Err(err),
    }
}

async fn commit_prepared_tier_delete_journal_entry_if_current(
    api: Arc<ECStore>,
    mut committed: Jentry,
    etag: String,
    lock_opts: &ObjectOptions,
) -> std::io::Result<Jentry> {
    committed.state = TierDeleteJournalState::Committed;
    let data = encode_tier_delete_journal_entry(&committed).map_err(std::io::Error::other)?;
    match config_boundary::save_config_with_opts(
        api.clone(),
        &tier_delete_journal_object_name(&committed),
        data,
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(etag),
                ..Default::default()
            }),
            namespace_lock_fence: lock_opts.namespace_lock_fence.clone(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(()) => Ok(committed),
        Err(Error::PreconditionFailed) => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "prepared tier delete journal changed before commit",
        )),
        Err(err) => Err(std::io::Error::other(err)),
    }
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
        TIER_DELETE_JOURNAL_EXACT_VERSION, TIER_DELETE_JOURNAL_STATE_VERSION, await_tier_delete_journal_recovery,
        decode_tier_delete_journal_entry, encode_tier_delete_journal_entry, object_info_references_tier_delete,
        record_tier_delete_journal_backend_identity, tier_delete_journal_object_name,
    };
    use crate::bucket::lifecycle::tier_sweeper::{Jentry, TierDeleteJournalState, TierDeleteSourceIdentity};
    use crate::error::Result;
    use crate::object_api::ObjectInfo;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn journal_entry() -> Jentry {
        Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([7; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Committed,
            source: None,
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
    fn tier_delete_transaction_roundtrips_prepared_source_identity() {
        let mut je = journal_entry();
        je.state = TierDeleteJournalState::Prepared;
        je.source = Some(TierDeleteSourceIdentity {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: Some("version".to_string()),
            versioned: true,
            version_suspended: false,
            data_dir: Some("data-dir".to_string()),
            etag: Some("etag".to_string()),
            mod_time: Some("mod-time".to_string()),
        });

        let encoded = encode_tier_delete_journal_entry(&je).expect("prepared transaction should encode");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("transaction should be JSON");
        assert_eq!(value["version"], serde_json::json!(5));
        assert_eq!(value["state"], serde_json::json!("Prepared"));
        assert!(value["source"].is_object());

        let decoded = decode_tier_delete_journal_entry(&encoded).expect("prepared transaction should decode");
        assert_eq!(decoded.state, TierDeleteJournalState::Prepared);
        assert_eq!(decoded.source, je.source);
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
