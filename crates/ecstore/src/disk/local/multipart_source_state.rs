// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Owned, independent metadata records for native multipart publication.

use super::{LocalDisk, multipart_staging::MultipartCommitStaging};
use crate::disk::multipart_source::*;
use crate::disk::{DiskAPI, DiskError, Error, RUSTFS_META_MULTIPART_BUCKET, Result, STORAGE_FORMAT_FILE, os};
use bytes::Bytes;
use rustfs_filemeta::{FileInfo, FileInfoOpts, FileMeta, get_file_info};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Error as IoError;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const STATE: &str = ".mpu-source-state";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeNegativeClaim {
    pub from: MultipartSourceSnapshot,
    pub round: MultipartRecoveryRound,
    pub owned_undo: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeUndoIntent {
    pub from: MultipartSourceSnapshot,
    pub round: MultipartRecoveryRound,
    pub capability_sha256: [u8; 32],
    pub target_undone: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceState {
    pub protocol_version: u16,
    pub disk_id: Uuid,
    pub physical_slot: usize,
    pub lineage: SourceLineage,
    pub generation: SourceGeneration,
    pub revision: u64,
    pub phase: MultipartSourcePhase,
    pub publication: Option<PublicationIdentity>,
    pub backup_owner: Uuid,
    pub original_sha256: [u8; 32],
    pub prepared_sha256: Option<[u8; 32]>,
    #[serde(default)]
    pub prepared_length: Option<u64>,
    pub previous_target_sha256: Option<[u8; 32]>,
    pub previous_target_length: Option<u64>,
    pub capability_sha256: Option<[u8; 32]>,
    pub recovery: Option<MultipartRecoveryRound>,
    pub negative_certificate_sha256: Option<[u8; 32]>,
    pub ready_certificate_sha256: Option<[u8; 32]>,
    pub terminal_certificate: Vec<MultipartSourceProof>,
    pub negative_claim: Option<NativeNegativeClaim>,
    pub undo: Option<NativeUndoIntent>,
    #[serde(default)]
    pub restore_from: Option<MultipartSourceSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort: Option<super::multipart_source_abort::NativeAbortState>,
}
impl SourceState {
    pub fn advance(&mut self, phase: MultipartSourcePhase) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(DiskError::FileCorrupt)?;
        self.phase = phase;
        Ok(())
    }
    pub(super) fn validate(&self, disk_id: Uuid, path: &str) -> Result<()> {
        self.lineage.validate()?;
        if self.protocol_version != MULTIPART_SOURCE_PROTOCOL_VERSION
            || self.disk_id != disk_id
            || self.disk_id.is_nil()
            || self.lineage.path != path
            || self.physical_slot >= self.lineage.geometry.disks()
            || self.backup_owner.is_nil()
            || self.generation.nonce.is_nil()
            || self.prepared_sha256.is_some() != self.prepared_length.is_some()
            || self.previous_target_sha256.is_some() != self.previous_target_length.is_some()
            || self.publication.as_ref().is_some_and(|p| p.lineage != self.lineage)
        {
            return Err(DiskError::FileCorrupt);
        }
        if let Some(abort) = &self.abort {
            abort.validate(self)?;
        } else if self.phase == MultipartSourcePhase::Aborting {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

pub(super) struct SourceHead {
    pub sha256: [u8; 32],
    pub length: u64,
    raw: Option<Bytes>,
}
impl SourceHead {
    pub fn raw(&self) -> Option<&Bytes> {
        self.raw.as_ref()
    }
}

pub(super) struct LoadedSource {
    pub root_missing: bool,
    pub state: Option<SourceState>,
    pub head: Option<SourceHead>,
    pub original: Option<Bytes>,
    pub info: Option<FileInfo>,
    pub snapshot: Option<MultipartSourceSnapshot>,
    pub head_matches_state: bool,
    pub head_matches_activation: bool,
}

pub(super) struct SourceStore {
    pub disk_id: Uuid,
    pub physical_slot: usize,
    pub disk_ids: Vec<Uuid>,
    pub root: PathBuf,
    pub base: PathBuf,
    pub source: PathBuf,
    pub path: String,
    pub publication_root: os::PublicationRoot,
    pub lease: Arc<os::NamespaceMutationLease>,
    pub owner: Option<Arc<dyn Send + Sync>>,
    pub authority: ProofVerificationContext,
}

/// A certificate can regain local runtime provenance only after this loader
/// checked its fixed owned record, state reference, round and complete digest.
pub(in crate::disk) struct ValidatedOwnedCertificate {
    digest: [u8; 32],
    round: MultipartRecoveryRound,
    kind: MultipartProofKind,
}
impl ValidatedOwnedCertificate {
    pub(in crate::disk) fn validate_certificate(&self, certificate: &MultipartCertificate) -> Result<()> {
        if certificate.round != self.round
            || raw_sha(&encode(certificate)?) != self.digest
            || certificate
                .proofs
                .iter()
                .any(|proof| proof.kind != self.kind || proof.recovery.as_ref() != Some(&self.round))
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

impl LocalDisk {
    pub(super) async fn open_multipart_source_store(
        &self,
        path: &str,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<SourceStore> {
        let owner = self
            .multipart_source_owner(RUSTFS_META_MULTIPART_BUCKET, path, false, true, owner)
            .await?;
        self.multipart_source_store_owned(path, owner).await
    }
    pub(super) async fn multipart_store_from_staging(&self, path: &str, staging: &MultipartCommitStaging) -> Result<SourceStore> {
        self.multipart_source_store_owned(path, staging.source_owner()).await
    }
    async fn multipart_source_store_owned(&self, path: &str, owner: Option<Arc<dyn Send + Sync>>) -> Result<SourceStore> {
        super::multipart_staging::reject_noncanonical_scope(&self.root, self.io_root(), RUSTFS_META_MULTIPART_BUCKET, path)?;
        let disk_id = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::UnformattedDisk)?;
        let format = self.format_info.read().await.data.clone();
        let format = crate::disk::format::FormatV3::try_from(format.as_ref()).map_err(|_| DiskError::CorruptedBackend)?;
        let (set, physical_slot) = format.find_disk_index_by_disk_id(disk_id)?;
        let disk_ids = format.erasure.sets.get(set).ok_or(DiskError::CorruptedBackend)?.clone();
        if (self.endpoint.disk_idx >= 0 && self.endpoint.disk_idx as usize != physical_slot)
            || (self.endpoint.set_idx >= 0 && self.endpoint.set_idx as usize != set)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let source = self.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, path)?;
        let logical = self.get_object_path(RUSTFS_META_MULTIPART_BUCKET, path)?;
        let lease = os::acquire_namespace_mutation_lease_with_owner(&logical, owner.clone()).await;
        Ok(SourceStore {
            disk_id,
            physical_slot,
            disk_ids,
            root: self.root.clone(),
            base: self.io_root().to_path_buf(),
            source,
            path: path.to_owned(),
            publication_root: self.publication_root.clone(),
            lease,
            owner,
            authority: self.multipart_proof_verification_context(),
        })
    }
}

pub(super) fn raw_sha(raw: &[u8]) -> [u8; 32] {
    Sha256::digest(raw).into()
}
pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(Error::other)
}
pub(super) fn decode<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T> {
    if raw.len() > MULTIPART_SOURCE_MAX_PAYLOAD_BYTES {
        return Err(DiskError::FileCorrupt);
    }
    let mut reader = rmp_serde::Deserializer::new(std::io::Cursor::new(raw));
    let value = T::deserialize(&mut reader).map_err(|_| DiskError::FileCorrupt)?;
    if reader.position() != raw.len() as u64 {
        return Err(DiskError::FileCorrupt);
    }
    Ok(value)
}
pub(super) fn read_optional(path: &Path, base: &Path, root: &os::PublicationRoot) -> Result<Option<Bytes>> {
    match os::read_multipart_source_metadata(path, base, root) {
        Ok(bytes) => Ok(Some(Bytes::from(bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn read_optional_in_pinned(
    pinned: &os::ExistingBaseDirectoryGuard,
    parent: &Path,
    name: &str,
    base: &Path,
    root: &os::PublicationRoot,
) -> Result<Option<Bytes>> {
    match os::read_multipart_source_metadata_in_pinned(pinned, parent, std::ffi::OsStr::new(name), base, root) {
        Ok(bytes) => Ok(Some(Bytes::from(bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
#[derive(Clone)]
pub(super) struct ActiveMultipartSourceEntry {
    pub(super) raw: Bytes,
    pub(super) fi: FileInfo,
    pub(super) unsettled_parts: std::collections::HashSet<usize>,
    pub(super) parts: std::collections::HashMap<usize, rustfs_filemeta::ObjectPartInfo>,
}

static ACTIVE_MULTIPART_SOURCE_HEADS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<PathBuf, ActiveMultipartSourceEntry>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

#[cfg(any(test, feature = "test-util"))]
static FORCE_ACTIVE_MULTIPART_SOURCE_CACHE_IN_TEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
pub(super) struct ActiveMultipartSourceCacheTestGuard;

#[cfg(test)]
impl Drop for ActiveMultipartSourceCacheTestGuard {
    fn drop(&mut self) {
        FORCE_ACTIVE_MULTIPART_SOURCE_CACHE_IN_TEST.store(false, std::sync::atomic::Ordering::Release);
        ACTIVE_MULTIPART_SOURCE_HEADS.lock().clear();
    }
}

#[cfg(test)]
pub(super) fn enable_active_multipart_source_cache_for_test() -> ActiveMultipartSourceCacheTestGuard {
    ACTIVE_MULTIPART_SOURCE_HEADS.lock().clear();
    FORCE_ACTIVE_MULTIPART_SOURCE_CACHE_IN_TEST.store(true, std::sync::atomic::Ordering::Release);
    ActiveMultipartSourceCacheTestGuard
}

#[inline]
pub(super) fn active_multipart_source_cache_enabled() -> bool {
    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
    {
        FORCE_ACTIVE_MULTIPART_SOURCE_CACHE_IN_TEST.load(std::sync::atomic::Ordering::Acquire)
            && !os::prepared_publication_test_hooks::has_any_hooks()
    }
    #[cfg(all(any(test, feature = "test-util"), windows))]
    {
        FORCE_ACTIVE_MULTIPART_SOURCE_CACHE_IN_TEST.load(std::sync::atomic::Ordering::Acquire)
    }
    #[cfg(not(any(test, feature = "test-util")))]
    {
        true
    }
}

pub(super) fn put_active_multipart_source_head(source: PathBuf, raw: Bytes, fi: FileInfo) {
    if !active_multipart_source_cache_enabled() {
        return;
    }
    let mut map = ACTIVE_MULTIPART_SOURCE_HEADS.lock();
    if map.len() >= 4096
        && let Some(evict_key) = map.keys().next().cloned()
    {
        map.remove(&evict_key);
    }
    map.insert(
        source,
        ActiveMultipartSourceEntry {
            raw,
            fi,
            unsettled_parts: std::collections::HashSet::new(),
            parts: std::collections::HashMap::new(),
        },
    );
}

pub(super) fn get_active_multipart_source_head(source: &Path) -> Option<(Bytes, FileInfo)> {
    if !active_multipart_source_cache_enabled() {
        return None;
    }
    ACTIVE_MULTIPART_SOURCE_HEADS
        .lock()
        .get(source)
        .map(|entry| (entry.raw.clone(), entry.fi.clone()))
}

pub(super) fn has_active_multipart_source_head(source: &Path) -> bool {
    active_multipart_source_cache_enabled() && ACTIVE_MULTIPART_SOURCE_HEADS.lock().contains_key(source)
}

pub(super) fn has_clean_active_multipart_source(source: &Path) -> bool {
    active_multipart_source_cache_enabled()
        && ACTIVE_MULTIPART_SOURCE_HEADS
            .lock()
            .get(source)
            .is_some_and(|entry| entry.unsettled_parts.is_empty())
}

pub(super) fn begin_active_multipart_part_transaction(source: &Path, part_number: usize) {
    if !active_multipart_source_cache_enabled() {
        return;
    }
    if let Some(entry) = ACTIVE_MULTIPART_SOURCE_HEADS.lock().get_mut(source) {
        entry.unsettled_parts.insert(part_number);
        entry.parts.remove(&part_number);
    }
}

pub(super) fn record_active_multipart_part_committed(source: &Path, part: rustfs_filemeta::ObjectPartInfo) {
    if !active_multipart_source_cache_enabled() {
        return;
    }
    if let Some(entry) = ACTIVE_MULTIPART_SOURCE_HEADS.lock().get_mut(source) {
        entry.parts.insert(part.number, part);
    }
}

pub(super) fn finish_active_multipart_part_transaction(
    source: &Path,
    part_number: Option<usize>,
    committed: bool,
    retired_existing_txn: bool,
) {
    let mut map = ACTIVE_MULTIPART_SOURCE_HEADS.lock();
    let Some(part_number) = part_number.filter(|_| committed) else {
        map.remove(source);
        return;
    };
    if !retired_existing_txn {
        return;
    }
    if let Some(entry) = map.get_mut(source)
        && !entry.unsettled_parts.remove(&part_number)
    {
        map.remove(source);
    }
}

pub(super) fn get_active_multipart_source_part(source: &Path, part_number: usize) -> Option<rustfs_filemeta::ObjectPartInfo> {
    if !active_multipart_source_cache_enabled() {
        return None;
    }
    ACTIVE_MULTIPART_SOURCE_HEADS
        .lock()
        .get(source)
        .filter(|entry| entry.unsettled_parts.is_empty())
        .and_then(|entry| entry.parts.get(&part_number).cloned())
}

pub(super) fn invalidate_active_multipart_source_head(source: &Path) {
    ACTIVE_MULTIPART_SOURCE_HEADS.lock().remove(source);
}

/// Canonical source mutations are admitted under SourceRW at the syscall,
/// not on the earlier async caller that may have timed out.
pub(super) fn validate_source_mutation(source: &Path, base: &Path, root: &os::PublicationRoot, initial: bool) -> Result<()> {
    if !initial && has_active_multipart_source_head(source) {
        return Ok(());
    }
    let pinned = match os::pin_multipart_source_directory(source, base, root) {
        Ok(pinned) => pinned,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return if initial { Ok(()) } else { Err(DiskError::FileNotFound) };
        }
        Err(error) => return Err(error.into()),
    };
    let state = read_optional_in_pinned(&pinned, source, STATE, base, root)?;
    let head = read_optional_in_pinned(&pinned, source, STORAGE_FORMAT_FILE, base, root)?;
    let Some(head) = head else {
        return if initial && state.is_none() {
            Ok(())
        } else {
            Err(DiskError::FileNotFound)
        };
    };
    // Admission has not selected a source version yet. A versioned Create
    // already has a concrete UUID, so an explicit null-slot lookup would
    // reject every subsequent Part write. Exact publication/backup lookups
    // below retain their separate null-version semantics.
    let fi = get_file_info(
        &head,
        RUSTFS_META_MULTIPART_BUCKET,
        "",
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .map_err(Error::other)?;
    native_source_protocol(&fi)?;
    if fi.deleted || !is_active_source(&fi)? {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    if let Some(raw) = state {
        let state: SourceState = decode(&raw)?;
        if state.phase != MultipartSourcePhase::Active || state.capability_sha256.is_some() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let condition = SourceConditionV1 {
            lineage: state.lineage.clone(),
            generation: state.generation,
            attempt: state.backup_owner,
        };
        if !condition.matches_original(&fi)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let status = source_status(&fi)?;
        if state.generation.epoch > 0
            && !status.is_some_and(|status| {
                !status.recovering
                    && status.generation == state.generation
                    && status.round == state.recovery.as_ref().map(|round| round.round)
                    && status.ready_sha256 == state.ready_certificate_sha256
            })
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
    }
    Ok(())
}

fn file_info(raw: &[u8], volume: &str, path: &str, version: Option<Uuid>) -> Result<FileInfo> {
    get_file_info(
        raw,
        volume,
        path,
        &version.unwrap_or_default().to_string(),
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .map_err(Error::other)
}
fn certificate_path(source: &Path, round: Uuid, ready: bool) -> PathBuf {
    source
        .join(".mpu-recovery")
        .join(round.to_string())
        .join(if ready { "ready.msgpack" } else { "negative.msgpack" })
}
pub(super) fn original_path(source: &Path, owner: Uuid) -> PathBuf {
    source
        .join(".mpu-completions")
        .join(owner.to_string())
        .join("original.xl.meta")
}
fn target_backup_path(source: &Path, owner: Uuid) -> PathBuf {
    source.join(".mpu-completions").join(owner.to_string()).join("target.xl.meta")
}

fn terminal_record_path(source: &Path, state: &SourceState) -> Option<PathBuf> {
    matches!(
        state.phase,
        MultipartSourcePhase::LocalTerminalSeal | MultipartSourcePhase::GlobalTerminalCertified
    )
    .then(|| {
        source
            .join(".mpu-completions")
            .join(state.backup_owner.to_string())
            .join(format!("terminal-{}.msgpack", state.revision))
    })
}

fn snapshot_for_state(
    disk_id: Uuid,
    state: &SourceState,
    state_raw: &[u8],
    head_sha256: Option<[u8; 32]>,
) -> Result<MultipartSourceSnapshot> {
    let state_sha256 = raw_sha(state_raw);
    let inventory = raw_sha(&encode(&(
        Some(state_sha256),
        head_sha256,
        state.original_sha256,
        state.negative_certificate_sha256,
        state.ready_certificate_sha256,
    ))?);
    Ok(MultipartSourceSnapshot {
        disk_id,
        physical_slot: state.physical_slot,
        lineage: state.lineage.clone(),
        generation: state.generation,
        revision: state.revision,
        phase: state.phase,
        state_sha256,
        head_sha256,
        inventory_sha256: inventory,
        capability_sha256: state.capability_sha256,
        publication: state.publication.clone(),
    })
}

impl SourceStore {
    pub fn validate_certificate_topology(&self, certificate: &MultipartCertificate) -> Result<()> {
        if certificate.disk_ids != self.disk_ids || certificate.disk_ids.get(self.physical_slot) != Some(&self.disk_id) {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(())
    }
    pub fn verify_certificate_cohort(&self, certificate: &MultipartCertificate) -> Result<()> {
        self.validate_certificate_topology(certificate)
    }
    pub fn issue_proof(
        &self,
        kind: MultipartProofKind,
        challenge: Uuid,
        recovery: Option<MultipartRecoveryRound>,
        snapshot: MultipartSourceSnapshot,
    ) -> Result<MultipartSourceProof> {
        MultipartSourceProof::issue(&self.authority, kind, challenge, recovery, snapshot)
    }
    pub async fn load(&self) -> Result<LoadedSource> {
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let path = self.path.clone();
        let disk_id = self.disk_id;
        let physical_slot = self.physical_slot;
        let disk_ids = self.disk_ids.clone();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            Ok(load_source(&source, &base, &root, &path, disk_id, physical_slot, &disk_ids))
        })
        .await
        .map_err(DiskError::from)?
    }
    pub async fn persist(&self, state: &SourceState) -> Result<()> {
        self.persist_with_source_sync(state, false).await.map(|_| ())
    }
    pub(super) async fn persist_with_source_sync(
        &self,
        state: &SourceState,
        sync_source_before: bool,
    ) -> Result<MultipartSourceSnapshot> {
        state.validate(self.disk_id, &self.path)?;
        let raw = encode(state)?;
        let snapshot = snapshot_for_state(self.disk_id, state, &raw, None)?;
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let terminal = terminal_record_path(&source, state);
        os::run_blocking_namespace_operation_with_sync(self.lease.clone(), true, move || {
            Self::persist_state_std(&source, &base, &root, &raw, terminal.as_deref(), sync_source_before)
        })
        .await
        .map_err(DiskError::from)?;
        Ok(snapshot)
    }
    fn persist_state_std(
        source: &Path,
        base: &Path,
        root: &os::PublicationRoot,
        raw: &[u8],
        terminal: Option<&Path>,
        sync_source_before: bool,
    ) -> std::io::Result<()> {
        invalidate_active_multipart_source_head(source);
        #[cfg(unix)]
        let mut pinned = os::pin_multipart_source_directory(source, base, root)?;
        #[cfg(not(unix))]
        let _pinned = os::pin_multipart_source_directory(source, base, root)?;
        if sync_source_before {
            #[cfg(unix)]
            {
                let opened = std::fs::File::from(pinned.pop().ok_or_else(|| IoError::other("empty source directory guard"))?);
                os::fsync_directory_handle_std(source, Some(&opened))?;
            }
            #[cfg(not(unix))]
            os::fsync_dir_std(source)?;
        }
        if let Some(path) = terminal {
            os::replace_multipart_source_metadata(path, raw, base, root)?;
        }
        os::replace_multipart_source_metadata(&source.join(STATE), raw, base, root)
    }
    async fn write_owned_backup_and_persist(&self, owner: Uuid, target: bool, bytes: &[u8], state: &SourceState) -> Result<()> {
        if owner.is_nil() {
            return Err(DiskError::FileCorrupt);
        }
        state.validate(self.disk_id, &self.path)?;
        let raw = encode(state)?;
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let terminal = terminal_record_path(&source, state);
        let bytes = bytes.to_vec();
        os::run_blocking_namespace_operation_with_sync(self.lease.clone(), true, move || {
            Self::write_owned_backup_std(&source, &base, &root, owner, target, &bytes, false)?;
            Self::persist_state_std(&source, &base, &root, &raw, terminal.as_deref(), false)
        })
        .await
        .map_err(DiskError::from)
    }
    #[cfg(test)]
    pub async fn original(&self, state: &SourceState) -> Result<Bytes> {
        let path = original_path(&self.source, state.backup_owner);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let expected = state.original_sha256;
        let bytes = os::run_blocking_namespace_operation(self.lease.clone(), move || {
            os::read_multipart_source_metadata(&path, &base, &root)
        })
        .await?;
        if raw_sha(&bytes) != expected {
            return Err(DiskError::FileCorrupt);
        }
        Ok(Bytes::from(bytes))
    }
    pub async fn ensure_state(&self, loaded: &LoadedSource) -> Result<SourceState> {
        if let Some(state) = &loaded.state {
            return Ok(state.clone());
        }
        let snapshot = loaded.snapshot.as_ref().ok_or(DiskError::FileCorrupt)?;
        let original = loaded.original.as_ref().ok_or(DiskError::FileCorrupt)?;
        if snapshot.phase != MultipartSourcePhase::Active || !loaded.head_matches_state {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let state = SourceState {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            disk_id: self.disk_id,
            physical_slot: snapshot.physical_slot,
            lineage: snapshot.lineage.clone(),
            generation: snapshot.generation,
            revision: snapshot.revision,
            phase: MultipartSourcePhase::Active,
            publication: None,
            backup_owner: Uuid::new_v4(),
            original_sha256: raw_sha(original),
            prepared_sha256: None,
            prepared_length: None,
            previous_target_sha256: None,
            previous_target_length: None,
            capability_sha256: None,
            recovery: None,
            negative_certificate_sha256: None,
            ready_certificate_sha256: None,
            terminal_certificate: Vec::new(),
            negative_claim: None,
            undo: None,
            restore_from: None,
            abort: None,
        };
        self.write_owned_backup_and_persist(state.backup_owner, false, original, &state)
            .await?;
        Ok(state)
    }
    #[cfg(test)]
    pub async fn write_owned_backup(&self, owner: Uuid, target: bool, bytes: &[u8]) -> Result<()> {
        if owner.is_nil() {
            return Err(DiskError::FileCorrupt);
        }
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let bytes = bytes.to_vec();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            Self::write_owned_backup_std(&source, &base, &root, owner, target, &bytes, true)
        })
        .await
        .map_err(DiskError::from)
    }
    fn write_owned_backup_std(
        source: &Path,
        base: &Path,
        root: &os::PublicationRoot,
        owner: Uuid,
        target: bool,
        bytes: &[u8],
        sync_source_root: bool,
    ) -> std::io::Result<()> {
        let path = if target {
            target_backup_path(source, owner)
        } else {
            original_path(source, owner)
        };
        let _pin = os::pin_multipart_source_directory(source, base, root)?;
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("source backup has no parent"))?;
        let _parents = os::mkdir_all_below_existing_base_std(parent, base, root)?;
        let existed = if target {
            match os::observe_multipart_source_metadata(&path, base, root, bytes.len() as u64) {
                Ok((length, sha)) if length == bytes.len() as u64 && sha == raw_sha(bytes) => true,
                Ok(_) => {
                    return Err(IoError::new(std::io::ErrorKind::AlreadyExists, "immutable target backup changed"));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error),
            }
        } else if let Some(previous) = read_optional(&path, base, root).map_err(IoError::other)? {
            if previous.as_ref() != bytes {
                return Err(IoError::new(std::io::ErrorKind::AlreadyExists, "immutable source backup changed"));
            }
            true
        } else {
            false
        };
        if !existed {
            os::replace_multipart_source_metadata(&path, bytes, base, root)?;
        }
        os::sync_multipart_owned_record_parents_options(source, parent, existed, sync_source_root, base, root)
    }
    pub async fn replace_head(&self, bytes: &[u8]) -> Result<()> {
        invalidate_active_multipart_source_head(&self.source);
        let path = self.source.join(STORAGE_FORMAT_FILE);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let bytes = bytes.to_vec();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            os::replace_multipart_source_metadata(&path, &bytes, &base, &root)
        })
        .await
        .map_err(DiskError::from)
    }
    /// Writes only the head; the caller persists its fence and owns revisions.
    pub async fn restore_original(&self, state: &mut SourceState, recovering: bool, ready: Option<[u8; 32]>) -> Result<()> {
        if state.phase == MultipartSourcePhase::Aborting || state.abort.is_some() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let loaded = self.load().await?;
        let actual = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        if encode(actual)? != encode(state)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let raw = loaded.original.as_ref().ok_or(DiskError::FileCorrupt)?;
        let allowed = match loaded.head.as_ref() {
            None => {
                state.phase == MultipartSourcePhase::Recovering
                    && state.recovery.is_some()
                    && (state.negative_certificate_sha256.is_some()
                        || state.undo.as_ref().is_some_and(|intent| intent.target_undone))
            }
            Some(head) if head.sha256 == state.original_sha256 || Some(head.sha256) == state.prepared_sha256 => true,
            Some(head)
                if state.restore_from.as_ref().is_some_and(|from| {
                    state.negative_certificate_sha256.is_some()
                        && state.recovery.as_ref().is_some_and(|round| {
                            round.generation == state.generation
                                && from.generation == round.from_generation
                                && from.disk_id == self.disk_id
                                && from.physical_slot == self.physical_slot
                                && from.lineage == state.lineage
                                && from.head_sha256 == Some(head.sha256)
                                && from.revision.checked_add(1) == Some(state.revision)
                        })
                }) =>
            {
                true
            }
            Some(head)
                if state.negative_claim.as_ref().is_some_and(|claim| {
                    Some(&claim.round) == state.recovery.as_ref()
                        && claim.round.generation == state.generation
                        && claim.from.disk_id == self.disk_id
                        && claim.from.physical_slot == self.physical_slot
                        && claim.from.lineage == state.lineage
                        && claim.from.generation == claim.round.from_generation
                        && claim.from.head_sha256 == Some(head.sha256)
                        && claim.from.revision.checked_add(if claim.owned_undo { 2 } else { 1 }) == Some(state.revision)
                }) =>
            {
                true
            }

            Some(_) => loaded.info.as_ref().is_some_and(|current| {
                let condition = SourceConditionV1 {
                    lineage: state.lineage.clone(),
                    generation: state.generation,
                    attempt: state.backup_owner,
                };
                condition.matches_original(current).unwrap_or(false)
                    && source_status(current).ok().flatten().is_some_and(|status| {
                        status.generation == state.generation
                            && status.round == state.recovery.as_ref().map(|round| round.round)
                            && ((status.recovering && status.ready_sha256.is_none())
                                || (!status.recovering
                                    && state.phase == MultipartSourcePhase::Active
                                    && status.ready_sha256 == state.ready_certificate_sha256
                                    && status.ready_sha256.is_some()))
                    })
            }),
        };
        if !allowed {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let mut info = file_info(raw, RUSTFS_META_MULTIPART_BUCKET, &self.path, state.lineage.original_version_id)?;
        strip_native_controls(&mut info.metadata);
        mark_native_source(&mut info);
        set_source_status(
            &mut info,
            &SourceStatus {
                generation: state.generation,
                recovering,
                round: state.recovery.as_ref().map(|round| round.round),
                ready_sha256: ready,
            },
        )?;
        let mut metadata = FileMeta::new();
        metadata.add_version(info).map_err(Error::other)?;
        self.replace_head(&metadata.marshal_msg().map_err(Error::other)?).await
    }
    pub async fn persist_certificate(&self, round: Uuid, ready: bool, certificate: &MultipartCertificate) -> Result<[u8; 32]> {
        if certificate.round.round != round || round.is_nil() {
            return Err(DiskError::FileCorrupt);
        }
        let raw = encode(certificate)?;
        let digest = raw_sha(&raw);
        let path = certificate_path(&self.source, round, ready);
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            let _pin = os::pin_multipart_source_directory(&source, &base, &root)?;
            let parent = path.parent().ok_or_else(|| IoError::other("certificate has no parent"))?;
            let _parents = os::mkdir_all_below_existing_base_std(parent, &base, &root)?;
            let existed = if let Some(previous) = read_optional(&path, &base, &root).map_err(IoError::other)? {
                if previous.as_ref() != raw.as_slice() {
                    return Err(IoError::new(std::io::ErrorKind::AlreadyExists, "immutable recovery certificate changed"));
                }
                true
            } else {
                false
            };
            if !existed {
                os::replace_multipart_source_metadata(&path, &raw, &base, &root)?;
            }
            os::sync_multipart_owned_record_parents(&source, parent, existed, &base, &root)
        })
        .await?;
        Ok(digest)
    }
    pub async fn read_certificate(&self, round: Uuid, ready: bool) -> Result<MultipartCertificate> {
        let loaded = self.load().await?;
        let state = loaded.state.as_ref().ok_or(DiskError::FileCorrupt)?;
        let expected = if ready {
            state.ready_certificate_sha256
        } else {
            state.negative_certificate_sha256
        }
        .ok_or(DiskError::FileCorrupt)?;
        let active_round = state.recovery.as_ref().ok_or(DiskError::FileCorrupt)?;
        if active_round.round != round {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let path = certificate_path(&self.source, round, ready);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let bytes = os::run_blocking_namespace_operation(self.lease.clone(), move || {
            os::read_multipart_source_metadata(&path, &base, &root)
        })
        .await?;
        if raw_sha(&bytes) != expected {
            return Err(DiskError::FileCorrupt);
        }
        let mut certificate: MultipartCertificate = decode(&bytes)?;
        let token = ValidatedOwnedCertificate {
            digest: expected,
            round: active_round.clone(),
            kind: if ready {
                MultipartProofKind::Restored
            } else {
                MultipartProofKind::Claimed
            },
        };
        hydrate_owned_certificate(&mut certificate, &self.authority, &token)?;
        Ok(certificate)
    }
    pub fn target_path(&self, publication: &PublicationIdentity) -> Result<PathBuf> {
        publication.lineage.validate()?;
        let (bucket, path) =
            super::build_local_disk_object_path(&self.base, &publication.lineage.bucket, &publication.lineage.object);
        super::check_local_disk_valid_object_path(&self.base, &bucket, &path)?;
        Ok(path)
    }
    pub async fn read_target(&self, publication: &PublicationIdentity) -> Result<Option<Bytes>> {
        let path = self.target_path(publication)?.join(STORAGE_FORMAT_FILE);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let _ = base;
        let (bytes, metadata) = super::read_file_exists(&path, &root).await?;
        Ok(metadata.map(|_| bytes))
    }
    pub fn target_matches(&self, publication: &PublicationIdentity, raw: &[u8]) -> Result<bool> {
        Self::target_matches_for_slot(publication, raw, self.physical_slot, self.disk_ids.len())
    }
    pub(super) fn target_matches_for_slot(
        publication: &PublicationIdentity,
        raw: &[u8],
        physical_slot: usize,
        disk_count: usize,
    ) -> Result<bool> {
        let info = file_info(raw, &publication.lineage.bucket, &publication.lineage.object, publication.version_id)?;
        let slot = publication
            .lineage
            .geometry
            .distribution
            .iter()
            .position(|index| *index == info.erasure.index)
            .ok_or(DiskError::FileCorrupt)?;
        let condition = SourceConditionV1 {
            lineage: publication.lineage.clone(),
            generation: publication.source_generation,
            attempt: publication.attempt,
        };
        let mut candidate = info.clone();
        let actual = condition.attach(&mut candidate)?;
        Ok(actual == *publication
            && publication_identity(&info)?.as_ref() == Some(publication)
            && info.erasure.data_blocks == publication.lineage.geometry.data_blocks
            && info.erasure.parity_blocks == publication.lineage.geometry.parity_blocks
            && info.erasure.block_size == publication.lineage.geometry.block_size
            && info.erasure.distribution == publication.lineage.geometry.distribution
            && slot == physical_slot
            && publication.lineage.geometry.disks() == disk_count)
    }
    pub async fn restore_exact_target(&self, state: &SourceState, owner: Option<Arc<dyn Send + Sync>>) -> Result<()> {
        let publication = state.publication.as_ref().ok_or(DiskError::FileCorrupt)?;
        let target = self.target_path(publication)?;
        let logical = self
            .root
            .join(target.strip_prefix(&self.base).map_err(|_| DiskError::InvalidPath)?);
        let lease = os::acquire_namespace_mutation_lease_with_owner(&logical, owner).await;
        self.restore_exact_target_with_lease(state, lease).await
    }
    async fn restore_exact_target_with_lease(&self, state: &SourceState, lease: Arc<os::NamespaceMutationLease>) -> Result<()> {
        let publication = state.publication.as_ref().ok_or(DiskError::FileCorrupt)?;
        // A resumed owned Undo may have published its exact predecessor before
        // persisting target_undone. Its full, unchanged intent is the authority;
        // an arbitrary absent target or an old target digest alone is not.
        let owned_retry = if let Some(undo) = state.undo.as_ref() {
            let loaded = self.load().await?;
            let current = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
            state.phase == MultipartSourcePhase::UndoIntent
                && encode(current)? == encode(state)?
                && loaded.head.is_none()
                && undo.from.disk_id == self.disk_id
                && undo.from.physical_slot == self.physical_slot
                && undo.from.lineage == state.lineage
                && undo.from.generation == state.generation
                && undo.from.publication.as_ref() == Some(publication)
                && undo.from.head_sha256.is_none()
                && matches!(undo.from.phase, MultipartSourcePhase::Prepared | MultipartSourcePhase::ConsumedUnsealed)
                && undo.from.revision.checked_add(1) == Some(state.revision)
                && undo.from.capability_sha256 == Some(undo.capability_sha256)
                && state.capability_sha256 == Some(undo.capability_sha256)
                && undo.round.lineage == state.lineage
                && undo.round.from_generation == state.generation
                && undo.round.old_attempt == publication.attempt
        } else {
            false
        };
        let previous = target_backup_path(&self.source, state.backup_owner);
        let predecessor = if let Some(expected) = state.previous_target_sha256 {
            let (bytes, metadata) = super::read_file_exists(&previous, &self.publication_root).await?;
            if metadata.is_none() || Some(bytes.len() as u64) != state.previous_target_length || raw_sha(&bytes) != expected {
                return Err(DiskError::FileCorrupt);
            }
            Some(bytes)
        } else {
            None
        };
        let current = self.read_target(publication).await?;
        let is_published = if let Some(raw) = current.as_ref() {
            Some(raw_sha(raw)) == state.prepared_sha256
                && Some(raw.len() as u64) == state.prepared_length
                && self.target_matches(publication, raw)?
        } else {
            false
        };
        let already_undone = owned_retry
            && current.as_ref().map(|raw| raw_sha(raw)) == state.previous_target_sha256
            && current.as_ref().map(|raw| raw.len() as u64) == state.previous_target_length
            && current.is_some() == predecessor.is_some();
        if !is_published && !already_undone {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let object = self.target_path(publication)?;
        let target = object.join(STORAGE_FORMAT_FILE);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(lease, move || {
            let _pin = os::pin_multipart_source_directory(&object, &base, &root)?;
            if already_undone {
                if predecessor.is_some() {
                    os::sync_file(&target)?;
                }
                os::fsync_dir_std(&object)?;
            } else if let Some(bytes) = predecessor {
                os::replace_multipart_source_metadata(&target, &bytes, &base, &root)?;
            } else {
                std::fs::remove_file(&target)?;
                os::fsync_dir_std(&object)?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }
    pub async fn undo_exact_publication(&self, state: &mut SourceState, owner: Option<Arc<dyn Send + Sync>>) -> Result<()> {
        self.restore_exact_target(state, owner).await?;
        state.undo.as_mut().ok_or(DiskError::FileCorrupt)?.target_undone = true;
        Ok(())
    }
}

pub(super) fn load_source(
    source: &Path,
    base: &Path,
    root: &os::PublicationRoot,
    path: &str,
    disk_id: Uuid,
    physical_slot: usize,
    disk_ids: &[Uuid],
) -> Result<LoadedSource> {
    let pinned = match os::pin_multipart_source_directory(source, base, root) {
        Ok(pinned) => pinned,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LoadedSource {
                root_missing: true,
                state: None,
                head: None,
                original: None,
                info: None,
                snapshot: None,
                head_matches_state: false,
                head_matches_activation: false,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let state_raw = read_optional_in_pinned(&pinned, source, STATE, base, root)?;
    let state: Option<SourceState> = state_raw.as_deref().map(decode).transpose()?;
    if state.is_none() && os::multipart_source_has_native_controls_in_pinned(source, &pinned)? {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    let head = if let Some(state) = state
        .as_ref()
        .filter(|state| state.prepared_length.is_some_and(|len| len > 64 * 1024))
    {
        let head_path = source.join(STORAGE_FORMAT_FILE);
        let maximum = state
            .prepared_length
            .unwrap_or(0)
            .max(MULTIPART_SOURCE_MAX_PAYLOAD_BYTES as u64);
        match os::observe_multipart_source_metadata(&head_path, base, root, maximum) {
            Ok((length, sha256)) => {
                let is_proxy = Some(sha256) == state.prepared_sha256 && Some(length) == state.prepared_length;
                let raw = if is_proxy {
                    None
                } else {
                    let raw = read_optional_in_pinned(&pinned, source, STORAGE_FORMAT_FILE, base, root)?
                        .ok_or(DiskError::FileCorrupt)?;
                    if raw_sha(&raw) != sha256 {
                        return Err(DiskError::FileCorrupt);
                    }
                    Some(raw)
                };
                Some(SourceHead { sha256, length, raw })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        }
    } else {
        read_optional_in_pinned(&pinned, source, STORAGE_FORMAT_FILE, base, root)?.map(|raw| {
            let sha256 = raw_sha(&raw);
            let length = raw.len() as u64;
            let is_proxy = state
                .as_ref()
                .is_some_and(|state| Some(sha256) == state.prepared_sha256 && Some(length) == state.prepared_length);
            SourceHead {
                sha256,
                length,
                raw: (!is_proxy).then_some(raw),
            }
        })
    };
    let mut original = None;
    let mut info = None;
    let mut snapshot = None;
    let mut head_matches_state = false;
    let mut head_matches_activation = false;
    if let Some(state) = &state {
        state.validate(disk_id, path)?;
        if state.physical_slot != physical_slot || state.lineage.geometry.disks() != disk_ids.len() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let raw = read_optional(&original_path(source, state.backup_owner), base, root)?.ok_or(DiskError::FileCorrupt)?;
        if raw_sha(&raw) != state.original_sha256 {
            return Err(DiskError::FileCorrupt);
        }
        let original_info = file_info(&raw, RUSTFS_META_MULTIPART_BUCKET, path, state.lineage.original_version_id)?;
        native_source_protocol(&original_info)?;
        super::multipart_source_abort::validate_retired_head(source, base, root, state, head.as_ref())?;
        let condition = SourceConditionV1 {
            lineage: state.lineage.clone(),
            generation: state.generation,
            attempt: state.backup_owner,
        };
        if !condition.matches_original(&original_info)?
            || state.lineage.geometry.distribution[state.physical_slot] != original_info.erasure.index
        {
            return Err(DiskError::FileCorrupt);
        }
        original = Some(raw);
        if let Some(head) = &head {
            if Some(head.sha256) == state.prepared_sha256 && Some(head.length) == state.prepared_length {
                head_matches_state = state.phase == MultipartSourcePhase::Prepared;
            } else {
                let raw = head.raw().ok_or(DiskError::FileCorrupt)?;
                let current = file_info(raw, RUSTFS_META_MULTIPART_BUCKET, path, state.lineage.original_version_id)?;
                if matches!(state.phase, MultipartSourcePhase::Active | MultipartSourcePhase::Recovering) {
                    if condition.matches_original(&current)? {
                        let status = source_status(&current)?;
                        let same = status.as_ref().is_some_and(|status| {
                            status.generation == state.generation
                                && status.round == state.recovery.as_ref().map(|round| round.round)
                        });
                        head_matches_state = if state.generation.epoch == 0 && status.is_none() {
                            state.phase == MultipartSourcePhase::Active
                        } else {
                            same && status.as_ref().is_some_and(|status| {
                                status.recovering == (state.phase == MultipartSourcePhase::Recovering)
                                    && status.ready_sha256 == state.ready_certificate_sha256
                            })
                        };
                        head_matches_activation = same
                            && state.ready_certificate_sha256.is_some()
                            && status.as_ref().is_some_and(|status| {
                                (status.recovering && status.ready_sha256.is_none())
                                    || (!status.recovering && status.ready_sha256 == state.ready_certificate_sha256)
                            });
                    }
                } else if state.phase == MultipartSourcePhase::Prepared {
                    head_matches_state = head.sha256 == state.original_sha256;
                }
                info = Some(current);
            }
        }
        let head_sha = head.as_ref().map(|head| head.sha256);
        snapshot = Some(snapshot_for_state(
            disk_id,
            state,
            state_raw.as_ref().ok_or(DiskError::FileCorrupt)?,
            head_sha,
        )?);
    } else if let Some(observed) = &head {
        let raw = observed.raw().ok_or(DiskError::FileCorrupt)?;
        let current = get_file_info(
            raw,
            RUSTFS_META_MULTIPART_BUCKET,
            path,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .map_err(Error::other)?;
        native_source_protocol(&current)?;
        if source_status(&current)?.is_some()
            || publication_identity(&current)?.is_some()
            || SourceConditionV1::take(&mut current.clone())?.is_some()
        {
            return Err(DiskError::FileCorrupt);
        }
        let bucket = rustfs_utils::http::metadata_compat::get_consistent_str(&current.metadata, "multipart-bucket")
            .ok_or(DiskError::FileCorrupt)?;
        let object = rustfs_utils::http::metadata_compat::get_consistent_str(&current.metadata, "multipart-object")
            .ok_or(DiskError::FileCorrupt)?;
        let condition =
            capture_source_condition(bucket, object, path, &current, current.data_dir.ok_or(DiskError::FileCorrupt)?)?;
        if condition.generation.epoch != 0 {
            return Err(DiskError::FileCorrupt);
        }
        let slot = condition
            .lineage
            .geometry
            .distribution
            .iter()
            .position(|index| *index == current.erasure.index)
            .ok_or(DiskError::FileCorrupt)?;
        if slot != physical_slot || condition.lineage.geometry.disks() != disk_ids.len() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let sha = raw_sha(raw);
        let inventory = raw_sha(&encode(&(None::<[u8; 32]>, Some(sha), sha, None::<[u8; 32]>, None::<[u8; 32]>))?);
        snapshot = Some(MultipartSourceSnapshot {
            disk_id,
            physical_slot: slot,
            lineage: condition.lineage,
            generation: condition.generation,
            revision: 0,
            phase: MultipartSourcePhase::Active,
            state_sha256: [0; 32],
            head_sha256: Some(sha),
            inventory_sha256: inventory,
            capability_sha256: None,
            publication: None,
        });
        original = Some(raw.clone());
        info = Some(current);
        head_matches_state = true;
    }
    Ok(LoadedSource {
        root_missing: false,
        state,
        head,
        original,
        info,
        snapshot,
        head_matches_state,
        head_matches_activation,
    })
}

fn load_uninitialized_source(
    raw: Bytes,
    path: &str,
    disk_id: Uuid,
    physical_slot: usize,
    disk_ids: &[Uuid],
) -> Result<LoadedSource> {
    let sha = raw_sha(&raw);
    let length = raw.len() as u64;
    let current = get_file_info(
        &raw,
        RUSTFS_META_MULTIPART_BUCKET,
        path,
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .map_err(Error::other)?;
    native_source_protocol(&current)?;
    if source_status(&current)?.is_some()
        || publication_identity(&current)?.is_some()
        || SourceConditionV1::take(&mut current.clone())?.is_some()
    {
        return Err(DiskError::FileCorrupt);
    }
    let bucket = rustfs_utils::http::metadata_compat::get_consistent_str(&current.metadata, "multipart-bucket")
        .ok_or(DiskError::FileCorrupt)?;
    let object = rustfs_utils::http::metadata_compat::get_consistent_str(&current.metadata, "multipart-object")
        .ok_or(DiskError::FileCorrupt)?;
    let condition = capture_source_condition(bucket, object, path, &current, current.data_dir.ok_or(DiskError::FileCorrupt)?)?;
    if condition.generation.epoch != 0 {
        return Err(DiskError::FileCorrupt);
    }
    let slot = condition
        .lineage
        .geometry
        .distribution
        .iter()
        .position(|index| *index == current.erasure.index)
        .ok_or(DiskError::FileCorrupt)?;
    if slot != physical_slot || condition.lineage.geometry.disks() != disk_ids.len() {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    let inventory = raw_sha(&encode(&(None::<[u8; 32]>, Some(sha), sha, None::<[u8; 32]>, None::<[u8; 32]>))?);
    let snapshot = Some(MultipartSourceSnapshot {
        disk_id,
        physical_slot: slot,
        lineage: condition.lineage,
        generation: condition.generation,
        revision: 0,
        phase: MultipartSourcePhase::Active,
        state_sha256: [0; 32],
        head_sha256: Some(sha),
        inventory_sha256: inventory,
        capability_sha256: None,
        publication: None,
    });
    Ok(LoadedSource {
        root_missing: false,
        state: None,
        head: Some(SourceHead {
            sha256: sha,
            length,
            raw: Some(raw.clone()),
        }),
        original: Some(raw),
        info: Some(current),
        snapshot,
        head_matches_state: true,
        head_matches_activation: false,
    })
}

pub(super) const RETIREMENT_ANCHOR: &str = ".mpu-source-retirement";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRetirementAnchor {
    protocol_version: u16,
    disk_id: Uuid,
    physical_slot: usize,
    publication: PublicationIdentity,
    source_original_sha256: [u8; 32],
    source_validation_receipt_id: Uuid,
}

/// Runtime-only authority from a durable, exact owned Undo. Neither callers nor
/// serialized DeleteOptions can create the missing-FI reclamation exception.
pub(super) struct OwnedMultipartReclaim {
    root: PathBuf,
    base: PathBuf,
    publication_root: os::PublicationRoot,
    disk_id: Uuid,
    physical_slot: usize,
    disk_ids: Vec<Uuid>,
    publication: PublicationIdentity,
    source_original_sha256: [u8; 32],
    source_validation_receipt_id: Uuid,
}

fn validate_retirement_anchor(
    bytes: &[u8],
    disk_id: Uuid,
    physical_slot: usize,
    publication: &PublicationIdentity,
) -> Result<NativeRetirementAnchor> {
    let anchor: NativeRetirementAnchor = decode(bytes)?;
    if anchor.protocol_version != MULTIPART_SOURCE_PROTOCOL_VERSION
        || anchor.disk_id != disk_id
        || anchor.physical_slot != physical_slot
        || anchor.publication != *publication
        || anchor.source_validation_receipt_id.is_nil()
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(anchor)
}

impl SourceStore {
    pub async fn owned_undo_reclaim(&self, state: &SourceState) -> Result<Arc<OwnedMultipartReclaim>> {
        let loaded = self.load().await?;
        let current = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        let publication = state.publication.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        let claim = state.negative_claim.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        let undo = state.undo.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        if encode(current)? != encode(state)?
            || state.phase != MultipartSourcePhase::Recovering
            || !loaded.head_matches_state
            || state.capability_sha256.is_some()
            || !claim.owned_undo
            || !undo.target_undone
            || undo.from != claim.from
            || undo.round != claim.round
            || state.recovery.as_ref() != Some(&claim.round)
            || state.generation != claim.round.generation
            || state.lineage != claim.round.lineage
            || publication.lineage != state.lineage
            || publication.source_generation != claim.round.from_generation
            || publication.attempt != claim.round.old_attempt
            || claim.from.disk_id != self.disk_id
            || claim.from.physical_slot != self.physical_slot
            || claim.from.lineage != state.lineage
            || claim.from.generation != publication.source_generation
            || claim.from.publication.as_ref() != Some(publication)
            || claim.from.head_sha256.is_some()
            || claim.from.capability_sha256 != Some(undo.capability_sha256)
            || !matches!(claim.from.phase, MultipartSourcePhase::Prepared | MultipartSourcePhase::ConsumedUnsealed)
            || claim.from.revision.checked_add(2) != Some(state.revision)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(Arc::new(OwnedMultipartReclaim {
            root: self.root.clone(),
            base: self.base.clone(),
            publication_root: self.publication_root.clone(),
            disk_id: self.disk_id,
            physical_slot: self.physical_slot,
            disk_ids: self.disk_ids.clone(),
            publication: publication.clone(),
            source_original_sha256: state.original_sha256,
            source_validation_receipt_id: state.backup_owner,
        }))
    }
}

impl LocalDisk {
    pub(super) async fn reclaim_owned_multipart_attempt(&self, store: &SourceStore, state: &SourceState) -> Result<()> {
        let proof = store.owned_undo_reclaim(state).await?;
        let key = super::SnapshotLeaseKey {
            volume: proof.publication.lineage.bucket.clone(),
            path: format!("{}/{}", proof.publication.lineage.object, proof.publication.final_data_dir),
        };
        let Some(claim) = self.claim_snapshot_delete(key, super::SnapshotPendingDelete::OwnedMultipart(proof.clone())) else {
            return Ok(());
        };
        self.reclaim_owned_multipart_data_dir(proof, store.owner.clone(), claim).await
    }

    pub(super) async fn reclaim_owned_multipart_data_dir(
        &self,
        proof: Arc<OwnedMultipartReclaim>,
        owner: Option<Arc<dyn Send + Sync>>,
        claim: super::SnapshotLeaseDeleteClaim,
    ) -> Result<()> {
        if self.root != proof.root || self.io_root() != proof.base || self.get_disk_id().await? != Some(proof.disk_id) {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let format = self.format_info.read().await.data.clone();
        let format = crate::disk::format::FormatV3::try_from(format.as_ref()).map_err(|_| DiskError::CorruptedBackend)?;
        let (set, slot) = format.find_disk_index_by_disk_id(proof.disk_id)?;
        if slot != proof.physical_slot || format.erasure.sets.get(set) != Some(&proof.disk_ids) {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let volume = proof.publication.lineage.bucket.clone();
        let object = proof.publication.lineage.object.clone();
        let path = format!("{object}/{}", proof.publication.final_data_dir);
        if claim.key.volume != volume || claim.key.path != path {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        // Snapshot admission precedes fresh metadata -> DD namespace ownership.
        // The private proof retains the pinned root, never the old upload lock.
        let metadata_owner = os::acquire_metadata_mutation_lease(&self.get_object_path(&volume, &object)?, owner).await;
        if self.has_delete_transaction_evidence(&volume, &object).await? {
            return Err(DiskError::DiskOngoingReq);
        }
        let object_path = self.io_get_object_path(&volume, &object)?;
        let directory = object_path.join(proof.publication.final_data_dir.to_string());
        let lease = os::acquire_namespace_mutation_lease_with_owner(&directory, Some(metadata_owner)).await;
        let sync = super::effective_durability(&volume).syncs_commit_metadata();
        let result = os::run_blocking_namespace_operation_with_sync(lease, sync, move || {
            let result = (|| {
                let base = &proof.base;
                let root = &proof.publication_root;
                let _parent = match os::pin_multipart_source_directory(&object_path, base, root) {
                    Ok(parent) => parent,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                match os::read_multipart_reclaim_target_metadata(&object_path.join(STORAGE_FORMAT_FILE), base, root) {
                    Ok(bytes) => {
                        let metadata = FileMeta::load(&bytes).map_err(IoError::other)?;
                        if metadata
                            .get_data_dirs()
                            .map_err(IoError::other)?
                            .into_iter()
                            .flatten()
                            .any(|referenced| referenced == proof.publication.final_data_dir)
                        {
                            return Err(DiskError::ConditionalDeleteConflict.into());
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                let parts = match super::orphan_data_dir_parts(&directory) {
                    Ok(parts) => parts,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return if sync { os::fsync_dir_std(&object_path) } else { Ok(()) };
                    }
                    Err(error) => return Err(error),
                };
                let _directory = os::pin_multipart_source_directory(&directory, base, root)?;
                if !parts.is_empty() {
                    let anchor_path = directory.join(RETIREMENT_ANCHOR);
                    let bytes = os::read_multipart_source_metadata(&anchor_path, base, root)?;
                    let anchor = validate_retirement_anchor(&bytes, proof.disk_id, proof.physical_slot, &proof.publication)?;
                    if anchor.source_original_sha256 != proof.source_original_sha256
                        || anchor.source_validation_receipt_id != proof.source_validation_receipt_id
                    {
                        return Err(DiskError::ConditionalDeleteConflict.into());
                    }
                    // Keep the exact owner anchor through every payload unlink and
                    // its barrier so a failed or cancelled reclaim remains retryable.
                    for part in parts.into_iter().filter(|part| part != &anchor_path) {
                        #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                        os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &part);
                        match std::fs::remove_file(part) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                    if sync {
                        os::fsync_dir_std(&directory)?;
                    }
                    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                    os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &anchor_path);
                    std::fs::remove_file(anchor_path)?;
                    if sync {
                        os::fsync_dir_std(&directory)?;
                    }
                }
                // An anchor-less empty DD is only rmdir'ed. A new child makes
                // this fail closed; no recursive removal can consume its bytes.
                std::fs::remove_dir(&directory)?;
                if sync {
                    os::fsync_dir_std(&object_path)?;
                }
                Ok(())
            })();
            // The actual callback owns this registry claim even if its async
            // waiter is cancelled while the physical unlink is still running.
            claim.finish(result.is_ok());
            result
        })
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::InvalidData {
                DiskError::FileCorrupt
            } else {
                super::to_file_error(error).into()
            }
        });
        self.io_backend.invalidate_cached_fds_under(&volume, &path);
        result
    }
}

impl SourceStore {
    /// This caller already owns SourceRW and destination metadata ownership.
    /// An Aborting survivor can confirm a historical target only with its
    /// independent, same-disk publication anchor. Closure never creates one.
    pub async fn validate_cleanup_target(&self, publication: &PublicationIdentity) -> Result<()> {
        publication.lineage.validate()?;
        let loaded = self.load().await?;
        let state = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        if state.phase != MultipartSourcePhase::Aborting
            || loaded.head.is_some()
            || state.lineage != publication.lineage
            || self.read_abort_certificate(&loaded).await?.is_none()
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let raw = self
            .read_target(publication)
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        if !self.target_matches(publication, &raw)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let anchor_path = self
            .target_path(publication)?
            .join(publication.final_data_dir.to_string())
            .join(RETIREMENT_ANCHOR);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let bytes = os::run_blocking_namespace_operation(self.lease.clone(), move || {
            os::read_multipart_source_metadata(&anchor_path, &base, &root)
        })
        .await?;
        validate_retirement_anchor(&bytes, self.disk_id, self.physical_slot, publication)?;
        self.sync_target(publication).await
    }
    /// No absent-source operation creates a head, record, capability or anchor.
    pub async fn confirm_root_missing(&self, publication: &PublicationIdentity, challenge: Uuid) -> Result<MultipartSourceProof> {
        if self.path != publication.lineage.path
            || publication.lineage.geometry.disks() != self.disk_ids.len()
            || challenge.is_nil()
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let raw = self
            .read_target(publication)
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        if !self.target_matches(publication, &raw)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let anchor_path = self
            .target_path(publication)?
            .join(publication.final_data_dir.to_string())
            .join(RETIREMENT_ANCHOR);
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let bytes = os::run_blocking_namespace_operation(self.lease.clone(), move || {
            match os::pin_multipart_source_directory(&source, &base, &root) {
                Ok(_) => {
                    return Err(IoError::new(std::io::ErrorKind::AlreadyExists, "multipart source remains present"));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            os::read_multipart_source_metadata(&anchor_path, &base, &root)
        })
        .await?;
        validate_retirement_anchor(&bytes, self.disk_id, self.physical_slot, publication)?;
        self.sync_target(publication).await?;
        self.sync_source_absence().await?;
        let anchor_sha = raw_sha(&bytes);
        let snapshot = MultipartSourceSnapshot {
            disk_id: self.disk_id,
            physical_slot: self.physical_slot,
            lineage: publication.lineage.clone(),
            generation: publication.source_generation,
            revision: 0,
            phase: MultipartSourcePhase::RootMissing,
            state_sha256: anchor_sha,
            head_sha256: None,
            inventory_sha256: raw_sha(&encode(&(raw_sha(&raw), anchor_sha))?),
            capability_sha256: None,
            publication: Some(publication.clone()),
        };
        self.issue_proof(MultipartProofKind::RootMissingTerminal, challenge, None, snapshot)
    }
}

/// One publication owns one capability. Neither response replay nor a dropped
/// async waiter can reconstruct it from its persisted hash.
/// Issued only after the locally encoded metadata passed full FI validation.
/// This token is never serialized or reconstructed from a caller's bytes.
struct ValidatedPreparedPublication {
    publication: PublicationIdentity,
    sha256: [u8; 32],
    length: u64,
}

type DeferredInitialPublication = (Vec<u8>, PathBuf, Vec<u8>);

pub(super) struct NativeMultipartPublication {
    pub store: Arc<SourceStore>,
    pub publication: PublicationIdentity,
    pub prepared_path: PathBuf,
    state: parking_lot::Mutex<SourceState>,
    capability: parking_lot::Mutex<Option<MultipartUndoCapability>>,
    validated_prepared: parking_lot::Mutex<Option<ValidatedPreparedPublication>>,
    deferred_initial: parking_lot::Mutex<Option<DeferredInitialPublication>>,
    pub legacy: bool,
}

impl LocalDisk {
    #[cfg(test)]
    pub(super) async fn prepare_native_multipart_publication(
        &self,
        path: &str,
        incoming: &mut FileInfo,
        staging: &MultipartCommitStaging,
    ) -> Result<NativeMultipartPublication> {
        self.prepare_native_multipart_publication_inner(path, incoming, staging, false)
            .await
    }

    pub(super) async fn prepare_native_multipart_publication_deferred(
        &self,
        path: &str,
        incoming: &mut FileInfo,
        staging: &MultipartCommitStaging,
    ) -> Result<NativeMultipartPublication> {
        self.prepare_native_multipart_publication_inner(path, incoming, staging, true)
            .await
    }

    async fn prepare_native_multipart_publication_inner(
        &self,
        path: &str,
        incoming: &mut FileInfo,
        staging: &MultipartCommitStaging,
        defer_persist: bool,
    ) -> Result<NativeMultipartPublication> {
        // Real Complete is parts-backed, including a zero-byte last part.
        if incoming.data.is_some() || incoming.inline_data() {
            return Err(DiskError::UnsupportedDisk);
        }
        let store = Arc::new(self.multipart_store_from_staging(path, staging).await?);
        let loaded = if defer_persist && let Some(raw) = staging.fresh_source_head() {
            load_uninitialized_source(raw, &store.path, store.disk_id, store.physical_slot, &store.disk_ids)?
        } else {
            store.load().await?
        };
        let snapshot = loaded.snapshot.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        let original = loaded
            .head
            .as_ref()
            .and_then(SourceHead::raw)
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        let info = loaded.info.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        if !loaded.head_matches_state || snapshot.phase != MultipartSourcePhase::Active || snapshot.capability_sha256.is_some() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let existing_publication = publication_identity(incoming)?;
        let supplied = SourceConditionV1::take(incoming)?;
        let legacy = supplied.is_none();
        let marked = native_source_protocol(info)?;
        if marked == legacy {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let condition = match supplied {
            Some(condition) => {
                condition.validate()?;
                if condition.lineage != snapshot.lineage
                    || condition.generation != snapshot.generation
                    || !condition.matches_original(info)?
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                condition
            }
            None => {
                if snapshot.generation.epoch != 0
                    || incoming.data_dir != Some(snapshot.lineage.original_data_dir)
                    || existing_publication.is_some()
                {
                    return Err(DiskError::UnsupportedDisk);
                }
                capture_source_condition(
                    &snapshot.lineage.bucket,
                    &snapshot.lineage.object,
                    path,
                    info,
                    incoming.data_dir.ok_or(DiskError::FileCorrupt)?,
                )?
            }
        };
        let publication = if legacy {
            condition.attach(incoming)?
        } else {
            let publication = existing_publication.ok_or(DiskError::ConditionalDeleteConflict)?;
            if publication.lineage != condition.lineage
                || publication.source_generation != condition.generation
                || publication.attempt != condition.attempt
                || publication.version_id != incoming.version_id.filter(|version| !version.is_nil())
                || Some(publication.final_data_dir) != incoming.data_dir.filter(|directory| !directory.is_nil())
                || Some(publication.modified_unix_nanos) != incoming.mod_time.map(|modified| modified.unix_timestamp_nanos())
            {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            publication
        };
        strip_transport_condition(&mut incoming.metadata);
        let capability = MultipartUndoCapability::new();
        let owner = Uuid::new_v4();
        let revision = snapshot.revision.checked_add(1).ok_or(DiskError::FileCorrupt)?;
        let state = SourceState {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            disk_id: store.disk_id,
            physical_slot: store.physical_slot,
            lineage: condition.lineage,
            generation: condition.generation,
            revision,
            phase: MultipartSourcePhase::Prepared,
            publication: Some(publication.clone()),
            backup_owner: owner,
            original_sha256: raw_sha(original),
            prepared_sha256: None,
            prepared_length: None,
            previous_target_sha256: None,
            previous_target_length: None,
            capability_sha256: Some(capability.digest()),
            recovery: None,
            negative_certificate_sha256: None,
            ready_certificate_sha256: None,
            terminal_certificate: Vec::new(),
            negative_claim: None,
            undo: None,
            restore_from: None,
            abort: None,
        };
        // Copy the small source FI to an independent immutable inode. Its
        // canonical inode is never truncated to build the destination proxy.
        let directory = staging.private_directory(publication.final_data_dir);
        let anchor_record = NativeRetirementAnchor {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            disk_id: store.disk_id,
            physical_slot: store.physical_slot,
            publication: publication.clone(),
            source_original_sha256: state.original_sha256,
            source_validation_receipt_id: state.backup_owner,
        };
        let anchor_bytes = encode(&anchor_record)?;
        state.validate(store.disk_id, &store.path)?;
        let original_bytes = original.to_vec();
        let deferred_initial = if defer_persist {
            Some((original_bytes, directory, anchor_bytes))
        } else {
            let state_raw = encode(&state)?;
            let source_dir = store.source.clone();
            let base_dir = store.base.clone();
            let pub_root = store.publication_root.clone();
            let terminal = terminal_record_path(&source_dir, &state);
            let can_mark_presynced = has_active_multipart_source_head(&source_dir)
                && super::effective_durability(RUSTFS_META_MULTIPART_BUCKET).syncs_data_shards();
            os::run_blocking_namespace_operation_with_sync(staging.lease.clone(), true, move || {
                SourceStore::write_owned_backup_std(&source_dir, &base_dir, &pub_root, owner, false, &original_bytes, false)?;
                SourceStore::persist_state_std(&source_dir, &base_dir, &pub_root, &state_raw, terminal.as_deref(), false)?;
                let _parent = os::mkdir_all_below_existing_base_std(&directory, &base_dir, &pub_root)?;
                os::replace_multipart_source_metadata(&directory.join(RETIREMENT_ANCHOR), &anchor_bytes, &base_dir, &pub_root)?;
                #[cfg(target_os = "linux")]
                if can_mark_presynced {
                    super::mark_presynced_tmp_data_dir(directory);
                }
                Ok(())
            })
            .await
            .map_err(DiskError::from)?;
            None
        };
        let prepared_path = store.source.join(format!(".mpu-prepared-{owner}"));
        let context = NativeMultipartPublication {
            store,
            publication,
            prepared_path,
            state: parking_lot::Mutex::new(state),
            capability: parking_lot::Mutex::new(Some(capability)),
            validated_prepared: parking_lot::Mutex::new(None),
            deferred_initial: parking_lot::Mutex::new(deferred_initial),
            legacy,
        };
        Ok(context)
    }
}

impl NativeMultipartPublication {
    pub fn canonical_head(&self) -> PathBuf {
        self.store.source.join(STORAGE_FORMAT_FILE)
    }
    #[cfg(test)]
    pub async fn prepare_metadata(&self, new_metadata: &[u8], old_metadata: Option<&[u8]>) -> Result<()> {
        let mut state = self.state.lock().clone();
        // Validate the actual persisted representation once, after FileMeta's
        // canonicalization. No unvalidated input can install a source proxy.
        if !self.store.target_matches(&self.publication, new_metadata)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let expected_state_raw = encode(&state)?;
        let validated = ValidatedPreparedPublication {
            publication: self.publication.clone(),
            sha256: raw_sha(new_metadata),
            length: new_metadata.len() as u64,
        };
        if let Some(previous) = old_metadata {
            state.previous_target_sha256 = Some(raw_sha(previous));
            state.previous_target_length = Some(previous.len() as u64);
        }
        state.prepared_sha256 = Some(validated.sha256);
        state.prepared_length = Some(validated.length);
        state.advance(MultipartSourcePhase::Prepared)?;
        state.validate(self.store.disk_id, &self.store.path)?;
        let next_state_raw = encode(&state)?;
        let source = self.store.source.clone();
        let base = self.store.base.clone();
        let root = self.store.publication_root.clone();
        let path = self.store.path.clone();
        let disk_id = self.store.disk_id;
        let physical_slot = self.store.physical_slot;
        let disk_ids = self.store.disk_ids.clone();
        let backup_owner = state.backup_owner;
        let old_bytes = old_metadata.map(|b| b.to_vec());
        let terminal = terminal_record_path(&source, &state);
        os::run_blocking_namespace_operation_with_sync(self.store.lease.clone(), true, move || {
            let current = load_source(&source, &base, &root, &path, disk_id, physical_slot, &disk_ids).map_err(IoError::other)?;
            let current_state = current
                .state
                .as_ref()
                .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
            let current_raw = encode(current_state).map_err(IoError::other)?;
            if current_raw != expected_state_raw || !current.head_matches_state {
                return Err(IoError::other(DiskError::ConditionalDeleteConflict));
            }
            if let Some(previous) = old_bytes.as_deref() {
                SourceStore::write_owned_backup_std(&source, &base, &root, backup_owner, true, previous, false)?;
            }
            SourceStore::persist_state_std(&source, &base, &root, &next_state_raw, terminal.as_deref(), false)
        })
        .await
        .map_err(DiskError::from)?;
        *self.state.lock() = state;
        *self.validated_prepared.lock() = Some(validated);
        Ok(())
    }
    pub(super) fn prepare_and_install_head_on_thread(
        &self,
        new_metadata: &[u8],
        old_metadata: Option<&[u8]>,
        dst_file_path: &Path,
        directory_guard: &os::RenameCommitGuard,
    ) -> Result<os::PreparedRenameSource> {
        let mut state = self.state.lock().clone();
        if !self.store.target_matches(&self.publication, new_metadata)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let deferred_initial = self.deferred_initial.lock().take();
        let expected_state_raw = encode(&state)?;
        let expected_lineage = state.lineage.clone();
        let expected_generation = state.generation;
        let expected_revision = state.revision;
        let validated = ValidatedPreparedPublication {
            publication: self.publication.clone(),
            sha256: raw_sha(new_metadata),
            length: new_metadata.len() as u64,
        };
        if let Some(previous) = old_metadata {
            state.previous_target_sha256 = Some(raw_sha(previous));
            state.previous_target_length = Some(previous.len() as u64);
        }
        state.prepared_sha256 = Some(validated.sha256);
        state.prepared_length = Some(validated.length);
        state.advance(MultipartSourcePhase::Prepared)?;
        state.validate(self.store.disk_id, &self.store.path)?;
        let next_state_raw = encode(&state)?;
        let source = &self.store.source;
        let base = &self.store.base;
        let root = &self.store.publication_root;
        let path = &self.store.path;
        let disk_id = self.store.disk_id;
        let physical_slot = self.store.physical_slot;
        let disk_ids = &self.store.disk_ids;
        let backup_owner = state.backup_owner;
        let can_mark_presynced = has_active_multipart_source_head(source)
            && super::effective_durability(RUSTFS_META_MULTIPART_BUCKET).syncs_data_shards();
        let terminal = terminal_record_path(source, &state);
        let prepared_path = &self.prepared_path;
        let canonical = self.canonical_head();
        let prepared = (|| -> std::io::Result<os::PreparedRenameSource> {
            if let Some((original_bytes, directory, anchor_bytes)) = deferred_initial {
                #[cfg(any(test, feature = "test-util"))]
                {
                    let current =
                        load_source(source, base, root, path, disk_id, physical_slot, disk_ids).map_err(IoError::other)?;
                    let snapshot = current
                        .snapshot
                        .as_ref()
                        .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                    let current_head = current
                        .head
                        .as_ref()
                        .and_then(SourceHead::raw)
                        .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                    if !current.head_matches_state
                        || snapshot.phase != MultipartSourcePhase::Active
                        || snapshot.capability_sha256.is_some()
                        || snapshot.lineage != expected_lineage
                        || snapshot.generation != expected_generation
                        || snapshot.revision.checked_add(1) != Some(expected_revision)
                        || current_head != original_bytes.as_slice()
                    {
                        return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                    }
                }
                #[cfg(not(any(test, feature = "test-util")))]
                let _ = (expected_lineage, expected_generation, expected_revision);
                SourceStore::write_owned_backup_std(source, base, root, backup_owner, false, &original_bytes, false)?;
                if let Some(previous) = old_metadata {
                    SourceStore::write_owned_backup_std(source, base, root, backup_owner, true, previous, false)?;
                }
                SourceStore::persist_state_std(source, base, root, &next_state_raw, terminal.as_deref(), false)?;
                let _parent = os::mkdir_all_below_existing_base_std(&directory, base, root)?;
                os::replace_multipart_source_metadata(&directory.join(RETIREMENT_ANCHOR), &anchor_bytes, base, root)?;
                #[cfg(target_os = "linux")]
                if can_mark_presynced {
                    super::mark_presynced_tmp_data_dir(directory);
                }
            } else {
                let current = load_source(source, base, root, path, disk_id, physical_slot, disk_ids).map_err(IoError::other)?;
                let current_state = current
                    .state
                    .as_ref()
                    .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                let current_raw = encode(current_state).map_err(IoError::other)?;
                if current_raw != expected_state_raw || !current.head_matches_state {
                    return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                }
                if let Some(previous) = old_metadata {
                    SourceStore::write_owned_backup_std(source, base, root, backup_owner, true, previous, false)?;
                }
                SourceStore::persist_state_std(source, base, root, &next_state_raw, terminal.as_deref(), false)?;
            }
            #[cfg(test)]
            super::run_owned_file_before_open(prepared_path);
            let mut prepared_metadata_source =
                os::create_prepared_rename_source_with_commit_guard(prepared_path, dst_file_path, directory_guard)?;
            prepared_metadata_source.write_all(new_metadata, true)?;
            os::install_multipart_prepared_head(prepared_metadata_source, &canonical, base, root)
        })()
        .map_err(DiskError::from)?;
        *self.state.lock() = state;
        *self.validated_prepared.lock() = Some(validated);
        Ok(prepared)
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn prepare_and_install_head(
        &self,
        new_metadata: &[u8],
        old_metadata: Option<&[u8]>,
        dst_file_path: &Path,
        directory_guard: &os::RenameCommitGuard,
        mutation_lease: Arc<os::NamespaceMutationLease>,
    ) -> Result<os::PreparedRenameSource> {
        let mut state = self.state.lock().clone();
        if !self.store.target_matches(&self.publication, new_metadata)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let deferred_initial = self.deferred_initial.lock().take();
        let expected_state_raw = encode(&state)?;
        let expected_lineage = state.lineage.clone();
        let expected_generation = state.generation;
        let expected_revision = state.revision;
        let validated = ValidatedPreparedPublication {
            publication: self.publication.clone(),
            sha256: raw_sha(new_metadata),
            length: new_metadata.len() as u64,
        };
        if let Some(previous) = old_metadata {
            state.previous_target_sha256 = Some(raw_sha(previous));
            state.previous_target_length = Some(previous.len() as u64);
        }
        state.prepared_sha256 = Some(validated.sha256);
        state.prepared_length = Some(validated.length);
        state.advance(MultipartSourcePhase::Prepared)?;
        state.validate(self.store.disk_id, &self.store.path)?;
        let next_state_raw = encode(&state)?;
        let source = self.store.source.clone();
        let base = self.store.base.clone();
        let root = self.store.publication_root.clone();
        let path = self.store.path.clone();
        let disk_id = self.store.disk_id;
        let physical_slot = self.store.physical_slot;
        let disk_ids = self.store.disk_ids.clone();
        let backup_owner = state.backup_owner;
        #[cfg(target_os = "linux")]
        let can_mark_presynced = has_active_multipart_source_head(&source)
            && super::effective_durability(RUSTFS_META_MULTIPART_BUCKET).syncs_data_shards();
        let old_bytes = old_metadata.map(|b| b.to_vec());
        let new_bytes = new_metadata.to_vec();
        let terminal = terminal_record_path(&source, &state);
        let prepared_path = self.prepared_path.clone();
        let canonical = self.canonical_head();
        let dst_file_path = dst_file_path.to_path_buf();
        let owned_directory_guard = directory_guard.clone();
        let prepared = os::run_blocking_namespace_operation_with_sync(mutation_lease, true, move || {
            let current = load_source(&source, &base, &root, &path, disk_id, physical_slot, &disk_ids).map_err(IoError::other)?;
            if let Some((original_bytes, directory, anchor_bytes)) = deferred_initial {
                let snapshot = current
                    .snapshot
                    .as_ref()
                    .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                let current_head = current
                    .head
                    .as_ref()
                    .and_then(SourceHead::raw)
                    .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                if !current.head_matches_state
                    || snapshot.phase != MultipartSourcePhase::Active
                    || snapshot.capability_sha256.is_some()
                    || snapshot.lineage != expected_lineage
                    || snapshot.generation != expected_generation
                    || snapshot.revision.checked_add(1) != Some(expected_revision)
                    || current_head != original_bytes.as_slice()
                {
                    return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                }
                SourceStore::write_owned_backup_std(&source, &base, &root, backup_owner, false, &original_bytes, false)?;
                if let Some(previous) = old_bytes.as_deref() {
                    SourceStore::write_owned_backup_std(&source, &base, &root, backup_owner, true, previous, false)?;
                }
                SourceStore::persist_state_std(&source, &base, &root, &next_state_raw, terminal.as_deref(), false)?;
                let _parent = os::mkdir_all_below_existing_base_std(&directory, &base, &root)?;
                os::replace_multipart_source_metadata(&directory.join(RETIREMENT_ANCHOR), &anchor_bytes, &base, &root)?;
                #[cfg(target_os = "linux")]
                if can_mark_presynced {
                    super::mark_presynced_tmp_data_dir(directory);
                }
            } else {
                let current_state = current
                    .state
                    .as_ref()
                    .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                let current_raw = encode(current_state).map_err(IoError::other)?;
                if current_raw != expected_state_raw || !current.head_matches_state {
                    return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                }
                if let Some(previous) = old_bytes.as_deref() {
                    SourceStore::write_owned_backup_std(&source, &base, &root, backup_owner, true, previous, false)?;
                }
                SourceStore::persist_state_std(&source, &base, &root, &next_state_raw, terminal.as_deref(), false)?;
            }
            #[cfg(test)]
            super::run_owned_file_before_open(&prepared_path);
            let mut prepared_metadata_source =
                os::create_prepared_rename_source_with_commit_guard(&prepared_path, &dst_file_path, &owned_directory_guard)?;
            prepared_metadata_source.write_all(&new_bytes, true)?;
            os::install_multipart_prepared_head(prepared_metadata_source, &canonical, &base, &root)
        })
        .await
        .map_err(DiskError::from)?;
        *self.state.lock() = state;
        *self.validated_prepared.lock() = Some(validated);
        Ok(prepared)
    }

    #[cfg(target_os = "linux")]
    pub async fn prepare_and_install_head(
        self: &Arc<Self>,
        new_metadata: &[u8],
        old_metadata: Option<&[u8]>,
        dst_file_path: &Path,
        directory_guard: &os::RenameCommitGuard,
        mutation_lease: Arc<os::NamespaceMutationLease>,
    ) -> Result<os::PreparedRenameSource> {
        let this = Arc::clone(self);
        let new_bytes = new_metadata.to_vec();
        let old_bytes = old_metadata.map(|b| b.to_vec());
        let dst_file_path = dst_file_path.to_path_buf();
        let owned_directory_guard = directory_guard.clone();
        os::run_blocking_namespace_operation_with_sync(mutation_lease, true, move || {
            this.prepare_and_install_head_on_thread(&new_bytes, old_bytes.as_deref(), &dst_file_path, &owned_directory_guard)
                .map_err(IoError::other)
        })
        .await
        .map_err(DiskError::from)
    }
    #[cfg(test)]
    pub async fn install_head(&self, prepared: os::PreparedRenameSource) -> Result<os::PreparedRenameSource> {
        {
            let guard = self.validated_prepared.lock();
            let validated = guard.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
            let state = self.state.lock();
            if validated.publication != self.publication
                || state.prepared_sha256 != Some(validated.sha256)
                || state.prepared_length != Some(validated.length)
            {
                return Err(DiskError::ConditionalDeleteConflict);
            }
        }
        let canonical = self.canonical_head();
        let base = self.store.base.clone();
        let root = self.store.publication_root.clone();
        os::run_blocking_namespace_operation(self.store.lease.clone(), move || {
            os::install_multipart_prepared_head(prepared, &canonical, &base, &root)
        })
        .await
        .map_err(DiskError::from)
    }
    /// Only the original unpublished owner can compensate. First make its
    /// exact PREPARED proxy durable again; it is a claimable negative witness,
    /// never an ACTIVE upload. A crash before target rollback can then either
    /// confirm a remaining fresh Q or claim H heads, without a zero-head gap.
    pub async fn restore_target_after_error(&self, lease: Arc<os::NamespaceMutationLease>) -> Result<()> {
        if self.capability.lock().is_none() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let mut state = self.state.lock().clone();
        let loaded = self.store.load().await?;
        let actual = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
        if encode(actual)? != encode(&state)?
            || state.publication.as_ref() != Some(&self.publication)
            || !matches!(state.phase, MultipartSourcePhase::Prepared | MultipartSourcePhase::ConsumedUnsealed)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let prepared = self
            .store
            .read_target(&self.publication)
            .await?
            .ok_or(DiskError::ConditionalDeleteConflict)?;
        if Some(raw_sha(&prepared)) != state.prepared_sha256
            || Some(prepared.len() as u64) != state.prepared_length
            || !self.store.target_matches(&self.publication, &prepared)?
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        if loaded
            .head
            .as_ref()
            .is_some_and(|head| Some(head.sha256) != state.prepared_sha256 || Some(head.length) != state.prepared_length)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        if state.phase != MultipartSourcePhase::Prepared {
            state.advance(MultipartSourcePhase::Prepared)?;
            self.store.persist(&state).await?;
            *self.state.lock() = state.clone();
        }
        if loaded.head.is_none() {
            // The original metadata is still an independent immutable backup.
            // This full proxy is bound by the already-owned length/SHA receipt.
            self.store.replace_head(&prepared).await?;
        }
        // Failure above must leave the target intact. Never Undo first and
        // return an error after consuming the last claimable source heads.
        self.store.restore_exact_target_with_lease(&state, lease).await
    }
    pub(super) fn finish_on_thread(
        &self,
        target_already_synced: bool,
        staging_cleanup: Option<(PathBuf, Arc<std::sync::atomic::AtomicBool>)>,
    ) -> Result<MultipartPublicationResult> {
        let mut state = self.state.lock().clone();
        let expected_state_raw = encode(&state)?;
        let (expected_sha256, expected_length) = {
            let guard = self.validated_prepared.lock();
            let validated = guard.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
            if validated.publication != self.publication
                || state.prepared_sha256 != Some(validated.sha256)
                || state.prepared_length != Some(validated.length)
            {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            (validated.sha256, validated.length)
        };
        let target_dir = self.store.target_path(&self.publication)?;
        state.advance(MultipartSourcePhase::ConsumedUnsealed)?;
        state.validate(self.store.disk_id, &self.store.path)?;
        let next_state_raw = encode(&state)?;
        let snapshot = snapshot_for_state(self.store.disk_id, &state, &next_state_raw, None)?;
        let source = &self.store.source;
        let base = &self.store.base;
        let root = &self.store.publication_root;
        let path = &self.store.path;
        let disk_id = self.store.disk_id;
        let physical_slot = self.store.physical_slot;
        let disk_ids = &self.store.disk_ids;
        let terminal = terminal_record_path(source, &state);
        (|| -> std::io::Result<()> {
            if !target_already_synced || cfg!(any(test, feature = "test-util")) {
                let current = load_source(source, base, root, path, disk_id, physical_slot, disk_ids).map_err(IoError::other)?;
                let current_state = current
                    .state
                    .as_ref()
                    .ok_or_else(|| IoError::other(DiskError::ConditionalDeleteConflict))?;
                let current_raw = encode(current_state).map_err(IoError::other)?;
                if current.head.is_some() || current_raw != expected_state_raw {
                    return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                }
            }
            if target_already_synced {
                #[cfg(any(test, feature = "test-util"))]
                {
                    let pinned = os::pin_multipart_source_directory(&target_dir, base, root).map_err(|error| {
                        if error.kind() == std::io::ErrorKind::NotFound {
                            IoError::other(DiskError::ConditionalDeleteConflict)
                        } else {
                            error
                        }
                    })?;
                    let target = os::read_multipart_source_metadata_in_pinned(
                        &pinned,
                        &target_dir,
                        std::ffi::OsStr::new(STORAGE_FORMAT_FILE),
                        base,
                        root,
                    )
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::NotFound {
                            IoError::other(DiskError::ConditionalDeleteConflict)
                        } else {
                            error
                        }
                    })?;
                    if target.len() as u64 != expected_length || raw_sha(&target) != expected_sha256 {
                        return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                    }
                }
            } else {
                os::read_and_sync_multipart_target_metadata(
                    &target_dir,
                    std::ffi::OsStr::new(STORAGE_FORMAT_FILE),
                    base,
                    root,
                    |target| {
                        if target.len() as u64 != expected_length || raw_sha(target) != expected_sha256 {
                            return Err(IoError::other(DiskError::ConditionalDeleteConflict));
                        }
                        Ok(())
                    },
                )
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        IoError::other(DiskError::ConditionalDeleteConflict)
                    } else {
                        error
                    }
                })?;
            }
            SourceStore::persist_state_std(source, base, root, &next_state_raw, terminal.as_deref(), true)?;
            if let Some((io_path, cleaned)) = staging_cleanup {
                match std::fs::remove_dir(&io_path) {
                    Ok(()) => cleaned.store(true, std::sync::atomic::Ordering::Release),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        cleaned.store(true, std::sync::atomic::Ordering::Release);
                    }
                    Err(_) => {}
                }
            }
            Ok(())
        })()
        .map_err(DiskError::from)?;
        *self.state.lock() = state;
        let proof = self
            .store
            .issue_proof(MultipartProofKind::Published, self.publication.attempt, None, snapshot)?;
        let capability = self.capability.lock().take().ok_or(DiskError::ConditionalDeleteConflict)?;
        Ok(MultipartPublicationResult {
            publication: self.publication.clone(),
            proof,
            undo_capability: capability,
        })
    }

    pub async fn finish(
        self: &Arc<Self>,
        target_lease: Arc<os::NamespaceMutationLease>,
        target_already_synced: bool,
        staging_cleanup: Option<(PathBuf, Arc<std::sync::atomic::AtomicBool>)>,
    ) -> Result<MultipartPublicationResult> {
        let this = Arc::clone(self);
        os::run_blocking_namespace_operation_with_sync(target_lease, true, move || {
            this.finish_on_thread(target_already_synced, staging_cleanup)
                .map_err(IoError::other)
        })
        .await
        .map_err(DiskError::from)
    }
}

impl SourceStore {
    pub async fn sync_target(&self, publication: &PublicationIdentity) -> Result<()> {
        let io = self.target_path(publication)?;
        let logical = self
            .root
            .join(io.strip_prefix(&self.base).map_err(|_| DiskError::InvalidPath)?);
        let lease = os::acquire_namespace_mutation_lease_with_owner(&logical, Some(self.lease.clone())).await;
        self.sync_target_with_lease(publication, lease).await
    }
    pub async fn sync_target_with_lease(
        &self,
        publication: &PublicationIdentity,
        lease: Arc<os::NamespaceMutationLease>,
    ) -> Result<()> {
        let directory = self.target_path(publication)?;
        let file = directory.join(STORAGE_FORMAT_FILE);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(lease, move || {
            let _pin = os::pin_multipart_source_directory(&directory, &base, &root)?;
            os::sync_file(&file)?;
            os::fsync_dir_std(&directory)
        })
        .await
        .map_err(DiskError::from)
    }
    pub async fn read_and_sync_target(
        &self,
        publication: &PublicationIdentity,
        validate: impl FnOnce(&[u8]) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let io = self.target_path(publication)?;
        let logical = self
            .root
            .join(io.strip_prefix(&self.base).map_err(|_| DiskError::InvalidPath)?);
        let lease = os::acquire_namespace_mutation_lease_with_owner(&logical, Some(self.lease.clone())).await;
        self.read_and_sync_target_with_lease(publication, lease, validate).await
    }
    pub async fn read_and_sync_target_with_lease(
        &self,
        publication: &PublicationIdentity,
        lease: Arc<os::NamespaceMutationLease>,
        validate: impl FnOnce(&[u8]) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let directory = self.target_path(publication)?;
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation_with_sync(lease, true, move || {
            os::read_and_sync_multipart_target_metadata(
                &directory,
                std::ffi::OsStr::new(STORAGE_FORMAT_FILE),
                &base,
                &root,
                |bytes| validate(bytes).map_err(IoError::other),
            )
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    IoError::other(DiskError::ConditionalDeleteConflict)
                } else {
                    error
                }
            })
        })
        .await
        .map_err(DiskError::from)
    }
    async fn sync_source_absence(&self) -> Result<()> {
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            let mut parent = source.parent().ok_or_else(|| IoError::other("source has no parent"))?;
            let mut found = false;
            for _ in 0..64 {
                match os::pin_multipart_source_directory(parent, &base, &root) {
                    Ok(pin) => {
                        os::fsync_dir_std(parent)?;
                        drop(pin);
                        found = true;
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        parent = parent.parent().ok_or_else(|| IoError::other("source ancestor is missing"))?;
                        if !parent.starts_with(&base) {
                            return Err(IoError::other("source absence escaped its mount"));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            if !found {
                return Err(IoError::other("source ancestor depth exceeded"));
            }
            match os::pin_multipart_source_directory(&source, &base, &root) {
                Ok(_) => Err(IoError::new(std::io::ErrorKind::AlreadyExists, "multipart source reappeared")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(DiskError::from)
    }
}

impl LocalDisk {
    /// TMP/heal never imports or issues provenance. Conserve only an existing
    /// physical DD owner's immutable record; exact current-FI confirmation is
    /// performed separately when a native proof is requested.
    pub(super) async fn retain_owned_multipart_anchor(
        &self,
        incoming: &FileInfo,
        bucket: &str,
        object: &str,
        source_directory: &Path,
        target_directory: &Path,
        lease: Arc<os::NamespaceMutationLease>,
    ) -> Result<()> {
        let source = source_directory.to_path_buf();
        let base = self.io_root().to_path_buf();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(lease.clone(), move || {
            match std::fs::symlink_metadata(source.join(RETIREMENT_ANCHOR)) {
                Ok(_) => Err(IoError::new(
                    std::io::ErrorKind::PermissionDenied,
                    "TMP cannot import multipart retirement provenance",
                )),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        })
        .await?;
        if !incoming.is_healing() {
            return Ok(());
        }
        let historical = publication_identity(incoming)?;
        fn validate_publication(publication: &PublicationIdentity) -> Result<()> {
            SourceConditionV1 {
                lineage: publication.lineage.clone(),
                generation: publication.source_generation,
                attempt: publication.attempt,
            }
            .validate()?;
            if publication.final_data_dir.is_nil() {
                return Err(DiskError::FileCorrupt);
            }
            Ok(())
        }
        // A Copy result can retain this valid historical tuple while changing
        // its own key/DD/version/mtime. It is not evidence for the current FI.
        if let Some(historical) = historical.as_ref() {
            validate_publication(historical)?;
        }
        let Some(directory) = incoming.data_dir.filter(|directory| !directory.is_nil()) else {
            return Ok(());
        };
        let expected = self.io_get_object_path(bucket, &format!("{object}/{directory}"))?;
        if super::normalize_path_components(target_directory) != super::normalize_path_components(&expected) {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let target = target_directory.to_path_buf();
        let raw = os::run_blocking_namespace_operation(lease.clone(), move || {
            match os::read_multipart_source_metadata(&target.join(RETIREMENT_ANCHOR), &base, &root) {
                Ok(raw) => Ok(Some(raw)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await?;
        let Some(raw) = raw else {
            return Ok(());
        };
        let anchor: NativeRetirementAnchor = match decode(&raw) {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        if validate_publication(&anchor.publication).is_err() {
            return Ok(());
        }
        let disk_id = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::UnformattedDisk)?;
        let format = self.format_info.read().await.data.clone();
        let format = crate::disk::format::FormatV3::try_from(format.as_ref()).map_err(|_| DiskError::CorruptedBackend)?;
        let (set, slot) = format.find_disk_index_by_disk_id(disk_id)?;
        let cohort = format.erasure.sets.get(set).ok_or(DiskError::CorruptedBackend)?;
        if anchor.protocol_version != MULTIPART_SOURCE_PROTOCOL_VERSION
            || anchor.disk_id != disk_id
            || anchor.physical_slot != slot
            || anchor.source_validation_receipt_id.is_nil()
            || anchor.publication.lineage.bucket != bucket
            || anchor.publication.lineage.object != object
            || anchor.publication.final_data_dir != directory
            || anchor.publication.lineage.geometry.disks() != cohort.len()
            || anchor.publication.lineage.geometry.distribution.get(slot).is_none()
        {
            return Ok(());
        }
        // Version-only Copy may share this DD with the original C version.
        // Even if C's local metadata is temporarily missing, retain its actual
        // producer evidence unchanged. This cannot vote for copied D: native
        // confirmation still requires the exact current C FI and source proof.
        let source = source_directory.to_path_buf();
        let base = self.io_root().to_path_buf();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(lease, move || {
            let _source = os::pin_multipart_source_directory(&source, &base, &root)?;
            os::replace_multipart_source_metadata(&source.join(RETIREMENT_ANCHOR), &raw, &base, &root)
        })
        .await
        .map_err(DiskError::from)
    }
}
