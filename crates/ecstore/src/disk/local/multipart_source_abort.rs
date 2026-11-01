// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! All-participant, irreversible source closure. No operation in this module undoes a target.
use super::{
    LocalDisk,
    multipart_source_state::{
        LoadedSource, SourceHead, SourceState, SourceStore, decode, encode, load_source, raw_sha, read_optional,
        read_optional_in_pinned,
    },
};
use crate::disk::{
    DiskError, Error, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_DELETED_BUCKET, Result, STORAGE_FORMAT_FILE,
    multipart_source::*, os,
};
use rustfs_filemeta::FileMeta;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const STATE: &str = ".mpu-source-state";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredHead {
    pub sha256: [u8; 32],
    pub length: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AbortCertificateRef {
    pub round: MultipartAbortRound,
    pub sha256: [u8; 32],
    pub slot: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeAbortState {
    pub round: MultipartAbortRound,
    pub pending_from: MultipartSourceSnapshot,
    pub retired_head: Option<RetiredHead>,
    pub committed: Option<AbortCertificateRef>,
}
impl NativeAbortState {
    pub fn validate(&self, state: &SourceState) -> Result<()> {
        self.round.validate()?;
        if state.phase != MultipartSourcePhase::Aborting
            || state.capability_sha256.is_some()
            || state.generation != self.round.generation
            || state.lineage != self.round.lineage
            || self.pending_from.lineage != state.lineage
            || self.pending_from.disk_id != state.disk_id
            || self.pending_from.physical_slot != state.physical_slot
            || self.pending_from.generation.epoch > state.generation.epoch
            || self.committed.as_ref().is_some_and(|reference| {
                reference.round.lineage != state.lineage
                    || reference.slot > 1
                    || reference.round.generation.epoch > state.generation.epoch
                    || (reference.round.generation.epoch == state.generation.epoch
                        && reference.round.generation != state.generation)
            })
        {
            return Err(DiskError::FileCorrupt);
        }
        if let Some(reference) = &self.committed {
            reference.round.validate()?;
        }
        Ok(())
    }
}
fn retired_path(source: &Path, state: &SourceState) -> PathBuf {
    source
        .join(".mpu-completions")
        .join(state.backup_owner.to_string())
        .join("aborted-head.xl.meta")
}
fn abort_certificate_path(source: &Path, owner: Uuid, slot: u8) -> PathBuf {
    source
        .join(".mpu-completions")
        .join(owner.to_string())
        .join(format!("abort-{slot}.msgpack"))
}

/// The only cold local provenance rehydration token; callers cannot construct it.
pub(in crate::disk) struct ValidatedAbortCertificate {
    digest: [u8; 32],
    round: MultipartAbortRound,
}
impl ValidatedAbortCertificate {
    pub(in crate::disk) fn validate_certificate(&self, certificate: &MultipartAbortCertificate) -> Result<()> {
        if certificate.digest()? != self.digest || certificate.round != self.round {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

pub(super) fn validate_retired_head(
    source: &Path,
    base: &Path,
    root: &os::PublicationRoot,
    state: &SourceState,
    head: Option<&SourceHead>,
) -> Result<()> {
    let Some(abort) = &state.abort else {
        return Ok(());
    };
    abort.validate(state)?;
    if let Some(head) = head {
        if abort.pending_from.head_sha256 != Some(head.sha256)
            && abort
                .retired_head
                .as_ref()
                .is_none_or(|receipt| receipt.sha256 != head.sha256 || receipt.length != head.length)
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
    } else if let Some(receipt) = &abort.retired_head {
        let observed = os::observe_multipart_source_metadata(&retired_path(source, state), base, root, receipt.length)?;
        if observed != (receipt.length, receipt.sha256) {
            return Err(DiskError::FileCorrupt);
        }
    }
    Ok(())
}

impl SourceStore {
    fn verify_abort_cohort(&self, certificate: &MultipartAbortCertificate) -> Result<()> {
        certificate.verify_with_authority(&self.authority)?;
        if certificate.disk_ids != self.disk_ids
            || certificate.disk_ids.get(self.physical_slot) != Some(&self.disk_id)
            || certificate.round.lineage.path != self.path
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(())
    }
    pub async fn read_abort_certificate(&self, loaded: &LoadedSource) -> Result<Option<MultipartAbortCertificate>> {
        let Some(reference) = loaded
            .state
            .as_ref()
            .and_then(|state| state.abort.as_ref())
            .and_then(|abort| abort.committed.as_ref())
        else {
            return Ok(None);
        };
        let owner = loaded.state.as_ref().ok_or(DiskError::FileCorrupt)?.backup_owner;
        let path = abort_certificate_path(&self.source, owner, reference.slot);
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let raw = os::run_blocking_namespace_operation(self.lease.clone(), move || {
            os::read_multipart_source_metadata(&path, &base, &root)
        })
        .await?;
        let mut certificate: MultipartAbortCertificate = decode(&raw)?;
        let validated = ValidatedAbortCertificate {
            digest: reference.sha256,
            round: reference.round.clone(),
        };
        certificate.hydrate_owned(&self.authority, &validated)?;
        self.verify_abort_cohort(&certificate)?;
        Ok(Some(certificate))
    }
    async fn persist_abort_certificate(
        &self,
        state: &SourceState,
        certificate: &MultipartAbortCertificate,
    ) -> Result<AbortCertificateRef> {
        self.verify_abort_cohort(certificate)?;
        let raw = encode(certificate)?;
        let digest = raw_sha(&raw);
        let slot = state
            .abort
            .as_ref()
            .and_then(|abort| abort.committed.as_ref())
            .map_or(0, |current| 1 - current.slot);
        let path = abort_certificate_path(&self.source, state.backup_owner, slot);
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            let _source = os::pin_multipart_source_directory(&source, &base, &root)?;
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("abort certificate has no parent"))?;
            let _parents = os::mkdir_all_below_existing_base_std(parent, &base, &root)?;
            // Only the unreferenced slot may be replaced. The old committed
            // slot remains recoverable until the subsequent state barrier.
            os::replace_multipart_source_metadata(&path, &raw, &base, &root)?;
            os::sync_multipart_owned_record_parents(&source, parent, true, &base, &root)
        })
        .await?;
        Ok(AbortCertificateRef {
            round: certificate.round.clone(),
            sha256: digest,
            slot,
        })
    }
    async fn retire_abort_head(&self, state: &SourceState) -> Result<()> {
        let abort = state.abort.as_ref().ok_or(DiskError::FileCorrupt)?;
        let source = self.source.clone();
        let base = self.base.clone();
        let root = self.publication_root.clone();
        let state = state.clone();
        let receipt = abort.retired_head.clone();
        os::run_blocking_namespace_operation(self.lease.clone(), move || {
            let _pin = os::pin_multipart_source_directory(&source, &base, &root)?;
            let canonical = source.join(STORAGE_FORMAT_FILE);
            if let Some(receipt) = receipt {
                let retired = retired_path(&source, &state);
                os::retire_multipart_source_head_std(&canonical, &retired, &base, &root, receipt.length, receipt.sha256)?;
                os::sync_multipart_owned_record_parents(
                    &source,
                    retired
                        .parent()
                        .ok_or_else(|| io::Error::other("retired head parent missing"))?,
                    true,
                    &base,
                    &root,
                )
            } else {
                match os::observe_multipart_source_metadata(&canonical, &base, &root, 0) {
                    Err(error) if error.kind() == ErrorKind::NotFound => os::fsync_dir_std(&source),
                    Err(error) => Err(error),
                    Ok(_) => Err(io::Error::new(ErrorKind::AlreadyExists, "closed source unexpectedly has a head")),
                }
            }
        })
        .await
        .map_err(DiskError::from)
    }
    async fn abort_proof(
        &self,
        kind: MultipartAbortProofKind,
        round: &MultipartAbortRound,
        certificate_sha256: Option<[u8; 32]>,
        publication: Option<PublicationIdentity>,
        challenge: Uuid,
    ) -> Result<MultipartAbortProof> {
        let loaded = self.load().await?;
        let snapshot = loaded.snapshot.ok_or(DiskError::ConditionalDeleteConflict)?;
        if loaded.head.is_some() {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        MultipartAbortProof::issue(&self.authority, kind, challenge, round.clone(), snapshot, certificate_sha256, publication)
    }
    async fn abort_inspection(&self) -> Result<MultipartSourceInspection> {
        let loaded = self.load().await?;
        let certificate = self.read_abort_certificate(&loaded).await?;
        Ok(MultipartSourceInspection {
            disk_id: self.disk_id,
            root_missing: loaded.root_missing,
            abort_round: loaded
                .state
                .as_ref()
                .and_then(|state| state.abort.as_ref().map(|abort| abort.round.clone())),
            abort_certificate: certificate,
            snapshot: loaded.snapshot,
            original: loaded.original,
            recovery: loaded.state.as_ref().and_then(|state| state.recovery.clone()),
            negative_certificate: None,
            ready_certificate: None,
            terminal_certificate: loaded.state.map_or_else(Vec::new, |state| state.terminal_certificate),
        })
    }
}

impl LocalDisk {
    pub(super) async fn recover_multipart_abort_owned(
        &self,
        store: SourceStore,
        operation: MultipartSourceOperation,
    ) -> Result<MultipartSourceResponse> {
        match operation {
            MultipartSourceOperation::UpgradeLegacy { expected } => {
                let loaded = store.load().await?;
                if loaded.snapshot.as_ref() != Some(&expected)
                    || loaded.state.is_some()
                    || expected.phase != MultipartSourcePhase::Active
                    || expected.generation.epoch != 0
                    || !loaded.head_matches_state
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let mut info = loaded.info.ok_or(DiskError::FileCorrupt)?;
                if native_source_protocol(&info)? {
                    return Ok(MultipartSourceResponse::LegacyUpgraded(store.abort_inspection().await?));
                }
                if recognized_native_source_controls(&info)? {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let source = store.source.clone();
                let base = store.base.clone();
                let root = store.publication_root.clone();
                os::run_blocking_namespace_operation(store.lease.clone(), move || {
                    let _pin = os::pin_multipart_source_directory(&source, &base, &root)?;
                    for entry in std::fs::read_dir(&source)? {
                        let entry = entry?;
                        if entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.get(..5).is_some_and(|prefix| prefix.eq_ignore_ascii_case(".mpu-")))
                        {
                            return Err(io::Error::new(ErrorKind::InvalidData, "legacy source has native owned records"));
                        }
                    }
                    Ok(())
                })
                .await?;
                mark_native_source(&mut info);
                let mut metadata = FileMeta::new();
                metadata.add_version(info).map_err(Error::other)?;
                store.replace_head(&metadata.marshal_msg().map_err(Error::other)?).await?;
                Ok(MultipartSourceResponse::LegacyUpgraded(store.abort_inspection().await?))
            }
            MultipartSourceOperation::PrepareAbort { expected, round } => {
                round.validate()?;
                let loaded = store.load().await?;
                if loaded.snapshot.as_ref() != Some(&expected) || expected.lineage != round.lineage {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let mut state = store.ensure_state(&loaded).await?;
                if state
                    .abort
                    .as_ref()
                    .and_then(|abort| abort.committed.as_ref())
                    .is_some_and(|committed| committed.round == round)
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                if state.abort.as_ref().is_none_or(|abort| abort.round != round) {
                    if round.generation.epoch <= state.generation.epoch {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    if let Some(head) = loaded.head.as_ref() {
                        let owned = head.sha256 == state.original_sha256
                            || Some(head.sha256) == state.prepared_sha256
                            || loaded.head_matches_state;
                        if !owned {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                    }
                    let previous = state.abort.take();
                    let retired = previous.as_ref().and_then(|abort| abort.retired_head.clone()).or_else(|| {
                        loaded.head.as_ref().map(|head| RetiredHead {
                            sha256: head.sha256,
                            length: head.length,
                        })
                    });
                    state.abort = Some(NativeAbortState {
                        round: round.clone(),
                        pending_from: expected,
                        retired_head: retired,
                        committed: previous.and_then(|abort| abort.committed),
                    });
                    state.generation = round.generation;
                    state.capability_sha256 = None;
                    state.advance(MultipartSourcePhase::Aborting)?;
                    store.persist(&state).await?;
                }
                store.retire_abort_head(&state).await?;
                Ok(MultipartSourceResponse::AbortPrepared(
                    store
                        .abort_proof(MultipartAbortProofKind::Prepared, &round, None, None, round.round)
                        .await?,
                ))
            }
            MultipartSourceOperation::CommitAbort { expected, certificate } => {
                let loaded = store.load().await?;
                if loaded.snapshot.as_ref() != Some(&expected) {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                store.verify_abort_cohort(&certificate)?;
                let mut state = loaded.state.ok_or(DiskError::ConditionalDeleteConflict)?;
                let abort = state.abort.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
                if state.phase != MultipartSourcePhase::Aborting
                    || loaded.head.is_some()
                    || certificate.round.lineage != state.lineage
                    || certificate.round.generation.epoch > state.generation.epoch
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let digest = certificate.digest()?;
                if abort.committed.as_ref().is_none_or(|reference| reference.sha256 != digest) {
                    let reference = store.persist_abort_certificate(&state, &certificate).await?;
                    state.abort.as_mut().ok_or(DiskError::FileCorrupt)?.committed = Some(reference);
                    state.advance(MultipartSourcePhase::Aborting)?;
                    store.persist(&state).await?;
                } else {
                    store
                        .read_abort_certificate(&store.load().await?)
                        .await?
                        .ok_or(DiskError::FileCorrupt)?;
                }
                Ok(MultipartSourceResponse::AbortCommitted(
                    store
                        .abort_proof(
                            MultipartAbortProofKind::Committed,
                            &certificate.round,
                            Some(digest),
                            None,
                            certificate.round.round,
                        )
                        .await?,
                ))
            }
            MultipartSourceOperation::DeleteAborted { expected, certificate } => {
                let loaded = store.load().await?;
                if loaded.snapshot.as_ref() != Some(&expected) {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                store.verify_abort_cohort(&certificate)?;
                let current = store
                    .read_abort_certificate(&loaded)
                    .await?
                    .ok_or(DiskError::ConditionalDeleteConflict)?;
                if current != certificate || loaded.head.is_some() {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let permit = AbortDeletePermit::new(&store, &expected, certificate.clone())?;
                super::multipart_source_state::invalidate_active_multipart_source_head(&store.source);
                let destination = self.io_get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, &Uuid::new_v4().to_string())?;
                let source = store.source.clone();
                let base = store.base.clone();
                let root = store.publication_root.clone();
                let lease = os::acquire_namespace_mutation_lease_with_owner(&destination, Some(store.lease.clone())).await;
                os::run_blocking_namespace_operation_with_sync(lease, true, move || {
                    permit.validate()?;
                    os::rename_multipart_source_owned_std(&source, &destination, &base, &root, || permit.validate())
                })
                .await?;
                let proof = MultipartAbortProof::issue(
                    &store.authority,
                    MultipartAbortProofKind::Deleted,
                    certificate.round.round,
                    certificate.round.clone(),
                    expected,
                    Some(certificate.digest()?),
                    None,
                )?;
                Ok(MultipartSourceResponse::AbortDeleted(proof))
            }
            MultipartSourceOperation::ConfirmCleanupTarget {
                expected,
                publication,
                challenge,
            } => {
                let loaded = store.load().await?;
                if loaded.snapshot.as_ref() != Some(&expected)
                    || expected.phase != MultipartSourcePhase::Aborting
                    || loaded.head.is_some()
                    || expected.lineage != publication.lineage
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let certificate = store
                    .read_abort_certificate(&loaded)
                    .await?
                    .ok_or(DiskError::ConditionalDeleteConflict)?;
                let target = self.get_object_path(&publication.lineage.bucket, &publication.lineage.object)?;
                let _owner = os::acquire_metadata_mutation_lease(&target, Some(store.lease.clone())).await;
                store.validate_cleanup_target(&publication).await?;
                store
                    .abort_proof(
                        MultipartAbortProofKind::CleanupTarget,
                        &certificate.round,
                        Some(certificate.digest()?),
                        Some(publication),
                        challenge,
                    )
                    .await
                    .map(MultipartSourceResponse::CleanupTargetConfirmed)
            }
            _ => Err(DiskError::UnsupportedDisk),
        }
    }
}

/// This permit is neither serializable nor constructible by a generic delete RPC.
struct AbortDeletePermit {
    source: PathBuf,
    base: PathBuf,
    root: os::PublicationRoot,
    path: String,
    disk_id: Uuid,
    slot: usize,
    disk_ids: Vec<Uuid>,
    expected: MultipartSourceSnapshot,
    certificate: MultipartAbortCertificate,
    authority: ProofVerificationContext,
}
impl AbortDeletePermit {
    fn new(store: &SourceStore, expected: &MultipartSourceSnapshot, certificate: MultipartAbortCertificate) -> Result<Self> {
        store.verify_abort_cohort(&certificate)?;
        Ok(Self {
            source: store.source.clone(),
            base: store.base.clone(),
            root: store.publication_root.clone(),
            path: store.path.clone(),
            disk_id: store.disk_id,
            slot: store.physical_slot,
            disk_ids: store.disk_ids.clone(),
            expected: expected.clone(),
            certificate,
            authority: store.authority.clone(),
        })
    }
    fn validate(&self) -> io::Result<()> {
        (|| -> Result<()> {
            self.certificate.verify_with_authority(&self.authority)?;
            let loaded = load_source(&self.source, &self.base, &self.root, &self.path, self.disk_id, self.slot, &self.disk_ids)?;
            if loaded.snapshot.as_ref() != Some(&self.expected) || loaded.head.is_some() {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            let reference = loaded
                .state
                .as_ref()
                .and_then(|state| state.abort.as_ref())
                .and_then(|abort| abort.committed.as_ref())
                .ok_or(DiskError::ConditionalDeleteConflict)?;
            if reference.sha256 != self.certificate.digest()? || reference.round != self.certificate.round {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            let raw = read_optional(
                &abort_certificate_path(
                    &self.source,
                    loaded.state.as_ref().ok_or(DiskError::FileCorrupt)?.backup_owner,
                    reference.slot,
                ),
                &self.base,
                &self.root,
            )?
            .ok_or(DiskError::FileCorrupt)?;
            if raw_sha(&raw) != reference.sha256 {
                return Err(DiskError::FileCorrupt);
            }
            Ok(())
        })()
        .map_err(io::Error::other)
    }
}

/// A local ownership token can only be obtained by acquiring the upload's
/// SourceRW. Appending another owner preserves that guard in the Arc chain.
#[derive(Clone)]
pub(super) struct MultipartDeleteOwner {
    owner: Option<Arc<dyn Send + Sync>>,
    source_key: Option<PathBuf>,
    admission_validated: bool,
}
impl MultipartDeleteOwner {
    pub(super) fn namespace_owner(&self) -> Option<Arc<dyn Send + Sync>> {
        self.owner.clone()
    }
    pub(super) fn mark_validated(&mut self) {
        self.admission_validated = true;
    }
    pub(super) fn is_validated(&self) -> bool {
        self.admission_validated
    }
    pub(super) fn chain(self, owner: Option<Arc<dyn Send + Sync>>) -> Self {
        let owner = match (self.owner, owner) {
            (None, owner) | (owner, None) => owner,
            (Some(first), Some(second)) if Arc::ptr_eq(&first, &second) => Some(first),
            (Some(first), Some(second)) => Some(Arc::new((first, second)) as Arc<dyn Send + Sync>),
        };
        Self {
            owner,
            source_key: self.source_key,
            admission_validated: self.admission_validated,
        }
    }
}
impl LocalDisk {
    fn multipart_delete_owner_key(&self, volume: &str, path: &str) -> Result<Option<PathBuf>> {
        super::multipart_staging::reject_noncanonical_scope(&self.root, self.io_root(), volume, path)?;
        Ok((volume == RUSTFS_META_MULTIPART_BUCKET).then(|| {
            self.root
                .join(RUSTFS_META_MULTIPART_BUCKET)
                .join(super::multipart_staging::upload_scope_root(path, true).to_ascii_lowercase())
        }))
    }
    pub(super) async fn acquire_multipart_delete_owner(
        &self,
        volume: &str,
        path: &str,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<MultipartDeleteOwner> {
        let source_key = self.multipart_delete_owner_key(volume, path)?;
        let owner = self.multipart_source_owner(volume, path, true, true, owner).await?;
        Ok(MultipartDeleteOwner {
            owner,
            source_key,
            admission_validated: false,
        })
    }
    pub(super) fn validate_multipart_delete_owner(
        &self,
        token: &MultipartDeleteOwner,
        volume: &str,
        path: &str,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        if token.source_key != self.multipart_delete_owner_key(volume, path)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(token.namespace_owner())
    }
}

/// No prefix-wide upload lock exists. Ordinary recursive ancestor deletion is
/// refused instead of scanning children and racing a newly closed upload.
#[derive(Clone)]
pub(super) struct OrdinaryMultipartDeleteCheck {
    source: PathBuf,
    base: PathBuf,
    root: os::PublicationRoot,
}
impl OrdinaryMultipartDeleteCheck {
    pub(super) fn source(&self) -> &Path {
        &self.source
    }
    pub(super) fn validate(&self) -> Result<()> {
        let pinned = match os::pin_multipart_source_directory(&self.source, &self.base, &self.root) {
            Ok(pinned) => pinned,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if let Some(raw) = read_optional_in_pinned(&pinned, &self.source, STATE, &self.base, &self.root)? {
            let state: SourceState = decode(&raw)?;
            if state.abort.is_some() || state.phase == MultipartSourcePhase::Aborting {
                return Err(DiskError::ConditionalDeleteConflict);
            }
        } else if os::multipart_source_has_native_controls_in_pinned(&self.source, &pinned)? {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(())
    }
    pub(super) fn callback(&self) -> Arc<dyn Fn() -> io::Result<()> + Send + Sync> {
        let check = self.clone();
        Arc::new(move || check.validate().map_err(io::Error::other))
    }
}
impl LocalDisk {
    /// Admission is read-only but may read metadata. The queued callback keeps
    /// SourceRW ownership and the job's resource unit after waiter cancellation.
    pub(super) async fn validate_multipart_delete_admission(
        &self,
        check: OrdinaryMultipartDeleteCheck,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<()> {
        self.blocking_queue
            .run_cancellable(move |_| {
                let _owner = owner;
                Ok(check.validate())
            })
            .await
            .map_err(DiskError::from)?
    }

    pub(super) fn multipart_delete_check(&self, path: &Path, recursive: bool) -> Result<Option<OrdinaryMultipartDeleteCheck>> {
        let Ok(relative) = path.strip_prefix(self.io_root()) else {
            return Ok(None);
        };
        let mut components = relative.components();
        if !components
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .is_some_and(|component| component.eq_ignore_ascii_case(crate::disk::RUSTFS_META_BUCKET))
        {
            return Ok(None);
        }
        match components.next().and_then(|component| component.as_os_str().to_str()) {
            Some(component) if component.eq_ignore_ascii_case("multipart") => {}
            None if recursive => return Err(DiskError::ConditionalDeleteConflict),
            _ => return Ok(None),
        }
        let relative = components.as_path();
        let relative = relative.to_str().ok_or(DiskError::InvalidPath)?;
        let upload = super::multipart_staging::upload_scope_root(relative, true);
        let source = self.io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, upload)?;
        // Only a production hash/upload root owns a recursive upload scope.
        // Empty SHA rmdir remains nonrecursive and unchanged.
        let canonical = Path::new(upload)
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split(['x', 'X']).next())
            .and_then(|name| Uuid::parse_str(name).ok())
            .is_some_and(|id| !id.is_nil());
        let hashed_parent = Path::new(upload)
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()));
        if recursive && !(canonical && hashed_parent) {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(Some(OrdinaryMultipartDeleteCheck {
            source,
            base: self.io_root().to_path_buf(),
            root: self.publication_root.clone(),
        }))
    }
}

#[cfg(test)]
mod lock_order_tests {
    include!("multipart_source_lock_order_tests.rs");
}
