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

//! Exceptional MPU source recovery. Ordinary completion does not enter this
//! protocol: only an indeterminate source or a failed publication needs its
//! negative certificate and all-participant activation barrier.

use crate::disk::error::{DiskError, Result};
use crate::disk::multipart_source::bind_multipart_proof_snapshot;
use crate::disk::multipart_source::{
    MULTIPART_SOURCE_PROTOCOL_VERSION, MultipartAbortCertificate, MultipartAbortProof, MultipartAbortProofKind,
    MultipartAbortRound, MultipartCertificate, MultipartProofKind, MultipartPublicationResult, MultipartRecoveryRound,
    MultipartSourceInspection, MultipartSourceOperation, MultipartSourcePhase, MultipartSourceProof, MultipartSourceRequest,
    MultipartSourceResponse, ProofVerificationContext, PublicationIdentity, SourceGeneration, SourceLineage,
    native_source_protocol, recognized_native_source_controls,
};
use crate::disk::{DiskAPI, DiskStore};
use futures::future::join_all;
use std::sync::Arc;
use uuid::Uuid;

pub(in crate::set_disk) enum SourceRecovery {
    Missing,
    Active,
    Activated,
    Terminal,
}

fn request(path: &str, operation: MultipartSourceOperation) -> MultipartSourceRequest {
    MultipartSourceRequest {
        protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
        path: path.to_owned(),
        operation,
    }
}

async fn recover_on_disk(
    disk: &DiskStore,
    request: MultipartSourceRequest,
    namespace_owner: Option<Arc<dyn Send + Sync>>,
) -> Result<MultipartSourceResponse> {
    match disk.as_ref() {
        crate::disk::Disk::Local(local) => {
            local
                .recover_multipart_source_with_namespace_owner(request, namespace_owner)
                .await
        }
        crate::disk::Disk::Remote(remote) => {
            let _namespace_owner = namespace_owner;
            remote.recover_multipart_source(request).await
        }
    }
}

async fn disk_ids(disks: &[Option<DiskStore>]) -> Vec<Option<Uuid>> {
    join_all(disks.iter().map(|disk| async {
        match disk {
            Some(disk) => disk.get_disk_id().await.ok().flatten().filter(|id| !id.is_nil()),
            None => None,
        }
    }))
    .await
}

fn verify_slot(
    proof: &MultipartSourceProof,
    slot: usize,
    id: Option<Uuid>,
    lineage: &SourceLineage,
    authority: &ProofVerificationContext,
) -> Result<()> {
    proof.verify_with_authority(authority)?;
    if id != Some(proof.snapshot.disk_id) || proof.snapshot.physical_slot != slot || &proof.snapshot.lineage != lineage {
        return Err(DiskError::FileCorrupt);
    }
    Ok(())
}

fn verify_recovery_shape(proof: &MultipartSourceProof, kind: MultipartProofKind, round: &MultipartRecoveryRound) -> Result<()> {
    let phase = if kind == MultipartProofKind::Activated {
        MultipartSourcePhase::Active
    } else {
        MultipartSourcePhase::Recovering
    };
    if proof.kind != kind
        || proof.challenge != round.round
        || proof.recovery.as_ref() != Some(round)
        || proof.snapshot.generation != round.generation
        || proof.snapshot.phase != phase
        || proof.snapshot.head_sha256.is_none()
        || proof.snapshot.capability_sha256.is_some()
    {
        return Err(DiskError::FileCorrupt);
    }
    Ok(())
}

async fn inspect(
    disks: &[Option<DiskStore>],
    path: &str,
    namespace_owner: Option<&Arc<dyn Send + Sync>>,
) -> Vec<Option<MultipartSourceInspection>> {
    join_all(disks.iter().map(|disk| async {
        let disk = disk.as_ref()?;
        match recover_on_disk(
            disk,
            request(path, MultipartSourceOperation::Inspect { lineage: None }),
            namespace_owner.cloned(),
        )
        .await
        {
            Ok(MultipartSourceResponse::Inspected(inspection)) => Some(inspection),
            _ => None,
        }
    }))
    .await
}

/// Fresh native seals revoke any old failed owner's authority before a replay
/// can acknowledge a target. A read-quorum or an earlier proof is insufficient.
pub(in crate::set_disk) async fn confirm_target(
    disks: &[Option<DiskStore>],
    publication: &PublicationIdentity,
    authority: &ProofVerificationContext,
) -> Result<()> {
    bind_multipart_proof_snapshot(disks, authority)?;
    let lineage = &publication.lineage;
    lineage.validate()?;
    if disks.len() != lineage.geometry.disks() {
        return Err(DiskError::FileCorrupt);
    }
    let ids = disk_ids(disks).await;
    let challenge = Uuid::new_v4();
    let proofs = join_all(disks.iter().enumerate().map(|(slot, disk)| {
        let expected_id = ids[slot];
        async move {
            let disk = disk.as_ref()?;
            let operation = MultipartSourceOperation::ConfirmTerminal {
                expected: None,
                publication: publication.clone(),
                challenge,
            };
            if let Ok(MultipartSourceResponse::TerminalConfirmed(proof)) =
                disk.recover_multipart_source(request(&lineage.path, operation)).await
            {
                // Verify complete evidence before reducing it to a slot identity.
                // Corrupt replies remain errors; only unavailable replies abstain.
                return Some((|| {
                    verify_slot(&proof, slot, expected_id, lineage, authority)?;
                    if proof.challenge != challenge
                        || proof.snapshot.publication.as_ref() != Some(publication)
                        || !matches!(proof.kind, MultipartProofKind::TerminalTarget | MultipartProofKind::RootMissingTerminal)
                    {
                        return Err(DiskError::FileCorrupt);
                    }
                    Ok(proof.snapshot.disk_id)
                })());
            }
            // A partial source delete can leave a real producer Aborting. Its
            // independent own anchor may still confirm this exact historical
            // target, without changing the source phase or restoring any rights.
            let inspection = match disk
                .recover_multipart_source(request(
                    &lineage.path,
                    MultipartSourceOperation::Inspect {
                        lineage: Some(lineage.clone()),
                    },
                ))
                .await
            {
                Ok(MultipartSourceResponse::Inspected(inspection)) => inspection,
                _ => return None,
            };
            let expected = inspection.snapshot?;
            if expected.phase != MultipartSourcePhase::Aborting {
                return None;
            }
            match disk
                .recover_multipart_source(request(
                    &lineage.path,
                    MultipartSourceOperation::ConfirmCleanupTarget {
                        expected,
                        publication: publication.clone(),
                        challenge,
                    },
                ))
                .await
            {
                Ok(MultipartSourceResponse::CleanupTargetConfirmed(proof)) => Some((|| {
                    abort_slot(&proof, slot, expected_id, lineage, authority)?;
                    if proof.kind != MultipartAbortProofKind::CleanupTarget
                        || proof.challenge != challenge
                        || proof.publication.as_ref() != Some(publication)
                    {
                        return Err(DiskError::FileCorrupt);
                    }
                    Ok(proof.snapshot.disk_id)
                })()),
                _ => None,
            }
        }
    }))
    .await;
    let mut confirmed = Vec::new();
    for proof in proofs.into_iter().flatten() {
        let disk_id = proof?;
        if confirmed.contains(&disk_id) {
            return Err(DiskError::FileCorrupt);
        }
        confirmed.push(disk_id);
    }
    if confirmed.len() < lineage.geometry.write_quorum() {
        return Err(DiskError::ErasureWriteQuorum);
    }
    Ok(())
}

/// Prove that a quorum-consumed upload remains terminal even after a later PUT
/// or DELETE removed the old target identity. This never acknowledges a body.
async fn seal_consumed(
    disks: &[Option<DiskStore>],
    ids: &[Option<Uuid>],
    inspections: &[Option<MultipartSourceInspection>],
    publication: &PublicationIdentity,
    authority: &ProofVerificationContext,
) -> Result<bool> {
    let challenge = Uuid::new_v4();
    let proofs = join_all(disks.iter().zip(inspections).map(|(disk, inspection)| async {
        let disk = disk.as_ref()?;
        let snapshot = inspection.as_ref()?.snapshot.as_ref()?;
        if snapshot.publication.as_ref() != Some(publication)
            || !matches!(
                snapshot.phase,
                MultipartSourcePhase::ConsumedUnsealed
                    | MultipartSourcePhase::LocalTerminalSeal
                    | MultipartSourcePhase::GlobalTerminalCertified
            )
        {
            return None;
        }
        let operation = MultipartSourceOperation::SealConsumedTerminal {
            expected: snapshot.clone(),
            publication: publication.clone(),
            challenge,
        };
        match disk
            .recover_multipart_source(request(&publication.lineage.path, operation))
            .await
        {
            Ok(MultipartSourceResponse::TerminalConfirmed(proof)) => Some(proof),
            _ => None,
        }
    }))
    .await;
    let mut confirmed = Vec::new();
    for (slot, proof) in proofs.into_iter().enumerate() {
        let Some(proof) = proof else { continue };
        verify_slot(&proof, slot, ids[slot], &publication.lineage, authority)?;
        if proof.kind != MultipartProofKind::TerminalConsumed
            || proof.challenge != challenge
            || proof.snapshot.publication.as_ref() != Some(publication)
        {
            return Err(DiskError::FileCorrupt);
        }
        if confirmed.contains(&proof.snapshot.disk_id) {
            return Err(DiskError::FileCorrupt);
        }
        confirmed.push(proof.snapshot.disk_id);
    }
    Ok(confirmed.len() >= publication.lineage.geometry.write_quorum())
}

fn compatible_inspections(
    inspections: &[Option<MultipartSourceInspection>],
    ids: &[Option<Uuid>],
    lineage: &SourceLineage,
) -> Result<()> {
    for (slot, inspection) in inspections.iter().enumerate() {
        if let Some(inspection) = inspection {
            if ids[slot] != Some(inspection.disk_id) {
                return Err(DiskError::FileCorrupt);
            }
            if let Some(snapshot) = &inspection.snapshot
                && (snapshot.physical_slot != slot || &snapshot.lineage != lineage)
            {
                return Err(DiskError::FileCorrupt);
            }
        }
    }
    Ok(())
}

/// Readonly cleanup metadata never grants publication or source-deletion rights.
pub(in crate::set_disk) struct CleanupSourceMetadata {
    pub metadata: Vec<rustfs_filemeta::FileInfo>,
    pub errors: Vec<Option<DiskError>>,
    pub certified_original: Option<rustfs_filemeta::FileInfo>,
}

/// A successful close is useful only to the guarded cleanup caller. Its owned
/// certificate must also be checked inside each actual delete executor.
pub(in crate::set_disk) enum SourceCleanupAuthority {
    Legacy,
    Native {
        certificate: Box<MultipartAbortCertificate>,
        snapshots: Vec<Option<crate::disk::multipart_source::MultipartSourceSnapshot>>,
    },
}

fn abort_slot(
    proof: &MultipartAbortProof,
    slot: usize,
    id: Option<Uuid>,
    lineage: &SourceLineage,
    authority: &ProofVerificationContext,
) -> Result<()> {
    proof.verify_with_authority(authority)?;
    if id != Some(proof.snapshot.disk_id)
        || proof.snapshot.physical_slot != slot
        || &proof.snapshot.lineage != lineage
        || proof.snapshot.phase != MultipartSourcePhase::Aborting
        || proof.snapshot.head_sha256.is_some()
        || proof.snapshot.capability_sha256.is_some()
    {
        return Err(DiskError::FileCorrupt);
    }
    Ok(())
}

fn abort_certificate_cohort(
    certificate: &MultipartAbortCertificate,
    ids: &[Option<Uuid>],
    authority: &ProofVerificationContext,
) -> Result<()> {
    certificate.verify_with_authority(authority)?;
    if ids.len() != certificate.disk_ids.len()
        || ids.iter().copied().collect::<Option<Vec<_>>>().as_ref() != Some(&certificate.disk_ids)
    {
        return Err(DiskError::FileCorrupt);
    }
    Ok(())
}

fn cleanup_original(
    inspection: &MultipartSourceInspection,
    slot: usize,
    id: Option<Uuid>,
    bucket: &str,
    object: &str,
    path: &str,
) -> Result<Option<rustfs_filemeta::FileInfo>> {
    if id != Some(inspection.disk_id) {
        return Err(DiskError::FileCorrupt);
    }
    let Some(snapshot) = inspection.snapshot.as_ref() else {
        if inspection.original.is_some() || inspection.abort_certificate.is_some() || inspection.abort_round.is_some() {
            return Err(DiskError::FileCorrupt);
        }
        return Ok(None);
    };
    snapshot.lineage.validate()?;
    if snapshot.disk_id != inspection.disk_id
        || snapshot.physical_slot != slot
        || snapshot.lineage.path != path
        || (!bucket.is_empty() && snapshot.lineage.bucket != bucket)
        || (!object.is_empty() && snapshot.lineage.object != object)
    {
        return Err(DiskError::FileCorrupt);
    }
    let raw = inspection.original.as_ref().ok_or(DiskError::FileCorrupt)?;
    let original = rustfs_filemeta::get_file_info(
        raw,
        crate::disk::RUSTFS_META_MULTIPART_BUCKET,
        path,
        "",
        rustfs_filemeta::FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .map_err(crate::disk::error::Error::other)?;
    let condition = crate::disk::multipart_source::capture_source_condition(
        &snapshot.lineage.bucket,
        &snapshot.lineage.object,
        path,
        &original,
        Uuid::new_v4(),
    )?;
    if rustfs_utils::http::get_consistent_str(&original.metadata, "multipart-bucket") != Some(snapshot.lineage.bucket.as_str())
        || rustfs_utils::http::get_consistent_str(&original.metadata, "multipart-object")
            != Some(snapshot.lineage.object.as_str())
        || condition.lineage != snapshot.lineage
        || original.erasure.distribution.get(slot) != Some(&original.erasure.index)
    {
        return Err(DiskError::FileCorrupt);
    }
    Ok(Some(original))
}

async fn cleanup_inspections(
    disks: &[Option<DiskStore>],
    path: &str,
    namespace_owner: Option<&Arc<dyn Send + Sync>>,
) -> Vec<Result<MultipartSourceInspection>> {
    join_all(disks.iter().map(|disk| async {
        let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
        match recover_on_disk(
            disk,
            request(path, MultipartSourceOperation::Inspect { lineage: None }),
            namespace_owner.cloned(),
        )
        .await?
        {
            MultipartSourceResponse::Inspected(inspection) => Ok(inspection),
            _ => Err(DiskError::FileCorrupt),
        }
    }))
    .await
}

fn existing_abort_certificate(
    inspections: &[Result<MultipartSourceInspection>],
    ids: &[Option<Uuid>],
    authority: &ProofVerificationContext,
) -> Result<Option<MultipartAbortCertificate>> {
    let mut selected: Option<MultipartAbortCertificate> = None;
    for inspection in inspections {
        let Ok(inspection) = inspection else { continue };
        let Some(certificate) = inspection.abort_certificate.as_ref() else { continue };
        abort_certificate_cohort(certificate, ids, authority)?;
        if let Some(current) = selected.as_ref() {
            if current.round.lineage != certificate.round.lineage {
                return Err(DiskError::FileCorrupt);
            }
            if (current.round.generation.epoch, current.round.round)
                >= (certificate.round.generation.epoch, certificate.round.round)
            {
                continue;
            }
        }
        selected = Some(certificate.clone());
    }
    Ok(selected)
}

/// Augment each physical slot with its independently validated native original.
/// A complete committed closure certificate can identify a cold residual below
/// a new read quorum; it is never returned as an ACTIVE source or an extra vote.
pub(in crate::set_disk) async fn cleanup_source_metadata(
    disks: &[Option<DiskStore>],
    scope: (&str, &str, &str),
    mut metadata: Vec<rustfs_filemeta::FileInfo>,
    mut errors: Vec<Option<DiskError>>,
    authority: &ProofVerificationContext,
) -> Result<CleanupSourceMetadata> {
    let (bucket, object, path) = scope;
    bind_multipart_proof_snapshot(disks, authority)?;
    let ids = disk_ids(disks).await;
    let inspections = cleanup_inspections(disks, path, None).await;
    if metadata.len() != disks.len() || errors.len() != disks.len() {
        return Err(DiskError::FileCorrupt);
    }
    let mut native = false;
    for (slot, info) in metadata.iter().enumerate() {
        if errors[slot].is_none() && recognized_native_source_controls(info)? {
            native = true;
        }
    }
    let mut originals = vec![None; disks.len()];
    for (slot, inspected) in inspections.iter().enumerate() {
        match inspected {
            Ok(inspection) => {
                if inspection
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.state_sha256 != [0; 32])
                    || inspection.abort_round.is_some()
                    || inspection.abort_certificate.is_some()
                {
                    native = true;
                }
                if let Some(original) = cleanup_original(inspection, slot, ids[slot], bucket, object, path)? {
                    native |= recognized_native_source_controls(&original)?;
                    originals[slot] = Some(original);
                }
            }
            Err(DiskError::UnsupportedDisk) => {}
            Err(DiskError::FileCorrupt | DiskError::ConditionalDeleteConflict) => {
                // A recognized corrupt native source cannot be downgraded to a
                // copied legacy recovery marker in the same physical slot.
                native = true;
            }
            Err(_) => {}
        }
    }
    if !native {
        return Ok(CleanupSourceMetadata {
            metadata,
            errors,
            certified_original: None,
        });
    }
    for (slot, original) in originals.iter_mut().enumerate() {
        if let Some(original) = original.take() {
            metadata[slot] = original;
            errors[slot] = None;
        } else {
            errors[slot] = Some(match &inspections[slot] {
                Err(error) => error.clone(),
                Ok(_) => DiskError::ErasureWriteQuorum,
            });
        }
    }
    let certificate = existing_abort_certificate(&inspections, &ids, authority)?;
    let certified_original = if let Some(certificate) = certificate {
        let mut original = None;
        for (slot, inspected) in inspections.iter().enumerate() {
            let inspection = inspected.as_ref().map_err(|_| DiskError::ErasureWriteQuorum)?;
            if ids[slot] != Some(inspection.disk_id) {
                return Err(DiskError::FileCorrupt);
            }
            if inspection.root_missing && inspection.snapshot.is_none() {
                continue;
            }
            let snapshot = inspection.snapshot.as_ref().ok_or(DiskError::ErasureWriteQuorum)?;
            if snapshot.phase != MultipartSourcePhase::Aborting
                || snapshot.lineage != certificate.round.lineage
                || snapshot.generation.epoch < certificate.round.generation.epoch
                || snapshot.head_sha256.is_some()
                || snapshot.capability_sha256.is_some()
            {
                return Err(DiskError::ErasureWriteQuorum);
            }
            if original.is_none() && errors[slot].is_none() {
                original = Some(metadata[slot].clone());
            }
        }
        original
    } else {
        None
    };
    Ok(CleanupSourceMetadata {
        metadata,
        errors,
        certified_original,
    })
}

/// Genuine untagged legacy heads are upgraded under physical SourceRW CAS
/// before the new native publisher dispatches. Marked ordinary completion does
/// not enter this exceptional fanout.
pub(in crate::set_disk) async fn upgrade_legacy_source(
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    path: &str,
    authority: &ProofVerificationContext,
) -> Result<()> {
    bind_multipart_proof_snapshot(disks, authority)?;
    let ids = disk_ids(disks).await;
    let inspections = cleanup_inspections(disks, path, None).await;
    let mut lineage = None;
    for (slot, inspected) in inspections.iter().enumerate() {
        let inspection = inspected.as_ref().map_err(|_| DiskError::ErasureWriteQuorum)?;
        let original =
            cleanup_original(inspection, slot, ids[slot], bucket, object, path)?.ok_or(DiskError::ErasureWriteQuorum)?;
        let snapshot = inspection.snapshot.as_ref().ok_or(DiskError::ErasureWriteQuorum)?;
        if snapshot.phase != MultipartSourcePhase::Active
            || snapshot.generation.epoch != 0
            || snapshot.state_sha256 != [0; 32]
            || snapshot.head_sha256.is_none()
        {
            return Err(DiskError::ErasureWriteQuorum);
        }
        native_source_protocol(&original)?;
        if lineage.as_ref().is_some_and(|lineage| lineage != &snapshot.lineage) {
            return Err(DiskError::FileCorrupt);
        }
        lineage = Some(snapshot.lineage.clone());
    }
    let results = join_all(disks.iter().zip(&inspections).enumerate().map(|(slot, (disk, inspected))| {
        let id = ids[slot];
        async move {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let expected = inspected
                .as_ref()
                .map_err(|_| DiskError::ErasureWriteQuorum)?
                .snapshot
                .clone()
                .ok_or(DiskError::ErasureWriteQuorum)?;
            match recover_on_disk(disk, request(path, MultipartSourceOperation::UpgradeLegacy { expected }), None).await? {
                MultipartSourceResponse::LegacyUpgraded(inspection) => {
                    let original =
                        cleanup_original(&inspection, slot, id, bucket, object, path)?.ok_or(DiskError::ErasureWriteQuorum)?;
                    if !native_source_protocol(&original)? {
                        return Err(DiskError::FileCorrupt);
                    }
                    Ok(())
                }
                _ => Err(DiskError::FileCorrupt),
            }
        }
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

/// Close every native participant without touching target metadata or bytes.
/// No source delete is dispatched until every surviving root has durably bound
/// the same complete N-proof certificate.
pub(in crate::set_disk) async fn prepare_source_cleanup(
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    path: &str,
    require_native: bool,
    authority: &ProofVerificationContext,
) -> Result<SourceCleanupAuthority> {
    bind_multipart_proof_snapshot(disks, authority)?;
    let ids = disk_ids(disks).await;
    let mut inspections = cleanup_inspections(disks, path, None).await;
    let mut native = require_native;
    for (slot, inspected) in inspections.iter().enumerate() {
        match inspected {
            Ok(inspection) => {
                let original = cleanup_original(inspection, slot, ids[slot], bucket, object, path)?;
                native |= inspection
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.state_sha256 != [0; 32])
                    || inspection.abort_round.is_some()
                    || inspection.abort_certificate.is_some();
                if let Some(original) = original {
                    native |= recognized_native_source_controls(&original)?;
                }
            }
            Err(DiskError::FileCorrupt | DiskError::ConditionalDeleteConflict) => native = true,
            Err(_) => {}
        }
    }
    if !native {
        return Ok(SourceCleanupAuthority::Legacy);
    }
    let mut certificate = existing_abort_certificate(&inspections, &ids, authority)?;
    if certificate.is_none() {
        let mut snapshots = Vec::with_capacity(disks.len());
        let mut lineage = None;
        let mut epoch = 0;
        for inspected in &inspections {
            let inspection = inspected.as_ref().map_err(|_| DiskError::ErasureWriteQuorum)?;
            let snapshot = inspection.snapshot.clone().ok_or(DiskError::ErasureWriteQuorum)?;
            if lineage.as_ref().is_some_and(|lineage| lineage != &snapshot.lineage) {
                return Err(DiskError::FileCorrupt);
            }
            epoch = epoch.max(snapshot.generation.epoch);
            lineage = Some(snapshot.lineage.clone());
            snapshots.push(snapshot);
        }
        let round = MultipartAbortRound {
            lineage: lineage.ok_or(DiskError::ErasureWriteQuorum)?,
            generation: SourceGeneration {
                epoch: epoch.checked_add(1).ok_or(DiskError::FileCorrupt)?,
                nonce: Uuid::new_v4(),
            },
            round: Uuid::new_v4(),
        };
        round.validate()?;
        let prepared = join_all(disks.iter().zip(snapshots).map(|(disk, expected)| {
            let round = round.clone();
            async move {
                let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
                match recover_on_disk(disk, request(path, MultipartSourceOperation::PrepareAbort { expected, round }), None)
                    .await?
                {
                    MultipartSourceResponse::AbortPrepared(proof) => Ok(proof),
                    _ => Err(DiskError::FileCorrupt),
                }
            }
        }))
        .await;
        let mut proofs = Vec::with_capacity(disks.len());
        for (slot, result) in prepared.into_iter().enumerate() {
            let proof = result?;
            abort_slot(&proof, slot, ids[slot], &round.lineage, authority)?;
            if proof.kind != MultipartAbortProofKind::Prepared
                || proof.round != round
                || proof.challenge != round.round
                || proof.snapshot.generation != round.generation
                || proof.certificate_sha256.is_some()
            {
                return Err(DiskError::FileCorrupt);
            }
            proofs.push(proof);
        }
        let complete = MultipartAbortCertificate {
            round,
            disk_ids: ids
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .ok_or(DiskError::ErasureWriteQuorum)?,
            proofs,
        };
        abort_certificate_cohort(&complete, &ids, authority)?;
        certificate = Some(complete);
        inspections = cleanup_inspections(disks, path, None).await;
    }
    let certificate = certificate.ok_or(DiskError::ErasureWriteQuorum)?;
    let digest = certificate.digest()?;
    let mut present = 0;
    for (slot, inspected) in inspections.iter().enumerate() {
        let inspection = inspected.as_ref().map_err(|_| DiskError::ErasureWriteQuorum)?;
        if ids[slot] != Some(inspection.disk_id) {
            return Err(DiskError::FileCorrupt);
        }
        if inspection.root_missing && inspection.snapshot.is_none() {
            continue;
        }
        let snapshot = inspection.snapshot.as_ref().ok_or(DiskError::ErasureWriteQuorum)?;
        if snapshot.phase != MultipartSourcePhase::Aborting
            || snapshot.lineage != certificate.round.lineage
            || snapshot.generation.epoch < certificate.round.generation.epoch
            || snapshot.head_sha256.is_some()
            || snapshot.capability_sha256.is_some()
        {
            return Err(DiskError::ErasureWriteQuorum);
        }
        present += 1;
    }
    if present == 0 {
        return Err(DiskError::FileNotFound);
    }
    let committed = join_all(disks.iter().zip(&inspections).map(|(disk, inspected)| {
        let certificate = certificate.clone();
        async move {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let inspection = inspected.as_ref().map_err(|_| DiskError::ErasureWriteQuorum)?;
            if inspection.root_missing && inspection.snapshot.is_none() {
                return Ok(None);
            }
            let expected = inspection.snapshot.clone().ok_or(DiskError::ErasureWriteQuorum)?;
            match recover_on_disk(disk, request(path, MultipartSourceOperation::CommitAbort { expected, certificate }), None)
                .await?
            {
                MultipartSourceResponse::AbortCommitted(proof) => Ok(Some(proof)),
                _ => Err(DiskError::FileCorrupt),
            }
        }
    }))
    .await;
    let mut snapshots = Vec::with_capacity(disks.len());
    for (slot, result) in committed.into_iter().enumerate() {
        let Some(proof) = result? else {
            snapshots.push(None);
            continue;
        };
        abort_slot(&proof, slot, ids[slot], &certificate.round.lineage, authority)?;
        if proof.kind != MultipartAbortProofKind::Committed
            || proof.round != certificate.round
            || proof.challenge != certificate.round.round
            || proof.certificate_sha256 != Some(digest)
        {
            return Err(DiskError::FileCorrupt);
        }
        snapshots.push(Some(proof.snapshot));
    }
    Ok(SourceCleanupAuthority::Native {
        certificate: Box::new(certificate),
        snapshots,
    })
}

pub(in crate::set_disk) async fn delete_closed_source(
    disks: &[Option<DiskStore>],
    path: &str,
    certificate: &MultipartAbortCertificate,
    snapshots: Vec<Option<crate::disk::multipart_source::MultipartSourceSnapshot>>,
    authority: &ProofVerificationContext,
    quorum: usize,
) -> Result<()> {
    let ids = disk_ids(disks).await;
    abort_certificate_cohort(certificate, &ids, authority)?;
    if snapshots.len() != disks.len() {
        return Err(DiskError::FileCorrupt);
    }
    let digest = certificate.digest()?;
    let deleted = join_all(disks.iter().zip(snapshots).map(|(disk, expected)| {
        let certificate = certificate.clone();
        async move {
            let Some(expected) = expected else {
                return Ok(None);
            };
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            match recover_on_disk(
                disk,
                request(path, MultipartSourceOperation::DeleteAborted { expected, certificate }),
                None,
            )
            .await?
            {
                MultipartSourceResponse::AbortDeleted(proof) => Ok(Some(proof)),
                _ => Err(DiskError::FileCorrupt),
            }
        }
    }))
    .await;
    let mut count = 0;
    let mut failed = false;
    for (slot, result) in deleted.into_iter().enumerate() {
        match result {
            Ok(Some(proof)) => {
                abort_slot(&proof, slot, ids[slot], &certificate.round.lineage, authority)?;
                if proof.kind != MultipartAbortProofKind::Deleted
                    || proof.round != certificate.round
                    || proof.challenge != certificate.round.round
                    || proof.certificate_sha256 != Some(digest)
                {
                    return Err(DiskError::FileCorrupt);
                }
                count += 1;
            }
            Ok(None) => {}
            Err(_) => failed = true,
        }
    }
    if count >= quorum {
        return Ok(());
    }
    if !failed {
        // A cold residual can be smaller than Q. Do not count absent roots as
        // votes: prove every configured root is physically absent instead.
        let after = cleanup_inspections(disks, path, None).await;
        if after.iter().enumerate().all(|(slot, inspected)| {
            inspected.as_ref().is_ok_and(|inspection| {
                ids[slot] == Some(inspection.disk_id) && inspection.root_missing && inspection.snapshot.is_none()
            })
        }) {
            return Ok(());
        }
    }
    Err(DiskError::ErasureWriteQuorum)
}

/// Capabilities enter only from the original failed publication owner. Cold
/// recovery passes `None`; inspection can never recreate an Undo capability.
pub(in crate::set_disk) async fn recover_source(
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    path: &str,
    mut owned: Option<Vec<Option<MultipartPublicationResult>>>,
    authority: &ProofVerificationContext,
    namespace_owner: Option<Arc<dyn Send + Sync>>,
) -> Result<SourceRecovery> {
    bind_multipart_proof_snapshot(disks, authority)?;
    let ids = disk_ids(disks).await;
    let mut inspections = inspect(disks, path, namespace_owner.as_ref()).await;
    if inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.snapshot.as_ref())
        .any(|snapshot| snapshot.phase == MultipartSourcePhase::Aborting)
    {
        return Err(DiskError::ErasureWriteQuorum);
    }
    let lineage = inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.snapshot.as_ref())
        .map(|snapshot| &snapshot.lineage)
        .next()
        .or_else(|| {
            owned
                .as_ref()
                .and_then(|owned| owned.iter().flatten().next().map(|result| &result.publication.lineage))
        })
        .cloned();
    let Some(lineage) = lineage else {
        return if inspections.iter().enumerate().all(|(slot, inspection)| {
            inspection
                .as_ref()
                .is_some_and(|inspection| inspection.snapshot.is_none() && ids[slot] == Some(inspection.disk_id))
        }) {
            Ok(SourceRecovery::Missing)
        } else {
            Err(DiskError::ErasureWriteQuorum)
        };
    };
    lineage.validate()?;
    if lineage.bucket != bucket || lineage.object != object || lineage.path != path || lineage.geometry.disks() != disks.len() {
        return Err(DiskError::FileCorrupt);
    }
    compatible_inspections(&inspections, &ids, &lineage)?;
    let q = lineage.geometry.write_quorum();
    let h = lineage.geometry.negative_quorum();
    if owned.is_none() {
        let mut publications = Vec::new();
        for publication in inspections
            .iter()
            .flatten()
            .filter_map(|inspection| inspection.snapshot.as_ref())
            .filter_map(|snapshot| snapshot.publication.as_ref())
        {
            if !publications.contains(publication) {
                publications.push(publication.clone());
            }
        }
        for publication in publications {
            let copies = inspections
                .iter()
                .flatten()
                .filter_map(|inspection| inspection.snapshot.as_ref())
                .filter(|snapshot| {
                    snapshot.publication.as_ref() == Some(&publication)
                        && matches!(
                            snapshot.phase,
                            MultipartSourcePhase::ConsumedUnsealed
                                | MultipartSourcePhase::LocalTerminalSeal
                                | MultipartSourcePhase::GlobalTerminalCertified
                        )
                })
                .count();
            if copies >= q && seal_consumed(disks, &ids, &inspections, &publication, authority).await? {
                return Ok(SourceRecovery::Terminal);
            }
        }
    }
    let active = inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.snapshot.as_ref())
        .filter(|snapshot| snapshot.phase == MultipartSourcePhase::Active)
        .collect::<Vec<_>>();
    if owned.is_none() && active.len() >= q && active.iter().all(|snapshot| snapshot.generation == active[0].generation) {
        return Ok(SourceRecovery::Active);
    }
    let snapshots = inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.snapshot.as_ref())
        .collect::<Vec<_>>();
    let generations = snapshots.iter().map(|snapshot| snapshot.generation).chain(
        owned
            .iter()
            .flat_map(|results| results.iter().flatten())
            .map(|result| result.publication.source_generation),
    );
    let generation = generations
        .clone()
        .max_by_key(|generation| generation.epoch)
        .ok_or(DiskError::ErasureWriteQuorum)?;
    if generations
        .filter(|candidate| candidate.epoch == generation.epoch)
        .any(|candidate| candidate != generation)
    {
        return Err(DiskError::ErasureWriteQuorum);
    }
    // A ready round is reusable only until its activated generation publishes
    // again. Its immutable certificate can remain on an unactivated participant
    // after another participant has consumed the new head. Resuming that stale
    // round would never recover a second crash: claim the current generation
    // into a fresh round instead, without combining historical publications.
    let generation_published = snapshots
        .iter()
        .filter_map(|snapshot| snapshot.publication.as_ref())
        .any(|publication| publication.source_generation == generation)
        || owned
            .iter()
            .flat_map(|results| results.iter().flatten())
            .any(|result| result.publication.source_generation == generation);
    let mut rounds = inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.recovery.as_ref())
        .filter(|round| round.generation == generation && !generation_published);
    let existing_round = rounds.next().cloned();
    if rounds.any(|round| Some(round) != existing_round.as_ref()) {
        return Err(DiskError::ErasureWriteQuorum);
    }
    let round = if let Some(round) = existing_round {
        round
    } else {
        let old_attempt = snapshots
            .iter()
            .filter_map(|snapshot| snapshot.publication.as_ref())
            .filter(|publication| publication.source_generation == generation)
            .map(|publication| publication.attempt)
            .next()
            .or_else(|| {
                owned
                    .iter()
                    .flat_map(|results| results.iter().flatten())
                    .find(|result| result.publication.source_generation == generation)
                    .map(|result| result.publication.attempt)
            })
            .ok_or(DiskError::ErasureWriteQuorum)?;
        MultipartRecoveryRound {
            lineage: lineage.clone(),
            from_generation: generation,
            old_attempt,
            generation: SourceGeneration {
                epoch: generation.epoch.checked_add(1).ok_or(DiskError::FileCorrupt)?,
                nonce: Uuid::new_v4(),
            },
            round: Uuid::new_v4(),
        }
    };
    if round.lineage != lineage {
        return Err(DiskError::FileCorrupt);
    }
    if inspections
        .iter()
        .flatten()
        .filter_map(|inspection| inspection.snapshot.as_ref())
        .filter_map(|snapshot| snapshot.publication.as_ref())
        .any(|publication| publication.source_generation == round.from_generation && publication.attempt != round.old_attempt)
    {
        // Disjoint old attempts are not one negative certificate.
        return Err(DiskError::ErasureWriteQuorum);
    }
    let fixed_ids = ids.iter().copied().collect::<Option<Vec<_>>>();
    let mut claims = Vec::new();
    for inspection in inspections.iter().flatten() {
        if let Some(certificate) = &inspection.negative_certificate
            && certificate.round == round
            && Some(&certificate.disk_ids) == fixed_ids.as_ref()
        {
            certificate.verify_with_authority(authority, MultipartProofKind::Claimed, h)?;
            claims = certificate.proofs.clone();
            break;
        }
    }
    if claims.len() < h {
        let mut capabilities = owned.take().unwrap_or_else(|| (0..disks.len()).map(|_| None).collect());
        if capabilities.len() != disks.len() {
            return Err(DiskError::FileCorrupt);
        }
        let results = join_all(disks.iter().zip(&inspections).enumerate().map(|(slot, (disk, inspection))| {
            let capability = capabilities[slot].take();
            let round = &round;
            let namespace_owner = namespace_owner.as_ref();
            async move {
                let disk = disk.as_ref()?;
                let operation = if let Some(result) = capability {
                    if result.verify_with_authority(authority).is_err() || result.publication.attempt != round.old_attempt {
                        return None;
                    }
                    // The original ACK supplies the exact expected snapshot.
                    // A failed Inspect is not grounds to discard a live
                    // owner's one-use authority before attempting native CAS.
                    MultipartSourceOperation::UndoAndClaim {
                        expected: Box::new(result.proof.snapshot),
                        publication: result.publication,
                        capability: result.undo_capability,
                        round: round.clone(),
                    }
                } else {
                    let snapshot = inspection.as_ref()?.snapshot.as_ref()?;
                    if !matches!(
                        snapshot.phase,
                        MultipartSourcePhase::Active | MultipartSourcePhase::Prepared | MultipartSourcePhase::Recovering
                    ) {
                        return None;
                    }
                    MultipartSourceOperation::Claim {
                        expected: snapshot.clone(),
                        round: round.clone(),
                    }
                };
                match recover_on_disk(disk, request(path, operation), namespace_owner.cloned()).await {
                    Ok(MultipartSourceResponse::Claimed(proof)) => Some(proof),
                    _ => None,
                }
            }
        }))
        .await;
        for (slot, proof) in results.into_iter().enumerate() {
            let Some(proof) = proof else { continue };
            verify_slot(&proof, slot, ids[slot], &lineage, authority)?;
            verify_recovery_shape(&proof, MultipartProofKind::Claimed, &round)?;
            claims.push(proof);
        }
    }
    if claims.len() < h {
        // Missing or unknown outcomes are availability failures, not proof
        // that the immutable records themselves are corrupt.
        return Err(DiskError::ErasureWriteQuorum);
    }
    // Make H claims durable before waiting for an offline participant's
    // identity/restoration. Losing the live owner's caps first could strand a
    // third unknown publication permanently; the blocked round is resumable.
    let fixed_ids = fixed_ids.ok_or(DiskError::ErasureWriteQuorum)?;
    let negative = MultipartCertificate {
        round: round.clone(),
        disk_ids: fixed_ids.clone(),
        proofs: claims,
    };
    negative.verify_with_authority(authority, MultipartProofKind::Claimed, h)?;
    for proof in &negative.proofs {
        verify_recovery_shape(proof, MultipartProofKind::Claimed, &negative.round)?;
    }
    inspections = inspect(disks, path, namespace_owner.as_ref()).await;
    compatible_inspections(&inspections, &ids, &lineage)?;
    let mut ready = None;
    for inspection in inspections.iter().flatten() {
        if let Some(certificate) = &inspection.ready_certificate
            && certificate.round == round
            && certificate.disk_ids == fixed_ids
        {
            certificate.verify_with_authority(authority, MultipartProofKind::Restored, disks.len())?;
            ready = Some(certificate.clone());
            break;
        }
    }
    let ready = if let Some(ready) = ready {
        ready
    } else {
        let restored = join_all(disks.iter().zip(&inspections).map(|(disk, inspection)| {
            let certificate = negative.clone();
            let namespace_owner = namespace_owner.as_ref();
            async move {
                let disk = disk.as_ref()?;
                let expected = inspection.as_ref()?.snapshot.clone()?;
                match recover_on_disk(
                    disk,
                    request(path, MultipartSourceOperation::Restore { expected, certificate }),
                    namespace_owner.cloned(),
                )
                .await
                {
                    Ok(MultipartSourceResponse::Restored(proof)) => Some(proof),
                    _ => None,
                }
            }
        }))
        .await;
        let mut restore_proofs = Vec::with_capacity(disks.len());
        for (slot, proof) in restored.into_iter().enumerate() {
            let proof = proof.ok_or(DiskError::ErasureWriteQuorum)?;
            verify_slot(&proof, slot, ids[slot], &lineage, authority)?;
            verify_recovery_shape(&proof, MultipartProofKind::Restored, &round)?;
            restore_proofs.push(proof);
        }
        let ready = MultipartCertificate {
            round,
            disk_ids: fixed_ids,
            proofs: restore_proofs,
        };
        ready.verify_with_authority(authority, MultipartProofKind::Restored, disks.len())?;
        ready
    };
    for proof in &ready.proofs {
        verify_recovery_shape(proof, MultipartProofKind::Restored, &ready.round)?;
    }
    // A crash after partial activation resumes the persisted common cert.
    // Reinspect current revisions; do not restore an already active head or
    // replay an obsolete per-slot expected state from the certificate.
    inspections = inspect(disks, path, namespace_owner.as_ref()).await;
    compatible_inspections(&inspections, &ids, &lineage)?;
    let activated = join_all(disks.iter().zip(&inspections).map(|(disk, inspection)| {
        let certificate = ready.clone();
        let namespace_owner = namespace_owner.as_ref();
        async move {
            let disk = disk.as_ref()?;
            let expected = inspection.as_ref()?.snapshot.clone()?;
            match recover_on_disk(
                disk,
                request(path, MultipartSourceOperation::Activate { expected, certificate }),
                namespace_owner.cloned(),
            )
            .await
            {
                Ok(MultipartSourceResponse::Activated(proof)) => Some(proof),
                _ => None,
            }
        }
    }))
    .await;
    let mut active_count = 0;
    for (slot, proof) in activated.into_iter().enumerate() {
        let Some(proof) = proof else { continue };
        verify_slot(&proof, slot, ids[slot], &lineage, authority)?;
        verify_recovery_shape(&proof, MultipartProofKind::Activated, &ready.round)?;
        active_count += 1;
    }
    if active_count < q {
        return Err(DiskError::ErasureWriteQuorum);
    }
    Ok(SourceRecovery::Activated)
}
