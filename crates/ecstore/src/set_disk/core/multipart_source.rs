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
    MULTIPART_SOURCE_PROTOCOL_VERSION, MultipartCertificate, MultipartProofKind, MultipartPublicationResult,
    MultipartRecoveryRound, MultipartSourceInspection, MultipartSourceOperation, MultipartSourcePhase, MultipartSourceProof,
    MultipartSourceRequest, MultipartSourceResponse, ProofVerificationContext, PublicationIdentity, SourceGeneration,
    SourceLineage,
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
    let proofs = join_all(disks.iter().map(|disk| async {
        let disk = disk.as_ref()?;
        let operation = MultipartSourceOperation::ConfirmTerminal {
            expected: None,
            publication: publication.clone(),
            challenge,
        };
        match disk.recover_multipart_source(request(&lineage.path, operation)).await {
            Ok(MultipartSourceResponse::TerminalConfirmed(proof)) => Some(proof),
            _ => None,
        }
    }))
    .await;
    let mut confirmed = Vec::new();
    for (slot, proof) in proofs.into_iter().enumerate() {
        let Some(proof) = proof else { continue };
        verify_slot(&proof, slot, ids[slot], lineage, authority)?;
        if proof.challenge != challenge
            || proof.snapshot.publication.as_ref() != Some(publication)
            || !matches!(proof.kind, MultipartProofKind::TerminalTarget | MultipartProofKind::RootMissingTerminal)
        {
            return Err(DiskError::FileCorrupt);
        }
        if confirmed.contains(&proof.snapshot.disk_id) {
            return Err(DiskError::FileCorrupt);
        }
        confirmed.push(proof.snapshot.disk_id);
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
            if let Some(snapshot) = &inspection.snapshot {
                if snapshot.physical_slot != slot || &snapshot.lineage != lineage {
                    return Err(DiskError::FileCorrupt);
                }
            }
        }
    }
    Ok(())
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
        if let Some(certificate) = &inspection.negative_certificate {
            if certificate.round == round && Some(&certificate.disk_ids) == fixed_ids.as_ref() {
                certificate.verify_with_authority(authority, MultipartProofKind::Claimed, h)?;
                claims = certificate.proofs.clone();
                break;
            }
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
                        expected: result.proof.snapshot,
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
        if let Some(certificate) = &inspection.ready_certificate {
            if certificate.round == round && certificate.disk_ids == fixed_ids {
                certificate.verify_with_authority(authority, MultipartProofKind::Restored, disks.len())?;
                ready = Some(certificate.clone());
                break;
            }
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
