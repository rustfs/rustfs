// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Fenced, exceptional recovery of one canonical multipart source.

use super::{
    LocalDisk,
    multipart_source_state::{LoadedSource, NativeNegativeClaim, NativeUndoIntent, SourceState, SourceStore},
};
use crate::disk::{
    DeleteOptions,
    error::{DiskError, Result},
    multipart_source::{
        MultipartCertificate, MultipartProofKind, MultipartRecoveryRound, MultipartSourceInspection, MultipartSourceOperation,
        MultipartSourcePhase, MultipartSourceProof, MultipartSourceRequest, MultipartSourceResponse, MultipartSourceSnapshot,
        PublicationIdentity,
    },
    os,
};
use uuid::Uuid;

fn validate_round(round: &MultipartRecoveryRound) -> Result<()> {
    round.lineage.validate()?;
    if round.from_generation.nonce.is_nil()
        || round.generation.nonce.is_nil()
        || round.old_attempt.is_nil()
        || round.round.is_nil()
        || round.generation.epoch <= round.from_generation.epoch
        || round.generation.nonce == round.from_generation.nonce
        || (round.from_generation.epoch == 0 && round.from_generation.nonce != round.lineage.original_data_dir)
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

fn require_expected(loaded: &LoadedSource, expected: &MultipartSourceSnapshot) -> Result<()> {
    if loaded.snapshot.as_ref() != Some(expected) {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

fn require_round_lineage(state: &SourceState, round: &MultipartRecoveryRound) -> Result<()> {
    validate_round(round)?;
    if state.lineage != round.lineage {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    Ok(())
}

fn same_claim(state: &SourceState, round: &MultipartRecoveryRound) -> bool {
    state.negative_claim.as_ref().is_some_and(|claim| {
        let advances = if claim.owned_undo { 2 } else { 1 };
        claim.round == *round
            && claim.from.revision.checked_add(advances) == Some(state.revision)
            && state.recovery.as_ref() == Some(round)
            && state.generation == round.generation
            && state.phase == MultipartSourcePhase::Recovering
            && state.capability_sha256.is_none()
    })
}

async fn present_proof(
    store: &SourceStore,
    kind: MultipartProofKind,
    round: &MultipartRecoveryRound,
) -> Result<MultipartSourceProof> {
    let loaded = store.load().await?;
    let snapshot = loaded.snapshot.ok_or(DiskError::ConditionalDeleteConflict)?;
    if !loaded.head_matches_state
        || snapshot.generation != round.generation
        || snapshot.lineage != round.lineage
        || snapshot.head_sha256.is_none()
        || snapshot.capability_sha256.is_some()
        || !matches!(
            (kind, snapshot.phase),
            (
                MultipartProofKind::Claimed | MultipartProofKind::Restored,
                MultipartSourcePhase::Recovering
            ) | (MultipartProofKind::Activated, MultipartSourcePhase::Active)
        )
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    store.issue_proof(kind, round.round, Some(round.clone()), snapshot)
}

fn validate_certificate_shape(
    certificate: &MultipartCertificate,
    kind: MultipartProofKind,
    actual: &MultipartSourceSnapshot,
) -> Result<()> {
    validate_round(&certificate.round)?;
    let geometry = &certificate.round.lineage.geometry;
    let quorum = match kind {
        MultipartProofKind::Claimed => geometry.negative_quorum(),
        MultipartProofKind::Restored => geometry.disks(),
        _ => return Err(DiskError::FileCorrupt),
    };
    if certificate.disk_ids.len() != geometry.disks()
        || certificate.proofs.len() < quorum
        || certificate.proofs.len() > geometry.disks()
        || actual.lineage != certificate.round.lineage
        || certificate.disk_ids.get(actual.physical_slot) != Some(&actual.disk_id)
    {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    for proof in &certificate.proofs {
        let snapshot = &proof.snapshot;
        if snapshot.generation != certificate.round.generation
            || snapshot.lineage != certificate.round.lineage
            || snapshot.phase != MultipartSourcePhase::Recovering
            || snapshot.head_sha256.is_none()
            || snapshot.capability_sha256.is_some()
            || proof.challenge != certificate.round.round
            || proof.recovery.as_ref() != Some(&certificate.round)
        {
            return Err(DiskError::FileCorrupt);
        }
    }
    Ok(())
}

impl LocalDisk {
    pub(super) async fn recover_multipart_source_native(
        &self,
        request: MultipartSourceRequest,
    ) -> Result<MultipartSourceResponse> {
        request.validate()?;
        // Source RW ownership precedes source namespace, target metadata, and
        // target publication ownership. Every filesystem tail retains it.
        let store = self.open_multipart_source_store(&request.path, None).await?;
        match request.operation {
            operation @ (MultipartSourceOperation::UpgradeLegacy { .. }
            | MultipartSourceOperation::PrepareAbort { .. }
            | MultipartSourceOperation::CommitAbort { .. }
            | MultipartSourceOperation::DeleteAborted { .. }
            | MultipartSourceOperation::ConfirmCleanupTarget { .. }) => {
                self.recover_multipart_abort_owned(store, operation).await
            }
            MultipartSourceOperation::Inspect { lineage } => {
                let loaded = store.load().await?;
                if let (Some(expected), Some(snapshot)) = (lineage.as_ref(), loaded.snapshot.as_ref())
                    && expected != &snapshot.lineage
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let mut negative_certificate = None;
                let mut ready_certificate = None;
                if let Some(state) = loaded.state.as_ref()
                    && state.phase != MultipartSourcePhase::Aborting
                    && let Some(round) = state.recovery.as_ref()
                {
                    if state.negative_certificate_sha256.is_some() {
                        negative_certificate = Some(store.read_certificate(round.round, false).await?);
                    }
                    if state.ready_certificate_sha256.is_some() {
                        ready_certificate = Some(store.read_certificate(round.round, true).await?);
                    }
                }
                let abort_certificate = store.read_abort_certificate(&loaded).await?;
                Ok(MultipartSourceResponse::Inspected(MultipartSourceInspection {
                    disk_id: store.disk_id,
                    snapshot: loaded.snapshot,
                    original: loaded.original,
                    recovery: loaded.state.as_ref().and_then(|state| state.recovery.clone()),
                    negative_certificate,
                    ready_certificate,
                    root_missing: store.root_missing().await?,
                    abort_round: loaded
                        .state
                        .as_ref()
                        .and_then(|state| state.abort.as_ref().map(|abort| abort.round.clone())),
                    abort_certificate,
                    terminal_certificate: loaded.state.map_or_else(Vec::new, |state| state.terminal_certificate),
                }))
            }
            MultipartSourceOperation::Claim { expected, round } => {
                let loaded = store.load().await?;
                require_expected(&loaded, &expected)?;
                validate_round(&round)?;
                if expected.lineage != round.lineage
                    || !matches!(
                        expected.phase,
                        MultipartSourcePhase::Active | MultipartSourcePhase::Prepared | MultipartSourcePhase::Recovering
                    )
                    || (expected.generation != round.from_generation && expected.generation != round.generation)
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let mut state = store.ensure_state(&loaded).await?;
                require_round_lineage(&state, &round)?;
                if same_claim(&state, &round) {
                    // An intent written before its head replacement resumes the
                    // same claim; a later Restore/Activate cannot reuse it.
                    if !loaded.head_matches_state {
                        let from_head = state.negative_claim.as_ref().and_then(|claim| claim.from.head_sha256);
                        if from_head.is_none() || loaded.snapshot.as_ref().and_then(|snapshot| snapshot.head_sha256) != from_head
                        {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                        store.restore_original(&mut state, true, None).await?;
                    }
                } else {
                    if state.generation != round.from_generation
                        || loaded.head.is_none()
                        || !loaded.head_matches_state
                        || !matches!(
                            state.phase,
                            MultipartSourcePhase::Active | MultipartSourcePhase::Prepared | MultipartSourcePhase::Recovering
                        )
                    {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    state.negative_claim = Some(NativeNegativeClaim {
                        from: expected,
                        round: round.clone(),
                        owned_undo: false,
                    });
                    state.generation = round.generation;
                    state.recovery = Some(round.clone());
                    state.capability_sha256 = None;
                    state.undo = None;
                    state.restore_from = None;
                    state.negative_certificate_sha256 = None;
                    state.ready_certificate_sha256 = None;
                    state.advance(MultipartSourcePhase::Recovering)?;
                    store.persist(&state).await?;
                    store.restore_original(&mut state, true, None).await?;
                }
                Ok(MultipartSourceResponse::Claimed(
                    present_proof(&store, MultipartProofKind::Claimed, &round).await?,
                ))
            }
            MultipartSourceOperation::UndoAndClaim {
                expected,
                publication,
                capability,
                round,
            } => {
                let expected = *expected;
                let loaded = store.load().await?;
                let mut state = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?.clone();
                require_round_lineage(&state, &round)?;
                if state.publication.as_ref() != Some(&publication)
                    || publication.lineage != round.lineage
                    || publication.source_generation != round.from_generation
                    || publication.attempt != round.old_attempt
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let capability_sha256 = capability.digest();
                if same_claim(&state, &round) {
                    let claim = state.negative_claim.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?;
                    if !claim.owned_undo
                        || claim.from != expected
                        || !state.undo.as_ref().is_some_and(|undo| {
                            undo.from == expected
                                && undo.round == round
                                && undo.capability_sha256 == capability_sha256
                                && undo.target_undone
                        })
                    {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    // No fresh undo right is issued; only the durable result of
                    // this exact, unchanged owned operation can be replayed.
                    if !loaded.head_matches_state {
                        if loaded.head.is_some() {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                        store.restore_original(&mut state, true, None).await?;
                    }
                    return Ok(MultipartSourceResponse::Claimed(
                        present_proof(&store, MultipartProofKind::Claimed, &round).await?,
                    ));
                }
                let resuming = state.phase == MultipartSourcePhase::UndoIntent
                    && state.undo.as_ref().is_some_and(|undo| {
                        undo.from == expected && undo.round == round && undo.capability_sha256 == capability_sha256
                    });
                if !resuming {
                    require_expected(&loaded, &expected)?;
                    if loaded.head.is_some()
                        || state.generation != round.from_generation
                        || state.capability_sha256 != Some(capability_sha256)
                        || !matches!(state.phase, MultipartSourcePhase::ConsumedUnsealed | MultipartSourcePhase::Prepared)
                        || !state.terminal_certificate.is_empty()
                    {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                }
                let target = self.get_object_path(&publication.lineage.bucket, &publication.lineage.object)?;
                let target_owner = os::acquire_metadata_mutation_lease(&target, store.owner.clone()).await;
                if !resuming {
                    let raw = store
                        .read_target(&publication)
                        .await?
                        .ok_or(DiskError::ConditionalDeleteConflict)?;
                    if !store.target_matches(&publication, &raw)? {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    state.undo = Some(NativeUndoIntent {
                        from: expected.clone(),
                        round: round.clone(),
                        capability_sha256,
                        target_undone: false,
                    });
                    state.advance(MultipartSourcePhase::UndoIntent)?;
                    store.persist(&state).await?;
                }
                if !state.undo.as_ref().is_some_and(|undo| undo.target_undone) {
                    store.undo_exact_publication(&mut state, Some(target_owner.clone())).await?;
                    store.persist(&state).await?;
                }
                state.negative_claim = Some(NativeNegativeClaim {
                    from: expected,
                    round: round.clone(),
                    owned_undo: true,
                });
                state.generation = round.generation;
                state.recovery = Some(round.clone());
                state.capability_sha256 = None;
                state.advance(MultipartSourcePhase::Recovering)?;
                store.persist(&state).await?;
                store.restore_original(&mut state, true, None).await?;
                let proof = present_proof(&store, MultipartProofKind::Claimed, &round).await?;
                drop(target_owner);
                self.reclaim_failed_multipart_attempt(&store, &publication).await?;
                Ok(MultipartSourceResponse::Claimed(proof))
            }
            MultipartSourceOperation::Restore { expected, certificate } => {
                let loaded = store.load().await?;
                require_expected(&loaded, &expected)?;
                validate_certificate_shape(&certificate, MultipartProofKind::Claimed, &expected)?;
                certificate.verify_with_authority(
                    &store.authority,
                    MultipartProofKind::Claimed,
                    certificate.round.lineage.geometry.negative_quorum(),
                )?;
                store.verify_certificate_cohort(&certificate)?;
                let round = &certificate.round;
                if expected.generation != round.from_generation && expected.generation != round.generation {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let mut state = store.ensure_state(&loaded).await?;
                require_round_lineage(&state, round)?;
                if state.phase == MultipartSourcePhase::GlobalTerminalCertified || !state.terminal_certificate.is_empty() {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                if state.generation == round.generation {
                    if state.phase != MultipartSourcePhase::Recovering
                        || state.recovery.as_ref() != Some(round)
                        || state.ready_certificate_sha256.is_some()
                    {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    if state.negative_certificate_sha256.is_some() {
                        if store.read_certificate(round.round, false).await? != certificate {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                        if !loaded.head_matches_state {
                            store.restore_original(&mut state, true, None).await?;
                        }
                        return Ok(MultipartSourceResponse::Restored(
                            present_proof(&store, MultipartProofKind::Restored, round).await?,
                        ));
                    }
                } else {
                    if state.generation != round.from_generation
                        || matches!(
                            state.phase,
                            MultipartSourcePhase::RootMissing
                                | MultipartSourcePhase::GlobalTerminalCertified
                                | MultipartSourcePhase::Aborting
                        )
                        || (matches!(
                            state.phase,
                            MultipartSourcePhase::Prepared
                                | MultipartSourcePhase::ConsumedUnsealed
                                | MultipartSourcePhase::UndoIntent
                                | MultipartSourcePhase::LocalTerminalSeal
                        ) && state.publication.as_ref().is_some_and(|publication| {
                            publication.source_generation != round.from_generation || publication.attempt != round.old_attempt
                        }))
                    {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                    // A matching H proof may supersede a partial local seal.
                    // It cannot undo a target or revive its old capability.
                    if state.phase == MultipartSourcePhase::LocalTerminalSeal && state.publication.is_none() {
                        return Err(DiskError::ConditionalDeleteConflict);
                    }
                }
                let certificate_sha256 = store.persist_certificate(round.round, false, &certificate).await?;
                state.restore_from = Some(expected.clone());
                state.generation = round.generation;
                state.recovery = Some(round.clone());
                state.negative_certificate_sha256 = Some(certificate_sha256);
                state.ready_certificate_sha256 = None;
                state.capability_sha256 = None;
                state.undo = None;
                state.advance(MultipartSourcePhase::Recovering)?;
                store.persist(&state).await?;
                store.restore_original(&mut state, true, None).await?;
                Ok(MultipartSourceResponse::Restored(
                    present_proof(&store, MultipartProofKind::Restored, round).await?,
                ))
            }
            MultipartSourceOperation::Activate { expected, certificate } => {
                let loaded = store.load().await?;
                require_expected(&loaded, &expected)?;
                validate_certificate_shape(&certificate, MultipartProofKind::Restored, &expected)?;
                certificate.verify_with_authority(
                    &store.authority,
                    MultipartProofKind::Restored,
                    certificate.round.lineage.geometry.disks(),
                )?;
                store.verify_certificate_cohort(&certificate)?;
                let mut state = loaded.state.as_ref().ok_or(DiskError::ConditionalDeleteConflict)?.clone();
                let round = &certificate.round;
                require_round_lineage(&state, round)?;
                if state.generation != round.generation
                    || state.recovery.as_ref() != Some(round)
                    || loaded.head.is_none()
                    || state.capability_sha256.is_some()
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
                let own = certificate
                    .proofs
                    .iter()
                    .find(|proof| proof.snapshot.disk_id == store.disk_id && proof.snapshot.physical_slot == state.physical_slot)
                    .ok_or(DiskError::ConditionalDeleteConflict)?;
                let certificate_sha256 = match state.phase {
                    MultipartSourcePhase::Recovering => {
                        if own.snapshot != expected || !loaded.head_matches_state || state.ready_certificate_sha256.is_some() {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                        let digest = store.persist_certificate(round.round, true, &certificate).await?;
                        state.ready_certificate_sha256 = Some(digest);
                        state.advance(MultipartSourcePhase::Active)?;
                        store.persist(&state).await?;
                        digest
                    }
                    MultipartSourcePhase::Active => {
                        if own.snapshot.revision.checked_add(1) != Some(state.revision)
                            || (!loaded.head_matches_state && !loaded.head_matches_activation)
                            || store.read_certificate(round.round, true).await? != certificate
                        {
                            return Err(DiskError::ConditionalDeleteConflict);
                        }
                        state.ready_certificate_sha256.ok_or(DiskError::ConditionalDeleteConflict)?
                    }
                    _ => return Err(DiskError::ConditionalDeleteConflict),
                };
                store.restore_original(&mut state, false, Some(certificate_sha256)).await?;
                Ok(MultipartSourceResponse::Activated(
                    present_proof(&store, MultipartProofKind::Activated, round).await?,
                ))
            }
            MultipartSourceOperation::ConfirmTerminal {
                expected,
                publication,
                challenge,
            } => {
                self.seal_multipart_source(&store, expected.as_ref(), &publication, challenge, true)
                    .await
            }
            MultipartSourceOperation::SealConsumedTerminal {
                expected,
                publication,
                challenge,
            } => {
                self.seal_multipart_source(&store, Some(&expected), &publication, challenge, false)
                    .await
            }
        }
    }

    async fn reclaim_failed_multipart_attempt(&self, store: &SourceStore, publication: &PublicationIdentity) -> Result<()> {
        let path = format!("{}/{}", publication.lineage.object, publication.final_data_dir);
        // A deferred snapshot reclaim can run after a heal adopted this DD.
        // The existing orphan path rechecks metadata and protected children
        // under a fresh metadata lease; never queue bare recursive deletion.
        match self
            .delete_data_dir_with_namespace_owner(
                &publication.lineage.bucket,
                &path,
                DeleteOptions {
                    orphan_data_dir: true,
                    ..Default::default()
                },
                store.owner.clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(DiskError::FileNotFound | DiskError::VolumeNotFound | DiskError::DiskOngoingReq | DiskError::FileCorrupt) => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn seal_multipart_source(
        &self,
        store: &SourceStore,
        expected: Option<&MultipartSourceSnapshot>,
        publication: &PublicationIdentity,
        challenge: Uuid,
        require_target: bool,
    ) -> Result<MultipartSourceResponse> {
        if challenge.is_nil() {
            return Err(DiskError::FileCorrupt);
        }
        let loaded = store.load().await?;
        let Some(mut state) = loaded.state.clone() else {
            if !require_target || expected.is_some() || loaded.head.is_some() {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            let target = self.get_object_path(&publication.lineage.bucket, &publication.lineage.object)?;
            let _target_owner = os::acquire_metadata_mutation_lease(&target, store.owner.clone()).await;
            // The private target-DD anchor proves this actual participant's
            // source validation and drained Init. Common target FI cannot.
            return Ok(MultipartSourceResponse::TerminalConfirmed(
                store.confirm_root_missing(publication, challenge).await?,
            ));
        };
        if let Some(expected) = expected {
            require_expected(&loaded, expected)?;
        }
        if loaded.head.is_some()
            || state.publication.as_ref() != Some(publication)
            || state.generation != publication.source_generation
            || state.undo.is_some()
            || (!require_target && state.phase == MultipartSourcePhase::Prepared)
            || !matches!(
                state.phase,
                MultipartSourcePhase::Prepared
                    | MultipartSourcePhase::ConsumedUnsealed
                    | MultipartSourcePhase::LocalTerminalSeal
                    | MultipartSourcePhase::GlobalTerminalCertified
            )
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        let target = self.get_object_path(&publication.lineage.bucket, &publication.lineage.object)?;
        let _target_owner = os::acquire_metadata_mutation_lease(&target, store.owner.clone()).await;
        if require_target {
            let raw = store
                .read_target(publication)
                .await?
                .ok_or(DiskError::ConditionalDeleteConflict)?;
            if !store.target_matches(publication, &raw)? {
                return Err(DiskError::ConditionalDeleteConflict);
            }
            store.sync_target(publication).await?;
        }
        if !matches!(
            state.phase,
            MultipartSourcePhase::LocalTerminalSeal | MultipartSourcePhase::GlobalTerminalCertified
        ) {
            state.capability_sha256 = None;
            state.advance(MultipartSourcePhase::LocalTerminalSeal)?;
            store.persist(&state).await?;
        }
        let current = store.load().await?;
        let snapshot = current.snapshot.ok_or(DiskError::ConditionalDeleteConflict)?;
        if snapshot.head_sha256.is_some()
            || snapshot.capability_sha256.is_some()
            || snapshot.publication.as_ref() != Some(publication)
            || !matches!(
                snapshot.phase,
                MultipartSourcePhase::LocalTerminalSeal | MultipartSourcePhase::GlobalTerminalCertified
            )
        {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(MultipartSourceResponse::TerminalConfirmed(store.issue_proof(
            if require_target {
                MultipartProofKind::TerminalTarget
            } else {
                MultipartProofKind::TerminalConsumed
            },
            challenge,
            None,
            snapshot,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        disk::{
            DiskAPI, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET,
            multipart_source::{MULTIPART_SOURCE_PROTOCOL_VERSION, ProofVerificationContext, SourceGeneration},
        },
        layout::{
            endpoint::Endpoint,
            endpoints::{EndpointServerPools, PoolEndpoints},
            format::FormatV3,
        },
        set_disk::{RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY},
    };
    use rustfs_filemeta::{FileInfo, FileInfoOpts, FileMeta, ObjectPartInfo, get_file_info};
    use sha2::{Digest, Sha256};

    // Construct the concrete native dispatcher future off each caller's poll
    // frame; boxing an already-built future would retain its stack temporary.
    #[inline(never)]
    fn heap_native<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
        Box::pin(build())
    }

    impl LocalDisk {
        fn recover_native_on_heap(
            &self,
            request: MultipartSourceRequest,
        ) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<MultipartSourceResponse>> + '_>> {
            heap_native(|| self.recover_multipart_source_native(request))
        }
    }

    mod abort_tests {
        include!("multipart_source_abort_tests.rs");
    }

    #[cfg(unix)]
    mod abort_race_tests {
        include!("multipart_source_abort_race_tests.rs");
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct UntrustedAnchorBlob {
        protocol_version: u16,
        disk_id: Uuid,
        physical_slot: usize,
        publication: PublicationIdentity,
        source_original_sha256: [u8; 32],
        source_validation_receipt_id: Uuid,
    }

    struct Fixture {
        disks: Vec<std::sync::Arc<LocalDisk>>,
        path: String,
        disk_ids: Vec<Uuid>,
        _root: tempfile::TempDir,
    }

    impl Fixture {
        async fn new() -> Self {
            Self::new_with_missing_source(None).await
        }

        async fn new_with_missing_source(missing: Option<usize>) -> Self {
            let root = tempfile::tempdir().expect("native recovery disk root");
            let format = FormatV3::new(1, 4);
            let original_data_dir = Uuid::new_v4();
            let created = time::OffsetDateTime::now_utc();
            let object_hash = hex_simd::encode_to_string(Sha256::digest(b"bucket/object"), hex_simd::AsciiCase::Lower);
            let path = format!("{object_hash}/{}x{}", Uuid::new_v4(), created.unix_timestamp_nanos());
            let mut endpoints = Vec::with_capacity(4);
            for slot in 0..4 {
                let disk_root = root.path().join(slot.to_string());
                std::fs::create_dir_all(disk_root.join(RUSTFS_META_BUCKET)).expect("native format directory");
                let mut local_format = format.clone();
                local_format.erasure.this = format.erasure.sets[0][slot];
                std::fs::write(
                    disk_root.join(RUSTFS_META_BUCKET).join(FORMAT_CONFIG_FILE),
                    local_format.to_json().expect("native format encoding"),
                )
                .expect("native format publication");
                let mut endpoint =
                    Endpoint::try_from(disk_root.to_str().expect("UTF-8 native disk root")).expect("native endpoint");
                endpoint.set_pool_index(0);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(slot);
                endpoints.push(endpoint);
                if missing == Some(slot) {
                    continue;
                }
                let mut source = FileInfo::new("object", 2, 2);
                source.erasure.distribution = vec![1, 2, 3, 4];
                source.erasure.index = slot + 1;
                source.data_dir = Some(original_data_dir);
                source.mod_time = Some(created);
                source
                    .metadata
                    .insert(RUSTFS_MULTIPART_BUCKET_KEY.to_owned(), "bucket".to_owned());
                source
                    .metadata
                    .insert(RUSTFS_MULTIPART_OBJECT_KEY.to_owned(), "object".to_owned());
                crate::disk::multipart_source::mark_native_source(&mut source);
                let mut meta = FileMeta::new();
                meta.add_version(source).expect("native original source version");
                let upload_root = disk_root.join(RUSTFS_META_MULTIPART_BUCKET).join(&path);
                std::fs::create_dir_all(upload_root.join(original_data_dir.to_string())).expect("native source data root");
                std::fs::write(upload_root.join("xl.meta"), meta.marshal_msg().expect("native source encoding"))
                    .expect("native source publication");
                std::fs::write(upload_root.join(original_data_dir.to_string()).join("part.1"), b"original shard")
                    .expect("native preserved shard");
            }
            let topology = EndpointServerPools::from(vec![PoolEndpoints {
                legacy: true,
                set_count: 1,
                drives_per_set: 4,
                endpoints: endpoints.clone().into(),
                cmd_line: String::new(),
                platform: String::new(),
            }]);
            let (_, authority) = ProofVerificationContext::for_topology(&topology).expect("native instance proof scope");
            let mut disks = Vec::with_capacity(4);
            for endpoint in endpoints {
                let disk = LocalDisk::new(&endpoint, false).await.expect("formatted native disk");
                disk.bind_multipart_proof_authority(&authority)
                    .expect("bind native disk proof scope");
                disks.push(std::sync::Arc::new(disk));
            }
            Self {
                _root: root,
                disks,
                path,
                disk_ids: format.erasure.sets[0].clone(),
            }
        }

        async fn reopen(&mut self) {
            let endpoints = self.disks.iter().map(|disk| disk.endpoint.clone()).collect::<Vec<_>>();
            self.disks.clear();
            let topology = EndpointServerPools::from(vec![PoolEndpoints {
                legacy: true,
                set_count: 1,
                drives_per_set: 4,
                endpoints: endpoints.clone().into(),
                cmd_line: String::new(),
                platform: String::new(),
            }]);
            let (_, authority) = ProofVerificationContext::for_topology(&topology).expect("fresh native instance authority");
            for endpoint in endpoints {
                let disk = LocalDisk::new(&endpoint, false).await.expect("reopen formatted native disk");
                disk.bind_multipart_proof_authority(&authority)
                    .expect("bind fresh native disk authority");
                self.disks.push(std::sync::Arc::new(disk));
            }
        }

        fn request(&self, operation: MultipartSourceOperation) -> MultipartSourceRequest {
            MultipartSourceRequest {
                protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
                path: self.path.clone(),
                operation,
            }
        }

        async fn inspect(&self, slot: usize) -> MultipartSourceInspection {
            match self.disks[slot]
                .recover_native_on_heap(self.request(MultipartSourceOperation::Inspect { lineage: None }))
                .await
                .expect("inspect actual native source")
            {
                MultipartSourceResponse::Inspected(inspection) => inspection,
                _ => panic!("Inspect must return native inspection"),
            }
        }

        async fn snapshot(&self, slot: usize) -> MultipartSourceSnapshot {
            self.inspect(slot).await.snapshot.expect("present native source state")
        }

        async fn start_round(&self) -> MultipartRecoveryRound {
            let snapshot = self.snapshot(0).await;
            MultipartRecoveryRound {
                lineage: snapshot.lineage,
                from_generation: snapshot.generation,
                old_attempt: Uuid::new_v4(),
                generation: SourceGeneration {
                    epoch: snapshot.generation.epoch + 1,
                    nonce: Uuid::new_v4(),
                },
                round: Uuid::new_v4(),
            }
        }

        async fn negative(&self, round: &MultipartRecoveryRound) -> MultipartCertificate {
            self.negative_on(round, &[0, 1]).await
        }

        async fn negative_on(&self, round: &MultipartRecoveryRound, slots: &[usize]) -> MultipartCertificate {
            let mut proofs = Vec::with_capacity(slots.len());
            for &slot in slots {
                let expected = self.snapshot(slot).await;
                match self.disks[slot]
                    .recover_native_on_heap(self.request(MultipartSourceOperation::Claim {
                        expected,
                        round: round.clone(),
                    }))
                    .await
                    .expect("durable native present-head claim")
                {
                    MultipartSourceResponse::Claimed(proof) => proofs.push(proof),
                    _ => panic!("Claim must return native claim proof"),
                }
            }
            MultipartCertificate {
                round: round.clone(),
                disk_ids: self.disk_ids.clone(),
                proofs,
            }
        }

        fn zero_info(&self, slot: usize, final_data_dir: Uuid, modified: time::OffsetDateTime) -> FileInfo {
            let raw = std::fs::read(self.head(slot)).expect("actual source head before native publication");
            let source = get_file_info(
                &raw,
                RUSTFS_META_MULTIPART_BUCKET,
                &self.path,
                "",
                FileInfoOpts {
                    data: false,
                    include_free_versions: false,
                    include_part_checksums: true,
                },
            )
            .expect("actual active source FI");
            let condition =
                crate::disk::multipart_source::capture_source_condition("bucket", "object", &self.path, &source, final_data_dir)
                    .expect("capture original native generation before mutation");
            let source_data = self
                .head(slot)
                .parent()
                .expect("canonical source root")
                .join(source.data_dir.expect("original source DD").to_string());
            let mut completed = source;
            completed.size = 0;
            completed.data = None;
            completed.data_dir = Some(final_data_dir);
            completed.mod_time = Some(modified);
            completed.parts.clear();
            completed.add_object_part(1, "zero-part".to_owned(), 0, Some(modified), 0, None, None);
            let staged_data = source_data.join(format!(".test-part-data-{}", Uuid::new_v4()));
            let staged_meta = source_data.join(format!(".test-part-meta-{}", Uuid::new_v4()));
            std::fs::write(&staged_data, b"").expect("real zero-byte selected shard");
            std::fs::write(&staged_meta, completed.parts[0].marshal_msg().expect("real selected zero part metadata"))
                .expect("stage selected zero-byte part generation");
            // Real part publication replaces inodes. In-place fixture
            // writes would corrupt an older final DD's retained hardlink.
            std::fs::rename(staged_data, source_data.join("part.1")).expect("publish selected zero shard generation");
            std::fs::rename(staged_meta, source_data.join("part.1.meta"))
                .expect("publish selected zero part metadata generation");
            crate::disk::multipart_source::strip_native_controls(&mut completed.metadata);
            completed.metadata.remove(RUSTFS_MULTIPART_BUCKET_KEY);
            completed.metadata.remove(RUSTFS_MULTIPART_OBJECT_KEY);
            condition
                .attach(&mut completed)
                .expect("native captured condition and common publication tuple");
            completed
        }

        async fn publish_zero_on(
            &self,
            slots: &[usize],
        ) -> Vec<(usize, crate::disk::multipart_source::MultipartPublicationResult)> {
            let final_data_dir = Uuid::new_v4();
            let modified = time::OffsetDateTime::now_utc();
            let mut results = Vec::with_capacity(slots.len());
            for &slot in slots {
                let completed = self.zero_info(slot, final_data_dir, modified);
                for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
                    match self.disks[slot].make_volume(volume).await {
                        Ok(()) | Err(DiskError::VolumeExists) => {}
                        Err(error) => panic!("prepare native publication volume {volume}: {error}"),
                    }
                }
                let result = self.disks[slot]
                    .rename_data(RUSTFS_META_MULTIPART_BUCKET, &self.path, completed, "bucket", "object")
                    .await
                    .expect("real native source-head publication");
                let publication = *result
                    .multipart_source
                    .expect("original publication response alone issues owned cap");
                publication
                    .verify_with_authority(&self.disks[slot].multipart_proof_verification_context())
                    .expect("actual producer proof");
                assert!(
                    !self.head(slot).exists(),
                    "actual atomic publication must consume the canonical source head"
                );
                let target_data = self.disks[slot]
                    .root
                    .join("bucket/object")
                    .join(publication.publication.final_data_dir.to_string());
                assert_eq!(
                    std::fs::read(target_data.join("part.1")).expect("zero-byte selected shard is published"),
                    b""
                );
                let part = ObjectPartInfo::unmarshal(
                    &std::fs::read(target_data.join("part.1.meta")).expect("zero-byte native part sidecar is published"),
                )
                .expect("decode published zero-byte part generation");
                assert_eq!((part.number, part.size, part.mod_time), (1, 0, Some(modified)));
                results.push((slot, publication));
            }
            results
        }

        fn round_for(publication: &PublicationIdentity) -> MultipartRecoveryRound {
            MultipartRecoveryRound {
                lineage: publication.lineage.clone(),
                from_generation: publication.source_generation,
                old_attempt: publication.attempt,
                generation: SourceGeneration {
                    epoch: publication
                        .source_generation
                        .epoch
                        .checked_add(1)
                        .expect("test recovery epoch"),
                    nonce: Uuid::new_v4(),
                },
                round: Uuid::new_v4(),
            }
        }

        async fn restore_all(&self, negative: &MultipartCertificate) -> MultipartCertificate {
            let mut proofs = Vec::with_capacity(4);
            for slot in 0..4 {
                let expected = self.snapshot(slot).await;
                match self.disks[slot]
                    .recover_native_on_heap(self.request(MultipartSourceOperation::Restore {
                        expected,
                        certificate: negative.clone(),
                    }))
                    .await
                    .expect("all-participant native head restoration")
                {
                    MultipartSourceResponse::Restored(proof) => proofs.push(proof),
                    _ => panic!("Restore must return native restored-head proof"),
                }
            }
            MultipartCertificate {
                round: negative.round.clone(),
                disk_ids: self.disk_ids.clone(),
                proofs,
            }
        }

        fn head(&self, slot: usize) -> std::path::PathBuf {
            self.disks[slot]
                .root
                .join(RUSTFS_META_MULTIPART_BUCKET)
                .join(&self.path)
                .join("xl.meta")
        }
    }

    #[tokio::test]
    async fn native_multipart_recovery_requires_all_n_and_keeps_nonactivated_witness() {
        let fixture = Fixture::new().await;
        let old = fixture.snapshot(0).await;
        let round = fixture.start_round().await;
        let negative = fixture.negative(&round).await;
        let ready = fixture.restore_all(&negative).await;
        let mut incomplete = ready.clone();
        incomplete.proofs.pop();
        let expected = fixture.snapshot(0).await;
        let before = std::fs::read(fixture.head(0)).expect("blocked source head before invalid activation");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: expected.clone(),
                    certificate: incomplete,
                }))
                .await
                .is_err(),
            "Q restored heads cannot substitute for all N"
        );
        assert_eq!(fixture.snapshot(0).await, expected);
        assert_eq!(std::fs::read(fixture.head(0)).expect("blocked source after rejected activation"), before);
        for slot in 0..3 {
            let expected = fixture.snapshot(slot).await;
            let response = fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected,
                    certificate: ready.clone(),
                }))
                .await
                .expect("activate from actual all-N ready certificate");
            assert!(matches!(response, MultipartSourceResponse::Activated(_)));
        }
        let remaining = fixture.snapshot(3).await;
        assert_eq!(remaining.phase, MultipartSourcePhase::Recovering);
        assert_eq!(remaining.generation, round.generation);
        assert!(
            remaining.head_sha256.is_some(),
            "nonactivated participant remains a new-generation witness"
        );
        for slot in 0..4 {
            let raw = std::fs::read(fixture.head(slot)).expect("restored head");
            let fi = get_file_info(
                &raw,
                RUSTFS_META_MULTIPART_BUCKET,
                &fixture.path,
                "",
                FileInfoOpts {
                    data: false,
                    include_free_versions: false,
                    include_part_checksums: true,
                },
            )
            .expect("restored FI decoding");
            assert_eq!(
                crate::disk::multipart_source::is_active_source(&fi).expect("source eligibility"),
                slot < 3
            );
        }
        let before = std::fs::read(fixture.head(0)).expect("active source before stale restore");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected: old,
                    certificate: negative,
                }))
                .await
                .is_err(),
            "old full snapshot cannot restore after activation advanced revision"
        );
        assert_eq!(std::fs::read(fixture.head(0)).expect("active source after stale restore"), before);
    }

    #[tokio::test]
    async fn native_multipart_restore_rejects_foreign_cohort_and_duplicate_votes_before_writes() {
        let fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let negative = fixture.negative(&round).await;
        let expected = fixture.snapshot(2).await;
        let before = std::fs::read(fixture.head(2)).expect("unmodified original source");
        let source_root = fixture.head(2).parent().expect("canonical source root").to_owned();
        let mut foreign = negative.clone();
        // The two signed voters remain authentic. A fabricated nonvoter ID
        // still cannot replace the native format's fixed fourth member.
        foreign.disk_ids[3] = Uuid::new_v4();
        let mut duplicate = negative.clone();
        duplicate.proofs[1] = duplicate.proofs[0].clone();
        let mut changed_slot = negative.clone();
        changed_slot.proofs[0].snapshot.physical_slot = 1;
        let mut changed_kind = negative.clone();
        changed_kind.proofs[0].kind = MultipartProofKind::TerminalConsumed;
        let mut changed_round = negative.clone();
        changed_round.round.round = Uuid::new_v4();
        let mut changed_generation = negative.clone();
        changed_generation.round.generation.epoch += 1;
        for (label, certificate) in [
            ("foreign nonvoter", foreign),
            ("duplicate voter", duplicate),
            ("copied voter slot", changed_slot),
            ("changed native proof kind", changed_kind),
            ("foreign recovery round", changed_round),
            ("foreign recovery generation", changed_generation),
        ] {
            assert!(
                fixture.disks[2]
                    .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                        expected: expected.clone(),
                        certificate,
                    }))
                    .await
                    .is_err(),
                "{label}: authentic physical membership and proof body precede recovery writes"
            );
            assert_eq!(fixture.snapshot(2).await, expected);
            assert_eq!(std::fs::read(fixture.head(2)).expect("source after rejected evidence"), before);
            assert!(
                !source_root.join(".mpu-source-state").exists(),
                "invalid evidence cannot initialize native state"
            );
            assert!(!source_root.join(".mpu-completions").exists(), "invalid evidence cannot create a backup");
            assert!(
                !source_root.join(".mpu-recovery").exists(),
                "invalid evidence cannot persist certificates"
            );
        }
    }

    #[tokio::test]
    async fn native_multipart_unknown_proof_kind_is_rejected_without_native_mutation() {
        let fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let certificate = fixture.negative(&round).await;
        let before = fixture.snapshot(2).await;
        let raw = std::fs::read(fixture.head(2)).expect("original head before unknown wire proof");
        let mut wire = rmp_serde::to_vec_named(&certificate).expect("actual MessagePack proof envelope");
        assert!(
            rmp_serde::from_slice::<MultipartCertificate>(&wire).is_ok(),
            "baseline packet must be valid before the one-field mutation"
        );
        let mut field = rmp_serde::to_vec_named("kind").expect("named proof field");
        field.extend_from_slice(&rmp_serde::to_vec_named(&MultipartProofKind::Claimed).expect("native claimed kind"));
        let offset = wire
            .windows(field.len())
            .position(|bytes| bytes == field)
            .expect("actual claimed proof field");
        let start = offset + field.len() - b"claimed".len();
        wire[start..start + b"claimed".len()].copy_from_slice(b"unknown");
        assert!(
            rmp_serde::from_slice::<MultipartCertificate>(&wire).is_err(),
            "the unknown enum variant alone fails strict decoding"
        );
        assert_eq!(fixture.snapshot(2).await, before);
        assert_eq!(std::fs::read(fixture.head(2)).expect("source after rejected wire proof"), raw);
    }

    #[tokio::test]
    async fn native_multipart_old_certificates_cannot_reopen_a_successor_generation() {
        let fixture = Fixture::new().await;
        let first = fixture.start_round().await;
        let negative = fixture.negative(&first).await;
        let ready = fixture.restore_all(&negative).await;
        for slot in 0..4 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("initial all-N recovery activation");
        }
        let second = fixture.start_round().await;
        let second_negative = fixture.negative(&second).await;
        assert_eq!(second_negative.round.from_generation, first.generation);
        assert_eq!(second_negative.round.generation.epoch, first.generation.epoch + 1);
        let expected = fixture.snapshot(0).await;
        let before = std::fs::read(fixture.head(0)).expect("second-generation claimed head");
        let stale = [
            MultipartSourceOperation::Restore {
                expected: expected.clone(),
                certificate: negative,
            },
            MultipartSourceOperation::Activate {
                expected: expected.clone(),
                certificate: ready,
            },
            MultipartSourceOperation::Claim {
                expected: expected.clone(),
                round: first,
            },
        ];
        for operation in stale {
            assert!(
                fixture.disks[0]
                    .recover_native_on_heap(fixture.request(operation))
                    .await
                    .is_err(),
                "current full snapshot cannot authorize an old generation's operation"
            );
            assert_eq!(fixture.snapshot(0).await, expected);
            assert_eq!(std::fs::read(fixture.head(0)).expect("successor after stale operation"), before);
        }
    }

    #[tokio::test]
    async fn native_multipart_cold_certificate_requires_its_unchanged_fixed_state_anchor() {
        let mut fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let negative = fixture.negative(&round).await;
        let bare_wire = rmp_serde::to_vec_named(&negative).expect("serialized proof has no local runtime authority");
        fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                expected: fixture.snapshot(0).await,
                certificate: negative,
            }))
            .await
            .expect("accept authentic H certificate before cold reopen");
        fixture.reopen().await;
        let bare: MultipartCertificate = rmp_serde::from_slice(&bare_wire).expect("decode local marker without native origin");
        assert!(
            bare.verify_with_authority(&fixture.disks[0].multipart_proof_verification_context(), MultipartProofKind::Claimed, 2)
                .is_err(),
            "wire deserialization alone never restores local proof authority"
        );
        let inspection = fixture.inspect(0).await;
        let hydrated = inspection
            .negative_certificate
            .expect("strict pinned native record restores historical provenance");
        hydrated
            .verify_with_authority(&fixture.disks[0].multipart_proof_verification_context(), MultipartProofKind::Claimed, 2)
            .expect("fresh instance verifies its anchored native certificate");
        let source = fixture.head(0).parent().expect("canonical source root").to_owned();
        let record = source
            .join(".mpu-recovery")
            .join(round.round.to_string())
            .join("negative.msgpack");
        let original_record = std::fs::read(&record).expect("owned certificate record");
        let original_head = std::fs::read(fixture.head(0)).expect("restored source before certificate corruption");
        let original_state = std::fs::read(source.join(".mpu-source-state")).expect("fixed state anchor");
        let mut wrong_round = hydrated;
        wrong_round.round.round = Uuid::new_v4();
        let wrong_round = rmp_serde::to_vec_named(&wrong_round).expect("different recovery round record");
        let mut changed = original_record.clone();
        changed[0] ^= 1;
        for bytes in [changed, wrong_round] {
            std::fs::write(&record, bytes).expect("inject changed owned certificate bytes");
            assert!(
                fixture.disks[0]
                    .recover_native_on_heap(fixture.request(MultipartSourceOperation::Inspect { lineage: None }))
                    .await
                    .is_err(),
                "changed bytes or wrong round cannot hydrate authority"
            );
            assert_eq!(std::fs::read(fixture.head(0)).expect("head after corrupt certificate"), original_head);
            assert_eq!(
                std::fs::read(source.join(".mpu-source-state")).expect("anchor after corrupt certificate"),
                original_state
            );
        }
        std::fs::remove_file(&record).expect("remove owned certificate record");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Inspect { lineage: None }))
                .await
                .is_err(),
            "missing owned record cannot hydrate authority"
        );
        std::fs::write(&record, original_record).expect("restore owned record for anchor-loss injection");
        let expected = fixture.snapshot(0).await;
        let mut missing_anchor: SourceState =
            rmp_serde::from_slice(&original_state).expect("decode fixed state corruption fixture");
        missing_anchor.negative_certificate_sha256 = None;
        let missing_anchor = rmp_serde::to_vec_named(&missing_anchor).expect("encode missing fixed-state certificate reference");
        std::fs::write(source.join(".mpu-source-state"), &missing_anchor).expect("inject missing owned-state anchor");
        {
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("native anchor-loss fixture owner");
            assert!(
                store.read_certificate(round.round, false).await.is_err(),
                "an unreferenced file cannot hydrate local provenance"
            );
        }
        if let Ok(MultipartSourceResponse::Inspected(inspection)) = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Inspect { lineage: None }))
            .await
        {
            assert!(
                inspection.negative_certificate.is_none(),
                "Inspect never scans an unreferenced certificate into authority"
            );
        }
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected,
                    certificate: bare,
                }))
                .await
                .is_err(),
            "bare caller bytes cannot replace a missing native authority anchor"
        );
        assert_eq!(
            std::fs::read(source.join(".mpu-source-state")).expect("state after rejected bare certificate"),
            missing_anchor
        );
        assert_eq!(
            std::fs::read(fixture.head(0)).expect("source after rejected bare certificate"),
            original_head
        );
    }

    #[tokio::test]
    async fn native_multipart_claim_resumes_durable_fence_before_head_replacement() {
        let fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let expected = fixture.snapshot(0).await;
        let original = std::fs::read(fixture.head(0)).expect("original active source");
        {
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("native source owner");
            let loaded = store.load().await.expect("original native source load");
            let mut state = store.ensure_state(&loaded).await.expect("independent original backup");
            state.negative_claim = Some(NativeNegativeClaim {
                from: expected.clone(),
                round: round.clone(),
                owned_undo: false,
            });
            state.generation = round.generation;
            state.recovery = Some(round.clone());
            state
                .advance(MultipartSourcePhase::Recovering)
                .expect("durable generation fence revision");
            store.persist(&state).await.expect("claim fence without head replacement");
            assert_eq!(store.original(&state).await.expect("independent source backup").as_ref(), original);
        }
        assert_eq!(std::fs::read(fixture.head(0)).expect("old head after injected claim crash"), original);
        let resume = fixture.snapshot(0).await;
        let result = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Claim {
                expected: resume,
                round: round.clone(),
            }))
            .await
            .expect("resume same durable claim intent");
        let MultipartSourceResponse::Claimed(proof) = result else {
            panic!("resumed claim result");
        };
        assert_eq!(proof.snapshot.generation, round.generation);
        assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Recovering);
        assert!(proof.snapshot.head_sha256.is_some());
        let before = std::fs::read(fixture.head(0)).expect("new-generation blocked head");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Claim { expected, round }))
                .await
                .is_err(),
            "old pre-fence snapshot is permanently stale"
        );
        assert_eq!(std::fs::read(fixture.head(0)).expect("head after stale claim"), before);
    }

    #[tokio::test]
    async fn native_multipart_activation_resumes_state_intent_without_recreating_removed_upload() {
        let fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let negative = fixture.negative(&round).await;
        let ready = fixture.restore_all(&negative).await;
        {
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("activation source owner");
            let loaded = store.load().await.expect("restored native source load");
            let mut state = loaded.state.expect("restored fixed state");
            let digest = store
                .persist_certificate(round.round, true, &ready)
                .await
                .expect("native ready evidence");
            state.ready_certificate_sha256 = Some(digest);
            state
                .advance(MultipartSourcePhase::Active)
                .expect("activation intent revision");
            store
                .persist(&state)
                .await
                .expect("durable activation intent before head replace");
        }
        let expected = fixture.snapshot(0).await;
        fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                expected,
                certificate: ready.clone(),
            }))
            .await
            .expect("resume exact all-N activation intent");
        assert_eq!(fixture.snapshot(0).await.phase, MultipartSourcePhase::Active);
        let expected = fixture.snapshot(0).await;
        fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                expected,
                certificate: ready.clone(),
            }))
            .await
            .expect("same unchanged active head is idempotent");
        // Simulate authoritative source-root removal. Neither old activation
        // nor restoration owns authority to create this upload again.
        let expected = fixture.snapshot(0).await;
        let source_root = fixture.head(0).parent().expect("canonical source root").to_owned();
        std::fs::remove_dir_all(&source_root).expect("remove canonical upload root");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected,
                    certificate: ready,
                }))
                .await
                .is_err()
        );
        assert!(!source_root.exists(), "activation must never mkdir an absent upload root");
    }

    #[tokio::test]
    async fn native_multipart_partial_local_seals_accept_h_restore_without_target_undo_or_cap_revival() {
        let fixture = Fixture::new().await;
        let mut publications = fixture.publish_zero_on(&[0, 1]).await;
        let publication = publications[0].1.publication.clone();
        let round = Fixture::round_for(&publication);
        let mut targets = Vec::new();
        let mut seals = Vec::new();
        for (slot, result) in &publications {
            let response = fixture.disks[*slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: Some(result.proof.snapshot.clone()),
                    publication: publication.clone(),
                    challenge: Uuid::new_v4(),
                }))
                .await
                .expect("seal an actual native publication");
            let MultipartSourceResponse::TerminalConfirmed(proof) = response else {
                panic!("native terminal confirmation");
            };
            assert_eq!(proof.snapshot.phase, MultipartSourcePhase::LocalTerminalSeal);
            assert!(proof.snapshot.capability_sha256.is_none());
            targets.push((
                *slot,
                std::fs::read(fixture.disks[*slot].root.join("bucket/object/xl.meta")).expect("published native target"),
            ));
            let store = fixture.disks[*slot]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("read native seal owner");
            let state = store
                .load()
                .await
                .expect("sealed native state")
                .state
                .expect("owned source state");
            let record = store
                .source
                .join(".mpu-completions")
                .join(state.backup_owner.to_string())
                .join(format!("terminal-{}.msgpack", state.revision));
            seals.push((record.clone(), std::fs::read(record).expect("immutable native local seal receipt")));
        }
        let negative = fixture.negative_on(&round, &[2, 3]).await;
        let ready = fixture.restore_all(&negative).await;
        for (slot, raw) in targets {
            let snapshot = fixture.snapshot(slot).await;
            assert_eq!(snapshot.generation, round.generation);
            assert_eq!(snapshot.phase, MultipartSourcePhase::Recovering);
            assert!(snapshot.capability_sha256.is_none());
            assert_eq!(
                std::fs::read(fixture.disks[slot].root.join("bucket/object/xl.meta")).expect("target after H restoration"),
                raw,
                "partial-seal restoration performs no target Undo"
            );
        }
        for (path, raw) in seals {
            assert_eq!(std::fs::read(path).expect("old local seal survives recovery"), raw);
        }
        let (slot, old) = publications.remove(0);
        let expected = fixture.snapshot(slot).await;
        let source = std::fs::read(fixture.head(slot)).expect("blocked restored source before stale cap");
        assert!(
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                    expected: Box::new(old.proof.snapshot),
                    publication: old.publication,
                    capability: old.undo_capability,
                    round: round.clone(),
                }))
                .await
                .is_err(),
            "old sealed capability cannot roll back or recreate source authority"
        );
        assert_eq!(fixture.snapshot(slot).await, expected);
        assert_eq!(std::fs::read(fixture.head(slot)).expect("source after rejected stale cap"), source);
        for slot in 0..3 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("partial-seal recovery activates only after real all-N restoration");
        }
    }

    #[tokio::test]
    async fn native_multipart_owned_undo_preserves_snapshot_then_reclaims_only_its_attempt() {
        let fixture = Fixture::new().await;
        for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
            match fixture.disks[0].make_volume(volume).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(error) => panic!("prepare predecessor volume: {error}"),
            }
        }
        let predecessor =
            super::super::test::test_file_info("object", Uuid::nil(), None, Some(bytes::Bytes::from_static(b"predecessor")));
        fixture.disks[0]
            .rename_data(RUSTFS_META_TMP_BUCKET, "real-predecessor", predecessor, "bucket", "object")
            .await
            .expect("actual predecessor PUT before failed multipart publication");
        let predecessor = std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("native predecessor FI");
        let result = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("one actual failed-operation publication")
            .1;
        let publication = result.publication.clone();
        let round = Fixture::round_for(&publication);
        let data_path = format!("object/{}", publication.final_data_dir);
        let data = fixture.disks[0].root.join("bucket").join(&data_path);
        let anchor = data.join(".mpu-source-retirement");
        let anchor_bytes = std::fs::read(&anchor).expect("actual target anchor before Undo");
        let token = fixture.disks[0]
            .acquire_snapshot_lease("bucket", &data_path)
            .await
            .expect("actual snapshot protects failed attempt DD");
        let capability_wire = rmp_serde::to_vec_named(&result.undo_capability).expect("same owner's opaque retry token");
        let expected = result.proof.snapshot.clone();
        let response = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                expected: Box::new(expected.clone()),
                publication: publication.clone(),
                capability: result.undo_capability,
                round: round.clone(),
            }))
            .await
            .expect("owned conditional target Undo and new-generation source claim");
        let MultipartSourceResponse::Claimed(owned_claim) = response else {
            panic!("owned Undo returns durable negative proof");
        };
        assert_eq!(owned_claim.snapshot.generation, round.generation);
        assert!(owned_claim.snapshot.head_sha256.is_some());
        assert_eq!(
            std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("predecessor after exact target Undo"),
            predecessor
        );
        assert_eq!(std::fs::read(&anchor).expect("snapshot retains attempt anchor"), anchor_bytes);
        fixture.disks[0]
            .release_snapshot_lease("bucket", &data_path, token)
            .await
            .expect("real snapshot release completes pending reclaim");
        assert!(!data.exists(), "snapshot release must reclaim the exact failed DD and anchor");
        let duplicate = rmp_serde::from_slice(&capability_wire).expect("same owned opaque request retry");
        let response = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                expected: Box::new(expected.clone()),
                publication: publication.clone(),
                capability: duplicate,
                round: round.clone(),
            }))
            .await
            .expect("unchanged owned claim result is idempotent");
        let MultipartSourceResponse::Claimed(duplicate_claim) = response else {
            panic!("idempotent owned claim proof");
        };
        assert_eq!(duplicate_claim.snapshot, owned_claim.snapshot);
        let mut negative = fixture.negative_on(&round, &[1]).await;
        negative.proofs.push(owned_claim);
        let ready = fixture.restore_all(&negative).await;
        for slot in 0..4 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("activate from actual mixed owned/present H proof");
        }
        let successor = fixture.publish_zero_on(&[0]).await.pop().expect("later actual publication").1;
        let current = fixture.snapshot(0).await;
        let target = std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("later target before stale Undo");
        let duplicate = rmp_serde::from_slice(&capability_wire).expect("old opaque capability replay");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                    expected: Box::new(expected),
                    publication,
                    capability: duplicate,
                    round,
                }))
                .await
                .is_err(),
            "old capability cannot roll back a later attempt or recreate its consumed source"
        );
        assert_eq!(fixture.snapshot(0).await, current);
        assert!(!fixture.head(0).exists());
        assert_eq!(
            std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("successor after stale Undo"),
            target
        );
        drop(successor);
    }

    #[tokio::test]
    async fn native_multipart_three_terminal_seals_exclude_h_and_global_certificate_rejects_old_restore() {
        let fixture = Fixture::new().await;
        let first = fixture.start_round().await;
        let old_negative = fixture.negative(&first).await;
        let ready = fixture.restore_all(&old_negative).await;
        for slot in 0..4 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("activate first native recovery generation");
        }
        let publications = fixture.publish_zero_on(&[0, 1, 2]).await;
        let publication = publications[0].1.publication.clone();
        let round = Fixture::round_for(&publication);
        let challenge = Uuid::new_v4();
        let mut terminal = Vec::new();
        for (slot, result) in publications {
            let response = fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: Some(result.proof.snapshot),
                    publication: publication.clone(),
                    challenge,
                }))
                .await
                .expect("durable actual target-matching terminal ACK");
            let MultipartSourceResponse::TerminalConfirmed(proof) = response else {
                panic!("terminal ACK result");
            };
            proof
                .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
                .expect("native terminal provenance");
            terminal.push(proof);
            let snapshot = fixture.snapshot(slot).await;
            assert!(
                fixture.disks[slot]
                    .recover_native_on_heap(fixture.request(MultipartSourceOperation::Claim {
                        expected: snapshot.clone(),
                        round: round.clone(),
                    }))
                    .await
                    .is_err(),
                "a terminal seal cannot issue a negative vote"
            );
            assert_eq!(fixture.snapshot(slot).await, snapshot);
            assert!(!fixture.head(slot).exists());
        }
        assert_eq!(terminal.len(), publication.lineage.geometry.write_quorum());
        let only_present = fixture.negative_on(&round, &[3]).await;
        assert!(
            only_present
                .verify_with_authority(&fixture.disks[0].multipart_proof_verification_context(), MultipartProofKind::Claimed, 2)
                .is_err(),
            "one remaining present source cannot form H after Q terminal seals"
        );
        let expected = fixture.snapshot(0).await;
        let target =
            std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("terminal target before insufficient H");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected,
                    certificate: only_present,
                }))
                .await
                .is_err(),
            "a caller count cannot replace native H"
        );
        {
            // The optional durable global class stores authentic Q ACKs;
            // this fixture never fabricates a negative proof or capability.
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("global terminal receipt owner");
            let mut state = store
                .load()
                .await
                .expect("local terminal state")
                .state
                .expect("owned terminal state");
            state.terminal_certificate = terminal;
            state
                .advance(MultipartSourcePhase::GlobalTerminalCertified)
                .expect("global receipt revision");
            store
                .persist(&state)
                .await
                .expect("persist authentic Q terminal certificate fixture");
        }
        let expected = fixture.snapshot(0).await;
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected: expected.clone(),
                    certificate: old_negative,
                }))
                .await
                .is_err(),
            "valid historical H cannot override a later global terminal decision"
        );
        assert_eq!(fixture.snapshot(0).await, expected);
        assert!(!fixture.head(0).exists(), "global terminal source cannot be recreated");
        assert_eq!(
            std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("target after old H"),
            target
        );
    }

    #[tokio::test]
    async fn native_multipart_old_restore_and_activation_cannot_recreate_a_later_consumed_head() {
        let fixture = Fixture::new().await;
        let round = fixture.start_round().await;
        let negative = fixture.negative(&round).await;
        let ready = fixture.restore_all(&negative).await;
        for slot in 0..4 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("activate native recovered sources");
        }
        let result = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("one actual successor publication")
            .1;
        let expected = fixture.snapshot(0).await;
        assert_eq!(expected.phase, MultipartSourcePhase::ConsumedUnsealed);
        let target = std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("successor target");
        for operation in [
            MultipartSourceOperation::Restore {
                expected: expected.clone(),
                certificate: negative,
            },
            MultipartSourceOperation::Activate {
                expected: expected.clone(),
                certificate: ready,
            },
        ] {
            assert!(
                fixture.disks[0]
                    .recover_native_on_heap(fixture.request(operation))
                    .await
                    .is_err(),
                "a current snapshot does not give historical certificates authority over a consumed head"
            );
            assert_eq!(fixture.snapshot(0).await, expected);
            assert!(!fixture.head(0).exists());
            assert_eq!(
                std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("successor after stale request"),
                target
            );
        }
        drop(result);
    }

    #[tokio::test]
    async fn native_multipart_cold_root_missing_uses_only_own_target_anchor() {
        let mut fixture = Fixture::new().await;
        let results = fixture.publish_zero_on(&[0, 1, 2]).await;
        let publication = results[0].1.publication.clone();
        for slot in 0..3 {
            fixture.disks[slot]
                .delete(
                    RUSTFS_META_MULTIPART_BUCKET,
                    &fixture.path,
                    DeleteOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("normal native source-root cleanup after publication");
            assert!(!fixture.head(slot).parent().expect("source root").exists());
        }
        drop(results);
        fixture.reopen().await;
        for slot in 0..3 {
            let response = fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: None,
                    publication: publication.clone(),
                    challenge: Uuid::new_v4(),
                }))
                .await
                .expect("fresh cold RootMissing confirmation from actual producing disk");
            let MultipartSourceResponse::TerminalConfirmed(proof) = response else {
                panic!("cold native terminal result");
            };
            assert_eq!(proof.kind, MultipartProofKind::RootMissingTerminal);
            proof
                .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
                .expect("fresh cold native proof provenance");
        }
        let anchor = |slot: usize| {
            fixture.disks[slot]
                .root
                .join("bucket/object")
                .join(publication.final_data_dir.to_string())
                .join(".mpu-source-retirement")
        };
        let donor = std::fs::read(anchor(0)).expect("actual producing-disk retirement anchor");
        let target =
            std::fs::read(fixture.disks[1].root.join("bucket/object/xl.meta")).expect("current native target before bad anchor");
        std::fs::write(anchor(1), donor).expect("inject donor anchor without rebinding disk identity");
        assert!(
            fixture.disks[1]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: None,
                    publication: publication.clone(),
                    challenge: Uuid::new_v4(),
                }))
                .await
                .is_err(),
            "a copied donor anchor cannot authorize this disk after cold reopen"
        );
        std::fs::remove_file(anchor(1)).expect("remove mismatched target anchor");
        assert!(
            fixture.disks[1]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: None,
                    publication,
                    challenge: Uuid::new_v4(),
                }))
                .await
                .is_err(),
            "a missing target anchor is a nonvote"
        );
        assert_eq!(
            std::fs::read(fixture.disks[1].root.join("bucket/object/xl.meta")).expect("target after rejected anchors"),
            target
        );
        assert!(
            !fixture.head(1).parent().expect("source root").exists(),
            "confirmation never recreates source or capabilities"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_multipart_confirm_waits_for_real_put_publication_on_configured_metadata_key() {
        let fixture = Fixture::new().await;
        let publication = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("actual old publication")
            .1
            .publication;
        let disk = std::sync::Arc::clone(&fixture.disks[0]);
        let destination = disk.io_get_object_path("bucket", "object").expect("pinned target path");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let _hook = os::prepared_publication_test_hooks::install(&destination.join("xl.meta"), move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let payload = bytes::Bytes::from_static(b"successor PUT");
        let mut successor = super::super::test::test_file_info("object", Uuid::nil(), None, Some(payload.clone()));
        // The real PUT producer marks inline shards before RenameData. The
        // generic legacy FI fixture does not set that representation flag.
        successor.set_inline_data();
        successor.erasure.data_blocks = publication.lineage.geometry.data_blocks;
        successor.erasure.parity_blocks = publication.lineage.geometry.parity_blocks;
        successor.erasure.block_size = publication.lineage.geometry.block_size;
        successor.erasure.distribution = publication.lineage.geometry.distribution.clone();
        successor.erasure.index = publication.lineage.geometry.distribution[0];
        let writer = tokio::spawn(async move {
            disk.rename_data(RUSTFS_META_TMP_BUCKET, "real-inline-successor", successor, "bucket", "object")
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
            .await
            .expect("actual PUT reaches physical publication")
            .expect("physical PUT pause entered");
        let mut confirm = Box::pin(fixture.disks[0].recover_native_on_heap(fixture.request(
            MultipartSourceOperation::ConfirmTerminal {
                expected: None,
                publication,
                challenge: Uuid::new_v4(),
            },
        )));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), confirm.as_mut())
                .await
                .is_err(),
            "the real PUT owns the configured metadata lease; replay cannot seal its predecessor while the syscall is paused"
        );
        release_tx.send(()).expect("release actual PUT publication");
        writer
            .await
            .expect("actual PUT task join")
            .expect("actual successor PUT completes");
        assert!(
            confirm.await.is_err(),
            "after serialization, the successor target must reject old completion confirmation"
        );
        let raw = std::fs::read(fixture.disks[0].root.join("bucket/object/xl.meta")).expect("committed successor FI");
        let info = get_file_info(
            &raw,
            "bucket",
            "object",
            "",
            FileInfoOpts {
                data: true,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("read actual successor inline body");
        assert!(info.inline_data(), "the committed successor keeps the actual PUT inline representation");
        assert_eq!(info.size, i64::try_from(payload.len()).expect("small successor body"));
        let committed = FileMeta::load(&raw).expect("actual committed successor FileMeta");
        let inline = committed
            .data
            .find(rustfs_filemeta::NULL_VERSION_ID)
            .expect("decode actual null-version inline map")
            .expect("committed successor payload exists");
        assert_eq!(
            inline.as_slice(),
            payload.as_ref(),
            "native inline storage preserves every successor byte"
        );
        assert_eq!(info.data, Some(payload));
        assert!(
            crate::disk::multipart_source::publication_identity(&info)
                .expect("successor marker read")
                .is_none()
        );
    }

    async fn put_actual_predecessor(fixture: &Fixture, slot: usize) -> Vec<u8> {
        for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
            match fixture.disks[slot].make_volume(volume).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(error) => panic!("prepare predecessor volume: {error}"),
            }
        }
        let geometry = fixture.snapshot(slot).await.lineage.geometry;
        let mut predecessor =
            super::super::test::test_file_info("object", Uuid::nil(), None, Some(bytes::Bytes::from_static(b"predecessor")));
        predecessor.erasure.data_blocks = geometry.data_blocks;
        predecessor.erasure.parity_blocks = geometry.parity_blocks;
        predecessor.erasure.block_size = geometry.block_size;
        predecessor.erasure.distribution = geometry.distribution.clone();
        predecessor.erasure.index = geometry.distribution[slot];
        fixture.disks[slot]
            .rename_data(RUSTFS_META_TMP_BUCKET, "physical-predecessor", predecessor, "bucket", "object")
            .await
            .expect("real predecessor PUT before injected multipart I/O failure");
        std::fs::read(fixture.disks[slot].root.join("bucket/object/xl.meta")).expect("actual predecessor bytes")
    }

    async fn activate_all_actual_heads(fixture: &Fixture, ready: &MultipartCertificate) {
        for slot in 0..4 {
            fixture.disks[slot]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Activate {
                    expected: fixture.snapshot(slot).await,
                    certificate: ready.clone(),
                }))
                .await
                .expect("activate actual all-N restored heads");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_post_publication_sync_failure_keeps_a_cold_recoverable_prepared_head() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let mut fixture = Fixture::new().await;
        let predecessor = put_actual_predecessor(&fixture, 0).await;
        let original = std::fs::read(fixture.head(0)).expect("independent source before preparation");
        let original_info = get_file_info(
            &original,
            RUSTFS_META_MULTIPART_BUCKET,
            &fixture.path,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("native original source version");
        let source_data = fixture
            .head(0)
            .parent()
            .expect("source root")
            .join(original_info.data_dir.expect("original DD").to_string());
        let completed = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
        let publication = crate::disk::multipart_source::publication_identity(&completed)
            .expect("captured native publication tuple")
            .expect("native completion marker");
        let selected_data = std::fs::read(source_data.join("part.1")).expect("selected source shard");
        let selected_meta = std::fs::read(source_data.join("part.1.meta")).expect("selected source generation");
        let target = fixture.disks[0]
            .io_get_object_path("bucket", "object")
            .expect("pinned physical target");
        let reached = Arc::new(AtomicBool::new(false));
        let reached_callback = Arc::clone(&reached);
        let target_callback = target.clone();
        let hook = os::prepared_publication_test_hooks::install(&target.join("xl.meta"), move || {
            reached_callback.store(true, Ordering::SeqCst);
            if os::dst_dir_fsync_group_commit_enabled() {
                os::fsync_dir_recorder::set_grouped_failure(&target_callback, std::io::ErrorKind::Other);
            } else {
                os::fsync_dir_recorder::set_failure(&target_callback, std::io::ErrorKind::Other);
            }
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fixture.disks[0].rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, completed, "bucket", "object"),
        )
        .await
        .expect("physical post-publication compensation must not deadlock");
        assert!(
            reached.load(Ordering::SeqCst),
            "fault is armed inside the actual canonical-head publication executor"
        );
        assert!(
            result.is_err(),
            "a failed target-parent durability barrier cannot return publication success"
        );
        drop(result);
        drop(hook);
        assert_eq!(std::fs::read(target.join("xl.meta")).expect("rolled-back target"), predecessor);
        assert_eq!(
            std::fs::read(source_data.join("part.1")).expect("selected shard after compensation"),
            selected_data
        );
        assert_eq!(
            std::fs::read(source_data.join("part.1.meta")).expect("selected metadata after compensation"),
            selected_meta
        );
        let failed_state = {
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("source after bounded error");
            let loaded = store.load().await.expect("actual compensated source records");
            let state = loaded.state.expect("durable PREPARED state");
            assert_eq!(state.phase, MultipartSourcePhase::Prepared);
            let head = loaded
                .head
                .expect("failed callback returns a PREPARED head before target Undo");
            assert_eq!(Some(head.sha256), state.prepared_sha256);
            assert_eq!(Some(head.length), state.prepared_length);
            assert!(loaded.head_matches_state);
            assert_eq!(store.original(&state).await.expect("owned independent original survives"), original);
            state
        };
        fixture.reopen().await;
        let target = fixture.disks[0]
            .io_get_object_path("bucket", "object")
            .expect("reopened pinned target");
        let inspection = fixture.inspect(0).await;
        assert_eq!(
            inspection.snapshot.as_ref().expect("cold prepared snapshot").phase,
            MultipartSourcePhase::Prepared
        );
        let round = Fixture::round_for(&publication);
        let negative = fixture.negative_on(&round, &[0, 1]).await;
        let ready = fixture.restore_all(&negative).await;
        activate_all_actual_heads(&fixture, &ready).await;
        let successor = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("publication after cold H/all-N recovery")
            .1;
        let current = fixture.snapshot(0).await;
        let current_target = std::fs::read(target.join("xl.meta")).expect("actual successor target");
        {
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("source for stale rollback check");
            let logical = fixture.disks[0]
                .get_object_path("bucket", "object")
                .expect("configured target ownership key");
            let owner = os::acquire_metadata_mutation_lease(&logical, store.owner.clone()).await;
            assert!(
                store.restore_exact_target(&failed_state, Some(owner.clone())).await.is_err(),
                "a historical failed context cannot restore its prior target over a new publication"
            );
        }
        assert_eq!(fixture.snapshot(0).await, current);
        assert!(!fixture.head(0).exists());
        assert_eq!(
            std::fs::read(target.join("xl.meta")).expect("successor after old rollback"),
            current_target
        );
        drop(successor);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_owned_undo_retries_after_real_target_restore_before_progress_publication() {
        use super::super::multipart_source_state::SourceState;
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };

        let fixture = Fixture::new().await;
        let predecessor = put_actual_predecessor(&fixture, 0).await;
        let result = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("real published failed-owner attempt")
            .1;
        let publication = result.publication.clone();
        let expected = result.proof.snapshot.clone();
        let round = Fixture::round_for(&publication);
        let capability_wire = rmp_serde::to_vec_named(&result.undo_capability).expect("same owner's opaque retry packet");
        let source = fixture.disks[0]
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &fixture.path)
            .expect("pinned physical source root for the actual persist hook");
        let state_path = source.join(".mpu-source-state");
        let target = fixture.disks[0]
            .io_get_object_path("bucket", "object")
            .expect("pinned target for physical Undo");
        let target_reached = Arc::new(AtomicBool::new(false));
        let progress_cut = Arc::new(AtomicBool::new(false));
        let false_record = Arc::new(Mutex::new(None::<Vec<u8>>));
        let progress_hook = Arc::new(Mutex::new(None::<os::prepared_publication_test_hooks::Guard>));
        let target_reached_callback = Arc::clone(&target_reached);
        let progress_cut_callback = Arc::clone(&progress_cut);
        let false_record_callback = Arc::clone(&false_record);
        let progress_hook_callback = Arc::clone(&progress_hook);
        let state_path_callback = state_path.clone();
        let undo_hook = os::prepared_publication_test_hooks::install(&target.join("xl.meta"), move || {
            target_reached_callback.store(true, Ordering::SeqCst);
            let hook = os::prepared_publication_test_hooks::install(&state_path_callback.clone(), move || {
                let raw = std::fs::read(&state_path_callback).expect("actual durable false Undo intent");
                let state: SourceState = rmp_serde::from_slice(&raw).expect("decode native fixed intent");
                assert_eq!(state.phase, MultipartSourcePhase::UndoIntent);
                assert!(!state.undo.as_ref().expect("owned durable intent").target_undone);
                let mut staged = std::fs::read_dir(&source)
                    .expect("native source record directory")
                    .map(|entry| entry.expect("native source record entry").path())
                    .filter(|path| {
                        path.file_name()
                            .is_some_and(|name| name.to_string_lossy().starts_with(".mpu-record-"))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    staged.len(),
                    1,
                    "only the actual unpublished progress inode is selected for fault injection"
                );
                let staged = staged.pop().expect("actual prepared progress inode");
                let progress: SourceState = rmp_serde::from_slice(&std::fs::read(&staged).expect("prepared true intent bytes"))
                    .expect("native progress record staged by production persist");
                assert_eq!(progress.phase, MultipartSourcePhase::UndoIntent);
                assert!(progress.undo.as_ref().expect("owned progress").target_undone);
                assert_eq!(progress.revision, state.revision);
                assert_eq!(progress.undo.as_ref().expect("progress").from, state.undo.as_ref().expect("intent").from);
                // Fail the real prepared rename before it can publish true.
                // The durable fixed record and all source/target metadata are
                // produced by the normal operations, never synthesized here.
                std::fs::remove_file(staged).expect("physical ENOENT on unpublished progress rename");
                *false_record_callback.lock().expect("capture actual fixed record") = Some(raw);
                progress_cut_callback.store(true, Ordering::SeqCst);
            });
            *progress_hook_callback.lock().expect("keep physical progress hook alive") = Some(hook);
        });
        let failed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fixture.disks[0].recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                expected: Box::new(expected.clone()),
                publication: publication.clone(),
                capability: result.undo_capability,
                round: round.clone(),
            })),
        )
        .await
        .expect("actual Undo persistence failure must return without deadlocking");
        assert!(target_reached.load(Ordering::SeqCst), "the actual target rollback hook must execute");
        assert!(
            progress_cut.load(Ordering::SeqCst),
            "the actual fixed-state publication hook must unlink the genuine unpublished true-progress inode"
        );
        assert!(failed.is_err(), "unpublished target-undone progress cannot issue a successful claim");
        drop(undo_hook);
        drop(progress_hook);
        assert_eq!(
            std::fs::read(target.join("xl.meta")).expect("actual target already restored"),
            predecessor
        );
        let false_record = false_record
            .lock()
            .expect("captured native receipt")
            .take()
            .expect("fixed false receipt");
        assert_eq!(
            std::fs::read(&state_path).expect("unchanged durable intent after failed true rename"),
            false_record
        );
        assert!(!fixture.head(0).exists());
        let pending = fixture.snapshot(0).await;
        assert_eq!(pending.phase, MultipartSourcePhase::UndoIntent);
        assert_eq!(pending.generation, expected.generation);
        let retry = rmp_serde::from_slice(&capability_wire).expect("same owned capability retry");
        let response = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                expected: Box::new(expected.clone()),
                publication: publication.clone(),
                capability: retry,
                round: round.clone(),
            }))
            .await
            .expect("strict owned prior-target after-image resumes the interrupted Undo");
        let MultipartSourceResponse::Claimed(owned) = response else {
            panic!("successful resumed owned claim");
        };
        assert_eq!(owned.snapshot.generation, round.generation);
        assert!(owned.snapshot.head_sha256.is_some());
        assert_eq!(
            std::fs::read(target.join("xl.meta")).expect("prior target after same-cap retry"),
            predecessor
        );
        let mut negative = fixture.negative_on(&round, &[1]).await;
        negative.proofs.push(owned);
        let ready = fixture.restore_all(&negative).await;
        activate_all_actual_heads(&fixture, &ready).await;
        let successor = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("new publication after owned recovery")
            .1;
        let current = fixture.snapshot(0).await;
        let current_target = std::fs::read(target.join("xl.meta")).expect("successor before stale owned retry");
        let stale = rmp_serde::from_slice(&capability_wire).expect("historical owned retry packet");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::UndoAndClaim {
                    expected: Box::new(expected),
                    publication,
                    capability: stale,
                    round,
                }))
                .await
                .is_err(),
            "an old intent cannot undo a successor after the same-cap retry has completed"
        );
        assert_eq!(fixture.snapshot(0).await, current);
        assert!(!fixture.head(0).exists());
        assert_eq!(
            std::fs::read(target.join("xl.meta")).expect("successor after stale owned retry"),
            current_target
        );
        drop(successor);
    }

    #[tokio::test]
    async fn native_multipart_large_prepared_history_is_a_cold_claimable_present_head() {
        let mut fixture = Fixture::new().await;
        let final_data_dir = Uuid::new_v4();
        let modified = time::OffsetDateTime::now_utc();
        let mut completed = fixture.zero_info(0, final_data_dir, modified);
        // A valid multi-version FileMeta can exceed the small source-FI limit.
        // Historical payloads are not read by a source-head inspection.
        let historical = completed.clone();
        let mut metadata = tokio::task::spawn_blocking(move || {
            let mut metadata = FileMeta::new();
            for version in 0..64 {
                let mut old = historical.clone();
                crate::disk::multipart_source::strip_native_controls(&mut old.metadata);
                old.version_id = Some(Uuid::new_v4());
                old.data_dir = Some(Uuid::new_v4());
                old.mod_time = Some(modified - time::Duration::seconds(version + 1));
                old.versioned = true;
                old.size = 5 * 1024 * 1024 * 10_000;
                old.parts = (1..=10_000)
                    .map(|number| ObjectPartInfo {
                        number,
                        size: 5 * 1024 * 1024,
                        actual_size: 5 * 1024 * 1024,
                        etag: "0123456789abcdef0123456789abcdef".to_owned(),
                        mod_time: old.mod_time,
                        ..Default::default()
                    })
                    .collect();
                old.validate_for_metadata_read().expect("valid historical multipart FI");
                metadata.add_version(old).expect("valid historical object version");
            }
            metadata
        })
        .await
        .expect("build valid multi-version metadata");
        let staging = fixture.disks[0]
            .prepare_multipart_commit_staging(&fixture.path, &completed, "bucket", None)
            .await
            .expect("real selected-part staging");
        let context = fixture.disks[0]
            .prepare_native_multipart_publication(&fixture.path, &mut completed, &staging)
            .await
            .expect("native original backup and prepared receipt");
        metadata.add_version(completed).expect("native current completion version");
        let raw = metadata.marshal_msg().expect("valid full historical FileMeta encoding");
        assert!(
            raw.len() > crate::disk::multipart_source::MULTIPART_SOURCE_MAX_PAYLOAD_BYTES,
            "fixture must cross 16 MiB without changing the original/control limits"
        );
        let decoded = FileMeta::load(&raw).expect("large proxy is actual valid FileMeta, not oversized junk");
        assert_eq!(decoded.versions.len(), 65);
        for version in &decoded.versions {
            let parsed = version
                .parse_version_meta()
                .expect("valid historical version metadata and CRC");
            let mut expected_header = parsed.header();
            // Native shallow headers encode the null slot as a fixed UUID;
            // the full version metadata decodes that same slot as None.
            expected_header.version_id = Some(expected_header.version_id.unwrap_or_else(Uuid::nil));
            assert_eq!(
                version.header, expected_header,
                "stored header signature, layout, native UUID slot and flags match metadata"
            );
            let info = version
                .into_fileinfo("bucket", "object", true)
                .expect("decode each real multipart version");
            info.validate_for_metadata_read()
                .expect("every version preserves a valid layout and part inventory");
            assert_eq!((info.erasure.data_blocks, info.erasure.parity_blocks, info.erasure.index), (2, 2, 1));
            assert_eq!(info.erasure.distribution, vec![1, 2, 3, 4]);
            assert!(info.data_dir.is_some_and(|value| !value.is_nil()));
            if info.version_id.is_none_or(|value| value.is_nil()) {
                assert_eq!((info.size, info.parts.len(), info.parts[0].number, info.parts[0].size), (0, 1, 1, 0));
            } else {
                assert_eq!(info.size, 5 * 1024 * 1024 * 10_000);
                assert_eq!(info.parts.len(), 10_000);
                assert!(info.parts.iter().enumerate().all(|(offset, part)| {
                    part.number == offset + 1 && part.size == 5 * 1024 * 1024 && part.actual_size == 5 * 1024 * 1024
                }));
            }
        }
        drop(decoded);
        drop(metadata);
        context
            .prepare_metadata(&raw, None)
            .await
            .expect("native digest and length before proxy installation");
        let prepared_path = context.prepared_path.clone();
        let canonical = context.canonical_head();
        let base = context.store.base.clone();
        let root = context.store.publication_root.clone();
        let prepared = os::run_blocking_namespace_operation(context.store.lease.clone(), move || {
            let source = prepared_path
                .parent()
                .ok_or_else(|| std::io::Error::other("prepared source parent"))?;
            let destination = canonical
                .parent()
                .ok_or_else(|| std::io::Error::other("canonical source parent"))?;
            let guard = os::prepare_rename_commit_guard(source, destination, &base, &root)?;
            let mut prepared = os::create_prepared_rename_source_with_commit_guard(&prepared_path, &canonical, &guard)?;
            prepared.write_all(&raw, true)?;
            Ok(prepared)
        })
        .await
        .expect("real independent prepared metadata file");
        drop(
            context
                .install_head(prepared)
                .await
                .expect("real atomic canonical prepared-head installation"),
        );
        let publication = context.publication.clone();
        let loaded = context.store.load().await.expect("large prepared observer");
        let head = loaded.head.as_ref().expect("prepared head remains present");
        assert!(
            head.raw().is_none(),
            "large proxy uses bounded streaming verification rather than a full raw allocation"
        );
        assert!(head.length > 16 * 1024 * 1024);
        assert!(loaded.head_matches_state);
        assert!(loaded.original.as_ref().expect("independent original FI").len() < 16 * 1024 * 1024);
        drop(loaded);
        drop(context);
        drop(staging);
        fixture.reopen().await;
        for disk in &fixture.disks {
            assert!(!disk.root.join("bucket/object/xl.meta").exists(), "no target metadata was ever published");
        }
        let inspection = fixture.inspect(0).await;
        let expected = inspection.snapshot.expect("cold large prepared source");
        assert_eq!(expected.phase, MultipartSourcePhase::Prepared);
        assert!(expected.head_sha256.is_some());
        let round = Fixture::round_for(&publication);
        let response = fixture.disks[0]
            .recover_native_on_heap(fixture.request(MultipartSourceOperation::Claim {
                expected,
                round: round.clone(),
            }))
            .await
            .expect("receipt-bound large prepared head is a genuine negative witness");
        let MultipartSourceResponse::Claimed(proof) = response else {
            panic!("large prepared native claim");
        };
        assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Recovering);
        assert_eq!(proof.snapshot.generation, round.generation);
        assert!(proof.snapshot.capability_sha256.is_none());
        assert!(
            std::fs::metadata(fixture.head(0))
                .expect("restored small original source head")
                .len()
                < 16 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn native_multipart_tmp_rejects_rebound_anchor_and_heal_preserves_only_own_record() {
        let fixture = Fixture::new().await;
        let result = fixture.publish_zero_on(&[0]).await.pop().expect("actual native producer").1;
        let publication = result.publication;
        let disk = &fixture.disks[0];
        let target_object = disk.io_get_object_path("bucket", "object").expect("pinned target object");
        let target_data = target_object.join(publication.final_data_dir.to_string());
        let original_anchor = std::fs::read(target_data.join(".mpu-source-retirement")).expect("actual own producing anchor");
        let original_target = std::fs::read(target_object.join("xl.meta")).expect("actual current target FI");
        let mut healing = get_file_info(
            &original_target,
            "bucket",
            "object",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("actual live version for same-disk heal");
        healing.set_healing();
        let source = disk
            .io_get_object_path(RUSTFS_META_TMP_BUCKET, &format!("same-disk-heal/{}", publication.final_data_dir))
            .expect("pinned private DD supplied by the real heal caller");
        std::fs::create_dir_all(&source).expect("same-version repair DD");
        {
            let metadata = os::acquire_metadata_mutation_lease(
                &disk.get_object_path("bucket", "object").expect("configured metadata key"),
                None,
            )
            .await;
            let lease =
                os::acquire_rename_data_mutation_lease_with_owner(&disk.root, "bucket", &target_object, None, Some(metadata))
                    .await;
            disk.retain_owned_multipart_anchor(&healing, "bucket", "object", &source, &target_data, lease)
                .await
                .expect("actual native heal retention helper");
        }
        assert_eq!(
            std::fs::read(source.join(".mpu-source-retirement")).expect("preserved own record"),
            original_anchor
        );
        let mut foreign: UntrustedAnchorBlob = rmp_serde::from_slice(&original_anchor).expect("actual anchor codec");
        foreign.disk_id = fixture.disk_ids[1];
        std::fs::write(
            target_data.join(".mpu-source-retirement"),
            rmp_serde::to_vec_named(&foreign).expect("donor-bound anchor bytes"),
        )
        .expect("inject foreign record on target");
        let no_provenance = disk
            .io_get_object_path(RUSTFS_META_TMP_BUCKET, &format!("foreign-target-heal/{}", publication.final_data_dir))
            .expect("pinned private DD for a nonproducing repair");
        std::fs::create_dir_all(&no_provenance).expect("repair without own producing record");
        {
            let metadata = os::acquire_metadata_mutation_lease(
                &disk.get_object_path("bucket", "object").expect("configured metadata key"),
                None,
            )
            .await;
            let lease =
                os::acquire_rename_data_mutation_lease_with_owner(&disk.root, "bucket", &target_object, None, Some(metadata))
                    .await;
            disk.retain_owned_multipart_anchor(&healing, "bucket", "object", &no_provenance, &target_data, lease)
                .await
                .expect("heal may proceed without importing donor authority");
        }
        assert!(
            !no_provenance.join(".mpu-source-retirement").exists(),
            "heal never rebinds a donor record as local authority"
        );
        std::fs::write(target_data.join(".mpu-source-retirement"), &original_anchor)
            .expect("restore native owned target fixture");
        let forged_data_dir = Uuid::new_v4();
        let forged_source = format!("forged-anchor/{forged_data_dir}");
        let staged = disk
            .io_get_object_path(RUSTFS_META_TMP_BUCKET, &forged_source)
            .expect("actual ordinary TMP I/O path");
        std::fs::create_dir_all(&staged).expect("untrusted ordinary TMP DD");
        let mut incoming = super::super::test::test_file_info("object", Uuid::nil(), Some(forged_data_dir), None);
        incoming.erasure.data_blocks = publication.lineage.geometry.data_blocks;
        incoming.erasure.parity_blocks = publication.lineage.geometry.parity_blocks;
        incoming.erasure.block_size = publication.lineage.geometry.block_size;
        incoming.erasure.distribution = publication.lineage.geometry.distribution.clone();
        incoming.erasure.index = publication.lineage.geometry.distribution[0];
        let condition = crate::disk::multipart_source::SourceConditionV1 {
            lineage: publication.lineage.clone(),
            generation: publication.source_generation,
            attempt: forged_data_dir,
        };
        let forged_publication = condition.attach(&mut incoming).expect("caller-authored common FI marker");
        let mut rebound: UntrustedAnchorBlob = rmp_serde::from_slice(&original_anchor).expect("untrusted record value");
        rebound.disk_id = fixture.disk_ids[0];
        rebound.physical_slot = 0;
        rebound.publication = forged_publication;
        rebound.source_validation_receipt_id = Uuid::new_v4();
        std::fs::write(
            staged.join(".mpu-source-retirement"),
            rmp_serde::to_vec_named(&rebound).expect("rebound caller bytes"),
        )
        .expect("untrusted incoming provenance blob");
        std::fs::write(staged.join("part.1"), b"x").expect("untrusted ordinary TMP part");
        assert!(
            disk.rename_data(RUSTFS_META_TMP_BUCKET, "forged-anchor", incoming, "bucket", "object")
                .await
                .is_err(),
            "ordinary TMP publication cannot import even a matching-ID caller anchor"
        );
        assert_eq!(
            std::fs::read(target_object.join("xl.meta")).expect("target after rejected TMP anchor"),
            original_target
        );
        assert_eq!(
            std::fs::read(target_data.join(".mpu-source-retirement")).expect("owned anchor after rejected TMP"),
            original_anchor
        );
        let raw_path = format!("raw-anchor/{}/.mpu-source-retirement", Uuid::new_v4());
        assert!(
            disk.write_all(RUSTFS_META_TMP_BUCKET, &raw_path, bytes::Bytes::from_static(b"caller bytes"))
                .await
                .is_err(),
            "the raw public writer cannot manufacture reserved provenance files"
        );
        assert!(!disk.root.join(RUSTFS_META_TMP_BUCKET).join(raw_path).exists());
    }

    async fn assert_cold_recovery_after_present_head_failure(
        fixture: &mut Fixture,
        publication: &PublicationIdentity,
        phase: MultipartSourcePhase,
    ) {
        fixture.reopen().await;
        let snapshot = fixture.snapshot(0).await;
        assert_eq!(snapshot.phase, phase);
        assert!(
            snapshot.head_sha256.is_some(),
            "a failed participant retains a real claimable source head"
        );
        let raw = std::fs::read(fixture.head(0)).expect("cold source before failed terminal confirmation");
        assert!(
            fixture.disks[0]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                    expected: Some(snapshot.clone()),
                    publication: publication.clone(),
                    challenge: Uuid::new_v4(),
                }))
                .await
                .is_err(),
            "unpublished or compensated preparation cannot supply a terminal vote"
        );
        assert_eq!(fixture.snapshot(0).await, snapshot);
        assert_eq!(std::fs::read(fixture.head(0)).expect("cold source after nonvote"), raw);
        let round = Fixture::round_for(publication);
        let negative = fixture.negative_on(&round, &[0, 1]).await;
        let ready = fixture.restore_all(&negative).await;
        activate_all_actual_heads(fixture, &ready).await;
        let fresh = fixture
            .publish_zero_on(&[0])
            .await
            .pop()
            .expect("fresh publication after actual cold recovery")
            .1;
        assert_eq!(fresh.publication.source_generation, round.generation);
        assert_ne!(fresh.publication.final_data_dir, publication.final_data_dir);
        drop(fresh);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_anchor_syscall_failures_never_ack_or_turn_private_records_into_votes() {
        use os::multipart_source_syscall_failures::{self as failures, Stage};

        for failure in [Some(Stage::Create), Some(Stage::Write), Some(Stage::FileSync), None] {
            let mut fixture = Fixture::new().await;
            for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
                match fixture.disks[0].make_volume(volume).await {
                    Ok(()) | Err(DiskError::VolumeExists) => {}
                    Err(error) => panic!("prepare anchor-fault volume: {error}"),
                }
            }
            let original = std::fs::read(fixture.head(0)).expect("original before actual anchor preparation");
            let mut completed = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
            let publication = crate::disk::multipart_source::publication_identity(&completed)
                .expect("actual completion tuple")
                .expect("captured source condition");
            let staging = fixture.disks[0]
                .prepare_multipart_commit_staging(&fixture.path, &completed, "bucket", None)
                .await
                .expect("actual validated selected-shard staging");
            let private_dd = staging.private_directory(publication.final_data_dir);
            let selected = std::fs::read(private_dd.join("part.1")).expect("real selected zero shard before anchor");
            let sidecar = std::fs::read(private_dd.join("part.1.meta")).expect("real selected generation before anchor");
            let guard = failure
                .map(|stage| failures::fail_once(stage, &private_dd.join(".mpu-source-retirement"), std::io::ErrorKind::Other));
            if failure.is_none() {
                os::fsync_dir_recorder::set_failure(&private_dd, std::io::ErrorKind::Other);
            }
            let prepared = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                fixture.disks[0].prepare_native_multipart_publication(&fixture.path, &mut completed, &staging),
            )
            .await
            .expect("actual anchor syscall failure must be bounded");
            assert!(
                prepared.is_err(),
                "anchor create/write/file-sync/DD-entry-sync failure cannot return a publication context"
            );
            if let Some(guard) = &guard {
                assert!(guard.was_triggered(), "the exact requested anchor syscall fault was consumed");
            } else {
                assert!(
                    os::fsync_dir_recorder::take_failure(&private_dd).is_none(),
                    "the actual anchor parent sync consumed the fault"
                );
            }
            drop(prepared);
            drop(guard);
            assert_eq!(std::fs::read(fixture.head(0)).expect("source head after failed anchor"), original);
            assert_eq!(std::fs::read(private_dd.join("part.1")).expect("selected shard remains"), selected);
            assert_eq!(std::fs::read(private_dd.join("part.1.meta")).expect("selected sidecar remains"), sidecar);
            assert!(
                !fixture.disks[0].root.join("bucket/object/xl.meta").exists(),
                "zero target publications before anchor barriers"
            );
            assert!(
                !fixture.disks[0]
                    .root
                    .join("bucket/object")
                    .join(publication.final_data_dir.to_string())
                    .exists()
            );
            drop(staging);
            assert_cold_recovery_after_present_head_failure(&mut fixture, &publication, MultipartSourcePhase::Prepared).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_dd_publish_consume_and_source_parent_failures_remain_cold_recoverable() {
        use os::multipart_source_syscall_failures::{self as failures, Stage};

        for cut in ["dd-publication", "canonical-consume", "source-parent-sync"] {
            let mut fixture = Fixture::new().await;
            let predecessor = put_actual_predecessor(&fixture, 0).await;
            let original = std::fs::read(fixture.head(0)).expect("source before publication cut");
            let completed = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
            let publication = crate::disk::multipart_source::publication_identity(&completed)
                .expect("actual completion tuple")
                .expect("captured native condition");
            let selected_root = fixture
                .head(0)
                .parent()
                .expect("actual upload root")
                .join(publication.lineage.original_data_dir.to_string());
            let selected = std::fs::read(selected_root.join("part.1")).expect("actual selected shard before failure");
            let sidecar = std::fs::read(selected_root.join("part.1.meta")).expect("actual selected generation before failure");
            let target = fixture.disks[0]
                .io_get_object_path("bucket", "object")
                .expect("actual pinned destination");
            let source = fixture.disks[0]
                .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &fixture.path)
                .expect("actual pinned source");
            let fault = match cut {
                "dd-publication" => Some(failures::fail_once(
                    Stage::Rename,
                    &target.join(publication.final_data_dir.to_string()),
                    std::io::ErrorKind::Other,
                )),
                "canonical-consume" => {
                    Some(failures::fail_once(Stage::Rename, &target.join("xl.meta"), std::io::ErrorKind::Other))
                }
                _ => None,
            };
            let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let source_parent_hook = if cut == "source-parent-sync" {
                let source = source.clone();
                let armed = std::sync::Arc::clone(&armed);
                Some(os::prepared_publication_test_hooks::install(&target.join("xl.meta"), move || {
                    armed.store(true, std::sync::atomic::Ordering::SeqCst);
                    os::fsync_dir_recorder::set_failure(&source, std::io::ErrorKind::Other);
                }))
            } else {
                None
            };
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                fixture.disks[0].rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, completed, "bucket", "object"),
            )
            .await
            .expect("actual publication failure compensation must be bounded");
            assert!(response.is_err(), "required {cut} failure cannot ACK a native publication");
            if let Some(fault) = &fault {
                assert!(fault.was_triggered(), "the requested physical publication rename failed");
            } else {
                assert!(armed.load(std::sync::atomic::Ordering::SeqCst));
                assert!(
                    os::fsync_dir_recorder::take_failure(&source).is_none(),
                    "consume-to-source-parent barrier actually failed"
                );
            }
            drop(response);
            drop(fault);
            drop(source_parent_hook);
            assert_eq!(
                std::fs::read(target.join("xl.meta")).expect("exact prior target after failure"),
                predecessor
            );
            assert_eq!(
                std::fs::read(selected_root.join("part.1")).expect("selected shard after failure"),
                selected
            );
            assert_eq!(
                std::fs::read(selected_root.join("part.1.meta")).expect("selected generation after failure"),
                sidecar
            );
            {
                let store = fixture.disks[0]
                    .open_multipart_source_store(&fixture.path, None)
                    .await
                    .expect("actual failed source records");
                let loaded = store.load().await.expect("failed native preparation remains inspectable");
                let state = loaded.state.as_ref().expect("actual preparation receipt");
                assert_eq!(state.phase, MultipartSourcePhase::Prepared);
                assert!(loaded.head_matches_state);
                assert_eq!(store.original(state).await.expect("independent original after failure"), original);
            }
            assert_cold_recovery_after_present_head_failure(&mut fixture, &publication, MultipartSourcePhase::Prepared).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_private_selected_entry_sync_failure_keeps_the_original_active_source() {
        use std::sync::{Arc, Mutex};

        let mut fixture = Fixture::new().await;
        for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
            match fixture.disks[0].make_volume(volume).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(error) => panic!("prepare selected-entry sync failure volume: {error}"),
            }
        }
        let original = std::fs::read(fixture.head(0)).expect("original active source before physical staging");
        let completed = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
        let publication = crate::disk::multipart_source::publication_identity(&completed)
            .expect("actual native tuple")
            .expect("captured source condition");
        let source_data = fixture.disks[0]
            .io_get_object_path(
                RUSTFS_META_MULTIPART_BUCKET,
                &format!("{}/{}", fixture.path, publication.lineage.original_data_dir),
            )
            .expect("actual selected source directory");
        let selected = std::fs::read(source_data.join("part.1")).expect("selected actual zero shard");
        let sidecar = std::fs::read(source_data.join("part.1.meta")).expect("selected actual zero generation");
        let tmp = fixture.disks[0]
            .io_get_bucket_path(RUSTFS_META_TMP_BUCKET)
            .expect("actual private attempt namespace");
        let actual_directory = Arc::new(Mutex::new(None::<std::path::PathBuf>));
        let actual_directory_callback = Arc::clone(&actual_directory);
        let final_dd = publication.final_data_dir;
        let directory_hook = os::prepared_publication_test_hooks::observe_hardlink_destination(&source_data, move |directory| {
            assert!(
                directory.starts_with(&tmp),
                "the observed physical destination belongs to the real private TMP namespace"
            );
            assert_eq!(
                directory.file_name().expect("actual private final DD").to_string_lossy(),
                final_dd.to_string()
            );
            os::fsync_dir_recorder::set_failure(directory, std::io::ErrorKind::Other);
            *actual_directory_callback.lock().expect("capture exact physical destination") = Some(directory.to_path_buf());
        });
        let staged = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fixture.disks[0].prepare_multipart_commit_staging(&fixture.path, &completed, "bucket", None),
        )
        .await
        .expect("actual selected-entry directory sync error must be bounded");
        assert!(
            staged.is_err(),
            "failed private selected-entry fsync cannot return a validated staging batch"
        );
        drop(staged);
        drop(directory_hook);
        let directory = actual_directory
            .lock()
            .expect("actual fault directory")
            .take()
            .expect("physical hardlink destination observer ran");
        assert!(
            os::fsync_dir_recorder::take_failure(&directory).is_none(),
            "the direct private-DD fsync consumed the exact fault"
        );
        assert_eq!(std::fs::read(fixture.head(0)).expect("unchanged original active head"), original);
        assert_eq!(
            std::fs::read(source_data.join("part.1")).expect("selected original shard after failure"),
            selected
        );
        assert_eq!(
            std::fs::read(source_data.join("part.1.meta")).expect("selected original sidecar after failure"),
            sidecar
        );
        assert!(!fixture.disks[0].root.join("bucket/object/xl.meta").exists());
        assert!(
            !fixture
                .head(0)
                .parent()
                .expect("upload root")
                .join(".mpu-source-state")
                .exists()
        );
        assert_cold_recovery_after_present_head_failure(&mut fixture, &publication, MultipartSourcePhase::Active).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_multipart_owned_record_ancestor_failures_are_rechecked_on_immutable_retry() {
        for depth in 0..3 {
            let fixture = Fixture::new().await;
            let original = std::fs::read(fixture.head(0)).expect("real source before immutable backup");
            let store = fixture.disks[0]
                .open_multipart_source_store(&fixture.path, None)
                .await
                .expect("native backup ownership");
            let owner = Uuid::new_v4();
            let leaf = store.source.join(".mpu-completions").join(owner.to_string());
            let ancestor = match depth {
                0 => leaf.clone(),
                1 => store.source.join(".mpu-completions"),
                _ => store.source.clone(),
            };
            for _ in 0..2 {
                os::fsync_dir_recorder::set_failure(&ancestor, std::io::ErrorKind::Other);
                assert!(
                    store.write_owned_backup(owner, false, &original).await.is_err(),
                    "new and immutable-existing backup writes require every ancestor barrier"
                );
                assert!(
                    os::fsync_dir_recorder::take_failure(&ancestor).is_none(),
                    "the selected backup ancestor barrier must actually run"
                );
                assert_eq!(
                    std::fs::read(leaf.join("original.xl.meta")).expect("genuine immutable copied source"),
                    original
                );
                assert_eq!(std::fs::read(fixture.head(0)).expect("authoritative source remains unchanged"), original);
                assert!(
                    !store.source.join(".mpu-source-state").exists(),
                    "an unaccepted backup does not invent source state or a cap"
                );
            }
            store
                .write_owned_backup(owner, false, &original)
                .await
                .expect("immutable-equal retry succeeds only after all parent barriers");
            assert!(os::fsync_dir_recorder::was_fsynced(&leaf));
            assert!(os::fsync_dir_recorder::was_fsynced(&store.source.join(".mpu-completions")));
            assert!(os::fsync_dir_recorder::was_fsynced(&store.source));
        }
        for depth in 0..3 {
            let fixture = Fixture::new().await;
            let round = fixture.start_round().await;
            let negative = fixture.negative(&round).await;
            {
                let store = fixture.disks[2]
                    .open_multipart_source_store(&fixture.path, None)
                    .await
                    .expect("establish the real legacy baseline");
                let loaded = store.load().await.expect("actual active source");
                store
                    .ensure_state(&loaded)
                    .await
                    .expect("baseline state before certificate fault");
            }
            let before = fixture.snapshot(2).await;
            let original = std::fs::read(fixture.head(2)).expect("source before real H certificate persistence");
            let source = fixture.disks[2]
                .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &fixture.path)
                .expect("pinned source root");
            let leaf = source.join(".mpu-recovery").join(round.round.to_string());
            let ancestor = match depth {
                0 => leaf.clone(),
                1 => source.join(".mpu-recovery"),
                _ => source.clone(),
            };
            for _ in 0..2 {
                os::fsync_dir_recorder::set_failure(&ancestor, std::io::ErrorKind::Other);
                assert!(
                    fixture.disks[2]
                        .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                            expected: before.clone(),
                            certificate: negative.clone(),
                        }))
                        .await
                        .is_err(),
                    "real H Restore cannot ACK before every certificate parent entry is durable, including immutable-equal retry"
                );
                assert!(
                    os::fsync_dir_recorder::take_failure(&ancestor).is_none(),
                    "the selected certificate ancestor barrier must actually run"
                );
                assert_eq!(fixture.snapshot(2).await, before);
                assert_eq!(std::fs::read(fixture.head(2)).expect("head after failed certificate barrier"), original);
                assert_eq!(
                    std::fs::read(leaf.join("negative.msgpack")).expect("genuine persisted native H vector"),
                    rmp_serde::to_vec_named(&negative).expect("same exact native certificate bytes")
                );
            }
            let restored = fixture.disks[2]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected: before,
                    certificate: negative,
                }))
                .await
                .expect("same real certificate is accepted after all barriers finish");
            assert!(matches!(restored, MultipartSourceResponse::Restored(_)));
            assert!(os::fsync_dir_recorder::was_fsynced(&leaf));
            assert!(os::fsync_dir_recorder::was_fsynced(&source.join(".mpu-recovery")));
            assert!(os::fsync_dir_recorder::was_fsynced(&source));
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_multipart_cancelled_init_keeps_source_ownership_through_its_actual_parent_sync() {
        use std::sync::{Arc, Mutex};

        let fixture = Fixture::new_with_missing_source(Some(0)).await;
        for volume in [RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, "bucket"] {
            match fixture.disks[0].make_volume(volume).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(error) => panic!("prepare actual Init volume: {error}"),
            }
        }
        let mut initial = get_file_info(
            &std::fs::read(fixture.head(1)).expect("real cohort's original Init FI"),
            RUSTFS_META_MULTIPART_BUCKET,
            &fixture.path,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("valid initial source metadata");
        initial.erasure.index = 1;
        initial.fresh = true;
        let mut initial_meta = FileMeta::new();
        initial_meta
            .add_version(initial.clone())
            .expect("actual Init FileMeta representation");
        let mut completed = get_file_info(
            &initial_meta.marshal_msg().expect("Init metadata codec"),
            RUSTFS_META_MULTIPART_BUCKET,
            &fixture.path,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("captured source matches the bytes Init will publish");
        let final_dd = Uuid::new_v4();
        let original_dd = completed.data_dir.expect("actual initial source DD");
        let condition =
            crate::disk::multipart_source::capture_source_condition("bucket", "object", &fixture.path, &completed, final_dd)
                .expect("capture actual initial lineage");
        completed.add_object_part(1, "zero-part".to_owned(), 0, completed.mod_time, 0, None, None);
        let selected_meta = completed.parts[0]
            .marshal_msg()
            .expect("actual selected zero-part generation");
        completed.data_dir = Some(final_dd);
        completed.metadata.remove(RUSTFS_MULTIPART_BUCKET_KEY);
        completed.metadata.remove(RUSTFS_MULTIPART_OBJECT_KEY);
        condition.attach(&mut completed).expect("native stage condition after Init");
        let source = fixture.disks[0]
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &fixture.path)
            .expect("physical missing source");
        let (rename_enter_tx, rename_enter_rx) = tokio::sync::oneshot::channel();
        let (rename_release_tx, rename_release_rx) = std::sync::mpsc::channel::<()>();
        let (parent_enter_tx, parent_enter_rx) = tokio::sync::oneshot::channel();
        let (parent_release_tx, parent_release_rx) = std::sync::mpsc::channel::<()>();
        let parent_guard = Arc::new(Mutex::new(None::<os::prepared_publication_test_hooks::Guard>));
        let parent_guard_callback = Arc::clone(&parent_guard);
        let source_callback = source.clone();
        let init_hook = os::prepared_publication_test_hooks::install_at(
            os::prepared_publication_test_hooks::Stage::Rename,
            &source.join("xl.meta"),
            move || {
                let guard = os::prepared_publication_test_hooks::install_at(
                    os::prepared_publication_test_hooks::Stage::DirFsync,
                    &source_callback,
                    move || {
                        let _ = parent_enter_tx.send(());
                        let _ = parent_release_rx.recv();
                    },
                );
                *parent_guard_callback.lock().expect("keep Init parent barrier callback alive") = Some(guard);
                let _ = rename_enter_tx.send(());
                let _ = rename_release_rx.recv();
            },
        );
        let disk = Arc::clone(&fixture.disks[0]);
        let path = fixture.path.clone();
        let init = tokio::spawn(async move { disk.write_metadata("", RUSTFS_META_MULTIPART_BUCKET, &path, initial).await });
        tokio::time::timeout(std::time::Duration::from_secs(10), rename_enter_rx)
            .await
            .expect("actual Init reaches its physical rename")
            .expect("Init physical rename paused");
        init.abort();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), init)
                .await
                .expect("logical Init waiter cancellation is bounded")
                .is_err()
        );
        let mut stage = Box::pin(fixture.disks[0].prepare_multipart_commit_staging(&fixture.path, &completed, "bucket", None));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), stage.as_mut())
                .await
                .is_err(),
            "cancelled Init's actual rename callback still owns source validation"
        );
        assert!(!fixture.head(0).exists());
        rename_release_tx.send(()).expect("release Init rename syscall");
        tokio::time::timeout(std::time::Duration::from_secs(10), parent_enter_rx)
            .await
            .expect("actual Init must continue its required parent sync after logical cancellation")
            .expect("Init physical parent sync paused");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), stage.as_mut())
                .await
                .is_err(),
            "source ownership lasts through the real parent sync, so no anchor can be issued early"
        );
        let tmp = fixture.disks[0].root.join(RUSTFS_META_TMP_BUCKET);
        assert!(
            !std::fs::read_dir(&tmp)
                .expect("actual private attempt root")
                .filter_map(std::result::Result::ok)
                .any(|entry| entry
                    .path()
                    .join(final_dd.to_string())
                    .join(".mpu-source-retirement")
                    .exists())
        );
        // The missing-slot fixture has just published Init's header. Give
        // the waiting completion real selected records while its source
        // writer is still physically paused, before staging may acquire it.
        let selected_source = source.join(original_dd.to_string());
        std::fs::create_dir_all(&selected_source).expect("actual selected source DD after Init publication");
        std::fs::write(selected_source.join("part.1"), b"").expect("actual selected zero-byte shard");
        std::fs::write(selected_source.join("part.1.meta"), &selected_meta).expect("actual selected zero-part sidecar");
        parent_release_tx
            .send(())
            .expect("finish Init's physical parent durability barrier");
        let staged = tokio::time::timeout(std::time::Duration::from_secs(10), stage)
            .await
            .expect("stage proceeds after real Init tail drains")
            .expect("actual staging validates the durable initial head");
        assert_eq!(
            std::fs::read(staged.private_directory(final_dd).join("part.1"))
                .expect("real selected zero shard staged after Init tail"),
            b""
        );
        assert_eq!(
            std::fs::read(staged.private_directory(final_dd).join("part.1.meta"))
                .expect("real selected generation staged after Init tail"),
            selected_meta
        );
        let native = fixture.disks[0]
            .prepare_native_multipart_publication(&fixture.path, &mut completed, &staged)
            .await
            .expect("native provenance is issued only after the complete physical Init tail");
        let anchor: UntrustedAnchorBlob = rmp_serde::from_slice(
            &std::fs::read(staged.private_directory(final_dd).join(".mpu-source-retirement"))
                .expect("actual native anchor after Init durability"),
        )
        .expect("actual owned anchor record");
        assert_eq!((anchor.disk_id, anchor.physical_slot), (fixture.disk_ids[0], 0));
        assert_eq!(anchor.publication, native.publication);
        drop(native);
        drop(staged);
        drop(init_hook);
        drop(parent_guard);
    }

    #[tokio::test]
    async fn native_multipart_late_part_mutators_cannot_recreate_removed_upload() {
        let fixture = Fixture::new().await;
        let snapshot = fixture.snapshot(0).await;
        let destination = format!("{}/{}/part.1", fixture.path, snapshot.lineage.original_data_dir);
        let source = format!("late-part-{}/part.1", Uuid::new_v4());
        let source_path = fixture.disks[0].root.join(RUSTFS_META_TMP_BUCKET).join(&source);
        std::fs::create_dir_all(source_path.parent().expect("late part temporary parent")).expect("late part temporary root");
        let payload = b"late part payload";
        std::fs::write(&source_path, payload).expect("real delayed part shard");
        let metadata = bytes::Bytes::from(
            ObjectPartInfo {
                number: 1,
                size: payload.len(),
                actual_size: i64::try_from(payload.len()).expect("small late part"),
                ..Default::default()
            }
            .marshal_msg()
            .expect("real delayed part metadata"),
        );
        let upload = fixture.head(0).parent().expect("canonical upload root").to_owned();
        std::fs::remove_dir_all(&upload).expect("authoritative source-root removal");
        assert!(
            fixture.disks[0]
                .prepare_part_transaction(
                    RUSTFS_META_TMP_BUCKET,
                    &source,
                    RUSTFS_META_MULTIPART_BUCKET,
                    &destination,
                    metadata.clone(),
                )
                .await
                .is_err(),
            "a late transaction prepare must reject before mkdir"
        );
        assert!(!upload.exists(), "late transaction prepare cannot recreate the upload");
        assert!(
            fixture.disks[0]
                .rename_part(RUSTFS_META_TMP_BUCKET, &source, RUSTFS_META_MULTIPART_BUCKET, &destination, metadata,)
                .await
                .is_err(),
            "a late part publication must reject before creating destination parents"
        );
        assert!(!upload.exists(), "late part publication cannot recreate the upload");
        assert_eq!(std::fs::read(source_path).expect("late shard remains retryable in TMP"), payload);
    }

    #[tokio::test]
    async fn native_multipart_restore_never_materializes_a_missing_initial_slot() {
        let fixture = Fixture::new_with_missing_source(Some(3)).await;
        let round = fixture.start_round().await;
        let mut expected = fixture.snapshot(0).await;
        expected.disk_id = fixture.disk_ids[3];
        expected.physical_slot = 3;
        let source_root = fixture.head(3).parent().expect("canonical source root").to_owned();
        assert!(!source_root.exists(), "legal Q-only origin never initialized the fourth source");
        let negative = fixture.negative(&round).await;
        assert!(
            fixture.disks[3]
                .recover_native_on_heap(fixture.request(MultipartSourceOperation::Restore {
                    expected,
                    certificate: negative,
                }))
                .await
                .is_err(),
            "H proves no old quorum; it cannot synthesize a never-existing original slot"
        );
        assert!(!source_root.exists(), "recovery must preserve the all-N-origin availability boundary");
    }
}
