// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{Fixture, put_actual_predecessor};
use crate::disk::{
    DiskAPI, DiskError, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET,
    multipart_source::{
        MultipartAbortCertificate, MultipartAbortProofKind, MultipartAbortRound, MultipartSourceOperation, MultipartSourcePhase,
        MultipartSourceResponse, PublicationIdentity, SourceGeneration, native_source_protocol, publication_identity,
        recognized_native_source_controls,
    },
    os::{self, multipart_source_syscall_failures as failures, prepared_publication_test_hooks as hooks},
};
use futures::FutureExt as _;
use rustfs_filemeta::{FileInfoOpts, get_file_info};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use time::OffsetDateTime;
use uuid::Uuid;

// Construct concrete native fixture futures off the caller's poll frame;
// boxing an already-built future would retain its large stack temporary.
#[inline(never)]
fn heap_native<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

async fn publication_volumes(fixture: &Fixture) {
    for disk in &fixture.disks {
        for volume in ["bucket", RUSTFS_META_TMP_BUCKET] {
            match disk.make_volume(volume).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(error) => panic!("prepare actual publication volume {volume}: {error}"),
            }
        }
    }
}

async fn abort_round(fixture: &Fixture) -> MultipartAbortRound {
    let snapshot = heap_native(|| fixture.snapshot(0)).await;
    MultipartAbortRound {
        lineage: snapshot.lineage,
        generation: SourceGeneration {
            epoch: snapshot.generation.epoch.checked_add(1).expect("fresh native abort epoch"),
            nonce: Uuid::new_v4(),
        },
        round: Uuid::new_v4(),
    }
}

async fn close_all(fixture: &Fixture, round: &MultipartAbortRound) -> MultipartAbortCertificate {
    let authority = fixture.disks[0].multipart_proof_verification_context();
    let mut proofs = Vec::with_capacity(4);
    for slot in 0..4 {
        let expected = heap_native(|| fixture.snapshot(slot)).await;
        let response = heap_native(|| {
            fixture.disks[slot].recover_multipart_source_native(fixture.request(MultipartSourceOperation::PrepareAbort {
                expected,
                round: round.clone(),
            }))
        })
        .await
        .expect("each actual participant durably closes its source");
        let MultipartSourceResponse::AbortPrepared(proof) = response else {
            panic!("actual PrepareAbort response");
        };
        proof
            .verify_with_authority(&authority)
            .expect("actual native preparation proof");
        assert_eq!(proof.kind, MultipartAbortProofKind::Prepared);
        assert_eq!(proof.snapshot.physical_slot, slot);
        assert_eq!(proof.snapshot.disk_id, fixture.disk_ids[slot]);
        assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
        assert!(proof.snapshot.head_sha256.is_none());
        proofs.push(proof);
    }
    let certificate = MultipartAbortCertificate {
        round: round.clone(),
        disk_ids: fixture.disk_ids.clone(),
        proofs,
    };
    certificate
        .verify_with_authority(&authority)
        .expect("all-N actual native closure certificate");
    for slot in 0..4 {
        let expected = heap_native(|| fixture.snapshot(slot)).await;
        let response = heap_native(|| {
            fixture.disks[slot].recover_multipart_source_native(fixture.request(MultipartSourceOperation::CommitAbort {
                expected,
                certificate: certificate.clone(),
            }))
        })
        .await
        .expect("all actual roots durably bind the closure certificate");
        let MultipartSourceResponse::AbortCommitted(proof) = response else {
            panic!("actual CommitAbort response");
        };
        proof
            .verify_with_authority(&authority)
            .expect("actual committed closure proof");
        assert_eq!(proof.kind, MultipartAbortProofKind::Committed);
        assert_eq!(proof.certificate_sha256, Some(certificate.digest().expect("exact certificate digest")));
        assert_eq!(heap_native(|| fixture.inspect(slot)).await.abort_certificate.as_ref(), Some(&certificate));
        assert!(!fixture.head(slot).exists(), "closure never leaves a reusable canonical head");
    }
    certificate
}

type TargetBytes = Option<(Vec<u8>, Vec<u8>, Vec<u8>)>;

fn target_bytes(fixture: &Fixture) -> Vec<TargetBytes> {
    fixture
        .disks
        .iter()
        .enumerate()
        .map(|(slot, disk)| {
            let target = disk.io_get_object_path("bucket", "object").expect("actual physical target");
            let raw = match std::fs::read(target.join("xl.meta")) {
                Ok(raw) => raw,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
                Err(error) => panic!("read actual target on slot {slot}: {error}"),
            };
            let info = get_file_info(
                &raw,
                "bucket",
                "object",
                "",
                FileInfoOpts {
                    data: false,
                    include_free_versions: false,
                    include_part_checksums: true,
                },
            )
            .expect("actual target metadata decodes");
            assert_eq!(info.erasure.distribution.get(slot), Some(&info.erasure.index));
            let data = target.join(info.data_dir.expect("actual native target DD").to_string());
            Some((
                raw,
                std::fs::read(data.join("part.1")).expect("actual native selected shard"),
                std::fs::read(data.join("part.1.meta")).expect("actual native selected sidecar"),
            ))
        })
        .collect()
}

fn assert_target_publications(fixture: &Fixture, expected: &PublicationIdentity, producers: &[usize]) {
    let targets = target_bytes(fixture);
    for (slot, target) in targets.iter().enumerate() {
        if !producers.contains(&slot) {
            assert!(target.is_none(), "slot {slot} never physically published the target");
            continue;
        }
        let raw = &target.as_ref().expect("actual physical target vote").0;
        let info = get_file_info(
            raw,
            "bucket",
            "object",
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("actual target vote decodes");
        assert_eq!(publication_identity(&info).expect("actual publication tuple").as_ref(), Some(expected));
        assert_eq!(info.data_dir, Some(expected.final_data_dir));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn native_abort_waits_for_a_real_late_third_publication_before_closing_all_sources() {
    let fixture = heap_native(Fixture::new).await;
    publication_volumes(&fixture).await;
    let published = heap_native(|| fixture.publish_zero_on(&[0, 1])).await;
    let expected_publication = published[0].1.publication.clone();
    assert_target_publications(&fixture, &expected_publication, &[0, 1]);
    let expected = heap_native(|| fixture.snapshot(2)).await;
    let round = abort_round(&fixture).await;
    let completed = fixture.zero_info(
        2,
        expected_publication.final_data_dir,
        OffsetDateTime::from_unix_timestamp_nanos(expected_publication.modified_unix_nanos).expect("shared publication time"),
    );
    let target = fixture.disks[2]
        .io_get_object_path("bucket", "object")
        .expect("late physical target");
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let hook = hooks::install(&target.join("xl.meta"), move || {
        let _ = entered_tx.send(());
        let _ = release_rx.recv();
    });
    let disk = fixture.disks[2].clone();
    let path = fixture.path.clone();
    let mut publication = tokio::spawn(async move {
        disk.rename_data(RUSTFS_META_MULTIPART_BUCKET, &path, completed, "bucket", "object")
            .await
    });
    let mut release = Some(release_tx);
    let mut joined = false;
    let observations = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("actual third publication callback must enter")
            .expect("physical callback notification");
        let prepared_head = std::fs::read(fixture.head(2)).expect("the actual callback owns a prepared source head");
        let mut abort = heap_native(|| {
            fixture.disks[2].recover_multipart_source_native(fixture.request(MultipartSourceOperation::PrepareAbort {
                expected,
                round: round.clone(),
            }))
        });
        // The native entry's first await is the SourceRW writer, before
        // disk inspection or mutation; polling reaches the real held owner.
        assert!(futures::poll!(abort.as_mut()).is_pending());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), abort.as_mut())
                .await
                .is_err(),
            "PrepareAbort must wait for the actual publication's SourceRW owner"
        );
        assert_eq!(std::fs::read(fixture.head(2)).expect("head while publication is blocked"), prepared_head);
        assert_target_publications(&fixture, &expected_publication, &[0, 1]);
        drop(release.take());
        let response = tokio::time::timeout(Duration::from_secs(10), &mut publication)
            .await
            .expect("actual publication callback drains")
            .expect("native publication task")
            .expect("third actual publication");
        joined = true;
        let result = response.multipart_source.expect("actual native publication result");
        result
            .verify_with_authority(&fixture.disks[2].multipart_proof_verification_context())
            .expect("actual third proof");
        assert_eq!(result.publication, expected_publication);
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(10), abort.as_mut())
                    .await
                    .expect("stale closure waiter drains"),
                Err(DiskError::ConditionalDeleteConflict)
            ),
            "the pre-publication snapshot cannot authorize a post-publication closure"
        );
        drop(abort);
        let before = target_bytes(&fixture);
        assert_target_publications(&fixture, &expected_publication, &[0, 1, 2]);
        heap_native(|| close_all(&fixture, &round)).await;
        assert_eq!(target_bytes(&fixture), before, "source closure cannot Undo the three actual target votes");
    })
    .catch_unwind()
    .await;
    drop(release.take());
    drop(hook);
    if !joined {
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut publication)
            .await
            .expect("release and drain actual callback");
    }
    if let Err(panic) = observations {
        std::panic::resume_unwind(panic);
    }
    drop(published);
}

#[cfg(unix)]
#[tokio::test]
async fn native_abort_preserves_target_votes_when_compensation_restores_prepared_head_but_target_undo_fails() {
    let fixture = heap_native(Fixture::new).await;
    publication_volumes(&fixture).await;
    let predecessor = heap_native(|| put_actual_predecessor(&fixture, 2)).await;
    let published = heap_native(|| fixture.publish_zero_on(&[0, 1])).await;
    let publication = published[0].1.publication.clone();
    let completed = fixture.zero_info(
        2,
        publication.final_data_dir,
        OffsetDateTime::from_unix_timestamp_nanos(publication.modified_unix_nanos).expect("shared publication time"),
    );
    let stale = completed.clone();
    let original = std::fs::read(fixture.head(2)).expect("exact original before physical publication");
    let target = fixture.disks[2]
        .io_get_object_path("bucket", "object")
        .expect("actual target for failed compensation");
    let undo_failure = Arc::new(Mutex::new(None::<failures::FailureGuard>));
    let undo_failure_callback = undo_failure.clone();
    let sync_hook = Arc::new(Mutex::new(None::<hooks::Guard>));
    let sync_hook_callback = sync_hook.clone();
    let target_callback = target.clone();
    let hook = hooks::install(&target.join("xl.meta"), move || {
        // macOS checks Rename again after PreparedRename. Arm the Undo fault
        // only at the actual parent sync, after the initial rename completes.
        let sync_target = target_callback.clone();
        let sync_key = if os::dst_dir_fsync_group_commit_enabled() {
            target_callback
                .canonicalize()
                .expect("group commit uses the canonical physical directory")
        } else {
            target_callback.clone()
        };
        let reached = hooks::install_at(hooks::Stage::DirFsync, &sync_key, move || {
            if os::dst_dir_fsync_group_commit_enabled() {
                os::fsync_dir_recorder::set_grouped_failure(&sync_target, std::io::ErrorKind::Other);
            } else {
                os::fsync_dir_recorder::set_failure(&sync_target, std::io::ErrorKind::Other);
            }
            *undo_failure_callback.lock().expect("retain exact Undo fault") = Some(failures::fail_once(
                failures::Stage::Rename,
                &sync_target.join("xl.meta"),
                std::io::ErrorKind::Other,
            ));
        });
        *sync_hook_callback.lock().expect("retain actual parent sync hook") = Some(reached);
    });
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        fixture.disks[2].rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, completed, "bucket", "object"),
    )
    .await
    .expect("actual failed compensation is bounded");
    assert!(
        response.is_err(),
        "failed durability and physical Undo cannot ACK the publication: {response:?}"
    );
    let native_error = format!("{:?}", response.as_ref().err());
    drop(response);
    drop(hook);
    drop(sync_hook);
    assert!(
        undo_failure
            .lock()
            .expect("actual Undo fault")
            .as_ref()
            .expect("target callback armed Undo fault")
            .was_triggered(),
        "the physical target Undo must actually fail: native_error={native_error}"
    );
    drop(undo_failure);
    let pending_sync_failure = if os::dst_dir_fsync_group_commit_enabled() {
        os::fsync_dir_recorder::take_grouped_failure(&target)
    } else {
        os::fsync_dir_recorder::take_failure(&target)
    };
    assert!(
        pending_sync_failure.is_none(),
        "the required real target-parent sync failure must be consumed: native_error={native_error}"
    );
    let inspection = heap_native(|| fixture.inspect(2)).await;
    let snapshot = inspection.snapshot.expect("actual prepared state after failed Undo");
    assert_eq!(snapshot.phase, MultipartSourcePhase::Prepared);
    assert!(
        snapshot.head_sha256.is_some(),
        "compensation restored the exact prepared source proxy first"
    );
    assert_eq!(inspection.original.as_deref(), Some(original.as_slice()));
    let before = target_bytes(&fixture);
    assert_ne!(before[2].as_ref().expect("failed Undo leaves actual target vote").0, predecessor);
    assert_eq!(
        std::fs::read(fixture.head(2)).expect("actual restored prepared head"),
        before[2].as_ref().expect("the failed Undo retained its real target vote").0
    );
    assert_target_publications(&fixture, &publication, &[0, 1, 2]);
    let round = abort_round(&fixture).await;
    heap_native(|| close_all(&fixture, &round)).await;
    assert_eq!(
        target_bytes(&fixture),
        before,
        "Abort never treats the restored prepared head as a target-negative vote"
    );
    assert!(
        fixture.disks[2]
            .rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, stale, "bucket", "object")
            .await
            .is_err(),
        "a captured old publication cannot reopen the closed source"
    );
    assert_eq!(target_bytes(&fixture), before, "the old publication cannot change retained target votes");
    drop(published);
}

#[cfg(unix)]
#[tokio::test]
async fn native_abort_waits_for_an_all_active_marked_source_stage_captured_before_reserve() {
    let fixture = heap_native(Fixture::new).await;
    publication_volumes(&fixture).await;
    let expected = heap_native(|| fixture.snapshot(0)).await;
    let round = abort_round(&fixture).await;
    for slot in 0..4 {
        let inspection = heap_native(|| fixture.inspect(slot)).await;
        let snapshot = inspection.snapshot.expect("all-ACTIVE actual source");
        assert_eq!(snapshot.phase, MultipartSourcePhase::Active);
        assert!(snapshot.publication.is_none());
        assert!(snapshot.capability_sha256.is_none());
        let raw = std::fs::read(fixture.head(slot)).expect("actual marked initial metadata");
        let info = get_file_info(
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
        .expect("actual marked initial FI");
        assert!(native_source_protocol(&info).expect("strict known birth marker"));
        assert!(recognized_native_source_controls(&info).expect("native classification includes the birth marker"));
        assert!(
            !fixture.head(slot).parent().unwrap().join(".mpu-source-state").exists(),
            "all-ACTIVE native Init has no private reservation state yet"
        );
    }
    let completed = fixture.zero_info(0, Uuid::new_v4(), OffsetDateTime::now_utc());
    let original = std::fs::read(fixture.head(0)).expect("original head before capture");
    let staging = heap_native(|| fixture.disks[0].prepare_multipart_commit_staging(&fixture.path, &completed, "bucket", None))
        .await
        .expect("real private stage captures the original SourceRW owner");
    assert!(
        !fixture.head(0).parent().unwrap().join(".mpu-source-state").exists(),
        "capture must precede native reserve and prepared proxy creation"
    );
    let mut abort = heap_native(|| {
        fixture.disks[0].recover_multipart_source_native(fixture.request(MultipartSourceOperation::PrepareAbort {
            expected,
            round: round.clone(),
        }))
    });
    assert!(
        futures::poll!(abort.as_mut()).is_pending(),
        "the actual pre-reserve owner must enqueue Abort"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), abort.as_mut())
            .await
            .is_err(),
        "all-ACTIVE metadata cannot allow Abort to overtake a real pre-reserve source owner"
    );
    assert_eq!(std::fs::read(fixture.head(0)).expect("head protected by the old stage"), original);
    assert_eq!(target_bytes(&fixture), vec![None; 4], "the captured stage has not published any target");
    drop(staging);
    let response = tokio::time::timeout(Duration::from_secs(10), abort.as_mut())
        .await
        .expect("PrepareAbort proceeds after the actual stage releases its SourceRW owner")
        .expect("unchanged all-ACTIVE snapshot authorizes closure");
    assert!(matches!(response, MultipartSourceResponse::AbortPrepared(_)));
    drop(abort);
    heap_native(|| close_all(&fixture, &round)).await;
    assert!(
        fixture.disks[0]
            .rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, completed, "bucket", "object")
            .await
            .is_err(),
        "the pre-reserve captured condition cannot publish after certified closure"
    );
    assert_eq!(target_bytes(&fixture), vec![None; 4], "fenced old staging must not create any target");
}
