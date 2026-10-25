// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::disk::multipart_source::{MultipartAbortCertificate, MultipartAbortProofKind, MultipartAbortRound};
use crate::disk::{DataDirDeleteStatus, DeleteOptions, RUSTFS_META_TMP_DELETED_BUCKET};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// Construct concrete native fixture futures in a separate frame as well as
// pinning them on the heap; boxing an already-built future in a test poll
// frame would still leave its large construction temporary on that stack.
#[inline(never)]
fn heap_native<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

fn exact_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut entries = BTreeMap::new();
    if !root.exists() {
        return entries;
    }
    let mut pending = vec![(root.to_path_buf(), 0usize)];
    while let Some((path, depth)) = pending.pop() {
        assert!(depth <= 16, "native fixture tree must remain bounded");
        let metadata = std::fs::symlink_metadata(&path).expect("fixture entry metadata");
        assert!(!metadata.file_type().is_symlink(), "native fixture contains no aliases");
        let relative = path
            .strip_prefix(root)
            .expect("entry belongs to fixed fixture root")
            .to_path_buf();
        if metadata.is_dir() {
            entries.insert(relative, None);
            for entry in std::fs::read_dir(path).expect("fixture directory") {
                pending.push((entry.expect("fixture directory entry").path(), depth + 1));
            }
        } else {
            assert!(metadata.is_file(), "only regular native records may be compared");
            entries.insert(relative, Some(std::fs::read(path).expect("exact native record bytes")));
        }
    }
    entries
}

fn source_root(fixture: &Fixture, slot: usize) -> PathBuf {
    fixture.head(slot).parent().expect("canonical source root").to_path_buf()
}

fn all_sources(fixture: &Fixture) -> Vec<BTreeMap<PathBuf, Option<Vec<u8>>>> {
    (0..4).map(|slot| exact_tree(&source_root(fixture, slot))).collect()
}

fn all_targets(fixture: &Fixture) -> Vec<BTreeMap<PathBuf, Option<Vec<u8>>>> {
    fixture
        .disks
        .iter()
        .map(|disk| exact_tree(&disk.root.join("bucket/object")))
        .collect()
}

async fn prepare_actual_abort(fixture: &Fixture) -> MultipartAbortCertificate {
    let snapshot = heap_native(|| fixture.snapshot(0)).await;
    let round = MultipartAbortRound {
        lineage: snapshot.lineage,
        generation: SourceGeneration {
            epoch: snapshot.generation.epoch + 1,
            nonce: Uuid::new_v4(),
        },
        round: Uuid::new_v4(),
    };
    let mut proofs = Vec::with_capacity(4);
    for slot in 0..4 {
        let response = fixture.disks[slot]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::PrepareAbort {
                expected: heap_native(|| fixture.snapshot(slot)).await,
                round: round.clone(),
            }))
            .await
            .expect("close the actual physical source under its writer");
        let MultipartSourceResponse::AbortPrepared(proof) = response else {
            panic!("PrepareAbort must return a native closure proof")
        };
        proof
            .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
            .expect("actual native preparation proof");
        assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
        assert_eq!(proof.snapshot.physical_slot, slot);
        assert_eq!(proof.snapshot.disk_id, fixture.disk_ids[slot]);
        assert!(proof.snapshot.head_sha256.is_none());
        assert!(proof.snapshot.capability_sha256.is_none());
        assert!(!fixture.head(slot).exists(), "PrepareAbort retires the canonical head durably");
        proofs.push(proof);
    }
    let certificate = MultipartAbortCertificate {
        round,
        disk_ids: fixture.disk_ids.clone(),
        proofs,
    };
    certificate
        .verify_with_authority(&fixture.disks[0].multipart_proof_verification_context())
        .expect("real all-N closure certificate");
    certificate
}

async fn commit_actual_abort(fixture: &Fixture, certificate: &MultipartAbortCertificate) {
    for slot in 0..4 {
        let response = fixture.disks[slot]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::CommitAbort {
                expected: heap_native(|| fixture.snapshot(slot)).await,
                certificate: certificate.clone(),
            }))
            .await
            .expect("bind the complete certificate on every actual source root");
        let MultipartSourceResponse::AbortCommitted(proof) = response else {
            panic!("CommitAbort must return native durable binding")
        };
        proof
            .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
            .expect("native committed binding proof");
        assert_eq!(
            proof.certificate_sha256,
            Some(certificate.digest().expect("digest of actual full-N certificate"))
        );
        assert_eq!(heap_native(|| fixture.inspect(slot)).await.abort_certificate.as_ref(), Some(certificate));
    }
}

async fn assert_generic_deletes_preserve_source(fixture: &Fixture, slot: usize) {
    let before = exact_tree(&source_root(fixture, slot));
    let disk = &fixture.disks[slot];
    for immediate in [false, true] {
        assert!(
            disk.delete(
                RUSTFS_META_MULTIPART_BUCKET,
                &fixture.path,
                DeleteOptions {
                    recursive: true,
                    immediate,
                    ..Default::default()
                }
            )
            .await
            .is_err(),
            "generic recursive deletion cannot discard durable closure evidence"
        );
        assert_eq!(exact_tree(&source_root(fixture, slot)), before);
    }
    assert!(
        disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, std::slice::from_ref(&fixture.path))
            .await
            .is_err(),
        "raw DeletePaths cannot move a closed upload to trash"
    );
    assert_eq!(exact_tree(&source_root(fixture, slot)), before);
    let prefix = fixture.path.rsplit_once('/').expect("real object hash prefix").0;
    assert!(
        disk.delete(
            RUSTFS_META_MULTIPART_BUCKET,
            prefix,
            DeleteOptions {
                recursive: true,
                ..Default::default()
            }
        )
        .await
        .is_err(),
        "an ancestor prefix cannot bypass an Aborting child"
    );
    assert_eq!(exact_tree(&source_root(fixture, slot)), before);
    assert!(
        disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, &[prefix.to_string()])
            .await
            .is_err()
    );
    assert_eq!(exact_tree(&source_root(fixture, slot)), before);
}

#[tokio::test]
async fn native_abort_pending_intent_is_not_a_delete_certificate_and_raw_cleanup_preserves_exact_evidence() {
    let fixture = heap_native(Fixture::new).await;
    let original = all_sources(&fixture);
    let certificate = heap_native(|| prepare_actual_abort(&fixture)).await;
    let before = all_sources(&fixture);
    for slot in 0..4 {
        let initial_head = original[slot].get(Path::new("xl.meta")).unwrap().as_ref().unwrap();
        for name in ["original.xl.meta", "aborted-head.xl.meta"] {
            let matching = before[slot]
                .iter()
                .filter(|(path, _)| path.file_name().is_some_and(|leaf| leaf == name))
                .map(|(_, bytes)| bytes.as_ref().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                matching,
                vec![initial_head],
                "the exact marked initial metadata must survive in both owned records"
            );
        }
        for (path, bytes) in &original[slot] {
            if path != Path::new("xl.meta") {
                assert_eq!(
                    before[slot].get(path),
                    Some(bytes),
                    "preparation cannot rewrite original source payload files"
                );
            }
        }
        let inspection = heap_native(|| fixture.inspect(slot)).await;
        assert_eq!(inspection.abort_round.as_ref(), Some(&certificate.round));
        assert!(
            inspection.abort_certificate.is_none(),
            "N preparations alone are not a durable committed binding"
        );
        heap_native(|| assert_generic_deletes_preserve_source(&fixture, slot)).await;
        assert!(
            fixture.disks[slot]
                .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                    expected: heap_native(|| fixture.snapshot(slot)).await,
                    certificate: certificate.clone(),
                }))
                .await
                .is_err(),
            "the executor must require this root's actual committed certificate"
        );
    }
    assert_eq!(
        all_sources(&fixture),
        before,
        "failed deletion must preserve every original, retired head and receipt byte"
    );
}

#[tokio::test]
async fn native_abort_delete_requires_all_n_actual_slots_and_the_current_exact_committed_binding() {
    let fixture = heap_native(Fixture::new).await;
    let first = heap_native(|| prepare_actual_abort(&fixture)).await;
    heap_native(|| commit_actual_abort(&fixture, &first)).await;
    let old_expected = heap_native(|| fixture.snapshot(0)).await;
    let mut fewer = first.clone();
    fewer.proofs.pop();
    let mut swapped = first.clone();
    swapped.disk_ids.swap(0, 1);
    let foreign_fixture = heap_native(Fixture::new).await;
    let foreign = heap_native(|| prepare_actual_abort(&foreign_fixture)).await;
    for certificate in [fewer, swapped, foreign] {
        let before = all_sources(&fixture);
        assert!(
            fixture.disks[0]
                .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                    expected: heap_native(|| fixture.snapshot(0)).await,
                    certificate,
                }))
                .await
                .is_err()
        );
        assert_eq!(all_sources(&fixture), before, "bad evidence cannot mutate any native source");
    }
    let before = all_sources(&fixture);
    assert!(
        fixture.disks[0]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                expected: heap_native(|| fixture.snapshot(1)).await,
                certificate: first.clone(),
            }))
            .await
            .is_err(),
        "a valid neighboring slot is not this participant's expected state"
    );
    assert_eq!(all_sources(&fixture), before);

    // A real later all-N intent must preserve the old committed decision until
    // its own complete certificate is durably committed; it is not a new binding.
    let second = heap_native(|| prepare_actual_abort(&fixture)).await;
    assert_ne!(second.round, first.round);
    let before = all_sources(&fixture);
    assert_eq!(heap_native(|| fixture.inspect(0)).await.abort_certificate.as_ref(), Some(&first));
    assert!(
        fixture.disks[0]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                expected: heap_native(|| fixture.snapshot(0)).await,
                certificate: second.clone(),
            }))
            .await
            .is_err(),
        "a prepared N certificate cannot substitute for the current committed binding"
    );
    assert_eq!(all_sources(&fixture), before);
    heap_native(|| commit_actual_abort(&fixture, &second)).await;
    let before = all_sources(&fixture);
    assert!(
        fixture.disks[0]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                expected: heap_native(|| fixture.snapshot(0)).await,
                certificate: first,
            }))
            .await
            .is_err(),
        "an old real certificate cannot erase a newer committed binding"
    );
    assert!(
        fixture.disks[0]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                expected: old_expected,
                certificate: second,
            }))
            .await
            .is_err(),
        "old state CAS cannot authorize a current certificate's delete"
    );
    assert_eq!(all_sources(&fixture), before);
}

#[tokio::test]
async fn native_abort_rejects_old_recovery_activation_init_part_and_publication_without_reopening_a_head() {
    let fixture = heap_native(Fixture::new).await;
    let initial_raw = std::fs::read(fixture.head(0)).expect("exact marked initial source head");
    let mut initial = get_file_info(
        &initial_raw,
        RUSTFS_META_MULTIPART_BUCKET,
        &fixture.path,
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: true,
        },
    )
    .expect("decode the actual marked initial source");
    initial.fresh = true;
    let recovery = heap_native(|| fixture.start_round()).await;
    let negative = heap_native(|| fixture.negative(&recovery)).await;
    let ready = heap_native(|| fixture.restore_all(&negative)).await;
    heap_native(|| activate_all_actual_heads(&fixture, &ready)).await;
    let old_incoming = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
    let certificate = heap_native(|| prepare_actual_abort(&fixture)).await;
    heap_native(|| commit_actual_abort(&fixture, &certificate)).await;
    let before = all_sources(&fixture);
    let current = heap_native(|| fixture.snapshot(0)).await;
    let fresh_recovery = MultipartRecoveryRound {
        lineage: current.lineage.clone(),
        from_generation: current.generation,
        old_attempt: Uuid::new_v4(),
        generation: SourceGeneration {
            epoch: current.generation.epoch + 1,
            nonce: Uuid::new_v4(),
        },
        round: Uuid::new_v4(),
    };
    for operation in [
        MultipartSourceOperation::Claim {
            expected: current.clone(),
            round: fresh_recovery,
        },
        MultipartSourceOperation::Restore {
            expected: current.clone(),
            certificate: negative,
        },
        MultipartSourceOperation::Activate {
            expected: current,
            certificate: ready,
        },
    ] {
        assert!(
            fixture.disks[0]
                .recover_multipart_source(fixture.request(operation))
                .await
                .is_err(),
            "normal recovery methods cannot reopen an irreversibly closed source"
        );
        assert_eq!(all_sources(&fixture), before);
    }
    assert!(
        fixture.disks[0]
            .write_metadata("", RUSTFS_META_MULTIPART_BUCKET, &fixture.path, initial.clone())
            .await
            .is_err(),
        "a late actual Init cannot recreate the retired head"
    );
    let new_part = format!("{}/{}/part.2", fixture.path, initial.data_dir.expect("original source data directory"));
    assert!(
        fixture.disks[0]
            .write_all(RUSTFS_META_MULTIPART_BUCKET, &new_part, bytes::Bytes::from_static(b"late part"))
            .await
            .is_err()
    );
    assert!(
        fixture.disks[0]
            .rename_data(RUSTFS_META_MULTIPART_BUCKET, &fixture.path, old_incoming, "bucket", "object")
            .await
            .is_err(),
        "an old captured native publication cannot become current by reading the closure generation"
    );
    assert_eq!(all_sources(&fixture), before);
    assert!(!fixture.head(0).exists());
}

#[cfg(not(windows))]
#[tokio::test]
async fn native_abort_delete_rechecks_the_committed_certificate_inside_the_actual_rename_callback() {
    use crate::disk::os::prepared_publication_test_hooks as hooks;
    use std::sync::atomic::{AtomicBool, Ordering};
    let fixture = heap_native(Fixture::new).await;
    match fixture.disks[0].make_volume(RUSTFS_META_TMP_DELETED_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("prepare the actual strict-delete rename parent: {error}"),
    }
    let certificate = heap_native(|| prepare_actual_abort(&fixture)).await;
    heap_native(|| commit_actual_abort(&fixture, &certificate)).await;
    let expected = heap_native(|| fixture.snapshot(0)).await;
    let source = fixture.disks[0]
        .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &fixture.path)
        .unwrap();
    let before = all_sources(&fixture);
    let original_certificate = rmp_serde::to_vec_named(&certificate).expect("actual native certificate encoding");
    let records = before[0]
        .iter()
        .filter(|(_, bytes)| bytes.as_ref() == Some(&original_certificate))
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1, "the first real commit has one exact durable certificate record");
    let certificate_path = source.join(records[0]);
    let entered = std::sync::Arc::new(AtomicBool::new(false));
    let entered_callback = entered.clone();
    let fault_path = certificate_path.clone();
    let hook = hooks::install_at(hooks::Stage::Rename, &source, move || {
        // Simulate physical record corruption AFTER handler prevalidation,
        // without manufacturing any alternate certificate or local authority.
        std::fs::write(fault_path, b"faulted committed certificate").expect("inject an actual callback-time record fault");
        entered_callback.store(true, Ordering::Release);
    });
    let outcome = fixture.disks[0]
        .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted { expected, certificate }))
        .await;
    drop(hook);
    assert!(
        entered.load(Ordering::Acquire),
        "the failure must occur at the actual physical rename, not an earlier rejection"
    );
    assert!(
        outcome.is_err(),
        "the syscall's second permit validation must reject changed committed bytes"
    );
    assert!(
        source_root(&fixture, 0).exists(),
        "a failed callback must leave the authoritative source in place"
    );
    std::fs::write(certificate_path, original_certificate).expect("remove only the injected corruption");
    assert_eq!(
        all_sources(&fixture),
        before,
        "no source evidence may be moved or deleted by the failed callback"
    );
}

#[tokio::test]
async fn native_abort_partial_delete_retains_the_producer_certificate_and_never_changes_exact_targets() {
    let mut fixture = heap_native(Fixture::new).await;
    for slot in 0..4 {
        heap_native(|| put_actual_predecessor(&fixture, slot)).await;
        match fixture.disks[slot].make_volume(RUSTFS_META_TMP_DELETED_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("prepare the ordinary native trash volume: {error}"),
        }
    }
    let produced = heap_native(|| fixture.publish_zero_on(&[0, 1, 2])).await;
    let publication = produced[0].1.publication.clone();
    assert_eq!(produced.len(), 3, "real target C has the ordinary write quorum only on producers 0/1/2");
    let targets = all_targets(&fixture);
    let certificate = heap_native(|| prepare_actual_abort(&fixture)).await;
    assert_eq!(all_targets(&fixture), targets, "preparation must not touch any target");
    heap_native(|| commit_actual_abort(&fixture, &certificate)).await;
    assert_eq!(all_targets(&fixture), targets, "committing source closure must not touch any target");
    for slot in [0, 1, 3] {
        let before = exact_tree(&source_root(&fixture, slot));
        let response = fixture.disks[slot]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::DeleteAborted {
                expected: heap_native(|| fixture.snapshot(slot)).await,
                certificate: certificate.clone(),
            }))
            .await
            .expect("strict certified native source retirement");
        let MultipartSourceResponse::AbortDeleted(proof) = response else {
            panic!("strict deletion returns its bound native outcome")
        };
        proof
            .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
            .expect("native proof of strict source retirement");
        assert_eq!(proof.kind, MultipartAbortProofKind::Deleted);
        assert!(!source_root(&fixture, slot).exists());
        let trash = fixture.disks[slot].root.join(RUSTFS_META_TMP_DELETED_BUCKET);
        let exact_retirements = std::fs::read_dir(trash)
            .expect("native trash entries after strict retirement")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().expect("native trash entry type").is_dir() && exact_tree(&entry.path()) == before)
            .count();
        assert_eq!(
            exact_retirements, 1,
            "atomic retirement preserves every exact source file in one native trash root"
        );
        assert_eq!(
            all_targets(&fixture),
            targets,
            "Abort must never restore, rewrite or delete any target version"
        );
    }
    // The coordinator may lose any/all deletion responses; the actual survivor
    // must preserve and cold-rehydrate the earlier all-N committed decision.
    drop(produced);
    heap_native(|| fixture.reopen()).await;
    let survivor = heap_native(|| fixture.inspect(2)).await;
    let cold_certificate = survivor
        .abort_certificate
        .expect("the only remaining source retains the committed N certificate");
    cold_certificate
        .verify_with_authority(&fixture.disks[2].multipart_proof_verification_context())
        .expect("rehydrated native authority for the committed certificate");
    assert_eq!(
        cold_certificate.digest().expect("digest of actual full-N certificate"),
        certificate.digest().expect("digest of actual full-N certificate")
    );
    let challenge = Uuid::new_v4();
    let response = fixture.disks[2]
        .recover_multipart_source(fixture.request(MultipartSourceOperation::ConfirmCleanupTarget {
            expected: survivor.snapshot.expect("remaining producer's exact Aborting state"),
            publication: publication.clone(),
            challenge,
        }))
        .await
        .expect("the true producer can confirm C while preserving its cleanup evidence");
    let MultipartSourceResponse::CleanupTargetConfirmed(proof) = response else {
        panic!("an existing Aborting source cannot be faked as RootMissing")
    };
    proof
        .verify_with_authority(&fixture.disks[2].multipart_proof_verification_context())
        .expect("actual surviving producer cleanup target proof");
    assert_eq!(proof.kind, MultipartAbortProofKind::CleanupTarget);
    assert_eq!(proof.challenge, challenge);
    assert_eq!(
        proof.certificate_sha256,
        Some(cold_certificate.digest().expect("digest of actual full-N certificate"))
    );
    assert_eq!(proof.publication.as_ref(), Some(&publication));
    assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
    for slot in [0, 1] {
        let response = fixture.disks[slot]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                expected: None,
                publication: publication.clone(),
                challenge,
            }))
            .await
            .expect("a deleted producer root still has its exact own target retirement anchor");
        let MultipartSourceResponse::TerminalConfirmed(proof) = response else {
            panic!("deleted producer target confirmation")
        };
        proof
            .verify_with_authority(&fixture.disks[slot].multipart_proof_verification_context())
            .expect("fresh native terminal proof of the deleted producer");
        assert_eq!(proof.kind, MultipartProofKind::RootMissingTerminal);
        assert_eq!(proof.challenge, challenge);
    }
    assert!(
        fixture.disks[3]
            .recover_multipart_source(fixture.request(MultipartSourceOperation::ConfirmTerminal {
                expected: None,
                publication,
                challenge,
            }))
            .await
            .is_err(),
        "the participant that never produced C cannot supply a target vote"
    );
    let survivor_source = exact_tree(&source_root(&fixture, 2));
    assert!(!survivor_source.is_empty());
    heap_native(|| assert_generic_deletes_preserve_source(&fixture, 2)).await;
    assert_eq!(all_targets(&fixture), targets);
}

#[tokio::test]
async fn native_abort_missing_state_with_actual_owned_records_never_becomes_legacy_or_missing() {
    let fixture = heap_native(Fixture::new).await;
    let initial = std::fs::read(fixture.head(0)).expect("exact actual marked source before publication");
    let produced = heap_native(|| fixture.publish_zero_on(&[0])).await;
    produced[0]
        .1
        .verify_with_authority(&fixture.disks[0].multipart_proof_verification_context())
        .expect("the orphan input must come from an actual native publisher");
    let snapshot = heap_native(|| fixture.snapshot(0)).await;
    assert_eq!(snapshot.phase, MultipartSourcePhase::ConsumedUnsealed);
    assert!(!fixture.head(0).exists(), "the canonical source head was physically consumed");
    drop(produced);
    let source = source_root(&fixture, 0);
    let prepared = exact_tree(&source);
    assert!(
        prepared.iter().any(|(path, bytes)| {
            path.file_name().is_some_and(|name| name == "original.xl.meta") && bytes.as_ref() == Some(&initial)
        }),
        "the original private native backup must really remain"
    );
    std::fs::remove_file(source.join(".mpu-source-state")).expect("inject only a lost native state record");
    let before = all_sources(&fixture);
    let targets = all_targets(&fixture);
    let inspected = fixture.disks[0]
        .recover_multipart_source(fixture.request(MultipartSourceOperation::Inspect { lineage: None }))
        .await;
    assert!(
        matches!(inspected, Err(DiskError::ConditionalDeleteConflict)),
        "known private native evidence without its state cannot become missing or legacy: {inspected:?}"
    );
    assert_eq!(
        all_sources(&fixture),
        before,
        "readonly inspection cannot repair or erase the corrupt input"
    );
    let deleted = fixture.disks[0]
        .delete(
            RUSTFS_META_MULTIPART_BUCKET,
            &fixture.path,
            DeleteOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(deleted, Err(DiskError::ConditionalDeleteConflict)),
        "ordinary delete must preserve orphan authority: {deleted:?}"
    );
    assert_eq!(all_sources(&fixture), before);
    let deleted = fixture.disks[0]
        .delete_paths(RUSTFS_META_MULTIPART_BUCKET, std::slice::from_ref(&fixture.path))
        .await;
    assert!(
        matches!(deleted, Err(DiskError::ConditionalDeleteConflict)),
        "raw DeletePaths must preserve orphan authority: {deleted:?}"
    );
    assert_eq!(all_sources(&fixture), before);
    heap_native(|| assert_generic_deletes_preserve_source(&fixture, 0)).await;
    assert_eq!(all_sources(&fixture), before, "every exact native evidence byte must remain retryable");
    assert_eq!(all_targets(&fixture), targets, "corrupt source-state handling cannot change target bytes");
}

async fn close_actual_source_owned_by_staging(
    fixture: &Fixture,
    staging: &crate::disk::local::multipart_staging::MultipartCommitStaging,
) {
    let store = heap_native(|| fixture.disks[0].multipart_store_from_staging(&fixture.path, staging))
        .await
        .expect("reuse the real staging SourceRW owner without reacquisition");
    let expected = heap_native(|| store.load())
        .await
        .expect("load the actual writer-owned source")
        .snapshot
        .expect("the selected source actually exists");
    assert_eq!(expected.phase, MultipartSourcePhase::Active);
    assert_eq!(expected.generation.epoch, 0, "the held stage has not reserved a publication");
    assert!(expected.capability_sha256.is_none());
    let round = MultipartAbortRound {
        lineage: expected.lineage.clone(),
        generation: SourceGeneration {
            epoch: expected.generation.epoch.checked_add(1).expect("fresh closure epoch"),
            nonce: Uuid::new_v4(),
        },
        round: Uuid::new_v4(),
    };
    let response = heap_native(|| {
        fixture.disks[0].recover_multipart_abort_owned(store, MultipartSourceOperation::PrepareAbort { expected, round })
    })
    .await
    .expect("the same actual SourceRW owner can durably close its source");
    let MultipartSourceResponse::AbortPrepared(proof) = response else {
        panic!("the owned native executor must produce an actual preparation proof");
    };
    proof
        .verify_with_authority(&fixture.disks[0].multipart_proof_verification_context())
        .expect("the closure proof comes from the actual formatted disk");
    assert_eq!(proof.kind, MultipartAbortProofKind::Prepared);
    assert_eq!(proof.snapshot.physical_slot, 0);
    assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
    assert!(proof.snapshot.head_sha256.is_none());
    assert!(!fixture.head(0).exists());
}

#[tokio::test]
async fn native_abort_public_data_dir_delete_waits_for_the_actual_staging_source_writer_and_then_denies_closure() {
    use std::time::Duration;
    let fixture = heap_native(Fixture::new).await;
    for slot in 0..4 {
        heap_native(|| put_actual_predecessor(&fixture, slot)).await;
    }
    match fixture.disks[0].make_volume(RUSTFS_META_TMP_DELETED_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("prepare a real public data-dir deletion destination: {error}"),
    }
    let initial = heap_native(|| fixture.snapshot(0)).await;
    let data_path = format!("{}/{}", fixture.path, initial.lineage.original_data_dir);
    let incoming = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
    let staged = heap_native(|| fixture.disks[0].prepare_multipart_commit_staging(&fixture.path, &incoming, "bucket", None))
        .await
        .expect("hold the real pre-reservation SourceRW writer and selected physical part");
    assert!(!source_root(&fixture, 0).join(".mpu-source-state").exists());
    let active_sources = all_sources(&fixture);
    let active_targets = all_targets(&fixture);
    let mut deleted = fixture.disks[0].delete_data_dir(
        RUSTFS_META_MULTIPART_BUCKET,
        &data_path,
        DeleteOptions {
            recursive: true,
            ..Default::default()
        },
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), deleted.as_mut())
            .await
            .is_err(),
        "public DeleteDataDir must acquire SourceRW before it can remove this actual selected directory"
    );
    assert_eq!(
        all_sources(&fixture),
        active_sources,
        "the queued public API cannot outrun the held stage"
    );
    assert_eq!(all_targets(&fixture), active_targets);
    heap_native(|| close_actual_source_owned_by_staging(&fixture, &staged)).await;
    let closed_sources = all_sources(&fixture);
    let closed_targets = all_targets(&fixture);
    drop(staged);
    let outcome = tokio::time::timeout(Duration::from_secs(10), deleted.as_mut())
        .await
        .expect("public deletion settles after the physical staging owner drains");
    assert!(
        matches!(outcome, Err(DiskError::ConditionalDeleteConflict)),
        "new Aborting state must deny the queued public API: {outcome:?}"
    );
    assert_eq!(all_sources(&fixture), closed_sources, "closure evidence and selected bytes remain exact");
    assert_eq!(all_targets(&fixture), closed_targets);
}

#[tokio::test]
async fn native_abort_deferred_snapshot_release_and_settlement_reacquire_source_rw_before_cleanup() {
    use std::time::Duration;
    for settle_while_reader_retained in [false, true] {
        let fixture = heap_native(Fixture::new).await;
        for slot in 0..4 {
            heap_native(|| put_actual_predecessor(&fixture, slot)).await;
        }
        match fixture.disks[0].make_volume(RUSTFS_META_TMP_DELETED_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("prepare actual deferred cleanup destination: {error}"),
        }
        let initial = heap_native(|| fixture.snapshot(0)).await;
        let data_path = format!("{}/{}", fixture.path, initial.lineage.original_data_dir);
        let incoming = fixture.zero_info(0, Uuid::new_v4(), time::OffsetDateTime::now_utc());
        let token = fixture.disks[0]
            .acquire_snapshot_lease(RUSTFS_META_MULTIPART_BUCKET, &data_path)
            .await
            .expect("retain an actual snapshot of the selected source directory");
        assert_eq!(
            fixture.disks[0]
                .delete_data_dir(
                    RUSTFS_META_MULTIPART_BUCKET,
                    &data_path,
                    DeleteOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("the real snapshot reader defers deletion"),
            DataDirDeleteStatus::Deferred
        );
        let staged = heap_native(|| fixture.disks[0].prepare_multipart_commit_staging(&fixture.path, &incoming, "bucket", None))
            .await
            .expect("hold the actual source writer after deferral but before physical reclaim");
        assert!(!source_root(&fixture, 0).join(".mpu-source-state").exists());
        let active_sources = all_sources(&fixture);
        let active_targets = all_targets(&fixture);
        let mut reclaim = heap_native(|| async {
            if settle_while_reader_retained {
                heap_native(|| fixture.disks[0].settle_pending_snapshot_deletes(RUSTFS_META_MULTIPART_BUCKET)).await;
                Ok(())
            } else {
                fixture.disks[0]
                    .release_snapshot_lease(RUSTFS_META_MULTIPART_BUCKET, &data_path, token)
                    .await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), reclaim.as_mut())
                .await
                .is_err(),
            "both last-reader release and volume settlement must reacquire the actual SourceRW writer"
        );
        assert_eq!(
            all_sources(&fixture),
            active_sources,
            "deferred reclamation cannot bypass the physical writer"
        );
        assert_eq!(all_targets(&fixture), active_targets);
        heap_native(|| close_actual_source_owned_by_staging(&fixture, &staged)).await;
        let closed_sources = all_sources(&fixture);
        let closed_targets = all_targets(&fixture);
        drop(staged);
        let outcome = tokio::time::timeout(Duration::from_secs(10), reclaim.as_mut())
            .await
            .expect("deferred reclaim settles after the staging owner drains");
        drop(reclaim);
        if settle_while_reader_retained {
            assert_eq!(outcome, Ok(()), "the settlement API reports errors by retaining pending cleanup");
            let release = fixture.disks[0]
                .release_snapshot_lease(RUSTFS_META_MULTIPART_BUCKET, &data_path, token)
                .await;
            assert!(
                matches!(release, Err(DiskError::ConditionalDeleteConflict)),
                "retained deferred cleanup still rejects the actual Aborting source: {release:?}"
            );
        } else {
            assert!(
                matches!(outcome, Err(DiskError::ConditionalDeleteConflict)),
                "last-reader cleanup must reject the new closure: {outcome:?}"
            );
        }
        assert_eq!(
            all_sources(&fixture),
            closed_sources,
            "last-reader cleanup preserves all closure evidence bytes"
        );
        assert_eq!(all_targets(&fixture), closed_targets);
        heap_native(|| fixture.disks[0].settle_pending_snapshot_deletes(RUSTFS_META_MULTIPART_BUCKET)).await;
        assert_eq!(
            all_sources(&fixture),
            closed_sources,
            "later volume settlement also preserves the actual closed source"
        );
        assert_eq!(all_targets(&fixture), closed_targets);
    }
}
