use super::*;
use crate::disk::SnapshotLeaseToken;

async fn deferred_owned_undo() -> (Fixture, PublicationIdentity, SnapshotLeaseToken) {
    let fixture = Fixture::new().await;
    let result = fixture
        .publish_zero_on(&[0])
        .await
        .pop()
        .expect("real first-key native publication")
        .1;
    let publication = result.publication.clone();
    let path = format!("object/{}", publication.final_data_dir);
    let token = fixture.disks[0]
        .acquire_snapshot_lease("bucket", &path)
        .await
        .expect("retain the actual published DD");
    fixture.disks[0]
        .recover_multipart_source_native(fixture.request(MultipartSourceOperation::UndoAndClaim {
            expected: Box::new(result.proof.snapshot),
            publication: publication.clone(),
            capability: result.undo_capability,
            round: Fixture::round_for(&publication),
        }))
        .await
        .expect("owned Undo restores absence and defers only its exact DD");
    assert!(!fixture.disks[0].root.join("bucket/object/xl.meta").exists());
    (fixture, publication, token)
}

#[tokio::test]
async fn native_multipart_owned_undo_deferred_reclaim_rechecks_successors_references_and_ownership() {
    for changed in [
        "successor",
        "referenced",
        "child",
        "foreign-anchor",
        "missing-anchor",
        "unknown-file",
    ] {
        let (fixture, publication, token) = Box::pin(deferred_owned_undo()).await;
        let disk = &fixture.disks[0];
        let path = format!("object/{}", publication.final_data_dir);
        let data = disk.io_get_object_path("bucket", &path).expect("actual failed DD");
        let part = std::fs::read(data.join("part.1")).expect("real selected shard before deferred callback");
        let sidecar = std::fs::read(data.join("part.1.meta")).expect("real selected generation before callback");
        let mut successor = None;
        let mut child = None;
        match changed {
            "successor" | "referenced" => {
                let referenced = (changed == "referenced").then_some(publication.final_data_dir);
                let info = super::super::super::test::test_file_info(
                    "object",
                    Uuid::new_v4(),
                    referenced,
                    Some(bytes::Bytes::from_static(b"newer PUT body")),
                );
                disk.rename_data(RUSTFS_META_TMP_BUCKET, "real-newer-put", info, "bucket", "object")
                    .await
                    .expect("real ordinary PUT while old failed DD is snapshot-protected");
                let raw = std::fs::read(disk.io_get_object_path("bucket", "object/xl.meta").expect("target FI"))
                    .expect("actual successor FI");
                let refs = FileMeta::load(&raw)
                    .expect("successor metadata")
                    .get_data_dirs()
                    .expect("all successor versions");
                assert_eq!(refs.contains(&Some(publication.final_data_dir)), changed == "referenced");
                successor = Some(raw);
            }
            "child" => {
                let child_path = format!("{path}/child");
                let info = super::super::super::test::test_file_info(
                    "child",
                    Uuid::nil(),
                    None,
                    Some(bytes::Bytes::from_static(b"child PUT body")),
                );
                disk.rename_data(RUSTFS_META_TMP_BUCKET, "real-child-put", info, "bucket", &child_path)
                    .await
                    .expect("real child PUT under the UUID-shaped S3 prefix");
                child = Some(std::fs::read(data.join("child/xl.meta")).expect("actual child metadata"));
            }
            "foreign-anchor" => {
                let mut anchor: UntrustedAnchorBlob =
                    rmp_serde::from_slice(&std::fs::read(data.join(".mpu-source-retirement")).unwrap()).unwrap();
                anchor.disk_id = Uuid::new_v4();
                std::fs::write(data.join(".mpu-source-retirement"), rmp_serde::to_vec_named(&anchor).unwrap()).unwrap();
            }
            "missing-anchor" => {
                std::fs::remove_file(data.join(".mpu-source-retirement")).unwrap();
            }
            "unknown-file" => {
                std::fs::write(data.join("unknown"), b"protected").unwrap();
            }
            _ => unreachable!(),
        }
        let anchor = std::fs::read(data.join(".mpu-source-retirement")).ok();
        let reclaimed = disk.release_snapshot_lease("bucket", &path, token).await;
        if changed == "successor" {
            reclaimed.expect("unreferenced owned failed DD is reclaimable beside a newer real PUT");
            assert!(!data.exists());
        } else {
            assert!(reclaimed.is_err(), "{changed} must reject the actual callback before deleting payload");
            assert_eq!(std::fs::read(data.join("part.1")).expect("protected actual shard"), part);
            assert_eq!(std::fs::read(data.join("part.1.meta")).expect("protected actual sidecar"), sidecar);
            assert_eq!(std::fs::read(data.join(".mpu-source-retirement")).ok(), anchor);
        }
        if let Some(raw) = successor {
            assert_eq!(std::fs::read(disk.io_get_object_path("bucket", "object/xl.meta").unwrap()).unwrap(), raw);
        }
        if let Some(raw) = child {
            assert_eq!(std::fs::read(data.join("child/xl.meta")).unwrap(), raw);
        }
        if changed == "unknown-file" {
            assert_eq!(std::fs::read(data.join("unknown")).unwrap(), b"protected");
        }
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn native_multipart_owned_undo_reclaim_keeps_anchor_until_payload_barrier_and_retries_empty_dd() {
    use crate::disk::os::prepared_publication_test_hooks as hooks;
    for after_anchor in [false, true] {
        let (fixture, publication, token) = Box::pin(deferred_owned_undo()).await;
        let disk = &fixture.disks[0];
        let path = format!("object/{}", publication.final_data_dir);
        let data = disk.io_get_object_path("bucket", &path).expect("actual DD barrier path");
        let anchor_path = data.join(".mpu-source-retirement");
        let anchor = std::fs::read(&anchor_path).expect("actual owner anchor");
        let _hook = if after_anchor {
            let directory = data.clone();
            Some(hooks::install_at(hooks::Stage::Remove, &anchor_path, move || {
                os::fsync_dir_recorder::set_failure(&directory, std::io::ErrorKind::Other);
            }))
        } else {
            os::fsync_dir_recorder::set_failure(&data, std::io::ErrorKind::Other);
            None
        };
        assert!(disk.release_snapshot_lease("bucket", &path, token).await.is_err());
        assert!(
            os::fsync_dir_recorder::take_failure(&data).is_none(),
            "the real selected DD barrier must fail"
        );
        assert!(!data.join("part.1").exists() && !data.join("part.1.meta").exists());
        if after_anchor {
            assert!(
                std::fs::read_dir(&data).unwrap().next().is_none(),
                "only an empty DD remains after the final anchor unlink"
            );
        } else {
            assert_eq!(std::fs::read(&anchor_path).expect("failed payload barrier retains authority"), anchor);
        }
        let retry = disk
            .acquire_snapshot_lease("bucket", &path)
            .await
            .expect("finished failed callback releases deleting");
        disk.release_snapshot_lease("bucket", &path, retry)
            .await
            .expect("same privately owned pending operation retries");
        assert!(!data.exists());
        disk.delete_volume("bucket", false)
            .await
            .expect("no owned native payload residue blocks empty bucket deletion");
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn native_multipart_owned_undo_cancelled_snapshot_release_keeps_physical_delete_claim() {
    use crate::disk::os::prepared_publication_test_hooks as hooks;
    let (fixture, publication, token) = Box::pin(deferred_owned_undo()).await;
    let disk = fixture.disks[0].clone();
    let path = format!("object/{}", publication.final_data_dir);
    let data = disk.io_get_object_path("bucket", &path).expect("actual callback DD");
    let (entered, receive_entered) = std::sync::mpsc::channel();
    let (release, receive_release) = std::sync::mpsc::channel();
    let _hook = hooks::install_at(hooks::Stage::Remove, &data.join("part.1"), move || {
        entered.send(()).expect("actual unlink callback entered");
        receive_release
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("release physical unlink");
    });
    let owned_disk = disk.clone();
    let owned_path = path.clone();
    let caller = tokio::spawn(async move { owned_disk.release_snapshot_lease("bucket", &owned_path, token).await });
    tokio::task::spawn_blocking(move || receive_entered.recv_timeout(std::time::Duration::from_secs(5)))
        .await
        .expect("hook waiter joins")
        .expect("real unlink is blocked before cancelling its waiter");
    caller.abort();
    assert!(caller.await.expect_err("cancelled waiter").is_cancelled());
    let key = super::super::super::SnapshotLeaseKey {
        volume: "bucket".to_owned(),
        path: path.clone(),
    };
    assert!(
        disk.snapshot_leases
            .lock()
            .entries
            .get(&key)
            .is_some_and(|entry| entry.deleting),
        "only the actual physical callback may release the deleting claim"
    );
    release.send(()).expect("finish actual unlink after waiter cancellation");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if !data.exists() && !disk.snapshot_leases.lock().entries.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("physical callback finishes and clears its owned registry claim");
    assert!(!data.exists());
    assert!(matches!(disk.acquire_snapshot_lease("bucket", &path).await, Err(DiskError::FileNotFound)));
}

#[tokio::test]
async fn native_multipart_owned_undo_rpc_retries_a_failed_physical_reclaim() {
    let fixture = Fixture::new().await;
    let result = fixture
        .publish_zero_on(&[0])
        .await
        .pop()
        .expect("actual failed-owner publication")
        .1;
    let publication = result.publication.clone();
    let round = Fixture::round_for(&publication);
    let expected = result.proof.snapshot.clone();
    let capability_wire = rmp_serde::to_vec_named(&result.undo_capability).expect("original opaque native cap");
    let path = format!("object/{}", publication.final_data_dir);
    let data = fixture.disks[0].io_get_object_path("bucket", &path).expect("actual final DD");
    let anchor = std::fs::read(data.join(".mpu-source-retirement")).expect("real native anchor");
    os::fsync_dir_recorder::set_failure(&data, std::io::ErrorKind::Other);
    assert!(
        fixture.disks[0]
            .recover_multipart_source_native(fixture.request(MultipartSourceOperation::UndoAndClaim {
                expected: Box::new(expected.clone()),
                publication: publication.clone(),
                capability: result.undo_capability,
                round: round.clone(),
            }))
            .await
            .is_err(),
        "a real reclaim barrier failure cannot become a successful first RPC"
    );
    assert!(
        os::fsync_dir_recorder::take_failure(&data).is_none(),
        "the selected physical reclaim barrier was reached"
    );
    let after_failure = fixture.snapshot(0).await;
    assert_eq!(after_failure.phase, MultipartSourcePhase::Recovering);
    assert!(after_failure.head_sha256.is_some() && after_failure.capability_sha256.is_none());
    assert!(!fixture.disks[0].root.join("bucket/object/xl.meta").exists());
    assert_eq!(
        std::fs::read(data.join(".mpu-source-retirement")).expect("failed payload barrier retains exact anchor"),
        anchor
    );
    let capability = rmp_serde::from_slice(&capability_wire).expect("same opaque capability on RPC replay");
    let response = fixture.disks[0]
        .recover_multipart_source_native(fixture.request(MultipartSourceOperation::UndoAndClaim {
            expected: Box::new(expected),
            publication,
            capability,
            round,
        }))
        .await
        .expect("same UndoAndClaim RPC resumes its qualified reclaim before returning Claimed");
    let MultipartSourceResponse::Claimed(proof) = response else {
        panic!("native owned RPC replay must return its durable claim");
    };
    assert_eq!(proof.snapshot, after_failure);
    assert!(
        !data.exists(),
        "the early same-claim RPC branch must actually finish its interrupted reclaim"
    );
    fixture.disks[0]
        .delete_volume("bucket", false)
        .await
        .expect("RPC replay leaves an actually empty bucket");
}

#[tokio::test]
async fn native_multipart_owned_undo_pending_proof_survives_both_generic_request_orders() {
    for ordinary_first in [false, true] {
        let fixture = Fixture::new().await;
        let result = fixture.publish_zero_on(&[0]).await.pop().expect("actual native producer").1;
        let publication = result.publication.clone();
        let path = format!("object/{}", publication.final_data_dir);
        let disk = &fixture.disks[0];
        let data = disk.io_get_object_path("bucket", &path).expect("actual failed native DD");
        let token = disk
            .acquire_snapshot_lease("bucket", &path)
            .await
            .expect("real snapshot protects this DD");
        let ordinary = || DeleteOptions {
            orphan_data_dir: true,
            ..Default::default()
        };
        if ordinary_first {
            assert_eq!(
                disk.delete_data_dir("bucket", &path, ordinary())
                    .await
                    .expect("generic request parks behind actual snapshot"),
                crate::disk::DataDirDeleteStatus::Deferred
            );
        }
        disk.recover_multipart_source_native(fixture.request(MultipartSourceOperation::UndoAndClaim {
            expected: Box::new(result.proof.snapshot),
            publication: publication.clone(),
            capability: result.undo_capability,
            round: Fixture::round_for(&publication),
        }))
        .await
        .expect("actual owned Undo qualifies missing-FI cleanup");
        if !ordinary_first {
            assert_eq!(
                disk.delete_data_dir("bucket", &path, ordinary())
                    .await
                    .expect("later generic request remains deferred"),
                crate::disk::DataDirDeleteStatus::Deferred
            );
        }
        let key = super::super::super::SnapshotLeaseKey {
            volume: "bucket".to_owned(),
            path: path.clone(),
        };
        assert!(
            matches!(
                disk.snapshot_leases
                    .lock()
                    .entries
                    .get(&key)
                    .and_then(|entry| entry.pending_delete.as_ref()),
                Some(super::super::super::SnapshotPendingDelete::OwnedMultipart(_))
            ),
            "the exact native proof cannot be lost to generic options, ordinary_first={ordinary_first}"
        );
        assert!(data.join(".mpu-source-retirement").exists());
        disk.release_snapshot_lease("bucket", &path, token)
            .await
            .expect("release replays the qualified native owner");
        assert!(!data.exists(), "both request orders must reclaim the owned DD after its snapshot drains");
        disk.delete_volume("bucket", false)
            .await
            .expect("no generic pending request strands an owned absent-target DD");
    }
}
