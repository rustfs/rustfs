// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::object_api::{ObjectOptions, PutObjReader, ShardIntegrityWriteMode};
use crate::storage_api_contracts::{
    bucket::{BucketOperations, MakeBucketOptions},
    object::{ObjectIO, ObjectOperations},
};
use tokio::io::AsyncReadExt;

async fn fixture() -> (Vec<tempfile::TempDir>, Arc<crate::store::ECStore>, String) {
    let (dirs, store) = crate::services::rebalance::test_store_with_persisted_rebalance_meta(Default::default()).await;
    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = format!("integrity-{}", Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("fixture bucket");
    (dirs, store, bucket)
}

fn request(mode: JobMode, digest: String) -> JobRequest {
    JobRequest {
        mode,
        items: vec![ItemRequest {
            key: "source".into(),
            version_id: Some("null".into()),
            expected_sha256: Some(digest),
            target_key: (mode == JobMode::Migrate).then(|| "target".into()),
        }],
        bytes_per_second: 1024 * 1024,
        max_object_bytes: 16 * 1024 * 1024,
    }
}

async fn finish(store: Arc<crate::store::ECStore>, bucket: &str, id: Uuid) -> Job {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let job = get_job(store.clone(), bucket, id).await.expect("durable job");
            if job.state != JobState::Running {
                return job;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("job must finish")
}

#[tokio::test]
#[serial_test::serial(shard_integrity_rollout)]
async fn audit_migration_and_ack_recovery_preserve_source_and_reject_conflicts() {
    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_SHARD_INTEGRITY_WRITE, Some("true")),
            (rustfs_config::ENV_SHARD_INTEGRITY_FLEET_CONFIRMED, Some("true")),
        ],
        async {
            let (_dirs, store, bucket) = fixture().await;
            let payload = vec![42; 256 * 1024 + 17];
            let expected = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &payload)
                .expect("digest")
                .encoded;
            store
                .put_object(
                    &bucket,
                    "source",
                    &mut PutObjReader::from_vec(payload.clone()),
                    &ObjectOptions {
                        user_defined: [
                            ("cache-control".into(), "max-age=300".into()),
                            ("content-disposition".into(), "attachment; filename=sample.bin".into()),
                            ("expires".into(), "2030-01-01T00:00:00Z".into()),
                            (rustfs_utils::http::headers::AMZ_OBJECT_TAGGING.into(), "kind=archive".into()),
                        ]
                        .into(),
                        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("source");
            let source = store
                .get_object_info(&bucket, "source", &ObjectOptions::default())
                .await
                .expect("source metadata");
            assert_eq!(source.user_tags.as_str(), "kind=archive", "source fixture tags");
            let before = model::fingerprint(&source);
            let page = inventory(store.clone(), &bucket, "", None, None, 1).await.expect("inventory");
            assert_eq!(page.items.len(), 1);
            assert_eq!(page.items[0].protection, Protection::Legacy);
            assert_eq!(page.items[0].version_id, "null", "inventory must round-trip an explicit null selector");
            let audit = create_job(store.clone(), &bucket, request(JobMode::Audit, expected.clone()))
                .await
                .expect("create audit");
            resume_job(store.clone(), &bucket, audit.id).await.expect("resume audit");
            let audited = finish(store.clone(), &bucket, audit.id).await;
            assert_eq!(audited.state, JobState::Complete, "{audited:?}");
            assert_eq!(audited.results[0].state, ItemState::Verified);

            let migration = create_job(store.clone(), &bucket, request(JobMode::Migrate, expected.clone()))
                .await
                .expect("create migration");
            resume_job(store.clone(), &bucket, migration.id)
                .await
                .expect("resume migration");
            let migrated = finish(store.clone(), &bucket, migration.id).await;
            assert_eq!(migrated.state, JobState::Complete, "{migrated:?}");
            assert_eq!(migrated.results[0].state, ItemState::Migrated);
            let mut reader = store
                .get_object_reader(&bucket, "target", None, Default::default(), &ObjectOptions::default())
                .await
                .expect("target");
            assert_eq!(model::protection(&reader.object_info), Protection::IndependentCommitment);
            assert_eq!(reader.object_info.user_tags.as_str(), "kind=archive");
            assert_eq!(reader.object_info.expires, source.expires);
            assert_eq!(
                reader.object_info.user_defined.get("cache-control"),
                source.user_defined.get("cache-control")
            );
            assert_eq!(
                reader.object_info.user_defined.get("content-disposition"),
                source.user_defined.get("content-disposition")
            );
            let mut actual = Vec::new();
            reader.stream.read_to_end(&mut actual).await.expect("target content");
            assert_eq!(actual, payload);
            let after = store
                .get_object_info(&bucket, "source", &ObjectOptions::default())
                .await
                .expect("source after");
            assert_eq!(model::fingerprint(&after), before);

            // Simulate a lost completion checkpoint after the create-only PUT committed.
            let (mut replay, etag) = load(&store, &bucket, migration.id).await.expect("load receipt");
            replay.state = JobState::Paused;
            replay.results[0].state = ItemState::Prepared;
            replay.revision += 1;
            save(&store, &replay, Some(&etag), None)
                .await
                .expect("lost acknowledgement fixture");
            resume_job(store.clone(), &bucket, replay.id)
                .await
                .expect("reconcile after restart");
            let reconciled = finish(store.clone(), &bucket, replay.id).await;
            assert_eq!(reconciled.results[0].state, ItemState::Migrated, "{reconciled:?}");

            let conflict = create_job(store.clone(), &bucket, request(JobMode::Migrate, expected))
                .await
                .expect("conflicting job");
            resume_job(store.clone(), &bucket, conflict.id)
                .await
                .expect("resume conflict");
            let conflict = finish(store.clone(), &bucket, conflict.id).await;
            assert_eq!(conflict.results[0].state, ItemState::Conflict, "{conflict:?}");
            let target = store
                .get_object_info(&bucket, "target", &ObjectOptions::default())
                .await
                .expect("unchanged target");
            assert_eq!(target.data_dir, reader.object_info.data_dir);
        },
    )
    .await;
}

#[tokio::test]
async fn mismatch_never_publishes_and_paused_job_survives_reload() {
    let (_dirs, store, bucket) = fixture().await;
    store
        .put_object(
            &bucket,
            "source",
            &mut PutObjReader::from_vec(b"actual".to_vec()),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                ..Default::default()
            },
        )
        .await
        .expect("source");
    let wrong = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, b"different")
        .expect("digest")
        .encoded;
    let job = create_job(store.clone(), &bucket, request(JobMode::Audit, wrong))
        .await
        .expect("job");
    assert_eq!(get_job(store.clone(), &bucket, job.id).await.expect("reload").state, JobState::Paused);
    resume_job(store.clone(), &bucket, job.id).await.expect("resume");
    let job = finish(store.clone(), &bucket, job.id).await;
    assert_eq!(job.results[0].state, ItemState::Mismatch, "{job:?}");
    assert!(
        store
            .get_object_info(&bucket, "target", &ObjectOptions::default())
            .await
            .is_err()
    );
}

#[test]
fn fingerprint_is_independent_of_metadata_insertion_order() {
    let mut a = crate::object_api::ObjectInfo::default();
    let mut first = std::collections::HashMap::new();
    first.insert("a".into(), "one".into());
    first.insert("b".into(), "two".into());
    a.user_defined = Arc::new(first);
    let mut b = a.clone();
    let mut second = std::collections::HashMap::new();
    second.insert("b".into(), "two".into());
    second.insert("a".into(), "one".into());
    b.user_defined = Arc::new(second);
    assert_eq!(model::fingerprint(&a), model::fingerprint(&b));
    b.data_dir = Some(Uuid::new_v4());
    assert_ne!(model::fingerprint(&a), model::fingerprint(&b));
}

#[test]
fn fingerprint_is_independent_of_part_checksum_insertion_order() {
    let mut checksums = [
        ("SHA256", "sha256"),
        ("SHA1", "sha1"),
        ("CRC32", "crc32"),
        ("CRC32C", "crc32c"),
    ];
    let a = crate::object_api::ObjectInfo {
        parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
            checksums: Some(checksums.map(|(key, value)| (key.to_string(), value.to_string())).into()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let mut b = a.clone();
    let expected = model::fingerprint(&a);
    for _ in 0..checksums.len() {
        checksums.rotate_left(1);
        Arc::make_mut(&mut b.parts)[0].checksums =
            Some(checksums.map(|(key, value)| (key.to_string(), value.to_string())).into());
        assert_eq!(expected, model::fingerprint(&b));
    }
    Arc::make_mut(&mut b.parts)[0]
        .checksums
        .as_mut()
        .expect("part checksums")
        .insert("SHA256".into(), "changed".into());
    assert_ne!(expected, model::fingerprint(&b));
}

#[test]
fn rejects_migration_into_a_source_key() {
    let request = JobRequest {
        mode: JobMode::Migrate,
        items: vec![ItemRequest {
            key: "source".into(),
            version_id: None,
            expected_sha256: None,
            target_key: Some("source".into()),
        }],
        bytes_per_second: 1024 * 1024,
        max_object_bytes: 1024,
    };
    assert!(request.validate().is_err());
}

#[test]
fn rejects_truncated_checksum_suffix_as_migration_evidence() {
    let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, b"payload").expect("checksum");
    let mut info = crate::object_api::ObjectInfo {
        checksum: Some(checksum.to_bytes(&[])),
        ..Default::default()
    };
    assert_eq!(model::stored_sha256(&info), Some(checksum.encoded));
    let mut bytes = info.checksum.take().expect("bytes").to_vec();
    bytes.push(0x80);
    info.checksum = Some(bytes.into());
    assert!(model::stored_sha256(&info).is_none());
}

#[tokio::test]
async fn unavailable_source_can_resume_and_cancel_intent_cannot_restart() {
    let (_dirs, store, bucket) = fixture().await;
    let payload = b"recovered".to_vec();
    let digest = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &payload)
        .expect("digest")
        .encoded;
    let job = create_job(store.clone(), &bucket, request(JobMode::Audit, digest))
        .await
        .expect("create");
    resume_job(store.clone(), &bucket, job.id).await.expect("start");
    let failed = finish(store.clone(), &bucket, job.id).await;
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.results[0].state, ItemState::Unavailable);
    store
        .put_object(
            &bucket,
            "source",
            &mut PutObjReader::from_vec(payload),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("recovered source");
    resume_job(store.clone(), &bucket, job.id).await.expect("retry");
    let completed = finish(store.clone(), &bucket, job.id).await;
    assert_eq!(completed.state, JobState::Complete, "{completed:?}");
    assert_eq!(completed.results[0].state, ItemState::Verified);

    // A process restart may leave the durable cancel intent without a live worker.
    let (mut interrupted, etag) = load(&store, &bucket, job.id).await.expect("load");
    interrupted.state = JobState::CancelRequested;
    interrupted.revision += 1;
    save(&store, &interrupted, Some(&etag), None).await.expect("persist intent");
    assert_eq!(
        control_job(store.clone(), &bucket, job.id, false).await.expect("pause").state,
        JobState::CancelRequested
    );
    assert_eq!(
        resume_job(store.clone(), &bucket, job.id)
            .await
            .expect("recover cancel")
            .state,
        JobState::Cancelled
    );
}

#[tokio::test]
async fn pause_and_cancel_are_durable_during_content_read() {
    let (_dirs, store, bucket) = fixture().await;
    let payload = vec![7; 512 * 1024];
    let digest = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &payload)
        .expect("digest")
        .encoded;
    store
        .put_object(
            &bucket,
            "source",
            &mut PutObjReader::from_vec(payload),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("source");
    let mut req = request(JobMode::Audit, digest);
    req.bytes_per_second = 64 * 1024;
    let job = create_job(store.clone(), &bucket, req).await.expect("create");
    resume_job(store.clone(), &bucket, job.id).await.expect("start");
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if get_job(store.clone(), &bucket, job.id).await.expect("status").results[0].state == ItemState::Prepared {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("prepared");
    let intent = control_job(store.clone(), &bucket, job.id, false).await.expect("pause");
    assert_eq!(intent.state, JobState::PauseRequested);
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let paused = get_job(store.clone(), &bucket, job.id).await.expect("status");
            if paused.state == JobState::Paused {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("pause acknowledged");
    assert_eq!(
        control_job(store.clone(), &bucket, job.id, true).await.expect("cancel").state,
        JobState::Cancelled
    );
    assert_eq!(
        resume_job(store.clone(), &bucket, job.id)
            .await
            .expect("terminal resume")
            .state,
        JobState::Cancelled
    );
}
