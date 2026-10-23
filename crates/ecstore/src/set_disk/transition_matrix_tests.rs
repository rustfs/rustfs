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

use super::{
    Arc, Duration, GetObjectMetadataCacheKey, HeaderMap, MakeBucketOptions, ObjectOptions, OffsetDateTime, PutObjReader,
    SetDisks, Uuid,
};
use crate::bucket::lifecycle::lifecycle::{TRANSITION_COMPLETE, TRANSITION_PENDING, TransitionOptions, expected_expiry_time};
use crate::ecstore_validation_blackbox::make_local_set_disks;
use crate::object_api::WriteCompletion;
use crate::services::tier::test_util::register_mock_tier;
use crate::storage_api_contracts::bucket::BucketOperations;
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
use rustfs_filemeta::{RestoreStatusOps as _, parse_restore_obj_status};
use tokio::io::AsyncReadExt;

async fn prime_metadata_generation(set_disks: &SetDisks, bucket: &str, object: &str) -> GetObjectMetadataCacheKey {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            set_disks
                .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
                .await
                .expect("object metadata should resolve");
            let generation = set_disks
                .get_object_metadata_cache_generation(bucket, object)
                .expect("metadata generation should be active");
            let key = GetObjectMetadataCacheKey::new(bucket, object, generation);
            if set_disks.get_object_metadata_cache.get(&key).await.is_some() {
                return key;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("metadata read should publish the generation under test")
}

async fn assert_generation_reclaimed(set_disks: &SetDisks, key: &GetObjectMetadataCacheKey) {
    set_disks.get_object_metadata_cache.run_pending_tasks().await;
    assert!(
        set_disks.get_object_metadata_cache.get(key).await.is_none(),
        "metadata mutation must physically reclaim the prior generation"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn transition_and_restore_reclaim_prior_metadata_generations() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "transition-restore-generation-bucket";
    let object = "object.bin";
    let payload = vec![0x5au8; 1024 * 1024];
    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(payload.clone());
    // Cache priming must not race a quorum-acknowledged PUT's remaining rename tail.
    let original = set_disks
        .put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written");
    let source_generation = prime_metadata_generation(&set_disks, bucket, object).await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&set_disks.instance_ctx().tier_config_mgr(), &tier_name).await;
    let transition_opts = ObjectOptions {
        transition: TransitionOptions {
            status: TRANSITION_PENDING.to_string(),
            tier: tier_name,
            etag: original.etag.clone().expect("source ETag should be present"),
            ..Default::default()
        },
        version_id: original.version_id.map(|version| version.to_string()),
        mod_time: original.mod_time,
        ..Default::default()
    };
    set_disks
        .transition_object(bucket, object, &transition_opts)
        .await
        .expect("transition should succeed");
    assert_generation_reclaimed(&set_disks, &source_generation).await;

    let transitioned_generation = prime_metadata_generation(&set_disks, bucket, object).await;
    let mut restore_opts = ObjectOptions::default();
    restore_opts.transition.restore_request.days = Some(1);
    let restore_started = OffsetDateTime::now_utc();
    let expiry_from_restore_start = temp_env::async_with_vars(
        [
            ("RUSTFS_ILM_DEBUG_DAY_SECS", Some("1")),
            ("RUSTFS_ILM_PROCESS_TIME", Some("1")),
        ],
        async {
            let expiry_from_restore_start = expected_expiry_time(restore_started, 1);
            let get_barrier = backend.arm_get_barrier().await;
            let restore_set = Arc::clone(&set_disks);
            let restore =
                tokio::spawn(async move { restore_set.restore_transitioned_object(bucket, object, &restore_opts).await });
            get_barrier.wait_until_paused().await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if expected_expiry_time(OffsetDateTime::now_utc(), 1) > expiry_from_restore_start {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("test clock should cross the next accelerated lifecycle boundary");
            get_barrier.release();
            restore
                .await
                .expect("restore task should join")
                .expect("restore should succeed");
            expiry_from_restore_start
        },
    )
    .await;
    assert_generation_reclaimed(&set_disks, &transitioned_generation).await;
    assert_eq!(backend.get_count().await, 1, "restore should read the remote candidate exactly once");

    let restored_info = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("restored object metadata should be readable");
    let restore_status = parse_restore_obj_status(
        restored_info
            .user_defined
            .get(rustfs_filemeta::metadata_keys::RESTORE)
            .expect("completed restore header should be present"),
    )
    .expect("completed restore header should parse");
    assert!(
        restore_status.expiry().expect("completed restore should have an expiry") > expiry_from_restore_start,
        "restore expiry must be based on completion, not the time the remote copy started"
    );

    let mut restored = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("restored object should be readable")
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("restored body should drain");
    assert_eq!(restored, payload);
    assert_eq!(backend.get_count().await, 1, "restored GET should use the local copy");
}

#[tokio::test]
#[serial_test::serial]
async fn prepared_snapshot_transition_duplicate_and_late_get_use_committed_remote_generation() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "transition-prepared-combo-bucket";
    let object = "object.bin";
    let payload = b"prepared metadata must not revive the released transition source".repeat(32_768);
    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(payload.clone());
    // Cache priming must not race a quorum-acknowledged PUT's remaining rename tail.
    let original = set_disks
        .put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written");

    let stale_generation = prime_metadata_generation(&set_disks, bucket, object).await;
    let stale_prepared = set_disks
        .prepare_get_object_metadata(bucket, object, &ObjectOptions::default())
        .await
        .expect("prepared metadata snapshot should resolve before transition");
    assert_ne!(
        stale_prepared.object_info().transitioned_object.status,
        TRANSITION_COMPLETE,
        "prepared snapshot must represent the pre-transition local source"
    );

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&set_disks.instance_ctx().tier_config_mgr(), &tier_name).await;
    let transition_opts = ObjectOptions {
        transition: TransitionOptions {
            status: TRANSITION_PENDING.to_string(),
            tier: tier_name,
            etag: original.etag.clone().expect("source ETag should be present"),
            ..Default::default()
        },
        version_id: original.version_id.map(|version| version.to_string()),
        mod_time: original.mod_time,
        ..Default::default()
    };

    set_disks
        .transition_object(bucket, object, &transition_opts)
        .await
        .expect("first transition should commit");
    assert_generation_reclaimed(&set_disks, &stale_generation).await;
    assert_eq!(backend.put_count().await, 1, "the winning transition should upload one remote candidate");
    let put_versions = backend.put_versions().await;
    assert_eq!(put_versions.len(), 1, "the winner should expose exactly one remote object/version");
    let remote_object = put_versions[0].0.clone();
    assert_eq!(
        backend
            .bytes(&remote_object)
            .await
            .expect("winning remote object should remain stored"),
        payload,
        "the committed remote candidate must contain the exact source body"
    );

    set_disks
        .transition_object(bucket, object, &transition_opts)
        .await
        .expect("duplicate transition should observe the committed transition and return success");
    assert_eq!(
        backend.put_count().await,
        1,
        "duplicate transition must not stream the already-released local source into a second remote PUT"
    );
    assert_eq!(
        backend.remove_count().await,
        0,
        "committed transition data must not be cleaned up by the duplicate"
    );

    let info = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("transitioned metadata should be visible after duplicate transition");
    assert_eq!(info.transitioned_object.status, TRANSITION_COMPLETE);
    assert_eq!(info.transitioned_object.name, remote_object);

    let mut restored = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("late GET should open the committed transitioned object")
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("late GET should drain the remote body");
    assert_eq!(restored, payload, "late GET must read the committed remote body byte-for-byte");
    assert_eq!(
        backend.get_count().await,
        1,
        "late GET must route to the remote generation instead of reopening the released local source"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn single_part_restore_copyback_stays_pending_before_finalization() {
    restore_copyback_stays_pending_before_finalization(false).await;
}

#[tokio::test]
#[serial_test::serial]
async fn multipart_restore_copyback_stays_pending_before_finalization() {
    restore_copyback_stays_pending_before_finalization(true).await;
}

async fn restore_copyback_stays_pending_before_finalization(multipart: bool) {
    use crate::bucket::lifecycle::bucket_lifecycle_ops::LifecycleOps as _;
    use crate::set_disk::replication::RestoreFinalizeBarrier;
    use crate::storage_api_contracts::multipart::{CompletePart, MultipartOperations as _};
    use rustfs_gateway_types::persistence::PersistedLifecycleConfiguration;
    use rustfs_lifecycle::Lifecycle as _;

    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "restore-before-finalization-bucket";
    let object = "object.bin";
    let payload = vec![0x5au8; 6 * 1024 * 1024];
    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let write_opts = ObjectOptions {
        write_completion: WriteCompletion::TailDrained,
        ..Default::default()
    };
    let original = if multipart {
        let upload = set_disks
            .new_multipart_upload(bucket, object, &write_opts)
            .await
            .expect("source multipart upload should be created");
        let mut parts = Vec::new();
        for (index, bytes) in payload.chunks(5 * 1024 * 1024).enumerate() {
            let mut reader = PutObjReader::from_vec(bytes.to_vec());
            let part = set_disks
                .put_object_part(bucket, object, &upload.upload_id, index + 1, &mut reader, &write_opts)
                .await
                .expect("source part should be written");
            parts.push(CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, parts, &write_opts)
            .await
            .expect("source multipart upload should complete")
    } else {
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &write_opts)
            .await
            .expect("source object should be written")
    };
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&set_disks.instance_ctx().tier_config_mgr(), &tier_name).await;
    set_disks
        .transition_object(
            bucket,
            object,
            &ObjectOptions {
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: tier_name.clone(),
                    etag: original.etag.clone().expect("source ETag should be present"),
                    ..Default::default()
                },
                version_id: original.version_id.map(|version| version.to_string()),
                mod_time: original.mod_time,
                ..Default::default()
            },
        )
        .await
        .expect("source should transition before restore");

    let operation_id = Uuid::new_v4().to_string();
    let mut operation_metadata = std::collections::HashMap::new();
    rustfs_utils::http::metadata_compat::insert_str(
        &mut operation_metadata,
        rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
        operation_id.clone(),
    );
    rustfs_utils::http::metadata_compat::insert_str(
        &mut operation_metadata,
        rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_WORKER_LOCK,
        rustfs_utils::http::metadata_compat::RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
    );
    operation_metadata.insert(
        rustfs_filemeta::metadata_keys::RESTORE.to_string(),
        "ongoing-request=\"true\"".to_string(),
    );
    set_disks
        .put_object_metadata(
            bucket,
            object,
            &ObjectOptions {
                eval_metadata: Some(operation_metadata.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("the restore operation generation should be installed");
    let barrier = RestoreFinalizeBarrier::install_before_lock(bucket, object);
    let restore_set = Arc::clone(&set_disks);
    let restore = tokio::spawn(async move {
        let mut opts = ObjectOptions {
            user_defined: operation_metadata,
            ..Default::default()
        };
        opts.transition.restore_request.days = Some(1);
        restore_set.restore_transitioned_object(bucket, object, &opts).await
    });
    tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
        .await
        .expect("copy-back must reach the gap before finalization acquires its lock");
    let pending = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("HEAD must remain available before finalization");
    let status = parse_restore_obj_status(
        pending
            .user_defined
            .get(rustfs_filemeta::metadata_keys::RESTORE)
            .expect("copy-back must retain restore metadata"),
    )
    .expect("pending restore status should parse");
    assert_eq!(
        pending.transitioned_object.status, TRANSITION_COMPLETE,
        "copy-back must retain the remote identity needed by recovery and cleanup"
    );
    backend.set_unreachable(true).await;
    let mut restored_before_finalization = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("pending copy-back must be readable locally before metadata finalization")
        .stream
        .read_to_end(&mut restored_before_finalization)
        .await
        .expect("pending local body should drain");
    assert_eq!(restored_before_finalization, payload, "pending local copy must preserve every byte");
    set_disks
        .transition_object(
            bucket,
            object,
            &ObjectOptions {
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: pending.transitioned_object.tier.clone(),
                    etag: pending.etag.clone().expect("pending ETag should be present"),
                    ..Default::default()
                },
                version_id: pending.version_id.map(|version| version.to_string()),
                mod_time: pending.mod_time,
                ..Default::default()
            },
        )
        .await
        .expect("a queued transition must skip the pending restore copy-back");
    let after_queued_transition = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("restore metadata should remain readable after a queued transition");
    assert_eq!(
        after_queued_transition.transitioned_object.status, TRANSITION_COMPLETE,
        "queued transition must not remove local copy-back data before finalization"
    );
    // Evaluate beyond any pre-copy deadline without sleeping or advancing the
    // process clock. Pending copy-back must never become a restore-expiry candidate.
    let lifecycle = PersistedLifecycleConfiguration::default();
    let expiry_evaluation_time = OffsetDateTime::now_utc() + time::Duration::days(2);
    let action = lifecycle
        .eval_inner(&pending.to_lifecycle_opts(), expiry_evaluation_time, 0)
        .await
        .action;

    barrier.release();
    tokio::time::timeout(Duration::from_secs(30), restore)
        .await
        .expect("released restore should complete promptly")
        .expect("restore task should join")
        .expect("copy-back should finalize successfully");

    assert!(status.on_going(), "copy-back must not advertise completion before metadata finalization");
    assert!(status.expiry().is_none(), "pending copy-back must not expose a completed expiry");
    assert!(
        !action.delete_restored(),
        "scanner must not reclaim copy-back before metadata finalization"
    );

    for (suffix, expected) in [
        (rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID, operation_id.as_str()),
        (
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_WORKER_LOCK,
            rustfs_utils::http::metadata_compat::RESTORE_WORKER_LOCK_PROTOCOL_V1,
        ),
    ] {
        for prefix in [
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
        ] {
            assert_eq!(
                pending.user_defined.get(&format!("{prefix}{suffix}")).map(String::as_str),
                Some(expected),
                "pending copy-back must retain each recovery marker alias"
            );
        }
    }

    let completed = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("finalized metadata should be readable");
    assert_eq!(
        completed.transitioned_object.status, TRANSITION_COMPLETE,
        "finalization must restore the completed transition state"
    );
    let completed_status = parse_restore_obj_status(
        completed
            .user_defined
            .get(rustfs_filemeta::metadata_keys::RESTORE)
            .expect("finalized restore status should be present"),
    )
    .expect("finalized restore status should parse");
    assert!(!completed_status.on_going(), "finalization must publish completion");
    assert!(completed_status.expiry().is_some(), "finalization must publish the completed expiry");
    for suffix in [
        rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
        rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_WORKER_LOCK,
    ] {
        for prefix in [
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
        ] {
            assert!(
                !completed.user_defined.contains_key(&format!("{prefix}{suffix}")),
                "finalization must remove every recovery marker alias"
            );
        }
    }
    let completed_action = lifecycle
        .eval_inner(&completed.to_lifecycle_opts(), expiry_evaluation_time, 0)
        .await
        .action;
    assert!(
        completed_action.delete_restored(),
        "the same lifecycle evaluator must expire a completed restore beyond its deadline"
    );
    backend.set_unreachable(true).await;
    let mut restored = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("completed restore must be readable with the remote tier unavailable")
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("local restored body should drain");
    assert_eq!(restored, payload, "finalized local copy must preserve every byte");
}

#[tokio::test]
#[serial_test::serial]
async fn restore_copyback_overwrite_preserves_remote_cleanup_owner() {
    assert_transition_overwrite_preserves_remote_cleanup_owner(true).await;
}

#[tokio::test]
#[serial_test::serial]
async fn completed_transition_overwrite_preserves_remote_cleanup_owner() {
    assert_transition_overwrite_preserves_remote_cleanup_owner(false).await;
}

async fn assert_transition_overwrite_preserves_remote_cleanup_owner(during_copyback: bool) {
    use crate::set_disk::replication::RestoreFinalizeBarrier;

    let (dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "restore-copyback-cleanup-owner";
    let object = "object.bin";
    let payload = vec![0x37u8; 6 * 1024 * 1024];
    let write_opts = ObjectOptions {
        write_completion: WriteCompletion::TailDrained,
        ..Default::default()
    };
    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("cleanup probe bucket should be created");
    let mut reader = PutObjReader::from_vec(payload);
    let original = set_disks
        .put_object(bucket, object, &mut reader, &write_opts)
        .await
        .expect("cleanup probe source should be written");
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&set_disks.instance_ctx().tier_config_mgr(), &tier_name).await;
    set_disks
        .transition_object(
            bucket,
            object,
            &ObjectOptions {
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: tier_name.clone(),
                    etag: original.etag.clone().expect("source ETag should be present"),
                    ..Default::default()
                },
                mod_time: original.mod_time,
                ..Default::default()
            },
        )
        .await
        .expect("cleanup probe source should transition");
    let source = set_disks
        .get_object_info(bucket, object, &ObjectOptions::default())
        .await
        .expect("transitioned cleanup source should resolve");
    assert_eq!(backend.object_count().await, 1, "transition must retain the real remote cleanup source");
    let mut barrier = None;
    let mut restore = None;
    if during_copyback {
        let operation_id = Uuid::new_v4();
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_id.to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_WORKER_LOCK,
            rustfs_utils::http::metadata_compat::RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
        );
        metadata.insert(
            rustfs_filemeta::metadata_keys::RESTORE.to_string(),
            "ongoing-request=\"true\"".to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(metadata.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("cleanup probe restore generation should be installed");
        let installed = RestoreFinalizeBarrier::install_before_lock(bucket, object);
        let restore_set = Arc::clone(&set_disks);
        restore = Some(tokio::spawn(async move {
            let mut opts = ObjectOptions {
                user_defined: metadata,
                ..Default::default()
            };
            opts.transition.restore_request.days = Some(1);
            restore_set.restore_transitioned_object(bucket, object, &opts).await
        }));
        tokio::time::timeout(Duration::from_secs(30), installed.wait_until_paused())
            .await
            .expect("real restore should publish local bytes before finalization");
        let pending = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("copy-back metadata should resolve");
        assert!(pending.restore_ongoing, "probe must hit an unfinished real restore");
        barrier = Some(installed);
    }
    let mut replacement = PutObjReader::from_vec(vec![0x69u8; 6 * 1024 * 1024]);
    set_disks
        .put_object(bucket, object, &mut replacement, &write_opts)
        .await
        .expect("ordinary overwrite must succeed while restore finalization is paused");
    if let Some(barrier) = barrier {
        barrier.release();
    }
    if let Some(restore) = restore {
        tokio::time::timeout(Duration::from_secs(30), restore)
            .await
            .expect("superseded restore must finish")
            .expect("superseded restore must not panic")
            .expect_err("restore finalization must not revive the overwritten generation");
    }
    let mut cleanup_owners = Vec::new();
    for dir in &dirs {
        let encoded = match tokio::fs::read(dir.path().join(bucket).join(object).join(crate::disk::STORAGE_FORMAT_FILE)).await {
            Ok(encoded) => encoded,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                cleanup_owners.push(0);
                continue;
            }
            Err(err) => panic!("cleanup source metadata should be readable: {err}"),
        };
        let versions = rustfs_filemeta::FileMeta::load(&encoded)
            .expect("cleanup source metadata should decode")
            .get_file_info_versions(bucket, object, false)
            .expect("cleanup source versions should decode");
        let count = versions
            .free_versions
            .iter()
            .filter(|version| {
                version.transition_tier == source.transitioned_object.tier
                    && version.transitioned_objname == source.transitioned_object.name
            })
            .count();
        cleanup_owners.push(count);
    }
    let remote_objects = backend.object_count().await;
    println!("restore_cleanup_probe during_copyback={during_copyback} owners={cleanup_owners:?} remote_objects={remote_objects}");
    assert!(
        remote_objects == 0 || cleanup_owners.iter().filter(|count| **count > 0).count() >= 3,
        "ordinary overwrite must remove the remote object or retain its cleanup owner at write quorum: during_copyback={during_copyback}, owners={cleanup_owners:?}, remote_objects={remote_objects}"
    );
}
