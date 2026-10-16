use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
use crate::{
    bucket::metadata_sys,
    disk::DiskAPI as _,
    error::Error,
    object_api::{ObjectInfo, ObjectOptions, PutObjReader, WriteCommitGuard, WriteCompletion},
    storage_api_contracts::{
        bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions},
        multipart::{CompletePart, MultipartOperations as _},
        namespace::NamespaceLocking as _,
        object::{ObjectIO as _, ObjectOperations as _},
    },
    store::ECStore,
};
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

fn object_key() -> String {
    format!("outputs/74656e616e74/{}/{}/image.png", Uuid::new_v4(), Uuid::new_v4())
}

fn options() -> ObjectOptions {
    ObjectOptions {
        versioned: true,
        write_completion: WriteCompletion::TailDrained,
        ..Default::default()
    }
}

fn body() -> PutObjReader {
    PutObjReader::from_vec(vec![0xA5; 273])
}

fn unsupported<T>(result: Result<T, Error>) {
    let Err(error) = result else {
        panic!("ARTIFACT_WRITE_UNSUPPORTED: operation unexpectedly succeeded")
    };
    assert!(
        error
            .to_string()
            .contains("artifact bucket does not support this write operation"),
        "ARTIFACT_WRITE_UNSUPPORTED: {error}"
    );
}

async fn bucket(store: &ECStore, name: &str, artifact: bool) -> Uuid {
    store
        .make_bucket(
            name,
            &MakeBucketOptions {
                artifact_base_prefix: artifact.then(|| "outputs/".to_owned()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create real versioned fixture bucket");
    store.bucket_incarnation_id(name).await.expect("persisted incarnation")
}

async fn snapshot(store: &ECStore, bucket: &str, object: &str) -> Vec<Vec<u8>> {
    let disks = store.pools[0].disk_set[0].disk_inventory().await;
    assert_eq!(disks.len(), 4);
    let mut versions = Vec::new();
    for disk in disks {
        versions.push(
            disk.expect("online fixture disk")
                .read_xl(bucket, object, false)
                .await
                .expect("physical version metadata")
                .buf,
        );
    }
    versions
}

async fn copy_info(store: &ECStore, bucket: &str, object: &str) -> ObjectInfo {
    let mut info = store
        .get_object_info(bucket, object, &options())
        .await
        .expect("real source object");
    info.put_object_reader = Some(body());
    info
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_write_entrypoint_object_mutations_are_rejected() {
    run_large_stack_async_test("artifact-object-entrypoints", object_mutations_case);
}

async fn object_mutations_case() {
    let root = tempfile::tempdir().expect("object entrypoint fixture directory");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-object-entrypoints", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let artifact = "artifact-entrypoint-object";
    let ordinary = "artifact-entrypoint-control";
    bucket(&store, artifact, true).await;
    bucket(&store, ordinary, false).await;
    let object = object_key();
    store
        .put_object(artifact, &object, &mut body(), &options())
        .await
        .expect("ordinary owned artifact PUT is supported");
    store
        .put_object(ordinary, &object, &mut body(), &options())
        .await
        .expect("ordinary control object");
    let before = snapshot(&store, artifact, &object).await;

    let mut source = copy_info(&store, ordinary, &object).await;
    unsupported(
        store
            .copy_object(ordinary, &object, artifact, &object, &mut source, &options(), &options())
            .await,
    );
    let mut source = copy_info(&store, artifact, &object).await;
    unsupported(
        store
            .copy_object(artifact, &object, artifact, &object, &mut source, &options(), &options())
            .await,
    );
    for opts in [
        ObjectOptions {
            replication_request: true,
            ..options()
        },
        ObjectOptions {
            data_movement: true,
            ..options()
        },
        ObjectOptions {
            preserve_ciphertext: true,
            ..options()
        },
        ObjectOptions {
            metadata_chg: true,
            ..options()
        },
        ObjectOptions {
            replication_status_writeback: Some(Box::default()),
            ..options()
        },
    ] {
        unsupported(store.put_object(artifact, &object, &mut body(), &opts).await);
    }
    unsupported(
        store
            .put_object_for_data_movement(
                artifact,
                &object,
                &mut body(),
                &ObjectOptions {
                    data_movement: true,
                    ..options()
                },
                None,
            )
            .await,
    );
    {
        let lock = store
            .new_ns_lock(artifact, &object)
            .await
            .expect("real borrowed target namespace");
        let guard = WriteCommitGuard::acquire(&lock, Duration::from_secs(30))
            .await
            .expect("real borrowed write owner");
        let mut opts = options();
        opts.add_write_commit_guard(&guard);
        unsupported(store.put_object(artifact, &object, &mut body(), &opts).await);
    }
    unsupported(store.transition_object(artifact, &object, &options()).await);
    unsupported(store.clone().restore_transitioned_object(artifact, &object, &options()).await);
    unsupported(
        store
            .put_object_metadata(
                artifact,
                &object,
                &ObjectOptions {
                    eval_metadata: Some(HashMap::from([("x-amz-meta-test".to_owned(), "changed".to_owned())])),
                    ..options()
                },
            )
            .await,
    );
    unsupported(store.put_object_tags(artifact, &object, "kind=artifact", &options()).await);
    unsupported(store.delete_object_tags(artifact, &object, &options()).await);
    assert_eq!(
        snapshot(&store, artifact, &object).await,
        before,
        "ARTIFACT_WRITE_UNSUPPORTED: every disk retains the exact original versions"
    );

    // An artifact source remains readable; only the destination write is classified.
    let mut source = copy_info(&store, artifact, &object).await;
    store
        .copy_object(artifact, &object, ordinary, "copied.png", &mut source, &options(), &options())
        .await
        .expect("copy into ordinary bucket stays supported");
    store
        .put_object(
            ordinary,
            &object,
            &mut body(),
            &ObjectOptions {
                replication_request: true,
                ..options()
            },
        )
        .await
        .expect("ordinary replication PUT stays supported");
    {
        let lock = store.new_ns_lock(ordinary, &object).await.expect("ordinary target namespace");
        let guard = WriteCommitGuard::acquire(&lock, Duration::from_secs(30))
            .await
            .expect("ordinary borrowed owner");
        let mut opts = options();
        opts.add_write_commit_guard(&guard);
        store
            .put_object(ordinary, &object, &mut body(), &opts)
            .await
            .expect("ordinary borrowed PUT stays supported");
    }
    store
        .put_object_metadata(
            ordinary,
            &object,
            &ObjectOptions {
                eval_metadata: Some(HashMap::from([("x-amz-meta-test".to_owned(), "changed".to_owned())])),
                ..options()
            },
        )
        .await
        .expect("ordinary metadata update stays supported");
    store
        .put_object_tags(ordinary, &object, "kind=ordinary", &options())
        .await
        .expect("ordinary tag update stays supported");
    store
        .delete_object_tags(ordinary, &object, &options())
        .await
        .expect("ordinary tag removal stays supported");
    assert_eq!(
        store
            .get_object_info(ordinary, &object, &options())
            .await
            .expect("ordinary object remains readable")
            .size,
        273
    );
    assert_eq!(
        snapshot(&store, artifact, &object).await,
        before,
        "ARTIFACT_WRITE_UNSUPPORTED: reading an artifact source cannot mutate its versions"
    );
    let tier = "ARTIFACT-ENTRYPOINT-CONTROL";
    let backend = crate::services::tier::test_util::register_mock_tier(&ctx.tier_config_mgr(), tier).await;
    let current = store
        .get_object_info(ordinary, &object, &options())
        .await
        .expect("ordinary source before transition");
    let exact = ObjectOptions {
        version_id: current.version_id.map(|version| version.to_string()),
        ..options()
    };
    store
        .transition_object(
            ordinary,
            &object,
            &ObjectOptions {
                transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                    status: rustfs_filemeta::TRANSITION_PENDING.to_owned(),
                    tier: tier.to_owned(),
                    etag: current.etag.expect("ordinary source etag"),
                    ..Default::default()
                },
                mod_time: current.mod_time,
                ..exact.clone()
            },
        )
        .await
        .expect("ordinary transition stays supported");
    assert_eq!(backend.object_count().await, 1);
    store
        .clone()
        .restore_transitioned_object(
            ordinary,
            &object,
            &ObjectOptions {
                transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                    restore_request: s3s::dto::RestoreRequest {
                        days: Some(1),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..exact
            },
        )
        .await
        .expect("ordinary restore stays supported");
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(Duration::from_secs(30)).await);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_write_entrypoint_multipart_rejects_writes_but_keeps_legacy_cleanup() {
    run_large_stack_async_test("artifact-multipart-entrypoints", multipart_case);
}

async fn multipart_case() {
    let root = tempfile::tempdir().expect("multipart entrypoint fixture directory");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-multipart-entrypoints", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let artifact = "artifact-entrypoint-multipart";
    let ordinary = "artifact-multipart-control";
    let incarnation = bucket(&store, artifact, true).await;
    bucket(&store, ordinary, false).await;
    let object = object_key();
    store
        .put_object(artifact, &object, &mut body(), &options())
        .await
        .expect("seed the current artifact version");
    let before = snapshot(&store, artifact, &object).await;
    let opts = ObjectOptions {
        expected_bucket_incarnation_id: Some(incarnation),
        ..options()
    };
    unsupported(store.new_multipart_upload(artifact, &object, &opts).await);
    // Seed actual legacy staging while holding the lifecycle owner required by the pool API.
    let lifecycle = store
        .acquire_bucket_lifecycle_read_lock(artifact)
        .await
        .expect("legacy upload lifecycle owner");
    let mut staging_opts = opts.clone();
    staging_opts.add_bucket_lifecycle_lock_guard(&lifecycle);
    let upload = store.pools[0]
        .new_multipart_upload(artifact, &object, &staging_opts)
        .await
        .expect("real legacy upload staging");
    let part = store.pools[0]
        .put_object_part(artifact, &object, &upload.upload_id, 1, &mut body(), &staging_opts)
        .await
        .expect("real legacy staged part");
    drop(staging_opts);
    drop(lifecycle);
    let parts = vec![CompletePart {
        part_num: part.part_num,
        etag: part.etag.clone(),
        ..Default::default()
    }];
    unsupported(
        store
            .put_object_part(artifact, &object, &upload.upload_id, 2, &mut body(), &opts)
            .await,
    );
    unsupported(
        store
            .clone()
            .complete_multipart_upload(artifact, &object, &upload.upload_id, parts.clone(), &opts)
            .await,
    );
    let movement = ObjectOptions {
        data_movement: true,
        part_number: Some(2),
        ..opts.clone()
    };
    unsupported(
        store
            .handle_new_multipart_upload_with_pool_idx(artifact, &object, &movement, None)
            .await,
    );
    unsupported(
        store
            .put_object_part_for_data_movement(0, artifact, &object, &upload.upload_id, &mut body(), &movement)
            .await,
    );
    unsupported(
        store
            .clone()
            .complete_multipart_upload_for_data_movement((0, None), artifact, &object, &upload.upload_id, parts, &movement)
            .await,
    );
    assert_eq!(
        snapshot(&store, artifact, &object).await,
        before,
        "ARTIFACT_WRITE_UNSUPPORTED: rejected multipart completion publishes no version"
    );
    let listing = store
        .list_multipart_uploads(artifact, &object, None, None, None, 100)
        .await
        .expect("list legacy uploads remains supported");
    assert_eq!(listing.uploads.len(), 1);
    assert_eq!(listing.uploads[0].upload_id, upload.upload_id);
    let staged = store
        .list_object_parts(artifact, &object, &upload.upload_id, None, 100, &opts)
        .await
        .expect("list legacy parts remains supported");
    assert_eq!(staged.parts.len(), 1);
    assert_eq!(staged.parts[0].etag, part.etag);
    store
        .abort_multipart_upload(artifact, &object, &upload.upload_id, &opts)
        .await
        .expect("abort legacy upload remains supported");
    assert!(
        store
            .list_multipart_uploads(artifact, &object, None, None, None, 100)
            .await
            .expect("list after abort")
            .uploads
            .is_empty()
    );
    assert_eq!(
        snapshot(&store, artifact, &object).await,
        before,
        "ARTIFACT_WRITE_UNSUPPORTED: legacy abort leaves published versions intact"
    );

    let upload = store
        .new_multipart_upload(ordinary, &object, &options())
        .await
        .expect("ordinary multipart initialization");
    let part = store
        .put_object_part(ordinary, &object, &upload.upload_id, 1, &mut body(), &options())
        .await
        .expect("ordinary part write");
    let completed = store
        .clone()
        .complete_multipart_upload(
            ordinary,
            &object,
            &upload.upload_id,
            vec![CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            }],
            &options(),
        )
        .await
        .expect("ordinary multipart completion");
    assert_eq!(completed.size, 273);
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(Duration::from_secs(30)).await);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_write_entrypoint_classification_is_bound_to_bucket_incarnation() {
    run_large_stack_async_test("artifact-entrypoint-recreation", recreation_case);
}

async fn recreation_case() {
    let root = tempfile::tempdir().expect("entrypoint recreation fixture directory");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-entrypoint-recreation", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let name = "artifact-entrypoint-recreated";
    let old_incarnation = bucket(&store, name, false).await;
    let object = object_key();
    let barrier =
        crate::set_disk::PutObjectCommitBarrier::install(name, &object, crate::set_disk::PutObjectCommitPause::BeforeNamespace);
    let put_store = store.clone();
    let put_object = object.clone();
    let put = tokio::spawn(async move {
        put_store
            .put_object(
                name,
                &put_object,
                &mut body(),
                &ObjectOptions {
                    replication_request: true,
                    ..options()
                },
            )
            .await
    });
    barrier.wait_until_paused().await;
    tokio::time::timeout(Duration::from_secs(30), store.delete_bucket(name, &DeleteBucketOptions::default()))
        .await
        .expect("staged old-generation upload does not prevent bucket deletion")
        .expect("delete old ordinary bucket");
    let new_incarnation = bucket(&store, name, true).await;
    assert_ne!(new_incarnation, old_incarnation);
    barrier.release();
    let error = tokio::time::timeout(Duration::from_secs(30), put)
        .await
        .expect("stale staged PUT terminates")
        .expect("PUT task joins")
        .expect_err("classification of an ordinary bucket cannot authorize the new artifact bucket");
    assert!(
        matches!(error, Error::BucketNotFound(ref bucket) if bucket == name),
        "ARTIFACT_WRITE_INCARNATION: {error}"
    );
    for disk in store.pools[0].disk_set[0].disk_inventory().await {
        let error = disk
            .expect("online fixture disk")
            .read_xl(name, &object, false)
            .await
            .err()
            .expect("no stale object metadata may be published");
        assert!(
            matches!(error, crate::disk::error::DiskError::FileNotFound),
            "ARTIFACT_WRITE_INCARNATION: {error}"
        );
    }
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(Duration::from_secs(30)).await);
}

#[test]
#[serial_test::serial(storage_class_env)]
fn artifact_write_entrypoint_lifecycle_fences_require_real_matching_owners() {
    run_large_stack_async_test("artifact-entrypoint-lifecycle-owner", lifecycle_owner_case);
}

async fn lifecycle_owner_case() {
    let root = tempfile::tempdir().expect("lifecycle owner fixture directory");
    let (ctx, store, shutdown) =
        without_storage_class_env(build_isolated_test_store(root.path(), "artifact-lifecycle-owner", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let ordinary = "artifact-lifecycle-ordinary";
    let artifact = "artifact-lifecycle-dedicated";
    bucket(&store, ordinary, false).await;
    bucket(&store, artifact, true).await;
    let object = object_key();
    for name in [ordinary, artifact] {
        store
            .put_object(name, &object, &mut body(), &options())
            .await
            .expect("initial real object");
    }
    let before = snapshot(&store, ordinary, &object).await;
    let artifact_before = snapshot(&store, artifact, &object).await;
    let unrelated = store
        .new_ns_lock(ordinary, "unrelated-object")
        .await
        .expect("unrelated namespace");
    let unrelated_owner = unrelated
        .get_read_lock(Duration::from_secs(30))
        .await
        .expect("live unrelated owner");
    let mut foreign_fence = options();
    foreign_fence.add_bucket_lifecycle_lock_guard(&unrelated_owner);
    let lifecycle = store
        .acquire_bucket_lifecycle_read_lock(ordinary)
        .await
        .expect("real lifecycle owner");
    let mut released_fence = options();
    released_fence.add_bucket_lifecycle_lock_guard(&lifecycle);
    let borrowed_snapshot = store
        .object_lock_config_snapshot_under_lifecycle_fence(
            ordinary,
            released_fence
                .bucket_lifecycle_lock_fence
                .as_ref()
                .expect("borrowed lifecycle fence"),
        )
        .await
        .expect("real snapshot retaining only its metadata lock");
    drop(lifecycle);
    let mut snapshot_without_owner = options();
    borrowed_snapshot.add_lock_fences(&mut snapshot_without_owner);
    snapshot_without_owner.object_lock_config_snapshot = Some(borrowed_snapshot);

    for opts in [foreign_fence, released_fence, snapshot_without_owner] {
        let mut source = copy_info(&store, ordinary, &object).await;
        for result in [
            store
                .put_object(
                    ordinary,
                    &object,
                    &mut body(),
                    &ObjectOptions {
                        replication_request: true,
                        ..opts.clone()
                    },
                )
                .await
                .map(|_| ()),
            store
                .copy_object(ordinary, &object, ordinary, &object, &mut source, &options(), &opts)
                .await
                .map(|_| ()),
            store
                .put_object_tags(ordinary, &object, "owner=fake", &opts)
                .await
                .map(|_| ()),
            store.put_object_metadata(ordinary, &object, &opts).await.map(|_| ()),
            store.clone().restore_transitioned_object(ordinary, &object, &opts).await,
            store.new_multipart_upload(ordinary, &object, &opts).await.map(|_| ()),
        ] {
            let error = result.expect_err("a fence without its matching lifecycle owner cannot authorize a write");
            assert!(
                error.to_string().contains("write requires an owned bucket lifecycle fence"),
                "ARTIFACT_WRITE_LIFECYCLE_OWNER: {error}"
            );
        }
        let error = store
            .put_object(artifact, &object, &mut body(), &opts)
            .await
            .expect_err("ordinary artifact PUT also requires the lifecycle owner");
        assert!(
            error.to_string().contains("write requires an owned bucket lifecycle fence"),
            "ARTIFACT_WRITE_LIFECYCLE_OWNER: {error}"
        );
        assert_eq!(
            snapshot(&store, ordinary, &object).await,
            before,
            "ARTIFACT_WRITE_LIFECYCLE_OWNER: ordinary metadata unchanged"
        );
        assert_eq!(
            snapshot(&store, artifact, &object).await,
            artifact_before,
            "ARTIFACT_WRITE_LIFECYCLE_OWNER: artifact versions unchanged"
        );
    }
    drop(unrelated_owner);
    assert!(
        store
            .list_multipart_uploads(ordinary, &object, None, None, None, 100)
            .await
            .expect("inspect rejected multipart creation")
            .uploads
            .is_empty()
    );

    let owned_snapshot = store
        .object_lock_config_snapshot(ordinary)
        .await
        .expect("real lifecycle-owning snapshot");
    let mut owned = options();
    owned_snapshot.add_lock_fences(&mut owned);
    owned.object_lock_config_snapshot = Some(owned_snapshot.clone());
    let lifecycle_namespace = store
        .new_ns_lock(ordinary, crate::storage_api_contracts::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .expect("actual bucket deletion namespace");
    assert!(
        lifecycle_namespace.get_write_lock(Duration::from_millis(100)).await.is_err(),
        "matching snapshot actually blocks the bucket deletion lock"
    );
    store
        .put_object_tags(ordinary, &object, "owner=real", &owned)
        .await
        .expect("matching owned snapshot permits ordinary tags");
    let upload = store
        .new_multipart_upload(ordinary, &object, &owned)
        .await
        .expect("matching owned snapshot permits ordinary multipart creation");
    store
        .abort_multipart_upload(ordinary, &object, &upload.upload_id, &owned)
        .await
        .expect("clean up supported multipart upload");
    drop(owned);
    drop(owned_snapshot);
    let artifact_snapshot = store
        .object_lock_config_snapshot(artifact)
        .await
        .expect("artifact lifecycle-owning snapshot");
    let mut owned = options();
    artifact_snapshot.add_lock_fences(&mut owned);
    owned.object_lock_config_snapshot = Some(artifact_snapshot);
    store
        .put_object(artifact, &object, &mut body(), &owned)
        .await
        .expect("matching owned snapshot permits ordinary artifact PUT");
    assert_ne!(
        snapshot(&store, artifact, &object).await,
        artifact_before,
        "supported artifact PUT publishes a new version"
    );
    drop(owned);
    shutdown.cancel();
    assert!(ctx.wait_for_detached_mutations(Duration::from_secs(30)).await);
}
