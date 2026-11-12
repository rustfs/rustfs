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

//! DeleteObject and DeleteObjects tests (batch E owns both).

use super::common::*;
use super::*;
use crate::app::test_seams;

async fn wait_for_object_absence(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if ecstore
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .is_err()
        {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state object-version integration test: runs serialized in the CI ILM Integration (serial) lane"]
async fn undo_delete_only_removes_the_current_matching_delete_marker() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-undo-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    create_test_bucket(&ecstore, bucket.as_str()).await;

    let put = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(b"visible again")))
        .content_length(Some(13))
        .build()
        .unwrap();
    Box::pin(test_seams::put_object(&usecase, &fs, build_request(put, Method::PUT)))
        .await
        .expect("object version should be written");
    let marker = Box::pin(
        usecase.execute_delete_object(build_request(
            DeleteObjectInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .build()
                .unwrap(),
            Method::DELETE,
        )),
    )
    .await
    .expect("delete marker should be created")
    .output
    .version_id
    .expect("versioned delete should return the marker version");

    let delete_marker = || {
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .version_id(Some(marker.clone()))
            .build()
            .unwrap()
    };
    let mut stale_req = build_request(delete_marker(), Method::DELETE);
    stale_req.headers.insert(
        RUSTFS_EXPECTED_CURRENT_VERSION_ID,
        HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
    );
    let err = Box::pin(usecase.execute_delete_object(stale_req))
        .await
        .expect_err("mismatched delete marker must fail");
    assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);

    let mut matching_req = build_request(delete_marker(), Method::DELETE);
    matching_req
        .headers
        .insert(RUSTFS_EXPECTED_CURRENT_VERSION_ID, HeaderValue::from_str(&marker).unwrap());
    Box::pin(usecase.execute_delete_object(matching_req))
        .await
        .expect("current matching delete marker should be removed");
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, b"visible again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn delete_transitioned_object_removes_remote_tier_copy_via_usecase() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"delete transitioned object through delete API";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(ecstore.clone(), bucket.as_str())
        .await
        .expect("Failed to enqueue transitioned object");

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition before delete usecase runs");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.contains(&remote_object).await);

    let mut req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );
    insert_header(&mut req.headers, SUFFIX_FORCE_DELETE, "true");
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to delete object through usecase");

    assert!(
        wait_for_object_absence(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "object should be removed from hot tier after delete usecase"
    );

    assert!(
        backend.wait_for_remote_absence(&remote_object, TRANSITION_WAIT_TIMEOUT).await,
        "transitioned object should be removed from remote tier after delete usecase"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn compensation_driven_transition_still_cleans_remote_tier_on_delete() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-compensation-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"compensation should still preserve delete cleanup";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(&backend, || async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    assert_eq!(backend.put_count().await, 1, "compensation should upload the version once");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.contains(&remote_object).await);

    let mut req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );
    insert_header(&mut req.headers, SUFFIX_FORCE_DELETE, "true");
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to delete object through usecase after compensation-driven transition");

    assert!(
        wait_for_object_absence(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "object should be removed from hot tier after delete usecase"
    );

    assert!(
        backend.wait_for_remote_absence(&remote_object, TRANSITION_WAIT_TIMEOUT).await,
        "transitioned object should be removed from remote tier after delete usecase"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn compensation_driven_versioned_delete_still_creates_delete_marker() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-versioned-delete-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"versioned delete should preserve transitioned remote version behind delete marker";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(&backend, || async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    assert_eq!(backend.put_count().await, 1, "compensation should upload the version once");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.contains(&remote_object).await);

    let req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );

    Box::pin(usecase.execute_delete_object(req))
        .await
        .expect("Failed to issue versioned delete after compensation-driven transition");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "versioned delete should create a delete marker after compensation-driven transition"
    );
    assert!(
        backend.contains(&remote_object).await,
        "creating a delete marker should not remove the transitioned remote object version"
    );
}

/// Regression pin for the DeleteObjects hot path: the bucket's versioning
/// configuration must be resolved exactly once per request — not once per
/// key, which used to cost up to 1000 identical metadata lookups per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
async fn delete_objects_resolves_bucket_versioning_once_per_request() {
    use super::super::storage_api::test::bucket::replication::DELETE_CONFIG_SNAPSHOT_LOADS;
    use super::super::storage_api::test::{ReqInfo, VERSIONING_CONFIG_LOOKUPS};
    use std::sync::atomic::Ordering;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-delobjs-vers-once-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;

    let keys = ["batch/a.txt", "batch/b.txt", "batch/c.txt"];
    for key in keys {
        upload_test_object(&ecstore, bucket.as_str(), key, b"delete objects versioning payload").await;
    }

    let input = DeleteObjectsInput::builder()
        .bucket(bucket.clone())
        .delete(Delete {
            objects: keys
                .iter()
                .map(|key| ObjectIdentifier {
                    key: key.to_string(),
                    version_id: None,
                    ..Default::default()
                })
                .collect(),
            quiet: None,
        })
        .build()
        .expect("delete objects input should build");

    let mut req = build_request(input, Method::POST);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let lookups_before = VERSIONING_CONFIG_LOOKUPS.load(Ordering::SeqCst);
    let snapshots_before = DELETE_CONFIG_SNAPSHOT_LOADS.load(Ordering::SeqCst);
    let response = usecase
        .execute_delete_objects(req)
        .await
        .expect("delete objects should succeed");

    let output = response.output;
    let errors = output.errors.unwrap_or_default();
    assert!(errors.is_empty(), "DeleteObjects reported per-key errors: {errors:?}");
    assert_eq!(
        output.deleted.unwrap_or_default().len(),
        keys.len(),
        "every requested key should be deleted"
    );

    let lookups = VERSIONING_CONFIG_LOOKUPS.load(Ordering::SeqCst) - lookups_before;
    let snapshots = DELETE_CONFIG_SNAPSHOT_LOADS.load(Ordering::SeqCst) - snapshots_before;
    assert_eq!(
        snapshots,
        1,
        "DeleteObjects must resolve delete admission config exactly once per request; got {snapshots} snapshots for {} keys",
        keys.len()
    );
    assert_eq!(lookups, 0, "DeleteObjects must not fall back to legacy per-key versioning lookups");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
async fn delete_objects_treats_empty_directory_version_as_absent() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delobjs-empty-version-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "directory/";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, b"").await;

    let input = DeleteObjectsInput::builder()
        .bucket(bucket)
        .delete(Delete {
            objects: vec![ObjectIdentifier {
                key: object.to_string(),
                version_id: Some(" \t ".to_string()),
                ..Default::default()
            }],
            quiet: None,
        })
        .build()
        .expect("delete objects input should build");
    let mut req = build_request(input, Method::POST);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let response = usecase
        .execute_delete_objects(req)
        .await
        .expect("empty VersionId should be treated as an absent version");
    let deleted = response
        .output
        .deleted
        .expect("delete response should contain the directory entry");

    assert_eq!(deleted.len(), 1);
    assert_eq!(
        deleted[0].delete_marker, None,
        "the synthetic null version must be purged, not hidden by a new marker"
    );
    assert_eq!(deleted[0].version_id, None, "the synthetic null sentinel must stay off the wire");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
async fn delete_objects_preserves_explicit_null_directory_version() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delobjs-null-version-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "directory/";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, b"").await;

    let input = DeleteObjectsInput::builder()
        .bucket(bucket)
        .delete(Delete {
            objects: vec![ObjectIdentifier {
                key: object.to_string(),
                version_id: Some("null".to_string()),
                ..Default::default()
            }],
            quiet: None,
        })
        .build()
        .expect("delete objects input should build");
    let mut req = build_request(input, Method::POST);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let response = usecase
        .execute_delete_objects(req)
        .await
        .expect("explicit null VersionId should delete the directory's null version");
    let deleted = response
        .output
        .deleted
        .expect("delete response should contain the directory entry");

    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].delete_marker, None);
    assert_eq!(deleted[0].version_id.as_deref(), Some("null"));
}

async fn install_malformed_versioning_config(bucket: &str) {
    use super::super::storage_api::test::{get_global_bucket_metadata_sys, set_bucket_metadata};

    let sys = get_global_bucket_metadata_sys().expect("bucket metadata system should be initialized");
    let metadata = {
        let sys = sys.read().await;
        sys.get(bucket)
            .await
            .expect("bucket metadata should be cached before corruption injection")
    };
    let mut metadata = (*metadata).clone();
    metadata.versioning_config_xml = b"<VersioningConfiguration>".to_vec();
    metadata.versioning_config = None;
    set_bucket_metadata(bucket.to_string(), metadata)
        .await
        .expect("malformed versioning metadata should be installed for the request test");
}

async fn install_malformed_replication_config(bucket: &str) {
    use super::super::storage_api::test::{get_global_bucket_metadata_sys, set_bucket_metadata};

    let sys = get_global_bucket_metadata_sys().expect("bucket metadata system should be initialized");
    let metadata = {
        let sys = sys.read().await;
        sys.get(bucket)
            .await
            .expect("bucket metadata should be cached before corruption injection")
    };
    let mut metadata = (*metadata).clone();
    metadata.replication_config_xml = b"<ReplicationConfiguration>".to_vec();
    metadata.replication_config = None;
    set_bucket_metadata(bucket.to_string(), metadata)
        .await
        .expect("malformed replication metadata should be installed for the request test");
}

#[tokio::test]
#[serial]
async fn delete_object_versioning_config_failure_leaves_latest_object_intact() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delete-versioning-fail-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    let payload = b"local delete must not commit before versioning admission";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, payload).await;
    install_malformed_versioning_config(&bucket).await;

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .build()
        .expect("delete input should build");
    let mut req = build_request(input, Method::DELETE);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let err = usecase
        .execute_delete_object(req)
        .await
        .expect_err("versioning config failure must reject DeleteObject");

    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, payload);
}

#[tokio::test]
#[serial]
async fn delete_object_replication_config_failure_leaves_latest_object_intact() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delete-repl-fail-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    let payload = b"local delete must not commit before replication admission";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, payload).await;
    install_malformed_replication_config(&bucket).await;

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .build()
        .expect("delete input should build");
    let mut req = build_request(input, Method::DELETE);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let err = usecase
        .execute_delete_object(req)
        .await
        .expect_err("replication config failure must reject DeleteObject");

    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn delete_object_uses_one_replication_config_generation() {
    use super::super::object_usecase::install_delete_snapshot_test_hook;
    use super::super::storage_api::test::ReqInfo;
    use super::super::storage_api::test::bucket::replication::take_scheduled_replication_deletes;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delete-config-generation-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    create_test_bucket(&ecstore, &bucket).await;

    let mut reader = PutObjReader::from_vec(b"generation-bound delete".to_vec());
    let uploaded = ecstore
        .put_object(
            &bucket,
            object,
            &mut reader,
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("versioned object should be uploaded");
    let version_id = uploaded.version_id.expect("versioned upload should return a version ID");
    let version_id_string = version_id.to_string();

    install_delete_replication_config(&bucket, true, false, None).await;
    assert!(take_scheduled_replication_deletes().is_empty());

    let loaded = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    install_delete_snapshot_test_hook(bucket.clone(), Arc::clone(&loaded), Arc::clone(&resume));

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .version_id(Some(version_id_string.clone()))
        .build()
        .expect("version delete input should build");
    let mut req = build_request(input, Method::DELETE);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let update_config = async {
        loaded.wait().await;
        install_delete_replication_config(&bucket, false, false, None).await;
        resume.wait().await;
    };
    let (response, ()) = tokio::join!(usecase.execute_delete_object(req), update_config);
    let response = response.expect("delete admitted by the first config generation should succeed");

    assert_eq!(response.output.version_id.as_deref(), Some(version_id_string.as_str()));
    let scheduled = take_scheduled_replication_deletes();
    assert_eq!(scheduled.len(), 1, "the first config generation must control post-delete queueing");
    assert_eq!(scheduled[0].version_id, Some(version_id));
    let state = scheduled[0]
        .replication_state
        .as_ref()
        .expect("admitted version purge should carry replication state");
    assert_eq!(
        state.version_purge_status_internal.as_deref(),
        Some("arn:aws:s3:::target-bucket=PENDING;")
    );
}

#[tokio::test]
#[serial]
async fn replica_delete_uses_authorized_source_version_and_its_tags() {
    use super::super::object_usecase::install_delete_source_test_hook;
    use super::super::storage_api::test::ReqInfo;
    use super::super::storage_api::test::bucket::replication::{ReplicationStatusType, take_scheduled_replication_deletes};
    use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
    use rustfs_utils::http::{SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID};
    use std::collections::HashMap;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-replica-source-version-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    create_test_bucket(&ecstore, &bucket).await;
    install_delete_replication_config(&bucket, true, true, Some(("generation", "updated"))).await;

    let mut old_opts = ObjectOptions {
        versioned: true,
        user_defined: HashMap::from([(AMZ_OBJECT_TAGGING.to_string(), "generation=old".to_string())]),
        ..Default::default()
    };
    old_opts.set_replica_status(ReplicationStatusType::Replica);
    let mut old_reader = PutObjReader::from_vec(b"old replica version".to_vec());
    let old = ecstore
        .put_object(&bucket, object, &mut old_reader, &old_opts)
        .await
        .expect("old replica version should be uploaded");
    let old_version_id = old.version_id.expect("old replica version should have a version ID");
    let old_version_id_string = old_version_id.to_string();

    let mut latest_opts = ObjectOptions {
        versioned: true,
        user_defined: HashMap::from([(AMZ_OBJECT_TAGGING.to_string(), "generation=latest".to_string())]),
        ..Default::default()
    };
    latest_opts.set_replica_status(ReplicationStatusType::Replica);
    let mut latest_reader = PutObjReader::from_vec(b"latest replica version".to_vec());
    let latest = ecstore
        .put_object(&bucket, object, &mut latest_reader, &latest_opts)
        .await
        .expect("latest replica version should be uploaded");
    assert_ne!(latest.version_id, Some(old_version_id));
    assert!(take_scheduled_replication_deletes().is_empty());

    let source_loaded = Arc::new(Barrier::new(2));
    let resume_delete = Arc::new(Barrier::new(2));
    install_delete_source_test_hook(bucket.clone(), Arc::clone(&source_loaded), Arc::clone(&resume_delete));

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .build()
        .expect("replica delete input should build");
    let mut req = build_request(input, Method::DELETE);
    req.headers
        .insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
    insert_header(&mut req.headers, SUFFIX_SOURCE_VERSION_ID, old_version_id_string.clone());
    insert_header(&mut req.headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let update_tags = async {
        source_loaded.wait().await;
        ecstore
            .put_object_tags(
                &bucket,
                object,
                "generation=updated",
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(old_version_id_string.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("tag update ordered before the locked delete should succeed");
        resume_delete.wait().await;
    };
    let (response, ()) = tokio::join!(usecase.execute_delete_object(req), update_tags);
    let response = response.expect("authorized replica version delete should succeed");

    assert_eq!(response.output.version_id.as_deref(), Some(old_version_id_string.as_str()));
    let scheduled = take_scheduled_replication_deletes();
    assert_eq!(
        scheduled.len(),
        1,
        "the tag committed after the advisory pre-read must control downstream fanout"
    );
    assert_eq!(scheduled[0].version_id, Some(old_version_id));
    assert_eq!(
        scheduled[0]
            .replication_state
            .as_ref()
            .and_then(|state| state.version_purge_status_internal.as_deref()),
        Some("arn:aws:s3:::target-bucket=PENDING;")
    );
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, b"latest replica version");
}

#[tokio::test]
#[serial]
async fn force_delete_replication_config_failure_leaves_prefix_intact() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-force-delete-repl-fail-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let prefix = "prefix/";
    let object = "prefix/object.txt";
    let payload = b"force delete must validate replication metadata before deleting descendants";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, payload).await;
    install_malformed_replication_config(&bucket).await;

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(prefix.to_string())
        .build()
        .expect("delete input should build");
    let mut req = build_request(input, Method::DELETE);
    insert_header(&mut req.headers, SUFFIX_FORCE_DELETE, "true");
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let err = usecase
        .execute_delete_object(req)
        .await
        .expect_err("replication config failure must reject force delete");

    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, payload);
}

#[tokio::test]
#[serial]
async fn replica_delete_replication_config_failure_leaves_object_intact() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-replica-delete-repl-fail-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    let payload = b"incoming replica delete must use the admitted request snapshot";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, payload).await;
    install_malformed_replication_config(&bucket).await;

    let input = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .build()
        .expect("delete input should build");
    let mut req = build_request(input, Method::DELETE);
    req.headers
        .insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let err = usecase
        .execute_delete_object(req)
        .await
        .expect_err("replication config failure must reject incoming replica delete");

    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, payload);
}

#[tokio::test]
#[serial]
async fn delete_objects_replication_config_failure_does_not_call_storage_delete() {
    use super::super::storage_api::test::ReqInfo;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-delete-repls-fail-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "object.txt";
    let payload = b"batch delete must fail closed on replication metadata errors";
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, object, payload).await;
    install_malformed_replication_config(&bucket).await;

    let input = DeleteObjectsInput::builder()
        .bucket(bucket.clone())
        .delete(Delete {
            objects: vec![ObjectIdentifier {
                key: object.to_string(),
                version_id: None,
                ..Default::default()
            }],
            quiet: None,
        })
        .build()
        .expect("delete objects input should build");
    let mut req = build_request(input, Method::POST);
    req.extensions.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    });

    let err = usecase
        .execute_delete_objects(req)
        .await
        .expect_err("replication config failure must reject DeleteObjects");

    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(read_object_bytes(&ecstore, &bucket, object).await, payload);
}

/// Regression pin for the nonexistent-bucket fast path: GET/HEAD/DeleteObject
/// must decide NoSuchBucket from the bucket-existence check, before any bucket
/// versioning lookup runs — and cheap request-shape validation still wins over
/// bucket existence for GET.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial"]
async fn nonexistent_bucket_fails_before_versioning_lookup_on_get_head_delete() {
    use super::super::storage_api::test::VERSIONING_CONFIG_LOOKUPS;
    use std::sync::atomic::Ordering;

    let (_disk_paths, _ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    // Never created.
    let bucket = format!("test-missing-{}", &Uuid::new_v4().simple().to_string()[..8]);

    // Request-shape validation keeps precedence over bucket existence: GET with
    // both range and partNumber is InvalidArgument even on a missing bucket.
    let get_invalid = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key("k.txt".to_string())
        .range(Some(Range::Int { first: 0, last: Some(1) }))
        .part_number(Some(1))
        .build()
        .expect("get input should build");
    let err = Box::pin(test_seams::get_object(&usecase, build_request(get_invalid, Method::GET)))
        .await
        .expect_err("range+partNumber must be rejected");
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

    let lookups_before = VERSIONING_CONFIG_LOOKUPS.load(Ordering::SeqCst);

    let get = GetObjectInput::builder()
        .bucket(bucket.clone())
        .key("k.txt".to_string())
        .build()
        .expect("get input should build");
    let err = Box::pin(test_seams::get_object(&usecase, build_request(get, Method::GET)))
        .await
        .expect_err("GET on a missing bucket must fail");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);

    // Bucket existence beats versionId-format validation (which lives in the
    // opts builders) for HEAD and DeleteObject.
    let head = HeadObjectInput::builder()
        .bucket(bucket.clone())
        .key("k.txt".to_string())
        .version_id(Some("not-a-uuid".to_string()))
        .build()
        .expect("head input should build");
    let err = Box::pin(test_seams::head_object(&usecase, build_request(head, Method::HEAD)))
        .await
        .expect_err("HEAD on a missing bucket must fail");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);

    let del = DeleteObjectInput::builder()
        .bucket(bucket.clone())
        .key("k.txt".to_string())
        .version_id(Some("not-a-uuid".to_string()))
        .build()
        .expect("delete input should build");
    let err = Box::pin(usecase.execute_delete_object(build_request(del, Method::DELETE)))
        .await
        .expect_err("DeleteObject on a missing bucket must fail");
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);

    let lookups = VERSIONING_CONFIG_LOOKUPS.load(Ordering::SeqCst) - lookups_before;
    assert_eq!(
        lookups, 0,
        "requests naming a nonexistent bucket must fail before any bucket versioning lookup; got {lookups}"
    );
}
