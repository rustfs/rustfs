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

//! CopyObject tests, including tests that also PUT (batch E owns CopyObject).

use super::common::*;
use super::*;
use crate::app::test_seams;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn copy_object_if_none_match_existing_destination_returns_precondition_failed() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let src_bucket = format!("test-copy-if-none-match-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-copy-if-none-match-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/destination.txt";
    let src_payload = b"conditional copy source payload";
    let dst_payload = b"conditional copy destination payload";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, src_payload).await;
    let _ = upload_test_object(&ecstore, dst_bucket.as_str(), dst_object, dst_payload).await;

    let dst_info = ecstore
        .get_object_info(dst_bucket.as_str(), dst_object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch destination object info");
    let dst_etag = dst_info.etag.expect("destination object should have an ETag");
    let initial_versions = live_object_version_count(&ecstore, dst_bucket.as_str(), dst_object).await;

    for (condition, expected_code) in [
        (ETagCondition::Any, S3ErrorCode::PreconditionFailed),
        (ETagCondition::ETag(ETag::Strong(dst_etag)), S3ErrorCode::NotImplemented),
        (
            ETagCondition::ETag(ETag::Strong("different-etag".to_string())),
            S3ErrorCode::NotImplemented,
        ),
    ] {
        let header = condition.to_http_header().expect("conditional COPY header should encode");
        // Cover header-only callers and the DTO-plus-header shape of parsed HTTP requests.
        for if_none_match in [None, Some(condition)] {
            let has_dto = if_none_match.is_some();
            let copy_input = CopyObjectInput::builder()
                .copy_source(CopySource::Bucket {
                    bucket: src_bucket.clone().into(),
                    key: src_object.to_string().into(),
                    version_id: None,
                })
                .bucket(dst_bucket.clone())
                .key(dst_object.to_string())
                .if_none_match(if_none_match)
                .build()
                .expect("conditional COPY input should build");
            let mut req = build_request(copy_input, Method::PUT);
            req.headers.insert(IF_NONE_MATCH, header.clone());

            let err = Box::pin(usecase.execute_copy_object(req))
                .await
                .expect_err("conditional COPY must reject the replacement");
            assert_eq!(err.code(), &expected_code, "header={header:?}, has_dto={has_dto}");
            assert_eq!(
                read_object_bytes(&ecstore, dst_bucket.as_str(), dst_object).await,
                dst_payload,
                "failed conditional CopyObject must not overwrite the current destination object"
            );
            assert_eq!(
                live_object_version_count(&ecstore, dst_bucket.as_str(), dst_object).await,
                initial_versions,
                "failed conditional CopyObject must not create a version"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state object-version integration test: runs serialized in the CI ILM Integration (serial) lane"]
async fn undo_copy_requires_the_observed_current_version() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();
    let bucket = format!("test-undo-copy-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    create_test_bucket(&ecstore, bucket.as_str()).await;

    let put = |payload: &'static [u8]| {
        PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(streaming_blob_from_bytes(payload)))
            .content_length(Some(payload.len() as i64))
            .build()
            .unwrap()
    };
    let original = Box::pin(test_seams::put_object(&usecase, &fs, build_request(put(b"original"), Method::PUT)))
        .await
        .expect("initial version should be written")
        .output
        .version_id
        .expect("versioned PUT should return a version ID");
    let observed_current = Box::pin(test_seams::put_object(&usecase, &fs, build_request(put(b"replacement"), Method::PUT)))
        .await
        .expect("replacement version should be written")
        .output
        .version_id
        .expect("versioned PUT should return a version ID");

    let copy = || {
        CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object.to_string().into(),
                version_id: Some(original.clone().into()),
            })
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap()
    };
    let mut stale_req = build_request(copy(), Method::PUT);
    stale_req.headers.insert(
        RUSTFS_EXPECTED_CURRENT_VERSION_ID,
        HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
    );
    let versions_before = live_object_version_count(&ecstore, bucket.as_str(), object).await;
    let err = Box::pin(usecase.execute_copy_object(stale_req))
        .await
        .expect_err("stale destination must fail");
    assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), object).await, versions_before);
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, b"replacement");

    let mut matching_req = build_request(copy(), Method::PUT);
    matching_req
        .headers
        .insert(RUSTFS_EXPECTED_CURRENT_VERSION_ID, HeaderValue::from_str(&observed_current).unwrap());
    Box::pin(usecase.execute_copy_object(matching_req))
        .await
        .expect("matching destination version should permit undo copy");
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, b"original");
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), object).await, versions_before + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state object-version integration test: runs serialized in the CI ILM Integration (serial) lane"]
async fn undo_copy_serializes_a_concurrent_put_until_after_commit() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let bucket = format!("test-undo-race-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    create_test_bucket(&ecstore, bucket.as_str()).await;

    let put = |payload: &'static [u8]| {
        PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(streaming_blob_from_bytes(payload)))
            .content_length(Some(payload.len() as i64))
            .build()
            .unwrap()
    };
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();
    let original = Box::pin(test_seams::put_object(&usecase, &fs, build_request(put(b"original"), Method::PUT)))
        .await
        .expect("original should be written")
        .output
        .version_id
        .expect("original should be versioned");
    let observed = Box::pin(test_seams::put_object(&usecase, &fs, build_request(put(b"observed"), Method::PUT)))
        .await
        .expect("observed should be written")
        .output
        .version_id
        .expect("observed should be versioned");
    let encoded_object = encode_dir_object(object);
    let lock = ecstore
        .new_ns_lock(bucket.as_str(), &encoded_object)
        .await
        .expect("namespace lock should be available");
    let blocker = lock
        .get_write_lock(Duration::from_secs(10))
        .await
        .expect("test should acquire blocker");

    let mut undo_req = build_request(
        CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object.to_string().into(),
                version_id: Some(original.into()),
            })
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::PUT,
    );
    undo_req
        .headers
        .insert(RUSTFS_EXPECTED_CURRENT_VERSION_ID, HeaderValue::from_str(&observed).unwrap());

    let undo = tokio::spawn(async move {
        let usecase = DefaultObjectUsecase::from_global();
        Box::pin(usecase.execute_copy_object(undo_req)).await
    });
    tokio::task::yield_now().await;
    let put = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(b"concurrent")))
        .content_length(Some(10))
        .build()
        .unwrap();
    let concurrent_put = tokio::spawn(async move {
        let fs = FS::new();
        let usecase = DefaultObjectUsecase::from_global();
        Box::pin(test_seams::put_object(&usecase, &fs, build_request(put, Method::PUT))).await
    });

    drop(blocker);
    let undo_result = undo.await.expect("undo task should not panic");
    let put_result = concurrent_put.await.expect("PUT task should not panic");
    assert!(put_result.is_ok(), "ordinary PUT should eventually succeed: {put_result:?}");
    if let Err(err) = undo_result {
        assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
    }
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, b"concurrent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn copy_object_allows_new_version_for_locked_destination_but_blocks_explicit_overwrite() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-copy-object-lock-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/destination.txt";
    let locked_payload = b"locked destination payload";
    let source_payload = b"copy source payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");
    let _ = upload_test_object(&ecstore, bucket.as_str(), src_object, source_payload).await;

    let retain_until = Timestamp::from(time::OffsetDateTime::now_utc().saturating_add(time::Duration::days(1)));
    let locked_input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .body(Some(streaming_blob_from_bytes(locked_payload)))
        .content_length(Some(locked_payload.len() as i64))
        .object_lock_mode(Some(ObjectLockMode::from_static(ObjectLockMode::COMPLIANCE)))
        .object_lock_retain_until_date(Some(retain_until))
        .build()
        .unwrap();

    Box::pin(test_seams::put_object(&usecase, &fs, build_request(locked_input, Method::PUT)))
        .await
        .expect("Failed to upload locked destination object");

    let locked_info = ecstore
        .get_object_info(bucket.as_str(), dst_object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch locked destination object info");
    let locked_version_id = locked_info
        .version_id
        .expect("locked destination should have a version ID")
        .to_string();

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    let copy_output = Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
        .await
        .expect("CopyObject should create a new version over a locked current version");
    let copied_version_id = copy_output
        .output
        .version_id
        .expect("versioned CopyObject should return the created version ID");

    assert_ne!(copied_version_id, locked_version_id);
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), dst_object).await, source_payload);
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), dst_object).await, 2);

    let explicit_overwrite_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .version_id(Some(locked_version_id))
        .build()
        .unwrap();

    let err = Box::pin(usecase.execute_copy_object(build_request(explicit_overwrite_input, Method::PUT)))
        .await
        .expect_err("explicit CopyObject overwrite of a locked version should be blocked");

    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    assert_eq!(
        err.message(),
        Some("Invalid argument: A destination versionId requires replication authorization")
    );
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), dst_object).await, source_payload);
    assert_eq!(live_object_version_count(&ecstore, bucket.as_str(), dst_object).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn concurrent_reverse_copy_object_does_not_deadlock_with_reader_locks() {
    temp_env::async_with_vars([(ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
        let (_disk_paths, ecstore) = setup_test_env().await;

        let bucket = format!("test-reverse-copy-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object_a = "test/a.bin";
        let object_b = "test/b.bin";
        let payload_a = vec![b'a'; 4 * 1024 * 1024];
        let payload_b = vec![b'b'; 4 * 1024 * 1024];

        create_test_bucket(&ecstore, bucket.as_str()).await;
        let _ = upload_test_object(&ecstore, bucket.as_str(), object_a, &payload_a).await;
        let _ = upload_test_object(&ecstore, bucket.as_str(), object_b, &payload_b).await;

        let a_to_b_input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object_a.to_string().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(object_b.to_string())
            .build()
            .unwrap();
        let b_to_a_input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: object_b.to_string().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(object_a.to_string())
            .build()
            .unwrap();

        let start = Arc::new(Barrier::new(3));
        let start_a = start.clone();
        let a_to_b = tokio::spawn(async move {
            start_a.wait().await;
            let usecase = DefaultObjectUsecase::from_global();
            Box::pin(usecase.execute_copy_object(build_request(a_to_b_input, Method::PUT))).await
        });
        let start_b = start.clone();
        let b_to_a = tokio::spawn(async move {
            start_b.wait().await;
            let usecase = DefaultObjectUsecase::from_global();
            Box::pin(usecase.execute_copy_object(build_request(b_to_a_input, Method::PUT))).await
        });

        start.wait().await;

        let (a_to_b_result, b_to_a_result) = tokio::time::timeout(Duration::from_secs(10), async {
            let (a_to_b_result, b_to_a_result) = tokio::join!(a_to_b, b_to_a);
            (
                a_to_b_result.expect("A-to-B CopyObject task should not panic"),
                b_to_a_result.expect("B-to-A CopyObject task should not panic"),
            )
        })
        .await
        .expect("reverse CopyObject operations should not deadlock");

        a_to_b_result.expect("A-to-B CopyObject should succeed");
        b_to_a_result.expect("B-to-A CopyObject should succeed");

        let final_a = read_object_bytes(&ecstore, bucket.as_str(), object_a).await;
        let final_b = read_object_bytes(&ecstore, bucket.as_str(), object_b).await;
        assert!(
            final_a == payload_a || final_a == payload_b,
            "object A must contain a complete copied payload"
        );
        assert!(
            final_b == payload_a || final_b == payload_b,
            "object B must contain a complete copied payload"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_and_copy_object_transition_immediately_via_usecases() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let put_bucket = format!("test-api-put-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let put_object = "test/object.txt";
    let put_payload = b"Hello, immediate transition through put API!";

    create_test_bucket(&ecstore, put_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(put_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let put_input = PutObjectInput::builder()
        .bucket(put_bucket.clone())
        .key(put_object.to_string())
        .body(Some(streaming_blob_from_bytes(put_payload)))
        .content_length(Some(put_payload.len() as i64))
        .build()
        .unwrap();

    Box::pin(test_seams::put_object(&usecase, &fs, build_request(put_input, Method::PUT)))
        .await
        .expect("Failed to put object through usecase");

    let put_info = wait_for_transition(&ecstore, put_bucket.as_str(), put_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition immediately after put usecase");

    assert_eq!(put_info.transitioned_object.status, "complete");
    assert_eq!(put_info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&put_info.transitioned_object.name).await);

    let src_bucket = format!("test-api-copy-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-api-copy-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/copied.txt";
    let copy_payload = b"copy object immediate transition through copy API";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(dst_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set destination lifecycle configuration");
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, copy_payload).await;

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: src_bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(dst_bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
        .await
        .expect("Failed to copy object through usecase");

    let copy_info = wait_for_transition(&ecstore, dst_bucket.as_str(), dst_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("copied object should transition immediately after copy usecase");

    assert_eq!(copy_info.transitioned_object.status, "complete");
    assert_eq!(copy_info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&copy_info.transitioned_object.name).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn compensation_driven_copy_still_completes_transition() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let src_bucket = format!("test-comp-copy-src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let dst_bucket = format!("test-comp-copy-dst-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "test/source.txt";
    let dst_object = "test/copied.txt";
    let payload = b"copy object should still transition after compensation";

    create_test_bucket(&ecstore, src_bucket.as_str()).await;
    create_test_bucket(&ecstore, dst_bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(dst_bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set destination lifecycle configuration");
    let _ = upload_test_object(&ecstore, src_bucket.as_str(), src_object, payload).await;

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: src_bucket.clone().into(),
            key: src_object.to_string().into(),
            version_id: None,
        })
        .bucket(dst_bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();

    with_forced_immediate_enqueue_timeout(&backend, || async {
        Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
            .await
            .expect("Failed to copy object through usecase");
    })
    .await;

    let info = wait_for_transition(&ecstore, dst_bucket.as_str(), dst_object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("copied object should eventually transition after compensation backfill");

    assert_eq!(backend.put_count().await, 1, "compensation should upload the copied version once");
    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&info.transitioned_object.name).await);
    assert_eq!(read_object_bytes(&ecstore, dst_bucket.as_str(), dst_object).await, payload);
}

/// CopyObject creates a new object on the destination key, so it must join
/// bucket replication exactly like PutObject: compute one replication decision
/// that drives both the persisted pending marker and the post-commit schedule
/// (MinIO CopyObjectHandler parity). A count of 0 means a copied object is
/// never scheduled for replication and — because the copy path clones the
/// source metadata wholesale — the destination silently inherits the source's
/// replication bookkeeping, faking a COMPLETED/REPLICA state for an object
/// that never replicated. The second half of this test pins the metadata
/// cleanup (MinIO filterReplicationStatusMetadata parity).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state usecase integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn copy_object_computes_replication_decision_and_strips_stale_status() {
    use super::super::storage_api::object_usecase::bucket::replication::MUST_REPLICATE_OBJECT_CALLS;
    use rustfs_utils::http::{
        SUFFIX_REPLICA_STATUS, SUFFIX_REPLICA_TIMESTAMP, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, get_str,
        insert_str,
    };
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-copy-repl-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let src_object = "copy/source.txt";
    let dst_object = "copy/destination.txt";
    let payload = b"copy replication decision payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;

    // Seed the source object with stale replication bookkeeping, as if it had
    // already replicated elsewhere (internal status/timestamp under both
    // compatibility prefixes, replica state, and the surfaced x-amz header).
    let mut stale_metadata = HashMap::new();
    insert_str(
        &mut stale_metadata,
        SUFFIX_REPLICATION_STATUS,
        "arn:minio:replication::stale:dst=COMPLETED;".to_string(),
    );
    insert_str(&mut stale_metadata, SUFFIX_REPLICATION_TIMESTAMP, "2024-01-01T00:00:00Z".to_string());
    insert_str(&mut stale_metadata, SUFFIX_REPLICA_STATUS, "REPLICA".to_string());
    insert_str(&mut stale_metadata, SUFFIX_REPLICA_TIMESTAMP, "2024-01-01T00:00:00Z".to_string());
    stale_metadata.insert(AMZ_BUCKET_REPLICATION_STATUS.to_string(), "COMPLETED".to_string());
    let mut reader = PutObjReader::from_vec(payload.to_vec());
    (*ecstore)
        .put_object(
            bucket.as_str(),
            src_object,
            &mut reader,
            &ObjectOptions {
                user_defined: stale_metadata,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to upload source object with stale replication metadata");

    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);

    let copy_input = CopyObjectInput::builder()
        .copy_source(CopySource::Bucket {
            bucket: bucket.clone().into(),
            key: src_object.into(),
            version_id: None,
        })
        .bucket(bucket.clone())
        .key(dst_object.to_string())
        .build()
        .unwrap();
    Box::pin(usecase.execute_copy_object(build_request(copy_input, Method::PUT)))
        .await
        .expect("Failed to copy object through usecase");

    assert_eq!(
        MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst),
        1,
        "CopyObject must compute the replication decision exactly once; 0 means the copied \
         object never enters bucket replication (P0-6, MinIO CopyObjectHandler parity)"
    );

    let copied = ecstore
        .get_object_info(bucket.as_str(), dst_object, &ObjectOptions::default())
        .await
        .expect("Failed to read copied destination object info");
    assert!(
        get_str(&copied.user_defined, SUFFIX_REPLICATION_STATUS).is_none(),
        "destination must not inherit the source's replication status; got {:?}",
        get_str(&copied.user_defined, SUFFIX_REPLICATION_STATUS)
    );
    assert!(
        get_str(&copied.user_defined, SUFFIX_REPLICATION_TIMESTAMP).is_none(),
        "destination must not inherit the source's replication timestamp"
    );
    assert!(
        get_str(&copied.user_defined, SUFFIX_REPLICA_STATUS).is_none(),
        "destination must not inherit the source's replica status"
    );
    assert!(
        !copied
            .user_defined
            .keys()
            .any(|k| k.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS)),
        "destination must not inherit the surfaced x-amz-replication-status header key"
    );
}
