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

//! PutObject tests (batch D owns PutObject).

use super::common::*;
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_object_if_none_match_existing_object_returns_precondition_failed() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-put-if-none-match-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let initial_payload = b"initial conditional put payload";
    let replacement_payload = b"replacement conditional put payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;

    let initial_input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(initial_payload)))
        .content_length(Some(initial_payload.len() as i64))
        .build()
        .unwrap();
    Box::pin(usecase.execute_put_object(&fs, build_request(initial_input, Method::PUT)))
        .await
        .expect("Failed to upload initial object through usecase");

    let existing_info = ecstore
        .get_object_info(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to fetch existing object info");
    let existing_etag = existing_info.etag.expect("existing object should have an ETag");
    let initial_versions = live_object_version_count(&ecstore, bucket.as_str(), object).await;

    for (condition, expected_code) in [
        (ETagCondition::Any, S3ErrorCode::PreconditionFailed),
        (ETagCondition::ETag(ETag::Strong(existing_etag)), S3ErrorCode::NotImplemented),
        (
            ETagCondition::ETag(ETag::Strong("different-etag".to_string())),
            S3ErrorCode::NotImplemented,
        ),
    ] {
        let header = condition.to_http_header().expect("conditional PUT header should encode");
        // Cover header-only callers and the DTO-plus-header shape of parsed HTTP requests.
        for if_none_match in [None, Some(condition)] {
            let has_dto = if_none_match.is_some();
            let replacement_input = PutObjectInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .body(Some(streaming_blob_from_bytes(replacement_payload)))
                .content_length(Some(replacement_payload.len() as i64))
                .if_none_match(if_none_match)
                .build()
                .expect("conditional replacement PUT input should build");
            let mut req = build_request(replacement_input, Method::PUT);
            req.headers.insert(IF_NONE_MATCH, header.clone());

            let err = Box::pin(usecase.execute_put_object(&fs, req))
                .await
                .expect_err("conditional PUT must reject the replacement");
            assert_eq!(err.code(), &expected_code, "header={header:?}, has_dto={has_dto}");
            assert_eq!(
                read_object_bytes(&ecstore, bucket.as_str(), object).await,
                initial_payload,
                "failed conditional PutObject must not overwrite the current object"
            );
            assert_eq!(
                live_object_version_count(&ecstore, bucket.as_str(), object).await,
                initial_versions,
                "failed conditional PutObject must not create a version"
            );
        }
    }
}

/// rustfs/backlog#1320: a single PUT must compute the replication decision
/// exactly once. Historically the PUT path computed `must_replicate_object`
/// twice with the same inputs — once before commit to persist the pending
/// replication metadata, and again after commit to drive the schedule. Besides
/// repeating the versioning/config/target traversal on the hot path, a
/// replication-config hot update between the two computations could split the
/// two phases (pending-without-schedule or schedule-without-pending). The fix
/// reuses one immutable decision for both phases; this test observes the
/// computation counter and fails if a second computation is reintroduced.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state usecase integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_object_computes_replication_decision_exactly_once() {
    use super::super::storage_api::object_usecase::bucket::replication::MUST_REPLICATE_OBJECT_CALLS;
    use std::sync::atomic::Ordering;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let fs = FS::new();
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-repl-decision-once-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"replication decision single-compute payload";

    create_test_bucket(&ecstore, bucket.as_str()).await;

    // Reset immediately before the single PUT so the counter reflects only this
    // request. `#[serial]` guarantees no other usecase PUT runs concurrently, so
    // no unrelated `must_replicate_object` call can perturb the count.
    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);

    let input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .body(Some(streaming_blob_from_bytes(payload)))
        .content_length(Some(payload.len() as i64))
        .build()
        .unwrap();
    Box::pin(usecase.execute_put_object(&fs, build_request(input, Method::PUT)))
        .await
        .expect("Failed to upload object through usecase");

    let calls = MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst);
    assert_eq!(
        calls, 1,
        "a single PUT must compute the replication decision exactly once; reverting to a \
         pre-commit + post-commit recompute makes this observe 2 (rustfs/backlog#1320)"
    );
}

/// Snowball auto-extract writes each archive member as an independent object,
/// so each member must join bucket replication like a regular PUT (MinIO
/// PutObjectExtract parity). The archive holds two file entries; a count other
/// than 2 means extracted objects bypass the replication decision entirely and
/// stay local forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state usecase integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_object_extract_computes_replication_decision_per_entry() {
    use super::super::storage_api::object_usecase::bucket::replication::MUST_REPLICATE_OBJECT_CALLS;
    use super::super::storage_api::test::ReqInfo;
    use super::super::storage_api::test::bucket::metadata::BUCKET_POLICY_CONFIG;
    use std::io::Cursor;
    use std::sync::atomic::Ordering;

    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let bucket = format!("test-extract-repl-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;

    // The extract path re-authorizes every extracted entry internally. The test
    // harness has no IAM system, so allow anonymous PutObject via bucket policy.
    let policy_json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:PutObject"],
                "Resource": [format!("arn:aws:s3:::{bucket}/*")]
            }
        ]
    })
    .to_string();
    metadata_sys::update(bucket.as_str(), BUCKET_POLICY_CONFIG, policy_json.into_bytes())
        .await
        .expect("Failed to install anonymous put bucket policy");

    let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
    for (path, data) in [
        ("member-one.txt", b"first member payload".as_slice()),
        ("nested/member-two.txt", b"second member payload".as_slice()),
    ] {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, path, Cursor::new(data))
            .await
            .expect("Failed to append archive member");
    }
    let archive = builder.into_inner().await.expect("Failed to finish archive").into_inner();

    MUST_REPLICATE_OBJECT_CALLS.store(0, Ordering::SeqCst);

    let input = PutObjectInput::builder()
        .bucket(bucket.clone())
        .key("archive.tar".to_string())
        .body(Some(streaming_blob_from_bytes(&archive)))
        .content_length(Some(archive.len() as i64))
        .build()
        .unwrap();
    let mut req = build_request(input, Method::PUT);
    req.extensions.insert(ReqInfo::default());
    Box::pin(usecase.execute_put_object_extract(req))
        .await
        .expect("Failed to extract archive through usecase");

    assert_eq!(
        MUST_REPLICATE_OBJECT_CALLS.load(Ordering::SeqCst),
        2,
        "snowball auto-extract must compute one replication decision per extracted member \
         object; 0 means extracted objects bypass bucket replication (P0-6 companion, MinIO \
         PutObjectExtract parity)"
    );
}
