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

//! Protocol mainflow behavior through the installed server's object use case.
//! These probes exercise the protocol client, not the HTTP transport or authorization hook.

use super::ProtocolStorageClient;
use super::policy_test::protocol_test_context;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::protocols::client::{
    BucketOperations, MakeBucketOptions, ObjectIO, StorageObjectOptions, StoragePutObjReader,
};
use http_body_util::BodyExt;
use rustfs_credentials::Credentials;
use rustfs_protocols::common::client::s3::StorageBackend;
use rustfs_s3_types::S3ErrorCode;

const KEY: &str = "directory/a ?#+%.txt";

async fn object_client(bucket: &str) -> (tempfile::TempDir, Credentials, ProtocolStorageClient, String) {
    let root = Credentials {
        access_key: "protocol-mainflow-root".into(),
        secret_key: "protocol-mainflow-secret".into(),
        status: "on".into(),
        ..Default::default()
    };
    let (temp, context, _iam) = protocol_test_context(root.clone()).await;
    let store = context.object_store();
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let mut reader = StoragePutObjReader::from_vec(b"protocol-data".to_vec());
    let stored = store
        .put_object(bucket, KEY, &mut reader, &StorageObjectOptions::default())
        .await
        .expect("seed object");
    let slot = ServerContextSlot::new();
    assert!(slot.install(context));
    (
        temp,
        root,
        crate::init::protocol_storage_client_for_server(slot),
        stored.etag.expect("stored ETag"),
    )
}

#[test]
#[serial_test::serial]
fn protocol_mainflow_reads_ranges_and_data() {
    crate::app::gating_test_env::run_large_stack_test("protocol-mainflow-read", || async {
        let bucket = "protocol-mainflow-read";
        let (_temp, root, backend, stored_etag) = object_client(bucket).await;
        for (start, expected) in [(None, b"protocol-data".as_slice()), (Some(9), b"data".as_slice())] {
            let output = backend.get_object(bucket, KEY, &root, start).await.expect("protocol GET");
            assert_eq!(output.content_length, Some(expected.len() as i64));
            assert_eq!(output.e_tag.as_ref().expect("GET ETag").opaque_tag(), stored_etag);
            let body = output
                .body
                .expect("GET body")
                .into_body()
                .collect()
                .await
                .expect("consume GET")
                .to_bytes();
            assert_eq!(body.as_ref(), expected);
            assert_eq!(output.version_id, None);
        }
        let ranged = backend
            .get_object_range(bucket, KEY, &root, 2, 4)
            .await
            .expect("bounded range");
        assert_eq!(ranged.content_range.as_deref(), Some("bytes 2-5/13"));
        assert_eq!(
            ranged
                .body
                .expect("range body")
                .into_body()
                .collect()
                .await
                .expect("consume range")
                .to_bytes()
                .as_ref(),
            b"otoc"
        );
    });
}

#[test]
#[serial_test::serial]
fn protocol_mainflow_heads_object_metadata() {
    crate::app::gating_test_env::run_large_stack_test("protocol-mainflow-head", || async {
        let bucket = "protocol-mainflow-head";
        let (_temp, root, backend, stored_etag) = object_client(bucket).await;
        let head = backend.head_object(bucket, KEY, &root).await.expect("protocol HEAD");
        assert_eq!(head.content_length, Some(13));
        assert_eq!(head.e_tag.as_ref().expect("HEAD ETag").opaque_tag(), stored_etag);
        assert_eq!(head.version_id, None);
    });
}

#[test]
#[serial_test::serial]
fn protocol_mainflow_deletes_object() {
    crate::app::gating_test_env::run_large_stack_test("protocol-mainflow-delete", || async {
        let bucket = "protocol-mainflow-delete";
        let (_temp, root, backend, _stored_etag) = object_client(bucket).await;
        let deleted = backend.delete_object(bucket, KEY, &root).await.expect("protocol DELETE");
        assert_eq!(deleted.version_id, None);
        assert_eq!(deleted.delete_marker, Some(false));
        let missing = backend
            .get_object(bucket, KEY, &root, None)
            .await
            .expect_err("deleted object is absent");
        assert_eq!(missing.code(), &S3ErrorCode::NoSuchKey);
    });
}

#[test]
#[serial_test::serial]
fn protocol_mainflow_refuses_missing_objects_buckets_and_unsatisfied_ranges() {
    crate::app::gating_test_env::run_large_stack_test("protocol-mainflow-errors", || async {
        let bucket = "protocol-mainflow-errors";
        let (_temp, root, backend, _stored_etag) = object_client(bucket).await;
        for target in [bucket, "protocol-absent-bucket"] {
            let expected = if target == bucket {
                S3ErrorCode::NoSuchKey
            } else {
                S3ErrorCode::NoSuchBucket
            };
            assert_eq!(
                backend
                    .get_object(target, "absent", &root, None)
                    .await
                    .expect_err("missing GET")
                    .code(),
                &expected
            );
            assert_eq!(
                backend
                    .get_object(target, "absent", &root, Some(1))
                    .await
                    .expect_err("missing open-range GET")
                    .code(),
                &expected
            );
            assert_eq!(
                backend
                    .get_object_range(target, "absent", &root, 1, 1)
                    .await
                    .expect_err("missing bounded-range GET")
                    .code(),
                &expected
            );
            assert_eq!(
                backend
                    .head_object(target, "absent", &root)
                    .await
                    .expect_err("missing HEAD")
                    .code(),
                &expected
            );
        }
        for start in [13, 14, 1024] {
            for error in [
                backend
                    .get_object(bucket, KEY, &root, Some(start))
                    .await
                    .expect_err("open range past end"),
                backend
                    .get_object_range(bucket, KEY, &root, start, 1)
                    .await
                    .expect_err("bounded range past end"),
            ] {
                assert_eq!(error.code(), &S3ErrorCode::InvalidRange);
            }
        }
        assert_eq!(
            backend
                .delete_object("protocol-absent-bucket", KEY, &root)
                .await
                .expect_err("missing DELETE bucket")
                .code(),
            &S3ErrorCode::NoSuchBucket
        );
    });
}
