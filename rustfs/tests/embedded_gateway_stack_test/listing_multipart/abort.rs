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

//! AbortMultipartUpload wire responses and staged part removal.

use super::super::{
    ENV_S3_STACK, GATEWAY, LEGACY, Outcome, ROOT_ACCESS_KEY, ROOT_SECRET_KEY, USER_SECRET_KEY, common,
    create_bucket_in_the_store, create_user_allowed_on_one_bucket, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_ecstore::api::disk::RUSTFS_META_MULTIPART_BUCKET;
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::MultipartOperations as _;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const BUCKET: &str = "abort-multipart-wire";
const OTHER: &str = "abort-multipart-other";
const KEY: &str = "directory/object";
const DENIED: &str = "other-object";
const USER: &str = "abortpartscoped";

pub(super) fn matrix_abort<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let store = get_global_app_context().expect("embedded server context").object_store();
        let key = "matrix-abort";
        let opts = Default::default();
        let upload = store
            .new_multipart_upload(bucket, key, &opts)
            .await
            .expect("stage a matrix upload");
        let result = outcome(
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload.upload_id)
                .send()
                .await,
        );
        if result.is_err() {
            store
                .abort_multipart_upload(bucket, key, &upload.upload_id, &opts)
                .await
                .expect("remove matrix fixture");
        }
        result
    })
}

fn operation_count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == S3Operation::AbortMultipartUpload.as_str())
        .map_or(0, |row| row.total)
}

async fn stage(client: &Client, bucket: &str, key: &str) -> (String, String) {
    let initiated = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("initiate fixture");
    let id = initiated.upload_id().expect("fixture UploadId").to_owned();
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"real staged part"))
        .send()
        .await
        .expect("upload fixture part");
    (id, part.e_tag().expect("fixture ETag").to_owned())
}

fn staged_part(volume: &Path, bucket: &str, key: &str) -> PathBuf {
    let key = rustfs_utils::path::encode_dir_object_ref(key);
    let hash = hex_simd::encode_to_string(Sha256::digest(format!("{bucket}/{key}")), hex_simd::AsciiCase::Lower);
    let path = volume.join(RUSTFS_META_MULTIPART_BUCKET).join(hash);
    let children = |path: &Path| {
        std::fs::read_dir(path)
            .expect("read staging directory")
            .map(|entry| entry.expect("read staging entry").path())
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>()
    };
    let parts: Vec<_> = children(&path)
        .into_iter()
        .flat_map(|session| children(&session))
        .map(|data| data.join("part.1"))
        .filter(|path| path.is_file())
        .collect();
    assert_eq!(parts.len(), 1, "{bucket}/{key} must have one physical part");
    parts.into_iter().next().expect("physical part path")
}

async fn abort(client: &Client, bucket: &str, key: &str, id: &str, expected: Option<(&str, u16)>) {
    let abort = client.abort_multipart_upload().bucket(bucket).key(key);
    let result = if id.is_empty() {
        // The SDK rejects empty UploadIds before sending a request.
        abort
            .upload_id("placeholder")
            .customize()
            .mutate_request(|request| {
                let (path, _) = request.uri().split_once('?').expect("serialized Abort query");
                request.set_uri(format!("{path}?uploadId=")).expect("empty uploadId URI");
            })
            .send()
            .await
    } else {
        abort.upload_id(id).send().await
    };
    if let Some((code, status)) = expected {
        let error = result.expect_err("Abort must be refused");
        assert_eq!(refusal(&error), (Some(code.to_owned()), Some(status)), "{bucket}/{key}: {error:?}");
        let raw = error.raw_response().expect("HTTP response");
        assert!(raw.headers().get("x-amz-request-id").is_some());
        assert!(raw.headers().get("x-request-id").is_some());
        if code == "NoSuchUpload" {
            let message = if id == "bad-id" {
                "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
            } else {
                "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
            };
            assert_eq!(error.message(), Some(message), "{bucket}/{key}");
        }
    } else {
        result.expect("Abort should remove the current upload");
    }
}

fn wire_contract(stack: Option<&'static str>) {
    temp_env::with_vars([(ENV_S3_STACK, stack)], || {
        common::run_embedded_test(move || async move {
            let volume = tempfile::tempdir().expect("isolated data volume");
            let server = RustFSServerBuilder::new()
                .address(format!("127.0.0.1:{}", find_available_port().expect("local port")))
                .access_key(ROOT_ACCESS_KEY)
                .secret_key(ROOT_SECRET_KEY)
                .volume(volume.path().to_string_lossy())
                .build()
                .await
                .expect("start embedded server");
            for bucket in [BUCKET, OTHER] {
                create_bucket_in_the_store(bucket).await;
            }
            let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
            let (id, _) = stage(&root, BUCKET, KEY).await;
            let physical = staged_part(volume.path(), BUCKET, KEY);

            let before = operation_count();
            abort(&root, BUCKET, KEY, &id, None).await;
            assert_eq!(operation_count(), before + 1);
            assert!(!physical.exists(), "successful Abort removed the physical staged part");
            let store = get_global_app_context().expect("embedded server context").object_store();
            assert!(
                store.get_multipart_info(BUCKET, KEY, &id, &Default::default()).await.is_err(),
                "the store no longer sees the upload"
            );
            abort(&root, BUCKET, KEY, &id, Some(("NoSuchUpload", 404))).await;
            abort(&root, BUCKET, KEY, "bad-id", Some(("NoSuchUpload", 404))).await;

            let directory = "directory/marker/";
            let initiated = root
                .create_multipart_upload()
                .bucket(BUCKET)
                .key(directory)
                .send()
                .await
                .expect("directory upload");
            let id = initiated.upload_id().expect("directory upload ID");
            abort(&root, BUCKET, directory, id, None).await;

            let completed_key = "completed";
            let (completed_id, etag) = stage(&root, BUCKET, completed_key).await;
            let part = aws_sdk_s3::types::CompletedPart::builder().part_number(1).e_tag(etag).build();
            root.complete_multipart_upload()
                .bucket(BUCKET)
                .key(completed_key)
                .upload_id(&completed_id)
                .multipart_upload(aws_sdk_s3::types::CompletedMultipartUpload::builder().parts(part).build())
                .send()
                .await
                .expect("complete fixture");
            abort(&root, BUCKET, completed_key, &completed_id, Some(("NoSuchUpload", 404))).await;

            let (id, _) = stage(&root, BUCKET, KEY).await;
            let physical = staged_part(volume.path(), BUCKET, KEY);
            let before = operation_count();
            let cap_error = root
                .abort_multipart_upload()
                .bucket(BUCKET)
                .key(KEY)
                .upload_id(&id)
                .customize()
                .mutate_request(|req| {
                    let uri = format!("{}&x-rustfs-max-total-object-size=17&x-rustfs-max-content-length=12", req.uri());
                    req.set_uri(uri).expect("capability query URI");
                })
                .send()
                .await
                .expect_err("Abort cannot take an upload capability");
            assert_eq!(refusal(&cap_error), (Some("InvalidRequest".to_owned()), Some(400)));
            assert!(
                cap_error
                    .message()
                    .is_some_and(|message| message.contains("x-rustfs-max-content-length"))
            );
            assert_eq!(operation_count(), before);
            assert!(physical.exists(), "refused capability preserved the physical part");

            let (denied_id, _) = stage(&root, BUCKET, DENIED).await;
            let denied_part = staged_part(volume.path(), BUCKET, DENIED);
            let (other_id, _) = stage(&root, OTHER, KEY).await;
            let other_part = staged_part(volume.path(), OTHER, KEY);
            for (bucket, key) in [(BUCKET, DENIED), (OTHER, KEY)] {
                abort(&root, bucket, key, &id, Some(("NoSuchUpload", 404))).await;
            }
            assert!(physical.exists() && denied_part.exists() && other_part.exists());

            create_user_allowed_on_one_bucket(USER, "s3:AbortMultipartUpload", &format!("{BUCKET}/{KEY}")).await;
            let scoped = s3_client(&server.endpoint(), USER, USER_SECRET_KEY);
            for (bucket, key, session, part) in [
                (BUCKET, DENIED, &denied_id, &denied_part),
                (OTHER, KEY, &other_id, &other_part),
            ] {
                let before = operation_count();
                abort(&scoped, bucket, key, session, Some(("AccessDenied", 403))).await;
                assert_eq!(operation_count(), before);
                assert!(part.exists(), "denied Abort preserved the physical staged part");
            }
            abort(&scoped, BUCKET, KEY, &id, None).await;
            assert!(!physical.exists());
            assert!(denied_part.exists() && other_part.exists());
            server.shutdown().await;
        });
    });
}

#[test]
#[serial_test::serial]
fn legacy_abort_multipart_fixed_main_contract() {
    wire_contract(LEGACY);
}

#[test]
#[serial_test::serial]
fn gateway_abort_multipart_wire_contract() {
    wire_contract(GATEWAY);
}

async fn empty_upload_id(server: rustfs::embedded::RustFSServer) {
    create_bucket_in_the_store(BUCKET).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    abort(&client, BUCKET, KEY, "", Some(("NoSuchUpload", 404))).await;
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn legacy_abort_multipart_empty_upload_id_returns_no_such_upload() {
    with_stack(LEGACY, empty_upload_id);
}

#[test]
#[serial_test::serial]
fn gateway_abort_multipart_empty_upload_id_returns_no_such_upload() {
    with_stack(GATEWAY, empty_upload_id);
}
