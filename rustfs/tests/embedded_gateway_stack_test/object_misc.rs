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

//! Refusal-matrix rows and behaviour of migration batch E2 (rustfs/backlog#2765): DeleteObject.

use super::bucket_configs::StatusCapture;
use super::{
    GATEWAY, LEGACY, MatrixRow, NO_POLICY_USER, Outcome, USER_SECRET_KEY, anonymous_client, create_bucket_in_the_store,
    create_user_without_a_policy, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::error::is_err_object_not_found;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{ObjectIO as _, ObjectOperations as _};

pub(crate) const MATRIX: &[MatrixRow] = &[MatrixRow {
    operation: "DeleteObject",
    metric: S3Operation::DeleteObject,
    has_error_body: true,
    call: delete_object,
}];

pub(super) const MATRIX_BUCKET: &str = "e2-delete-matrix";

fn delete_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_object().bucket(MATRIX_BUCKET).key("matrix-absent").send().await) })
}

const DELETE_BUCKET: &str = "e2-delete-object";
/// A bucket name RustFS creates and the AWS grammar refuses (`sthree-` is reserved).
const RESERVED_NAME_BUCKET: &str = "sthree-e2-delete";

async fn put_in_the_store(bucket: &str, key: &str) {
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .put_object(bucket, key, &mut PutObjReader::from_vec(b"e2".to_vec()), &ObjectOptions::default())
        .await
        .expect("put object in the store");
}

async fn is_in_the_store(bucket: &str, key: &str) -> bool {
    match get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .get_object_info(bucket, key, &ObjectOptions::default())
        .await
    {
        Ok(_) => true,
        Err(error) if is_err_object_not_found(&error) => false,
        Err(error) => panic!("object lookup failed: {error}"),
    }
}

/// DeleteObject refuses whoever it must refuse, with the legacy code and before touching the
/// object, then deletes for root and answers the delete-marker header. A key with a NUL byte is
/// `400 InvalidArgument` and a key over 1024 bytes `400 KeyTooLongError` on both stacks, the
/// legacy answers; `nul_key_message` is the wording of the first where the stack's is pinned.
async fn delete_object_refuses_then_deletes(server: RustFSServer, nul_key_message: Option<&'static str>) {
    create_bucket_in_the_store(DELETE_BUCKET).await;
    create_bucket_in_the_store(RESERVED_NAME_BUCKET).await;
    create_user_without_a_policy(NO_POLICY_USER).await;
    put_in_the_store(DELETE_BUCKET, "doomed").await;
    put_in_the_store(RESERVED_NAME_BUCKET, "doomed").await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let no_policy = s3_client(&endpoint, NO_POLICY_USER, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    for (caller, client) in [("anonymous", &anonymous), ("no policy", &no_policy)] {
        let error = client
            .delete_object()
            .bucket(DELETE_BUCKET)
            .key("doomed")
            .send()
            .await
            .expect_err(caller);
        assert_eq!(refusal(&error), (Some("AccessDenied".to_owned()), Some(403)), "{caller}");
    }
    assert!(is_in_the_store(DELETE_BUCKET, "doomed").await, "a refused delete removed the object");

    // Exercise a missing bucket.
    let error = no_policy
        .delete_object()
        .bucket("e2-no-such-bucket")
        .key("doomed")
        .send()
        .await
        .expect_err("missing bucket");
    assert_eq!(refusal(&error), (Some("NoSuchBucket".to_owned()), Some(404)));

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("doomed")
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "maybe");
        })
        .send()
        .await
        .expect_err("invalid force-delete header");
    assert_eq!(refusal(&error), (Some("InvalidRequest".to_owned()), Some(400)));
    assert!(is_in_the_store(DELETE_BUCKET, "doomed").await, "a refused delete removed the object");

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("bad\0key")
        .send()
        .await
        .expect_err("NUL key");
    assert_eq!(refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
    if let Some(message) = nul_key_message {
        assert_eq!(error.message(), Some(message));
    }

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("k".repeat(1025))
        .send()
        .await
        .expect_err("a key over 1024 bytes");
    assert_eq!(refusal(&error), (Some("KeyTooLongError".to_owned()), Some(400)));

    let status = StatusCapture::default();
    root.delete_object()
        .bucket(DELETE_BUCKET)
        .key("absent")
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("delete absent object");
    assert_eq!(status.take(), Some(204), "no-op delete status");

    for bucket in [DELETE_BUCKET, RESERVED_NAME_BUCKET] {
        let output = root
            .delete_object()
            .bucket(bucket)
            .key("doomed")
            .send()
            .await
            .unwrap_or_else(|error| panic!("root deletes from {bucket}: {error:?}"));
        assert_eq!(output.delete_marker(), Some(false), "{bucket}");
        assert!(!is_in_the_store(bucket, "doomed").await, "{bucket}: the object survived its delete");
    }

    if nul_key_message.is_some() {
        const LOCK_BUCKET: &str = "e2-delete-locked";
        root.create_bucket()
            .bucket(LOCK_BUCKET)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await
            .expect("create lock bucket");
        for bucket in [DELETE_BUCKET, RESERVED_NAME_BUCKET, LOCK_BUCKET, "e2-no-such-bucket"] {
            for version in [None, Some("invalid-version")] {
                let too_long = root
                    .delete_object()
                    .bucket(bucket)
                    .key("k".repeat(1025))
                    .set_version_id(version.map(str::to_owned))
                    .send()
                    .await
                    .expect_err("key too long");
                assert_eq!(refusal(&too_long), (Some("KeyTooLongError".to_owned()), Some(400)));
                assert_eq!(too_long.message(), Some("Your key is too long."));
                let error = root
                    .delete_object()
                    .bucket(bucket)
                    .key("bad\0key")
                    .set_version_id(version.map(str::to_owned))
                    .send()
                    .await
                    .expect_err("invalid key");
                if bucket == "e2-no-such-bucket" {
                    assert_eq!(refusal(&error), (Some("NoSuchBucket".to_owned()), Some(404)));
                    assert_eq!(error.message(), Some("The specified bucket does not exist"));
                } else {
                    assert_eq!(refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
                    assert_eq!(error.message(), Some("Object key contains invalid control characters: \"bad\\0key\""));
                }
            }
            let invalid_version = root
                .delete_object()
                .bucket(bucket)
                .key("normal")
                .version_id("invalid-version")
                .send()
                .await
                .expect_err("invalid version");
            let expected = if bucket == "e2-no-such-bucket" {
                ("NoSuchBucket", 404, "The specified bucket does not exist")
            } else {
                ("InvalidArgument", 400, "Invalid argument")
            };
            assert_eq!(refusal(&invalid_version), (Some(expected.0.to_owned()), Some(expected.1)));
            assert_eq!(invalid_version.message(), Some(expected.2));
        }
    }
    server.shutdown().await;
}

/// The gateway words its own refusal of a NUL key; the code and the status are what the stacks share.
#[test]
fn the_gateway_stack_refuses_then_deletes_an_object() {
    with_stack(GATEWAY, |server| delete_object_refuses_then_deletes(server, None));
}

/// The legacy edge preserves the key validation result and error ordering.
#[test]
fn the_legacy_stack_refuses_then_deletes_an_object() {
    with_stack(LEGACY, |server| {
        delete_object_refuses_then_deletes(server, Some("Object key contains invalid control characters: \"bad\\0key\""))
    });
}
