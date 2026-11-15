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

//! Refusal-matrix rows and behaviour of DeleteObject and RestoreObject.

use super::bucket_configs::StatusCapture;
use super::{
    GATEWAY, LEGACY, MatrixRow, NO_POLICY_USER, Outcome, USER_SECRET_KEY, anonymous_client, create_bucket_in_the_store,
    create_user_without_a_policy, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, RestoreRequest, VersioningConfiguration};
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::error::is_err_object_not_found;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_ecstore::api::runtime::global_tier_config_mgr;
use rustfs_ecstore::api::tier::test_util::{MockWarmBackend, register_mock_tier};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{ObjectIO as _, ObjectOperations as _};

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "DeleteObject",
        metric: S3Operation::DeleteObject,
        has_error_body: true,
        call: delete_object,
    },
    MatrixRow {
        operation: "RestoreObject",
        metric: S3Operation::RestoreObject,
        has_error_body: true,
        call: restore_object,
    },
];

pub(super) const MATRIX_BUCKET: &str = "e2-delete-matrix";

fn delete_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_object().bucket(MATRIX_BUCKET).key("matrix-absent").send().await) })
}

const RESTORE_KEY: &str = "archive";
const RESTORE_TIER: &str = "RESTORETIER";

fn restore_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(
            client
                .restore_object()
                .bucket(MATRIX_BUCKET)
                .key(RESTORE_KEY)
                .restore_request(RestoreRequest::builder().days(1).build())
                .send()
                .await,
        )
    })
}

pub(super) async fn seed_restore_object(bucket: &str, versioned: bool) -> (MockWarmBackend, Option<String>) {
    let backend = register_mock_tier(&global_tier_config_mgr(), RESTORE_TIER).await;
    let store = get_global_app_context().expect("embedded server context").object_store();
    let uploaded = store
        .put_object(
            bucket,
            RESTORE_KEY,
            &mut PutObjReader::from_vec(b"archived bytes".to_vec()),
            &ObjectOptions {
                versioned,
                ..Default::default()
            },
        )
        .await
        .expect("write restore fixture");
    let mut opts = ObjectOptions {
        version_id: uploaded.version_id.map(|version| version.to_string()),
        versioned,
        mod_time: uploaded.mod_time,
        ..Default::default()
    };
    opts.transition.status = "pending".to_owned();
    opts.transition.tier = RESTORE_TIER.to_owned();
    opts.transition.etag = uploaded.etag.unwrap_or_default();
    store
        .transition_object(bucket, RESTORE_KEY, &opts)
        .await
        .expect("transition restore fixture");
    let archived = store
        .get_object_info(bucket, RESTORE_KEY, &ObjectOptions::default())
        .await
        .expect("read transitioned fixture");
    assert_eq!(archived.transitioned_object.status, "complete");
    (backend, opts.version_id)
}

async fn restore_admits_once_and_renews_in_place(server: RustFSServer) {
    const BUCKET: &str = "e2-restore";
    create_bucket_in_the_store(BUCKET).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable fixture versioning");
    let (backend, version) = seed_restore_object(BUCKET, true).await;
    let version = version.expect("archive has a UUID version");
    client
        .put_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .body(ByteStream::from_static(b"current before restore"))
        .send()
        .await
        .expect("create a distinct current version before restore");
    let restore = |days| {
        client
            .restore_object()
            .bucket(BUCKET)
            .key(RESTORE_KEY)
            .version_id(&version)
            .restore_request(RestoreRequest::builder().days(days).build())
    };
    client.put_bucket_policy().bucket(BUCKET).policy(format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":"*","Action":"s3:RestoreObject","Resource":"arn:aws:s3:::{BUCKET}/{RESTORE_KEY}"}}]}}"#
    )).send().await.expect("set explicit RestoreObject deny");
    let denied = outcome(restore(1).send().await);
    let untouched = client
        .head_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .version_id(&version)
        .send()
        .await
        .expect("head denied fixture");
    assert_eq!(
        (denied.err().map(|error| (error.status, error.code)), untouched.restore()),
        (Some((Some(403), Some("AccessDenied".to_owned()))), None),
        "denied restore changed admission"
    );
    assert_eq!(backend.get_count().await, 0, "denial triggered a tier read");
    client
        .delete_bucket_policy()
        .bucket(BUCKET)
        .send()
        .await
        .expect("remove explicit deny");

    let get_barrier = backend.arm_get_barrier().await;
    let status = StatusCapture::default();
    let admitted = restore(1)
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("restore admission");
    assert_eq!(status.take(), Some(200));
    assert_eq!(admitted.request_charged().map(|value| value.as_str()), Some("requester"));
    assert_eq!(admitted.restore_output_path(), None);
    get_barrier.wait_until_paused().await;
    let conflict = restore(1).send().await.expect_err("duplicate ongoing restore");
    assert_eq!(refusal(&conflict), (Some("RestoreAlreadyInProgress".to_owned()), Some(409)));
    let current = client
        .put_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .body(ByteStream::from_static(b"intervening PUT"))
        .send()
        .await
        .expect("PUT while copy-back is reading the tier");
    let replacement_version = current.version_id().expect("replacement UUID version");
    get_barrier.release();

    let before = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let head = client
                .head_object()
                .bucket(BUCKET)
                .key(RESTORE_KEY)
                .version_id(&version)
                .send()
                .await
                .expect("head restoring object");
            if let Some(header) = head
                .restore()
                .filter(|header| header.starts_with("ongoing-request=\"false\""))
            {
                break header.to_owned();
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("wait for restored copy");
    let renewed = restore(2)
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("restore renewal");
    assert_eq!(status.take(), Some(200));
    assert_eq!(renewed.request_charged().map(|value| value.as_str()), Some("requester"));
    assert_eq!(renewed.restore_output_path(), None);
    let output = client
        .get_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .version_id(&version)
        .send()
        .await
        .expect("read renewed copy");
    assert!(
        output
            .restore()
            .is_some_and(|header| header.starts_with("ongoing-request=\"false\"") && header != before)
    );
    assert_eq!(
        output
            .body
            .collect()
            .await
            .expect("collect renewed copy")
            .into_bytes()
            .as_ref(),
        b"archived bytes"
    );
    assert_eq!(backend.get_count().await, 1, "renewal must reuse the local copy");
    let current = client
        .get_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .send()
        .await
        .expect("read intervening PUT");
    assert_eq!(current.version_id(), Some(replacement_version));
    assert_eq!(current.restore(), None);
    assert_eq!(
        current
            .body
            .collect()
            .await
            .expect("collect intervening PUT")
            .into_bytes()
            .as_ref(),
        b"intervening PUT"
    );
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_admits_and_renews_restore_object() {
    with_stack(GATEWAY, restore_admits_once_and_renews_in_place);
}

#[test]
fn the_legacy_stack_admits_and_renews_restore_object() {
    with_stack(LEGACY, restore_admits_once_and_renews_in_place);
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
