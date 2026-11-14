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

//! Refusal-matrix rows, calls and behaviour tests of migration batch B1 (rustfs/backlog#2766).
//! GetObjectTorrent answers every caller alike, so it is in `ANSWERS_EVERY_CALLER_ALIKE`, not in the
//! matrix. Every object these calls name is written through the store first, so no row depends on
//! another batch's operations.

use super::{
    GATEWAY, LEGACY, MatrixRow, Outcome, USER_SECRET_KEY, create_bucket_in_the_store, create_user_allowed_on_one_bucket, outcome,
    refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::{AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, Owner, Permission, Type};
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{BucketOperations, MakeBucketOptions, ObjectIO};
use std::collections::HashMap;

/// The object every call names.
const KEY: &str = "object-meta";
pub(super) const MATRIX_BUCKET: &str = "object-meta-matrix";

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "GetObjectAcl",
        metric: S3Operation::GetObjectAcl,
        has_error_body: true,
        call: get_object_acl,
    },
    MatrixRow {
        operation: "PutObjectAcl",
        metric: S3Operation::PutObjectAcl,
        has_error_body: true,
        call: put_object_acl,
    },
    MatrixRow {
        operation: "GetObjectLegalHold",
        metric: S3Operation::GetObjectLegalHold,
        has_error_body: true,
        call: get_object_legal_hold,
    },
    MatrixRow {
        operation: "GetObjectRetention",
        metric: S3Operation::GetObjectRetention,
        has_error_body: true,
        call: get_object_retention,
    },
    MatrixRow {
        operation: "GetObjectTagging",
        metric: S3Operation::GetObjectTagging,
        has_error_body: true,
        call: get_object_tagging,
    },
    MatrixRow {
        operation: "DeleteObjectTagging",
        metric: S3Operation::DeleteObjectTagging,
        has_error_body: true,
        call: delete_object_tagging,
    },
];

pub(crate) fn get_object_torrent<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_object_torrent().bucket(bucket).key(KEY).send().await) })
}

fn get_object_acl<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        put_object_in_the_store(bucket, KEY, HashMap::new()).await;
        outcome(client.get_object_acl().bucket(bucket).key(KEY).send().await)
    })
}

fn put_object_acl<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        put_object_in_the_store(bucket, KEY, HashMap::new()).await;
        let request = client.put_object_acl().bucket(bucket).key(KEY).acl(ObjectCannedAcl::Private);
        outcome(request.send().await)
    })
}

fn get_object_legal_hold<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        let bucket = locked_bucket(bucket).await;
        outcome(client.get_object_legal_hold().bucket(&bucket).key(KEY).send().await)
    })
}

fn get_object_retention<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        let bucket = locked_bucket(bucket).await;
        outcome(client.get_object_retention().bucket(&bucket).key(KEY).send().await)
    })
}

fn get_object_tagging<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        put_object_in_the_store(bucket, KEY, HashMap::new()).await;
        outcome(client.get_object_tagging().bucket(bucket).key(KEY).send().await)
    })
}

fn delete_object_tagging<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        put_object_in_the_store(bucket, KEY, HashMap::new()).await;
        outcome(client.delete_object_tagging().bucket(bucket).key(KEY).send().await)
    })
}

/// A bucket with Object Lock enabled next to `bucket`, holding `KEY`.
async fn locked_bucket(bucket: &str) -> String {
    let locked = format!("{bucket}-locked");
    let options = MakeBucketOptions {
        lock_enabled: true,
        versioning_enabled: true,
        ..Default::default()
    };
    // A second call finds the bucket made by the first.
    let _ = get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .make_bucket(&locked, &options)
        .await;
    put_object_in_the_store(&locked, KEY, HashMap::new()).await;
    locked
}

/// Writes `key` with `metadata` as stored user metadata.
async fn put_object_in_the_store(bucket: &str, key: &str, metadata: HashMap<String, String>) {
    let options = ObjectOptions {
        user_defined: metadata,
        ..Default::default()
    };
    let mut reader = PutObjReader::from_vec(b"object meta".to_vec());
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .put_object(bucket, key, &mut reader, &options)
        .await
        .expect("write the object through the store");
}

/// What the object-metadata reads and the ACL write answer, refusals first: a missing key, XML
/// grants, a bucket without Object Lock; then the stored values, as stored.
async fn object_metadata_answers_as_stored(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "object-meta-answers";
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store(BUCKET).await;
    put_object_in_the_store(BUCKET, KEY, HashMap::new()).await;

    let missing = client
        .get_object_acl()
        .bucket(BUCKET)
        .key("missing")
        .send()
        .await
        .expect_err("a missing key");
    assert_eq!(refusal(&missing), (Some("NoSuchKey".to_owned()), Some(404)));

    let grantee = Grantee::builder()
        .r#type(Type::CanonicalUser)
        .id("owner")
        .build()
        .expect("a grantee");
    let grant = Grant::builder().grantee(grantee).permission(Permission::Read).build();
    let grants = AccessControlPolicy::builder()
        .owner(Owner::builder().id("owner").build())
        .grants(grant)
        .build();
    let refused = client
        .put_object_acl()
        .bucket(BUCKET)
        .key(KEY)
        .access_control_policy(grants)
        .send()
        .await
        .expect_err("XML grants are refused");
    assert_eq!(refusal(&refused), (Some("NotImplemented".to_owned()), Some(501)));

    let unlocked = client
        .get_object_legal_hold()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await
        .expect_err("no Object Lock");
    assert_eq!(refusal(&unlocked), (Some("InvalidRequest".to_owned()), Some(400)));
    let unlocked = client
        .get_object_retention()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await
        .expect_err("no Object Lock");
    assert_eq!(refusal(&unlocked), (Some("InvalidRequest".to_owned()), Some(400)));

    let locked = locked_bucket(BUCKET).await;
    let missing = client
        .get_object_retention()
        .bucket(&locked)
        .key("missing")
        .send()
        .await
        .expect_err("a missing key");
    assert_eq!(refusal(&missing), (Some("InternalError".to_owned()), Some(500)));

    let hold = client
        .get_object_legal_hold()
        .bucket(&locked)
        .key(KEY)
        .send()
        .await
        .expect("the default hold");
    assert_eq!(
        hold.legal_hold().and_then(|hold| hold.status()).map(|status| status.as_str()),
        Some("OFF")
    );

    // Values outside the model's sets, as a replicated or migrated object may hold them.
    let stored = HashMap::from([
        ("x-amz-object-lock-legal-hold".to_owned(), "on".to_owned()),
        ("x-amz-object-lock-mode".to_owned(), "governance".to_owned()),
        ("x-amz-object-lock-retain-until-date".to_owned(), "2035-01-02T00:00:00+01:00".to_owned()),
    ]);
    put_object_in_the_store(&locked, "stored", stored).await;
    let hold = client
        .get_object_legal_hold()
        .bucket(&locked)
        .key("stored")
        .send()
        .await
        .expect("the stored hold");
    assert_eq!(hold.legal_hold().and_then(|hold| hold.status()).map(|status| status.as_str()), Some("on"));
    let retention = client
        .get_object_retention()
        .bucket(&locked)
        .key("stored")
        .send()
        .await
        .expect("the stored retention");
    let retention = retention.retention().expect("a retention");
    assert_eq!(retention.mode().map(|mode| mode.as_str()), Some("governance"));
    let until = retention.retain_until_date().expect("a retain-until date");
    assert_eq!(until.secs(), 2_051_305_200, "the stored instant, 2035-01-01T23:00:00Z");

    let missing = client
        .get_object_tagging()
        .bucket(BUCKET)
        .key("missing")
        .send()
        .await
        .expect_err("a missing key");
    assert_eq!(refusal(&missing), (Some("NoSuchKey".to_owned()), Some(404)));
    let malformed = client
        .get_object_tagging()
        .bucket(BUCKET)
        .key(KEY)
        .version_id("not-a-version")
        .send()
        .await
        .expect_err("a malformed version id");
    assert_eq!(refusal(&malformed), (Some("InvalidArgument".to_owned()), Some(400)));

    // A key and a value a strict tag grammar would refuse, as a migrated object may hold them.
    let tags = HashMap::from([("X-Amz-Tagging".to_owned(), "odd%20key=v%2Fw&k=".to_owned())]);
    put_object_in_the_store(BUCKET, "tagged", tags).await;
    let tagging = client
        .get_object_tagging()
        .bucket(BUCKET)
        .key("tagged")
        .send()
        .await
        .expect("the stored tags");
    let mut stored: Vec<_> = tagging
        .tag_set()
        .iter()
        .map(|tag| (tag.key().to_owned(), tag.value().to_owned()))
        .collect();
    stored.sort();
    assert_eq!(stored, [("k".to_owned(), String::new()), ("odd key".to_owned(), "v/w".to_owned())]);
    client
        .delete_object_tagging()
        .bucket(BUCKET)
        .key("tagged")
        .send()
        .await
        .expect("the tags are deleted");
    let tagging = client
        .get_object_tagging()
        .bucket(BUCKET)
        .key("tagged")
        .send()
        .await
        .expect("no tags");
    assert!(tagging.tag_set().is_empty(), "{tagging:?}");

    client
        .put_object_acl()
        .bucket(BUCKET)
        .key(KEY)
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("a canned ACL");
    let acl = client.get_object_acl().bucket(BUCKET).key(KEY).send().await.expect("the ACL");
    let grants: Vec<_> = acl
        .grants()
        .iter()
        .map(|grant| grant.permission().map(|p| p.as_str().to_owned()))
        .collect();
    assert_eq!(grants, [Some("FULL_CONTROL".to_owned())], "a canned ACL grants nothing more");

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_object_metadata_as_stored() {
    with_stack(GATEWAY, object_metadata_answers_as_stored);
}

#[test]
fn the_legacy_stack_answers_object_metadata_as_stored() {
    with_stack(LEGACY, object_metadata_answers_as_stored);
}

/// The tagging actions swap to their version actions when a version is named: a user allowed only
/// the unversioned action is refused with a version, and one allowed only the version action is
/// refused without one.
async fn the_tagging_action_follows_the_named_version(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "object-meta-swap";
    create_bucket_in_the_store(BUCKET).await;
    put_object_in_the_store(BUCKET, KEY, HashMap::new()).await;
    let objects = format!("{BUCKET}/*");
    let endpoint = server.endpoint();
    create_user_allowed_on_one_bucket("objectmetaunversioned", "s3:GetObjectTagging", &objects).await;
    create_user_allowed_on_one_bucket("objectmetaversioned", "s3:GetObjectVersionTagging", &objects).await;
    let unversioned = s3_client(&endpoint, "objectmetaunversioned", USER_SECRET_KEY);
    let versioned = s3_client(&endpoint, "objectmetaversioned", USER_SECRET_KEY);
    let read = |client: &Client, version: Option<&str>| {
        let request = client
            .get_object_tagging()
            .bucket(BUCKET)
            .key(KEY)
            .set_version_id(version.map(str::to_owned));
        async move { outcome(request.send().await).map_err(|refusal| (refusal.code, refusal.status)) }
    };
    let denied = Err((Some("AccessDenied".to_owned()), Some(403)));

    assert_eq!(read(&unversioned, None).await, Ok(()), "the unversioned action without a version");
    assert_eq!(read(&unversioned, Some("null")).await, denied, "the unversioned action with a version");
    assert_eq!(read(&versioned, Some("null")).await, Ok(()), "the version action with a version");
    assert_eq!(read(&versioned, None).await, denied, "the version action without a version");
    let delete = outcome(unversioned.delete_object_tagging().bucket(BUCKET).key(KEY).send().await);
    assert_eq!(delete.map_err(|refusal| refusal.status), Err(Some(403)), "another tagging action");

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_swaps_the_tagging_action_for_a_named_version() {
    with_stack(GATEWAY, the_tagging_action_follows_the_named_version);
}

#[test]
fn the_legacy_stack_swaps_the_tagging_action_for_a_named_version() {
    with_stack(LEGACY, the_tagging_action_follows_the_named_version);
}

/// Checks boundary keys across bucket configurations.
async fn keys_outside_the_floor_are_refused(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "object-meta-keys";
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store(BUCKET).await;
    let locked = locked_bucket(BUCKET).await;
    let reserved = "sthree-object-meta-keys";
    create_bucket_in_the_store(reserved).await;
    assert!(super::bucket_exists_in_the_store(reserved).await);
    put_object_in_the_store(reserved, KEY, HashMap::new()).await;
    assert_eq!(answer(client.get_object_acl().bucket(reserved).key(KEY).send().await), (None, None, None));
    let long_key = "k".repeat(1025);
    for (operation, call) in KEY_CALLS {
        for (bucket, kind) in [(BUCKET, 0), (locked.as_str(), 1), ("object-meta-missing", 2), (reserved, 0)] {
            let (code, status, message) = match (*operation, kind) {
                ("GetObjectTorrent", _) => ("NoSuchKey", 404, "The specified key does not exist."),
                ("GetObjectLegalHold", 0) | ("GetObjectRetention", 0) => {
                    ("InvalidRequest", 400, "Bucket is missing ObjectLockConfiguration")
                }
                ("GetObjectLegalHold", 1) | ("GetObjectRetention", 1) => {
                    ("InternalError", 500, "XML response contains a control character")
                }
                ("GetObjectLegalHold", _) => ("NoSuchBucket", 404, "The specified bucket does not exist"),
                ("GetObjectRetention", _) => ("InternalError", 500, "Failed to load Object Lock configuration"),
                _ => ("InvalidArgument", 400, "Invalid argument"),
            };
            let expected = (Some(code.to_owned()), Some(status), Some(message.to_owned()));
            assert_eq!(call(&client, bucket, "a\0b").await, expected, "{operation} bucket {bucket}");
        }
        let expected = (Some("KeyTooLongError".to_owned()), Some(400), Some("Your key is too long.".to_owned()));
        assert_eq!(call(&client, BUCKET, &long_key).await, expected, "{operation} long key");
    }
    server.shutdown().await;
}

type KeyCall = for<'a> fn(&'a Client, &'a str, &'a str) -> BoxFuture<'a, (Option<String>, Option<u16>, Option<String>)>;

const KEY_CALLS: &[(&str, KeyCall)] = &[
    ("GetObjectAcl", |c, b, k| {
        Box::pin(async move { answer(c.get_object_acl().bucket(b).key(k).send().await) })
    }),
    ("PutObjectAcl", |c, b, k| {
        Box::pin(async move { answer(c.put_object_acl().bucket(b).key(k).acl(ObjectCannedAcl::Private).send().await) })
    }),
    ("GetObjectLegalHold", |c, b, k| {
        Box::pin(async move { answer(c.get_object_legal_hold().bucket(b).key(k).send().await) })
    }),
    ("GetObjectRetention", |c, b, k| {
        Box::pin(async move { answer(c.get_object_retention().bucket(b).key(k).send().await) })
    }),
    ("GetObjectTagging", |c, b, k| {
        Box::pin(async move { answer(c.get_object_tagging().bucket(b).key(k).send().await) })
    }),
    ("DeleteObjectTagging", |c, b, k| {
        Box::pin(async move { answer(c.delete_object_tagging().bucket(b).key(k).send().await) })
    }),
    ("GetObjectTorrent", |c, b, k| {
        Box::pin(async move { answer(c.get_object_torrent().bucket(b).key(k).send().await) })
    }),
];

/// The code, status and message a refused call carries.
fn answer<T, E: ProvideErrorMetadata>(result: Result<T, SdkError<E>>) -> (Option<String>, Option<u16>, Option<String>) {
    match result {
        Ok(_) => (None, None, None),
        Err(error) => {
            let (code, status) = refusal(&error);
            (code, status, error.message().map(str::to_owned))
        }
    }
}

#[test]
fn the_legacy_stack_refuses_keys_outside_the_floor() {
    with_stack(LEGACY, keys_outside_the_floor_are_refused);
}
