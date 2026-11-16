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
use aws_sdk_s3::types::{AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, Owner, Permission, Tag, Tagging, Type};
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
        operation: "PutObjectLegalHold",
        metric: S3Operation::PutObjectLegalHold,
        has_error_body: true,
        call: put_object_legal_hold,
    },
    MatrixRow {
        operation: "PutObjectRetention",
        metric: S3Operation::PutObjectRetention,
        has_error_body: true,
        call: put_object_retention,
    },
    MatrixRow {
        operation: "GetObjectTagging",
        metric: S3Operation::GetObjectTagging,
        has_error_body: true,
        call: get_object_tagging,
    },
    MatrixRow {
        operation: "PutObjectTagging",
        metric: S3Operation::PutObjectTagging,
        has_error_body: true,
        call: put_object_tagging,
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

fn put_object_legal_hold<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let bucket = format!("{MATRIX_BUCKET}-locked");
        outcome(
            client
                .put_object_legal_hold()
                .bucket(bucket)
                .key(KEY)
                .legal_hold(legal_hold("ON"))
                .send()
                .await,
        )
    })
}

fn put_object_retention<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let bucket = format!("{MATRIX_BUCKET}-locked");
        outcome(
            client
                .put_object_retention()
                .bucket(bucket)
                .key(KEY)
                .retention(retention("2099-01-01T00:00:00Z"))
                .send()
                .await,
        )
    })
}

fn get_object_tagging<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    let bucket = MATRIX_BUCKET;
    Box::pin(async move {
        put_object_in_the_store(bucket, KEY, HashMap::new()).await;
        outcome(client.get_object_tagging().bucket(bucket).key(KEY).send().await)
    })
}

fn put_object_tagging<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        put_object_in_the_store(MATRIX_BUCKET, KEY, HashMap::new()).await;
        outcome(put_tags(client, MATRIX_BUCKET, KEY, None, &[]).send().await)
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

fn put_tags(
    client: &Client,
    bucket: &str,
    key: &str,
    version: Option<&str>,
    pairs: &[(&str, &str)],
) -> aws_sdk_s3::operation::put_object_tagging::builders::PutObjectTaggingFluentBuilder {
    let tag_set = pairs
        .iter()
        .map(|(key, value)| Tag::builder().key(*key).value(*value).build().unwrap())
        .collect();
    let tagging = Tagging::builder().set_tag_set(Some(tag_set)).build().unwrap();
    client
        .put_object_tagging()
        .bucket(bucket)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .tagging(tagging)
}

async fn tagged_object(client: &Client, bucket: &str, key: &str, payload: &'static [u8], tagging: &str) -> Option<String> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(payload))
        .send()
        .await
        .unwrap()
        .version_id()
        .map(str::to_owned)
}

async fn object_payload(client: &Client, bucket: &str, key: &str, version: Option<&str>) -> (Option<String>, Vec<u8>) {
    let output = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap();
    (
        output.version_id().map(str::to_owned),
        output.body.collect().await.unwrap().into_bytes().to_vec(),
    )
}

fn tagging_operation_count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == S3Operation::PutObjectTagging.as_str())
        .map_or(0, |row| row.total)
}

async fn read_tags(client: &Client, bucket: &str, key: &str, version: Option<&str>) -> Vec<(String, String)> {
    client
        .get_object_tagging()
        .bucket(bucket)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap()
        .tag_set()
        .iter()
        .map(|tag| (tag.key().to_owned(), tag.value().to_owned()))
        .collect()
}

async fn attach_tagging_policy(user: &str, action: &str, resource: &str, condition: serde_json::Value) {
    super::create_user_without_a_policy(user).await;
    let document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": [action], "Resource": [resource], "Condition": condition}]
    })
    .to_string();
    let policy = rustfs_policy::policy::Policy::parse_config(document.as_bytes()).unwrap();
    let iam = get_global_app_context().unwrap().iam().handle();
    iam.set_policy(user, policy).await.unwrap();
    iam.policy_db_set(user, rustfs_iam::store::UserType::Reg, false, user)
        .await
        .unwrap();
}

async fn put_object_tagging_authorization(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "put-object-tagging-policy";
    const OTHER: &str = "alternate";
    const CURRENT: &str = "tagging-current";
    const KEYS: &str = "tagging-keys";
    const VERSION: &str = "tagging-version";
    const WRONG: &str = "tagging-wrong-action";

    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    create_bucket_in_the_store(BUCKET).await;
    for key in [KEY, OTHER] {
        tagged_object(&root, BUCKET, key, b"preserved", "old=before").await;
    }
    let resource = format!("arn:aws:s3:::{BUCKET}/{KEY}");
    attach_tagging_policy(
        CURRENT,
        "s3:PutObjectTagging",
        &resource,
        serde_json::json!({
            "StringEquals": {"s3:RequestObjectTag/project": "approved"}
        }),
    )
    .await;
    attach_tagging_policy(
        KEYS,
        "s3:PutObjectTagging",
        &resource,
        serde_json::json!({
            "ForAllValues:StringLike": {"s3:RequestObjectTagKeys": ["project"]}
        }),
    )
    .await;
    create_user_allowed_on_one_bucket(VERSION, "s3:PutObjectVersionTagging", &format!("{BUCKET}/{KEY}")).await;
    create_user_allowed_on_one_bucket(WRONG, "s3:GetObjectTagging", &format!("{BUCKET}/{KEY}")).await;
    let current = s3_client(&endpoint, CURRENT, USER_SECRET_KEY);
    let keys = s3_client(&endpoint, KEYS, USER_SECRET_KEY);
    let version = s3_client(&endpoint, VERSION, USER_SECRET_KEY);
    let wrong = s3_client(&endpoint, WRONG, USER_SECRET_KEY);
    let anonymous = super::anonymous_client(&endpoint).await;

    let approved = [("project", "approved")];
    let denied = [("project", "denied")];
    let unexpected = [("forbidden", "approved")];
    let old = [("old".to_owned(), "before".to_owned())];
    let stored = [("project".to_owned(), "approved".to_owned())];
    assert_eq!(read_tags(&root, BUCKET, KEY, None).await, old);
    let grant = "project=approved";
    let refuse = "project=denied";
    for (label, client, key, named_version, pairs, header, allowed) in [
        ("XML value", &current, KEY, None, approved.as_slice(), refuse, true),
        ("spoofed value", &current, KEY, None, &denied, grant, false),
        ("empty XML set", &current, KEY, None, &[], grant, false),
        ("empty version", &current, KEY, Some(""), &approved, grant, false),
        ("null version", &current, KEY, Some("null"), &approved, grant, false),
        ("wrong resource", &current, OTHER, None, &approved, grant, false),
        ("XML key", &keys, KEY, None, &approved, "forbidden=approved", true),
        ("spoofed key", &keys, KEY, None, &unexpected, grant, false),
        ("version-only policy", &version, KEY, None, &denied, refuse, false),
        ("null version allowed", &version, KEY, Some("null"), &approved, refuse, true),
        ("wrong action", &wrong, KEY, None, &denied, refuse, false),
        ("anonymous", &anonymous, KEY, None, &denied, refuse, false),
    ] {
        let before = tagging_operation_count();
        let result = put_tags(client, BUCKET, key, named_version, pairs)
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert("x-amz-tagging", header);
            })
            .send()
            .await;
        if allowed {
            assert_eq!(
                result.unwrap_or_else(|error| panic!("{label}: {error:?}")).version_id(),
                named_version,
                "{label}"
            );
        } else {
            assert_eq!(
                refusal(&result.expect_err(label)),
                (Some("AccessDenied".to_owned()), Some(403)),
                "{label}"
            );
        }
        assert_eq!(tagging_operation_count() - before, u64::from(allowed), "{label}");
        for (key, expected) in [(KEY, &stored), (OTHER, &old)] {
            assert_eq!(&read_tags(&root, BUCKET, key, None).await, expected, "{label}: {key}");
            assert_eq!(
                object_payload(&root, BUCKET, key, None).await,
                (None, b"preserved".to_vec()),
                "{label}: {key}"
            );
        }
    }
    server.shutdown().await;
}

#[test]
fn gateway_put_object_tagging_authorizes_body_version_and_resource() {
    with_stack(GATEWAY, put_object_tagging_authorization);
}

#[test]
fn legacy_put_object_tagging_authorizes_body_version_and_resource() {
    with_stack(LEGACY, put_object_tagging_authorization);
}

async fn put_object_tagging_validation(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "put-object-tagging-validation";
    create_bucket_in_the_store(BUCKET).await;
    put_object_in_the_store(BUCKET, KEY, HashMap::new()).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let keys: Vec<_> = (0..11).map(|key| key.to_string()).collect();
    let too_many: Vec<_> = keys.iter().map(|key| (key.as_str(), "")).collect();
    let (long_key, long_value) = ("𐐀".repeat(65), "𐐀".repeat(129));
    let duplicates = [("same", "one"), ("same", "two")];
    for (pairs, version, code) in [
        (too_many.as_slice(), None, "InvalidTag"),
        (&[("", "value")], None, "InvalidTag"),
        (&[("tag", "invalid?")], None, "InvalidTag"),
        (&[(long_key.as_str(), "value")], None, "InvalidTag"),
        (&[("tag", long_value.as_str())], None, "InvalidTag"),
        (&duplicates, Some("bad"), "InvalidTag"),
        (&[("tag", "value")], Some("bad"), "InvalidArgument"),
        (&[("tag", "value")], Some(""), "InvalidArgument"),
    ] {
        let result = put_tags(&client, BUCKET, KEY, version, pairs).send().await;
        assert_eq!(refusal(&result.unwrap_err()), (Some(code.to_owned()), Some(400)), "{pairs:?} {version:?}");
        assert!(read_tags(&client, BUCKET, KEY, None).await.is_empty(), "{pairs:?}");
    }
    let (key, value) = ("𐐀".repeat(64), "𐐀".repeat(128));
    put_tags(&client, BUCKET, KEY, None, &[(&key, &value)]).send().await.unwrap();
    assert_eq!(read_tags(&client, BUCKET, KEY, None).await, [(key, value)]);
    server.shutdown().await;
}

#[test]
fn gateway_put_object_tagging_validates_tags_before_version() {
    with_stack(GATEWAY, put_object_tagging_validation);
}

#[test]
fn legacy_put_object_tagging_validates_tags_before_version() {
    with_stack(LEGACY, put_object_tagging_validation);
}

fn put_object_tagging_survives_restart(stack: Option<&'static str>) {
    temp_env::with_var(super::ENV_S3_STACK, stack, || {
        super::common::run_embedded_test(|| async {
            use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
            use rustfs::embedded::{RustFSServerBuilder, find_available_port};

            const BUCKET: &str = "put-object-tagging-history";
            let volume = tempfile::tempdir().unwrap();
            let start = || {
                RustFSServerBuilder::new()
                    .address(format!("127.0.0.1:{}", find_available_port().unwrap()))
                    .access_key(super::ROOT_ACCESS_KEY)
                    .secret_key(super::ROOT_SECRET_KEY)
                    .volume(volume.path().to_string_lossy())
                    .build()
            };
            let server = start().await.unwrap();
            let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
            create_bucket_in_the_store(BUCKET).await;
            root.put_bucket_versioning()
                .bucket(BUCKET)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await
                .unwrap();
            let first = tagged_object(&root, BUCKET, KEY, b"first", "old=first").await.unwrap();
            let second = tagged_object(&root, BUCKET, KEY, b"second", "old=second").await.unwrap();
            let first_tags = [
                ("project".to_owned(), "approved".to_owned()),
                ("with space".to_owned(), String::new()),
            ];
            let current_tags = [("old".to_owned(), "second".to_owned())];
            let notify = rustfs_notify::notification_system().expect("embedded notification runtime");
            let mut events = notify.subscribe_live_events();
            let resource = format!("arn:aws:s3:::{BUCKET}/{KEY}");
            attach_tagging_policy(
                "history-writer",
                "s3:PutObjectVersionTagging",
                &resource,
                serde_json::json!({"StringEquals":{"s3:VersionId": first}}),
            )
            .await;
            let writer = s3_client(&server.endpoint(), "history-writer", USER_SECRET_KEY);
            let result = put_tags(&writer, BUCKET, KEY, Some(&first), &[("project", "approved"), ("with space", "")])
                .send()
                .await
                .unwrap();
            assert_eq!(result.version_id(), Some(first.as_str()));
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let event = events.recv().await.unwrap();
                    if event.event_name == rustfs_s3_types::EventName::ObjectTaggingPut {
                        break event;
                    }
                }
            })
            .await
            .expect("committed tagging event");
            assert_eq!(event.s3.bucket.name, BUCKET);
            assert_eq!(event.s3.object.key, KEY);
            assert_eq!(event.s3.object.version_id.as_deref(), Some(first.as_str()));
            server.shutdown().await;

            let server = start().await.unwrap();
            let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
            for (version, payload, expected) in [
                (Some(first.as_str()), b"first".as_slice(), first_tags.as_slice()),
                (None, b"second".as_slice(), current_tags.as_slice()),
            ] {
                assert_eq!(read_tags(&root, BUCKET, KEY, version).await, expected);
                let (seen_version, seen_payload) = object_payload(&root, BUCKET, KEY, version).await;
                assert_eq!(seen_version.as_deref(), version.or(Some(second.as_str())));
                assert_eq!(seen_payload, payload);
            }
            let cleared = put_tags(&root, BUCKET, KEY, None, &[]).send().await.unwrap();
            assert!(cleared.version_id().is_none());
            assert!(read_tags(&root, BUCKET, KEY, None).await.is_empty());
            assert_eq!(read_tags(&root, BUCKET, KEY, Some(&first)).await, first_tags);
            server.shutdown().await;
        });
    });
}

#[test]
fn gateway_put_object_tagging_replaces_stored_tags() {
    put_object_tagging_survives_restart(GATEWAY);
}

#[test]
fn legacy_put_object_tagging_replaces_stored_tags() {
    put_object_tagging_survives_restart(LEGACY);
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

fn legal_hold(status: &str) -> aws_sdk_s3::types::ObjectLockLegalHold {
    aws_sdk_s3::types::ObjectLockLegalHold::builder()
        .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::from(status))
        .build()
}

fn retention(until: &str) -> aws_sdk_s3::types::ObjectLockRetention {
    aws_sdk_s3::types::ObjectLockRetention::builder()
        .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Governance)
        .retain_until_date(
            aws_sdk_s3::primitives::DateTime::from_str(until, aws_sdk_s3::primitives::DateTimeFormat::DateTime).unwrap(),
        )
        .build()
}

async fn lock_write_fixture(volume: &std::path::Path) -> (rustfs::embedded::RustFSServer, String, String, String) {
    use rustfs::embedded::{RustFSServerBuilder, find_available_port};
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{}", find_available_port().unwrap()))
        .access_key(super::ROOT_ACCESS_KEY)
        .secret_key(super::ROOT_SECRET_KEY)
        .volume(volume.to_string_lossy())
        .build()
        .await
        .unwrap();
    let bucket = locked_bucket("lock-write").await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let old = tagged_object(&client, &bucket, KEY, b"old object", "test=old").await.unwrap();
    let latest = tagged_object(&client, &bucket, KEY, b"new object", "test=latest")
        .await
        .unwrap();
    (server, bucket, old, latest)
}

fn lock_disk_version(volume: &std::path::Path, bucket: &str, version: &str) -> rustfs_filemeta::FileInfo {
    let bytes = std::fs::read(volume.join(bucket).join(KEY).join("xl.meta")).unwrap();
    rustfs_filemeta::FileMeta::load(&bytes)
        .unwrap()
        .list_versions(bucket, KEY, true)
        .unwrap()
        .into_iter()
        .find(|info| info.version_id == Some(uuid::Uuid::parse_str(version).unwrap()))
        .unwrap()
}

fn lock_operation_count(operation: S3Operation) -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == operation.as_str())
        .map_or(0, |row| row.total)
}

fn put_legal_hold_on_an_explicit_version(stack: Option<&'static str>) {
    temp_env::with_var(super::ENV_S3_STACK, stack, || {
        super::common::run_embedded_test(|| async {
            let volume = tempfile::tempdir().unwrap();
            let (server, bucket, old, latest) = lock_write_fixture(volume.path()).await;
            let initial = lock_disk_version(volume.path(), &bucket, &old);
            let untouched = lock_disk_version(volume.path(), &bucket, &latest);
            let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
            attach_tagging_policy(
                "hold-body-writer",
                "s3:PutObjectLegalHold",
                &format!("arn:aws:s3:::{bucket}/{KEY}"),
                serde_json::json!({"StringEquals":{"s3:object-lock-legal-hold": "ON", "s3:VersionId": old}}),
            )
            .await;
            let writer = s3_client(&server.endpoint(), "hold-body-writer", USER_SECRET_KEY);
            let write = |client: &Client, version: &str, status: &str| {
                client
                    .put_object_legal_hold()
                    .bucket(&bucket)
                    .key(KEY)
                    .version_id(version)
                    .legal_hold(legal_hold(status))
            };
            let before = lock_operation_count(S3Operation::PutObjectLegalHold);
            for result in [
                write(&writer, &latest, "ON").send().await,
                write(&writer, &old, "OFF").send().await,
            ] {
                assert_eq!(refusal(&result.unwrap_err()), (Some("AccessDenied".to_owned()), Some(403)));
            }
            assert_eq!(lock_disk_version(volume.path(), &bucket, &old), initial);
            assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), untouched);
            for (version, status, code, http) in [
                ("not-a-version", "ON", "InvalidArgument", 400),
                (&old, "INVALID", "MalformedXML", 400),
                ("c8164ea4-fb02-11ee-8564-5a3aed339b15", "ON", "InternalError", 500),
            ] {
                let result = write(&root, version, status).send().await;
                assert_eq!(refusal(&result.unwrap_err()), (Some(code.to_owned()), Some(http)));
            }
            assert_eq!(lock_disk_version(volume.path(), &bucket, &old), initial);
            assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), untouched);
            let count = lock_operation_count(S3Operation::PutObjectLegalHold);
            assert!(count > before, "admitted failures enter the operation");
            let response = write(&writer, &old, "ON").send().await.unwrap();
            assert_eq!(response.request_charged().map(|value| value.as_str()), Some("requester"));
            assert_eq!(lock_operation_count(S3Operation::PutObjectLegalHold) - count, 1);
            let changed = lock_disk_version(volume.path(), &bucket, &old);
            assert_eq!(changed.metadata.get("x-amz-object-lock-legal-hold").map(String::as_str), Some("ON"));
            assert_eq!(changed.metadata.get("x-amz-tagging"), initial.metadata.get("x-amz-tagging"));
            assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), untouched);
            assert_eq!(object_payload(&root, &bucket, KEY, Some(&old)).await.1, b"old object");
            server.shutdown().await;
        });
    });
}

fn put_retention_on_an_explicit_version(stack: Option<&'static str>) {
    temp_env::with_var(super::ENV_S3_STACK, stack, || {
        super::common::run_embedded_test(|| async {
            let volume = tempfile::tempdir().unwrap();
            let (server, bucket, old, latest) = lock_write_fixture(volume.path()).await;
            let initial = lock_disk_version(volume.path(), &bucket, &old);
            let untouched = lock_disk_version(volume.path(), &bucket, &latest);
            let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
            attach_tagging_policy(
                "retention-body-writer",
                "s3:PutObjectRetention",
                &format!("arn:aws:s3:::{bucket}/{KEY}"),
                serde_json::json!({"StringEquals":{"s3:object-lock-mode": "GOVERNANCE", "s3:VersionId": old}}),
            )
            .await;
            let writer = s3_client(&server.endpoint(), "retention-body-writer", USER_SECRET_KEY);
            let write = |client: &Client, version: &str, until: &str| {
                client
                    .put_object_retention()
                    .bucket(&bucket)
                    .key(KEY)
                    .version_id(version)
                    .retention(retention(until))
            };
            const END: &str = "2099-01-03T00:00:00.123Z";
            for refusal_result in [
                write(&writer, &latest, END).send().await,
                write(&writer, &old, END).bypass_governance_retention(true).send().await,
            ] {
                assert_eq!(refusal(&refusal_result.unwrap_err()), (Some("AccessDenied".to_owned()), Some(403)));
            }
            for (version, until, code, http) in [
                ("not-a-version", END, "InvalidArgument", 400),
                (&old, "2000-01-01T00:00:00Z", "InvalidArgument", 400),
                ("c8164ea4-fb02-11ee-8564-5a3aed339b15", END, "NoSuchVersion", 404),
            ] {
                let result = write(&root, version, until).send().await;
                assert_eq!(refusal(&result.unwrap_err()), (Some(code.to_owned()), Some(http)));
            }
            assert_eq!(lock_disk_version(volume.path(), &bucket, &old), initial);
            assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), untouched);
            let count = lock_operation_count(S3Operation::PutObjectRetention);
            let response = write(&writer, &old, END).send().await.unwrap();
            assert_eq!(response.request_charged().map(|value| value.as_str()), Some("requester"));
            assert_eq!(lock_operation_count(S3Operation::PutObjectRetention) - count, 1);
            let changed = lock_disk_version(volume.path(), &bucket, &old);
            assert_eq!(changed.metadata.get("x-amz-object-lock-mode").map(String::as_str), Some("GOVERNANCE"));
            let cutoff = changed
                .metadata
                .get("x-amz-object-lock-retain-until-date")
                .expect("committed cutoff");
            assert_eq!(cutoff, "2099-01-03T00:00:00.123Z");
            assert_eq!(changed.metadata.get("x-amz-tagging"), initial.metadata.get("x-amz-tagging"));
            assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), untouched);
            assert_eq!(object_payload(&root, &bucket, KEY, Some(&old)).await.1, b"old object");
            let result = write(&root, &old, "2099-01-01T00:00:00Z").send().await;
            assert_eq!(refusal(&result.unwrap_err()), (Some("AccessDenied".to_owned()), Some(403)));
            assert_eq!(lock_disk_version(volume.path(), &bucket, &old), changed);
            server.shutdown().await;
        });
    });
}

#[test]
fn gateway_put_object_legal_hold_edits_only_the_authorized_version() {
    put_legal_hold_on_an_explicit_version(GATEWAY);
}

#[test]
fn legacy_put_object_legal_hold_edits_only_the_authorized_version() {
    put_legal_hold_on_an_explicit_version(LEGACY);
}

#[test]
fn gateway_put_object_retention_edits_only_the_authorized_version() {
    put_retention_on_an_explicit_version(GATEWAY);
}

#[test]
fn legacy_put_object_retention_edits_only_the_authorized_version() {
    put_retention_on_an_explicit_version(LEGACY);
}

const RETAIN_UNTIL_HEADER: &str = "x-amz-object-lock-retain-until-date";
const RETENTION_DATES: [&str; 3] = [
    "2099-01-02T08:00:00.123456789+08:00",
    "2099-01-03T16:30:00.987654321-07:30",
    "2099-01-05T00:00:00.000000001Z",
];

async fn write_retention_xml(
    client: &super::object_write::WireClient<'_>,
    path: &str,
    xml: &str,
) -> super::object_write::WireAnswer {
    use md5::Digest as _;

    let md5 = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(xml.as_bytes()));
    client
        .request(
            "PUT",
            path,
            xml.as_bytes(),
            &[("content-type", "application/xml"), ("content-md5", &md5)],
            Some(xml.len()),
        )
        .await
}

#[test]
fn gateway_put_object_retention_preserves_the_authorized_date_on_disk() {
    use super::object_write::WireClient;

    temp_env::with_var(super::ENV_S3_STACK, GATEWAY, || {
        super::common::run_embedded_test(|| async {
            let volume = tempfile::tempdir().unwrap();
            let (server, bucket, old, latest) = lock_write_fixture(volume.path()).await;
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            let writer = WireClient {
                endpoint: &endpoint,
                access_key: "retention-date-writer",
                secret_key: USER_SECRET_KEY,
            };
            attach_tagging_policy(
                writer.access_key,
                "s3:PutObjectRetention",
                &format!("arn:aws:s3:::{bucket}/{KEY}"),
                serde_json::json!({"StringEquals": {
                    "s3:VersionId": old,
                    "s3:object-lock-retain-until-date": RETENTION_DATES[0]
                }}),
            )
            .await;

            let path = format!("/{bucket}/{KEY}?retention&versionId={old}");
            let metadata_path = volume.path().join(&bucket).join(KEY).join("xl.meta");
            let initial = std::fs::read(&metadata_path).unwrap();
            let latest_initial = lock_disk_version(volume.path(), &bucket, &latest);
            let xml = |date| format!("<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{date}</RetainUntilDate></Retention>");

            for (actor, body, status, code) in [
                (&writer, xml("2099-01-02T00:00:00.123456789Z"), 403, "<Code>AccessDenied</Code>"),
                (&writer, "<Retention/>".to_owned(), 403, "<Code>AccessDenied</Code>"),
                (&root, xml("invalid-date"), 400, "<Code>MalformedXML</Code>"),
            ] {
                let answer = write_retention_xml(actor, &path, &body).await;
                assert_eq!(answer.status, status, "{}", answer.body);
                assert!(answer.body.contains(code), "{}", answer.body);
                assert_eq!(std::fs::read(&metadata_path).unwrap(), initial);
            }

            for (actor, date) in [
                (&writer, RETENTION_DATES[0]),
                (&root, RETENTION_DATES[1]),
                (&root, RETENTION_DATES[2]),
            ] {
                let answer = write_retention_xml(actor, &path, &xml(date)).await;
                assert_eq!(answer.status, 200, "retain until {date}: {}", answer.body);
                let changed = lock_disk_version(volume.path(), &bucket, &old);
                assert_eq!(changed.metadata.get(RETAIN_UNTIL_HEADER).map(String::as_str), Some(date));
                assert_eq!(lock_disk_version(volume.path(), &bucket, &latest), latest_initial);
            }
            server.shutdown().await;
        });
    });
}

#[test]
fn gateway_put_object_preserves_retention_header_date_on_disk() {
    use super::object_write::WireClient;

    temp_env::with_var(super::ENV_S3_STACK, GATEWAY, || {
        super::common::run_embedded_test(|| async {
            let volume = tempfile::tempdir().unwrap();
            let (server, bucket, old, latest) = lock_write_fixture(volume.path()).await;
            let endpoint = server.endpoint();
            let client = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            let path = format!("/{bucket}/{KEY}");
            let metadata_path = volume.path().join(&bucket).join(KEY).join("xl.meta");
            let initial = std::fs::read(&metadata_path).unwrap();
            let old_initial = lock_disk_version(volume.path(), &bucket, &old).metadata;
            let latest_initial = lock_disk_version(volume.path(), &bucket, &latest).metadata;
            let mode = ("x-amz-object-lock-mode", "GOVERNANCE");

            for headers in [vec![mode], vec![mode, (RETAIN_UNTIL_HEADER, "invalid-date")]] {
                let answer = client.request("PUT", &path, b"object", &headers, Some(6)).await;
                assert_eq!(answer.status, 400, "{}", answer.body);
                assert_eq!(std::fs::read(&metadata_path).unwrap(), initial);
            }

            for date in [
                None,
                Some(RETENTION_DATES[0]),
                Some(RETENTION_DATES[1]),
                Some(RETENTION_DATES[2]),
            ] {
                let headers = date.map_or_else(Vec::new, |date| vec![mode, (RETAIN_UNTIL_HEADER, date)]);
                let answer = client.request("PUT", &path, b"object", &headers, Some(6)).await;
                assert_eq!(answer.status, 200, "retain until {date:?}: {}", answer.body);
                let written = lock_disk_version(volume.path(), &bucket, &answer.headers["x-amz-version-id"]);
                assert_eq!(written.metadata.get(RETAIN_UNTIL_HEADER).map(String::as_str), date);
                assert_eq!(lock_disk_version(volume.path(), &bucket, &old).metadata, old_initial);
                assert_eq!(lock_disk_version(volume.path(), &bucket, &latest).metadata, latest_initial);
            }
            server.shutdown().await;
        });
    });
}
