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

//! Refusal-matrix rows of migration batch A2 (rustfs/backlog#2765).

use super::{GATEWAY, LEGACY, MatrixRow, Outcome, create_bucket_in_the_store, outcome, refusal, s3_client, with_stack};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_bucket_versioning::{
    PutBucketVersioningError, PutBucketVersioningOutput, builders::PutBucketVersioningFluentBuilder,
};
use aws_sdk_s3::types::{AccessControlPolicy, BucketCannedAcl, BucketVersioningStatus, VersioningConfiguration};
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_s3_ops::S3Operation;

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "GetBucketVersioning",
        metric: S3Operation::GetBucketVersioning,
        has_error_body: true,
        call: get_bucket_versioning,
    },
    MatrixRow {
        operation: "PutBucketVersioning",
        metric: S3Operation::PutBucketVersioning,
        has_error_body: true,
        call: put_enabled_bucket_versioning,
    },
    MatrixRow {
        operation: "GetBucketPolicy",
        metric: S3Operation::GetBucketPolicy,
        has_error_body: true,
        call: get_bucket_policy,
    },
    MatrixRow {
        operation: "GetBucketPolicyStatus",
        metric: S3Operation::GetBucketPolicyStatus,
        has_error_body: true,
        call: get_bucket_policy_status,
    },
    MatrixRow {
        operation: "GetBucketAcl",
        metric: S3Operation::GetBucketAcl,
        has_error_body: true,
        call: get_bucket_acl,
    },
    MatrixRow {
        operation: "PutBucketAcl",
        metric: S3Operation::PutBucketAcl,
        has_error_body: true,
        call: put_bucket_acl,
    },
    MatrixRow {
        operation: "GetBucketNotificationConfiguration",
        metric: S3Operation::GetBucketNotificationConfiguration,
        has_error_body: true,
        call: get_bucket_notification,
    },
    MatrixRow {
        operation: "PutBucketNotificationConfiguration",
        metric: S3Operation::PutBucketNotificationConfiguration,
        has_error_body: true,
        call: put_empty_bucket_notification,
    },
    MatrixRow {
        operation: "PutBucketPolicy",
        metric: S3Operation::PutBucketPolicy,
        has_error_body: true,
        call: put_bucket_policy,
    },
    MatrixRow {
        operation: "DeleteBucketPolicy",
        metric: S3Operation::DeleteBucketPolicy,
        has_error_body: true,
        call: delete_bucket_policy,
    },
];

fn put_bucket_policy<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    use md5::{Digest as _, Md5};

    Box::pin(async move {
        let status = super::bucket_configs::StatusCapture::default();
        let content_md5 = base64_simd::STANDARD.encode_to_string(Md5::digest(INERT_POLICY.as_bytes()));
        outcome(
            client
                .put_bucket_policy()
                .bucket(bucket)
                .policy(INERT_POLICY)
                .content_md5(content_md5)
                .customize()
                .interceptor(status.clone())
                .send()
                .await
                .map(|_| assert_eq!(status.take(), Some(204))),
        )
    })
}

pub(super) fn delete_bucket_policy<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let status = super::bucket_configs::StatusCapture::default();
        outcome(
            client
                .delete_bucket_policy()
                .bucket(bucket)
                .customize()
                .interceptor(status.clone())
                .send()
                .await
                .map(|_| {
                    assert_eq!(status.take(), Some(204));
                }),
        )
    })
}

fn get_bucket_versioning<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_versioning().bucket(bucket).send().await) })
}

fn put_enabled_bucket_versioning<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(put_versioning(client, bucket, BucketVersioningStatus::Enabled).send().await) })
}

/// A policy that grants nobody anything the matrix exercises, so GetBucketPolicy has a document to
/// answer without changing what any other row is admitted for.
const INERT_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::rustfs:user/nobody"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::gateway-stack-matrix/*"]}]}"#;

fn get_bucket_policy<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        store_bucket_config(bucket, "policy.json", INERT_POLICY).await;
        outcome(client.get_bucket_policy().bucket(bucket).send().await)
    })
}

fn get_bucket_policy_status<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_policy_status().bucket(bucket).send().await) })
}

fn put_bucket_acl<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.put_bucket_acl().bucket(bucket).send().await) })
}

fn get_bucket_acl<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_acl().bucket(bucket).send().await) })
}

fn get_bucket_notification<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_notification_configuration().bucket(bucket).send().await) })
}

/// Stores a bucket configuration document the way the metadata system holds it, so a read is
/// tested without the operation that writes it.
async fn store_bucket_config(bucket: &str, file: &str, document: &str) {
    let metadata = get_global_app_context()
        .expect("embedded server context")
        .bucket_metadata()
        .handle()
        .expect("bucket metadata system");
    metadata
        .read()
        .await
        .update(bucket, file, document.as_bytes().to_vec())
        .await
        .expect("store the bucket configuration");
}

/// The stored names a request is served for. The AWS SDK treats a `--x-s3` name as a directory
/// bucket and refuses it on the client, so `x--x-s3` is left out; `xn--abc` is refused with 400
/// by both stacks before any operation runs, and is pinned apart below.
const SDK_REACHABLE_NAMES: [&str; 3] = ["a-s3alias", "sthree-x", "x--ol-s3"];

const ENABLED_VERSIONING: &str = r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#;

/// GetBucketVersioning answers a bucket the store holds under any name the store can hold, with
/// the status it holds, and refuses a bucket it does not hold.
async fn get_bucket_versioning_answers_every_stored_bucket(server: RustFSServer) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for name in SDK_REACHABLE_NAMES {
        create_bucket_in_the_store(name).await;
        let unconfigured = client.get_bucket_versioning().bucket(name).send().await.expect(name);
        assert_eq!(unconfigured.status(), None, "{name}");
    }
    store_bucket_config(SDK_REACHABLE_NAMES[0], "versioning.xml", ENABLED_VERSIONING).await;
    let enabled = client
        .get_bucket_versioning()
        .bucket(SDK_REACHABLE_NAMES[0])
        .send()
        .await
        .expect("enabled");
    assert_eq!(enabled.status(), Some(&BucketVersioningStatus::Enabled));

    create_bucket_in_the_store("xn--abc").await;
    let reserved = client
        .get_bucket_versioning()
        .bucket("xn--abc")
        .send()
        .await
        .expect_err("a refused name");
    assert_eq!(refusal(&reserved), (Some("InvalidBucketName".to_owned()), Some(400)), "{reserved:?}");

    let missing = client.get_bucket_versioning().bucket("a2-no-such-bucket").send().await;
    assert_eq!(
        refusal(&missing.expect_err("a missing bucket")),
        (Some("NoSuchBucket".to_owned()), Some(404))
    );
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_every_stored_bucket_for_versioning() {
    with_stack(GATEWAY, get_bucket_versioning_answers_every_stored_bucket);
}

#[test]
fn the_legacy_stack_answers_every_stored_bucket_for_versioning() {
    with_stack(LEGACY, get_bucket_versioning_answers_every_stored_bucket);
}

/// A bucket policy document: refused when there is none, answered verbatim when there is, and
/// the owner reads it even when the policy denies everyone; policy status follows the document.
async fn bucket_policy_reads_follow_the_stored_document(server: RustFSServer) {
    const DENY_ALL: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:GetBucketPolicy"],"Resource":["arn:aws:s3:::a2-policy"]}]}"#;
    const PUBLIC_LIST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::a2-public"]}]}"#;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store("a2-policy").await;
    create_bucket_in_the_store("a2-public").await;

    let none = client
        .get_bucket_policy()
        .bucket("a2-policy")
        .send()
        .await
        .expect_err("no policy yet");
    assert_eq!(refusal(&none), (Some("NoSuchBucketPolicy".to_owned()), Some(404)));
    let missing = client
        .get_bucket_policy()
        .bucket("a2-no-such-bucket")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)), "{missing:?}");
    let status = client
        .get_bucket_policy_status()
        .bucket("a2-public")
        .send()
        .await
        .expect("status");
    assert_eq!(status.policy_status().and_then(|status| status.is_public()), Some(false));

    store_bucket_config("a2-policy", "policy.json", DENY_ALL).await;
    let owner = client
        .get_bucket_policy()
        .bucket("a2-policy")
        .send()
        .await
        .expect("the owner bypasses the Deny");
    assert_eq!(owner.policy(), Some(DENY_ALL));

    store_bucket_config("a2-public", "policy.json", PUBLIC_LIST).await;
    let status = client
        .get_bucket_policy_status()
        .bucket("a2-public")
        .send()
        .await
        .expect("status");
    assert_eq!(status.policy_status().and_then(|status| status.is_public()), Some(true));
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_reads_the_stored_bucket_policy() {
    with_stack(GATEWAY, bucket_policy_reads_follow_the_stored_document);
}

#[test]
fn the_legacy_stack_reads_the_stored_bucket_policy() {
    with_stack(LEGACY, bucket_policy_reads_follow_the_stored_document);
}

/// GetBucketAcl answers the one owner with full control for any bucket the store holds.
async fn get_bucket_acl_answers_the_owner_with_full_control(server: RustFSServer) {
    use aws_sdk_s3::types::{Permission, Type};
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store(SDK_REACHABLE_NAMES[1]).await;

    let acl = client
        .get_bucket_acl()
        .bucket(SDK_REACHABLE_NAMES[1])
        .send()
        .await
        .expect("acl");
    assert!(acl.owner().and_then(|owner| owner.id()).is_some());
    let [grant] = acl.grants() else { panic!("one grant: {:?}", acl.grants()) };
    assert_eq!(grant.permission(), Some(&Permission::FullControl));
    assert_eq!(grant.grantee().map(|grantee| grantee.r#type()), Some(&Type::CanonicalUser));
    let grantee = grant.grantee().expect("a grantee");
    assert_eq!((grantee.id(), grantee.display_name()), (None, None), "the grantee names no identity");

    let missing = client
        .get_bucket_acl()
        .bucket("a2-no-such-bucket")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)), "{missing:?}");
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_the_bucket_acl() {
    with_stack(GATEWAY, get_bucket_acl_answers_the_owner_with_full_control);
}

#[test]
fn the_legacy_stack_answers_the_bucket_acl() {
    with_stack(LEGACY, get_bucket_acl_answers_the_owner_with_full_control);
}

async fn bucket_acl_mutations(server: RustFSServer) {
    use super::{USER_SECRET_KEY, anonymous_client, create_user_allowed_on_one_bucket};

    const BUCKET: &str = "bucket-acl-write-s3alias";
    const OTHER: &str = "bucket-acl-write-other";
    const MISSING: &str = "bucket-acl-write-missing";
    const WRITER: &str = "bucketaclwriter";
    const READER: &str = "bucketaclreader";
    const UNSUPPORTED: &str = "ACL XML grants are not supported; use canned ACL headers or omit ACL";

    for bucket in [BUCKET, OTHER] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketAcl", BUCKET).await;
    create_user_allowed_on_one_bucket(READER, "s3:GetBucketAcl", BUCKET).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, WRITER, USER_SECRET_KEY);
    let reader = s3_client(&endpoint, READER, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    let before = root.get_bucket_acl().bucket(BUCKET).send().await.expect("original ACL");
    assert_eq!(put_bucket_acl(&writer, BUCKET).await, Ok(()));
    writer
        .put_bucket_acl()
        .bucket(BUCKET)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect("accept canned ACL header");
    writer
        .put_bucket_acl()
        .bucket(BUCKET)
        .grant_read("id=\"fixture-owner\"")
        .send()
        .await
        .expect("accept grant header");

    for (client, bucket) in [(&writer, OTHER), (&reader, BUCKET), (&anonymous, BUCKET)] {
        let err = put_bucket_acl(client, bucket).await.expect_err("permission required");
        assert_eq!((err.code.as_deref(), err.status), (Some("AccessDenied"), Some(403)));
    }
    for (client, bucket, expected) in [
        (&writer, BUCKET, ("NotImplemented", 501)),
        (&root, MISSING, ("NoSuchBucket", 404)),
        (&reader, BUCKET, ("AccessDenied", 403)),
    ] {
        let error = client
            .put_bucket_acl()
            .bucket(bucket)
            .access_control_policy(AccessControlPolicy::builder().build())
            .send()
            .await
            .expect_err("XML policy");
        assert_eq!(refusal(&error), (Some(expected.0.to_owned()), Some(expected.1)));
        if expected.1 == 501 {
            assert_eq!(error.as_service_error().and_then(|error| error.meta().message()), Some(UNSUPPORTED));
        }
    }
    let after = root.get_bucket_acl().bucket(BUCKET).send().await.expect("current ACL");
    assert_eq!(after.owner(), before.owner());
    assert_eq!(after.grants(), before.grants());

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_preserves_bucket_acl_write_behavior() {
    with_stack(GATEWAY, bucket_acl_mutations);
}

#[test]
fn the_legacy_stack_preserves_bucket_acl_write_behavior() {
    with_stack(LEGACY, bucket_acl_mutations);
}

const QUEUE_NOTIFICATION: &str = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>a2-queue</Id><Queue>arn:rustfs:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>"#;

/// GetBucketNotificationConfiguration answers an empty configuration when none is stored, the
/// stored queue rule (event, filter and id) when one is, for any bucket name the store holds, and
/// refuses a bucket the store does not hold.
async fn get_bucket_notification_answers_the_stored_configuration(server: RustFSServer) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for name in ["a2-notification", "sthree-x", "a-s3alias"] {
        create_bucket_in_the_store(name).await;
        let empty = client
            .get_bucket_notification_configuration()
            .bucket(name)
            .send()
            .await
            .expect(name);
        assert!(
            empty.queue_configurations().is_empty() && empty.topic_configurations().is_empty(),
            "{name}"
        );
    }

    store_bucket_config(SDK_REACHABLE_NAMES[1], "notification.xml", QUEUE_NOTIFICATION).await;
    let stored = client
        .get_bucket_notification_configuration()
        .bucket(SDK_REACHABLE_NAMES[1])
        .send()
        .await
        .expect("stored configuration");
    let [queue] = stored.queue_configurations() else { panic!("one queue rule: {stored:?}") };
    assert_eq!((queue.id(), queue.queue_arn()), (Some("a2-queue"), "arn:rustfs:sqs:us-east-1:1:webhook"));
    assert_eq!(queue.events(), &[aws_sdk_s3::types::Event::S3ObjectCreated]);
    let rules = queue.filter().and_then(|filter| filter.key()).map(|key| key.filter_rules());
    let [rule] = rules.expect("stored filter") else { panic!("one filter rule") };
    assert_eq!(rule.name(), Some(&aws_sdk_s3::types::FilterRuleName::Prefix));
    assert_eq!(rule.value(), Some("logs/"));

    let extended =
        QUEUE_NOTIFICATION.replace("<QueueConfiguration>", "<Future><Nested>ignored</Nested></Future><QueueConfiguration>");
    store_bucket_config("a2-notification", "notification.xml", &extended).await;
    let unknown = client
        .get_bucket_notification_configuration()
        .bucket("a2-notification")
        .send()
        .await
        .expect("unknown stored element");
    assert_eq!(unknown.queue_configurations(), stored.queue_configurations());

    let missing = client
        .get_bucket_notification_configuration()
        .bucket("a2-no-such-bucket")
        .send()
        .await;
    let missing = missing.expect_err("a missing bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)), "{missing:?}");
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_the_stored_notification_configuration() {
    with_stack(GATEWAY, get_bucket_notification_answers_the_stored_configuration);
}

#[test]
fn the_legacy_stack_answers_the_stored_notification_configuration() {
    with_stack(LEGACY, get_bucket_notification_answers_the_stored_configuration);
}

async fn notification_authorization_is_bound_to_the_requested_bucket(server: RustFSServer) {
    const USER: &str = "notification-reader";
    const BUCKET: &str = "notification-allowed";
    const OTHER: &str = "notification-other";
    create_bucket_in_the_store(BUCKET).await;
    create_bucket_in_the_store(OTHER).await;
    super::create_user_allowed_on_one_bucket(USER, "s3:GetBucketNotification", BUCKET).await;
    let client = s3_client(&server.endpoint(), USER, super::USER_SECRET_KEY);
    let handled_count = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.op == S3Operation::GetBucketNotificationConfiguration.as_str())
            .map_or(0, |metric| metric.total)
    };
    let before = handled_count();
    client
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .expect("allowed read");
    assert_eq!(handled_count() - before, 1, "the admitted edge counts once");
    let wrong = client
        .get_bucket_notification_configuration()
        .bucket(OTHER)
        .send()
        .await
        .expect_err("wrong bucket");
    assert_eq!(refusal(&wrong), (Some("AccessDenied".into()), Some(403)));
    super::create_user_without_a_policy("notification-no-policy").await;
    let unprivileged = s3_client(&server.endpoint(), "notification-no-policy", super::USER_SECRET_KEY);
    let implicit = unprivileged
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .expect_err("implicit deny");
    assert_eq!(refusal(&implicit), (Some("AccessDenied".into()), Some(403)));
    let deny = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":["*"]}},"Action":["s3:GetBucketNotification"],"Resource":["arn:aws:s3:::{BUCKET}"]}}]}}"#
    );
    store_bucket_config(BUCKET, "policy.json", &deny).await;
    let explicit = client
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .expect_err("explicit deny overrides user allow");
    assert_eq!(refusal(&explicit), (Some("AccessDenied".into()), Some(403)));
    assert_eq!(handled_count() - before, 1, "authorization refusals do not reach the edge counter");
    server.shutdown().await;
}

#[test]
fn notification_authorization_binds_the_bucket_on_the_gateway_stack() {
    with_stack(GATEWAY, notification_authorization_is_bound_to_the_requested_bucket);
}

#[test]
fn notification_authorization_binds_the_bucket_on_the_legacy_stack() {
    with_stack(LEGACY, notification_authorization_is_bound_to_the_requested_bucket);
}

const VERSIONING_ENABLED: &str = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
const VERSIONING_SUSPENDED_BYTES: &[u8] = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";

fn put_versioning(client: &Client, bucket: &str, status: BucketVersioningStatus) -> PutBucketVersioningFluentBuilder {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
}

async fn put_versioning_document(
    client: &Client,
    bucket: &str,
    document: &'static str,
) -> Result<PutBucketVersioningOutput, Box<SdkError<PutBucketVersioningError>>> {
    use md5::{Digest as _, Md5};

    let content_md5 = base64_simd::STANDARD.encode_to_string(Md5::digest(document.as_bytes()));
    put_versioning(client, bucket, BucketVersioningStatus::Enabled)
        .content_md5(content_md5)
        .customize()
        .mutate_request(move |request| {
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(document);
            let headers = request.headers_mut();
            headers.remove("content-length");
            headers.remove("x-amz-sdk-checksum-algorithm");
            headers.remove("x-amz-checksum-crc32");
        })
        .send()
        .await
        .map_err(Box::new)
}

async fn stored_versioning(bucket: &str) -> Vec<u8> {
    rustfs_ecstore::api::bucket::metadata_sys::get_config_from_disk(bucket)
        .await
        .expect("bucket metadata on disk")
        .versioning_config_xml
}

async fn versioning_writes_preserve_refusal_order_and_stored_bytes(server: RustFSServer) {
    use aws_sdk_s3::error::ProvideErrorMetadata as _;

    const WRITER: &str = "versioningwriter";
    const BUCKET: &str = "versioning-allowed";
    const OTHER: &str = "versioning-other";
    const LOCKED: &str = "versioning-locked";
    const REPLICATED: &str = "versioning-replicated";
    const MISSING: &str = "versioning-missing";
    for bucket in [BUCKET, OTHER, LOCKED, REPLICATED, "xn--abc"]
        .into_iter()
        .chain(SDK_REACHABLE_NAMES)
    {
        create_bucket_in_the_store(bucket).await;
        store_bucket_config(bucket, "versioning.xml", VERSIONING_ENABLED).await;
    }
    const LOCK: &str = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>";
    const REPLICATION: &str = "<ReplicationConfiguration><Role>arn:aws:s3:::target-bucket</Role><Rule><ID>rule1</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::target-bucket</Bucket></Destination></Rule></ReplicationConfiguration>";
    store_bucket_config(LOCKED, "replication.xml", REPLICATION).await;
    store_bucket_config(LOCKED, "object-lock.xml", LOCK).await;
    store_bucket_config(REPLICATED, "replication.xml", REPLICATION).await;

    super::create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketVersioning", BUCKET).await;
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let writer = s3_client(&server.endpoint(), WRITER, super::USER_SECRET_KEY);
    let handled = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.op == S3Operation::PutBucketVersioning.as_str())
            .map_or(0, |metric| metric.total)
    };
    let before = handled();
    let status = super::bucket_configs::StatusCapture::default();
    put_versioning(&writer, BUCKET, BucketVersioningStatus::Suspended)
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("authorized versioning update");
    assert_eq!(status.take(), Some(200));
    assert_eq!(stored_versioning(BUCKET).await, VERSIONING_SUSPENDED_BYTES);
    assert_eq!(handled() - before, 1, "the admitted edge counts once");

    let denied = put_versioning(&writer, OTHER, BucketVersioningStatus::Suspended)
        .send()
        .await
        .expect_err("bucket outside the writer's policy");
    assert_eq!(refusal(&denied), (Some("AccessDenied".into()), Some(403)));
    assert_eq!(stored_versioning(OTHER).await, VERSIONING_ENABLED.as_bytes());
    assert_eq!(handled() - before, 1, "authorization refusals do not reach the edge counter");

    for (bucket, code, http, message) in [
        (MISSING, "NoSuchBucket", 404, None),
        ("xn--abc", "InvalidBucketName", 400, None),
        (
            LOCKED,
            "InvalidBucketState",
            409,
            Some("An Object Lock configuration is present on this bucket, versioning cannot be suspended."),
        ),
        (
            REPLICATED,
            "InvalidBucketState",
            409,
            Some("A replication configuration is present on this bucket, bucket wide versioning cannot be suspended."),
        ),
    ] {
        let error = put_versioning(&root, bucket, BucketVersioningStatus::Suspended)
            .send()
            .await
            .expect_err(bucket);
        assert_eq!(refusal(&error), (Some(code.into()), Some(http)), "{bucket}");
        if let Some(message) = message {
            assert_eq!(error.message(), Some(message), "{bucket}");
        }
        if bucket != MISSING {
            assert_eq!(stored_versioning(bucket).await, VERSIONING_ENABLED.as_bytes(), "{bucket}");
        }
    }

    for bucket in SDK_REACHABLE_NAMES {
        put_versioning(&root, bucket, BucketVersioningStatus::Suspended)
            .send()
            .await
            .expect(bucket);
        assert_eq!(stored_versioning(bucket).await, VERSIONING_SUSPENDED_BYTES, "{bucket}");
    }

    for (client, bucket, code, http) in [
        (&root, OTHER, "InvalidArgument", 400),
        (&root, MISSING, "NoSuchBucket", 404),
        (&writer, OTHER, "AccessDenied", 403),
    ] {
        let invalid = put_versioning(client, bucket, BucketVersioningStatus::from("FutureStatus"))
            .send()
            .await
            .expect_err("invalid versioning status");
        assert_eq!(refusal(&invalid), (Some(code.into()), Some(http)), "{bucket}");
        if code == "InvalidArgument" {
            assert_eq!(invalid.message(), Some("bucket versioning configuration has an invalid status"));
        }
    }

    let malformed = put_versioning_document(&root, OTHER, "<VersioningConfiguration>")
        .await
        .expect_err("incomplete versioning document");
    assert_eq!(refusal(malformed.as_ref()), (Some("MalformedXML".into()), Some(400)));
    assert_eq!(stored_versioning(OTHER).await, VERSIONING_ENABLED.as_bytes());

    const EXTENSIONS: &str = "<VersioningConfiguration><Status>Enabled</Status><ExcludeFolders>true</ExcludeFolders><ExcludedPrefixes><Prefix>archive/</Prefix></ExcludedPrefixes><ExcludedPrefixes/></VersioningConfiguration>";
    const STORED_EXTENSIONS: &[u8] = b"<VersioningConfiguration><ExcludeFolders>true</ExcludeFolders><ExcludedPrefixes><Prefix>archive/</Prefix></ExcludedPrefixes><ExcludedPrefixes></ExcludedPrefixes><Status>Enabled</Status></VersioningConfiguration>";
    put_versioning_document(&root, OTHER, EXTENSIONS)
        .await
        .expect("versioning extensions");
    assert_eq!(stored_versioning(OTHER).await, STORED_EXTENSIONS);
    put_versioning_document(&root, OTHER, "Enabled")
        .await
        .expect("bare versioning status");
    assert_eq!(stored_versioning(OTHER).await, VERSIONING_ENABLED.as_bytes());
    server.shutdown().await;
}

#[test]
fn legacy_put_versioning_preserves_refusal_order_and_stored_bytes() {
    with_stack(LEGACY, versioning_writes_preserve_refusal_order_and_stored_bytes);
}

#[test]
fn gateway_put_versioning_preserves_refusal_order_and_stored_bytes() {
    with_stack(GATEWAY, versioning_writes_preserve_refusal_order_and_stored_bytes);
}

pub(super) fn with_notification_stack<F, Fut>(stack: Option<&'static str>, test: F)
where
    F: FnOnce(RustFSServer) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind webhook fixture");
    listener.set_nonblocking(true).expect("set webhook listener nonblocking");
    let endpoint = format!("http://{}", listener.local_addr().expect("webhook fixture address"));
    temp_env::with_var(rustfs_utils::egress::ENV_OUTBOUND_ALLOW_ORIGINS, Some(endpoint.clone()), || {
        with_stack(stack, move |server| async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("register webhook listener");
            let router = axum::Router::new().route("/", axum::routing::get(|| async { http::StatusCode::OK }));
            let health = tokio::spawn(async move { axum::serve(listener, router).await.expect("serve webhook fixture") });
            let queue = tempfile::tempdir().expect("webhook queue");
            let mut target = rustfs_config::server_config::KVS::new();
            target.insert(rustfs_config::ENABLE_KEY.to_owned(), "on".to_owned());
            target.insert(rustfs_config::WEBHOOK_ENDPOINT.to_owned(), endpoint);
            target.insert(
                rustfs_config::WEBHOOK_QUEUE_DIR.to_owned(),
                queue.path().to_str().expect("valid queue path").to_owned(),
            );
            let notify = rustfs_notify::notification_system().expect("embedded notification runtime");
            let mut config = notify.config_snapshot().await;
            config
                .0
                .entry(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS.to_owned())
                .or_default()
                .insert("notification-http".to_owned(), target);
            notify
                .set_targets_enabled(true, Some(config))
                .await
                .expect("register loopback webhook target");
            assert!(
                notify
                    .get_active_targets()
                    .await
                    .iter()
                    .any(|target| target.id == "notification-http")
            );
            test(server).await;
            health.abort();
        });
    });
}

fn put_empty_bucket_notification<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(put_notification(client, bucket).send().await) })
}

const EMPTY_NOTIFICATION: &str = "<NotificationConfiguration></NotificationConfiguration>";
const EVENT_BRIDGE_NOTIFICATION: &str =
    "<NotificationConfiguration><EventBridgeConfiguration></EventBridgeConfiguration></NotificationConfiguration>";

fn put_notification(
    client: &Client,
    bucket: &str,
) -> aws_sdk_s3::operation::put_bucket_notification_configuration::builders::PutBucketNotificationConfigurationFluentBuilder {
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(aws_sdk_s3::types::NotificationConfiguration::builder().build())
}

async fn put_notification_document(
    client: &Client,
    bucket: &str,
    document: &'static str,
) -> Result<
    aws_sdk_s3::operation::put_bucket_notification_configuration::PutBucketNotificationConfigurationOutput,
    Box<SdkError<aws_sdk_s3::operation::put_bucket_notification_configuration::PutBucketNotificationConfigurationError>>,
> {
    use md5::{Digest as _, Md5};

    let content_md5 = base64_simd::STANDARD.encode_to_string(Md5::digest(document.as_bytes()));
    put_notification(client, bucket)
        .customize()
        .mutate_request(move |request| {
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(document);
            let headers = request.headers_mut();
            headers.remove("content-length");
            headers.remove("x-amz-sdk-checksum-algorithm");
            headers.remove("x-amz-checksum-crc32");
            headers.insert("content-md5", content_md5.clone());
        })
        .send()
        .await
        .map_err(Box::new)
}

async fn stored_notification(bucket: &str) -> Vec<u8> {
    rustfs_ecstore::api::bucket::metadata_sys::get_config_from_disk(bucket)
        .await
        .expect("bucket metadata on disk")
        .notification_config_xml
}

async fn notification_writes_preserve_authorization_and_stored_bytes(server: RustFSServer) {
    use aws_sdk_s3::error::ProvideErrorMetadata as _;

    const ALLOWED: &str = "notification-put-allowed";
    const READABLE: &str = "notification-put-readable";
    const MISSING: &str = "notification-put-missing";
    const WRITER: &str = "notificationwriter";
    const READER: &str = "notificationreader";
    const NO_POLICY: &str = "notificationnopolicy";
    const DENIED: &str = "notificationexplicitdeny";

    for bucket in [ALLOWED, READABLE, "sthree-x", "a-s3alias"] {
        create_bucket_in_the_store(bucket).await;
        store_bucket_config(bucket, "notification.xml", EMPTY_NOTIFICATION).await;
    }
    super::create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketNotification", ALLOWED).await;
    super::create_user_allowed_on_one_bucket(READER, "s3:GetBucketNotification", READABLE).await;
    super::create_user_without_a_policy(NO_POLICY).await;
    super::create_user_without_a_policy(DENIED).await;
    let policy = rustfs_policy::policy::Policy::parse_config(
        br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutBucketNotification"],"Resource":["arn:aws:s3:::notification-put-readable"]},{"Effect":"Deny","Action":["s3:PutBucketNotification"],"Resource":["arn:aws:s3:::notification-put-readable"]}]}"#,
    ).expect("parse explicit deny");
    let iam = get_global_app_context().expect("embedded server context").iam().handle();
    iam.set_policy(DENIED, policy).await.expect("store policy");
    iam.policy_db_set(DENIED, rustfs_iam::store::UserType::Reg, false, DENIED)
        .await
        .expect("attach policy");

    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, WRITER, super::USER_SECRET_KEY);
    let reader = s3_client(&endpoint, READER, super::USER_SECRET_KEY);
    let unprivileged = s3_client(&endpoint, NO_POLICY, super::USER_SECRET_KEY);
    let denied = s3_client(&endpoint, DENIED, super::USER_SECRET_KEY);

    let metric = S3Operation::PutBucketNotificationConfiguration.as_str();
    let handled = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .iter()
            .find(|counter| counter.op == metric)
            .map_or(0, |counter| counter.total)
    };
    let handled_before = handled();
    let before = super::s3_request_counts();
    put_notification_document(&writer, ALLOWED, EVENT_BRIDGE_NOTIFICATION)
        .await
        .expect("authorized notification update");
    assert_eq!(stored_notification(ALLOWED).await, EVENT_BRIDGE_NOTIFICATION.as_bytes());
    assert_eq!(super::counted_since(&before), [(("PUT", metric, "2xx"), 1)].into());
    assert_eq!(handled() - handled_before, 1, "the admitted edge counts once");
    let stored = root
        .get_bucket_notification_configuration()
        .bucket(ALLOWED)
        .send()
        .await
        .expect("Get after Put");
    assert!(stored.event_bridge_configuration().is_some());

    reader
        .get_bucket_notification_configuration()
        .bucket(READABLE)
        .send()
        .await
        .expect("reader can Get");
    const BAD_FILTER: &str = "<NotificationConfiguration><QueueConfiguration><Id>queue</Id><Queue>arn:rustfs:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>../secret</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>";
    for (client, bucket, document) in [
        (&reader, READABLE, EVENT_BRIDGE_NOTIFICATION),
        (&reader, READABLE, BAD_FILTER),
        (&unprivileged, READABLE, EVENT_BRIDGE_NOTIFICATION),
        (&denied, READABLE, EVENT_BRIDGE_NOTIFICATION),
        (&reader, MISSING, EVENT_BRIDGE_NOTIFICATION),
    ] {
        let before = super::s3_request_counts();
        let handled_before = handled();
        let result = put_notification_document(client, bucket, document).await;
        if bucket == READABLE {
            assert_eq!(stored_notification(bucket).await, EMPTY_NOTIFICATION.as_bytes());
        }
        assert_eq!(
            refusal(result.expect_err(bucket).as_ref()),
            (Some("AccessDenied".into()), Some(403)),
            "{bucket}"
        );
        let counts = super::counted_since(&before);
        assert_eq!(counts.len(), 1, "{bucket}: {counts:?}");
        assert_eq!(counts.values().sum::<u64>(), 1, "{bucket}: {counts:?}");
        assert!(
            counts
                .keys()
                .all(|(method, _, outcome)| *method == "PUT" && *outcome == "4xx")
        );
        assert_eq!(handled(), handled_before);
    }

    let before = super::s3_request_counts();
    let handled_before = handled();
    let invalid = put_notification_document(&root, READABLE, BAD_FILTER)
        .await
        .expect_err("invalid filter");
    assert_eq!(refusal(invalid.as_ref()), (Some("InvalidArgument".into()), Some(400)));
    assert_eq!(
        invalid.message(),
        Some("invalid notification filter value (len=9) (QueueConfiguration id=queue)")
    );
    assert_eq!(stored_notification(READABLE).await, EMPTY_NOTIFICATION.as_bytes());
    assert_eq!(super::counted_since(&before), [(("PUT", metric, "4xx"), 1)].into());
    assert_eq!(handled() - handled_before, 1, "the admitted edge counts once");

    for bucket in ["sthree-x", "a-s3alias"] {
        put_notification_document(&root, bucket, EVENT_BRIDGE_NOTIFICATION)
            .await
            .expect(bucket);
        assert_eq!(stored_notification(bucket).await, EVENT_BRIDGE_NOTIFICATION.as_bytes());
    }
    server.shutdown().await;
}

#[test]
fn legacy_put_notification_preserves_authorization_and_stored_bytes() {
    with_notification_stack(LEGACY, notification_writes_preserve_authorization_and_stored_bytes);
}

#[test]
fn gateway_put_notification_preserves_authorization_and_stored_bytes() {
    with_notification_stack(GATEWAY, notification_writes_preserve_authorization_and_stored_bytes);
}

#[test]
fn gateway_put_notification_rejects_get_only_before_writing() {
    with_notification_stack(GATEWAY, |server| async move {
        const BUCKET: &str = "notification-get-only";
        const READER: &str = "notificationgetonly";
        create_bucket_in_the_store(BUCKET).await;
        store_bucket_config(BUCKET, "notification.xml", EMPTY_NOTIFICATION).await;
        super::create_user_allowed_on_one_bucket(READER, "s3:GetBucketNotification", BUCKET).await;
        let reader = s3_client(&server.endpoint(), READER, super::USER_SECRET_KEY);
        reader
            .get_bucket_notification_configuration()
            .bucket(BUCKET)
            .send()
            .await
            .expect("the Get-only principal can read this bucket");
        let result = put_notification_document(&reader, BUCKET, EVENT_BRIDGE_NOTIFICATION).await;
        assert_eq!(
            stored_notification(BUCKET).await,
            EMPTY_NOTIFICATION.as_bytes(),
            "a Get-only principal must not change stored notification bytes"
        );
        assert_eq!(
            refusal(result.expect_err("Get-only Put must be denied").as_ref()),
            (Some("AccessDenied".into()), Some(403))
        );
        server.shutdown().await;
    });
}

#[test]
fn gateway_put_notification_rejects_other_bucket_writer_before_writing() {
    with_notification_stack(GATEWAY, |server| async move {
        const ALLOWED: &str = "notification-scope-allowed";
        const DENIED: &str = "notification-scope-denied";
        const WRITER: &str = "notificationscopewriter";
        for bucket in [ALLOWED, DENIED] {
            create_bucket_in_the_store(bucket).await;
            store_bucket_config(bucket, "notification.xml", EMPTY_NOTIFICATION).await;
        }
        super::create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketNotification", ALLOWED).await;
        let writer = s3_client(&server.endpoint(), WRITER, super::USER_SECRET_KEY);
        let result = put_notification_document(&writer, DENIED, EVENT_BRIDGE_NOTIFICATION).await;
        assert_eq!(
            stored_notification(DENIED).await,
            EMPTY_NOTIFICATION.as_bytes(),
            "permission on another bucket must not allow changing this bucket"
        );
        assert_eq!(
            refusal(result.expect_err("another bucket's writer must be denied").as_ref()),
            (Some("AccessDenied".into()), Some(403))
        );
        assert_eq!(stored_notification(ALLOWED).await, EMPTY_NOTIFICATION.as_bytes());
        server.shutdown().await;
    });
}
