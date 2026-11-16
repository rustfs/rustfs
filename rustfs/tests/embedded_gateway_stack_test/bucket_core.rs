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

//! Refusal-matrix rows of migration batch A4 and of ListBuckets (rustfs/backlog#2765).

use super::{
    GATEWAY, LEGACY, MatrixRow, Outcome, USER_SECRET_KEY, anonymous_client, create_bucket_in_the_store,
    create_user_allowed_on_one_bucket, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{
    AccelerateConfiguration, BucketAccelerateStatus, BucketLocationConstraint, BucketLoggingStatus, CreateBucketConfiguration,
    IndexDocument, LoggingEnabled, Payer, RequestPaymentConfiguration, WebsiteConfiguration,
};
use futures::future::BoxFuture;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::bucket::{
    metadata::{BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_WEBSITE_CONFIG},
    metadata_sys,
};
use rustfs_s3_ops::S3Operation;

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "ListBuckets",
        metric: S3Operation::ListBuckets,
        has_error_body: true,
        call: list_buckets,
    },
    MatrixRow {
        operation: "HeadBucket",
        metric: S3Operation::HeadBucket,
        has_error_body: false,
        call: head_bucket,
    },
    MatrixRow {
        operation: "CreateBucket",
        metric: S3Operation::CreateBucket,
        has_error_body: true,
        call: create_bucket,
    },
    MatrixRow {
        operation: "DeleteBucket",
        metric: S3Operation::DeleteBucket,
        has_error_body: true,
        call: delete_bucket,
    },
    MatrixRow {
        operation: "GetBucketLocation",
        metric: S3Operation::GetBucketLocation,
        has_error_body: true,
        call: get_bucket_location,
    },
    MatrixRow {
        operation: "GetBucketAccelerateConfiguration",
        metric: S3Operation::GetBucketAccelerateConfiguration,
        has_error_body: true,
        call: get_bucket_accelerate_configuration,
    },
    MatrixRow {
        operation: "GetBucketRequestPayment",
        metric: S3Operation::GetBucketRequestPayment,
        has_error_body: true,
        call: get_bucket_request_payment,
    },
    MatrixRow {
        operation: "GetBucketWebsite",
        metric: S3Operation::GetBucketWebsite,
        has_error_body: true,
        call: get_bucket_website,
    },
    MatrixRow {
        operation: "GetBucketLogging",
        metric: S3Operation::GetBucketLogging,
        has_error_body: true,
        call: get_bucket_logging,
    },
    MatrixRow {
        operation: "PutBucketLogging",
        metric: S3Operation::PutBucketLogging,
        has_error_body: true,
        call: put_bucket_logging,
    },
    MatrixRow {
        operation: "PutBucketAccelerateConfiguration",
        metric: S3Operation::PutBucketAccelerateConfiguration,
        has_error_body: true,
        call: put_bucket_accelerate_configuration,
    },
    MatrixRow {
        operation: "PutBucketRequestPayment",
        metric: S3Operation::PutBucketRequestPayment,
        has_error_body: true,
        call: put_bucket_request_payment,
    },
    MatrixRow {
        operation: "PutBucketWebsite",
        metric: S3Operation::PutBucketWebsite,
        has_error_body: true,
        call: put_bucket_website,
    },
    MatrixRow {
        operation: "DeleteBucketWebsite",
        metric: S3Operation::DeleteBucketWebsite,
        has_error_body: true,
        call: delete_bucket_website,
    },
];

fn list_buckets<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_buckets().send().await) })
}

fn head_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.head_bucket().bucket(bucket).send().await) })
}

fn get_bucket_location<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_location().bucket(bucket).send().await) })
}

fn get_bucket_accelerate_configuration<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_accelerate_configuration().bucket(bucket).send().await) })
}

fn get_bucket_request_payment<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_request_payment().bucket(bucket).send().await) })
}

fn get_bucket_website<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_website().bucket(bucket).send().await) })
}

fn get_bucket_logging<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_logging().bucket(bucket).send().await) })
}

/// Re-creating the existing matrix bucket: the default region answers it `200`.
fn create_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.create_bucket().bucket(bucket).send().await) })
}

/// Deletes a bucket of its own beside the matrix bucket, created in the store first when an
/// earlier call deleted it, so every caller meets an existing, empty bucket.
fn delete_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let doomed = format!("{bucket}-doomed");
        if !super::bucket_exists_in_the_store(&doomed).await {
            super::create_bucket_in_the_store(&doomed).await;
        }
        outcome(client.delete_bucket().bucket(doomed).send().await)
    })
}

/// One CreateBucket request by root: the bucket, its `LocationConstraint`, the refusal it gets
/// (`None` when served) and whether the bucket exists afterwards.
type CreateCase = (&'static str, Option<&'static str>, Option<(Option<&'static str>, Option<u16>)>, bool);

/// The server runs in the default region, so a foreign constraint and `EU` are refused. The four
/// reserved names are created as before; `xn--` is refused before the operation
/// runs.
const CREATE_CASES: [CreateCase; 9] = [
    ("create-fresh", None, None, true),
    ("create-fresh", None, None, true),
    (
        "create-foreign",
        Some("ap-south-1"),
        Some((Some("InvalidLocationConstraint"), Some(400))),
        false,
    ),
    ("create-eu", Some("EU"), Some((Some("InvalidLocationConstraint"), Some(400))), false),
    ("sthree-x", None, None, true),
    ("a-s3alias", None, None, true),
    ("x--ol-s3", None, None, true),
    // The SDK sends nothing for a `--x-s3` name: it treats it as a directory bucket and refuses it
    // on the client. The wire answer, `200` on the legacy stack, was measured with a signed curl.
    ("x--x-s3", None, Some((None, None)), false),
    ("xn--abc", None, Some((Some("InvalidBucketName"), Some(400))), false),
];

async fn create_bucket_answers_as_before(server: RustFSServer) {
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let mut observed = Vec::new();
    for (bucket, location, _, _) in CREATE_CASES {
        let configuration = location.map(|location| {
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(location))
                .build()
        });
        let created = root
            .create_bucket()
            .bucket(bucket)
            .set_create_bucket_configuration(configuration)
            .send()
            .await;
        let refused = outcome(created).err().map(|refusal| (refusal.code, refusal.status));
        let exists = super::bucket_exists_in_the_store(bucket).await;
        observed.push((bucket, location, refused, exists));
    }

    let expected: Vec<_> = CREATE_CASES
        .iter()
        .map(|&(bucket, location, refused, exists)| {
            (bucket, location, refused.map(|(code, status)| (code.map(str::to_owned), status)), exists)
        })
        .collect();
    assert_eq!(observed, expected);
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_creates_buckets_as_before() {
    with_stack(GATEWAY, create_bucket_answers_as_before);
}

#[test]
fn the_legacy_stack_creates_buckets_as_before() {
    with_stack(LEGACY, create_bucket_answers_as_before);
}

/// One DeleteBucket request by root: the bucket, the force-delete header it carries, whether the
/// bucket holds an object, the refusal it gets (`None` when served) and whether the bucket still
/// exists afterwards.
type DeleteCase = (
    &'static str,
    Option<(&'static str, &'static str)>,
    bool,
    Option<(&'static str, u16)>,
    bool,
);

const NOT_EMPTY: Option<(&str, u16)> = Some(("BucketNotEmpty", 409));
const RUSTFS_INVALID: Option<(&str, u16)> = Some(("InvalidRequest", 400));
const MINIO_INVALID: Option<(&str, u16)> = Some(("InvalidArgument", 400));

/// The legacy answers, measured: `x-rustfs-force-delete` reads RustFS's wide boolean grammar,
/// `x-minio-force-delete` only `true` and `false`. The force rows use a bucket that holds an
/// object, so a refused value that deleted anyway, or a `false` read as `true`, cannot pass. A
/// name the AWS grammar reserves is deleted like any other.
const DELETE_CASES: [DeleteCase; 14] = [
    ("delete-missing", None, false, Some(("NoSuchBucket", 404)), false),
    ("delete-plain", None, false, None, false),
    ("sthree-delete", None, false, None, false),
    ("delete-full", None, true, NOT_EMPTY, true),
    ("delete-rustfs-true", Some(("x-rustfs-force-delete", "true")), true, None, false),
    ("delete-rustfs-upper", Some(("x-rustfs-force-delete", "TRUE")), true, None, false),
    ("delete-rustfs-one", Some(("x-rustfs-force-delete", "1")), true, None, false),
    (
        "delete-rustfs-maybe",
        Some(("x-rustfs-force-delete", "maybe")),
        true,
        RUSTFS_INVALID,
        true,
    ),
    ("delete-rustfs-false", Some(("x-rustfs-force-delete", "false")), true, NOT_EMPTY, true),
    ("delete-minio-true", Some(("x-minio-force-delete", "true")), true, None, false),
    ("delete-minio-upper", Some(("x-minio-force-delete", "TRUE")), true, MINIO_INVALID, true),
    ("delete-minio-one", Some(("x-minio-force-delete", "1")), true, MINIO_INVALID, true),
    ("delete-minio-maybe", Some(("x-minio-force-delete", "maybe")), true, MINIO_INVALID, true),
    ("delete-minio-false", Some(("x-minio-force-delete", "false")), true, NOT_EMPTY, true),
];

async fn delete_bucket_answers_as_before(server: RustFSServer) {
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let mut observed = Vec::new();
    for (bucket, header, holds_an_object, _, _) in DELETE_CASES {
        if bucket != "delete-missing" {
            super::create_bucket_in_the_store(bucket).await;
        }
        if holds_an_object {
            super::put_object_in_the_store(bucket, "kept").await;
        }
        let deleted = root
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |request| {
                if let Some((name, value)) = header {
                    request.headers_mut().insert(name, value);
                }
            })
            .send()
            .await;
        let refused = outcome(deleted).err().map(|refusal| (refusal.code, refusal.status));
        let exists = super::bucket_exists_in_the_store(bucket).await;
        observed.push((bucket, refused, exists));
    }

    let expected: Vec<_> = DELETE_CASES
        .iter()
        .map(|&(bucket, _, _, refused, exists)| {
            (bucket, refused.map(|(code, status)| (Some(code.to_owned()), Some(status))), exists)
        })
        .collect();
    assert_eq!(observed, expected);
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_deletes_buckets_as_before() {
    with_stack(GATEWAY, delete_bucket_answers_as_before);
}

#[test]
fn the_legacy_stack_deletes_buckets_as_before() {
    with_stack(LEGACY, delete_bucket_answers_as_before);
}

/// A bucket in the default region has an empty location constraint, under an ordinary name and
/// under one the AWS grammar reserves; a missing bucket is `NoSuchBucket`.
async fn get_bucket_location_answers_as_before(server: RustFSServer) {
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for bucket in ["located", "located-s3alias"] {
        super::create_bucket_in_the_store(bucket).await;
        let located = root.get_bucket_location().bucket(bucket).send().await.expect(bucket);
        // The default region is written as an empty `LocationConstraint` element, which the SDK
        // reads as an empty constraint.
        assert_eq!(located.location_constraint().map(|constraint| constraint.as_str()), Some(""), "{bucket}");
    }

    let missing = root.get_bucket_location().bucket("not-located").send().await;
    let refusal = outcome(missing).expect_err("a missing bucket is refused");
    assert_eq!((refusal.code.as_deref(), refusal.status), (Some("NoSuchBucket"), Some(404)));
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_locates_buckets_as_before() {
    with_stack(GATEWAY, get_bucket_location_answers_as_before);
}

#[test]
fn the_legacy_stack_locates_buckets_as_before() {
    with_stack(LEGACY, get_bucket_location_answers_as_before);
}

async fn force_delete_header_order(server: RustFSServer) {
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let anonymous = super::anonymous_client(&endpoint).await;
    for (bucket, rustfs_value, minio_value, expected) in [
        ("force-order-true", "true", "false", None),
        ("force-order-false", "false", "true", Some(("BucketNotEmpty", 409))),
        ("force-order-invalid", "true", "maybe", Some(("InvalidArgument", 400))),
    ] {
        super::create_bucket_in_the_store(bucket).await;
        super::put_object_in_the_store(bucket, "kept").await;
        let result = root
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert("x-rustfs-force-delete", rustfs_value);
                request.headers_mut().insert("x-minio-force-delete", minio_value);
            })
            .send()
            .await;
        let actual = outcome(result).err().map(|e| (e.code, e.status));
        assert_eq!(actual, expected.map(|(code, status)| (Some(code.to_owned()), Some(status))), "{bucket}");
    }
    let result = anonymous
        .delete_bucket()
        .bucket("force-order-anonymous")
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-minio-force-delete", "maybe");
        })
        .send()
        .await;
    let error = outcome(result).expect_err("invalid header must be refused");
    assert_eq!((error.code.as_deref(), error.status), (Some("InvalidArgument"), Some(400)));
    server.shutdown().await;
}

#[test]
fn the_legacy_stack_orders_force_delete_headers() {
    with_stack(LEGACY, force_delete_header_order);
}

#[test]
fn the_gateway_stack_orders_force_delete_headers() {
    with_stack(GATEWAY, force_delete_header_order);
}

const WEBSITE_CONFIG: &[u8] = br#"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>old/</KeyPrefixEquals></Condition><Redirect><ReplaceKeyPrefixWith>new/</ReplaceKeyPrefixWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>"#;

async fn seed_config(bucket: &str, file: &str, xml: &[u8]) {
    metadata_sys::update(bucket, file, xml.to_vec())
        .await
        .expect("seed stored bucket configuration");
}

pub(super) async fn seed_website(bucket: &str) {
    seed_config(bucket, BUCKET_WEBSITE_CONFIG, WEBSITE_CONFIG).await;
}

async fn core_configuration_reads(server: RustFSServer, gateway: bool) {
    const CONFIGURED: &str = "core-config-granted";
    const EMPTY: &str = "core-config-empty";
    const UNUSUAL: &str = "core-config-unusual";
    const MISSING: &str = "core-config-absent";
    for bucket in [CONFIGURED, EMPTY, UNUSUAL] {
        create_bucket_in_the_store(bucket).await;
    }
    seed_website(CONFIGURED).await;
    seed_config(
        CONFIGURED,
        BUCKET_ACCELERATE_CONFIG,
        b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>",
    )
    .await;
    seed_config(
        CONFIGURED,
        BUCKET_REQUEST_PAYMENT_CONFIG,
        b"<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>",
    )
    .await;
    seed_config(
        CONFIGURED,
        BUCKET_LOGGING_CONFIG,
        b"<BucketLoggingStatus><LoggingEnabled><TargetBucket>core-config-empty</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>",
    ).await;
    let long_error_key = "a".repeat(1025);
    let unusual_website = format!(
        "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>{long_error_key}</Key></ErrorDocument></WebsiteConfiguration>"
    );
    seed_config(UNUSUAL, BUCKET_WEBSITE_CONFIG, unusual_website.as_bytes()).await;

    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let policy_user = "coreconfigpolicyreader";
    let logging_user = "coreconfigloggingreader";
    create_user_allowed_on_one_bucket(policy_user, "s3:GetBucketPolicy", CONFIGURED).await;
    create_user_allowed_on_one_bucket(logging_user, "s3:GetBucketLogging", CONFIGURED).await;
    let policy = s3_client(&endpoint, policy_user, USER_SECRET_KEY);
    let logging = s3_client(&endpoint, logging_user, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    for row in MATRIX.iter().filter(|row| {
        matches!(
            row.metric,
            S3Operation::GetBucketAccelerateConfiguration
                | S3Operation::GetBucketRequestPayment
                | S3Operation::GetBucketWebsite
                | S3Operation::GetBucketLogging
        )
    }) {
        let (allowed, wrong_action) = if row.metric == S3Operation::GetBucketLogging {
            (&logging, &policy)
        } else {
            (&policy, &logging)
        };
        let operation = row.operation;
        let call = row.call;
        assert_eq!(call(allowed, CONFIGURED).await, Ok(()), "{operation} grant");
        for (caller, bucket) in [(allowed, EMPTY), (wrong_action, CONFIGURED), (&anonymous, CONFIGURED)] {
            let error = call(caller, bucket).await.expect_err(operation);
            assert_eq!((error.code.as_deref(), error.status), (Some("AccessDenied"), Some(403)), "{operation}");
        }
        let error = call(&root, MISSING).await.expect_err("missing bucket");
        assert_eq!((error.code.as_deref(), error.status), (Some("NoSuchBucket"), Some(404)), "{operation}");
    }

    let accelerate = root
        .get_bucket_accelerate_configuration()
        .bucket(CONFIGURED)
        .send()
        .await
        .unwrap();
    assert_eq!(accelerate.status().map(|status| status.as_str()), Some("Enabled"));
    let accelerate = root.get_bucket_accelerate_configuration().bucket(EMPTY).send().await.unwrap();
    assert!(accelerate.status().is_none());

    let payment = root.get_bucket_request_payment().bucket(CONFIGURED).send().await.unwrap();
    assert_eq!(payment.payer().map(|payer| payer.as_str()), Some("Requester"));
    let payment = root.get_bucket_request_payment().bucket(EMPTY).send().await.unwrap();
    assert_eq!(payment.payer().map(|payer| payer.as_str()), Some("BucketOwner"));

    let website = root.get_bucket_website().bucket(CONFIGURED).send().await.unwrap();
    assert_eq!(website.error_document().unwrap().key(), "error.html");
    assert_eq!(website.index_document().unwrap().suffix(), "index.html");
    let [rule] = website.routing_rules() else { panic!("expected one routing rule") };
    assert_eq!(rule.condition().unwrap().key_prefix_equals(), Some("old/"));
    assert_eq!(rule.redirect().unwrap().replace_key_prefix_with(), Some("new/"));
    let missing_website = root.get_bucket_website().bucket(EMPTY).send().await.unwrap_err();
    assert_eq!(refusal(&missing_website), (Some("NoSuchWebsiteConfiguration".to_owned()), Some(404)));
    let unusual_website = root.get_bucket_website().bucket(UNUSUAL).send().await;
    if gateway {
        assert_eq!(refusal(&unusual_website.unwrap_err()), (Some("InternalError".to_owned()), Some(500)));
    } else {
        assert_eq!(unusual_website.unwrap().error_document().unwrap().key(), long_error_key);
    }

    let output = root.get_bucket_logging().bucket(CONFIGURED).send().await.unwrap();
    let enabled = output.logging_enabled().expect("stored logging target");
    assert_eq!(enabled.target_bucket(), EMPTY);
    assert_eq!(enabled.target_prefix(), "logs/");
    let output = root.get_bucket_logging().bucket(EMPTY).send().await.unwrap();
    assert!(output.logging_enabled().is_none());

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_reads_core_bucket_configurations() {
    with_stack(GATEWAY, |server| core_configuration_reads(server, true));
}

#[test]
fn the_legacy_stack_reads_core_bucket_configurations() {
    with_stack(LEGACY, |server| core_configuration_reads(server, false));
}

fn put_bucket_logging<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let enabled = LoggingEnabled::builder()
            .target_bucket("logging-target")
            .target_prefix("café & tea/")
            .build()
            .expect("logging target and prefix");
        let status = BucketLoggingStatus::builder().logging_enabled(enabled).build();
        outcome(
            client
                .put_bucket_logging()
                .bucket(bucket)
                .bucket_logging_status(status)
                .send()
                .await,
        )
    })
}

async fn bucket_logging_mutations(server: RustFSServer) {
    const BUCKET: &str = "logging-write-granted-s3alias";
    const OTHER: &str = "logging-write-other";
    const MISSING: &str = "logging-write-missing";
    const WRITER: &str = "bucketloggingwriter";
    const READER: &str = "bucketloggingreader";

    for bucket in [BUCKET, OTHER] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketLogging", BUCKET).await;
    create_user_allowed_on_one_bucket(READER, "s3:GetBucketLogging", BUCKET).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, WRITER, USER_SECRET_KEY);
    let reader = s3_client(&endpoint, READER, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    assert_eq!(put_bucket_logging(&writer, BUCKET).await, Ok(()), "grant on the requested bucket");
    for (client, bucket, expected) in [
        (&writer, OTHER, ("AccessDenied", 403)),
        (&reader, BUCKET, ("AccessDenied", 403)),
        (&anonymous, BUCKET, ("AccessDenied", 403)),
        (&root, MISSING, ("NoSuchBucket", 404)),
    ] {
        let error = put_bucket_logging(client, bucket).await.expect_err("logging write refusal");
        assert_eq!((error.code.as_deref(), error.status), (Some(expected.0), Some(expected.1)));
    }
    let stored = metadata_sys::get_config_from_disk(BUCKET)
        .await
        .expect("durable logging config");
    assert_eq!(
        stored.logging_config_xml,
        "<BucketLoggingStatus><LoggingEnabled><TargetBucket>logging-target</TargetBucket><TargetPrefix>café &amp; tea/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>".as_bytes()
    );
    let response = reader
        .get_bucket_logging()
        .bucket(BUCKET)
        .send()
        .await
        .expect("read logging config");
    let enabled = response.logging_enabled().expect("logging is enabled");
    assert_eq!((enabled.target_bucket(), enabled.target_prefix()), ("logging-target", "café & tea/"));
    assert!(
        metadata_sys::get_config_from_disk(OTHER)
            .await
            .expect("untouched bucket")
            .logging_config_xml
            .is_empty()
    );

    writer
        .put_bucket_logging()
        .bucket(BUCKET)
        .bucket_logging_status(BucketLoggingStatus::builder().build())
        .send()
        .await
        .expect("disable logging");
    let stored = metadata_sys::get_config_from_disk(BUCKET)
        .await
        .expect("durable disabled logging config");
    assert_eq!(stored.logging_config_xml, b"<BucketLoggingStatus></BucketLoggingStatus>");
    let response = reader
        .get_bucket_logging()
        .bucket(BUCKET)
        .send()
        .await
        .expect("read disabled logging config");
    assert!(response.logging_enabled().is_none());

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_mutates_bucket_logging() {
    with_stack(GATEWAY, bucket_logging_mutations);
}

#[test]
fn the_legacy_stack_mutates_bucket_logging() {
    with_stack(LEGACY, bucket_logging_mutations);
}

fn put_bucket_accelerate_configuration<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let config = AccelerateConfiguration::builder()
            .status(BucketAccelerateStatus::Enabled)
            .build();
        outcome(
            client
                .put_bucket_accelerate_configuration()
                .bucket(bucket)
                .accelerate_configuration(config)
                .send()
                .await,
        )
    })
}

fn put_bucket_request_payment<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let config = RequestPaymentConfiguration::builder()
            .payer(Payer::Requester)
            .build()
            .expect("requester payer");
        outcome(
            client
                .put_bucket_request_payment()
                .bucket(bucket)
                .request_payment_configuration(config)
                .send()
                .await,
        )
    })
}

fn website_configuration(suffix: &str) -> WebsiteConfiguration {
    let index = IndexDocument::builder()
        .suffix(suffix)
        .build()
        .expect("index document suffix");
    WebsiteConfiguration::builder().index_document(index).build()
}

fn put_bucket_website<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(
            client
                .put_bucket_website()
                .bucket(bucket)
                .website_configuration(website_configuration("index & pages.html"))
                .send()
                .await,
        )
    })
}

fn delete_bucket_website<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_bucket_website().bucket(bucket).send().await) })
}

async fn core_configuration_mutations(server: RustFSServer) {
    const WRITABLE: &str = "core-writes-granted-s3alias";
    const OTHER: &str = "core-writes-other";
    const MISSING: &str = "core-writes-missing";
    for bucket in [WRITABLE, OTHER] {
        create_bucket_in_the_store(bucket).await;
    }
    seed_website(OTHER).await;

    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = "coreconfigpolicywriter";
    let reader = "coreconfigpolicyreader";
    create_user_allowed_on_one_bucket(writer, "s3:PutBucketPolicy", WRITABLE).await;
    create_user_allowed_on_one_bucket(reader, "s3:GetBucketPolicy", WRITABLE).await;
    let writer = s3_client(&endpoint, writer, USER_SECRET_KEY);
    let reader = s3_client(&endpoint, reader, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    for row in MATRIX.iter().filter(|row| {
        matches!(
            row.metric,
            S3Operation::PutBucketAccelerateConfiguration
                | S3Operation::PutBucketRequestPayment
                | S3Operation::PutBucketWebsite
                | S3Operation::DeleteBucketWebsite
        )
    }) {
        let operation = row.operation;
        let call = row.call;
        assert_eq!(call(&writer, WRITABLE).await, Ok(()), "{operation} grant");
        for (caller, bucket) in [(&writer, OTHER), (&reader, WRITABLE), (&anonymous, WRITABLE)] {
            let error = call(caller, bucket).await.expect_err(operation);
            assert_eq!((error.code.as_deref(), error.status), (Some("AccessDenied"), Some(403)), "{operation}");
        }
        let error = call(&root, MISSING).await.expect_err(operation);
        assert_eq!((error.code.as_deref(), error.status), (Some("NoSuchBucket"), Some(404)), "{operation}");

        let stored = metadata_sys::get_config_from_disk(WRITABLE)
            .await
            .expect("persisted bucket metadata");
        match row.metric {
            S3Operation::PutBucketAccelerateConfiguration => {
                let config = stored.accelerate_config.expect("stored acceleration");
                assert_eq!(config.status.as_deref(), Some("Enabled"));
            }
            S3Operation::PutBucketRequestPayment => {
                let config = stored.request_payment_config.expect("stored request payment");
                assert_eq!(config.payer, "Requester");
            }
            S3Operation::PutBucketWebsite => {
                let config = stored.website_config.expect("stored website");
                assert_eq!(config.index_document.expect("index document").suffix, "index & pages.html");
            }
            S3Operation::DeleteBucketWebsite => assert!(stored.website_config_xml.is_empty()),
            _ => unreachable!("only core bucket configuration mutations"),
        }
    }
    let stored = metadata_sys::get_config_from_disk(OTHER)
        .await
        .expect("persisted bucket metadata");
    assert!(stored.accelerate_config_xml.is_empty());
    assert!(stored.request_payment_config_xml.is_empty());
    assert_eq!(stored.website_config_xml, WEBSITE_CONFIG);

    for (client, bucket, expected) in [
        (&writer, WRITABLE, ("MalformedXML", 400)),
        (&root, MISSING, ("NoSuchBucket", 404)),
        (&reader, WRITABLE, ("AccessDenied", 403)),
    ] {
        let result = client
            .put_bucket_website()
            .bucket(bucket)
            .website_configuration(website_configuration("nested/index.html"))
            .send()
            .await;
        let error = outcome(result).expect_err("invalid website suffix");
        assert_eq!((error.code.as_deref(), error.status), (Some(expected.0), Some(expected.1)));
    }
    assert!(
        metadata_sys::get_config_from_disk(WRITABLE)
            .await
            .unwrap()
            .website_config_xml
            .is_empty()
    );
    assert_eq!(delete_bucket_website(&writer, WRITABLE).await, Ok(()), "delete without a stored website");

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_mutates_core_bucket_configurations() {
    with_stack(GATEWAY, core_configuration_mutations);
}

#[test]
fn the_legacy_stack_mutates_core_bucket_configurations() {
    with_stack(LEGACY, core_configuration_mutations);
}
