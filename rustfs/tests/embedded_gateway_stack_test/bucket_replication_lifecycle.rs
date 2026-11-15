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

//! Refusal-matrix rows and behaviour of migration batch A3 (rustfs/backlog#2765): the lifecycle,
//! replication and Object Lock configuration reads.
//!
//! Configurations are stored through the bucket metadata, not through their Put operations, so
//! these tests do not depend on another increment being served.

use super::bucket_configs::StatusCapture;
use super::{
    GATEWAY, LEGACY, MatrixRow, Outcome, Refusal, USER_SECRET_KEY, create_bucket_in_the_store, create_user_allowed_on_one_bucket,
    outcome, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_REPLICATION_CONFIG, OBJECT_LOCK_CONFIG};
use rustfs_ecstore::api::bucket::metadata_sys;
use rustfs_iam::store::UserType;
use rustfs_policy::policy::Policy;
use rustfs_s3_ops::S3Operation;

fn lifecycle_delete_metric_total() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|snapshot| snapshot.op == S3Operation::DeleteBucketLifecycle.as_str())
        .map(|snapshot| snapshot.total)
        .unwrap_or_default()
}

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "PutBucketLifecycleConfiguration",
        metric: S3Operation::PutBucketLifecycleConfiguration,
        has_error_body: true,
        call: put_lifecycle_matrix,
    },
    MatrixRow {
        operation: "GetBucketLifecycleConfiguration",
        metric: S3Operation::GetBucketLifecycleConfiguration,
        has_error_body: true,
        call: stored::<0>,
    },
    MatrixRow {
        operation: "GetBucketReplication",
        metric: S3Operation::GetBucketReplication,
        has_error_body: true,
        call: stored::<1>,
    },
    MatrixRow {
        operation: "GetObjectLockConfiguration",
        metric: S3Operation::GetObjectLockConfiguration,
        has_error_body: true,
        call: stored::<2>,
    },
];

/// Rules are disabled, so the stored documents change nothing another family's rows observe.
const LIFECYCLE: &[u8] = b"<LifecycleConfiguration><Rule><Expiration><Days>30</Days></Expiration><Filter><Prefix>logs/</Prefix>\
</Filter><ID>expire-logs</ID><Status>Disabled</Status><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition>\
</Rule></LifecycleConfiguration>";
const REPLICATION: &[u8] = b"<ReplicationConfiguration><Role>arn:rustfs:replication:us-east-1:target:remote</Role><Rule>\
<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::remote</Bucket>\
</Destination><ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication><Filter><Prefix>a/</Prefix>\
</Filter><ID>to-remote</ID><Priority>3</Priority><Status>Disabled</Status></Rule></ReplicationConfiguration>";
const OBJECT_LOCK: &[u8] = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
<Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>";

async fn store_configurations(bucket: &str) {
    for (file, document) in [
        (BUCKET_LIFECYCLE_CONFIG, LIFECYCLE),
        (BUCKET_REPLICATION_CONFIG, REPLICATION),
        (OBJECT_LOCK_CONFIG, OBJECT_LOCK),
    ] {
        metadata_sys::update(bucket, file, document.to_vec())
            .await
            .unwrap_or_else(|error| panic!("store {file} on {bucket}: {error}"));
    }
}

/// The matrix serves root only where something is stored, so each row stores first.
fn stored<'a, const READ: usize>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        store_configurations(bucket).await;
        READS[READ](client, bucket).await
    })
}

fn get_lifecycle<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_lifecycle_configuration().bucket(bucket).send().await) })
}

fn get_replication<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_replication().bucket(bucket).send().await) })
}

fn get_object_lock<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_object_lock_configuration().bucket(bucket).send().await) })
}

const READS: [for<'a> fn(&'a Client, &'a str) -> BoxFuture<'a, Outcome>; 3] = [get_lifecycle, get_replication, get_object_lock];

/// An ordinary name, and one RustFS creates and serves that the AWS bucket grammar reserves.
const STORED_BUCKETS: [&str; 2] = ["a3-stored", "sthree-a3-stored"];
const EMPTY_BUCKETS: [&str; 2] = ["a3-empty", "a3-empty-s3alias"];
const MISSING_BUCKET: &str = "a3-missing";
const SCOPED_USER: &str = "gatewaystacka3scoped";

fn refused(code: &str, status: u16) -> Outcome {
    Err(Refusal {
        code: Some(code.to_owned()),
        status: Some(status),
        request_ids: [None, None],
    })
}

/// The code and status of `outcome`, without its request ids.
fn answer(outcome: Outcome) -> Outcome {
    outcome.map_err(|refusal| Refusal {
        request_ids: [None, None],
        ..refusal
    })
}

/// A user allowed the three reads on `buckets` only.
async fn create_user_scoped_to(buckets: [&str; 2]) {
    let iam = get_global_app_context().expect("embedded server context").iam().handle();
    let actions = r#"["s3:GetBucketLifecycle","s3:GetReplicationConfiguration","s3:GetBucketObjectLockConfiguration"]"#;
    let [first, second] = buckets;
    let resources = format!(r#"["arn:aws:s3:::{first}","arn:aws:s3:::{second}"]"#);
    let document =
        format!(r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":{actions},"Resource":{resources}}}]}}"#);
    let policy = Policy::parse_config(document.as_bytes()).expect("scoped policy");
    iam.set_policy("a3-scoped-reads", policy).await.expect("set scoped policy");
    iam.create_user(
        SCOPED_USER,
        &rustfs_madmin::AddOrUpdateUserReq {
            secret_key: USER_SECRET_KEY.to_string(),
            policy: None,
            status: rustfs_madmin::AccountStatus::Enabled,
        },
    )
    .await
    .expect("create scoped user");
    iam.policy_db_set(SCOPED_USER, UserType::Reg, false, "a3-scoped-reads")
        .await
        .expect("attach scoped policy");
}

/// The three reads answer what is stored, refuse a missing bucket and a missing configuration
/// with their own codes, and refuse a caller allowed on other buckets only, for an ordinary and a
/// reserved-shaped name alike.
async fn the_configuration_reads_answer_what_is_stored(server: RustFSServer) {
    for bucket in STORED_BUCKETS.into_iter().chain(EMPTY_BUCKETS) {
        create_bucket_in_the_store(bucket).await;
    }
    for bucket in STORED_BUCKETS {
        store_configurations(bucket).await;
    }
    create_user_scoped_to(STORED_BUCKETS).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let scoped = s3_client(&endpoint, SCOPED_USER, USER_SECRET_KEY);

    let absent = [
        refused("NoSuchLifecycleConfiguration", 404),
        refused("ReplicationConfigurationNotFoundError", 404),
        refused("ObjectLockConfigurationNotFoundError", 404),
    ];
    for (read, absent) in READS.into_iter().zip(absent) {
        assert_eq!(answer(read(&root, MISSING_BUCKET).await), refused("NoSuchBucket", 404));
        for (stored, empty) in STORED_BUCKETS.into_iter().zip(EMPTY_BUCKETS) {
            assert_eq!(answer(read(&root, empty).await), absent, "{empty}");
            assert_eq!(answer(read(&scoped, empty).await), refused("AccessDenied", 403), "{empty}");
            assert_eq!(read(&scoped, stored).await, Ok(()), "{stored}");
        }
    }
    for bucket in STORED_BUCKETS {
        assert_answers_what_is_stored(&root, bucket).await;
    }

    server.shutdown().await;
}

async fn assert_answers_what_is_stored(root: &Client, bucket: &str) {
    let lifecycle = root
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("stored lifecycle");
    let rules: Vec<_> = lifecycle
        .rules()
        .iter()
        .map(|rule| {
            let transitions: Vec<_> = rule
                .transitions()
                .iter()
                .map(|transition| (transition.days(), transition.storage_class().map(|class| class.as_str().to_owned())))
                .collect();
            let expiration = rule.expiration().and_then(|expiration| expiration.days());
            let prefix = rule.filter().and_then(|filter| filter.prefix()).map(str::to_owned);
            (
                rule.id().map(str::to_owned),
                rule.status().as_str().to_owned(),
                prefix,
                expiration,
                transitions,
            )
        })
        .collect();
    let expected_rule = (
        Some("expire-logs".to_owned()),
        "Disabled".to_owned(),
        Some("logs/".to_owned()),
        Some(30),
        vec![(Some(10), Some("WARM".to_owned()))],
    );
    assert_eq!(rules, [expected_rule]);

    let replication = root
        .get_bucket_replication()
        .bucket(bucket)
        .send()
        .await
        .expect("stored replication");
    let config = replication.replication_configuration().expect("a replication configuration");
    assert_eq!(config.role(), "arn:rustfs:replication:us-east-1:target:remote");
    let rules: Vec<_> = config
        .rules()
        .iter()
        .map(|rule| {
            (
                rule.id().map(str::to_owned),
                rule.priority(),
                rule.status().as_str().to_owned(),
                rule.destination().map(|destination| destination.bucket().to_owned()),
                rule.existing_object_replication()
                    .map(|existing| existing.status().as_str().to_owned()),
                rule.delete_marker_replication()
                    .and_then(|marker| marker.status())
                    .map(|status| status.as_str().to_owned()),
            )
        })
        .collect();
    let expected_rule = (
        Some("to-remote".to_owned()),
        Some(3),
        "Disabled".to_owned(),
        Some("arn:aws:s3:::remote".to_owned()),
        Some("Enabled".to_owned()),
        Some("Enabled".to_owned()),
    );
    assert_eq!(rules, [expected_rule]);

    let lock = root
        .get_object_lock_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("stored object lock");
    let config = lock.object_lock_configuration().expect("an object lock configuration");
    assert_eq!(config.object_lock_enabled().map(|enabled| enabled.as_str()), Some("Enabled"));
    let retention = config
        .rule()
        .and_then(|rule| rule.default_retention())
        .expect("a default retention");
    assert_eq!(
        (retention.mode().map(|mode| mode.as_str()), retention.days(), retention.years()),
        (Some("GOVERNANCE"), Some(1), None)
    );
}

#[test]
fn the_gateway_stack_answers_the_configuration_reads_as_stored() {
    with_stack(GATEWAY, the_configuration_reads_answer_what_is_stored);
}

#[test]
fn the_legacy_stack_answers_the_configuration_reads_as_stored() {
    with_stack(LEGACY, the_configuration_reads_answer_what_is_stored);
}

/// The ordinary and the reserved-shaped bucket the mutations write to.
const MUTATED_BUCKETS: [&str; 2] = ["a3-mutated", "sthree-a3-mutated"];
/// A bucket the writer below is allowed on, so its refusals elsewhere are about the resource.
const WRITER_BUCKET: &str = "a3-writer-own";
const WRITER_USER: &str = "gatewaystacka3writer";
/// The tier the lifecycle documents transition to; registered as a mock backend.
const TIER: &str = "WARM";

/// Replaces the body the SDK serialized with `document`, as a client that writes its own XML
/// sends it: the SDK's checksum headers go, a Content-MD5 of `document` is set, and the request
/// is signed after the change.
fn send_document(request: &mut aws_smithy_runtime_api::http::Request, document: &'static str) {
    use md5::Digest as _;
    for name in [
        "content-md5",
        "x-amz-sdk-checksum-algorithm",
        "x-amz-checksum-crc32",
        "x-amz-checksum-crc32c",
    ] {
        request.headers_mut().remove(name);
    }
    let md5 = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(document.as_bytes()));
    request.headers_mut().insert("content-md5", md5);
    request.headers_mut().insert("content-length", document.len().to_string());
    *request.body_mut() = aws_smithy_types::body::SdkBody::from(document);
}

fn lifecycle_document() -> aws_sdk_s3::types::BucketLifecycleConfiguration {
    use aws_sdk_s3::types::{BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
    let rule = LifecycleRule::builder()
        .status(ExpirationStatus::Disabled)
        .filter(LifecycleRuleFilter::builder().prefix("placeholder/").build())
        .build()
        .expect("placeholder rule");
    BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("placeholder configuration")
}

/// A PutBucketLifecycleConfiguration carrying `document` as its body.
async fn put_lifecycle(client: &Client, bucket: &str, document: &'static str) -> Outcome {
    let put = client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_document())
        .customize()
        .mutate_request(move |request| send_document(request, document));
    outcome(put.send().await)
}

/// The lifecycle bytes the store holds for `bucket`, without the expiry stamp: the stamp is the
/// wall clock of the write whenever the document holds an expiry action.
async fn stored_lifecycle(bucket: &str) -> String {
    let metadata = metadata_sys::get(bucket).await.expect("bucket metadata");
    let stored = String::from_utf8(metadata.lifecycle_config_xml.clone()).expect("stored lifecycle is UTF-8");
    match (stored.find("<ExpiryUpdatedAt>"), stored.find("</ExpiryUpdatedAt>")) {
        (Some(start), Some(end)) => format!("{}{}", &stored[..start], &stored[end + "</ExpiryUpdatedAt>".len()..]),
        _ => stored,
    }
}

/// Every member a stored lifecycle rule can carry without an expiry action, two rules without an
/// ID, a dated transition, and a Filter with every predicate.
const EVERY_MEMBER: &str = "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix>\
<Tag><Key>tier</Key><Value>cold</Value></Tag><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan><ObjectSizeLessThan>99\
</ObjectSizeLessThan></And></Filter><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition>\
<NoncurrentVersionTransition><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions>\
<StorageClass>WARM</StorageClass></NoncurrentVersionTransition>\
</Rule><Rule><ID>rule-1</ID><Status>Disabled</Status><Prefix>old/</Prefix><Transition><Date>2099-01-01T00:00:00Z</Date>\
<StorageClass>WARM</StorageClass></Transition><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation>\
</AbortIncompleteMultipartUpload></Rule><Rule><Status>Disabled</Status><Filter><Tag><Key>k</Key><Value>v</Value></Tag>\
</Filter><Transition><Days>1</Days><StorageClass>WARM</StorageClass></Transition></Rule><Rule><ID>markers</ID>\
<Status>Disabled</Status><Filter><Prefix>markers/</Prefix></Filter><DelMarkerExpiration><Days>4</Days>\
</DelMarkerExpiration></Rule></LifecycleConfiguration>";
/// A midnight in the local time of a +08:00 offset.
const OFFSET_MIDNIGHT: &str = "<LifecycleConfiguration><Rule><ID>offset</ID><Status>Disabled</Status><Filter><Prefix>a/\
</Prefix></Filter><Expiration><Date>2099-01-01T00:00:00+08:00</Date></Expiration></Rule></LifecycleConfiguration>";
/// A midnight in UTC, written in the local time of a +08:00 offset.
const OFFSET_UTC_MIDNIGHT: &str = "<LifecycleConfiguration><Rule><ID>offset</ID><Status>Disabled</Status><Filter><Prefix>a/\
</Prefix></Filter><Expiration><Date>2099-01-01T08:00:00+08:00</Date></Expiration></Rule></LifecycleConfiguration>";
const OFFSET_TRANSITION: &str = "<LifecycleConfiguration><Rule><ID>days</ID><Status>Disabled</Status><Filter><Prefix>new/</Prefix>\
</Filter><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition></Rule><Rule><ID>date</ID><Status>Disabled</Status>\
<Filter><Prefix>old/</Prefix></Filter><Transition><Date>1970-01-01T00:00:00-08:00</Date><StorageClass>WARM</StorageClass>\
</Transition></Rule></LifecycleConfiguration>";
const NEGATIVE_TRANSITION_INSTANT: &str = "<LifecycleConfiguration><Rule><ID>date</ID><Status>Disabled</Status><Filter><Prefix>old/</Prefix>\
</Filter><Transition><Date>1970-01-01T00:00:00+08:00</Date><StorageClass>WARM</StorageClass></Transition></Rule></LifecycleConfiguration>";
const OFFSET_SUB_MILLISECOND: &str = "<LifecycleConfiguration><Rule><ID>fraction</ID><Status>Disabled</Status><Filter><Prefix>a/\
</Prefix></Filter><Expiration><Date>2099-01-01T00:00:00.0001+08:00</Date></Expiration></Rule></LifecycleConfiguration>";
const SUB_MILLISECOND: &str = "<LifecycleConfiguration><Rule><ID>fraction</ID><Status>Disabled</Status><Filter><Prefix>a/\
</Prefix></Filter><Expiration><Date>2099-01-01T00:00:00.0001Z</Date></Expiration></Rule></LifecycleConfiguration>";
const MALFORMED: &str = "<LifecycleConfiguration><Rule><Status>Enabled</Status>";
const LOWERCASE_STATUS: &str = "<LifecycleConfiguration><Rule><ID>r</ID><Status>enabled</Status><Filter><Prefix>a/</Prefix>\
</Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
const ZERO_DAYS: &str = "<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>a/</Prefix>\
</Filter><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>";
const UNKNOWN_TIER: &str = "<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>a/</Prefix>\
</Filter><Transition><Days>1</Days><StorageClass>NOPE</StorageClass></Transition></Rule></LifecycleConfiguration>";
const TWO_PREDICATES: &str = "<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>a/</Prefix>\
<Tag><Key>k</Key><Value>v</Value></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
const NO_RULE: &str = "<LifecycleConfiguration></LifecycleConfiguration>";

const EVERY_MEMBER_STORED: &str = "<LifecycleConfiguration><Rule><Filter><And><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan><ObjectSizeLessThan>99</ObjectSizeLessThan><Prefix>logs/</Prefix><Tag><Key>tier</Key><Value>cold</Value></Tag></And></Filter><ID>rule-0</ID><NoncurrentVersionTransition><NewerNoncurrentVersions>2</NewerNoncurrentVersions><NoncurrentDays>3</NoncurrentDays><StorageClass>WARM</StorageClass></NoncurrentVersionTransition><Status>Enabled</Status><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition></Rule><Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload><ID>rule-1</ID><Prefix>old/</Prefix><Status>Disabled</Status><Transition><Date>2099-01-01T00:00:00.000Z</Date><StorageClass>WARM</StorageClass></Transition></Rule><Rule><Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter><ID>rule-2</ID><Status>Disabled</Status><Transition><Days>1</Days><StorageClass>WARM</StorageClass></Transition></Rule><Rule><DelMarkerExpiration><Days>4</Days></DelMarkerExpiration><Filter><Prefix>markers/</Prefix></Filter><ID>markers</ID><Status>Disabled</Status></Rule></LifecycleConfiguration>";
const OFFSET_MIDNIGHT_STORED: &str = "<LifecycleConfiguration><Rule><Expiration><Date>2099-01-01T00:00:00.000Z</Date></Expiration><Filter><Prefix>a/</Prefix></Filter><ID>offset</ID><Status>Disabled</Status></Rule></LifecycleConfiguration>";

const OFFSET_TRANSITION_STORED: &str = "<LifecycleConfiguration><Rule><Filter><Prefix>new/</Prefix></Filter><ID>days</ID><Status>Disabled</Status><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition></Rule><Rule><Filter><Prefix>old/</Prefix></Filter><ID>date</ID><Status>Disabled</Status><Transition><Date>1970-01-01T00:00:00.000Z</Date><StorageClass>WARM</StorageClass></Transition></Rule></LifecycleConfiguration>";

const VALID_DAY: &str = "<LifecycleConfiguration><Rule><ID>expiry</ID><Status>Disabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>";

async fn lifecycle_put_preserves_responses_and_storage(server: RustFSServer) {
    for bucket in MUTATED_BUCKETS.into_iter().chain([WRITER_BUCKET]) {
        create_bucket_in_the_store(bucket).await;
    }
    rustfs_ecstore::api::tier::test_util::register_mock_tier(&rustfs_ecstore::api::runtime::global_tier_config_mgr(), TIER).await;
    create_user_allowed_on_one_bucket(WRITER_USER, "s3:PutBucketLifecycle", WRITER_BUCKET).await;
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let writer = s3_client(&server.endpoint(), WRITER_USER, USER_SECRET_KEY);
    assert_eq!(
        answer(put_lifecycle(&root, MISSING_BUCKET, EVERY_MEMBER).await),
        refused("NoSuchBucket", 404)
    );
    for bucket in MUTATED_BUCKETS {
        for (label, document, code) in [
            ("malformed", MALFORMED, "MalformedXML"),
            ("lowercase", LOWERCASE_STATUS, "MalformedXML"),
            ("zero", ZERO_DAYS, "InvalidArgument"),
            ("unknown tier", UNKNOWN_TIER, "InvalidArgument"),
            ("two predicates", TWO_PREDICATES, "MalformedXML"),
            ("no rule", NO_RULE, "MalformedXML"),
            ("offset utc midnight", OFFSET_UTC_MIDNIGHT, "InvalidArgument"),
            ("sub-millisecond", SUB_MILLISECOND, "InvalidArgument"),
            ("offset sub-millisecond", OFFSET_SUB_MILLISECOND, "InvalidArgument"),
            ("negative transition instant", NEGATIVE_TRANSITION_INSTANT, "InvalidArgument"),
        ] {
            let before = stored_lifecycle(bucket).await;
            assert_eq!(
                answer(put_lifecycle(&root, bucket, document).await),
                refused(code, 400),
                "{bucket} {label}"
            );
            assert_eq!(stored_lifecycle(bucket).await, before, "{bucket} {label} must not store");
        }
        let before = stored_lifecycle(bucket).await;
        assert_eq!(answer(put_lifecycle(&writer, bucket, EVERY_MEMBER).await), refused("AccessDenied", 403));
        assert_eq!(stored_lifecycle(bucket).await, before, "a wrong-resource refusal must not store");
        assert_eq!(put_lifecycle(&root, bucket, OFFSET_MIDNIGHT).await, Ok(()));
        assert_eq!(stored_lifecycle(bucket).await, OFFSET_MIDNIGHT_STORED);
        assert_eq!(put_lifecycle(&root, bucket, OFFSET_TRANSITION).await, Ok(()));
        assert_eq!(stored_lifecycle(bucket).await, OFFSET_TRANSITION_STORED);
        assert_eq!(put_lifecycle(&root, bucket, EVERY_MEMBER).await, Ok(()));
        assert_eq!(stored_lifecycle(bucket).await, EVERY_MEMBER_STORED);
    }
    assert_eq!(put_lifecycle(&writer, WRITER_BUCKET, EVERY_MEMBER).await, Ok(()));
    assert_eq!(stored_lifecycle(WRITER_BUCKET).await, EVERY_MEMBER_STORED);
    server.shutdown().await;
}

#[test]
fn legacy_lifecycle_put_preserves_responses_and_storage() {
    with_stack(LEGACY, lifecycle_put_preserves_responses_and_storage);
}

#[test]
fn gateway_lifecycle_put_preserves_responses_and_storage() {
    with_stack(GATEWAY, lifecycle_put_preserves_responses_and_storage);
}

#[test]
fn gateway_lifecycle_put_accepts_owned_configuration() {
    with_stack(GATEWAY, |server| async move {
        create_bucket_in_the_store(WRITER_BUCKET).await;
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        assert_eq!(put_lifecycle(&client, WRITER_BUCKET, VALID_DAY).await, Ok(()));
        server.shutdown().await;
    });
}

fn put_lifecycle_matrix<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(put_lifecycle(client, bucket, VALID_DAY))
}

async fn lifecycle_put_authorization_uses_action_and_bucket(server: RustFSServer) {
    for bucket in [WRITER_BUCKET, MUTATED_BUCKETS[0]] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(WRITER_USER, "s3:PutBucketLifecycle", WRITER_BUCKET).await;
    let writer = s3_client(&server.endpoint(), WRITER_USER, USER_SECRET_KEY);
    assert_eq!(put_lifecycle(&writer, WRITER_BUCKET, VALID_DAY).await, Ok(()));
    assert!(!stored_lifecycle(WRITER_BUCKET).await.is_empty());
    assert_eq!(
        answer(put_lifecycle(&writer, MUTATED_BUCKETS[0], VALID_DAY).await),
        refused("AccessDenied", 403)
    );
    assert!(stored_lifecycle(MUTATED_BUCKETS[0]).await.is_empty());
    server.shutdown().await;
}

async fn lifecycle_xml_on_disk(bucket: &str) -> Vec<u8> {
    metadata_sys::get_config_from_disk(bucket)
        .await
        .expect("persisted bucket metadata")
        .lifecycle_config_xml
}

async fn lifecycle_delete_preserves_responses_and_storage(server: RustFSServer) {
    let configured = "a3-delete-configured";
    let unconfigured = "a3-delete-empty";
    let alias = "sthree-a3-delete";
    let no_action_bucket = "a3-delete-no-action";
    for bucket in [
        configured,
        unconfigured,
        alias,
        WRITER_BUCKET,
        MUTATED_BUCKETS[0],
        no_action_bucket,
    ] {
        create_bucket_in_the_store(bucket).await;
    }
    for bucket in [configured, alias, MUTATED_BUCKETS[0], WRITER_BUCKET, no_action_bucket] {
        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, LIFECYCLE.to_vec())
            .await
            .expect("store lifecycle configuration");
    }
    create_user_allowed_on_one_bucket(WRITER_USER, "s3:PutBucketLifecycle", WRITER_BUCKET).await;
    let reader_user = "gatewaystacka3lifecyclereader";
    create_user_allowed_on_one_bucket(reader_user, "s3:GetBucketLifecycle", no_action_bucket).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, WRITER_USER, USER_SECRET_KEY);
    let reader = s3_client(&endpoint, reader_user, USER_SECRET_KEY);

    for (client, bucket) in [
        (&root, configured),
        (&root, unconfigured),
        (&root, alias),
        (&writer, WRITER_BUCKET),
    ] {
        let before = lifecycle_delete_metric_total();
        let status = StatusCapture::default();
        let result = client
            .delete_bucket_lifecycle()
            .bucket(bucket)
            .customize()
            .interceptor(status.clone())
            .send()
            .await;
        assert_eq!(outcome(result), Ok(()), "{bucket} delete with granted lifecycle action");
        assert_eq!(status.take(), Some(204), "{bucket} wire status");
        assert!(lifecycle_xml_on_disk(bucket).await.is_empty(), "{bucket} stored bytes");
        assert_eq!(lifecycle_delete_metric_total(), before + 1, "{bucket} operation metric count");
    }

    for (client, bucket, label) in [
        (&writer, MUTATED_BUCKETS[0], "writer policy names a different bucket"),
        (&reader, no_action_bucket, "reader policy lacks the lifecycle write action"),
    ] {
        let error = client.delete_bucket_lifecycle().bucket(bucket).send().await.expect_err(label);
        assert_eq!((error.code(), error.message()), (Some("AccessDenied"), Some("Access Denied")), "{label}");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(403), "{label}");
        assert_eq!(lifecycle_xml_on_disk(bucket).await, LIFECYCLE, "{label} preserves bytes");
    }

    let missing = root.delete_bucket_lifecycle().bucket("a3-delete-missing").send().await;
    let error = missing.expect_err("missing bucket is refused");
    assert_eq!(
        (error.code(), error.message()),
        (Some("NoSuchBucket"), Some("The specified bucket does not exist"))
    );
    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404));

    server.shutdown().await;
}

#[test]
fn legacy_lifecycle_delete_preserves_responses_and_storage() {
    with_stack(LEGACY, lifecycle_delete_preserves_responses_and_storage);
}

#[test]
fn gateway_lifecycle_delete_preserves_responses_and_storage() {
    with_stack(GATEWAY, lifecycle_delete_preserves_responses_and_storage);
}

#[test]
fn gateway_lifecycle_put_authorization_uses_action_and_bucket() {
    with_stack(GATEWAY, lifecycle_put_authorization_uses_action_and_bucket);
}

#[test]
fn legacy_lifecycle_put_authorization_uses_action_and_bucket() {
    with_stack(LEGACY, lifecycle_put_authorization_uses_action_and_bucket);
}

#[test]
fn gateway_lifecycle_put_keeps_transition_fields() {
    with_stack(GATEWAY, |server| async move {
        create_bucket_in_the_store(WRITER_BUCKET).await;
        rustfs_ecstore::api::tier::test_util::register_mock_tier(&rustfs_ecstore::api::runtime::global_tier_config_mgr(), TIER)
            .await;
        let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        assert_eq!(put_lifecycle(&root, WRITER_BUCKET, EVERY_MEMBER).await, Ok(()));
        assert_eq!(stored_lifecycle(WRITER_BUCKET).await, EVERY_MEMBER_STORED);
        server.shutdown().await;
    });
}

#[test]
fn gateway_lifecycle_put_rejects_submillisecond_date() {
    with_stack(GATEWAY, |server| async move {
        create_bucket_in_the_store(WRITER_BUCKET).await;
        let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        assert_eq!(
            answer(put_lifecycle(&root, WRITER_BUCKET, SUB_MILLISECOND).await),
            refused("InvalidArgument", 400)
        );
        assert!(stored_lifecycle(WRITER_BUCKET).await.is_empty());
        server.shutdown().await;
    });
}
