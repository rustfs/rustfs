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

//! The SDK script both stacks answer, and the operation list it must cover.
//!
//! Every operation of `impl S3 for FS` (`rustfs/src/storage/ecfs.rs`) gets a `normal` request
//! and, where the operation has one, a `negative` request that must fail with a 4xx. Setup
//! requests go through the same recorder under their own case label, so nothing the script
//! sends is left out of the comparison. Values minted per process (upload IDs, timestamps) are
//! never recorded, and the comparator reduces a version ID to whether it is `null`.

use super::compare::Observation;
use crate::common::{RustFSTestEnvironment, build_test_s3_config};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::{BoxError, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    AccelerateConfiguration, BucketAccelerateStatus, BucketCannedAcl, BucketLifecycleConfiguration, BucketLoggingStatus,
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule, CsvInput, CsvOutput,
    DefaultRetention, Delete, DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ExpirationStatus,
    ExpressionType, FileHeaderInfo, IndexDocument, InputSerialization, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    NotificationConfiguration, ObjectAttributes, ObjectCannedAcl, ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
    OutputSerialization, Payer, PublicAccessBlockConfiguration, ReplicationConfiguration, ReplicationRule, ReplicationRuleFilter,
    ReplicationRuleStatus, RequestPaymentConfiguration, RestoreRequest, SelectObjectContentEventStream, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, Tagging,
    VersioningConfiguration, WebsiteConfiguration,
};
use md5::{Digest as _, Md5};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BUCKET: &str = "dual-stack-main";
const LOCK_BUCKET: &str = "dual-stack-lock";
const SCRATCH_BUCKET: &str = "dual-stack-scratch";
const MISSING_BUCKET: &str = "dual-stack-missing";
/// Uppercase and `_` are not allowed in a bucket name.
const INVALID_BUCKET: &str = "Dual_Stack_Invalid";
const README: &str = "docs/readme.txt";
const COPY: &str = "docs/copy.txt";
const CSV: &str = "data.csv";
const LOCKED: &str = "locked.txt";
const SSE_C_KEY_NAME: &str = "sse-c.txt";
const ABSENT: &str = "absent.txt";
const MPU: &str = "mpu/one.bin";
const MPU_COPY: &str = "mpu/copy.bin";
const UNKNOWN_UPLOAD: &str = "dual-stack-unknown-upload";
/// 2099-01-01T00:00:00Z: a retention date both stacks receive verbatim.
const RETAIN_UNTIL_SECS: i64 = 4_070_908_800;
const SELECT_DEADLINE: Duration = Duration::from_secs(30);

type Fields = Vec<(&'static str, String)>;
type Captured = (u16, Vec<(String, String)>);

/// Every method of `impl S3 for FS` in `rustfs/src/storage/ecfs.rs`, by S3 operation name, sorted.
/// Derived with
/// `sed -n '/^impl S3 for FS {/,/^}/p' rustfs/src/storage/ecfs.rs | grep -oE '^    async fn [a-z_0-9]+'`;
/// this crate does not read another crate's source, so a new method there must be added here
/// and to the script.
pub(crate) const OPERATIONS: [&str; 73] = [
    "AbortMultipartUpload",
    "CompleteMultipartUpload",
    "CopyObject",
    "CreateBucket",
    "CreateMultipartUpload",
    "DeleteBucket",
    "DeleteBucketCors",
    "DeleteBucketEncryption",
    "DeleteBucketLifecycle",
    "DeleteBucketPolicy",
    "DeleteBucketReplication",
    "DeleteBucketTagging",
    "DeleteBucketWebsite",
    "DeleteObject",
    "DeleteObjectTagging",
    "DeleteObjects",
    "DeletePublicAccessBlock",
    "GetBucketAccelerateConfiguration",
    "GetBucketAcl",
    "GetBucketCors",
    "GetBucketEncryption",
    "GetBucketLifecycleConfiguration",
    "GetBucketLocation",
    "GetBucketLogging",
    "GetBucketNotificationConfiguration",
    "GetBucketPolicy",
    "GetBucketPolicyStatus",
    "GetBucketReplication",
    "GetBucketRequestPayment",
    "GetBucketTagging",
    "GetBucketVersioning",
    "GetBucketWebsite",
    "GetObject",
    "GetObjectAcl",
    "GetObjectAttributes",
    "GetObjectLegalHold",
    "GetObjectLockConfiguration",
    "GetObjectRetention",
    "GetObjectTagging",
    "GetObjectTorrent",
    "GetPublicAccessBlock",
    "HeadBucket",
    "HeadObject",
    "ListBuckets",
    "ListMultipartUploads",
    "ListObjectVersions",
    "ListObjects",
    "ListObjectsV2",
    "ListParts",
    "PutBucketAccelerateConfiguration",
    "PutBucketAcl",
    "PutBucketCors",
    "PutBucketEncryption",
    "PutBucketLifecycleConfiguration",
    "PutBucketLogging",
    "PutBucketNotificationConfiguration",
    "PutBucketPolicy",
    "PutBucketReplication",
    "PutBucketRequestPayment",
    "PutBucketTagging",
    "PutBucketVersioning",
    "PutBucketWebsite",
    "PutObject",
    "PutObjectAcl",
    "PutObjectLegalHold",
    "PutObjectLockConfiguration",
    "PutObjectRetention",
    "PutObjectTagging",
    "PutPublicAccessBlock",
    "RestoreObject",
    "SelectObjectContent",
    "UploadPart",
    "UploadPartCopy",
];

/// Records the status and headers of the one response each request receives (retries are off).
#[derive(Clone, Debug, Default)]
struct ResponseCapture(Arc<Mutex<Option<Captured>>>);

impl ResponseCapture {
    fn take(&self) -> Option<Captured> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Intercept for ResponseCapture {
    fn name(&self) -> &'static str {
        "dual-stack-response-capture"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        *self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("response capture mutex was poisoned"))? =
            Some((response.status().as_u16(), headers));
        Ok(())
    }
}

fn error_code<E: ProvideErrorMetadata>(error: &SdkError<E>) -> String {
    match error {
        SdkError::ServiceError(service) => service.err().code().unwrap_or("<service-error-without-code>").to_owned(),
        SdkError::ConstructionFailure(_) => "<construction-failure>".to_owned(),
        SdkError::TimeoutError(_) => "<timeout>".to_owned(),
        SdkError::DispatchFailure(_) => "<dispatch-failure>".to_owned(),
        SdkError::ResponseError(_) => "<response-error>".to_owned(),
        _ => "<sdk-error>".to_owned(),
    }
}

fn none<T>(_: &T) -> Fields {
    Vec::new()
}

fn md5_hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Md5::digest(bytes), hex_simd::AsciiCase::Lower)
}

struct Recorder {
    capture: ResponseCapture,
    observations: Vec<Observation>,
}

impl Recorder {
    /// Sends one request and records what came back. The request future is lazy, so nothing is
    /// on the wire before the stale capture is cleared.
    async fn step<T, E>(
        &mut self,
        op: &'static str,
        case: &'static str,
        request: impl Future<Output = Result<T, SdkError<E>>>,
        fields: impl FnOnce(&T) -> Fields,
    ) -> Option<T>
    where
        E: ProvideErrorMetadata,
    {
        let _ = self.capture.take();
        let result = request.await;
        let (status, headers) = self.capture.take().unwrap_or_default();
        let headers = headers.iter().map(|(name, value)| (name.as_str(), value.as_str()));
        match result {
            Ok(output) => {
                let observation = fields(&output)
                    .into_iter()
                    .fold(Observation::new(op, case, status, None, headers), |observation, (name, value)| {
                        observation.with_field(name, value)
                    });
                self.observations.push(observation);
                Some(output)
            }
            Err(error) => {
                let mut observation = Observation::new(op, case, status, Some(error_code(&error)), headers);
                observation.message = error.as_service_error().and_then(|e| e.message()).map(str::to_owned);
                self.observations.push(observation);
                None
            }
        }
    }

    /// Adds a field decoded after the response headers arrived (a streamed body).
    fn amend(&mut self, name: &str, value: String) {
        if let Some(last) = self.observations.last_mut() {
            last.fields.insert(name.to_owned(), value);
        }
    }
}

fn client(env: &RustFSTestEnvironment, capture: &ResponseCapture, access_key: &str, secret_key: &str) -> Client {
    let config = build_test_s3_config(&env.url, access_key, secret_key, None, "dual-stack-differential")
        .to_builder()
        // One request, one response: a retry would hide the first answer.
        .retry_config(RetryConfig::disabled())
        .interceptor(capture.clone())
        .build();
    Client::from_conf(config)
}

struct SseC {
    key: String,
    key_md5: String,
}

fn sse_c() -> SseC {
    let raw = [7u8; 32];
    SseC {
        key: base64_simd::STANDARD.encode_to_string(raw),
        key_md5: base64_simd::STANDARD.encode_to_string(Md5::digest(raw)),
    }
}

fn tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().expect("tag"))
        .build()
        .expect("tagging")
}

fn tags(set: &[Tag]) -> String {
    format!("{:?}", set.iter().map(|tag| (tag.key(), tag.value())).collect::<Vec<_>>())
}

fn grants(grants: &[aws_sdk_s3::types::Grant]) -> String {
    let mut permissions: Vec<String> = grants.iter().map(|grant| format!("{:?}", grant.permission())).collect();
    permissions.sort();
    format!("{permissions:?}")
}

/// Runs the whole script against one server and returns its transcript.
pub(crate) async fn run(env: &RustFSTestEnvironment) -> Vec<Observation> {
    let capture = ResponseCapture::default();
    let c = client(env, &capture, &env.access_key, &env.secret_key);
    let stranger = client(env, &capture, "dual-stack-stranger", "dual-stack-stranger-secret");
    let mut s = Recorder {
        capture,
        observations: Vec::new(),
    };
    // Each section holds dozens of SDK futures; boxing keeps the test future off the thread
    // stack, which a debug build otherwise overflows.
    Box::pin(buckets(&mut s, &c, &stranger)).await;
    Box::pin(objects(&mut s, &c)).await;
    Box::pin(bucket_subresources(&mut s, &c)).await;
    Box::pin(bucket_configuration(&mut s, &c)).await;
    Box::pin(object_lock(&mut s, &c)).await;
    Box::pin(multipart(&mut s, &c)).await;
    Box::pin(object_extras(&mut s, &c)).await;
    Box::pin(deletes_and_listings(&mut s, &c)).await;
    s.observations
}

async fn buckets(s: &mut Recorder, c: &Client, stranger: &Client) {
    s.step("CreateBucket", "normal", c.create_bucket().bucket(BUCKET).send(), none)
        .await;
    // Re-creating an owned bucket answers 200 in us-east-1, so it is not the negative case.
    s.step("CreateBucket", "owned", c.create_bucket().bucket(BUCKET).send(), none)
        .await;
    s.step("CreateBucket", "negative", c.create_bucket().bucket(INVALID_BUCKET).send(), none)
        .await;
    let lock = c.create_bucket().bucket(LOCK_BUCKET).object_lock_enabled_for_bucket(true);
    s.step("CreateBucket", "object-lock", lock.send(), none).await;
    s.step("CreateBucket", "scratch", c.create_bucket().bucket(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("HeadBucket", "normal", c.head_bucket().bucket(BUCKET).send(), none)
        .await;
    s.step("HeadBucket", "negative", c.head_bucket().bucket(MISSING_BUCKET).send(), none)
        .await;
    s.step("ListBuckets", "normal", c.list_buckets().send(), |o| {
        let mut names: Vec<_> = o.buckets().iter().filter_map(|bucket| bucket.name()).collect();
        names.sort_unstable();
        vec![("buckets", format!("{names:?}"))]
    })
    .await;
    s.step("ListBuckets", "negative", stranger.list_buckets().send(), none).await;
    s.step("GetBucketLocation", "normal", c.get_bucket_location().bucket(BUCKET).send(), |o| {
        let location = o
            .location_constraint()
            .map(|constraint| constraint.as_str())
            .unwrap_or_default();
        vec![("location_constraint", location.to_owned())]
    })
    .await;
    let missing = c.get_bucket_location().bucket(MISSING_BUCKET);
    s.step("GetBucketLocation", "negative", missing.send(), none).await;
    let enabled = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    let put = c
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(enabled.clone());
    s.step("PutBucketVersioning", "normal", put.send(), none).await;
    let put = c
        .put_bucket_versioning()
        .bucket(MISSING_BUCKET)
        .versioning_configuration(enabled);
    s.step("PutBucketVersioning", "negative", put.send(), none).await;
    s.step("GetBucketVersioning", "normal", c.get_bucket_versioning().bucket(BUCKET).send(), |o| {
        vec![("status", format!("{:?}", o.status()))]
    })
    .await;
    let missing = c.get_bucket_versioning().bucket(MISSING_BUCKET);
    s.step("GetBucketVersioning", "negative", missing.send(), none).await;
}

async fn objects(s: &mut Recorder, c: &Client) {
    let put = |bucket: &str, key: &str, body: &'static [u8]| {
        c.put_object()
            .bucket(bucket)
            .key(key)
            .content_type("text/plain")
            .body(ByteStream::from_static(body))
    };
    s.step("PutObject", "normal", put(BUCKET, README, b"hello from both stacks\n").send(), none)
        .await;
    s.step("PutObject", "negative", put(MISSING_BUCKET, README, b"x").send(), none)
        .await;
    s.step("PutObject", "seed-a", put(BUCKET, "docs/a.txt", b"a").send(), none)
        .await;
    s.step("PutObject", "seed-b", put(BUCKET, "logs/b.txt", b"b").send(), none)
        .await;
    let csv = put(BUCKET, CSV, b"name,size\nalpha,1\nbeta,2\n").content_type("text/csv");
    s.step("PutObject", "seed-csv", csv.send(), none).await;
    s.step("PutObject", "seed-locked", put(LOCK_BUCKET, LOCKED, b"locked").send(), none)
        .await;
    let key = sse_c();
    let encrypted = put(BUCKET, SSE_C_KEY_NAME, b"customer key")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key.key)
        .sse_customer_key_md5(&key.key_md5);
    s.step("PutObject", "sse-c", encrypted.send(), none).await;

    s.step("HeadObject", "normal", c.head_object().bucket(BUCKET).key(README).send(), none)
        .await;
    s.step("HeadObject", "negative", c.head_object().bucket(BUCKET).key(ABSENT).send(), none)
        .await;
    let head = c
        .head_object()
        .bucket(BUCKET)
        .key(SSE_C_KEY_NAME)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key.key)
        .sse_customer_key_md5(&key.key_md5);
    s.step("HeadObject", "sse-c", head.send(), none).await;

    if let Some(output) = s
        .step("GetObject", "normal", c.get_object().bucket(BUCKET).key(README).send(), none)
        .await
    {
        let body = match output.body.collect().await {
            Ok(body) => md5_hex(&body.into_bytes()),
            Err(_) => "<body-error>".to_owned(),
        };
        s.amend("body_md5", body);
    }
    let unsatisfiable = c.get_object().bucket(BUCKET).key(README).range("bytes=1000-2000");
    s.step("GetObject", "negative", unsatisfiable.send(), none).await;

    let attributes = |key: &str| {
        c.get_object_attributes()
            .bucket(BUCKET)
            .key(key)
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectSize)
    };
    s.step("GetObjectAttributes", "normal", attributes(README).send(), |o| {
        vec![
            ("etag", format!("{:?}", o.e_tag())),
            ("size", format!("{:?}", o.object_size())),
        ]
    })
    .await;
    s.step("GetObjectAttributes", "negative", attributes(ABSENT).send(), none)
        .await;

    let copy = |source: &str| {
        c.copy_object()
            .bucket(BUCKET)
            .key(COPY)
            .copy_source(format!("{BUCKET}/{source}"))
    };
    s.step("CopyObject", "normal", copy(README).send(), |o| {
        vec![("etag", format!("{:?}", o.copy_object_result().and_then(|r| r.e_tag())))]
    })
    .await;
    s.step("CopyObject", "negative", copy(ABSENT).send(), none).await;

    let put_tags = |key: &str| c.put_object_tagging().bucket(BUCKET).key(key).tagging(tagging("k", "v"));
    s.step("PutObjectTagging", "normal", put_tags(README).send(), none).await;
    s.step("PutObjectTagging", "negative", put_tags(ABSENT).send(), none).await;
    let get_tags = |key: &str| c.get_object_tagging().bucket(BUCKET).key(key);
    s.step("GetObjectTagging", "normal", get_tags(README).send(), |o| {
        vec![("tags", tags(o.tag_set()))]
    })
    .await;
    s.step("GetObjectTagging", "negative", get_tags(ABSENT).send(), none).await;
    let delete_tags = |key: &str| c.delete_object_tagging().bucket(BUCKET).key(key);
    s.step("DeleteObjectTagging", "normal", delete_tags(README).send(), none)
        .await;
    s.step("DeleteObjectTagging", "negative", delete_tags(ABSENT).send(), none)
        .await;

    let put_acl = |key: &str| c.put_object_acl().bucket(BUCKET).key(key).acl(ObjectCannedAcl::Private);
    s.step("PutObjectAcl", "normal", put_acl(README).send(), none).await;
    s.step("PutObjectAcl", "negative", put_acl(ABSENT).send(), none).await;
    let get_acl = |key: &str| c.get_object_acl().bucket(BUCKET).key(key);
    s.step("GetObjectAcl", "normal", get_acl(README).send(), |o| vec![("grants", grants(o.grants()))])
        .await;
    s.step("GetObjectAcl", "negative", get_acl(ABSENT).send(), none).await;
}

async fn bucket_subresources(s: &mut Recorder, c: &Client) {
    let put_acl = |bucket: &str| c.put_bucket_acl().bucket(bucket).acl(BucketCannedAcl::Private);
    s.step("PutBucketAcl", "normal", put_acl(BUCKET).send(), none).await;
    s.step("PutBucketAcl", "negative", put_acl(MISSING_BUCKET).send(), none).await;
    let get_acl = |bucket: &str| c.get_bucket_acl().bucket(bucket);
    s.step("GetBucketAcl", "normal", get_acl(BUCKET).send(), |o| vec![("grants", grants(o.grants()))])
        .await;
    s.step("GetBucketAcl", "negative", get_acl(MISSING_BUCKET).send(), none).await;

    let put_tags = |bucket: &str| c.put_bucket_tagging().bucket(bucket).tagging(tagging("team", "storage"));
    s.step("PutBucketTagging", "normal", put_tags(BUCKET).send(), none).await;
    s.step("PutBucketTagging", "negative", put_tags(MISSING_BUCKET).send(), none)
        .await;
    let get_tags = || c.get_bucket_tagging().bucket(BUCKET);
    s.step("GetBucketTagging", "normal", get_tags().send(), |o| vec![("tags", tags(o.tag_set()))])
        .await;
    let delete_tags = |bucket: &str| c.delete_bucket_tagging().bucket(bucket);
    s.step("DeleteBucketTagging", "normal", delete_tags(BUCKET).send(), none)
        .await;
    s.step("GetBucketTagging", "negative", get_tags().send(), none).await;
    s.step("DeleteBucketTagging", "negative", delete_tags(MISSING_BUCKET).send(), none)
        .await;

    let cors = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("GET")
                .allowed_origins("https://example.com")
                .build()
                .expect("cors rule"),
        )
        .build()
        .expect("cors configuration");
    let put_cors = |bucket: &str| c.put_bucket_cors().bucket(bucket).cors_configuration(cors.clone());
    s.step("PutBucketCors", "normal", put_cors(BUCKET).send(), none).await;
    s.step("PutBucketCors", "negative", put_cors(MISSING_BUCKET).send(), none)
        .await;
    let get_cors = || c.get_bucket_cors().bucket(BUCKET);
    s.step("GetBucketCors", "normal", get_cors().send(), |o| {
        let rules: Vec<_> = o
            .cors_rules()
            .iter()
            .map(|rule| (rule.allowed_methods(), rule.allowed_origins()))
            .collect();
        vec![("rules", format!("{rules:?}"))]
    })
    .await;
    let delete_cors = |bucket: &str| c.delete_bucket_cors().bucket(bucket);
    s.step("DeleteBucketCors", "normal", delete_cors(BUCKET).send(), none).await;
    s.step("GetBucketCors", "negative", get_cors().send(), none).await;
    s.step("DeleteBucketCors", "negative", delete_cors(MISSING_BUCKET).send(), none)
        .await;

    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":["*"]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{BUCKET}/public/*"]}}]}}"#
    );
    s.step(
        "PutBucketPolicy",
        "normal",
        c.put_bucket_policy().bucket(BUCKET).policy(policy).send(),
        none,
    )
    .await;
    let malformed = c.put_bucket_policy().bucket(BUCKET).policy("{");
    s.step("PutBucketPolicy", "negative", malformed.send(), none).await;
    let get_policy = || c.get_bucket_policy().bucket(BUCKET);
    s.step("GetBucketPolicy", "normal", get_policy().send(), none).await;
    let status = |bucket: &str| c.get_bucket_policy_status().bucket(bucket);
    s.step("GetBucketPolicyStatus", "normal", status(BUCKET).send(), |o| {
        vec![("is_public", format!("{:?}", o.policy_status().and_then(|p| p.is_public())))]
    })
    .await;
    s.step("GetBucketPolicyStatus", "negative", status(MISSING_BUCKET).send(), none)
        .await;
    let delete_policy = |bucket: &str| c.delete_bucket_policy().bucket(bucket);
    s.step("DeleteBucketPolicy", "normal", delete_policy(BUCKET).send(), none)
        .await;
    s.step("GetBucketPolicy", "negative", get_policy().send(), none).await;
    s.step("DeleteBucketPolicy", "negative", delete_policy(MISSING_BUCKET).send(), none)
        .await;
}

async fn bucket_configuration(s: &mut Recorder, c: &Client) {
    let encryption = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()
                        .expect("default encryption"),
                )
                .build(),
        )
        .build()
        .expect("encryption configuration");
    let put = |bucket: &str| {
        c.put_bucket_encryption()
            .bucket(bucket)
            .server_side_encryption_configuration(encryption.clone())
    };
    s.step("PutBucketEncryption", "normal", put(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("PutBucketEncryption", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = || c.get_bucket_encryption().bucket(SCRATCH_BUCKET);
    s.step("GetBucketEncryption", "normal", get().send(), |o| {
        let algorithms: Vec<_> = o
            .server_side_encryption_configuration()
            .map(|config| config.rules())
            .unwrap_or_default()
            .iter()
            .filter_map(|rule| rule.apply_server_side_encryption_by_default())
            .map(|default| default.sse_algorithm().as_str().to_owned())
            .collect();
        vec![("algorithms", format!("{algorithms:?}"))]
    })
    .await;
    let delete = |bucket: &str| c.delete_bucket_encryption().bucket(bucket);
    s.step("DeleteBucketEncryption", "normal", delete(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("GetBucketEncryption", "negative", get().send(), none).await;
    s.step("DeleteBucketEncryption", "negative", delete(MISSING_BUCKET).send(), none)
        .await;

    let lifecycle = BucketLifecycleConfiguration::builder()
        .rules(
            LifecycleRule::builder()
                .id("expire-tmp")
                .filter(LifecycleRuleFilter::builder().prefix("tmp/").build())
                .status(ExpirationStatus::Enabled)
                .expiration(LifecycleExpiration::builder().days(30).build())
                .build()
                .expect("lifecycle rule"),
        )
        .build()
        .expect("lifecycle configuration");
    let put = |bucket: &str| {
        c.put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(lifecycle.clone())
    };
    s.step("PutBucketLifecycleConfiguration", "normal", put(BUCKET).send(), none)
        .await;
    s.step("PutBucketLifecycleConfiguration", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = || c.get_bucket_lifecycle_configuration().bucket(BUCKET);
    s.step("GetBucketLifecycleConfiguration", "normal", get().send(), |o| {
        let rules: Vec<_> = o.rules().iter().map(|rule| (rule.id(), rule.status().as_str())).collect();
        vec![("rules", format!("{rules:?}"))]
    })
    .await;
    let delete = |bucket: &str| c.delete_bucket_lifecycle().bucket(bucket);
    s.step("DeleteBucketLifecycle", "normal", delete(BUCKET).send(), none).await;
    s.step("GetBucketLifecycleConfiguration", "negative", get().send(), none)
        .await;
    s.step("DeleteBucketLifecycle", "negative", delete(MISSING_BUCKET).send(), none)
        .await;

    let replication = ReplicationConfiguration::builder()
        .role("")
        .rules(
            ReplicationRule::builder()
                .id("to-remote")
                .priority(1)
                .status(ReplicationRuleStatus::Enabled)
                .filter(ReplicationRuleFilter::builder().prefix("").build())
                .delete_marker_replication(
                    DeleteMarkerReplication::builder()
                        .status(DeleteMarkerReplicationStatus::Disabled)
                        .build(),
                )
                .destination(
                    Destination::builder()
                        .bucket("arn:aws:s3:::dual-stack-remote")
                        .build()
                        .expect("replication destination"),
                )
                .build()
                .expect("replication rule"),
        )
        .build()
        .expect("replication configuration");
    let put = |bucket: &str| {
        c.put_bucket_replication()
            .bucket(bucket)
            .replication_configuration(replication.clone())
    };
    s.step("PutBucketReplication", "normal", put(BUCKET).send(), none).await;
    s.step("PutBucketReplication", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_bucket_replication().bucket(bucket);
    s.step("GetBucketReplication", "normal", get(BUCKET).send(), none).await;
    s.step("GetBucketReplication", "negative", get(MISSING_BUCKET).send(), none)
        .await;
    let delete = |bucket: &str| c.delete_bucket_replication().bucket(bucket);
    s.step("DeleteBucketReplication", "normal", delete(BUCKET).send(), none).await;
    s.step("DeleteBucketReplication", "negative", delete(MISSING_BUCKET).send(), none)
        .await;

    let website = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().expect("index document"))
        .build();
    let put = |bucket: &str| c.put_bucket_website().bucket(bucket).website_configuration(website.clone());
    s.step("PutBucketWebsite", "normal", put(BUCKET).send(), none).await;
    s.step("PutBucketWebsite", "negative", put(MISSING_BUCKET).send(), none).await;
    let get = || c.get_bucket_website().bucket(BUCKET);
    s.step("GetBucketWebsite", "normal", get().send(), |o| {
        vec![("index", format!("{:?}", o.index_document().map(|index| index.suffix())))]
    })
    .await;
    let delete = |bucket: &str| c.delete_bucket_website().bucket(bucket);
    s.step("DeleteBucketWebsite", "normal", delete(BUCKET).send(), none).await;
    s.step("GetBucketWebsite", "negative", get().send(), none).await;
    s.step("DeleteBucketWebsite", "negative", delete(MISSING_BUCKET).send(), none)
        .await;

    let block = PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(true)
        .build();
    let put = |bucket: &str| {
        c.put_public_access_block()
            .bucket(bucket)
            .public_access_block_configuration(block.clone())
    };
    s.step("PutPublicAccessBlock", "normal", put(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("PutPublicAccessBlock", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_public_access_block().bucket(bucket);
    s.step("GetPublicAccessBlock", "normal", get(SCRATCH_BUCKET).send(), |o| {
        vec![("configuration", format!("{:?}", o.public_access_block_configuration()))]
    })
    .await;
    let delete = |bucket: &str| c.delete_public_access_block().bucket(bucket);
    s.step("DeletePublicAccessBlock", "normal", delete(SCRATCH_BUCKET).send(), none)
        .await;
    // Legacy answers a missing configuration with a 5xx, so that request is not the negative case.
    s.step("GetPublicAccessBlock", "unconfigured", get(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("GetPublicAccessBlock", "negative", get(MISSING_BUCKET).send(), none)
        .await;
    s.step("DeletePublicAccessBlock", "negative", delete(MISSING_BUCKET).send(), none)
        .await;

    let accelerate = AccelerateConfiguration::builder()
        .status(BucketAccelerateStatus::Suspended)
        .build();
    let put = |bucket: &str| {
        c.put_bucket_accelerate_configuration()
            .bucket(bucket)
            .accelerate_configuration(accelerate.clone())
    };
    s.step("PutBucketAccelerateConfiguration", "normal", put(BUCKET).send(), none)
        .await;
    s.step("PutBucketAccelerateConfiguration", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_bucket_accelerate_configuration().bucket(bucket);
    s.step("GetBucketAccelerateConfiguration", "normal", get(BUCKET).send(), |o| {
        vec![("status", format!("{:?}", o.status()))]
    })
    .await;
    s.step("GetBucketAccelerateConfiguration", "negative", get(MISSING_BUCKET).send(), none)
        .await;

    let payment = RequestPaymentConfiguration::builder()
        .payer(Payer::BucketOwner)
        .build()
        .expect("request payment configuration");
    let put = |bucket: &str| {
        c.put_bucket_request_payment()
            .bucket(bucket)
            .request_payment_configuration(payment.clone())
    };
    s.step("PutBucketRequestPayment", "normal", put(BUCKET).send(), none).await;
    s.step("PutBucketRequestPayment", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_bucket_request_payment().bucket(bucket);
    s.step("GetBucketRequestPayment", "normal", get(BUCKET).send(), |o| {
        vec![("payer", format!("{:?}", o.payer()))]
    })
    .await;
    s.step("GetBucketRequestPayment", "negative", get(MISSING_BUCKET).send(), none)
        .await;

    let put = |bucket: &str| {
        c.put_bucket_logging()
            .bucket(bucket)
            .bucket_logging_status(BucketLoggingStatus::builder().build())
    };
    s.step("PutBucketLogging", "normal", put(BUCKET).send(), none).await;
    s.step("PutBucketLogging", "negative", put(MISSING_BUCKET).send(), none).await;
    let get = |bucket: &str| c.get_bucket_logging().bucket(bucket);
    s.step("GetBucketLogging", "normal", get(BUCKET).send(), |o| {
        vec![("enabled", o.logging_enabled().is_some().to_string())]
    })
    .await;
    s.step("GetBucketLogging", "negative", get(MISSING_BUCKET).send(), none).await;

    let put = |bucket: &str| {
        c.put_bucket_notification_configuration()
            .bucket(bucket)
            .notification_configuration(NotificationConfiguration::builder().build())
    };
    s.step("PutBucketNotificationConfiguration", "normal", put(BUCKET).send(), none)
        .await;
    s.step("PutBucketNotificationConfiguration", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_bucket_notification_configuration().bucket(bucket);
    s.step("GetBucketNotificationConfiguration", "normal", get(BUCKET).send(), |o| {
        let counts = (
            o.topic_configurations().len(),
            o.queue_configurations().len(),
            o.lambda_function_configurations().len(),
        );
        vec![("configurations", format!("{counts:?}"))]
    })
    .await;
    s.step("GetBucketNotificationConfiguration", "negative", get(MISSING_BUCKET).send(), none)
        .await;
}

async fn object_lock(s: &mut Recorder, c: &Client) {
    let lock = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();
    let put = |bucket: &str| {
        c.put_object_lock_configuration()
            .bucket(bucket)
            .object_lock_configuration(lock.clone())
    };
    s.step("PutObjectLockConfiguration", "normal", put(LOCK_BUCKET).send(), none)
        .await;
    s.step("PutObjectLockConfiguration", "negative", put(MISSING_BUCKET).send(), none)
        .await;
    let get = |bucket: &str| c.get_object_lock_configuration().bucket(bucket);
    s.step("GetObjectLockConfiguration", "normal", get(LOCK_BUCKET).send(), |o| {
        let config = o.object_lock_configuration();
        vec![
            ("enabled", format!("{:?}", config.and_then(|config| config.object_lock_enabled()))),
            (
                "default_retention",
                format!(
                    "{:?}",
                    config
                        .and_then(|config| config.rule())
                        .and_then(|rule| rule.default_retention())
                ),
            ),
        ]
    })
    .await;
    s.step("GetObjectLockConfiguration", "negative", get(BUCKET).send(), none)
        .await;

    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(DateTime::from_secs(RETAIN_UNTIL_SECS))
        .build();
    let put = |key: &str| {
        c.put_object_retention()
            .bucket(LOCK_BUCKET)
            .key(key)
            .retention(retention.clone())
    };
    s.step("PutObjectRetention", "normal", put(LOCKED).send(), none).await;
    s.step("PutObjectRetention", "negative", put(ABSENT).send(), none).await;
    let get = |bucket: &str, key: &str| c.get_object_retention().bucket(bucket).key(key);
    s.step("GetObjectRetention", "normal", get(LOCK_BUCKET, LOCKED).send(), |o| {
        let retention = o.retention();
        vec![
            ("mode", format!("{:?}", retention.and_then(|r| r.mode()))),
            ("retain_until", format!("{:?}", retention.and_then(|r| r.retain_until_date()))),
        ]
    })
    .await;
    s.step("GetObjectRetention", "negative", get(BUCKET, README).send(), none)
        .await;

    // Legacy answers a legal hold on a missing key with a 5xx, so that request is not the
    // negative case; a bucket without object lock is.
    let hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    let put = |bucket: &str, key: &str| c.put_object_legal_hold().bucket(bucket).key(key).legal_hold(hold.clone());
    s.step("PutObjectLegalHold", "normal", put(LOCK_BUCKET, LOCKED).send(), none)
        .await;
    s.step("PutObjectLegalHold", "absent-key", put(LOCK_BUCKET, ABSENT).send(), none)
        .await;
    s.step("PutObjectLegalHold", "negative", put(BUCKET, README).send(), none)
        .await;
    let get = |bucket: &str, key: &str| c.get_object_legal_hold().bucket(bucket).key(key);
    s.step("GetObjectLegalHold", "normal", get(LOCK_BUCKET, LOCKED).send(), |o| {
        vec![("status", format!("{:?}", o.legal_hold().and_then(|hold| hold.status())))]
    })
    .await;
    s.step("GetObjectLegalHold", "absent-key", get(LOCK_BUCKET, ABSENT).send(), none)
        .await;
    s.step("GetObjectLegalHold", "negative", get(BUCKET, README).send(), none)
        .await;
}

async fn multipart(s: &mut Recorder, c: &Client) {
    let create = |bucket: &str, key: &str| c.create_multipart_upload().bucket(bucket).key(key);
    let upload = s
        .step("CreateMultipartUpload", "normal", create(BUCKET, MPU).send(), |o| {
            vec![("key", format!("{:?}", o.key()))]
        })
        .await
        .and_then(|output| output.upload_id().map(str::to_owned))
        .unwrap_or_else(|| UNKNOWN_UPLOAD.to_owned());
    s.step("CreateMultipartUpload", "negative", create(MISSING_BUCKET, MPU).send(), none)
        .await;
    let part = |upload_id: &str| {
        c.upload_part()
            .bucket(BUCKET)
            .key(MPU)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"the only part"))
    };
    let etag = s
        .step("UploadPart", "normal", part(&upload).send(), none)
        .await
        .and_then(|output| output.e_tag().map(str::to_owned))
        .unwrap_or_default();
    s.step("UploadPart", "negative", part(UNKNOWN_UPLOAD).send(), none).await;
    let list = |upload_id: &str| c.list_parts().bucket(BUCKET).key(MPU).upload_id(upload_id);
    s.step("ListParts", "normal", list(&upload).send(), |o| {
        let parts: Vec<_> = o
            .parts()
            .iter()
            .map(|part| (part.part_number(), part.size(), part.e_tag()))
            .collect();
        vec![("parts", format!("{parts:?}"))]
    })
    .await;
    s.step("ListParts", "negative", list(UNKNOWN_UPLOAD).send(), none).await;
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
        .build();
    let complete = |upload_id: &str| {
        c.complete_multipart_upload()
            .bucket(BUCKET)
            .key(MPU)
            .upload_id(upload_id)
            .multipart_upload(completed.clone())
    };
    s.step("CompleteMultipartUpload", "normal", complete(&upload).send(), |o| {
        vec![("etag", format!("{:?}", o.e_tag()))]
    })
    .await;
    s.step("CompleteMultipartUpload", "negative", complete(UNKNOWN_UPLOAD).send(), none)
        .await;

    let copy_upload = s
        .step("CreateMultipartUpload", "copy-target", create(BUCKET, MPU_COPY).send(), none)
        .await
        .and_then(|output| output.upload_id().map(str::to_owned))
        .unwrap_or_else(|| UNKNOWN_UPLOAD.to_owned());
    let copy = |source: &str| {
        c.upload_part_copy()
            .bucket(BUCKET)
            .key(MPU_COPY)
            .upload_id(&copy_upload)
            .part_number(1)
            .copy_source(format!("{BUCKET}/{source}"))
    };
    s.step("UploadPartCopy", "normal", copy(README).send(), |o| {
        vec![("etag", format!("{:?}", o.copy_part_result().and_then(|r| r.e_tag())))]
    })
    .await;
    s.step("UploadPartCopy", "negative", copy(ABSENT).send(), none).await;
    let uploads = |bucket: &str| c.list_multipart_uploads().bucket(bucket);
    s.step("ListMultipartUploads", "normal", uploads(BUCKET).send(), |o| {
        let keys: Vec<_> = o.uploads().iter().map(|upload| upload.key()).collect();
        vec![("keys", format!("{keys:?}"))]
    })
    .await;
    s.step("ListMultipartUploads", "negative", uploads(MISSING_BUCKET).send(), none)
        .await;
    let abort = || {
        c.abort_multipart_upload()
            .bucket(BUCKET)
            .key(MPU_COPY)
            .upload_id(&copy_upload)
    };
    s.step("AbortMultipartUpload", "normal", abort().send(), none).await;
    s.step("AbortMultipartUpload", "negative", abort().send(), none).await;
}

async fn object_extras(s: &mut Recorder, c: &Client) {
    let restore = |key: &str| {
        c.restore_object()
            .bucket(BUCKET)
            .key(key)
            .restore_request(RestoreRequest::builder().days(1).build())
    };
    s.step("RestoreObject", "normal", restore(README).send(), none).await;
    s.step("RestoreObject", "negative", restore(ABSENT).send(), none).await;

    let select = |key: &str| {
        c.select_object_content()
            .bucket(BUCKET)
            .key(key)
            .expression("SELECT s.name FROM S3Object s")
            .expression_type(ExpressionType::Sql)
            .input_serialization(
                InputSerialization::builder()
                    .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
                    .build(),
            )
            .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
    };
    if let Some(mut output) = s.step("SelectObjectContent", "normal", select(CSV).send(), none).await {
        let mut records = Vec::new();
        let drained = tokio::time::timeout(SELECT_DEADLINE, async {
            loop {
                match output.payload.recv().await {
                    Ok(Some(SelectObjectContentEventStream::Records(event))) => {
                        records.extend_from_slice(event.payload().map(|blob| blob.as_ref()).unwrap_or_default());
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => return "end".to_owned(),
                    Err(error) => return format!("error:{:?}", error.as_service_error().and_then(|e| e.code())),
                }
            }
        })
        .await
        .unwrap_or_else(|_| "<deadline>".to_owned());
        s.amend("stream", drained);
        s.amend("records_md5", md5_hex(&records));
    }
    s.step("SelectObjectContent", "negative", select(ABSENT).send(), none).await;

    let torrent = |key: &str| c.get_object_torrent().bucket(BUCKET).key(key);
    s.step("GetObjectTorrent", "normal", torrent(README).send(), none).await;
    s.step("GetObjectTorrent", "negative", torrent(ABSENT).send(), none).await;
}

async fn deletes_and_listings(s: &mut Recorder, c: &Client) {
    s.step("DeleteObject", "normal", c.delete_object().bucket(BUCKET).key(COPY).send(), none)
        .await;
    let missing = c.delete_object().bucket(MISSING_BUCKET).key(COPY);
    s.step("DeleteObject", "negative", missing.send(), none).await;
    let delete = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("docs/a.txt")
                .build()
                .expect("object identifier"),
        )
        .objects(ObjectIdentifier::builder().key(ABSENT).build().expect("object identifier"))
        .build()
        .expect("delete request");
    let delete_objects = |bucket: &str| c.delete_objects().bucket(bucket).delete(delete.clone());
    s.step("DeleteObjects", "normal", delete_objects(BUCKET).send(), |o| {
        let mut deleted: Vec<_> = o.deleted().iter().map(|d| (d.key(), d.delete_marker())).collect();
        deleted.sort_unstable();
        let errors: Vec<_> = o.errors().iter().map(|e| (e.key(), e.code())).collect();
        vec![("deleted", format!("{deleted:?}")), ("errors", format!("{errors:?}"))]
    })
    .await;
    s.step("DeleteObjects", "negative", delete_objects(MISSING_BUCKET).send(), none)
        .await;

    let list = |bucket: &str| c.list_objects().bucket(bucket).prefix("docs/");
    s.step("ListObjects", "normal", list(BUCKET).send(), |o| {
        let keys: Vec<_> = o.contents().iter().map(|object| (object.key(), object.size())).collect();
        vec![("keys", format!("{keys:?}"))]
    })
    .await;
    s.step("ListObjects", "negative", list(MISSING_BUCKET).send(), none).await;
    let list = |bucket: &str| c.list_objects_v2().bucket(bucket).delimiter("/");
    s.step("ListObjectsV2", "normal", list(BUCKET).send(), |o| {
        let keys: Vec<_> = o.contents().iter().map(|object| object.key()).collect();
        let prefixes: Vec<_> = o.common_prefixes().iter().map(|prefix| prefix.prefix()).collect();
        vec![
            ("keys", format!("{keys:?}")),
            ("prefixes", format!("{prefixes:?}")),
            ("key_count", format!("{:?}", o.key_count())),
        ]
    })
    .await;
    s.step("ListObjectsV2", "negative", list(MISSING_BUCKET).send(), none).await;
    let versions = |bucket: &str| c.list_object_versions().bucket(bucket);
    s.step("ListObjectVersions", "normal", versions(BUCKET).send(), |o| {
        let versions: Vec<_> = o.versions().iter().map(|v| (v.key(), v.is_latest(), v.size())).collect();
        let markers: Vec<_> = o.delete_markers().iter().map(|m| (m.key(), m.is_latest())).collect();
        vec![
            ("versions", format!("{versions:?}")),
            ("delete_markers", format!("{markers:?}")),
        ]
    })
    .await;
    s.step("ListObjectVersions", "negative", versions(MISSING_BUCKET).send(), none)
        .await;

    s.step("DeleteBucket", "normal", c.delete_bucket().bucket(SCRATCH_BUCKET).send(), none)
        .await;
    s.step("DeleteBucket", "negative", c.delete_bucket().bucket(BUCKET).send(), none)
        .await;
}

#[cfg(test)]
mod tests {
    use super::OPERATIONS;

    #[test]
    fn the_operation_list_is_sorted_without_duplicates() {
        assert!(
            OPERATIONS.windows(2).all(|pair| pair[0] < pair[1]),
            "OPERATIONS must be strictly sorted: {OPERATIONS:?}"
        );
    }
}
