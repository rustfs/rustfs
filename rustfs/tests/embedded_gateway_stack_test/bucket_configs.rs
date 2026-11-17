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

//! Refusal-matrix rows and behaviour of migration batch A1 (rustfs/backlog#2765): bucket CORS,
//! tagging, public access block and encryption. Every behaviour test runs on both stacks.

use super::{
    GATEWAY, LEGACY, MatrixRow, Outcome, USER_SECRET_KEY, create_bucket_in_the_store, create_user_allowed_on_one_bucket, outcome,
    s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::interceptors::{
    BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef,
};
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::{BoxError, ProvideErrorMetadata};
use aws_sdk_s3::operation::put_bucket_cors::builders::PutBucketCorsFluentBuilder;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule, PublicAccessBlockConfiguration, ServerSideEncryption};
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::bucket::metadata_sys;
use rustfs_s3_ops::S3Operation;
use std::sync::{Arc, Mutex};

const GET_CORS: MatrixRow = MatrixRow {
    operation: "GetBucketCors",
    metric: S3Operation::GetBucketCors,
    has_error_body: true,
    call: get_bucket_cors,
};
const GET_ENCRYPTION: MatrixRow = MatrixRow {
    operation: "GetBucketEncryption",
    metric: S3Operation::GetBucketEncryption,
    has_error_body: true,
    call: get_bucket_encryption,
};
const GET_TAGGING: MatrixRow = MatrixRow {
    operation: "GetBucketTagging",
    metric: S3Operation::GetBucketTagging,
    has_error_body: true,
    call: get_bucket_tagging,
};

pub(super) const ENCRYPTION_MATRIX_BUCKET: &str = "config-read-encryption-matrix";

pub(crate) const MATRIX: &[MatrixRow] = &[
    GET_CORS,
    MatrixRow {
        call: get_bucket_encryption_in_matrix,
        ..GET_ENCRYPTION
    },
    GET_TAGGING,
    MatrixRow {
        operation: "PutPublicAccessBlock",
        metric: S3Operation::PutPublicAccessBlock,
        has_error_body: true,
        call: put_public_access_block,
    },
    MatrixRow {
        operation: "GetPublicAccessBlock",
        metric: S3Operation::GetPublicAccessBlock,
        has_error_body: true,
        call: get_public_access_block,
    },
    MatrixRow {
        operation: "DeletePublicAccessBlock",
        metric: S3Operation::DeletePublicAccessBlock,
        has_error_body: true,
        call: delete_public_access_block,
    },
    MatrixRow {
        operation: "PutBucketCors",
        metric: S3Operation::PutBucketCors,
        has_error_body: true,
        call: put_bucket_cors,
    },
    MatrixRow {
        operation: "DeleteBucketCors",
        metric: S3Operation::DeleteBucketCors,
        has_error_body: true,
        call: delete_bucket_cors,
    },
    MatrixRow {
        operation: "PutBucketEncryption",
        metric: S3Operation::PutBucketEncryption,
        has_error_body: true,
        call: put_bucket_encryption,
    },
    MatrixRow {
        operation: "PutBucketTagging",
        metric: S3Operation::PutBucketTagging,
        has_error_body: true,
        call: put_bucket_tagging,
    },
    MatrixRow {
        operation: "DeleteBucketEncryption",
        metric: S3Operation::DeleteBucketEncryption,
        has_error_body: true,
        call: delete_bucket_encryption,
    },
    MatrixRow {
        operation: "DeleteBucketTagging",
        metric: S3Operation::DeleteBucketTagging,
        has_error_body: true,
        call: delete_bucket_tagging,
    },
];

fn put_bucket_encryption<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    use aws_sdk_s3::types::{ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule};

    Box::pin(async move {
        let status = StatusCapture::default();
        let rule = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(
                ServerSideEncryptionByDefault::builder()
                    .sse_algorithm(ServerSideEncryption::Aes256)
                    .build()
                    .expect("encryption algorithm"),
            )
            .build();
        let config = ServerSideEncryptionConfiguration::builder()
            .rules(rule)
            .build()
            .expect("encryption rule");
        outcome(
            client
                .put_bucket_encryption()
                .bucket(ENCRYPTION_MATRIX_BUCKET)
                .server_side_encryption_configuration(config)
                .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
                .customize()
                .interceptor(status.clone())
                .send()
                .await
                .map(|_| assert_eq!(status.take(), Some(200))),
        )
    })
}

fn put_bucket_tagging<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    use aws_sdk_s3::types::{Tag, Tagging};

    Box::pin(async move {
        let status = StatusCapture::default();
        let tag = Tag::builder().key("team").value("gateway").build().expect("tag");
        let tagging = Tagging::builder().tag_set(tag).build().expect("tag set");
        outcome(
            client
                .put_bucket_tagging()
                .bucket(bucket)
                .tagging(tagging)
                .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
                .customize()
                .interceptor(status.clone())
                .send()
                .await
                .map(|_| assert_eq!(status.take(), Some(200))),
        )
    })
}

fn delete_bucket_encryption<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let status = StatusCapture::default();
        outcome(
            client
                .delete_bucket_encryption()
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

fn delete_bucket_tagging<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let status = StatusCapture::default();
        outcome(
            client
                .delete_bucket_tagging()
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

async fn bucket_configuration_deletes_authorize_the_existing_action_and_bucket(server: RustFSServer) {
    const ALLOWED: &str = "config-delete-authorized";
    const OTHER: &str = "config-delete-other";
    const READER: &str = "config-delete-reader";
    for bucket in [ALLOWED, OTHER] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(READER, "s3:ListBucket", ALLOWED).await;
    let endpoint = server.endpoint();
    let reader = s3_client(&endpoint, READER, USER_SECRET_KEY);
    type Delete = for<'a> fn(&'a Client, &'a str) -> BoxFuture<'a, Outcome>;
    let deletes: [(&str, &str, Delete); 3] = [
        ("config-delete-sse", "s3:PutBucketEncryption", delete_bucket_encryption),
        (
            "config-delete-policy",
            "s3:DeleteBucketPolicy",
            super::bucket_access::delete_bucket_policy,
        ),
        ("config-delete-tags", "s3:PutBucketTagging", delete_bucket_tagging),
    ];
    for (user, action, delete) in deletes {
        create_user_allowed_on_one_bucket(user, action, ALLOWED).await;
        let writer = s3_client(&endpoint, user, USER_SECRET_KEY);
        assert_read_error(delete(&reader, ALLOWED).await, "AccessDenied", 403, action);
        assert_read_error(delete(&writer, OTHER).await, "AccessDenied", 403, action);
        assert_eq!(delete(&writer, ALLOWED).await, Ok(()), "{action} on the authorized bucket");
    }
    server.shutdown().await;
}

#[test]
fn gateway_bucket_configuration_deletes_authorize_the_existing_action_and_bucket() {
    with_stack(GATEWAY, bucket_configuration_deletes_authorize_the_existing_action_and_bucket);
}

#[test]
fn legacy_bucket_configuration_deletes_authorize_the_existing_action_and_bucket() {
    with_stack(LEGACY, bucket_configuration_deletes_authorize_the_existing_action_and_bucket);
}

#[test]
fn tag_deletion_reaches_opa_with_unreadable_tags_on_both_stacks() {
    const STACK: &str = "RUSTFS_TEST_TAG_REPAIR_STACK";
    match std::env::var(STACK).as_deref() {
        Ok("gateway") => with_stack(GATEWAY, tag_deletion_reaches_opa_with_unreadable_tags),
        Ok("legacy") => with_stack(LEGACY, tag_deletion_reaches_opa_with_unreadable_tags),
        _ => {
            // The IAM plug-in is process-wide, so isolate it from the other embedded tests.
            for stack in ["gateway", "legacy"] {
                let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "bucket_configs::tag_deletion_reaches_opa_with_unreadable_tags_on_both_stacks",
                    ])
                    .env(STACK, stack)
                    .env("NO_PROXY", "127.0.0.1,localhost")
                    .env("no_proxy", "127.0.0.1,localhost")
                    .output()
                    .expect("run isolated OPA test");
                let stdout = String::from_utf8_lossy(&output.stdout);
                assert!(
                    output.status.success() && stdout.contains("test result: ok. 1 passed;"),
                    "{stack} OPA test failed:\n{stdout}\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
}

async fn tag_deletion_reaches_opa_with_unreadable_tags(server: RustFSServer) {
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use rustfs_policy::policy::opa::{Args, AuthZPlugin};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BUCKET: &str = "config-delete-opa";
    const ACTOR: &str = "config-delete-opa-user";
    const UNREADABLE_TAGS: &[u8] = b"<Tagging><TagSet>";
    create_bucket_in_the_store(BUCKET).await;
    super::create_user_without_a_policy(ACTOR).await;
    let context = get_global_app_context().expect("application context");
    let metadata_system = context.bucket_metadata().handle().expect("bucket metadata system");
    let mut metadata = metadata_system
        .read()
        .await
        .get_config_from_disk(BUCKET)
        .await
        .expect("bucket metadata");
    metadata.tagging_config_xml = UNREADABLE_TAGS.to_vec();
    metadata.tagging_config = None;
    metadata
        .save_with_store(context.object_store())
        .await
        .expect("persist unreadable tags");
    metadata_system.write().await.set(BUCKET.to_owned(), Arc::new(metadata)).await;

    let decisions = Arc::new(AtomicUsize::new(0));
    let record = Arc::clone(&decisions);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind OPA fixture");
    let url = format!("http://{}/decision", listener.local_addr().expect("OPA fixture address"));
    let router = axum::Router::new().route(
        "/decision",
        axum::routing::post(move |axum::Json(payload): axum::Json<serde_json::Value>| async move {
            record.fetch_add(1, Ordering::Relaxed);
            assert_eq!(payload["input"]["resource"]["bucket"], BUCKET);
            assert_eq!(payload["input"]["action"], "s3:PutBucketTagging");
            axum::Json(serde_json::json!({"result": true}))
        }),
    );
    let opa = tokio::spawn(async move { axum::serve(listener, router).await.expect("serve OPA fixture") });
    IamSys::<ObjectStore>::set_policy_plugin_client(AuthZPlugin::new(Args {
        url,
        auth_token: String::new(),
    }))
    .await;

    let client = s3_client(&server.endpoint(), ACTOR, USER_SECRET_KEY);
    let client = Client::from_conf(
        client
            .config()
            .to_builder()
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .build(),
    );
    assert_read_error(
        get_bucket_tagging(&client, BUCKET).await,
        "InternalError",
        500,
        "read with unreadable tags",
    );
    assert_eq!(decisions.load(Ordering::Relaxed), 0, "ordinary authorization cannot read the tags");

    let deletion = delete_bucket_tagging(&client, BUCKET).await;
    assert_eq!(
        decisions.load(Ordering::Relaxed),
        1,
        "tag deletion reaches OPA before storage checks the tags"
    );
    assert_read_error(deletion, "ServiceUnavailable", 503, "storage refuses to rewrite unreadable tags");
    let stored = metadata_system
        .read()
        .await
        .get_config_from_disk(BUCKET)
        .await
        .expect("reload durable metadata");
    assert_eq!(stored.tagging_config_xml, UNREADABLE_TAGS);
    assert_read_error(
        get_bucket_tagging(&client, BUCKET).await,
        "InternalError",
        500,
        "read after the refused delete",
    );
    assert_eq!(decisions.load(Ordering::Relaxed), 1);
    opa.abort();
    server.shutdown().await;
}

fn put_bucket_cors<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(put_cors(client, bucket, cors_document()).send().await) })
}

fn delete_bucket_cors<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_bucket_cors().bucket(bucket).send().await) })
}

fn put_cors(client: &Client, bucket: &str, document: CorsConfiguration) -> PutBucketCorsFluentBuilder {
    client.put_bucket_cors().bucket(bucket).cors_configuration(document)
}

/// A document with every member of a rule, and a rule with only the required ones.
fn cors_document() -> CorsConfiguration {
    let full = CorsRule::builder()
        .id("full")
        .allowed_headers("x-a")
        .allowed_headers("x-b")
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_origins("https://a.example")
        .expose_headers("ETag")
        .max_age_seconds(300)
        .build();
    let minimal = CorsRule::builder().allowed_methods("HEAD").allowed_origins("*").build();
    CorsConfiguration::builder()
        .set_cors_rules(Some([full, minimal].map(|rule| rule.expect("valid rule")).to_vec()))
        .build()
        .expect("valid configuration")
}

/// One rule allowing `method` from `origin`.
fn single_rule(method: &str, origin: &str) -> CorsConfiguration {
    let rule = CorsRule::builder().allowed_methods(method).allowed_origins(origin).build();
    CorsConfiguration::builder()
        .cors_rules(rule.expect("valid rule"))
        .build()
        .expect("valid configuration")
}

/// Records the status and request ID headers of the response.
#[derive(Clone, Debug, Default)]
pub(super) struct StatusCapture(Arc<Mutex<Option<CapturedResponse>>>);

#[derive(Debug)]
pub(super) struct CapturedResponse {
    pub(super) status: u16,
    pub(super) request_ids: [Option<String>; 2],
}

impl StatusCapture {
    pub(super) fn take(&self) -> Option<u16> {
        self.take_response().map(|response| response.status)
    }

    pub(super) fn take_response(&self) -> Option<CapturedResponse> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Intercept for StatusCapture {
    fn name(&self) -> &'static str {
        "bucket-configs-status-capture"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let header = |name| response.headers().get(name).map(str::to_owned);
        *self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("status capture mutex was poisoned"))? = Some(CapturedResponse {
            status: response.status().as_u16(),
            request_ids: [header("x-amz-request-id"), header("x-request-id")],
        });
        Ok(())
    }
}

fn is_integrity_claim(name: &str) -> bool {
    name == "content-md5" || name.starts_with("x-amz-checksum-") || name == "x-amz-sdk-checksum-algorithm"
}

/// Removes every integrity claim the SDK attaches to a request body, before the request is signed,
/// and records the claims still on the request as it is sent.
#[derive(Clone, Debug, Default)]
struct WithoutIntegrityClaim(Arc<Mutex<Option<Vec<String>>>>);

impl WithoutIntegrityClaim {
    fn sent_claims(&self) -> Option<Vec<String>> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Intercept for WithoutIntegrityClaim {
    fn name(&self) -> &'static str {
        "bucket-configs-without-integrity-claim"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request_mut().headers_mut();
        let claims: Vec<String> = headers
            .iter()
            .map(|(name, _)| name.to_owned())
            .filter(|name| is_integrity_claim(name))
            .collect();
        for name in claims {
            headers.remove(name);
        }
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let sent = context
            .request()
            .headers()
            .iter()
            .map(|(name, _)| name.to_owned())
            .filter(|name| is_integrity_claim(name))
            .collect();
        *self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("claim capture mutex was poisoned"))? = Some(sent);
        Ok(())
    }
}

/// The bytes PutBucketCors stores for [`cors_document`]: the persisted writer's form.
const PERSISTED_CORS: &str = concat!(
    "<CORSConfiguration><CORSRule><AllowedHeader>x-a</AllowedHeader><AllowedHeader>x-b</AllowedHeader>",
    "<AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>https://a.example</AllowedOrigin>",
    "<ExposeHeader>ETag</ExposeHeader><ID>full</ID><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule>",
    "<CORSRule><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>",
);

const PERSISTED_ENCRYPTION: &str = concat!(
    "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>",
    "<SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault>",
    "<BucketKeyEnabled>false</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>",
);
const PERSISTED_TAGS: &str = concat!(
    "<Tagging><TagSet><Tag><Key>alpha</Key><Value>blue team</Value></Tag>",
    "<Tag><Key>empty</Key><Value></Value></Tag></TagSet></Tagging>",
);

pub(super) async fn store_read_configs(bucket: &str, encryption_bucket: &str) {
    let metadata = get_global_app_context()
        .expect("embedded server context")
        .bucket_metadata()
        .handle()
        .expect("bucket metadata system");
    for (bucket, file, document) in [
        (bucket, "cors.xml", PERSISTED_CORS),
        (encryption_bucket, "bucket-encryption.xml", PERSISTED_ENCRYPTION),
        (bucket, "tagging.xml", PERSISTED_TAGS),
    ] {
        metadata
            .read()
            .await
            .update(bucket, file, document.as_bytes().to_vec())
            .await
            .expect("store the bucket configuration");
    }
}

fn get_bucket_cors<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(client.get_bucket_cors().bucket(bucket).send().await.map(|config| {
            assert_eq!(config.cors_rules(), cors_document().cors_rules(), "stored CORS rules");
        }))
    })
}

fn get_bucket_encryption<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(client.get_bucket_encryption().bucket(bucket).send().await.map(|output| {
            let config = output.server_side_encryption_configuration().expect("stored encryption");
            let [rule] = config.rules() else { panic!("expected one rule: {:?}", config.rules()) };
            let default = rule.apply_server_side_encryption_by_default().expect("stored algorithm");
            assert_eq!(default.sse_algorithm(), &ServerSideEncryption::Aes256);
            assert_eq!(default.kms_master_key_id(), None);
            assert_eq!(rule.bucket_key_enabled(), Some(false));
        }))
    })
}

fn get_bucket_encryption_in_matrix<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    get_bucket_encryption(client, ENCRYPTION_MATRIX_BUCKET)
}

fn get_bucket_tagging<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(client.get_bucket_tagging().bucket(bucket).send().await.map(|output| {
            let tags: Vec<_> = output.tag_set().iter().map(|tag| (tag.key(), tag.value())).collect();
            assert_eq!(tags, [("alpha", "blue team"), ("empty", "")], "stored tag keys and values");
        }))
    })
}

const READ_GRANTED: &str = "config-read-granted";
const READ_OTHER: &str = "config-read-other";
const READ_EMPTY: &str = "config-read-empty";
const READ_MISSING: &str = "config-read-missing";
const READ_RESERVED: &str = "config-read-stored-s3alias";

fn assert_read_error(result: Outcome, code: &str, status: u16, case: &str) {
    let error = result.expect_err(case);
    assert_eq!((error.code.as_deref(), error.status), (Some(code), Some(status)), "{case}");
}

async fn configuration_read(
    server: RustFSServer,
    operation: MatrixRow,
    action: &str,
    other_action: &str,
    missing_config: (&str, u16),
) {
    for bucket in [READ_GRANTED, READ_OTHER, READ_EMPTY, READ_RESERVED] {
        create_bucket_in_the_store(bucket).await;
    }
    for bucket in [READ_GRANTED, READ_OTHER, READ_RESERVED] {
        store_read_configs(bucket, bucket).await;
    }
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let call = operation.call;
    assert_eq!(call(&root, READ_RESERVED).await, Ok(()), "stored reserved-shaped bucket");
    assert_read_error(call(&root, READ_MISSING).await, "NoSuchBucket", 404, "missing bucket");
    assert_read_error(call(&root, READ_EMPTY).await, missing_config.0, missing_config.1, "missing configuration");

    create_user_allowed_on_one_bucket("config-reader", action, READ_GRANTED).await;
    create_user_allowed_on_one_bucket("config-other-reader", other_action, READ_GRANTED).await;
    let reader = s3_client(&endpoint, "config-reader", USER_SECRET_KEY);
    let other_reader = s3_client(&endpoint, "config-other-reader", USER_SECRET_KEY);
    let handled_count = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.op == operation.metric.as_str())
            .map_or(0, |metric| metric.total)
    };
    let before = handled_count();
    assert_read_error(call(&other_reader, READ_GRANTED).await, "AccessDenied", 403, "wrong IAM action");
    assert_read_error(call(&reader, READ_OTHER).await, "AccessDenied", 403, "wrong IAM resource");
    assert_eq!(handled_count(), before, "authorization refusals do not reach the edge counter");
    assert_eq!(call(&reader, READ_GRANTED).await, Ok(()), "allowed IAM action and resource");
    assert_eq!(handled_count() - before, 1, "the admitted edge counts once");
    server.shutdown().await;
}

async fn cors_read(server: RustFSServer) {
    configuration_read(
        server,
        GET_CORS,
        "s3:GetBucketCors",
        "s3:GetBucketTagging",
        ("NoSuchCORSConfiguration", 404),
    )
    .await;
}

async fn encryption_read(server: RustFSServer) {
    configuration_read(
        server,
        GET_ENCRYPTION,
        "s3:GetBucketEncryption",
        "s3:GetBucketCors",
        ("ServerSideEncryptionConfigurationNotFoundError", 400),
    )
    .await;
}

async fn tagging_read(server: RustFSServer) {
    configuration_read(
        server,
        GET_TAGGING,
        "s3:GetBucketTagging",
        "s3:GetBucketEncryption",
        ("NoSuchTagSet", 404),
    )
    .await;
}

#[test]
fn the_gateway_stack_reads_bucket_cors() {
    with_stack(GATEWAY, cors_read);
}

#[test]
fn the_legacy_stack_reads_bucket_cors() {
    with_stack(LEGACY, cors_read);
}

#[test]
fn the_gateway_stack_reads_bucket_encryption() {
    with_stack(GATEWAY, encryption_read);
}

#[test]
fn the_legacy_stack_reads_bucket_encryption() {
    with_stack(LEGACY, encryption_read);
}

#[test]
fn the_gateway_stack_reads_bucket_tagging() {
    with_stack(GATEWAY, tagging_read);
}

#[test]
fn the_legacy_stack_reads_bucket_tagging() {
    with_stack(LEGACY, tagging_read);
}

const CORS_BUCKET: &str = "gateway-stack-cors";
const CORS_GRANTED_BUCKET: &str = "gateway-stack-cors-granted";
/// A name RustFS creates and the AWS bucket grammar refuses (reserved suffix).
const CORS_RUSTFS_ONLY_BUCKET: &str = "gateway-stack-cors-s3alias";
const CORS_WRITER: &str = "gatewaystackcorswriter";

async fn stored_cors(bucket: &str) -> String {
    let metadata = metadata_sys::get(bucket).await.expect("bucket metadata");
    String::from_utf8_lossy(&metadata.cors_config_xml).into_owned()
}

/// PutBucketCors stores the persisted writer's bytes for a document, also on a bucket name only
/// RustFS admits; DeleteBucketCors answers 204 and removes them, whether or not a document exists.
/// Refused: a caller whose policy names another bucket, and the documents the codecs refuse.
async fn the_cors_document_is_stored_and_removed(server: RustFSServer) {
    for bucket in [CORS_BUCKET, CORS_GRANTED_BUCKET, CORS_RUSTFS_ONLY_BUCKET] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(CORS_WRITER, "s3:PutBucketCors", CORS_GRANTED_BUCKET).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, CORS_WRITER, USER_SECRET_KEY);
    let refused = |result: Outcome| result.map_err(|refusal| (refusal.code, refusal.status));

    let wrong_bucket = outcome(put_cors(&writer, CORS_BUCKET, cors_document()).send().await);
    assert_eq!(refused(wrong_bucket), Err((Some("AccessDenied".to_owned()), Some(403))));
    assert_eq!(stored_cors(CORS_BUCKET).await, "", "a refused write stores nothing");
    let granted = outcome(put_cors(&writer, CORS_GRANTED_BUCKET, cors_document()).send().await);
    assert_eq!(refused(granted), Ok(()));

    let unknown_method = outcome(put_cors(&root, CORS_BUCKET, single_rule("PATCH", "*")).send().await);
    let two_wildcards = outcome(
        put_cors(&root, CORS_BUCKET, single_rule("GET", "https://*.*.example"))
            .send()
            .await,
    );
    let without_claim = WithoutIntegrityClaim::default();
    let no_integrity_claim = outcome(
        put_cors(&root, CORS_BUCKET, cors_document())
            .customize()
            .interceptor(without_claim.clone())
            .send()
            .await,
    );
    assert_eq!(without_claim.sent_claims(), Some(vec![]), "the request left without an integrity claim");
    let observed = [unknown_method, two_wildcards, no_integrity_claim].map(refused);
    assert_eq!(observed, [Ok(()), Ok(()), Ok(())], "unknown method, two wildcards, no integrity claim");

    for bucket in [CORS_BUCKET, CORS_RUSTFS_ONLY_BUCKET] {
        assert_eq!(
            refused(outcome(put_cors(&root, bucket, cors_document()).send().await)),
            Ok(()),
            "{bucket}"
        );
        assert_eq!(stored_cors(bucket).await, PERSISTED_CORS, "{bucket}");
    }

    for attempt in ["configured", "unconfigured"] {
        let status = StatusCapture::default();
        let deleted = root
            .delete_bucket_cors()
            .bucket(CORS_BUCKET)
            .customize()
            .interceptor(status.clone())
            .send()
            .await;
        assert_eq!((refused(outcome(deleted)), status.take()), (Ok(()), Some(204)), "{attempt}");
        assert_eq!(stored_cors(CORS_BUCKET).await, "", "{attempt}");
    }

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_stores_and_removes_the_cors_document() {
    with_stack(GATEWAY, the_cors_document_is_stored_and_removed);
}

#[test]
fn the_legacy_stack_stores_and_removes_the_cors_document() {
    with_stack(LEGACY, the_cors_document_is_stored_and_removed);
}

const PUBLIC_ACCESS_BLOCK_XML: &str = concat!(
    "<PublicAccessBlockConfiguration><BlockPublicPolicy>false</BlockPublicPolicy>",
    "<IgnorePublicAcls>true</IgnorePublicAcls><RestrictPublicBuckets>true</RestrictPublicBuckets>",
    "</PublicAccessBlockConfiguration>",
);

fn public_access_block_document() -> PublicAccessBlockConfiguration {
    PublicAccessBlockConfiguration::builder()
        .ignore_public_acls(true)
        .block_public_policy(false)
        .restrict_public_buckets(true)
        .build()
}

fn put_public_access_block<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(
            client
                .put_public_access_block()
                .bucket(bucket)
                .public_access_block_configuration(public_access_block_document())
                .send()
                .await,
        )
    })
}

fn get_public_access_block<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(client.get_public_access_block().bucket(bucket).send().await.map(|output| {
            assert_eq!(
                output.public_access_block_configuration(),
                Some(&public_access_block_document()),
                "persisted switches, including the omitted field"
            );
        }))
    })
}

fn delete_public_access_block<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_public_access_block().bucket(bucket).send().await) })
}

async fn stored_public_access_block(bucket: &str) -> Vec<u8> {
    metadata_sys::get(bucket)
        .await
        .expect("bucket metadata")
        .public_access_block_config_xml
        .clone()
}

async fn public_access_block_configuration(server: RustFSServer, checks_expected_owner: bool) {
    const GRANTED: &str = "public-access-block-granted";
    const OTHER: &str = "public-access-block-other";
    const RESERVED: &str = "public-access-block-s3alias";
    const MISSING: &str = "public-access-block-missing";
    const WRITER: &str = "publicaccessblockwriter";
    const READER: &str = "publicaccessblockreader";
    const DELETER: &str = "publicaccessblockdeleter";

    for bucket in [GRANTED, OTHER, RESERVED] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(WRITER, "s3:PutBucketPublicAccessBlock", GRANTED).await;
    create_user_allowed_on_one_bucket(READER, "s3:GetBucketPublicAccessBlock", GRANTED).await;
    create_user_allowed_on_one_bucket(DELETER, "s3:DeleteBucketPublicAccessBlock", GRANTED).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, WRITER, USER_SECRET_KEY);
    let reader = s3_client(&endpoint, READER, USER_SECRET_KEY);
    let deleter = s3_client(&endpoint, DELETER, USER_SECRET_KEY);

    assert_read_error(put_public_access_block(&writer, OTHER).await, "AccessDenied", 403, "write outside grant");
    assert!(stored_public_access_block(OTHER).await.is_empty());
    assert_read_error(get_public_access_block(&reader, OTHER).await, "AccessDenied", 403, "read outside grant");
    assert_read_error(
        delete_public_access_block(&deleter, OTHER).await,
        "AccessDenied",
        403,
        "delete outside grant",
    );
    assert_read_error(
        get_public_access_block(&writer, GRANTED).await,
        "AccessDenied",
        403,
        "put grant cannot read",
    );
    assert_read_error(
        put_public_access_block(&reader, GRANTED).await,
        "AccessDenied",
        403,
        "get grant cannot write",
    );
    assert_read_error(
        delete_public_access_block(&writer, GRANTED).await,
        "AccessDenied",
        403,
        "put grant cannot delete",
    );
    assert_read_error(get_public_access_block(&root, MISSING).await, "NoSuchBucket", 404, "read missing bucket");
    assert_read_error(put_public_access_block(&root, MISSING).await, "NoSuchBucket", 404, "write missing bucket");
    assert_read_error(
        delete_public_access_block(&root, MISSING).await,
        "NoSuchBucket",
        404,
        "delete missing bucket",
    );

    assert_eq!(
        put_public_access_block(&writer, GRANTED).await,
        Ok(()),
        "writer on matching action and resource"
    );
    assert_eq!(stored_public_access_block(GRANTED).await, PUBLIC_ACCESS_BLOCK_XML.as_bytes());
    assert_eq!(
        get_public_access_block(&reader, GRANTED).await,
        Ok(()),
        "reader on matching action and resource"
    );

    let wrong_owner = outcome(
        root.get_public_access_block()
            .bucket(GRANTED)
            .expected_bucket_owner("123456789012")
            .send()
            .await,
    );
    if checks_expected_owner {
        assert_read_error(wrong_owner, "AccessDenied", 403, "expected owner mismatch");
    } else {
        assert_eq!(wrong_owner, Ok(()), "legacy expected owner header handling");
    }

    for attempt in ["configured", "unconfigured"] {
        let status = StatusCapture::default();
        let deleted = deleter
            .delete_public_access_block()
            .bucket(GRANTED)
            .customize()
            .interceptor(status.clone())
            .send()
            .await;
        assert_eq!((outcome(deleted), status.take()), (Ok(()), Some(204)), "{attempt}");
        assert!(stored_public_access_block(GRANTED).await.is_empty(), "{attempt}");
        let error = reader
            .get_public_access_block()
            .bucket(GRANTED)
            .send()
            .await
            .expect_err(attempt);
        assert_eq!(error.message(), Some("Public access block configuration does not exist"), "{attempt}");
        assert_read_error(outcome(Err::<(), _>(error)), "NoSuchPublicAccessBlockConfiguration", 500, attempt);
    }

    assert_eq!(put_public_access_block(&root, RESERVED).await, Ok(()));
    assert_eq!(get_public_access_block(&root, RESERVED).await, Ok(()));
    assert_eq!(delete_public_access_block(&root, RESERVED).await, Ok(()));
    assert!(stored_public_access_block(RESERVED).await.is_empty());
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_serves_public_access_block_configuration() {
    with_stack(GATEWAY, |server| public_access_block_configuration(server, true));
}

#[test]
fn the_legacy_stack_serves_public_access_block_configuration() {
    with_stack(LEGACY, |server| public_access_block_configuration(server, false));
}
