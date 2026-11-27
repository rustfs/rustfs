// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end Swift default bucket SSE and enforced per-key KMS authorization tests.
//!
//! Runs in its own integration test binary so `RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true`
//! can be settled cleanly before `rustfs_sse::startup` initializes its process-wide
//! `LazyLock`, and so a counting `ObjectEncryptionResolver` can be installed on the
//! process bootstrap `InstanceContext` to verify that unauthorized Swift GET and COPY
//! requests are denied strictly BEFORE ECStore unwraps the stored DEK.

#![recursion_limit = "256"]
#![cfg(feature = "swift")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use base64_simd::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use rustfs_config::ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY;
use rustfs_credentials::Credentials;
use rustfs_iam::store::UserType;
use rustfs_kms::{KmsConfig, KmsServiceManager, init_global_kms_service_manager, service::ObjectEncryptionService};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use rustfs_policy::policy::action::KmsAction;
use rustfs_protocols::swift::SwiftError;
use rustfs_protocols::swift::bulk::{ArchiveFormat, BulkExtractResponse, handle_bulk_extract};
use rustfs_protocols::swift::container::ContainerMapper;
use rustfs_protocols::swift::dlo::{handle_dlo_get, handle_dlo_register};
use rustfs_protocols::swift::formpost::{generate_signature, handle_formpost};
use rustfs_protocols::swift::handler::SwiftRequestBody;
use rustfs_protocols::swift::object::{copy_object, get_object, head_object, put_object};
use rustfs_protocols::swift::slo::{handle_slo_get, handle_slo_put};
use rustfs_sse::{
    BucketEncryption, DecryptionRequest, EncryptionKeyKind, KmsKeyAuthorizer, SseError, SseHost, SseKmsPrincipal,
    contains_managed_encryption_metadata, sse_decryption,
};
use rustfs_test_utils::TestECStoreEnv;
use serde_json::json;
use tokio::io::AsyncReadExt;

mod ecstore_test_compat;
use ecstore_test_compat::{
    BUCKET_SSECONFIG, EncryptionResolutionError, EncryptionResolutionErrorKind, ObjectEncryptionResolver, ReadEncryptionMaterial,
    ReadEncryptionMode, ReadEncryptionRequest, bootstrap_ctx, update_bucket_config,
};

const TEST_KMS_KEY_ID: &str = "swift-kms-test-key";

struct NoopAuthorizer;

#[async_trait]
impl KmsKeyAuthorizer for NoopAuthorizer {
    async fn is_allowed(&self, _principal: &SseKmsPrincipal, _action: KmsAction, _key_id: &str) -> Result<bool, SseError> {
        Ok(true)
    }
}

struct TestReadSseHost;

#[async_trait]
impl SseHost for TestReadSseHost {
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        let manager = rustfs_kms::get_global_kms_service_manager()?;
        manager.get_encryption_service().await
    }

    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        rustfs_kms::get_global_kms_service_manager()
    }

    async fn bucket_encryption(&self, _bucket: &str) -> Result<Option<BucketEncryption>, SseError> {
        Ok(None)
    }

    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        &NoopAuthorizer
    }
}

/// Counting `ObjectEncryptionResolver` backed by `rustfs_sse::sse_decryption`,
/// mirroring the production `SseObjectEncryptionResolver` while counting every
/// managed DEK unwrap attempt so tests can prove pre-unwrap authorization.
struct CountingSseObjectEncryptionResolver {
    managed_unwrap_calls: AtomicUsize,
}

impl CountingSseObjectEncryptionResolver {
    fn new() -> Self {
        Self {
            managed_unwrap_calls: AtomicUsize::new(0),
        }
    }

    fn managed_unwrap_calls(&self) -> usize {
        self.managed_unwrap_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectEncryptionResolver for CountingSseObjectEncryptionResolver {
    async fn resolve_read_material(
        &self,
        request: ReadEncryptionRequest<'_>,
    ) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
        if contains_managed_encryption_metadata(request.metadata) {
            self.managed_unwrap_calls.fetch_add(1, Ordering::SeqCst);
        }

        let material = sse_decryption(
            &TestReadSseHost,
            DecryptionRequest {
                bucket: request.bucket,
                key: request.object,
                metadata: request.metadata,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: None,
            },
        )
        .await
        .map_err(|err| EncryptionResolutionError::new(EncryptionResolutionErrorKind::ServiceUnavailable, err.message))?;

        Ok(material.map(|mat| ReadEncryptionMaterial {
            key_bytes: mat.key_bytes,
            mode: match mat.key_kind {
                EncryptionKeyKind::Direct => ReadEncryptionMode::Direct {
                    base_nonce: mat.base_nonce,
                },
                EncryptionKeyKind::Object => ReadEncryptionMode::Object,
            },
        }))
    }
}

fn keystone_credentials(project_id: &str, access_key: &str) -> Credentials {
    let mut claims = HashMap::new();
    claims.insert("keystone_project_id".to_string(), json!(project_id));
    claims.insert("keystone_roles".to_string(), json!(["member"]));

    Credentials {
        access_key: access_key.to_string(),
        claims: Some(claims),
        ..Default::default()
    }
}

fn headers_with_len(len: usize) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", len.to_string().parse().expect("valid header"));
    headers
}

async fn set_bucket_default_sse_s3(bucket: &str) {
    let xml = r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    update_bucket_config(bucket, BUCKET_SSECONFIG, xml.as_bytes().to_vec())
        .await
        .expect("persist SSE-S3 bucket default encryption");
}

async fn set_bucket_default_sse_kms(bucket: &str, key_id: &str) {
    let xml = format!(
        r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{key_id}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#
    );
    update_bucket_config(bucket, BUCKET_SSECONFIG, xml.into_bytes())
        .await
        .expect("persist SSE-KMS bucket default encryption");
}

async fn make_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut builder = tokio_tar::Builder::new(buf);
    for &(name, data) in files {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, Cursor::new(data))
            .await
            .expect("append tar entry");
    }
    builder.into_inner().await.expect("finish tar").into_inner()
}

fn build_formpost_multipart(boundary: &str, fields: &[(&str, &str)], files: &[(&str, &str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        out.extend_from_slice(format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes());
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    for (name, filename, data) in files {
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        out.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    out
}

#[test]
fn swift_default_bucket_sse_and_pre_unwrap_kms_decrypt_authz() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build multi-thread test runtime");

    let handle = runtime.spawn(Box::pin(run_swift_default_bucket_sse_and_pre_unwrap_kms_decrypt_authz()));
    runtime.block_on(handle).expect("SSE/KMS integration task panicked");
}

async fn run_swift_default_bucket_sse_and_pre_unwrap_kms_decrypt_authz() {
    // Settle `RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true` cleanly before any SSE operation runs.
    let report = temp_env::with_var(ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY, Some("true"), rustfs_sse::startup::init);
    assert!(report.key_policy_enforced, "test process must enforce per-key SSE-KMS authorization");

    // Install counting read-side encryption resolver on the process bootstrap context.
    let resolver = Arc::new(CountingSseObjectEncryptionResolver::new());
    assert!(
        bootstrap_ctx().set_object_encryption_resolver(resolver.clone()).is_ok(),
        "fresh bootstrap context should accept encryption resolver"
    );

    // Start ECStore, static KMS, and IAM.
    let env = TestECStoreEnv::builder().prefix("swift_sse_kms").build().await;

    let kms_manager = init_global_kms_service_manager();
    let kms_cfg = KmsConfig::static_kms(TEST_KMS_KEY_ID.to_string(), BASE64_STANDARD.encode_to_string([0x42u8; 32]));
    kms_manager.configure(kms_cfg).await.expect("configure static KMS");
    kms_manager.start().await.expect("start static KMS");

    let iam = rustfs_iam::init_iam_sys(env.ecstore.clone()).await.expect("init IAM");

    // Create three active IAM identities in the same Keystone project:
    // - `swift-kms-full`: allowed `kms:GenerateDataKey` + `kms:Decrypt` on TEST_KMS_KEY_ID
    // - `swift-kms-write-only`: allowed ONLY `kms:GenerateDataKey` on TEST_KMS_KEY_ID (no `kms:Decrypt`)
    // - `swift-kms-denied`: active user with no KMS grants
    for user in ["swift-kms-full", "swift-kms-write-only", "swift-kms-denied"] {
        iam.create_user(
            user,
            &AddOrUpdateUserReq {
                secret_key: "swift-test-secret-key".to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create active IAM user");
    }

    let key_resource = format!("arn:aws:kms:::key/{TEST_KMS_KEY_ID}");
    let full_policy_bytes = serde_json::to_vec(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
            "Resource": [key_resource]
        }]
    }))
    .expect("serialize full policy");
    iam.set_policy("swift-kms-full-policy", Policy::parse_config(&full_policy_bytes).expect("parse policy"))
        .await
        .expect("store full policy");
    iam.policy_db_set("swift-kms-full", UserType::Reg, false, "swift-kms-full-policy")
        .await
        .expect("bind full policy");

    let write_only_bytes = serde_json::to_vec(&json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["kms:GenerateDataKey"],
            "Resource": [format!("arn:aws:kms:::key/{TEST_KMS_KEY_ID}")]
        }]
    }))
    .expect("serialize write-only policy");
    iam.set_policy(
        "swift-kms-write-only-policy",
        Policy::parse_config(&write_only_bytes).expect("parse policy"),
    )
    .await
    .expect("store write-only policy");
    iam.policy_db_set("swift-kms-write-only", UserType::Reg, false, "swift-kms-write-only-policy")
        .await
        .expect("bind write-only policy");

    let project_id = "swiftsseproj";
    let account = format!("AUTH_{project_id}");
    let mapper = ContainerMapper::default();
    let creds_full = keystone_credentials(project_id, "swift-kms-full");
    let creds_write_only = keystone_credentials(project_id, "swift-kms-write-only");
    let creds_denied = keystone_credentials(project_id, "swift-kms-denied");

    let plain_bucket = mapper.swift_to_s3_bucket("plain-src", project_id);
    let sse_s3_bucket = mapper.swift_to_s3_bucket("default-sse-s3", project_id);
    let sse_kms_bucket = mapper.swift_to_s3_bucket("default-sse-kms", project_id);
    let copy_dst_bucket = mapper.swift_to_s3_bucket("copy-dst", project_id);

    for bucket in [&plain_bucket, &sse_s3_bucket, &sse_kms_bucket, &copy_dst_bucket] {
        env.make_bucket(bucket, false).await;
    }
    set_bucket_default_sse_s3(&sse_s3_bucket).await;
    set_bucket_default_sse_kms(&sse_kms_bucket, TEST_KMS_KEY_ID).await;

    // -------------------------------------------------------------------------
    // 1. Default bucket SSE-S3 (AES256) on PUT, Bulk Extract, FormPost, and COPY
    // -------------------------------------------------------------------------
    let s3_plaintext = b"swift default SSE-S3 confidential payload".to_vec();

    // Missing Content-Length on default-SSE bucket must fail closed with 411 LengthRequired.
    let err = put_object(
        &account,
        "default-sse-s3",
        "missing-len.txt",
        &creds_denied,
        Cursor::new(s3_plaintext.clone()),
        &HeaderMap::new(),
    )
    .await
    .expect_err("default SSE write without Content-Length must return 411");
    assert!(matches!(err, SwiftError::LengthRequired(_)));

    // Known-length PUT encrypts under bucket default SSE-S3.
    put_object(
        &account,
        "default-sse-s3",
        "put.txt",
        &creds_denied,
        Cursor::new(s3_plaintext.clone()),
        &headers_with_len(s3_plaintext.len()),
    )
    .await
    .expect("PUT to SSE-S3 bucket succeeds");

    let info = head_object(&account, "default-sse-s3", "put.txt", &creds_denied)
        .await
        .expect("HEAD encrypted object");
    assert_eq!(
        info.user_defined.get("x-amz-server-side-encryption").map(String::as_str),
        Some("AES256"),
        "Swift PUT must apply bucket default SSE-S3"
    );
    assert_eq!(info.get_actual_size().ok(), Some(s3_plaintext.len() as i64));

    let unwraps_before_s3_get = resolver.managed_unwrap_calls();
    let mut reader = get_object(&account, "default-sse-s3", "put.txt", &creds_denied, None)
        .await
        .expect("GET SSE-S3 object");
    let mut decrypted = Vec::new();
    reader
        .stream
        .read_to_end(&mut decrypted)
        .await
        .expect("read decrypted stream");
    assert_eq!(decrypted, s3_plaintext);
    assert!(
        resolver.managed_unwrap_calls() > unwraps_before_s3_get,
        "SSE-S3 GET must unwrap encrypted object via resolver"
    );

    // Bulk extract into default-SSE-S3 container encrypts extracted objects.
    let tar_payload = b"bulk extracted secret";
    let tar = make_tar(&[("extracted.txt", tar_payload)]).await;
    let bulk_resp = handle_bulk_extract(&account, "default-sse-s3", ArchiveFormat::Tar, tar, &creds_denied)
        .await
        .expect("bulk extract into SSE-S3 container");
    let bulk_body = bulk_resp.into_body().collect().await.expect("body").to_bytes();
    let bulk_parsed: BulkExtractResponse = serde_json::from_slice(&bulk_body).expect("json");
    assert_eq!(bulk_parsed.number_files_created, 1);
    let bulk_info = head_object(&account, "default-sse-s3", "extracted.txt", &creds_denied)
        .await
        .expect("HEAD bulk extracted object");
    assert_eq!(
        bulk_info.user_defined.get("x-amz-server-side-encryption").map(String::as_str),
        Some("AES256")
    );

    // FormPost into default-SSE-S3 container encrypts uploaded files.
    let fp_path = format!("/v1/{account}/default-sse-s3");
    let fp_redirect = "https://portal.example.com/ok";
    let fp_key = "sse-formpost-key";
    let fp_exp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;
    let fp_sig = generate_signature(&fp_path, fp_redirect, 4096, 2, fp_exp, fp_key).expect("sign");
    let fp_boundary = "----SseFormPost";
    let fp_body = build_formpost_multipart(
        fp_boundary,
        &[
            ("redirect", fp_redirect),
            ("max_file_size", "4096"),
            ("max_file_count", "2"),
            ("expires", &fp_exp.to_string()),
            ("signature", &fp_sig),
        ],
        &[("file", "formpost.txt", b"formpost secret")],
    );
    let fp_resp = handle_formpost(
        &account,
        "default-sse-s3",
        &fp_path,
        &format!("multipart/form-data; boundary={fp_boundary}"),
        fp_body,
        fp_key,
        &creds_denied,
    )
    .await
    .expect("formpost into SSE-S3 container");
    assert_eq!(fp_resp.status(), StatusCode::SEE_OTHER);
    let fp_info = head_object(&account, "default-sse-s3", "formpost.txt", &creds_denied)
        .await
        .expect("HEAD formpost encrypted object");
    assert_eq!(
        fp_info.user_defined.get("x-amz-server-side-encryption").map(String::as_str),
        Some("AES256")
    );

    // COPY from plaintext container into default-SSE-S3 container encrypts destination.
    let copy_plain = b"plaintext copied into encrypted container".to_vec();
    put_object(
        &account,
        "plain-src",
        "src.txt",
        &creds_denied,
        Cursor::new(copy_plain.clone()),
        &headers_with_len(copy_plain.len()),
    )
    .await
    .expect("put plaintext source");
    copy_object(
        &account,
        "plain-src",
        "src.txt",
        &account,
        "default-sse-s3",
        "copied-encrypted.txt",
        &creds_denied,
        &HeaderMap::new(),
    )
    .await
    .expect("COPY into SSE-S3 container");
    let copied_info = head_object(&account, "default-sse-s3", "copied-encrypted.txt", &creds_denied)
        .await
        .expect("HEAD copied encrypted object");
    assert_eq!(
        copied_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(String::as_str),
        Some("AES256")
    );
    let mut copied_reader = get_object(&account, "default-sse-s3", "copied-encrypted.txt", &creds_denied, None)
        .await
        .expect("GET copied encrypted object");
    let mut copied_bytes = Vec::new();
    copied_reader
        .stream
        .read_to_end(&mut copied_bytes)
        .await
        .expect("read copied stream");
    assert_eq!(copied_bytes, copy_plain);

    // -------------------------------------------------------------------------
    // 2. Default bucket SSE-KMS + enforced `kms:GenerateDataKey` & pre-unwrap `kms:Decrypt`
    // -------------------------------------------------------------------------
    let kms_plaintext = b"top-secret kms-protected swift object".to_vec();

    // Caller without `kms:GenerateDataKey` is denied with 403 Forbidden and nothing is written.
    let err = put_object(
        &account,
        "default-sse-kms",
        "secret.txt",
        &creds_denied,
        Cursor::new(kms_plaintext.clone()),
        &headers_with_len(kms_plaintext.len()),
    )
    .await
    .expect_err("PUT without kms:GenerateDataKey must fail with 403");
    assert!(matches!(err, SwiftError::Forbidden(_)), "expected Forbidden, got {err:?}");
    assert!(matches!(
        head_object(&account, "default-sse-kms", "secret.txt", &creds_full).await,
        Err(SwiftError::NotFound(_))
    ));

    // Caller WITH `kms:GenerateDataKey` (write-only user) succeeds in encrypting the object.
    put_object(
        &account,
        "default-sse-kms",
        "secret.txt",
        &creds_write_only,
        Cursor::new(kms_plaintext.clone()),
        &headers_with_len(kms_plaintext.len()),
    )
    .await
    .expect("PUT with kms:GenerateDataKey succeeds");

    let kms_info = head_object(&account, "default-sse-kms", "secret.txt", &creds_full)
        .await
        .expect("HEAD SSE-KMS object");
    assert_eq!(
        kms_info.user_defined.get("x-amz-server-side-encryption").map(String::as_str),
        Some("aws:kms")
    );
    assert_eq!(
        kms_info
            .user_defined
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .map(String::as_str),
        Some(TEST_KMS_KEY_ID)
    );

    // Pre-unwrap invariant: callers WITHOUT `kms:Decrypt` (`kms_write_only` and `kms_denied`)
    // MUST receive 403 Forbidden AND trigger ZERO DEK unwrap attempts in ECStore.
    let unwraps_before_denied = resolver.managed_unwrap_calls();

    let err = get_object(&account, "default-sse-kms", "secret.txt", &creds_write_only, None)
        .await
        .err()
        .expect("write-only user without kms:Decrypt must be denied on GET");
    assert!(matches!(err, SwiftError::Forbidden(_)), "expected Forbidden, got {err:?}");
    assert_eq!(
        resolver.managed_unwrap_calls(),
        unwraps_before_denied,
        "denied Swift GET must fail strictly BEFORE DEK unwrap"
    );

    let err = get_object(&account, "default-sse-kms", "secret.txt", &creds_denied, None)
        .await
        .err()
        .expect("unprivileged user must be denied on GET");
    assert!(matches!(err, SwiftError::Forbidden(_)));
    assert_eq!(
        resolver.managed_unwrap_calls(),
        unwraps_before_denied,
        "denied Swift GET must never reach resolver"
    );

    let err = copy_object(
        &account,
        "default-sse-kms",
        "secret.txt",
        &account,
        "copy-dst",
        "exfil.txt",
        &creds_write_only,
        &HeaderMap::new(),
    )
    .await
    .expect_err("COPY of SSE-KMS source without kms:Decrypt must fail with 403");
    assert!(matches!(err, SwiftError::Forbidden(_)));
    assert_eq!(
        resolver.managed_unwrap_calls(),
        unwraps_before_denied,
        "denied Swift COPY must fail strictly BEFORE DEK unwrap"
    );
    assert!(matches!(
        head_object(&account, "copy-dst", "exfil.txt", &creds_full).await,
        Err(SwiftError::NotFound(_))
    ));

    // Caller WITH `kms:Decrypt` succeeds on GET and COPY and reads back exact plaintext.
    let mut kms_reader = get_object(&account, "default-sse-kms", "secret.txt", &creds_full, None)
        .await
        .expect("authorized user with kms:Decrypt succeeds on GET");
    let mut kms_read_back = Vec::new();
    kms_reader
        .stream
        .read_to_end(&mut kms_read_back)
        .await
        .expect("read decrypted KMS stream");
    assert_eq!(kms_read_back, kms_plaintext);
    assert!(
        resolver.managed_unwrap_calls() > unwraps_before_denied,
        "authorized GET must unwrap DEK via resolver"
    );

    copy_object(
        &account,
        "default-sse-kms",
        "secret.txt",
        &account,
        "copy-dst",
        "authorized-copy.txt",
        &creds_full,
        &HeaderMap::new(),
    )
    .await
    .expect("authorized COPY of SSE-KMS source succeeds");
    let mut dst_reader = get_object(&account, "copy-dst", "authorized-copy.txt", &creds_full, None)
        .await
        .expect("GET copied object");
    let mut dst_bytes = Vec::new();
    dst_reader.stream.read_to_end(&mut dst_bytes).await.expect("read dst");
    assert_eq!(dst_bytes, kms_plaintext);

    // -------------------------------------------------------------------------
    // 3. Default-SSE self-copy (no deadlock) + SLO PUT/GET & DLO GET framing
    // -------------------------------------------------------------------------
    let mut self_copy_headers = HeaderMap::new();
    self_copy_headers.insert("x-object-meta-version", "v2".parse().expect("valid header"));
    copy_object(
        &account,
        "default-sse-kms",
        "secret.txt",
        &account,
        "default-sse-kms",
        "secret.txt",
        &creds_full,
        &self_copy_headers,
    )
    .await
    .expect("SSE-KMS self-copy must complete without deadlocking");
    let self_copied_info = head_object(&account, "default-sse-kms", "secret.txt", &creds_full)
        .await
        .expect("HEAD after SSE-KMS self-copy");
    assert_eq!(
        self_copied_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(String::as_str),
        Some("aws:kms")
    );
    assert_eq!(self_copied_info.user_defined.get("version").map(String::as_str), Some("v2"));
    let mut self_copied_reader = get_object(&account, "default-sse-kms", "secret.txt", &creds_full, None)
        .await
        .expect("GET after SSE-KMS self-copy");
    let mut self_copied_bytes = Vec::new();
    self_copied_reader
        .stream
        .read_to_end(&mut self_copied_bytes)
        .await
        .expect("read self-copied KMS stream");
    assert_eq!(self_copied_bytes, kms_plaintext);

    // SLO PUT + GET over default-SSE-S3 segments (verifies logical plaintext size validation).
    let seg1 = b"encrypted-slo-001".to_vec(); // 17 bytes
    let seg2 = b"encrypted-slo-00002".to_vec(); // 19 bytes
    let etag1 = put_object(
        &account,
        "default-sse-s3",
        "slo-seg-1",
        &creds_denied,
        Cursor::new(seg1.clone()),
        &headers_with_len(seg1.len()),
    )
    .await
    .expect("put encrypted SLO seg1");
    let etag2 = put_object(
        &account,
        "default-sse-s3",
        "slo-seg-2",
        &creds_denied,
        Cursor::new(seg2.clone()),
        &headers_with_len(seg2.len()),
    )
    .await
    .expect("put encrypted SLO seg2");

    let slo_manifest_json = serde_json::to_vec(&json!({
        "segments": [
            {
                "path": "/default-sse-s3/slo-seg-1",
                "size_bytes": seg1.len(),
                "etag": etag1.trim_matches('"')
            },
            {
                "path": "/default-sse-s3/slo-seg-2",
                "size_bytes": seg2.len(),
                "etag": etag2.trim_matches('"')
            }
        ]
    }))
    .expect("serialize SLO manifest");
    let slo_body: SwiftRequestBody = UnsyncBoxBody::new(
        Full::new(Bytes::from(slo_manifest_json.clone())).map_err(|never: std::convert::Infallible| match never {}),
    );
    let slo_put_resp = handle_slo_put(
        &account,
        "default-sse-s3",
        "encrypted-slo",
        slo_body,
        &headers_with_len(slo_manifest_json.len()),
        &Some(creds_denied.clone()),
    )
    .await
    .expect("SLO PUT over encrypted segments must validate logical plaintext sizes");
    assert_eq!(slo_put_resp.status(), StatusCode::CREATED);

    let slo_get_resp = handle_slo_get(
        &account,
        "default-sse-s3",
        "encrypted-slo",
        &HeaderMap::new(),
        &Some(creds_denied.clone()),
    )
    .await
    .expect("SLO GET over encrypted segments succeeds");
    let mut expected_slo = seg1.clone();
    expected_slo.extend_from_slice(&seg2);
    assert_eq!(
        slo_get_resp.headers().get("content-length").and_then(|v| v.to_str().ok()),
        Some(expected_slo.len().to_string().as_str())
    );
    let slo_bytes = slo_get_resp.into_body().collect().await.expect("collect SLO").to_bytes();
    assert_eq!(slo_bytes.as_ref(), expected_slo.as_slice());

    // DLO register + GET (full and Range) over default-SSE-S3 segments (verifies plaintext framing).
    let dlo1 = b"dlo-part-alpha".to_vec(); // 14 bytes
    let dlo2 = b"dlo-part-beta-tail".to_vec(); // 18 bytes
    put_object(
        &account,
        "default-sse-s3",
        "dlo-segs/001",
        &creds_denied,
        Cursor::new(dlo1.clone()),
        &headers_with_len(dlo1.len()),
    )
    .await
    .expect("put encrypted DLO seg1");
    put_object(
        &account,
        "default-sse-s3",
        "dlo-segs/002",
        &creds_denied,
        Cursor::new(dlo2.clone()),
        &headers_with_len(dlo2.len()),
    )
    .await
    .expect("put encrypted DLO seg2");

    let dlo_reg = handle_dlo_register(
        &account,
        "default-sse-s3",
        "encrypted-dlo",
        "default-sse-s3/dlo-segs/",
        &Some(creds_denied.clone()),
    )
    .await
    .expect("register DLO in encrypted container");
    assert_eq!(dlo_reg.status(), StatusCode::CREATED);

    let mut expected_dlo = dlo1.clone();
    expected_dlo.extend_from_slice(&dlo2);

    let dlo_full_resp = handle_dlo_get(
        &account,
        "default-sse-s3",
        "encrypted-dlo",
        &HeaderMap::new(),
        &Some(creds_denied.clone()),
        "default-sse-s3/dlo-segs/".to_string(),
    )
    .await
    .expect("DLO full GET over encrypted segments succeeds");
    assert_eq!(
        dlo_full_resp.headers().get("content-length").and_then(|v| v.to_str().ok()),
        Some(expected_dlo.len().to_string().as_str()),
        "DLO Content-Length must equal plaintext total, not ciphertext size"
    );
    let dlo_full_bytes = dlo_full_resp.into_body().collect().await.expect("collect DLO").to_bytes();
    assert_eq!(dlo_full_bytes.as_ref(), expected_dlo.as_slice());

    let mut dlo_range_headers = HeaderMap::new();
    dlo_range_headers.insert("range", "bytes=10-20".parse().expect("valid range"));
    let dlo_range_resp = handle_dlo_get(
        &account,
        "default-sse-s3",
        "encrypted-dlo",
        &dlo_range_headers,
        &Some(creds_denied.clone()),
        "default-sse-s3/dlo-segs/".to_string(),
    )
    .await
    .expect("DLO range GET over encrypted segments succeeds");
    assert_eq!(dlo_range_resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        dlo_range_resp.headers().get("content-range").and_then(|v| v.to_str().ok()),
        Some("bytes 10-20/32")
    );
    assert_eq!(dlo_range_resp.headers().get("content-length").and_then(|v| v.to_str().ok()), Some("11"));
    let dlo_range_bytes = dlo_range_resp
        .into_body()
        .collect()
        .await
        .expect("collect DLO range")
        .to_bytes();
    assert_eq!(dlo_range_bytes.as_ref(), &expected_dlo[10..=20]);

    kms_manager.stop().await.expect("stop static KMS");
}
