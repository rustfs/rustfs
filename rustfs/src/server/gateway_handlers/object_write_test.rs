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

//! PutObject edge authorization and native frame tests against real storage.

use super::*;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::server::http::access::test_support::{
    AuthorizationRequest, BucketOperations as _, CompletePart, DeleteBucketOptions, ETag, ETagCondition, FS, MakeBucketOptions,
    MultipartOperations as _, ObjectIO as _, ObjectOperations as _, ObjectOptions, PutObjectInput as LegacyPutObjectInput,
    ReqInfo, S3Access as _, S3Request, get_bucket_metadata, set_bucket_metadata,
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method};
use rustfs_gateway::{Limits, MetaView, SseConfig, TargetKind, TransportSecurity, WireRequest};
use rustfs_gateway_stream::{ByteStream, PayloadCaps, PayloadRead, PayloadStream, StreamError, TrailingHeaders};
use serial_test::serial;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

fn request<O: rustfs_gateway::Operation<DerivedResources = rustfs_gateway::NoDerived>>(input: O::Input) -> Req<O> {
    let wire = WireRequest::accept(
        http::Request::builder()
            .uri("/")
            .header("host", "localhost")
            .body(Bytes::new())
            .unwrap(),
        &Limits::default(),
    )
    .unwrap();
    let meta = MetaView::of(&wire, TargetKind::Service).unwrap();
    let proof = rustfs_gateway::enforce_sse(&meta, TransportSecurity::Encrypted, &SseConfig::strict()).unwrap();
    Req::new(input, proof)
}

async fn fixture(actions: &[&str]) -> (Arc<ServerContextSlot>, String) {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server = ServerContextSlot::new();
    assert!(server.install(crate::app::gating_test_env::shared_gating_ambient().await));
    let bucket = format!("put-gateway-{}", uuid::Uuid::new_v4());
    store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
    if !actions.is_empty() {
        set_policy(
            &bucket,
            serde_json::json!({
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": actions,
                "Resource": format!("arn:aws:s3:::{bucket}/allowed"),
            }),
        )
        .await;
    }
    (server, bucket)
}

async fn set_policy(bucket: &str, statement: serde_json::Value) {
    let policy = serde_json::json!({"Version": "2012-10-17", "Statement": [statement]}).to_string();
    let mut metadata = (*get_bucket_metadata(bucket).await.unwrap()).clone();
    metadata.policy_config = Some(serde_json::from_str(&policy).unwrap());
    metadata.policy_config_json = policy.into_bytes();
    set_bucket_metadata(bucket.to_owned(), metadata).await.unwrap();
}

fn envelope(server: &Arc<ServerContextSlot>, headers: HeaderMap) -> RequestEnvelope {
    let mut legacy = build_request((), Method::PUT);
    legacy.extensions.insert(Arc::clone(server));
    legacy.extensions.insert(ReqInfo::default());
    legacy.headers = headers;
    RequestEnvelope::from_s3s(&legacy)
}

#[tokio::test]
#[serial]
async fn put_authorization_checks_action_resource_and_write_privileges_on_both_edges() {
    let cases = [
        (vec!["s3:PutObject"], "allowed", None, true),
        (vec![], "allowed", None, false),
        (vec!["s3:PutObject"], "denied", None, false),
        (vec!["s3:GetObject"], "allowed", None, false),
        (vec!["s3:PutObject"], "allowed", Some(("if-match", "\"tag\"")), false),
        (vec!["s3:PutObject"], "allowed", Some(("dto-if-match", "\"tag\"")), false),
        (vec!["s3:PutObject"], "allowed", Some(("x-amz-replication-status", "REPLICA")), false),
        (vec!["s3:PutObject"], "allowed", Some(("x-amz-object-lock-legal-hold", "ON")), false),
        (vec!["s3:PutObject"], "allowed", Some(("x-amz-object-lock-mode", "GOVERNANCE")), false),
        (
            vec!["s3:PutObject"],
            "allowed",
            Some(("x-amz-object-lock-retain-until-date", "2030-01-01T00:00:00Z")),
            false,
        ),
        (vec!["s3:PutObject", "s3:GetObject"], "allowed", Some(("if-match", "\"tag\"")), true),
        (
            vec!["s3:PutObject", "s3:PutObjectLegalHold"],
            "allowed",
            Some(("x-amz-object-lock-legal-hold", "ON")),
            true,
        ),
        (
            vec!["s3:PutObject", "s3:PutObjectRetention"],
            "allowed",
            Some(("x-amz-object-lock-mode", "GOVERNANCE")),
            true,
        ),
    ];
    for (actions, key, extra, allowed) in cases {
        let (server, bucket) = fixture(&actions).await;
        let mut headers = HeaderMap::new();
        if let Some((name, value)) = extra {
            headers.insert(name, value.parse().unwrap());
        }
        let retain = headers
            .get("x-amz-object-lock-retain-until-date")
            .map(|_| time::OffsetDateTime::from_unix_timestamp(1_893_456_000).unwrap());
        let input = dto::PutObjectInput {
            object_lock_retain_until_date: retain.map(crate::app::dto_values::timestamp),
            bucket: rustfs_gateway_types::BucketName::new(bucket.clone()).unwrap(),
            key: rustfs_gateway_types::ObjectKey::new(key).unwrap(),
            if_match: headers
                .get("if-match")
                .or_else(|| headers.get("dto-if-match"))
                .map(|value| value.to_str().unwrap().to_owned()),
            object_lock_legal_hold_status: headers
                .get("x-amz-object-lock-legal-hold")
                .map(|_| dto::ObjectLockLegalHoldStatus::custom("ON")),
            object_lock_mode: headers
                .get("x-amz-object-lock-mode")
                .map(|_| dto::ObjectLockMode::custom("GOVERNANCE")),
            ..Default::default()
        };
        let mut native = envelope(&server, headers.clone());
        let native_result = <dto::PutObject as Served>::authorize(&mut native, &request(input)).await;
        let mut legacy = build_request(
            LegacyPutObjectInput {
                object_lock_retain_until_date: retain.map(Into::into),
                bucket: bucket.clone(),
                key: key.to_owned(),
                if_match: headers
                    .get("if-match")
                    .or_else(|| headers.get("dto-if-match"))
                    .map(|_| ETagCondition::ETag(ETag::Strong("tag".to_owned()))),
                object_lock_legal_hold_status: headers.get("x-amz-object-lock-legal-hold").map(|_| "ON".to_owned().into()),
                object_lock_mode: headers.get("x-amz-object-lock-mode").map(|_| "GOVERNANCE".to_owned().into()),
                ..Default::default()
            },
            Method::PUT,
        );
        legacy.headers = headers;
        legacy.extensions.insert(Arc::clone(&server));
        legacy.extensions.insert(ReqInfo::default());
        let legacy_result = FS::with_server_ctx(Arc::clone(&server)).put_object(&mut legacy).await;
        for result in [
            native_result.map_err(|error| error.code().as_str().to_owned()),
            legacy_result.map_err(|error| error.code().as_str().to_owned()),
        ] {
            assert_eq!(
                result,
                if allowed { Ok(()) } else { Err("AccessDenied".to_owned()) },
                "{actions:?} {key} {extra:?}"
            );
        }
        assert_eq!(AuthorizationRequest::requested_retain_until(&native).map(|value| value.0), retain);
        let info = native.req_info().unwrap();
        assert_eq!(info.bucket.as_deref(), Some(bucket.as_str()));
        assert_eq!(info.object.as_deref(), Some(key));
        if allowed {
            assert!(native.bucket_generation().is_some());
        }
    }
}

#[tokio::test]
#[serial]
async fn put_authorization_honors_request_object_tag_policy_on_both_edges() {
    let (server, bucket) = fixture(&[]).await;
    set_policy(
        &bucket,
        serde_json::json!({
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket}/allowed"),
            "Condition": {"StringEquals": {"s3:RequestObjectTag/visibility": "private"}},
        }),
    )
    .await;
    for (tagging, allowed) in [
        (Some("visibility=private"), true),
        (Some("visibility=public"), false),
        (None, false),
    ] {
        let mut headers = HeaderMap::new();
        if let Some(value) = tagging {
            headers.insert("x-amz-tagging", HeaderValue::from_static(value));
        }
        let mut native = envelope(&server, headers.clone());
        let request = request(dto::PutObjectInput {
            bucket: rustfs_gateway_types::BucketName::new(bucket.clone()).unwrap(),
            key: rustfs_gateway_types::ObjectKey::new("allowed").unwrap(),
            tagging: tagging.map(str::to_owned),
            ..Default::default()
        });
        let native = <dto::PutObject as Served>::authorize(&mut native, &request)
            .await
            .err()
            .map(|error| error.code().as_str().to_owned());
        let mut legacy = build_request(
            LegacyPutObjectInput {
                bucket: bucket.clone(),
                key: "allowed".to_owned(),
                tagging: tagging.map(str::to_owned),
                ..Default::default()
            },
            Method::PUT,
        );
        legacy.headers = headers;
        legacy.extensions.insert(Arc::clone(&server));
        legacy.extensions.insert(ReqInfo::default());
        let legacy = FS::with_server_ctx(Arc::clone(&server))
            .put_object(&mut legacy)
            .await
            .err()
            .map(|error| error.code().as_str().to_owned());
        let expected = (!allowed).then_some("AccessDenied");
        assert_eq!(legacy.as_deref(), expected, "legacy edge with {tagging:?}");
        assert_eq!(native.as_deref(), expected, "gateway edge with {tagging:?}");
    }
}

#[tokio::test]
#[serial]
async fn put_authorization_installs_the_decoded_retention_date() {
    use crate::server::gateway_handlers::error::handler_error;
    use rustfs_gateway::{Handler, HandlerResult, RegionSet, ServiceBuilder, SigV4Authenticator, StaticCredentials, allow_when};

    const DATE: &str = "2099-01-02T08:00:00.123456789+08:00";
    struct Probe(Arc<ServerContextSlot>);

    impl Handler<dto::PutObject> for Probe {
        async fn call(&self, request: Req<dto::PutObject>) -> HandlerResult<dto::PutObject> {
            let mut envelope = envelope(&self.0, HeaderMap::new());
            <dto::PutObject as Served>::authorize(&mut envelope, &request)
                .await
                .map_err(handler_error)?;
            let date = AuthorizationRequest::requested_retain_until(&envelope)
                .expect("authorized date")
                .0;
            assert_eq!(date.format(&time::format_description::well_known::Rfc3339).unwrap(), DATE);
            Ok(Resp::new(dto::PutObjectOutput::default()))
        }
    }

    let (server, bucket) = fixture(&[]).await;
    set_policy(
        &bucket,
        serde_json::json!({
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": ["s3:PutObject", "s3:PutObjectRetention"],
            "Resource": format!("arn:aws:s3:::{bucket}/allowed"),
            "Condition": {"NumericGreaterThanEquals": {"s3:object-lock-remaining-retention-days": "1"}},
        }),
    )
    .await;
    let authenticator =
        SigV4Authenticator::new(Arc::new(StaticCredentials::new()), RegionSet::new(["us-east-1"]).unwrap()).rustfs_profile();
    let service = ServiceBuilder::new()
        .rustfs_profile()
        .authenticator(authenticator)
        .authorizer(allow_when(|_| true))
        .register::<dto::PutObject, _>(Arc::new(Probe(server)))
        .build()
        .unwrap();
    let write = |date: Option<&str>| {
        let mut request = http::Request::builder()
            .method(Method::PUT)
            .uri(format!("/{bucket}/allowed"))
            .header("host", "localhost")
            .header("content-length", "0");
        if let Some(date) = date {
            request = request
                .header("x-amz-object-lock-mode", "GOVERNANCE")
                .header("x-amz-object-lock-retain-until-date", date);
        }
        request.body(http_body_util::Empty::<Bytes>::new()).unwrap()
    };
    assert_eq!(service.call(write(None)).await.status(), http::StatusCode::FORBIDDEN);
    assert_eq!(service.call(write(Some(DATE))).await.status(), http::StatusCode::OK);
}

#[derive(Debug, thiserror::Error)]
#[error("fixture producer failure")]
struct FixtureProducerFailure;

struct Frames(VecDeque<Result<PayloadRead, StreamError>>);
impl PayloadStream for Frames {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        Poll::Ready(self.0.pop_front().unwrap_or_else(|| Err(StreamError::polled_after_eof())))
    }
    fn caps(&self) -> PayloadCaps {
        PayloadCaps::PUSH
    }
    fn len_hint(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
#[serial]
async fn native_put_frames_reach_checksum_validation_and_storage() {
    let (server, bucket) = fixture(&["s3:PutObject"]).await;
    for (case, length, checksum, producer_error, expected) in [
        ("valid", 3, "NSRBwg==", false, None),
        ("bad-checksum", 3, "AAAAAA==", false, Some("BadDigest")),
        ("short", 4, "NSRBwg==", false, Some("IncompleteBody")),
        ("producer", 3, "NSRBwg==", true, Some("InternalError")),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-crc32"));
        headers.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("CRC32"));
        headers.insert("x-amz-decoded-content-length", length.to_string().parse().unwrap());
        let mut trailers = HeaderMap::new();
        trailers.insert("x-amz-checksum-crc32", checksum.parse().unwrap());
        let end = if producer_error {
            Err(StreamError::upstream(Box::new(FixtureProducerFailure)))
        } else {
            Ok(PayloadRead::Eof {
                trailers: TrailingHeaders::from_header_map(trailers),
            })
        };
        let body = ByteStream::new(Box::pin(Frames(VecDeque::from([
            Ok(PayloadRead::Chunk(Bytes::from_static(b"abc"))),
            end,
        ]))))
        .unwrap();
        let req = request(dto::PutObjectInput {
            bucket: rustfs_gateway_types::BucketName::new(bucket.clone()).unwrap(),
            key: rustfs_gateway_types::ObjectKey::new("allowed").unwrap(),
            body: Some(body),
            content_length: length,
            checksum_algorithm: Some(dto::ChecksumAlgorithm::custom("CRC32")),
            ..Default::default()
        });
        let mut envelope = envelope(&server, headers);
        <dto::PutObject as Served>::authorize(&mut envelope, &req).await.unwrap();
        let result = <dto::PutObject as Served>::execute(server.app_context(), envelope, req).await;
        match expected {
            None => {
                let response = result.unwrap();
                assert_eq!(
                    response.output().unwrap().e_tag,
                    crate::app::dto_values::stored_etag("900150983cd24fb0d6963f7d28e17f72")
                );
                let (_, status, headers) = response.into_parts();
                assert_eq!(status, 200, "{case}");
                assert_eq!(headers["x-amz-checksum-crc32"], checksum);
                let stored = server
                    .object_store()
                    .unwrap()
                    .get_object_info(&bucket, "allowed", &ObjectOptions::default())
                    .await
                    .unwrap();
                assert_eq!(stored.size, 3);
                assert_eq!(stored.etag.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
                server
                    .object_store()
                    .unwrap()
                    .delete_object(&bucket, "allowed", ObjectOptions::default())
                    .await
                    .unwrap();
            }
            Some(code) => {
                let error = match result {
                    Ok(_) => panic!("{case} must refuse"),
                    Err(error) => error,
                };
                assert_eq!(error.code().as_str(), code, "{case}: {error}");
                if producer_error {
                    let mut source = std::error::Error::source(&error);
                    let mut native_error = false;
                    let mut producer_error = false;
                    for _ in 0..8 {
                        let Some(error) = source else { break };
                        native_error |= error.is::<StreamError>();
                        producer_error |= error.is::<FixtureProducerFailure>();
                        source = error
                            .downcast_ref::<std::io::Error>()
                            .and_then(std::io::Error::get_ref)
                            .map(|inner| inner as &dyn std::error::Error)
                            .or_else(|| error.source());
                    }
                    assert!(native_error, "the native producer error must remain in the cause chain");
                    assert!(producer_error, "the concrete producer failure must remain in the cause chain");
                }
                assert!(
                    server
                        .object_store()
                        .unwrap()
                        .get_object_info(&bucket, "allowed", &ObjectOptions::default())
                        .await
                        .is_err(),
                    "{case} must not publish"
                );
            }
        }
    }
}

fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: http::Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
#[serial]
async fn put_source_version_selects_versioned_read_permission() {
    const VERSION: &str = "0194e0f1-0000-7000-8000-000000000000";
    for query in [false, true] {
        for (read, allowed) in [("s3:GetObject", false), ("s3:GetObjectVersion", true)] {
            let (server, bucket) = fixture(&["s3:PutObject", read, "s3:ReplicateObject"]).await;
            let mut headers = HeaderMap::new();
            let mut legacy = build_request((), Method::PUT);
            legacy.extensions.insert(Arc::clone(&server));
            legacy.extensions.insert(ReqInfo::default());
            if query {
                legacy.uri = format!("/?versionId={VERSION}").parse().unwrap();
            } else {
                headers.insert("x-minio-source-version-id", HeaderValue::from_static(VERSION));
            }
            legacy.headers = headers;
            let mut envelope = RequestEnvelope::from_s3s(&legacy);
            let req = request(dto::PutObjectInput {
                bucket: rustfs_gateway_types::BucketName::new(bucket).unwrap(),
                key: rustfs_gateway_types::ObjectKey::new("allowed").unwrap(),
                if_match: Some("\"tag\"".to_owned()),
                ..Default::default()
            });
            let result = <dto::PutObject as Served>::authorize(&mut envelope, &req).await;
            assert_eq!(
                result.map_err(|error| error.code().as_str().to_owned()),
                if allowed { Ok(()) } else { Err("AccessDenied".to_owned()) }
            );
            assert_eq!(envelope.req_info().unwrap().version_id.as_deref(), Some(VERSION));
        }
    }
}

#[test]
fn native_put_projection_preserves_unvalidated_write_fields() {
    let input = dto::PutObjectInput {
        bucket: rustfs_gateway_types::BucketName::new("put-fields").unwrap(),
        key: rustfs_gateway_types::ObjectKey::new("key").unwrap(),
        storage_class: Some(dto::StorageClass::custom("INVALID")),
        server_side_encryption: Some(dto::ServerSideEncryption::custom("INVALID")),
        object_lock_mode: Some(dto::ObjectLockMode::custom("INVALID")),
        object_lock_legal_hold_status: Some(dto::ObjectLockLegalHoldStatus::custom("INVALID")),
        tagging: Some("classification=public".to_owned()),
        metadata: [("purpose".to_owned(), "projection".to_owned())].into_iter().collect(),
        checksum_spec: Some(rustfs_gateway_types::ChecksumSpec::parse_header("x-amz-checksum-crc32", "NSRBwg==").unwrap()),
        ..Default::default()
    };
    let input = native_input(input, None);
    assert_eq!(input.storage_class.as_deref(), Some("INVALID"));
    assert_eq!(input.server_side_encryption.as_deref(), Some("INVALID"));
    assert_eq!(input.object_lock_mode.as_deref(), Some("INVALID"));
    assert_eq!(input.object_lock_legal_hold_status.as_deref(), Some("INVALID"));
    assert_eq!(input.tagging.as_deref(), Some("classification=public"));
    assert_eq!(input.metadata.unwrap().get("purpose").map(String::as_str), Some("projection"));
    assert_eq!(input.checksum_crc32.as_deref(), Some("NSRBwg=="));
}

#[tokio::test]
#[serial]
async fn legacy_post_projection_keeps_early_refusal_order() {
    for (offset, invalid, actions, expected) in [
        (true, true, vec![], Some("NotImplemented")),
        (false, true, vec![], Some("InvalidArgument")),
        (false, false, vec![], Some("AccessDenied")),
        (false, false, vec!["s3:PutObject"], None),
    ] {
        let (server, bucket) = fixture(&actions).await;
        let mut req = build_request(
            LegacyPutObjectInput {
                bucket,
                key: "allowed".to_owned(),
                storage_class: Some(if invalid { "BAD\nFIELD" } else { "STANDARD" }.to_owned().into()),
                ..Default::default()
            },
            Method::POST,
        );
        req.extensions.insert(Arc::clone(&server));
        req.extensions.insert(ReqInfo::default());
        if offset {
            req.headers.insert("x-amz-write-offset-bytes", HeaderValue::from_static("0"));
        }
        let result = FS::with_server_ctx(server).put_object(&mut req).await;
        assert_eq!(result.err().map(|error| error.code().as_str().to_owned()), expected.map(str::to_owned));
    }
}

#[test]
fn native_put_response_refuses_a_missing_etag() {
    let error = match native_output(PutObjectWriteOutput::default()) {
        Ok(_) => panic!("an absent ETag must not become an empty tag"),
        Err(error) => error,
    };
    assert_eq!(error.code().as_str(), "InternalError");
}

fn part_input(bucket: &str, upload_id: &str, part_number: i32) -> dto::UploadPartInput {
    dto::UploadPartInput {
        bucket: rustfs_gateway_types::BucketName::new(bucket).unwrap(),
        key: rustfs_gateway_types::ObjectKey::new("allowed").unwrap(),
        upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(upload_id),
        part_number,
        content_length: 3,
        body: Some(ByteStream::from_bytes(Bytes::from_static(b"abc"))),
        ..Default::default()
    }
}

#[tokio::test]
#[serial]
async fn native_upload_part_rejects_missing_and_stale_generation() {
    let (server, bucket) = fixture(&["s3:PutObject"]).await;
    let store = server.object_store().unwrap();
    let options = ObjectOptions::default();
    let key = "allowed";

    // The legitimate authorization captures an incarnation while the request is still being admitted.
    let mut authorized = envelope(&server, HeaderMap::new());
    let request_without_session = request::<dto::UploadPart>(part_input(&bucket, "not-yet-active", 1));
    <dto::UploadPart as Served>::authorize(&mut authorized, &request_without_session)
        .await
        .unwrap();
    let failed =
        <dto::UploadPart as Served>::execute(server.app_context(), authorized.fork_for_authorization(), request_without_session)
            .await
            .err()
            .unwrap();
    assert_eq!(failed.code().as_str(), "NoSuchUpload");

    store.delete_bucket(&bucket, &DeleteBucketOptions::default()).await.unwrap();
    store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
    let session = store.new_multipart_upload(&bucket, key, &options).await.unwrap().upload_id;

    let missing = envelope(&server, HeaderMap::new());
    for (envelope, code) in [(missing, "InternalError"), (authorized, "NoSuchBucket")] {
        let request = request::<dto::UploadPart>(part_input(&bucket, &session, 1));
        let error = <dto::UploadPart as Served>::execute(server.app_context(), envelope, request)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code().as_str(), code);
        assert!(
            store
                .list_object_parts(&bucket, key, &session, None, 20, &options)
                .await
                .unwrap()
                .parts
                .is_empty()
        );
    }

    // The identical part is writable under a freshly captured generation.
    set_policy(
        &bucket,
        serde_json::json!({
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket}/{key}"),
        }),
    )
    .await;
    let req = request::<dto::UploadPart>(part_input(&bucket, &session, 1));
    let mut fresh = envelope(&server, HeaderMap::new());
    <dto::UploadPart as Served>::authorize(&mut fresh, &req).await.unwrap();
    <dto::UploadPart as Served>::execute(server.app_context(), fresh, req)
        .await
        .unwrap();
    let parts = store
        .list_object_parts(&bucket, key, &session, None, 20, &options)
        .await
        .unwrap()
        .parts;
    assert_eq!(
        parts.first().and_then(|part| part.etag.as_deref()),
        Some("900150983cd24fb0d6963f7d28e17f72")
    );
}

#[tokio::test]
#[serial]
async fn native_upload_part_uses_frames_and_leaves_the_prior_part_on_failure() {
    use tokio::io::AsyncReadExt;

    let (server, bucket) = fixture(&["s3:PutObject"]).await;
    let store = server.object_store().unwrap();
    let options = ObjectOptions::default();
    let key = "allowed";
    let upload_id = store.new_multipart_upload(&bucket, key, &options).await.unwrap().upload_id;
    let etag = "900150983cd24fb0d6963f7d28e17f72";
    for (label, payload, size, checksum, expected) in [
        ("valid", b"abc", 3, Some("NSRBwg=="), None),
        ("missing trailer", b"xyz", 3, None, Some("BadDigest")),
        ("wrong checksum", b"xyz", 3, Some("AAAAAA=="), Some("BadDigest")),
        ("short", b"xyz", 4, Some("6466Zw=="), Some("IncompleteBody")),
        ("digest error", b"xyz", 3, None, Some("BadDigest")),
        ("signature error", b"xyz", 3, None, Some("SignatureDoesNotMatch")),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-crc32"));
        let mut trailers = HeaderMap::new();
        if let Some(value) = checksum {
            trailers.insert("x-amz-checksum-crc32", HeaderValue::from_static(value));
        }
        let end = match label {
            "digest error" => Err(StreamError::upstream(Box::new(rustfs_rio::BadDigest {
                expected_md5: String::new(),
                calculated_md5: String::new(),
            }))),
            "signature error" => Err(StreamError::upstream(Box::new(
                crate::app::storage_api::s3::stream::aws_chunked_stream::AwsChunkedStreamError::SignatureMismatch,
            ))),
            _ => Ok(PayloadRead::Eof {
                trailers: TrailingHeaders::from_header_map(trailers),
            }),
        };
        let mut input = part_input(&bucket, &upload_id, 1);
        input.content_length = size;
        input.body = Some(
            ByteStream::new(Box::pin(Frames(VecDeque::from([
                Ok(PayloadRead::Chunk(Bytes::from_static(payload))),
                end,
            ]))))
            .unwrap(),
        );
        // The decoded trailer declaration wins even with another SDK algorithm value.
        input.checksum_algorithm = Some(dto::ChecksumAlgorithm::custom("SHA256"));
        let request = request::<dto::UploadPart>(input);
        let mut context = envelope(&server, headers);
        <dto::UploadPart as Served>::authorize(&mut context, &request).await.unwrap();
        let result = <dto::UploadPart as Served>::execute(server.app_context(), context, request).await;
        assert_eq!(result.as_ref().err().map(|error| error.code().as_str()), expected, "{label}");
        if let Ok(response) = result {
            assert_eq!(response.output().unwrap().e_tag, crate::app::dto_values::stored_etag(etag));
            let (_, status, headers) = response.into_parts();
            assert_eq!(status, 200);
            assert_eq!(headers["x-amz-checksum-crc32"], "NSRBwg==");
        }
        let parts = store
            .list_object_parts(&bucket, key, &upload_id, None, 20, &options)
            .await
            .unwrap()
            .parts;
        assert_eq!(parts.len(), 1, "{label}");
        assert_eq!(
            (parts[0].part_num, parts[0].size, parts[0].etag.as_deref()),
            (1, 3, Some(etag)),
            "{label}"
        );
    }
    store
        .clone()
        .complete_multipart_upload(
            &bucket,
            key,
            &upload_id,
            vec![CompletePart {
                part_num: 1,
                etag: Some(etag.to_owned()),
                ..Default::default()
            }],
            &options,
        )
        .await
        .unwrap();
    let mut old = store
        .get_object_reader(&bucket, key, None, HeaderMap::new(), &options)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    old.stream.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"abc");
}
