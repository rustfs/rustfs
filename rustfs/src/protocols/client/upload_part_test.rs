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

//! UploadPart protocol-client observations against the unchanged legacy dispatch.
//! These probes observe storage, body demand and responses; they do not define ownership rules.

use super::super::super::{
    ETag as LegacyETag, LegacyS3Error, LegacyS3ErrorCode, ServerSideEncryption as LegacyEncryption,
    UploadPartInput as LegacyInput, UploadPartOutput as LegacyOutput,
};
use super::*;
use crate::app::storage_api::test::contract::multipart::MultipartOperations;
use crate::storage_api::protocols::client::StorageObjectOptions;
use rustfs_gateway_stream::{ByteStream, PayloadCaps, PayloadRead, PayloadStream, StreamError, TrailingHeaders};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

struct CountedBody {
    polls: Arc<AtomicUsize>,
    bytes: Option<bytes::Bytes>,
    failure: bool,
}

impl PayloadStream for CountedBody {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        if std::mem::take(&mut self.failure) {
            return Poll::Ready(Err(StreamError::upstream(Box::new(std::io::Error::other("protocol-producer-failure")))));
        }
        Poll::Ready(Ok(match self.bytes.take() {
            Some(bytes) => PayloadRead::Chunk(bytes),
            None => PayloadRead::Eof {
                trailers: TrailingHeaders::empty(),
            },
        }))
    }

    fn caps(&self) -> PayloadCaps {
        PayloadCaps::PUSH
    }
    fn len_hint(&self) -> Option<u64> {
        None
    }
}

fn input(bucket: &str, number: i32, length: i64) -> (dto::UploadPartInput, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let body = ByteStream::new(Box::pin(CountedBody {
        polls: polls.clone(),
        bytes: Some(bytes::Bytes::from_static(b"abc")),
        failure: false,
    }))
    .expect("body caps");
    (
        dto::UploadPartInput {
            bucket: crate::app::dto_values::stored_bucket_name(bucket).expect("stored bucket"),
            key: crate::app::dto_values::stored_object_key("directory/a ?#+%.txt").expect("stored key"),
            part_number: number,
            content_length: length,
            body: Some(body),
            ..Default::default()
        },
        polls,
    )
}

fn count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == rustfs_s3_ops::S3Operation::UploadPart.as_str())
        .map_or(0, |row| row.total)
}

#[test]
#[serial_test::serial]
fn upload_part_refusals_match_original_without_persisting_parts() {
    crate::app::gating_test_env::run_large_stack_test("protocol-part-refusals", || async {
        let bucket = "protocol-part-refusals";
        let (_temp, root, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let created = backend
            .create_multipart_upload(
                dto::CreateMultipartUploadInput {
                    bucket: crate::app::dto_values::stored_bucket_name(bucket).expect("bucket"),
                    key: crate::app::dto_values::stored_object_key("directory/a ?#+%.txt").expect("key"),
                    ..Default::default()
                },
                &root,
            )
            .await
            .expect("create upload");
        let store = backend.server_ctx.app_context().expect("context").object_store();
        let probe = crate::app::storage_api::helper::MutationCompletionProbe::install(bucket);
        for case in [
            "zero",
            "negative",
            "too-large",
            "missing-upload",
            "wrong-key",
            "no-body",
            "unknown-length",
            "short-body",
            "source-error",
            "wrong-md5",
            "malformed-md5",
            "bad-sse",
            "bad-sse-md5",
        ] {
            let make = || {
                let (mut request, polls) = input(bucket, 1, 3);
                let mut upload_id = created.upload_id.as_str();
                match case {
                    "zero" => request.part_number = 0,
                    "negative" => request.part_number = -1,
                    "too-large" => request.part_number = 10001,
                    "missing-upload" => upload_id = "missing/+?=&%",
                    "wrong-key" => request.key = crate::app::dto_values::stored_object_key("different").expect("key"),
                    "no-body" => request.body = None,
                    "unknown-length" => request.content_length = -1,
                    "wrong-md5" => request.content_md5 = Some("AAAAAAAAAAAAAAAAAAAAAA==".into()),
                    "short-body" => request.content_length = 4,
                    "source-error" => {
                        request.body = Some(
                            ByteStream::new(Box::pin(CountedBody {
                                polls: polls.clone(),
                                bytes: None,
                                failure: true,
                            }))
                            .expect("producer caps"),
                        )
                    }
                    "malformed-md5" => request.content_md5 = Some("bad".into()),
                    "bad-sse" => request.sse_customer_algorithm = Some("bad".into()),
                    "bad-sse-md5" => {
                        request.sse_customer_algorithm = Some("AES256".into());
                        request.sse_customer_key_md5 = Some("bad".into());
                    }
                    _ => unreachable!(),
                }
                (MultipartRequest::upload_part(request, upload_id), polls)
            };
            let (request, new_polls) = make();
            let before = count();
            let actual = backend.upload_part(request, &root).await;
            let actual_count = count() - before;
            let parts = store
                .list_object_parts(
                    bucket,
                    "directory/a ?#+%.txt",
                    &created.upload_id,
                    None,
                    100,
                    &StorageObjectOptions::default(),
                )
                .await
                .expect("parts")
                .parts;
            let actual_audit = audit_facts(&probe);
            let new_polls = new_polls.load(Ordering::Relaxed);
            let refusal = actual.expect_err("expected upload refusal");
            if case == "missing-upload" {
                assert_eq!(refusal.code(), &S3ErrorCode::NoSuchUpload);
                assert_eq!(refusal.status_code(), Some(http::StatusCode::NOT_FOUND));
                assert!(!refusal.message().unwrap_or_default().contains("missing/+?=&%"));
            }
            assert_eq!(parts.len(), 0, "{case}: no parts persisted");
            assert_eq!(actual_count, 1, "{case}: count");
            assert_eq!(actual_audit.len(), 1, "{case}: audit");
            assert_eq!(
                new_polls > 0,
                matches!(case, "wrong-md5" | "short-body" | "source-error"),
                "{case}: polls={new_polls}"
            );
        }
    });
}

#[test]
#[serial_test::serial]
fn upload_part_client_preserves_stored_bytes_and_bound_context() {
    crate::app::gating_test_env::run_large_stack_test("protocol-upload-part-bound", || async {
        use http_body_util::BodyExt;
        let bucket = "protocol-upload-part-bound";
        let (_foreign_temp, root, foreign_client, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let foreign = foreign_client.server_ctx.app_context().expect("foreign context");
        let (_temp, _, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let bound = backend.server_ctx.app_context().expect("bound context");
        assert!(Arc::ptr_eq(&crate::runtime_sources::current_app_context().expect("ambient"), &foreign));
        let created = backend
            .create_multipart_upload(
                dto::CreateMultipartUploadInput {
                    bucket: crate::app::dto_values::stored_bucket_name(bucket).expect("bucket"),
                    key: crate::app::dto_values::stored_object_key("directory/a ?#+%.txt").expect("key"),
                    ..Default::default()
                },
                &root,
            )
            .await
            .expect("create upload");
        for part_num in [1, 2] {
            let (mut request, polls) = input(bucket, part_num, 3);
            request.checksum_spec =
                Some(rustfs_gateway_types::ChecksumSpec::parse_header("x-amz-checksum-crc32", "NSRBwg==").expect("checksum"));
            let before = count();
            let output = backend
                .upload_part(MultipartRequest::upload_part(request, &created.upload_id), &root)
                .await
                .expect("upload part");
            assert_eq!(count() - before, 1);
            assert!(polls.load(Ordering::Relaxed) > 0, "successful upload must poll its producer");
            let _ = &output.e_tag;
        }
        let options = StorageObjectOptions::default();
        let parts = bound
            .object_store()
            .list_object_parts(bucket, "directory/a ?#+%.txt", &created.upload_id, None, 100, &options)
            .await
            .expect("bound parts")
            .parts;
        assert_eq!(
            parts
                .iter()
                .map(|part| (part.part_num, part.size, part.etag.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (1, 3, Some("900150983cd24fb0d6963f7d28e17f72")),
                (2, 3, Some("900150983cd24fb0d6963f7d28e17f72"))
            ]
        );
        assert!(
            foreign
                .object_store()
                .get_multipart_info(bucket, "directory/a ?#+%.txt", &created.upload_id, &options)
                .await
                .is_err()
        );
        let complete = dto::CompleteMultipartUploadInput {
            bucket: created.bucket,
            key: created.key,
            multipart_upload: dto::CompletedMultipartUpload {
                parts: vec![dto::CompletedPart {
                    part_number: 2,
                    e_tag: Some(rustfs_gateway_types::ETag::new("900150983cd24fb0d6963f7d28e17f72").expect("tag")),
                    ..Default::default()
                }],
            },
            ..Default::default()
        };
        backend
            .complete_multipart_upload(MultipartRequest::complete(complete, &created.upload_id), &root)
            .await
            .expect("complete");
        let read = backend
            .get_object(bucket, "directory/a ?#+%.txt", &root, None)
            .await
            .expect("read");
        assert_eq!(
            read.body
                .expect("body")
                .into_body()
                .collect()
                .await
                .expect("bytes")
                .to_bytes()
                .as_ref(),
            b"abc"
        );
    });
}

#[test]
#[serial_test::serial]
fn upload_part_checksum_observations_match_original() {
    crate::app::gating_test_env::run_large_stack_test("protocol-part-checksum", || async {
        let bucket = "protocol-part-checksum";
        let (_temp, root, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let created = backend
            .create_multipart_upload(
                dto::CreateMultipartUploadInput {
                    bucket: crate::app::dto_values::stored_bucket_name(bucket).expect("bucket"),
                    key: crate::app::dto_values::stored_object_key("directory/a ?#+%.txt").expect("key"),
                    ..Default::default()
                },
                &root,
            )
            .await
            .expect("create upload");
        let (mut request, _) = input(bucket, 1, 3);
        request.checksum_spec =
            Some(rustfs_gateway_types::ChecksumSpec::parse_header("x-amz-checksum-crc32", "AAAAAA==").expect("checksum"));
        let result = backend
            .upload_part(MultipartRequest::upload_part(request, &created.upload_id), &root)
            .await;
        assert!(result.is_ok(), "checksum projection remains representable");
    });
}

fn input_fields(value: crate::app::multipart_usecase::UploadPartWriteInput) -> serde_json::Value {
    let crate::app::multipart_usecase::UploadPartTarget::Legacy { bucket, key, upload_id } = value.target else {
        panic!("protocol client must retain its legacy upload target");
    };
    serde_json::json!({
        "bucket": bucket, "key": key, "upload_id": upload_id, "number": value.part_number,
        "length": value.content_length, "body": value.body.is_some(), "trailers": value.trailing_headers.is_some(),
        "frame_trailers": value.frame_trailers.is_some(), "md5": value.content_md5,
        "algorithm": value.checksum_algorithm, "crc32": value.checksum_crc32, "crc32c": value.checksum_crc32c,
        "crc64": value.checksum_crc64nvme, "sha1": value.checksum_sha1, "sha256": value.checksum_sha256,
        "sse_algorithm": value.sse_customer_algorithm, "sse_key": value.sse_customer_key, "sse_md5": value.sse_customer_key_md5,
    })
}

#[test]
fn upload_part_input_projection_matches_original_fields() {
    use rustfs_gateway_types::ChecksumAlgorithm as Algorithm;

    for value in ["", "value"] {
        for length in [-1, 0, 3] {
            for &algorithm in Algorithm::ALL {
                let native = dto::UploadPartInput {
                    bucket: crate::app::dto_values::stored_bucket_name("sftp_host_keys").expect("stored bucket"),
                    key: crate::app::dto_values::stored_object_key("directory/a ?#+%.txt").expect("stored key"),
                    upload_id: rustfs_gateway_types::UploadIdClaim::from_wire("upload/+?=&%"),
                    part_number: 10000,
                    content_length: length,
                    checksum_spec: Some(
                        rustfs_gateway_types::ChecksumSpec::from_digest(algorithm, &vec![0; algorithm.digest_len()])
                            .expect("checksum"),
                    ),
                    checksum_algorithm: Some(dto::ChecksumAlgorithm::custom(value)),
                    content_md5: Some(value.into()),
                    sse_customer_algorithm: Some(value.into()),
                    sse_customer_key: Some(rustfs_gateway_types::SseCustomerKey::new(value.into())),
                    sse_customer_key_md5: Some(value.into()),
                    ..Default::default()
                };
                let mut legacy = LegacyInput::builder()
                    .bucket("sftp_host_keys".to_owned())
                    .key("directory/a ?#+%.txt".to_owned())
                    .upload_id("upload/+?=&%".to_owned())
                    .part_number(10000)
                    .build()
                    .expect("legacy input");
                legacy.content_length = Some(length);
                let nonempty = (!value.is_empty()).then(|| value.to_owned());
                legacy.content_md5 = nonempty.clone();
                legacy.checksum_algorithm = nonempty.clone().map(Into::into);
                legacy.sse_customer_algorithm = nonempty.clone();
                legacy.sse_customer_key = Some(value.to_owned());
                legacy.sse_customer_key_md5 = nonempty;
                let checksum = match algorithm {
                    Algorithm::Crc32 => &mut legacy.checksum_crc32,
                    Algorithm::Crc32c => &mut legacy.checksum_crc32c,
                    Algorithm::Crc64Nvme => &mut legacy.checksum_crc64nvme,
                    Algorithm::Sha1 => &mut legacy.checksum_sha1,
                    Algorithm::Sha256 => &mut legacy.checksum_sha256,
                    Algorithm::Sha512 => &mut legacy.checksum_sha512,
                    Algorithm::Md5 => &mut legacy.checksum_md5,
                    Algorithm::XxHash64 => &mut legacy.checksum_xxhash64,
                    Algorithm::XxHash3 => &mut legacy.checksum_xxhash3,
                    Algorithm::XxHash128 => &mut legacy.checksum_xxhash128,
                    other => panic!("missing legacy test vector for {other:?}"),
                };
                *checksum = Some(base64_simd::STANDARD.encode_to_string(vec![0; algorithm.digest_len()]));
                let expected = crate::app::multipart_usecase::UploadPartWriteInput::from_legacy(legacy, None);
                assert_eq!(
                    input_fields(upload_part_input(native, "upload/+?=&%".into())),
                    input_fields(expected),
                    "{algorithm:?}, length {length}, value {value:?}"
                );
            }
        }
    }
}

#[test]
fn upload_part_output_projection_matches_original_fields_and_refusals() {
    use crate::app::multipart_usecase::UploadPartWriteOutput;
    // The refusal order and messages were measured against s3s 5761ddfe and
    // the gateway-types 1c7486f conversion. The SDK fields below remain an independent oracle.
    for (raw, tag) in [
        (None, None),
        (Some("abc"), Some(LegacyETag::Strong("abc".into()))),
        (Some("\"abc\""), Some(LegacyETag::Strong("abc".into()))),
        (Some("W/\"abc\""), Some(LegacyETag::Weak("abc".into()))),
        (Some(""), Some(LegacyETag::Strong("".into()))),
        (Some("\"\""), Some(LegacyETag::Strong("".into()))),
        (Some("a\nb"), Some(LegacyETag::Strong("a\nb".into()))),
        (Some("a\"b"), Some(LegacyETag::Strong("a\"b".into()))),
    ] {
        for (index, checksum) in [
            (0, None),
            (0, Some("AAAAAA==")),
            (1, Some("AAAAAA==")),
            (2, Some("AAAAAAAAAAA=")),
            (3, Some("AAAAAAAAAAAAAAAAAAAAAAAAAAA=")),
            (4, Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")),
            (0, Some("bad")),
            (5, Some("AAAAAA==")),
        ] {
            let field = |slot| {
                checksum
                    .filter(|_| index == slot || (index == 5 && slot < 2))
                    .map(str::to_owned)
            };
            let current = UploadPartWriteOutput {
                e_tag: raw.map(str::to_owned),
                checksum_crc32: field(0),
                checksum_crc32c: field(1),
                checksum_crc64nvme: field(2),
                checksum_sha1: field(3),
                checksum_sha256: field(4),
                server_side_encryption: Some("AES256".into()),
                ssekms_key_id: Some("test-key-id".into()),
                sse_customer_algorithm: Some("AES256".into()),
                sse_customer_key_md5: Some("test-md5".into()),
                headers: HeaderMap::new(),
            };
            let old = LegacyOutput {
                e_tag: tag.clone(),
                checksum_crc32: field(0),
                checksum_crc32c: field(1),
                checksum_crc64nvme: field(2),
                checksum_sha1: field(3),
                checksum_sha256: field(4),
                server_side_encryption: Some(LegacyEncryption::from("AES256".to_owned())),
                ssekms_key_id: Some("test-key-id".into()),
                sse_customer_algorithm: Some("AES256".into()),
                sse_customer_key_md5: Some("test-md5".into()),
                ..Default::default()
            };
            let tag_value = old.e_tag.as_ref().map(|tag| match tag {
                LegacyETag::Strong(value) => (false, value.as_str()),
                LegacyETag::Weak(value) => (true, value.as_str()),
            });
            let refusal = match (tag_value, index, checksum) {
                (None, _, _) => Some("e_tag: the gateway shape requires this member"),
                (Some((_, "" | "a\nb")), _, _) => Some("e_tag: not an entity tag the gateway can write"),
                (_, 5, _) => Some("checksum_spec: the gateway shape carries one checksum, so a second would be lost"),
                (_, 0, Some("bad")) => Some("checksum_crc32: not a checksum value of this algorithm's width"),
                _ => None,
            };
            let native = upload_part_output(current);
            if let Some(reason) = refusal {
                assert_eq!(
                    error_facts(native.expect_err("original conversion refused this output")),
                    (
                        "InternalError".to_owned(),
                        Some(format!(
                            "failed to convert UploadPart output through the gateway compatibility seam: {reason}"
                        )),
                        Some(http::StatusCode::INTERNAL_SERVER_ERROR),
                        None
                    ),
                    "{raw:?}, {index}, {checksum:?}"
                );
                continue;
            }
            let actual = native.expect("representable legacy output");
            let (weak, value) = tag_value.expect("valid ETag");
            let expected_tag = if weak {
                rustfs_gateway_types::ETag::new_weak(value.to_owned())
            } else {
                rustfs_gateway_types::ETag::new(value.to_owned())
            }
            .expect("recorded valid ETag");
            assert_eq!(format!("{:?}", actual.e_tag), format!("{expected_tag:?}"));
            let expected_checksum = [
                ("CRC32", old.checksum_crc32),
                ("CRC32C", old.checksum_crc32c),
                ("CRC64NVME", old.checksum_crc64nvme),
                ("SHA1", old.checksum_sha1),
                ("SHA256", old.checksum_sha256),
            ]
            .into_iter()
            .find_map(|(name, value)| value.map(|value| (name, value)));
            assert_eq!(
                actual
                    .checksum_spec
                    .map(|spec| (spec.algorithm().wire_name(), spec.render_base64().to_owned())),
                expected_checksum
            );
            assert_eq!(
                actual.server_side_encryption.as_ref().map(|value| value.as_str()),
                old.server_side_encryption.as_ref().map(|value| value.as_str())
            );
            assert_eq!(actual.ssekms_key_id, old.ssekms_key_id);
            assert_eq!(actual.sse_customer_algorithm, old.sse_customer_algorithm);
            assert_eq!(actual.sse_customer_key_md5, old.sse_customer_key_md5);
            assert_eq!(actual.bucket_key_enabled, old.bucket_key_enabled);
            assert_eq!(
                actual.request_charged.as_ref().map(|value| value.as_str()),
                old.request_charged.as_ref().map(|value| value.as_str())
            );
        }
    }
}

#[test]
fn upload_part_error_projection_matches_original_defaults() {
    for (code, legacy_code) in [
        (S3ErrorCode::IncompleteBody, LegacyS3ErrorCode::IncompleteBody),
        (S3ErrorCode::UnexpectedContent, LegacyS3ErrorCode::UnexpectedContent),
        (S3ErrorCode::EntityTooLarge, LegacyS3ErrorCode::EntityTooLarge),
    ] {
        for message in [None, Some("explicit failure")] {
            let actual = match message {
                Some(message) => S3Error::with_message(code.clone(), message),
                None => S3Error::new(code.clone()),
            };
            let expected = match message {
                Some(message) => LegacyS3Error::with_message(legacy_code.clone(), message),
                None => LegacyS3Error::new(legacy_code.clone()),
            };
            assert_eq!(
                error_facts(upload_part_error(actual)),
                error_facts(crate::error::owned_error_for_test(expected))
            );
        }
    }
}

fn audit_facts(probe: &crate::app::storage_api::helper::MutationCompletionProbe) -> Vec<serde_json::Value> {
    probe
        .audits
        .lock()
        .expect("audits")
        .drain(..)
        .map(|entry| {
            serde_json::json!({
                "name": entry.api.name, "bucket": entry.api.bucket, "object": entry.api.object,
                "status": entry.api.status, "code": entry.api.status_code, "request_headers": entry.req_header,
                "response_headers": entry.resp_header, "query": entry.req_query, "path": entry.req_path,
                "access_key": entry.access_key,
            })
        })
        .collect()
}
