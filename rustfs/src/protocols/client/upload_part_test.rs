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
    ETag as LegacyETag, LegacyS3Error, LegacyS3ErrorCode, S3Request as LegacyRequest, ServerSideEncryption as LegacyEncryption,
    UploadPartOutput as LegacyOutput,
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

fn input(bucket: &str, upload: &str, number: i32, length: i64) -> (dto::UploadPartInput, Arc<AtomicUsize>) {
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
            upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(upload),
            part_number: number,
            content_length: length,
            body: Some(body),
            ..Default::default()
        },
        polls,
    )
}

async fn original(
    backend: &ProtocolStorageClient,
    input: dto::UploadPartInput,
    credentials: &rustfs_credentials::Credentials,
) -> S3Result<dto::UploadPartOutput> {
    let bucket = input.bucket.as_str().to_owned();
    let key = input.key.as_str().to_owned();
    let length = input.content_length;
    let input = compat::generated::ops::upload_part::input_to_s3s(input)
        .map_err(|error| super::super::super::conversion_error("UploadPart input", error))?;
    let number = input.part_number.to_string();
    let uri = super::super::super::build_object_uri(
        &bucket,
        &key,
        &[("partNumber", Some(&number)), ("uploadId", Some(&input.upload_id))],
    )?;
    let mut headers = HeaderMap::new();
    if let Ok(value) = length.to_string().parse() {
        headers.insert("content-length", value);
    }
    let request = ProtocolStorageClient::create_request(
        input,
        Method::PUT,
        uri,
        RequestParams {
            bucket: Some(bucket),
            object: Some(key),
            credentials,
        },
    )?;
    let response = backend.fs.upload_part(LegacyRequest { headers, ..request }).await?;
    compat::generated::ops::upload_part::output_from_s3s(response.output)
        .map_err(|error| super::super::super::conversion_error("UploadPart output", error))
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
        let store = backend.fs.server_ctx().app_context().expect("context").object_store();
        let probe = crate::app::storage_api::helper::MutationCompletionProbe::install(bucket);
        let mut observations = Vec::new();
        let mut controls = Vec::new();
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
                let (mut request, polls) = input(bucket, &created.upload_id, 1, 3);
                match case {
                    "zero" => request.part_number = 0,
                    "negative" => request.part_number = -1,
                    "too-large" => request.part_number = 10001,
                    "missing-upload" => request.upload_id = rustfs_gateway_types::UploadIdClaim::from_wire("missing/+?=&%"),
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
                (request, polls)
            };
            let (request, old_polls) = make();
            let before = count();
            let expected = original(&backend, request, &root).await.expect_err("original refusal");
            let old_count = count() - before;
            let expected_audit = audit_facts(&probe);
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
            let old_polls = old_polls.load(Ordering::Relaxed);
            let new_polls = new_polls.load(Ordering::Relaxed);
            controls.push((case, old_count, expected_audit.len(), old_polls > 0));
            observations.push((
                case,
                (parts.len(), actual.err().map(error_facts), actual_count, new_polls, actual_audit),
                (0, Some(error_facts(expected)), old_count, old_polls, expected_audit),
            ));
        }
        let bad_controls: Vec<_> = controls
            .into_iter()
            .filter(|(case, count, audits, polled)| {
                *count != 1 || *audits != 1 || *polled != matches!(*case, "wrong-md5" | "short-body" | "source-error")
            })
            .collect();
        let differences: Vec<_> = observations
            .into_iter()
            .filter(|(_, actual, expected)| actual != expected)
            .collect();
        assert!(bad_controls.is_empty(), "original request controls: {bad_controls:#?}");
        assert!(differences.is_empty(), "UploadPart refusal differences: {differences:#?}");
    });
}

#[test]
#[serial_test::serial]
fn upload_part_client_preserves_stored_bytes_and_bound_context() {
    crate::app::gating_test_env::run_large_stack_test("protocol-upload-part-bound", || async {
        use http_body_util::BodyExt;
        let bucket = "protocol-upload-part-bound";
        let (_foreign_temp, root, foreign_client, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let foreign = foreign_client.fs.server_ctx().app_context().expect("foreign context");
        let (_temp, _, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let bound = backend.fs.server_ctx().app_context().expect("bound context");
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
        let mut outputs = Vec::new();
        for legacy in [true, false] {
            let (mut request, polls) = input(bucket, &created.upload_id, if legacy { 1 } else { 2 }, 3);
            request.checksum_spec =
                Some(rustfs_gateway_types::ChecksumSpec::parse_header("x-amz-checksum-crc32", "NSRBwg==").expect("checksum"));
            let before = count();
            let result = if legacy {
                original(&backend, request, &root).await
            } else {
                backend.upload_part(request, &root).await
            };
            let output = result.expect("upload part");
            assert_eq!(count() - before, 1);
            assert!(polls.load(Ordering::Relaxed) > 0, "successful upload must poll its producer");
            outputs.push(format!("{output:?}"));
        }
        assert_eq!(outputs[1], outputs[0]);
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
            upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(&created.upload_id),
            multipart_upload: dto::CompletedMultipartUpload {
                parts: vec![dto::CompletedPart {
                    part_number: 2,
                    e_tag: Some(rustfs_gateway_types::ETag::new("900150983cd24fb0d6963f7d28e17f72").expect("tag")),
                    ..Default::default()
                }],
            },
            ..Default::default()
        };
        backend.complete_multipart_upload(complete, &root).await.expect("complete");
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
        let mut outcomes = Vec::new();
        for legacy in [true, false] {
            let (mut request, _) = input(bucket, &created.upload_id, if legacy { 1 } else { 2 }, 3);
            request.checksum_spec =
                Some(rustfs_gateway_types::ChecksumSpec::parse_header("x-amz-checksum-crc32", "AAAAAA==").expect("checksum"));
            let result = if legacy {
                original(&backend, request, &root).await
            } else {
                backend.upload_part(request, &root).await
            };
            outcomes.push(result.map(|output| format!("{output:?}")).map_err(error_facts));
        }
        assert_eq!(outcomes[1], outcomes[0]);
        assert!(outcomes[0].is_ok(), "original checksum projection remains representable");
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
    for value in ["", "value"] {
        for length in [-1, 0, 3] {
            for &algorithm in rustfs_gateway_types::ChecksumAlgorithm::ALL {
                let make = || dto::UploadPartInput {
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
                let legacy = compat::generated::ops::upload_part::input_to_s3s(make()).expect("original input");
                let expected = crate::app::multipart_usecase::UploadPartWriteInput::from_legacy(legacy, None);
                assert_eq!(
                    input_fields(upload_part_input(make(), "upload/+?=&%".into())),
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
    let mut observations = Vec::new();
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
            let mut current = UploadPartWriteOutput {
                e_tag: raw.map(str::to_owned),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_crc64nvme: None,
                checksum_sha1: None,
                checksum_sha256: None,
                server_side_encryption: Some("AES256".into()),
                ssekms_key_id: Some("test-key-id".into()),
                sse_customer_algorithm: Some("AES256".into()),
                sse_customer_key_md5: Some("test-md5".into()),
                headers: HeaderMap::new(),
            };
            let mut old = LegacyOutput {
                e_tag: tag.clone(),
                server_side_encryption: Some(LegacyEncryption::from("AES256".to_owned())),
                ssekms_key_id: Some("test-key-id".into()),
                sse_customer_algorithm: Some("AES256".into()),
                sse_customer_key_md5: Some("test-md5".into()),
                ..Default::default()
            };
            let checksum = checksum.map(str::to_owned);
            match index {
                0 => {
                    current.checksum_crc32 = checksum.clone();
                    old.checksum_crc32 = checksum;
                }
                1 => {
                    current.checksum_crc32c = checksum.clone();
                    old.checksum_crc32c = checksum;
                }
                2 => {
                    current.checksum_crc64nvme = checksum.clone();
                    old.checksum_crc64nvme = checksum;
                }
                3 => {
                    current.checksum_sha1 = checksum.clone();
                    old.checksum_sha1 = checksum;
                }
                4 => {
                    current.checksum_sha256 = checksum.clone();
                    old.checksum_sha256 = checksum;
                }
                _ => {
                    current.checksum_crc32 = checksum.clone();
                    current.checksum_crc32c = checksum.clone();
                    old.checksum_crc32 = checksum.clone();
                    old.checksum_crc32c = checksum;
                }
            }
            let expected = compat::generated::ops::upload_part::output_from_s3s(old)
                .map_err(|error| super::super::super::conversion_error("UploadPart output", error));
            observations.push((
                raw,
                index,
                upload_part_output(current)
                    .map(|output| format!("{output:?}"))
                    .map_err(error_facts),
                expected.map(|output| format!("{output:?}")).map_err(error_facts),
            ));
        }
    }
    let differences: Vec<_> = observations
        .into_iter()
        .filter(|(_, _, actual, expected)| actual != expected)
        .collect();
    assert!(differences.is_empty(), "UploadPart output differences: {differences:#?}");
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
            assert_eq!(error_facts(upload_part_error(actual)), error_facts(expected.into()));
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
