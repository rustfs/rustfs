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

//! Protocol multipart compatibility probes against the original conversion and storage edge.
//! These tests do not define storage or authorization rules.

use super::super::{RequestParams, S3, gateway_s3s_compat as compat};
use super::*;
use http::{HeaderMap, Method};
use rustfs_gateway_types::compat::s3s_5761ddfe::leaf::RequestWire;
use rustfs_protocols::common::client::s3::StorageBackend;
use rustfs_s3_types::{S3Error, S3ErrorCode};

fn create_input() -> dto::CreateMultipartUploadInput {
    dto::CreateMultipartUploadInput {
        bucket: rustfs_gateway_types::BucketName::new("multipart-fields").expect("bucket"),
        key: rustfs_gateway_types::ObjectKey::new("a ?#+%.txt").expect("key"),
        ..Default::default()
    }
}

fn original_create(
    input: dto::CreateMultipartUploadInput,
) -> S3Result<crate::app::multipart_usecase::CreateMultipartUploadFields> {
    let uri = super::super::build_object_uri(input.bucket.as_str(), input.key.as_str(), &[("uploads", None)])?;
    let context = super::super::gateway_request_context(Method::POST, &uri, HeaderMap::new());
    let input = compat::generated::ops::create_multipart_upload::input_to_s3s(input, &RequestWire::of(&context))
        .map_err(|error| super::super::conversion_error("CreateMultipartUpload input", error))?;
    Ok(crate::app::multipart_usecase::CreateMultipartUploadFields::from_legacy(input))
}

#[test]
fn multipart_create_projection_matches_original_fields_and_times() {
    for empty in [false, true] {
        for seconds in [1_800_000_000, -1] {
            let make = || {
                let mut input = create_input();
                input.tagging = Some(if empty { "" } else { "label=value" }.into());
                input.storage_class = Some(dto::StorageClass::custom(if empty { "" } else { "STANDARD" }));
                input.object_lock_mode = Some(dto::ObjectLockMode::custom(if empty { "" } else { "GOVERNANCE" }));
                input.object_lock_legal_hold_status = Some(dto::ObjectLockLegalHoldStatus::custom(if empty { "" } else { "ON" }));
                input.sse_customer_algorithm = Some(if empty { "" } else { "AES256" }.into());
                input.ssekms_key_id = Some(if empty { "" } else { "key-label" }.into());
                input.sse_customer_key_md5 = Some(if empty { "" } else { "digest" }.into());
                input.metadata.insert("label".into(), "value".into());
                input.object_lock_retain_until_date = Some(Timestamp::from_secs_nanos(seconds, 123_456_789).expect("timestamp"));
                input
            };
            let expected = original_create(make()).expect("original input");
            let actual = create_multipart_input(make()).expect("owned input");
            assert_eq!(
                (actual.bucket, actual.key, actual.metadata, actual.tagging, actual.storage_class),
                (expected.bucket, expected.key, expected.metadata, expected.tagging, expected.storage_class)
            );
            assert_eq!(
                (
                    actual.server_side_encryption,
                    actual.sse_customer_algorithm,
                    actual.sse_customer_key_md5,
                    actual.ssekms_key_id
                ),
                (
                    expected.server_side_encryption,
                    expected.sse_customer_algorithm,
                    expected.sse_customer_key_md5,
                    expected.ssekms_key_id
                )
            );
            assert_eq!(
                (
                    actual.object_lock_mode,
                    actual.object_lock_legal_hold_status,
                    actual.object_lock_retain_until_date
                ),
                (
                    expected.object_lock_mode,
                    expected.object_lock_legal_hold_status,
                    expected.object_lock_retain_until_date
                )
            );
        }
    }
}

fn error_facts(error: S3Error) -> (String, Option<String>, Option<http::StatusCode>, Option<HeaderMap>) {
    (
        error.code().as_str().to_owned(),
        error.message().map(str::to_owned),
        error.status_code(),
        error.headers().cloned(),
    )
}

#[test]
fn multipart_create_projection_preserves_refusals() {
    for field in 0..4 {
        let make = || {
            let mut input = create_input();
            match field {
                0 => input.object_lock_event_hold = Some(dto::ObjectLockEventHold::custom("ON")),
                1 => input.object_lock_event_hold_duration_days = Some(0),
                2 => input.object_lock_event_hold_duration_years = Some(-1),
                _ => input.object_lock_retain_until_date = Some(Timestamp::from_secs(i64::MAX)),
            }
            input
        };
        let expected = original_create(make()).err().expect("original refusal");
        let actual = create_multipart_input(make()).err().expect("owned refusal");
        assert_eq!(error_facts(actual), error_facts(expected), "field {field}");
    }
}

fn complete_input() -> dto::CompleteMultipartUploadInput {
    dto::CompleteMultipartUploadInput {
        bucket: rustfs_gateway_types::BucketName::new("multipart-fields").expect("bucket"),
        key: rustfs_gateway_types::ObjectKey::new("key").expect("key"),
        upload_id: rustfs_gateway_types::UploadIdClaim::from_wire("missing-upload"),
        ..Default::default()
    }
}

#[test]
fn multipart_complete_projection_matches_original_parts_and_conditions() {
    for condition in [
        None,
        Some(""),
        Some("*"),
        Some("abc-1"),
        Some("\"abc\""),
        Some("W/\"abc\""),
        Some("bad tag"),
        Some("\"one\", \"two\""),
        Some("W/\"one\", W/\"two\""),
    ] {
        let make = || {
            let mut input = complete_input();
            input.if_match = condition.map(str::to_owned);
            input.if_none_match = condition.map(str::to_owned);
            input.sse_customer_algorithm = Some(String::new());
            input.multipart_upload.parts.push(dto::CompletedPart {
                part_number: -1,
                e_tag: Some(ETag::new_weak("a&b").expect("weak tag")),
                checksum_crc32: Some(String::new()),
                ..Default::default()
            });
            input
        };
        let expected = compat::generated::ops::complete_multipart_upload::input_to_s3s(make())
            .map_err(|error| super::super::conversion_error("CompleteMultipartUpload input", error))
            .and_then(|input| {
                compat::generated::ops::complete_multipart_upload::input_from_s3s(input)
                    .map_err(crate::app::dto_values::seam_error)
            });
        let actual = complete_multipart_input(make());
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(
                    (actual.if_match, actual.if_none_match, actual.sse_customer_algorithm),
                    (expected.if_match, expected.if_none_match, expected.sse_customer_algorithm)
                );
                let snapshot = |parts: dto::CompletedMultipartUpload| {
                    parts
                        .parts
                        .into_iter()
                        .map(|part| (part.part_number, part.e_tag.map(|tag| tag.opaque_tag().to_owned()), part.checksum_crc32))
                        .collect::<Vec<_>>()
                };
                assert_eq!(snapshot(actual.multipart_upload), snapshot(expected.multipart_upload));
                assert!(actual.checksum_spec.is_none());
            }
            (Err(actual), Err(expected)) => assert_eq!(error_facts(actual), error_facts(expected)),
            _ => panic!("owned and original conversion outcomes differ"),
        }
    }
}

#[test]
fn multipart_completion_projection_preserves_original_escaped_tags() {
    for (opaque, weak) in [("abc", false), ("a&b<c", false), ("a\"b", true)] {
        let input = dto::CompleteMultipartUploadOutput {
            e_tag: Some(if weak { ETag::new_weak(opaque) } else { ETag::new(opaque) }.expect("tag")),
            ..Default::default()
        };
        let mut legacy =
            compat::generated::ops::complete_multipart_upload::output_to_s3s(input.clone()).expect("original output");
        let stored = if weak { format!("W/\"{opaque}\"") } else { opaque.to_owned() };
        legacy.e_tag = Some(crate::app::storage_api::object_utils::to_s3s_xml_etag(&stored));
        let expected = compat::generated::ops::complete_multipart_upload::output_from_s3s(legacy).expect("original answer");
        let actual = complete_multipart_output(input).expect("owned answer");
        let tag = |value: Option<ETag>| value.map(|value| (value.opaque_tag().to_owned(), value.is_weak()));
        assert_eq!(tag(actual.e_tag), tag(expected.e_tag));
    }
}

#[test]
#[serial_test::serial]
fn multipart_refusals_match_original_client_errors() {
    crate::app::gating_test_env::run_large_stack_test("protocol-multipart-original-errors", || async {
        let bucket = "multipart-original-errors";
        let (_temp, root, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        for (part, condition) in [
            (None, None),
            (Some(0), None),
            (Some(-1), None),
            (Some(10001), None),
            (Some(1), None),
            (Some(1), Some("bad tag")),
            (Some(1), Some("missing")),
        ] {
            let make = || {
                let mut input = complete_input();
                input.bucket = rustfs_gateway_types::BucketName::new(bucket).expect("bucket");
                input.key = rustfs_gateway_types::ObjectKey::new("directory/a ?#+%.txt").expect("key");
                input.if_match = condition.map(str::to_owned);
                if let Some(part_number) = part {
                    input.multipart_upload.parts.push(dto::CompletedPart {
                        part_number,
                        ..Default::default()
                    });
                }
                input
            };
            let expected = match compat::generated::ops::complete_multipart_upload::input_to_s3s(make()) {
                Err(error) => super::super::conversion_error("CompleteMultipartUpload input", error),
                Ok(legacy) => {
                    let uri =
                        super::super::build_object_uri(bucket, "directory/a ?#+%.txt", &[("uploadId", Some("missing-upload"))])
                            .expect("URI");
                    let request = ProtocolStorageClient::create_request(
                        legacy,
                        Method::POST,
                        uri,
                        RequestParams {
                            bucket: Some(bucket.into()),
                            object: Some("directory/a ?#+%.txt".into()),
                            credentials: &root,
                        },
                    )
                    .expect("request");
                    backend
                        .fs
                        .complete_multipart_upload(request)
                        .await
                        .err()
                        .map(S3Error::from)
                        .expect("original refusal")
                }
            };
            let actual = backend
                .complete_multipart_upload(make(), &root)
                .await
                .expect_err("owned refusal");
            assert_eq!(error_facts(actual), error_facts(expected), "part {part:?}, condition {condition:?}");
        }
        let error = backend
            .abort_multipart_upload(
                dto::AbortMultipartUploadInput {
                    bucket: rustfs_gateway_types::BucketName::new(bucket).expect("bucket"),
                    key: rustfs_gateway_types::ObjectKey::new("key").expect("key"),
                    upload_id: rustfs_gateway_types::UploadIdClaim::from_wire("missing-upload"),
                    ..Default::default()
                },
                &root,
            )
            .await
            .expect_err("missing upload");
        assert_eq!(error.code(), &S3ErrorCode::NoSuchUpload);
    });
}

#[test]
fn multipart_completion_projection_preserves_header_only_field_behavior() {
    let output = dto::CompleteMultipartUploadOutput {
        checksum_header_xxhash3: Some("AAAAAAAAAAA=".into()),
        sse_customer_algorithm: Some("AES256".into()),
        sse_customer_key_md5: Some("digest".into()),
        ..Default::default()
    };
    let output = complete_multipart_output(output).expect("header fields remain valid");
    assert!(output.checksum_header_xxhash3.is_none());
    assert!(output.sse_customer_algorithm.is_none());
    assert!(output.sse_customer_key_md5.is_none());
    let error = complete_multipart_output(dto::CompleteMultipartUploadOutput {
        sse_customer_algorithm: Some("bad\nvalue".into()),
        ..Default::default()
    })
    .expect_err("invalid original response header");
    assert_eq!(error.code(), &S3ErrorCode::InternalError);
    assert!(
        error
            .message()
            .expect("message")
            .contains("x-amz-server-side-encryption-customer-algorithm: not a header value")
    );
}

#[test]
fn multipart_envelopes_preserve_original_request_facts() {
    let credentials = rustfs_credentials::Credentials {
        access_key: "multipart-facts".into(),
        secret_key: "test-secret".into(),
        ..Default::default()
    };
    for (method, query) in [
        (Method::POST, vec![("uploads", None)]),
        (Method::POST, vec![("uploadId", Some("session/+?=&%"))]),
        (Method::DELETE, vec![("uploadId", Some("session/+?=&%"))]),
    ] {
        let uri = super::super::build_object_uri("multipart-fields", "a ?#+%.txt", &query).expect("URI");
        let params = || RequestParams {
            bucket: Some("multipart-fields".into()),
            object: Some("a ?#+%.txt".into()),
            credentials: &credentials,
        };
        let request = ProtocolStorageClient::create_request((), method.clone(), uri.clone(), params()).expect("original request");
        let envelope = ProtocolStorageClient::create_envelope(method, uri, params());
        assert_eq!(
            (envelope.method(), envelope.uri(), envelope.headers()),
            (&request.method, &request.uri, &request.headers)
        );
        let original = request.extensions.get::<super::super::ReqInfo>().expect("original facts");
        let owned = envelope.req_info().expect("owned facts");
        assert_eq!(
            (
                &owned.bucket,
                &owned.object,
                &owned.version_id,
                owned.is_owner,
                owned.keystone_session_present
            ),
            (
                &original.bucket,
                &original.object,
                &original.version_id,
                original.is_owner,
                original.keystone_session_present
            )
        );
        assert_eq!(
            owned.cred.as_ref().map(|value| &value.access_key),
            original.cred.as_ref().map(|value| &value.access_key)
        );
        assert!(envelope.server_context().is_none());
    }
}

#[test]
#[serial_test::serial]
fn multipart_uses_bound_store_with_a_foreign_ambient_context() {
    crate::app::gating_test_env::run_large_stack_test("protocol-multipart-bound-store", || async {
        use crate::app::storage_api::test::contract::multipart::MultipartOperations;
        use crate::storage_api::protocols::client::StorageObjectOptions;
        let bucket = "multipart-bound-store";
        let (_foreign_temp, root, foreign_client, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let foreign = foreign_client.fs.server_ctx().app_context().expect("foreign context");
        let (_temp, _, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let bound = backend.fs.server_ctx().app_context().expect("bound context");
        let ambient = crate::runtime_sources::current_app_context().expect("published foreign context");
        assert!(std::sync::Arc::ptr_eq(&ambient, &foreign));
        assert!(!std::sync::Arc::ptr_eq(&ambient, &bound));
        for legacy in [true, false] {
            let mut input = create_input();
            input.bucket = rustfs_gateway_types::BucketName::new(bucket).expect("bucket");
            let key = input.key.as_str().to_owned();
            let params = || RequestParams {
                bucket: Some(bucket.to_owned()),
                object: Some(key.clone()),
                credentials: &root,
            };
            let created = if legacy {
                let uri = super::super::build_object_uri(bucket, &key, &[("uploads", None)]).expect("URI");
                let context = super::super::gateway_request_context(Method::POST, &uri, HeaderMap::new());
                let input = compat::generated::ops::create_multipart_upload::input_to_s3s(input, &RequestWire::of(&context))
                    .expect("original input");
                let request = ProtocolStorageClient::create_request(input, Method::POST, uri, params()).expect("request");
                let output = backend
                    .fs
                    .create_multipart_upload(request)
                    .await
                    .expect("original bound create")
                    .output;
                compat::generated::ops::create_multipart_upload::output_from_s3s(output).expect("original output")
            } else {
                backend
                    .create_multipart_upload(input, &root)
                    .await
                    .expect("owned bound create")
            };
            let options = StorageObjectOptions::default();
            let target_lookup = bound
                .object_store()
                .get_multipart_info(bucket, &key, &created.upload_id, &options)
                .await;
            let ambient_lookup = foreign
                .object_store()
                .get_multipart_info(bucket, &key, &created.upload_id, &options)
                .await;
            assert!(
                target_lookup.is_ok() && ambient_lookup.is_err(),
                "legacy={legacy}: target lookup succeeded={}, ambient lookup succeeded={}",
                target_lookup.is_ok(),
                ambient_lookup.is_ok()
            );
            let input = dto::AbortMultipartUploadInput {
                bucket: created.bucket,
                key: created.key,
                upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(&created.upload_id),
                ..Default::default()
            };
            if legacy {
                let uri = super::super::build_object_uri(bucket, &key, &[("uploadId", Some(&created.upload_id))]).expect("URI");
                let input = compat::generated::ops::abort_multipart_upload::input_to_s3s(input).expect("original input");
                let request = ProtocolStorageClient::create_request(input, Method::DELETE, uri, params()).expect("request");
                backend
                    .fs
                    .abort_multipart_upload(request)
                    .await
                    .expect("original bound abort");
            } else {
                backend.abort_multipart_upload(input, &root).await.expect("owned bound abort");
            }
            assert!(
                bound
                    .object_store()
                    .get_multipart_info(bucket, &key, &created.upload_id, &options)
                    .await
                    .is_err()
            );
        }
    });
}

#[test]
fn multipart_create_output_matches_original_for_stored_names_and_empty_values() {
    for name in ["multipart-fields", "a-s3alias"] {
        for value in ["", "value"] {
            let legacy = super::super::CreateMultipartUploadOutput {
                bucket: Some(name.into()),
                key: Some("key".into()),
                upload_id: Some(value.into()),
                sse_customer_algorithm: Some(value.into()),
                sse_customer_key_md5: Some(value.into()),
                ssekms_key_id: Some(value.into()),
                ..Default::default()
            };
            let expected = compat::generated::ops::create_multipart_upload::output_from_s3s(legacy)
                .map_err(|error| super::super::conversion_error("CreateMultipartUpload output", error));
            let actual = create_multipart_output(crate::app::multipart_usecase::CreateMultipartUploadResult {
                bucket: name.into(),
                key: "key".into(),
                upload_id: value.into(),
                sse_customer_algorithm: Some(value.into()),
                sse_customer_key_md5: Some(value.into()),
                ssekms_key_id: Some(value.into()),
                server_side_encryption: None,
                checksum_algorithm: None,
                checksum_type: None,
            });
            let snapshot = |output: dto::CreateMultipartUploadOutput| {
                (
                    output.bucket.as_str().to_owned(),
                    output.key.as_str().to_owned(),
                    output.upload_id,
                    output.sse_customer_algorithm,
                    output.sse_customer_key_md5,
                    output.ssekms_key_id,
                )
            };
            assert_eq!(actual.map(snapshot).map_err(error_facts), expected.map(snapshot).map_err(error_facts));
        }
    }
}
