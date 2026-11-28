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

use super::super::RequestParams;
use super::*;
use http::{HeaderMap, Method};
use rustfs_protocols::common::client::s3::{MultipartRequest, StorageBackend};
use rustfs_s3_types::{S3Error, S3ErrorCode};

fn create_input() -> dto::CreateMultipartUploadInput {
    dto::CreateMultipartUploadInput {
        bucket: rustfs_gateway_types::BucketName::new("multipart-fields").expect("bucket"),
        key: rustfs_gateway_types::ObjectKey::new("a ?#+%.txt").expect("key"),
        ..Default::default()
    }
}

#[test]
fn multipart_create_projection_matches_original_fields_and_times() {
    for empty in [false, true] {
        for (seconds, expected_nanos) in [(1_800_000_000, 1_800_000_000_123_456_789_i128), (-1, -876_543_211_i128)] {
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
            let text = |value: &str| (!empty).then(|| value.to_owned());
            let expected = crate::app::multipart_usecase::CreateMultipartUploadFields {
                bucket: "multipart-fields".into(),
                key: "a ?#+%.txt".into(),
                metadata: Some([("label".into(), "value".into())].into()),
                tagging: text("label=value"),
                storage_class: text("STANDARD"),
                server_side_encryption: None,
                sse_customer_algorithm: text("AES256"),
                sse_customer_key: None,
                sse_customer_key_md5: text("digest"),
                ssekms_key_id: text("key-label"),
                object_lock_legal_hold_status: text("ON"),
                object_lock_mode: text("GOVERNANCE"),
                object_lock_retain_until_date: Some(
                    time::OffsetDateTime::from_unix_timestamp_nanos(expected_nanos).expect("timestamp"),
                ),
            };
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
        let reason = [
            "object_lock_event_hold: an Object Lock event hold, which no pinned s3s shape holds and RustFS does not store",
            "object_lock_event_hold_duration_days: an Object Lock event hold, which no pinned s3s shape holds and RustFS does not store",
            "object_lock_event_hold_duration_years: an Object Lock event hold, which no pinned s3s shape holds and RustFS does not store",
            "object_lock_retain_until_date: an instant outside what the s3s shape can hold",
        ][field];
        let actual = create_multipart_input(make()).err().expect("owned refusal");
        assert_eq!(
            error_facts(actual),
            (
                "InternalError".into(),
                Some(format!(
                    "failed to convert CreateMultipartUpload input through the gateway compatibility seam: {reason}"
                )),
                Some(http::StatusCode::INTERNAL_SERVER_ERROR),
                None
            ),
            "field {field}"
        );
    }
}

fn complete_input() -> dto::CompleteMultipartUploadInput {
    dto::CompleteMultipartUploadInput {
        bucket: rustfs_gateway_types::BucketName::new("multipart-fields").expect("bucket"),
        key: rustfs_gateway_types::ObjectKey::new("key").expect("key"),
        ..Default::default()
    }
}

#[test]
fn multipart_complete_projection_matches_original_parts_and_conditions() {
    for (condition, expected) in [
        (None, Ok(None)),
        (Some(""), Ok(None)),
        (Some("*"), Ok(Some("*"))),
        (Some("abc-1"), Ok(Some("\"abc-1\""))),
        (Some("\"abc\""), Ok(Some("\"abc\""))),
        (Some("W/\"abc\""), Ok(Some("W/\"abc\""))),
        (Some("bad tag"), Err(())),
        (Some("\"one\", \"two\""), Ok(Some("\"one\", \"two\""))),
        (Some("W/\"one\", W/\"two\""), Ok(Some("W/\"one\", W/\"two\""))),
    ] {
        let make = || {
            let mut input = complete_input();
            input.upload_id = rustfs_gateway_types::UploadIdClaim::from_wire("missing-upload");
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
        let actual = complete_multipart_input(make());
        match expected {
            Ok(parsed) => {
                let actual = actual.expect("accepted condition");
                let header = parsed.map(str::to_owned);
                assert_eq!(actual.if_match, header);
                assert_eq!(actual.if_none_match, header);
                assert!(actual.sse_customer_algorithm.is_none());
                assert!(actual.checksum_spec.is_none());
                let [part] = actual.multipart_upload.parts.as_slice() else { panic!("expected one part") };
                assert_eq!(part.part_number, -1);
                assert_eq!(part.e_tag.as_ref().map(ETag::opaque_tag), Some("a&b"));
                assert_eq!(part.checksum_crc32.as_deref(), Some(""));
            }
            Err(()) => assert_eq!(error_facts(actual.expect_err("rejected condition")), ("InternalError".into(), Some("failed to convert CompleteMultipartUpload input through the gateway compatibility seam: if_match: not an entity-tag condition the s3s input can hold".into()), Some(http::StatusCode::INTERNAL_SERVER_ERROR), None)),
        }
    }
}

#[test]
fn multipart_completion_projection_preserves_original_escaped_tags() {
    for (opaque, weak, expected) in [
        ("abc", false, "abc"),
        ("a&b<c", false, "a&amp;b&lt;c"),
        ("a\"b", true, "a&quot;b"),
    ] {
        let input = dto::CompleteMultipartUploadOutput {
            e_tag: Some(if weak { ETag::new_weak(opaque) } else { ETag::new(opaque) }.expect("tag")),
            ..Default::default()
        };
        let tag = complete_multipart_output(input)
            .expect("completion output")
            .e_tag
            .expect("ETag");
        assert_eq!((tag.opaque_tag(), tag.is_weak()), (expected, weak));
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
            let actual = backend
                .complete_multipart_upload(MultipartRequest::complete(make(), "missing-upload"), &root)
                .await
                .expect_err("owned refusal");
            assert_ne!(actual.code().as_str(), "", "part {part:?}, condition {condition:?}");
        }
        let error = backend
            .abort_multipart_upload(
                MultipartRequest::abort(
                    dto::AbortMultipartUploadInput {
                        bucket: rustfs_gateway_types::BucketName::new(bucket).expect("bucket"),
                        key: rustfs_gateway_types::ObjectKey::new("key").expect("key"),
                        ..Default::default()
                    },
                    "missing-upload",
                ),
                &root,
            )
            .await
            .expect_err("missing upload");
        assert_eq!(error.code(), &S3ErrorCode::NoSuchUpload);
    });
}

#[test]
fn multipart_completion_projection_preserves_header_only_field_behavior() {
    let headers = [
        "x-amz-checksum-md5",
        "x-amz-checksum-sha512",
        "x-amz-checksum-xxhash3",
        "x-amz-checksum-xxhash64",
        "x-amz-checksum-xxhash128",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key-md5",
    ];
    for first_invalid in 0..=headers.len() {
        let valid = ["md5", "sha512", "AAAAAAAAAAA=", "xxhash64", "xxhash128", "AES256", "digest"];
        let value = |index: usize| Some(if index < first_invalid { valid[index] } else { "bad\nvalue" }.to_owned());
        let output = dto::CompleteMultipartUploadOutput {
            checksum_header_md5: value(0),
            checksum_header_sha512: value(1),
            checksum_header_xxhash3: value(2),
            checksum_header_xxhash64: value(3),
            checksum_header_xxhash128: value(4),
            sse_customer_algorithm: value(5),
            sse_customer_key_md5: value(6),
            ..Default::default()
        };
        let result = complete_multipart_output(output);
        if let Some(header) = headers.get(first_invalid) {
            let error = result.expect_err("invalid response header");
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert_eq!(
                error.message(),
                Some(format!("failed to convert through the gateway compatibility seam: {header}: not a header value").as_str())
            );
            assert_eq!(error.status_code(), Some(http::StatusCode::INTERNAL_SERVER_ERROR));
            assert!(error.headers().is_none());
            assert!(std::error::Error::source(&error).is_none());
        } else {
            let output = result.expect("valid response headers");
            assert_eq!(
                [
                    output.checksum_header_md5,
                    output.checksum_header_sha512,
                    output.checksum_header_xxhash3,
                    output.checksum_header_xxhash64,
                    output.checksum_header_xxhash128,
                    output.sse_customer_algorithm,
                    output.sse_customer_key_md5
                ],
                [const { None }; 7]
            );
        }
    }
}

#[test]
fn multipart_envelopes_preserve_original_request_facts() {
    let credentials = rustfs_credentials::Credentials {
        access_key: "multipart-facts".into(),
        secret_key: "test-secret".into(),
        ..Default::default()
    };
    for (method, query, expected_uri) in [
        (Method::POST, vec![("uploads", None)], "/multipart-fields/a%20%3F%23+%25.txt?uploads"),
        (
            Method::POST,
            vec![("uploadId", Some("session/+?=&%"))],
            "/multipart-fields/a%20%3F%23+%25.txt?uploadId=session%2F%2B%3F%3D%26%25",
        ),
        (
            Method::DELETE,
            vec![("uploadId", Some("session/+?=&%"))],
            "/multipart-fields/a%20%3F%23+%25.txt?uploadId=session%2F%2B%3F%3D%26%25",
        ),
    ] {
        let uri = super::super::build_object_uri("multipart-fields", "a ?#+%.txt", &query).expect("URI");
        assert_eq!(uri.to_string(), expected_uri);
        let params = RequestParams {
            bucket: Some("multipart-fields".into()),
            object: Some("a ?#+%.txt".into()),
            credentials: &credentials,
        };
        let envelope = ProtocolStorageClient::new(crate::runtime_sources::ServerContextSlot::new()).create_envelope(
            method.clone(),
            uri.clone(),
            params,
        );
        assert_eq!((envelope.method(), envelope.uri(), envelope.headers().is_empty()), (&method, &uri, true));
        let owned = envelope.req_info().expect("owned facts");
        assert_eq!(
            (
                owned.bucket.as_deref(),
                owned.object.as_deref(),
                owned.version_id.as_deref(),
                owned.is_owner,
                owned.keystone_session_present
            ),
            (Some("multipart-fields"), Some("a ?#+%.txt"), None, false, None)
        );
        assert_eq!(owned.cred.as_ref().map(|value| value.access_key.as_str()), Some("multipart-facts"));
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
        let foreign = foreign_client.server_ctx.app_context().expect("foreign context");
        let (_temp, _, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let bound = backend.server_ctx.app_context().expect("bound context");
        let ambient = crate::runtime_sources::current_app_context().expect("published foreign context");
        assert!(std::sync::Arc::ptr_eq(&ambient, &foreign));
        assert!(!std::sync::Arc::ptr_eq(&ambient, &bound));
        {
            let mut input = create_input();
            input.bucket = rustfs_gateway_types::BucketName::new(bucket).expect("bucket");
            let key = input.key.as_str().to_owned();
            let created = backend
                .create_multipart_upload(input, &root)
                .await
                .expect("owned bound create");
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
                "target lookup succeeded={}, ambient lookup succeeded={}",
                target_lookup.is_ok(),
                ambient_lookup.is_ok()
            );
            let input = dto::AbortMultipartUploadInput {
                bucket: created.bucket,
                key: created.key,
                ..Default::default()
            };
            backend
                .abort_multipart_upload(MultipartRequest::abort(input, &created.upload_id), &root)
                .await
                .expect("owned bound abort");
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
                response_headers: http::HeaderMap::new(),
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
            assert_eq!(
                snapshot(actual.expect("stored names are representable")),
                (
                    name.to_owned(),
                    "key".to_owned(),
                    value.to_owned(),
                    Some(value.to_owned()),
                    Some(value.to_owned()),
                    Some(value.to_owned()),
                )
            );
        }
    }
}

#[path = "upload_part_test.rs"]
mod upload_part_tests;
