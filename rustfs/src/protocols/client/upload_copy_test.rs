// Copyright 2026 RustFS Team
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

//! UploadPartCopy protocol compatibility against the installed legacy library and FS edge.
//! These probes preserve multipart storage and leave protocol authorization at its existing entry point.

use super::*;
use crate::app::multipart_usecase::{DefaultMultipartUsecase, UploadPartCopyWriteInput, UploadPartCopyWriteOutput};
use crate::app::storage_api::s3::S3Response;
use rustfs_gateway_types::{CopySourceValue, UploadIdClaim, dto};
use rustfs_protocols::common::client::s3::StorageBackend;
use rustfs_s3_types::S3Error;

fn input(bucket: &str, upload: &str) -> dto::UploadPartCopyInput {
    dto::UploadPartCopyInput {
        bucket: rustfs_gateway_types::BucketName::new(bucket).expect("bucket"),
        key: rustfs_gateway_types::ObjectKey::new("copy-target").expect("key"),
        upload_id: UploadIdClaim::from_wire(upload),
        part_number: 1,
        copy_source: CopySourceValue::Bucket {
            bucket: bucket.into(),
            key: "directory/a ?#+%.txt".into(),
            version_id: None,
        },
        copy_source_if_match: None,
        copy_source_if_modified_since: None,
        copy_source_if_none_match: None,
        copy_source_if_unmodified_since: None,
        copy_source_range: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        request_payer: None,
        expected_bucket_owner: None,
        expected_source_bucket_owner: None,
    }
}

fn unsupported_source(outpost: bool) -> CopySourceValue {
    if outpost {
        CopySourceValue::Outpost {
            partition: "aws".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            outpost_id: "op-test".into(),
            key: "source".into(),
            version_id: Some("bad-version".into()),
        }
    } else {
        CopySourceValue::AccessPoint {
            partition: "aws".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            access_point_name: "ap".into(),
            key: "source".into(),
            version_id: Some("bad-version".into()),
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

fn copy_count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == rustfs_s3_ops::S3Operation::UploadPartCopy.as_str())
        .map_or(0, |row| row.total)
}

#[test]
#[serial_test::serial]
fn protocol_upload_copy_original_refusal_order() {
    crate::app::gating_test_env::run_large_stack_test("protocol-upload-copy-order", || async {
        use crate::app::storage_api::test::contract::multipart::MultipartOperations;
        use crate::storage_api::protocols::client::StorageObjectOptions;
        let bucket = "protocol-upload-copy-order";
        let (_temp, root, backend, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let context = backend.server_ctx.app_context().expect("bound context");
        let store = context.object_store();
        let options = StorageObjectOptions::default();
        let probe = crate::app::storage_api::helper::MutationCompletionProbe::install(bucket);
        let audit_operations = || {
            probe
                .audits
                .lock()
                .expect("audits")
                .drain(..)
                .map(|entry| entry.api.name)
                .collect::<Vec<_>>()
        };
        let upload = store
            .new_multipart_upload(bucket, "copy-target", &options)
            .await
            .expect("upload");
        let before = store
            .get_multipart_info(bucket, "copy-target", &upload.upload_id, &options)
            .await
            .expect("metadata");
        for case in 0..13 {
            let make = || {
                let mut request = input(bucket, &upload.upload_id);
                match case {
                    0 => {
                        request.part_number = 0;
                        request.copy_source = CopySourceValue::Bucket {
                            bucket: "Upper".into(),
                            key: "key".into(),
                            version_id: None,
                        };
                    }
                    1 => {
                        request.part_number = 0;
                        request.copy_source_range = Some("bad-range".into());
                        request.copy_source = CopySourceValue::Bucket {
                            bucket: bucket.into(),
                            key: "key".into(),
                            version_id: Some("bad-version".into()),
                        };
                    }
                    2 => {
                        request.part_number = 0;
                        request.copy_source_range = Some("bad-range".into());
                    }
                    3 => request.part_number = 0,
                    4 => {
                        request.upload_id = UploadIdClaim::from_wire("missing");
                        request.copy_source_if_match = Some("missing".into());
                    }
                    5 => {
                        request.copy_source_if_match = Some("missing".into());
                        request.copy_source_range = Some("bytes=100-200".into());
                    }
                    6 => request.copy_source_if_match = Some("W/\"missing\"".into()),
                    7 => request.copy_source_range = Some("bytes=100-200".into()),
                    11 | 12 => {
                        request.part_number = 0;
                        request.copy_source_range = Some("bad-range".into());
                        request.copy_source = unsupported_source(case == 12);
                    }
                    _ => {
                        request.part_number = 0;
                        request.copy_source = CopySourceValue::Bucket {
                            bucket: if case == 10 { "Upper" } else { bucket }.into(),
                            key: if case == 9 {
                                format!("{}a", "é".repeat(512))
                            } else {
                                "a".repeat(1025)
                            },
                            version_id: None,
                        };
                    }
                }
                request
            };
            let expected_count = if matches!(case, 0 | 8..=10) { 0 } else { 1 };
            let before_count = copy_count();
            let actual = backend.upload_part_copy(make(), &root).await.expect_err("client refusal");
            assert_eq!(copy_count() - before_count, expected_count);
            assert_eq!(
                audit_operations(),
                vec![Some(rustfs_s3_ops::S3Operation::UploadPartCopy.as_str().to_owned()); expected_count as usize],
                "audit for case {case}"
            );
            let actual_facts = error_facts(actual);
            let expected = actual_facts.clone();
            let (code, message, status) = match case {
                0 | 10 => (
                    "InternalError",
                    "failed to convert UploadPartCopy copy_source through the gateway compatibility seam: ParseAmzCopySourceError: InvalidBucketName",
                    500,
                ),
                1 => ("InvalidArgument", "Invalid version id specified in copy source", 400),
                2 => ("InvalidArgument", "Invalid range format", 400),
                3 => ("InvalidArgument", "partNumber must be between 1 and 10000", 400),
                4 => (
                    "NoSuchUpload",
                    "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                    404,
                ),
                5 | 6 => ("PreconditionFailed", "At least one of the preconditions you specified did not hold.", 412),
                7 => ("InvalidRange", "The requested range is not satisfiable", 416),
                11 | 12 => (
                    "NotImplemented",
                    "A header you provided implies functionality that is not implemented.",
                    501,
                ),
                _ => (
                    "InternalError",
                    "failed to convert UploadPartCopy copy_source through the gateway compatibility seam: ParseAmzCopySourceError: InvalidKey",
                    500,
                ),
            };
            assert_eq!(
                expected,
                (
                    code.into(),
                    Some(message.into()),
                    Some(http::StatusCode::from_u16(status).expect("status")),
                    None
                ),
                "old-main row {case}"
            );
            assert_eq!(actual_facts, expected, "case {case}");
            let parts = store
                .list_object_parts(bucket, "copy-target", &upload.upload_id, None, 1000, &options)
                .await
                .expect("parts");
            assert!(parts.parts.is_empty(), "case {case} must not persist a part");
            let after = store
                .get_multipart_info(bucket, "copy-target", &upload.upload_id, &options)
                .await
                .expect("metadata after refusal");
            assert_eq!(
                (&after.bucket, &after.object, &after.upload_id, &after.user_defined, after.initiated),
                (&before.bucket, &before.object, &before.upload_id, &before.user_defined, before.initiated)
            );
        }
    });
}

#[test]
#[serial_test::serial]
fn protocol_upload_copy_original_range_and_metadata() {
    crate::app::gating_test_env::run_large_stack_test("protocol-upload-copy-range", || async {
        use crate::app::storage_api::multipart_usecase::contract::multipart::CompletePart;
        use crate::app::storage_api::test::contract::multipart::MultipartOperations;
        use crate::app::storage_api::test::contract::object::ObjectIO;
        use crate::storage_api::protocols::client::{StorageObjectOptions, StoragePutObjReader};
        use tokio::io::AsyncReadExt;
        let bucket = "protocol-upload-copy-range";
        let (_foreign_temp, _, foreign, _) = crate::protocols::mainflow_test::object_client(bucket).await;
        let ambient = foreign.server_ctx.app_context().expect("foreign context");
        let options = StorageObjectOptions::default();
        let foreign_etag = ambient
            .object_store()
            .put_object(
                bucket,
                "directory/a ?#+%.txt",
                &mut StoragePutObjReader::from_vec(b"foreign-data".to_vec()),
                &options,
            )
            .await
            .expect("foreign source")
            .etag
            .expect("foreign tag");
        let (_temp, root, backend, source_etag) = crate::protocols::mainflow_test::object_client(bucket).await;
        let context = backend.server_ctx.app_context().expect("bound context");
        let store = context.object_store();
        let options = StorageObjectOptions::default();
        assert!(!std::sync::Arc::ptr_eq(&context, &ambient));
        assert!(std::sync::Arc::ptr_eq(
            &crate::runtime_sources::current_app_context().expect("ambient"),
            &ambient
        ));
        let fallback = crate::init::protocol_storage_client_for_server(crate::runtime_sources::ServerContextSlot::new());
        for (backend, store, source_etag, expected_bytes) in [
            (&backend, store, source_etag, b"otoco".as_slice()),
            (&fallback, ambient.object_store(), foreign_etag, b"reign".as_slice()),
        ] {
            {
                let upload = store
                    .new_multipart_upload(bucket, "copy-target", &options)
                    .await
                    .expect("upload");
                let before = store
                    .get_multipart_info(bucket, "copy-target", &upload.upload_id, &options)
                    .await
                    .expect("metadata");
                let mut request = input(bucket, &upload.upload_id);
                request.copy_source_range = Some("bytes=2-6".into());
                request.copy_source_if_match = Some(format!("\"{source_etag}\""));
                let before_count = copy_count();
                let output = backend.upload_part_copy(request, &root).await.expect("range copy");
                assert_eq!(copy_count() - before_count, 1);
                let result = output;
                assert!(result.last_modified.is_some());
                let etag = result.e_tag;
                let after = store
                    .get_multipart_info(bucket, "copy-target", &upload.upload_id, &options)
                    .await
                    .expect("metadata after copy");
                assert_eq!(
                    (&after.bucket, &after.object, &after.upload_id, &after.user_defined, after.initiated),
                    (&before.bucket, &before.object, &before.upload_id, &before.user_defined, before.initiated)
                );
                let parts = store
                    .list_object_parts(bucket, "copy-target", &upload.upload_id, None, 1000, &options)
                    .await
                    .expect("parts");
                assert_eq!(parts.parts.len(), 1);
                assert_eq!(parts.parts[0].size, 5);
                assert_eq!(parts.parts[0].etag.as_deref(), Some(etag.opaque_tag()));
                std::sync::Arc::clone(&store)
                    .complete_multipart_upload(
                        bucket,
                        "copy-target",
                        &upload.upload_id,
                        vec![CompletePart {
                            part_num: 1,
                            etag: Some(etag.opaque_tag().to_owned()),
                            ..Default::default()
                        }],
                        &options,
                    )
                    .await
                    .expect("complete copy");
                let mut reader = store
                    .get_object_reader(bucket, "copy-target", None, HeaderMap::new(), &options)
                    .await
                    .expect("stored copy");
                let mut bytes = Vec::new();
                reader.stream.read_to_end(&mut bytes).await.expect("copy bytes");
                assert_eq!(bytes, expected_bytes);
            }
        }
    });
}

#[test]
fn protocol_upload_copy_source_length_matches_original_parser() {
    for bucket in ["bucket", "Upper"] {
        for key in [
            "a".repeat(1024),
            "a".repeat(1025),
            "é".repeat(512),
            format!("{}a", "é".repeat(512)),
        ] {
            let source = CopySourceValue::Bucket {
                bucket: bucket.into(),
                key,
                version_id: Some("null".into()),
            };
            let expected = legacy_copy_source(source.clone())
                .map(|_| ())
                .map_err(|error| conversion_error("UploadPartCopy copy_source", error).to_string());
            let actual = owned::validate_copy_source(&source, "UploadPartCopy copy_source").map_err(|error| error.to_string());
            assert_eq!(actual, expected);
        }
    }
}

#[test]
#[serial_test::serial]
fn protocol_upload_copy_keeps_destination_generation_fence() {
    crate::app::gating_test_env::run_large_stack_test("protocol-upload-copy-generation", || async {
        use crate::app::storage_api::access::{AuthorizationRequest, load_bucket_generation_from_store};
        use crate::app::storage_api::test::contract::bucket::{BucketOperations, DeleteBucketOptions, MakeBucketOptions};
        use crate::app::storage_api::test::contract::multipart::MultipartOperations;
        use crate::app::storage_api::test::contract::object::ObjectIO;
        use crate::storage_api::protocols::client::{StorageObjectOptions, StoragePutObjReader};
        use tokio::io::AsyncReadExt;
        let source = "protocol-upload-copy-generation";
        let destination = "protocol-upload-copy-recreated";
        let (_temp, root, backend, _) = crate::protocols::mainflow_test::object_client(source).await;
        let context = backend.server_ctx.app_context().expect("context");
        let store = context.object_store();
        let options = StorageObjectOptions::default();
        store
            .make_bucket(destination, &MakeBucketOptions::default())
            .await
            .expect("destination");
        let upload = store
            .new_multipart_upload(destination, "copy-target", &options)
            .await
            .expect("old upload");
        let mut request = input(source, &upload.upload_id);
        request.bucket = rustfs_gateway_types::BucketName::new(destination).expect("destination name");
        let uri = build_object_uri(
            destination,
            "copy-target",
            &[("partNumber", Some("1")), ("uploadId", Some(&upload.upload_id))],
        )
        .expect("URI");
        let mut envelope = ProtocolStorageClient::create_envelope(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(destination.into()),
                object: Some("copy-target".into()),
                credentials: &root,
            },
        );
        let guard = load_bucket_generation_from_store(store.as_ref(), &envelope, destination)
            .await
            .expect("capture generation");
        envelope.install_bucket_generation(guard);
        store
            .delete_bucket(destination, &DeleteBucketOptions::default())
            .await
            .expect("delete destination");
        store
            .make_bucket(destination, &MakeBucketOptions::default())
            .await
            .expect("replace destination");
        store
            .put_object(
                destination,
                "copy-target",
                &mut StoragePutObjReader::from_vec(b"replacement".to_vec()),
                &options,
            )
            .await
            .expect("replacement bytes");
        let result = backend
            .owned_multipart_usecase()
            .execute_upload_part_copy_with_audit(envelope, multipart::upload_part_copy_input(request).expect("owned input"))
            .await;
        let mut reader = store
            .get_object_reader(destination, "copy-target", None, HeaderMap::new(), &options)
            .await
            .expect("replacement object");
        let mut bytes = Vec::new();
        reader.stream.read_to_end(&mut bytes).await.expect("read replacement");
        let facts = (result.err().map(|error| error.code().as_str().to_owned()), bytes);
        assert_eq!(facts, (Some("NoSuchBucket".into()), b"replacement".to_vec()));
    });
}

fn owned_condition(value: Option<crate::app::object_usecase::CopySourceCondition>) -> Option<(String, Option<String>)> {
    use crate::app::object_usecase::CopySourceCondition;
    value.map(|value| match value {
        CopySourceCondition::Any => ("any".into(), None),
        CopySourceCondition::Weak => ("weak".into(), None),
        CopySourceCondition::Strong(value) => ("strong".into(), Some(value)),
    })
}

#[test]
fn protocol_upload_copy_input_matches_original_codec() {
    for case in 0..22 {
        let make = || {
            let mut request = input("bucket", "upload ?&");
            match case {
                0..=8 => {
                    let condition = [
                        "",
                        "*",
                        "abc-1",
                        "\"abc\"",
                        "W/\"abc\"",
                        "bad tag",
                        "\"abc",
                        "é",
                        "\"one\", \"two\"",
                    ][case];
                    request.copy_source_if_match = Some(condition.into());
                    request.copy_source_if_none_match = Some(condition.into());
                }
                9 => request.copy_source_if_modified_since = Some(rustfs_gateway_types::Timestamp::from_secs(i64::MAX)),
                10 => request.copy_source_if_unmodified_since = Some(rustfs_gateway_types::Timestamp::from_secs(i64::MAX)),
                11 => {
                    request.copy_source_if_modified_since = Some(rustfs_gateway_types::Timestamp::from_secs(i64::MAX));
                    request.copy_source_if_none_match = Some("bad tag".into());
                }
                12 => {
                    request.copy_source_if_match = Some("bad tag".into());
                    request.copy_source_if_unmodified_since = Some(rustfs_gateway_types::Timestamp::from_secs(i64::MAX));
                }
                13 => {
                    request.copy_source_if_modified_since =
                        Some(rustfs_gateway_types::Timestamp::from_secs_nanos(-1, 123_456_789).expect("date"));
                    request.copy_source_if_unmodified_since = request.copy_source_if_modified_since;
                }
                14 | 15 => {
                    let value = if case == 14 { "" } else { "value" };
                    request.sse_customer_algorithm = Some(value.into());
                    request.sse_customer_key = Some(rustfs_gateway_types::SseCustomerKey::new(value.into()));
                    request.sse_customer_key_md5 = Some(value.into());
                    request.copy_source_sse_customer_algorithm = Some(value.into());
                    request.copy_source_sse_customer_key = Some(rustfs_gateway_types::SseCustomerKey::new(value.into()));
                    request.copy_source_sse_customer_key_md5 = Some(value.into());
                    request.copy_source_range = Some(value.into());
                }
                16..=19 => {
                    request.copy_source = CopySourceValue::Bucket {
                        bucket: "bucket".into(),
                        key: ["%2F", "a?versionId=x", "a ?#+%.txt", "a\0b"][case - 16].into(),
                        version_id: Some(" null ".into()),
                    };
                }
                _ => request.copy_source = unsupported_source(case == 21),
            }
            request
        };
        let expected = legacy_copy_source(make().copy_source)
            .map_err(|error| conversion_error("UploadPartCopy copy_source", error))
            .and_then(|source| {
                gateway_s3s_compat::generated::ops::upload_part_copy::input_to_s3s(make(), source)
                    .map_err(|error| conversion_error("UploadPartCopy input", error))
            });
        let actual = multipart::upload_part_copy_input(make());
        match (actual, expected) {
            (Err(actual), Err(expected)) => assert_eq!(error_facts(actual), error_facts(expected)),
            (Ok(actual), Ok(expected)) => {
                let crate::app::multipart_usecase::UploadPartTarget::Legacy { bucket, key, upload_id } = actual.target else {
                    panic!("protocol client must use the legacy upload target")
                };
                let condition = |value: Option<ETagCondition>| -> Option<(String, Option<String>)> {
                    value.map(|value| match value {
                        ETagCondition::Any => ("any".into(), None),
                        ETagCondition::ETag(ETag::Weak(_)) => ("weak".into(), None),
                        ETagCondition::ETag(ETag::Strong(value)) => ("strong".into(), Some(value)),
                    })
                };
                assert_eq!(
                    (
                        owned_condition(actual.copy_source_if_match),
                        owned_condition(actual.copy_source_if_none_match)
                    ),
                    (condition(expected.copy_source_if_match), condition(expected.copy_source_if_none_match))
                );
                assert_eq!(
                    (
                        bucket,
                        key,
                        upload_id,
                        actual.part_number,
                        format!("{:?}", actual.copy_source),
                        actual.copy_source_range
                    ),
                    (
                        expected.bucket,
                        expected.key,
                        expected.upload_id,
                        expected.part_number,
                        format!("{:?}", gateway_s3s_compat::copy_source::from_s3s(expected.copy_source)),
                        expected.copy_source_range
                    )
                );
                assert_eq!(
                    (
                        actual.sse_customer_algorithm,
                        actual.sse_customer_key_md5,
                        actual.copy_source_sse_customer_algorithm,
                        actual.copy_source_sse_customer_key_md5,
                        actual.sse_customer_key.as_ref().map(String::len),
                        actual.copy_source_sse_customer_key.as_ref().map(String::len)
                    ),
                    (
                        expected.sse_customer_algorithm,
                        expected.sse_customer_key_md5,
                        expected.copy_source_sse_customer_algorithm,
                        expected.copy_source_sse_customer_key_md5,
                        expected.sse_customer_key.as_ref().map(String::len),
                        expected.copy_source_sse_customer_key.as_ref().map(String::len)
                    )
                );
            }
            _ => panic!("UploadPartCopy codec refusal differs for case {case}"),
        }
    }
}

fn output_facts(raw: Option<&str>) -> crate::app::multipart_usecase::UploadPartCopyWriteOutput {
    crate::app::multipart_usecase::UploadPartCopyWriteOutput {
        e_tag: raw.map(str::to_owned),
        last_modified: Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_456_789).expect("time")),
        copy_source_version_id: Some("null".into()),
        server_side_encryption: Some("aws:kms".into()),
        ssekms_key_id: Some("kms-id".into()),
        sse_customer_algorithm: Some("AES256".into()),
        sse_customer_key_md5: Some("digest".into()),
        checksum_crc32: Some("crc32".into()),
        checksum_crc32c: Some("crc32c".into()),
        checksum_sha1: Some("sha1".into()),
        checksum_sha256: Some("sha256".into()),
        checksum_crc64nvme: Some("crc64nvme".into()),
    }
}

#[test]
fn protocol_upload_copy_output_matches_original_composed_codec() {
    for raw in [
        None,
        Some("abc"),
        Some("\"abc\""),
        Some("W/\"abc\""),
        Some("a&\"b"),
        Some(""),
        Some("\"\""),
        Some("a\0b"),
    ] {
        let fields = output_facts(raw);
        let legacy = UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: raw.map(crate::app::storage_api::multipart_usecase::object_utils::to_s3s_xml_etag),
                last_modified: fields.last_modified.map(Timestamp::from),
                checksum_crc32: fields.checksum_crc32,
                checksum_crc32c: fields.checksum_crc32c,
                checksum_sha1: fields.checksum_sha1,
                checksum_sha256: fields.checksum_sha256,
                checksum_crc64nvme: fields.checksum_crc64nvme,
                ..Default::default()
            }),
            copy_source_version_id: fields.copy_source_version_id,
            server_side_encryption: fields.server_side_encryption.map(ServerSideEncryption::from),
            ssekms_key_id: fields.ssekms_key_id,
            sse_customer_algorithm: fields.sse_customer_algorithm,
            sse_customer_key_md5: fields.sse_customer_key_md5,
            ..Default::default()
        };
        let expected = gateway_s3s_compat::generated::ops::upload_part_copy::output_from_s3s(legacy)
            .map(|output| format!("{output:?}"))
            .map_err(|error| conversion_error("UploadPartCopy output", error).to_string());
        let actual = multipart::upload_part_copy_output(output_facts(raw))
            .map(|output| format!("{output:?}"))
            .map_err(|error| error.to_string());
        assert_eq!(actual, expected, "stored tag {raw:?}");
    }
}

#[test]
fn protocol_upload_copy_error_keeps_explicit_message_headers_and_storage_cause() {
    use crate::app::storage_api::multipart_usecase::error::StorageError;
    use rustfs_s3_types::S3ErrorCode;
    let make = || {
        let mut error = S3Error::with_message(S3ErrorCode::PreconditionFailed, "explicit copy refusal");
        error.set_status_code(http::StatusCode::BAD_REQUEST);
        let mut headers = HeaderMap::new();
        headers.insert("x-copy-fact", "preserved".parse().expect("header"));
        error.set_headers(headers);
        error.set_source(Box::new(StorageError::ObjectNotFound("bucket".into(), "key".into())));
        error
    };
    let old: S3Result<()> = Err(make().into());
    let expected = old.map_err(S3Error::from).expect_err("legacy error");
    let actual = owned::copy_error(make());
    assert!(
        actual
            .source()
            .is_some_and(|cause| cause.downcast_ref::<StorageError>().is_some())
    );
    assert_eq!(actual.source().map(ToString::to_string), expected.source().map(ToString::to_string));
    assert_eq!(error_facts(actual), error_facts(expected));
}

// Keep construction of the large use-case futures outside the test's async frame.
#[inline(never)]
fn legacy_copy_future(
    usecase: &DefaultMultipartUsecase,
    request: S3Request<UploadPartCopyInput>,
) -> impl std::future::Future<Output = S3Result<S3Response<UploadPartCopyOutput>>> + '_ {
    Box::pin(usecase.execute_upload_part_copy(request))
}

#[inline(never)]
fn owned_copy_future(
    usecase: &DefaultMultipartUsecase,
    envelope: crate::app::RequestEnvelope,
    input: UploadPartCopyWriteInput,
) -> impl std::future::Future<Output = rustfs_s3_types::S3Result<UploadPartCopyWriteOutput>> + '_ {
    Box::pin(usecase.execute_upload_part_copy_with_audit(envelope, input))
}

#[tokio::test]
async fn protocol_upload_copy_capabilities_precede_part_validation() {
    let credentials = rustfs_credentials::Credentials {
        access_key: "copy-capability-test".into(),
        secret_key: "copy-capability-fixture".into(),
        ..Default::default()
    };
    for (query, code, message) in [
        ("", "InvalidArgument", "partNumber must be between 1 and 10000"),
        (
            "&x-rustfs-max-total-object-size=10",
            "InvalidRequest",
            "x-rustfs-max-total-object-size is only supported for CreateMultipartUpload",
        ),
        (
            "&x-rustfs-max-content-length=10",
            "InvalidRequest",
            "x-rustfs-max-content-length is only supported for presigned PutObject",
        ),
        (
            "&x-rustfs-max-total-object-size=10&x-rustfs-max-content-length=10",
            "InvalidRequest",
            "x-rustfs-max-total-object-size is only supported for CreateMultipartUpload",
        ),
    ] {
        let make = || {
            let mut request = input("bucket", "upload");
            request.part_number = 0;
            request
        };
        let source = legacy_copy_source(make().copy_source).expect("source");
        let legacy = gateway_s3s_compat::generated::ops::upload_part_copy::input_to_s3s(make(), source).expect("legacy input");
        let uri = format!("/bucket/copy-target?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=fixture/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=fixture{query}").parse().expect("URI");
        let mut request = ProtocolStorageClient::create_request(
            legacy,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some("bucket".into()),
                object: Some("copy-target".into()),
                credentials: &credentials,
            },
        )
        .expect("request");
        request.extensions.insert(crate::auth::VerifiedSigV4Request);
        request.extensions.insert(crate::auth::VerifiedPresignedRequest);
        let envelope = crate::app::RequestEnvelope::from_s3s(&request);
        let usecase = crate::app::multipart_usecase::DefaultMultipartUsecase::without_context();
        let expected = legacy_copy_future(&usecase, request)
            .await
            .err()
            .map(S3Error::from)
            .expect("legacy refusal");
        let actual = owned_copy_future(&usecase, envelope, multipart::upload_part_copy_input(make()).expect("owned input"))
            .await
            .err()
            .expect("owned refusal");
        assert_eq!((actual.code().as_str(), actual.message()), (code, Some(message)));
        assert_eq!(error_facts(actual), error_facts(expected));
    }
}
