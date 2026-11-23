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

//! CopyObject compatibility at the protocol storage boundary.

use super::*;
use crate::storage_api::protocols::client::{
    BucketOperations, MakeBucketOptions, ObjectIO, StorageObjectOptions, StoragePutObjReader,
};
use http_body_util::BodyExt;
use rustfs_protocols::common::client::s3::StorageBackend;

fn input() -> gateway_dto::CopyObjectInput {
    gateway_dto::CopyObjectInput::builder(rustfs_gateway_types::CopySourceValue::Bucket {
        bucket: "copy-source".into(),
        key: "a ?&/%é".into(),
        version_id: None,
    })
    .bucket(BucketName::new("copy-target".to_owned()).expect("bucket"))
    .key(rustfs_gateway_types::ObjectKey::new("destination".to_owned()).expect("key"))
    .build()
}

fn count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|row| row.op == rustfs_s3_ops::S3Operation::CopyObject.as_str())
        .map_or(0, |row| row.total)
}

fn outcome(result: rustfs_s3_types::S3Result<gateway_dto::CopyObjectOutput>) -> serde_json::Value {
    match result {
        Ok(output) => {
            serde_json::json!({"etag":output.e_tag.map(|tag|format!("{tag:?}")),"nanos":output.last_modified.map(|at|at.subsec_nanos()%1_000_000),"version":output.version_id,"source_version":output.copy_source_version_id})
        }
        Err(error) => {
            serde_json::json!({"code":format!("{:?}",error.code()),"message":error.message(),"status":error.status_code().map(|status|status.as_u16()),"headers":format!("{:?}",error.headers())})
        }
    }
}

#[test]
#[serial_test::serial]
fn protocol_copy_runtime_contract() {
    crate::app::gating_test_env::run_large_stack_test("protocol-copy", || async {
        let root = rustfs_credentials::Credentials {
            access_key: "copy-root".into(),
            secret_key: "copy-secret".into(),
            status: "on".into(),
            ..Default::default()
        };
        let (_foreign_temp, foreign, _foreign_iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
        let ambient = crate::runtime_sources::publish_test_app_context(foreign.clone());
        assert!(std::sync::Arc::ptr_eq(&ambient, &foreign));
        let (_temp, target, _iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
        for context in [&target, &ambient] {
            for bucket in ["copy-source", "copy-target"] {
                context
                    .object_store()
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("bucket");
            }
            context
                .object_store()
                .put_object(
                    "copy-source",
                    "a ?&/%é",
                    &mut StoragePutObjReader::from_vec(if std::sync::Arc::ptr_eq(context, &target) {
                        b"bound".to_vec()
                    } else {
                        b"ambient".to_vec()
                    }),
                    &StorageObjectOptions::default(),
                )
                .await
                .expect("source");
        }
        let slot = crate::runtime_sources::ServerContextSlot::new();
        assert!(slot.install(target.clone()));
        let client = ProtocolStorageClient::new(slot);
        // Prove that the observer can distinguish zero, one and an edge increment.
        assert_eq!(count(), 0);
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::CopyObject);
        assert_eq!(count(), 1);

        let mut rows = Vec::new();
        for case in [
            "normal",
            "missing-source",
            "bad-version",
            "invalid-class",
            "specific-none",
            "weak-match",
            "strong-match",
            "invalid-match",
            "empty-match",
            "bad-source-bucket",
            "bad-source-key",
            "bad-destination",
            "tagging",
            "same-source",
        ] {
            let mut request = input();
            match case {
                "missing-source" => {
                    request.copy_source = rustfs_gateway_types::CopySourceValue::Bucket {
                        bucket: "copy-source".into(),
                        key: "missing".into(),
                        version_id: None,
                    }
                }
                "bad-version" => {
                    request.copy_source = rustfs_gateway_types::CopySourceValue::Bucket {
                        bucket: "copy-source".into(),
                        key: "a ?&/%é".into(),
                        version_id: Some("bad".into()),
                    }
                }
                "invalid-class" => request.storage_class = Some(gateway_dto::StorageClass::custom("BAD".to_owned())),
                "specific-none" => request.if_none_match = Some("abc".into()),
                "weak-match" => request.copy_source_if_match = Some("W/\"abc\"".into()),
                "strong-match" => request.copy_source_if_match = Some("\"abc\"".into()),
                "invalid-match" => request.copy_source_if_match = Some("\"abc".into()),
                "empty-match" => request.copy_source_if_match = Some("".into()),
                "bad-source-bucket" => {
                    request.copy_source = rustfs_gateway_types::CopySourceValue::Bucket {
                        bucket: "x".into(),
                        key: "key".into(),
                        version_id: None,
                    }
                }
                "bad-source-key" => {
                    request.copy_source = rustfs_gateway_types::CopySourceValue::Bucket {
                        bucket: "copy-source".into(),
                        key: "".into(),
                        version_id: None,
                    }
                }
                "bad-destination" => {
                    request.bucket = crate::app::dto_values::stored_bucket_name("a-s3alias").expect("stored reserved name")
                }
                "tagging" => request.tagging = Some("bad=%FF".into()),
                "same-source" => {
                    request.bucket = BucketName::new("copy-source".to_owned()).expect("source");
                    request.key = rustfs_gateway_types::ObjectKey::new("a ?&/%é".to_owned()).expect("source key");
                }
                _ => {}
            }
            let before = count();
            let result = outcome(client.copy_object(request, &root).await);
            rows.push(serde_json::json!({"case":case,"result":result,"count":count()-before}));
        }
        let before = count();
        let fallback = ProtocolStorageClient::new(crate::runtime_sources::ServerContextSlot::new());
        let fallback = outcome(fallback.copy_object(input(), &root).await);
        let present = |context: std::sync::Arc<crate::runtime_sources::AppContext>| {
            let root = root.clone();
            async move {
                let slot = crate::runtime_sources::ServerContextSlot::new();
                assert!(slot.install(context));
                let client = ProtocolStorageClient::new(slot);
                match client.get_object("copy-target", "destination", &root, None).await {
                    Ok(output) => Ok(output
                        .body
                        .expect("copied body")
                        .into_body()
                        .collect()
                        .await
                        .expect("copied bytes")
                        .to_bytes()
                        .to_vec()),
                    Err(error) => Err(error.to_string()),
                }
            }
        };
        let actual = serde_json::json!({"rows":rows,"fallback":fallback,"fallback_count":count()-before,"bound":present(target).await,"ambient":present(ambient).await});
        println!("COPY_OBSERVATIONS={actual}");
        assert_eq!(
            actual,
            serde_json::json!({"ambient": {"Ok": [97, 109, 98, 105, 101, 110, 116]}, "bound": {"Ok": [98, 111, 117, 110, 100]}, "fallback": {"etag": "ETag { weak: false, any: false, stored: false, tag: \"fff66a4d00c8ed7bb7814ff29d0faca2\" }", "nanos": 0, "source_version": null, "version": null}, "fallback_count": 1, "rows": [{"case": "normal", "count": 1, "result": {"etag": "ETag { weak: false, any: false, stored: false, tag: \"b9f2b582ee75a7259c62ce162cebe368\" }", "nanos": 0, "source_version": null, "version": null}}, {"case": "missing-source", "count": 1, "result": {"code": "NoSuchKey", "headers": "None", "message": "The specified key does not exist.", "status": 404}}, {"case": "bad-version", "count": 1, "result": {"code": "InvalidArgument", "headers": "None", "message": "Invalid version id specified in copy source", "status": 400}}, {"case": "invalid-class", "count": 1, "result": {"code": "InvalidStorageClass", "headers": "None", "message": "The storage class you specified is not valid.", "status": 400}}, {"case": "specific-none", "count": 1, "result": {"code": "NotImplemented", "headers": "None", "message": "A header you provided implies functionality that is not implemented.", "status": 501}}, {"case": "weak-match", "count": 1, "result": {"code": "PreconditionFailed", "headers": "None", "message": "At least one of the preconditions you specified did not hold.", "status": 412}}, {"case": "strong-match", "count": 1, "result": {"code": "PreconditionFailed", "headers": "None", "message": "At least one of the preconditions you specified did not hold.", "status": 412}}, {"case": "invalid-match", "count": 0, "result": {"code": "InternalError", "headers": "None", "message": "failed to convert CopyObject input through the gateway compatibility seam: copy_source_if_match: not an entity-tag condition the s3s input can hold", "status": 500}}, {"case": "empty-match", "count": 1, "result": {"etag": "ETag { weak: false, any: false, stored: false, tag: \"b9f2b582ee75a7259c62ce162cebe368\" }", "nanos": 0, "source_version": null, "version": null}}, {"case": "bad-source-bucket", "count": 0, "result": {"code": "InternalError", "headers": "None", "message": "failed to convert CopyObject copy_source through the gateway compatibility seam: ParseAmzCopySourceError: InvalidBucketName", "status": 500}}, {"case": "bad-source-key", "count": 1, "result": {"code": "InvalidArgument", "headers": "None", "message": "Invalid object key", "status": 400}}, {"case": "bad-destination", "count": 1, "result": {"code": "InternalError", "headers": "None", "message": "We encountered an internal error, please try again.", "status": 500}}, {"case": "tagging", "count": 1, "result": {"code": "InvalidRequest", "headers": "None", "message": "The Tagging header requires the REPLACE tagging directive", "status": 400}}, {"case": "same-source", "count": 1, "result": {"code": "InvalidRequest", "headers": "None", "message": "Cannot copy an object to itself. Source and destination must be different.", "status": 400}}]})
        );
    });
}

fn legacy_input() -> CopyObjectInput {
    CopyObjectInput::builder()
        .bucket("copy-target".into())
        .key("destination".into())
        .copy_source(CopySource::parse("/copy-source/a%20%3F%26%2F%25%C3%A9").expect("legacy source"))
        .build()
        .expect("legacy copy input")
}

use crate::app::object_usecase::{CopyObjectWriteInput, CopyObjectWriteOutput, CopySourceCondition};

macro_rules! optional_input_facts {
    ($input:expr) => {
        serde_json::json!({
            "bucket": $input.bucket,
            "key": $input.key,
            "version_id": $input.version_id,
            "cache_control": $input.cache_control,
            "content_disposition": $input.content_disposition,
            "content_encoding": $input.content_encoding,
            "content_language": $input.content_language,
            "content_type": $input.content_type,
            "expires": $input.expires,
            "metadata": $input.metadata,
            "tagging": $input.tagging,
            "website_redirect_location": $input.website_redirect_location,
            "ssekms_key_id": $input.ssekms_key_id,
            "sse_customer_algorithm": $input.sse_customer_algorithm,
            "sse_customer_key": $input.sse_customer_key,
            "sse_customer_key_md5": $input.sse_customer_key_md5,
            "copy_source_sse_customer_algorithm": $input.copy_source_sse_customer_algorithm,
            "copy_source_sse_customer_key": $input.copy_source_sse_customer_key,
            "copy_source_sse_customer_key_md5": $input.copy_source_sse_customer_key_md5,
            "metadata_directive": $input.metadata_directive.as_ref().map(|value|value.as_str()),
            "tagging_directive": $input.tagging_directive.as_ref().map(|value|value.as_str()),
            "object_lock_mode": $input.object_lock_mode.as_ref().map(|value|value.as_str()),
            "object_lock_legal_hold_status": $input.object_lock_legal_hold_status.as_ref().map(|value|value.as_str()),
            "server_side_encryption": $input.server_side_encryption.as_ref().map(|value|value.as_str()),
            "storage_class": $input.storage_class.as_ref().map(|value|value.as_str()),
            "checksum_algorithm": $input.checksum_algorithm.as_ref().map(|value|value.as_str()),
        })
    };
}

fn owned_condition(value: Option<CopySourceCondition>) -> serde_json::Value {
    match value {
        None => serde_json::Value::Null,
        Some(CopySourceCondition::Any) => serde_json::json!(["any"]),
        Some(CopySourceCondition::Weak) => serde_json::json!(["weak"]),
        Some(CopySourceCondition::Strong(value)) => serde_json::json!(["strong", value]),
    }
}

fn legacy_condition(value: Option<ETagCondition>) -> serde_json::Value {
    match value {
        None => serde_json::Value::Null,
        Some(ETagCondition::Any) => serde_json::json!(["any"]),
        Some(ETagCondition::ETag(ETag::Weak(_))) => serde_json::json!(["weak"]),
        Some(ETagCondition::ETag(ETag::Strong(value))) => serde_json::json!(["strong", value]),
    }
}

fn source_facts(source: Option<rustfs_gateway_types::CopySourceValue>) -> serde_json::Value {
    match source {
        Some(rustfs_gateway_types::CopySourceValue::Bucket { bucket, key, version_id }) => {
            serde_json::json!([bucket, key, version_id])
        }
        _ => serde_json::Value::Null,
    }
}

fn legacy_source_facts(source: CopySource) -> serde_json::Value {
    match source {
        CopySource::Bucket { bucket, key, version_id } => serde_json::json!([bucket, key, version_id]),
        _ => serde_json::Value::Null,
    }
}

fn owned_input_facts(input: CopyObjectWriteInput) -> serde_json::Value {
    let fields = optional_input_facts!(input);
    serde_json::json!({"fields":fields,"source":source_facts(input.copy_source),"none":input.specific_if_none_match,
        "match":owned_condition(input.copy_source_if_match),"not_match":owned_condition(input.copy_source_if_none_match),
        "retain":input.object_lock_retain_until_date.map(|value|value.unix_timestamp_nanos().to_string())})
}

fn legacy_input_facts(input: CopyObjectInput) -> serde_json::Value {
    let fields = optional_input_facts!(input);
    serde_json::json!({"fields":fields,"source":legacy_source_facts(input.copy_source),"none":matches!(input.if_none_match,Some(ETagCondition::ETag(_))),
        "match":legacy_condition(input.copy_source_if_match),"not_match":legacy_condition(input.copy_source_if_none_match),
        "retain":input.object_lock_retain_until_date.map(|value|time::OffsetDateTime::from(value).unix_timestamp_nanos().to_string())})
}

fn assert_copy_input(native: gateway_dto::CopyObjectInput, expected: Result<CopyObjectInput, String>) {
    assert_eq!(
        owned::copy_input(native)
            .map(owned_input_facts)
            .map_err(|error| error.to_string()),
        expected
            .map(legacy_input_facts)
            .map_err(|reason| conversion_error("CopyObject input", reason).to_string())
    );
}

fn copy_output_facts(tag: Option<&str>) -> CopyObjectWriteOutput {
    CopyObjectWriteOutput {
        e_tag: tag.map(str::to_owned),
        last_modified: Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_456_789).expect("instant")),
        copy_source_version_id: None,
        version_id: None,
        server_side_encryption: None,
        ssekms_key_id: None,
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        checksum_crc32: None,
        checksum_crc32c: None,
        checksum_sha1: None,
        checksum_sha256: None,
        checksum_crc64nvme: None,
        checksum_md5: None,
        checksum_sha512: None,
        checksum_xxhash3: None,
        checksum_xxhash64: None,
        checksum_xxhash128: None,
        checksum_type: None,
    }
}

#[test]
fn protocol_copy_request_head_matches_legacy() {
    for (secret, expected_head_secret) in [("copy-secret", Some("copy-secret")), ("", None)] {
        let credentials = rustfs_credentials::Credentials {
            access_key: "copy-identity".into(),
            secret_key: secret.into(),
            session_token: "identity-token".into(),
            status: "on".into(),
            ..Default::default()
        };
        let uri = build_object_uri("copy-target", "a ?&/%é", &[]).expect("request URI");
        let envelope = ProtocolStorageClient::create_envelope(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some("copy-target".to_owned()),
                object: Some("a ?&/%é".to_owned()),
                credentials: &credentials,
            },
        );

        assert_eq!(envelope.method(), &Method::PUT);
        assert_eq!(envelope.uri().to_string(), "/copy-target/a%20%3F&/%25%C3%A9");
        assert!(envelope.headers().is_empty());
        assert!(envelope.region().is_none());
        assert!(envelope.service().is_none());
        assert!(envelope.server_context().is_none());
        assert!(envelope.verified_presigned().is_none());

        let head_credentials = envelope.credentials().expect("request credentials");
        assert_eq!(head_credentials.access_key(), "copy-identity");
        assert_eq!(head_credentials.secret_key(), expected_head_secret);
        assert!(head_credentials.session_token().is_none());

        let info = envelope.req_info().expect("request info");
        let identity = info.cred.as_ref().expect("request identity");
        assert_eq!(identity.access_key, "copy-identity");
        assert_eq!(identity.secret_key, secret);
        assert_eq!(identity.session_token, "identity-token");
        assert_eq!(info.bucket.as_deref(), Some("copy-target"));
        assert_eq!(info.object.as_deref(), Some("a ?&/%é"));
        assert!(info.version_id.is_none());
        assert!(!info.is_owner);
        assert!(info.keystone_session_present.is_none());
        assert!(!info.replication_request_authorized);
        assert!(info.region.is_none());
        assert!(info.request_context.is_some());
        assert!(!info.suppress_denial_log);
        assert!(!info.suppress_denial_audit);
    }
}

#[test]
fn protocol_copy_input_projection_matches_legacy() {
    for field in [
        "if_match",
        "if_none_match",
        "copy_source_if_match",
        "copy_source_if_none_match",
    ] {
        for value in [
            "",
            "*",
            " * ",
            "abc",
            "W/abc",
            "\"abc\"",
            "W/\"abc\"",
            "\"\"",
            "\"abc",
            "abc\"",
            "\"a b\"",
            "\"a\\b\"",
            "\"a\"b\"",
            "\"a\nb\"",
            "a,b",
            "é",
            "\"é\"",
        ] {
            let mut request = input();
            match field {
                "if_match" => request.if_match = Some(value.into()),
                "if_none_match" => request.if_none_match = Some(value.into()),
                "copy_source_if_match" => request.copy_source_if_match = Some(value.into()),
                _ => request.copy_source_if_none_match = Some(value.into()),
            }
            let expected = (!value.is_empty())
                .then(|| ETagCondition::parse_http_header(value.as_bytes()))
                .transpose()
                .map(|condition| {
                    let mut old = legacy_input();
                    match field {
                        "if_match" => old.if_match = condition,
                        "if_none_match" => old.if_none_match = condition,
                        "copy_source_if_match" => old.copy_source_if_match = condition,
                        _ => old.copy_source_if_none_match = condition,
                    }
                    old
                })
                .map_err(|_| format!("{field}: not an entity-tag condition the s3s input can hold"));
            assert_copy_input(request, expected);
        }
    }
    for value in ["", "value"] {
        let mut request = input();
        let mut old = legacy_input();
        macro_rules! optional {
            ($($field:ident),+) => {$(
                request.$field = Some(value.to_owned());
                old.$field = (!value.is_empty()).then(|| value.to_owned());
            )+};
        }
        optional!(
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            tagging,
            ssekms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key_md5,
            website_redirect_location
        );
        request.expires = Some(OpaqueString::from(value));
        old.expires = (!value.is_empty()).then(|| value.to_owned());
        macro_rules! enums {
            ($($field:ident => $kind:ident),+) => {$(
                request.$field = Some(gateway_dto::$kind::custom(value));
                old.$field = (!value.is_empty()).then(|| value.to_owned().into());
            )+};
        }
        enums!(
            metadata_directive => MetadataDirective,
            tagging_directive => TaggingDirective,
            object_lock_legal_hold_status => ObjectLockLegalHoldStatus,
            object_lock_mode => ObjectLockMode,
            storage_class => StorageClass,
            server_side_encryption => ServerSideEncryption,
            checksum_algorithm => ChecksumAlgorithm
        );
        assert_copy_input(request, Ok(old));
    }
    for (secs, nanos) in [
        (1_700_000_000, 123_456_789),
        (-1, 123_456_789),
        (0, 123_456_789),
        (i64::MAX, 0),
    ] {
        let mut request = input();
        request.object_lock_retain_until_date =
            Some(rustfs_gateway_types::Timestamp::from_secs_nanos(secs, nanos).expect("instant"));
        let expected = Timestamp::parse(TimestampFormat::EpochSeconds, &format!("{secs}.{nanos:09}"))
            .map(|date| {
                let mut old = legacy_input();
                old.object_lock_retain_until_date = Some(date);
                old
            })
            .map_err(|_| "object_lock_retain_until_date: an instant outside what the s3s shape can hold".to_owned());
        assert_copy_input(request, expected);
    }
    for case in [
        "event-hold",
        "event-days",
        "event-years",
        "source-modified-time",
        "source-unmodified-time",
    ] {
        let mut request = input();
        match case {
            "event-hold" => request.object_lock_event_hold = Some(gateway_dto::ObjectLockEventHold::custom("ON".to_owned())),
            "event-days" => request.object_lock_event_hold_duration_days = Some(0),
            "event-years" => request.object_lock_event_hold_duration_years = Some(1),
            "source-modified-time" => {
                request.copy_source_if_modified_since =
                    Some(rustfs_gateway_types::Timestamp::from_secs_nanos(i64::MAX, 0).expect("gateway timestamp"))
            }
            _ => {
                request.copy_source_if_unmodified_since =
                    Some(rustfs_gateway_types::Timestamp::from_secs_nanos(i64::MAX, 0).expect("gateway timestamp"))
            }
        }
        let (field, detail) = match case {
            "event-hold" | "event-days" | "event-years" => (
                match case {
                    "event-hold" => "object_lock_event_hold",
                    "event-days" => "object_lock_event_hold_duration_days",
                    _ => "object_lock_event_hold_duration_years",
                },
                "an Object Lock event hold, which no pinned s3s shape holds and RustFS does not store",
            ),
            "source-modified-time" => ("copy_source_if_modified_since", "an instant outside what the s3s shape can hold"),
            _ => ("copy_source_if_unmodified_since", "an instant outside what the s3s shape can hold"),
        };
        assert_copy_input(request, Err(format!("{field}: {detail}")));
    }
}

#[test]
fn protocol_copy_source_projection_matches_legacy() {
    let mut rows = Vec::new();
    for bucket in [
        "x",
        "ab",
        "abc",
        "bad_bucket",
        "Abc",
        "127.0.0.1",
        "a-s3alias",
        "xn--reserved",
        "-abc",
        "abc-",
        "a..b",
        "a.-b",
        "a.b",
        "a/b",
        "",
        "abc\0",
    ] {
        for key in [
            "key".to_owned(),
            "".to_owned(),
            "/leading".to_owned(),
            "trailing/".to_owned(),
            "a?versionId=x".to_owned(),
            "%FF".to_owned(),
            "a\0b".to_owned(),
            "a".repeat(1024),
            "a".repeat(1025),
            "é".repeat(512),
            format!("{}a", "é".repeat(512)),
        ] {
            let source = rustfs_gateway_types::CopySourceValue::Bucket {
                bucket: bucket.into(),
                key: key.clone(),
                version_id: Some("ver ?&%é".into()),
            };
            let current = owned::copy_source(source.clone())
                .map(source_facts)
                .map_err(|error| error.to_string());
            let old = legacy_copy_source(source)
                .map(legacy_source_facts)
                .map_err(|error| conversion_error("CopyObject copy_source", error).to_string());
            if current != old {
                rows.push(serde_json::json!([bucket, key, current, old]));
            }
        }
    }
    assert!(rows.is_empty(), "CopyObject source projection mismatches: {rows:#?}");
}

#[test]
fn protocol_copy_output_projection_matches_legacy() {
    for (raw, tag) in [
        (None, None),
        (Some("abc"), Some(ETag::Strong("abc".to_owned()))),
        (Some("\"abc\""), Some(ETag::Strong("abc".to_owned()))),
        (Some("W/\"abc\""), Some(ETag::Weak("abc".to_owned()))),
        (Some("a&\"b"), Some(ETag::Strong("a&amp;&quot;b".to_owned()))),
        (Some(""), Some(ETag::Strong(String::new()))),
        (Some("\"\""), Some(ETag::Strong(String::new()))),
        (Some("a\nb"), Some(ETag::Strong("a\nb".to_owned()))),
    ] {
        let actual = owned::copy_output(copy_output_facts(raw));
        if matches!(raw, Some("" | "\"\"" | "a\nb")) {
            assert_eq!(
                actual.unwrap_err().to_string(),
                conversion_error("CopyObject output", "e_tag: not an entity tag the gateway can write").to_string()
            );
            continue;
        }
        let tag = tag.map(|tag| {
            match tag {
                ETag::Strong(value) => rustfs_gateway_types::ETag::new(value),
                ETag::Weak(value) => rustfs_gateway_types::ETag::new_weak(value),
            }
            .expect("recorded valid entity tag")
        });
        let expected = gateway_dto::CopyObjectOutput {
            e_tag: tag,
            last_modified: Some(
                rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_000_000).expect("wire precision"),
            ),
            ..Default::default()
        };
        assert_eq!(format!("{:?}", actual.expect("copy output")), format!("{expected:?}"), "{raw:?}");
    }
    let mut current = copy_output_facts(Some("populated"));
    let mut expected = gateway_dto::CopyObjectOutput {
        e_tag: Some(rustfs_gateway_types::ETag::new("populated").expect("tag")),
        last_modified: Some(
            rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_000_000).expect("wire precision"),
        ),
        ..Default::default()
    };
    macro_rules! top_fields {
        ($($field:ident),*) => {$(current.$field=Some(stringify!($field).into());expected.$field=Some(stringify!($field).into());)*};
    }
    top_fields!(
        version_id,
        copy_source_version_id,
        ssekms_key_id,
        sse_customer_algorithm,
        sse_customer_key_md5
    );
    current.server_side_encryption = Some("AES256".into());
    expected.server_side_encryption = Some(gateway_dto::ServerSideEncryption::custom("AES256"));
    top_fields!(
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
        checksum_md5,
        checksum_sha512,
        checksum_xxhash3,
        checksum_xxhash64,
        checksum_xxhash128
    );
    current.checksum_type = Some("FULL_OBJECT".into());
    expected.checksum_type = Some("FULL_OBJECT".into());
    assert_eq!(
        format!("{:?}", owned::copy_output(current).expect("populated copy output")),
        format!("{expected:?}")
    );
}

#[test]
fn protocol_copy_error_answers_match_legacy() {
    use rustfs_s3_types::{S3Error, S3ErrorCode};
    let mut rows = Vec::new();
    for code in [
        S3ErrorCode::InvalidStorageClass,
        S3ErrorCode::PreconditionFailed,
        S3ErrorCode::UnexpectedContent,
        S3ErrorCode::NotImplemented,
        S3ErrorCode::InternalError,
    ] {
        let old: S3Result<()> = Err(crate::error::legacy_error_for_test(S3Error::new(code.clone())));
        let old = old.map_err(crate::error::owned_error_for_test).expect_err("legacy error");
        let current = owned::copy_error(S3Error::new(code.clone()));
        if (old.code(), old.message(), old.status_code(), old.headers())
            != (current.code(), current.message(), current.status_code(), current.headers())
        {
            rows.push((
                code,
                format!("{:?}:{:?}:{:?}:{:?}", old.code(), old.message(), old.status_code(), old.headers()),
                format!(
                    "{:?}:{:?}:{:?}:{:?}",
                    current.code(),
                    current.message(),
                    current.status_code(),
                    current.headers()
                ),
            ));
        }
    }
    let make = || {
        let mut error = S3Error::with_message(S3ErrorCode::PreconditionFailed, "copy refusal");
        error.set_status_code(http::StatusCode::BAD_REQUEST);
        let mut headers = HeaderMap::new();
        headers.insert("x-copy-fact", "preserved".parse().expect("fixture header"));
        error.set_headers(headers);
        error
    };
    let old: S3Result<()> = Err(crate::error::legacy_error_for_test(make()));
    let old = old.map_err(crate::error::owned_error_for_test).expect_err("legacy refusal");
    let current = owned::copy_error(make());
    if (old.code(), old.message(), old.status_code(), old.headers())
        != (current.code(), current.message(), current.status_code(), current.headers())
    {
        rows.push((
            S3ErrorCode::PreconditionFailed,
            format!("{:?}:{:?}:{:?}:{:?}", old.code(), old.message(), old.status_code(), old.headers()),
            format!(
                "{:?}:{:?}:{:?}:{:?}",
                current.code(),
                current.message(),
                current.status_code(),
                current.headers()
            ),
        ));
    }
    assert!(rows.is_empty(), "CopyObject error projection mismatches: {rows:#?}");
}
