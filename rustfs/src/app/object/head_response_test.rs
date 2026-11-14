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

//! Regression tests for the HeadObject response facts and their HTTP and legacy projections.
//! The existing completion probe exposes the AuditEntry before asynchronous delivery.

use super::tests::head_input;
use super::*;
use http::Method;

#[test]
fn head_stored_etag_facts_and_legacy_projection_preserve_spelling() {
    head_stored_etag_facts_preserve_spelling();
    head_legacy_etag_projection_preserves_spelling();
}

#[test]
fn head_stored_etag_facts_preserve_spelling() {
    for raw in [
        "abc",
        "\"abc\"",
        "W/\"abc\"",
        "W/\"\"",
        "\"\"",
        "",
        "*",
        " abc ",
        "abc_def",
        "a&b",
        "\"a&b\"",
        "\"a b\"",
        "\"a\\b\"",
        "\"a\"b\"",
        "w/\"abc\"",
        "\"abc",
        "W/abc",
        "a\rb",
        "a\nb",
        "a\0b",
        "é",
    ] {
        let expected = to_s3s_etag(raw);
        let fact = ResponseEtag::from_stored(raw);
        assert_eq!(fact.opaque, expected.value(), "{raw:?}");
        assert_eq!(fact.weak, expected.as_weak().is_some(), "{raw:?}");
    }
}

#[test]
fn head_legacy_etag_projection_preserves_spelling() {
    for weak in [false, true] {
        for opaque in ["", "abc", "a\\b", "a\"b", "a\rb", "é"] {
            let fact = ResponseEtag {
                opaque: opaque.to_owned(),
                weak,
            };
            let output = HeadObjectOutputFacts {
                dto: gateway_dto::HeadObjectOutput::default(),
                etag: Some(fact),
            }
            .into_s3s()
            .unwrap();
            let tag = output.e_tag.expect("legacy typed caller keeps the fact");
            assert_eq!(tag.value(), opaque);
            assert_eq!(tag.as_weak().is_some(), weak);
        }
    }
}

#[test]
fn head_proxy_etag_mapping_matches_the_legacy_parser() {
    let mut inputs = vec![
        "abc".to_owned(),
        String::new(),
        "*".to_owned(),
        " abc ".to_owned(),
        "abc_def".to_owned(),
        "W/\"\"".to_owned(),
        "W/abc".to_owned(),
        "w/\"abc\"".to_owned(),
        "\"a\\b\"".to_owned(),
        "\"a\tb\"".to_owned(),
        "\"a\"b\"".to_owned(),
    ];
    for byte in 0..=255u8 {
        inputs.push(format!("\"a{}b\"", char::from(byte)));
        inputs.push(format!("W/\"a{}b\"", char::from(byte)));
        inputs.push(char::from(byte).to_string());
    }
    for raw in inputs {
        let expected = crate::app::storage_api::s3::dto::ETag::from_str(&raw).ok();
        let sdk = aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
            .e_tag(&raw)
            .build();
        let facts = DefaultObjectUsecase::proxy_sdk_head_output(sdk);
        assert!(facts.dto.e_tag.is_none(), "ETag is carried as a response fact");
        assert_eq!(facts.into_s3s().unwrap().e_tag, expected, "{raw:?}");
    }
    let absent =
        DefaultObjectUsecase::proxy_sdk_head_output(aws_sdk_s3::operation::head_object::HeadObjectOutput::builder().build());
    assert!(absent.into_s3s().unwrap().e_tag.is_none());
}

#[derive(Clone, Copy)]
struct HeadEtagRow {
    stored: &'static str,
    opaque: &'static str,
    weak: bool,
    status: u16,
    header: Option<&'static [u8]>,
    conditional_status: u16,
    conditional_header: Option<&'static [u8]>,
}

const VALID_HEAD_ETAGS: &[HeadEtagRow] = &[
    HeadEtagRow {
        stored: "abc",
        opaque: "abc",
        weak: false,
        status: 200,
        header: Some(&[34, 97, 98, 99, 34]),
        conditional_status: 304,
        conditional_header: Some(&[34, 97, 98, 99, 34]),
    },
    HeadEtagRow {
        stored: "\"\"",
        opaque: "",
        weak: false,
        status: 200,
        header: Some(&[34, 34]),
        conditional_status: 500,
        conditional_header: None,
    },
    HeadEtagRow {
        stored: "W/\"\"",
        opaque: "",
        weak: true,
        status: 200,
        header: Some(&[87, 47, 34, 34]),
        conditional_status: 304,
        conditional_header: Some(&[34, 87, 47, 34]),
    },
    HeadEtagRow {
        stored: "\"a\\b\"",
        opaque: "a\\b",
        weak: false,
        status: 200,
        header: Some(&[34, 97, 92, 98, 34]),
        conditional_status: 304,
        conditional_header: Some(&[34, 97, 92, 98, 34]),
    },
    HeadEtagRow {
        stored: "\"a\"b\"",
        opaque: "a\"b",
        weak: false,
        status: 200,
        header: Some(&[34, 97, 34, 98, 34]),
        conditional_status: 304,
        conditional_header: Some(&[34, 97, 34, 98, 34]),
    },
    HeadEtagRow {
        stored: "é",
        opaque: "é",
        weak: false,
        status: 200,
        header: Some(&[34, 195, 169, 34]),
        conditional_status: 304,
        conditional_header: Some(&[34, 195, 169, 34]),
    },
];

const INVALID_HEAD_ETAGS: &[HeadEtagRow] = &[
    VALID_HEAD_ETAGS[0],
    HeadEtagRow {
        stored: "a\rb",
        opaque: "a\rb",
        weak: false,
        status: 500,
        header: None,
        conditional_status: 500,
        conditional_header: None,
    },
    HeadEtagRow {
        stored: "a\nb",
        opaque: "a\nb",
        weak: false,
        status: 500,
        header: None,
        conditional_status: 500,
        conditional_header: None,
    },
    HeadEtagRow {
        stored: "a\0b",
        opaque: "a\0b",
        weak: false,
        status: 500,
        header: None,
        conditional_status: 500,
        conditional_header: None,
    },
];

fn head_etag_wire_and_audit(stack: Option<&'static str>, rows: &'static [HeadEtagRow]) {
    temp_env::with_vars(
        [
            ("RUSTFS_S3_STACK", stack),
            ("RUSTFS_AUDIT_ENABLE", Some("true")),
            ("RUSTFS_NOTIFY_ENABLE", Some("false")),
        ],
        || {
            crate::app::gating_test_env::run_large_stack_test("head-etag-wire-audit", move || async move {
                use crate::app::storage_api::object_usecase::helper::MutationCompletionProbe;
                use crate::app::storage_api::test::ReqInfo;
                use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
                use crate::app::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
                let server = head_test_server().await;
                crate::server::refresh_audit_module_enabled();
                let context = crate::app::runtime_sources::current_app_context().unwrap();
                let store = context.object_store();
                let bucket = "head-etag-values";
                store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
                store.update_bucket_metadata_config(bucket, crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
            serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject"],"Resource":format!("arn:aws:s3:::{bucket}/*")}]})).unwrap()).await.unwrap();
                let probe = MutationCompletionProbe::install(bucket);
                let usecase = DefaultObjectUsecase::with_context(Some(context.clone()));
                let mut wire = Vec::new();
                let mut expected_wire = Vec::new();
                let mut expected_audits = Vec::new();
                for (index, row) in rows.iter().enumerate() {
                    let key = format!("etag-{index}");
                    store
                        .put_object(
                            bucket,
                            &key,
                            &mut PutObjReader::from_vec(b"payload".to_vec()),
                            &ObjectOptions {
                                preserve_etag: Some(row.stored.to_owned()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("store the exact ETag fixture");
                    let stored = store.get_object_info(bucket, &key, &ObjectOptions::default()).await.unwrap();
                    assert_eq!(stored.etag.as_deref(), Some(row.stored), "fixture must reach HEAD unchanged");
                    let mut request = head_input(bucket, &key, None);
                    request.extensions.insert(ReqInfo {
                        bucket: Some(bucket.to_owned()),
                        object: Some(key.clone()),
                        ..Default::default()
                    });
                    let internal = crate::app::test_seams::head_object(&usecase, request)
                        .await
                        .expect("unencoded legacy response");
                    let typed = internal.output.e_tag.expect("internal caller retains the ETag");
                    assert_eq!(typed.value(), row.opaque, "{}", row.stored);
                    assert_eq!(typed.as_weak().is_some(), row.weak, "{}", row.stored);
                    expected_audits.push((Some("s3:HeadObject".to_owned()), Some("success".to_owned()), Some(200)));
                    probe.wait_for(index * 3 + 1, 0).await;
                    for conditional in [false, true] {
                        let response = head_wire_response(&server, bucket, &key, conditional).await;
                        let status = response.status().as_u16();
                        let etag = response
                            .headers()
                            .get(http::header::ETAG)
                            .map(|value| value.as_bytes().to_vec());
                        let body = response.into_body();
                        probe.wait_for(index * 3 + if conditional { 3 } else { 2 }, 0).await;
                        wire.push((row.stored, conditional, status, etag, body.len()));
                        let (status, header) = if conditional {
                            (row.conditional_status, row.conditional_header)
                        } else {
                            (row.status, row.header)
                        };
                        expected_wire.push((row.stored, conditional, status, header.map(<[u8]>::to_vec), 0));
                        expected_audits.push((
                            Some("s3:HeadObject".to_owned()),
                            Some(if conditional { "failure" } else { "success" }.to_owned()),
                            Some(if conditional { 500 } else { 200 }),
                        ));
                    }
                }
                let audits = probe
                    .audits
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|entry| (entry.api.name.clone(), entry.api.status.clone(), entry.api.status_code))
                    .collect::<Vec<_>>();
                assert_eq!(audits, expected_audits, "encoding does not rewrite the completed use-case audit");
                assert_eq!(wire, expected_wire, "HeadObject preserves its legacy status and ETag bytes");
                server.shutdown().await;
            })
        },
    );
}

#[test]
#[serial_test::serial]
fn legacy_head_etag_wire_preserves_typed_values_and_audit() {
    head_etag_wire_and_audit(None, VALID_HEAD_ETAGS);
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn gateway_head_etag_wire_preserves_typed_values_and_audit() {
    head_etag_wire_and_audit(Some("gateway"), VALID_HEAD_ETAGS);
}

#[test]
#[serial_test::serial]
fn legacy_head_invalid_etag_is_rejected_after_audit() {
    head_etag_wire_and_audit(None, INVALID_HEAD_ETAGS);
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn gateway_head_invalid_etag_is_rejected_after_audit() {
    head_etag_wire_and_audit(Some("gateway"), INVALID_HEAD_ETAGS);
}

async fn head_test_server() -> crate::embedded::RustFSServer {
    use crate::embedded::{RustFSServerBuilder, find_available_port};
    RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{}", find_available_port().unwrap()))
        .access_key("headfixture")
        .secret_key("head-fixture-test-secret")
        .build()
        .await
        .unwrap()
}

async fn head_wire_response(
    server: &crate::embedded::RustFSServer,
    bucket: &str,
    key: &str,
    conditional: bool,
) -> http::Response<bytes::Bytes> {
    use http_body_util::BodyExt as _;
    use hyper_util::rt::TokioIo;
    let socket = tokio::net::TcpStream::connect(server.address()).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket)).await.unwrap();
    let transport = tokio::spawn(connection);
    let mut builder = http::Request::builder()
        .method(Method::HEAD)
        .uri(format!("/{bucket}/{key}"))
        .header(http::header::HOST, "localhost");
    if conditional {
        builder = builder.header(http::header::IF_NONE_MATCH, "*");
    }
    let response = sender
        .send_request(builder.body(http_body_util::Empty::<bytes::Bytes>::new()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    drop(sender);
    transport.abort();
    let _ = transport.await;
    http::Response::from_parts(parts, body)
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn gateway_head_authorization_uses_the_request_bound_context() {
    temp_env::with_var("RUSTFS_S3_STACK", Some("gateway"), || {
        crate::app::gating_test_env::run_large_stack_test("head-gateway-authorization", || async {
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            use crate::app::storage_api::test::contract::object::ObjectIO as _;
            use rustfs_io_metrics::s3_http_metrics::s3_http_metrics_snapshot;
            let server = head_test_server().await;
            let context = crate::app::runtime_sources::current_app_context().unwrap();
            let store = context.object_store();
            let bucket = "head-authorization";
            store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
            store
                .put_object(
                    bucket,
                    "object",
                    &mut PutObjReader::from_vec(b"payload".to_vec()),
                    &ObjectOptions::default(),
                )
                .await
                .unwrap();
            for (allow, deny, expected) in [(false, false, 403), (true, true, 403), (true, false, 200)] {
                let mut statements = vec![
                    serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/another-object")}),
                ];
                for (enabled, effect) in [(allow, "Allow"), (deny, "Deny")] {
                    if enabled {
                        statements.push(serde_json::json!({"Effect":effect,"Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")}));
                    }
                }
                store
                    .update_bucket_metadata_config(
                        bucket,
                        crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                    )
                    .await
                    .unwrap();
                let before: u64 = s3_http_metrics_snapshot().iter().map(|series| series.total).sum();
                let response = head_wire_response(&server, bucket, "object", false).await;
                let status = response.status().as_u16();
                let length = response.headers().get(http::header::CONTENT_LENGTH).cloned();
                assert!(response.body().is_empty());
                assert_eq!(status, expected, "allow={allow}, deny={deny}");
                if status == 200 {
                    assert_eq!(length.unwrap(), "7");
                }
                let after: u64 = s3_http_metrics_snapshot().iter().map(|series| series.total).sum();
                assert_eq!(after - before, 1, "one observed HEAD request");
            }
            server.shutdown().await;
        });
    });
}

fn head_conditional_headers_preserve_stored_facts(stack: Option<&'static str>) {
    temp_env::with_var("RUSTFS_S3_STACK", stack, || {
        crate::app::gating_test_env::run_large_stack_test("head-conditional-headers", || async {
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            use crate::app::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
            let server = head_test_server().await;
            let store = crate::app::runtime_sources::current_app_context().unwrap().object_store();
            let bucket = "head-cached-version";
            store
                .make_bucket(
                    bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            store.update_bucket_metadata_config(bucket, crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")}]})).unwrap()).await.unwrap();
            store
                .put_object(
                    bucket,
                    "object",
                    &mut PutObjReader::from_vec(b"payload".to_vec()),
                    &ObjectOptions {
                        versioned: true,
                        preserve_etag: Some("abc".to_owned()),
                        user_defined: HashMap::from([
                            ("cache-control".to_owned(), "max-age=7".to_owned()),
                            ("content-location".to_owned(), "/snapshot".to_owned()),
                            ("expires".to_owned(), "Wed, 21 Oct 2037 07:28:00 GMT".to_owned()),
                            ("vary".to_owned(), "Origin, Accept-Encoding".to_owned()),
                        ]),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let stored = store
                .get_object_info(bucket, "object", &ObjectOptions::default())
                .await
                .unwrap();
            let version = stored.version_id.expect("versioned fixture").to_string();
            let modified = stored.mod_time.expect("stored modification time").format(RFC1123).unwrap();
            let ordinary = head_wire_response(&server, bucket, "object", false).await;
            assert_eq!(ordinary.status(), http::StatusCode::OK);
            assert_eq!(ordinary.headers()[http::header::CONTENT_LENGTH], "7");
            for key in ["object".to_owned(), format!("object?versionId={version}")] {
                let response = head_wire_response(&server, bucket, &key, true).await;
                assert_eq!(response.status(), http::StatusCode::NOT_MODIFIED, "{key}");
                for (name, expected) in [
                    ("etag", "\"abc\""),
                    ("last-modified", modified.as_str()),
                    ("cache-control", "max-age=7"),
                    ("content-location", "/snapshot"),
                    ("expires", "Wed, 21 Oct 2037 07:28:00 GMT"),
                    ("vary", "Origin, Accept-Encoding"),
                    ("x-amz-version-id", version.as_str()),
                ] {
                    assert_eq!(
                        response.headers().get(name).map(http::HeaderValue::as_bytes),
                        Some(expected.as_bytes()),
                        "{key}: {name}"
                    );
                }
                assert!(response.body().is_empty());
            }
            server.shutdown().await;
        });
    });
}

#[test]
#[serial_test::serial]
fn legacy_head_conditional_headers_preserve_stored_facts() {
    head_conditional_headers_preserve_stored_facts(None);
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn gateway_head_conditional_headers_preserve_stored_facts() {
    head_conditional_headers_preserve_stored_facts(Some("gateway"));
}
