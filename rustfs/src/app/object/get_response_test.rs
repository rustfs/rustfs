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

//! GetObject typed, wire, completion, and body-handoff regression contracts.
//! Expected responses were measured on a279b4b before the owned migration.

use super::*;
use crate::app::storage_api::s3::dto::Range;
use crate::app::storage_api::test::ReqInfo;
use http::Method;

fn get_request(input: GetObjectInput) -> S3Request<GetObjectInput> {
    let info = ReqInfo {
        bucket: Some(input.bucket.clone()),
        object: Some(input.key.clone()),
        version_id: input.version_id.clone(),
        ..Default::default()
    };
    let mut request = build_request(input, Method::GET);
    request.extensions.insert(info);
    request
}

async fn wire_get(
    server: &crate::embedded::RustFSServer,
    path: &str,
    headers: &[(&str, &str)],
    admitted: bool,
) -> http::Response<Bytes> {
    use http_body_util::BodyExt as _;
    use hyper_util::rt::TokioIo;
    let before = operation_counts();
    let socket = tokio::net::TcpStream::connect(server.address()).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket)).await.unwrap();
    let transport = tokio::spawn(connection);
    let mut builder = http::Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(http::header::HOST, "localhost");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = sender
        .send_request(builder.body(http_body_util::Empty::<Bytes>::new()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    drop(sender);
    transport.abort();
    let _ = transport.await;
    let delta: std::collections::BTreeMap<_, _> = operation_counts()
        .into_iter()
        .filter_map(|(op, count)| {
            let delta = count - before.get(op).copied().unwrap_or(0);
            (delta > 0).then_some((op, delta))
        })
        .collect();
    let expected = if admitted {
        std::collections::BTreeMap::from([(rustfs_s3_ops::S3Operation::GetObject.as_str(), 1)])
    } else {
        std::collections::BTreeMap::new()
    };
    if !admitted {
        assert_eq!(parts.status, StatusCode::FORBIDDEN, "authorization refusal for {path}");
    }
    assert_eq!(delta, expected, "the admitted operation is counted once, after authorization");
    http::Response::from_parts(parts, body)
}

fn operation_counts() -> std::collections::BTreeMap<&'static str, u64> {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .map(|row| (row.op, row.total))
        .collect()
}

#[test]
#[serial_test::serial]
fn get_boundary_legacy() {
    get_boundary(None);
}

#[cfg(feature = "gateway-stack")]
#[test]
#[serial_test::serial]
fn get_boundary_gateway() {
    get_boundary(Some("gateway"));
}

fn get_boundary(stack: Option<&str>) {
    temp_env::with_vars(
        [
            ("RUSTFS_S3_STACK", stack),
            ("RUSTFS_AUDIT_ENABLE", Some("true")),
            ("RUSTFS_SSE_C_REQUIRE_TLS", Some("false")),
        ],
        || {
            crate::app::gating_test_env::run_large_stack_test("get-boundary-observations", || async {
                use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
                use crate::app::storage_api::test::contract::object::{ObjectIO as _, ObjectOperations as _};
                use crate::embedded::{RustFSServerBuilder, find_available_port};
                let server = RustFSServerBuilder::new()
                    .address(format!("127.0.0.1:{}", find_available_port().unwrap()))
                    .access_key("getfixture")
                    .secret_key("get-fixture-test-secret")
                    .build()
                    .await
                    .unwrap();
                crate::server::refresh_audit_module_enabled();
                let context = crate::app::runtime_sources::current_app_context().unwrap();
                let store = context.object_store();
                let usecase = DefaultObjectUsecase::with_context(Some(context));
                let bucket = "get-boundary-facts";
                let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(bucket);
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
                for (index, etag) in ["abc", "W/\"\"", "\"a\\b\"", "\"a\"b\"", "é", "a\rb", "a\nb", "a\0b"]
                    .iter()
                    .enumerate()
                {
                    let key = format!("object-{index}");
                    let info = store
                        .put_object(
                            bucket,
                            &key,
                            &mut PutObjReader::from_vec(b"payload".to_vec()),
                            &ObjectOptions {
                                versioned: true,
                                preserve_etag: Some((*etag).to_owned()),
                                user_defined: HashMap::from([
                                    ("cache-control".to_owned(), "max-age=7".to_owned()),
                                    ("content-location".to_owned(), "/snapshot".to_owned()),
                                    ("expires".to_owned(), "Wed, 21 Oct 2037 07:28:00 GMT".to_owned()),
                                    ("vary".to_owned(), "Origin".to_owned()),
                                ]),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    let request = get_request(GetObjectInput {
                        bucket: bucket.to_owned(),
                        key: key.clone(),
                        ..Default::default()
                    });
                    let response = crate::app::test_seams::get_object(&usecase, request).await.unwrap();
                    let tag = response.output.e_tag.unwrap();
                    assert_eq!(tag.value(), ["abc", "", "a\\b", "a\"b", "é", "a\rb", "a\nb", "a\0b"][index]);
                    assert_eq!(tag.as_weak().is_some(), index == 1);
                    assert_eq!(response.status, None);
                    for (case, suffix, headers) in [
                        ("ordinary", String::new(), vec![]),
                        ("latest304", String::new(), vec![("if-none-match", "*")]),
                        (
                            "version304",
                            format!("?versionId={}", info.version_id.unwrap()),
                            vec![("if-none-match", "*")],
                        ),
                        ("precondition", String::new(), vec![("if-match", "\"different\"")]),
                        ("range", String::new(), vec![("range", "bytes=1-3")]),
                        ("unsatisfied", String::new(), vec![("range", "bytes=99-100")]),
                    ] {
                        let range = match case {
                            "range" => Some(Range::Int { first: 1, last: Some(3) }),
                            "unsatisfied" => Some(Range::Int {
                                first: 99,
                                last: Some(100),
                            }),
                            _ => None,
                        };
                        let input = GetObjectInput {
                            bucket: bucket.to_owned(),
                            key: key.clone(),
                            range,
                            version_id: (case == "version304").then(|| info.version_id.unwrap().to_string()),
                            ..Default::default()
                        };
                        let mut request = get_request(input);
                        for (name, value) in &headers {
                            request.headers.insert(
                                http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                                HeaderValue::from_str(value).unwrap(),
                            );
                        }
                        match crate::app::test_seams::get_object(&usecase, request).await {
                            Ok(response) => {
                                assert!(matches!(case, "ordinary" | "range"));
                                assert_eq!(response.status, None);
                                assert_eq!(response.output.content_range.as_deref(), (case == "range").then_some("bytes 1-3/7"));
                            }
                            Err(error) => {
                                let expected = match case {
                                    "latest304" | "version304" if index >= 5 => 500,
                                    "latest304" | "version304" => 304,
                                    "precondition" => 412,
                                    "unsatisfied" => 416,
                                    _ => panic!("unexpected typed error: {case}: {error}"),
                                };
                                assert_eq!(error.status_code().unwrap().as_u16(), expected);
                                if expected == 304 {
                                    assert_eq!(error.code(), &S3ErrorCode::NotModified);
                                    assert_conditional_headers(error.headers().unwrap(), index, &info);
                                } else {
                                    assert!(error.headers().is_none());
                                    assert_eq!(
                                        error.code(),
                                        &match expected {
                                            412 => S3ErrorCode::PreconditionFailed,
                                            416 => S3ErrorCode::InvalidRange,
                                            _ => S3ErrorCode::InternalError,
                                        }
                                    );
                                }
                            }
                        }
                        let response = wire_get(&server, &format!("/{bucket}/{key}{suffix}"), &headers, true).await;
                        let expected = match case {
                            "ordinary" | "range" | "latest304" | "version304" if index >= 5 => 500,
                            "ordinary" => 200,
                            "range" => 206,
                            "latest304" | "version304" => 304,
                            "precondition" => 412,
                            "unsatisfied" => 416,
                            _ => unreachable!(),
                        };
                        assert_eq!(response.status().as_u16(), expected, "{index}: {case}");
                        match expected {
                            200 | 206 => {
                                assert_eq!(response.body().as_ref(), if expected == 200 { &b"payload"[..] } else { &b"ayl"[..] });
                                assert_eq!(response.headers()[http::header::ETAG].as_bytes(), normal_etag(index));
                                assert_eq!(
                                    response.headers().get(http::header::CONTENT_RANGE).map(HeaderValue::as_bytes),
                                    (expected == 206).then_some(&b"bytes 1-3/7"[..])
                                );
                            }
                            304 => {
                                assert!(response.body().is_empty());
                                assert_conditional_headers(response.headers(), index, &info);
                            }
                            412 => assert_error_body(&response, "PreconditionFailed"),
                            416 => {
                                assert_error_body(&response, "InvalidRange");
                                assert!(!response.headers().contains_key(http::header::CONTENT_RANGE));
                            }
                            _ => assert_error_body(&response, "InternalError"),
                        };
                    }
                    probe.wait_for((index + 1) * 13, 0).await;
                }
                let audits = probe
                    .audits
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|entry| (entry.api.name.clone(), entry.api.status.clone(), entry.api.status_code))
                    .collect::<Vec<_>>();
                assert_eq!(audits.len(), 8 * 13);
                for (index, row) in audits.as_chunks::<13>().0.iter().enumerate() {
                    let conditional = if index < 5 { 304 } else { 500 };
                    let statuses = [
                        200,
                        200,
                        200,
                        conditional,
                        conditional,
                        conditional,
                        conditional,
                        412,
                        412,
                        200,
                        200,
                        416,
                        416,
                    ];
                    for ((name, state, code), expected) in row.iter().zip(statuses) {
                        assert_eq!(name.as_deref(), Some("s3:GetObject"));
                        assert_eq!(*code, Some(expected));
                        assert_eq!(state.as_deref(), Some(if expected == 200 { "success" } else { "failure" }));
                    }
                }
                for (key, part, range) in [
                    ("", Some(0), None),
                    ("object-0", Some(0), None),
                    ("object-0", Some(1), Some(Range::Int { first: 0, last: Some(1) })),
                ] {
                    let input = GetObjectInput {
                        bucket: "missing-boundary-bucket".to_owned(),
                        key: key.to_owned(),
                        part_number: part,
                        range,
                        ..Default::default()
                    };
                    let Err(error) = crate::app::test_seams::get_object(&usecase, get_request(input)).await else {
                        panic!("invalid input must fail")
                    };
                    assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
                    assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
                    assert_eq!(
                        error.message(),
                        Some(if key.is_empty() {
                            "Invalid object key"
                        } else if part == Some(0) {
                            "GET: partNumber must be between 1 and 10000"
                        } else {
                            "range and part_number invalid"
                        })
                    );
                }
                let response = wire_get(&server, &format!("/{bucket}/missing"), &[], true).await;
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
                assert_error_body(&response, "NoSuchKey");
                signed_override_contract(&server, bucket).await;
                for (case, headers) in [
                    ("ordinary", vec![]),
                    ("condition", vec![("if-none-match", "*")]),
                    ("failed-match", vec![("if-match", "\"different\"")]),
                    ("range", vec![("range", "bytes=1-3")]),
                ] {
                    let response = wire_get(&server, &format!("/{bucket}/missing"), &headers, true).await;
                    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{case}");
                    assert_error_body(&response, "NoSuchKey");
                }
                let marker = store
                    .delete_object(
                        bucket,
                        "object-0",
                        ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                for path in [
                    format!("/{bucket}/object-0"),
                    format!("/{bucket}/object-0?versionId={}", marker.version_id.unwrap()),
                ] {
                    let response = wire_get(&server, &path, &[], true).await;
                    let explicit = path.contains('?');
                    assert_eq!(response.status().as_u16(), if explicit { 405 } else { 404 });
                    assert_eq!(response.headers()["x-amz-delete-marker"], "true");
                    assert_eq!(response.headers()["x-amz-version-id"], marker.version_id.unwrap().to_string());
                    assert_eq!(response.headers().contains_key(http::header::LAST_MODIFIED), explicit);
                    assert_error_body(&response, if explicit { "MethodNotAllowed" } else { "NoSuchKey" });
                }
                ssec_wire_contract(&usecase, &server, bucket).await;
                for reserved in ["sthree-get-boundary", "get-boundary-s3alias"] {
                    store.make_bucket(reserved, &MakeBucketOptions::default()).await.unwrap();
                    store.update_bucket_metadata_config(reserved, crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{reserved}/*")}]})).unwrap()).await.unwrap();
                    store
                        .put_object(
                            reserved,
                            "object",
                            &mut PutObjReader::from_vec(b"payload".to_vec()),
                            &ObjectOptions::default(),
                        )
                        .await
                        .unwrap();
                    let typed = crate::app::test_seams::get_object(
                        &usecase,
                        get_request(GetObjectInput {
                            bucket: reserved.to_owned(),
                            key: "object".to_owned(),
                            ..Default::default()
                        }),
                    )
                    .await
                    .unwrap();
                    let wire = wire_get(&server, &format!("/{reserved}/object"), &[], true).await;
                    assert_eq!(typed.status, None);
                    assert_eq!(wire.status(), StatusCode::OK);
                    assert_eq!(wire.body().as_ref(), b"payload");
                }
                store.update_bucket_metadata_config(bucket, crate::app::storage_api::object_usecase::bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")}]})).unwrap()).await.unwrap();
                for key in ["object-1", "missing"] {
                    for (case, headers) in [
                        ("ordinary", vec![]),
                        ("condition", vec![("if-none-match", "*")]),
                        ("failed-match", vec![("if-match", "\"different\"")]),
                        ("range", vec![("range", "bytes=1-3")]),
                    ] {
                        let response = wire_get(&server, &format!("/{bucket}/{key}"), &headers, false).await;
                        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{key}: {case}");
                        assert_error_body(&response, "AccessDenied");
                    }
                }
                server.shutdown().await;
            });
        },
    );
}

async fn signed_override_contract(server: &crate::embedded::RustFSServer, bucket: &str) {
    use aws_sdk_s3::config::{Credentials, Region};
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .credentials_provider(Credentials::new(server.access_key(), server.secret_key(), None, None, "test"))
            .region(Region::new("us-east-1"))
            .endpoint_url(server.endpoint())
            .force_path_style(true)
            .behavior_version_latest()
            .build(),
    );
    for (case, conditional, ranged) in [
        ("ordinary", false, false),
        ("range", false, true),
        ("conditional", true, false),
    ] {
        let mut request = client
            .get_object()
            .bucket(bucket)
            .key("object-0")
            .response_cache_control("max-age=11")
            .response_content_type("text/plain; charset=utf-8")
            .response_content_disposition("attachment; filename=\"a b.txt\"")
            .response_content_language("en-US")
            .response_content_encoding("identity")
            .response_expires(aws_sdk_s3::primitives::DateTime::from_secs(1700000000));
        if conditional {
            request = request.if_none_match("*");
        }
        if ranged {
            request = request.range("bytes=1-3");
        }
        match request.send().await {
            Ok(output) => {
                assert!(!conditional);
                assert_eq!(output.content_type(), Some("text/plain; charset=utf-8"));
                assert_eq!(output.cache_control(), Some("max-age=11"));
                assert_eq!(output.content_disposition(), Some("attachment; filename=\"a b.txt\""));
                assert_eq!(output.content_language(), Some("en-US"));
                assert_eq!(output.content_encoding(), Some("identity"));
                assert_eq!(output.expires_string(), Some("Tue, 14 Nov 2023 22:13:20 GMT"));
                assert_eq!(output.content_range(), ranged.then_some("bytes 1-3/7"));
                let body = output.body.collect().await.unwrap().into_bytes();
                assert_eq!(body.as_ref(), if ranged { &b"ayl"[..] } else { &b"payload"[..] }, "{case}");
            }
            Err(error) => {
                let response = error.raw_response().unwrap();
                assert!(conditional, "{case}: {error}");
                assert_eq!(response.status().as_u16(), 304);
                assert_eq!(response.headers().get("cache-control"), Some("max-age=7"));
                assert_eq!(response.headers().get("expires"), Some("Wed, 21 Oct 2037 07:28:00 GMT"));
            }
        }
    }
}

async fn ssec_wire_contract(usecase: &DefaultObjectUsecase, server: &crate::embedded::RustFSServer, bucket: &str) {
    use crate::app::storage_api::s3::dto::PutObjectInput;
    use md5::{Digest as _, Md5};
    let key = [0x2a_u8; 32];
    let encoded = base64_simd::STANDARD.encode_to_string(key);
    let digest = base64_simd::STANDARD.encode_to_string(Md5::digest(key));
    let wrong = [0x2b_u8; 32];
    let wrong_encoded = base64_simd::STANDARD.encode_to_string(wrong);
    let wrong_digest = base64_simd::STANDARD.encode_to_string(Md5::digest(wrong));
    let input = PutObjectInput::builder()
        .bucket(bucket.to_owned())
        .key("encrypted".to_owned())
        .body(Some(StreamingBlob::from_bytes(Bytes::from_static(b"protected payload"))))
        .content_length(Some(17))
        .sse_customer_algorithm(Some("AES256".to_owned()))
        .sse_customer_key(Some(encoded.clone()))
        .sse_customer_key_md5(Some(digest.clone()))
        .build()
        .unwrap();
    crate::app::test_seams::put_object(usecase, &FS::new(), build_request(input, Method::PUT))
        .await
        .unwrap();
    for (case, valid, wrong_key, condition, range) in [
        ("ordinary", true, false, false, false),
        ("missing", false, false, false, false),
        ("wrong", true, true, false, false),
        ("missing-conditional", false, false, true, false),
        ("missing-range", false, false, false, true),
        ("wrong-conditional", true, true, true, false),
        ("wrong-range", true, true, false, true),
        ("conditional", true, false, true, false),
        ("range", true, false, false, true),
    ] {
        let mut headers = Vec::new();
        if valid {
            headers.extend([
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    if wrong_key { wrong_encoded.as_str() } else { encoded.as_str() },
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    if wrong_key { wrong_digest.as_str() } else { digest.as_str() },
                ),
            ]);
        }
        if condition {
            headers.push(("if-none-match", "*"));
        }
        if range {
            headers.push(("range", "bytes=1-3"));
        }
        let response = wire_get(server, &format!("/{bucket}/encrypted"), &headers, true).await;
        if !valid || wrong_key {
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{case}");
            assert_error_body(&response, "InvalidRequest");
            let message = if wrong_key {
                "The provided encryption parameters did not match the ones used originally to encrypt the object."
            } else {
                "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
            };
            assert!(String::from_utf8_lossy(response.body()).contains(message));
        } else {
            assert_eq!(
                response.status().as_u16(),
                if condition {
                    304
                } else if range {
                    206
                } else {
                    200
                }
            );
            assert_eq!(
                response.body().as_ref(),
                if condition {
                    &b""[..]
                } else if range {
                    &b"rot"[..]
                } else {
                    &b"protected payload"[..]
                }
            );
        }
    }
}

#[derive(Clone)]
struct CopyProbeReader {
    remaining: usize,
    writes: Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
}

impl tokio::io::AsyncRead for CopyProbeReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let length = self.remaining.min(buf.remaining());
        if length > 0 {
            let target = buf.initialize_unfilled_to(length);
            self.writes.lock().unwrap().push((target.as_ptr() as usize, length));
            target.fill(b'x');
            buf.advance(length);
            self.remaining -= length;
        }
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn get_reader_handoff_preserves_baseline_copy_count() {
    for length in [0, 1, 4097, 131073] {
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = CopyProbeReader {
            remaining: length,
            writes: Arc::clone(&writes),
        };
        let mut body = DefaultObjectUsecase::build_reader_blob(
            reader,
            length as i64,
            "copy-probe",
            None,
            4096,
            GetObjectStreamStrategy::Standard,
            "copy-probe-bucket",
            "object",
            GetObjectBodyLifecycle::disabled(),
            None,
        )
        .await
        .unwrap()
        .into_s3s();
        let mut chunks = Vec::new();
        let mut observed_writes = 0;
        let mut retained = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.unwrap();
            let begin = chunk.as_ptr() as usize;
            let end = begin + chunk.len();
            let writes = writes.lock().unwrap();
            let mut intervals: Vec<_> = writes[observed_writes..]
                .iter()
                .map(|(write, count)| (begin.max(*write), end.min(write + count)))
                .filter(|(first, last)| first < last)
                .collect();
            intervals.sort_unstable();
            let mut cursor = begin;
            for (first, last) in intervals {
                retained += last.saturating_sub(cursor.max(first));
                cursor = cursor.max(last);
            }
            observed_writes = writes.len();
            chunks.push(chunk);
        }
        let writes = writes.lock().unwrap();
        let total: usize = chunks.iter().map(Bytes::len).sum();
        assert_eq!(total, length);
        assert!(chunks.iter().all(|chunk| chunk.iter().all(|byte| *byte == b'x')));
        assert_eq!(
            total.saturating_sub(retained),
            usize::from(length > 0),
            "only the established primed byte is copied"
        );
        assert_eq!(writes.len(), chunks.len());
    }
}

#[tokio::test]
#[serial_test::serial]
async fn get_memory_handoff_retains_the_original_allocation() {
    for once in ["false", "true"] {
        let bytes = Bytes::from(vec![b'm'; 8193]);
        let address = bytes.as_ptr() as usize;
        let mut body = temp_env::with_var(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, Some(once), || {
            DefaultObjectUsecase::build_memory_bytes_blob(
                bytes.clone(),
                bytes.len() as i64,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                GetObjectBodyLifecycle::disabled(),
            )
        })
        .into_s3s();
        let mut chunks = Vec::new();
        while let Some(chunk) = body.next().await {
            chunks.push(chunk.unwrap());
        }
        let retained: usize = chunks
            .iter()
            .map(|chunk| {
                let start = chunk.as_ptr() as usize;
                (start + chunk.len())
                    .min(address + bytes.len())
                    .saturating_sub(start.max(address))
            })
            .sum();
        assert_eq!(chunks.iter().map(Bytes::len).sum::<usize>(), bytes.len());
        assert_eq!(retained, bytes.len());
        assert_eq!(retained, 8193, "{once}");
    }
}

fn normal_etag(index: usize) -> &'static [u8] {
    [b"\"abc\"".as_slice(), b"W/\"\"", b"\"a\\b\"", b"\"a\"b\"", b"\"\xc3\xa9\""][index]
}

fn assert_conditional_headers(headers: &HeaderMap, index: usize, info: &ObjectInfo) {
    let etag = if index == 1 { &b"\"W/\""[..] } else { normal_etag(index) };
    assert_eq!(headers[http::header::ETAG].as_bytes(), etag);
    assert_eq!(headers[http::header::CACHE_CONTROL], "max-age=7");
    assert_eq!(headers[http::header::CONTENT_LOCATION], "/snapshot");
    assert_eq!(headers[http::header::VARY], "Origin");
    assert_eq!(headers[http::header::EXPIRES], "Wed, 21 Oct 2037 07:28:00 GMT");
    assert_eq!(headers["x-amz-version-id"], info.version_id.unwrap().to_string());
    assert_eq!(headers[http::header::LAST_MODIFIED], info.mod_time.unwrap().format(RFC1123).unwrap());
}

fn assert_error_body(response: &http::Response<Bytes>, code: &str) {
    let body = String::from_utf8_lossy(response.body());
    assert!(body.contains(&format!("<Code>{code}</Code>")), "{body}");
    let message = match code {
        "InternalError" => Some("We encountered an internal error. Please try again."),
        "NoSuchKey" => Some("The specified key does not exist."),
        "AccessDenied" => Some("Access Denied"),
        "PreconditionFailed" => Some("At least one of the preconditions you specified did not hold."),
        "InvalidRange" => Some("The requested range is not satisfiable"),
        _ => None,
    };
    if let Some(message) = message {
        assert!(body.contains(&format!("<Message>{message}</Message>")), "{body}");
    }
}
