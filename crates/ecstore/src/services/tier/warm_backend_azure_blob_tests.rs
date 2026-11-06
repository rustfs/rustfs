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

use super::*;
use crate::services::tier::warm_backend::{WarmBackendImpl, check_warm_backend};
use rustfs_utils::http::metadata_compat::insert_str;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    disconnect: bool,
}

fn reply(status: StatusCode, headers: &[(&str, &str)], body: &[u8]) -> Reply {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        insert_header(&mut map, name, value).expect("fixture headers should be valid");
    }
    Reply {
        status,
        headers: map,
        body: Bytes::copy_from_slice(body),
        disconnect: false,
    }
}

fn present(version: Option<&str>) -> Reply {
    let mut response = reply(StatusCode::OK, &[("content-length", "6"), ("etag", "\"original\"")], b"");
    if let Some(version) = version {
        insert_header(&mut response.headers, "x-ms-version-id", version).expect("fixture version should be valid");
    }
    response
}

fn missing() -> Reply {
    reply(StatusCode::NOT_FOUND, &[("x-ms-error-code", "BlobNotFound")], b"")
}

#[derive(Debug)]
struct Captured {
    method: String,
    target: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

async fn capture(stream: &mut TcpStream) -> Captured {
    let mut bytes = Vec::new();
    let boundary = loop {
        let mut buffer = [0_u8; 4096];
        let count = stream.read(&mut buffer).await.expect("fixture request should be readable");
        assert_ne!(count, 0, "request must include a complete HTTP head");
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(bytes.len() <= 32 * 1024, "request headers must remain bounded");
    };
    let text = std::str::from_utf8(&bytes[..boundary]).expect("request headers should be ASCII");
    let mut lines = text.split("\r\n");
    let mut request_line = lines.next().expect("request line").split_whitespace();
    let method = request_line.next().expect("request method").to_string();
    let target = request_line.next().expect("request target").to_string();
    let mut headers = HeaderMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').expect("request header should include a colon");
        insert_header(&mut headers, name, value.trim()).expect("request headers should parse");
    }
    let length = headers
        .get("content-length")
        .map(|value| {
            value
                .to_str()
                .expect("length should be ASCII")
                .parse::<usize>()
                .expect("length should be numeric")
        })
        .unwrap_or_default();
    assert!(length <= 17 * 1024 * 1024, "fixture only accepts its bounded test bodies");
    let mut body = bytes[boundary..].to_vec();
    let received = body.len();
    body.resize(length, 0);
    stream
        .read_exact(&mut body[received..])
        .await
        .expect("request body should match its declared length");
    Captured {
        method,
        target,
        headers,
        body,
    }
}

async fn fixture(responses: Vec<Reply>) -> (WarmBackendAzureBlob, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener should bind");
    let address = listener.local_addr().expect("fixture address");
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for mut response in responses {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .expect("expected request must arrive")
                .expect("fixture should accept");
            requests.push(capture(&mut stream).await);
            if response.disconnect {
                continue;
            }
            if !response.headers.contains_key("content-length") {
                insert_header(&mut response.headers, "content-length", &response.body.len().to_string())
                    .expect("fixture length should be valid");
            }
            let mut wire = format!(
                "HTTP/1.1 {} {}\r\nConnection: close\r\n",
                response.status.as_u16(),
                response.status.canonical_reason().unwrap_or("")
            );
            for (name, value) in &response.headers {
                wire.push_str(&format!("{name}: {}\r\n", value.to_str().expect("fixture header is ASCII")));
            }
            wire.push_str("\r\n");
            stream.write_all(wire.as_bytes()).await.expect("fixture should send headers");
            stream.write_all(&response.body).await.expect("fixture should send body");
        }
        requests
    });
    rustfs_tls_runtime::ensure_crypto_provider().expect("fixture TLS provider should initialize");
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .read_timeout(Duration::from_secs(5))
        .build()
        .expect("fixture client should build");
    (
        WarmBackendAzureBlob {
            client,
            endpoint: Url::parse(&format!("http://{address}")).expect("fixture endpoint"),
            account: "account".to_string(),
            key: vec![7; 32],
            container: "container".to_string(),
            prefix: "cold".to_string(),
            request_timeout: Duration::from_secs(5),
        },
        task,
    )
}

#[tokio::test]
async fn mutation_probe_uses_blob_shared_key_and_cleans_up_with_an_etag() {
    let responses = vec![
        reply(StatusCode::CREATED, &[], b""),
        present(None),
        reply(StatusCode::PARTIAL_CONTENT, &[("content-range", "bytes 0-5/6")], b"RustFS"),
        present(None),
        reply(StatusCode::ACCEPTED, &[], b""),
        missing(),
    ];
    let (backend, task) = fixture(responses).await;
    let backend: WarmBackendImpl = Box::new(backend);
    check_warm_backend(Some(&backend))
        .await
        .expect("native Blob mutation probe should succeed");
    let requests = task.await.expect("fixture should join");
    assert_eq!(
        requests.iter().map(|request| request.method.as_str()).collect::<Vec<_>>(),
        ["PUT", "HEAD", "GET", "HEAD", "DELETE", "HEAD"]
    );
    assert!(requests[0].target.starts_with("/container/cold/rustfs-tier-probe-"));
    assert_eq!(requests[0].body, b"RustFS");
    assert_eq!(requests[0].headers["x-ms-blob-type"], "BlockBlob");
    assert_eq!(requests[0].headers["if-none-match"], "*");
    assert_eq!(requests[4].headers["if-match"], "\"original\"");
    for request in &requests {
        assert_eq!(request.target, requests[0].target, "the whole probe must address the same blob");
        assert_eq!(request.headers["x-ms-version"], API_VERSION);
        assert!(
            request.headers["authorization"]
                .to_str()
                .expect("authorization is ASCII")
                .starts_with("SharedKey account:")
        );
        assert!(!request.headers.contains_key("x-amz-date"));
        assert!(!request.target.contains("location"));
        assert!(!request.target.contains("versionid"));
    }
}

#[tokio::test]
async fn rejected_put_preserves_azure_error_and_never_deletes_an_existing_blob() {
    for (status, code) in [
        (StatusCode::BAD_REQUEST, "InvalidHeaderValue"),
        (StatusCode::PRECONDITION_FAILED, "ConditionNotMet"),
    ] {
        let (backend, task) = fixture(vec![reply(
            status,
            &[("x-ms-error-code", code), ("x-ms-request-id", "abc-123")],
            b"<Error><Code>PRIVATE-RESPONSE-BODY</Code></Error>",
        )])
        .await;
        let backend: WarmBackendImpl = Box::new(backend);
        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("deterministic rejected write must fail");
        assert!(err.message.contains(code));
        assert!(err.message.contains("abc-123"));
        assert!(!err.message.contains("PRIVATE-RESPONSE-BODY"));
        assert!(!err.message.contains("cleanup is incomplete"));
        let requests = task.await.expect("fixture should join");
        assert_eq!(requests.len(), 1, "a rejected conditional PUT must not trigger cleanup");
    }
}

#[tokio::test]
async fn empty_version_headers_use_the_unversioned_read_and_delete_paths() {
    let mut head = present(Some(""));
    head.headers.insert("content-length", HeaderValue::from_static("0"));
    let (backend, task) = fixture(vec![
        reply(StatusCode::CREATED, &[("x-ms-version-id", "")], b""),
        reply(StatusCode::OK, &[("x-ms-version-id", "")], b""),
        head,
        reply(StatusCode::ACCEPTED, &[], b""),
    ])
    .await;
    let version = backend
        .put("empty", ReaderImpl::Body(Bytes::new()), 0)
        .await
        .expect("empty version is unversioned");
    assert!(version.is_empty());
    let mut reader = backend
        .get("empty", &version, WarmBackendGetOpts::default())
        .await
        .expect("unversioned read");
    let mut body = Vec::new();
    reader.read_to_end(&mut body).await.expect("empty blob should read");
    assert!(body.is_empty());
    backend
        .remove("empty", &version)
        .await
        .expect("unversioned conditional delete");
    for request in task.await.expect("fixture should join") {
        assert_eq!(request.target, "/container/cold/empty");
    }
}

#[tokio::test]
async fn rejected_credentials_are_reported_without_an_uncertain_cleanup() {
    let (backend, task) = fixture(vec![reply(
        StatusCode::FORBIDDEN,
        &[("x-ms-error-code", "AuthenticationFailed")],
        b"",
    )])
    .await;
    let backend: WarmBackendImpl = Box::new(backend);
    let err = check_warm_backend(Some(&backend))
        .await
        .expect_err("invalid credentials must fail");
    assert_eq!(err.code, crate::services::tier::tier_handlers::ERR_TIER_INVALID_CREDENTIALS.code);
    assert_eq!(task.await.expect("fixture should join").len(), 1);
}

#[tokio::test]
async fn lost_put_response_is_not_replayed_and_its_candidate_is_cleaned() {
    let mut lost = reply(StatusCode::CREATED, &[], b"");
    lost.disconnect = true;
    let (backend, task) = fixture(vec![
        lost,
        present(None),
        present(None),
        reply(StatusCode::ACCEPTED, &[], b""),
        missing(),
        missing(),
    ])
    .await;
    let backend: WarmBackendImpl = Box::new(backend);
    let err = check_warm_backend(Some(&backend))
        .await
        .expect_err("a lost write acknowledgement must remain a failure");
    assert!(!err.message.contains("cleanup is incomplete"));
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests.iter().filter(|request| request.method == "PUT").count(), 1);
    assert_eq!(requests.iter().filter(|request| request.method == "DELETE").count(), 1);
}

#[tokio::test]
async fn block_upload_commits_only_the_complete_declared_body() {
    let (backend, task) = fixture(vec![
        reply(StatusCode::CREATED, &[], b""),
        reply(StatusCode::CREATED, &[], b""),
        reply(StatusCode::CREATED, &[], b""),
    ])
    .await;
    let length = usize::try_from(MIN_BLOCK_BYTES + 5).expect("fixture size");
    let bytes = Bytes::from(vec![42; length]);
    assert_eq!(
        backend
            .put(
                "large",
                ReaderImpl::ObjectBody(ReadCloser::new(Cursor::new(bytes.clone()))),
                i64::try_from(length).expect("fixture length")
            )
            .await
            .expect("block upload"),
        ""
    );
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests.len(), 3);
    let mut block_ids = Vec::new();
    let mut body = Vec::new();
    for request in &requests[..2] {
        let url = Url::parse(&format!("http://fixture{}", request.target)).expect("captured URL");
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query["comp"], "block");
        block_ids.push(query["blockid"].clone());
        assert!(request.headers.contains_key("content-md5"));
        body.extend_from_slice(&request.body);
    }
    assert_eq!(body, bytes);
    assert_ne!(block_ids[0], block_ids[1]);
    assert!(requests[2].target.ends_with("?comp=blocklist"));
    assert_eq!(requests[2].headers["if-none-match"], "*");
    let expected = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><Uncommitted>{}</Uncommitted><Uncommitted>{}</Uncommitted></BlockList>",
        block_ids[0], block_ids[1],
    );
    assert_eq!(requests[2].body, expected.as_bytes());
}

#[tokio::test]
async fn server_error_remains_visible_after_successful_probe_compensation() {
    let (backend, task) = fixture(vec![
        reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            &[("x-ms-error-code", "InternalError"), ("x-ms-request-id", "error-123")],
            b"",
        ),
        present(None),
        present(None),
        reply(StatusCode::ACCEPTED, &[], b""),
        missing(),
        missing(),
    ])
    .await;
    let backend: WarmBackendImpl = Box::new(backend);
    let error = check_warm_backend(Some(&backend))
        .await
        .expect_err("failed upload remains an error after cleanup");
    assert!(error.message.contains("500"));
    assert!(error.message.contains("InternalError"));
    assert!(error.message.contains("error-123"));
    assert!(!error.message.contains("cleanup is incomplete"));
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests.iter().filter(|request| request.method == "PUT").count(), 1);
    assert_eq!(requests.iter().filter(|request| request.method == "DELETE").count(), 1);
}

#[tokio::test]
async fn empty_blob_and_declared_length_failures_are_handled_before_commit() {
    let (backend, task) = fixture(vec![reply(StatusCode::CREATED, &[], b"")]).await;
    backend
        .put("empty", ReaderImpl::Body(Bytes::new()), 0)
        .await
        .expect("empty upload");
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests[0].headers["content-length"], "0");
    assert_eq!(requests[0].headers["content-md5"], "1B2M2Y8AsgTpgAmY7PhCfg==");
    for (body, kind) in [
        (b"short".as_slice(), ErrorKind::UnexpectedEof),
        (b"too-long".as_slice(), ErrorKind::InvalidInput),
    ] {
        let (backend, task) = fixture(vec![]).await;
        let err = backend
            .put("bad", ReaderImpl::Body(Bytes::copy_from_slice(body)), 6)
            .await
            .expect_err("length mismatch");
        assert_eq!(err.kind(), kind);
        assert!(task.await.expect("fixture should join").is_empty());
    }
    let (backend, task) = fixture(vec![reply(StatusCode::CREATED, &[], b"")]).await;
    let body = Bytes::from(vec![0; usize::try_from(MIN_BLOCK_BYTES + 1).expect("fixture size")]);
    let err = backend
        .put(
            "short-block",
            ReaderImpl::Body(body),
            i64::try_from(MIN_BLOCK_BYTES + 2).expect("fixture length"),
        )
        .await
        .expect_err("short final block");
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(
        task.await.expect("fixture should join").len(),
        1,
        "short input must never commit its staged blocks"
    );
}

#[tokio::test]
async fn range_get_preserves_key_encoding_and_exact_version_for_delete() {
    let version = "2026-11-06T01:02:03.0000000Z";
    let (backend, task) = fixture(vec![
        reply(
            StatusCode::PARTIAL_CONTENT,
            &[("content-range", "bytes 1-3/6"), ("x-ms-version-id", version)],
            b"ust",
        ),
        reply(StatusCode::ACCEPTED, &[], b""),
    ])
    .await;
    let object = "dir/a b+?#%2F.bin";
    let mut reader = backend
        .get(
            object,
            version,
            WarmBackendGetOpts {
                start_offset: 1,
                length: 3,
            },
        )
        .await
        .expect("versioned range read");
    let mut body = Vec::new();
    reader.read_to_end(&mut body).await.expect("range body");
    assert_eq!(body, b"ust");
    backend.remove_exact(object, version).await.expect("exact delete");
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests.len(), 2, "exact delete needs no current-version HEAD");
    for request in requests {
        let url = Url::parse(&format!("http://fixture{}", request.target)).expect("captured URL");
        assert_eq!(
            urlencoding::decode(url.path()).expect("decoded path"),
            format!("/container/cold/{object}")
        );
        let query: Vec<_> = url.query_pairs().into_owned().collect();
        assert_eq!(query, [("versionid".to_string(), version.to_string())]);
    }
}

#[tokio::test]
async fn version_drift_and_incorrect_ranges_fail_closed() {
    let (backend, task) = fixture(vec![present(Some("new-version"))]).await;
    let err = backend
        .remove("changed", "")
        .await
        .expect_err("unversioned delete must reject version drift");
    assert!(err.to_string().contains("became versioned"));
    assert_eq!(task.await.expect("fixture should join").len(), 1);
    for headers in [
        vec![("content-range", "bytes 0-2/6")],
        vec![("content-range", "bytes 1-3/6"), ("x-ms-version-id", "unexpected-version")],
    ] {
        let (backend, task) = fixture(vec![reply(StatusCode::PARTIAL_CONTENT, &headers, b"ust")]).await;
        let result = backend
            .get(
                "object",
                "",
                WarmBackendGetOpts {
                    start_offset: 1,
                    length: 3,
                },
            )
            .await;
        assert!(result.is_err(), "wrong offset or an unexpected version cannot satisfy the read");
        task.await.expect("fixture should join");
    }
}

#[tokio::test]
async fn unversioned_delete_keeps_the_head_precondition_when_the_blob_changes() {
    let (backend, task) = fixture(vec![
        present(None),
        reply(StatusCode::PRECONDITION_FAILED, &[("x-ms-error-code", "ConditionNotMet")], b""),
    ])
    .await;
    assert!(backend.remove("changed", "").await.is_err());
    let requests = task.await.expect("fixture should join");
    assert_eq!(requests[1].headers["if-match"], "\"original\"");
}

#[tokio::test]
async fn absence_requires_blob_not_found_and_container_listing_is_bounded() {
    for (response, missing_expected) in [
        (missing(), true),
        (reply(StatusCode::NOT_FOUND, &[("x-ms-error-code", "ContainerNotFound")], b""), false),
        (reply(StatusCode::NOT_FOUND, &[], b""), false),
    ] {
        let (backend, task) = fixture(vec![response]).await;
        let result = backend.probe_transition_candidate("object").await;
        if missing_expected {
            assert_eq!(result.expect("explicit blob absence"), TransitionCandidateProbe::Missing);
        } else {
            assert!(result.is_err(), "an absent container or an ambiguous HEAD is not blob absence");
        }
        task.await.expect("fixture should join");
    }
    for (body, in_use) in [
        (b"<EnumerationResults><Blobs/><NextMarker/></EnumerationResults>".as_slice(), false),
        (
            b"<EnumerationResults><Blobs><Blob><Name>cold/object</Name></Blob></Blobs><NextMarker/></EnumerationResults>"
                .as_slice(),
            true,
        ),
        (
            b"<EnumerationResults><Blobs/><NextMarker>next</NextMarker></EnumerationResults>".as_slice(),
            true,
        ),
    ] {
        let (backend, task) = fixture(vec![reply(StatusCode::OK, &[], body)]).await;
        assert_eq!(backend.in_use().await.expect("native container listing"), in_use);
        let requests = task.await.expect("fixture should join");
        assert!(
            requests[0]
                .target
                .contains("restype=container&comp=list&prefix=cold%2F&maxresults=1")
        );
    }
    let oversized = vec![b' '; MAX_CONTROL_BYTES + 1];
    let (backend, task) = fixture(vec![reply(StatusCode::OK, &[], &oversized)]).await;
    assert!(backend.in_use().await.is_err(), "oversized listing cannot establish an empty destination");
    task.await.expect("fixture should join");
}

#[tokio::test]
async fn recovery_requires_the_uploaded_transaction_and_destination_identity() {
    let identity = TransitionCandidateIdentity {
        transaction_id: uuid::Uuid::new_v4(),
        destination_id: [3; 32],
    };
    let mut metadata = HashMap::new();
    insert_str(&mut metadata, SUFFIX_TRANSITION_TRANSACTION_ID, identity.transaction_id.to_string());
    insert_str(&mut metadata, SUFFIX_TRANSITION_TIER_DESTINATION_ID, hex(identity.destination_id));
    let headers = metadata_headers(metadata).expect("identity metadata should encode for Azure");
    assert_eq!(
        headers
            .keys()
            .filter(|name| name.as_str().starts_with(METADATA_PREFIX))
            .count(),
        4
    );
    let mut owned = present(Some("owned-version"));
    owned.headers.extend(headers);
    let (backend, task) = fixture(vec![owned, present(None)]).await;
    assert_eq!(
        backend
            .probe_transition_candidate_for("object", identity)
            .await
            .expect("owned candidate"),
        TransitionCandidateProbe::VersionedPresent("owned-version".to_string())
    );
    assert_eq!(
        backend
            .probe_transition_candidate_for("object", identity)
            .await
            .expect("foreign candidate classification"),
        TransitionCandidateProbe::Ambiguous
    );
    task.await.expect("fixture should join");
}

#[test]
fn configuration_xml_and_size_limits_fail_before_network_io() {
    let mut config = TierAzure {
        access_key: "account".to_string(),
        secret_key: base64_simd::STANDARD.encode_to_string([7; 32]),
        bucket: "container".to_string(),
        ..Default::default()
    };
    assert!(
        WarmBackendAzureBlob::new(&config).is_ok(),
        "public endpoint should be derived from the account"
    );
    config.endpoint = "http://127.0.0.1:10000".to_string();
    assert!(
        WarmBackendAzureBlob::new(&config).is_err(),
        "production constructor must apply the outbound guard"
    );
    config.endpoint = "https://account.blob.example/path".to_string();
    assert!(
        WarmBackendAzureBlob::new(&config).is_err(),
        "an endpoint path cannot rewrite object addressing"
    );
    for name in [".", "..", "a/../b", "a/./b"] {
        assert!(validate_object_name(name).is_err());
    }
    assert_eq!(
        block_size(MAX_BLOCK_BYTES * MAX_BLOCKS).expect("largest supported object"),
        MAX_BLOCK_BYTES
    );
    assert!(block_size(MAX_BLOCK_BYTES * MAX_BLOCKS + 1).is_err());
    for xml in [
        b"<html>not a container</html>".as_slice(),
        b"<EnumerationResults><Blobs>".as_slice(),
        b"<!DOCTYPE x><EnumerationResults><Blobs/></EnumerationResults>".as_slice(),
        b"<EnumerationResults><Other><Blobs/></Other></EnumerationResults>".as_slice(),
        b"garbage<EnumerationResults><Blobs/></EnumerationResults>".as_slice(),
    ] {
        assert!(list_has_objects(xml).is_err());
    }
    assert_eq!(
        xml_error_code(b"<Error><Code>BlobNotFound</Code></Error>").as_deref(),
        Some("BlobNotFound")
    );
    assert!(xml_error_code(b"<Error><Code>BlobNotFound</Code>").is_none());
    assert!(xml_error_code(b"<Error><Code>BlobNotFound</Code></Error>garbage").is_none());
}

#[tokio::test]
async fn control_response_has_a_total_deadline_after_headers_arrive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("control fixture should bind");
    let address = listener.local_addr().expect("control fixture address");
    let (release, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("control fixture should accept");
        capture(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n")
            .await
            .expect("control response headers");
        let _ = wait.await;
    });
    let (mut backend, empty) = fixture(Vec::new()).await;
    empty.await.expect("unused fixture should close");
    backend.endpoint = Url::parse(&format!("http://{address}")).expect("control endpoint should parse");
    let response = backend
        .send(Method::GET, backend.container_url().expect("container URL"), HeaderMap::new(), None)
        .await
        .expect("headers should arrive before testing the body deadline");
    let err = tokio::time::timeout(Duration::from_secs(1), read_control_body(response, Duration::from_millis(20)))
        .await
        .expect("body deadline must be shorter than the transport idle timeout")
        .expect_err("an unfinished control response must time out");
    assert_eq!(err.kind(), ErrorKind::TimedOut);
    let _ = release.send(());
    server.await.expect("control fixture should join");
}
