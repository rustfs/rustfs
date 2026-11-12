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

use bytes::Bytes;
use http::{Method, StatusCode};
use rustfs_s3_client::{
    api_error_response::to_error_response,
    credentials::{Credentials, SignatureType, Static, Value},
    remote_error_code::RemoteErrorCode,
    transition_api::{
        BucketLookupType, MAX_S3_ERROR_RESPONSE_SIZE, ObjectReader, Options, ReaderImpl, RequestMetadata, TransitionClient,
        TransitionClientTimeouts,
    },
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Copy)]
enum Reply {
    Http(StatusCode, &'static str),
    Disconnect,
    Truncated,
    StallHeaders,
    MalformedHttp,
    Oversized,
}

fn metadata(body: ReaderImpl, content_length: i64) -> RequestMetadata {
    RequestMetadata {
        bucket_name: "bucket".into(),
        object_name: "object".into(),
        bucket_location: "us-east-1".into(),
        content_body: body,
        content_length,
        pre_sign_url: false,
        query_values: Default::default(),
        custom_header: Default::default(),
        extra_pre_sign_header: None,
        expires: 0,
        content_md5_base64: String::new(),
        content_sha256_hex: String::new(),
        stream_sha256: false,
        trailer: Default::default(),
    }
}

fn empty_metadata() -> RequestMetadata {
    metadata(ReaderImpl::Body(Bytes::new()), 0)
}

async fn run_case(
    method: Method,
    metadata: &mut RequestMetadata,
    attempts: i64,
    replies: Vec<Reply>,
) -> (Result<StatusCode, std::io::Error>, Vec<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind retry fixture");
    let endpoint = listener.local_addr().expect("fixture address").to_string();
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        let mut stalled_sockets = Vec::new();
        loop {
            let (mut socket, _) = tokio::select! {
                _ = &mut stopped => break,
                accepted = listener.accept() => accepted.expect("accept retry request"),
            };
            let mut request = Vec::new();
            let header_end = loop {
                let mut buffer = [0; 1024];
                let size = socket.read(&mut buffer).await.expect("read request headers");
                assert_ne!(size, 0, "client closed before sending headers");
                request.extend_from_slice(&buffer[..size]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
            assert!(headers.contains("authorization: aws4-hmac-sha256"), "each attempt must be signed");
            let length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map(|value| value.parse::<usize>().expect("numeric content length"))
                .unwrap_or(0);
            while request.len() < header_end + length {
                let mut buffer = [0; 1024];
                let size = socket.read(&mut buffer).await.expect("read request body");
                assert_ne!(size, 0, "client closed before sending the complete body");
                request.extend_from_slice(&buffer[..size]);
            }
            let reply = replies[requests.len().min(replies.len() - 1)];
            requests.push(request);
            let (status, body, extra_length) = match reply {
                Reply::Disconnect => continue,
                Reply::StallHeaders => {
                    stalled_sockets.push(socket);
                    continue;
                }
                Reply::MalformedHttp => {
                    socket
                        .write_all(b"invalid HTTP\r\n\r\n")
                        .await
                        .expect("write malformed response");
                    continue;
                }
                Reply::Truncated => (StatusCode::SERVICE_UNAVAILABLE, "<Error>".to_string(), 100),
                Reply::Oversized => (StatusCode::SERVICE_UNAVAILABLE, "x".repeat(MAX_S3_ERROR_RESPONSE_SIZE + 1), 0),
                Reply::Http(status, body) => (status, body.to_string(), 0),
            };
            let is_head = requests.last().expect("recorded request").starts_with(b"HEAD ");
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\nx-amz-request-id: fixture\r\n\r\n{}",
                body.len() + extra_length,
                if is_head { "" } else { &body },
            );
            socket.write_all(response.as_bytes()).await.expect("write fixture response");
        }
        requests
    });
    let client = TransitionClient::new_with_timeouts(
        &endpoint,
        Options {
            creds: Credentials::new(Static(Value {
                access_key_id: "access-key".into(),
                secret_access_key: "secret-key".into(),
                signer_type: SignatureType::SignatureV4,
                ..Default::default()
            })),
            region: "us-east-1".into(),
            bucket_lookup: BucketLookupType::BucketLookupPath,
            max_retries: attempts,
            ..Default::default()
        },
        "s3",
        TransitionClientTimeouts::new(Duration::from_secs(1), Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await
    .expect("build real transition client");
    let result = tokio::time::timeout(Duration::from_secs(5), client.execute_method(method, metadata)).await;
    let _ = stop.send(());
    let requests = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("fixture must stop")
        .expect("fixture must join");
    (result.expect("retry loop must terminate").map(|response| response.status()), requests)
}

#[tokio::test]
async fn retries_transient_http_statuses_then_succeeds() {
    for method in [Method::GET, Method::HEAD] {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            let (result, requests) = run_case(
                method.clone(),
                &mut empty_metadata(),
                2,
                vec![Reply::Http(status, ""), Reply::Http(StatusCode::OK, "")],
            )
            .await;
            assert_eq!(result.expect("transient status should recover"), StatusCode::OK);
            assert_eq!(requests.len(), 2, "{method} {status} must issue a second request");
        }
    }
}

#[tokio::test]
async fn retries_s3_error_codes_even_with_a_non_retryable_http_status() {
    for body in [
        "<Error><Code>SlowDown</Code></Error>",
        "<Error><Code>Throttling</Code></Error>",
        "<Error><Code>ThrottlingException</Code></Error>",
        "<Error><Code>RequestTimeout</Code></Error>",
    ] {
        let (result, requests) = run_case(
            Method::GET,
            &mut empty_metadata(),
            2,
            vec![Reply::Http(StatusCode::BAD_REQUEST, body), Reply::Http(StatusCode::OK, "")],
        )
        .await;
        assert_eq!(result.expect("transient S3 error should recover"), StatusCode::OK);
        assert_eq!(requests.len(), 2, "{body} must trigger a retry");
    }
}

#[tokio::test]
async fn retries_disconnected_transport_and_interrupted_error_body() {
    for first_reply in [Reply::Disconnect, Reply::Truncated, Reply::StallHeaders] {
        let (result, requests) =
            run_case(Method::GET, &mut empty_metadata(), 2, vec![first_reply, Reply::Http(StatusCode::OK, "")]).await;
        assert_eq!(result.expect("interrupted request should recover"), StatusCode::OK);
        assert_eq!(requests.len(), 2);
    }
}

#[tokio::test]
async fn retry_exhaustion_preserves_the_transport_timeout() {
    let (result, requests) = run_case(Method::GET, &mut empty_metadata(), 2, vec![Reply::StallHeaders]).await;
    assert_eq!(result.expect_err("both attempts timed out").kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn malformed_and_oversized_responses_are_not_retried() {
    for reply in [Reply::MalformedHttp, Reply::Oversized] {
        let (result, requests) = run_case(Method::GET, &mut empty_metadata(), 3, vec![reply]).await;
        let error = result.expect_err("invalid responses must fail");
        if matches!(reply, Reply::Oversized) {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        }
        assert_eq!(requests.len(), 1, "permanent protocol errors must not be replayed");
    }
}

#[tokio::test]
async fn invalid_request_headers_fail_before_dispatch() {
    let mut metadata = empty_metadata();
    metadata.content_md5_base64 = "invalid\nheader".into();
    let (result, requests) = run_case(Method::GET, &mut metadata, 3, vec![Reply::Http(StatusCode::OK, "")]).await;
    assert_eq!(result.expect_err("invalid header must fail").kind(), std::io::ErrorKind::InvalidInput);
    assert!(requests.is_empty(), "request construction failures must not reach the server");
}

#[tokio::test]
async fn retry_exhaustion_returns_the_last_s3_error() {
    let (result, requests) = run_case(
        Method::GET,
        &mut empty_metadata(),
        3,
        vec![
            Reply::Http(StatusCode::BAD_GATEWAY, ""),
            Reply::Http(StatusCode::SERVICE_UNAVAILABLE, "<Error><Code>SlowDown</Code></Error>"),
        ],
    )
    .await;
    let error = to_error_response(&result.expect_err("all attempts failed"));
    assert_eq!(requests.len(), 3, "max_retries counts total attempts");
    assert_eq!(error.status_code, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, RemoteErrorCode::from_wire("SlowDown"));
}

#[tokio::test]
async fn permanent_errors_and_single_attempts_are_not_retried() {
    for (status, body, attempts) in [
        (StatusCode::FORBIDDEN, "<Error><Code>AccessDenied</Code></Error>", 3),
        (StatusCode::NOT_FOUND, "<Error><Code>NoSuchKey</Code></Error>", 3),
        (StatusCode::BAD_REQUEST, "<Error><Code>InvalidArgument</Code></Error>", 3),
        (StatusCode::SERVICE_UNAVAILABLE, "<Error><Code>SlowDown</Code></Error>", 1),
        (StatusCode::OK, "", 3),
    ] {
        let (result, requests) = run_case(Method::GET, &mut empty_metadata(), attempts, vec![Reply::Http(status, body)]).await;
        assert_eq!(result.is_ok(), status.is_success());
        assert_eq!(requests.len(), 1, "{status} with {attempts} attempts");
    }
}

#[tokio::test]
async fn buffered_upload_parts_replay_identical_body_bytes() {
    let mut metadata = metadata(ReaderImpl::Body(Bytes::from_static(b"part bytes")), 10);
    metadata.query_values.insert("uploadId".into(), "upload-1".into());
    metadata.query_values.insert("partNumber".into(), "1".into());
    let (result, requests) = run_case(
        Method::PUT,
        &mut metadata,
        2,
        vec![
            Reply::Http(StatusCode::SERVICE_UNAVAILABLE, ""),
            Reply::Http(StatusCode::OK, ""),
        ],
    )
    .await;
    assert_eq!(result.expect("buffered upload part should recover"), StatusCode::OK);
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(request.ends_with(b"\r\n\r\npart bytes"), "every attempt must send the full body");
    }
}

#[tokio::test]
async fn non_replayable_bodies_and_object_mutations_are_not_retried() {
    for method in [Method::GET, Method::PUT, Method::POST, Method::DELETE] {
        let mut metadata = if method == Method::GET {
            metadata(ReaderImpl::ObjectBody(ObjectReader::new(std::io::Cursor::new(b"stream"))), 6)
        } else {
            empty_metadata()
        };
        let (result, requests) =
            run_case(method.clone(), &mut metadata, 3, vec![Reply::Http(StatusCode::SERVICE_UNAVAILABLE, "")]).await;
        assert!(result.is_err(), "{method} must return the first failure");
        assert_eq!(requests.len(), 1, "{method} must not be replayed");
    }
    let mut metadata = metadata(ReaderImpl::ObjectBody(ObjectReader::new(std::io::Cursor::new(b"stream"))), 6);
    metadata.query_values.insert("uploadId".into(), "upload-1".into());
    metadata.query_values.insert("partNumber".into(), "1".into());
    let (result, requests) =
        run_case(Method::PUT, &mut metadata, 3, vec![Reply::Http(StatusCode::SERVICE_UNAVAILABLE, "")]).await;
    assert!(result.is_err(), "streaming upload parts must return the first failure");
    assert_eq!(requests.len(), 1, "a consumed object reader must not be replayed");
    assert!(requests[0].ends_with(b"\r\n\r\nstream"));
}
