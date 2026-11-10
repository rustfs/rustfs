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

use rustfs_s3_client::{
    api_get_options::GetObjectOptions,
    credentials::{Credentials, SignatureType, Static, Value},
    transition_api::{BucketLookupType, Options, TransitionClient, TransitionClientTimeouts},
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn run_case(ranged: bool) -> bool {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fixture");
    let endpoint = listener.local_addr().expect("local endpoint").to_string();
    let (headers_sent, headers_ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("GET connection");
        let mut request = Vec::new();
        loop {
            let mut buffer = [0_u8; 1024];
            let n = socket.read(&mut buffer).await.expect("GET headers");
            assert_ne!(n, 0);
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).expect("ASCII HTTP request");
        assert!(request.starts_with("GET /bucket/object "), "{request}");
        assert_eq!(request.to_ascii_lowercase().contains("\r\nrange: bytes=0-7\r\n"), ranged, "{request}");
        let status = if ranged { "206 Partial Content" } else { "200 OK" };
        let range_header = if ranged { "Content-Range: bytes 0-7/8\r\n" } else { "" };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: 8\r\n{range_header}Content-Type: application/octet-stream\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\nRust"
        );
        socket.write_all(response.as_bytes()).await.expect("first body bytes");
        headers_sent.send(()).expect("signal response head");
        released.await.expect("release remaining body");
        socket.write_all(b"FS!!").await.expect("last body bytes");
    });
    let options = Options {
        creds: Credentials::new(Static(Value {
            access_key_id: "access-key".into(),
            secret_access_key: "secret-key".into(),
            signer_type: SignatureType::SignatureV4,
            ..Default::default()
        })),
        region: "us-east-1".into(),
        bucket_lookup: BucketLookupType::BucketLookupAuto,
        max_retries: 1,
        ..Default::default()
    };
    let client = TransitionClient::new_with_timeouts(&endpoint, options, "s3", TransitionClientTimeouts::default())
        .await
        .expect("production client");
    let client = rustfs_s3_client::transition_api::TransitionCore(std::sync::Arc::new(client));
    let mut opts = GetObjectOptions::default();
    if ranged {
        opts.set_range(0, 7).expect("closed production range");
    }
    let get = client.get_object("bucket", "object", &opts);
    tokio::pin!(get);
    let probe = async {
        tokio::select! {
            result = &mut get => Some(result),
            _ = headers_ready => None,
        }
    }
    .await;
    let before_eof = match probe {
        Some(result) => Ok(result),
        None => tokio::time::timeout(Duration::from_secs(2), &mut get).await,
    };
    let returned_before_body_end = before_eof.is_ok();
    let mut body = Vec::new();
    let mut reader = match before_eof {
        Ok(result) => {
            let (_, _, mut reader) = result.expect("streaming GET");
            let mut prefix = [0; 4];
            tokio::time::timeout(Duration::from_secs(2), reader.read_exact(&mut prefix))
                .await
                .expect("first bytes must arrive before the remote body ends")
                .expect("first body bytes");
            body.extend_from_slice(&prefix);
            release.send(()).expect("release fixture");
            reader
        }
        Err(_) => {
            release.send(()).expect("release fixture");
            let (_, _, reader) = get.await.expect("buffered GET after full body");
            reader
        }
    };
    reader.read_to_end(&mut body).await.expect("response bytes");
    assert_eq!(body, b"RustFS!!");
    server.await.expect("fixture server");
    returned_before_body_end
}

#[tokio::test]
async fn get_object_returns_reader_before_remote_response_ends() {
    let full = run_case(false).await;
    let closed_range = run_case(true).await;
    assert!(full && closed_range, "tier GET must stream: full={full}, closed_range={closed_range}");
}

#[tokio::test]
async fn dropping_get_object_reader_closes_the_unfinished_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fixture");
    let endpoint = listener.local_addr().expect("local endpoint").to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("GET connection");
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let size = socket.read(&mut buffer).await.expect("GET headers");
            assert_ne!(size, 0);
            request.extend_from_slice(&buffer[..size]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nRust")
            .await
            .expect("partial response");
        let closed = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut [0; 1]))
            .await
            .expect("dropping the reader must release the remote connection");
        match closed {
            Ok(size) => assert_eq!(size, 0, "a canceled body must not send another request"),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
        }
    });
    let options = Options {
        creds: Credentials::new(Static(Value {
            access_key_id: "access-key".into(),
            secret_access_key: "secret-key".into(),
            signer_type: SignatureType::SignatureV4,
            ..Default::default()
        })),
        region: "us-east-1".into(),
        ..Default::default()
    };
    let client = TransitionClient::new_with_timeouts(&endpoint, options, "s3", TransitionClientTimeouts::default())
        .await
        .expect("production client");
    let (_, _, reader) = tokio::time::timeout(
        Duration::from_secs(2),
        client.get_object_inner("bucket", "object", &GetObjectOptions::default()),
    )
    .await
    .expect("headers must return before the response completes")
    .expect("GET reader");
    drop(reader);
    server.await.expect("fixture server");
}
