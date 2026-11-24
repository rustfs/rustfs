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

use super::{BoxError, RustFSTestEnvironment};
use crate::common::signed_request_with_client;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    primitives::ByteStream,
    types::ObjectAttributes,
};
use bytes::Bytes;
use flate2::read::GzDecoder;
use http::{HeaderMap, HeaderValue, Method, StatusCode, Version, header};
use http_body_util::{BodyExt, Empty};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use md5::{Digest, Md5};
use std::io::Read;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

async fn request(
    env: &RustFSTestEnvironment,
    method: Method,
    url: &str,
    headers: HeaderMap,
) -> Result<reqwest::Response, BoxError> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers)
        .http1_only()
        .build()?;
    let response = signed_request_with_client(&client, method, url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.version(), Version::HTTP_11);
    Ok(response)
}

// HTTP clients discard HEAD bodies. Read through EOF so an illegal body fails the contract.
async fn head_on_wire(env: &RustFSTestEnvironment, bucket: &str, expected: StatusCode) -> Result<HeaderMap, BoxError> {
    let signed = env
        .create_s3_client()
        .head_object()
        .bucket(bucket)
        .key("normal.txt")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;
    let uri: http::Uri = signed.uri().parse()?;
    assert_eq!(uri.authority().map(|a| a.as_str()), Some(env.address.as_str()));
    let target = uri.path_and_query().ok_or("missing HEAD target")?;
    let mut request = format!("HEAD {target} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n", env.address);
    for (name, value) in signed.headers() {
        use std::fmt::Write;
        write!(request, "{name}: {value}\r\n")?;
    }
    request.push_str("\r\n");

    const LIMIT: usize = 64 * 1024;
    let mut reply = Vec::new();
    timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(&env.address).await?;
        stream.write_all(request.as_bytes()).await?;
        stream.take((LIMIT + 1) as u64).read_to_end(&mut reply).await
    })
    .await??;
    assert!(reply.len() <= LIMIT, "HEAD response exceeds limit");
    let end = reply
        .windows(4)
        .position(|v| v == b"\r\n\r\n")
        .ok_or("incomplete HEAD response")?;
    assert_eq!(reply.len(), end + 4, "unexpected body bytes after HEAD response headers");
    let mut lines = std::str::from_utf8(&reply[..end])?.split("\r\n");
    let mut status = lines.next().ok_or("missing HEAD status line")?.split_ascii_whitespace();
    assert_eq!(status.next(), Some("HTTP/1.1"));
    assert_eq!(status.next(), Some(expected.as_str()));
    let mut headers = HeaderMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or("invalid HEAD response header")?;
        headers.append(header::HeaderName::from_bytes(name.as_bytes())?, HeaderValue::from_str(value.trim())?);
    }
    Ok(headers)
}

async fn check_h2_body(signed: PresignedRequest, status: StatusCode, expected: &[u8]) -> Result<(), BoxError> {
    let client = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
    let mut req = signed.into_http_1x_request(Empty::<Bytes>::new());
    req.headers_mut()
        .insert(header::ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    timeout(Duration::from_secs(10), async {
        let response = client.request(req).await?;
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(response.status(), status);
        let mut body = response.into_body();
        let mut frames = 0;
        let mut received = 0;
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame?.into_data() {
                frames += 1;
                assert!(expected[received..].starts_with(&data), "HTTP/2 DATA differs from stored content");
                received += data.len();
            }
        }
        assert_eq!(received, expected.len());
        assert_eq!(frames == 0, expected.is_empty(), "unexpected HTTP/2 DATA frame count for {status}");
        Ok::<_, BoxError>(())
    })
    .await?
}

pub(super) async fn check(env: &RustFSTestEnvironment, bucket: &str) -> Result<(), BoxError> {
    let sdk = env.create_s3_client();
    let text = b"ordinary middleware payload\n".repeat(1200);
    let png = b"image-control\0".repeat(2000);
    let short = b"hello".as_slice();
    for (key, body, mime) in [
        ("normal.txt", text.as_slice(), "text/plain"),
        ("short.txt", short, "text/plain"),
        ("image.png", png.as_slice(), "image/png"),
    ] {
        sdk.put_object()
            .bucket(bucket)
            .key(key)
            .content_type(mime)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await?;
    }
    let root = format!("{}/{bucket}", env.url);
    let url = format!("{root}/normal.txt");
    let digest = hex_simd::encode_to_string(Md5::digest(&text), hex_simd::AsciiCase::Lower);
    let etag = format!("\"{digest}\"");

    let head = head_on_wire(env, bucket, StatusCode::OK).await?;
    assert_eq!(head[header::ETAG], etag.as_str());
    assert_eq!(head[header::CONTENT_LENGTH].to_str()?.parse::<usize>()?, text.len());

    for (key, accepted, expected_encoding, expected_body) in [
        ("normal.txt", "gzip", "gzip", text.as_slice()),
        ("normal.txt", "identity", "identity", text.as_slice()),
        ("short.txt", "gzip", "identity", short),
        ("image.png", "gzip", "identity", png.as_slice()),
    ] {
        let headers = HeaderMap::from_iter([(header::ACCEPT_ENCODING, HeaderValue::from_static(accepted))]);
        let res = request(env, Method::GET, &format!("{root}/{key}"), headers).await?;
        assert_eq!(res.status(), StatusCode::OK, "{key} accepting {accepted}");
        let encoding = res
            .headers()
            .get(header::CONTENT_ENCODING)
            .map(HeaderValue::to_str)
            .transpose()?
            .unwrap_or("identity");
        assert_eq!(encoding, expected_encoding, "{key} accepting {accepted}");
        let bytes = res.bytes().await?;
        if expected_encoding == "gzip" {
            let mut decoded = Vec::new();
            GzDecoder::new(bytes.as_ref()).read_to_end(&mut decoded)?;
            assert!(decoded == expected_body, "decompressed {key} differs from stored bytes");
        } else {
            assert!(bytes.as_ref() == expected_body, "{key} differs from stored bytes");
        }
    }

    let headers = HeaderMap::from_iter([
        (header::RANGE, HeaderValue::from_static("bytes=5-19")),
        (header::ACCEPT_ENCODING, HeaderValue::from_static("identity")),
    ]);
    let range = request(env, Method::GET, &url, headers).await?;
    assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.headers()[header::CONTENT_RANGE], format!("bytes 5-19/{}", text.len()));
    assert_eq!(range.bytes().await?.as_ref(), &text[5..20]);

    for (condition, expected, body) in [
        (etag.as_str(), StatusCode::NOT_MODIFIED, [].as_slice()),
        ("\"other-etag\"", StatusCode::OK, text.as_slice()),
    ] {
        let signed = sdk
            .get_object()
            .bucket(bucket)
            .key("normal.txt")
            .if_none_match(condition)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
            .await?;
        check_h2_body(signed, expected, body).await?;
    }

    let attrs = sdk
        .get_object_attributes()
        .bucket(bucket)
        .key("normal.txt")
        .object_attributes(ObjectAttributes::Etag)
        .send()
        .await?;
    assert_eq!(attrs.e_tag(), Some(digest.as_str()));

    let deleted = sdk
        .delete_object()
        .bucket(bucket)
        .key("normal.txt")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;
    check_h2_body(deleted, StatusCode::NO_CONTENT, &[]).await?;
    head_on_wire(env, bucket, StatusCode::NOT_FOUND).await?;
    Ok(())
}
