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

//! Regression test for TLS/HTTP2 `HEAD` responses on missing objects.
//!
//! Before the fix, RustFS returned `404` for a missing object but still wrote
//! the XML error payload on a `HEAD` request. Under HTTP/2 this emitted DATA
//! frames after the response headers, which clients surfaced as a protocol
//! error. This test keeps the request at the raw HTTPS layer so it can validate
//! the final wire-facing behavior rather than SDK-level error mapping.

#![cfg(test)]

use crate::common::{init_logging, workspace_root};
use http::header::HOST;
use http::Version;
use reqwest::{Certificate, Client, Response, StatusCode};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::error::Error;
use tokio::fs;
use tracing::info;

const ENDPOINT: &str = "https://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-head-tls-bodyless-bucket";

async fn local_https_h2_client() -> Result<Client, Box<dyn Error + Send + Sync>> {
    let ca_path = workspace_root().join("target").join("tls").join("ca.crt");
    let ca_pem = fs::read(ca_path).await?;
    let ca_cert = Certificate::from_pem(&ca_pem)?;

    Ok(Client::builder()
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .add_root_certificate(ca_cert)
        .build()?)
}

async fn signed_empty_request(
    client: &Client,
    method: http::Method,
    url: &str,
) -> Result<Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let request = http::Request::builder()
        .method(method.as_str())
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .body(Body::empty())?;

    let signed = sign_v4(request, 0, ACCESS_KEY, SECRET_KEY, "", "us-east-1");

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let mut builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }

    Ok(builder.send().await?)
}

async fn ensure_bucket_exists(client: &Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    let bucket_url = format!("{ENDPOINT}/{BUCKET}/");
    let response = signed_empty_request(client, http::Method::HEAD, &bucket_url).await?;

    if response.status() == StatusCode::OK {
        return Ok(());
    }

    let response = signed_empty_request(client, http::Method::PUT, &bucket_url).await?;
    match response.status() {
        StatusCode::OK => Ok(()),
        StatusCode::CONFLICT => Ok(()),
        status => Err(format!("unexpected bucket setup status: {status}").into()),
    }
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS TLS server at https://localhost:9000"]
async fn test_head_missing_object_over_tls_http2_is_bodyless() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let client = local_https_h2_client().await?;
    ensure_bucket_exists(&client).await?;

    let missing_key = "head-does-not-exist.txt";
    let object_url = format!("{ENDPOINT}/{BUCKET}/{missing_key}");

    let get_response = signed_empty_request(&client, http::Method::GET, &object_url).await?;
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    let get_version = get_response.version();
    let get_body = get_response.bytes().await?;
    let get_body_text = String::from_utf8_lossy(&get_body);
    assert!(
        get_body_text.contains("<Code>NoSuchKey</Code>") || get_body_text.contains("<Code>NoSuchObject</Code>"),
        "GET missing-object error body should expose NoSuchKey/NoSuchObject, got: {}",
        get_body_text
    );
    info!("GET missing object over TLS used {:?} and returned {} bytes", get_version, get_body.len());

    let head_response = signed_empty_request(&client, http::Method::HEAD, &object_url).await?;
    assert_eq!(head_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(head_response.version(), Version::HTTP_2, "HEAD regression test must exercise HTTP/2");
    let head_body = head_response.bytes().await?;
    assert!(
        head_body.is_empty(),
        "HEAD missing-object response must not send body bytes over TLS/HTTP2, got {} bytes: {:?}",
        head_body.len(),
        head_body
    );

    Ok(())
}
