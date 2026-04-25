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

use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
use http::Version;
use http::header::HOST;
use rcgen::generate_simple_self_signed;
use reqwest::{Certificate, Client, Response, StatusCode};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::error::Error;
use std::path::Path;
use std::process::Command;
use tokio::fs;
use tokio::time::{Duration, sleep};
use tracing::info;

const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-head-tls-bodyless-bucket";

async fn generate_tls_bundle(tls_dir: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    fs::create_dir_all(tls_dir).await?;
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    fs::write(tls_dir.join("rustfs_cert.pem"), cert_pem.as_bytes()).await?;
    fs::write(tls_dir.join("rustfs_key.pem"), key_pem.as_bytes()).await?;

    Ok(cert_pem.into_bytes())
}

fn local_https_h2_client(ca_pem: &[u8]) -> Result<Client, Box<dyn Error + Send + Sync>> {
    let _ca_cert = Certificate::from_pem(ca_pem)?;
    Ok(Client::builder()
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .danger_accept_invalid_certs(true)
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

async fn ensure_bucket_exists(client: &Client, endpoint: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let bucket_url = format!("{endpoint}/{BUCKET}/");
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

async fn wait_for_tls_server_ready(client: &Client, endpoint: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ready_url = format!("{endpoint}/");
    for _attempt in 0..60 {
        match signed_empty_request(client, http::Method::GET, &ready_url).await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => sleep(Duration::from_millis(500)).await,
        }
    }

    Err("RustFS TLS server failed to become ready within 30 seconds".into())
}

async fn start_tls_rustfs_server(env: &mut RustFSTestEnvironment, tls_dir: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let binary_path = rustfs_binary_path();
    let mut command = Command::new(&binary_path);
    command
        .env("RUST_LOG", "rustfs=info,rustfs_notify=debug")
        .env("RUSTFS_TLS_PATH", tls_dir)
        .current_dir(&env.temp_dir);

    for key in [
        "RUSTFS_ADDRESS",
        "RUSTFS_VOLUMES",
        "RUSTFS_ACCESS_KEY",
        "RUSTFS_SECRET_KEY",
        "RUSTFS_TLS_PATH",
        "RUSTFS_OBS_LOG_DIRECTORY",
    ] {
        command.env_remove(key);
    }

    let process = command
        .env("RUSTFS_TLS_PATH", tls_dir)
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .args([
            "--address",
            &env.address,
            "--access-key",
            &env.access_key,
            "--secret-key",
            &env.secret_key,
            &env.temp_dir,
        ])
        .spawn()?;

    env.process = Some(process);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_head_missing_object_over_tls_http2_is_bodyless() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let tls_dir = std::path::PathBuf::from(&env.temp_dir).join("tls");
    let ca_pem = generate_tls_bundle(&tls_dir).await?;
    start_tls_rustfs_server(&mut env, &tls_dir).await?;

    let endpoint = format!("https://{}", env.address);
    let client = local_https_h2_client(&ca_pem)?;
    wait_for_tls_server_ready(&client, &endpoint).await?;
    ensure_bucket_exists(&client, &endpoint).await?;

    let missing_key = "head-does-not-exist.txt";
    let object_url = format!("{endpoint}/{BUCKET}/{missing_key}");

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
