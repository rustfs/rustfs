// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use bytes::Bytes;
use chrono::Utc;
use futures::stream;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, Method, Response};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use sha2::{Digest, Sha256};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes))
}

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn signed_admin_request(
    client: &Client,
    endpoint: &str,
    method: Method,
    path: &str,
    access_key: &str,
    secret_key: &str,
    payload_hash: &str,
) -> reqwest::RequestBuilder {
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .expect("embedded endpoint scheme");
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let canonical_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!("{}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}", method.as_str());
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_request.as_bytes()));
    let date_key = hmac(format!("AWS4{secret_key}").as_bytes(), &date);
    let region_key = hmac(&date_key, "us-east-1");
    let service_key = hmac(&region_key, "s3");
    let signing_key = hmac(&service_key, "aws4_request");
    let signature = hex(hmac(&signing_key, &string_to_sign));
    let authorization =
        format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}");

    client
        .request(method, format!("{endpoint}{path}"))
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
}

async fn signed_bytes_request(
    client: &Client,
    endpoint: &str,
    method: Method,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &'static [u8],
) -> Response {
    signed_admin_request(client, endpoint, method, path, access_key, secret_key, &sha256_hex(body))
        .body(body)
        .send()
        .await
        .expect("signed admin request")
}

#[tokio::test]
async fn diagnostic_handlers_enforce_advertised_runtime_contract() {
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("diagnostic-e2e-access")
        .secret_key("diagnostic-e2e-secret")
        .build()
        .await
        .expect("start embedded server");
    let endpoint = server.endpoint();
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("HTTP client");

    let response = signed_bytes_request(
        &client,
        &endpoint,
        Method::POST,
        "/rustfs/admin/v3/speedtest/client/devnull",
        server.access_key(),
        server.secret_key(),
        b"bounded",
    )
    .await;
    assert!(response.status().is_success());
    let value: serde_json::Value = response.json().await.expect("client devnull JSON");
    assert_eq!(value["kind"], "client-devnull");
    assert_eq!(value["measured"], true);
    assert_eq!(value["rx_bytes"], 7);

    let oversized = signed_admin_request(
        &client,
        &endpoint,
        Method::POST,
        "/rustfs/admin/v3/speedtest/client/devnull",
        server.access_key(),
        server.secret_key(),
        &sha256_hex(b""),
    )
    .header("content-length", 1024_u64 * 1024 * 1024 + 1)
    .body(reqwest::Body::wrap_stream(stream::pending::<Result<Bytes, std::io::Error>>()))
    .send()
    .await
    .expect("oversized request must receive an early response");
    assert_eq!(oversized.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        oversized
            .text()
            .await
            .expect("oversized error body")
            .contains("EntityTooLarge")
    );

    let stalled_client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("stalled HTTP client");
    let mut stalled = Vec::new();
    for _ in 0..4 {
        let client = stalled_client.clone();
        let endpoint = endpoint.clone();
        let access_key = server.access_key().to_string();
        let secret_key = server.secret_key().to_string();
        stalled.push(tokio::spawn(async move {
            signed_admin_request(
                &client,
                &endpoint,
                Method::POST,
                "/rustfs/admin/v3/speedtest/client/devnull",
                &access_key,
                &secret_key,
                &sha256_hex(b"x"),
            )
            .header("content-length", 1)
            .body(reqwest::Body::wrap_stream(stream::pending::<Result<Bytes, std::io::Error>>()))
            .send()
            .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let probe = signed_bytes_request(
                &client,
                &endpoint,
                Method::POST,
                "/rustfs/admin/v3/speedtest/client/devnull",
                server.access_key(),
                server.secret_key(),
                b"x",
            )
            .await;
            let status = probe.status();
            let body = probe.text().await.expect("admission probe body");
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                assert!(body.contains("SlowDown"));
                break;
            }
            assert!(status.is_success(), "unexpected admission probe status {status}: {body}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stalled requests must occupy every admission slot");
    for request in stalled {
        request.abort();
    }

    let netperf = signed_bytes_request(
        &client,
        &endpoint,
        Method::POST,
        "/rustfs/admin/v3/site-replication/netperf",
        server.access_key(),
        server.secret_key(),
        b"",
    )
    .await;
    assert!(netperf.status().is_success());
    let gob = netperf.bytes().await.expect("site netperf gob");
    assert!(
        gob.windows(b"site-replication netperf is unsupported because RustFS does not perform peer traffic".len())
            .any(|window| window == b"site-replication netperf is unsupported because RustFS does not perform peer traffic")
    );

    server.shutdown().await;
}
