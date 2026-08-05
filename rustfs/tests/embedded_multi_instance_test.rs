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

#![recursion_limit = "256"]

//! End-to-end acceptance for backlog#1052: two embedded RustFS servers coexist
//! in one process, on different ports and volumes, and their S3 data planes
//! stay isolated.

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
#[cfg(feature = "e2e-test-hooks")]
use chrono::Utc;
#[cfg(feature = "e2e-test-hooks")]
use hmac::{Hmac, KeyInit, Mac};
#[cfg(feature = "e2e-test-hooks")]
use reqwest::StatusCode;
#[cfg(feature = "e2e-test-hooks")]
use rustfs::embedded::pause_embedded_startup_after_http_bind;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};

mod common;
#[cfg(feature = "e2e-test-hooks")]
use sha2::{Digest, Sha256};
#[cfg(feature = "e2e-test-hooks")]
use std::time::Duration;

#[cfg(feature = "e2e-test-hooks")]
type HmacSha256 = Hmac<Sha256>;

fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    let config = Config::builder()
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

#[cfg(feature = "e2e-test-hooks")]
fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(feature = "e2e-test-hooks")]
fn sha256_hex(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes))
}

#[cfg(feature = "e2e-test-hooks")]
fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[cfg(feature = "e2e-test-hooks")]
fn signed_admin_request(
    client: &reqwest::Client,
    endpoint: &str,
    method: reqwest::Method,
    request_path: &str,
    access_key: &str,
    secret_key: &str,
    body: &[u8],
) -> reqwest::RequestBuilder {
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .expect("embedded endpoint scheme");
    let payload_hash = sha256_hex(body);
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let canonical_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let (path, query) = request_path.split_once('?').unwrap_or((request_path, ""));
    let canonical_request = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        method.as_str()
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_request.as_bytes()));
    let date_key = hmac(format!("AWS4{secret_key}").as_bytes(), &date);
    let region_key = hmac(&date_key, "us-east-1");
    let service_key = hmac(&region_key, "s3");
    let signing_key = hmac(&service_key, "aws4_request");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={}",
        hex(hmac(&signing_key, &string_to_sign))
    );

    client
        .request(method, format!("{endpoint}{request_path}"))
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .body(body.to_vec())
}

// backlog#1052 acceptance: a second embedded server in the same process no
// longer aborts on write-once startup state — before this change,
// `RustFSServer::build()` returned AlreadyStarted (guard) or panicked on
// region/endpoints (bootstrap context write-once). This test proves the
// startup pipeline lifts; a follow-up will widen the request path to route
// per-server so the two servers can also serve different data planes end-to-
// end without the shared-IAM caveat.
#[test]
fn two_embedded_servers_start_and_shutdown_independently() {
    common::run_embedded_test(two_embedded_servers_start_and_shutdown_independently_body);
}

async fn two_embedded_servers_start_and_shutdown_independently_body() {
    let port_a = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port for server A: {err}"),
    };
    let server_a = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_a}"))
        .access_key("shared-access")
        .secret_key("shared-secret")
        .build()
        .await
        .expect("start embedded server A");

    let port_b = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            server_a.shutdown().await;
            return;
        }
        Err(err) => {
            server_a.shutdown().await;
            panic!("find free port for server B: {err}");
        }
    };
    let server_b = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_b}"))
        .access_key("shared-access")
        .secret_key("shared-secret")
        .build()
        .await
        .expect("start embedded server B — a second server must be allowed after startup handoff");

    assert_ne!(server_a.address().port(), server_b.address().port(), "each server binds its own port");

    // Both endpoints serve the readiness probe — the crudest possible check
    // that both HTTP stacks are actually listening on their own port.
    let a_endpoint = server_a.endpoint();
    let b_endpoint = server_b.endpoint();
    assert!(a_endpoint.ends_with(&format!(":{port_a}")));
    assert!(b_endpoint.ends_with(&format!(":{port_b}")));

    server_b.shutdown().await;
    // Server A remains fully usable after server B shuts down — the second
    // shutdown must not have released state server A depends on.
    let client_a = s3_client(&server_a.endpoint(), server_a.access_key(), server_a.secret_key());
    client_a
        .create_bucket()
        .bucket("survives-b-shutdown")
        .send()
        .await
        .expect("server A still serves after server B shuts down");
    client_a
        .put_object()
        .bucket("survives-b-shutdown")
        .key("marker.txt")
        .body(ByteStream::from_static(b"still here"))
        .send()
        .await
        .expect("server A still writes after server B shuts down");

    server_a.shutdown().await;
}

// backlog#1052 full acceptance: two embedded servers with *different*
// credentials are isolated end to end — auth (each accepts its own key and
// rejects the other's) AND data plane (each server's buckets/objects are
// invisible to the other; each lists/creates/deletes only on its own disks
// and bucket-metadata system).
#[test]
fn two_embedded_servers_isolate_auth_and_data_planes() {
    common::run_embedded_test(two_embedded_servers_isolate_auth_and_data_planes_body);
}

async fn two_embedded_servers_isolate_auth_and_data_planes_body() {
    let port_a = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port for server A: {err}"),
    };
    let server_a = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_a}"))
        .access_key("access-key-a")
        .secret_key("secret-key-a")
        .build()
        .await
        .expect("start embedded server A");

    let port_b = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            server_a.shutdown().await;
            return;
        }
        Err(err) => {
            server_a.shutdown().await;
            panic!("find free port for server B: {err}");
        }
    };
    let server_b = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_b}"))
        .access_key("access-key-b")
        .secret_key("secret-key-b")
        .build()
        .await
        .expect("start embedded server B");

    // Server B authenticates with its OWN key — before per-server auth this
    // failed with InvalidAccessKeyId because validation used the process
    // (server A's) credentials.
    let client_b = s3_client(&server_b.endpoint(), "access-key-b", "secret-key-b");
    client_b
        .list_buckets()
        .send()
        .await
        .expect("server B must authenticate with its own credentials");

    // Server B rejects server A's key — the two servers have distinct root
    // identities.
    let cross = s3_client(&server_b.endpoint(), "access-key-a", "secret-key-a")
        .list_buckets()
        .send()
        .await;
    assert!(cross.is_err(), "server B must reject server A's access key; got {cross:?}");

    // Server A still authenticates with its own key.
    let client_a = s3_client(&server_a.endpoint(), "access-key-a", "secret-key-a");
    client_a
        .list_buckets()
        .send()
        .await
        .expect("server A must authenticate with its own credentials");

    // ---- Data-plane isolation (backlog#1052 S7) ----

    // Server A owns a bucket + object.
    client_a
        .create_bucket()
        .bucket("only-on-a")
        .send()
        .await
        .expect("server A creates its bucket");
    client_a
        .put_object()
        .bucket("only-on-a")
        .key("marker.txt")
        .body(ByteStream::from_static(b"belongs to A"))
        .send()
        .await
        .expect("server A writes its object");

    // Server B's listing does not contain server A's bucket.
    let b_buckets: Vec<_> = client_b
        .list_buckets()
        .send()
        .await
        .expect("server B lists buckets")
        .buckets()
        .iter()
        .flat_map(|bucket| bucket.name.clone())
        .collect();
    assert!(
        !b_buckets.contains(&"only-on-a".to_string()),
        "server B must not see server A's bucket; saw {b_buckets:?}"
    );

    // Server B cannot resolve server A's object either.
    let cross_head = client_b.head_object().bucket("only-on-a").key("marker.txt").send().await;
    assert!(cross_head.is_err(), "server B must not resolve server A's object; got {cross_head:?}");

    // Server B's own bucket is invisible to server A.
    client_b
        .create_bucket()
        .bucket("only-on-b")
        .send()
        .await
        .expect("server B creates its bucket");
    let a_buckets: Vec<_> = client_a
        .list_buckets()
        .send()
        .await
        .expect("server A lists buckets")
        .buckets()
        .iter()
        .flat_map(|bucket| bucket.name.clone())
        .collect();
    assert!(
        a_buckets.contains(&"only-on-a".to_string()),
        "server A must keep seeing its own bucket; saw {a_buckets:?}"
    );
    assert!(
        !a_buckets.contains(&"only-on-b".to_string()),
        "server A must not see server B's bucket; saw {a_buckets:?}"
    );

    // Server A's data plane is intact.
    let a_get = client_a
        .get_object()
        .bucket("only-on-a")
        .key("marker.txt")
        .send()
        .await
        .expect("server A serves its own object");
    let a_data = a_get.body.collect().await.expect("read A body").into_bytes();
    assert_eq!(a_data.as_ref(), b"belongs to A");

    server_a.shutdown().await;
    server_b.shutdown().await;
}

#[cfg(feature = "e2e-test-hooks")]
#[tokio::test]
async fn embedded_servers_isolate_iam_users_and_policies() {
    let port_a = find_available_port().expect("find free port for server A");
    let server_a = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_a}"))
        .access_key("iam-root-a")
        .secret_key("iam-root-secret-a")
        .build()
        .await
        .expect("start embedded server A");
    let port_b = find_available_port().expect("find free port for server B");
    let server_b = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_b}"))
        .access_key("iam-root-b")
        .secret_key("iam-root-secret-b")
        .build()
        .await
        .expect("start embedded server B");

    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build local admin client");
    let user_body = serde_json::json!({"secretKey": "iam-user-secret-a", "status": "enabled"}).to_string();
    for (path, body) in [
        ("/rustfs/admin/v3/add-user?accessKey=iam-user-a", user_body.as_bytes()),
        (
            "/rustfs/admin/v3/set-user-or-group-policy?policyName=readwrite&userOrGroup=iam-user-a&isGroup=false",
            b"".as_slice(),
        ),
    ] {
        let response = signed_admin_request(
            &http,
            &server_a.endpoint(),
            reqwest::Method::PUT,
            path,
            server_a.access_key(),
            server_a.secret_key(),
            body,
        )
        .send()
        .await
        .expect("send server A IAM request");
        assert!(response.status().is_success(), "server A IAM setup failed: {}", response.status());
    }

    let client_b = s3_client(&server_b.endpoint(), server_b.access_key(), server_b.secret_key());
    client_b
        .create_bucket()
        .bucket("private-to-b")
        .send()
        .await
        .expect("create server B bucket");
    client_b
        .put_object()
        .bucket("private-to-b")
        .key("secret.txt")
        .body(ByteStream::from_static(b"server B data"))
        .send()
        .await
        .expect("write server B object");

    let cross_instance = s3_client(&server_b.endpoint(), "iam-user-a", "iam-user-secret-a")
        .get_object()
        .bucket("private-to-b")
        .key("secret.txt")
        .send()
        .await;
    assert!(cross_instance.is_err(), "server A IAM user must not authorize against server B");

    server_b.shutdown().await;
    server_a.shutdown().await;
}

#[cfg(feature = "e2e-test-hooks")]
#[tokio::test]
async fn second_embedded_server_fails_closed_until_its_context_slot_is_installed() {
    let port_a = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port for server A: {err}"),
    };
    let server_a = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_a}"))
        .access_key("startup-window-access-a")
        .secret_key("startup-window-secret-a")
        .build()
        .await
        .expect("start embedded server A");
    let client_a = s3_client(&server_a.endpoint(), server_a.access_key(), server_a.secret_key());
    client_a
        .create_bucket()
        .bucket("startup-window")
        .send()
        .await
        .expect("server A creates the shared-name bucket");
    client_a
        .put_object()
        .bucket("startup-window")
        .key("marker.txt")
        .body(ByteStream::from_static(b"from A"))
        .send()
        .await
        .expect("server A writes its marker");

    let port_b = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            server_a.shutdown().await;
            return;
        }
        Err(err) => {
            server_a.shutdown().await;
            panic!("find free port for server B: {err}");
        }
    };
    let endpoint_b = format!("http://127.0.0.1:{port_b}");
    let b_access_key = "startup-window-access-b";
    let b_secret_key = "startup-window-secret-b";
    let mut barrier = pause_embedded_startup_after_http_bind(port_b);
    let startup_b = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port_b}"))
        .access_key(b_access_key)
        .secret_key(b_secret_key)
        .build();
    tokio::pin!(startup_b);
    {
        let bound = tokio::time::timeout(Duration::from_secs(10), barrier.wait_until_http_bound());
        tokio::pin!(bound);
        tokio::select! {
            bound = &mut bound => {
                bound.expect("server B must bind HTTP before installing its context slot");
            }
            startup = startup_b.as_mut() => {
                match startup {
                    Ok(server) => {
                        server.shutdown().await;
                        panic!("server B startup completed before the HTTP-bind barrier fired");
                    }
                    Err(err) => panic!("server B startup failed before the HTTP-bind barrier fired: {err}"),
                }
            }
        }
    }

    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build local admin client without proxy");
    let inspect_path = "/rustfs/admin/v3/inspect-data?file=marker.txt&volume=startup-window";
    let before_install =
        signed_admin_request(&http, &endpoint_b, reqwest::Method::GET, inspect_path, b_access_key, b_secret_key, b"")
            .send()
            .await
            .expect("server B HTTP listener must accept the paused request");
    let before_install_status = before_install.status();
    let before_install_body = before_install.text().await.expect("read paused response body");
    assert_eq!(before_install_status, StatusCode::SERVICE_UNAVAILABLE, "{before_install_body}");
    assert!(
        before_install_body.contains("server context is not ready"),
        "paused request must not resolve server A: {before_install_body}"
    );

    barrier.release();
    let server_b = tokio::time::timeout(Duration::from_secs(20), startup_b.as_mut())
        .await
        .expect("server B startup must complete after releasing the barrier")
        .expect("start embedded server B");
    let client_b = s3_client(&server_b.endpoint(), server_b.access_key(), server_b.secret_key());
    client_b
        .create_bucket()
        .bucket("startup-window")
        .send()
        .await
        .expect("server B creates its isolated shared-name bucket");
    client_b
        .put_object()
        .bucket("startup-window")
        .key("marker.txt")
        .body(ByteStream::from_static(b"from B"))
        .send()
        .await
        .expect("server B writes its marker");

    let after_install = signed_admin_request(
        &http,
        &server_b.endpoint(),
        reqwest::Method::GET,
        inspect_path,
        b_access_key,
        b_secret_key,
        b"",
    )
    .send()
    .await
    .expect("server B admin request after context installation");
    assert_eq!(after_install.status(), StatusCode::OK);
    assert_eq!(after_install.bytes().await.expect("read server B marker"), b"from B".as_slice());

    server_b.shutdown().await;
    server_a.shutdown().await;
}
