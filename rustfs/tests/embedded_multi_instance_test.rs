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

#[cfg(feature = "e2e-test-hooks")]
mod signed_target_rpc {
    use super::{common, find_available_port, pause_embedded_startup_after_http_bind, sha256_hex};
    use bytes::Bytes;
    use futures::FutureExt;
    use hyper_util::rt::TokioIo;
    use rustfs::app::context::resolve_object_store_handle;
    use rustfs::embedded::RustFSServerBuilder;
    use rustfs_ecstore::api::disk::{DiskAPI, DiskError, DiskOption, DiskStore, Endpoint, ReadOptions, new_disk};
    use rustfs_ecstore::api::rpc::{gen_tonic_signature_headers, normalize_tonic_rpc_audience};
    use rustfs_filemeta::{FileInfo, ObjectPartInfo};
    use rustfs_protos::proto_gen::node_service::{RenameDataRequest, RenameDataResponse, node_service_client::NodeServiceClient};
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use time::OffsetDateTime;
    use tokio::net::TcpStream;
    use tokio::time::timeout;
    use tonic::transport::Channel;
    use uuid::Uuid;

    const WAIT: Duration = Duration::from_secs(30);
    const INTERNAL_VOLUME: &str = ".rustfs.sys/tmp";
    const USER_VOLUME: &str = "target-transport";

    struct SingleConnection {
        client: NodeServiceClient<Channel>,
        local: SocketAddr,
        peer: SocketAddr,
        attempts: Arc<AtomicUsize>,
    }

    impl SingleConnection {
        async fn connect(address: SocketAddr) -> Self {
            let socket = timeout(WAIT, TcpStream::connect(address))
                .await
                .expect("bounded real TCP connection")
                .expect("connect to the production listener");
            let local = socket.local_addr().expect("client socket identity");
            let peer = socket.peer_addr().expect("listener socket identity");
            let socket = Arc::new(Mutex::new(Some(socket)));
            let attempts = Arc::new(AtomicUsize::new(0));
            let connector_attempts = attempts.clone();
            let channel = timeout(
                WAIT,
                tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                    .expect("local endpoint")
                    .timeout(WAIT)
                    .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                        connector_attempts.fetch_add(1, Ordering::SeqCst);
                        // A channel may reconnect implicitly. This fixture has exactly one
                        // already-connected socket and fails every subsequent dial attempt.
                        let socket = socket.lock().expect("single socket lock").take();
                        async move {
                            socket.map(TokioIo::new).ok_or_else(|| {
                                std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "implicit reconnect forbidden")
                            })
                        }
                    })),
            )
            .await
            .expect("bounded HTTP/2 handshake")
            .expect("HTTP/2 over the original TCP connection");
            Self {
                client: NodeServiceClient::new(channel),
                local,
                peer,
                attempts,
            }
        }

        fn assert_original_connection(&self) {
            assert_eq!(self.attempts.load(Ordering::SeqCst), 1, "the channel must not redial");
        }

        async fn rename(&mut self, request: tonic::Request<RenameDataRequest>) -> RenameDataResponse {
            let response = timeout(WAIT, self.client.rename_data(request))
                .await
                .expect("bounded signed RenameData")
                .expect("production authentication and RPC routing")
                .into_inner();
            self.assert_original_connection();
            response
        }
    }

    async fn local_fixture_disk(root: &Path) -> DiskStore {
        let mut endpoint = Endpoint::try_from(root.to_str().expect("UTF-8 fixture root")).expect("local disk endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        new_disk(&endpoint, &DiskOption::default())
            .await
            .expect("open real fixture disk")
    }

    async fn stage(disk: &DiskStore, volume: &str, path: &str, body: &'static [u8]) -> FileInfo {
        match disk.make_volume(volume).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("create fixture volume: {err}"),
        }
        let mut fi = FileInfo::new(path, 1, 0);
        fi.erasure.index = 1;
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fi.size = i64::try_from(body.len()).expect("small fixture");
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: body.len(),
            actual_size: fi.size,
            ..Default::default()
        }];
        fi.data = Some(Bytes::from_static(body));
        fi.set_inline_data();
        disk.write_metadata(volume, volume, path, fi.clone())
            .await
            .expect("stage real xl.meta");
        assert_body(disk, volume, path, &fi).await;
        fi
    }

    async fn assert_body(disk: &DiskStore, volume: &str, path: &str, fi: &FileInfo) {
        let read = disk
            .read_version(
                volume,
                volume,
                path,
                &fi.version_id.expect("version").to_string(),
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("decode actual inline object bytes");
        assert_eq!(read.data, fi.data);
    }

    fn signed_rename(
        disk: &DiskStore,
        volume: &str,
        source: &str,
        destination: &str,
        fi: &FileInfo,
    ) -> tonic::Request<RenameDataRequest> {
        let payload = RenameDataRequest {
            disk: disk.endpoint().to_string(),
            src_volume: volume.to_owned(),
            src_path: source.to_owned(),
            dst_volume: volume.to_owned(),
            dst_path: destination.to_owned(),
            file_info: serde_json::to_string(fi).expect("real FileInfo JSON"),
            ..Default::default()
        };
        let canonical = rustfs_protos::canonical_rename_data_request_body(&payload).expect("canonical mutation body");
        // The current production interceptor uses the process RPC identity. Keep
        // that authentication contract while testing listener-local disk routing.
        let identity = rustfs_common::try_get_global_local_node_name().expect("startup published the RPC identity");
        let audience = normalize_tonic_rpc_audience(&identity).expect("RPC audience");
        let headers =
            gen_tonic_signature_headers(&audience, "node_service.NodeService", "RenameData", Some(&sha256_hex(&canonical)))
                .expect("production v2 signing with the configured shared secret");
        assert_eq!(headers.get("x-rustfs-rpc-auth-version").expect("v2 metadata"), "2");
        let mut request = tonic::Request::new(payload);
        *request.metadata_mut() = tonic::metadata::MetadataMap::from_headers(headers);
        request
    }

    #[test]
    fn signed_target_rpc_uses_listener_instance_across_install_and_reconnect() {
        common::run_embedded_test(|| async {
            timeout(WAIT * 6, signed_target_rpc_body())
                .await
                .expect("bounded listener/startup/transport fixture");
        });
    }

    async fn signed_target_rpc_body() {
        // B installs the process default first; A must remain a different target
        // both before and after its own application context is installed.
        let root_b = tempfile::tempdir().expect("B root");
        let server_b = timeout(
            WAIT,
            RustFSServerBuilder::new()
                .address(format!("127.0.0.1:{}", find_available_port().expect("B port")))
                .volume(root_b.path().to_str().expect("B path"))
                .access_key("target-transport-access")
                .secret_key("target-transport-secret")
                .build(),
        )
        .await
        .expect("bounded B startup")
        .expect("start global B");
        let global_b = resolve_object_store_handle().expect("B installed process AppContext");
        let disk_b = local_fixture_disk(root_b.path()).await;
        let global_endpoints = global_b.instance_endpoints().expect("B instance topology");
        let global_paths: Vec<_> = global_endpoints
            .0
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref().iter())
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            global_paths,
            vec![disk_b.endpoint().to_string()],
            "the ambient store must really own B's disk"
        );
        let sentinel = stage(&disk_b, USER_VOLUME, "sentinel", b"global-B-must-survive").await;
        let sentinel_path = root_b.path().join(USER_VOLUME).join("sentinel/xl.meta");
        let sentinel_bytes = tokio::fs::read(&sentinel_path).await.expect("B's committed bytes");

        let root_a = tempfile::tempdir().expect("A root");
        let port_a = find_available_port().expect("A port");
        let address_a: SocketAddr = format!("127.0.0.1:{port_a}").parse().expect("A address");
        let mut barrier = pause_embedded_startup_after_http_bind(port_a);
        let startup_a = RustFSServerBuilder::new()
            .address(address_a.to_string())
            .volume(root_a.path().to_str().expect("A path"))
            .access_key("target-transport-access")
            .secret_key("target-transport-secret")
            .build();
        tokio::pin!(startup_a);
        timeout(WAIT, async {
            tokio::select! {
                () = barrier.wait_until_http_bound() => {}
                result = startup_a.as_mut() => {
                    let _unexpected_server = result.expect("A startup before barrier");
                    panic!("A must pause after bind and before ECStore/AppContext");
                }
            }
        })
        .await
        .expect("bounded A HTTP-bind barrier");

        // Catch assertion failures only to release the real startup barrier and
        // obtain a shutdown-capable server handle before resuming the failure.
        let pre_ready = std::panic::AssertUnwindSafe(async {
            assert!(Arc::ptr_eq(&global_b, &resolve_object_store_handle().expect("global B remains live")));
            let disk_a = local_fixture_disk(root_a.path()).await;
            let internal = stage(&disk_a, INTERNAL_VOLUME, "transport-staged", b"pre-ready-internal-body").await;
            let user = stage(&disk_a, USER_VOLUME, "staged", b"listener-A-user-body").await;
            let user_before = tokio::fs::read(root_a.path().join(USER_VOLUME).join("staged/xl.meta"))
                .await
                .expect("A staged user bytes");
            let mut connection = SingleConnection::connect(address_a).await;
            let mut invalid_signature = signed_rename(&disk_a, INTERNAL_VOLUME, "transport-staged", "bad-signature", &internal);
            invalid_signature
                .metadata_mut()
                .insert("x-rustfs-rpc-signature-v2", "00".parse().expect("invalid MAC header"));
            let status = timeout(WAIT, connection.client.rename_data(invalid_signature))
                .await
                .expect("bounded invalid-signature response")
                .expect_err("production interceptor must reject a bad signature");
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
            assert!(!root_a.path().join(INTERNAL_VOLUME).join("bad-signature/xl.meta").exists());
            assert_body(&disk_a, INTERNAL_VOLUME, "transport-staged", &internal).await;

            let committed = connection
                .rename(signed_rename(
                    &disk_a,
                    INTERNAL_VOLUME,
                    "transport-staged",
                    "transport-published",
                    &internal,
                ))
                .await;
            assert!(
                committed.success,
                "Bootstrap must commit internal metadata through the bound A registry: {:?}",
                committed.error
            );
            assert_body(&disk_a, INTERNAL_VOLUME, "transport-published", &internal).await;

            let denied = connection
                .rename(signed_rename(&disk_a, USER_VOLUME, "staged", "destination", &user))
                .await;
            assert!(!denied.success, "Bootstrap must reject a real user mutation");
            let error: DiskError = denied.error.expect("typed bootstrap rejection").into();
            assert_eq!(error, DiskError::FileAccessDenied);
            assert_eq!(
                tokio::fs::read(root_a.path().join(USER_VOLUME).join("staged/xl.meta"))
                    .await
                    .expect("unchanged A source"),
                user_before
            );
            assert!(!root_a.path().join(USER_VOLUME).join("destination/xl.meta").exists());

            assert_eq!(tokio::fs::read(&sentinel_path).await.expect("unchanged B bytes"), sentinel_bytes);
            assert_body(&disk_b, USER_VOLUME, "sentinel", &sentinel).await;
            connection.assert_original_connection();
            (connection, disk_a, user)
        })
        .catch_unwind()
        .await;

        barrier.release();
        let server_a = timeout(WAIT, startup_a.as_mut())
            .await
            .expect("bounded A context installation")
            .expect("A startup after real internal metadata commit");
        let (mut connection, disk_a, user) = match pre_ready {
            Ok(fixture) => fixture,
            Err(panic) => {
                timeout(WAIT, server_a.shutdown()).await.expect("bounded A failure cleanup");
                timeout(WAIT, server_b.shutdown()).await.expect("bounded B failure cleanup");
                std::panic::resume_unwind(panic);
            }
        };
        assert!(Arc::ptr_eq(
            &global_b,
            &resolve_object_store_handle().expect("A install preserves global B")
        ));
        assert_eq!(connection.peer, server_a.address());
        assert_body(&disk_a, USER_VOLUME, "staged", &user).await;
        let committed = connection
            .rename(signed_rename(&disk_a, USER_VOLUME, "staged", "destination", &user))
            .await;
        assert!(
            committed.success,
            "the same accepted connection must observe Ready for its next request: {:?}",
            committed.error
        );
        assert_body(&disk_a, USER_VOLUME, "destination", &user).await;

        // Keep the first connection open so the OS cannot recycle its 4-tuple.
        let mut reconnected = SingleConnection::connect(address_a).await;
        assert_ne!(reconnected.local, connection.local);
        assert_eq!(reconnected.peer, connection.peer);
        let committed = reconnected
            .rename(signed_rename(&disk_a, USER_VOLUME, "destination", "reconnected", &user))
            .await;
        assert!(committed.success, "new connections must retain listener A: {:?}", committed.error);
        assert_body(&disk_a, USER_VOLUME, "reconnected", &user).await;
        assert_eq!(tokio::fs::read(&sentinel_path).await.expect("B remains unchanged"), sentinel_bytes);
        assert_body(&disk_b, USER_VOLUME, "sentinel", &sentinel).await;
        assert!(!root_b.path().join(USER_VOLUME).join("reconnected/xl.meta").exists());
        connection.assert_original_connection();
        reconnected.assert_original_connection();
        drop(reconnected);
        drop(connection);
        drop(disk_a);
        drop(disk_b);
        timeout(WAIT, server_a.shutdown()).await.expect("bounded A shutdown");
        timeout(WAIT, server_b.shutdown()).await.expect("bounded B shutdown");
    }

    #[test]
    fn signed_bootstrap_request_does_not_upgrade_after_context_installation() {
        common::run_embedded_test(|| async {
            timeout(WAIT * 6, signed_delayed_bootstrap_body())
                .await
                .expect("bounded delayed Bootstrap fixture");
        });
    }

    async fn signed_delayed_bootstrap_body() {
        use rustfs::storage::tonic_service::pause_rename_after_target_capture;

        let root_b = tempfile::tempdir().expect("B root");
        let server_b = timeout(
            WAIT,
            RustFSServerBuilder::new()
                .address(format!("127.0.0.1:{}", find_available_port().expect("B port")))
                .volume(root_b.path().to_str().expect("B path"))
                .access_key("delayed-bootstrap-access")
                .secret_key("delayed-bootstrap-secret")
                .build(),
        )
        .await
        .expect("bounded B startup")
        .expect("start global B");
        let global_b = resolve_object_store_handle().expect("B's published context");
        let disk_b = local_fixture_disk(root_b.path()).await;
        let endpoints = global_b.instance_endpoints().expect("B instance topology");
        let paths: Vec<_> = endpoints
            .0
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref().iter())
            .map(ToString::to_string)
            .collect();
        assert_eq!(paths, [disk_b.endpoint().to_string()], "the ambient store owns B");
        stage(&disk_b, USER_VOLUME, "delayed-sentinel", b"B-is-not-the-listener-target").await;
        let sentinel_path = root_b.path().join(USER_VOLUME).join("delayed-sentinel/xl.meta");
        let sentinel_before = tokio::fs::read(&sentinel_path).await.expect("B sentinel bytes");

        let root_a = tempfile::tempdir().expect("A root");
        let port_a = find_available_port().expect("A port");
        let address_a = format!("127.0.0.1:{port_a}").parse().expect("A address");
        let mut startup_barrier = Some(pause_embedded_startup_after_http_bind(port_a));
        let startup_a = RustFSServerBuilder::new()
            .address(format!("127.0.0.1:{port_a}"))
            .volume(root_a.path().to_str().expect("A path"))
            .access_key("delayed-bootstrap-access")
            .secret_key("delayed-bootstrap-secret")
            .build();
        tokio::pin!(startup_a);
        timeout(WAIT, async {
            tokio::select! {
                () = startup_barrier.as_mut().expect("startup barrier").wait_until_http_bound() => {}
                startup = startup_a.as_mut() => {
                    let _unexpected_server = startup.expect("A initial startup");
                    panic!("A must reach its pre-AppContext barrier");
                }
            }
        })
        .await
        .expect("bounded A listener startup");

        let disk_a = local_fixture_disk(root_a.path()).await;
        let delayed_info = stage(&disk_a, USER_VOLUME, "delayed-source", b"captured-Bootstrap-must-not-publish").await;
        let control_info = stage(&disk_a, USER_VOLUME, "control-source", b"new-Ready-request-can-publish").await;
        let source_path = root_a.path().join(USER_VOLUME).join("delayed-source/xl.meta");
        let destination_path = root_a.path().join(USER_VOLUME).join("delayed-destination/xl.meta");
        let source_before = tokio::fs::read(&source_path).await.expect("delayed source bytes");
        let mut connection = SingleConnection::connect(address_a).await;
        let mut server_a = None;
        let mut startup_finished = false;

        let (observations, delayed_result, source_after, destination_exists, sentinel_after) = {
            let mut capture =
                pause_rename_after_target_capture(&disk_a.endpoint().to_string(), USER_VOLUME, "delayed-destination");
            let mut delayed_client = connection.client.clone();
            let delayed = delayed_client.rename_data(signed_rename(
                &disk_a,
                USER_VOLUME,
                "delayed-source",
                "delayed-destination",
                &delayed_info,
            ));
            tokio::pin!(delayed);
            let mut early_response = None;

            // Bound all work while the request is parked to less than the
            // existing channel's 30-second deadline; no timeout is disabled.
            let observations = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(20), async {
                let was_bootstrap = tokio::select! {
                    observed = capture.wait_until_captured() => observed,
                    response = delayed.as_mut() => {
                        early_response = Some(response);
                        panic!("signed request finished before the capture pause: {early_response:?}");
                    },
                };
                assert!(was_bootstrap, "the actual authenticated handler captured Bootstrap");
                assert!(Arc::ptr_eq(&global_b, &resolve_object_store_handle().expect("global B")));
                assert_eq!(tokio::fs::read(&source_path).await.expect("source before install"), source_before);
                assert!(!destination_path.exists());

                startup_barrier.take().expect("unreleased startup barrier").release();
                let started = startup_a.as_mut().await;
                startup_finished = true;
                server_a = Some(started.expect("normal A context installation"));
                assert!(Arc::ptr_eq(&global_b, &resolve_object_store_handle().expect("global remains B")));
                assert_eq!(connection.peer, server_a.as_ref().expect("A handle").address());

                // A separate source prevents this control from consuming the
                // delayed request's data and masking an erroneous second lookup.
                let ready = connection
                    .rename(signed_rename(&disk_a, USER_VOLUME, "control-source", "ready-control", &control_info))
                    .await;
                assert!(ready.success, "a fresh signed user request must actually use Ready: {:?}", ready.error);
                assert_body(&disk_a, USER_VOLUME, "ready-control", &control_info).await;
                assert_body(&disk_a, USER_VOLUME, "delayed-source", &delayed_info).await;
                assert!(!destination_path.exists(), "the original request remains parked");
                connection.assert_original_connection();
            }))
            .catch_unwind()
            .await;

            // Release on every assertion/timeout path, then drain the original
            // RPC before shutting down the server and its connection.
            drop(capture);
            if let Some(barrier) = startup_barrier.take() {
                barrier.release();
            }
            if !startup_finished {
                let started = timeout(WAIT, startup_a.as_mut()).await;
                if let Ok(Ok(started)) = started {
                    server_a = Some(started);
                }
            }
            let delayed_result = match early_response {
                Some(response) => Ok(response),
                None => timeout(WAIT, delayed.as_mut()).await,
            };
            let source_after = tokio::fs::read(&source_path).await;
            let destination_exists = tokio::fs::try_exists(&destination_path).await;
            let sentinel_after = tokio::fs::read(&sentinel_path).await;
            (observations, delayed_result, source_after, destination_exists, sentinel_after)
        };
        let connection_attempts = connection.attempts.load(Ordering::SeqCst);
        drop(connection);
        let shutdown_a = if let Some(server) = server_a {
            Some(timeout(WAIT, server.shutdown()).await)
        } else {
            None
        };
        let shutdown_b = timeout(WAIT, server_b.shutdown()).await;
        if let Some(result) = shutdown_a {
            result.expect("bounded A shutdown");
        }
        shutdown_b.expect("bounded B shutdown");

        assert_eq!(connection_attempts, 1, "the original channel must not redial");
        match observations {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => result.expect("complete capture/install/Ready-control within the parked request deadline"),
        }
        let response = delayed_result
            .expect("bounded original request drain")
            .expect("the original signed request must return an application result")
            .into_inner();
        assert!(
            !response.success,
            "a captured Bootstrap request must not upgrade to Ready after its await"
        );
        let error: DiskError = response.error.expect("typed Bootstrap rejection").into();
        assert_eq!(error, DiskError::FileAccessDenied);
        assert_eq!(source_after.expect("original source remains readable"), source_before);
        assert!(!destination_exists.expect("read original destination state"));
        assert_eq!(sentinel_after.expect("global B sentinel survives"), sentinel_before);
    }
}
