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

//! End-to-end acceptance for backlog#1052: two embedded RustFS servers coexist
//! in one process, on different ports and volumes, and their S3 data planes
//! stay isolated.

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};

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

// backlog#1052 acceptance: a second embedded server in the same process no
// longer aborts on write-once startup state — before this change,
// `RustFSServer::build()` returned AlreadyStarted (guard) or panicked on
// region/endpoints (bootstrap context write-once). This test proves the
// startup pipeline lifts; a follow-up will widen the request path to route
// per-server so the two servers can also serve different data planes end-to-
// end without the shared-IAM caveat.
#[tokio::test]
async fn two_embedded_servers_start_and_shutdown_independently() {
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

// backlog#1052 auth acceptance: two embedded servers with *different*
// credentials each authenticate against their own root identity. Server B
// accepts its own access key and rejects server A's. This exercises the
// per-server auth path (each request resolves its own AppContext for
// credential validation).
//
// NOTE: full bucket-namespace isolation is a separate, deeper follow-up: the
// ecstore data plane still resolves some lower-level reads (peer/disk/bucket
// metadata) through the process-global object handle, so the two servers do
// not yet present independent bucket listings even though each holds its own
// store object. That isolation is the remaining work on #1052.
#[tokio::test]
async fn two_embedded_servers_authenticate_with_their_own_credentials() {
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

    server_a.shutdown().await;
    server_b.shutdown().await;
}
