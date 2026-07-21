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

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_notify::{NotificationRuntimeState, notification_system};

fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "test");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

#[tokio::test]
async fn notification_runtime_stays_enabled_until_the_last_embedded_owner_drains() {
    temp_env::async_with_vars([(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"))], async {
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
            .expect("start embedded server B");

        let notification = notification_system().expect("embedded startup should initialize notification runtime");
        let active_state = notification.runtime_lifecycle_state();
        assert!(matches!(active_state, NotificationRuntimeState::TargetsEnabled { .. }));
        assert!(notification.runtime_lifecycle_is_converged());

        server_b.shutdown().await;
        assert_eq!(
            notification.runtime_lifecycle_state(),
            active_state,
            "one owner must not suspend the shared notification runtime"
        );
        assert!(notification.runtime_lifecycle_is_converged());

        let client_a = s3_client(&server_a.endpoint(), server_a.access_key(), server_a.secret_key());
        client_a
            .create_bucket()
            .bucket("survives-notify-owner-shutdown")
            .send()
            .await
            .expect("server A should remain usable after server B shuts down");
        client_a
            .put_object()
            .bucket("survives-notify-owner-shutdown")
            .key("marker.txt")
            .body(ByteStream::from_static(b"still here"))
            .send()
            .await
            .expect("server A should still write after server B shuts down");

        server_a.shutdown().await;
        assert_eq!(notification.runtime_lifecycle_state(), NotificationRuntimeState::LiveOnly);
        assert!(notification.runtime_lifecycle_is_converged());
    })
    .await;
}
