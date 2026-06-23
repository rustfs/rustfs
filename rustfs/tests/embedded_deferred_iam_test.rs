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
use aws_sdk_s3::{Client, Config};
use reqwest::StatusCode;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_config::{ENV_TEST_IAM_FAIL_INIT_ATTEMPTS, ENV_TEST_IAM_RETRY_INTERVAL_MS};
use std::time::Duration;
use temp_env::async_with_vars;

fn response_preview(body: &str) -> String {
    body.chars().take(512).collect()
}

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

#[cfg(debug_assertions)]
#[tokio::test]
async fn test_embedded_server_recovers_after_deferred_iam_bootstrap() {
    async_with_vars(
        [
            (ENV_TEST_IAM_FAIL_INIT_ATTEMPTS, Some("1")),
            (ENV_TEST_IAM_RETRY_INTERVAL_MS, Some("500")),
        ],
        async {
            let port = match find_available_port() {
                Ok(port) => port,
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
                Err(err) => panic!("find free port: {err}"),
            };
            let server = RustFSServerBuilder::new()
                .address(format!("127.0.0.1:{port}"))
                .access_key("testaccesskey")
                .secret_key("testsecretkey")
                .build()
                .await
                .expect("start embedded server with deferred IAM bootstrap");

            let endpoint = server.endpoint();
            let http = reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("build local readiness client without proxy");
            let ready_url = format!("{endpoint}/health/ready");

            let initial_ready = http
                .get(&ready_url)
                .send()
                .await
                .expect("readiness probe should respond during deferred bootstrap");
            let initial_status = initial_ready.status();
            let initial_body = initial_ready
                .text()
                .await
                .expect("readiness response body should be readable");
            assert_eq!(
                initial_status,
                StatusCode::SERVICE_UNAVAILABLE,
                "initial readiness response body: {}",
                response_preview(&initial_body)
            );

            let mut last_ready_response = None;
            let recovered = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let response = http
                        .get(&ready_url)
                        .send()
                        .await
                        .expect("readiness probe should keep responding");
                    let status = response.status();
                    if status == StatusCode::OK {
                        return true;
                    }
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|err| format!("failed to read body: {err}"));
                    last_ready_response = Some((status, response_preview(&body)));
                }
            })
            .await
            .unwrap_or(false);
            assert!(
                recovered,
                "readiness should recover after deferred IAM bootstrap succeeds; last response: {:?}",
                last_ready_response
            );

            let client = s3_client(&endpoint, server.access_key(), server.secret_key());
            client
                .create_bucket()
                .bucket("deferred-bucket")
                .send()
                .await
                .expect("create bucket after readiness recovery");

            server.shutdown().await;
        },
    )
    .await;
}
