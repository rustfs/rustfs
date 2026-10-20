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

use reqwest::{Client, StatusCode};
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_ecstore::api::{config::com::save_config, rebalance::RebalanceMeta};
use s3s_sigv4::{
    AmzDate, EMPTY_STRING_SHA256_HASH, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use std::{path::Path, sync::Arc, time::Duration};

mod common;

const ACCESS_KEY: &str = "rebalance-status-test";
const SECRET_KEY: &str = "rebalance-status-test-secret";
const DENIED_KEY: &str = "rebalance-status-denied";
const STATUS_PATH: &str = "/rustfs/admin/v3/rebalance/status";
const IDLE_XML: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchResource</Code><Message>pool rebalance is not started</Message></Error>";

fn signed_status_request(
    client: &Client,
    endpoint: &str,
    path: &str,
    access_key: &str,
    secret_key: &str,
) -> reqwest::RequestBuilder {
    let host = endpoint.strip_prefix("http://").expect("local HTTP endpoint");
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let date = AmzDate::parse(&timestamp).expect("valid signing date");
    let canonical = create_canonical_request(
        "GET",
        path,
        &[] as &[(&str, &str)],
        [
            ("host", host),
            ("x-amz-content-sha256", EMPTY_STRING_SHA256_HASH),
            ("x-amz-date", timestamp.as_str()),
        ],
        Payload::SingleChunk(EMPTY_STRING_SHA256_HASH),
    );
    let string_to_sign = create_string_to_sign(&canonical, &date, "us-east-1", "s3");
    let signature = calculate_signature(&string_to_sign, secret_key, &date, "us-east-1", "s3");
    client
        .get(format!("{endpoint}{path}"))
        .header("host", host)
        .header("x-amz-date", &timestamp)
        .header("x-amz-content-sha256", EMPTY_STRING_SHA256_HASH)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}",
                date.fmt_date()
            ),
        )
}

async fn status_response_with_logs(
    request: reqwest::RequestBuilder,
    log_path: &Path,
) -> (StatusCode, reqwest::header::HeaderMap, bytes::Bytes, String) {
    let before = usize::try_from(std::fs::metadata(log_path).expect("captured log metadata").len()).expect("log size fits usize");
    let response = request.send().await.expect("send status request");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.expect("read response body");
    let logs = std::fs::read_to_string(log_path).expect("read captured log");
    (status, headers, body, logs[before..].to_string())
}

#[test]
fn rebalance_status_preserves_http_contract_and_log_severity() {
    let log = tempfile::NamedTempFile::new().expect("create captured log");
    tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter(
            tracing_subscriber::EnvFilter::new("off")
                .add_directive("s3s::ops=debug".parse().expect("S3 route log filter"))
                .add_directive(
                    "rustfs::admin::handlers::rebalance=debug"
                        .parse()
                        .expect("rebalance log filter"),
                ),
        )
        .with_writer(log.reopen().expect("open captured log writer"))
        .try_init()
        .expect("install request log capture");

    common::run_embedded_test(move || async move {
        let port = find_available_port().expect("find local test port");
        let server = RustFSServerBuilder::new()
            .address(format!("127.0.0.1:{port}"))
            .access_key(ACCESS_KEY)
            .secret_key(SECRET_KEY)
            .build()
            .await
            .expect("start embedded RustFS");
        let endpoint = server.endpoint();
        let client = Client::builder().no_proxy().timeout(Duration::from_secs(30)).build().unwrap();

        for path in [STATUS_PATH, "/minio/admin/v3/rebalance/status"] {
            for _ in 0..2 {
                let (status, headers, body, logs) = status_response_with_logs(
                    signed_status_request(&client, &endpoint, path, ACCESS_KEY, SECRET_KEY),
                    log.path(),
                )
                .await;
                assert_eq!(status, StatusCode::NOT_FOUND);
                assert_eq!(headers[reqwest::header::CONTENT_TYPE], "application/xml");
                assert_eq!(body.as_ref(), IDLE_XML);
                assert!(!logs.contains("ERROR"), "idle polls must not log errors: {logs}");
                assert!(!logs.contains("WARN"), "idle polls must not be reported as rejected requests: {logs}");
            }
        }

        let (status, _, body, _) = status_response_with_logs(
            signed_status_request(&client, &endpoint, STATUS_PATH, ACCESS_KEY, "incorrect-secret"),
            log.path(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(&body).contains("SignatureDoesNotMatch"));

        let context = get_global_app_context().expect("embedded server context");
        context
            .iam()
            .handle()
            .create_user(
                DENIED_KEY,
                &rustfs_madmin::AddOrUpdateUserReq {
                    secret_key: SECRET_KEY.to_string(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Enabled,
                },
            )
            .await
            .expect("create user without rebalance permission");
        let (status, _, body, _) =
            status_response_with_logs(signed_status_request(&client, &endpoint, STATUS_PATH, DENIED_KEY, SECRET_KEY), log.path())
                .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(&body).contains("AccessDenied"));

        let store = context.object_store();
        let meta = RebalanceMeta {
            id: "status-test-run".to_string(),
            pool_stats: vec![Default::default()],
            ..Default::default()
        };
        meta.save(Arc::clone(&store.pools[0]))
            .await
            .expect("persist valid rebalance metadata");
        let (status, headers, body, logs) =
            status_response_with_logs(signed_status_request(&client, &endpoint, STATUS_PATH, ACCESS_KEY, SECRET_KEY), log.path())
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[reqwest::header::CONTENT_TYPE], "application/json");
        let status: serde_json::Value = serde_json::from_slice(&body).expect("status JSON");
        assert_eq!(status["id"], "status-test-run");
        assert!(!logs.contains("ERROR"), "valid metadata must not log errors: {logs}");

        save_config(store, "rebalance.bin", b"bad".to_vec())
            .await
            .expect("inject corrupt rebalance metadata");
        let (status, _, body, logs) =
            status_response_with_logs(signed_status_request(&client, &endpoint, STATUS_PATH, ACCESS_KEY, SECRET_KEY), log.path())
                .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(String::from_utf8_lossy(&body).contains("<Code>InternalError</Code>"));
        assert!(
            logs.contains("ERROR") && logs.contains("custom route returns error"),
            "unexpected failure must retain diagnostics: {logs}"
        );
        server.shutdown().await;
    });
}
