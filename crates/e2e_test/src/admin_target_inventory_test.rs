// Copyright 2026 RustFS Team
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

//! Exercise the target inventory through HTTP authentication and authorization.
//! Handler-level tests separately control reconciliation locks and config faults.

use crate::common::{RustFSTestEnvironment, admin_create_user, admin_request, init_logging, local_http_client};
use http::header::{AUTHORIZATION, HOST};
use reqwest::StatusCode;
use rustfs_signer::{constants::UNSIGNED_PAYLOAD, sign_v4};
use s3s::Body;
use serde_json::Value;
use std::{error::Error, time::Duration};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const SECRET: &str = "inventory-acceptance-token";
const LIMITED_KEY: &str = "inventory-limited";
const LIMITED_SECRET: &str = "inventory-limited-secret";
const PATHS: [&str; 5] = [
    "/rustfs/admin/v3/target/list",
    "/rustfs/admin/v3/audit/target/list",
    "/rustfs/admin/v4/plugins/instances",
    "/rustfs/admin/v4/plugins/instances/builtin:webhook:notify:acceptance",
    "/rustfs/admin/v4/extensions/instances",
];

async fn duplicate_authorization(
    env: &RustFSTestEnvironment,
    path: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}{path}", env.url);
    let uri: http::Uri = url.parse()?;
    let authority = uri.authority().ok_or("inventory URL missing authority")?.to_string();
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .body(Body::empty())?;
    let signed = sign_v4(request, 0, &env.access_key, &env.secret_key, "", "us-east-1");
    let mut headers = signed.headers().clone();
    let authorization = headers.get(AUTHORIZATION).ok_or("signer omitted authorization")?.clone();
    headers.append(AUTHORIZATION, authorization);
    assert_eq!(headers.get_all(AUTHORIZATION).iter().count(), 2);
    Ok(local_http_client().get(url).headers(headers).send().await?)
}

fn assert_unloaded_target(entry: &Value, path: &str) {
    assert_eq!(entry["service"], "webhook", "{path}: unexpected target service");
    assert!(
        entry["account_id"]
            .as_str()
            .is_some_and(|id| id.eq_ignore_ascii_case("acceptance")),
        "{path}: fixture target is missing"
    );
    assert_eq!(entry["status"], "offline", "{path}: unloaded targets must remain visible as offline");
    if !path.contains("/extensions/") {
        assert_eq!(entry["health_reason"], "not_loaded_in_runtime", "{path}: unexpected health reason");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn target_inventory_preserves_unloaded_targets_and_http_auth_boundaries() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    // Enabled destinations remain configured while both delivery modules are
    // disabled, so inventory reads must not require a loaded target runtime.
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_NOTIFY_ENABLE", "false"),
            ("RUSTFS_AUDIT_ENABLE", "false"),
            ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_ACCEPTANCE", "on"),
            ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_ACCEPTANCE", "http://127.0.0.1:9/hook"),
            ("RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN_ACCEPTANCE", SECRET),
            ("RUSTFS_AUDIT_WEBHOOK_ENABLE_ACCEPTANCE", "on"),
            ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_ACCEPTANCE", "http://127.0.0.1:9/hook"),
            ("RUSTFS_AUDIT_WEBHOOK_AUTH_TOKEN_ACCEPTANCE", SECRET),
        ],
    )
    .await?;
    admin_create_user(&env, LIMITED_KEY, LIMITED_SECRET).await?;

    tokio::time::timeout(Duration::from_secs(60), async {
        for path in PATHS {
            let (status, body) = admin_request(&env.url, http::Method::GET, path, None, &env.access_key, &env.secret_key).await?;
            assert_eq!(status, StatusCode::OK, "{path}: signed admin inventory failed");
            assert!(!body.contains(SECRET), "{path}: inventory exposed a target secret");
            let value: Value = serde_json::from_str(&body)?;
            let entries: Vec<&Value> = if path.ends_with("/target/list") {
                let field = if path.contains("/audit/") {
                    "audit_endpoints"
                } else {
                    "notification_endpoints"
                };
                value[field].as_array().ok_or("missing endpoint inventory")?.iter().collect()
            } else if path.ends_with("/instances") {
                value["instances"]
                    .as_array()
                    .ok_or("missing instance inventory")?
                    .iter()
                    .collect()
            } else {
                vec![&value]
            };
            let targets: Vec<_> = entries
                .into_iter()
                .filter(|entry| {
                    entry["account_id"]
                        .as_str()
                        .is_some_and(|id| id.eq_ignore_ascii_case("acceptance"))
                })
                .collect();
            let expected = if path.ends_with("/instances") { 2 } else { 1 };
            assert_eq!(targets.len(), expected, "{path}: configured targets disappeared");
            for entry in &targets {
                assert_unloaded_target(entry, path);
                if path.contains("/v4/") {
                    assert_eq!(entry["config"]["auth_token"], "***redacted***", "{path}: missing secret redaction");
                }
            }
            if expected == 2 {
                for domain in ["notify", "audit"] {
                    assert!(targets.iter().any(|entry| entry["domain"] == domain), "{path}: missing {domain} domain");
                }
            }

            let unsigned = local_http_client().get(format!("{}{path}", env.url)).send().await?;
            assert_eq!(unsigned.status(), StatusCode::FORBIDDEN, "{path}: unsigned request was not denied");
            let body = unsigned.text().await?;
            assert!(body.contains("<Code>AccessDenied</Code>") && body.contains("Signature is required"));

            let ambiguous = duplicate_authorization(&env, path).await?;
            assert_eq!(
                ambiguous.status(),
                StatusCode::FORBIDDEN,
                "{path}: duplicate authorization was not rejected"
            );
            let body = ambiguous.text().await?;
            assert!(body.contains("<Code>AccessDenied</Code>") && body.contains("Signature is required"));

            let (status, body) = admin_request(&env.url, http::Method::GET, path, None, LIMITED_KEY, LIMITED_SECRET).await?;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path}: principal without admin policy was not denied");
            assert!(body.contains("<Code>AccessDenied</Code>"), "{path}: unexpected authorization error");
            assert!(!body.contains("Signature is required") && !body.contains("SignatureDoesNotMatch"));
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    env.stop_server();
    Ok(())
}
