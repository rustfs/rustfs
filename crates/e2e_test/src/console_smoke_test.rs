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

//! Over-the-wire smoke net for the embedded console listener (backlog#1154
//! peri-4). The console is a second HTTP listener that serves the full S3/admin
//! surface plus the unauthenticated console routes under `/rustfs/console`;
//! before this test its behaviour was only covered by in-process router tests
//! (`rustfs/src/admin/console.rs`), so a misconfiguration that exposed
//! credentials through the public console endpoints or fail-opened the
//! protected surface on the console port had no regression net.
//!
//! Pinned wire contract (real binary, real TCP):
//!   * `/rustfs/console/version` and `/rustfs/console/license` answer 200 JSON
//!     without authentication, with complete fields and no credential material.
//!   * `/rustfs/console/license` exposes only the coarse `licensed` flag.
//!   * the console SPA prefix dispatches to the static handler, never to the
//!     S3 API (no S3 error XML), whether or not console assets are embedded.
//!   * unauthenticated requests to the admin API and the S3 root on the console
//!     listener are denied (403 AccessDenied) — the extra listener does not
//!     fail-open the protected surface.
//!   * a listener with the console disabled (the main S3 port here) does not
//!     serve the unauthenticated console endpoints at all.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use serial_test::serial;
use std::error::Error;
use tokio::time::{Duration, sleep};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

/// Polls the console version endpoint until the console listener accepts
/// requests. The harness readiness check only covers the S3 listener; the
/// console listener of the same process may come up moments later.
async fn wait_for_console_ready(console_base: &str) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let client = local_http_client();
    let url = format!("{console_base}/rustfs/console/version");
    let mut last_err = String::new();
    for _ in 0..40 {
        match client.get(&url).send().await {
            Ok(response) if response.status() == reqwest::StatusCode::OK => return Ok(response),
            Ok(response) => last_err = format!("status {}", response.status()),
            Err(err) => last_err = err.to_string(),
        }
        sleep(Duration::from_millis(250)).await;
    }
    Err(format!("console listener at {console_base} never became ready: {last_err}").into())
}

#[tokio::test]
#[serial]
async fn test_console_over_the_wire_smoke() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    // A unique secret makes the credential-leak assertions below sharp: any
    // occurrence of it in a console response body is a genuine leak, not a
    // collision with a common default string.
    env.access_key = format!("peri4ak{}", uuid::Uuid::new_v4().simple());
    env.secret_key = format!("peri4sk{}", uuid::Uuid::new_v4().simple());

    let console_port = RustFSTestEnvironment::find_available_port().await?;
    let console_address = format!("127.0.0.1:{console_port}");
    let console_base = format!("http://{console_address}");
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_CONSOLE_ENABLE", "true"),
            ("RUSTFS_CONSOLE_ADDRESS", console_address.as_str()),
        ],
    )
    .await?;

    let client = local_http_client();

    // --- /rustfs/console/version: 200 JSON, complete fields, no secrets ------
    let version_response = wait_for_console_ready(&console_base).await?;
    let content_type = version_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(content_type.starts_with("application/json"), "version content-type: {content_type}");
    let version_body = version_response.text().await?;
    let version_json: serde_json::Value = serde_json::from_str(&version_body)?;
    for field in ["version", "version_info", "date"] {
        assert!(
            version_json[field].as_str().is_some_and(|v| !v.is_empty()),
            "console version field {field} missing or empty: {version_body}"
        );
    }
    assert!(
        !version_body.contains(&env.secret_key) && !version_body.contains(&env.access_key),
        "console version response leaks credentials: {version_body}"
    );

    // --- /rustfs/console/license: coarse licensed flag only ------------------
    let license_response = client.get(format!("{console_base}/rustfs/console/license")).send().await?;
    assert_eq!(license_response.status(), reqwest::StatusCode::OK);
    let license_body = license_response.text().await?;
    let license_json: serde_json::Value = serde_json::from_str(&license_body)?;
    assert!(
        license_json["licensed"].is_boolean(),
        "license payload must expose a licensed bool: {license_body}"
    );
    let license_keys: Vec<&String> = license_json.as_object().map(|o| o.keys().collect()).unwrap_or_default();
    assert_eq!(
        license_keys,
        vec!["licensed"],
        "public license endpoint must not expose license metadata: {license_body}"
    );
    assert!(
        !license_body.contains(&env.secret_key) && !license_body.contains(&env.access_key),
        "console license response leaks credentials: {license_body}"
    );

    // --- console SPA prefix dispatches to the static handler, not the S3 API -
    // With release console assets embedded this is 200 text/html; a from-source
    // binary embeds an empty static dir and serves the handler's 404 fallback.
    // Either way it must never fall through to an S3 handler (S3 error XML).
    let spa_response = client.get(format!("{console_base}/rustfs/console/")).send().await?;
    let spa_status = spa_response.status();
    let spa_content_type = spa_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let spa_body = spa_response.text().await?;
    assert!(
        !spa_body.contains("<Error>"),
        "console SPA route fell through to the S3 API: status {spa_status}, body {spa_body}"
    );
    match spa_status {
        reqwest::StatusCode::OK => {
            assert!(
                spa_content_type.starts_with("text/html"),
                "embedded console index must be html, got {spa_content_type}"
            );
        }
        reqwest::StatusCode::NOT_FOUND => {
            assert!(
                spa_body.contains("RustFS"),
                "asset-less console 404 must come from the console static handler: {spa_body}"
            );
        }
        other => panic!("console SPA route returned unexpected status {other}: {spa_body}"),
    }

    // --- protected surface on the console listener stays authenticated -------
    let admin_response = client.get(format!("{console_base}/rustfs/admin/v3/info")).send().await?;
    assert_eq!(
        admin_response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "unauthenticated admin API on the console listener must be denied"
    );
    let admin_body = admin_response.text().await?;
    assert!(admin_body.contains("AccessDenied"), "admin denial must be AccessDenied: {admin_body}");

    let s3_root_response = client.get(format!("{console_base}/")).send().await?;
    assert_eq!(
        s3_root_response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "unauthenticated S3 root on the console listener must be denied"
    );

    // --- console disabled on a listener means no console surface at all ------
    // The main S3 listener of this same process runs with the console disabled,
    // so its console paths must not answer with the unauthenticated console
    // payloads (they fall through to the authenticated S3 surface instead).
    let s3_listener_console = client.get(format!("{}/rustfs/console/version", env.url)).send().await?;
    assert_ne!(
        s3_listener_console.status(),
        reqwest::StatusCode::OK,
        "console endpoints must not be served on a console-disabled listener"
    );

    env.stop_server();
    Ok(())
}
