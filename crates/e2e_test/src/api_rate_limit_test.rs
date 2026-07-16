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

//! E2E coverage for the opt-in per-client S3 API rate limit (backlog#1191):
//! the layer must be wired into the real server stack, reject over-limit
//! clients with `429` + `Retry-After`, keep health probes exempt, and stay
//! completely inert with default configuration.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use serial_test::serial;
use tracing::info;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[serial]
async fn api_rate_limit_enforces_429_with_retry_after_when_enabled() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    // Burst 50 leaves headroom for the readiness-poll ListBuckets calls that
    // share the loopback client IP; refill (60 rpm = 1/s) is slow enough that
    // a rapid burst below reliably exhausts the bucket.
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_API_RATE_LIMIT_ENABLE", "true"),
            ("RUSTFS_API_RATE_LIMIT_RPM", "60"),
            ("RUSTFS_API_RATE_LIMIT_BURST", "50"),
        ],
    )
    .await?;

    let client = local_http_client();
    let list_buckets_url = format!("{}/", env.url);

    let mut throttled = None;
    let mut allowed = 0usize;
    for _ in 0..80 {
        let response = client.get(&list_buckets_url).send().await?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            throttled = Some(response);
            break;
        }
        allowed += 1;
    }

    let throttled = throttled.unwrap_or_else(|| panic!("no 429 within 80 rapid requests ({allowed} allowed) at burst 50"));
    assert!(allowed > 0, "healthy traffic below the burst must not be throttled");
    info!("rate limit engaged after {allowed} allowed requests");

    let retry_after = throttled
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .expect("429 must carry a numeric Retry-After header");
    assert!(retry_after >= 1, "Retry-After must be at least one second, got {retry_after}");
    assert_eq!(
        throttled.headers().get("x-ratelimit-limit").and_then(|v| v.to_str().ok()),
        Some("60"),
        "429 must expose the configured limit"
    );

    let body = throttled.text().await?;
    assert!(body.contains("<Code>TooManyRequests</Code>"), "S3-style error body expected: {body}");

    // Health probes stay exempt even while the client budget is exhausted.
    let health = client.get(format!("{}/health", env.url)).send().await?;
    assert_ne!(
        health.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "health probes must never be rate limited"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn api_rate_limit_stays_inert_by_default() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let client = local_http_client();
    let list_buckets_url = format!("{}/", env.url);

    for i in 0..80 {
        let response = client.get(&list_buckets_url).send().await?;
        assert_ne!(
            response.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "request {i} was throttled although rate limiting is disabled by default"
        );
    }

    Ok(())
}
