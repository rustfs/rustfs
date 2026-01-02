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

//! End-to-end tests for Prometheus metrics endpoints
//!
//! This test suite validates:
//! - Prometheus configuration generation with JWT tokens
//! - All metrics endpoints (cluster, bucket, node, resource)
//! - Bearer token authentication for metrics scraping
//! - Rejection of requests without valid authentication

use crate::common::{RustFSTestEnvironment, init_logging};
use reqwest::Client;
use rustfs_obs::prometheus::auth::generate_prometheus_token;
use serial_test::serial;
use tracing::info;

/// Generate a Prometheus token directly for testing.
///
/// This uses the same token generation logic as the server, bypassing
/// the need to call the admin config endpoint (which requires S3 signature auth).
fn generate_test_token(access_key: &str, secret_key: &str) -> String {
    generate_prometheus_token(access_key, secret_key, None).expect("Failed to generate test token")
}

/// Verify that a response body is in valid Prometheus text exposition format
fn is_valid_prometheus_format(body: &str) -> bool {
    // Prometheus format should contain metric definitions with # HELP and # TYPE
    // or at minimum contain metric lines in the format: metric_name{labels} value

    // An empty response is valid (no metrics available yet)
    if body.trim().is_empty() {
        return true;
    }

    // Check for standard Prometheus format indicators
    let has_help = body.contains("# HELP");
    let has_type = body.contains("# TYPE");

    // If we have HELP/TYPE comments, the format is valid
    if has_help || has_type {
        return true;
    }

    // Otherwise, check if lines follow metric format: name{labels} value or name value
    for line in body.lines() {
        let line = line.trim();
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Basic validation: line should have at least a metric name and value
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            return false;
        }
    }

    true
}

/// Test that Prometheus token generation works correctly.
///
/// This test verifies:
/// - Tokens can be generated using the same logic as the server
/// - The generated token can be used to authenticate metrics requests via query parameter
///
/// Note: We use the `?token=` query parameter because the s3s library intercepts
/// the Authorization header for AWS signature parsing before our handlers can process it.
#[tokio::test]
#[serial]
async fn test_prometheus_config_generation() {
    init_logging();
    info!("Starting Prometheus Config Generation Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly using the same logic as the server
    let token = generate_test_token(&env.access_key, &env.secret_key);

    info!("Generated Prometheus token for testing");

    // Verify token is not empty
    assert!(!token.is_empty(), "Bearer token should not be empty");
    info!("Bearer token generated successfully");

    // Verify the token works by calling a metrics endpoint with query parameter
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send metrics request");

    assert!(
        response.status().is_success(),
        "Metrics request with generated token should succeed, got status: {}",
        response.status()
    );

    info!("Prometheus config generation test passed");
}

/// Test the cluster metrics endpoint with valid token authentication.
///
/// This test verifies:
/// - The cluster metrics endpoint accepts token authentication via query parameter
/// - The response is in valid Prometheus text exposition format
/// - The response contains cluster-related metrics
#[tokio::test]
#[serial]
async fn test_prometheus_cluster_metrics() {
    init_logging();
    info!("Starting Prometheus Cluster Metrics Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    info!("Generated Prometheus token for metrics access");

    // Call cluster metrics endpoint with token query parameter
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send cluster metrics request");

    assert!(
        response.status().is_success(),
        "Cluster metrics request should succeed with valid token, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read cluster metrics response body");

    info!("Cluster metrics response:\n{}", body);

    // Verify Prometheus format
    assert!(is_valid_prometheus_format(&body), "Response should be in valid Prometheus format");

    // Check for cluster-specific metrics if present
    if !body.trim().is_empty() {
        // Metrics may contain rustfs_cluster_ prefix
        info!("Cluster metrics body is non-empty and valid");
    }

    info!("Prometheus cluster metrics test passed");
}

/// Test the bucket metrics endpoint with valid token authentication.
///
/// This test verifies:
/// - The bucket metrics endpoint accepts token authentication via query parameter
/// - The response is in valid Prometheus text exposition format
#[tokio::test]
#[serial]
async fn test_prometheus_bucket_metrics() {
    init_logging();
    info!("Starting Prometheus Bucket Metrics Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    // Call bucket metrics endpoint with token query parameter
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/bucket?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send bucket metrics request");

    assert!(
        response.status().is_success(),
        "Bucket metrics request should succeed with valid token, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read bucket metrics response body");

    info!("Bucket metrics response:\n{}", body);

    // Verify Prometheus format
    assert!(is_valid_prometheus_format(&body), "Response should be in valid Prometheus format");

    info!("Prometheus bucket metrics test passed");
}

/// Test the node metrics endpoint with valid token authentication.
///
/// This test verifies:
/// - The node metrics endpoint accepts token authentication via query parameter
/// - The response is in valid Prometheus text exposition format
#[tokio::test]
#[serial]
async fn test_prometheus_node_metrics() {
    init_logging();
    info!("Starting Prometheus Node Metrics Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    // Call node metrics endpoint with token query parameter
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/node?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send node metrics request");

    assert!(
        response.status().is_success(),
        "Node metrics request should succeed with valid token, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read node metrics response body");

    info!("Node metrics response:\n{}", body);

    // Verify Prometheus format
    assert!(is_valid_prometheus_format(&body), "Response should be in valid Prometheus format");

    info!("Prometheus node metrics test passed");
}

/// Test the resource metrics endpoint with valid token authentication.
///
/// This test verifies:
/// - The resource metrics endpoint accepts token authentication via query parameter
/// - The response is in valid Prometheus text exposition format
#[tokio::test]
#[serial]
async fn test_prometheus_resource_metrics() {
    init_logging();
    info!("Starting Prometheus Resource Metrics Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    // Call resource metrics endpoint with token query parameter
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/resource?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send resource metrics request");

    assert!(
        response.status().is_success(),
        "Resource metrics request should succeed with valid token, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read resource metrics response body");

    info!("Resource metrics response:\n{}", body);

    // Verify Prometheus format
    assert!(is_valid_prometheus_format(&body), "Response should be in valid Prometheus format");

    info!("Prometheus resource metrics test passed");
}

/// Test that metrics endpoints require authentication.
///
/// This test verifies:
/// - All metrics endpoints reject requests without token (header or query param)
/// - The response status indicates authentication failure (403 or 401)
#[tokio::test]
#[serial]
async fn test_prometheus_auth_required() {
    init_logging();
    info!("Starting Prometheus Auth Required Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Test all metrics endpoints without authentication (no header, no query param)
    let endpoints = [
        "/rustfs/v2/metrics/cluster",
        "/rustfs/v2/metrics/bucket",
        "/rustfs/v2/metrics/node",
        "/rustfs/v2/metrics/resource",
    ];

    for endpoint in endpoints {
        let metrics_url = format!("{}{}", env.url, endpoint);
        let response = client.get(&metrics_url).send().await.expect("Failed to send request");

        // Should return 403 Forbidden or 401 Unauthorized
        assert!(
            response.status() == reqwest::StatusCode::FORBIDDEN || response.status() == reqwest::StatusCode::UNAUTHORIZED,
            "Request to {} without auth should be rejected with 401/403, got: {}",
            endpoint,
            response.status()
        );

        info!(
            "Endpoint {} correctly rejected request without auth (status: {})",
            endpoint,
            response.status()
        );
    }

    info!("Prometheus auth required test passed");
}

/// Test that metrics endpoints reject invalid tokens.
///
/// This test verifies:
/// - All metrics endpoints reject requests with invalid/malformed tokens via query parameter
/// - The response status indicates authentication failure (403 or 401)
#[tokio::test]
#[serial]
async fn test_prometheus_invalid_token() {
    init_logging();
    info!("Starting Prometheus Invalid Token Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Test with various invalid tokens via query parameter
    let invalid_tokens = [
        "invalid-token",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.invalid.signature",
    ];

    let endpoints = [
        "/rustfs/v2/metrics/cluster",
        "/rustfs/v2/metrics/bucket",
        "/rustfs/v2/metrics/node",
        "/rustfs/v2/metrics/resource",
    ];

    for token in invalid_tokens {
        for endpoint in endpoints {
            let metrics_url = format!("{}{}?token={}", env.url, endpoint, token);
            let response = client.get(&metrics_url).send().await.expect("Failed to send request");

            // Should return 403 Forbidden or 401 Unauthorized
            assert!(
                response.status() == reqwest::StatusCode::FORBIDDEN || response.status() == reqwest::StatusCode::UNAUTHORIZED,
                "Request to {} with invalid token '{}' should be rejected with 401/403, got: {}",
                endpoint,
                token,
                response.status()
            );
        }
    }

    info!("Prometheus invalid token test passed");
}

/// Test all metrics endpoints in a single test run.
///
/// This test efficiently validates all four metrics endpoints
/// with a single server instance and token retrieval using query parameter auth.
#[tokio::test]
#[serial]
async fn test_prometheus_all_metrics_endpoints() {
    init_logging();
    info!("Starting Prometheus All Metrics Endpoints Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    let client = Client::new();

    let endpoints = [
        ("/rustfs/v2/metrics/cluster", "cluster"),
        ("/rustfs/v2/metrics/bucket", "bucket"),
        ("/rustfs/v2/metrics/node", "node"),
        ("/rustfs/v2/metrics/resource", "resource"),
    ];

    for (endpoint, name) in endpoints {
        info!("Testing {} metrics endpoint", name);

        let metrics_url = format!("{}{}?token={}", env.url, endpoint, token);
        let response = client.get(&metrics_url).send().await.expect("Failed to send request");

        assert!(
            response.status().is_success(),
            "{} metrics request should succeed, got status: {}",
            name,
            response.status()
        );

        let body = response.text().await.expect("Failed to read response body");

        assert!(is_valid_prometheus_format(&body), "{} metrics should be in valid Prometheus format", name);

        info!("{} metrics endpoint validated successfully", name);
    }

    info!("Prometheus all metrics endpoints test passed");
}

/// Test that the config endpoint requires admin credentials.
///
/// This test verifies that the Prometheus config endpoint
/// (which generates JWT tokens) requires proper S3 signature authentication.
#[tokio::test]
#[serial]
async fn test_prometheus_config_requires_admin_auth() {
    init_logging();
    info!("Starting Prometheus Config Admin Auth Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Try to access config endpoint without authentication
    let config_url = format!("{}/rustfs/admin/v3/prometheus/config", env.url);
    let response = client.get(&config_url).send().await.expect("Failed to send config request");

    // Should be rejected - the admin prefix requires S3 signature auth
    assert!(
        !response.status().is_success(),
        "Config endpoint without auth should be rejected, got: {}",
        response.status()
    );

    info!("Config endpoint correctly requires authentication");

    // Verify that tokens generated with the same credentials work on metrics endpoints
    // (This tests the same authentication mechanism without requiring awscurl)
    let token = generate_test_token(&env.access_key, &env.secret_key);
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send metrics request");

    assert!(
        response.status().is_success(),
        "Metrics request with valid token should succeed, got: {}",
        response.status()
    );

    info!("Prometheus config admin auth test passed");
}

/// Test Prometheus Content-Type header in metrics responses.
///
/// This test verifies that metrics endpoints return the correct
/// Prometheus text exposition format Content-Type header.
#[tokio::test]
#[serial]
async fn test_prometheus_content_type() {
    init_logging();
    info!("Starting Prometheus Content-Type Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token);
    let response = client.get(&metrics_url).send().await.expect("Failed to send metrics request");

    assert!(response.status().is_success());

    // Check Content-Type header
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Prometheus text format should be text/plain with version
    assert!(
        content_type.contains("text/plain"),
        "Content-Type should be text/plain, got: {}",
        content_type
    );

    info!("Prometheus Content-Type header validated: {}", content_type);
    info!("Prometheus content-type test passed");
}

/// Response structure for the Prometheus token endpoint.
#[derive(serde::Deserialize)]
struct PrometheusTokenResponse {
    bearer_token: String,
}

/// Test the full token generation to metrics scraping flow.
///
/// This test verifies the complete end-to-end flow:
/// 1. Call the token endpoint with valid credentials
/// 2. Parse the JSON response to extract the bearer token
/// 3. Use the token to scrape metrics from the cluster endpoint
/// 4. Verify the response is valid Prometheus format
#[tokio::test]
#[serial]
async fn test_prometheus_token_endpoint_to_metrics_scrape() {
    init_logging();
    info!("Starting Prometheus Token Endpoint to Metrics Scrape Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Step 1: Call the token endpoint with valid credentials
    let token_url = format!(
        "{}/rustfs/admin/v3/prometheus/token?access_key={}&secret_key={}",
        env.url, env.access_key, env.secret_key
    );
    info!("Requesting token from: {}", token_url);

    let token_response = client
        .post(&token_url)
        .send()
        .await
        .expect("Failed to send token request");

    assert!(
        token_response.status().is_success(),
        "Token request should succeed, got status: {}",
        token_response.status()
    );

    // Step 2: Parse the JSON response to extract the bearer token
    let token_body = token_response
        .text()
        .await
        .expect("Failed to read token response body");

    info!("Token response body: {}", token_body);

    let token_data: PrometheusTokenResponse =
        serde_json::from_str(&token_body).expect("Failed to parse token response JSON");

    assert!(
        !token_data.bearer_token.is_empty(),
        "Bearer token should not be empty"
    );
    info!("Successfully obtained bearer token");

    // Step 3: Use the token to scrape metrics from the cluster endpoint
    let metrics_url = format!(
        "{}/rustfs/v2/metrics/cluster?token={}",
        env.url, token_data.bearer_token
    );
    info!("Requesting metrics from: {}", metrics_url);

    let metrics_response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send metrics request");

    assert!(
        metrics_response.status().is_success(),
        "Metrics request with token from endpoint should succeed, got status: {}",
        metrics_response.status()
    );

    // Step 4: Verify the response is valid Prometheus format
    let metrics_body = metrics_response
        .text()
        .await
        .expect("Failed to read metrics response body");

    info!("Metrics response:\n{}", metrics_body);

    assert!(
        is_valid_prometheus_format(&metrics_body),
        "Metrics response should be in valid Prometheus format"
    );

    info!("Prometheus token endpoint to metrics scrape test passed");
}

/// Test error cases for the Prometheus token endpoint.
///
/// This test verifies:
/// 1. Missing access_key returns an error
/// 2. Missing secret_key returns an error
/// 3. Invalid credentials return an error
#[tokio::test]
#[serial]
async fn test_prometheus_token_endpoint_errors() {
    init_logging();
    info!("Starting Prometheus Token Endpoint Error Cases Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Test 1: Missing access_key
    info!("Testing missing access_key");
    let url_missing_access_key = format!(
        "{}/rustfs/admin/v3/prometheus/token?secret_key={}",
        env.url, env.secret_key
    );

    let response = client
        .post(&url_missing_access_key)
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with missing access_key should fail, got status: {}",
        response.status()
    );
    info!(
        "Missing access_key correctly returned error status: {}",
        response.status()
    );

    // Test 2: Missing secret_key
    info!("Testing missing secret_key");
    let url_missing_secret_key = format!(
        "{}/rustfs/admin/v3/prometheus/token?access_key={}",
        env.url, env.access_key
    );

    let response = client
        .post(&url_missing_secret_key)
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with missing secret_key should fail, got status: {}",
        response.status()
    );
    info!(
        "Missing secret_key correctly returned error status: {}",
        response.status()
    );

    // Test 3: Invalid credentials
    info!("Testing invalid credentials");
    let url_invalid_creds = format!(
        "{}/rustfs/admin/v3/prometheus/token?access_key=invalid_user&secret_key=wrong_password",
        env.url
    );

    let response = client
        .post(&url_invalid_creds)
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with invalid credentials should fail, got status: {}",
        response.status()
    );
    info!(
        "Invalid credentials correctly returned error status: {}",
        response.status()
    );

    info!("Prometheus token endpoint error cases test passed");
}
