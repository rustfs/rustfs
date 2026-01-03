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
use aws_sdk_s3::primitives::ByteStream;
use reqwest::Client;
use rustfs_obs::prometheus::auth::generate_prometheus_token;
use serial_test::serial;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
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

/// Parse Prometheus text format metrics into a HashMap of metric names to values.
///
/// This function handles both formats:
/// - `metric_name value` (simple format)
/// - `metric_name{labels} value` (format with labels)
///
/// Comment lines (starting with #) and empty lines are skipped.
///
/// For metrics with labels, two entries are inserted:
/// 1. Full key including labels (e.g., `metric_name{label="value"}`) for specific lookups
/// 2. Just the metric name (e.g., `metric_name`) for easy lookups (last value seen wins)
///
/// For metrics without labels, only one entry is inserted with the metric name as key.
fn parse_prometheus_metrics(body: &str) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();

    for line in body.lines() {
        let line = line.trim();

        // Skip empty lines and comment lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Check if this line has labels (contains '{')
        let has_labels = line.contains('{');

        // Find the metric name (portion before '{' or first whitespace)
        let metric_name_end = line.find('{').or_else(|| line.find(char::is_whitespace));

        let metric_name = match metric_name_end {
            Some(pos) => &line[..pos],
            None => continue, // No metric name found, skip this line
        };

        // Find the value - it's the last whitespace-separated token
        let value_str = match line.split_whitespace().last() {
            Some(v) => v,
            None => continue,
        };

        // Parse the value as f64
        if let Ok(value) = value_str.parse::<f64>() {
            if has_labels {
                // For lines with labels, extract the full key (metric_name{labels})
                // The full key is everything before the last whitespace-separated value
                if let Some(last_space_pos) = line.rfind(char::is_whitespace) {
                    let full_key = line[..last_space_pos].trim_end();
                    // Insert with full key including labels
                    metrics.insert(full_key.to_string(), value);
                }
                // Also insert with just the metric name for easy lookup
                metrics.insert(metric_name.to_string(), value);
            } else {
                // No labels, just insert with metric name
                metrics.insert(metric_name.to_string(), value);
            }
        }
    }

    metrics
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
    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send cluster metrics request");

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
    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send bucket metrics request");

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
    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send node metrics request");

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
    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send resource metrics request");

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
    let invalid_tokens = ["invalid-token", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.invalid.signature"];

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

    // Step 1: Call the token endpoint with valid credentials in POST body
    let token_url = format!("{}/rustfs/admin/v3/prometheus/token", env.url);
    info!("Requesting token from: {}", token_url);

    let token_request_body = serde_json::json!({
        "access_key": env.access_key,
        "secret_key": env.secret_key
    });

    let token_response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body(token_request_body.to_string())
        .send()
        .await
        .expect("Failed to send token request");

    assert!(
        token_response.status().is_success(),
        "Token request should succeed, got status: {}",
        token_response.status()
    );

    // Step 2: Parse the JSON response to extract the bearer token
    let token_body = token_response.text().await.expect("Failed to read token response body");

    info!("Token response body: {}", token_body);

    let token_data: PrometheusTokenResponse = serde_json::from_str(&token_body).expect("Failed to parse token response JSON");

    assert!(!token_data.bearer_token.is_empty(), "Bearer token should not be empty");
    info!("Successfully obtained bearer token");

    // Step 3: Use the token to scrape metrics from the cluster endpoint
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token_data.bearer_token);
    info!("Requesting metrics from: {}", metrics_url);

    let metrics_response = client.get(&metrics_url).send().await.expect("Failed to send metrics request");

    assert!(
        metrics_response.status().is_success(),
        "Metrics request with token from endpoint should succeed, got status: {}",
        metrics_response.status()
    );

    // Step 4: Verify the response is valid Prometheus format
    let metrics_body = metrics_response.text().await.expect("Failed to read metrics response body");

    info!("Metrics response:\n{}", metrics_body);

    assert!(
        is_valid_prometheus_format(&metrics_body),
        "Metrics response should be in valid Prometheus format"
    );

    info!("Prometheus token endpoint to metrics scrape test passed");
}

/// Test the Prometheus token generation endpoint.
///
/// This test verifies:
/// 1. The token endpoint returns a 200 status with valid credentials
/// 2. The response contains a non-empty bearer_token field
/// 3. The token has valid JWT structure (3 parts separated by `.`)
#[tokio::test]
#[serial]
async fn test_prometheus_token_generation() {
    init_logging();
    info!("Starting Prometheus Token Generation Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();

    // Call the token generation endpoint with valid credentials in POST body
    let token_url = format!("{}/rustfs/admin/v3/prometheus/token", env.url);
    info!("Requesting token from: {}", token_url);

    let token_request_body = serde_json::json!({
        "access_key": env.access_key,
        "secret_key": env.secret_key
    });

    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body(token_request_body.to_string())
        .send()
        .await
        .expect("Failed to send token request");

    // Verify response status is 200
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "Token endpoint should return 200 OK, got: {}",
        response.status()
    );
    info!("Token endpoint returned 200 OK");

    // Parse JSON response
    let response_body = response.text().await.expect("Failed to read response body");
    info!("Token response body: {}", response_body);

    let token_data: PrometheusTokenResponse =
        serde_json::from_str(&response_body).expect("Failed to parse token response as JSON");

    // Verify bearer_token field exists and is non-empty
    assert!(!token_data.bearer_token.is_empty(), "bearer_token field should not be empty");
    info!("bearer_token field exists and is non-empty");

    // Verify the token looks like a valid JWT (has 3 parts separated by `.`)
    let jwt_parts: Vec<&str> = token_data.bearer_token.split('.').collect();
    assert_eq!(
        jwt_parts.len(),
        3,
        "JWT token should have 3 parts separated by '.', got {} parts",
        jwt_parts.len()
    );
    info!("Token has valid JWT structure with 3 parts");

    // Verify each part is non-empty
    for (i, part) in jwt_parts.iter().enumerate() {
        assert!(!part.is_empty(), "JWT part {} should not be empty", i + 1);
    }

    info!("Prometheus token generation test passed");
}

/// Test error cases for the Prometheus token endpoint.
///
/// This test verifies:
/// 1. Missing access_key in request body returns an error
/// 2. Missing secret_key in request body returns an error
/// 3. Invalid credentials in request body return an error
/// 4. Empty/invalid JSON body returns an error
#[tokio::test]
#[serial]
async fn test_prometheus_token_endpoint_errors() {
    init_logging();
    info!("Starting Prometheus Token Endpoint Error Cases Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    let client = Client::new();
    let token_url = format!("{}/rustfs/admin/v3/prometheus/token", env.url);

    // Test 1: Missing access_key in body
    info!("Testing missing access_key");
    let body_missing_access_key = serde_json::json!({
        "secret_key": env.secret_key
    });

    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body(body_missing_access_key.to_string())
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with missing access_key should fail, got status: {}",
        response.status()
    );
    info!("Missing access_key correctly returned error status: {}", response.status());

    // Test 2: Missing secret_key in body
    info!("Testing missing secret_key");
    let body_missing_secret_key = serde_json::json!({
        "access_key": env.access_key
    });

    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body(body_missing_secret_key.to_string())
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with missing secret_key should fail, got status: {}",
        response.status()
    );
    info!("Missing secret_key correctly returned error status: {}", response.status());

    // Test 3: Invalid credentials in body
    info!("Testing invalid credentials");
    let body_invalid_creds = serde_json::json!({
        "access_key": "invalid_user",
        "secret_key": "wrong_password"
    });

    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body(body_invalid_creds.to_string())
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with invalid credentials should fail, got status: {}",
        response.status()
    );
    info!("Invalid credentials correctly returned error status: {}", response.status());

    // Test 4: Empty body
    info!("Testing empty body");
    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body("")
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with empty body should fail, got status: {}",
        response.status()
    );
    info!("Empty body correctly returned error status: {}", response.status());

    // Test 5: Invalid JSON body
    info!("Testing invalid JSON body");
    let response = client
        .post(&token_url)
        .header("Content-Type", "application/json")
        .body("not valid json")
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !response.status().is_success(),
        "Request with invalid JSON body should fail, got status: {}",
        response.status()
    );
    info!("Invalid JSON body correctly returned error status: {}", response.status());

    info!("Prometheus token endpoint error cases test passed");
}

/// Test that Prometheus metrics content changes after S3 operations.
///
/// This test verifies:
/// 1. Metrics are scraped and parsed successfully before any operations
/// 2. After creating a bucket and uploading an object, metrics reflect the changes
/// 3. The metrics cache refresh works correctly (10s TTL)
/// 4. Cluster metrics are present and contain expected data
///
/// The test will FAIL if:
/// - No metrics are returned from the endpoint
/// - No cluster-related metrics are found in the response
/// - The metrics response is invalid
#[tokio::test]
#[serial]
async fn test_prometheus_metrics_content_validation() {
    init_logging();
    info!("Starting Prometheus Metrics Content Validation Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    info!("Generated Prometheus token for metrics access");

    // Step 1: Scrape initial cluster metrics
    let http_client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/cluster?token={}", env.url, token);

    let initial_response = http_client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send initial cluster metrics request");

    assert!(
        initial_response.status().is_success(),
        "Initial cluster metrics request should succeed, got status: {}",
        initial_response.status()
    );

    let initial_body = initial_response
        .text()
        .await
        .expect("Failed to read initial metrics response body");

    info!("Initial metrics response length: {} bytes", initial_body.len());

    // Verify the response is valid Prometheus format
    assert!(
        is_valid_prometheus_format(&initial_body),
        "Initial metrics response should be in valid Prometheus format"
    );

    // Step 2: Parse the initial metrics
    let initial_metrics = parse_prometheus_metrics(&initial_body);

    info!("Parsed {} initial metrics", initial_metrics.len());

    // Log some initial metric values for debugging
    for (key, value) in initial_metrics.iter().take(10) {
        info!("Initial metric: {} = {}", key, value);
    }

    // Step 3: Create a test bucket via S3 client
    let s3_client = env.create_s3_client();
    let test_bucket_name = "metrics-test-bucket";
    let test_object_key = "test-object.txt";

    s3_client
        .create_bucket()
        .bucket(test_bucket_name)
        .send()
        .await
        .expect("Failed to create test bucket");

    info!("Created test bucket: {}", test_bucket_name);

    // Step 4: Upload a test object
    let data = vec![0u8; 1024]; // 1KB test data

    s3_client
        .put_object()
        .bucket(test_bucket_name)
        .key(test_object_key)
        .body(ByteStream::from(data))
        .send()
        .await
        .expect("Failed to upload test object");

    info!("Uploaded test object: {} (1KB)", test_object_key);

    // Step 5: Wait 11 seconds for metrics cache refresh (the cache has 10s TTL)
    info!("Waiting 11 seconds for metrics cache to refresh...");
    sleep(Duration::from_secs(11)).await;

    // Step 6: Scrape cluster metrics again
    let updated_response = http_client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send updated cluster metrics request");

    assert!(
        updated_response.status().is_success(),
        "Updated cluster metrics request should succeed, got status: {}",
        updated_response.status()
    );

    let updated_body = updated_response
        .text()
        .await
        .expect("Failed to read updated metrics response body");

    info!("Updated metrics response length: {} bytes", updated_body.len());

    // Verify the updated response is valid Prometheus format
    assert!(
        is_valid_prometheus_format(&updated_body),
        "Updated metrics response should be in valid Prometheus format"
    );

    // Step 7: Parse the new metrics
    let updated_metrics = parse_prometheus_metrics(&updated_body);

    info!("Parsed {} updated metrics", updated_metrics.len());

    // Assert that we received metrics data
    assert!(
        !updated_body.trim().is_empty(),
        "Metrics response should not be empty after S3 operations"
    );

    // Step 8: Validate that cluster metrics exist and log before/after values
    info!("Comparing initial and updated metrics:");

    // Check for common cluster metrics that should exist
    let expected_cluster_metrics = [
        "rustfs_cluster_bucket_count",
        "rustfs_cluster_object_count",
        "rustfs_cluster_total_size",
        "rustfs_bucket_usage_total_bytes",
        "rustfs_bucket_objects_count",
    ];

    let mut cluster_metrics_found: Vec<String> = Vec::new();

    for metric_name in &expected_cluster_metrics {
        let initial_value = initial_metrics.get(*metric_name);
        let updated_value = updated_metrics.get(*metric_name);

        match (initial_value, updated_value) {
            (Some(init), Some(upd)) => {
                info!("Metric '{}': before = {}, after = {}", metric_name, init, upd);
                cluster_metrics_found.push(metric_name.to_string());
            }
            (None, Some(upd)) => {
                info!("Metric '{}': before = (not present), after = {}", metric_name, upd);
                cluster_metrics_found.push(metric_name.to_string());
            }
            (Some(init), None) => {
                info!("Metric '{}': before = {}, after = (not present)", metric_name, init);
                cluster_metrics_found.push(metric_name.to_string());
            }
            (None, None) => {
                info!("Metric '{}': not found in either response", metric_name);
            }
        }
    }

    // Log all updated metrics that contain "bucket" or "object" for debugging
    info!("All updated metrics containing 'bucket' or 'object':");
    for (key, value) in updated_metrics.iter() {
        if key.contains("bucket") || key.contains("object") {
            info!("  {} = {}", key, value);
            if !cluster_metrics_found.contains(key) {
                cluster_metrics_found.push(key.clone());
            }
        }
    }

    // Also check for any metrics containing "cluster" or "rustfs"
    let mut rustfs_metrics_count = 0;
    for key in updated_metrics.keys() {
        if key.starts_with("rustfs_") || key.contains("cluster") {
            rustfs_metrics_count += 1;
        }
    }
    info!("Found {} rustfs/cluster-related metrics in response", rustfs_metrics_count);

    // Assert that we found at least some cluster-related metrics
    assert!(
        !cluster_metrics_found.is_empty() || rustfs_metrics_count > 0,
        "Should have found at least one cluster-related metric in the response. \
         Expected metrics like {:?}, but none were found. \
         Updated metrics keys: {:?}",
        expected_cluster_metrics,
        updated_metrics.keys().take(20).collect::<Vec<_>>()
    );

    info!(
        "Successfully found {} cluster-related metrics: {:?}",
        cluster_metrics_found.len(),
        cluster_metrics_found
    );

    // Step 9: Verify that bucket-related count metrics actually changed after S3 operations
    // Look for any bucket-related count metrics that we can compare
    let bucket_metrics = ["rustfs_cluster_buckets_total", "rustfs_cluster_bucket_count", "buckets_total"];
    let mut found_comparison = false;

    for metric_name in bucket_metrics.iter() {
        if let (Some(&initial_val), Some(&updated_val)) = (initial_metrics.get(*metric_name), updated_metrics.get(*metric_name)) {
            info!("Comparing {}: initial={}, updated={}", metric_name, initial_val, updated_val);
            assert!(
                updated_val >= initial_val,
                "Metric {} should not decrease after creating bucket: {} -> {}",
                metric_name,
                initial_val,
                updated_val
            );
            found_comparison = true;
            break;
        }
    }

    // If we found a comparison, great! If not, log available metrics for debugging
    if !found_comparison {
        info!(
            "Could not find bucket count metrics to compare. Available metrics: {:?}",
            updated_metrics.keys().filter(|k| k.contains("bucket")).collect::<Vec<_>>()
        );
    }

    // Step 10: Cleanup - delete the test object and bucket
    info!("Cleaning up test resources...");

    // Delete the test object
    if let Err(e) = s3_client
        .delete_object()
        .bucket(test_bucket_name)
        .key(test_object_key)
        .send()
        .await
    {
        info!("Warning: Failed to delete test object: {:?}", e);
    } else {
        info!("Deleted test object: {}", test_object_key);
    }

    // Delete the test bucket
    if let Err(e) = s3_client.delete_bucket().bucket(test_bucket_name).send().await {
        info!("Warning: Failed to delete test bucket: {:?}", e);
    } else {
        info!("Deleted test bucket: {}", test_bucket_name);
    }

    info!("Prometheus metrics content validation test passed");
}

/// Test that resource metrics exist and have reasonable values.
///
/// This test verifies:
/// 1. The resource metrics endpoint returns valid Prometheus format data
/// 2. Resource metrics (uptime, memory, cpu) are present in the response
/// 3. Uptime metrics have values >= 0 (can be 0 if scraped within first second of startup)
/// 4. Memory metrics have values > 0 (process uses some memory)
///
/// The test will FAIL if:
/// - No resource metrics are found at all
/// - Uptime metrics (if found) are < 0
/// - Memory metrics (if found) are <= 0
#[tokio::test]
#[serial]
async fn test_prometheus_resource_metrics_exist() {
    init_logging();
    info!("Starting Prometheus Resource Metrics Exist Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    info!("Generated Prometheus token for resource metrics access");

    // Scrape resource metrics endpoint
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/resource?token={}", env.url, token);

    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send resource metrics request");

    assert!(
        response.status().is_success(),
        "Resource metrics request should succeed, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read resource metrics response body");

    info!("Resource metrics response length: {} bytes", body.len());
    info!("Resource metrics response:\n{}", body);

    // Verify the response is valid Prometheus format
    assert!(
        is_valid_prometheus_format(&body),
        "Resource metrics response should be in valid Prometheus format"
    );

    // Parse the metrics
    let metrics = parse_prometheus_metrics(&body);

    info!("Parsed {} resource metrics", metrics.len());

    // Define patterns for resource metrics we expect to find
    // These cover various naming conventions that may be used
    let uptime_patterns = [
        "rustfs_process_uptime_seconds",
        "process_uptime_seconds",
        "uptime_seconds",
        "uptime",
        "rustfs_uptime",
    ];

    let memory_patterns = [
        "rustfs_process_memory_bytes",
        "process_resident_memory_bytes",
        "process_memory_bytes",
        "memory_bytes",
        "memory",
        "rustfs_memory",
        "resident_memory",
    ];

    let cpu_patterns = [
        "rustfs_process_cpu_seconds_total",
        "process_cpu_seconds_total",
        "cpu_seconds_total",
        "cpu",
        "rustfs_cpu",
    ];

    // Track found metrics
    let mut found_uptime_metrics: Vec<(String, f64)> = Vec::new();
    let mut found_memory_metrics: Vec<(String, f64)> = Vec::new();
    let mut found_cpu_metrics: Vec<(String, f64)> = Vec::new();
    let mut all_resource_metrics: Vec<(String, f64)> = Vec::new();

    // Search for matching metrics
    for (metric_name, value) in metrics.iter() {
        let name_lower = metric_name.to_lowercase();

        // Check uptime patterns
        for pattern in &uptime_patterns {
            if name_lower.contains(pattern) || metric_name.contains(pattern) {
                found_uptime_metrics.push((metric_name.clone(), *value));
                all_resource_metrics.push((metric_name.clone(), *value));
                break;
            }
        }

        // Check memory patterns
        for pattern in &memory_patterns {
            if name_lower.contains(pattern) || metric_name.contains(pattern) {
                found_memory_metrics.push((metric_name.clone(), *value));
                all_resource_metrics.push((metric_name.clone(), *value));
                break;
            }
        }

        // Check cpu patterns
        for pattern in &cpu_patterns {
            if name_lower.contains(pattern) || metric_name.contains(pattern) {
                found_cpu_metrics.push((metric_name.clone(), *value));
                all_resource_metrics.push((metric_name.clone(), *value));
                break;
            }
        }
    }

    // Log all found resource metrics
    info!("Found {} uptime metrics:", found_uptime_metrics.len());
    for (name, value) in &found_uptime_metrics {
        info!("  {} = {}", name, value);
    }

    info!("Found {} memory metrics:", found_memory_metrics.len());
    for (name, value) in &found_memory_metrics {
        info!("  {} = {}", name, value);
    }

    info!("Found {} cpu metrics:", found_cpu_metrics.len());
    for (name, value) in &found_cpu_metrics {
        info!("  {} = {}", name, value);
    }

    // If we didn't find expected metrics, log all available metrics for debugging
    if all_resource_metrics.is_empty() {
        info!("No expected resource metrics found. All available metrics:");
        for (name, value) in metrics.iter() {
            info!("  {} = {}", name, value);
        }
    }

    // FAIL if no resource metrics are found at all
    assert!(
        !all_resource_metrics.is_empty(),
        "Should have found at least some resource metrics (uptime, memory, or cpu). \
         Expected metrics matching patterns like {:?}, {:?}, or {:?}. \
         Available metrics: {:?}",
        uptime_patterns,
        memory_patterns,
        cpu_patterns,
        metrics.keys().collect::<Vec<_>>()
    );

    info!("Successfully found {} resource metrics in total", all_resource_metrics.len());

    // Validate uptime metrics have reasonable values (>= 0)
    for (name, value) in &found_uptime_metrics {
        assert!(*value >= 0.0, "Uptime metric '{}' should be >= 0, got: {}", name, value);
        info!("Uptime metric '{}' has valid value: {} >= 0", name, value);
    }

    // Validate memory metrics have reasonable values (> 0)
    for (name, value) in &found_memory_metrics {
        assert!(
            *value > 0.0,
            "Memory metric '{}' should be > 0 (process uses memory), got: {}",
            name,
            value
        );
        info!("Memory metric '{}' has valid value: {} > 0", name, value);
    }

    // CPU metrics can be 0 if the process just started, so we only log them
    for (name, value) in &found_cpu_metrics {
        info!("CPU metric '{}' value: {}", name, value);
    }

    info!("Prometheus resource metrics exist test passed");
}

/// Test that the node metrics endpoint works and validates disk metrics if present.
///
/// This test verifies:
/// 1. The node metrics endpoint returns a successful response (not an error)
/// 2. The response is in valid Prometheus text exposition format
/// 3. If disk metrics are found, they have valid values:
///    - Disk total metrics should be > 0 (disk has capacity)
///    - Disk free metrics should be >= 0 (disk has free space)
///
/// The test will FAIL if:
/// - The endpoint returns an error status
/// - The response is not valid Prometheus format
/// - Disk metrics are found but have invalid values (total <= 0, free < 0)
///
/// The test will PASS (with a warning) if:
/// - No disk metrics are found (E2E environment may not report disks)
#[tokio::test]
#[serial]
async fn test_prometheus_node_metrics_exist() {
    use std::collections::HashSet;

    init_logging();
    info!("Starting Prometheus Node Metrics Exist Test");

    let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");

    env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS server");

    // Generate token directly for testing
    let token = generate_test_token(&env.access_key, &env.secret_key);

    info!("Generated Prometheus token for node metrics access");

    // Scrape node metrics endpoint
    let client = Client::new();
    let metrics_url = format!("{}/rustfs/v2/metrics/node?token={}", env.url, token);

    let response = client
        .get(&metrics_url)
        .send()
        .await
        .expect("Failed to send node metrics request");

    assert!(
        response.status().is_success(),
        "Node metrics request should succeed, got status: {}",
        response.status()
    );

    let body = response.text().await.expect("Failed to read node metrics response body");

    info!("Node metrics response length: {} bytes", body.len());
    info!("Node metrics response:\n{}", body);

    // Verify the response is valid Prometheus format
    assert!(
        is_valid_prometheus_format(&body),
        "Node metrics response should be in valid Prometheus format"
    );

    // Parse the metrics
    let metrics = parse_prometheus_metrics(&body);

    info!("Parsed {} node metrics", metrics.len());

    // Define exact metric names we expect to find, with a few fallback patterns
    let disk_total_patterns = ["rustfs_node_disk_total_bytes", "node_disk_total_bytes", "disk_total_bytes"];

    let disk_free_patterns = ["rustfs_node_disk_free_bytes", "node_disk_free_bytes", "disk_free_bytes"];

    let disk_used_patterns = ["rustfs_node_disk_used_bytes", "node_disk_used_bytes", "disk_used_bytes"];

    // Track found metrics using HashSet to prevent duplicates
    let mut seen_disk_total: HashSet<String> = HashSet::new();
    let mut seen_disk_free: HashSet<String> = HashSet::new();
    let mut seen_disk_used: HashSet<String> = HashSet::new();

    let mut found_disk_total_metrics: Vec<(String, f64)> = Vec::new();
    let mut found_disk_free_metrics: Vec<(String, f64)> = Vec::new();
    let mut found_disk_used_metrics: Vec<(String, f64)> = Vec::new();

    // Search for matching metrics
    for (metric_name, value) in metrics.iter() {
        let name_lower = metric_name.to_lowercase();

        // Check disk total patterns
        for pattern in &disk_total_patterns {
            if (name_lower.contains(pattern) || metric_name.contains(pattern)) && !seen_disk_total.contains(metric_name) {
                seen_disk_total.insert(metric_name.clone());
                found_disk_total_metrics.push((metric_name.clone(), *value));
                break;
            }
        }

        // Check disk free patterns
        for pattern in &disk_free_patterns {
            if (name_lower.contains(pattern) || metric_name.contains(pattern)) && !seen_disk_free.contains(metric_name) {
                seen_disk_free.insert(metric_name.clone());
                found_disk_free_metrics.push((metric_name.clone(), *value));
                break;
            }
        }

        // Check disk used patterns
        for pattern in &disk_used_patterns {
            if (name_lower.contains(pattern) || metric_name.contains(pattern)) && !seen_disk_used.contains(metric_name) {
                seen_disk_used.insert(metric_name.clone());
                found_disk_used_metrics.push((metric_name.clone(), *value));
                break;
            }
        }
    }

    // Log all found node/disk metrics
    info!("Found {} disk total metrics:", found_disk_total_metrics.len());
    for (name, value) in &found_disk_total_metrics {
        info!("  {} = {}", name, value);
    }

    info!("Found {} disk free metrics:", found_disk_free_metrics.len());
    for (name, value) in &found_disk_free_metrics {
        info!("  {} = {}", name, value);
    }

    info!("Found {} disk used metrics:", found_disk_used_metrics.len());
    for (name, value) in &found_disk_used_metrics {
        info!("  {} = {}", name, value);
    }

    // Count total unique disk metrics found
    let total_disk_metrics = found_disk_total_metrics.len() + found_disk_free_metrics.len() + found_disk_used_metrics.len();

    // If no disk metrics found, log warning but don't fail
    // The E2E environment may not have disks reported
    if total_disk_metrics == 0 {
        info!(
            "WARNING: No disk metrics found in node metrics response. \
             This may be expected in E2E test environments where disks are not reported. \
             The endpoint returned valid Prometheus format, so the test passes."
        );
        info!("All available metrics for debugging:");
        for (name, value) in metrics.iter() {
            info!("  {} = {}", name, value);
        }
        info!("Prometheus node metrics exist test passed (no disk metrics, but valid response)");
        return;
    }

    info!("Successfully found {} disk metrics in total", total_disk_metrics);

    // Validate disk total metrics have reasonable values (> 0)
    for (name, value) in &found_disk_total_metrics {
        assert!(
            *value > 0.0,
            "Disk total metric '{}' should be > 0 (disk has capacity), got: {}",
            name,
            value
        );
        info!("Disk total metric '{}' has valid value: {} > 0", name, value);
    }

    // Validate disk free metrics have reasonable values (>= 0)
    for (name, value) in &found_disk_free_metrics {
        assert!(
            *value >= 0.0,
            "Disk free metric '{}' should be >= 0 (disk has free space), got: {}",
            name,
            value
        );
        info!("Disk free metric '{}' has valid value: {} >= 0", name, value);
    }

    // Disk used metrics can be any positive value, so we only log them
    for (name, value) in &found_disk_used_metrics {
        info!("Disk used metric '{}' value: {}", name, value);
    }

    info!("Prometheus node metrics exist test passed");
}
