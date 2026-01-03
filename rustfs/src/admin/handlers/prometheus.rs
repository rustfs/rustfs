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

//! HTTP handlers for Prometheus metrics endpoints.
//!
//! This module provides handlers for serving RustFS metrics in Prometheus
//! text exposition format. These endpoints are designed to be scraped by
//! Prometheus servers for monitoring and alerting.
//!
//! # Performance
//!
//! Each endpoint uses a 10-second cache to avoid redundant data collection.
//! This is shorter than typical Prometheus scrape intervals (15-60s) to
//! ensure reasonably fresh data while minimizing CPU overhead.
//!
//! # Endpoints
//!
//! - `/rustfs/v2/metrics/cluster` - Cluster-wide capacity and usage metrics
//! - `/rustfs/v2/metrics/bucket` - Per-bucket usage and quota metrics
//! - `/rustfs/v2/metrics/node` - Per-node disk capacity and health metrics
//! - `/rustfs/v2/metrics/resource` - System resource metrics (CPU, memory, uptime)
//! - `/rustfs/admin/v3/prometheus/config` - Generate Prometheus scrape configuration
//!
//! # Authentication
//!
//! All metrics endpoints require Bearer token authentication. Tokens can be
//! generated using the `/rustfs/admin/v3/prometheus/config` endpoint.

use super::super::router::Operation;
use http::{HeaderMap, HeaderValue};
use hyper::StatusCode;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::{get_global_action_cred, get_global_secret_key};
use rustfs_ecstore::bucket::metadata_sys::get_quota_config;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::{BucketOptions, StorageAPI};
use rustfs_obs::prometheus::{
    auth::{AuthError, generate_prometheus_config, generate_prometheus_token, verify_bearer_token_with_query},
    cache::MetricsCache,
    collectors::{
        BucketStats, ClusterStats, DiskStats, ResourceStats, collect_bucket_metrics, collect_cluster_metrics,
        collect_node_metrics, collect_resource_metrics,
    },
    render_metrics,
};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::sync::{LazyLock, OnceLock};
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tracing::{error, warn};

/// Content-Type header value for Prometheus text exposition format.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Global caches for each metrics endpoint.
///
/// TTL is configurable via `RUSTFS_PROMETHEUS_CACHE_TTL` environment variable (in seconds).
/// Default is 10 seconds. Set to 0 to disable caching.
static CLUSTER_CACHE: LazyLock<MetricsCache> = LazyLock::new(MetricsCache::with_configured_ttl);
static BUCKET_CACHE: LazyLock<MetricsCache> = LazyLock::new(MetricsCache::with_configured_ttl);
static NODE_CACHE: LazyLock<MetricsCache> = LazyLock::new(MetricsCache::with_configured_ttl);
static RESOURCE_CACHE: LazyLock<MetricsCache> = LazyLock::new(MetricsCache::with_configured_ttl);

/// Process start time for calculating uptime.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Get the process start time, initializing it on first call.
#[inline]
fn get_process_start() -> &'static Instant {
    PROCESS_START.get_or_init(Instant::now)
}

/// Verify Bearer token from request headers or query parameter.
///
/// This function checks for the JWT token in two places:
/// 1. The `Authorization: Bearer <token>` header (standard approach)
/// 2. The `?token=<token>` query parameter (fallback for when s3s intercepts the Authorization header)
///
/// Returns an S3Error if authentication fails.
#[inline]
fn verify_prometheus_auth(headers: &HeaderMap, uri: &http::Uri) -> S3Result<()> {
    let secret_key = get_global_secret_key();
    if secret_key.is_empty() {
        return Err(s3_error!(InternalError, "server credentials not initialized"));
    }

    match verify_bearer_token_with_query(headers, uri, &secret_key) {
        Ok(_) => Ok(()),
        Err(AuthError::MissingAuthHeader) => {
            Err(s3_error!(AccessDenied, "missing authorization header or token query parameter"))
        }
        Err(AuthError::InvalidAuthFormat) => {
            Err(s3_error!(AccessDenied, "invalid authorization format, expected 'Bearer <token>'"))
        }
        Err(AuthError::InvalidToken(msg)) => Err(s3_error!(AccessDenied, "invalid or expired token: {}", msg)),
        Err(AuthError::TokenGenerationFailed(msg)) => Err(s3_error!(InternalError, "token verification failed: {}", msg)),
    }
}

/// Create response with Prometheus content type.
#[inline]
fn prometheus_response(body: String) -> S3Response<(StatusCode, Body)> {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        PROMETHEUS_CONTENT_TYPE
            .parse::<HeaderValue>()
            .expect("PROMETHEUS_CONTENT_TYPE is a valid header value"),
    );
    S3Response::with_headers((StatusCode::OK, Body::from(body)), headers)
}

/// Collect cluster statistics from the storage layer.
async fn collect_cluster_stats() -> ClusterStats {
    let Some(store) = new_object_layer_fn() else {
        return ClusterStats::default();
    };

    let storage_info = store.storage_info().await;

    let raw_capacity: u64 = storage_info.disks.iter().map(|d| d.total_space).sum();
    let used: u64 = storage_info.disks.iter().map(|d| d.used_space).sum();
    let usable_capacity = get_total_usable_capacity(&storage_info.disks, &storage_info) as u64;
    let free = get_total_usable_capacity_free(&storage_info.disks, &storage_info) as u64;

    // Get bucket and object counts from data usage info
    let (buckets_count, objects_count) = match load_data_usage_from_backend(store.clone()).await {
        Ok(data_usage) => (data_usage.buckets_count, data_usage.objects_total_count),
        Err(e) => {
            warn!("Failed to load data usage from backend: {}", e);
            // Fall back to bucket list for buckets_count, objects_count stays 0
            let buckets = match store
                .list_bucket(&BucketOptions {
                    cached: true,
                    ..Default::default()
                })
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!("Failed to list buckets for cluster metrics: {}", e);
                    Vec::new()
                }
            };
            (buckets.len() as u64, 0)
        }
    };

    ClusterStats {
        raw_capacity_bytes: raw_capacity,
        usable_capacity_bytes: usable_capacity,
        used_bytes: used,
        free_bytes: free,
        objects_count,
        buckets_count,
    }
}

/// Collect bucket statistics from the storage layer.
async fn collect_bucket_stats() -> Vec<BucketStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    // Load data usage info from backend to get bucket sizes and object counts
    let data_usage = match load_data_usage_from_backend(store.clone()).await {
        Ok(info) => Some(info),
        Err(e) => {
            warn!("Failed to load data usage from backend for bucket metrics: {}", e);
            None
        }
    };

    let buckets = match store
        .list_bucket(&BucketOptions {
            cached: true,
            ..Default::default()
        })
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to list buckets for metrics: {}", e);
            return Vec::new();
        }
    };

    // Build bucket stats with real data from DataUsageInfo
    let mut stats = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        if bucket.name.starts_with('.') {
            continue;
        }

        // Get size and objects_count from data usage info
        let (size_bytes, objects_count) = data_usage
            .as_ref()
            .and_then(|du| du.buckets_usage.get(&bucket.name))
            .map(|bui| (bui.size, bui.objects_count))
            .unwrap_or((0, 0));

        // Get quota from bucket metadata
        let quota_bytes = match get_quota_config(&bucket.name).await {
            Ok((quota, _)) => quota.quota_bytes(),
            Err(_) => 0, // No quota configured or error
        };

        stats.push(BucketStats {
            name: bucket.name,
            size_bytes,
            objects_count,
            quota_bytes,
        });
    }

    stats
}

/// Collect disk statistics from the storage layer.
async fn collect_disk_stats() -> Vec<DiskStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    let storage_info = store.storage_info().await;

    storage_info
        .disks
        .iter()
        .map(|disk| DiskStats {
            server: disk.endpoint.clone(),
            drive: disk.drive_path.clone(),
            total_bytes: disk.total_space,
            used_bytes: disk.used_space,
            free_bytes: disk.available_space,
        })
        .collect()
}

/// Collect resource statistics for the current process.
///
/// Collects:
/// - Uptime: Calculated from process start time
/// - Memory: Process resident set size from sysinfo
/// - CPU: Process CPU usage percentage from sysinfo
#[inline]
fn collect_process_stats() -> ResourceStats {
    let uptime_seconds = get_process_start().elapsed().as_secs();

    // Use sysinfo for process metrics
    let mut sys = System::new();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    if let Some(process) = sys.process(pid) {
        ResourceStats {
            cpu_percent: process.cpu_usage() as f64,
            memory_bytes: process.memory(),
            uptime_seconds,
        }
    } else {
        // Fallback if process not found
        ResourceStats {
            cpu_percent: 0.0,
            memory_bytes: 0,
            uptime_seconds,
        }
    }
}

/// Handler for `/rustfs/v2/metrics/cluster` endpoint.
///
/// Returns cluster-wide metrics including:
/// - Total raw storage capacity
/// - Usable capacity (after erasure coding overhead)
/// - Used and free storage
/// - Object and bucket counts
pub struct ClusterMetricsHandler;

#[async_trait::async_trait]
impl Operation for ClusterMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        verify_prometheus_auth(&req.headers, &req.uri)?;

        let body = CLUSTER_CACHE
            .get_or_compute_async(|| async {
                let stats = collect_cluster_stats().await;
                let metrics = collect_cluster_metrics(&stats);
                render_metrics(&metrics)
            })
            .await;

        Ok(prometheus_response(body))
    }
}

/// Handler for `/rustfs/v2/metrics/bucket` endpoint.
///
/// Returns per-bucket metrics including:
/// - Bucket size in bytes
/// - Object count per bucket
/// - Quota information (if configured)
pub struct BucketMetricsHandler;

#[async_trait::async_trait]
impl Operation for BucketMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        verify_prometheus_auth(&req.headers, &req.uri)?;

        let body = BUCKET_CACHE
            .get_or_compute_async(|| async {
                let stats = collect_bucket_stats().await;
                let metrics = collect_bucket_metrics(&stats);
                render_metrics(&metrics)
            })
            .await;

        Ok(prometheus_response(body))
    }
}

/// Handler for `/rustfs/v2/metrics/node` endpoint.
///
/// Returns per-node disk metrics including:
/// - Total disk capacity
/// - Used disk space
/// - Free disk space
pub struct NodeMetricsHandler;

#[async_trait::async_trait]
impl Operation for NodeMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        verify_prometheus_auth(&req.headers, &req.uri)?;

        let body = NODE_CACHE
            .get_or_compute_async(|| async {
                let stats = collect_disk_stats().await;
                let metrics = collect_node_metrics(&stats);
                render_metrics(&metrics)
            })
            .await;

        Ok(prometheus_response(body))
    }
}

/// Handler for `/rustfs/v2/metrics/resource` endpoint.
///
/// Returns system resource metrics including:
/// - CPU usage percentage
/// - Memory usage in bytes
/// - Process uptime in seconds
pub struct ResourceMetricsHandler;

#[async_trait::async_trait]
impl Operation for ResourceMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        verify_prometheus_auth(&req.headers, &req.uri)?;

        let body = RESOURCE_CACHE.get_or_compute(|| {
            let stats = collect_process_stats();
            let metrics = collect_resource_metrics(&stats);
            render_metrics(&metrics)
        });

        Ok(prometheus_response(body))
    }
}

/// Response structure for Prometheus configuration endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusConfigResponse {
    /// Bearer token for authenticating Prometheus scraper
    pub bearer_token: String,
    /// YAML configuration for Prometheus scrape_configs
    pub scrape_config: String,
}

/// Handler for `/rustfs/admin/v3/prometheus/config` endpoint.
///
/// Generates a Prometheus scrape configuration YAML with a JWT bearer token.
/// This endpoint requires admin credentials (S3 signature authentication).
///
/// Query parameters:
/// - `scheme` - URL scheme ("http" or "https"), defaults to "http"
/// - `endpoint` - Override the server endpoint, defaults to request Host header
pub struct PrometheusConfigHandler;

#[async_trait::async_trait]
impl Operation for PrometheusConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // This endpoint requires admin credentials, not Bearer token
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        // Verify this is an admin user
        let Some(admin_cred) = get_global_action_cred() else {
            return Err(s3_error!(InternalError, "server credentials not initialized"));
        };

        // Check if the requester is the admin
        if !crate::auth::constant_time_eq(&input_cred.access_key, &admin_cred.access_key) {
            return Err(s3_error!(AccessDenied, "admin credentials required"));
        }

        // Extract query parameters
        let scheme = extract_query_param(&req.uri, "scheme").unwrap_or_else(|| "http".to_string());
        let endpoint = extract_query_param(&req.uri, "endpoint").unwrap_or_else(|| {
            req.headers
                .get(http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .unwrap_or("localhost:9000")
                .to_string()
        });

        // Generate token using admin's secret key
        let token = match generate_prometheus_token(&admin_cred.access_key, &admin_cred.secret_key, None) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to generate Prometheus token: {}", e);
                return Err(s3_error!(InternalError, "failed to generate token"));
            }
        };

        let scrape_config = generate_prometheus_config(&token, &endpoint, &scheme);

        let response = PrometheusConfigResponse {
            bearer_token: token,
            scrape_config,
        };

        let data = serde_json::to_vec(&response).map_err(|e| {
            error!("Failed to serialize Prometheus config response: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize response")
        })?;

        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/json"
                .parse::<HeaderValue>()
                .expect("application/json is a valid header value"),
        );

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Extract a query parameter from the URI.
#[inline]
fn extract_query_param(uri: &http::Uri, key: &str) -> Option<String> {
    uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            if k == key {
                urlencoding::decode(v).ok().map(|s| s.into_owned())
            } else {
                None
            }
        })
    })
}

/// Request structure for Prometheus token generation.
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusTokenRequest {
    /// The access key (username) for authentication
    pub access_key: String,
    /// The secret key (password) for authentication
    pub secret_key: String,
}

/// Response structure for Prometheus token endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusTokenResponse {
    /// Bearer token for authenticating Prometheus scraper
    pub bearer_token: String,
}

/// Handler for Prometheus token generation via POST body.
///
/// This endpoint accepts credentials in the JSON request body and returns a JWT bearer token
/// that can be used to authenticate Prometheus scrape requests.
///
/// # Authentication
///
/// The request must include a JSON body with `access_key` and `secret_key` fields
/// containing the admin credentials.
///
/// # Example
///
/// ```bash
/// curl -X POST "https://localhost:9000/rustfs/admin/v3/prometheus/token" \
///   -H "Content-Type: application/json" \
///   -d '{"access_key": "admin", "secret_key": "password"}'
/// ```
///
/// # Response
///
/// On success, returns a JSON response with the bearer token:
/// ```json
/// {"bearer_token": "<jwt_token>"}
/// ```
///
/// # Security
///
/// - Always use HTTPS in production to encrypt credentials in transit
/// - The returned token should be treated as a sensitive credential
/// - Consider using the `/rustfs/admin/v3/prometheus/config` endpoint instead,
///   which uses S3 signature authentication
pub struct PrometheusTokenHandler;

#[async_trait::async_trait]
impl Operation for PrometheusTokenHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Read and parse the request body
        let mut input = req.input;
        let body_bytes = input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|e| {
            error!("Failed to read request body: {}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        let token_request: PrometheusTokenRequest = serde_json::from_slice(&body_bytes).map_err(|e| {
            error!("Failed to parse token request: {}", e);
            s3_error!(
                InvalidRequest,
                "invalid JSON body, expected {{\"access_key\": \"...\", \"secret_key\": \"...\"}}"
            )
        })?;

        let access_key = token_request.access_key;
        let secret_key = token_request.secret_key;

        // Get admin credentials
        let admin_cred =
            get_global_action_cred().ok_or_else(|| s3_error!(InternalError, "server credentials not initialized"))?;

        // Validate credentials using constant-time comparison
        if !crate::auth::constant_time_eq(&access_key, &admin_cred.access_key)
            || !crate::auth::constant_time_eq(&secret_key, &admin_cred.secret_key)
        {
            return Err(s3_error!(AccessDenied, "invalid credentials"));
        }

        // Generate JWT token
        let token = generate_prometheus_token(&admin_cred.access_key, &admin_cred.secret_key, None).map_err(|e| {
            error!("Failed to generate Prometheus token: {}", e);
            s3_error!(InternalError, "failed to generate token")
        })?;

        let response = PrometheusTokenResponse { bearer_token: token };

        let data = serde_json::to_vec(&response).map_err(|e| {
            error!("Failed to serialize Prometheus token response: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize response")
        })?;

        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/json"
                .parse::<HeaderValue>()
                .expect("application/json is a valid header value"),
        );

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_query_param() {
        let uri: http::Uri = "http://localhost/test?scheme=https&endpoint=server:9000".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "scheme"), Some("https".to_string()));
        assert_eq!(extract_query_param(&uri, "endpoint"), Some("server:9000".to_string()));
        assert_eq!(extract_query_param(&uri, "missing"), None);
    }

    #[test]
    fn test_extract_query_param_url_encoded() {
        let uri: http::Uri = "http://localhost/test?endpoint=server%3A9000".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "endpoint"), Some("server:9000".to_string()));
    }

    #[test]
    fn test_extract_query_param_no_query() {
        let uri: http::Uri = "http://localhost/test".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "scheme"), None);
    }

    #[test]
    fn test_prometheus_content_type() {
        assert_eq!(PROMETHEUS_CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8");
    }

    #[test]
    fn test_prometheus_response_creates_correct_headers() {
        let response = prometheus_response("test_metric 1".to_string());
        assert!(response.headers.get(CONTENT_TYPE).is_some());
    }

    #[test]
    fn test_collect_process_stats_returns_real_values() {
        let stats = collect_process_stats();
        // CPU usage should be a valid percentage (0-100+ depending on core count)
        // On first call it may be 0.0 since sysinfo needs time to collect CPU data
        assert!(stats.cpu_percent >= 0.0, "CPU percent should not be negative");
        // Memory should be non-zero (using sysinfo)
        #[cfg(target_os = "linux")]
        assert!(stats.memory_bytes > 0, "Expected non-zero memory on Linux");
        // Uptime should be at least 0 (could be 0 if called immediately after process start)
        // Just verify it doesn't panic and returns a reasonable value
        assert!(stats.uptime_seconds < 86400 * 365 * 100); // Less than 100 years
    }

    #[test]
    fn test_prometheus_config_response_serialization() {
        let response = PrometheusConfigResponse {
            bearer_token: "test-token".to_string(),
            scrape_config: "scrape_configs:\n- job_name: test".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("bearer_token"));
        assert!(json.contains("scrape_config"));

        let deserialized: PrometheusConfigResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bearer_token, "test-token");
    }

    #[test]
    fn test_prometheus_token_response_serialization() {
        let response = PrometheusTokenResponse {
            bearer_token: "jwt-test-token".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("bearer_token"));
        assert!(json.contains("jwt-test-token"));

        let deserialized: PrometheusTokenResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bearer_token, "jwt-test-token");
    }
}
