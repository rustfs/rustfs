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

//! On-Demand Migration admin API contract (ODM-07, rustfs/backlog#2154).
//!
//! Wire types for `PUT`/`GET`/`DELETE /v3/on-demand-migration/{bucket}` and
//! `GET .../status`, mirroring the server's config model
//! (`crates/ecstore/src/bucket/on_demand_migration/config.rs`) and handler
//! responses (`rustfs/src/admin/handlers/on_demand_migration.rs`). The SDK
//! owns its own copies, madmin-go style; the fixtures under
//! `fixtures/on_demand_migration/` are the contract both sides pin
//! byte-for-byte, so field order, defaults and `null` handling here must
//! match the server exactly.

use crate::client::{AdminClient, AdminClientError, percent_encode_path_segment};
use http::Method;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Config schema version this client speaks.
pub const ON_DEMAND_MIGRATION_CONFIG_VERSION: u32 = 1;

/// Query flag that validates and probes a config without saving it.
const DRY_RUN_QUERY: &str = "dry-run";

/// Bucket-level on-demand migration configuration (request body of the
/// `PUT`, redacted copy in every response).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub source: OnDemandMigrationSource,
    #[serde(default)]
    pub filter: OnDemandMigrationFilter,
    #[serde(default)]
    pub policy: OnDemandMigrationPolicy,
}

impl OnDemandMigrationConfig {
    /// A config with the documented defaults for everything but the source.
    pub fn new(source: OnDemandMigrationSource) -> Self {
        Self {
            version: ON_DEMAND_MIGRATION_CONFIG_VERSION,
            enabled: true,
            source,
            filter: OnDemandMigrationFilter::default(),
            policy: OnDemandMigrationPolicy::default(),
        }
    }
}

/// The external S3-compatible source bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationSource {
    pub provider: OnDemandMigrationProvider,
    /// `http(s)://host[:port]`; optional only for `aws`, where it derives from `region`.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub path_style: OnDemandMigrationPathStyle,
    /// `None` means anonymous access to a public source bucket.
    #[serde(default)]
    pub credentials: Option<OnDemandMigrationCredentials>,
    #[serde(default)]
    pub tls: OnDemandMigrationTls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDemandMigrationProvider {
    S3,
    Aws,
    Minio,
    Rustfs,
    R2,
    Gcs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDemandMigrationPathStyle {
    #[default]
    Auto,
    Path,
    Virtual,
}

/// Static source credentials. `Debug` never prints the secret or the
/// session token; responses carry them as `REDACTED`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationCredentials {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

impl fmt::Debug for OnDemandMigrationCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnDemandMigrationCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"REDACTED")
            .field("session_token", &self.session_token.as_ref().map(|_| "REDACTED"))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OnDemandMigrationTls {
    #[serde(default)]
    pub skip_verify: bool,
    #[serde(default)]
    pub ca_cert_pem: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OnDemandMigrationFilter {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub source_prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationHeadPolicy {
    #[default]
    Proxy,
    LocalOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationRangeGetPolicy {
    #[default]
    ServeAndBackfill,
    ServeOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationSourceErrorPolicy {
    #[default]
    Propagate,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationSourceTimeout {
    #[serde(default = "default_connect_ms")]
    pub connect_ms: u64,
    #[serde(default = "default_first_byte_ms")]
    pub first_byte_ms: u64,
    #[serde(default = "default_idle_ms")]
    pub idle_ms: u64,
}

impl Default for OnDemandMigrationSourceTimeout {
    fn default() -> Self {
        Self {
            connect_ms: default_connect_ms(),
            first_byte_ms: default_first_byte_ms(),
            idle_ms: default_idle_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationPolicy {
    #[serde(default)]
    pub head: OnDemandMigrationHeadPolicy,
    #[serde(default)]
    pub range_get: OnDemandMigrationRangeGetPolicy,
    #[serde(default)]
    pub source_error: OnDemandMigrationSourceErrorPolicy,
    #[serde(default = "default_true")]
    pub respect_local_delete_marker: bool,
    #[serde(default = "default_true")]
    pub preserve_etag: bool,
    #[serde(default)]
    pub copy_tags: bool,
    #[serde(default = "default_true")]
    pub emit_events: bool,
    #[serde(default = "default_negative_cache_ttl_secs")]
    pub negative_cache_ttl_secs: u64,
    #[serde(default = "default_inline_max_bytes")]
    pub inline_max_bytes: u64,
    #[serde(default = "default_multipart_part_size_bytes")]
    pub multipart_part_size_bytes: u64,
    #[serde(default = "default_max_concurrent_pulls")]
    pub max_concurrent_pulls: u32,
    #[serde(default = "default_pull_queue_capacity")]
    pub pull_queue_capacity: u32,
    #[serde(default)]
    pub source_timeout: OnDemandMigrationSourceTimeout,
    #[serde(default)]
    pub bandwidth_limit_bytes_per_sec: Option<u64>,
}

impl Default for OnDemandMigrationPolicy {
    fn default() -> Self {
        Self {
            head: OnDemandMigrationHeadPolicy::default(),
            range_get: OnDemandMigrationRangeGetPolicy::default(),
            source_error: OnDemandMigrationSourceErrorPolicy::default(),
            respect_local_delete_marker: true,
            preserve_etag: true,
            copy_tags: false,
            emit_events: true,
            negative_cache_ttl_secs: default_negative_cache_ttl_secs(),
            inline_max_bytes: default_inline_max_bytes(),
            multipart_part_size_bytes: default_multipart_part_size_bytes(),
            max_concurrent_pulls: default_max_concurrent_pulls(),
            pull_queue_capacity: default_pull_queue_capacity(),
            source_timeout: OnDemandMigrationSourceTimeout::default(),
            bandwidth_limit_bytes_per_sec: None,
        }
    }
}

const MIB: u64 = 1024 * 1024;

fn default_version() -> u32 {
    ON_DEMAND_MIGRATION_CONFIG_VERSION
}
fn default_true() -> bool {
    true
}
fn default_negative_cache_ttl_secs() -> u64 {
    30
}
fn default_inline_max_bytes() -> u64 {
    16 * MIB
}
fn default_multipart_part_size_bytes() -> u64 {
    64 * MIB
}
fn default_max_concurrent_pulls() -> u32 {
    8
}
fn default_pull_queue_capacity() -> u32 {
    1024
}
fn default_connect_ms() -> u64 {
    5000
}
fn default_first_byte_ms() -> u64 {
    15_000
}
fn default_idle_ms() -> u64 {
    30_000
}

/// What the source answered during `PUT` validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationProbe {
    pub reachable: bool,
    pub listable: bool,
    #[serde(default)]
    pub sample_key: Option<String>,
}

/// `PUT` response: the redacted config plus the probe summary. `updated_at`
/// is `None` for a dry run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationSetResponse {
    pub bucket: String,
    pub dry_run: bool,
    pub config: OnDemandMigrationConfig,
    #[serde(default)]
    pub updated_at: Option<String>,
    pub probe: OnDemandMigrationProbe,
}

/// `GET` response: the redacted config and its RFC 3339 save time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationGetResponse {
    pub bucket: String,
    pub config: OnDemandMigrationConfig,
    pub updated_at: String,
}

/// `GET .../status` response. Runtime counters are added by later ODM tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationStatus {
    pub configured: bool,
    pub enabled: bool,
    pub module_enabled: bool,
}

fn config_path(bucket: &str) -> String {
    format!("/v3/on-demand-migration/{}", percent_encode_path_segment(bucket))
}

impl AdminClient {
    /// Configure the on-demand migration source of `bucket`. With `dry_run`
    /// the server validates and probes the source but saves nothing.
    pub async fn set_on_demand_migration(
        &self,
        bucket: &str,
        config: &OnDemandMigrationConfig,
        dry_run: bool,
    ) -> Result<OnDemandMigrationSetResponse, AdminClientError> {
        let body = serde_json::to_vec(config).map_err(|err| AdminClientError::Decode {
            message: err.to_string(),
        })?;
        let mut query = Vec::new();
        if dry_run {
            query.push((DRY_RUN_QUERY, "true".to_string()));
        }
        let url = self.url_for(&config_path(bucket), &query)?;
        let request = self.sign_and_build(Method::PUT, url, body, Some("application/json")).await?;
        self.execute(request).await
    }

    /// Read the (redacted) on-demand migration config of `bucket`. A bucket
    /// without one answers HTTP 404 `NoSuchConfiguration`.
    pub async fn get_on_demand_migration(&self, bucket: &str) -> Result<OnDemandMigrationGetResponse, AdminClientError> {
        self.get_json(&config_path(bucket)).await
    }

    /// Clear the on-demand migration config of `bucket`; already-pulled
    /// objects stay. Idempotent: a bucket without a config still answers 204.
    pub async fn delete_on_demand_migration(&self, bucket: &str) -> Result<(), AdminClientError> {
        let url = self.url_for(&config_path(bucket), &[])?;
        let request = self.sign_and_build(Method::DELETE, url, Vec::new(), None).await?;
        self.execute_no_content(request).await
    }

    /// Read the on-demand migration status of `bucket`.
    pub async fn on_demand_migration_status(&self, bucket: &str) -> Result<OnDemandMigrationStatus, AdminClientError> {
        self.get_json(&format!("{}/status", config_path(bucket))).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestServer;

    const SET_REQUEST_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/set_request.json");
    const SET_RESPONSE_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/set_response.json");
    const GET_RESPONSE_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/get_response.json");
    const STATUS_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/status.json");

    fn round_trip<T: Serialize + for<'de> Deserialize<'de>>(fixture: &str) -> T {
        let value: T = serde_json::from_str(fixture.trim()).expect("fixture decodes");
        let reserialized = serde_json::to_string(&value).expect("fixture re-encodes");
        assert_eq!(
            reserialized,
            fixture.trim(),
            "client wire shape must reproduce the server fixture byte for byte"
        );
        value
    }

    #[test]
    fn config_fixture_round_trips_byte_for_byte() {
        let config: OnDemandMigrationConfig = round_trip(SET_REQUEST_FIXTURE);
        assert_eq!(config.version, ON_DEMAND_MIGRATION_CONFIG_VERSION);
        assert_eq!(config.source.provider, OnDemandMigrationProvider::Minio);
        assert_eq!(config.source.path_style, OnDemandMigrationPathStyle::Auto);
        assert_eq!(config.filter.source_prefix.as_deref(), Some("photos/"));
        assert_eq!(
            config.policy,
            OnDemandMigrationPolicy::default(),
            "fixture policy is the documented default"
        );
        assert_eq!(config.source.tls, OnDemandMigrationTls::default());
    }

    #[test]
    fn set_response_fixture_round_trips_and_is_redacted() {
        let response: OnDemandMigrationSetResponse = round_trip(SET_RESPONSE_FIXTURE);
        assert_eq!(response.bucket, "photos");
        assert!(!response.dry_run);
        assert_eq!(response.updated_at.as_deref(), Some("2026-09-02T10:00:00Z"));
        assert_eq!(response.probe.sample_key.as_deref(), Some("photos/2024/01.jpg"));
        let credentials = response.config.source.credentials.expect("credentials present");
        assert_eq!(credentials.secret_key, "REDACTED");
        assert!(!format!("{credentials:?}").contains("sourceSecretKey123"));
    }

    #[test]
    fn get_response_and_status_fixtures_round_trip() {
        let response: OnDemandMigrationGetResponse = round_trip(GET_RESPONSE_FIXTURE);
        assert_eq!(response.updated_at, "2026-09-02T10:00:00Z");
        let status: OnDemandMigrationStatus = round_trip(STATUS_FIXTURE);
        assert!(status.configured && status.enabled && !status.module_enabled);
    }

    #[test]
    fn minimal_config_expands_to_the_server_defaults() {
        let config = OnDemandMigrationConfig::new(OnDemandMigrationSource {
            provider: OnDemandMigrationProvider::Minio,
            endpoint: Some("https://source.example.com:9000".to_string()),
            region: "us-east-1".to_string(),
            bucket: "legacy-photos".to_string(),
            path_style: OnDemandMigrationPathStyle::Auto,
            credentials: Some(OnDemandMigrationCredentials {
                access_key: "AKIASOURCE".to_string(),
                secret_key: "sourceSecretKey123".to_string(),
                session_token: None,
            }),
            tls: OnDemandMigrationTls::default(),
        });
        let mut expected: OnDemandMigrationConfig = serde_json::from_str(SET_REQUEST_FIXTURE.trim()).expect("fixture");
        expected.filter.source_prefix = None;
        assert_eq!(config, expected);

        // A client-side minimal document decodes with the same defaults.
        let minimal: OnDemandMigrationConfig = serde_json::from_str(
            r#"{"source":{"provider":"s3","endpoint":"https://s.example","region":"us-east-1","bucket":"b"}}"#,
        )
        .expect("minimal decodes");
        assert!(minimal.enabled);
        assert_eq!(minimal.policy.max_concurrent_pulls, 8);
        assert!(minimal.source.credentials.is_none());
    }

    #[test]
    fn credentials_debug_never_prints_secrets() {
        let credentials = OnDemandMigrationCredentials {
            access_key: "AKIASOURCE".to_string(),
            secret_key: "sourceSecretKey123".to_string(),
            session_token: Some("token-value".to_string()),
        };
        let rendered = format!("{credentials:?}");
        assert!(rendered.contains("AKIASOURCE"));
        assert!(!rendered.contains("sourceSecretKey123"));
        assert!(!rendered.contains("token-value"));
    }

    #[tokio::test]
    async fn set_sends_the_config_as_the_signed_put_body() {
        let server = TestServer::spawn(SET_RESPONSE_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let config: OnDemandMigrationConfig = serde_json::from_str(SET_REQUEST_FIXTURE.trim()).unwrap();

        let response = client
            .set_on_demand_migration("photos", &config, false)
            .await
            .expect("set decodes");
        assert_eq!(response.config.source.credentials.unwrap().secret_key, "REDACTED");

        let request = server.recorded();
        assert_eq!(request.method, "PUT");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos");
        assert_eq!(request.query, "", "dry-run must not be sent unless requested");
        assert_eq!(request.header("content-type").as_deref(), Some("application/json"));
        assert!(
            request
                .header("authorization")
                .is_some_and(|auth| auth.starts_with("AWS4-HMAC-SHA256"))
        );
        assert_eq!(request.body, SET_REQUEST_FIXTURE.trim(), "the body is the canonical config document");
    }

    #[tokio::test]
    async fn dry_run_adds_the_query_flag_and_tolerates_a_null_timestamp() {
        let dry_run_body = SET_RESPONSE_FIXTURE
            .trim()
            .replace(r#""dry_run":false"#, r#""dry_run":true"#)
            .replace(r#""updated_at":"2026-09-02T10:00:00Z""#, r#""updated_at":null"#);
        let leaked: &'static str = Box::leak(dry_run_body.into_boxed_str());
        let server = TestServer::spawn(leaked, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let config: OnDemandMigrationConfig = serde_json::from_str(SET_REQUEST_FIXTURE.trim()).unwrap();

        let response = client
            .set_on_demand_migration("my bucket", &config, true)
            .await
            .expect("dry run decodes");
        assert!(response.dry_run);
        assert_eq!(response.updated_at, None);

        let request = server.recorded();
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/my%20bucket");
        assert_eq!(request.query, "dry-run=true");
    }

    #[tokio::test]
    async fn get_and_status_use_the_registered_routes() {
        let server = TestServer::spawn(GET_RESPONSE_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let response = client.get_on_demand_migration("photos").await.expect("get decodes");
        assert_eq!(response.updated_at, "2026-09-02T10:00:00Z");
        let request = server.recorded();
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos");

        let server = TestServer::spawn(STATUS_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let status = client.on_demand_migration_status("photos").await.expect("status decodes");
        assert!(status.configured);
        let request = server.recorded();
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos/status");
    }

    #[tokio::test]
    async fn delete_accepts_an_empty_204_and_surfaces_other_statuses() {
        let server = TestServer::spawn("", 204).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        client.delete_on_demand_migration("photos").await.expect("204 is success");
        let request = server.recorded();
        assert_eq!(request.method, "DELETE");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos");

        let server = TestServer::spawn(r#"{"code":"NoSuchConfiguration","message":"not configured"}"#, 404).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        match client.get_on_demand_migration("photos").await.unwrap_err() {
            AdminClientError::HttpStatus { status, body } => {
                assert_eq!(status, 404);
                assert!(body.contains("NoSuchConfiguration"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }
}
