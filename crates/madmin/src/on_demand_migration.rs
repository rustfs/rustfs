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
//! Wire types for `PUT`/`GET`/`DELETE /v3/on-demand-migration/{bucket}`,
//! `GET .../status`, `POST .../backfill?op=start|cancel` and
//! `GET .../backfill` (ODM-12), mirroring the server's config model
//! (`crates/ecstore/src/bucket/on_demand_migration/config.rs`) and handler
//! responses (`rustfs/src/admin/handlers/on_demand_migration.rs`). The SDK
//! owns its own copies, madmin-go style; the fixtures under
//! `fixtures/on_demand_migration/` are the contract both sides pin
//! byte-for-byte, so field order, defaults and `null` handling here must
//! match the server exactly.

use crate::client::{AdminClient, AdminClientError, percent_encode_path_segment};
use http::Method;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Config schema version this client speaks.
pub const ON_DEMAND_MIGRATION_CONFIG_VERSION: u32 = 1;

/// Query flag that validates and probes a config without saving it.
const DRY_RUN_QUERY: &str = "dry-run";
/// `POST .../backfill?op=` selector.
const BACKFILL_OP_QUERY: &str = "op";

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
    /// `None` means anonymous access to a public source bucket. The native
    /// providers carry their credentials in `azure` / `gcs` instead.
    #[serde(default)]
    pub credentials: Option<OnDemandMigrationCredentials>,
    #[serde(default)]
    pub tls: OnDemandMigrationTls,
    /// Required for `azure` and rejected for every other provider.
    #[serde(default)]
    pub azure: Option<OnDemandMigrationAzure>,
    /// Required for `gcs_native` and rejected for every other provider.
    #[serde(default)]
    pub gcs: Option<OnDemandMigrationGcs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDemandMigrationProvider {
    S3,
    Aws,
    Minio,
    Rustfs,
    R2,
    /// GCS XML interoperability API with HMAC keys.
    Gcs,
    /// Native Azure Blob service.
    Azure,
    /// Native GCS JSON API with a service-account key.
    #[serde(rename = "gcs_native")]
    GcsNative,
}

/// Native Azure Blob parameters. The container is `source.bucket`; exactly one
/// of `account_key` and `sas_token` is set. Responses carry both as `REDACTED`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationAzure {
    pub account: String,
    #[serde(default)]
    pub account_key: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

impl fmt::Debug for OnDemandMigrationAzure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnDemandMigrationAzure")
            .field("account", &self.account)
            .field("account_key", &self.account_key.as_ref().map(|_| "REDACTED"))
            .field("sas_token", &self.sas_token.as_ref().map(|_| "REDACTED"))
            .finish()
    }
}

/// Native GCS parameters. The bucket is `source.bucket`; the key JSON embeds a
/// private key, so responses carry it as `REDACTED`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationGcs {
    pub service_account_json: String,
}

impl fmt::Debug for OnDemandMigrationGcs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnDemandMigrationGcs")
            .field("service_account_json", &"REDACTED")
            .finish()
    }
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
    /// Merge the source listing into `ListObjectsV2` (rustfs/backlog#2164).
    #[serde(default)]
    pub list_through: bool,
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
            list_through: false,
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

/// `GET .../status` response: the switch state plus this node's runtime
/// snapshot of the bucket. The runtime fields are `null` while the bucket has
/// no live state on the answering node (module off, config absent or
/// disabled); `provider` and `endpoint_host` then still describe the saved
/// config, if any. `backfill` is present once the bucket had a backfill job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnDemandMigrationStatus {
    pub configured: bool,
    pub enabled: bool,
    pub module_enabled: bool,
    #[serde(default)]
    pub provider: Option<String>,
    /// Host of the source endpoint, without scheme or port.
    #[serde(default)]
    pub endpoint_host: Option<String>,
    #[serde(default)]
    pub breaker: Option<OnDemandMigrationBreaker>,
    #[serde(default)]
    pub counters: Option<OnDemandMigrationCounters>,
    #[serde(default)]
    pub last_source_error: Option<OnDemandMigrationSourceError>,
    #[serde(default)]
    pub inflight_pulls: u64,
    #[serde(default)]
    pub queue_depth: u64,
    /// `source_hit / (source_hit + local GETs)`; `None` when the server has
    /// no per-bucket GET total to divide by (it never reports a made-up 0).
    #[serde(default)]
    pub served_by_source_ratio: Option<f64>,
    /// RFC 3339 save time of the config; `None` when not configured.
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Counters of the bucket's latest backfill job; absent until the bucket
    /// has had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill: Option<OnDemandMigrationBackfillSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationBreakerState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBreaker {
    pub state: OnDemandMigrationBreakerState,
    /// RFC 3339 time the breaker last opened; `None` while closed or when
    /// the server does not report it.
    #[serde(default)]
    pub opened_at: Option<String>,
}

/// Lifetime counters of the bucket's runtime on the answering node. Keys are
/// the fixed label values of the Prometheus series with the same names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationCounters {
    /// `op -> outcome -> count`.
    pub requests_total: BTreeMap<String, BTreeMap<String, u64>>,
    pub pulled_bytes_total: u64,
    /// `path -> count`.
    pub pulled_objects_total: BTreeMap<String, u64>,
    /// `reason -> count`.
    pub pull_failures_total: BTreeMap<String, u64>,
    pub source_latency: OnDemandMigrationSourceLatency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationSourceLatency {
    pub buckets: Vec<OnDemandMigrationLatencyBucket>,
    /// Total observations, including those above the last bound.
    pub count: u64,
    pub sum_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationLatencyBucket {
    /// Upper bound of the bucket in milliseconds.
    pub le_ms: u64,
    /// Cumulative observations at or below `le_ms`.
    pub count: u64,
}

/// The most recent source failure: class only, never the key or message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationSourceError {
    pub class: String,
    /// RFC 3339.
    pub at: String,
}

/// Lifecycle of a backfill job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationBackfillState {
    Pending,
    Running,
    Paused,
    Cancelled,
    Completed,
    CompletedWithFailures,
    Failed,
}

impl OnDemandMigrationBackfillState {
    /// Whether the job still runs (or is about to).
    pub fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}

/// What to do with a listed key that already exists locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDemandMigrationSkipExisting {
    #[default]
    Always,
    EtagOrSize,
}

/// Body of `POST .../backfill?op=start`; every field is optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_existing: Option<OnDemandMigrationSkipExisting>,
    #[serde(default)]
    pub dry_run: bool,
}

/// Last failure recorded by a backfill job; `key_hash` is a hash, never the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillError {
    pub class: String,
    #[serde(default)]
    pub key_hash: Option<String>,
    pub at: String,
}

/// Node running a backfill job and the lease it holds (RFC 3339).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillOwner {
    pub node: String,
    pub lease_until: String,
}

/// The backfill checkpoint as the server stores it. Field order is the
/// on-disk and on-wire contract; unknown fields from newer servers are
/// tolerated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillJob {
    pub format_version: u32,
    pub job_id: String,
    pub state: OnDemandMigrationBackfillState,
    pub config_updated_at: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub skip_existing: OnDemandMigrationSkipExisting,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub listed: u64,
    #[serde(default)]
    pub enqueued: u64,
    #[serde(default)]
    pub pulled: u64,
    #[serde(default)]
    pub skipped_existing: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub last_key: Option<String>,
    #[serde(default)]
    pub last_error: Option<OnDemandMigrationBackfillError>,
    #[serde(default)]
    pub failed_keys: Vec<String>,
    pub started_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub owner: Option<OnDemandMigrationBackfillOwner>,
}

/// `POST`/`GET .../backfill` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillResponse {
    pub bucket: String,
    pub job: OnDemandMigrationBackfillJob,
}

/// Counters of the latest backfill job, embedded in the status response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnDemandMigrationBackfillSummary {
    pub job_id: String,
    pub state: OnDemandMigrationBackfillState,
    pub listed: u64,
    pub enqueued: u64,
    pub pulled: u64,
    pub skipped_existing: u64,
    pub failed: u64,
    pub bytes: u64,
    pub updated_at: String,
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

    /// Start the backfill job of `bucket`. A job that still holds its lease
    /// answers HTTP 409 `OnDemandMigrationBackfillRunning`.
    pub async fn start_on_demand_migration_backfill(
        &self,
        bucket: &str,
        request: &OnDemandMigrationBackfillRequest,
    ) -> Result<OnDemandMigrationBackfillResponse, AdminClientError> {
        let body = serde_json::to_vec(request).map_err(|err| AdminClientError::Decode {
            message: err.to_string(),
        })?;
        self.post_json(&backfill_path(bucket), &[(BACKFILL_OP_QUERY, "start".to_string())], body)
            .await
    }

    /// Cancel the backfill job of `bucket`; idempotent on a finished job. A
    /// bucket that never had a job answers HTTP 404 `NoSuchBackfillJob`.
    pub async fn cancel_on_demand_migration_backfill(
        &self,
        bucket: &str,
    ) -> Result<OnDemandMigrationBackfillResponse, AdminClientError> {
        self.post_json(&backfill_path(bucket), &[(BACKFILL_OP_QUERY, "cancel".to_string())], Vec::new())
            .await
    }

    /// Read the backfill checkpoint of `bucket` (404 `NoSuchBackfillJob`
    /// when none was ever started).
    pub async fn on_demand_migration_backfill(
        &self,
        bucket: &str,
    ) -> Result<OnDemandMigrationBackfillResponse, AdminClientError> {
        self.get_json(&backfill_path(bucket)).await
    }
}

fn backfill_path(bucket: &str) -> String {
    format!("{}/backfill", config_path(bucket))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestServer;

    const SET_REQUEST_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/set_request.json");
    const SET_RESPONSE_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/set_response.json");
    const GET_RESPONSE_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/get_response.json");
    const STATUS_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/status.json");
    const STATUS_WITH_BACKFILL_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/status_with_backfill.json");
    const BACKFILL_JOB_FIXTURE: &str = include_str!("../fixtures/on_demand_migration/backfill_job.json");

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
        assert!(status.configured && status.enabled && status.module_enabled);
        assert_eq!(status.provider.as_deref(), Some("minio"));
        assert_eq!(status.endpoint_host.as_deref(), Some("source.example.com"));
        let breaker = status.breaker.expect("breaker present");
        assert_eq!(breaker.state, OnDemandMigrationBreakerState::HalfOpen);
        assert_eq!(breaker.opened_at, None);
        let counters = status.counters.expect("counters present");
        assert_eq!(counters.requests_total["get"]["source_hit"], 2);
        assert_eq!(counters.requests_total["head"]["negative_cached"], 1);
        assert_eq!(counters.pulled_bytes_total, 4096);
        assert_eq!(counters.pulled_objects_total["inline"], 1);
        assert_eq!(counters.pull_failures_total["source_timeout"], 1);
        assert_eq!(counters.source_latency.buckets.len(), 14);
        assert_eq!(counters.source_latency.count, 3);
        assert_eq!(counters.source_latency.sum_ms, 90_753);
        let last_error = status.last_source_error.expect("last source error present");
        assert_eq!(last_error.class, "server_error");
        assert_eq!(last_error.at, "2026-09-02T10:00:00Z");
        assert_eq!(status.inflight_pulls, 1);
        assert_eq!(status.queue_depth, 1);
        assert_eq!(status.served_by_source_ratio, None, "the ratio is null, never a fabricated 0");
        assert_eq!(status.updated_at.as_deref(), Some("2026-09-02T10:00:00Z"));
        assert!(status.backfill.is_none(), "a bucket without a job carries no summary");
    }

    #[test]
    fn status_without_runtime_state_decodes_with_null_runtime_fields() {
        let status: OnDemandMigrationStatus = serde_json::from_str(
            r#"{"configured":false,"enabled":false,"module_enabled":false,"provider":null,"endpoint_host":null,"breaker":null,"counters":null,"last_source_error":null,"inflight_pulls":0,"queue_depth":0,"served_by_source_ratio":null,"updated_at":null}"#,
        )
        .expect("status decodes");
        assert!(!status.configured);
        assert_eq!(status.provider, None);
        assert_eq!(status.breaker, None);
        assert_eq!(status.counters, None);
        assert_eq!(status.served_by_source_ratio, None);
        assert_eq!(status.updated_at, None);
    }

    #[test]
    fn backfill_fixtures_round_trip_byte_for_byte() {
        let response: OnDemandMigrationBackfillResponse = round_trip(BACKFILL_JOB_FIXTURE);
        assert_eq!(response.bucket, "photos");
        let job = &response.job;
        assert_eq!(job.format_version, 1);
        assert_eq!(job.state, OnDemandMigrationBackfillState::Running);
        assert!(job.state.is_active());
        assert_eq!(job.skip_existing, OnDemandMigrationSkipExisting::Always);
        assert_eq!(job.continuation_token.as_deref(), Some("cGhvdG9zLzEwMDA="));
        assert_eq!((job.listed, job.enqueued, job.pulled, job.failed), (2000, 1500, 1400, 3));
        assert_eq!(job.failed_keys, vec!["9f2c3b0a1d4e5f60".to_string()]);
        assert_eq!(job.last_error.as_ref().map(|e| e.class.as_str()), Some("source_timeout"));
        assert_eq!(job.owner.as_ref().map(|o| o.node.as_str()), Some("node-a:9000"));

        let status: OnDemandMigrationStatus = round_trip(STATUS_WITH_BACKFILL_FIXTURE);
        let summary = status.backfill.expect("summary present");
        assert_eq!(summary.job_id, job.job_id);
        assert_eq!(summary.state, OnDemandMigrationBackfillState::Running);
        assert_eq!(summary.bytes, 73_400_320);

        // A newer server may add checkpoint fields; the client keeps decoding.
        let newer =
            BACKFILL_JOB_FIXTURE
                .trim()
                .replacen("\"listed\":2000", "\"listed\":2000,\"throttle_hint\":{\"mode\":\"soft\"}", 1);
        let decoded: OnDemandMigrationBackfillResponse = serde_json::from_str(&newer).expect("unknown fields tolerated");
        assert_eq!(decoded.job.listed, 2000);
    }

    #[test]
    fn backfill_request_serializes_only_what_was_set() {
        let minimal = serde_json::to_string(&OnDemandMigrationBackfillRequest::default()).expect("serialize");
        assert_eq!(minimal, r#"{"dry_run":false}"#);
        let full = serde_json::to_string(&OnDemandMigrationBackfillRequest {
            prefix: Some("photos/".to_string()),
            skip_existing: Some(OnDemandMigrationSkipExisting::EtagOrSize),
            dry_run: true,
        })
        .expect("serialize");
        assert_eq!(full, r#"{"prefix":"photos/","skip_existing":"etag_or_size","dry_run":true}"#);
    }

    #[tokio::test]
    async fn backfill_start_cancel_and_get_use_the_registered_route() {
        let server = TestServer::spawn(BACKFILL_JOB_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let response = client
            .start_on_demand_migration_backfill(
                "photos",
                &OnDemandMigrationBackfillRequest {
                    prefix: Some("photos/".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("start decodes");
        assert_eq!(response.job.state, OnDemandMigrationBackfillState::Running);
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos/backfill");
        assert_eq!(request.query, "op=start");
        assert_eq!(request.header("content-type").as_deref(), Some("application/json"));
        assert_eq!(request.body, r#"{"prefix":"photos/","dry_run":false}"#);

        let server = TestServer::spawn(BACKFILL_JOB_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        client
            .cancel_on_demand_migration_backfill("photos")
            .await
            .expect("cancel decodes");
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.query, "op=cancel");
        assert_eq!(request.body, "", "cancel sends no body");

        let server = TestServer::spawn(BACKFILL_JOB_FIXTURE, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        client.on_demand_migration_backfill("photos").await.expect("get decodes");
        let request = server.recorded();
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/rustfs/admin/v3/on-demand-migration/photos/backfill");

        let server = TestServer::spawn(
            r#"{"code":"OnDemandMigrationBackfillRunning","message":"a backfill job is already running"}"#,
            409,
        )
        .await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        match client
            .start_on_demand_migration_backfill("photos", &OnDemandMigrationBackfillRequest::default())
            .await
            .unwrap_err()
        {
            AdminClientError::HttpStatus { status, body } => {
                assert_eq!(status, 409);
                assert!(body.contains("OnDemandMigrationBackfillRunning"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
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
            azure: None,
            gcs: None,
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
    fn native_provider_documents_round_trip_and_hide_their_secrets() {
        for (label, json) in [
            (
                "azure",
                r#"{"provider":"azure","endpoint":null,"region":"auto","bucket":"legacy-photos","path_style":"auto","credentials":null,"tls":{"skip_verify":false,"ca_cert_pem":null},"azure":{"account":"legacyaccount","account_key":null,"sas_token":"sv=2021-08-06&sig=topsecret"},"gcs":null}"#,
            ),
            (
                "gcs_native",
                r#"{"provider":"gcs_native","endpoint":null,"region":"auto","bucket":"legacy-photos","path_style":"auto","credentials":null,"tls":{"skip_verify":false,"ca_cert_pem":null},"azure":null,"gcs":{"service_account_json":"{\"type\":\"service_account\"}"}}"#,
            ),
        ] {
            let source: OnDemandMigrationSource = serde_json::from_str(json).unwrap_or_else(|err| panic!("{label}: {err}"));
            assert_eq!(
                serde_json::to_string(&source).expect("re-encodes"),
                json,
                "{label} must reproduce the server wire shape byte for byte"
            );
        }

        let azure = OnDemandMigrationAzure {
            account: "legacyaccount".to_string(),
            account_key: Some("c2VjcmV0".to_string()),
            sas_token: Some("sig=topsecret".to_string()),
        };
        let rendered = format!("{azure:?}");
        assert!(rendered.contains("legacyaccount"));
        assert!(!rendered.contains("c2VjcmV0"), "{rendered}");
        assert!(!rendered.contains("topsecret"), "{rendered}");

        let gcs = OnDemandMigrationGcs {
            service_account_json: r#"{"private_key":"-----BEGIN PRIVATE KEY-----"}"#.to_string(),
        };
        assert!(!format!("{gcs:?}").contains("PRIVATE KEY"), "{gcs:?}");
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
