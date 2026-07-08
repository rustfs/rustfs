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

use crate::bucket::metadata::BucketMetadata;
use crate::bucket::metadata_sys::get_bucket_targets_config;
use crate::bucket::metadata_sys::get_replication_config;
use crate::bucket::replication::{ReplicationStatusType, ReplicationTargetConfigBridge};
use crate::bucket::target::ARN;
use crate::bucket::target::BucketTargetType;
use crate::bucket::target::{self, BucketTarget, BucketTargets, Credentials};
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::runtime::sources as runtime_sources;
use aws_credential_types::Credentials as SdkCredentials;
use aws_sdk_s3::config::Region as SdkRegion;
use aws_sdk_s3::config::SharedHttpClient;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumMode, CompletedMultipartUpload, CompletedPart, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
};
use aws_sdk_s3::{Client as S3Client, Config as S3Config, operation::head_object::HeadObjectOutput};
use aws_sdk_s3::{config::SharedCredentialsProvider, types::BucketVersioningStatus};
use aws_smithy_http_client::{Builder as SmithyHttpClientBuilder, tls as smithy_tls};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{
    HttpConnector as SmithyHttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use reqwest::Client as HttpClient;
use rustfs_config::{DEFAULT_TRUST_LEAF_CERT_AS_CA, ENV_TRUST_LEAF_CERT_AS_CA, RUSTFS_CA_CERT, RUSTFS_TLS_CERT};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_STORAGE_CLASS, AMZ_WEBSITE_REDIRECT_LOCATION, is_amz_header, is_minio_header,
    is_rustfs_header, is_standard_header, is_storageclass_header,
};
use rustfs_utils::http::{
    SUFFIX_FORCE_DELETE, SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK,
    SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID, insert_header,
};
use rustls_pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tower::Service;
use tracing::error;
use tracing::warn;
use url::Url;
use uuid::Uuid;

const DEFAULT_HEALTH_CHECK_DURATION: Duration = Duration::from_secs(5);
const DEFAULT_HEALTH_CHECK_RELOAD_DURATION: Duration = Duration::from_secs(30 * 60);
const REDACTED_CREDENTIAL: &str = "<redacted>";

pub static GLOBAL_BUCKET_TARGET_SYS: OnceLock<BucketTargetSys> = OnceLock::new();

fn replication_target_versioning_enabled(versioning: Option<&BucketVersioningStatus>) -> bool {
    matches!(versioning, Some(BucketVersioningStatus::Enabled))
}

#[derive(Debug, Clone)]
pub struct ArnTarget {
    pub client: Option<Arc<TargetClient>>,
    pub last_refresh: OffsetDateTime,
}

impl Default for ArnTarget {
    fn default() -> Self {
        Self {
            client: None,
            last_refresh: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl ArnTarget {
    pub fn with_client(client: Arc<TargetClient>) -> Self {
        Self {
            client: Some(client),
            last_refresh: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArnErrs {
    pub count: i64,
    pub update_in_progress: bool,
    pub bucket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastMinuteLatency {
    times: Vec<Duration>,
    #[serde(skip, default = "instant_now")]
    start_time: Instant,
}

fn instant_now() -> Instant {
    Instant::now()
}

impl Default for LastMinuteLatency {
    fn default() -> Self {
        Self {
            times: Vec::new(),
            start_time: Instant::now(),
        }
    }
}

impl LastMinuteLatency {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, duration: Duration) {
        let now = Instant::now();
        // Remove entries older than 1 minute
        self.times
            .retain(|_| now.duration_since(self.start_time) < Duration::from_secs(60));
        self.times.push(duration);
    }

    pub fn get_total(&self) -> LatencyAverage {
        if self.times.is_empty() {
            return LatencyAverage {
                avg: Duration::from_secs(0),
            };
        }
        let total: Duration = self.times.iter().sum();
        LatencyAverage {
            avg: total / self.times.len() as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyAverage {
    pub avg: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyStat {
    pub lastmin: LastMinuteLatency,
    pub curr: Duration,
    pub avg: Duration,
    pub peak: Duration,
    pub n: i64,
}

impl LatencyStat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, duration: Duration) {
        self.lastmin.add(duration);
        self.n += 1;
        if duration > self.peak {
            self.peak = duration;
        }
        self.curr = self.lastmin.get_total().avg;
        self.avg = Duration::from_nanos(
            (self.avg.as_nanos() as i64 * (self.n - 1) + self.curr.as_nanos() as i64) as u64 / self.n as u64,
        );
    }
}

#[derive(Debug, Clone)]
pub struct EpHealth {
    pub endpoint: String,
    pub scheme: String,
    pub online: bool,
    pub last_online: Option<OffsetDateTime>,
    pub last_hc_at: Option<OffsetDateTime>,
    pub offline_duration: Duration,
    pub offline_count: u64,
    pub latency: LatencyStat,
}

impl Default for EpHealth {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            scheme: String::new(),
            online: true,
            last_online: None,
            last_hc_at: None,
            offline_duration: Duration::from_secs(0),
            offline_count: 0,
            latency: LatencyStat::new(),
        }
    }
}

fn endpoint_health_key(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn update_endpoint_health(health: &mut EpHealth, online: bool, latency: Duration, now: OffsetDateTime) {
    let prev_online = health.online;
    health.online = online;
    health.last_hc_at = Some(now);
    health.latency.update(latency);

    if online {
        health.last_online = Some(now);
        return;
    }

    if prev_online {
        health.offline_count += 1;
    }
    health.offline_duration += latency;
}

#[derive(Debug, Default)]
pub struct BucketTargetSys {
    pub arn_remotes_map: Arc<RwLock<HashMap<String, ArnTarget>>>,
    pub targets_map: Arc<RwLock<HashMap<String, Vec<BucketTarget>>>>,
    pub h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    pub hc_client: Arc<HttpClient>,
    pub a_mutex: Arc<Mutex<HashMap<String, ArnErrs>>>,
    pub arn_errs_map: Arc<RwLock<HashMap<String, ArnErrs>>>,
}

impl BucketTargetSys {
    pub fn get() -> &'static Self {
        GLOBAL_BUCKET_TARGET_SYS.get_or_init(Self::new)
    }

    fn new() -> Self {
        Self {
            arn_remotes_map: Arc::new(RwLock::new(HashMap::new())),
            targets_map: Arc::new(RwLock::new(HashMap::new())),
            h_mutex: Arc::new(RwLock::new(HashMap::new())),
            hc_client: Arc::new(HttpClient::new()),
            a_mutex: Arc::new(Mutex::new(HashMap::new())),
            arn_errs_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn is_offline(&self, url: &Url) -> bool {
        let key = endpoint_health_key(url);
        {
            let health_map = self.h_mutex.read().await;
            if let Some(health) = health_map.get(&key) {
                return !health.online;
            }
        }
        // Initialize health check if not exists
        self.init_hc(url).await;
        false
    }

    pub async fn mark_offline(&self, url: &Url) {
        let key = endpoint_health_key(url);
        let mut health_map = self.h_mutex.write().await;
        if let Some(health) = health_map.get_mut(&key) {
            update_endpoint_health(health, false, Duration::from_secs(0), OffsetDateTime::now_utc());
        }
    }

    pub async fn init_hc(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        let host = endpoint_health_key(url);
        health_map.insert(
            host.clone(),
            EpHealth {
                endpoint: host,
                scheme: url.scheme().to_string(),
                online: true,
                ..Default::default()
            },
        );
    }

    pub async fn heartbeat(&self) {
        let mut interval = tokio::time::interval(DEFAULT_HEALTH_CHECK_DURATION);
        loop {
            interval.tick().await;

            let endpoints = {
                let health_map = self.h_mutex.read().await;
                health_map
                    .iter()
                    .map(|(endpoint, health)| (endpoint.clone(), health.scheme.clone()))
                    .collect::<Vec<_>>()
            };

            for (endpoint, scheme) in endpoints {
                // Perform health check
                let start = Instant::now();
                let online = self.check_endpoint_health(&endpoint, &scheme).await;
                let duration = start.elapsed();

                {
                    let mut health_map = self.h_mutex.write().await;
                    if let Some(health) = health_map.get_mut(&endpoint) {
                        update_endpoint_health(health, online, duration, OffsetDateTime::now_utc());
                    }
                }
            }
        }
    }

    async fn check_endpoint_health(&self, endpoint: &str, scheme: &str) -> bool {
        let scheme = if scheme.is_empty() { "https" } else { scheme };
        let url = format!("{scheme}://{endpoint}/");
        match self.hc_client.head(url).timeout(Duration::from_secs(3)).send().await {
            Ok(response) => response.status().as_u16() < 500,
            Err(_) => false,
        }
    }

    pub async fn health_stats(&self) -> HashMap<String, EpHealth> {
        let health_map = self.h_mutex.read().await;
        health_map.clone()
    }

    pub async fn list_targets(&self, bucket: &str, arn_type: &str) -> Vec<BucketTarget> {
        let health_stats = self.health_stats().await;
        let mut targets = Vec::new();

        if !bucket.is_empty() {
            if let Ok(bucket_targets) = self.list_bucket_targets(bucket).await {
                for mut target in bucket_targets.targets {
                    if arn_type.is_empty() || target.target_type.to_string() == arn_type {
                        if let Some(health) = health_stats.get(&target.endpoint) {
                            target.total_downtime = health.offline_duration;
                            target.online = health.online;
                            target.last_online = health.last_online;
                            target.latency = target::LatencyStat {
                                curr: health.latency.curr,
                                avg: health.latency.avg,
                                max: health.latency.peak,
                            };
                            target.offline_count = health.offline_count;
                        }
                        targets.push(target);
                    }
                }
            }
            return targets;
        }

        let targets_map = self.targets_map.read().await;
        for bucket_targets in targets_map.values() {
            for mut target in bucket_targets.iter().cloned() {
                if arn_type.is_empty() || target.target_type.to_string() == arn_type {
                    if let Some(health) = health_stats.get(&target.endpoint) {
                        target.total_downtime = health.offline_duration;
                        target.online = health.online;
                        target.last_online = health.last_online;
                        target.latency = target::LatencyStat {
                            curr: health.latency.curr,
                            avg: health.latency.avg,
                            max: health.latency.peak,
                        };
                        target.offline_count = health.offline_count;
                    }
                    targets.push(target);
                }
            }
        }

        targets
    }

    pub async fn list_bucket_targets(&self, bucket: &str) -> Result<BucketTargets, BucketTargetError> {
        let targets_map = self.targets_map.read().await;
        if let Some(targets) = targets_map.get(bucket) {
            Ok(BucketTargets {
                targets: targets.clone(),
            })
        } else {
            Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: bucket.to_string(),
            })
        }
    }

    pub async fn delete(&self, bucket: &str) {
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;

        if let Some(targets) = targets_map.remove(bucket) {
            for target in targets {
                arn_remotes_map.remove(&target.arn);
            }
        }
    }

    pub async fn set_target(
        &self,
        bucket: &str,
        target: &BucketTarget,
        update: bool,
    ) -> Result<BucketTargets, BucketTargetError> {
        self.validate_target(bucket, target).await?;

        let mut bucket_targets = match self.list_bucket_targets(bucket).await {
            Ok(targets) => targets,
            Err(BucketTargetError::BucketRemoteTargetNotFound { .. }) => BucketTargets::default(),
            Err(err) => return Err(err),
        };

        Self::upsert_target_entry(&mut bucket_targets.targets, target, update)?;

        Ok(bucket_targets)
    }

    pub async fn validate_target(&self, bucket: &str, target: &BucketTarget) -> Result<(), BucketTargetError> {
        if !target.target_type.is_valid() {
            return Err(BucketTargetError::BucketRemoteArnTypeInvalid {
                bucket: bucket.to_string(),
            });
        }

        let target_client = self.get_remote_target_client_internal(target).await?;

        // Validate target credentials
        if !self.validate_target_credentials(target).await? {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: target.target_bucket.clone(),
            });
        }

        match target_client.bucket_exists(&target.target_bucket).await {
            Ok(false) => {
                return Err(BucketTargetError::BucketRemoteTargetNotFound {
                    bucket: target.target_bucket.clone(),
                });
            }
            Err(e) => {
                return Err(BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: target.credentials.as_ref().map(|c| c.access_key.clone()).unwrap_or_default(),
                    error: e.to_string(),
                });
            }
            Ok(true) => {}
        }

        if target.target_type == BucketTargetType::ReplicationService {
            if !BucketVersioningSys::enabled(bucket).await {
                return Err(BucketTargetError::BucketReplicationSourceNotVersioned {
                    bucket: bucket.to_string(),
                });
            }

            let versioning = target_client
                .get_bucket_versioning(&target.target_bucket)
                .await
                .map_err(|e| BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: target.credentials.as_ref().map(|c| c.access_key.clone()).unwrap_or_default(),
                    error: e.to_string(),
                })?;

            if !replication_target_versioning_enabled(versioning.as_ref()) {
                return Err(BucketTargetError::BucketRemoteTargetNotVersioned {
                    bucket: target.target_bucket.to_string(),
                });
            }
        }

        Ok(())
    }

    fn upsert_target_entry(
        bucket_targets: &mut Vec<BucketTarget>,
        target: &BucketTarget,
        update: bool,
    ) -> Result<(), BucketTargetError> {
        let mut found = false;

        for (idx, existing_target) in bucket_targets.iter().enumerate() {
            if existing_target.target_type.to_string() == target.target_type.to_string() {
                if existing_target.arn == target.arn {
                    if !update {
                        return Err(BucketTargetError::BucketRemoteAlreadyExists {
                            bucket: existing_target.target_bucket.clone(),
                        });
                    }
                    bucket_targets[idx] = target.clone();
                    found = true;
                    break;
                }
                if existing_target.endpoint == target.endpoint {
                    return Err(BucketTargetError::BucketRemoteAlreadyExists {
                        bucket: existing_target.target_bucket.clone(),
                    });
                }
            }
        }

        if !found && !update {
            bucket_targets.push(target.clone());
        }

        Ok(())
    }

    pub async fn remove_target(&self, bucket: &str, arn_str: &str) -> Result<BucketTargets, BucketTargetError> {
        if arn_str.is_empty() {
            return Err(BucketTargetError::BucketRemoteArnInvalid {
                bucket: bucket.to_string(),
            });
        }

        let arn = ARN::from_str(arn_str).map_err(|_e| BucketTargetError::BucketRemoteArnInvalid {
            bucket: bucket.to_string(),
        })?;

        if arn.arn_type == BucketTargetType::ReplicationService
            && let Ok((config, _)) = get_replication_config(bucket).await
            && ReplicationTargetConfigBridge::target_is_used_by_rules(&config, arn_str)
        {
            let arn_remotes_map = self.arn_remotes_map.read().await;
            if arn_remotes_map.get(arn_str).is_some() {
                return Err(BucketTargetError::BucketRemoteRemoveDisallowed {
                    bucket: bucket.to_string(),
                });
            }
        }

        let targets = self.list_bucket_targets(bucket).await?;
        let new_targets: Vec<BucketTarget> = targets.targets.iter().filter(|t| t.arn != arn_str).cloned().collect();

        if new_targets.len() == targets.targets.len() {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: bucket.to_string(),
            });
        }

        Ok(BucketTargets { targets: new_targets })
    }

    pub async fn mark_refresh_in_progress(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        arn_errs.entry(arn.to_string()).or_insert_with(|| ArnErrs {
            bucket: bucket.to_string(),
            update_in_progress: true,
            count: 1,
        });
    }

    pub async fn mark_refresh_done(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        if let Some(err) = arn_errs.get_mut(arn) {
            err.update_in_progress = false;
            err.bucket = bucket.to_string();
        }
    }

    pub async fn is_reloading_target(&self, _bucket: &str, arn: &str) -> bool {
        let arn_errs = self.arn_errs_map.read().await;
        arn_errs.get(arn).map(|err| err.update_in_progress).unwrap_or(false)
    }

    pub async fn inc_arn_errs(&self, _bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        if let Some(err) = arn_errs.get_mut(arn) {
            err.count += 1;
        }
    }

    pub async fn get_remote_target_client(&self, bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        let (cli, last_refresh) = {
            self.arn_remotes_map
                .read()
                .await
                .get(arn)
                .map(|target| (target.client.clone(), Some(target.last_refresh)))
                .unwrap_or((None, None))
        };

        if let Some(cli) = cli {
            return Some(cli);
        }

        // TODO: spawn a task to reload the target
        if self.is_reloading_target(bucket, arn).await {
            return None;
        }

        if let Some(last_refresh) = last_refresh {
            let now = OffsetDateTime::now_utc();
            if now - last_refresh < Duration::from_secs(60 * 5) {
                return None;
            }
        }

        match get_bucket_targets_config(bucket).await {
            Ok(bucket_targets) => {
                self.mark_refresh_in_progress(bucket, arn).await;
                self.update_all_targets(bucket, Some(&bucket_targets)).await;
                self.mark_refresh_done(bucket, arn).await;
            }
            Err(e) => {
                error!("get bucket targets config error:{}", e);
            }
        };

        let cli = self
            .arn_remotes_map
            .read()
            .await
            .get(arn)
            .and_then(|target| target.client.clone());
        if cli.is_some() {
            return cli;
        }

        self.inc_arn_errs(bucket, arn).await;
        None
    }

    pub async fn get_remote_target_client_internal(&self, target: &BucketTarget) -> Result<TargetClient, BucketTargetError> {
        let Some(credentials) = &target.credentials else {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: target.target_bucket.clone(),
            });
        };

        let creds = SdkCredentials::builder()
            .access_key_id(credentials.access_key.clone())
            .secret_access_key(credentials.secret_key.clone())
            .account_id(target.reset_id.clone())
            .provider_name("bucket_target_sys")
            .build();

        let endpoint = if target.secure {
            format!("https://{}", target.endpoint)
        } else {
            format!("http://{}", target.endpoint)
        };
        let parsed_endpoint = Url::parse(&endpoint).map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
            bucket: target.target_bucket.clone(),
            access_key: credentials.access_key.clone(),
            error: format!("invalid target endpoint: {err}"),
        })?;
        validate_replication_target_endpoint(&parsed_endpoint).map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
            bucket: target.target_bucket.clone(),
            access_key: credentials.access_key.clone(),
            error: format!("target endpoint is not allowed: {err}"),
        })?;

        let mut config_builder = S3Config::builder()
            .endpoint_url(endpoint.clone())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(SdkRegion::new(target.region.clone()))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest());

        if should_force_path_style(target) {
            config_builder = config_builder.force_path_style(true);
        }

        if let Some(http_client) =
            build_aws_s3_http_client_for_target(target)
                .await
                .map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: credentials.access_key.clone(),
                    error: err.to_string(),
                })?
        {
            config_builder = config_builder.http_client(http_client);
        }

        let config = config_builder.build();

        Ok(TargetClient {
            endpoint,
            credentials: target.credentials.clone(),
            bucket: target.target_bucket.clone(),
            storage_class: target.storage_class.clone(),
            disable_proxy: target.disable_proxy,
            arn: target.arn.clone(),
            reset_id: target.reset_id.clone(),
            secure: target.secure,
            health_check_duration: target.health_check_duration,
            replicate_sync: target.replication_sync,
            client: Arc::new(S3Client::from_conf(config)),
        })
    }

    async fn validate_target_credentials(&self, _target: &BucketTarget) -> Result<bool, BucketTargetError> {
        // In a real implementation, you would validate the credentials
        // by making actual API calls to the target
        Ok(true)
    }

    fn update_bandwidth_limit(&self, bucket: &str, arn: &str, limit: i64) {
        if let Some(bucket_monitor) = runtime_sources::bucket_monitor() {
            if limit == 0 {
                bucket_monitor.delete_bucket_throttle(bucket, arn);
                return;
            }
            bucket_monitor.set_bandwidth_limit(bucket, arn, limit);
        } else {
            error!(
                "Global bucket monitor uninitialized; skipping bandwidth limit update for bucket '{}' and ARN '{}'",
                bucket, arn
            );
        }
    }

    pub async fn get_remote_target_client_by_arn(&self, _bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        let arn_remotes_map = self.arn_remotes_map.read().await;
        arn_remotes_map.get(arn).and_then(|target| target.client.clone())
    }

    pub async fn get_remote_bucket_target_by_arn(&self, bucket: &str, arn: &str) -> Option<BucketTarget> {
        let targets_map = self.targets_map.read().await;
        targets_map
            .get(bucket)
            .and_then(|targets| targets.iter().find(|t| t.arn == arn).cloned())
    }

    pub async fn update_all_targets(&self, bucket: &str, targets: Option<&BucketTargets>) {
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        // Remove existing targets
        if let Some(existing_targets) = targets_map.remove(bucket) {
            for target in existing_targets {
                arn_remotes_map.remove(&target.arn);
                self.update_bandwidth_limit(bucket, &target.arn, 0);
            }
        }

        // Add new targets
        if let Some(new_targets) = targets
            && !new_targets.is_empty()
        {
            for target in &new_targets.targets {
                if let Ok(client) = self.get_remote_target_client_internal(target).await {
                    arn_remotes_map.insert(
                        target.arn.clone(),
                        ArnTarget {
                            client: Some(Arc::new(client)),
                            last_refresh: OffsetDateTime::now_utc(),
                        },
                    );
                    self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
                }
            }
            targets_map.insert(bucket.to_string(), new_targets.targets.clone());
        }
    }

    pub async fn set(&self, bucket: &str, meta: &BucketMetadata) {
        let Some(config) = &meta.bucket_target_config else {
            return;
        };

        if config.is_empty() {
            return;
        }

        for target in config.targets.iter() {
            let cli = match self.get_remote_target_client_internal(target).await {
                Ok(cli) => cli,
                Err(e) => {
                    warn!("get_remote_target_client_internal error:{}", e);
                    continue;
                }
            };

            {
                let arn_target = ArnTarget::with_client(Arc::new(cli));
                let mut arn_remotes_map = self.arn_remotes_map.write().await;
                arn_remotes_map.insert(target.arn.clone(), arn_target);
            }
            self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
        }

        let mut targets_map = self.targets_map.write().await;
        targets_map.insert(bucket.to_string(), config.targets.clone());
    }

    // getRemoteARN gets existing ARN for an endpoint or generates a new one.
    pub async fn get_remote_arn(&self, bucket: &str, target: Option<&BucketTarget>, depl_id: &str) -> (String, bool) {
        let Some(target) = target else {
            return (String::new(), false);
        };

        {
            let targets_map = self.targets_map.read().await;
            if let Some(targets) = targets_map.get(bucket) {
                for tgt in targets {
                    if tgt.target_type == target.target_type
                        && tgt.target_bucket == target.target_bucket
                        && target.endpoint == tgt.endpoint
                        && tgt
                            .credentials
                            .as_ref()
                            .map(|c| {
                                let default_creds = Credentials::default();
                                c.access_key == target.credentials.as_ref().unwrap_or(&default_creds).access_key
                            })
                            .unwrap_or(false)
                    {
                        return (tgt.arn.clone(), true);
                    }
                }
            }
        }

        if !target.target_type.is_valid() {
            return (String::new(), false);
        }
        let arn = generate_arn(target, depl_id);
        (arn, false)
    }
}

#[derive(Debug)]
struct AcceptAnyServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct TargetHyperHttpConnector<C> {
    client: HyperClient<C, SdkBody>,
}

impl<C> fmt::Debug for TargetHyperHttpConnector<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetHyperHttpConnector")
            .field("client", &"** hyper client **")
            .finish()
    }
}

impl<C> SmithyHttpConnector for TargetHyperHttpConnector<C>
where
    C: Clone + Send + Sync + 'static,
    C: Service<Uri>,
    C::Response:
        hyper::rt::Read + hyper::rt::Write + hyper_util::client::legacy::connect::Connection + Send + Sync + Unpin + 'static,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };

        let mut client = self.client.clone();
        let fut = client.call(request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(|err| ConnectorError::io(err.into()))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response).map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

fn ensure_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

fn has_custom_ca_pem(target: &BucketTarget) -> bool {
    !target.ca_cert_pem.trim().is_empty()
}

fn validate_replication_target_endpoint(url: &Url) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Ok(()) => Ok(()),
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => Ok(()),
        Err(err) => Err(err),
    }
}

fn build_insecure_aws_s3_http_client() -> SharedHttpClient {
    ensure_rustls_crypto_provider();

    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
        .with_no_client_auth();

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let mut client_builder = HyperClient::builder(TokioExecutor::new());
    client_builder.pool_timer(TokioTimer::new());
    let client = client_builder.build(https);
    let connector = SharedHttpConnector::new(TargetHyperHttpConnector { client });

    http_client_fn(move |_settings, _components| connector.clone())
}

fn build_aws_s3_http_client_from_target_ca_pem(ca_cert_pem: &str) -> Result<SharedHttpClient, BucketTargetError> {
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(ca_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| BucketTargetError::Io(std::io::Error::other(format!("invalid target CA PEM: {err}"))))?;

    if certs.is_empty() {
        return Err(BucketTargetError::Io(std::io::Error::other(
            "invalid target CA PEM: no certificates found",
        )));
    }

    let mut trust_store = smithy_tls::TrustStore::empty();
    trust_store.add_pem_certificate(ca_cert_pem.as_bytes());

    let tls_context = smithy_tls::TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .map_err(|err| BucketTargetError::Io(std::io::Error::other(format!("invalid target CA PEM: {err}"))))?;

    Ok(SmithyHttpClientBuilder::new()
        .tls_provider(smithy_tls::Provider::rustls(smithy_tls::rustls_provider::CryptoMode::AwsLc))
        .tls_context(tls_context)
        .build_https())
}

async fn build_aws_s3_http_client_for_target(target: &BucketTarget) -> Result<Option<SharedHttpClient>, BucketTargetError> {
    if !target.secure {
        return Ok(None);
    }

    if target.skip_tls_verify {
        return Ok(Some(build_insecure_aws_s3_http_client()));
    }

    if has_custom_ca_pem(target) {
        return build_aws_s3_http_client_from_target_ca_pem(&target.ca_cert_pem).map(Some);
    }

    Ok(build_aws_s3_http_client_from_tls_path().await)
}

async fn build_aws_s3_http_client_from_tls_path() -> Option<aws_sdk_s3::config::SharedHttpClient> {
    let tls_path = rustfs_utils::get_env_str(rustfs_config::ENV_RUSTFS_TLS_PATH, rustfs_config::DEFAULT_RUSTFS_TLS_PATH);
    if tls_path.is_empty() {
        return None;
    }

    let tls_dir = Path::new(&tls_path);
    let mut trust_store = smithy_tls::TrustStore::empty();
    let mut has_custom_certs = false;

    let ca_path = tls_dir.join(RUSTFS_CA_CERT);
    match tokio::fs::read(&ca_path).await {
        Ok(pem) => {
            trust_store.add_pem_certificate(pem);
            has_custom_certs = true;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("failed to read custom CA bundle {:?} for replication client: {}", ca_path, e),
    }

    if rustfs_utils::get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA) {
        let leaf_cert_path = tls_dir.join(RUSTFS_TLS_CERT);
        match tokio::fs::read(&leaf_cert_path).await {
            Ok(pem) => {
                trust_store.add_pem_certificate(pem);
                has_custom_certs = true;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!("failed to read leaf cert {:?} for replication client trust store: {}", leaf_cert_path, e),
        }
    }

    if !has_custom_certs {
        return None;
    }

    let tls_context = match smithy_tls::TlsContext::builder().with_trust_store(trust_store).build() {
        Ok(ctx) => ctx,
        Err(e) => {
            warn!("failed to build AWS SDK TLS context for replication client: {}", e);
            return None;
        }
    };

    Some(
        SmithyHttpClientBuilder::new()
            .tls_provider(smithy_tls::Provider::rustls(smithy_tls::rustls_provider::CryptoMode::AwsLc))
            .tls_context(tls_context)
            .build_https(),
    )
}

fn should_force_path_style(target: &BucketTarget) -> bool {
    match target.path.trim().to_ascii_lowercase().as_str() {
        // Explicit DNS/virtual-hosted-style requested by user.
        "dns" | "off" | "false" => false,
        // Explicit path-style or legacy boolean-like values.
        "path" | "on" | "true" => true,
        // `auto` and empty are defaulted to path-style for custom S3-compatible endpoints.
        "auto" | "" => true,
        // Unknown values: prefer compatibility with S3-compatible services.
        _ => true,
    }
}

// generate ARN that is unique to this target type
fn generate_arn(t: &BucketTarget, depl_id: &str) -> String {
    let uuid = if depl_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        depl_id.to_string()
    };
    let arn = ARN {
        arn_type: t.target_type.clone(),
        id: uuid,
        region: t.region.clone(),
        bucket: t.target_bucket.clone(),
    };
    arn.to_string()
}

pub struct RemoveObjectOptions {
    pub force_delete: bool,
    pub governance_bypass: bool,
    pub replication_delete_marker: bool,
    pub replication_mtime: Option<OffsetDateTime>,
    pub replication_status: ReplicationStatusType,
    pub replication_request: bool,
    pub replication_validity_check: bool,
}

fn build_remove_object_headers(version_id: Option<&str>, opts: &RemoveObjectOptions) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if opts.force_delete {
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "true");
    }
    if opts.governance_bypass {
        headers.insert(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, "true".parse().unwrap());
    }

    if opts.replication_delete_marker {
        insert_header(&mut headers, SUFFIX_SOURCE_DELETEMARKER, "true");
    }

    if let Some(t) = opts.replication_mtime {
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, t.format(&Rfc3339).unwrap_or_default());
    }

    if !opts.replication_status.is_empty() {
        headers.insert(AMZ_BUCKET_REPLICATION_STATUS, opts.replication_status.as_str().parse().unwrap());
    }

    if let Some(version_id) = version_id {
        insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, version_id);
    }

    if opts.replication_request {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    }
    if opts.replication_validity_check {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    }

    headers
}

/// Resolve the S3 `versionId` query parameter for a target DELETE.
///
/// A replication delete omits the `versionId` query param ONLY when it is
/// propagating a delete-marker CREATION (`replication_delete_marker`), so the
/// target mints its own marker. A version purge / delete-marker purge / force
/// delete must address the exact version — otherwise a generic (non-MinIO /
/// non-RustFS) S3 target ignores the internal `x-*-source-version-id` header
/// and silently creates a delete marker instead of removing the version, while
/// the source stamps `VersionPurgeStatus=Complete` (backlog#799 B8 / #857).
/// Non-replication callers always pass the version through unchanged.
fn resolve_delete_api_version_id(version_id: Option<String>, opts: &RemoveObjectOptions) -> Option<String> {
    if opts.replication_request && opts.replication_delete_marker {
        None
    } else {
        version_id
    }
}

#[derive(Debug, Clone)]
pub struct AdvancedPutOptions {
    pub source_version_id: String,
    pub source_etag: String,
    pub replication_status: ReplicationStatusType,
    pub source_mtime: OffsetDateTime,
    pub replication_request: bool,
    pub retention_timestamp: OffsetDateTime,
    pub tagging_timestamp: OffsetDateTime,
    pub legalhold_timestamp: OffsetDateTime,
    pub replication_validity_check: bool,
}

impl Default for AdvancedPutOptions {
    fn default() -> Self {
        Self {
            source_version_id: "".to_string(),
            source_etag: "".to_string(),
            replication_status: ReplicationStatusType::Pending,
            source_mtime: OffsetDateTime::now_utc(),
            replication_request: false,
            retention_timestamp: OffsetDateTime::now_utc(),
            tagging_timestamp: OffsetDateTime::now_utc(),
            legalhold_timestamp: OffsetDateTime::now_utc(),
            replication_validity_check: false,
        }
    }
}

#[derive(Clone)]
pub struct PutObjectOptions {
    pub user_metadata: HashMap<String, String>,
    pub user_tags: HashMap<String, String>,
    //pub progress: ReaderImpl,
    pub content_type: String,
    pub content_encoding: String,
    pub content_disposition: String,
    pub content_language: String,
    pub cache_control: String,
    pub expires: OffsetDateTime,
    pub mode: Option<ObjectLockRetentionMode>,
    pub retain_until_date: OffsetDateTime,
    //pub server_side_encryption: encrypt::ServerSide,
    pub num_threads: u64,
    pub storage_class: String,
    pub website_redirect_location: String,
    pub part_size: u64,
    pub legalhold: Option<ObjectLockLegalHoldStatus>,
    pub send_content_md5: bool,
    pub disable_content_sha256: bool,
    pub disable_multipart: bool,
    pub auto_checksum: Option<ChecksumMode>,
    pub checksum: Option<ChecksumMode>,
    pub concurrent_stream_parts: bool,
    pub internal: AdvancedPutOptions,
    pub custom_header: HeaderMap,
}

impl Default for PutObjectOptions {
    fn default() -> Self {
        Self {
            user_metadata: HashMap::new(),
            user_tags: HashMap::new(),
            //progress: ReaderImpl::Body(Bytes::new()),
            content_type: "".to_string(),
            content_encoding: "".to_string(),
            content_disposition: "".to_string(),
            content_language: "".to_string(),
            cache_control: "".to_string(),
            expires: OffsetDateTime::UNIX_EPOCH,
            mode: None,
            retain_until_date: OffsetDateTime::UNIX_EPOCH,
            //server_side_encryption: encrypt.ServerSide::default(),
            num_threads: 0,
            storage_class: "".to_string(),
            website_redirect_location: "".to_string(),
            part_size: 0,
            legalhold: None,
            send_content_md5: false,
            disable_content_sha256: false,
            disable_multipart: false,
            auto_checksum: None,
            checksum: None,
            concurrent_stream_parts: false,
            internal: AdvancedPutOptions::default(),
            custom_header: HeaderMap::new(),
        }
    }
}

#[allow(dead_code)]
impl PutObjectOptions {
    fn set_match_etag(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

    fn set_match_etag_except(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

    /// Insert `value` as header `name`, skipping values that are not valid
    /// HTTP header values (with a warning) instead of panicking mid-replication.
    fn insert_checked(header: &mut HeaderMap, name: &'static str, value: &str) {
        match HeaderValue::from_str(value) {
            Ok(v) => {
                header.insert(name, v);
            }
            Err(_) => warn!("skipping header {} with invalid value", name),
        }
    }

    pub fn header(&self) -> HeaderMap {
        let mut header = HeaderMap::new();

        let mut content_type = self.content_type.clone();
        if content_type.is_empty() {
            content_type = "application/octet-stream".to_string();
        }
        match HeaderValue::from_str(&content_type) {
            Ok(v) => {
                header.insert("Content-Type", v);
            }
            Err(_) => {
                warn!("invalid Content-Type header value, falling back to application/octet-stream");
                header.insert("Content-Type", HeaderValue::from_static("application/octet-stream"));
            }
        }

        if !self.content_encoding.is_empty() {
            Self::insert_checked(&mut header, "Content-Encoding", &self.content_encoding);
        }
        if !self.content_disposition.is_empty() {
            Self::insert_checked(&mut header, "Content-Disposition", &self.content_disposition);
        }
        if !self.content_language.is_empty() {
            Self::insert_checked(&mut header, "Content-Language", &self.content_language);
        }
        if !self.cache_control.is_empty() {
            Self::insert_checked(&mut header, "Cache-Control", &self.cache_control);
        }

        if self.expires.unix_timestamp() != 0 {
            match self.expires.format(&Rfc3339) {
                Ok(expires) => Self::insert_checked(&mut header, "Expires", &expires),
                Err(err) => warn!("skipping Expires header, format failed: {}", err),
            }
        }

        if let Some(mode) = &self.mode {
            Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_MODE, mode.as_str());
        }

        if self.retain_until_date.unix_timestamp() != 0 {
            match self.retain_until_date.format(&Rfc3339) {
                Ok(retain_until) => Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, &retain_until),
                Err(err) => warn!("skipping object-lock retain-until-date header, format failed: {}", err),
            }
        }

        if let Some(legalhold) = &self.legalhold {
            Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_LEGAL_HOLD, legalhold.as_str());
        }

        if !self.storage_class.is_empty() {
            Self::insert_checked(&mut header, AMZ_STORAGE_CLASS, &self.storage_class);
        }

        if !self.website_redirect_location.is_empty() {
            Self::insert_checked(&mut header, AMZ_WEBSITE_REDIRECT_LOCATION, &self.website_redirect_location);
        }

        if !self.internal.replication_status.as_str().is_empty() {
            Self::insert_checked(&mut header, AMZ_BUCKET_REPLICATION_STATUS, self.internal.replication_status.as_str());
        }

        for (k, v) in &self.user_metadata {
            let Ok(header_value) = HeaderValue::from_str(v) else {
                warn!("skipping user metadata header with invalid value: {}", k);
                continue;
            };
            if is_amz_header(k) || is_standard_header(k) || is_storageclass_header(k) || is_rustfs_header(k) || is_minio_header(k)
            {
                if let Ok(header_name) = HeaderName::from_bytes(k.as_bytes()) {
                    header.insert(header_name, header_value);
                }
            } else if let Ok(header_name) = HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()) {
                header.insert(header_name, header_value);
            }
        }

        for (k, v) in self.custom_header.iter() {
            header.insert(k.clone(), v.clone());
        }

        if !self.internal.source_version_id.is_empty() {
            insert_header(&mut header, SUFFIX_SOURCE_VERSION_ID, &self.internal.source_version_id);
        }
        if !self.internal.source_etag.is_empty() {
            insert_header(&mut header, SUFFIX_SOURCE_ETAG, &self.internal.source_etag);
        }
        if self.internal.source_mtime.unix_timestamp() != 0 {
            insert_header(
                &mut header,
                SUFFIX_SOURCE_MTIME,
                self.internal.source_mtime.format(&Rfc3339).unwrap_or_default(),
            );
        }

        if self.internal.replication_request {
            insert_header(&mut header, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        }

        header
    }

    fn validate(&self, _c: Arc<TargetClient>) -> Result<(), std::io::Error> {
        //if self.checksum.is_set() {
        /*if !self.trailing_header_support {
            return Err(Error::from(err_invalid_argument("Checksum requires Client with TrailingHeaders enabled")));
        }*/
        /*else if self.override_signer_type == SignatureType::SignatureV2 {
            return Err(Error::from(err_invalid_argument("Checksum cannot be used with v2 signatures")));
        }*/
        //}

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutObjectPartOptions {
    pub md5_base64: String,
    pub sha256_hex: String,
    //pub sse: encrypt.ServerSide,
    pub custom_header: HeaderMap,
    pub trailer: HeaderMap,
    pub disable_content_sha256: bool,
}

#[derive(Debug)]
pub struct S3ClientError {
    pub error: String,
    pub status_code: Option<StatusCode>,
    pub code: Option<String>,
    pub message: Option<String>,
}
impl S3ClientError {
    pub fn new(value: impl Into<String>) -> Self {
        Self::with_metadata(value, None, None, None)
    }

    pub fn with_metadata(
        error: impl Into<String>,
        status_code: Option<StatusCode>,
        code: Option<String>,
        message: Option<String>,
    ) -> Self {
        S3ClientError {
            error: error.into(),
            status_code,
            code,
            message,
        }
    }

    pub fn add_message(self, message: impl Into<String>) -> Self {
        S3ClientError {
            error: format!("{}: {}", message.into(), self.error),
            status_code: self.status_code,
            code: self.code,
            message: self.message,
        }
    }
}

impl<T: aws_sdk_s3::error::ProvideErrorMetadata> From<T> for S3ClientError {
    fn from(value: T) -> Self {
        let code = value.code().map(String::from);
        let message = value.message().map(String::from);
        let error = match (code.as_deref(), message.as_deref()) {
            (Some(code), Some(message)) => format!("{code}: {message}"),
            (Some(code), None) => code.to_string(),
            (None, Some(message)) => message.to_string(),
            (None, None) => "unknown remote error".to_string(),
        };

        S3ClientError::with_metadata(error, None, code, message)
    }
}

impl std::error::Error for S3ClientError {}

impl std::fmt::Display for S3ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

#[derive(Debug)]
pub struct TargetClient {
    pub endpoint: String,
    pub credentials: Option<Credentials>,
    pub bucket: String,
    pub storage_class: String,
    pub disable_proxy: bool,
    pub arn: String,
    pub reset_id: String,
    pub secure: bool,
    pub health_check_duration: Duration,
    pub replicate_sync: bool,
    pub client: Arc<S3Client>,
}

impl TargetClient {
    pub fn to_url(&self) -> Url {
        Url::parse(&self.endpoint).unwrap()
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, S3ClientError> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                SdkError::ServiceError(oe) => match oe.into_err() {
                    HeadBucketError::NotFound(_) => Ok(false),
                    other => {
                        warn!(
                            "failed to check bucket exists for bucket:{bucket} please check the bucket name and credentials, error:{:?}",
                            other
                        );
                        let message = other.meta().meta();
                        Err(S3ClientError::with_metadata(
                            format!(
                                "failed to check bucket exists for bucket:{bucket} please check the bucket name and credentials, error:{:?}",
                                message
                            ),
                            None,
                            message.code().map(ToOwned::to_owned),
                            message.message().map(ToOwned::to_owned),
                        ))
                    }
                },
                SdkError::DispatchFailure(e) => Err(S3ClientError::new(format!(
                    "failed to dispatch bucket exists for bucket:{bucket} error:{e:?}"
                ))),

                _ => Err(S3ClientError::new(format!(
                    "failed to check bucket exists for bucket:{bucket} error:{e:?}"
                ))),
            },
        }
    }

    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<Option<BucketVersioningStatus>, S3ClientError> {
        match self.client.get_bucket_versioning().bucket(bucket).send().await {
            Ok(res) => Ok(res.status),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(version_id)
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e),
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        body: ByteStream,
        opts: &PutObjectOptions,
    ) -> Result<(), S3ClientError> {
        let mut headers = opts.header();

        let builder = self.client.put_object();

        let version_id = opts.internal.source_version_id.clone();
        if !version_id.is_empty() {
            insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &version_id);
        }

        match builder
            .bucket(bucket)
            .key(object)
            .content_length(size)
            .body(body)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }

                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match e {
                SdkError::ServiceError(service_err) => {
                    let err = service_err.into_err();
                    let meta = err.meta();
                    let error = match (meta.code(), meta.message()) {
                        (Some(code), Some(message)) => format!("put_object failed: {code}: {message}"),
                        (Some(code), None) => format!("put_object failed: {code}"),
                        (None, Some(message)) => format!("put_object failed: {message}"),
                        (None, None) => format!("put_object failed: {err:?}"),
                    };
                    Err(S3ClientError::with_metadata(
                        error,
                        None,
                        meta.code().map(ToOwned::to_owned),
                        meta.message().map(ToOwned::to_owned),
                    ))
                }
                SdkError::DispatchFailure(dispatch_err) => Err(S3ClientError::new(format!(
                    "put_object dispatch failure for bucket:{bucket} object:{object}: {dispatch_err:?}"
                ))),
                other => Err(S3ClientError::new(format!(
                    "put_object request failed for bucket:{bucket} object:{object}: {other:?}"
                ))),
            },
        }
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        let mut headers = HeaderMap::new();
        let version_id = opts.internal.source_version_id.clone();
        if !version_id.is_empty() {
            insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &version_id);
        }
        if opts.internal.replication_request {
            insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        }

        match self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(object)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res.upload_id.unwrap_or_default()),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: i32,
        size: i64,
        body: ByteStream,
        opts: &PutObjectPartOptions,
    ) -> Result<UploadPartOutput, S3ClientError> {
        let headers = opts.custom_header.clone();

        match self
            .client
            .upload_part()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .part_number(part_id)
            .content_length(size)
            .body(body)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
        opts: &PutObjectOptions,
    ) -> Result<CompleteMultipartUploadOutput, S3ClientError> {
        let multipart_upload = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();

        let headers = opts.header();

        match self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .multipart_upload(multipart_upload)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn remove_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        opts: RemoveObjectOptions,
    ) -> Result<(), S3ClientError> {
        let headers = build_remove_object_headers(version_id.as_deref(), &opts);
        let api_version_id = resolve_delete_api_version_id(version_id, &opts);

        match self
            .client
            .delete_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(api_version_id)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(_res) => Ok(()),
            Err(e) => match e {
                SdkError::ServiceError(service_err) => {
                    let err = service_err.into_err();
                    let meta = err.meta();
                    let error = match (meta.code(), meta.message()) {
                        (Some(code), Some(message)) => format!("remove_object failed: {code}: {message}"),
                        (Some(code), None) => format!("remove_object failed: {code}"),
                        (None, Some(message)) => format!("remove_object failed: {message}"),
                        (None, None) => format!("remove_object failed: {err:?}"),
                    };
                    Err(S3ClientError::with_metadata(
                        error,
                        None,
                        meta.code().map(ToOwned::to_owned),
                        meta.message().map(ToOwned::to_owned),
                    ))
                }
                SdkError::DispatchFailure(dispatch_err) => Err(S3ClientError::new(format!(
                    "remove_object dispatch failure for bucket:{bucket} object:{object}: {dispatch_err:?}"
                ))),
                other => Err(S3ClientError::new(format!(
                    "remove_object request failed for bucket:{bucket} object:{object}: {other:?}"
                ))),
            },
        }
    }
}

#[derive(Debug)]
pub enum BucketTargetError {
    BucketRemoteTargetNotFound {
        bucket: String,
    },
    BucketRemoteArnTypeInvalid {
        bucket: String,
    },
    BucketRemoteAlreadyExists {
        bucket: String,
    },
    BucketRemoteArnInvalid {
        bucket: String,
    },
    RemoteTargetConnectionErr {
        bucket: String,
        access_key: String,
        error: String,
    },
    BucketReplicationSourceNotVersioned {
        bucket: String,
    },
    BucketRemoteTargetNotVersioned {
        bucket: String,
    },
    BucketRemoteRemoveDisallowed {
        bucket: String,
    },

    Io(std::io::Error),
}

impl fmt::Display for BucketTargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketTargetError::BucketRemoteTargetNotFound { bucket } => {
                write!(f, "Remote target not found for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteArnTypeInvalid { bucket } => {
                write!(f, "Invalid ARN type for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteAlreadyExists { bucket } => {
                write!(f, "Remote target already exists for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteArnInvalid { bucket } => {
                write!(f, "Invalid ARN for bucket: {bucket}")
            }
            BucketTargetError::RemoteTargetConnectionErr {
                bucket,
                access_key: _,
                error,
            } => {
                write!(
                    f,
                    "Connection error for bucket: {bucket}, access key: {REDACTED_CREDENTIAL}, error: {error}"
                )
            }
            BucketTargetError::BucketReplicationSourceNotVersioned { bucket } => {
                write!(f, "Replication source bucket not versioned: {bucket}")
            }
            BucketTargetError::BucketRemoteTargetNotVersioned { bucket } => {
                write!(f, "Remote target bucket not versioned: {bucket}")
            }
            BucketTargetError::BucketRemoteRemoveDisallowed { bucket } => {
                write!(f, "Remote target removal disallowed for bucket: {bucket}")
            }
            BucketTargetError::Io(e) => write!(f, "IO error: {e}"),
        }
    }
}

impl From<std::io::Error> for BucketTargetError {
    fn from(e: std::io::Error) -> Self {
        BucketTargetError::Io(e)
    }
}

impl Error for BucketTargetError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;

    #[test]
    fn replication_target_versioning_enabled_requires_enabled_status() {
        let enabled = BucketVersioningStatus::Enabled;
        let suspended = BucketVersioningStatus::Suspended;

        assert!(replication_target_versioning_enabled(Some(&enabled)));
        assert!(!replication_target_versioning_enabled(Some(&suspended)));
        assert!(!replication_target_versioning_enabled(None));
    }

    #[test]
    fn remote_target_connection_error_display_redacts_access_key() {
        let err = BucketTargetError::RemoteTargetConnectionErr {
            bucket: "target".to_string(),
            access_key: "sensitive-access-key".to_string(),
            error: "connection refused".to_string(),
        };
        let message = err.to_string();

        assert!(message.contains(REDACTED_CREDENTIAL));
        assert!(!message.contains("sensitive-access-key"));
        assert!(message.contains("connection refused"));
    }

    #[test]
    fn endpoint_health_key_preserves_explicit_port() {
        let url = Url::parse("https://remote.example:9443").expect("url should parse");

        assert_eq!(endpoint_health_key(&url), "remote.example:9443");
    }

    #[test]
    fn update_endpoint_health_counts_offline_transitions() {
        let mut health = EpHealth::default();
        let now = OffsetDateTime::now_utc();

        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);
        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);
        update_endpoint_health(&mut health, true, Duration::from_millis(10), now);
        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);

        assert_eq!(health.offline_count, 2);
        assert_eq!(health.offline_duration, Duration::from_millis(75));
        assert_eq!(health.last_online, Some(now));
    }

    #[tokio::test]
    async fn list_targets_applies_health_stats_for_endpoint_with_port() {
        let sys = BucketTargetSys::default();
        let url = Url::parse("https://remote.example:9443").expect("url should parse");
        sys.init_hc(&url).await;
        sys.mark_offline(&url).await;

        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "remote.example:9443".to_string(),
                arn: "arn:rustfs:replication:us-east-1:bucket:id".to_string(),
                target_type: BucketTargetType::ReplicationService,
                ..Default::default()
            }],
        );

        let targets = sys.list_targets("", "").await;

        assert_eq!(targets.len(), 1);
        assert!(!targets[0].online);
        assert_eq!(targets[0].offline_count, 1);
    }

    #[test]
    fn build_remove_object_headers_includes_internal_version_id_for_replication_delete() {
        let version_id = Uuid::new_v4().to_string();
        let headers = build_remove_object_headers(
            Some(version_id.as_str()),
            &RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: true,
                replication_mtime: None,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        );

        assert_eq!(
            rustfs_utils::http::get_header(&headers, SUFFIX_SOURCE_VERSION_ID).as_deref(),
            Some(version_id.as_str()),
            "replication delete requests must preserve the version id in internal headers"
        );
    }

    #[test]
    fn build_remove_object_headers_omits_delete_marker_flag_for_marker_version_purge() {
        let version_id = Uuid::new_v4().to_string();
        let headers = build_remove_object_headers(
            Some(version_id.as_str()),
            &RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: false,
                replication_mtime: None,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        );

        assert!(
            rustfs_utils::http::get_header(&headers, SUFFIX_SOURCE_DELETEMARKER).is_none(),
            "delete-marker version purges must not masquerade as delete-marker creations"
        );
    }

    fn remove_opts(replication_request: bool, replication_delete_marker: bool) -> RemoveObjectOptions {
        RemoveObjectOptions {
            force_delete: false,
            governance_bypass: false,
            replication_delete_marker,
            replication_mtime: None,
            replication_status: ReplicationStatusType::Replica,
            replication_request,
            replication_validity_check: false,
        }
    }

    #[test]
    fn version_purge_sends_versionid_query_param_to_generic_target() {
        // A replication VERSION PURGE (delete_marker=false) must carry the S3
        // `?versionId=` query param so a generic S3 target removes that exact
        // version instead of silently creating a delete marker (backlog#799 B8).
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(true, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[test]
    fn delete_marker_propagation_omits_versionid_query_param() {
        // Propagating a delete-marker CREATION (delete_marker=true): the target
        // must mint its own marker, so no `versionId` query param is sent.
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid), &remove_opts(true, true));
        assert_eq!(got, None);
    }

    #[test]
    fn non_replication_delete_passes_version_through() {
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(false, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[test]
    fn delete_marker_purge_addresses_exact_version() {
        // Purging a specific delete-marker version on the target
        // (replication_delete_marker_purge_remove_options → delete_marker=false)
        // must target that version, not degenerate to a new marker.
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(true, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[test]
    fn put_object_headers_include_non_empty_source_etag_only() {
        let mut opts = PutObjectOptions::default();

        assert!(
            rustfs_utils::http::get_header(&opts.header(), SUFFIX_SOURCE_ETAG).is_none(),
            "empty source etag must not be sent to replication targets"
        );

        opts.internal.source_etag = "etag-1".to_string();

        assert_eq!(
            rustfs_utils::http::get_header(&opts.header(), SUFFIX_SOURCE_ETAG).as_deref(),
            Some("etag-1"),
            "replication targets need the source etag for idempotency checks"
        );
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_rejects_loopback_endpoint() {
        let sys = BucketTargetSys::default();
        let err = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "127.0.0.1:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect_err("loopback endpoint should be rejected");

        assert!(err.to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_allows_private_ip_endpoint() {
        let sys = BucketTargetSys::default();
        let client = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                skip_tls_verify: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect("private IP endpoints should be allowed for replication targets");

        assert_eq!(client.endpoint, "https://192.168.1.10:9000");
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_allows_custom_ca_pem() {
        let sys = BucketTargetSys::default();
        let cert = generate_simple_self_signed(vec!["192.168.1.10".to_string()]).expect("certificate should generate");
        let client = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                ca_cert_pem: cert.cert.pem(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect("custom CA PEM should build a target client");

        assert_eq!(client.endpoint, "https://192.168.1.10:9000");
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_rejects_invalid_custom_ca_pem() {
        let sys = BucketTargetSys::default();
        let err = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                ca_cert_pem: "not a pem".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect_err("invalid custom CA PEM should be rejected");

        assert!(err.to_string().contains("invalid target CA PEM"));
    }
}
