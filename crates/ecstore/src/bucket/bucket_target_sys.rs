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
use crate::bucket::replication::ObjectOpts;
use crate::bucket::replication::ReplicationConfigurationExt;
use crate::bucket::target::ARN;
use crate::bucket::target::BucketTargetType;
use crate::bucket::target::{self, BucketTarget, BucketTargets, Credentials};
use crate::bucket::versioning_sys::BucketVersioningSys;
use aws_credential_types::Credentials as SdkCredentials;
use aws_sdk_s3::config::Region as SdkRegion;
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
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use reqwest::Client as HttpClient;
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_STORAGE_CLASS, AMZ_WEBSITE_REDIRECT_LOCATION, RUSTFS_BUCKET_REPLICATION_CHECK,
    RUSTFS_BUCKET_REPLICATION_DELETE_MARKER, RUSTFS_BUCKET_REPLICATION_REQUEST, RUSTFS_BUCKET_SOURCE_ETAG,
    RUSTFS_BUCKET_SOURCE_MTIME, RUSTFS_BUCKET_SOURCE_VERSION_ID, RUSTFS_FORCE_DELETE, is_amz_header, is_minio_header,
    is_rustfs_header, is_standard_header, is_storageclass_header,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::error;
use tracing::warn;
use url::Url;
use uuid::Uuid;

const DEFAULT_HEALTH_CHECK_DURATION: Duration = Duration::from_secs(5);
const DEFAULT_HEALTH_CHECK_RELOAD_DURATION: Duration = Duration::from_secs(30 * 60);

pub static GLOBAL_BUCKET_TARGET_SYS: OnceLock<BucketTargetSys> = OnceLock::new();

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
            latency: LatencyStat::new(),
        }
    }
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
        {
            let health_map = self.h_mutex.read().await;
            if let Some(health) = health_map.get(url.host_str().unwrap_or("")) {
                return !health.online;
            }
        }
        // Initialize health check if not exists
        self.init_hc(url).await;
        false
    }

    pub async fn mark_offline(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        if let Some(health) = health_map.get_mut(url.host_str().unwrap_or("")) {
            health.online = false;
        }
    }

    pub async fn init_hc(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        let host = url.host_str().unwrap_or("").to_string();
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
                health_map.keys().cloned().collect::<Vec<_>>()
            };

            for endpoint in endpoints {
                // Perform health check
                let start = Instant::now();
                let online = self.check_endpoint_health(&endpoint).await;
                let duration = start.elapsed();

                {
                    let mut health_map = self.h_mutex.write().await;
                    if let Some(health) = health_map.get_mut(&endpoint) {
                        let prev_online = health.online;
                        health.online = online;
                        health.last_hc_at = Some(OffsetDateTime::now_utc());
                        health.latency.update(duration);

                        if online {
                            health.last_online = Some(OffsetDateTime::now_utc());
                        } else if prev_online {
                            // Just went offline
                            health.offline_duration += duration;
                        }
                    }
                }
            }
        }
    }

    async fn check_endpoint_health(&self, _endpoint: &str) -> bool {
        true
        // TODO: Health check

        // // Simple health check implementation
        // // In a real implementation, you would make actual HTTP requests
        // match self
        //     .hc_client
        //     .get(format!("https://{}/rustfs/health/ready", endpoint))
        //     .timeout(Duration::from_secs(3))
        //     .send()
        //     .await
        // {
        //     Ok(response) => response.status().is_success(),
        //     Err(_) => false,
        // }
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

    pub async fn set_target(&self, bucket: &str, target: &BucketTarget, update: bool) -> Result<(), BucketTargetError> {
        if !target.target_type.is_valid() && !update {
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
                .map_err(|_e| BucketTargetError::BucketReplicationSourceNotVersioned {
                    bucket: bucket.to_string(),
                })?;

            if versioning.is_none() {
                return Err(BucketTargetError::BucketReplicationSourceNotVersioned {
                    bucket: bucket.to_string(),
                });
            }
        }

        {
            let mut targets_map = self.targets_map.write().await;
            let bucket_targets = targets_map.entry(bucket.to_string()).or_insert_with(Vec::new);
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
        }

        {
            let mut arn_remotes_map = self.arn_remotes_map.write().await;
            arn_remotes_map.insert(
                target.arn.clone(),
                ArnTarget {
                    client: Some(Arc::new(target_client)),
                    last_refresh: OffsetDateTime::now_utc(),
                },
            );
        }

        self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
        Ok(())
    }

    pub async fn remove_target(&self, bucket: &str, arn_str: &str) -> Result<(), BucketTargetError> {
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
        {
            for rule in config.filter_target_arns(&ObjectOpts {
                op_type: ReplicationType::All,
                ..Default::default()
            }) {
                if rule == arn_str || config.role == arn_str {
                    let arn_remotes_map = self.arn_remotes_map.read().await;
                    if arn_remotes_map.get(arn_str).is_some() {
                        return Err(BucketTargetError::BucketRemoteRemoveDisallowed {
                            bucket: bucket.to_string(),
                        });
                    }
                }
            }
        }

        {
            let mut targets_map = self.targets_map.write().await;

            let Some(targets) = targets_map.get(bucket) else {
                return Err(BucketTargetError::BucketRemoteTargetNotFound {
                    bucket: bucket.to_string(),
                });
            };

            let new_targets: Vec<BucketTarget> = targets.iter().filter(|t| t.arn != arn_str).cloned().collect();

            if new_targets.len() == targets.len() {
                return Err(BucketTargetError::BucketRemoteTargetNotFound {
                    bucket: bucket.to_string(),
                });
            }

            targets_map.insert(bucket.to_string(), new_targets);
        }

        {
            self.arn_remotes_map.write().await.remove(arn_str);
        }

        self.update_bandwidth_limit(bucket, arn_str, 0);

        Ok(())
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
            return Some(cli.clone());
        }

        // TODO: spawn a task to reload the target
        if self.is_reloading_target(bucket, arn).await {
            return None;
        }

        if let Some(last_refresh) = last_refresh {
            let now = OffsetDateTime::now_utc();
            if now - last_refresh > Duration::from_secs(60 * 5) {
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

        let config = S3Config::builder()
            .endpoint_url(endpoint.clone())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(SdkRegion::new(target.region.clone()))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .build();

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

    fn update_bandwidth_limit(&self, _bucket: &str, _arn: &str, _limit: i64) {
        // Implementation for bandwidth limit update
        // This would interact with the global bucket monitor
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
                            .map(|c| c.access_key == target.credentials.as_ref().unwrap_or(&Credentials::default()).access_key)
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

    pub fn header(&self) -> HeaderMap {
        let mut header = HeaderMap::new();

        let mut content_type = self.content_type.clone();
        if content_type.is_empty() {
            content_type = "application/octet-stream".to_string();
        }
        header.insert("Content-Type", HeaderValue::from_str(&content_type).expect("err"));

        if !self.content_encoding.is_empty() {
            header.insert("Content-Encoding", HeaderValue::from_str(&self.content_encoding).expect("err"));
        }
        if !self.content_disposition.is_empty() {
            header.insert("Content-Disposition", HeaderValue::from_str(&self.content_disposition).expect("err"));
        }
        if !self.content_language.is_empty() {
            header.insert("Content-Language", HeaderValue::from_str(&self.content_language).expect("err"));
        }
        if !self.cache_control.is_empty() {
            header.insert("Cache-Control", HeaderValue::from_str(&self.cache_control).expect("err"));
        }

        if self.expires.unix_timestamp() != 0 {
            header.insert("Expires", HeaderValue::from_str(&self.expires.format(&Rfc3339).unwrap()).expect("err")); //rustfs invalid header
        }

        if let Some(mode) = &self.mode {
            header.insert(AMZ_OBJECT_LOCK_MODE, HeaderValue::from_str(mode.as_str()).expect("err"));
        }

        if self.retain_until_date.unix_timestamp() != 0 {
            header.insert(
                AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
                HeaderValue::from_str(&self.retain_until_date.format(&Rfc3339).unwrap()).expect("err"),
            );
        }

        if let Some(legalhold) = &self.legalhold {
            header.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD, HeaderValue::from_str(legalhold.as_str()).expect("err"));
        }

        if !self.storage_class.is_empty() {
            header.insert(AMZ_STORAGE_CLASS, HeaderValue::from_str(&self.storage_class).expect("err"));
        }

        if !self.website_redirect_location.is_empty() {
            header.insert(
                AMZ_WEBSITE_REDIRECT_LOCATION,
                HeaderValue::from_str(&self.website_redirect_location).expect("err"),
            );
        }

        if !self.internal.replication_status.as_str().is_empty() {
            header.insert(
                AMZ_BUCKET_REPLICATION_STATUS,
                HeaderValue::from_str(self.internal.replication_status.as_str()).expect("err"),
            );
        }

        for (k, v) in &self.user_metadata {
            if is_amz_header(k) || is_standard_header(k) || is_storageclass_header(k) || is_rustfs_header(k) || is_minio_header(k)
            {
                if let Ok(header_name) = HeaderName::from_bytes(k.as_bytes()) {
                    header.insert(header_name, HeaderValue::from_str(v).unwrap());
                }
            } else if let Ok(header_name) = HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()) {
                header.insert(header_name, HeaderValue::from_str(v).unwrap());
            }
        }

        for (k, v) in self.custom_header.iter() {
            header.insert(k.clone(), v.clone());
        }

        if !self.internal.source_version_id.is_empty() {
            header.insert(
                RUSTFS_BUCKET_SOURCE_VERSION_ID,
                HeaderValue::from_str(&self.internal.source_version_id).expect("err"),
            );
        }
        if self.internal.source_etag.is_empty() {
            header.insert(RUSTFS_BUCKET_SOURCE_ETAG, HeaderValue::from_str(&self.internal.source_etag).expect("err"));
        }
        if self.internal.source_mtime.unix_timestamp() != 0 {
            header.insert(
                RUSTFS_BUCKET_SOURCE_MTIME,
                HeaderValue::from_str(&self.internal.source_mtime.unix_timestamp().to_string()).expect("err"),
            );
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
        S3ClientError {
            error: value.into(),
            status_code: None,
            code: None,
            message: None,
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
        S3ClientError {
            error: format!(
                "{}: {}",
                value.code().map(String::from).unwrap_or("unknown code".into()),
                value.message().map(String::from).unwrap_or("missing reason".into()),
            ),
            status_code: None,
            code: None,
            message: None,
        }
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
        let scheme = if self.secure { "https" } else { "http" };
        Url::parse(&format!("{scheme}://{}", self.endpoint)).unwrap()
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, S3ClientError> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                SdkError::ServiceError(oe) => match oe.into_err() {
                    HeadBucketError::NotFound(_) => Ok(false),
                    other => Err(S3ClientError::new(format!(
                        "failed to check bucket exists for bucket:{bucket} please check the bucket name and credentials, error:{other:?}"
                    ))),
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
        let headers = opts.header();

        let builder = self.client.put_object();

        match builder
            .bucket(bucket)
            .key(object)
            .content_length(size)
            .body(body)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    let key_str = k.unwrap().as_str().to_string();
                    let value_str = v.to_str().unwrap_or("").to_string();
                    req.headers_mut().insert(key_str, value_str);
                }

                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        _opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        match self.client.create_multipart_upload().bucket(bucket).key(object).send().await {
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
                    let key_str = k.unwrap().as_str().to_string();
                    let value_str = v.to_str().unwrap_or("").to_string();
                    req.headers_mut().insert(key_str, value_str);
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
                    let key_str = k.unwrap().as_str().to_string();
                    let value_str = v.to_str().unwrap_or("").to_string();
                    req.headers_mut().insert(key_str, value_str);
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
        let mut headers = HeaderMap::new();
        if opts.force_delete {
            headers.insert(RUSTFS_FORCE_DELETE, "true".parse().unwrap());
        }
        if opts.governance_bypass {
            headers.insert(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, "true".parse().unwrap());
        }

        if opts.replication_delete_marker {
            headers.insert(RUSTFS_BUCKET_REPLICATION_DELETE_MARKER, "true".parse().unwrap());
        }

        if let Some(t) = opts.replication_mtime {
            headers.insert(
                RUSTFS_BUCKET_SOURCE_MTIME,
                t.format(&Rfc3339).unwrap_or_default().as_str().parse().unwrap(),
            );
        }

        if !opts.replication_status.is_empty() {
            headers.insert(AMZ_BUCKET_REPLICATION_STATUS, opts.replication_status.as_str().parse().unwrap());
        }

        if opts.replication_request {
            headers.insert(RUSTFS_BUCKET_REPLICATION_REQUEST, "true".parse().unwrap());
        }
        if opts.replication_validity_check {
            headers.insert(RUSTFS_BUCKET_REPLICATION_CHECK, "true".parse().unwrap());
        }

        match self
            .client
            .delete_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(version_id)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    let key_str = k.unwrap().as_str().to_string();
                    let value_str = v.to_str().unwrap_or("").to_string();
                    req.headers_mut().insert(key_str, value_str);
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(_res) => Ok(()),
            Err(e) => Err(e.into()),
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
                access_key,
                error,
            } => {
                write!(f, "Connection error for bucket: {bucket}, access key: {access_key}, error: {error}")
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
