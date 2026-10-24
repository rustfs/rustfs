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

//! Quota admin handlers for HTTP API

use crate::admin::auth::{validate_admin_request, validate_admin_request_with_bucket};
use crate::admin::handlers::site_replication::site_replication_bucket_meta_hook;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    AppContext, app_context_from_req, current_bucket_metadata_handle, current_notification_system_for_context,
    current_object_store_handle,
};
use crate::admin::storage_api::bucket::metadata_sys::{self, BucketMetadataSys};
use crate::admin::storage_api::bucket::quota::checker::{QuotaChecker, record_reservation_protocol_migration};
use crate::admin::storage_api::bucket::quota::{
    BucketQuota, QUOTA_RESERVATION_PROTOCOL_V2, QuotaError, QuotaOperation, QuotaProtocolMigration,
};
use crate::admin::storage_api::contract::bucket::{BucketOperations as _, BucketOptions};
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::server::ADMIN_PREFIX;
use crate::server::RemoteAddr;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use s3s::{Body, S3Error, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use url::form_urlencoded;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_QUOTA: &str = "quota";
const EVENT_ADMIN_QUOTA_STATE: &str = "admin_quota_state";

async fn notify_quota_metadata_reload(context: Option<Arc<AppContext>>, bucket: String, action: &'static str) {
    let Some(notification_sys) = current_notification_system_for_context(context.as_deref()) else {
        return;
    };
    // The metadata transaction has committed and released its locks. Await
    // healthy peers, but keep the reload running if the request is cancelled.
    // Failed peers converge through the periodic metadata refresh.
    let _ = tokio::spawn(async move {
        if let Err(error) = notification_sys.load_bucket_metadata(&bucket).await {
            warn!(
                event = EVENT_ADMIN_QUOTA_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_QUOTA,
                state = "peer_metadata_reload_failed",
                action,
                bucket = %bucket,
                error = %error,
                "admin quota state"
            );
        }
    })
    .await;
}

#[derive(Debug, Deserialize)]
pub struct SetBucketQuotaRequest {
    pub quota: Option<u64>,
    #[serde(default = "default_quota_type")]
    pub quota_type: String,
    /// Optional durable reservation protocol. Omitted keeps the v1 default.
    #[serde(default)]
    pub reservation_protocol: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct CompatibleBucketQuotaRequest {
    #[serde(default)]
    quota: Option<u64>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default, alias = "quotatype")]
    quota_type: Option<String>,
    #[serde(default)]
    reservation_protocol: Option<u32>,
}

#[derive(Debug, Serialize)]
struct CompatibleBucketQuotaResponse {
    quota: u64,
    size: u64,
    rate: u64,
    requests: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    quotatype: String,
}

fn default_quota_type() -> String {
    rustfs_config::QUOTA_TYPE_HARD.to_string()
}

fn is_compat_set_bucket_quota_path(path: &str) -> bool {
    path.ends_with("/v3/set-bucket-quota")
}

fn is_compat_get_bucket_quota_path(path: &str) -> bool {
    path.ends_with("/v3/get-bucket-quota")
}

fn bucket_from_params_or_query(params: &Params<'_, '_>, uri: &hyper::Uri) -> String {
    if let Some(bucket) = params.get("bucket") {
        return bucket.to_string();
    }

    if let Some(query) = uri.query() {
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            if key == "bucket" {
                return value.into_owned();
            }
        }
    }

    String::new()
}

fn parse_set_bucket_quota_request(body: &[u8]) -> Result<SetBucketQuotaRequest, s3s::S3Error> {
    if body.is_empty() {
        return Ok(SetBucketQuotaRequest {
            quota: None,
            quota_type: default_quota_type(),
            reservation_protocol: None,
        });
    }

    let request: CompatibleBucketQuotaRequest =
        serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;

    Ok(SetBucketQuotaRequest {
        quota: request
            .size
            .filter(|quota| *quota > 0)
            .or_else(|| request.quota.filter(|quota| *quota > 0)),
        quota_type: request.quota_type.unwrap_or_else(default_quota_type),
        reservation_protocol: request.reservation_protocol,
    })
}

fn compat_bucket_quota_response(quota: &BucketQuota) -> CompatibleBucketQuotaResponse {
    let size = quota.quota.unwrap_or(0);

    CompatibleBucketQuotaResponse {
        quota: size,
        size,
        rate: 0,
        requests: 0,
        quotatype: if size > 0 { "hard".to_string() } else { String::new() },
    }
}

fn map_quota_query_error(error: QuotaError, bucket: &str, operation: &str) -> S3Error {
    match error {
        QuotaError::ConfigNotFound { .. } => s3_error!(NoSuchBucket, "bucket not found: {}", bucket),
        QuotaError::UsageUnavailable { .. } => {
            s3_error!(ServiceUnavailable, "authoritative bucket usage is not available yet")
        }
        error => s3_error!(InternalError, "failed to {}: {}", operation, error),
    }
}

#[derive(Debug, Serialize)]
pub struct BucketQuotaResponse {
    pub bucket: String,
    pub quota: Option<u64>,
    /// Current usage size in bytes; `null` when no usage is available,
    /// never reported as zero.
    pub size: Option<u64>,
    pub quota_type: String,
}

#[derive(Debug, Serialize)]
pub struct BucketQuotaStats {
    pub bucket: String,
    pub quota_limit: Option<u64>,
    /// `null` when the bucket is admitted by the reservation ledger and no
    /// scanner usage is available; never reported as zero.
    pub current_usage: Option<u64>,
    pub remaining_quota: Option<u64>,
    pub usage_percentage: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct CheckQuotaRequest {
    pub operation_type: String,
    pub operation_size: u64,
}

#[derive(Debug, Serialize)]
pub struct CheckQuotaResponse {
    pub bucket: String,
    pub operation_type: String,
    pub operation_size: u64,
    pub allowed: bool,
    /// `null` when the bucket is admitted by the reservation ledger and no
    /// scanner usage is available; never reported as zero.
    pub current_usage: Option<u64>,
    pub quota_limit: Option<u64>,
    pub remaining_quota: Option<u64>,
}

/// One bucket of the reservation protocol migration report.
#[derive(Debug, Serialize)]
pub struct BucketQuotaProtocolMigrationEntry {
    pub bucket: String,
    /// `migrated`, `already_durable`, `no_quota`, `skipped` or `failed`.
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation_protocol: Option<u32>,
    /// The skip reason or the failure message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct BucketQuotaProtocolMigrationReport {
    pub target_reservation_protocol: u32,
    pub migrated: usize,
    pub already_durable: usize,
    pub no_quota: usize,
    pub skipped: usize,
    pub failed: usize,
    pub buckets: Vec<BucketQuotaProtocolMigrationEntry>,
}

impl BucketQuotaProtocolMigrationReport {
    fn record(&mut self, bucket: String, outcome: &Result<QuotaProtocolMigration, QuotaError>) {
        let entry = match outcome {
            Ok(QuotaProtocolMigration::Migrated {
                reservation_protocol,
                updated_at,
            }) => {
                self.migrated += 1;
                BucketQuotaProtocolMigrationEntry {
                    bucket,
                    outcome: "migrated",
                    reservation_protocol: Some(*reservation_protocol),
                    reason: None,
                    updated_at: updated_at.format(&Rfc3339).ok(),
                }
            }
            Ok(QuotaProtocolMigration::AlreadyDurable { reservation_protocol }) => {
                self.already_durable += 1;
                BucketQuotaProtocolMigrationEntry {
                    bucket,
                    outcome: "already_durable",
                    reservation_protocol: Some(*reservation_protocol),
                    reason: None,
                    updated_at: None,
                }
            }
            Ok(QuotaProtocolMigration::NoQuota) => {
                self.no_quota += 1;
                BucketQuotaProtocolMigrationEntry {
                    bucket,
                    outcome: "no_quota",
                    reservation_protocol: None,
                    reason: None,
                    updated_at: None,
                }
            }
            Ok(QuotaProtocolMigration::Skipped { reason }) => {
                self.skipped += 1;
                BucketQuotaProtocolMigrationEntry {
                    bucket,
                    outcome: "skipped",
                    reservation_protocol: None,
                    reason: Some(reason.as_str().to_string()),
                    updated_at: None,
                }
            }
            Err(err) => {
                self.failed += 1;
                BucketQuotaProtocolMigrationEntry {
                    bucket,
                    outcome: "failed",
                    reservation_protocol: None,
                    reason: Some(err.to_string()),
                    updated_at: None,
                }
            }
        };
        self.buckets.push(entry);
    }
}

/// Quota management handlers
pub struct SetBucketQuotaHandler;
pub struct GetBucketQuotaHandler;
pub struct ClearBucketQuotaHandler;
pub struct GetBucketQuotaStatsHandler;
pub struct CheckBucketQuotaHandler;
pub struct MigrateBucketQuotaProtocolHandler;

fn bucket_metadata_from_context() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    current_bucket_metadata_handle()
}

async fn current_usage_from_context(bucket: &str) -> Option<u64> {
    let store = current_object_store_handle()?;

    crate::admin::storage_api::data_usage::load_data_usage_from_backend(store)
        .await
        .ok()?
        .buckets_usage
        .get(bucket)
        .map(|bucket_usage| bucket_usage.size)
}

pub fn register_quota_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-bucket-quota").as_str(),
        AdminOperation(&SetBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/get-bucket-quota").as_str(),
        AdminOperation(&GetBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota/{bucket}").as_str(),
        AdminOperation(&SetBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota/{bucket}").as_str(),
        AdminOperation(&GetBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota/{bucket}").as_str(),
        AdminOperation(&ClearBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota-stats/{bucket}").as_str(),
        AdminOperation(&GetBucketQuotaStatsHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota-check/{bucket}").as_str(),
        AdminOperation(&CheckBucketQuotaHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/quota-migration").as_str(),
        AdminOperation(&MigrateBucketQuotaProtocolHandler {}),
    )?;

    Ok(())
}

#[async_trait::async_trait]
impl Operation for SetBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "set_bucket_quota",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetBucketQuotaAdminAction)],
            remote_addr,
        )
        .await?;

        let bucket = bucket_from_params_or_query(&params, &req.uri);
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request = parse_set_bucket_quota_request(&body)?;

        if request.quota_type.to_uppercase() != rustfs_config::QUOTA_TYPE_HARD {
            return Err(s3_error!(InvalidArgument, "{}", rustfs_config::QUOTA_INVALID_TYPE_ERROR_MSG));
        }

        let fleet_proof = if request.quota.is_some() {
            Some(crate::admin::storage_api::acquire_cross_pool_fence_fleet_proof().ok_or_else(|| {
                S3Error::with_message(
                    s3s::S3ErrorCode::ServiceUnavailable,
                    "durable quota capability is not confirmed across the cluster".to_string(),
                )
            })?)
        } else {
            None
        };

        let quota = match request.reservation_protocol {
            None | Some(crate::admin::storage_api::bucket::quota::QUOTA_RESERVATION_PROTOCOL_V1) => {
                BucketQuota::new(request.quota)
            }
            Some(crate::admin::storage_api::bucket::quota::QUOTA_RESERVATION_PROTOCOL_V2) => match request.quota {
                Some(limit) => BucketQuota::new_sharded(limit),
                None => BucketQuota::new(None),
            },
            Some(_) => return Err(s3_error!(InvalidArgument, "unsupported quota reservation protocol")),
        };

        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "{}", rustfs_config::QUOTA_METADATA_SYSTEM_ERROR_MSG))?;
        let mut quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let updated_at = match fleet_proof.as_ref() {
            Some(fleet_proof) => {
                quota_checker
                    .set_durable_quota_config_if_incarnation(&bucket, quota.clone(), expected_incarnation_id, fleet_proof)
                    .await
            }
            None => {
                quota_checker
                    .set_quota_config_if_incarnation(&bucket, quota.clone(), expected_incarnation_id)
                    .await
            }
        }
        .map_err(ApiError::from)?;

        notify_quota_metadata_reload(app_context_from_req(&req), bucket.clone(), "set_bucket_quota").await;

        if let Err(err) = site_replication_bucket_meta_hook(SRBucketMeta {
            bucket: bucket.clone(),
            r#type: "quota-config".to_string(),
            quota: Some(
                serde_json::to_value(&quota).map_err(|e| s3_error!(InternalError, "failed to encode quota payload: {}", e))?,
            ),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                event = EVENT_ADMIN_QUOTA_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_QUOTA,
                action = "set_bucket_quota",
                bucket = %bucket,
                state = "site_replication_hook_failed",
                error = ?err,
                "admin quota state"
            );
        }

        // Get real-time usage from data usage system
        let current_usage = current_usage_from_context(&bucket).await;

        let json = if is_compat_set_bucket_quota_path(req.uri.path()) {
            String::new()
        } else {
            let response = BucketQuotaResponse {
                bucket,
                quota: quota.quota,
                size: current_usage,
                quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
            };

            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?
        };

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "get_bucket_quota",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let bucket = bucket_from_params_or_query(&params, &req.uri);
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }
        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request_with_bucket(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::S3Action(S3Action::GetBucketQuotaAction)],
            remote_addr,
            &bucket,
        )
        .await?;

        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "bucket metadata system is not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        if is_compat_get_bucket_quota_path(req.uri.path()) {
            if !quota_checker.bucket_exists(&bucket).await {
                return Err(s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
            }
            let quota = quota_checker
                .get_quota_config(&bucket)
                .await
                .map_err(|e| s3_error!(InternalError, "failed to get quota: {}", e))?;
            let json = serde_json::to_string(&compat_bucket_quota_response(&quota))
                .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            return Ok(S3Response::new((StatusCode::OK, Body::from(json))));
        }

        let (quota, current_usage) = quota_checker
            .get_quota_stats(&bucket)
            .await
            .map_err(|error| map_quota_query_error(error, &bucket, "get quota"))?;

        let response = BucketQuotaResponse {
            bucket,
            quota: quota.quota,
            size: current_usage,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for ClearBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "clear_bucket_quota",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetBucketQuotaAdminAction)],
            remote_addr,
        )
        .await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;

        info!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "clear_bucket_quota",
            bucket = %bucket,
            state = "clearing",
            "admin quota state"
        );

        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "bucket metadata system is not initialized"))?;

        let mut quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        // Clear quota (set to None)
        let quota = BucketQuota::new(None);
        let updated_at = quota_checker
            .set_quota_config_if_incarnation(&bucket, quota.clone(), expected_incarnation_id)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to clear quota: {}", e))?;

        notify_quota_metadata_reload(app_context_from_req(&req), bucket.clone(), "clear_bucket_quota").await;

        if let Err(err) = site_replication_bucket_meta_hook(SRBucketMeta {
            bucket: bucket.clone(),
            r#type: "quota-config".to_string(),
            quota: Some(
                serde_json::to_value(&quota).map_err(|e| s3_error!(InternalError, "failed to encode quota payload: {}", e))?,
            ),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                event = EVENT_ADMIN_QUOTA_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_QUOTA,
                action = "clear_bucket_quota",
                bucket = %bucket,
                state = "site_replication_hook_failed",
                error = ?err,
                "admin quota state"
            );
        }

        info!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "clear_bucket_quota",
            bucket = %bucket,
            state = "cleared",
            "admin quota state"
        );

        // Get real-time usage from data usage system
        let current_usage = current_usage_from_context(&bucket).await;

        let response = BucketQuotaResponse {
            bucket,
            quota: None,
            size: current_usage,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketQuotaStatsHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "get_bucket_quota_stats",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request_with_bucket(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::S3Action(S3Action::GetBucketQuotaAction)],
            remote_addr,
            &bucket,
        )
        .await?;

        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "bucket metadata system is not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let (quota, current_usage) = quota_checker
            .get_quota_stats(&bucket)
            .await
            .map_err(|error| map_quota_query_error(error, &bucket, "get quota stats"))?;

        let usage_percentage = match (quota.quota, current_usage) {
            (Some(limit), Some(current_usage)) if limit > 0 => Some((current_usage as f64 / limit as f64) * 100.0),
            _ => None,
        };

        let remaining_quota = current_usage.and_then(|current_usage| quota.get_remaining_quota(current_usage));

        let response = BucketQuotaStats {
            bucket,
            quota_limit: quota.quota,
            current_usage,
            remaining_quota,
            usage_percentage,
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[async_trait::async_trait]
impl Operation for CheckBucketQuotaHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "check_bucket_quota",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let bucket = params.get("bucket").unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket name is required"));
        }

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request_with_bucket(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::S3Action(S3Action::GetBucketQuotaAction)],
            remote_addr,
            &bucket,
        )
        .await?;

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CheckQuotaRequest = if body.is_empty() {
            return Err(s3_error!(InvalidRequest, "request body cannot be empty"));
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        debug!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "check_bucket_quota",
            bucket = %bucket,
            operation = %request.operation_type,
            operation_size = request.operation_size,
            state = "checking",
            "admin quota state"
        );

        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "bucket metadata system is not initialized"))?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock.clone());

        let operation: QuotaOperation = match request.operation_type.to_uppercase().as_str() {
            "PUT" | "PUTOBJECT" => QuotaOperation::PutObject,
            "POST" | "POSTOBJECT" => QuotaOperation::PostObject,
            "COPY" | "COPYOBJECT" => QuotaOperation::CopyObject,
            "DELETE" | "DELETEOBJECT" => QuotaOperation::DeleteObject,
            _ => QuotaOperation::PutObject, // Default to PUT operation
        };

        let result = quota_checker
            .check_quota_with_usage_reporting(&bucket, operation, request.operation_size, true)
            .await
            .map_err(|error| map_quota_query_error(error, &bucket, "check quota"))?;

        let response = CheckQuotaResponse {
            bucket,
            operation_type: request.operation_type,
            operation_size: request.operation_size,
            allowed: result.allowed,
            current_usage: result.current_usage,
            quota_limit: result.quota_limit,
            remaining_quota: result.remaining,
        };

        let json =
            serde_json::to_string(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

/// `POST /rustfs/admin/v3/quota-migration`: re-save every quota that predates
/// the reservation protocols under the sharded durable protocol and report
/// each bucket (rustfs/backlog#2904). The limit and type are unchanged; a
/// bucket that fails is reported, not retried, and never stops the sweep.
#[async_trait::async_trait]
impl Operation for MigrateBucketQuotaProtocolHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "migrate_bucket_quota_protocol",
            state = "requested",
            "admin quota state"
        );

        let Some(ref cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetBucketQuotaAdminAction)],
            remote_addr,
        )
        .await?;

        let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
        let metadata_sys_lock = bucket_metadata_from_context()
            .ok_or_else(|| s3_error!(InternalError, "{}", rustfs_config::QUOTA_METADATA_SYSTEM_ERROR_MSG))?;
        let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

        let quota_checker = QuotaChecker::new(metadata_sys_lock);
        let mut report = BucketQuotaProtocolMigrationReport {
            target_reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2,
            ..Default::default()
        };
        for bucket in buckets {
            let outcome = quota_checker.migrate_reservation_protocol(&bucket.name).await;
            record_reservation_protocol_migration(&bucket.name, "admin", &outcome);
            report.record(bucket.name, &outcome);
        }

        info!(
            event = EVENT_ADMIN_QUOTA_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            action = "migrate_bucket_quota_protocol",
            state = "completed",
            migrated = report.migrated,
            already_durable = report.already_durable,
            no_quota = report.no_quota,
            skipped = report.skipped,
            failed = report.failed,
            "admin quota state"
        );

        let json = serde_json::to_string(&report).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(json))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_quota_type() {
        assert_eq!(default_quota_type(), "HARD");
    }

    #[test]
    fn parse_set_bucket_quota_request_accepts_compat_shape() {
        let request = parse_set_bucket_quota_request(br#"{"quota":1073741824,"size":1073741824,"quotatype":"hard"}"#)
            .expect("parse quota request");

        assert_eq!(request.quota, Some(1073741824));
        assert_eq!(request.quota_type, "hard");
        assert_eq!(request.reservation_protocol, None);
    }

    #[test]
    fn parse_set_bucket_quota_request_prefers_non_zero_quota_over_zero_size() {
        let request =
            parse_set_bucket_quota_request(br#"{"quota":1073741824,"size":0,"quotatype":"hard"}"#).expect("parse quota request");

        assert_eq!(request.quota, Some(1073741824));
        assert_eq!(request.quota_type, "hard");
    }

    #[test]
    fn parse_set_bucket_quota_request_accepts_sharded_protocol() {
        let request = parse_set_bucket_quota_request(br#"{"quota":1073741824,"reservation_protocol":2,"quota_type":"hard"}"#)
            .expect("parse sharded quota request");

        assert_eq!(request.quota, Some(1073741824));
        assert_eq!(request.reservation_protocol, Some(2));
    }

    #[test]
    fn compat_bucket_quota_response_uses_external_field_names() {
        let quota = BucketQuota::new(Some(1024));
        let json = serde_json::to_string(&compat_bucket_quota_response(&quota)).expect("serialize");

        assert!(json.contains("\"quota\":1024"));
        assert!(json.contains("\"size\":1024"));
        assert!(json.contains("\"quotatype\":\"hard\""));
    }

    #[test]
    fn unknown_authoritative_usage_is_retryable() {
        let error = map_quota_query_error(
            QuotaError::UsageUnavailable {
                bucket: "test-bucket".to_string(),
            },
            "test-bucket",
            "check quota",
        );

        assert_eq!(error.code(), &s3s::S3ErrorCode::ServiceUnavailable);
    }

    /// rustfs/backlog#2904: unknown usage on a ledger-admitted bucket is
    /// reported as `null`, never as zero.
    #[test]
    fn quota_stats_report_unknown_usage_as_null() {
        let stats = BucketQuotaStats {
            bucket: "test-bucket".to_string(),
            quota_limit: Some(2048),
            current_usage: None,
            remaining_quota: None,
            usage_percentage: None,
        };
        let value = serde_json::to_value(&stats).expect("stats should serialize");
        assert!(value["current_usage"].is_null());
        assert!(value["remaining_quota"].is_null());

        let check = CheckQuotaResponse {
            bucket: "test-bucket".to_string(),
            operation_type: "PUT".to_string(),
            operation_size: 512,
            allowed: true,
            current_usage: None,
            quota_limit: Some(2048),
            remaining_quota: None,
        };
        let value = serde_json::to_value(&check).expect("check response should serialize");
        assert!(value["current_usage"].is_null());
        assert_eq!(value["allowed"], serde_json::Value::Bool(true));
    }

    #[test]
    fn quota_protocol_migration_report_counts_every_outcome() {
        use crate::admin::storage_api::bucket::quota::QuotaMigrationSkipReason;

        let mut report = BucketQuotaProtocolMigrationReport {
            target_reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2,
            ..Default::default()
        };
        report.record(
            "a".to_string(),
            &Ok(QuotaProtocolMigration::Migrated {
                reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2,
                updated_at: time::OffsetDateTime::UNIX_EPOCH,
            }),
        );
        report.record("b".to_string(), &Ok(QuotaProtocolMigration::AlreadyDurable { reservation_protocol: 1 }));
        report.record("c".to_string(), &Ok(QuotaProtocolMigration::NoQuota));
        report.record(
            "d".to_string(),
            &Ok(QuotaProtocolMigration::Skipped {
                reason: QuotaMigrationSkipReason::QuotaRevisionChanged,
            }),
        );
        report.record("e".to_string(), &Err(QuotaError::UsageUnavailable { bucket: "e".to_string() }));

        assert_eq!(
            (report.migrated, report.already_durable, report.no_quota, report.skipped, report.failed),
            (1, 1, 1, 1, 1)
        );
        let value = serde_json::to_value(&report).expect("report should serialize");
        assert_eq!(value["target_reservation_protocol"], 2);
        assert_eq!(value["buckets"][0]["outcome"], "migrated");
        assert_eq!(value["buckets"][0]["reservation_protocol"], 2);
        assert_eq!(value["buckets"][0]["updated_at"], "1970-01-01T00:00:00Z");
        assert_eq!(value["buckets"][1]["reservation_protocol"], 1);
        assert!(value["buckets"][2].get("reason").is_none());
        assert_eq!(value["buckets"][3]["reason"], "quota_revision_changed");
        assert_eq!(value["buckets"][4]["outcome"], "failed");
        assert!(
            value["buckets"][4]["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("e"))
        );
    }

    #[test]
    fn test_quota_operation_parsing() {
        let parse_operation = |operation: &str| match operation.to_uppercase().as_str() {
            "PUT" | "PUTOBJECT" => QuotaOperation::PutObject,
            "POST" | "POSTOBJECT" => QuotaOperation::PostObject,
            "COPY" | "COPYOBJECT" => QuotaOperation::CopyObject,
            "DELETE" | "DELETEOBJECT" => QuotaOperation::DeleteObject,
            _ => QuotaOperation::PutObject,
        };

        assert!(matches!(parse_operation("put"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("PUT"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("PutObject"), QuotaOperation::PutObject));
        assert!(matches!(parse_operation("post"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("POST"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("PostObject"), QuotaOperation::PostObject));
        assert!(matches!(parse_operation("copy"), QuotaOperation::CopyObject));
        assert!(matches!(parse_operation("DELETE"), QuotaOperation::DeleteObject));
        assert!(matches!(parse_operation("unknown"), QuotaOperation::PutObject));
    }

    #[tokio::test]
    async fn test_quota_response_serialization() {
        let response = BucketQuotaResponse {
            bucket: "test-bucket".to_string(),
            quota: Some(2147483648),
            size: Some(1073741824),
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("test-bucket"));
        assert!(json.contains("2147483648"));
        assert!(json.contains("HARD"));
    }

    #[test]
    fn quota_response_reports_unknown_usage_as_null() {
        let response = BucketQuotaResponse {
            bucket: "test-bucket".to_string(),
            quota: Some(2147483648),
            size: None,
            quota_type: rustfs_config::QUOTA_TYPE_HARD.to_string(),
        };

        let json: serde_json::Value = serde_json::to_value(&response).unwrap();
        assert!(json["size"].is_null(), "unknown usage must not be reported as zero: {json}");
    }
}
