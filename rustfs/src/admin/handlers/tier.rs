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
#![allow(unused_variables, unused_mut, unused_must_use)]

use crate::admin::runtime_sources::object_store_from_extensions;
use crate::admin::storage_api::runtime_sources::TierConfigMgr;
use crate::admin::storage_api::tier::{
    AdminError, ClusterTierDailyStats, DailyAllTierStats, ECStore, ERR_TIER_ALREADY_EXISTS, ERR_TIER_BACKEND_IN_USE,
    ERR_TIER_BACKEND_NOT_EMPTY, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CONFIG, ERR_TIER_INVALID_CREDENTIALS,
    ERR_TIER_MISSING_CREDENTIALS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND, ERR_TIER_RESERVED_NAME, TierConfig,
    TierConfigUpdateError, TierCreds, TierType,
};
use crate::{
    admin::runtime_sources::{current_daily_tier_stats, current_notification_system, current_tier_config_handle},
    admin::{
        auth::authorize_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    server::ADMIN_PREFIX,
};
use http::{HeaderMap, StatusCode, Uri};
use hyper::Method;
use matchit::Params;
use percent_encoding::percent_decode_str;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_data_usage::TierStats;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde_urlencoded::from_bytes;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_TIER: &str = "tier";
const EVENT_ADMIN_TIER_STATE: &str = "admin_tier_state";

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct AddTierQuery {
    #[serde(rename = "accessKey")]
    pub access_key: Option<String>,
    pub status: Option<String>,
    #[serde(rename = "secretKey")]
    pub secret_key: Option<String>,
    #[serde(rename = "serviceName")]
    pub service_name: Option<String>,
    #[serde(rename = "sessionToken")]
    pub session_token: Option<String>,
    pub tier: Option<String>,
    #[serde(rename = "tierName")]
    pub tier_name: Option<String>,
    #[serde(rename = "tierType")]
    pub tier_type: Option<String>,
    pub force: Option<String>,
}

pub struct AddTier {}

fn wasabi_payload_name(config: &TierConfig) -> S3Result<String> {
    config
        .wasabi
        .as_ref()
        .map(|wasabi| wasabi.name.clone())
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing Wasabi configuration"))
}

fn spawn_transition_tier_config_propagation(action: &'static str) {
    if let Some(notification_sys) = current_notification_system() {
        debug!(
            event = EVENT_ADMIN_TIER_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_TIER,
            action,
            result = "propagation_started",
            "admin tier state"
        );
        notification_sys.spawn_transition_tier_config_reload_workers();
    }
}

fn tier_mutation_error(
    update_error: TierConfigUpdateError,
    action: &'static str,
    failure_code: &'static str,
) -> Result<AdminError, S3Error> {
    match update_error {
        TierConfigUpdateError::Load(err) => {
            warn!(
                event = EVENT_ADMIN_TIER_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_TIER,
                action,
                result = "reload_failed",
                error = ?err,
                "admin tier state"
            );
            Err(S3Error::with_message(
                S3ErrorCode::Custom(failure_code.into()),
                format!("tier reload failed. {err}"),
            ))
        }
        TierConfigUpdateError::Save(err) => {
            warn!(
                event = EVENT_ADMIN_TIER_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_TIER,
                action,
                result = "save_failed",
                error = ?err,
                "admin tier state"
            );
            Err(S3Error::with_message(S3ErrorCode::Custom(failure_code.into()), "tier save failed"))
        }
        TierConfigUpdateError::Mutation(err) | TierConfigUpdateError::Publish(err) => Ok(err),
    }
}

fn tier_backend_error_response(err: &AdminError) -> Option<S3Error> {
    let (code, message, status_code) = if err.code == ERR_TIER_BACKEND_IN_USE.code {
        (
            "XMinioAdminTierBackendInUse",
            "Specified remote tier is already in use",
            StatusCode::CONFLICT,
        )
    } else if err.code == ERR_TIER_BACKEND_NOT_EMPTY.code {
        (
            "XMinioAdminTierBackendNotEmpty",
            "Specified remote backend is not empty",
            StatusCode::BAD_REQUEST,
        )
    } else {
        return None;
    };

    let mut response = S3Error::with_message(S3ErrorCode::Custom(code.into()), message);
    response.set_status_code(status_code);
    Some(response)
}

fn clear_tier_error_response(err: &AdminError) -> S3Error {
    S3Error::with_message(S3ErrorCode::Custom("TierClearFailed".into()), format!("tier clear failed. {err}"))
}

fn resolve_tier_name(uri: &Uri, params: &Params<'_, '_>) -> S3Result<String> {
    if let Some(tier) = params.get("tier") {
        let decoded = percent_decode_str(tier)
            .decode_utf8()
            .map_err(|_| s3_error!(InvalidArgument, "invalid tier path parameter"))?;
        let trimmed = decoded.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    let query = if let Some(query) = uri.query() {
        let input: AddTierQuery = from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
        input
    } else {
        AddTierQuery::default()
    };

    Ok(require_tier_name(query.tier.as_deref())?.to_string())
}

pub fn register_tier_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(&ListTiers {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier-stats").as_str(),
        AdminOperation(&GetTierInfo {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tier}").as_str(),
        AdminOperation(&VerifyTier {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(&RemoveTier {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier").as_str(),
        AdminOperation(&AddTier {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/{tiername}").as_str(),
        AdminOperation(&EditTier {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/tier/clear").as_str(),
        AdminOperation(&ClearTier {}),
    )?;

    Ok(())
}

#[async_trait::async_trait]
impl Operation for AddTier {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::SetTierAction)]).await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "add_tier",
                    result = "body_read_failed",
                    error = ?e,
                    "admin tier state"
                );
                return Err(s3_error!(InvalidRequest, "tier configuration body too large or failed to read"));
            }
        };

        let mut args: TierConfig = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid JSON: {e}")))?;

        match args.tier_type {
            TierType::S3 => {
                args.name = args
                    .s3
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing S3 configuration"))?
                    .name;
            }
            TierType::Wasabi => {
                args.name = wasabi_payload_name(&args)?;
            }
            TierType::RustFS => {
                args.name = args
                    .rustfs
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing RustFS configuration"))?
                    .name;
            }
            TierType::MinIO => {
                args.name = args
                    .minio
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing MinIO configuration"))?
                    .name;
            }
            TierType::Aliyun => {
                args.name = args
                    .aliyun
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing Aliyun configuration"))?
                    .name;
            }
            TierType::Tencent => {
                args.name = args
                    .tencent
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing Tencent configuration"))?
                    .name;
            }
            TierType::Huaweicloud => {
                args.name = args
                    .huaweicloud
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing Huawei Cloud configuration"))?
                    .name;
            }
            TierType::Azure => {
                args.name = args
                    .azure
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing Azure configuration"))?
                    .name;
            }
            TierType::GCS => {
                args.name = args
                    .gcs
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing GCS configuration"))?
                    .name;
            }
            TierType::R2 => {
                args.name = args
                    .r2
                    .clone()
                    .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "missing R2 configuration"))?
                    .name;
            }
            _ => (),
        }
        debug!(
            event = EVENT_ADMIN_TIER_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_TIER,
            action = "add_tier",
            tier_name = %args.name,
            tier_type = ?args.tier_type,
            state = "decoded",
            "admin tier state"
        );
        let tier_name_for_log = args.name.clone();

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "add_tier",
                    result = "force_parse_failed",
                    error = ?e,
                    "admin tier state"
                );
                s3_error!(InvalidRequest, "invalid force flag")
            })?;
        }
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let tier_config_mgr_handle = current_tier_config_handle();
        if let Err(update_err) = TierConfigMgr::add_and_save(&tier_config_mgr_handle, store, args, force).await {
            let err = tier_mutation_error(update_err, "add_tier", "TierAddFailed")?;
            return if err.code == ERR_TIER_RESERVED_NAME.code {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "add_tier",
                    tier_name = %tier_name_for_log,
                    result = "reserved_name_rejected",
                    "admin tier state"
                );
                Err(s3_error!(InvalidRequest, "Cannot use reserved tier name"))
            } else if err.code == ERR_TIER_ALREADY_EXISTS.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameAlreadyExist".into()),
                    "tier name already exists",
                ))
            } else if err.code == ERR_TIER_NAME_NOT_UPPERCASE.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameNotUppercase".into()),
                    "tier name must be uppercase",
                ))
            } else if let Some(response) = tier_backend_error_response(&err) {
                Err(response)
            } else if err.code == ERR_TIER_CONNECT_ERR.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierConnectError".into()),
                    "tier connectivity check failed",
                ))
            } else if err.code == ERR_TIER_INVALID_CONFIG.code {
                Err(S3Error::with_message(S3ErrorCode::InvalidArgument, err.message))
            } else if err.code == ERR_TIER_INVALID_CREDENTIALS.code {
                Err(S3Error::with_message(S3ErrorCode::Custom(err.code.clone().into()), err.message))
            } else {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "add_tier",
                    tier_name = %tier_name_for_log,
                    result = "add_failed",
                    error = ?err,
                    "admin tier state"
                );
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierAddFailed".into()),
                    format!("tier add failed. {err}"),
                ))
            };
        }
        spawn_transition_tier_config_propagation("add");

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct EditTier {}
#[async_trait::async_trait]
impl Operation for EditTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::SetTierAction)]).await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "edit_tier",
                    result = "body_read_failed",
                    error = ?e,
                    "admin tier state"
                );
                return Err(s3_error!(InvalidRequest, "tier configuration body too large or failed to read"));
            }
        };

        let creds: TierCreds = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid JSON: {e}")))?;

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        debug!(
            event = EVENT_ADMIN_TIER_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_TIER,
            action = "edit_tier",
            tier_name = %tier_name,
            state = "decoded",
            "admin tier state"
        );

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let tier_config_mgr_handle = current_tier_config_handle();
        if let Err(update_err) = TierConfigMgr::edit_and_save(&tier_config_mgr_handle, store, &tier_name, creds).await {
            let err = tier_mutation_error(update_err, "edit_tier", "TierEditFailed")?;
            return if err.code == ERR_TIER_NOT_FOUND.code {
                Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found"))
            } else if err.code == ERR_TIER_MISSING_CREDENTIALS.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierMissingCredentials".into()),
                    "tier credentials are required",
                ))
            } else {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "edit_tier",
                    tier_name = %tier_name,
                    result = "edit_failed",
                    error = ?err,
                    "admin tier state"
                );
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierEditFailed".into()),
                    format!("tier edit failed. {err}"),
                ))
            };
        }
        spawn_transition_tier_config_propagation("edit");

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct BucketQuery {
    #[serde(rename = "bucket")]
    pub bucket: String,
}
pub struct ListTiers {}
#[async_trait::async_trait]
impl Operation for ListTiers {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: BucketQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                BucketQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::ListTierAction)]).await?;

        let tier_config_mgr_handle = current_tier_config_handle();
        let tier_config_mgr = tier_config_mgr_handle.read().await;
        let tiers = tier_config_mgr.list_tiers();

        let data = serde_json::to_vec(&tiers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tiers err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct RemoveTier {}
#[async_trait::async_trait]
impl Operation for RemoveTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::SetTierAction)]).await?;

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "remove_tier",
                    result = "force_parse_failed",
                    error = ?e,
                    "admin tier state"
                );
                s3_error!(InvalidRequest, "invalid force flag")
            })?;
        }

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let tier_config_mgr_handle = current_tier_config_handle();
        if let Err(update_err) = TierConfigMgr::remove_and_save(&tier_config_mgr_handle, store, &tier_name, force).await {
            let err = tier_mutation_error(update_err, "remove_tier", "TierRemoveFailed")?;
            return if err.code == ERR_TIER_NOT_FOUND.code {
                Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found"))
            } else if let Some(response) = tier_backend_error_response(&err) {
                Err(response)
            } else {
                warn!(
                    event = EVENT_ADMIN_TIER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_TIER,
                    action = "remove_tier",
                    tier_name = %tier_name,
                    result = "remove_failed",
                    error = ?err,
                    "admin tier state"
                );
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierRemoveFailed".into()),
                    format!("tier remove failed. {err}"),
                ))
            };
        }
        spawn_transition_tier_config_propagation("remove");

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct VerifyTier {}
#[async_trait::async_trait]
impl Operation for VerifyTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::ListTierAction)]).await?;

        let tier = resolve_tier_name(&req.uri, &params)?;
        let tier_config_mgr_handle = current_tier_config_handle();
        TierConfigMgr::verify_without_manager_lock(&tier_config_mgr_handle, &tier)
            .await
            .map_err(map_tier_verify_error)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/// Version of the `GET /v3/tier-stats` response body.
///
/// Version 1 was a bare `{"<TIER>": {"total_size": ...}}` map holding the
/// answering process's rolling 24-hour transition counters, with nothing in
/// the body saying that it was neither a cluster total nor an inventory.
/// Version 2 separates the two quantities and carries the reporting coverage
/// behind each. Callers pinned to version 1 request it with `?format=legacy`.
const TIER_STATS_CONTRACT_VERSION: u32 = 2;

const TIER_STATS_FORMAT_LEGACY: &str = "legacy";

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct TierStatsQuery {
    pub tier: Option<String>,
    /// `legacy` returns the version 1 body. Any other value is rejected rather
    /// than silently answered in the current format.
    pub format: Option<String>,
}

/// Counters shared by the inventory and rolling-activity views.
///
/// The field names are the camelCase spelling admin clients expect from the
/// `madmin` tier stats shape (`totalSize`, `numVersions`, `numObjects`), not
/// the Rust field names the version 1 body leaked.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TierStatsBody {
    total_size: u64,
    num_versions: u64,
    num_objects: u64,
}

impl From<TierStats> for TierStatsBody {
    fn from(stats: TierStats) -> Self {
        Self {
            total_size: stats.total_size,
            num_versions: stats.num_versions,
            num_objects: stats.num_objects,
        }
    }
}

/// Where the stored-inventory numbers came from.
///
/// `accounted` is the only status whose per-tier `inventory` values are
/// present; the other two say why they are absent instead of reporting a
/// plausible zero (`crates/data-usage/src/data_usage.rs` documents that an
/// absent per-tier accounting means "not accounted", never "zero").
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TierInventoryStatusBody {
    status: &'static str,
    /// Scanner snapshot time the inventory was taken from, RFC 3339.
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

/// How much of the cluster the rolling activity counters cover.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TierActivityStatusBody {
    status: &'static str,
    nodes_reporting: usize,
    nodes_expected: usize,
    unavailable_nodes: Vec<String>,
}

#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TierInfoBody {
    name: String,
    /// The configured remote tier type. Absent when the name carries stats but
    /// is not a configured remote tier: a local storage class the scanner
    /// accounts for, or a tier removed since the snapshot was taken.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    tier_type: Option<TierType>,
    /// Cluster-wide stored inventory for this tier. Absent whenever
    /// `inventory.status` is not `accounted`.
    #[serde(skip_serializing_if = "Option::is_none")]
    inventory: Option<TierStatsBody>,
    /// Transitions this cluster completed into the tier during the rolling
    /// 24 hours, summed over the reporting nodes. Each node counts only its
    /// own completions, so a transition retried across nodes is counted once,
    /// by the node that committed it.
    transitions_last24h: TierStatsBody,
    /// Newest hour boundary the merged rolling ring has been aged to, RFC 3339.
    #[serde(skip_serializing_if = "Option::is_none")]
    transitions_updated_at: Option<String>,
}

#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TierStatsBodyV2 {
    contract_version: u32,
    inventory: TierInventoryStatusBody,
    activity: TierActivityStatusBody,
    tiers: Vec<TierInfoBody>,
}

pub struct GetTierInfo {}
#[async_trait::async_trait]
impl Operation for GetTierInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::ListTierAction)]).await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: TierStatsQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                TierStatsQuery::default()
            }
        };

        let tier_name = if query.tier.is_some() {
            Some(require_tier_name(query.tier.as_deref())?)
        } else {
            None
        };

        let data = if tier_stats_wants_legacy_format(&query)? {
            serde_json::to_vec(&filter_tier_stats(current_daily_tier_stats(), tier_name))
        } else {
            let store = object_store_from_extensions(&req.extensions);
            serde_json::to_vec(&tier_stats_body(store, tier_name).await)
        }
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tier err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// Assemble the version 2 body from the two independent sources.
///
/// The stored inventory comes from the persisted scanner snapshot, which is
/// already cluster-wide; the rolling activity is per node and is summed over
/// the members that answer. Neither source can stand in for the other, so a
/// failure in one leaves the other's fields populated and says so in its own
/// status.
async fn tier_stats_body(store: Option<Arc<ECStore>>, tier_name: Option<&str>) -> TierStatsBodyV2 {
    let (inventory_status, inventory) = tier_inventory(store).await;
    let activity = cluster_tier_daily_stats().await;
    let tier_types = {
        let tier_config_mgr_handle = current_tier_config_handle();
        let tier_config_mgr = tier_config_mgr_handle.read().await;
        tier_config_mgr
            .list_tiers()
            .into_iter()
            .map(|tier| (tier.name, tier.tier_type))
            .collect()
    };

    assemble_tier_stats_body(tier_types, inventory_status, inventory, activity, tier_name)
}

/// Join the configured tiers, the stored inventory and the rolling activity
/// into one body.
///
/// A tier is listed when any of the three sources knows it: a configured tier
/// with no data yet must still appear, and a tier that carries data but no
/// configuration must not be dropped just because it cannot be typed.
fn assemble_tier_stats_body(
    tier_types: HashMap<String, TierType>,
    inventory_status: TierInventoryStatusBody,
    inventory: HashMap<String, TierStats>,
    activity: ClusterTierDailyStats,
    tier_name: Option<&str>,
) -> TierStatsBodyV2 {
    let mut names: BTreeSet<&str> = tier_types.keys().map(String::as_str).collect();
    names.extend(inventory.keys().map(String::as_str));
    names.extend(activity.stats.keys().map(String::as_str));

    let tiers = names
        .into_iter()
        .filter(|name| tier_name.is_none_or(|requested| name.eq_ignore_ascii_case(requested)))
        .map(|name| {
            let daily = activity.stats.get(name);
            TierInfoBody {
                name: name.to_string(),
                tier_type: tier_types.get(name).cloned(),
                inventory: inventory.get(name).copied().map(TierStatsBody::from),
                transitions_last24h: daily.map(|stats| stats.total()).unwrap_or_default().into(),
                transitions_updated_at: daily.and_then(|stats| format_rfc3339(stats.updated_at())),
            }
        })
        .collect();

    TierStatsBodyV2 {
        contract_version: TIER_STATS_CONTRACT_VERSION,
        inventory: inventory_status,
        activity: TierActivityStatusBody {
            status: if activity.is_complete() { "complete" } else { "partial" },
            nodes_reporting: activity.nodes_reporting,
            nodes_expected: activity.nodes_expected,
            unavailable_nodes: activity.unavailable_nodes,
        },
        tiers,
    }
}

/// Resolve the requested body version.
///
/// An unrecognized value is rejected rather than answered in the current
/// format: a caller that asked for a shape it can parse must not receive a
/// different one with a 200.
fn tier_stats_wants_legacy_format(query: &TierStatsQuery) -> S3Result<bool> {
    match query.format.as_deref().map(str::trim).filter(|format| !format.is_empty()) {
        None => Ok(false),
        Some(TIER_STATS_FORMAT_LEGACY) => Ok(true),
        Some(_) => Err(invalid_tier_query("unsupported tier-stats format")),
    }
}

/// The rolling ring of every cluster member that answers.
///
/// Without a notification system this process is the whole cluster it can
/// speak for, so its own ring is reported as a complete single-member result
/// rather than as a cluster total it cannot prove.
async fn cluster_tier_daily_stats() -> ClusterTierDailyStats {
    let local = current_daily_tier_stats();
    match current_notification_system() {
        Some(notification_sys) => notification_sys.tier_daily_stats(local).await,
        None => ClusterTierDailyStats {
            stats: local,
            nodes_reporting: 1,
            nodes_expected: 1,
            unavailable_nodes: Vec::new(),
        },
    }
}

async fn tier_inventory(store: Option<Arc<ECStore>>) -> (TierInventoryStatusBody, HashMap<String, TierStats>) {
    let Some(store) = store else {
        return (
            TierInventoryStatusBody {
                status: "unavailable",
                updated_at: None,
                detail: Some("object store is not initialized".to_string()),
            },
            HashMap::new(),
        );
    };

    match crate::admin::storage_api::data_usage::load_admin_data_usage_from_backend_cached(store).await {
        Err(err) => (
            TierInventoryStatusBody {
                status: "unavailable",
                updated_at: None,
                detail: Some(format!("usage snapshot could not be read: {err}")),
            },
            HashMap::new(),
        ),
        Ok(usage) => {
            let updated_at = usage.last_update.and_then(|updated| {
                OffsetDateTime::from(updated)
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
            });
            match usage.tier_stats {
                Some(tier_stats) => (
                    TierInventoryStatusBody {
                        status: "accounted",
                        updated_at,
                        detail: None,
                    },
                    tier_stats.tiers,
                ),
                None => (
                    TierInventoryStatusBody {
                        status: "not-accounted",
                        updated_at,
                        detail: Some("the persisted usage snapshot carries no per-tier accounting".to_string()),
                    },
                    HashMap::new(),
                ),
            }
        }
    }
}

fn format_rfc3339(at: OffsetDateTime) -> Option<String> {
    at.format(&time::format_description::well_known::Rfc3339).ok()
}

/// One constructor for every tier query rejection, so the mutation handlers
/// and the stats handler cannot drift into different faults for the same
/// class of bad request.
fn invalid_tier_query(message: &str) -> S3Error {
    s3_error!(InvalidArgument, "{message}")
}

fn optional_tier_name(tier: Option<&str>) -> Option<&str> {
    tier.map(str::trim).filter(|tier| !tier.is_empty())
}

fn require_tier_name(tier: Option<&str>) -> S3Result<&str> {
    optional_tier_name(tier).ok_or_else(|| invalid_tier_query("tier is required"))
}

fn filter_tier_stats(daily_stats: DailyAllTierStats, tier_name: Option<&str>) -> HashMap<String, TierStats> {
    daily_stats
        .into_iter()
        .filter_map(|(name, stats)| {
            if tier_name.is_some_and(|requested| !name.eq_ignore_ascii_case(requested)) {
                return None;
            }

            Some((name, stats.total()))
        })
        .collect()
}

fn map_tier_verify_error(err: std::io::Error) -> S3Error {
    if let Some(admin_err) = err.get_ref().and_then(|inner| inner.downcast_ref::<AdminError>()) {
        return match admin_err.code.as_str() {
            code if code == ERR_TIER_NOT_FOUND.code => {
                S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found!")
            }
            code if code == ERR_TIER_CONNECT_ERR.code => S3Error::with_message(
                S3ErrorCode::Custom("TierVerificationFailed".into()),
                format!("tier verification failed. {}", admin_err.message),
            ),
            _ => S3Error::with_message(
                S3ErrorCode::Custom("TierVerificationFailed".into()),
                format!("tier verification failed. {}", admin_err.message),
            ),
        };
    }

    S3Error::with_message(
        S3ErrorCode::Custom("TierVerificationFailed".into()),
        format!("tier verification failed. {err}"),
    )
}

#[derive(Debug, serde::Deserialize, Default)]
pub struct ClearTierQuery {
    pub rand: Option<String>,
    pub force: String,
}

fn parse_clear_tier_query(uri: &Uri) -> S3Result<ClearTierQuery> {
    let mut parsed = ClearTierQuery::default();
    let mut seen = HashSet::with_capacity(2);
    let Some(query) = uri.query() else {
        return Ok(parsed);
    };

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "rand" => {
                if !seen.insert("rand") {
                    return Err(s3_error!(InvalidArgument, "duplicate clear-tier query parameter"));
                }
                parsed.rand = Some(value.into_owned());
            }
            "force" => {
                if !seen.insert("force") {
                    return Err(s3_error!(InvalidArgument, "duplicate clear-tier query parameter"));
                }
                match value.as_ref() {
                    "true" | "false" => parsed.force = value.into_owned(),
                    _ => return Err(s3_error!(InvalidArgument, "invalid force flag")),
                }
            }
            _ => return Err(s3_error!(InvalidArgument, "unknown clear-tier query parameter")),
        }
    }

    Ok(parsed)
}

pub struct ClearTier {}
#[async_trait::async_trait]
impl Operation for ClearTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = parse_clear_tier_query(&req.uri)?;

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::SetTierAction)]).await?;

        let mut force: bool = false;
        let force_str = query.force;
        if !force_str.is_empty() {
            force = force_str
                .parse()
                .map_err(|_e| s3_error!(InvalidArgument, "invalid force flag"))?;
        }

        let t = OffsetDateTime::now_utc();
        let mut rand = "AGD1R25GI3I1GJGUGJFD7FBS4DFAASDF".to_string();
        rand.insert_str(3, &t.day().to_string());
        rand.insert_str(17, &t.month().to_string());
        rand.insert_str(23, &t.year().to_string());
        warn!(
            event = EVENT_ADMIN_TIER_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_TIER,
            action = "clear_tier",
            state = "challenge_generated",
            "admin tier state"
        );
        if query.rand != Some(rand) {
            return Err(s3_error!(InvalidRequest, "invalid clear-tier confirmation token"));
        };

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let tier_config_mgr_handle = current_tier_config_handle();
        if let Err(update_err) = TierConfigMgr::clear_and_save(&tier_config_mgr_handle, store, force).await {
            let err = match update_err {
                TierConfigUpdateError::Load(err) => {
                    warn!(
                        event = EVENT_ADMIN_TIER_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_TIER,
                        action = "clear_tier",
                        result = "reload_failed",
                        error = ?err,
                        "admin tier state"
                    );
                    return Err(S3Error::with_message(
                        S3ErrorCode::Custom("TierClearFailed".into()),
                        format!("tier clear failed. {err}"),
                    ));
                }
                TierConfigUpdateError::Save(err) => {
                    warn!(
                        event = EVENT_ADMIN_TIER_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_TIER,
                        action = "clear_tier",
                        result = "save_failed",
                        error = ?err,
                        "admin tier state"
                    );
                    return Err(S3Error::with_message(S3ErrorCode::Custom("TierEditFailed".into()), "tier save failed"));
                }
                TierConfigUpdateError::Mutation(err) | TierConfigUpdateError::Publish(err) => err,
            };
            warn!(
                event = EVENT_ADMIN_TIER_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_TIER,
                action = "clear_tier",
                result = "clear_failed",
                error = ?err,
                "admin tier state"
            );
            return Err(clear_tier_error_response(&err));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::storage_api::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    use http::Uri;
    use matchit::Router;

    #[test]
    fn wasabi_payload_name_requires_nested_configuration() {
        let config: TierConfig = serde_json::from_slice(
            br#"{"type":"wasabi","wasabi":{"name":"WASABI-FIRST","accessKey":"ak","secretKey":"sk","bucket":"archive","region":"us-east-1"}}"#,
        )
        .expect("Wasabi AddTier payload should decode");
        assert_eq!(wasabi_payload_name(&config).expect("Wasabi payload name should exist"), "WASABI-FIRST");

        let missing: TierConfig = serde_json::from_slice(br#"{"type":"wasabi"}"#).expect("type-only payload should decode");
        let err = wasabi_payload_name(&missing).expect_err("missing Wasabi payload must fail at the AddTier boundary");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("missing Wasabi configuration"));
    }

    #[test]
    fn resolve_tier_name_prefers_path_parameter() {
        let uri: Uri = "/rustfs/admin/v3/tier/HOT?tier=COLD".parse().expect("uri should parse");
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/tier/{tier}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/tier/HOT").expect("route should match");

        let tier = resolve_tier_name(&uri, &matched.params).expect("path parameter should resolve");
        assert_eq!(tier, "HOT");
    }

    #[test]
    fn resolve_tier_name_falls_back_to_query_parameter() {
        let uri: Uri = "/rustfs/admin/v3/tier-stats?tier=WARM".parse().expect("uri should parse");
        let mut router: Router<()> = Router::new();
        router.insert("/", ()).expect("root route should insert");
        let params = router.at("/").expect("root route should match").params;

        let tier = resolve_tier_name(&uri, &params).expect("query parameter should resolve");
        assert_eq!(tier, "WARM");
    }

    #[test]
    fn resolve_tier_name_falls_back_when_path_parameter_is_blank() {
        let uri: Uri = "/rustfs/admin/v3/tier/%20?tier=WARM".parse().expect("uri should parse");
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/tier/{tier}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/tier/%20").expect("route should match");

        let tier = resolve_tier_name(&uri, &matched.params).expect("query parameter should resolve");
        assert_eq!(tier, "WARM");
    }

    #[test]
    fn resolve_tier_name_preserves_plus_in_path_parameter() {
        let uri: Uri = "/rustfs/admin/v3/tier/WARM+PLUS".parse().expect("uri should parse");
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/tier/{tier}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/tier/WARM+PLUS").expect("route should match");

        let tier = resolve_tier_name(&uri, &matched.params).expect("path parameter should resolve");
        assert_eq!(tier, "WARM+PLUS");
    }

    #[test]
    fn resolve_tier_name_rejects_blank_path_without_query_fallback() {
        let uri: Uri = "/rustfs/admin/v3/tier/%20".parse().expect("uri should parse");
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/tier/{tier}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/tier/%20").expect("route should match");

        let err = resolve_tier_name(&uri, &matched.params).expect_err("blank path should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("tier is required"));
    }

    #[test]
    fn require_tier_name_rejects_missing_value() {
        let err = require_tier_name(None).expect_err("missing tier should return an error");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("tier is required"));
    }

    #[test]
    fn require_tier_name_rejects_empty_value() {
        let err = require_tier_name(Some("   ")).expect_err("empty tier should return an error");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("tier is required"));
    }

    #[test]
    fn filter_tier_stats_returns_all_tiers_without_filter() {
        let stats = filter_tier_stats(sample_daily_stats(), None);

        assert_eq!(stats.len(), 2);
        assert_eq!(
            stats.get("WARM"),
            Some(&TierStats {
                total_size: 15,
                num_versions: 3,
                num_objects: 1,
            })
        );
        assert_eq!(
            stats.get("ARCHIVE"),
            Some(&TierStats {
                total_size: 9,
                num_versions: 1,
                num_objects: 1,
            })
        );
    }

    #[test]
    fn filter_tier_stats_applies_case_insensitive_filter() {
        let stats = filter_tier_stats(sample_daily_stats(), Some("warm"));

        assert_eq!(stats.len(), 1);
        assert_eq!(
            stats.get("WARM"),
            Some(&TierStats {
                total_size: 15,
                num_versions: 3,
                num_objects: 1,
            })
        );
    }

    #[test]
    fn map_tier_verify_error_preserves_not_found() {
        let err = std::io::Error::other(ERR_TIER_NOT_FOUND.clone());
        let mapped = map_tier_verify_error(err);

        assert_eq!(mapped.code(), &S3ErrorCode::Custom("TierNotFound".into()));
        assert_eq!(mapped.message(), Some("tier not found!"));
    }

    #[test]
    fn map_tier_verify_error_wraps_other_failures() {
        let err = std::io::Error::other("backend unavailable");
        let mapped = map_tier_verify_error(err);

        assert_eq!(mapped.code(), &S3ErrorCode::Custom("TierVerificationFailed".into()));
        assert_eq!(mapped.message(), Some("tier verification failed. backend unavailable"));
    }

    #[test]
    fn tier_mutation_error_preserves_reload_and_save_responses() {
        let reload = tier_mutation_error(
            TierConfigUpdateError::Load(std::io::Error::other("read failed")),
            "add_tier",
            "TierAddFailed",
        )
        .expect_err("reload error should map to an S3 response");
        assert_eq!(reload.code(), &S3ErrorCode::Custom("TierAddFailed".into()));
        assert_eq!(reload.message(), Some("tier reload failed. read failed"));

        let save = tier_mutation_error(
            TierConfigUpdateError::Save(std::io::Error::other("conditional write failed")),
            "edit_tier",
            "TierEditFailed",
        )
        .expect_err("save error should map to an S3 response");
        assert_eq!(save.code(), &S3ErrorCode::Custom("TierEditFailed".into()));
        assert_eq!(save.message(), Some("tier save failed"));
    }

    #[test]
    fn tier_backend_errors_use_minio_wire_contract() {
        let in_use = tier_backend_error_response(&ERR_TIER_BACKEND_IN_USE)
            .expect("backend-in-use errors should have a compatible response");
        assert_eq!(in_use.code(), &S3ErrorCode::Custom("XMinioAdminTierBackendInUse".into()));
        assert_eq!(in_use.message(), Some("Specified remote tier is already in use"));
        assert_eq!(in_use.status_code(), Some(StatusCode::CONFLICT));

        let not_empty = tier_backend_error_response(&ERR_TIER_BACKEND_NOT_EMPTY)
            .expect("backend-not-empty errors should have a compatible response");
        assert_eq!(not_empty.code(), &S3ErrorCode::Custom("XMinioAdminTierBackendNotEmpty".into()));
        assert_eq!(not_empty.message(), Some("Specified remote backend is not empty"));
        assert_eq!(not_empty.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn tier_backend_error_response_canonicalizes_by_code_only() {
        let lifecycle_reference = AdminError {
            code: ERR_TIER_BACKEND_IN_USE.code.clone(),
            message: "tier WARM is referenced by lifecycle configuration".to_string(),
            status_code: StatusCode::IM_A_TEAPOT,
        };
        let response = tier_backend_error_response(&lifecycle_reference)
            .expect("a lifecycle reference should retain the backend-in-use wire contract");
        assert_eq!(response.code(), &S3ErrorCode::Custom("XMinioAdminTierBackendInUse".into()));
        assert_eq!(response.message(), Some("Specified remote tier is already in use"));
        assert_eq!(response.status_code(), Some(StatusCode::CONFLICT));

        let unknown = AdminError {
            code: "XRustFSAdminTierUnknown".to_string(),
            message: "unknown tier failure".to_string(),
            status_code: StatusCode::CONFLICT,
        };
        assert!(tier_backend_error_response(&unknown).is_none());
    }

    #[test]
    fn clear_preserves_legacy_backend_not_empty_error_code() {
        let response = clear_tier_error_response(&ERR_TIER_BACKEND_NOT_EMPTY);
        assert_eq!(response.code(), &S3ErrorCode::Custom("TierClearFailed".into()));
        assert!(
            response
                .message()
                .is_some_and(|message| message.starts_with("tier clear failed."))
        );

        let response = clear_tier_error_response(&ERR_TIER_BACKEND_IN_USE);
        assert_eq!(response.code(), &S3ErrorCode::Custom("TierClearFailed".into()));
        assert!(
            response
                .message()
                .is_some_and(|message| message.starts_with("tier clear failed."))
        );

        assert!(tier_backend_error_response(&ERR_TIER_NOT_FOUND).is_none());
    }

    #[test]
    fn parse_clear_tier_query_rejects_unknown_duplicate_and_invalid_force() {
        for raw in [
            "/rustfs/admin/v3/tier?rand=token&force=yes",
            "/rustfs/admin/v3/tier?rand=token&rand=other",
            "/rustfs/admin/v3/tier?rand=token&unexpected=true",
        ] {
            let uri: Uri = raw.parse().expect("uri should parse");
            let err = parse_clear_tier_query(&uri).expect_err("strict clear-tier query should reject malformed input");
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn parse_clear_tier_query_accepts_valid_force() {
        let uri: Uri = "/rustfs/admin/v3/tier?rand=token&force=true"
            .parse()
            .expect("uri should parse");
        let query = parse_clear_tier_query(&uri).expect("valid clear-tier query should parse");

        assert_eq!(query.rand.as_deref(), Some("token"));
        assert_eq!(query.force, "true");
    }

    fn accounted_inventory() -> TierInventoryStatusBody {
        TierInventoryStatusBody {
            status: "accounted",
            updated_at: None,
            detail: None,
        }
    }

    fn complete_activity(stats: DailyAllTierStats) -> ClusterTierDailyStats {
        ClusterTierDailyStats {
            stats,
            nodes_reporting: 2,
            nodes_expected: 2,
            unavailable_nodes: Vec::new(),
        }
    }

    #[test]
    fn tier_stats_body_separates_inventory_from_rolling_activity() {
        let inventory = HashMap::from([(
            "WARM".to_string(),
            TierStats {
                total_size: 4096,
                num_versions: 8,
                num_objects: 8,
            },
        )]);

        let body = assemble_tier_stats_body(
            HashMap::from([("WARM".to_string(), TierType::S3)]),
            accounted_inventory(),
            inventory,
            complete_activity(sample_daily_stats()),
            Some("WARM"),
        );

        let warm = body.tiers.first().expect("the requested tier must be present");
        assert_eq!(
            warm.inventory,
            Some(TierStatsBody {
                total_size: 4096,
                num_versions: 8,
                num_objects: 8,
            }),
            "inventory must come from the stored accounting, not the rolling window"
        );
        assert_eq!(
            warm.transitions_last24h,
            TierStatsBody {
                total_size: 15,
                num_versions: 3,
                num_objects: 1,
            },
            "rolling activity must come from the transition ring, not the inventory"
        );
        assert_eq!(warm.tier_type, Some(TierType::S3));
    }

    #[test]
    fn a_configured_tier_without_data_is_still_listed() {
        let body = assemble_tier_stats_body(
            HashMap::from([("COLD".to_string(), TierType::S3)]),
            accounted_inventory(),
            HashMap::new(),
            complete_activity(DailyAllTierStats::new()),
            None,
        );

        let cold = body
            .tiers
            .first()
            .expect("a configured tier must be listed before it has data");
        assert_eq!(cold.name, "COLD");
        assert_eq!(cold.inventory, None, "an unaccounted tier must not report a zero inventory");
        assert_eq!(
            cold.transitions_last24h,
            TierStatsBody {
                total_size: 0,
                num_versions: 0,
                num_objects: 0,
            }
        );
        assert_eq!(cold.transitions_updated_at, None);
    }

    #[test]
    fn a_tier_with_activity_but_no_configuration_keeps_its_counters_untyped() {
        let body = assemble_tier_stats_body(
            HashMap::new(),
            accounted_inventory(),
            HashMap::new(),
            complete_activity(sample_daily_stats()),
            Some("ARCHIVE"),
        );

        let archive = body
            .tiers
            .first()
            .expect("a tier with data must be listed without a configuration");
        assert_eq!(archive.tier_type, None, "an unconfigured name must not be given a type");
        assert_eq!(
            archive.transitions_last24h,
            TierStatsBody {
                total_size: 9,
                num_versions: 1,
                num_objects: 1,
            }
        );
    }

    #[test]
    fn a_restarted_cluster_keeps_its_inventory_and_loses_only_the_rolling_window() {
        // The rolling window lives in each process's memory and the inventory
        // lives in the persisted usage snapshot, so a restart empties one and
        // leaves the other intact. Reporting the emptied window as the tier's
        // contents is the confusion the two fields exist to prevent.
        let inventory = HashMap::from([(
            "WARM".to_string(),
            TierStats {
                total_size: 4096,
                num_versions: 8,
                num_objects: 8,
            },
        )]);

        let body = assemble_tier_stats_body(
            HashMap::from([("WARM".to_string(), TierType::S3)]),
            accounted_inventory(),
            inventory,
            complete_activity(DailyAllTierStats::new()),
            Some("WARM"),
        );

        let warm = body.tiers.first().expect("the tier must survive a restart");
        assert_eq!(
            warm.inventory,
            Some(TierStatsBody {
                total_size: 4096,
                num_versions: 8,
                num_objects: 8,
            }),
            "a restart must not empty the stored inventory"
        );
        assert_eq!(
            warm.transitions_last24h,
            TierStatsBody {
                total_size: 0,
                num_versions: 0,
                num_objects: 0,
            },
            "a restart empties the rolling window, and that must stay visible as zero activity"
        );
    }

    #[test]
    fn an_unreachable_node_makes_the_activity_partial() {
        let body = assemble_tier_stats_body(
            HashMap::new(),
            accounted_inventory(),
            HashMap::new(),
            ClusterTierDailyStats {
                stats: sample_daily_stats(),
                nodes_reporting: 1,
                nodes_expected: 2,
                unavailable_nodes: vec!["10.0.0.2:9000".to_string()],
            },
            None,
        );

        assert_eq!(
            body.activity.status, "partial",
            "a sum over part of the cluster must not be presented as a cluster total"
        );
        assert_eq!(body.activity.nodes_reporting, 1);
        assert_eq!(body.activity.nodes_expected, 2);
        assert_eq!(body.activity.unavailable_nodes, vec!["10.0.0.2:9000".to_string()]);
    }

    #[test]
    fn an_unavailable_inventory_omits_the_per_tier_values() {
        let body = assemble_tier_stats_body(
            HashMap::from([("WARM".to_string(), TierType::S3)]),
            TierInventoryStatusBody {
                status: "unavailable",
                updated_at: None,
                detail: Some("usage snapshot could not be read".to_string()),
            },
            HashMap::new(),
            complete_activity(sample_daily_stats()),
            Some("WARM"),
        );

        assert_eq!(body.inventory.status, "unavailable");
        assert_eq!(
            body.tiers.first().expect("the tier must still be listed").inventory,
            None,
            "an unreadable snapshot must not be rendered as an empty tier"
        );
    }

    #[test]
    fn the_body_names_its_own_contract_version() {
        let body = assemble_tier_stats_body(
            HashMap::new(),
            accounted_inventory(),
            HashMap::new(),
            complete_activity(DailyAllTierStats::new()),
            None,
        );
        let encoded = serde_json::to_value(&body).expect("the body must serialize");

        assert_eq!(encoded["contractVersion"], 2);
        assert!(
            encoded["activity"]["nodesExpected"].is_number(),
            "the activity coverage must reach the wire"
        );
    }

    #[test]
    fn tier_stats_counters_use_the_madmin_field_spelling() {
        let encoded = serde_json::to_value(TierStatsBody::from(TierStats {
            total_size: 1,
            num_versions: 2,
            num_objects: 3,
        }))
        .expect("counters must serialize");

        assert_eq!(encoded["totalSize"], 1);
        assert_eq!(encoded["numVersions"], 2);
        assert_eq!(encoded["numObjects"], 3);
    }

    #[test]
    fn the_legacy_format_is_opt_in_and_unknown_formats_are_rejected() {
        assert!(!tier_stats_wants_legacy_format(&TierStatsQuery::default()).expect("no format is the current contract"));
        assert!(
            tier_stats_wants_legacy_format(&TierStatsQuery {
                tier: None,
                format: Some("legacy".to_string()),
            })
            .expect("legacy must stay reachable")
        );

        let err = tier_stats_wants_legacy_format(&TierStatsQuery {
            tier: None,
            format: Some("v3".to_string()),
        })
        .expect_err("an unknown format must not be answered in another shape");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    fn sample_daily_stats() -> DailyAllTierStats {
        let mut warm = LastDayTierStats::default();
        warm.add_stats(TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        });
        warm.add_stats(TierStats {
            total_size: 5,
            num_versions: 2,
            num_objects: 0,
        });

        let mut archive = LastDayTierStats::default();
        archive.add_stats(TierStats {
            total_size: 9,
            num_versions: 1,
            num_objects: 1,
        });

        let mut stats = DailyAllTierStats::new();
        stats.insert("WARM".to_string(), warm);
        stats.insert("ARCHIVE".to_string(), archive);
        stats
    }

    fn credential_less_request(method: Method, uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn assert_missing_credentials(operation: &dyn Operation, method: Method, uri: &'static str, message: &str) {
        let err = operation
            .call(credential_less_request(method, uri), Params::new())
            .await
            .expect_err("a tier admin request without credentials must fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(message));
    }

    /// The shared gate reports "get cred failed"; the per-handler pre-check keeps
    /// the message each endpoint has always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn tier_handlers_keep_their_missing_credentials_response() {
        assert_missing_credentials(&AddTier {}, Method::PUT, "/rustfs/admin/v3/tier", "authentication required").await;
        assert_missing_credentials(&EditTier {}, Method::POST, "/rustfs/admin/v3/tier/WARM", "authentication required").await;
        assert_missing_credentials(&ListTiers {}, Method::GET, "/rustfs/admin/v3/tiers", "get cred failed").await;
        assert_missing_credentials(&RemoveTier {}, Method::DELETE, "/rustfs/admin/v3/tier/WARM", "authentication required").await;
        assert_missing_credentials(&VerifyTier {}, Method::GET, "/rustfs/admin/v3/tier/WARM", "authentication required").await;
        assert_missing_credentials(&GetTierInfo {}, Method::GET, "/rustfs/admin/v3/tier-stats", "get cred failed").await;
        assert_missing_credentials(&ClearTier {}, Method::DELETE, "/rustfs/admin/v3/tiers", "authentication required").await;
    }

    fn source_block<'a>(production: &'a str, marker: &str) -> &'a str {
        let block = production
            .split_once(marker)
            .unwrap_or_else(|| panic!("{marker} should exist"))
            .1;
        let end = ["\npub struct ", "\nfn ", "\n#[derive(", "\n#[cfg(test)]"]
            .into_iter()
            .filter_map(|boundary| block.find(boundary))
            .min()
            .unwrap_or(block.len());
        &block[..end]
    }

    fn assert_shared_gate_wiring(block: &str, item: &str, actions: &[&str], binds_credentials: bool) {
        assert_eq!(
            block.matches("authorize_admin_request(").count(),
            1,
            "{item} must use exactly one shared gate"
        );
        assert_eq!(
            block.matches("Action::AdminAction(").count(),
            actions.len(),
            "{item} must preserve its exact action-vector length"
        );
        for action in actions {
            assert!(block.contains(&format!("AdminAction::{action}")), "{item} must authorize with {action}");
        }
        assert_eq!(
            block.contains("let cred = authorize_admin_request("),
            binds_credentials,
            "{item} credential binding must match its payload-processing contract"
        );
    }

    #[test]
    fn tier_handlers_use_the_shared_admin_gate_with_their_actions() {
        let production = include_str!("tier.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");

        for (handler, action) in [
            ("AddTier", "SetTierAction"),
            ("EditTier", "SetTierAction"),
            ("ListTiers", "ListTierAction"),
            ("RemoveTier", "SetTierAction"),
            ("VerifyTier", "ListTierAction"),
            ("GetTierInfo", "ListTierAction"),
            ("ClearTier", "SetTierAction"),
        ] {
            let block = source_block(production, &format!("impl Operation for {handler}"));
            assert_shared_gate_wiring(block, handler, &[action], false);
        }

        assert!(!production.contains("check_key_valid(get_session_token"));
    }
}
