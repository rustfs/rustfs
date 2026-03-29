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

use crate::{
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    app::context::resolve_tier_config_handle,
    auth::{check_key_valid, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::Uri;
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use percent_encoding::percent_decode_str;
use rustfs_common::data_usage::TierStats;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState;
use rustfs_ecstore::{
    bucket::lifecycle::tier_last_day_stats::DailyAllTierStats,
    client::admin_handler_utils::AdminError,
    config::storageclass,
    tier::{
        tier::{ERR_TIER_BACKEND_IN_USE, ERR_TIER_BACKEND_NOT_EMPTY, ERR_TIER_MISSING_CREDENTIALS},
        tier_admin::TierCreds,
        tier_config::{TierConfig, TierType},
        tier_handlers::{
            ERR_TIER_ALREADY_EXISTS, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_NAME_NOT_UPPERCASE,
            ERR_TIER_NOT_FOUND,
        },
    },
};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::{debug, warn};

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct AddTierQuery {
    #[serde(rename = "accessKey")]
    #[allow(dead_code)]
    pub access_key: Option<String>,
    #[allow(dead_code)]
    pub status: Option<String>,
    #[serde(rename = "secretKey")]
    #[allow(dead_code)]
    pub secret_key: Option<String>,
    #[serde(rename = "serviceName")]
    #[allow(dead_code)]
    pub service_name: Option<String>,
    #[serde(rename = "sessionToken")]
    #[allow(dead_code)]
    pub session_token: Option<String>,
    pub tier: Option<String>,
    #[serde(rename = "tierName")]
    #[allow(dead_code)]
    pub tier_name: Option<String>,
    #[serde(rename = "tierType")]
    #[allow(dead_code)]
    pub tier_type: Option<String>,
    pub force: Option<String>,
}

pub struct AddTier {}

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

    Ok(require_tier_name(&query)?.to_string())
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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "tier configuration body too large or failed to read"));
            }
        };

        let mut args: TierConfig = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        match args.tier_type {
            TierType::S3 => {
                args.name = args.s3.clone().unwrap().name;
            }
            TierType::RustFS => {
                args.name = args.rustfs.clone().unwrap().name;
            }
            TierType::MinIO => {
                args.name = args.minio.clone().unwrap().name;
            }
            TierType::Aliyun => {
                args.name = args.aliyun.clone().unwrap().name;
            }
            TierType::Tencent => {
                args.name = args.tencent.clone().unwrap().name;
            }
            TierType::Huaweicloud => {
                args.name = args.huaweicloud.clone().unwrap().name;
            }
            TierType::Azure => {
                args.name = args.azure.clone().unwrap().name;
            }
            TierType::GCS => {
                args.name = args.gcs.clone().unwrap().name;
            }
            TierType::R2 => {
                args.name = args.r2.clone().unwrap().name;
            }
            _ => (),
        }
        debug!("add tier args {:?}", args);

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!("parse force failed, e: {:?}", e);
                s3_error!(InvalidRequest, "parse force failed")
            })?;
        }
        match args.name.as_str() {
            storageclass::STANDARD | storageclass::RRS => {
                warn!("tier reserved name, args.name: {}", args.name);
                return Err(s3_error!(InvalidRequest, "Cannot use reserved tier name"));
            }
            &_ => (),
        }

        let tier_config_mgr_handle = resolve_tier_config_handle();
        let mut tier_config_mgr = tier_config_mgr_handle.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.add(args, force).await {
            return if err.code == ERR_TIER_ALREADY_EXISTS.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameAlreadyExist".into()),
                    "tier name already exists!",
                ))
            } else if err.code == ERR_TIER_NAME_NOT_UPPERCASE.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameNotUppercase".into()),
                    "tier name not uppercase!",
                ))
            } else if err.code == ERR_TIER_BACKEND_IN_USE.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierNameBackendInUse!".into()),
                    "tier name backend in use!",
                ))
            } else if err.code == ERR_TIER_CONNECT_ERR.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierConnectError".into()),
                    "tier connect error!",
                ))
            } else if err.code == ERR_TIER_INVALID_CREDENTIALS.code {
                Err(S3Error::with_message(S3ErrorCode::Custom(err.code.clone().into()), err.message.clone()))
            } else {
                warn!("tier_config_mgr add failed, e: {:?}", err);
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierAddFailed".into()),
                    format!("tier add failed. {err}"),
                ))
            };
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierAddFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "tier configuration body too large or failed to read"));
            }
        };

        let creds: TierCreds = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        debug!("edit tier args {:?}", creds);

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        let tier_config_mgr_handle = resolve_tier_config_handle();
        let mut tier_config_mgr = tier_config_mgr_handle.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.edit(&tier_name, creds).await {
            return if err.code == ERR_TIER_NOT_FOUND.code {
                Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found!"))
            } else if err.code == ERR_TIER_MISSING_CREDENTIALS.code {
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierMissingCredentials".into()),
                    "tier missing credentials!",
                ))
            } else {
                warn!("tier_config_mgr edit failed, e: {:?}", err);
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierEditFailed".into()),
                    format!("tier edit failed. {err}"),
                ))
            };
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierEditFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct BucketQuery {
    #[serde(rename = "bucket")]
    #[allow(dead_code)]
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

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ListTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let tier_config_mgr_handle = resolve_tier_config_handle();
        let tier_config_mgr = tier_config_mgr_handle.read().await;
        let tiers = tier_config_mgr.list_tiers();

        let data = serde_json::to_vec(&tiers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tiers err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut force: bool = false;
        let force_str = query.force.clone().unwrap_or_default();
        if !force_str.is_empty() {
            force = force_str.parse().map_err(|e| {
                warn!("parse force failed, e: {:?}", e);
                s3_error!(InvalidRequest, "parse force failed")
            })?;
        }

        let tier_name = params.get("tiername").map(|s| s.to_string()).unwrap_or_default();

        let tier_config_mgr_handle = resolve_tier_config_handle();
        let mut tier_config_mgr = tier_config_mgr_handle.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.remove(&tier_name, force).await {
            return if err.code == ERR_TIER_NOT_FOUND.code {
                Err(S3Error::with_message(S3ErrorCode::Custom("TierNotFound".into()), "tier not found."))
            } else if err.code == ERR_TIER_BACKEND_NOT_EMPTY.code {
                Err(S3Error::with_message(S3ErrorCode::Custom("TierNameBackendInUse".into()), "tier is used."))
            } else {
                warn!("tier_config_mgr remove failed, e: {:?}", err);
                Err(S3Error::with_message(
                    S3ErrorCode::Custom("TierRemoveFailed".into()),
                    format!("tier remove failed. {err}"),
                ))
            };
        }

        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierRemoveFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[allow(dead_code)]
pub struct VerifyTier {}
#[async_trait::async_trait]
impl Operation for VerifyTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ListTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let tier = resolve_tier_name(&req.uri, &params)?;
        let tier_config_mgr_handle = resolve_tier_config_handle();
        let mut tier_config_mgr = tier_config_mgr_handle.write().await;
        tier_config_mgr.verify(&tier).await.map_err(map_tier_verify_error)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct GetTierInfo {}
#[async_trait::async_trait]
impl Operation for GetTierInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ListTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                AddTierQuery::default()
            }
        };

        let tier_name = if query.tier.is_some() {
            Some(require_tier_name(&query)?)
        } else {
            None
        };
        let info = filter_tier_stats(GLOBAL_TransitionState.get_daily_all_tier_stats(), tier_name);

        let data = serde_json::to_vec(&info)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tier err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

fn optional_tier_name(query: &AddTierQuery) -> Option<&str> {
    query.tier.as_deref().map(str::trim).filter(|tier| !tier.is_empty())
}

fn require_tier_name(query: &AddTierQuery) -> S3Result<&str> {
    optional_tier_name(query).ok_or_else(|| s3_error!(InvalidArgument, "tier is required"))
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

#[allow(dead_code)]
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

pub struct ClearTier {}
#[async_trait::async_trait]
impl Operation for ClearTier {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ClearTierQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                ClearTierQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::SetTierAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut force: bool = false;
        let force_str = query.force;
        if !force_str.is_empty() {
            force = force_str.parse().unwrap();
        }

        let t = OffsetDateTime::now_utc();
        let mut rand = "AGD1R25GI3I1GJGUGJFD7FBS4DFAASDF".to_string();
        rand.insert_str(3, &t.day().to_string());
        rand.insert_str(17, &t.month().to_string());
        rand.insert_str(23, &t.year().to_string());
        warn!("tier_config_mgr rand: {}", rand);
        if query.rand != Some(rand) {
            return Err(s3_error!(InvalidRequest, "get rand failed"));
        };

        let tier_config_mgr_handle = resolve_tier_config_handle();
        let mut tier_config_mgr = tier_config_mgr_handle.write().await;
        //tier_config_mgr.reload(api);
        if let Err(err) = tier_config_mgr.clear_tier(force).await {
            warn!("tier_config_mgr clear failed, e: {:?}", err);
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("TierClearFailed".into()),
                format!("tier clear failed. {err}"),
            ));
        }
        if let Err(e) = tier_config_mgr.save().await {
            warn!("tier_config_mgr save failed, e: {:?}", e);
            return Err(S3Error::with_message(S3ErrorCode::Custom("TierEditFailed".into()), "tier save failed"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Uri;
    use matchit::Router;
    use rustfs_ecstore::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;

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
        let err = require_tier_name(&AddTierQuery::default()).expect_err("missing tier should return an error");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("tier is required"));
    }

    #[test]
    fn require_tier_name_rejects_empty_value() {
        let err = require_tier_name(&AddTierQuery {
            tier: Some("   ".to_string()),
            ..Default::default()
        })
        .expect_err("empty tier should return an error");

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
}
