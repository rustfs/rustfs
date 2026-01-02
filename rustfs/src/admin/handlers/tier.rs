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
    admin::{auth::validate_admin_request, router::Operation},
    auth::{check_key_valid, get_session_token},
    server::RemoteAddr,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::{
    config::storageclass,
    global::GLOBAL_TierConfigMgr,
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
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
            vec![Action::AdminAction(AdminAction::ListTierAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        tier_config_mgr.verify(&query.tier.unwrap()).await;

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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
        let info = tier_config_mgr.get(&query.tier.unwrap());

        let data = serde_json::to_vec(&info)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal tier err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
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
