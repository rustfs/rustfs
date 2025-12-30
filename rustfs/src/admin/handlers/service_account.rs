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

use crate::admin::utils::has_space_be;
use crate::auth::{constant_time_eq, get_condition_values, get_session_token};
use crate::server::RemoteAddr;
use crate::{admin::router::Operation, auth::check_key_valid};
use http::HeaderMap;
use hyper::StatusCode;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::get_global_action_cred;
use rustfs_iam::error::is_err_no_such_service_account;
use rustfs_iam::sys::{NewServiceAccountOpts, UpdateServiceAccountOpts};
use rustfs_madmin::{
    AddServiceAccountReq, AddServiceAccountResp, Credentials, InfoServiceAccountResp, ListServiceAccountsResp,
    ServiceAccountInfo, UpdateServiceAccountReq,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_policy::policy::{Args, Policy};
use s3s::S3ErrorCode::InvalidRequest;
use s3s::header::CONTENT_LENGTH;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use tracing::{debug, warn};

pub struct AddServiceAccount {}
#[async_trait::async_trait]
impl Operation for AddServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AddServiceAccount ");
        let Some(req_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &req_cred.access_key).await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(
                    InvalidRequest,
                    "service account configuration body too large or failed to read"
                ));
            }
        };

        let create_req: AddServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        // create_req.expiration = create_req.expiration.and_then(|expire| expire.replace_millisecond(0).ok());

        if has_space_be(&create_req.access_key) {
            return Err(s3_error!(InvalidRequest, "access key has spaces"));
        }

        create_req
            .validate()
            .map_err(|e| S3Error::with_message(InvalidRequest, e.to_string()))?;

        let session_policy = if let Some(policy) = &create_req.policy {
            let p = Policy::parse_config(policy.as_bytes()).map_err(|e| {
                debug!("parse policy failed, e: {:?}", e);
                s3_error!(InvalidArgument, "parse policy failed")
            })?;
            Some(p)
        } else {
            None
        };

        let Some(sys_cred) = get_global_action_cred() else {
            return Err(s3_error!(InvalidRequest, "get sys cred failed"));
        };

        if constant_time_eq(&sys_cred.access_key, &create_req.access_key) {
            return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
        }

        let mut target_user = if let Some(u) = create_req.target_user {
            u
        } else {
            cred.access_key.clone()
        };

        let req_user = cred.access_key.clone();
        let mut req_parent_user = cred.access_key.clone();
        let req_groups = cred.groups.clone();
        let mut req_is_derived_cred = false;

        if cred.is_service_account() || cred.is_temp() {
            req_parent_user = cred.parent_user.clone();
            req_is_derived_cred = true;
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let deny_only = constant_time_eq(&cred.access_key, &target_user) || constant_time_eq(&cred.parent_user, &target_user);

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::CreateServiceAccountAdminAction),
                bucket: "",
                conditions: &get_condition_values(
                    &req.headers,
                    &cred,
                    None,
                    None,
                    req.extensions.get::<RemoteAddr>().map(|a| a.0),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        if target_user != cred.access_key {
            let has_user = iam_store.get_user(&target_user).await;
            if has_user.is_none() && target_user != sys_cred.access_key {
                return Err(s3_error!(InvalidRequest, "target user not exist"));
            }
        }

        let is_svc_acc = target_user == req_user || target_user == req_parent_user;

        let mut target_groups = None;
        let mut opts = NewServiceAccountOpts {
            access_key: create_req.access_key,
            secret_key: create_req.secret_key,
            name: create_req.name,
            description: create_req.description,
            expiration: create_req.expiration,
            session_policy,
            ..Default::default()
        };

        if is_svc_acc {
            if req_is_derived_cred {
                if req_parent_user.is_empty() {
                    return Err(s3_error!(AccessDenied, "only derived cred can create service account"));
                }
                target_user = req_parent_user;
            }

            target_groups = req_groups;

            if let Some(claims) = cred.claims {
                if opts.claims.is_none() {
                    opts.claims = Some(HashMap::new());
                }

                for (k, v) in claims.iter() {
                    if claims.contains_key("exp") {
                        continue;
                    }

                    opts.claims.as_mut().unwrap().insert(k.clone(), v.clone());
                }
            }
        }

        let (new_cred, _) = iam_store
            .new_service_account(&target_user, target_groups, opts)
            .await
            .map_err(|e| {
                debug!("create service account failed, e: {:?}", e);
                s3_error!(InternalError, "create service account failed, e: {:?}", e)
            })?;

        let resp = AddServiceAccountResp {
            credentials: Credentials {
                access_key: &new_cred.access_key,
                secret_key: &new_cred.secret_key,
                session_token: None,
                expiration: new_cred.expiration,
            },
        };

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

#[derive(Debug, Default, Deserialize)]
struct AccessKeyQuery {
    #[serde(rename = "accessKey")]
    pub access_key: String,
}

pub struct UpdateServiceAccount {}
#[async_trait::async_trait]
impl Operation for UpdateServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle UpdateServiceAccount");

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let access_key = query.access_key;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        // let svc_account = iam_store.get_service_account(&access_key).await.map_err(|e| {
        //     debug!("get service account failed, e: {:?}", e);
        //     s3_error!(InternalError, "get service account failed")
        // })?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(
                    InvalidRequest,
                    "service account configuration body too large or failed to read"
                ));
            }
        };

        let update_req: UpdateServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        update_req
            .validate()
            .map_err(|e| S3Error::with_message(InvalidRequest, e.to_string()))?;

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::UpdateServiceAccountAdminAction),
                bucket: "",
                conditions: &get_condition_values(
                    &req.headers,
                    &cred,
                    None,
                    None,
                    req.extensions.get::<RemoteAddr>().map(|a| a.0),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only: false,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let sp = {
            if let Some(policy) = update_req.new_policy {
                let sp = Policy::parse_config(policy.as_bytes()).map_err(|e| {
                    debug!("parse policy failed, e: {:?}", e);
                    s3_error!(InvalidArgument, "parse policy failed")
                })?;

                if sp.version.is_empty() && sp.statements.is_empty() {
                    None
                } else {
                    Some(sp)
                }
            } else {
                None
            }
        };

        let opts = UpdateServiceAccountOpts {
            secret_key: update_req.new_secret_key,
            status: update_req.new_status,
            name: update_req.new_name,
            description: update_req.new_description,
            expiration: update_req.new_expiration,
            session_policy: sp,
        };

        let _ = iam_store.update_service_account(&access_key, opts).await.map_err(|e| {
            debug!("update service account failed, e: {:?}", e);
            s3_error!(InternalError, "update service account failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct InfoServiceAccount {}
#[async_trait::async_trait]
impl Operation for InfoServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InfoServiceAccount");

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let access_key = query.access_key;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let (svc_account, session_policy) = iam_store.get_service_account(&access_key).await.map_err(|e| {
            debug!("get service account failed, e: {:?}", e);
            s3_error!(InternalError, "get service account failed")
        })?;

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                bucket: "",
                conditions: &get_condition_values(
                    &req.headers,
                    &cred,
                    None,
                    None,
                    req.extensions.get::<RemoteAddr>().map(|a| a.0),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only: false,
            })
            .await
        {
            let user = if cred.parent_user.is_empty() {
                &cred.access_key
            } else {
                &cred.parent_user
            };
            if user != &svc_account.parent_user {
                return Err(s3_error!(AccessDenied, "access denied"));
            }
        }

        let implied_policy = if let Some(policy) = session_policy.as_ref() {
            policy.version.is_empty() && policy.statements.is_empty()
        } else {
            true
        };

        let svc_account_policy = {
            if !implied_policy {
                session_policy
            } else {
                let policies = iam_store
                    .policy_db_get(&svc_account.parent_user, &svc_account.groups)
                    .await
                    .map_err(|e| {
                        debug!("get service account policy failed, e: {:?}", e);
                        s3_error!(InternalError, "get service account policy failed")
                    })?;

                Some(iam_store.get_combined_policy(&policies).await)
            }
        };

        let policy = {
            if let Some(policy) = svc_account_policy {
                Some(serde_json::to_string(&policy).map_err(|e| {
                    debug!("marshal policy failed, e: {:?}", e);
                    s3_error!(InternalError, "marshal policy failed")
                })?)
            } else {
                None
            }
        };

        let resp = InfoServiceAccountResp {
            parent_user: svc_account.parent_user,
            account_status: svc_account.status,
            implied_policy,
            name: svc_account.name,
            description: svc_account.description,
            expiration: svc_account.expiration,
            policy,
        };

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ListServiceAccountQuery {
    pub user: Option<String>,
}

pub struct ListServiceAccount {}
#[async_trait::async_trait]
impl Operation for ListServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListServiceAccount");

        let query = {
            if let Some(query) = req.uri.query() {
                let input: ListServiceAccountQuery = from_bytes(query.as_bytes())
                    .map_err(|_e| s3_error!(InvalidArgument, "invalid service account query parameters"))?;
                input
            } else {
                ListServiceAccountQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key)
                .await
                .map_err(|e| {
                    debug!("check key failed: {e:?}");
                    s3_error!(InternalError, "check key failed")
                })?;

        // let target_account = if let Some(user) = query.user {
        //     if user != input_cred.access_key {
        //         user
        //     } else if cred.parent_user.is_empty() {
        //         input_cred.access_key
        //     } else {
        //         cred.parent_user
        //     }
        // } else if cred.parent_user.is_empty() {
        //     input_cred.access_key
        // } else {
        //     cred.parent_user
        // };

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let target_account = if query.user.as_ref().is_some_and(|v| v != &cred.access_key) {
            if !iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::AdminAction(AdminAction::UpdateServiceAccountAdminAction),
                    bucket: "",
                    conditions: &get_condition_values(
                        &req.headers,
                        &cred,
                        None,
                        None,
                        req.extensions.get::<RemoteAddr>().map(|a| a.0),
                    ),
                    is_owner: owner,
                    object: "",
                    claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                    deny_only: false,
                })
                .await
            {
                return Err(s3_error!(AccessDenied, "access denied"));
            }

            query.user.unwrap_or_default()
        } else if cred.parent_user.is_empty() {
            cred.access_key
        } else {
            cred.parent_user
        };

        let service_accounts = iam_store.list_service_accounts(&target_account).await.map_err(|e| {
            debug!("list service account failed: {e:?}");
            s3_error!(InternalError, "list service account failed")
        })?;

        let accounts: Vec<ServiceAccountInfo> = service_accounts
            .into_iter()
            .map(|sa| ServiceAccountInfo {
                parent_user: sa.parent_user.clone(),
                account_status: sa.status.clone(),
                implied_policy: sa.is_implied_policy(), // or set according to your logic
                access_key: sa.access_key,
                name: sa.name,
                description: sa.description,
                expiration: sa.expiration,
            })
            .collect();

        let data = serde_json::to_vec(&ListServiceAccountsResp { accounts })
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct DeleteServiceAccount {}
#[async_trait::async_trait]
impl Operation for DeleteServiceAccount {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DeleteServiceAccount");
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key)
                .await
                .map_err(|e| {
                    debug!("check key failed: {e:?}");
                    s3_error!(InternalError, "check key failed")
                })?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AccessKeyQuery = from_bytes(query.as_bytes())
                    .map_err(|_e| s3_error!(InvalidArgument, "invalid access key query parameters"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let svc_account = match iam_store.get_service_account(&query.access_key).await {
            Ok((res, _)) => Some(res),
            Err(err) => {
                if is_err_no_such_service_account(&err) {
                    return Err(s3_error!(InvalidRequest, "service account not exist"));
                }

                None
            }
        };

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::RemoveServiceAccountAdminAction),
                bucket: "",
                conditions: &get_condition_values(
                    &req.headers,
                    &cred,
                    None,
                    None,
                    req.extensions.get::<RemoteAddr>().map(|a| a.0),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only: false,
            })
            .await
        {
            let user = if cred.parent_user.is_empty() {
                &cred.access_key
            } else {
                &cred.parent_user
            };

            if svc_account.is_some_and(|v| &v.parent_user != user) {
                return Err(s3_error!(InvalidRequest, "service account not exist"));
            }
        }

        iam_store.delete_service_account(&query.access_key, true).await.map_err(|e| {
            debug!("delete service account failed, e: {:?}", e);
            s3_error!(InternalError, "delete service account failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}
