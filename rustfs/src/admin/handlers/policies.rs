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

use crate::{
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
        utils::has_space_be,
    },
    auth::{check_key_valid, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::get_global_action_cred;
use rustfs_iam::error::is_err_no_such_user;
use rustfs_iam::store::MappedPolicy;
use rustfs_policy::policy::{
    Policy,
    action::{Action, AdminAction},
};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use tracing::warn;

pub fn register_iam_policy_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-canned-policies").as_str(),
        AdminOperation(&ListCannedPolicies {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-canned-policy").as_str(),
        AdminOperation(&InfoCannedPolicy {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-canned-policy").as_str(),
        AdminOperation(&AddCannedPolicy {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-canned-policy").as_str(),
        AdminOperation(&RemoveCannedPolicy {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-user-or-group-policy").as_str(),
        AdminOperation(&SetPolicyForUserOrGroup {}),
    )?;

    Ok(())
}

#[derive(Debug, Deserialize, Default)]
pub struct BucketQuery {
    pub bucket: String,
}

pub struct ListCannedPolicies {}
#[async_trait::async_trait]
impl Operation for ListCannedPolicies {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListCannedPolicies");

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
            vec![Action::AdminAction(AdminAction::ListUserPoliciesAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: BucketQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                BucketQuery::default()
            }
        };

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        let policies = iam_store.list_polices(&query.bucket).await.map_err(|e| {
            warn!("list policies failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let kvs: HashMap<String, Policy> = policies
            .into_iter()
            .filter(|(_, v)| serde_json::to_string(v).is_ok())
            .collect();

        let body = serde_json::to_vec(&kvs).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct PolicyNameQuery {
    pub name: String,
}

pub struct AddCannedPolicy {}
#[async_trait::async_trait]
impl Operation for AddCannedPolicy {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AddCannedPolicy");

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
            vec![Action::AdminAction(AdminAction::CreatePolicyAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: PolicyNameQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                PolicyNameQuery::default()
            }
        };

        if query.name.is_empty() {
            return Err(s3_error!(InvalidArgument, "policy name is empty"));
        }

        if has_space_be(&query.name) {
            return Err(s3_error!(InvalidArgument, "policy name has space"));
        }

        let mut input = req.input;
        let policy_bytes = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "policy configuration body too large or failed to read"));
            }
        };

        let policy = Policy::parse_config(policy_bytes.as_ref()).map_err(|e| {
            warn!("parse policy failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InvalidRequest, e.to_string())
        })?;

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        iam_store.set_policy(&query.name, policy).await.map_err(|e| {
            warn!("set policy failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct InfoCannedPolicy {}
#[async_trait::async_trait]
impl Operation for InfoCannedPolicy {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InfoCannedPolicy");

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
            vec![Action::AdminAction(AdminAction::GetPolicyAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: PolicyNameQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                PolicyNameQuery::default()
            }
        };

        if query.name.is_empty() {
            return Err(s3_error!(InvalidArgument, "policy name is empty"));
        }

        let policies = MappedPolicy::new(&query.name).to_slice();
        if policies.len() != 1 {
            return Err(s3_error!(InvalidArgument, "too many policies"));
        }

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        let pd = iam_store.info_policy(&query.name).await.map_err(|e| {
            warn!("info policy failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let body = serde_json::to_vec(&pd).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct RemoveCannedPolicy {}
#[async_trait::async_trait]
impl Operation for RemoveCannedPolicy {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RemoveCannedPolicy");

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
            vec![Action::AdminAction(AdminAction::DeletePolicyAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: PolicyNameQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                PolicyNameQuery::default()
            }
        };

        if query.name.is_empty() {
            return Err(s3_error!(InvalidArgument, "policy name is empty"));
        }

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        iam_store.delete_policy(&query.name, true).await.map_err(|e| {
            warn!("delete policy failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct SetPolicyForUserOrGroupQuery {
    #[serde(rename = "policyName")]
    pub policy_name: String,
    #[serde(rename = "userOrGroup")]
    pub user_or_group: String,
    #[serde(rename = "isGroup")]
    pub is_group: bool,
}

pub struct SetPolicyForUserOrGroup {}
#[async_trait::async_trait]
impl Operation for SetPolicyForUserOrGroup {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle SetPolicyForUserOrGroup");

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
            vec![Action::AdminAction(AdminAction::AttachPolicyAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: SetPolicyForUserOrGroupQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                SetPolicyForUserOrGroupQuery::default()
            }
        };

        if query.user_or_group.is_empty() {
            return Err(s3_error!(InvalidArgument, "user or group is empty"));
        }

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        if !query.is_group {
            match iam_store.is_temp_user(&query.user_or_group).await {
                Ok((ok, _)) => {
                    if ok {
                        return Err(s3_error!(InvalidArgument, "temp user can't set policy"));
                    }
                }
                Err(err) => {
                    if !is_err_no_such_user(&err) {
                        warn!("is temp user failed, e: {:?}", err);
                        return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
                    }
                }
            };

            let Some(sys_cred) = get_global_action_cred() else {
                return Err(s3_error!(InternalError, "get global action cred failed"));
            };

            if query.user_or_group == sys_cred.access_key {
                return Err(s3_error!(InvalidArgument, "can't set policy for system user"));
            }
        }

        if !query.is_group {
            if iam_store.get_user(&query.user_or_group).await.is_none() {
                return Err(s3_error!(InvalidArgument, "user not exist"));
            }
        } else {
            iam_store.get_group_description(&query.user_or_group).await.map_err(|e| {
                warn!("get group description failed, e: {:?}", e);
                S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
            })?;
        }

        iam_store
            .policy_db_set(&query.user_or_group, rustfs_iam::store::UserType::Reg, query.is_group, &query.policy_name)
            .await
            .map_err(|e| {
                warn!("policy db set failed, e: {:?}", e);
                S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
            })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}
