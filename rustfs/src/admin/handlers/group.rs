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
    admin::{auth::validate_admin_request, router::Operation, utils::has_space_be},
    auth::{check_key_valid, constant_time_eq, get_session_token},
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::get_global_action_cred;
use rustfs_iam::error::{is_err_no_such_group, is_err_no_such_user};
use rustfs_madmin::GroupAddRemove;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use tracing::warn;

#[derive(Debug, Deserialize, Default)]
pub struct GroupQuery {
    pub group: String,
    pub status: Option<String>,
}

pub struct ListGroups {}
#[async_trait::async_trait]
impl Operation for ListGroups {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListGroups");

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
            vec![Action::AdminAction(AdminAction::ListGroupsAdminAction)],
        )
        .await?;

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        let groups = iam_store.list_groups_load().await.map_err(|e| {
            warn!("list groups failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let body = serde_json::to_vec(&groups).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct GetGroup {}
#[async_trait::async_trait]
impl Operation for GetGroup {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle GetGroup");

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
            vec![Action::AdminAction(AdminAction::GetGroupAdminAction)],
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: GroupQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                GroupQuery::default()
            }
        };
        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        let g = iam_store.get_group_description(&query.group).await.map_err(|e| {
            warn!("get group failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let body = serde_json::to_vec(&g).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct SetGroupStatus {}
#[async_trait::async_trait]
impl Operation for SetGroupStatus {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle SetGroupStatus");

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
            vec![Action::AdminAction(AdminAction::EnableGroupAdminAction)],
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: GroupQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                GroupQuery::default()
            }
        };

        if query.group.is_empty() {
            return Err(s3_error!(InvalidArgument, "group is required"));
        }

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        if let Some(status) = query.status {
            match status.as_str() {
                "enabled" => {
                    iam_store.set_group_status(&query.group, true).await.map_err(|e| {
                        warn!("enable group failed, e: {:?}", e);
                        S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
                    })?;
                }
                "disabled" => {
                    iam_store.set_group_status(&query.group, false).await.map_err(|e| {
                        warn!("enable group failed, e: {:?}", e);
                        S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
                    })?;
                }
                _ => {
                    return Err(s3_error!(InvalidArgument, "invalid status"));
                }
            }
        } else {
            return Err(s3_error!(InvalidArgument, "status is required"));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct UpdateGroupMembers {}
#[async_trait::async_trait]
impl Operation for UpdateGroupMembers {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle UpdateGroupMembers");

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
            vec![Action::AdminAction(AdminAction::AddUserToGroupAdminAction)],
        )
        .await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "group configuration body too large or failed to read"));
            }
        };

        let args: GroupAddRemove = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        warn!("UpdateGroupMembers args {:?}", args);

        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        for member in args.members.iter() {
            match iam_store.is_temp_user(member).await {
                Ok((is_temp, _)) => {
                    if is_temp {
                        return Err(S3Error::with_message(
                            S3ErrorCode::MethodNotAllowed,
                            format!("can't add temp user {member}"),
                        ));
                    }

                    get_global_action_cred()
                        .map(|cred| {
                            if constant_time_eq(&cred.access_key, member) {
                                return Err(S3Error::with_message(
                                    S3ErrorCode::MethodNotAllowed,
                                    format!("can't add root {member}"),
                                ));
                            }
                            Ok(())
                        })
                        .unwrap_or_else(|| {
                            Err(S3Error::with_message(S3ErrorCode::InternalError, "get global cred failed".to_string()))
                        })?;
                }
                Err(e) => {
                    if !is_err_no_such_user(&e) {
                        return Err(S3Error::with_message(S3ErrorCode::InternalError, e.to_string()));
                    }
                }
            }
        }

        if args.is_remove {
            warn!("remove group members");
            iam_store
                .remove_users_from_group(&args.group, args.members)
                .await
                .map_err(|e| {
                    warn!("remove group members failed, e: {:?}", e);
                    S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
                })?;
        } else {
            warn!("add group members");

            if let Err(err) = iam_store.get_group_description(&args.group).await {
                if is_err_no_such_group(&err) && has_space_be(&args.group) {
                    return Err(s3_error!(InvalidArgument, "not such group"));
                }
            }

            iam_store.add_users_to_group(&args.group, args.members).await.map_err(|e| {
                warn!("add group members failed, e: {:?}", e);
                S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
            })?;
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}
