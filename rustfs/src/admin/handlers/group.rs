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

use super::iam_error::iam_error_to_s3_error;
use crate::{
    admin::{
        auth::validate_admin_request,
        handlers::site_replication::site_replication_iam_change_hook,
        router::{AdminOperation, Operation, S3Router},
        utils::has_space_be,
    },
    app::context::resolve_action_credentials,
    auth::{check_key_valid, constant_time_eq, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use percent_encoding::percent_decode_str;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_iam::error::{is_err_no_such_group, is_err_no_such_user};
use rustfs_madmin::{GroupAddRemove, GroupStatus, SITE_REPL_API_VERSION, SRGroupInfo, SRIAMItem};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use tracing::warn;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_GROUP: &str = "group";
const EVENT_ADMIN_GROUP_STATE: &str = "admin_group_state";

pub fn register_group_management_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/groups").as_str(),
        AdminOperation(&ListGroups {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/group").as_str(),
        AdminOperation(&GetGroup {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/group/{group}").as_str(),
        AdminOperation(&DeleteGroup {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-group-status").as_str(),
        AdminOperation(&SetGroupStatus {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-group-members").as_str(),
        AdminOperation(&UpdateGroupMembers {}),
    )?;

    Ok(())
}

#[derive(Debug, Deserialize, Default)]
pub struct GroupQuery {
    pub group: String,
    pub status: Option<String>,
}

pub struct ListGroups {}
#[async_trait::async_trait]
impl Operation for ListGroups {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "list_groups",
            state = "requested",
            "admin group state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ListGroupsAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InternalError, "iam is not initialized"));
        };

        let groups = iam_store.list_groups_load().await.map_err(|e| {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "list_groups",
                result = "load_failed",
                error = ?e,
                "admin group state"
            );
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        let body = serde_json::to_vec(&groups).map_err(|e| s3_error!(InternalError, "failed to serialize response: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct GetGroup {}
#[async_trait::async_trait]
impl Operation for GetGroup {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "get_group",
            state = "requested",
            "admin group state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::GetGroupAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: GroupQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                GroupQuery::default()
            }
        };
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InternalError, "iam is not initialized"));
        };

        let g = iam_store.get_group_description(&query.group).await.map_err(|e| {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "get_group",
                group = %query.group,
                result = "load_failed",
                error = ?e,
                "admin group state"
            );
            iam_error_to_s3_error(e)
        })?;

        let body = serde_json::to_vec(&g).map_err(|e| s3_error!(InternalError, "failed to serialize response: {:?}", e))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

/// Deletes an empty user group.
///
/// # Arguments
/// * `group` - The name of the group to delete
///
/// # Returns
/// - `200 OK` - Group deleted successfully
/// - `400 Bad Request` - Group name missing or invalid
/// - `401 Unauthorized` - Insufficient permissions
/// - `404 Not Found` - Group does not exist
/// - `409 Conflict` - Group contains members and cannot be deleted
/// - `500 Internal Server Error` - Server-side error
///
/// # Example
/// ```text
/// DELETE /rustfs/admin/v3/group/developers
/// ```
pub struct DeleteGroup {}
#[async_trait::async_trait]
impl Operation for DeleteGroup {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "delete_group",
            state = "requested",
            "admin group state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::RemoveUserFromGroupAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let group = decode_delete_group_name(&params)?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InternalError, "iam is not initialized"));
        };

        let updated_at = iam_store.remove_users_from_group(&group, vec![]).await.map_err(|e| {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "delete_group",
                group = %group,
                result = "delete_failed",
                error = ?e,
                "admin group state"
            );
            match e {
                rustfs_iam::error::Error::GroupNotEmpty => {
                    s3_error!(InvalidRequest, "group is not empty")
                }
                rustfs_iam::error::Error::InvalidArgument => {
                    s3_error!(InvalidArgument, "invalid group request: {e}")
                }
                _ => {
                    if is_err_no_such_group(&e) {
                        iam_error_to_s3_error(e)
                    } else {
                        s3_error!(InternalError, "failed to delete group: {e}")
                    }
                }
            }
        })?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "group-info".to_string(),
            group_info: Some(SRGroupInfo {
                update_req: GroupAddRemove {
                    group: group.to_string(),
                    members: vec![],
                    status: GroupStatus::Enabled,
                    is_remove: true,
                },
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "delete_group",
                result = "site_replication_hook_failed",
                error = %err,
                "admin group state"
            );
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

fn decode_delete_group_name<'a>(params: &'a Params<'_, '_>) -> S3Result<std::borrow::Cow<'a, str>> {
    let group_raw = params
        .get("group")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing group name in request"))?
        .trim();

    // Path segments stay percent-encoded in `req.uri.path()` / matchit; IAM uses decoded names (same as GET query).
    let decoded = percent_decode_str(group_raw)
        .decode_utf8()
        .map_err(|_| s3_error!(InvalidArgument, "invalid group name encoding"))?;
    let group = decoded.trim();

    if group.is_empty() || group.len() > 256 {
        return Err(s3_error!(InvalidArgument, "invalid group name"));
    }

    if group.contains(['/', '\\', '\0']) {
        return Err(s3_error!(InvalidArgument, "group name contains invalid characters"));
    }

    if group.len() == decoded.len() {
        Ok(decoded)
    } else {
        Ok(std::borrow::Cow::Owned(group.to_string()))
    }
}

pub struct SetGroupStatus {}
#[async_trait::async_trait]
impl Operation for SetGroupStatus {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "set_group_status",
            state = "requested",
            "admin group state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::EnableGroupAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: GroupQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                GroupQuery::default()
            }
        };

        if query.group.is_empty() {
            return Err(s3_error!(InvalidArgument, "group is required"));
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InternalError, "iam is not initialized"));
        };

        let updated_at = if let Some(status) = query.status.as_deref() {
            match status {
                "enabled" => iam_store.set_group_status(&query.group, true).await.map_err(|e| {
                    warn!(
                        event = EVENT_ADMIN_GROUP_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_GROUP,
                        action = "set_group_status",
                        group = %query.group,
                        status = "enabled",
                        result = "update_failed",
                        error = ?e,
                        "admin group state"
                    );
                    iam_error_to_s3_error(e)
                })?,
                "disabled" => iam_store.set_group_status(&query.group, false).await.map_err(|e| {
                    warn!(
                        event = EVENT_ADMIN_GROUP_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_GROUP,
                        action = "set_group_status",
                        group = %query.group,
                        status = "disabled",
                        result = "update_failed",
                        error = ?e,
                        "admin group state"
                    );
                    iam_error_to_s3_error(e)
                })?,
                _ => {
                    return Err(s3_error!(InvalidArgument, "invalid status"));
                }
            }
        } else {
            return Err(s3_error!(InvalidArgument, "status is required"));
        };

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "group-info".to_string(),
            group_info: Some(SRGroupInfo {
                update_req: GroupAddRemove {
                    group: query.group.clone(),
                    members: vec![],
                    status: if matches!(query.status.as_deref(), Some("disabled")) {
                        GroupStatus::Disabled
                    } else {
                        GroupStatus::Enabled
                    },
                    is_remove: false,
                },
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "set_group_status",
                result = "site_replication_hook_failed",
                error = %err,
                "admin group state"
            );
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
        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "update_group_members",
            state = "requested",
            "admin group state"
        );

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::AddUserToGroupAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_GROUP_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_GROUP,
                    action = "update_group_members",
                    result = "body_read_failed",
                    error = ?e,
                    "admin group state"
                );
                return Err(s3_error!(InvalidRequest, "group configuration body too large or failed to read"));
            }
        };

        let args: GroupAddRemove = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid JSON: {e}")))?;

        warn!(
            event = EVENT_ADMIN_GROUP_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_GROUP,
            action = "update_group_members",
            group = %args.group,
            member_count = args.members.len(),
            remove = args.is_remove,
            state = "decoded",
            "admin group state"
        );

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InternalError, "iam is not initialized"));
        };

        for member in args.members.iter() {
            match iam_store.is_temp_user(member).await {
                Ok((is_temp, _)) => {
                    if is_temp {
                        return Err(S3Error::with_message(
                            S3ErrorCode::MethodNotAllowed,
                            format!("can't add temp user {member}"),
                        ));
                    }

                    resolve_action_credentials()
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
                            Err(S3Error::with_message(
                                S3ErrorCode::InternalError,
                                "failed to load global credentials".to_string(),
                            ))
                        })?;
                }
                Err(e) => {
                    if !is_err_no_such_user(&e) {
                        return Err(iam_error_to_s3_error(e));
                    }
                }
            }
        }

        let updated_at = if args.is_remove {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "remove_group_members",
                group = %args.group,
                member_count = args.members.len(),
                state = "requested",
                "admin group state"
            );
            iam_store
                .remove_users_from_group(&args.group, args.members.clone())
                .await
                .map_err(|e| {
                    warn!(
                        event = EVENT_ADMIN_GROUP_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_GROUP,
                        action = "remove_group_members",
                        group = %args.group,
                        result = "update_failed",
                        error = ?e,
                        "admin group state"
                    );
                    iam_error_to_s3_error(e)
                })?
        } else {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "add_group_members",
                group = %args.group,
                member_count = args.members.len(),
                state = "requested",
                "admin group state"
            );

            if let Err(err) = iam_store.get_group_description(&args.group).await
                && is_err_no_such_group(&err)
                && has_space_be(&args.group)
            {
                return Err(s3_error!(InvalidArgument, "group not found"));
            }

            iam_store
                .add_users_to_group(&args.group, args.members.clone())
                .await
                .map_err(|e| {
                    warn!(
                        event = EVENT_ADMIN_GROUP_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_GROUP,
                        action = "add_group_members",
                        group = %args.group,
                        result = "update_failed",
                        error = ?e,
                        "admin group state"
                    );
                    iam_error_to_s3_error(e)
                })?
        };

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "group-info".to_string(),
            group_info: Some(SRGroupInfo {
                update_req: args,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                event = EVENT_ADMIN_GROUP_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_GROUP,
                action = "update_group_members",
                result = "site_replication_hook_failed",
                error = %err,
                "admin group state"
            );
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
    use matchit::Router;

    fn with_delete_group_params<T>(path: &str, f: impl FnOnce(&Params<'_, '_>) -> T) -> T {
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/group/{group}", ())
            .expect("route should insert");

        let matched = router.at(path).expect("route should match");
        f(&matched.params)
    }

    #[test]
    fn decode_delete_group_name_percent_decodes_path_segment() {
        let group = with_delete_group_params("/rustfs/admin/v3/group/dev%2Bops%20team", |params| {
            decode_delete_group_name(params).map(|group| group.into_owned())
        })
        .expect("encoded group name should decode");

        assert_eq!(group, "dev+ops team");
    }

    #[test]
    fn decode_delete_group_name_rejects_invalid_utf8() {
        let err = with_delete_group_params("/rustfs/admin/v3/group/%FF", |params| {
            decode_delete_group_name(params).map(|group| group.into_owned())
        })
        .expect_err("invalid utf-8 should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("invalid group name encoding"));
    }

    #[test]
    fn decode_delete_group_name_rejects_blank_name_after_decoding() {
        let err = with_delete_group_params("/rustfs/admin/v3/group/%20", |params| {
            decode_delete_group_name(params).map(|group| group.into_owned())
        })
        .expect_err("blank group should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("invalid group name"));
    }

    #[test]
    fn decode_delete_group_name_rejects_path_separator_after_decoding() {
        let err = with_delete_group_params("/rustfs/admin/v3/group/team%2Fops", |params| {
            decode_delete_group_name(params).map(|group| group.into_owned())
        })
        .expect_err("decoded slash should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("group name contains invalid characters"));
    }
}
