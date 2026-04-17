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
        handlers::site_replication::site_replication_iam_change_hook,
        router::{AdminOperation, Operation, S3Router},
        utils::{encode_compatible_admin_payload, has_space_be, read_compatible_admin_body},
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
use rustfs_madmin::{
    GroupPolicyEntities, PolicyEntities, PolicyEntitiesResult, SITE_REPL_API_VERSION, SRIAMItem, SRPolicyMapping,
    UserPolicyEntities,
};
use rustfs_policy::policy::{
    Policy,
    action::{Action, AdminAction},
};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::{Deserialize, Serialize};
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::warn;
use url::form_urlencoded;

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
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-policy").as_str(),
        AdminOperation(&SetPolicyForUserOrGroup {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/idp/builtin/policy/attach").as_str(),
        AdminOperation(&AttachPolicyBuiltin {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/idp/builtin/policy/detach").as_str(),
        AdminOperation(&DetachPolicyBuiltin {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/idp/builtin/policy-entities").as_str(),
        AdminOperation(&ListPolicyEntitiesBuiltin {}),
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

        if policy.version.is_empty() {
            return Err(s3_error!(InvalidArgument, "policy version is empty"));
        }
        let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        let updated_at = iam_store.set_policy(&query.name, policy.clone()).await.map_err(|e| {
            warn!("set policy failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "policy".to_string(),
            name: query.name.clone(),
            policy: Some(
                serde_json::to_value(&policy).map_err(|e| s3_error!(InternalError, "marshal policy failed, e: {:?}", e))?,
            ),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(policy = %query.name, error = ?err, "site replication policy add hook failed");
        }

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

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "policy".to_string(),
            name: query.name.clone(),
            updated_at: Some(OffsetDateTime::now_utc()),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(policy = %query.name, error = ?err, "site replication policy delete hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct SetPolicyForUserOrGroupQuery {
    #[serde(default)]
    #[serde(rename = "policyName", alias = "policy")]
    pub policy_name: String,
    #[serde(rename = "userOrGroup", alias = "user-or-group")]
    pub user_or_group: String,
    #[serde(rename = "isGroup", alias = "is-group")]
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

        let updated_at = iam_store
            .policy_db_set(&query.user_or_group, rustfs_iam::store::UserType::Reg, query.is_group, &query.policy_name)
            .await
            .map_err(|e| {
                warn!("policy db set failed, e: {:?}", e);
                S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
            })?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "policy-mapping".to_string(),
            policy_mapping: Some(SRPolicyMapping {
                user_or_group: query.user_or_group.clone(),
                user_type: rustfs_iam::store::UserType::Reg.to_u64(),
                is_group: query.is_group,
                policy: query.policy_name.clone(),
                updated_at: Some(updated_at),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(target = %query.user_or_group, error = ?err, "site replication policy mapping hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Deserialize, Default)]
struct PolicyAssociationReq {
    #[serde(default)]
    policies: Vec<String>,
    #[serde(default)]
    user: String,
    #[serde(default)]
    group: String,
}

#[derive(Debug, Serialize)]
struct PolicyAssociationResp {
    #[serde(rename = "policiesAttached", skip_serializing_if = "Vec::is_empty")]
    policies_attached: Vec<String>,
    #[serde(rename = "policiesDetached", skip_serializing_if = "Vec::is_empty")]
    policies_detached: Vec<String>,
    #[serde(rename = "updatedAt", with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

#[derive(Debug, Deserialize, Default)]
struct PolicyEntitiesQuery {
    users: Vec<String>,
    groups: Vec<String>,
    policies: Vec<String>,
}

fn split_policy_names(policy_names: &str) -> Vec<String> {
    policy_names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn attach_policy_names(existing: &[String], requested: &[String]) -> (Vec<String>, Vec<String>) {
    let mut updated = existing.to_vec();
    let mut attached = Vec::new();

    for policy in requested {
        if updated.iter().any(|current| current == policy) {
            continue;
        }
        updated.push(policy.clone());
        attached.push(policy.clone());
    }

    (updated, attached)
}

fn detach_policy_names(existing: &[String], requested: &[String]) -> (Vec<String>, Vec<String>) {
    let mut detached = Vec::new();
    let updated = existing
        .iter()
        .filter(|policy| {
            let should_detach = requested.iter().any(|requested_policy| requested_policy == *policy);
            if should_detach {
                detached.push((*policy).clone());
            }
            !should_detach
        })
        .cloned()
        .collect();

    (updated, detached)
}

fn validate_policy_association_req(req: &PolicyAssociationReq) -> S3Result<()> {
    if req.policies.is_empty() {
        return Err(s3_error!(InvalidArgument, "no policy names were given"));
    }

    if req.policies.iter().any(|policy| policy.is_empty()) {
        return Err(s3_error!(InvalidArgument, "an empty policy name was given"));
    }

    let has_user = !req.user.is_empty();
    let has_group = !req.group.is_empty();

    match (has_user, has_group) {
        (false, false) => Err(s3_error!(InvalidArgument, "no user or group association was given")),
        (true, true) => Err(s3_error!(InvalidArgument, "either a group or a user association must be given, not both")),
        _ => Ok(()),
    }
}

fn parse_policy_entities_query(query: Option<&str>) -> PolicyEntitiesQuery {
    let mut parsed = PolicyEntitiesQuery::default();
    let Some(query) = query else {
        return parsed;
    };

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "user" => parsed.users.push(value.into_owned()),
            "group" => parsed.groups.push(value.into_owned()),
            "policy" => parsed.policies.push(value.into_owned()),
            _ => {}
        }
    }

    parsed
}

fn split_policy_list(policy_names: Option<&String>) -> Vec<String> {
    policy_names.map_or_else(Vec::new, |names| split_policy_names(names))
}

fn direct_user_policy_names(user_info: &rustfs_madmin::UserInfo) -> Vec<String> {
    split_policy_list(user_info.policy_name.as_ref())
}

fn sorted_group_policy_entities(mut entities: Vec<GroupPolicyEntities>) -> Vec<GroupPolicyEntities> {
    entities.sort_by(|left, right| left.group.cmp(&right.group));
    entities
}

fn build_policy_mappings(
    user_mappings: &[UserPolicyEntities],
    group_mappings: &[GroupPolicyEntities],
    requested_policies: &[String],
) -> Vec<PolicyEntities> {
    let mut policy_map: HashMap<String, PolicyEntities> = HashMap::new();

    for user_mapping in user_mappings {
        for policy in &user_mapping.policies {
            let entry = policy_map.entry(policy.clone()).or_insert_with(|| PolicyEntities {
                policy: policy.clone(),
                ..Default::default()
            });
            entry.users.push(user_mapping.user.clone());
        }
    }

    for group_mapping in group_mappings {
        for policy in &group_mapping.policies {
            let entry = policy_map.entry(policy.clone()).or_insert_with(|| PolicyEntities {
                policy: policy.clone(),
                ..Default::default()
            });
            entry.groups.push(group_mapping.group.clone());
        }
    }

    let mut results: Vec<PolicyEntities> = policy_map
        .into_values()
        .filter_map(|mut mapping| {
            if !requested_policies.is_empty() && !requested_policies.iter().any(|policy| policy == &mapping.policy) {
                return None;
            }
            mapping.users.sort();
            mapping.users.dedup();
            mapping.groups.sort();
            mapping.groups.dedup();
            Some(mapping)
        })
        .collect();
    results.sort_by(|left, right| left.policy.cmp(&right.policy));
    results
}

async fn collect_group_policy_mappings(
    iam_store: &std::sync::Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
    requested_groups: &[String],
) -> S3Result<HashMap<String, GroupPolicyEntities>> {
    let mut mappings = HashMap::new();
    let groups = if requested_groups.is_empty() {
        iam_store
            .list_groups_load()
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?
    } else {
        requested_groups.to_vec()
    };

    for group in groups {
        let group_desc = iam_store.get_group_description(&group).await.map_err(|e| {
            warn!("get group description failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;
        let policies = split_policy_names(&group_desc.policy);
        if policies.is_empty() {
            continue;
        }
        mappings.insert(group.clone(), GroupPolicyEntities { group, policies });
    }

    Ok(mappings)
}

async fn handle_builtin_policy_entities(req: S3Request<Body>) -> S3Result<S3Response<(StatusCode, Body)>> {
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
        vec![
            Action::AdminAction(AdminAction::ListGroupsAdminAction),
            Action::AdminAction(AdminAction::ListUsersAdminAction),
            Action::AdminAction(AdminAction::ListUserPoliciesAdminAction),
        ],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await?;

    let query = parse_policy_entities_query(req.uri.query());

    let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

    let all_group_policy_mappings = collect_group_policy_mappings(&iam_store, &[]).await?;
    let users = iam_store.list_users().await.map_err(|e| {
        warn!("list users failed, e: {:?}", e);
        S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
    })?;

    let all_user_policy_mappings = users
        .iter()
        .filter_map(|(user, user_info)| {
            let policies = split_policy_list(user_info.policy_name.as_ref());
            if policies.is_empty() {
                return None;
            }
            Some(UserPolicyEntities {
                user: user.clone(),
                policies,
                member_of_mappings: Vec::new(),
            })
        })
        .collect::<Vec<_>>();

    let user_mappings = if query.users.is_empty() {
        Vec::new()
    } else {
        let mut mappings = Vec::new();
        for user in &query.users {
            let Some(user_info) = users.get(user) else {
                continue;
            };

            let mut member_of_mappings = user_info
                .member_of
                .as_ref()
                .map(|groups| {
                    groups
                        .iter()
                        .filter_map(|group| all_group_policy_mappings.get(group).cloned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            member_of_mappings = sorted_group_policy_entities(member_of_mappings);

            let policies = split_policy_list(user_info.policy_name.as_ref());
            if policies.is_empty() && member_of_mappings.is_empty() {
                continue;
            }

            mappings.push(UserPolicyEntities {
                user: user.clone(),
                policies,
                member_of_mappings,
            });
        }
        mappings.sort_by(|left, right| left.user.cmp(&right.user));
        mappings
    };

    let mut group_mappings = if query.groups.is_empty() {
        Vec::new()
    } else {
        query
            .groups
            .iter()
            .filter_map(|group| all_group_policy_mappings.get(group).cloned())
            .collect::<Vec<_>>()
    };
    group_mappings = sorted_group_policy_entities(group_mappings);

    let policy_mappings = if query.users.is_empty() && query.groups.is_empty() && query.policies.is_empty() {
        build_policy_mappings(
            &all_user_policy_mappings,
            &all_group_policy_mappings.values().cloned().collect::<Vec<_>>(),
            &[],
        )
    } else if !query.policies.is_empty() {
        build_policy_mappings(
            &all_user_policy_mappings,
            &all_group_policy_mappings.values().cloned().collect::<Vec<_>>(),
            &query.policies,
        )
    } else {
        Vec::new()
    };

    let resp = PolicyEntitiesResult {
        timestamp: OffsetDateTime::now_utc(),
        user_mappings,
        group_mappings,
        policy_mappings,
    };

    let req_path = req.uri.path().to_string();
    let body =
        serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal policy entities body failed, e: {:?}", e))?;
    let (body, content_type) = encode_compatible_admin_payload(&req_path, &cred.secret_key, body)?;

    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, content_type.parse().unwrap());
    header.insert(CONTENT_LENGTH, body.len().to_string().parse().unwrap());
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
}

async fn handle_builtin_policy_association(req: S3Request<Body>, is_attach: bool) -> S3Result<S3Response<(StatusCode, Body)>> {
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

    let req_path = req.uri.path().to_string();
    let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &req_path, &cred.secret_key).await?;
    let assoc_req: PolicyAssociationReq = serde_json::from_slice(&body)
        .map_err(|e| s3_error!(InvalidRequest, "unmarshal policy association body failed, e: {:?}", e))?;
    validate_policy_association_req(&assoc_req)?;

    let Ok(iam_store) = rustfs_iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

    let (target_name, is_group, existing_policies) = if !assoc_req.user.is_empty() {
        match iam_store.is_temp_user(&assoc_req.user).await {
            Ok((true, _)) => return Err(s3_error!(InvalidArgument, "temp user can't set policy")),
            Ok((false, _)) => {}
            Err(err) => {
                if !is_err_no_such_user(&err) {
                    warn!("is temp user failed, e: {:?}", err);
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
                }
            }
        }

        let Some(sys_cred) = get_global_action_cred() else {
            return Err(s3_error!(InternalError, "get global action cred failed"));
        };

        if assoc_req.user == sys_cred.access_key {
            return Err(s3_error!(InvalidArgument, "can't set policy for system user"));
        }

        if iam_store.get_user(&assoc_req.user).await.is_none() {
            return Err(s3_error!(InvalidArgument, "user not exist"));
        }

        let user_info = iam_store.get_user_info(&assoc_req.user).await.map_err(|e| {
            warn!("get user info failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        (assoc_req.user, false, direct_user_policy_names(&user_info))
    } else {
        let group_desc = iam_store.get_group_description(&assoc_req.group).await.map_err(|e| {
            warn!("get group description failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        (assoc_req.group, true, split_policy_names(&group_desc.policy))
    };

    let (updated_policies, changed_policies) = if is_attach {
        attach_policy_names(&existing_policies, &assoc_req.policies)
    } else {
        detach_policy_names(&existing_policies, &assoc_req.policies)
    };

    let updated_at = iam_store
        .policy_db_set(&target_name, rustfs_iam::store::UserType::Reg, is_group, &updated_policies.join(","))
        .await
        .map_err(|e| {
            warn!("policy db set failed, e: {:?}", e);
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

    if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
        r#type: "policy-mapping".to_string(),
        policy_mapping: Some(SRPolicyMapping {
            user_or_group: target_name.clone(),
            user_type: rustfs_iam::store::UserType::Reg.to_u64(),
            is_group,
            policy: updated_policies.join(","),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        }),
        updated_at: Some(updated_at),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    })
    .await
    {
        warn!(target = %target_name, error = ?err, "site replication policy association hook failed");
    }

    let policies_attached = if is_attach { changed_policies.clone() } else { Vec::new() };
    let policies_detached = if is_attach { Vec::new() } else { changed_policies };

    let resp = PolicyAssociationResp {
        policies_attached,
        policies_detached,
        updated_at,
    };

    let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
    let (body, content_type) = encode_compatible_admin_payload(&req_path, &cred.secret_key, body)?;

    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, content_type.parse().unwrap());
    header.insert(CONTENT_LENGTH, body.len().to_string().parse().unwrap());
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
}

pub struct AttachPolicyBuiltin {}
#[async_trait::async_trait]
impl Operation for AttachPolicyBuiltin {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_builtin_policy_association(req, true).await
    }
}

pub struct DetachPolicyBuiltin {}
#[async_trait::async_trait]
impl Operation for DetachPolicyBuiltin {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_builtin_policy_association(req, false).await
    }
}

pub struct ListPolicyEntitiesBuiltin {}
#[async_trait::async_trait]
impl Operation for ListPolicyEntitiesBuiltin {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_builtin_policy_entities(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GroupPolicyEntities, PolicyAssociationReq, SetPolicyForUserOrGroupQuery, UserPolicyEntities, attach_policy_names,
        build_policy_mappings, detach_policy_names, direct_user_policy_names, parse_policy_entities_query,
        validate_policy_association_req,
    };
    use rustfs_madmin::UserInfo;

    #[test]
    fn set_policy_query_supports_external_parameter_names() {
        let query: SetPolicyForUserOrGroupQuery =
            serde_urlencoded::from_str("policy=readwrite&user-or-group=test-user&is-group=true").expect("query should parse");

        assert_eq!(query.policy_name, "readwrite");
        assert_eq!(query.user_or_group, "test-user");
        assert!(query.is_group);
    }

    #[test]
    fn set_policy_query_supports_rustfs_parameter_names() {
        let query: SetPolicyForUserOrGroupQuery =
            serde_urlencoded::from_str("policyName=readwrite&userOrGroup=test-user&isGroup=false").expect("query should parse");

        assert_eq!(query.policy_name, "readwrite");
        assert_eq!(query.user_or_group, "test-user");
        assert!(!query.is_group);
    }

    #[test]
    fn set_policy_query_allows_missing_policy_name_for_policy_removal() {
        let query: SetPolicyForUserOrGroupQuery =
            serde_urlencoded::from_str("userOrGroup=test-group&isGroup=true").expect("query should parse");

        assert!(query.policy_name.is_empty());
        assert_eq!(query.user_or_group, "test-group");
        assert!(query.is_group);
    }

    #[test]
    fn policy_association_req_requires_exactly_one_target() {
        let err = validate_policy_association_req(&PolicyAssociationReq {
            policies: vec!["readonly".to_string()],
            user: "user-a".to_string(),
            group: "group-a".to_string(),
        })
        .expect_err("request should be invalid");

        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn attach_policy_names_appends_only_missing_values() {
        let existing = vec!["readonly".to_string()];
        let requested = vec!["readonly".to_string(), "writeonly".to_string()];

        let (updated, attached) = attach_policy_names(&existing, &requested);

        assert_eq!(updated, vec!["readonly".to_string(), "writeonly".to_string()]);
        assert_eq!(attached, vec!["writeonly".to_string()]);
    }

    #[test]
    fn detach_policy_names_removes_only_requested_values() {
        let existing = vec!["readonly".to_string(), "writeonly".to_string()];
        let requested = vec!["writeonly".to_string(), "missing".to_string()];

        let (updated, detached) = detach_policy_names(&existing, &requested);

        assert_eq!(updated, vec!["readonly".to_string()]);
        assert_eq!(detached, vec!["writeonly".to_string()]);
    }

    #[test]
    fn policy_entities_query_supports_repeated_external_parameters() {
        let query = parse_policy_entities_query(Some("user=alice&user=bob&group=ops&policy=readonly&policy=writeonly"));

        assert_eq!(query.users, vec!["alice".to_string(), "bob".to_string()]);
        assert_eq!(query.groups, vec!["ops".to_string()]);
        assert_eq!(query.policies, vec!["readonly".to_string(), "writeonly".to_string()]);
    }

    #[test]
    fn build_policy_mappings_indexes_users_and_groups() {
        let user_mappings = vec![UserPolicyEntities {
            user: "alice".to_string(),
            policies: vec!["readonly".to_string()],
            member_of_mappings: Vec::new(),
        }];
        let group_mappings = vec![GroupPolicyEntities {
            group: "ops".to_string(),
            policies: vec!["readonly".to_string(), "writeonly".to_string()],
        }];

        let mappings = build_policy_mappings(&user_mappings, &group_mappings, &[]);

        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].policy, "readonly");
        assert_eq!(mappings[0].users, vec!["alice".to_string()]);
        assert_eq!(mappings[0].groups, vec!["ops".to_string()]);
        assert_eq!(mappings[1].policy, "writeonly");
        assert_eq!(mappings[1].groups, vec!["ops".to_string()]);
    }

    #[test]
    fn direct_user_policy_names_only_reads_direct_mapping() {
        let user_info = UserInfo {
            policy_name: Some("readonly,writeonly".to_string()),
            member_of: Some(vec!["disabled-group".to_string()]),
            ..Default::default()
        };

        assert_eq!(
            direct_user_policy_names(&user_info),
            vec!["readonly".to_string(), "writeonly".to_string()]
        );
    }
}
