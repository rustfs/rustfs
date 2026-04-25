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

use crate::admin::handlers::site_replication::site_replication_iam_change_hook;
use crate::admin::utils::{encode_compatible_admin_payload, has_space_be, is_compat_admin_request, read_compatible_admin_body};
use crate::auth::{constant_time_eq, get_condition_values, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::{
    admin::router::{AdminOperation, Operation, S3Router},
    auth::check_key_valid,
};
use http::HeaderMap;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::{Credentials as StoredCredentials, get_global_action_cred};
use rustfs_iam::error::{is_err_no_such_service_account, is_err_no_such_temp_account};
use rustfs_iam::store::Store as IamStore;
use rustfs_iam::sys::{NewServiceAccountOpts, UpdateServiceAccountOpts};
use rustfs_madmin::{
    ACCESS_KEY_LIST_ALL, ACCESS_KEY_LIST_STS_ONLY, ACCESS_KEY_LIST_SVCACC_ONLY, ACCESS_KEY_LIST_USERS_ONLY, AddServiceAccountReq,
    AddServiceAccountResp, Credentials, InfoAccessKeyResp, InfoServiceAccountResp, LDAPSpecificAccessKeyInfo, ListAccessKeysResp,
    ListServiceAccountsResp, OpenIDSpecificAccessKeyInfo, SITE_REPL_API_VERSION, SRIAMItem, SRSessionPolicy, SRSvcAccChange,
    SRSvcAccCreate, SRSvcAccDelete, SRSvcAccUpdate, ServiceAccountInfo, TemporaryAccountInfoResp, UpdateServiceAccountReq,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_policy::policy::{Args, Policy};
use s3s::S3ErrorCode::InvalidRequest;
use s3s::header::CONTENT_LENGTH;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::{debug, warn};
use url::form_urlencoded;

fn sr_session_policy_from_value(value: Option<&serde_json::Value>) -> S3Result<SRSessionPolicy> {
    let Some(value) = value else {
        return Ok(SRSessionPolicy::default());
    };

    let raw = serde_json::to_string(value).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
    SRSessionPolicy::from_json(&raw).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))
}

fn compat_time_sentinel() -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH
}

fn list_expiration_or_sentinel(expiration: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    Some(expiration.unwrap_or_else(compat_time_sentinel))
}

fn expiration_for_admin_path(path: &str, expiration: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    if is_compat_admin_request(path) {
        list_expiration_or_sentinel(expiration)
    } else {
        expiration
    }
}

fn delete_service_account_success_status(path: &str) -> StatusCode {
    if is_compat_admin_request(path) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::OK
    }
}

fn merge_derived_service_account_claims(
    target_claims: &mut HashMap<String, serde_json::Value>,
    source_claims: &HashMap<String, serde_json::Value>,
) {
    for (key, value) in source_claims {
        if key == "exp" {
            continue;
        }
        target_claims.insert(key.clone(), value.clone());
    }
}

fn is_service_account_owner_of(caller: &StoredCredentials, target_parent_user: &str) -> bool {
    let caller_parent = if caller.parent_user.is_empty() {
        caller.access_key.as_str()
    } else {
        caller.parent_user.as_str()
    };

    caller_parent == target_parent_user
}

fn map_service_account_lookup_error(err: rustfs_iam::error::Error, action: &str) -> S3Error {
    debug!("{action}, e: {:?}", err);
    if is_err_no_such_service_account(&err) {
        s3_error!(InvalidRequest, "service account not exist")
    } else {
        s3_error!(InternalError, "{action}")
    }
}

fn map_temp_account_lookup_error(err: rustfs_iam::error::Error, action: &str) -> S3Error {
    debug!("{action}, e: {:?}", err);
    if is_err_no_such_temp_account(&err) {
        s3_error!(InvalidRequest, "access key not exist")
    } else {
        s3_error!(InternalError, "{action}")
    }
}

fn parse_update_service_account_policy(new_policy: Option<serde_json::Value>) -> S3Result<Option<Policy>> {
    let Some(policy) = new_policy else {
        return Ok(None);
    };

    let policy_bytes = serde_json::to_vec(&policy).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
    let sp = Policy::parse_config(&policy_bytes).map_err(|e| {
        debug!("parse policy failed, e: {:?}", e);
        s3_error!(InvalidArgument, "parse policy failed")
    })?;

    Ok(Some(sp))
}

pub fn register_service_account_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-service-account").as_str(),
        AdminOperation(&UpdateServiceAccount {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-service-account").as_str(),
        AdminOperation(&InfoServiceAccount {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/temporary-account-info").as_str(),
        AdminOperation(&TemporaryAccountInfo {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-access-key").as_str(),
        AdminOperation(&InfoAccessKey {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-service-accounts").as_str(),
        AdminOperation(&ListServiceAccount {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-access-keys-bulk").as_str(),
        AdminOperation(&ListAccessKeysBulk {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-accounts").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-account").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-accounts").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-account").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;

    Ok(())
}

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

        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;

        let create_req: AddServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        // create_req.expiration = create_req.expiration.and_then(|expire| expire.replace_millisecond(0).ok());

        if has_space_be(&create_req.access_key) {
            return Err(s3_error!(InvalidRequest, "access key has spaces"));
        }

        create_req.validate().map_err(|e| S3Error::with_message(InvalidRequest, e))?;

        let session_policy = if let Some(policy) = &create_req.policy {
            let policy_bytes =
                serde_json::to_vec(policy).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
            let p = Policy::parse_config(&policy_bytes).map_err(|e| {
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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false, // Always require explicit Allow permission
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
            description: create_req.description.or(create_req.comment),
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

                merge_derived_service_account_claims(opts.claims.as_mut().unwrap(), &claims);
            }
        }

        let replication_claims = opts.claims.clone().unwrap_or_default();
        let replication_policy = create_req
            .policy
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
        let replication_groups = target_groups.clone().unwrap_or_default();
        let replication_name = opts.name.clone().unwrap_or_default();
        let replication_description = opts.description.clone().unwrap_or_default();
        let replication_expiration = opts.expiration;

        let (new_cred, _) = iam_store
            .new_service_account(&target_user, target_groups, opts)
            .await
            .map_err(|e| {
                debug!("create service account failed, e: {:?}", e);
                s3_error!(InternalError, "create service account failed, e: {:?}", e)
            })?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "service-account".to_string(),
            svc_acc_change: Some(SRSvcAccChange {
                create: Some(SRSvcAccCreate {
                    parent: target_user.clone(),
                    access_key: new_cred.access_key.clone(),
                    secret_key: new_cred.secret_key.clone(),
                    groups: replication_groups,
                    claims: replication_claims,
                    session_policy: replication_policy
                        .as_deref()
                        .map(SRSessionPolicy::from_json)
                        .transpose()
                        .map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?
                        .unwrap_or_default(),
                    status: String::new(),
                    name: replication_name,
                    description: replication_description,
                    expiration: replication_expiration,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(OffsetDateTime::now_utc()),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %new_cred.access_key, error = ?err, "site replication add service account hook failed");
        }

        let resp = AddServiceAccountResp {
            credentials: Credentials {
                access_key: &new_cred.access_key,
                secret_key: &new_cred.secret_key,
                session_token: None,
                expiration: new_cred.expiration,
            },
        };

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

#[derive(Debug, Default, Deserialize)]
struct AccessKeyQuery {
    #[serde(rename = "accessKey", alias = "access-key")]
    pub access_key: String,
}

fn request_user_name(cred: &StoredCredentials) -> &str {
    if cred.parent_user.is_empty() {
        &cred.access_key
    } else {
        &cred.parent_user
    }
}

async fn build_info_service_account_resp<T: IamStore>(
    iam_store: &rustfs_iam::sys::IamSys<T>,
    account: &StoredCredentials,
    session_policy: Option<rustfs_policy::policy::Policy>,
) -> S3Result<InfoServiceAccountResp> {
    let implied_policy = session_policy
        .as_ref()
        .is_none_or(|policy| policy.version.is_empty() && policy.statements.is_empty());

    let effective_policy = if implied_policy {
        let policies = iam_store
            .policy_db_get(&account.parent_user, &account.groups)
            .await
            .map_err(|e| {
                debug!("get service account policy failed, e: {:?}", e);
                s3_error!(InternalError, "get service account policy failed")
            })?;

        Some(iam_store.get_combined_policy(&policies).await)
    } else {
        session_policy
    };

    let policy = effective_policy
        .map(|policy| {
            serde_json::to_string(&policy).map_err(|e| {
                debug!("marshal policy failed, e: {:?}", e);
                s3_error!(InternalError, "marshal policy failed")
            })
        })
        .transpose()?;

    Ok(InfoServiceAccountResp {
        parent_user: account.parent_user.clone(),
        account_status: account.status.clone(),
        implied_policy,
        name: account.name.clone(),
        description: account.description.clone(),
        expiration: account.expiration,
        policy,
    })
}

fn guess_user_provider(credentials: &StoredCredentials) -> &'static str {
    if !credentials.is_service_account() && !credentials.is_temp() {
        return "builtin";
    }

    let Some(claims) = credentials.claims.as_ref() else {
        return "builtin";
    };

    if claims.contains_key("ldap:user") || claims.contains_key("ldap:username") {
        return "ldap";
    }

    if claims.contains_key("sub") {
        return "openid";
    }

    "builtin"
}

fn ldap_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> LDAPSpecificAccessKeyInfo {
    let username = claims
        .and_then(|claims| {
            claims
                .get("ldap:user")
                .or_else(|| claims.get("ldap:username"))
                .and_then(|value| value.as_str())
        })
        .map(ToOwned::to_owned);

    LDAPSpecificAccessKeyInfo { username }
}

fn openid_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> OpenIDSpecificAccessKeyInfo {
    let user_id = claims
        .and_then(|claims| claims.get("sub"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let display_name = claims
        .and_then(|claims| claims.get("name"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);

    OpenIDSpecificAccessKeyInfo {
        config_name: None,
        user_id: user_id.clone(),
        user_id_claim: user_id.as_ref().map(|_| "sub".to_string()),
        display_name: display_name.clone(),
        display_name_claim: display_name.as_ref().map(|_| "name".to_string()),
    }
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

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        // let svc_account = iam_store.get_service_account(&access_key).await.map_err(|e| {
        //     debug!("get service account failed, e: {:?}", e);
        //     s3_error!(InternalError, "get service account failed")
        // })?;

        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), input_cred.secret_key.expose())
                .await?;

        let update_req: UpdateServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        update_req.validate().map_err(|e| S3Error::with_message(InvalidRequest, e))?;

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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let (svc_account, _) = iam_store
            .get_service_account(&access_key)
            .await
            .map_err(|e| map_service_account_lookup_error(e, "get service account failed"))?;

        if !is_service_account_owner_of(&cred, &svc_account.parent_user) {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let new_secret_key = update_req.new_secret_key.clone();
        let new_status = update_req.new_status.clone();
        let new_name = update_req.new_name.clone();
        let new_description = update_req.new_description.clone();
        let new_expiration = update_req.new_expiration;
        let new_policy = update_req.new_policy.clone();

        let sp = parse_update_service_account_policy(new_policy.clone())?;

        let opts = UpdateServiceAccountOpts {
            secret_key: new_secret_key.clone(),
            status: new_status.clone(),
            name: new_name.clone(),
            description: new_description.clone(),
            expiration: new_expiration,
            session_policy: sp,
        };

        let updated_at = iam_store
            .update_service_account(&access_key, opts)
            .await
            .map_err(|e| map_service_account_lookup_error(e, "update service account failed"))?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "service-account".to_string(),
            svc_acc_change: Some(SRSvcAccChange {
                update: Some(SRSvcAccUpdate {
                    access_key: access_key.clone(),
                    secret_key: new_secret_key.unwrap_or_default(),
                    status: new_status.unwrap_or_default(),
                    name: new_name.unwrap_or_default(),
                    description: new_description.unwrap_or_default(),
                    session_policy: sr_session_policy_from_value(new_policy.as_ref())?,
                    expiration: new_expiration,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %access_key, error = ?err, "site replication update service account hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::NO_CONTENT, Body::empty()), header))
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

        let (svc_account, session_policy) = iam_store
            .get_service_account(&access_key)
            .await
            .map_err(|e| map_service_account_lookup_error(e, "get service account failed"))?;

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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
            && request_user_name(&cred) != svc_account.parent_user
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let resp = build_info_service_account_resp(&iam_store, &svc_account, session_policy).await?;

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct TemporaryAccountInfo {}
#[async_trait::async_trait]
impl Operation for TemporaryAccountInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle TemporaryAccountInfo");

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

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListTemporaryAccountsAdminAction),
                bucket: "",
                conditions: &get_condition_values(
                    &req.headers,
                    &cred,
                    None,
                    None,
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let (temp_account, session_policy) = iam_store
            .get_temporary_account(&query.access_key)
            .await
            .map_err(|e| map_temp_account_lookup_error(e, "get temporary account failed"))?;

        let resp: TemporaryAccountInfoResp = build_info_service_account_resp(&iam_store, &temp_account, session_policy).await?;
        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

pub struct InfoAccessKey {}
#[async_trait::async_trait]
impl Operation for InfoAccessKey {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InfoAccessKey");

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let access_key = if query.access_key.is_empty() {
            cred.access_key.clone()
        } else {
            query.access_key
        };

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let target_cred = iam_store.get_user(&access_key).await.map(|identity| identity.credentials);

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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            let Some(target_cred) = target_cred.as_ref() else {
                return Err(s3_error!(AccessDenied, "access denied"));
            };

            if request_user_name(&cred) != target_cred.parent_user {
                return Err(s3_error!(AccessDenied, "access denied"));
            }
        }

        let Some(target_cred) = target_cred else {
            return Err(s3_error!(InvalidRequest, "access key not exist"));
        };

        let (user_type, session_policy) = if target_cred.is_temp() {
            let (_, session_policy) = iam_store.get_temporary_account(&access_key).await.map_err(|e| {
                debug!("get temporary account failed, e: {:?}", e);
                if is_err_no_such_temp_account(&e) {
                    s3_error!(InvalidRequest, "access key not exist")
                } else {
                    s3_error!(InternalError, "get temporary account failed")
                }
            })?;
            ("STS".to_string(), session_policy)
        } else if target_cred.is_service_account() {
            let (_, session_policy) = iam_store.get_service_account(&access_key).await.map_err(|e| {
                debug!("get service account failed, e: {:?}", e);
                if is_err_no_such_service_account(&e) {
                    s3_error!(InvalidRequest, "access key not exist")
                } else {
                    s3_error!(InternalError, "get service account failed")
                }
            })?;
            ("Service Account".to_string(), session_policy)
        } else {
            return Err(s3_error!(InvalidRequest, "access key not exist"));
        };

        let user_provider = guess_user_provider(&target_cred).to_string();
        let resp = InfoAccessKeyResp {
            access_key,
            info: build_info_service_account_resp(&iam_store, &target_cred, session_policy).await?,
            user_type,
            user_provider: user_provider.clone(),
            ldap_specific_info: if user_provider == "ldap" {
                ldap_specific_info(target_cred.claims.as_ref())
            } else {
                LDAPSpecificAccessKeyInfo::default()
            },
            open_id_specific_info: if user_provider == "openid" {
                openid_specific_info(target_cred.claims.as_ref())
            } else {
                OpenIDSpecificAccessKeyInfo::default()
            },
        };

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

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
            // Cross-user listing must be authorized by ListServiceAccounts, matching the
            // sibling InfoServiceAccount/InfoAccessKey/ListAccessKeysBulk handlers.
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
                        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                    ),
                    is_owner: owner,
                    object: "",
                    claims: cred.claims_or_empty(),
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
                expiration: expiration_for_admin_path(req.uri.path(), sa.expiration),
            })
            .collect();

        let data = serde_json::to_vec(&ListServiceAccountsResp { accounts })
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {e}")))?;
        let (data, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, data)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[derive(Debug, Deserialize)]
struct ListAccessKeysQuery {
    users: Vec<String>,
    all: bool,
    list_type: String,
}

fn default_access_key_list_type() -> String {
    ACCESS_KEY_LIST_ALL.to_string()
}

impl Default for ListAccessKeysQuery {
    fn default() -> Self {
        Self {
            users: Vec::new(),
            all: false,
            list_type: default_access_key_list_type(),
        }
    }
}

fn parse_bool_param(value: &str) -> bool {
    matches!(value, "true" | "1" | "on" | "yes")
}

fn parse_list_access_keys_query(query: Option<&str>) -> ListAccessKeysQuery {
    let mut parsed = ListAccessKeysQuery::default();

    let Some(query) = query else {
        return parsed;
    };

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "users" if !value.is_empty() => {
                parsed.users.push(value.into_owned());
            }
            "all" => parsed.all = parse_bool_param(value.as_ref()),
            "listType" => parsed.list_type = value.into_owned(),
            _ => {}
        }
    }

    parsed
}

pub struct ListAccessKeysBulk {}
#[async_trait::async_trait]
impl Operation for ListAccessKeysBulk {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListAccessKeysBulk");

        let query = { parse_list_access_keys_query(req.uri.query()) };

        if query.all && !query.users.is_empty() {
            return Err(s3_error!(InvalidRequest, "either specify users or all, not both"));
        }

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let mut requested_users = query.users;
        let mut self_only = !query.all && requested_users.is_empty();
        if !query.all && requested_users.len() == 1 {
            let requested_user = &requested_users[0];
            if requested_user == &cred.access_key || requested_user == &cred.parent_user {
                self_only = true;
            }
        }

        if query.all
            && !iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::AdminAction(AdminAction::ListUsersAdminAction),
                    bucket: "",
                    conditions: &get_condition_values(
                        &req.headers,
                        &cred,
                        None,
                        None,
                        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                    ),
                    is_owner: owner,
                    object: "",
                    claims: cred.claims_or_empty(),
                    deny_only: false,
                })
                .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: self_only,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        if self_only && requested_users.is_empty() {
            requested_users.push(request_user_name(&cred).to_string());
        }

        let checked_user_list = if query.all {
            let mut users = iam_store
                .list_users()
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("list users err {e}")))?
                .into_keys()
                .collect::<Vec<_>>();

            if let Some(sys_cred) = get_global_action_cred() {
                users.push(sys_cred.access_key);
            }
            users
        } else {
            // Keep requested identities as-is. Some valid parent users (for example external
            // identities) may not be persisted as regular IAM users, but can still own keys.
            requested_users
        };

        let (list_sts_keys, list_service_accounts) = match query.list_type.as_str() {
            ACCESS_KEY_LIST_USERS_ONLY => (false, false),
            ACCESS_KEY_LIST_STS_ONLY => (true, false),
            ACCESS_KEY_LIST_SVCACC_ONLY => (false, true),
            ACCESS_KEY_LIST_ALL => (true, true),
            _ => return Err(s3_error!(InvalidRequest, "invalid list type")),
        };

        let mut access_key_map = HashMap::new();
        for user in checked_user_list {
            let mut access_keys = ListAccessKeysResp::default();

            if list_sts_keys {
                let sts_keys = iam_store.list_sts_accounts(&user).await.map_err(|e| {
                    debug!("list sts account failed: {e:?}");
                    s3_error!(InternalError, "list sts account failed")
                })?;

                access_keys.sts_keys = sts_keys
                    .into_iter()
                    .map(|sts| ServiceAccountInfo {
                        parent_user: String::new(),
                        account_status: String::new(),
                        implied_policy: false,
                        access_key: sts.access_key,
                        name: sts.name,
                        description: sts.description,
                        expiration: expiration_for_admin_path(req.uri.path(), sts.expiration),
                    })
                    .collect();

                if !list_service_accounts && access_keys.sts_keys.is_empty() {
                    continue;
                }
            }

            if list_service_accounts {
                let service_accounts = iam_store.list_service_accounts(&user).await.map_err(|e| {
                    debug!("list service account failed: {e:?}");
                    s3_error!(InternalError, "list service account failed")
                })?;

                access_keys.service_accounts = service_accounts
                    .into_iter()
                    .map(|svc| ServiceAccountInfo {
                        parent_user: String::new(),
                        account_status: String::new(),
                        implied_policy: false,
                        access_key: svc.access_key,
                        name: svc.name,
                        description: svc.description,
                        expiration: expiration_for_admin_path(req.uri.path(), svc.expiration),
                    })
                    .collect();

                if !list_sts_keys && access_keys.service_accounts.is_empty() {
                    continue;
                }
            }

            access_key_map.insert(user, access_keys);
        }

        let data = serde_json::to_vec(&access_key_map)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal access keys err {e}")))?;
        let (data, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, data)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

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
                    req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                ),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
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

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "service-account".to_string(),
            svc_acc_change: Some(SRSvcAccChange {
                delete: Some(SRSvcAccDelete {
                    access_key: query.access_key.clone(),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(OffsetDateTime::now_utc()),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %query.access_key, error = ?err, "site replication delete service account hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers(
            (delete_service_account_success_status(req.uri.path()), Body::empty()),
            header,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA;
    use serde_json::json;
    use serde_urlencoded::from_bytes;

    #[test]
    fn access_key_query_supports_external_alias() {
        let query: AccessKeyQuery = from_bytes(b"access-key=test-access-key").expect("parse query");
        assert_eq!(query.access_key, "test-access-key");
    }

    #[test]
    fn guess_user_provider_detects_builtin_accounts() {
        let credentials = StoredCredentials {
            access_key: "builtin-user".to_string(),
            secret_key: "secret-key".to_string(),
            ..Default::default()
        };

        assert_eq!(guess_user_provider(&credentials), "builtin");
    }

    #[test]
    fn guess_user_provider_detects_ldap_accounts() {
        let credentials = StoredCredentials {
            access_key: "svc".to_string(),
            secret_key: "secret-key".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([
                (IAM_POLICY_CLAIM_NAME_SA.to_string(), json!("embedded")),
                ("ldap:user".to_string(), json!("uid=rustfs,ou=people,dc=example,dc=com")),
            ])),
            ..Default::default()
        };

        assert_eq!(guess_user_provider(&credentials), "ldap");
    }

    #[test]
    fn guess_user_provider_detects_openid_accounts() {
        let credentials = StoredCredentials {
            access_key: "sts".to_string(),
            secret_key: "secret-key".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([("sub".to_string(), json!("subject-123"))])),
            ..Default::default()
        };

        assert_eq!(guess_user_provider(&credentials), "openid");
    }

    #[test]
    fn provider_specific_info_uses_known_claims() {
        let claims = HashMap::from([
            ("ldap:user".to_string(), json!("uid=rustfs,ou=people,dc=example,dc=com")),
            ("sub".to_string(), json!("subject-123")),
            ("name".to_string(), json!("RustFS User")),
        ]);

        let ldap_info = ldap_specific_info(Some(&claims));
        let openid_info = openid_specific_info(Some(&claims));

        assert_eq!(ldap_info.username.as_deref(), Some("uid=rustfs,ou=people,dc=example,dc=com"));
        assert_eq!(openid_info.user_id.as_deref(), Some("subject-123"));
        assert_eq!(openid_info.user_id_claim.as_deref(), Some("sub"));
        assert_eq!(openid_info.display_name.as_deref(), Some("RustFS User"));
        assert_eq!(openid_info.display_name_claim.as_deref(), Some("name"));
    }

    #[test]
    fn list_access_keys_query_parses_external_parameters() {
        let query = parse_list_access_keys_query(Some("users=alice&users=bob&all=true&listType=svcacc-only"));

        assert_eq!(query.users, vec!["alice".to_string(), "bob".to_string()]);
        assert!(query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_SVCACC_ONLY);
    }

    #[test]
    fn list_access_keys_query_ignores_empty_users_values() {
        let query = parse_list_access_keys_query(Some("users=&users=alice&users=&listType=all"));

        assert_eq!(query.users, vec!["alice".to_string()]);
        assert!(!query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
    }

    #[test]
    fn list_access_keys_query_all_with_empty_users_does_not_conflict() {
        let query = parse_list_access_keys_query(Some("users=&all=true&listType=all"));

        assert!(query.users.is_empty());
        assert!(query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
        assert!(!query.all || query.users.is_empty());
    }

    #[test]
    fn list_access_keys_query_defaults_to_all_list_type() {
        let query = ListAccessKeysQuery::default();
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
    }

    #[test]
    fn list_service_account_cross_user_uses_list_service_accounts_action() {
        let src = include_str!("service_account.rs");
        let list_start = src
            .find("impl Operation for ListServiceAccount")
            .expect("ListServiceAccount operation should exist");
        let list_block = &src[list_start..];
        let list_end = list_block
            .find("struct ListAccessKeysQuery")
            .expect("ListAccessKeysQuery marker should exist");
        let list_block = &list_block[..list_end];

        assert!(
            list_block.contains("query.user.as_ref().is_some_and(") && list_block.contains("v != &cred.access_key"),
            "cross-user ListServiceAccount path should stay explicitly guarded"
        );
        assert!(
            list_block.contains("ListServiceAccountsAdminAction"),
            "cross-user ListServiceAccount should authorize with ListServiceAccountsAdminAction"
        );
        assert!(
            !list_block.contains("UpdateServiceAccountAdminAction"),
            "cross-user ListServiceAccount must not require UpdateServiceAccountAdminAction"
        );
    }

    #[test]
    fn delete_service_account_uses_external_success_status() {
        assert_eq!(
            delete_service_account_success_status("/minio/admin/v3/delete-service-account"),
            StatusCode::NO_CONTENT
        );
    }

    #[test]
    fn delete_service_account_keeps_rustfs_console_success_status() {
        assert_eq!(
            delete_service_account_success_status("/rustfs/admin/v3/delete-service-account"),
            StatusCode::OK
        );
    }

    #[test]
    fn expiration_for_external_admin_path_uses_sentinel() {
        assert_eq!(
            expiration_for_admin_path("/minio/admin/v3/list-service-accounts", None),
            Some(OffsetDateTime::UNIX_EPOCH)
        );
    }

    #[test]
    fn expiration_for_rustfs_admin_path_preserves_none() {
        assert_eq!(expiration_for_admin_path("/rustfs/admin/v3/list-service-accounts", None), None);
    }

    #[test]
    fn map_service_account_lookup_error_reports_missing_accounts_explicitly() {
        let err = map_service_account_lookup_error(
            rustfs_iam::error::Error::NoSuchServiceAccount("missing".to_string()),
            "get service account failed",
        );

        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("service account not exist"));
    }

    #[test]
    fn map_temp_account_lookup_error_reports_missing_access_keys_explicitly() {
        let err = map_temp_account_lookup_error(
            rustfs_iam::error::Error::NoSuchTempAccount("missing".to_string()),
            "get temporary account failed",
        );

        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("access key not exist"));
    }

    #[test]
    fn parse_update_service_account_policy_keeps_explicit_empty_policy() {
        let policy = parse_update_service_account_policy(Some(json!({}))).expect("empty policy should parse");

        assert!(policy.is_some());
        let policy = policy.unwrap();
        assert!(policy.version.is_empty());
        assert!(policy.statements.is_empty());
    }

    #[test]
    fn update_service_account_requires_requester_parent_match() {
        let parent_owner = StoredCredentials {
            access_key: "owner-user".to_string(),
            parent_user: String::new(),
            ..Default::default()
        };
        let derived_owner = StoredCredentials {
            access_key: "sa-user".to_string(),
            parent_user: "owner-user".to_string(),
            ..Default::default()
        };
        let foreign_user = StoredCredentials {
            access_key: "other".to_string(),
            parent_user: String::new(),
            ..Default::default()
        };

        assert!(is_service_account_owner_of(&parent_owner, "owner-user"));
        assert!(is_service_account_owner_of(&derived_owner, "owner-user"));
        assert!(!is_service_account_owner_of(&foreign_user, "owner-user"));
    }

    #[test]
    fn merge_derived_service_account_claims_skips_only_expiration() {
        let mut merged = HashMap::new();
        let source = HashMap::from([
            ("exp".to_string(), json!(123456)),
            ("parent".to_string(), json!("owner-user")),
            ("custom".to_string(), json!("value")),
        ]);

        merge_derived_service_account_claims(&mut merged, &source);

        assert!(!merged.contains_key("exp"));
        assert_eq!(merged.get("parent"), Some(&json!("owner-user")));
        assert_eq!(merged.get("custom"), Some(&json!("value")));
    }
}
