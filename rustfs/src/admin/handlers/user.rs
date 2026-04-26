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
use super::{account_info, group, service_account, user_iam, user_lifecycle, user_policy_binding};
use crate::{
    admin::{
        auth::validate_admin_request,
        handlers::site_replication::site_replication_iam_change_hook,
        router::{AdminOperation, Operation, S3Router},
        utils::{encode_compatible_admin_payload, has_space_be, read_compatible_admin_body},
    },
    auth::{check_key_valid, constant_time_eq, get_session_token},
    server::RemoteAddr,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::{MAX_ADMIN_REQUEST_BODY_SIZE, MAX_IAM_IMPORT_SIZE};
use rustfs_credentials::{Credentials, get_global_action_cred};
use rustfs_iam::{
    store::{GroupInfo, MappedPolicy, UserType},
    sys::{NewServiceAccountOpts, UpdateServiceAccountOpts},
};
use rustfs_madmin::{
    AccountStatus, AddOrUpdateUserReq, IAMEntities, IAMErrEntities, IAMErrEntity, IAMErrPolicyEntity, SITE_REPL_API_VERSION,
    SRIAMItem, SRIAMUser,
    user::{ImportIAMResult, SRSessionPolicy, SRSvcAccCreate},
};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::path::path_join_buf;
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::io::{Read as _, Write};
use std::{collections::HashMap, io::Cursor, str::from_utf8};
use tracing::{debug, warn};
use zip::{ZipArchive, ZipWriter, result::ZipError, write::SimpleFileOptions};

#[derive(Debug, Deserialize, Default)]
pub struct AddUserQuery {
    #[serde(rename = "accessKey", alias = "access-key")]
    pub access_key: Option<String>,
    pub status: Option<String>,
}

pub fn register_user_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    account_info::register_account_info_route(r)?;
    user_lifecycle::register_user_lifecycle_route(r)?;
    group::register_group_management_route(r)?;
    service_account::register_service_account_route(r)?;
    user_iam::register_user_iam_route(r)?;
    user_policy_binding::register_user_policy_binding_route(r)?;

    Ok(())
}

/// Returns the IAM parent user identity for a temporary (Console/STS) session credential.
///
/// Prefers `parent_user` when non-empty; otherwise reads the JWT `parent` claim.
/// Returns [`None`] when neither is available.
fn temp_identity_parent(requester: &Credentials) -> Option<&str> {
    if !requester.parent_user.is_empty() {
        return Some(requester.parent_user.as_str());
    }
    requester
        .claims
        .as_ref()
        .and_then(|c| c.get("parent"))
        .and_then(|v| v.as_str())
}

/// How the IAM parent identity was resolved for logging (no JWT claim values).
fn parent_identity_source(requester: &Credentials) -> &'static str {
    if !requester.parent_user.is_empty() {
        "parent_user_field"
    } else if requester.claims.as_ref().and_then(|c| c.get("parent")).is_some() {
        "jwt_parent"
    } else {
        "none"
    }
}

/// Returns `true` when admin policy checks should use `deny_only` mode (only explicit **Deny** blocks;
/// absence of **Allow** does not deny).
///
/// Eligible cases:
/// - **Long-term credentials**: target is the requester's own `access_key` and there is no `parent_user`.
/// - **Console/STS session (temp)**: target equals the IAM user this session represents, resolved via
///   [`temp_identity_parent`] as `parent_user` when set, else the JWT claim `parent` (some stores
///   persist only the token).
///
/// Service accounts always use full Allow/Deny evaluation.
fn should_check_deny_only(target_access_key: &str, requester: &Credentials) -> bool {
    if requester.is_service_account() {
        return false;
    }
    if requester.is_temp() {
        return temp_identity_parent(requester).is_some_and(|p| p == target_access_key);
    }
    target_access_key == requester.access_key && requester.parent_user.is_empty()
}

fn should_reject_group_import_name(group_name: &str, group_lookup: &rustfs_iam::error::Error) -> bool {
    has_space_be(group_name) || !matches!(group_lookup, rustfs_iam::error::Error::NoSuchGroup(_))
}

fn should_restore_group_as_disabled(status: &str) -> bool {
    status.eq_ignore_ascii_case(rustfs_iam::sys::STATUS_DISABLED)
}

fn imported_service_account_status(status: &str) -> Option<String> {
    if status.eq_ignore_ascii_case(rustfs_policy::auth::ACCOUNT_OFF)
        || status.eq_ignore_ascii_case(rustfs_madmin::AccountStatus::Disabled.as_ref())
    {
        return Some(rustfs_policy::auth::ACCOUNT_OFF.to_string());
    }

    if status.eq_ignore_ascii_case(rustfs_policy::auth::ACCOUNT_ON)
        || status.eq_ignore_ascii_case(rustfs_madmin::AccountStatus::Enabled.as_ref())
    {
        return Some(rustfs_policy::auth::ACCOUNT_ON.to_string());
    }

    None
}

pub struct AddUser {}
#[async_trait::async_trait]
impl Operation for AddUser {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddUserQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AddUserQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let ak = query.access_key.as_deref().unwrap_or_default();

        if ak.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;

        // let body_bytes = decrypt_data(input_cred.secret_key.expose().as_bytes(), &body)
        //     .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("decrypt_data err {}", e)))?;

        let args: AddOrUpdateUserReq = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        if args.secret_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        if let Some(sys_cred) = get_global_action_cred()
            && constant_time_eq(&sys_cred.access_key, ak)
        {
            return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        if let Some(user) = iam_store.get_user(ak).await {
            if (user.credentials.is_temp() || user.credentials.is_service_account()) && cred.parent_user == ak {
                return Err(s3_error!(InvalidArgument, "can't create user with service account access key"));
            }
        } else if has_space_be(ak) {
            return Err(s3_error!(InvalidArgument, "access key has space"));
        }

        if from_utf8(ak.as_bytes()).is_err() {
            return Err(s3_error!(InvalidArgument, "access key is not utf8"));
        }

        let check_deny_only = should_check_deny_only(ak, &cred);

        debug!(
            target = "rustfs::admin::handlers::user",
            operation = "AddUser",
            query_access_key = %ak,
            signer_access_key = %cred.access_key,
            is_temp = cred.is_temp(),
            is_service_account = cred.is_service_account(),
            parent_user = %cred.parent_user,
            parent_identity_source = %parent_identity_source(&cred),
            jwt_parent_claim_present = cred.claims.as_ref().and_then(|c| c.get("parent")).is_some(),
            check_deny_only,
            is_owner = owner,
            "authorization context before validate_admin_request (no secrets)"
        );

        // For eligible self operations, only explicit Deny should block the request.
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            check_deny_only,
            vec![Action::AdminAction(AdminAction::CreateUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let updated_at = iam_store
            .create_user(ak, &args)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("create_user err {e}")))?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "iam-user".to_string(),
            iam_user: Some(SRIAMUser {
                access_key: ak.to_string(),
                is_delete_req: false,
                user_req: Some(args.clone()),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %ak, error = ?err, "site replication create user hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct SetUserStatus {}
#[async_trait::async_trait]
impl Operation for SetUserStatus {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddUserQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                AddUserQuery::default()
            }
        };

        let ak = query.access_key.as_deref().unwrap_or_default();

        if ak.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        if constant_time_eq(&input_cred.access_key, ak) {
            return Err(s3_error!(InvalidArgument, "can't change status of self"));
        }

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::EnableUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let status = AccountStatus::try_from(query.status.as_deref().unwrap_or_default())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, e))?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        iam_store
            .set_user_status(ak, status)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("set_user_status err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct BucketQuery {
    #[serde(rename = "bucket")]
    pub bucket: String,
}
pub struct ListUsers {}
#[async_trait::async_trait]
impl Operation for ListUsers {
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
            vec![Action::AdminAction(AdminAction::ListUsersAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: BucketQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                BucketQuery::default()
            }
        };

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let users = {
            if !query.bucket.is_empty() {
                iam_store
                    .list_bucket_users(query.bucket.as_str())
                    .await
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?
            } else {
                iam_store
                    .list_users()
                    .await
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?
            }
        };

        let data = serde_json::to_vec(&users)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {e}")))?;
        let (data, content_type) = encode_compatible_admin_payload(req.uri.path(), &cred.secret_key, data)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct RemoveUser {}
#[async_trait::async_trait]
impl Operation for RemoveUser {
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
            vec![Action::AdminAction(AdminAction::DeleteUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddUserQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                AddUserQuery::default()
            }
        };

        let ak = query.access_key.as_deref().unwrap_or_default();

        if ak.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let sys_cred = get_global_action_cred()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "get_global_action_cred failed"))?;

        if ak == sys_cred.access_key || ak == cred.access_key || cred.parent_user == ak {
            return Err(s3_error!(InvalidArgument, "can't remove self"));
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let (is_temp, _) = iam_store
            .is_temp_user(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("is_temp_user err {e}")))?;

        if is_temp {
            return Err(s3_error!(InvalidArgument, "can't remove temp user"));
        }

        let (is_service_account, _) = iam_store
            .is_service_account(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("is_service_account err {e}")))?;
        if is_service_account {
            return Err(s3_error!(InvalidArgument, "can't remove service account"));
        }

        iam_store
            .delete_user(ak, true)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("delete_user err {e}")))?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "iam-user".to_string(),
            iam_user: Some(SRIAMUser {
                access_key: ak.to_string(),
                is_delete_req: true,
                user_req: None,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(time::OffsetDateTime::now_utc()),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %ak, error = ?err, "site replication delete user hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct GetUserInfo {}
#[async_trait::async_trait]
impl Operation for GetUserInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: AddUserQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                AddUserQuery::default()
            }
        };

        let ak = query.access_key.as_deref().unwrap_or_default();

        if ak.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let check_deny_only = should_check_deny_only(ak, &cred);

        debug!(
            target = "rustfs::admin::handlers::user",
            operation = "GetUserInfo",
            query_access_key = %ak,
            signer_access_key = %cred.access_key,
            is_temp = cred.is_temp(),
            is_service_account = cred.is_service_account(),
            parent_user = %cred.parent_user,
            parent_identity_source = %parent_identity_source(&cred),
            jwt_parent_claim_present = cred.claims.as_ref().and_then(|c| c.get("parent")).is_some(),
            check_deny_only,
            is_owner = owner,
            "authorization context before validate_admin_request (no secrets)"
        );

        // For eligible self operations, only explicit Deny should block the request.
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            check_deny_only,
            vec![Action::AdminAction(AdminAction::GetUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let info = iam_store.get_user_info(ak).await.map_err(iam_error_to_s3_error)?;

        let data = serde_json::to_vec(&info)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal user err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

const ALL_POLICIES_FILE: &str = "policies.json";
const ALL_USERS_FILE: &str = "users.json";
const ALL_GROUPS_FILE: &str = "groups.json";
const ALL_SVC_ACCTS_FILE: &str = "svcaccts.json";
const USER_POLICY_MAPPINGS_FILE: &str = "user_mappings.json";
const GROUP_POLICY_MAPPINGS_FILE: &str = "group_mappings.json";
const STS_USER_POLICY_MAPPINGS_FILE: &str = "stsuser_mappings.json";
const GROUP_POLICY_MAPPING_USER_TYPE: UserType = UserType::Reg;

const IAM_ASSETS_DIR: &str = "iam-assets";

const IAM_EXPORT_FILES: &[&str] = &[
    ALL_POLICIES_FILE,
    ALL_USERS_FILE,
    ALL_GROUPS_FILE,
    ALL_SVC_ACCTS_FILE,
    USER_POLICY_MAPPINGS_FILE,
    GROUP_POLICY_MAPPINGS_FILE,
    STS_USER_POLICY_MAPPINGS_FILE,
];

pub struct ExportIam {}
#[async_trait::async_trait]
impl Operation for ExportIam {
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
            vec![Action::AdminAction(AdminAction::ExportIAMAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let mut zip_writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default();

        for &file in IAM_EXPORT_FILES {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, file]);
            match file {
                ALL_POLICIES_FILE => {
                    let policies: HashMap<String, rustfs_policy::policy::Policy> = iam_store
                        .list_polices("")
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    let json_str = serde_json::to_vec(&policies)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                ALL_USERS_FILE => {
                    let mut users = HashMap::new();
                    iam_store
                        .load_users(UserType::Reg, &mut users)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                    let users: HashMap<String, AddOrUpdateUserReq> = users
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k,
                                AddOrUpdateUserReq {
                                    secret_key: v.credentials.secret_key,
                                    status: {
                                        if v.credentials.status == "off" {
                                            AccountStatus::Disabled
                                        } else {
                                            AccountStatus::Enabled
                                        }
                                    },
                                    policy: None,
                                },
                            )
                        })
                        .collect::<HashMap<String, AddOrUpdateUserReq>>();

                    let json_str = serde_json::to_vec(&users)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                ALL_GROUPS_FILE => {
                    let mut groups: HashMap<String, GroupInfo> = HashMap::new();
                    iam_store
                        .load_groups(&mut groups)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                    let json_str = serde_json::to_vec(&groups)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                ALL_SVC_ACCTS_FILE => {
                    let mut service_accounts = HashMap::new();
                    iam_store
                        .load_users(UserType::Svc, &mut service_accounts)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                    let mut svc_accts: HashMap<String, SRSvcAccCreate> = HashMap::new();
                    for (k, acc) in service_accounts {
                        if k == "siteReplicatorSvcAcc" {
                            continue;
                        }

                        let claims = iam_store
                            .get_claims_for_svc_acc(&acc.credentials.access_key)
                            .await
                            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                        let (sa, police) = iam_store
                            .get_service_account(&acc.credentials.access_key)
                            .await
                            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                        let police_json = if let Some(police) = police {
                            serde_json::to_string(&police)
                                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?
                        } else {
                            "null".to_string()
                        };

                        let svc_acc_create_req = SRSvcAccCreate {
                            parent: acc.credentials.parent_user,
                            access_key: k.clone(),
                            secret_key: acc.credentials.secret_key,
                            groups: acc.credentials.groups.unwrap_or_default(),
                            claims,
                            session_policy: SRSessionPolicy::from_json(&police_json).unwrap_or_default(),
                            status: acc.credentials.status,
                            name: sa.name.unwrap_or_default(),
                            description: sa.description.unwrap_or_default(),
                            expiration: sa.expiration,
                            api_version: None,
                        };
                        svc_accts.insert(k.clone(), svc_acc_create_req);
                    }

                    let json_str = serde_json::to_vec(&svc_accts)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                USER_POLICY_MAPPINGS_FILE => {
                    let mut user_policy_mappings: HashMap<String, MappedPolicy> = HashMap::new();
                    iam_store
                        .load_mapped_policies(UserType::Reg, false, &mut user_policy_mappings)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                    let json_str = serde_json::to_vec(&user_policy_mappings)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                GROUP_POLICY_MAPPINGS_FILE => {
                    let mut group_policy_mappings = HashMap::new();
                    iam_store
                        .load_mapped_policies(GROUP_POLICY_MAPPING_USER_TYPE, true, &mut group_policy_mappings)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

                    let json_str = serde_json::to_vec(&group_policy_mappings)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                STS_USER_POLICY_MAPPINGS_FILE => {
                    let mut sts_user_policy_mappings: HashMap<String, MappedPolicy> = HashMap::new();
                    iam_store
                        .load_mapped_policies(UserType::Sts, false, &mut sts_user_policy_mappings)
                        .await
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    let json_str = serde_json::to_vec(&sts_user_policy_mappings)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .start_file(file_path, options)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    zip_writer
                        .write_all(&json_str)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                }
                _ => continue,
            }
        }

        let zip_bytes = zip_writer
            .finish()
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/zip".parse().unwrap());
        header.insert(CONTENT_DISPOSITION, "attachment; filename=iam-assets.zip".parse().unwrap());
        header.insert(CONTENT_LENGTH, zip_bytes.get_ref().len().to_string().parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(zip_bytes.into_inner())), header))
    }
}

pub struct ImportIam {}
#[async_trait::async_trait]
impl Operation for ImportIam {
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
            vec![Action::AdminAction(AdminAction::ImportIAMAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_IAM_IMPORT_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };

        let mut zip_reader =
            ZipArchive::new(Cursor::new(body)).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let skipped = IAMEntities::default();
        let mut removed = IAMEntities::default();
        let mut added = IAMEntities::default();
        let mut failed = IAMErrEntities::default();

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, ALL_POLICIES_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let policies: HashMap<String, rustfs_policy::policy::Policy> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (name, policy) in policies {
                    if policy.is_empty() {
                        let res = iam_store.delete_policy(&name, true).await;
                        removed.policies.push(name.clone());
                        if let Err(e) = res {
                            return Err(s3_error!(InternalError, "delete policy failed, name: {name}, err: {e}"));
                        }
                        continue;
                    }

                    let res = iam_store.set_policy(&name, policy).await;
                    added.policies.push(name.clone());
                    if let Err(e) = res {
                        return Err(s3_error!(InternalError, "set policy failed, name: {name}, err: {e}"));
                    }
                }
            }
        }

        let Some(sys_cred) = get_global_action_cred() else {
            return Err(s3_error!(InvalidRequest, "get sys cred failed"));
        };

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, ALL_USERS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let users: HashMap<String, AddOrUpdateUserReq> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (ak, req) in users {
                    if ak == sys_cred.access_key {
                        return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
                    }

                    if let Some(u) = iam_store.get_user(&ak).await {
                        if u.credentials.is_temp() || u.credentials.is_service_account() {
                            return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
                        }
                    } else if has_space_be(&ak) {
                        return Err(s3_error!(InvalidArgument, "has space be"));
                    }

                    if let Err(e) = iam_store.create_user(&ak, &req).await {
                        failed.users.push(IAMErrEntity {
                            name: ak.clone(),
                            error: e.to_string(),
                        });
                    } else {
                        added.users.push(ak.clone());
                    }
                }
            }
        }

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, ALL_GROUPS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let groups: HashMap<String, GroupInfo> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (group_name, group_info) in groups {
                    if let Err(e) = iam_store.get_group_description(&group_name).await
                        && should_reject_group_import_name(&group_name, &e)
                    {
                        return Err(s3_error!(InvalidArgument, "group not found or has space be"));
                    }

                    if let Err(e) = iam_store.add_users_to_group(&group_name, group_info.members.clone()).await {
                        failed.groups.push(IAMErrEntity {
                            name: group_name.clone(),
                            error: e.to_string(),
                        });
                    } else {
                        if should_restore_group_as_disabled(&group_info.status) {
                            iam_store.set_group_status(&group_name, false).await.map_err(|e| {
                                S3Error::with_message(
                                    S3ErrorCode::InternalError,
                                    format!("set group status failed, name: {group_name}, err: {e}"),
                                )
                            })?;
                        }
                        added.groups.push(group_name.clone());
                    }
                }
            }
        }

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, ALL_SVC_ACCTS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let svc_accts: HashMap<String, SRSvcAccCreate> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (ak, req) in svc_accts {
                    if skipped.service_accounts.contains(&ak) {
                        continue;
                    }

                    let sp = if let Some(ps) = req.session_policy.as_str() {
                        let sp = rustfs_policy::policy::Policy::parse_config(ps.as_bytes())
                            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                        Some(sp)
                    } else {
                        None
                    };

                    if has_space_be(&ak) {
                        return Err(s3_error!(InvalidArgument, "has space be {ak}"));
                    }

                    let mut update = true;

                    if let Err(e) = iam_store.get_service_account(&req.access_key).await {
                        if !matches!(e, rustfs_iam::error::Error::NoSuchServiceAccount(_)) {
                            return Err(s3_error!(InvalidArgument, "failed to get service account {ak} {e}"));
                        }
                        update = false;
                    }

                    if update {
                        iam_store.delete_service_account(&req.access_key, true).await.map_err(|e| {
                            S3Error::with_message(
                                S3ErrorCode::InternalError,
                                format!("failed to delete service account {ak} {e}"),
                            )
                        })?;
                    }

                    let opts = NewServiceAccountOpts {
                        session_policy: sp,
                        access_key: ak.clone(),
                        secret_key: req.secret_key,
                        name: Some(req.name),
                        description: Some(req.description),
                        expiration: req.expiration,
                        allow_site_replicator_account: false,
                        claims: Some(req.claims),
                    };

                    let groups = if req.groups.is_empty() { None } else { Some(req.groups) };

                    if let Err(e) = iam_store.new_service_account(&req.parent, groups, opts).await {
                        failed.service_accounts.push(IAMErrEntity {
                            name: ak.clone(),
                            error: e.to_string(),
                        });
                    } else {
                        if let Some(status) = imported_service_account_status(&req.status)
                            && status == rustfs_policy::auth::ACCOUNT_OFF
                        {
                            iam_store
                                .update_service_account(
                                    &ak,
                                    UpdateServiceAccountOpts {
                                        session_policy: None,
                                        secret_key: None,
                                        name: None,
                                        description: None,
                                        expiration: None,
                                        status: Some(status),
                                    },
                                )
                                .await
                                .map_err(|e| {
                                    S3Error::with_message(
                                        S3ErrorCode::InternalError,
                                        format!("update service account status failed, name: {ak}, err: {e}"),
                                    )
                                })?;
                        }
                        added.service_accounts.push(ak.clone());
                    }
                }
            }
        }

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, USER_POLICY_MAPPINGS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let user_policy_mappings: HashMap<String, MappedPolicy> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (user_name, policies) in user_policy_mappings {
                    let has_temp = match iam_store.is_temp_user(&user_name).await {
                        Ok((has_temp, _)) => has_temp,
                        Err(e) => {
                            if !matches!(e, rustfs_iam::error::Error::NoSuchUser(_)) {
                                return Err(s3_error!(InternalError, "is temp user failed, name: {user_name}, err: {e}"));
                            }
                            false
                        }
                    };

                    if has_temp {
                        return Err(s3_error!(InvalidArgument, "can't set policy for temp user {user_name}"));
                    }

                    if let Err(e) = iam_store
                        .policy_db_set(&user_name, UserType::Reg, false, &policies.policies)
                        .await
                    {
                        failed.user_policies.push(IAMErrPolicyEntity {
                            name: user_name.clone(),
                            error: e.to_string(),
                            policies: policies.policies.split(',').map(|s| s.to_string()).collect(),
                        });
                    } else {
                        added.user_policies.push(HashMap::from([(
                            user_name.clone(),
                            policies.policies.split(',').map(|s| s.to_string()).collect(),
                        )]));
                    }
                }
            }
        }

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, GROUP_POLICY_MAPPINGS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let group_policy_mappings: HashMap<String, MappedPolicy> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (group_name, policies) in group_policy_mappings {
                    if skipped.groups.contains(&group_name) {
                        continue;
                    }

                    if let Err(e) = iam_store
                        .policy_db_set(&group_name, GROUP_POLICY_MAPPING_USER_TYPE, true, &policies.policies)
                        .await
                    {
                        failed.group_policies.push(IAMErrPolicyEntity {
                            name: group_name.clone(),
                            error: e.to_string(),
                            policies: policies.policies.split(',').map(|s| s.to_string()).collect(),
                        });
                    } else {
                        added.group_policies.push(HashMap::from([(
                            group_name.clone(),
                            policies.policies.split(',').map(|s| s.to_string()).collect(),
                        )]));
                    }
                }
            }
        }

        {
            let file_path = path_join_buf(&[IAM_ASSETS_DIR, STS_USER_POLICY_MAPPINGS_FILE]);
            let file_content = match zip_reader.by_name(file_path.as_str()) {
                Err(ZipError::FileNotFound) => None,
                Err(_) => return Err(s3_error!(InvalidRequest, "get file failed")),
                Ok(file) => {
                    let mut file = file;
                    let mut file_content = Vec::new();
                    file.read_to_end(&mut file_content)
                        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                    Some(file_content)
                }
            };

            if let Some(file_content) = file_content {
                let sts_user_policy_mappings: HashMap<String, MappedPolicy> = serde_json::from_slice(&file_content)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
                for (user_name, policies) in sts_user_policy_mappings {
                    if skipped.users.contains(&user_name) {
                        continue;
                    }

                    let has_temp = match iam_store.is_temp_user(&user_name).await {
                        Ok((has_temp, _)) => has_temp,
                        Err(e) => {
                            if !matches!(e, rustfs_iam::error::Error::NoSuchUser(_)) {
                                return Err(s3_error!(InternalError, "is temp user failed, name: {user_name}, err: {e}"));
                            }
                            false
                        }
                    };

                    if has_temp {
                        return Err(s3_error!(InvalidArgument, "can't set policy for temp user {user_name}"));
                    }

                    if let Err(e) = iam_store
                        .policy_db_set(&user_name, UserType::Sts, false, &policies.policies)
                        .await
                    {
                        failed.sts_policies.push(IAMErrPolicyEntity {
                            name: user_name.clone(),
                            error: e.to_string(),
                            policies: policies.policies.split(',').map(|s| s.to_string()).collect(),
                        });
                    } else {
                        added.sts_policies.push(HashMap::from([(
                            user_name.clone(),
                            policies.policies.split(',').map(|s| s.to_string()).collect(),
                        )]));
                    }
                }
            }
        }

        let ret = ImportIAMResult {
            skipped,
            removed,
            added,
            failed,
        };

        let body = serde_json::to_vec(&ret).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), header))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GROUP_POLICY_MAPPING_USER_TYPE, imported_service_account_status, should_check_deny_only, should_reject_group_import_name,
        should_restore_group_as_disabled,
    };
    use rustfs_credentials::{Credentials, IAM_POLICY_CLAIM_NAME_SA};
    use rustfs_iam::error::Error as IamError;
    use rustfs_madmin::user::SRSvcAccCreate;
    use serde_json::Value;
    use std::collections::HashMap;

    #[test]
    fn test_should_check_deny_only_for_regular_self_request() {
        let cred = Credentials {
            access_key: "alice".to_string(),
            ..Default::default()
        };
        assert!(should_check_deny_only("alice", &cred));
    }

    #[test]
    fn test_should_not_check_deny_only_for_other_user_request() {
        let cred = Credentials {
            access_key: "bob".to_string(),
            ..Default::default()
        };
        assert!(!should_check_deny_only("alice", &cred));
    }

    #[test]
    fn test_should_not_check_deny_only_for_temp_without_parent() {
        // Session token present but no parent_user — not a well-formed self session for deny_only.
        let cred = Credentials {
            access_key: "alice".to_string(),
            session_token: "temp-token".to_string(),
            ..Default::default()
        };
        assert!(!should_check_deny_only("alice", &cred));
    }

    #[test]
    fn test_should_check_deny_only_for_temp_when_target_matches_parent_user() {
        // Console/AssumeRole: signing AK is a session key; IAM user is parent_user.
        let cred = Credentials {
            access_key: "VV0V3VYJK2PV6EG45X2Y".to_string(),
            session_token: "jwt-session-token".to_string(),
            parent_user: "1923".to_string(),
            ..Default::default()
        };
        assert!(should_check_deny_only("1923", &cred));
        assert!(!should_check_deny_only("1924", &cred));
        assert!(!should_check_deny_only("VV0V3VYJK2PV6EG45X2Y", &cred));
    }

    #[test]
    fn test_should_check_deny_only_for_temp_when_parent_only_in_jwt_claims() {
        // STS identity may omit `parentUser` on disk; `check_key_valid` still fills `claims["parent"]`.
        let mut claims = HashMap::new();
        claims.insert("parent".to_string(), Value::String("1923".to_string()));
        let cred = Credentials {
            access_key: "39KNO04Z34D6T4AGL6E6".to_string(),
            session_token: "jwt-session-token".to_string(),
            claims: Some(claims),
            ..Default::default()
        };
        assert!(should_check_deny_only("1923", &cred));
        assert!(!should_check_deny_only("1924", &cred));
    }

    #[test]
    fn test_should_not_check_deny_only_for_service_account_credentials() {
        let mut claims = HashMap::new();
        claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_string(), Value::String("policy".to_string()));
        let cred = Credentials {
            access_key: "alice".to_string(),
            parent_user: "parent-user".to_string(),
            claims: Some(claims),
            ..Default::default()
        };
        assert!(!should_check_deny_only("alice", &cred));
    }

    #[test]
    fn test_should_not_check_deny_only_when_parent_user_present() {
        let cred = Credentials {
            access_key: "alice".to_string(),
            parent_user: "parent-user".to_string(),
            ..Default::default()
        };
        assert!(!should_check_deny_only("alice", &cred));
    }

    #[test]
    fn test_group_import_allows_missing_group_without_spaces() {
        assert!(!should_reject_group_import_name(
            "new-group",
            &IamError::NoSuchGroup("new-group".to_string())
        ));
    }

    #[test]
    fn test_group_import_rejects_group_names_with_spaces() {
        assert!(should_reject_group_import_name(
            " bad-group",
            &IamError::NoSuchGroup(" bad-group".to_string())
        ));
    }

    #[test]
    fn test_group_import_restores_disabled_status_only_when_needed() {
        assert!(should_restore_group_as_disabled("disabled"));
        assert!(!should_restore_group_as_disabled("enabled"));
    }

    #[test]
    fn test_imported_service_account_status_maps_on_and_off() {
        assert_eq!(imported_service_account_status("off").as_deref(), Some("off"));
        assert_eq!(imported_service_account_status("on").as_deref(), Some("on"));
        assert_eq!(imported_service_account_status("disabled").as_deref(), Some("off"));
        assert_eq!(imported_service_account_status("enabled").as_deref(), Some("on"));
        assert!(imported_service_account_status("unknown").is_none());
    }

    #[test]
    fn test_service_account_import_accepts_null_groups_and_epoch_expiration() {
        let payload = r#"{
            "svcalpha": {
                "parent": "useralpha",
                "accessKey": "svcalpha",
                "secretKey": "svcAlphaSecret123",
                "groups": null,
                "claims": {
                    "accessKey": "svcalpha",
                    "parent": "useralpha",
                    "sa-policy": "inherited-policy"
                },
                "sessionPolicy": null,
                "status": "on",
                "name": "uploaderKey",
                "description": "alpha upload key",
                "expiration": "1970-01-01T00:00:00Z"
            }
        }"#;

        let svc_accts: HashMap<String, SRSvcAccCreate> = serde_json::from_str(payload).unwrap();
        let svc = svc_accts.get("svcalpha").unwrap();

        assert!(svc.groups.is_empty());
        assert!(svc.expiration.is_none());
    }

    #[test]
    fn test_service_account_import_preserves_non_zero_expiration() {
        let payload = r#"{
            "svcalpha": {
                "parent": "useralpha",
                "accessKey": "svcalpha",
                "secretKey": "svcAlphaSecret123",
                "groups": [],
                "claims": {},
                "sessionPolicy": null,
                "status": "on",
                "name": "uploaderKey",
                "description": "alpha upload key",
                "expiration": "2030-01-02T03:04:05Z"
            }
        }"#;

        let svc_accts: HashMap<String, SRSvcAccCreate> = serde_json::from_str(payload).unwrap();
        let svc = svc_accts.get("svcalpha").unwrap();

        assert_eq!(svc.expiration.map(|expiration| expiration.unix_timestamp()), Some(1893553445));
    }

    #[test]
    fn test_group_policy_mappings_use_regular_user_type() {
        assert_eq!(GROUP_POLICY_MAPPING_USER_TYPE, rustfs_iam::store::UserType::Reg);
    }
}
