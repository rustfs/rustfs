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
    server::RemoteAddr,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::{MAX_ADMIN_REQUEST_BODY_SIZE, MAX_IAM_IMPORT_SIZE};
use rustfs_credentials::get_global_action_cred;
use rustfs_iam::{
    store::{GroupInfo, MappedPolicy, UserType},
    sys::NewServiceAccountOpts,
};
use rustfs_madmin::{
    AccountStatus, AddOrUpdateUserReq, IAMEntities, IAMErrEntities, IAMErrEntity, IAMErrPolicyEntity,
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
use tracing::warn;
use zip::{ZipArchive, ZipWriter, result::ZipError, write::SimpleFileOptions};

#[derive(Debug, Deserialize, Default)]
pub struct AddUserQuery {
    #[serde(rename = "accessKey")]
    pub access_key: Option<String>,
    pub status: Option<String>,
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

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };

        // let body_bytes = decrypt_data(input_cred.secret_key.expose().as_bytes(), &body)
        //     .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("decrypt_data err {}", e)))?;

        let args: AddOrUpdateUserReq = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {e}")))?;

        if args.secret_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        if let Some(sys_cred) = get_global_action_cred() {
            if constant_time_eq(&sys_cred.access_key, ak) {
                return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
            }
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

        let deny_only = ak == cred.access_key;
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            deny_only,
            vec![Action::AdminAction(AdminAction::CreateUserAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        iam_store
            .create_user(ak, &args)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("create_user err {e}")))?;

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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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

        // TODO: IAMChangeHook

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

        let deny_only = ak == cred.access_key;
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            deny_only,
            vec![Action::AdminAction(AdminAction::GetUserAdminAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
        )
        .await?;

        let info = iam_store
            .get_user_info(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

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
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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
                        .load_mapped_policies(UserType::Reg, true, &mut group_policy_mappings)
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
            vec![Action::AdminAction(AdminAction::ExportIAMAction)],
            req.extensions.get::<RemoteAddr>().map(|a| a.0),
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
                    if let Err(e) = iam_store.get_group_description(&group_name).await {
                        if matches!(e, rustfs_iam::error::Error::NoSuchGroup(_)) || has_space_be(&group_name) {
                            return Err(s3_error!(InvalidArgument, "group not found or has space be"));
                        }
                    }

                    if let Err(e) = iam_store.add_users_to_group(&group_name, group_info.members.clone()).await {
                        failed.groups.push(IAMErrEntity {
                            name: group_name.clone(),
                            error: e.to_string(),
                        });
                    } else {
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
                        .policy_db_set(&group_name, UserType::None, true, &policies.policies)
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
