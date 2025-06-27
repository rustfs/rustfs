use crate::{
    admin::{router::Operation, utils::has_space_be},
    auth::{check_key_valid, get_condition_values, get_session_token},
};
use ecstore::global::get_global_action_cred;
use http::{HeaderMap, StatusCode};
use madmin::{AccountStatus, AddOrUpdateUserReq};
use matchit::Params;
use policy::policy::{
    Args,
    action::{Action, AdminAction},
};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::{collections::HashMap, str::from_utf8};
use tracing::warn;

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
        let body = match input.store_all_unlimited().await {
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
            if sys_cred.access_key == ak {
                return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
            }
        }

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

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
        let conditions = get_condition_values(&req.headers, &cred);
        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::CreateUserAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        iam_store
            .create_user(ak, &args)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("create_user err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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

        if input_cred.access_key == ak {
            return Err(s3_error!(InvalidArgument, "can't change status of self"));
        }

        let status = AccountStatus::try_from(query.status.as_deref().unwrap_or_default())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, e))?;

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

        iam_store
            .set_user_status(ak, status)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("set_user_status err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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
        let query = {
            if let Some(query) = req.uri.query() {
                let input: BucketQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                BucketQuery::default()
            }
        };

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

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

        // let Some(input_cred) = req.credentials else {
        //     return Err(s3_error!(InvalidRequest, "get cred failed"));
        // };

        // let body = encrypt_data(input_cred.secret_key.expose().as_bytes(), &data)
        //     .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("encrypt_data err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct RemoveUser {}
#[async_trait::async_trait]
impl Operation for RemoveUser {
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

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

        let (is_temp, _) = iam_store
            .is_temp_user(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("is_temp_user err {e}")))?;

        if is_temp {
            return Err(s3_error!(InvalidArgument, "can't remove temp user"));
        }

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let sys_cred = get_global_action_cred()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "get_global_action_cred failed"))?;

        if ak == sys_cred.access_key || ak == cred.access_key {
            warn!(
                "can't remove self or system access key {}, {}, {}",
                ak, sys_cred.access_key, cred.access_key
            );
            return Err(s3_error!(InvalidArgument, "can't remove self"));
        }

        iam_store
            .delete_user(ak, true)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("delete_user err {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

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

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let deny_only = ak == cred.access_key;
        let conditions = get_condition_values(&req.headers, &cred);
        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::GetUserAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
                deny_only,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

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
