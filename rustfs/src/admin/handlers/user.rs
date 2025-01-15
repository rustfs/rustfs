use http::{HeaderMap, StatusCode};
use iam::get_global_action_cred;
use madmin::{AccountStatus, AddOrUpdateUserReq};
use matchit::Params;
use s3s::{header::CONTENT_TYPE, s3_error, Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use tracing::warn;

use crate::admin::{
    handlers::{check_key_valid, get_session_token},
    router::Operation,
};

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
        warn!("handle AddUser");
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

        // let Some(body) = req.input.bytes() else {
        //     return Err(s3_error!(InvalidRequest, "get body failed"));
        // };

        // let body_bytes = decrypt_data(input_cred.secret_key.expose().as_bytes(), &body)
        //     .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("decrypt_data err {}", e)))?;

        let args: AddOrUpdateUserReq = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {}", e)))?;

        warn!("add user args {:?}", args);

        if args.secret_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        if let Some(sys_cred) = get_global_action_cred() {
            if sys_cred.access_key == ak {
                return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
            }
        }

        if let (Some(user), true) = iam::get_user(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {}", e)))?
        {
            if user.credentials.is_temp() || user.credentials.is_service_account() {
                return Err(s3_error!(InvalidArgument, "can't create user with service account access key"));
            }
        }

        let token = get_session_token(&req.headers);

        let (cred, _) = check_key_valid(token, &input_cred.access_key).await?;

        if (cred.is_temp() || cred.is_service_account()) && cred.parent_user == input_cred.access_key {
            return Err(s3_error!(InvalidArgument, "can't create user with service account access key"));
        }

        iam::create_user(ak, &args.secret_key, "enabled")
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("create_user err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct SetUserStatus {}
#[async_trait::async_trait]
impl Operation for SetUserStatus {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle SetUserStatus");

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

        iam::set_user_status(ak, status)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("set_user_status err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct ListUsers {}
#[async_trait::async_trait]
impl Operation for ListUsers {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListUsers");
        let users = iam::list_users()
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        let data = serde_json::to_vec(&users)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {}", e)))?;

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
        warn!("handle RemoveUser");
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

        let (is_temp, _) = iam::is_temp_user(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("is_temp_user err {}", e)))?;

        if is_temp {
            return Err(s3_error!(InvalidArgument, "can't remove temp user"));
        }

        iam::delete_user(ak, true)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("delete_user err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

pub struct GetUserInfo {}
#[async_trait::async_trait]
impl Operation for GetUserInfo {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle GetUserInfo");
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

        let info = iam::get_user_info(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        let data = serde_json::to_vec(&info)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal user err {}", e)))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}
