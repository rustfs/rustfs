use futures::TryFutureExt;
use http::StatusCode;
use iam::get_global_action_cred;
use matchit::Params;
use s3s::{s3_error, Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use tracing::warn;

use crate::admin::router::Operation;

#[derive(Debug, Deserialize, Default)]
pub struct AddUserQuery {
    #[serde(rename = "accessKey")]
    pub access_key: Option<String>,
}

pub struct AddUser {}
#[async_trait::async_trait]
impl Operation for AddUser {
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

        warn!("handle AddUser");
        return Err(s3_error!(NotImplemented));
    }
}

fn check_claims_from_token(_token: &str) -> bool {
    true
}

pub struct SetUserStatus {}
#[async_trait::async_trait]
impl Operation for SetUserStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        return Err(s3_error!(NotImplemented));
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

        let body = serde_json::to_string(&users)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {}", e)))?;
        Ok(S3Response::new((StatusCode::OK, Body::from(body))))
    }
}

pub struct RemoveUser {}
#[async_trait::async_trait]
impl Operation for RemoveUser {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RemoveUser");
        return Err(s3_error!(NotImplemented));
    }
}

pub struct GetUserInfo {}
#[async_trait::async_trait]
impl Operation for GetUserInfo {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        return Err(s3_error!(NotImplemented));
    }
}
