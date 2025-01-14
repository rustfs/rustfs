use http::StatusCode;
use iam::{error::is_err_no_such_user, get_global_action_cred};
use madmin::GroupAddRemove;
use matchit::Params;
use s3s::{s3_error, Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use tracing::warn;

use crate::admin::router::Operation;

pub struct ListGroups {}
#[async_trait::async_trait]
impl Operation for ListGroups {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListGroups");

        Err(s3_error!(NotImplemented))
    }
}

pub struct Group {}
#[async_trait::async_trait]
impl Operation for Group {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle Group");

        Err(s3_error!(NotImplemented))
    }
}

pub struct UpdateGroupMembers {}
#[async_trait::async_trait]
impl Operation for UpdateGroupMembers {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle UpdateGroupMembers");

        let Some(body) = req.input.bytes() else {
            return Err(s3_error!(InvalidRequest, "get body failed"));
        };

        let args: GroupAddRemove = serde_json::from_slice(&body)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("unmarshal body err {}", e)))?;

        warn!("UpdateGroupMembers args {:?}", args);

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InternalError, "iam not init")) };

        for member in args.members.iter() {
            match iam_store.is_temp_user(member).await {
                Ok((is_temp, _)) => {
                    if is_temp {
                        return Err(S3Error::with_message(
                            S3ErrorCode::MethodNotAllowed,
                            format!("can't add temp user {}", member),
                        ));
                    }

                    get_global_action_cred()
                        .map(|cred| {
                            if cred.access_key == *member {
                                return Err(S3Error::with_message(
                                    S3ErrorCode::MethodNotAllowed,
                                    format!("can't add root {}", member),
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
        } else {
            warn!("add group members");
        }

        Err(s3_error!(NotImplemented))
    }
}

pub struct SetGroupStatus {}
#[async_trait::async_trait]
impl Operation for SetGroupStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle SetGroupStatus");

        Err(s3_error!(NotImplemented))
    }
}
