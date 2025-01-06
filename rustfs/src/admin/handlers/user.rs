use http::StatusCode;
use matchit::Params;
use s3s::{s3_error, Body, S3Request, S3Response, S3Result};

use crate::admin::router::Operation;

pub struct AddUser {}
#[async_trait::async_trait]
impl Operation for AddUser {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        return Err(s3_error!(NotImplemented));
    }
}

pub struct SetUserStatus {}
#[async_trait::async_trait]
impl Operation for SetUserStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        return Err(s3_error!(NotImplemented));
    }
}
