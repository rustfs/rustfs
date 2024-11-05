use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::{Body, S3Request, S3Response, S3Result};

#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    fn method(&self) -> Method;
    fn uri(&self) -> &'static str;
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>>;
}
