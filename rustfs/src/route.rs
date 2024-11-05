use common::error::{Error, Result};

use matchit::Params;
use router::{operation::Operation, router::S3Router};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};
use tracing::warn;

use hyper::Method;
use hyper::StatusCode;

pub fn make_admin_route() -> Result<impl S3Route> {
    let mut r = S3Router::new();

    r.insert(AdminOperation(&InfoHandler {}))?;
    r.insert(AdminOperation(&ListPoolHandler {}))?;

    Ok(r)
}

struct AdminOperation(&'static dyn Operation);

#[async_trait::async_trait]
impl Operation for AdminOperation {
    fn method(&self) -> Method {
        self.0.method()
    }
    fn uri(&self) -> &'static str {
        self.0.uri()
    }
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        self.0.call(req, params).await
    }
}

struct InfoHandler {}

#[async_trait::async_trait]
impl Operation for InfoHandler {
    fn method(&self) -> Method {
        Method::GET
    }
    fn uri(&self) -> &'static str {
        "/v3/info"
    }
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle info");

        unimplemented!()
    }
}

struct ListPoolHandler {}

#[async_trait::async_trait]
impl Operation for ListPoolHandler {
    fn method(&self) -> Method {
        Method::GET
    }
    fn uri(&self) -> &'static str {
        "/v3/pool/list"
    }
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle info");

        unimplemented!()
    }
}
