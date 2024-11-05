use std::collections::HashMap;
use std::str::FromStr;

use hyper::http::Extensions;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;

use s3s::route::S3Route;
use s3s::s3_error;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use tracing::warn;

use crate::operation::Operation;
use common::error::Result;
use matchit::Router;

const ADMIN_PREFIX: &str = "/rustfs/admin";
// const ADMIN_VERSION: &str = "v3";

pub struct S3Router<T> {
    router: Router<T>,
}

impl<T: Operation> S3Router<T> {
    pub fn new() -> Self {
        let router = Router::new();

        Self { router }
    }

    pub fn insert(&mut self, operation: T) -> Result<()> {
        let path = Self::make_route_str(operation.method(), &operation.uri());

        warn!("set uri {}", &path);

        self.router.insert(path, operation)?;

        Ok(())
    }

    fn make_route_str(method: Method, path: &str) -> String {
        format!("{}|{}{}", method.as_str(), ADMIN_PREFIX, path)
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: Operation,
{
    fn is_match(&self, _method: &Method, uri: &Uri, _headers: &HeaderMap, _: &mut Extensions) -> bool {
        uri.path().starts_with("/rustfs/admin")
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let uri = format!("{}|{}", &req.method, req.uri.path());

        // warn!("get uri {}", &uri);

        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            return op.call(req, mat.params).await;
        }

        return Err(s3_error!(NotImplemented));
    }
}
