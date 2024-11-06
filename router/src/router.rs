use common::error::Result;
use hyper::http::Extensions;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use matchit::Params;
use matchit::Router;
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;

pub struct S3Router<T> {
    router: Router<T>,
}

impl<T: Operation> S3Router<T> {
    pub fn new() -> Self {
        let router = Router::new();

        Self { router }
    }

    pub fn insert(&mut self, method: Method, path: &str, operation: T) -> Result<()> {
        let path = Self::make_route_str(method, path);

        // warn!("set uri {}", &path);

        self.router.insert(path, operation)?;

        Ok(())
    }

    fn make_route_str(method: Method, path: &str) -> String {
        format!("{}|{}", method.as_str(), path)
    }
}

impl<T: Operation> Default for S3Router<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: Operation,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
        // AssumeRole
        if method == Method::POST && uri.path() == "/" {
            if let Some(val) = headers.get(header::CONTENT_TYPE) {
                if val.as_bytes() == b"application/x-www-form-urlencoded" {
                    return true;
                }
            }
        }

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

#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    // fn method() -> Method;
    // fn uri() -> &'static str;
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>>;
}

pub struct AdminOperation(pub &'static dyn Operation);

#[async_trait::async_trait]
impl Operation for AdminOperation {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        self.0.call(req, params).await
    }
}
