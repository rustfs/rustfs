use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, Mac};
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use hyper::http::Extensions;
use matchit::Params;
use matchit::Router;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

use super::ADMIN_PREFIX;
use super::RUSTFS_ADMIN_PREFIX;
use super::rpc::RPC_PREFIX;
use iam::get_global_action_cred;

type HmacSha256 = Hmac<Sha256>;

const SIGNATURE_HEADER: &str = "x-rustfs-signature";
const TIMESTAMP_HEADER: &str = "x-rustfs-timestamp";
const SIGNATURE_VALID_DURATION: u64 = 300; // 5 minutes

/// Get the shared secret for HMAC signing
fn get_shared_secret() -> String {
    if let Some(cred) = get_global_action_cred() {
        cred.secret_key
    } else {
        // Fallback to environment variable if global credentials are not available
        std::env::var("RUSTFS_RPC_SECRET").unwrap_or_else(|_| "rustfs-default-secret".to_string())
    }
}

/// Generate HMAC-SHA256 signature for the given data
fn generate_signature(secret: &str, url: &str, method: &str, timestamp: u64) -> String {
    let data = format!("{}|{}|{}", url, method, timestamp);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    let result = mac.finalize();
    general_purpose::STANDARD.encode(result.into_bytes())
}

/// Verify the request signature for RPC requests
fn verify_rpc_signature(req: &S3Request<Body>) -> S3Result<()> {
    let secret = get_shared_secret();

    // Get signature from header
    let signature = req
        .headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3_error!(InvalidArgument, "Missing signature header"))?;

    // Get timestamp from header
    let timestamp_str = req
        .headers
        .get(TIMESTAMP_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3_error!(InvalidArgument, "Missing timestamp header"))?;

    let timestamp: u64 = timestamp_str
        .parse()
        .map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp format"))?;

    // Check timestamp validity (prevent replay attacks)
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    if current_time.saturating_sub(timestamp) > SIGNATURE_VALID_DURATION {
        return Err(s3_error!(InvalidArgument, "Request timestamp expired"));
    }

    // Generate expected signature
    let url = req.uri.to_string();
    let method = req.method.as_str();
    let expected_signature = generate_signature(&secret, &url, method, timestamp);

    // Compare signatures
    if signature != expected_signature {
        return Err(s3_error!(AccessDenied, "Invalid signature"));
    }

    Ok(())
}

pub struct S3Router<T> {
    router: Router<T>,
}

impl<T: Operation> S3Router<T> {
    pub fn new() -> Self {
        let router = Router::new();

        Self { router }
    }

    pub fn insert(&mut self, method: Method, path: &str, operation: T) -> std::io::Result<()> {
        let path = Self::make_route_str(method, path);

        // warn!("set uri {}", &path);

        self.router.insert(path, operation).map_err(std::io::Error::other)?;

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

        uri.path().starts_with(ADMIN_PREFIX) || uri.path().starts_with(RPC_PREFIX) || uri.path().starts_with(RUSTFS_ADMIN_PREFIX)
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let uri = format!("{}|{}", &req.method, req.uri.path());

        // warn!("get uri {}", &uri);

        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            let mut resp = op.call(req, mat.params).await?;
            resp.status = Some(resp.output.0);
            return Ok(resp.map_output(|x| x.1));
        }

        return Err(s3_error!(NotImplemented));
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        // Check RPC signature verification
        if req.uri.path().starts_with(RPC_PREFIX) {
            // Skip signature verification for HEAD requests (health checks)
            if req.method != Method::HEAD {
                verify_rpc_signature(req)?;
            }
            return Ok(());
        }

        // For non-RPC admin requests, check credentials
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
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
