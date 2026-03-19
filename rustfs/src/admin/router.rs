// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::admin::console::{is_console_path, make_console_server};
use crate::admin::handlers::oidc::is_oidc_path;
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::{
    ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, RPC_PREFIX,
};
use crate::storage::access::{ReqInfo, authorize_request};
use http::HeaderValue;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use hyper::http::Extensions;
use matchit::Params;
use matchit::Router;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::replication::BucketStats;
use rustfs_ecstore::bucket::replication::GLOBAL_REPLICATION_STATS;
use rustfs_ecstore::bucket::versioning::VersioningApi;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::rpc::verify_rpc_signature;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{global::get_global_region, new_object_layer_fn};
use rustfs_madmin::utils::parse_duration;
use rustfs_policy::policy::action::{Action, S3Action};
use s3s::Body;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use tower::Service;
use tracing::error;
use url::form_urlencoded;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationExtRoute {
    MetricsV1,
    MetricsV2,
    Check,
    ResetStart,
    ResetStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationExtRequest {
    bucket: String,
    route: ReplicationExtRoute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MiscExtRoute {
    ObjectLambda { bucket: String, object: String },
    ListenNotification { bucket: Option<String> },
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetResponse {
    #[serde(rename = "Targets")]
    targets: Vec<ReplicationResetTarget>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetTarget {
    #[serde(rename = "Arn")]
    arn: String,
    #[serde(rename = "ResetID")]
    reset_id: String,
}

fn parse_query_pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn query_value_exact(uri: &Uri, key: &str) -> Option<String> {
    parse_query_pairs(uri)
        .into_iter()
        .find_map(|(k, v)| if k == key { Some(v) } else { None })
}

fn extract_bucket_for_bucket_level_path(path: &str) -> Option<String> {
    let bucket = path.strip_prefix('/')?;
    if bucket.is_empty() || bucket.contains('/') {
        return None;
    }
    Some(bucket.to_string())
}

fn extract_bucket_object_path(path: &str) -> Option<(String, String)> {
    let path = path.strip_prefix('/')?;
    let (bucket, object) = path.split_once('/')?;
    if bucket.is_empty() || object.is_empty() {
        return None;
    }
    Some((bucket.to_string(), object.to_string()))
}

fn parse_replication_extension_request(method: &Method, uri: &Uri) -> Option<ReplicationExtRequest> {
    let bucket = extract_bucket_for_bucket_level_path(uri.path())?;

    if method == Method::PUT && query_value_exact(uri, "replication-reset").as_deref() == Some("") {
        return Some(ReplicationExtRequest {
            bucket,
            route: ReplicationExtRoute::ResetStart,
        });
    }

    if method == Method::GET {
        if query_value_exact(uri, "replication-reset-status").as_deref() == Some("") {
            return Some(ReplicationExtRequest {
                bucket,
                route: ReplicationExtRoute::ResetStatus,
            });
        }
        if let Some(value) = query_value_exact(uri, "replication-metrics") {
            if value == "2" {
                return Some(ReplicationExtRequest {
                    bucket,
                    route: ReplicationExtRoute::MetricsV2,
                });
            }
            if value.is_empty() {
                return Some(ReplicationExtRequest {
                    bucket,
                    route: ReplicationExtRoute::MetricsV1,
                });
            }
        }
        if query_value_exact(uri, "replication-check").as_deref() == Some("") {
            return Some(ReplicationExtRequest {
                bucket,
                route: ReplicationExtRoute::Check,
            });
        }
    }

    None
}

fn parse_misc_extension_request(method: &Method, uri: &Uri) -> Option<MiscExtRoute> {
    if method != Method::GET {
        return None;
    }

    if query_value_exact(uri, "lambdaArn").is_some()
        && let Some((bucket, object)) = extract_bucket_object_path(uri.path())
    {
        return Some(MiscExtRoute::ObjectLambda { bucket, object });
    }

    if query_value_exact(uri, "events").is_some() {
        if uri.path() == "/" {
            return Some(MiscExtRoute::ListenNotification { bucket: None });
        }
        if let Some(bucket) = extract_bucket_for_bucket_level_path(uri.path()) {
            return Some(MiscExtRoute::ListenNotification { bucket: Some(bucket) });
        }
    }

    None
}

async fn ensure_replication_bucket_ready(bucket: &str) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
    };

    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    match metadata_sys::get_replication_config(bucket).await {
        Ok(_) => Ok(()),
        Err(rustfs_ecstore::error::StorageError::ConfigNotFound) => Err(s3_error!(ReplicationConfigurationNotFoundError)),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

async fn build_replication_metrics_response(bucket: &str, route: ReplicationExtRoute) -> S3Result<S3Response<Body>> {
    let bucket_stats = match GLOBAL_REPLICATION_STATS.get() {
        Some(stats) => stats.get_latest_replication_stats(bucket).await,
        None => BucketStats::default(),
    };

    let body = match route {
        ReplicationExtRoute::MetricsV1 => {
            serde_json::to_vec(&bucket_stats.replication_stats).map_err(|e| s3_error!(InternalError, "{e}"))?
        }
        ReplicationExtRoute::MetricsV2 => serde_json::to_vec(&bucket_stats).map_err(|e| s3_error!(InternalError, "{e}"))?,
        ReplicationExtRoute::Check | ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            return Err(s3_error!(InternalError, "invalid route for metrics response"));
        }
    };

    let mut resp = S3Response::with_status(Body::from(body), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

async fn authorize_replication_extension_request(req: &mut S3Request<Body>, ext_req: &ReplicationExtRequest) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, is_owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket: Some(ext_req.bucket.clone()),
        object: None,
        version_id: None,
        region: get_global_region(),
    });

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })?;

    let action = match ext_req.route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 | ReplicationExtRoute::Check => {
            Action::S3Action(S3Action::GetReplicationConfigurationAction)
        }
        ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            Action::S3Action(S3Action::ResetBucketReplicationStateAction)
        }
    };
    authorize_request(req, action).await
}

fn parse_reset_start_target(uri: &Uri) -> S3Result<ReplicationResetTarget> {
    let arn = query_value_exact(uri, "arn")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "arn query parameter is required"))?;

    if let Some(older_than) = query_value_exact(uri, "older-than")
        && !older_than.is_empty()
    {
        parse_duration(&older_than).map_err(|err| s3_error!(InvalidRequest, "invalid older-than query parameter: {err}"))?;
    }

    let reset_id = query_value_exact(uri, "reset-id")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    Ok(ReplicationResetTarget { arn, reset_id })
}

fn build_replication_reset_response(targets: Vec<ReplicationResetTarget>) -> S3Result<S3Response<Body>> {
    let data = serde_json::to_vec(&ReplicationResetResponse { targets }).map_err(|e| s3_error!(InternalError, "{e}"))?;
    let mut resp = S3Response::with_status(Body::from(data), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

async fn handle_replication_extension_request(
    req: &mut S3Request<Body>,
    ext_req: &ReplicationExtRequest,
) -> S3Result<S3Response<Body>> {
    authorize_replication_extension_request(req, ext_req).await?;
    ensure_replication_bucket_ready(&ext_req.bucket).await?;

    match ext_req.route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 => {
            build_replication_metrics_response(&ext_req.bucket, ext_req.route).await
        }
        ReplicationExtRoute::Check => {
            let (versioning, _) = metadata_sys::get_versioning_config(&ext_req.bucket)
                .await
                .map_err(ApiError::from)?;
            if !versioning.enabled() && !BucketVersioningSys::enabled(&ext_req.bucket).await {
                return Err(s3_error!(
                    InvalidRequest,
                    "replication validation requires bucket versioning to be enabled"
                ));
            }
            Ok(S3Response::with_status(Body::from(String::new()), StatusCode::OK))
        }
        ReplicationExtRoute::ResetStatus => build_replication_reset_response(Vec::new()),
        ReplicationExtRoute::ResetStart => {
            let target = parse_reset_start_target(&req.uri)?;
            build_replication_reset_response(vec![target])
        }
    }
}

async fn authorize_misc_extension_request(req: &mut S3Request<Body>, route: &MiscExtRoute) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, is_owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let (bucket, object, action) = match route {
        MiscExtRoute::ObjectLambda { bucket, object } => {
            (Some(bucket.clone()), Some(object.clone()), Action::S3Action(S3Action::GetObjectAction))
        }
        MiscExtRoute::ListenNotification { bucket: Some(bucket) } => {
            (Some(bucket.clone()), None, Action::S3Action(S3Action::ListenBucketNotificationAction))
        }
        MiscExtRoute::ListenNotification { bucket: None } => (None, None, Action::S3Action(S3Action::ListenNotificationAction)),
    };

    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket,
        object,
        version_id: None,
        region: get_global_region(),
    });

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })?;

    authorize_request(req, action).await
}

async fn handle_misc_extension_request(req: &mut S3Request<Body>, route: &MiscExtRoute) -> S3Result<S3Response<Body>> {
    authorize_misc_extension_request(req, route).await?;

    match route {
        MiscExtRoute::ObjectLambda { bucket, .. } => {
            let Some(store) = new_object_layer_fn() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
            };
            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;
            Err(s3_error!(NotImplemented, "object lambda route is not implemented"))
        }
        MiscExtRoute::ListenNotification { bucket } => {
            if let Some(bucket_name) = bucket {
                let Some(store) = new_object_layer_fn() else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
                };
                store
                    .get_bucket_info(bucket_name, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;
            }
            Err(s3_error!(NotImplemented, "listen notification route is not implemented"))
        }
    }
}

pub struct S3Router<T> {
    router: Router<T>,
    console_enabled: bool,
    console_router: Option<axum::routing::RouterIntoService<Body>>,
}

fn is_public_health_path(path: &str) -> bool {
    path == HEALTH_PREFIX || path == HEALTH_READY_PATH
}

fn is_admin_path(path: &str) -> bool {
    path.starts_with(ADMIN_PREFIX) || path.starts_with(MINIO_ADMIN_PREFIX)
}

fn canonicalize_admin_path(path: &str) -> std::borrow::Cow<'_, str> {
    if let Some(suffix) = path.strip_prefix(MINIO_ADMIN_PREFIX) {
        return std::borrow::Cow::Owned(format!("{ADMIN_PREFIX}{suffix}"));
    }

    std::borrow::Cow::Borrowed(path)
}

impl<T: Operation> S3Router<T> {
    pub fn new(console_enabled: bool) -> Self {
        let router = Router::new();

        let console_router = if console_enabled {
            Some(make_console_server().into_service::<Body>())
        } else {
            None
        };

        Self {
            router,
            console_enabled,
            console_router,
        }
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

#[cfg(test)]
impl<T: Operation> S3Router<T> {
    pub(crate) fn contains_route(&self, method: Method, path: &str) -> bool {
        let route = Self::make_route_str(method, path);
        self.router.at(&route).is_ok()
    }

    pub(crate) fn contains_compatible_route(&self, method: Method, path: &str) -> bool {
        let canonical_path = canonicalize_admin_path(path);
        let route = Self::make_route_str(method, canonical_path.as_ref());
        self.router.at(&route).is_ok()
    }
}

impl<T: Operation> Default for S3Router<T> {
    fn default() -> Self {
        Self::new(false)
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: Operation,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
        if parse_replication_extension_request(method, uri).is_some() || parse_misc_extension_request(method, uri).is_some() {
            return true;
        }

        let path = uri.path();

        // Profiling endpoints
        if method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return true;
        }

        // Health check
        if (method == Method::HEAD || method == Method::GET) && is_public_health_path(path) {
            return true;
        }

        // AssumeRole
        if method == Method::POST
            && path == "/"
            && headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.split(';').next().unwrap_or("").trim().to_lowercase())
                .map(|ct| ct == "application/x-www-form-urlencoded")
                .unwrap_or(false)
        {
            return true;
        }

        is_admin_path(path) || path.starts_with(RPC_PREFIX) || is_console_path(path)
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if parse_replication_extension_request(&req.method, &req.uri).is_some()
            || parse_misc_extension_request(&req.method, &req.uri).is_some()
        {
            return match req.credentials {
                Some(_) => Ok(()),
                None => Err(s3_error!(AccessDenied, "Signature is required")),
            };
        }

        // Allow unauthenticated access to health check
        let path = req.uri.path();

        // Profiling endpoints
        if req.method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return Ok(());
        }

        // Health check
        if (req.method == Method::HEAD || req.method == Method::GET) && is_public_health_path(path) {
            return Ok(());
        }

        // Allow unauthenticated access to console static files if console is enabled
        if self.console_enabled && is_console_path(path) {
            return Ok(());
        }

        // Allow unauthenticated access to OIDC endpoints (user not yet authenticated)
        if is_oidc_path(path) {
            return Ok(());
        }

        // Check RPC signature verification
        if req.uri.path().starts_with(RPC_PREFIX) {
            // Skip signature verification for HEAD requests (health checks)
            if req.method != Method::HEAD {
                verify_rpc_signature(&req.uri.to_string(), &req.method, &req.headers).map_err(|e| {
                    error!("RPC signature verification failed: {}", e);
                    s3_error!(AccessDenied, "{}", e)
                })?;
            }
            return Ok(());
        }

        // Allow unauthenticated STS requests to POST / (AssumeRoleWithWebIdentity
        // doesn't use SigV4 — the JWT token in the request body is the authentication).
        // The handler dispatches on the Action parameter: AssumeRole will reject if
        // credentials are missing, AssumeRoleWithWebIdentity will validate the JWT.
        // Require application/x-www-form-urlencoded Content-Type to narrow the bypass.
        if req.method == Method::POST
            && path == "/"
            && req.credentials.is_none()
            && req
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| {
                    ct.split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .eq_ignore_ascii_case("application/x-www-form-urlencoded")
                })
                .unwrap_or(false)
        {
            return Ok(());
        }

        // For non-RPC admin requests, check credentials
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if let Some(ext_req) = parse_replication_extension_request(&req.method, &req.uri) {
            return handle_replication_extension_request(&mut req, &ext_req).await;
        }
        if let Some(ext_req) = parse_misc_extension_request(&req.method, &req.uri) {
            return handle_misc_extension_request(&mut req, &ext_req).await;
        }

        // Console requests should be handled by console router first (including OPTIONS)
        // Console has its own CORS layer configured
        if self.console_enabled && is_console_path(req.uri.path()) {
            if let Some(console_router) = &self.console_router {
                let mut console_router = console_router.clone();
                let req = convert_request(req);
                let result = console_router.call(req).await;
                return match result {
                    Ok(resp) => Ok(convert_response(resp)),
                    Err(e) => Err(s3_error!(InternalError, "{}", e)),
                };
            }
            return Err(s3_error!(InternalError, "console is not enabled"));
        }

        let canonical_path = canonicalize_admin_path(req.uri.path());
        let uri = format!("{}|{}", &req.method, canonical_path.as_ref());

        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            let mut resp = op.call(req, mat.params).await?;
            resp.status = Some(resp.output.0);
            let response = resp.map_output(|x| x.1);

            return Ok(response);
        }

        Err(s3_error!(NotImplemented))
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::Method;
    use http::Uri;
    use s3s::S3Request;

    #[test]
    fn canonicalize_admin_path_maps_compat_prefix_to_rustfs_prefix() {
        assert_eq!(canonicalize_admin_path("/minio/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
        assert_eq!(canonicalize_admin_path("/rustfs/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
    }

    #[test]
    fn is_admin_path_accepts_rustfs_and_compat_prefixes() {
        assert!(is_admin_path("/rustfs/admin/v3/info"));
        assert!(is_admin_path("/minio/admin/v3/info"));
        assert!(!is_admin_path("/bucket/object"));
    }

    #[test]
    fn parse_replication_extension_request_matches_metrics_and_check() {
        let metrics: Uri = "/demo-bucket?replication-metrics".parse().expect("uri should parse");
        let metrics_v2: Uri = "/demo-bucket?replication-metrics=2".parse().expect("uri should parse");
        let check: Uri = "/demo-bucket?replication-check".parse().expect("uri should parse");
        let reset_status: Uri = "/demo-bucket?replication-reset-status".parse().expect("uri should parse");
        let reset_start: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");

        let m = parse_replication_extension_request(&Method::GET, &metrics).expect("metrics route should parse");
        assert_eq!(m.bucket, "demo-bucket");
        assert_eq!(m.route, ReplicationExtRoute::MetricsV1);

        let v2 = parse_replication_extension_request(&Method::GET, &metrics_v2).expect("metrics v2 route should parse");
        assert_eq!(v2.bucket, "demo-bucket");
        assert_eq!(v2.route, ReplicationExtRoute::MetricsV2);

        let c = parse_replication_extension_request(&Method::GET, &check).expect("check route should parse");
        assert_eq!(c.bucket, "demo-bucket");
        assert_eq!(c.route, ReplicationExtRoute::Check);

        let rs = parse_replication_extension_request(&Method::GET, &reset_status).expect("reset status route should parse");
        assert_eq!(rs.bucket, "demo-bucket");
        assert_eq!(rs.route, ReplicationExtRoute::ResetStatus);

        let r = parse_replication_extension_request(&Method::PUT, &reset_start).expect("reset start route should parse");
        assert_eq!(r.bucket, "demo-bucket");
        assert_eq!(r.route, ReplicationExtRoute::ResetStart);
    }

    #[test]
    fn parse_replication_extension_request_rejects_object_level_and_invalid_query_values() {
        let object_level: Uri = "/demo-bucket/path/file?replication-metrics"
            .parse()
            .expect("uri should parse");
        let invalid_value: Uri = "/demo-bucket?replication-metrics=1".parse().expect("uri should parse");
        let wrong_method: Uri = "/demo-bucket?replication-check".parse().expect("uri should parse");
        let wrong_method_reset: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");
        let wrong_method_status: Uri = "/demo-bucket?replication-reset-status".parse().expect("uri should parse");

        assert!(parse_replication_extension_request(&Method::GET, &object_level).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &invalid_value).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &wrong_method_reset).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method_status).is_none());
    }

    #[test]
    fn parse_misc_extension_request_matches_object_lambda_and_listen_notification() {
        let object_lambda: Uri = "/demo-bucket/path/to/object.txt?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let listen_bucket: Uri = "/demo-bucket?events=s3:ObjectCreated:*".parse().expect("uri should parse");
        let listen_root: Uri = "/?events=s3:ObjectRemoved:*".parse().expect("uri should parse");

        let object_route = parse_misc_extension_request(&Method::GET, &object_lambda).expect("object lambda route should parse");
        assert_eq!(
            object_route,
            MiscExtRoute::ObjectLambda {
                bucket: "demo-bucket".to_string(),
                object: "path/to/object.txt".to_string()
            }
        );

        let listen_bucket_route =
            parse_misc_extension_request(&Method::GET, &listen_bucket).expect("bucket listen route should parse");
        assert_eq!(
            listen_bucket_route,
            MiscExtRoute::ListenNotification {
                bucket: Some("demo-bucket".to_string())
            }
        );

        let listen_root_route = parse_misc_extension_request(&Method::GET, &listen_root).expect("root listen route should parse");
        assert_eq!(listen_root_route, MiscExtRoute::ListenNotification { bucket: None });
    }

    #[test]
    fn parse_misc_extension_request_rejects_invalid_paths_or_methods() {
        let bucket_without_object: Uri = "/demo-bucket?lambdaArn=arn%3Atarget".parse().expect("uri should parse");
        let wrong_method_lambda: Uri = "/demo-bucket/object?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let object_level_listen: Uri = "/demo-bucket/object?events=s3:ObjectCreated:*"
            .parse()
            .expect("uri should parse");

        assert!(parse_misc_extension_request(&Method::GET, &bucket_without_object).is_none());
        assert!(parse_misc_extension_request(&Method::PUT, &wrong_method_lambda).is_none());
        assert!(parse_misc_extension_request(&Method::GET, &object_level_listen).is_none());
    }

    #[tokio::test]
    async fn check_access_rejects_anonymous_replication_extension_request() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket?replication-metrics".parse().expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("anonymous extension request must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn check_access_rejects_anonymous_misc_extension_request() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/path/object.txt?lambdaArn=arn%3Atarget"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("anonymous extension request must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Extra {
    pub credentials: Option<s3s::auth::Credentials>,
    pub region: Option<s3s::region::Region>,
    pub service: Option<String>,
}

fn convert_request(req: S3Request<Body>) -> http::Request<Body> {
    let (mut parts, _) = http::Request::new(Body::empty()).into_parts();
    parts.method = req.method;
    parts.uri = req.uri;
    parts.headers = req.headers;
    parts.extensions = req.extensions;
    parts.extensions.insert(Extra {
        credentials: req.credentials,
        region: req.region,
        service: req.service,
    });
    http::Request::from_parts(parts, req.input)
}

fn convert_response(resp: http::Response<axum::body::Body>) -> S3Response<Body> {
    let (parts, body) = resp.into_parts();
    let mut s3_resp = S3Response::new(Body::http_body_unsync(body));
    s3_resp.status = Some(parts.status);
    s3_resp.headers = parts.headers;
    s3_resp.extensions = parts.extensions;
    s3_resp
}
