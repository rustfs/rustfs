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

//! Bucket engine custom operations: `POST /{bucket}?x-rustfs-engine-op=<op>`.
//!
//! The route matches before s3s dispatch. Like the metadata route it resolves
//! the SigV4-verified credentials itself (custom routes bypass `S3Access`),
//! then authorizes the S3 action the engine declared for the operation, so an
//! engine operation is never weaker than the object operation it stands for.
//! Unsigned `x-amz-*` headers on SigV4 requests are already rejected by the
//! server's header guard layer. The body is bounded by the operation's
//! declared limit before the engine sees it.

use crate::app::metadata_route::bucket_from_request;
use crate::auth::{check_key_valid, get_session_token};
use crate::bucket_engine::dispatch::bound_bucket;
use crate::bucket_engine::{BoundBucket, binding::engine_error};
use crate::storage::access::{ReqInfo, authorize_request, req_info_mut};
use async_trait::async_trait;
use http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use rustfs_bucket_engine::{CustomOperationSpec, CustomRequest, ENGINE_OPERATION_QUERY, OperationAction, OperationName};
use rustfs_policy::policy::action::{Action, S3Action};
use s3s::host::MultiDomain;
use s3s::route::S3Route;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::sync::Arc;
use url::form_urlencoded;

/// Response headers an engine may set; everything else is dropped so an
/// engine cannot override framework or security headers.
const ENGINE_RESPONSE_HEADER_PREFIX: &str = "x-rustfs-engine-";

pub(crate) struct EngineOperationRoute<R> {
    inner: R,
    host: Option<MultiDomain>,
}

pub(crate) fn with_engine_operation_route<R>(inner: R, host: Option<MultiDomain>) -> EngineOperationRoute<R> {
    EngineOperationRoute { inner, host }
}

/// The engine operation a request names, if it is one.
#[derive(Debug)]
struct OperationTarget {
    bucket: String,
    operation: String,
}

fn operation_target(method: &Method, uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> Option<OperationTarget> {
    if method != Method::POST {
        return None;
    }
    let query = uri.query()?;
    let operation = form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == ENGINE_OPERATION_QUERY)
        .map(|(_, value)| value.into_owned())?;
    // A request that names an operation but no bucket still matches, so it is
    // refused here instead of reaching another handler.
    let bucket = bucket_from_request(uri, headers, host).unwrap_or_default();
    Some(OperationTarget { bucket, operation })
}

/// Resolves the bucket's engine and the declared operation.
fn resolve(target: &OperationTarget) -> S3Result<(Arc<BoundBucket>, CustomOperationSpec)> {
    if target.bucket.is_empty() {
        return Err(s3_error!(InvalidRequest, "engine operations need a bucket"));
    }
    let operation = OperationName::new(target.operation.as_str())
        .map_err(|err| s3_error!(InvalidArgument, "invalid {}: {}", ENGINE_OPERATION_QUERY, err))?;
    let bound = bound_bucket(&target.bucket)?.ok_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "engine operations need a bucket bound to an engine".to_owned(),
        )
    })?;
    let spec = bound
        .engine
        .descriptor()
        .capabilities
        .custom_operation(&operation)
        .cloned()
        .ok_or_else(|| {
            S3Error::with_message(
                S3ErrorCode::NotImplemented,
                format!("engine {} has no operation {}", bound.context.binding.engine.as_str(), operation.as_str()),
            )
        })?;
    Ok((bound, spec))
}

fn operation_action(action: OperationAction) -> Action {
    Action::S3Action(match action {
        OperationAction::GetObject => S3Action::GetObjectAction,
        OperationAction::PutObject => S3Action::PutObjectAction,
        OperationAction::DeleteObject => S3Action::DeleteObjectAction,
        OperationAction::ListBucket => S3Action::ListBucketAction,
    })
}

async fn check_operation_access(req: &mut S3Request<Body>, target: OperationTarget) -> S3Result<()> {
    let (_, spec) = resolve(&target)?;

    // Same credential resolution as the metadata route: custom routes bypass
    // the S3Access hook that fills ReqInfo for ordinary S3 requests.
    let (cred, is_owner) = if let Some(input_cred) = &req.credentials {
        let (cred, is_owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        (Some(cred), is_owner)
    } else {
        (None, false)
    };
    {
        if req_info_mut(req).is_err() {
            req.extensions.insert(ReqInfo::default());
        }
        let req_info = req_info_mut(req)?;
        req_info.cred = cred;
        req_info.is_owner = is_owner;
        req_info.bucket = Some(target.bucket);
        req_info.object = None;
        req_info.version_id = None;
    }
    authorize_request(req, operation_action(spec.action)).await
}

async fn call_operation(req: S3Request<Body>, target: OperationTarget) -> S3Result<S3Response<Body>> {
    // Resolved again: the binding may have changed since authorization, and
    // the call must use the engine and limits that are current now.
    let (bound, spec) = resolve(&target)?;
    let limit = usize::try_from(spec.max_request_bytes).unwrap_or(usize::MAX);
    let mut input = req.input;
    let body = input
        .store_all_limited(limit)
        .await
        .map_err(|err| s3_error!(EntityTooLarge, "engine operation body exceeds {} bytes: {}", spec.max_request_bytes, err))?;
    let request = CustomRequest {
        operation: spec.name.clone(),
        body,
        rdma: None,
    };
    let response = bound
        .engine
        .custom_operation(&bound.context, request)
        .await
        .map_err(engine_error)?;

    let mut headers = HeaderMap::new();
    for (name, value) in response.headers {
        let Ok(name) = HeaderName::try_from(name.to_ascii_lowercase()) else {
            continue;
        };
        if !name.as_str().starts_with(ENGINE_RESPONSE_HEADER_PREFIX) {
            continue;
        }
        if let Ok(value) = HeaderValue::try_from(value) {
            headers.insert(name, value);
        }
    }
    let mut output = S3Response::new(Body::from(response.body));
    output.status = Some(StatusCode::OK);
    output.headers = headers;
    Ok(output)
}

#[async_trait]
impl<R> S3Route for EngineOperationRoute<R>
where
    R: S3Route,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool {
        operation_target(method, uri, headers, self.host.as_ref()).is_some()
            || self.inner.is_match(method, uri, headers, extensions)
    }

    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        match operation_target(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            Some(target) => check_operation_access(req, target).await,
            None => self.inner.check_access(req).await,
        }
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        match operation_target(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            Some(target) => call_operation(req, target).await,
            None => self.inner.call(req).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_engine::EngineRegistry;
    use crate::bucket_engine::memory::{MemoryEngine, PREFIX_QUERY_OPERATION, PrefixQueryResponse};
    use bytes::Bytes;
    use rustfs_bucket_engine::ObjectWriteMeta;

    fn uri(value: &'static str) -> Uri {
        Uri::from_static(value)
    }

    fn target(bucket: &str, operation: &str) -> OperationTarget {
        OperationTarget {
            bucket: bucket.to_owned(),
            operation: operation.to_owned(),
        }
    }

    /// Binds `bucket` to the memory engine in the process-wide registry the
    /// route resolves through; each test uses its own bucket name.
    async fn bind_memory_bucket(bucket: &str) -> Arc<BoundBucket> {
        let registry = EngineRegistry::global();
        let _ = registry.register_engine(Arc::new(MemoryEngine::new()));
        if let Some(task) = registry.publish(bucket, Some((br#"{"engine":"memory","config_version":1}"#, 7))) {
            task.await.expect("reconcile task");
        }
        bound_bucket(bucket).expect("route").expect("bound bucket")
    }

    fn code<T>(result: S3Result<T>) -> S3ErrorCode {
        result.err().expect("expected an error").code().clone()
    }

    #[test]
    fn only_post_with_the_operation_query_matches() {
        let headers = HeaderMap::new();
        let matched = operation_target(&Method::POST, &uri("/kv?x-rustfs-engine-op=prefix-query"), &headers, None)
            .expect("engine operation");
        assert_eq!(matched.bucket, "kv");
        assert_eq!(matched.operation, "prefix-query");

        assert!(operation_target(&Method::GET, &uri("/kv?x-rustfs-engine-op=prefix-query"), &headers, None).is_none());
        assert!(operation_target(&Method::POST, &uri("/kv?delete"), &headers, None).is_none());
        assert!(operation_target(&Method::POST, &uri("/kv"), &headers, None).is_none());
        // An operation on an object path still matches and is refused, rather
        // than falling through to the S3 handlers.
        let object_path = operation_target(&Method::POST, &uri("/kv/key?x-rustfs-engine-op=prefix-query"), &headers, None)
            .expect("engine operation");
        assert_eq!(code(resolve(&object_path)), S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn resolution_needs_a_bound_bucket_and_a_declared_operation() {
        bind_memory_bucket("route-resolve").await;

        let (_, spec) = resolve(&target("route-resolve", PREFIX_QUERY_OPERATION)).expect("declared operation");
        assert_eq!(spec.action, OperationAction::GetObject);
        assert_eq!(operation_action(spec.action), Action::S3Action(S3Action::GetObjectAction));

        assert_eq!(code(resolve(&target("route-resolve", "batch-read"))), S3ErrorCode::NotImplemented);
        assert_eq!(code(resolve(&target("route-resolve", "Bad Name"))), S3ErrorCode::InvalidArgument);
        assert_eq!(
            code(resolve(&target("route-unbound", PREFIX_QUERY_OPERATION))),
            S3ErrorCode::NotImplemented
        );
    }

    fn post(bucket: &str, body: &'static str) -> (S3Request<Body>, OperationTarget) {
        let request = S3Request {
            input: Body::from(Bytes::from_static(body.as_bytes())),
            method: Method::POST,
            uri: uri("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        (request, target(bucket, PREFIX_QUERY_OPERATION))
    }

    #[tokio::test]
    async fn prefix_query_runs_through_the_engine_with_filtered_headers() {
        let bound = bind_memory_bucket("route-call").await;
        for key in ["p1", "p2"] {
            bound
                .engine
                .put_object(&bound.context, key, Bytes::from_static(b"page"), ObjectWriteMeta::default())
                .await
                .expect("put");
        }

        let (request, target) = post("route-call", r#"{"keys":["p1","p2","p3","p1"]}"#);
        let response = call_operation(request, target).await.expect("prefix query");
        assert_eq!(response.status, Some(StatusCode::OK));
        assert_eq!(
            response
                .headers
                .get("x-rustfs-engine-hits")
                .and_then(|value| value.to_str().ok()),
            Some("2")
        );
        let mut output = response.output;
        let body = output.store_all_limited(1024).await.expect("body");
        let parsed: PrefixQueryResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(parsed, PrefixQueryResponse { hits: 2, bytes: 8 });

        let (request, target) = post("route-call", r#"{"keys":"p1"}"#);
        assert_eq!(code(call_operation(request, target).await), S3ErrorCode::InvalidArgument);
    }
}
