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
//! `DoubleSlashListBucketsCompatLayer`: an s3s compatibility patch, installed
//! by `server::http::external_service_stack!` and pinned there by
//! `stack_census`.
//!
//! Patches: s3s reads `GET //` as an empty bucket name and refuses it with
//! `InvalidBucketName` before routing, where MinIO routes it to `ListBuckets`
//! (rustfs/backlog#601). Introduced in rustfs/rustfs#4336.
//! Replaced by: the gateway route table, in which `//` and `/` both name
//! `ListBuckets` (gateway `docs/middleware.md`, "The nine RustFS tower patch
//! layers", row 3).
//! Removed by: T2.12 (rustfs/backlog#2771).

use http::{Method, Request as HttpRequest, Response, Uri};
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Rewrites the request-target of a root-level double-slash `GET //` to `GET /`
/// so it routes to `ListBuckets`, matching MinIO browser compatibility.
///
/// The AWS S3 browser client (SigV4) concatenates the endpoint with a leading
/// slash and emits `GET //` for `ListBuckets`. The `s3s` path parser strips the
/// single leading slash and then treats the remaining `/` as an empty bucket
/// name, rejecting the request with `InvalidBucketName` before it is routed.
/// MinIO's `api-router.go` registers an explicit `//` route to `ListBuckets`;
/// this layer reproduces that behavior by collapsing the path to `/` up front.
///
/// The rewrite is intentionally limited to a path that is exactly `//` (optionally
/// followed by a query string). It never touches bucket/object requests, so object
/// keys with embedded or leading slashes are unaffected (those are normalized by
/// `s3s` via `normalize_forward_slash_path`).
#[derive(Clone)]
pub struct DoubleSlashListBucketsCompatLayer;

impl<S> Layer<S> for DoubleSlashListBucketsCompatLayer {
    type Service = DoubleSlashListBucketsCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DoubleSlashListBucketsCompatService { inner }
    }
}

#[derive(Clone)]
pub struct DoubleSlashListBucketsCompatService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for DoubleSlashListBucketsCompatService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        if req.method() == Method::GET
            && let Some(rewritten) = rewrite_double_slash_root(req.uri())
        {
            *req.uri_mut() = rewritten;
        }
        self.inner.call(req)
    }
}

/// Returns the `/`-rooted URI when `uri` has a request-target path of exactly `//`.
///
/// Returns `None` for every other path so the rewrite is confined to the
/// root-level double-slash case.
fn rewrite_double_slash_root(uri: &Uri) -> Option<Uri> {
    if uri.path() != "//" {
        return None;
    }

    let mut parts = uri.clone().into_parts();
    let path_and_query = match uri.query() {
        Some(query) => format!("/?{query}"),
        None => "/".to_string(),
    };
    parts.path_and_query = Some(path_and_query.parse().ok()?);
    Uri::from_parts(parts).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::future::{Ready, ready};
    use http::Request;
    use http_body_util::Full;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn rewrite_double_slash_root_only_matches_exact_double_slash() {
        // Exactly `//` is rewritten to `/` (query preserved).
        assert_eq!(
            rewrite_double_slash_root(&Uri::from_static("//")).map(|u| u.to_string()),
            Some("/".to_string())
        );
        assert_eq!(
            rewrite_double_slash_root(&Uri::from_static("//?x-id=ListBuckets")).map(|u| u.to_string()),
            Some("/?x-id=ListBuckets".to_string())
        );

        // Everything else is left untouched.
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("//bucket")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/bucket")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/bucket//key")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("///")), None);
    }

    /// Inner service that records the request-target path it received and
    /// returns an empty response, so the layer's URI rewrite can be observed.
    #[derive(Clone, Default)]
    struct UriCaptureService {
        path: Arc<Mutex<Option<String>>>,
    }

    impl<B> Service<Request<B>> for UriCaptureService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<B>) -> Self::Future {
            *self.path.lock().expect("path lock") = Some(req.uri().path().to_string());
            ready(Ok(Response::new(Full::from(Bytes::new()))))
        }
    }

    #[tokio::test]
    async fn double_slash_compat_layer_rewrites_get_double_slash() {
        let inner = UriCaptureService::default();
        let seen = inner.path.clone();
        let mut service = DoubleSlashListBucketsCompatLayer.layer(inner);

        let observe = |seen: &Arc<Mutex<Option<String>>>| seen.lock().expect("path lock").clone();

        // `GET //` becomes `GET /`.
        let req = Request::builder()
            .method(Method::GET)
            .uri("//")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("/"));

        // Non-GET `//` is not rewritten.
        let req = Request::builder()
            .method(Method::PUT)
            .uri("//")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("//"));

        // `GET //bucket` is not rewritten.
        let req = Request::builder()
            .method(Method::GET)
            .uri("//bucket")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("//bucket"));
    }
}
