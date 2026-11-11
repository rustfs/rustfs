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

//! The S3 protocol stack one server runs, selected once at startup (rustfs/backlog#2734 T2.5).
//!
//! Responsible for: [`S3StackService`], the service mounted where the legacy S3 service used to
//! be, and [`select_stack`], the only place a stack is chosen. The whole stack is one or the
//! other: no request is routed between stacks by its operation, and a request the gateway stack
//! refuses is answered by that refusal and never retried on the legacy stack.
//! NOT responsible for: assembling either stack (`server/http.rs` builds the legacy service,
//! `server/gateway_stack.rs` the gateway one), or the middleware around them.
//! Upstream: `server/http.rs`. Downstream: the legacy S3 service and the gateway stack.
//!
//! This file names neither stack's types: it is generic over two services with one response
//! type, so `server/http.rs` decides what they are and how their bodies meet.
//!
//! Two S3-shaped families are interim legacy routes, each in its own file so that the PR which
//! migrates one deletes only that file (rustfs/backlog#2734 T2.11): website-host requests
//! (`stack/website.rs`) and the `?metadata=true` listings (`stack/metadata.rs`).

mod metadata;
mod website;

use super::layer::is_sts_query_request;
use super::prefix::{HEALTH_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, has_path_prefix, is_admin_path};
use crate::admin::console::is_console_path;
use crate::config::S3Stack;
use http::{HeaderMap, Method, Request, Uri};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tower::Service;

pub(crate) use metadata::MetadataListing;
pub(crate) use website::WebsiteHosts;

/// The MinIO-compatible health probes share this prefix.
const MINIO_HEALTH_PREFIX: &str = "/minio/health";

/// The S3 service of one server: the legacy stack, or the gateway stack with the legacy router
/// kept for the routes that are not S3 operations.
#[derive(Clone)]
pub(crate) struct S3StackService<L, G> {
    legacy: L,
    gateway: Option<G>,
    website: WebsiteHosts,
    metadata: MetadataListing,
}

impl<L, G> S3StackService<L, G> {
    /// The stack this service runs.
    pub(crate) fn selected(&self) -> S3Stack {
        match self.gateway {
            Some(_) => S3Stack::Gateway,
            None => S3Stack::Legacy,
        }
    }
}

/// Builds the service for the selected stack.
///
/// `assemble_gateway` runs only when the gateway stack is selected, and its failure is the
/// failure of the whole selection: the caller refuses to start. `website` and `metadata` are the
/// legacy service's own website domains and metadata-listing hosts, so the gateway stack sends
/// the legacy service exactly the requests that service would serve as those two families.
///
/// # Errors
///
/// Whatever `assemble_gateway` refused with. The legacy stack is never substituted.
pub(crate) fn select_stack<L, G, E>(
    stack: S3Stack,
    legacy: L,
    website: WebsiteHosts,
    metadata: MetadataListing,
    assemble_gateway: impl FnOnce() -> Result<G, E>,
) -> Result<S3StackService<L, G>, E> {
    let gateway = match stack {
        S3Stack::Legacy => None,
        S3Stack::Gateway => Some(assemble_gateway()?),
    };
    Ok(S3StackService {
        legacy,
        gateway,
        website,
        metadata,
    })
}

/// Whether the legacy router answers this request while the gateway stack is selected: website
/// requests and the metadata listings (interim, tested first as legacy tests them), then the
/// admin and table-catalog APIs, the console, STS, health and profiling, none of which is an S3
/// operation. Every other request is the gateway's, whatever it answers.
fn is_legacy_only_route(
    website: &WebsiteHosts,
    metadata: &MetadataListing,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> bool {
    let path = uri.path();
    website.matches(uri, headers)
        || metadata.matches(method, uri, headers)
        || is_admin_path(path)
        || is_console_path(path)
        || has_path_prefix(path, HEALTH_PREFIX)
        || has_path_prefix(path, MINIO_HEALTH_PREFIX)
        || path == PROFILE_CPU_PATH
        || path == PROFILE_MEMORY_PATH
        || is_sts_query_request(method, uri, headers)
}

impl<L, G, B, R, E> Service<Request<B>> for S3StackService<L, G>
where
    L: Service<Request<B>, Response = http::Response<R>, Error = E>,
    G: Service<Request<B>, Response = http::Response<R>, Error = E>,
    L::Future: Send + 'static,
    G::Future: Send + 'static,
{
    type Response = http::Response<R>;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(gateway) = &mut self.gateway {
            ready!(gateway.poll_ready(cx))?;
        }
        self.legacy.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        match &mut self.gateway {
            // REMOVED BY T2.11: the gateway serves these routes itself once they are migrated.
            Some(gateway)
                if !is_legacy_only_route(&self.website, &self.metadata, request.method(), request.uri(), request.headers()) =>
            {
                Box::pin(gateway.call(request))
            }
            _ => Box::pin(self.legacy.call(request)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Response, StatusCode};
    use std::convert::Infallible;
    use std::future::{Ready, ready as ready_future};

    /// Answers every request with its own name.
    #[derive(Clone)]
    struct Named(&'static str);

    impl Service<Request<()>> for Named {
        type Response = Response<&'static str>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            ready_future(Ok(Response::new(self.0)))
        }
    }

    fn request(method: Method, target: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(target)
            .body(())
            .expect("static request")
    }

    async fn answered_by(service: &mut S3StackService<Named, Named>, request: Request<()>) -> &'static str {
        let response = service.call(request).await.expect("infallible");
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body()
    }

    /// The website domain the tests configure.
    const WEBSITE_DOMAIN: &str = "site.test";

    /// `select_stack` with one website domain and path-style metadata listings, as a server
    /// with `RUSTFS_WEBSITE_DOMAINS=site.test` and no `RUSTFS_SERVER_DOMAINS` runs.
    fn select<E>(stack: S3Stack, assemble: impl FnOnce() -> Result<Named, E>) -> Result<S3StackService<Named, Named>, E> {
        select_stack(
            stack,
            Named("legacy"),
            WebsiteHosts::new(vec![WEBSITE_DOMAIN.to_owned()]),
            MetadataListing::new(None).expect("no server domain"),
            assemble,
        )
    }

    fn gateway_stack() -> S3StackService<Named, Named> {
        select(S3Stack::Gateway, || Ok::<_, Infallible>(Named("gateway"))).expect("assembles")
    }

    fn on_host(method: Method, host: &str, target: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(target)
            .header(http::header::HOST, host)
            .body(())
            .expect("static request")
    }

    #[test]
    fn a_gateway_assembly_failure_fails_the_selection_instead_of_falling_back() {
        let selection = select(S3Stack::Gateway, || Err("the assembly was refused"));

        assert_eq!(selection.err(), Some("the assembly was refused"));
    }

    #[test]
    fn the_legacy_selection_never_assembles_a_gateway() {
        let service = select::<&str>(S3Stack::Legacy, || panic!("the legacy stack must not assemble the gateway"))
            .expect("legacy needs no assembly");

        assert_eq!(service.selected(), S3Stack::Legacy);
    }

    #[tokio::test]
    async fn the_legacy_stack_answers_every_request_itself() {
        let mut service = select(S3Stack::Legacy, || Ok::<_, Infallible>(Named("gateway"))).expect("legacy needs no assembly");

        for target in ["/", "/bucket/key", "/rustfs/admin/v3/info", "/health"] {
            assert_eq!(answered_by(&mut service, request(Method::GET, target)).await, "legacy", "{target}");
        }
        for (host, target) in [
            ("bucket.site.test", "/"),
            ("127.0.0.1:9000", "/bucket?list-type=2&metadata=true"),
        ] {
            assert_eq!(
                answered_by(&mut service, on_host(Method::GET, host, target)).await,
                "legacy",
                "{host} {target}"
            );
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_answers_every_s3_request_whatever_the_operation() {
        let mut service = gateway_stack();
        assert_eq!(service.selected(), S3Stack::Gateway);

        for (method, target) in [
            (Method::GET, "/"),
            (Method::GET, "/bucket/key"),
            (Method::PUT, "/bucket/key"),
            (Method::DELETE, "/bucket"),
            (Method::GET, "/bucket?versions"),
            (Method::POST, "/bucket/key?uploads"),
            // Prefixes of the legacy-only routes that are bucket names, not those routes.
            (Method::GET, "/healthy/key"),
            (Method::GET, "/rustfs/administrator"),
            (Method::GET, "/minio/healthcheck"),
            (Method::GET, "/profile/cpus"),
        ] {
            assert_eq!(
                answered_by(&mut service, request(method.clone(), target)).await,
                "gateway",
                "{method} {target}"
            );
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_leaves_the_non_s3_routes_to_the_legacy_router() {
        let mut service = gateway_stack();

        for target in [
            "/rustfs/admin/v3/info",
            "/minio/admin/v3/info",
            "/iceberg/v1/config",
            "/health",
            "/health/ready",
            "/minio/health/live",
            "/profile/cpu",
            "/profile/memory",
        ] {
            assert_eq!(answered_by(&mut service, request(Method::GET, target)).await, "legacy", "{target}");
        }
    }

    #[tokio::test]
    async fn an_sts_form_post_stays_on_the_legacy_router_and_a_plain_post_does_not() {
        let mut service = gateway_stack();
        let sts = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(())
            .expect("static request");

        assert!(
            is_sts_query_request(sts.method(), sts.uri(), sts.headers()),
            "the fixture is an STS request"
        );
        assert_eq!(answered_by(&mut service, sts).await, "legacy");
        assert_eq!(answered_by(&mut service, request(Method::POST, "/")).await, "gateway");
    }

    #[tokio::test]
    async fn the_gateway_stack_leaves_every_website_host_request_to_the_legacy_router() {
        let mut service = gateway_stack();

        // Whatever the method, path or query: legacy decides website hosts before anything else,
        // so a website-host request that looks like ListBuckets, an admin call or a metadata
        // listing is still a website request.
        for (method, host, target) in [
            (Method::GET, "bucket.site.test", "/"),
            (Method::HEAD, "bucket.site.test:9000", "/index.html"),
            (Method::POST, "bucket.site.test", "/"),
            (Method::GET, "site.test", "/"),
            (Method::GET, "bucket.site.test", "/rustfs/admin/v3/info"),
            (Method::GET, "bucket.site.test", "/?list-type=2&metadata=true"),
        ] {
            assert_eq!(
                answered_by(&mut service, on_host(method.clone(), host, target)).await,
                "legacy",
                "{method} {host} {target}"
            );
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_leaves_the_metadata_listings_to_the_legacy_router() {
        let mut service = gateway_stack();

        for target in ["/bucket?list-type=2&metadata=true", "/bucket?versions&metadata=true"] {
            assert_eq!(
                answered_by(&mut service, on_host(Method::GET, "127.0.0.1:9000", target)).await,
                "legacy",
                "{target}"
            );
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_keeps_every_near_miss_of_the_interim_routes() {
        let mut service = gateway_stack();

        for (method, host, target) in [
            // Not a website host.
            (Method::GET, "127.0.0.1:9000", "/"),
            (Method::GET, "evilsite.test", "/"),
            (Method::GET, "bucket.site.test.evil.example", "/"),
            (Method::GET, "localhost:9000", "/site.test/index.html"),
            // Not a metadata listing.
            (Method::GET, "127.0.0.1:9000", "/bucket?list-type=2"),
            (Method::GET, "127.0.0.1:9000", "/bucket?versions"),
            (Method::GET, "127.0.0.1:9000", "/bucket?list-type=2&metadata=false"),
            (Method::GET, "127.0.0.1:9000", "/bucket?list-type=2&metadata="),
            (Method::GET, "127.0.0.1:9000", "/bucket/key?list-type=2&metadata=true"),
            (Method::GET, "127.0.0.1:9000", "/bucket?metadata=true"),
            (Method::PUT, "127.0.0.1:9000", "/bucket?list-type=2&metadata=true"),
        ] {
            assert_eq!(
                answered_by(&mut service, on_host(method.clone(), host, target)).await,
                "gateway",
                "{method} {host} {target}"
            );
        }
    }
}
