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

use super::layer::is_sts_query_request;
use super::prefix::{HEALTH_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, has_path_prefix, is_admin_path};
use crate::admin::console::is_console_path;
use crate::config::S3Stack;
use http::{HeaderMap, Method, Request, Uri};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tower::Service;

/// The MinIO-compatible health probes share this prefix.
const MINIO_HEALTH_PREFIX: &str = "/minio/health";

/// The S3 service of one server: the legacy stack, or the gateway stack with the legacy router
/// kept for the routes that are not S3 operations.
#[derive(Clone)]
pub(crate) struct S3StackService<L, G> {
    legacy: L,
    gateway: Option<G>,
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
/// failure of the whole selection: the caller refuses to start.
///
/// # Errors
///
/// Whatever `assemble_gateway` refused with. The legacy stack is never substituted.
pub(crate) fn select_stack<L, G, E>(
    stack: S3Stack,
    legacy: L,
    assemble_gateway: impl FnOnce() -> Result<G, E>,
) -> Result<S3StackService<L, G>, E> {
    let gateway = match stack {
        S3Stack::Legacy => None,
        S3Stack::Gateway => Some(assemble_gateway()?),
    };
    Ok(S3StackService { legacy, gateway })
}

/// Whether the legacy router answers this request while the gateway stack is selected: the admin
/// and table-catalog APIs, the console, STS, health and profiling, none of which is an S3
/// operation. Every other request is the gateway's, whatever it answers.
fn is_legacy_only_route(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    let path = uri.path();
    is_admin_path(path)
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
            Some(gateway) if !is_legacy_only_route(request.method(), request.uri(), request.headers()) => {
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

    fn gateway_stack() -> S3StackService<Named, Named> {
        select_stack(S3Stack::Gateway, Named("legacy"), || Ok::<_, Infallible>(Named("gateway"))).expect("assembles")
    }

    #[test]
    fn a_gateway_assembly_failure_fails_the_selection_instead_of_falling_back() {
        let selection = select_stack::<_, Named, _>(S3Stack::Gateway, Named("legacy"), || Err("the assembly was refused"));

        assert_eq!(selection.err(), Some("the assembly was refused"));
    }

    #[test]
    fn the_legacy_selection_never_assembles_a_gateway() {
        let service = select_stack::<_, Named, &str>(S3Stack::Legacy, Named("legacy"), || {
            panic!("the legacy stack must not assemble the gateway")
        })
        .expect("legacy needs no assembly");

        assert_eq!(service.selected(), S3Stack::Legacy);
    }

    #[tokio::test]
    async fn the_legacy_stack_answers_every_request_itself() {
        let mut service = select_stack(S3Stack::Legacy, Named("legacy"), || Ok::<_, Infallible>(Named("gateway")))
            .expect("legacy needs no assembly");

        for target in ["/", "/bucket/key", "/rustfs/admin/v3/info", "/health"] {
            assert_eq!(answered_by(&mut service, request(Method::GET, target)).await, "legacy", "{target}");
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
}
