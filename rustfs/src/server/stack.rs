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
//! refuses is answered by that refusal and never retried on the legacy stack. With the gateway
//! selected, console static requests and signed system-status reads use their native
//! services; the legacy service receives the remaining claimed extension and control-plane routes.
//! NOT responsible for: assembling either stack (`server/http.rs` builds the legacy service,
//! `server/gateway_stack.rs` the gateway one), or the middleware around them.
//! Upstream: `server/http.rs`. Downstream: the native console, legacy S3 service and gateway stack.
//!
//! This file names no stack's types: it is generic over the services with one response
//! type, so `server/http.rs` decides what they are and how their bodies meet.

use crate::admin::router::{claims_native_admin_route, claims_native_console_route, claims_route};
use crate::config::S3Stack;
use http::Request;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tower::Service;

/// The S3 service of one server: the legacy stack, or the gateway stack with the legacy router
/// kept for the routes that are not S3 operations.
#[derive(Clone)]
pub(crate) struct S3StackService<L, G, C = G> {
    legacy: L,
    gateway: Option<G>,
    native_console: Option<C>,
    native_admin: Option<G>,
}

impl<L, G, C> S3StackService<L, G, C> {
    /// Attaches the console service used only by the gateway selection.
    pub(crate) fn with_native_console<N>(self, native_console: Option<N>) -> S3StackService<L, G, N> {
        S3StackService {
            legacy: self.legacy,
            gateway: self.gateway,
            native_console,
            native_admin: self.native_admin,
        }
    }

    /// Attaches the signed admin service used only by the gateway selection.
    pub(crate) fn with_native_admin(mut self, native_admin: Option<G>) -> Self {
        self.native_admin = native_admin;
        self
    }

    /// The stack this service was built with, as the startup line names it: read from the service
    /// that answers, never from the configuration that asked for it.
    pub(crate) fn assembled(&self) -> &'static str {
        match self.gateway {
            Some(_) => "gateway",
            None => "legacy",
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
    Ok(S3StackService {
        legacy,
        gateway,
        native_console: None,
        native_admin: None,
    })
}

impl<L, G, C, B, R, E> Service<Request<B>> for S3StackService<L, G, C>
where
    L: Service<Request<B>, Response = http::Response<R>, Error = E>,
    G: Service<Request<B>, Response = http::Response<R>, Error = E>,
    C: Service<Request<B>, Response = http::Response<R>, Error = E>,
    L::Future: Send + 'static,
    G::Future: Send + 'static,
    C::Future: Send + 'static,
    R: Default,
{
    type Response = http::Response<R>;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(gateway) = &mut self.gateway {
            ready!(gateway.poll_ready(cx))?;
            if let Some(admin) = &mut self.native_admin {
                ready!(admin.poll_ready(cx))?;
            }
            if let Some(console) = &mut self.native_console {
                ready!(console.poll_ready(cx))?;
            }
        }
        self.legacy.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        if let Some(gateway) = &mut self.gateway {
            if claims_route(request.method(), request.uri(), request.headers())
                && (request.headers().contains_key("x-auth-token") || request.headers().contains_key("x-storage-token"))
            {
                return Box::pin(async {
                    let mut response = http::Response::new(R::default());
                    *response.status_mut() = http::StatusCode::FORBIDDEN;
                    Ok(response)
                });
            }
            if let Some(console) = &mut self.native_console
                && claims_native_console_route(request.method(), request.uri())
            {
                return Box::pin(console.call(request));
            }
            if let Some(admin) = &mut self.native_admin
                && claims_native_admin_route(request.method(), request.uri())
            {
                return Box::pin(admin.call(request));
            }
            if !claims_route(request.method(), request.uri(), request.headers()) {
                return Box::pin(gateway.call(request));
            }
        }
        Box::pin(self.legacy.call(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Response, StatusCode};
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

        assert_eq!(service.assembled(), "legacy");
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
        assert_eq!(service.assembled(), "gateway");

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
            // The control-plane paths under a method or a suffix the legacy router does not answer.
            (Method::PUT, "/health/x"),
            (Method::GET, "/health/x"),
            (Method::PUT, "/profile/cpu"),
            (Method::DELETE, "/health"),
            (Method::GET, "/minio/health/x"),
            (Method::GET, "/minio/health/live"),
            // Near misses of the RustFS extension routes: a wrong method, an object level.
            (Method::POST, "/bucket?replication-reset"),
            (Method::GET, "/bucket/key?events"),
            (Method::PUT, "/bucket?events"),
            (Method::GET, "/bucket?lambdaArn=arn"),
        ] {
            assert_eq!(
                answered_by(&mut service, request(method.clone(), target)).await,
                "gateway",
                "{method} {target}"
            );
        }
    }

    #[tokio::test]
    async fn the_gateway_stack_leaves_the_routes_the_legacy_router_claims_to_it() {
        let mut service = gateway_stack();

        for (method, target) in [
            (Method::GET, "/rustfs/admin/v3/info"),
            (Method::PUT, "/rustfs/admin/v3/add-user"),
            (Method::GET, "/minio/admin/v3/info"),
            (Method::GET, "/iceberg/v1/config"),
            (Method::GET, "/health"),
            (Method::HEAD, "/health/ready"),
            (Method::GET, "/profile/cpu"),
            (Method::GET, "/profile/memory"),
            (Method::GET, "/bucket?replication-metrics"),
            (Method::PUT, "/bucket?replication-reset"),
            (Method::GET, "/bucket?events"),
            (Method::GET, "/?events="),
            (Method::GET, "/bucket/key?lambdaArn=arn"),
        ] {
            assert_eq!(
                answered_by(&mut service, request(method.clone(), target)).await,
                "legacy",
                "{method} {target}"
            );
        }
    }

    #[derive(Clone)]
    struct Counting {
        name: &'static str,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Service<Request<()>> for Counting {
        type Response = Response<&'static str>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _request: Request<()>) -> Self::Future {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ready_future(Ok(Response::new(self.name)))
        }
    }

    #[tokio::test]
    async fn gateway_keystone_tokens_are_refused_before_non_s3_legacy_dispatch() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let console_path = format!("{}/index.html", crate::server::console_prefix());
        for stack in [S3Stack::Gateway, S3Stack::Legacy] {
            for (method, target) in [
                (Method::GET, "/rustfs/admin/v3/info"),
                (Method::GET, "/minio/admin/v3/info"),
                (Method::GET, "/rustfs/admin/v3/datausageinfo"),
                (Method::GET, "/minio/admin/v3/datausageinfo"),
                (Method::GET, "/rustfs/admin/v4/plugins/catalog"),
                (Method::GET, "/minio/admin/v4/plugins/catalog"),
                (Method::GET, "/rustfs/admin/v3/module-switches"),
                (Method::GET, "/minio/admin/v3/module-switches"),
                (Method::POST, "/?Action=AssumeRole&Version=2011-06-15"),
                (Method::GET, "/bucket?replication-metrics"),
                (Method::GET, console_path.as_str()),
            ] {
                for header in [None, Some("x-auth-token"), Some("x-storage-token")] {
                    let legacy_calls = Arc::new(AtomicUsize::new(0));
                    let gateway_calls = Arc::new(AtomicUsize::new(0));
                    let console_calls = Arc::new(AtomicUsize::new(0));
                    let admin_calls = Arc::new(AtomicUsize::new(0));
                    let legacy = Counting {
                        name: "legacy",
                        calls: Arc::clone(&legacy_calls),
                    };
                    let gateway = Counting {
                        name: "gateway",
                        calls: Arc::clone(&gateway_calls),
                    };
                    let console = Counting {
                        name: "console",
                        calls: Arc::clone(&console_calls),
                    };
                    let admin = Counting {
                        name: "admin",
                        calls: Arc::clone(&admin_calls),
                    };
                    let mut service = select_stack(stack, legacy, || Ok::<_, Infallible>(gateway))
                        .unwrap()
                        .with_native_console(Some(console))
                        .with_native_admin(Some(admin));
                    let mut request = request(method.clone(), target);
                    if method == Method::POST {
                        request
                            .headers_mut()
                            .insert(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded".parse().unwrap());
                    }
                    if let Some(header) = header {
                        request.headers_mut().insert(header, "validated-token".parse().unwrap());
                    }
                    let response = service.call(request).await.unwrap();
                    if stack == S3Stack::Gateway && header.is_some() {
                        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{target} {header:?}");
                        assert_eq!(legacy_calls.load(Ordering::SeqCst), 0, "refused request reached legacy");
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0, "refused request reached native console");
                        assert_eq!(admin_calls.load(Ordering::SeqCst), 0, "refused request reached native admin");
                    } else if stack == S3Stack::Gateway && target == console_path {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"console");
                        assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
                        assert_eq!(console_calls.load(Ordering::SeqCst), 1);
                    } else if stack == S3Stack::Gateway
                        && (target.starts_with("/rustfs/admin/") || target.starts_with("/minio/admin/"))
                    {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"admin");
                        assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0);
                    } else {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"legacy");
                        assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0);
                    }
                    assert_eq!(gateway_calls.load(Ordering::SeqCst), 0, "non-S3 request reached gateway");
                    assert_eq!(
                        admin_calls.load(Ordering::SeqCst),
                        usize::from(
                            stack == S3Stack::Gateway
                                && header.is_none()
                                && (target.starts_with("/rustfs/admin/") || target.starts_with("/minio/admin/"))
                        )
                    );
                }
            }
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

        assert_eq!(answered_by(&mut service, sts).await, "legacy");
        assert_eq!(answered_by(&mut service, request(Method::POST, "/")).await, "gateway");
    }
    #[tokio::test]
    async fn native_admin_selection_is_exact_and_host_independent() {
        let mut service = gateway_stack().with_native_admin(Some(Named("admin")));
        for host in ["localhost", "bucket.s3.example.test"] {
            for target in [
                "/rustfs/admin/v3/datausageinfo",
                "/minio/admin/v3/datausageinfo?detail=1",
                "/rustfs/admin/v4/plugins/catalog",
                "/minio/admin/v4/plugins/catalog?detail=1",
                "/rustfs/admin/v3/module-switches",
                "/minio/admin/v3/module-switches?detail=1",
                "/rustfs/admin/v3/info",
                "/minio/admin/v3/info?detail=1",
                "/rustfs/admin/v3/storageinfo",
                "/minio/admin/v3/storageinfo?detail=a%2Fb",
                "/rustfs/admin/v4/runtime/capabilities",
                "/minio/admin/v4/runtime/capabilities?detail=a%2Fb",
                "/rustfs/admin/v3/object-data-cache/stats",
                "/minio/admin/v3/object-data-cache/stats?detail=a%2Fb",
            ] {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri(target)
                    .header("host", host)
                    .body(())
                    .unwrap();
                assert_eq!(answered_by(&mut service, req).await, "admin", "{host} {target}");
            }
        }
        for suffix in [
            "/v3/storageinfo",
            "/v4/runtime/capabilities",
            "/v3/object-data-cache/stats",
            "/v3/datausageinfo",
            "/v4/plugins/catalog",
            "/v3/module-switches",
        ] {
            for prefix in ["/rustfs/admin", "/minio/admin"] {
                for (method, tail) in [
                    (Method::POST, ""),
                    (Method::PUT, ""),
                    (Method::HEAD, ""),
                    (Method::DELETE, ""),
                    (Method::GET, "/"),
                    (Method::GET, "-neighbor"),
                    (Method::GET, "?lambdaArn=arn"),
                    (Method::GET, "?lambdaArn="),
                    (Method::GET, "?lambdaArn=one&lambdaArn=two"),
                    (Method::GET, "?lambdaArn=%ZZ"),
                ] {
                    let target = format!("{prefix}{suffix}{tail}");
                    assert_eq!(answered_by(&mut service, request(method, &target)).await, "legacy", "{target}");
                }
            }
        }
        for (method, target) in [
            (Method::GET, "/rustfs/admin/v3/info/"),
            (Method::GET, "/rustfs/admin/v3/info-neighbor"),
            (Method::GET, "/rustfs/admin//v3/info"),
            (Method::GET, "/%72ustfs/admin/v3/info"),
            (Method::GET, "/rustfs/admin/v4/info"),
            (Method::POST, "/rustfs/admin/v3/info"),
            (Method::GET, "/iceberg/v1/config"),
            (Method::GET, "/profile/cpu"),
            (Method::GET, "/bucket?events"),
            (Method::GET, "/bucket/key"),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=arn"),
            (Method::GET, "/minio/admin/v3/info?lambdaArn="),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=one&lambdaArn=two"),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=%ZZ"),
        ] {
            let expected = if claims_route(&method, &target.parse().unwrap(), &http::HeaderMap::new()) {
                "legacy"
            } else {
                "gateway"
            };
            assert_eq!(answered_by(&mut service, request(method, target)).await, expected, "{target}");
        }
        let mut legacy = select_stack(S3Stack::Legacy, Named("legacy"), || Ok::<_, Infallible>(Named("gateway")))
            .unwrap()
            .with_native_admin(Some(Named("admin")));
        assert_eq!(answered_by(&mut legacy, request(Method::GET, "/rustfs/admin/v3/info")).await, "legacy");
    }
}
