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

//! The S3 protocol stack one server runs (rustfs/backlog#2734 T2.5).
//!
//! Responsible for: [`S3StackService`], the service mounted under the middleware. A request to a
//! website host is answered by the website service before any other routing; console static
//! requests and signed system-status reads use their native services; every other route the
//! admin router claims goes to the control-plane host, and every remaining request is the gateway
//! S3 service's.
//! NOT responsible for: assembling the stack (`server/gateway_stack.rs`), or the middleware around
//! it (`server/http.rs`).
//! Upstream: `server/http.rs`. Downstream: the native website and console, the gateway stack and
//! its control-plane host.
//!
//! This file names no stack's types: it is generic over the services with one response
//! type, so `server/http.rs` decides what they are and how their bodies meet.

use crate::admin::router::{claims_native_admin_route, claims_native_console_route, claims_route};
use http::Request;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tower::Service;

/// The S3 service of one server: the gateway stack with its website and control-plane services.
#[derive(Clone)]
pub(crate) struct S3StackService<G, C = G, W = G> {
    services: GatewayServices<G, C, W>,
}

/// Whether a request's target and `Host` select the service it is paired with.
pub(crate) type HostClaim = Arc<dyn Fn(&http::Uri, &http::HeaderMap) -> bool + Send + Sync>;

/// The gateway S3 service, the website host, and the hosts of the routes the admin router claims.
#[derive(Clone)]
struct GatewayServices<G, C, W> {
    s3: G,
    native_website: Option<(HostClaim, W)>,
    native_console: Option<C>,
    native_admin: Option<G>,
    control_plane: Option<G>,
}

impl<G> S3StackService<G> {
    /// The stack around the gateway S3 service `s3`, with no other host attached yet.
    pub(crate) fn new(s3: G) -> Self {
        Self {
            services: GatewayServices {
                s3,
                native_website: None,
                native_console: None,
                native_admin: None,
                control_plane: None,
            },
        }
    }
}

impl<G, C, W> S3StackService<G, C, W> {
    /// Attaches the native console service.
    pub(crate) fn with_native_console<N>(self, native_console: Option<N>) -> S3StackService<G, N, W> {
        let services = self.services;
        S3StackService {
            services: GatewayServices {
                s3: services.s3,
                native_website: services.native_website,
                native_console,
                native_admin: services.native_admin,
                control_plane: services.control_plane,
            },
        }
    }

    /// Attaches the website service: every request `claim` selects goes to it before any other
    /// routing.
    pub(crate) fn with_native_website<N>(self, native_website: Option<(HostClaim, N)>) -> S3StackService<G, C, N> {
        let services = self.services;
        S3StackService {
            services: GatewayServices {
                s3: services.s3,
                native_website,
                native_console: services.native_console,
                native_admin: services.native_admin,
                control_plane: services.control_plane,
            },
        }
    }

    /// Attaches the signed admin service.
    pub(crate) fn with_native_admin(mut self, native_admin: Option<G>) -> Self {
        self.services.native_admin = native_admin;
        self
    }

    /// Attaches the host of every other claimed route.
    pub(crate) fn with_control_plane(mut self, control_plane: Option<G>) -> Self {
        self.services.control_plane = control_plane;
        self
    }
}

type Answer<R, E> = Pin<Box<dyn Future<Output = Result<http::Response<R>, E>> + Send>>;

/// An answer with `status` and no body, written without any service.
fn bare<R: Default, E>(status: http::StatusCode) -> Answer<R, E> {
    Box::pin(async move {
        let mut response = http::Response::new(R::default());
        *response.status_mut() = status;
        Ok(response)
    })
}

impl<G, C, W> GatewayServices<G, C, W> {
    fn call<B, R, E>(&mut self, request: Request<B>) -> Answer<R, E>
    where
        G: Service<Request<B>, Response = http::Response<R>, Error = E>,
        C: Service<Request<B>, Response = http::Response<R>, Error = E>,
        W: Service<Request<B>, Response = http::Response<R>, Error = E>,
        G::Future: Send + 'static,
        C::Future: Send + 'static,
        W::Future: Send + 'static,
        R: Default,
    {
        if let Some((claim, website)) = &mut self.native_website
            && claim(request.uri(), request.headers())
        {
            return Box::pin(website.call(request));
        }
        let claimed = claims_route(request.method(), request.uri(), request.headers());
        if claimed && (request.headers().contains_key("x-auth-token") || request.headers().contains_key("x-storage-token")) {
            return bare(http::StatusCode::FORBIDDEN);
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
        if !claimed {
            return Box::pin(self.s3.call(request));
        }
        match &mut self.control_plane {
            Some(control_plane) => Box::pin(control_plane.call(request)),
            // Only a selection assembled without its host gets here; it answers, never falls back.
            None => bare(http::StatusCode::NOT_IMPLEMENTED),
        }
    }
}

impl<G, C, W, B, R, E> Service<Request<B>> for S3StackService<G, C, W>
where
    G: Service<Request<B>, Response = http::Response<R>, Error = E>,
    C: Service<Request<B>, Response = http::Response<R>, Error = E>,
    W: Service<Request<B>, Response = http::Response<R>, Error = E>,
    G::Future: Send + 'static,
    C::Future: Send + 'static,
    W::Future: Send + 'static,
    R: Default,
{
    type Response = http::Response<R>;
    type Error = E;
    type Future = Answer<R, E>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let services = &mut self.services;
        ready!(services.s3.poll_ready(cx))?;
        for host in [&mut services.native_admin, &mut services.control_plane]
            .into_iter()
            .flatten()
        {
            ready!(host.poll_ready(cx))?;
        }
        if let Some(console) = &mut services.native_console {
            ready!(console.poll_ready(cx))?;
        }
        if let Some((_, website)) = &mut services.native_website {
            ready!(website.poll_ready(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.services.call(request)
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
        S3StackService::new(Named("gateway")).with_control_plane(Some(Named("control-plane")))
    }

    #[tokio::test]
    async fn the_gateway_stack_answers_every_s3_request_whatever_the_operation() {
        let mut service = gateway_stack();

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
    async fn the_gateway_stack_hands_the_routes_the_legacy_router_claims_to_its_control_plane() {
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
                "control-plane",
                "{method} {target}"
            );
        }
    }

    #[tokio::test]
    async fn a_gateway_selection_without_its_host_answers_a_claimed_route_itself() {
        let mut service = S3StackService::new(Named("gateway"));
        let response = service
            .call(request(Method::PUT, "/rustfs/admin/v3/add-user"))
            .await
            .expect("infallible");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(response.into_body(), "");
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
    async fn gateway_keystone_tokens_are_refused_before_non_s3_dispatch() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let console_path = format!("{}/index.html", crate::server::console_prefix());
        {
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
                    let gateway_calls = Arc::new(AtomicUsize::new(0));
                    let console_calls = Arc::new(AtomicUsize::new(0));
                    let admin_calls = Arc::new(AtomicUsize::new(0));
                    let control_calls = Arc::new(AtomicUsize::new(0));
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
                    let control = Counting {
                        name: "control-plane",
                        calls: Arc::clone(&control_calls),
                    };
                    let mut service = S3StackService::new(gateway)
                        .with_native_console(Some(console))
                        .with_native_admin(Some(admin))
                        .with_control_plane(Some(control));
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
                    if header.is_some() {
                        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{target} {header:?}");
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0, "refused request reached native console");
                        assert_eq!(admin_calls.load(Ordering::SeqCst), 0, "refused request reached native admin");
                        assert_eq!(control_calls.load(Ordering::SeqCst), 0, "refused request reached the control plane");
                    } else if target == console_path {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"console");
                        assert_eq!(console_calls.load(Ordering::SeqCst), 1);
                    } else if target.starts_with("/rustfs/admin/") || target.starts_with("/minio/admin/") {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"admin");
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0);
                    } else {
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(response.body(), &"control-plane");
                        assert_eq!(control_calls.load(Ordering::SeqCst), 1);
                        assert_eq!(console_calls.load(Ordering::SeqCst), 0);
                    }
                    assert_eq!(gateway_calls.load(Ordering::SeqCst), 0, "non-S3 request reached gateway");
                    assert_eq!(
                        admin_calls.load(Ordering::SeqCst),
                        usize::from(
                            header.is_none() && (target.starts_with("/rustfs/admin/") || target.starts_with("/minio/admin/"))
                        )
                    );
                }
            }
        }
    }

    /// Every class of request the gateway selection can be sent, the claimed ones included.
    fn every_request_class() -> Vec<Request<()>> {
        let console = format!("{}/index.html", crate::server::console_prefix());
        let mut requests: Vec<Request<()>> = [
            (Method::GET, "/rustfs/admin/v3/list-users"),
            (Method::PUT, "/rustfs/admin/v3/add-user?accessKey=a"),
            (Method::GET, "/minio/admin/v3/list-users"),
            (Method::GET, "/rustfs/admin/v3/info"),
            (Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip?token=t"),
            (Method::GET, "/iceberg/v1/config"),
            (Method::GET, "/_iceberg/v1/ns/tables"),
            (Method::GET, "/profile/cpu"),
            (Method::GET, "/health"),
            (Method::PUT, "/bucket?replication-reset"),
            (Method::GET, "/bucket?events=s3:ObjectCreated:*"),
            (Method::GET, "/bucket/key?lambdaArn=arn"),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=arn"),
            (Method::GET, console.as_str()),
            (Method::GET, "/bucket/key"),
            (Method::PUT, "/bucket"),
        ]
        .into_iter()
        .map(|(method, target)| request(method, target))
        .collect();
        let mut sts = request(Method::POST, "/");
        sts.headers_mut()
            .insert(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded".parse().unwrap());
        requests.push(sts);
        let mut keystone = request(Method::GET, "/rustfs/admin/v3/list-users");
        keystone.headers_mut().insert("x-auth-token", "token".parse().unwrap());
        requests.push(keystone);
        requests
    }

    #[tokio::test]
    async fn every_request_class_reaches_one_gateway_service() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = |name| Counting {
            name,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let (gateway, console, admin, control) =
            (counter("gateway"), counter("console"), counter("admin"), counter("control-plane"));
        let calls = [&gateway, &console, &admin, &control].map(|service| Arc::clone(&service.calls));
        let mut service = S3StackService::new(gateway)
            .with_native_console(Some(console))
            .with_native_admin(Some(admin))
            .with_control_plane(Some(control));
        let requests = every_request_class();
        let count = requests.len();
        for request in requests {
            service.call(request).await.unwrap();
        }

        let [gateway, console, admin, control] = calls.map(|calls| calls.load(Ordering::SeqCst));
        assert!(
            gateway > 0 && console > 0 && admin > 0 && control > 0,
            "{gateway} {console} {admin} {control}"
        );
        // The Keystone token is refused by the selection itself.
        assert_eq!(gateway + console + admin + control, count - 1);
    }

    #[tokio::test]
    async fn an_sts_form_post_goes_to_the_control_plane_and_a_plain_post_does_not() {
        let mut service = gateway_stack();
        let sts = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(())
            .expect("static request");

        assert_eq!(answered_by(&mut service, sts).await, "control-plane");
        assert_eq!(answered_by(&mut service, request(Method::POST, "/")).await, "gateway");
    }
    /// A gateway stack whose website host claims every request to `*.website.test`.
    fn gateway_stack_with_website() -> S3StackService<Named, Named, Named> {
        let claim: HostClaim = Arc::new(|_uri: &http::Uri, headers: &http::HeaderMap| {
            headers
                .get(http::header::HOST)
                .and_then(|host| host.to_str().ok())
                .is_some_and(|host| host.ends_with(".website.test"))
        });
        gateway_stack()
            .with_native_admin(Some(Named("admin")))
            .with_native_console(Some(Named("console")))
            .with_native_website(Some((claim, Named("website"))))
    }

    fn to_host(method: Method, target: &str, host: &str, extra: Option<(&'static str, &'static str)>) -> Request<()> {
        let mut builder = Request::builder().method(method).uri(target).header(http::header::HOST, host);
        if let Some((name, value)) = extra {
            builder = builder.header(name, value);
        }
        builder.body(()).expect("static request")
    }

    #[tokio::test]
    async fn a_website_host_is_answered_by_the_website_service_before_any_other_route() {
        let mut service = gateway_stack_with_website();
        let console = format!("{}/index.html", crate::server::console_prefix());
        for (method, target, extra) in [
            (Method::GET, "/", None),
            (Method::HEAD, "/index.html", None),
            (Method::POST, "/", Some(("content-type", "application/x-www-form-urlencoded"))),
            (Method::GET, "/rustfs/admin/v3/info", None),
            (Method::GET, "/rustfs/admin/v3/info", Some(("x-auth-token", "token"))),
            (Method::GET, "/rustfs/admin/v3/datausageinfo", None),
            (Method::GET, "/health", None),
            (Method::GET, "/profile/cpu", None),
            (Method::GET, "/bucket?replication-metrics", None),
            (Method::GET, "/bucket?list-type=2&metadata=true", None),
            (Method::GET, console.as_str(), None),
            (Method::PUT, "/bucket/key", None),
        ] {
            let request = to_host(method.clone(), target, "site.website.test", extra);
            assert_eq!(answered_by_any(&mut service, request).await, "website", "{method} {target} {extra:?}");
        }
    }

    #[tokio::test]
    async fn a_request_to_any_other_host_never_reaches_the_website_service() {
        let mut service = gateway_stack_with_website();
        let console = format!("{}/index.html", crate::server::console_prefix());
        for (method, target, host, expected) in [
            (Method::GET, "/", "localhost", "gateway"),
            (Method::GET, "/bucket/key", "website.test.example", "gateway"),
            (Method::GET, "/bucket?list-type=2&metadata=true", "localhost", "gateway"),
            (Method::GET, "/rustfs/admin/v3/info", "localhost", "admin"),
            (Method::GET, "/health", "localhost", "control-plane"),
            (Method::GET, "/bucket?replication-metrics", "s3.example.test", "control-plane"),
            (Method::GET, console.as_str(), "localhost", "console"),
        ] {
            let request = to_host(method.clone(), target, host, None);
            assert_eq!(answered_by_any(&mut service, request).await, expected, "{method} {host} {target}");
        }
        let refused = service
            .call(to_host(Method::GET, "/health", "localhost", Some(("x-auth-token", "token"))))
            .await
            .expect("infallible");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN, "a Keystone token on a claimed route");
    }

    async fn answered_by_any<C, W>(service: &mut S3StackService<Named, C, W>, request: Request<()>) -> &'static str
    where
        C: Service<Request<()>, Response = Response<&'static str>, Error = Infallible>,
        W: Service<Request<()>, Response = Response<&'static str>, Error = Infallible>,
        C::Future: Send + 'static,
        W::Future: Send + 'static,
    {
        let response = service.call(request).await.expect("infallible");
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body()
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
                "/rustfs/admin/v4/cluster/snapshot",
                "/minio/admin/v4/cluster/snapshot?detail=1",
                "/rustfs/admin/v4/extensions/catalog",
                "/minio/admin/v4/extensions/catalog?detail=1",
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
                "/iceberg/v1/config",
                "/_iceberg/v1/config?warehouse=warehouse-a",
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
            "/v4/cluster/snapshot",
            "/v4/extensions/catalog",
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
                    assert_eq!(answered_by(&mut service, request(method, &target)).await, "control-plane", "{target}");
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
            (Method::POST, "/iceberg/v1/config"),
            (Method::POST, "/_iceberg/v1/config"),
            (Method::GET, "/iceberg/v1/config/"),
            (Method::GET, "/_iceberg/v1/config-neighbor"),
            (Method::GET, "/iceberg/v10/config"),
            (Method::GET, "/_iceberg/v1suffix/config"),
            (Method::GET, "/profile/cpu"),
            (Method::GET, "/bucket?events"),
            (Method::GET, "/bucket/key"),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=arn"),
            (Method::GET, "/minio/admin/v3/info?lambdaArn="),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=one&lambdaArn=two"),
            (Method::GET, "/rustfs/admin/v3/info?lambdaArn=%ZZ"),
        ] {
            let expected = if claims_route(&method, &target.parse().unwrap(), &http::HeaderMap::new()) {
                "control-plane"
            } else {
                "gateway"
            };
            assert_eq!(answered_by(&mut service, request(method, target)).await, expected, "{target}");
        }
    }
}
