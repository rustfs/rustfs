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
//! `BodylessStatusFixLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s serialises every `S3Error` as an XML body, including the
//! `1xx`/`204`/`205`/`304` responses that RFC 9110 §15 forbids a body on; h2
//! clients read the extra DATA frames as a connection failure (`GOAWAY`).
//! Introduced in rustfs/rustfs#2627.
//! Replaced by: the gateway's response invariants, applied once to every
//! response (gateway `docs/middleware.md`, "The nine RustFS tower patch
//! layers", row 1).
//! Removed by: T2.12 (rustfs/backlog#2771).

use crate::server::hybrid::HybridBody;
use bytes::Bytes;
use http::{Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Tower middleware that strips the body (and body-describing headers) from
/// responses whose HTTP status code MUST NOT carry a body per RFC 9110 §6.4.1
/// and §15 (1xx, 204, 205, 304).
///
/// s3s already suppresses bodyless error responses. This layer also protects
/// REST routes and custom responses: unlike the HTTP/1.1 encoder, hyper's
/// HTTP/2 sender relies on the body rather than the status to end the stream.
#[derive(Clone)]
pub struct BodylessStatusFixLayer;

impl<S> Layer<S> for BodylessStatusFixLayer {
    type Service = BodylessStatusFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BodylessStatusFixService { inner }
    }
}

#[derive(Clone)]
pub struct BodylessStatusFixService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for BodylessStatusFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    RestBody::Error: Into<S::Error> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !is_bodyless_status(response.status()) {
                return Ok(response);
            }

            let (mut parts, body) = response.into_parts();
            let response = match body {
                HybridBody::Rest { .. } => {
                    parts.headers.remove(http::header::CONTENT_LENGTH);
                    parts.headers.remove(http::header::CONTENT_TYPE);
                    parts.headers.remove(http::header::TRANSFER_ENCODING);
                    Response::from_parts(
                        parts,
                        HybridBody::Rest {
                            rest_body: RestBody::from(Bytes::new()),
                        },
                    )
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

fn is_bodyless_status(status: StatusCode) -> bool {
    status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::RESET_CONTENT
        || status == StatusCode::NOT_MODIFIED
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;
    use http_body_util::Empty;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;

    // The production service takes `Request<Incoming>`, but `Incoming` can't be
    // constructed in unit tests. `BodylessStatusFixService` doesn't inspect the
    // request body, so parameterising over an arbitrary `B` is safe here.
    #[derive(Clone)]
    struct FixedResponse {
        status: StatusCode,
        body: Bytes,
        content_type: Option<&'static str>,
    }

    impl<B: Send + 'static> Service<Request<B>> for FixedResponse {
        type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            let this = self.clone();
            Box::pin(async move {
                let body = this.body.clone();
                let len = body.len();
                let mut builder = Response::builder().status(this.status);
                builder = builder.header(http::header::CONTENT_LENGTH, len.to_string());
                if let Some(ct) = this.content_type {
                    builder = builder.header(http::header::CONTENT_TYPE, ct);
                }
                builder = builder.header(http::header::ETAG, "\"abc123\"");
                Ok(builder
                    .body(HybridBody::Rest {
                        rest_body: Full::from(body),
                    })
                    .expect("build response"))
            })
        }
    }

    fn empty_request() -> Request<()> {
        Request::builder().uri("/").body(()).expect("request")
    }

    async fn collect_body<B: Body<Data = Bytes>>(body: B) -> Bytes
    where
        B::Error: std::fmt::Debug,
    {
        BodyExt::collect(body).await.expect("collect body").to_bytes()
    }

    #[tokio::test]
    async fn strips_body_and_content_headers_for_304() {
        let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
            status: StatusCode::NOT_MODIFIED,
            body: Bytes::from_static(b"<Error><Code>NotModified</Code></Error>"),
            content_type: Some("application/xml"),
        });

        let res = svc.call(empty_request()).await.expect("service call");
        let (parts, body) = res.into_parts();

        assert_eq!(parts.status, StatusCode::NOT_MODIFIED);
        assert!(parts.headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(parts.headers.get(http::header::CONTENT_TYPE).is_none());
        assert_eq!(parts.headers.get(http::header::ETAG).unwrap(), "\"abc123\"");

        let bytes = collect_body(body).await;
        assert!(bytes.is_empty(), "304 response body must be empty");
    }

    #[tokio::test]
    async fn bodyless_304_is_valid_on_http1_and_http2() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::service::TowerToHyperService;
        for http2 in [false, true] {
            let (client_io, server_io) = tokio::io::duplex(4096);
            let service = TowerToHyperService::new(BodylessStatusFixLayer.layer(FixedResponse {
                status: StatusCode::NOT_MODIFIED,
                body: Bytes::from_static(b"<Error><Code>NotModified</Code></Error>"),
                content_type: Some("application/xml"),
            }));
            let server = tokio::spawn(async move {
                if http2 {
                    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(server_io), service)
                        .await
                        .expect("HTTP/2 server");
                } else {
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(server_io), service)
                        .await
                        .expect("HTTP/1 server");
                }
            });
            let request = Request::builder()
                .uri("http://localhost/object")
                .body(Empty::<Bytes>::new())
                .expect("request");
            let (response, client) = if http2 {
                let (mut sender, connection) =
                    hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
                        .await
                        .expect("HTTP/2 handshake");
                let client = tokio::spawn(async move {
                    connection.await.expect("HTTP/2 client");
                });
                (sender.send_request(request).await.expect("HTTP/2 response"), client)
            } else {
                let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                    .await
                    .expect("HTTP/1 handshake");
                let client = tokio::spawn(async move {
                    connection.await.expect("HTTP/1 client");
                });
                (sender.send_request(request).await.expect("HTTP/1 response"), client)
            };
            assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
            assert_eq!(response.headers()[http::header::ETAG], "\"abc123\"");
            assert!(!response.headers().contains_key(http::header::CONTENT_LENGTH));
            assert!(!response.headers().contains_key(http::header::CONTENT_TYPE));
            assert!(collect_body(response.into_body()).await.is_empty());
            client.abort();
            server.abort();
        }
    }

    #[tokio::test]
    async fn strips_body_for_204() {
        let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
            status: StatusCode::NO_CONTENT,
            body: Bytes::from_static(b"unexpected"),
            content_type: None,
        });

        let res = svc.call(empty_request()).await.expect("service call");
        let (parts, body) = res.into_parts();

        assert_eq!(parts.status, StatusCode::NO_CONTENT);
        assert!(parts.headers.get(http::header::CONTENT_LENGTH).is_none());

        let bytes = collect_body(body).await;
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn preserves_body_for_200() {
        let payload = Bytes::from_static(b"hello");
        let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
            status: StatusCode::OK,
            body: payload.clone(),
            content_type: Some("text/plain"),
        });

        let res = svc.call(empty_request()).await.expect("service call");
        let (parts, body) = res.into_parts();

        assert_eq!(parts.status, StatusCode::OK);
        assert_eq!(parts.headers.get(http::header::CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(
            parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
            payload.len().to_string().as_str()
        );

        let bytes = collect_body(body).await;
        assert_eq!(bytes, payload);
    }

    #[test]
    fn is_bodyless_status_matches_rfc9110_statuses() {
        assert!(is_bodyless_status(StatusCode::CONTINUE));
        assert!(is_bodyless_status(StatusCode::SWITCHING_PROTOCOLS));
        assert!(is_bodyless_status(StatusCode::NO_CONTENT));
        assert!(is_bodyless_status(StatusCode::RESET_CONTENT));
        assert!(is_bodyless_status(StatusCode::NOT_MODIFIED));

        assert!(!is_bodyless_status(StatusCode::OK));
        assert!(!is_bodyless_status(StatusCode::PARTIAL_CONTENT));
        assert!(!is_bodyless_status(StatusCode::NOT_FOUND));
        assert!(!is_bodyless_status(StatusCode::PRECONDITION_FAILED));
        assert!(!is_bodyless_status(StatusCode::INTERNAL_SERVER_ERROR));
    }
}
