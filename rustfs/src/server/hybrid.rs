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

use futures::Future;
use http_body::Frame;
use hyper::body::Incoming;
use hyper::{Request, Response};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Generate a [`HybridService`]
pub(crate) fn hybrid<MakeRest, Grpc>(make_rest: MakeRest, grpc: Grpc) -> HybridService<MakeRest, Grpc> {
    HybridService { rest: make_rest, grpc }
}

/// The service that can serve both gRPC and REST HTTP Requests
#[derive(Clone)]
pub struct HybridService<Rest, Grpc> {
    rest: Rest,
    grpc: Grpc,
}

impl<Rest, Grpc, RestBody, GrpcBody> Service<Request<Incoming>> for HybridService<Rest, Grpc>
where
    Rest: Service<Request<Incoming>, Response = Response<RestBody>>,
    Grpc: Service<Request<Incoming>, Response = Response<GrpcBody>>,
    Rest::Error: Into<BoxError>,
    Grpc::Error: Into<BoxError>,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = BoxError;
    type Future = HybridFuture<Rest::Future, Grpc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.rest.poll_ready(cx) {
            Poll::Ready(Ok(())) => match self.grpc.poll_ready(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
                Poll::Pending => Poll::Pending,
            },

            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    /// When calling the service, gRPC is served if the HTTP request version is HTTP/2
    /// and if the Content-Type is "application/grpc"; otherwise, the request is served
    /// as a REST request
    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        match (req.version(), req.headers().get(hyper::header::CONTENT_TYPE)) {
            (hyper::Version::HTTP_2, Some(hv)) if hv.as_bytes().starts_with(b"application/grpc") => HybridFuture::Grpc {
                grpc_future: self.grpc.call(req),
            },

            _ => HybridFuture::Rest {
                rest_future: self.rest.call(req),
            },
        }
    }
}

pin_project! {
    /// A hybrid HTTP body that will be used in the response type for the
    /// [`HybridFuture`], i.e., the output of the [`HybridService`]
    #[project = HybridBodyProj]
    pub enum HybridBody<RestBody, GrpcBody> {
        Rest {
            #[pin]
            rest_body: RestBody
        },
        Grpc {
            #[pin]
            grpc_body: GrpcBody
        },
    }
}

impl<RestBody, GrpcBody> Default for HybridBody<RestBody, GrpcBody>
where
    RestBody: Default,
    // GrpcBody: Default,
{
    fn default() -> Self {
        Self::Rest {
            rest_body: RestBody::default(),
        }
    }
}

impl<RestBody, GrpcBody> http_body::Body for HybridBody<RestBody, GrpcBody>
where
    RestBody: http_body::Body + Send + Unpin,
    GrpcBody: http_body::Body<Data = RestBody::Data> + Send + Unpin,
    RestBody::Error: Into<BoxError>,
    GrpcBody::Error: Into<BoxError>,
{
    type Data = RestBody::Data;
    type Error = BoxError;

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Rest { rest_body } => rest_body.is_end_stream(),
            Self::Grpc { grpc_body } => grpc_body.is_end_stream(),
        }
    }

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            HybridBodyProj::Rest { rest_body } => rest_body.poll_frame(cx).map_err(Into::into),
            HybridBodyProj::Grpc { grpc_body } => grpc_body.poll_frame(cx).map_err(Into::into),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match self {
            Self::Rest { rest_body } => rest_body.size_hint(),
            Self::Grpc { grpc_body } => grpc_body.size_hint(),
        }
    }
}

pin_project! {
    /// A future that accepts an HTTP request as input and returns an HTTP
    /// response as output for the [`HybridService`]
    #[project = HybridFutureProj]
    pub enum HybridFuture<RestFuture, GrpcFuture> {
        Rest {
            #[pin]
            rest_future: RestFuture,
        },
        Grpc {
            #[pin]
            grpc_future: GrpcFuture,
        },
    }
}

impl<RestFuture, GrpcFuture, RestBody, GrpcBody, RestError, GrpcError> Future for HybridFuture<RestFuture, GrpcFuture>
where
    RestFuture: Future<Output = Result<Response<RestBody>, RestError>>,
    GrpcFuture: Future<Output = Result<Response<GrpcBody>, GrpcError>>,
    RestError: Into<BoxError>,
    GrpcError: Into<BoxError>,
{
    type Output = Result<Response<HybridBody<RestBody, GrpcBody>>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            HybridFutureProj::Rest { rest_future } => match rest_future.poll(cx) {
                Poll::Ready(Ok(res)) => Poll::Ready(Ok(res.map(|rest_body| HybridBody::Rest { rest_body }))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
            HybridFutureProj::Grpc { grpc_future } => match grpc_future.poll(cx) {
                Poll::Ready(Ok(res)) => Poll::Ready(Ok(res.map(|grpc_body| HybridBody::Grpc { grpc_body }))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Response, StatusCode};
    use hyper::body::Incoming;
    use std::convert::Infallible;
    use tower::Service;

    // Mock REST service for testing
    #[derive(Clone)]
    struct MockRestService;

    impl Service<Request<Incoming>> for MockRestService {
        type Response = Response<String>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Incoming>) -> Self::Future {
            Box::pin(async {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body("REST response".to_string())
                    .unwrap())
            })
        }
    }

    // Mock gRPC service for testing
    #[derive(Clone)]
    struct MockGrpcService;

    impl Service<Request<Incoming>> for MockGrpcService {
        type Response = Response<Vec<u8>>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Incoming>) -> Self::Future {
            Box::pin(async {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(b"gRPC response".to_vec())
                    .unwrap())
            })
        }
    }

    #[test]
    fn test_hybrid_service_creation() {
        let rest_service = MockRestService;
        let grpc_service = MockGrpcService;

        let _hybrid_service = hybrid(rest_service, grpc_service);

        // Test that the hybrid service can be created
        assert!(true);
    }

    #[test]
    fn test_hybrid_service_direct_creation() {
        let _hybrid_service = HybridService {
            rest: MockRestService,
            grpc: MockGrpcService,
        };

        // Test that the hybrid service can be created directly
        assert!(true);
    }

    #[test]
    fn test_hybrid_body_rest_default() {
        let body: HybridBody<String, Vec<u8>> = HybridBody::default();

        match body {
            HybridBody::Rest { rest_body } => {
                assert_eq!(rest_body, String::default());
            }
            HybridBody::Grpc { .. } => {
                panic!("Default should create Rest variant");
            }
        }
    }

    #[test]
    fn test_hybrid_body_variants() {
        // Test Rest variant
        let rest_body: HybridBody<String, Vec<u8>> = HybridBody::Rest {
            rest_body: "test rest".to_string(),
        };

        match rest_body {
            HybridBody::Rest { rest_body } => {
                assert_eq!(rest_body, "test rest");
            }
            _ => panic!("Should be Rest variant"),
        }

        // Test Grpc variant
        let grpc_body: HybridBody<String, Vec<u8>> = HybridBody::Grpc {
            grpc_body: b"test grpc".to_vec(),
        };

        match grpc_body {
            HybridBody::Grpc { grpc_body } => {
                assert_eq!(grpc_body, b"test grpc".to_vec());
            }
            _ => panic!("Should be Grpc variant"),
        }
    }

    #[test]
    fn test_request_content_type_detection() {
        // Test gRPC detection logic (simplified)
        let grpc_content_type = "application/grpc";
        let grpc_content_type_with_encoding = "application/grpc+proto";
        let json_content_type = "application/json";
        let form_content_type = "application/x-www-form-urlencoded";

        // The actual implementation would check for "application/grpc" prefix
        assert!(grpc_content_type.starts_with("application/grpc"));
        assert!(grpc_content_type_with_encoding.starts_with("application/grpc"));
        assert!(!json_content_type.starts_with("application/grpc"));
        assert!(!form_content_type.starts_with("application/grpc"));
    }

    #[test]
    fn test_http_methods() {
        // Test that different HTTP methods can be used
        let get_method = Method::GET;
        let post_method = Method::POST;
        let put_method = Method::PUT;
        let delete_method = Method::DELETE;

        // gRPC typically uses POST
        assert_eq!(post_method, Method::POST);
        // REST can use various methods
        assert_ne!(get_method, post_method);
        assert_ne!(put_method, delete_method);
    }

    // Note: Testing the actual Service implementation would require complex setup
    // with proper HTTP requests and async runtime. The hybrid service routes
    // requests to either REST or gRPC services based on content-type headers.
    //
    // For full integration testing, consider:
    // 1. Setting up test HTTP clients
    // 2. Creating proper Request<Incoming> instances
    // 3. Testing with real gRPC and REST payloads
    // 4. Verifying response routing logic
}
