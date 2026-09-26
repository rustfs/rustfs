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
use std::time::Instant;
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Generate a [`HybridService`]
pub(crate) fn hybrid<MakeRest, Grpc>(make_rest: MakeRest, grpc: Grpc) -> HybridService<MakeRest, Grpc> {
    HybridService { rest: make_rest, grpc }
}

pub(crate) fn is_grpc_request<B>(req: &Request<B>) -> bool {
    matches!(
        (req.version(), req.headers().get(hyper::header::CONTENT_TYPE)),
        (hyper::Version::HTTP_2, Some(value)) if value.as_bytes().starts_with(b"application/grpc")
    )
}

fn rename_data_grpc_stage(path: &str) -> bool {
    matches!(
        path,
        "/node_service.NodeService/RenameData" | "/node_service.NodeService/RenameDataAtIncarnation"
    )
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
        if is_grpc_request(&req) {
            let observe_rename_data = rename_data_grpc_stage(req.uri().path());
            HybridFuture::Grpc {
                grpc_future: self.grpc.call(req),
                observe_rename_data,
                service_future_started: rustfs_io_metrics::put_stage_timer(),
            }
        } else {
            HybridFuture::Rest {
                rest_future: self.rest.call(req),
            }
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
            grpc_body: GrpcBody,
            first_frame_started: Option<Instant>,
            body_started: Option<Instant>,
            first_frame_recorded: bool,
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
            Self::Grpc { grpc_body, .. } => grpc_body.is_end_stream(),
        }
    }

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            HybridBodyProj::Rest { rest_body } => rest_body.poll_frame(cx).map_err(Into::into),
            HybridBodyProj::Grpc {
                mut grpc_body,
                first_frame_started,
                body_started,
                first_frame_recorded,
            } => {
                let poll = grpc_body.as_mut().poll_frame(cx).map_err(Into::into);
                if !*first_frame_recorded && let Poll::Ready(_) = &poll {
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_TRANSPORT_FIRST_FRAME,
                        first_frame_started.take(),
                    );
                    *first_frame_recorded = true;
                }
                let body_complete = matches!(&poll, Poll::Ready(None))
                    || (matches!(&poll, Poll::Ready(Some(Ok(_)))) && grpc_body.as_ref().get_ref().is_end_stream());
                if body_complete {
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_TRANSPORT_BODY_COMPLETE,
                        body_started.take(),
                    );
                }
                poll
            }
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match self {
            Self::Rest { rest_body } => rest_body.size_hint(),
            Self::Grpc { grpc_body, .. } => grpc_body.size_hint(),
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
            observe_rename_data: bool,
            service_future_started: Option<Instant>,
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
            HybridFutureProj::Grpc {
                grpc_future,
                observe_rename_data,
                service_future_started,
            } => match grpc_future.poll(cx) {
                Poll::Ready(Ok(res)) => {
                    if *observe_rename_data {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_TRANSPORT_SERVICE_FUTURE,
                            service_future_started.take(),
                        );
                    }
                    let first_frame_started = (*observe_rename_data).then(Instant::now);
                    let body_started = (*observe_rename_data).then(Instant::now);
                    Poll::Ready(Ok(res.map(|grpc_body| HybridBody::Grpc {
                        grpc_body,
                        first_frame_started,
                        body_started,
                        first_frame_recorded: !*observe_rename_data,
                    })))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
