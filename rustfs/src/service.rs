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
