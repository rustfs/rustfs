//! Axum integration example for s3s
//!
//! This example demonstrates how to use `S3Service` directly with Axum.
//! `S3Service` implements `tower::Service<http::Request<B>>` for any body type
//! that implements `http_body::Body`, which allows it to work with Axum.
//!
//! Note: Axum's `fallback_service` requires `Error = Infallible`, so we use
//! `axum::error_handling::HandleError` to convert `S3Service` errors to HTTP responses.
//!
//! This example also demonstrates the use of `S3Route` to handle custom routes
//! within the S3 service, such as STS `AssumeRole` requests.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example axum
//! ```
//!
//! Then you can access:
//! - Health check: <http://localhost:8014/health>
//! - S3 endpoint: <http://localhost:8014/>

use s3s::dto::{GetObjectInput, GetObjectOutput};
use s3s::route::S3Route;
use s3s::service::S3ServiceBuilder;
use s3s::{Body, HttpError, S3, S3Request, S3Response, S3Result};

use axum::Router;
use axum::error_handling::HandleError;
use axum::http::{Extensions, HeaderMap, Method, Response, StatusCode, Uri};
use axum::routing::get;
use tokio::net::TcpListener;

/// A minimal S3 implementation for demonstration purposes
#[derive(Debug, Clone)]
struct DummyS3;

#[async_trait::async_trait]
impl S3 for DummyS3 {
    async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        tracing::info!("DummyS3 GetObject called");
        Err(s3s::s3_error!(NotImplemented, "GetObject is not implemented"))
    }
}

/// Custom route that handles STS `AssumeRole` requests
/// This demonstrates how to use `S3Route` to intercept specific requests
/// before they reach the main S3 service.
#[derive(Debug, Clone)]
struct AssumeRoleRoute;

#[async_trait::async_trait]
impl S3Route for AssumeRoleRoute {
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
        // Match POST requests to "/" with content-type "application/x-www-form-urlencoded"
        // This is the typical pattern for STS AssumeRole requests
        if method == Method::POST
            && uri.path() == "/"
            && let Some(val) = headers.get(hyper::header::CONTENT_TYPE)
            && val.as_bytes() == b"application/x-www-form-urlencoded"
        {
            return true;
        }
        false
    }

    async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
        // For demonstration, allow all requests without credentials
        Ok(())
    }

    async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        tracing::info!("AssumeRole route called");
        Err(s3s::s3_error!(NotImplemented, "STS operations are not supported in this example"))
    }
}

/// A simple health check endpoint
async fn health_check() -> &'static str {
    tracing::info!("Health check endpoint called");
    "OK"
}

/// Error handler for HTTP-level errors.
/// Note: S3 application errors (e.g., `NoSuchBucket`) are already converted to
/// proper HTTP responses by `S3Service` and won't reach this handler.
async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "S3 service error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error".to_string()))
        .unwrap()
}

#[tokio::main]
async fn main() {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    // Create the S3 service with a custom route
    // The AssumeRoleRoute will intercept STS requests before they reach the S3 implementation
    let s3_service = {
        let mut builder = S3ServiceBuilder::new(DummyS3);
        builder.set_route(AssumeRoleRoute);
        builder.build()
    };

    // Use HandleError to convert S3Service errors to responses
    // This makes the service compatible with Axum's fallback_service which requires Error = Infallible
    let s3_service = HandleError::new(s3_service, handle_s3_error);

    // Build an Axum router with:
    // - A health check endpoint at /health
    // - S3Service as the fallback for all other routes
    let app = Router::new().route("/health", get(health_check)).fallback_service(s3_service);

    // Bind and serve
    let addr = "127.0.0.1:8014";
    let listener = TcpListener::bind(addr).await.unwrap();

    tracing::info!("Axum server listening on http://{}", addr);
    tracing::info!("Health check: http://{}/health", addr);
    tracing::info!("S3 endpoint: http://{}/", addr);
    tracing::info!("Press Ctrl+C to stop");

    axum::serve(listener, app).await.unwrap();
}
