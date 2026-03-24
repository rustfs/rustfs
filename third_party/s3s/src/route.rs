//! Custom route support
//!
//! This module provides the [`S3Route`] trait for implementing custom routes that can
//! intercept requests before they reach the standard S3 operation handlers.
//!
//! # Overview
//!
//! Custom routes allow you to:
//!
//! - Handle non-S3 endpoints (e.g., health checks, metrics)
//! - Implement custom authentication flows (e.g., STS `AssumeRole`)
//! - Add middleware-like functionality
//! - Route specific requests to custom handlers
//!
//! # Example
//!
//! ```
//! use s3s::route::S3Route;
//! use s3s::{Body, S3Request, S3Response, S3Result};
//! use hyper::{HeaderMap, Method, Uri};
//! use hyper::http::Extensions;
//!
//! // Custom route for health checks
//! #[derive(Clone)]
//! struct HealthCheckRoute;
//!
//! #[async_trait::async_trait]
//! impl S3Route for HealthCheckRoute {
//!     fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
//!         method == Method::GET && uri.path() == "/health"
//!     }
//!
//!     // Override to allow unauthenticated health checks
//!     async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
//!         Ok(())
//!     }
//!
//!     async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
//!         Ok(S3Response::new(Body::from("OK".to_string())))
//!     }
//! }
//! ```
//!
//! # Integration with `S3Service`
//!
//! ```
//! use s3s::service::S3ServiceBuilder;
//! use s3s::route::S3Route;
//! use s3s::{S3, S3Request, S3Response, S3Result, Body};
//! use s3s::dto::{GetObjectInput, GetObjectOutput};
//! use hyper::{HeaderMap, Method, Uri};
//! use hyper::http::Extensions;
//!
//! #[derive(Clone)]
//! struct MyS3;
//!
//! #[async_trait::async_trait]
//! impl S3 for MyS3 {
//! #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
//! #       Err(s3s::s3_error!(NotImplemented))
//! #   }
//!     // Implement S3 operations
//! }
//!
//! #[derive(Clone)]
//! struct MyRoute;
//!
//! #[async_trait::async_trait]
//! impl S3Route for MyRoute {
//!     fn is_match(&self, _method: &Method, _uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
//!         false
//!     }
//!
//!     async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
//!         Err(s3s::s3_error!(NotImplemented))
//!     }
//! }
//!
//! let mut builder = S3ServiceBuilder::new(MyS3);
//! builder.set_route(MyRoute);
//! let service = builder.build();
//! ```

use crate::Body;
use crate::S3Request;
use crate::S3Response;
use crate::S3Result;

use hyper::HeaderMap;
use hyper::Method;
use hyper::Uri;
use hyper::http::Extensions;

/// Custom route handler for S3 requests.
///
/// This trait allows you to intercept and handle specific requests before they reach
/// the standard S3 operation handlers. Routes are checked before S3 operations are
/// invoked, allowing you to implement custom endpoints or middleware.
///
/// # Example
///
/// ```
/// use s3s::route::S3Route;
/// use s3s::{Body, S3Request, S3Response, S3Result};
/// use hyper::{HeaderMap, Method, Uri};
/// use hyper::http::Extensions;
///
/// // Custom route for STS AssumeRole
/// #[derive(Clone)]
/// struct AssumeRoleRoute;
///
/// #[async_trait::async_trait]
/// impl S3Route for AssumeRoleRoute {
///     fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
///         method == Method::POST
///             && uri.path() == "/"
///             && headers.get("content-type")
///                 .and_then(|v| v.to_str().ok())
///                 == Some("application/x-www-form-urlencoded")
///     }
///
///     async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
///         // Handle AssumeRole request
///         // Parse form data, generate temporary credentials, etc.
/// #       Err(s3s::s3_error!(NotImplemented))
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait S3Route: Send + Sync + 'static {
    /// Determines if this route should handle the request.
    ///
    /// This method is called before S3 operations are invoked. If it returns `true`,
    /// the [`call`](Self::call) method will be invoked instead of the standard S3 handler.
    ///
    /// # Arguments
    ///
    /// * `method` - The HTTP method (GET, POST, etc.)
    /// * `uri` - The request URI
    /// * `headers` - The HTTP headers
    /// * `extensions` - HTTP extensions (can be used to store custom data)
    ///
    /// # Returns
    ///
    /// `true` if this route should handle the request, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::route::S3Route;
    /// use s3s::{Body, S3Request, S3Response, S3Result};
    /// use hyper::{HeaderMap, Method, Uri};
    /// use hyper::http::Extensions;
    ///
    /// struct MetricsRoute;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Route for MetricsRoute {
    ///     fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
    ///         // Match GET /metrics
    ///         method == Method::GET && uri.path() == "/metrics"
    ///     }
    /// #   async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    /// }
    /// ```
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool;

    /// Checks access permissions for the request.
    ///
    /// This method is called after [`is_match`](Self::is_match) returns true and before [`call`](Self::call) is invoked.
    /// The default implementation requires valid credentials (returns error if credentials are None).
    ///
    /// Override this method to implement custom access control for your route.
    ///
    /// # Arguments
    ///
    /// * `req` - The S3 request (mutable to allow modifications)
    ///
    /// # Returns
    ///
    /// `Ok(())` if access is granted, `Err(S3Error)` otherwise.
    ///
    /// # Default Behavior
    ///
    /// The default implementation denies requests without credentials:
    ///
    /// ```
    /// # use s3s::route::S3Route;
    /// # use s3s::{Body, S3Request, S3Response, S3Result};
    /// # use hyper::{HeaderMap, Method, Uri};
    /// # use hyper::http::Extensions;
    /// # struct MyRoute;
    /// # #[async_trait::async_trait]
    /// # impl S3Route for MyRoute {
    /// #   fn is_match(&self, _method: &Method, _uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool { false }
    /// async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
    ///     match req.credentials {
    ///         Some(_) => Ok(()),
    ///         None => Err(s3s::s3_error!(AccessDenied, "Signature is required")),
    ///     }
    /// }
    /// #   async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    /// # }
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::route::S3Route;
    /// use s3s::{Body, S3Request, S3Response, S3Result};
    /// use hyper::{HeaderMap, Method, Uri};
    /// use hyper::http::Extensions;
    ///
    /// struct PublicRoute;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Route for PublicRoute {
    /// #   fn is_match(&self, _method: &Method, _uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool { false }
    ///     // Allow access without credentials
    ///     async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
    ///         Ok(())
    ///     }
    /// #   async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    /// }
    /// ```
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }

    /// Handles the request for this route.
    ///
    /// This method is called if `is_match` returns true and `check_access` succeeds.
    /// Implement your custom request handling logic here.
    ///
    /// # Arguments
    ///
    /// * `req` - The S3 request to handle
    ///
    /// # Returns
    ///
    /// A successful response or an S3 error.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::route::S3Route;
    /// use s3s::{Body, S3Request, S3Response, S3Result};
    /// use hyper::{HeaderMap, Method, Uri};
    /// use hyper::http::Extensions;
    ///
    /// struct HealthCheckRoute;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Route for HealthCheckRoute {
    /// #   fn is_match(&self, _method: &Method, _uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool { true }
    ///     async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    ///         // Return a simple OK response
    ///         let response = S3Response::new(Body::from("OK".to_string()));
    ///         Ok(response)
    ///     }
    /// }
    /// ```
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::header;

    #[allow(dead_code)]
    pub struct AssumeRole {}

    #[async_trait::async_trait]
    impl S3Route for AssumeRole {
        fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
            if method == Method::POST
                && uri.path() == "/"
                && let Some(val) = headers.get(header::CONTENT_TYPE)
                && val.as_bytes() == b"application/x-www-form-urlencoded"
            {
                return true;
            }
            false
        }

        async fn call(&self, _: S3Request<Body>) -> S3Result<S3Response<Body>> {
            tracing::debug!("call AssumeRole");
            return Err(s3_error!(NotImplemented));
        }
    }
}
