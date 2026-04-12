//! S3 Service and Builder
//!
//! This module provides the core [`S3Service`] and [`S3ServiceBuilder`] types for creating
//! S3-compatible HTTP services.
//!
//! # Overview
//!
//! [`S3Service`] is a hyper and tower service that handles S3 HTTP requests. It coordinates
//! various components:
//!
//! - **S3 Implementation**: Your business logic implementing the [`S3`] trait
//! - **Configuration**: Service settings via [`S3ConfigProvider`]
//! - **Authentication**: Optional request signing verification via [`S3Auth`]
//! - **Authorization**: Optional access control via [`S3Access`]
//! - **Host Parsing**: Optional virtual host handling via [`S3Host`]
//! - **Custom Routes**: Optional route interception via [`S3Route`]
//! - **Validation**: Optional bucket/object name validation via [`NameValidation`]
//!
//! # Example
//!
//! ```
//! use s3s::{S3, S3Request, S3Response, S3Result};
//! use s3s::service::S3ServiceBuilder;
//! use s3s::dto::{GetObjectInput, GetObjectOutput};
//! use s3s::auth::SimpleAuth;
//!
//! #[derive(Clone)]
//! struct MyS3;
//!
//! #[async_trait::async_trait]
//! impl S3 for MyS3 {
//!     // Implement S3 operations
//! #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
//! #       Err(s3s::s3_error!(NotImplemented))
//! #   }
//! }
//!
//! // Create a basic service
//! let service = S3ServiceBuilder::new(MyS3).build();
//!
//! // Or configure it with authentication
//! let mut builder = S3ServiceBuilder::new(MyS3);
//! builder.set_auth(SimpleAuth::from_single("ACCESS_KEY", "SECRET_KEY"));
//! let service = builder.build();
//! ```
//!
//! # Builder Pattern
//!
//! [`S3ServiceBuilder`] uses the builder pattern to configure the service. All components
//! are optional except for the [`S3`] implementation itself. If not provided, sensible
//! defaults are used:
//!
//! - **Config**: [`StaticConfigProvider::default()`]
//! - **Auth**: None (no authentication required)
//! - **Access**: None (no custom access policy; when auth is enabled, uses the default access check that denies anonymous requests)
//! - **Host**: None (assumes path-style requests)
//! - **Route**: None (no custom routes)
//! - **Validation**: None (uses AWS-compatible validation)

use crate::access::S3Access;
use crate::auth::S3Auth;
use crate::config::{S3ConfigProvider, StaticConfigProvider};
use crate::host::S3Host;
use crate::http::{Body, Request};
use crate::route::S3Route;
use crate::s3_trait::S3;
use crate::validation::NameValidation;
use crate::{HttpError, HttpRequest, HttpResponse};

use std::any::TypeId;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use tracing::{debug, error};

/// Builder for [`S3Service`].
///
/// This builder allows you to configure an S3 service with various optional components
/// before creating the final service instance.
///
/// # Example
///
/// ```
/// use s3s::{S3, S3Request, S3Response, S3Result};
/// use s3s::service::S3ServiceBuilder;
/// use s3s::dto::{GetObjectInput, GetObjectOutput};
/// use s3s::auth::SimpleAuth;
/// use s3s::config::{S3Config, StaticConfigProvider};
/// use std::sync::Arc;
///
/// #[derive(Clone)]
/// struct MyS3;
///
/// #[async_trait::async_trait]
/// impl S3 for MyS3 {
/// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
/// #       Err(s3s::s3_error!(NotImplemented))
/// #   }
///     // Implement S3 operations
/// }
///
/// // Create a basic service
/// let service = S3ServiceBuilder::new(MyS3).build();
///
/// // Create a service with custom configuration
/// let mut builder = S3ServiceBuilder::new(MyS3);
///
/// // Set authentication
/// builder.set_auth(SimpleAuth::from_single("ACCESS_KEY", "SECRET_KEY"));
///
/// // Set custom configuration
/// let mut custom_config = S3Config::default();
/// custom_config.xml_max_body_size = 10 * 1024 * 1024;
/// builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(custom_config))));
///
/// let service = builder.build();
/// ```
pub struct S3ServiceBuilder {
    s3: Arc<dyn S3>,
    config: Option<Arc<dyn S3ConfigProvider>>,
    host: Option<Box<dyn S3Host>>,
    auth: Option<Box<dyn S3Auth>>,
    access: Option<Box<dyn S3Access>>,
    route: Option<Box<dyn S3Route>>,
    validation: Option<Box<dyn NameValidation>>,
}

impl S3ServiceBuilder {
    /// Creates a new builder with the given S3 implementation.
    ///
    /// All other components are optional and can be set using the `set_*` methods.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let builder = S3ServiceBuilder::new(MyS3);
    /// ```
    #[must_use]
    pub fn new(s3: impl S3) -> Self {
        Self {
            s3: Arc::new(s3),
            config: None,
            host: None,
            auth: None,
            access: None,
            route: None,
            validation: None,
        }
    }

    /// Sets the configuration provider for the service.
    ///
    /// The configuration provider supplies runtime configuration values such as
    /// maximum body sizes and other limits.
    ///
    /// If not set, defaults to [`StaticConfigProvider::default()`].
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::config::{S3Config, StaticConfigProvider};
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    /// use std::sync::Arc;
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// let mut custom_config = S3Config::default();
    /// custom_config.xml_max_body_size = 10 * 1024 * 1024;
    /// builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(custom_config))));
    /// ```
    pub fn set_config(&mut self, config: Arc<dyn S3ConfigProvider>) {
        self.config = Some(config);
    }

    /// Sets the host parser for the service.
    ///
    /// The host parser handles virtual-hosted-style requests by parsing the Host header
    /// to extract bucket names and regions.
    ///
    /// If not set, the service assumes path-style requests.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::host::SingleDomain;
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// builder.set_host(SingleDomain::new("s3.example.com").unwrap());
    /// ```
    pub fn set_host(&mut self, host: impl S3Host) {
        self.host = Some(Box::new(host));
    }

    /// Sets the authentication provider for the service.
    ///
    /// The authentication provider verifies AWS Signature Version 4 or Version 2 signatures
    /// on incoming requests.
    ///
    /// If not set, unsigned requests are allowed, but signed requests will fail with
    /// `NotImplemented` ("This service has no authentication provider").
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::auth::SimpleAuth;
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// builder.set_auth(SimpleAuth::from_single("ACCESS_KEY", "SECRET_KEY"));
    /// ```
    pub fn set_auth(&mut self, auth: impl S3Auth) {
        self.auth = Some(Box::new(auth));
    }

    /// Sets the access control provider for the service.
    ///
    /// The access control provider performs authorization checks to determine if a
    /// request should be allowed based on the authenticated identity.
    ///
    /// **Important**: Access control checks are only performed when authentication is
    /// configured. You must also call [`set_auth`](Self::set_auth) to configure
    /// authentication; otherwise, access checks will be skipped entirely regardless of this
    /// setting.
    ///
    /// When authentication is configured, access checks are still run for anonymous or
    /// unsigned requests where `cx.credentials()` is `None`; the default access check will
    /// then deny those requests.
    ///
    /// If not set, a default access check is performed that allows all authenticated
    /// requests (i.e., when `cx.credentials()` is `Some(_)`) and denies anonymous/unsigned
    /// requests.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::access::{S3Access, S3AccessContext};
    /// use s3s::auth::SimpleAuth;
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// // Custom access control implementation
    /// #[derive(Clone)]
    /// struct MyAccessControl;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Access for MyAccessControl {
    ///     async fn check(&self, _cx: &mut S3AccessContext<'_>) -> S3Result<()> {
    ///         // Your access control logic here
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// // Note: access checks are only enforced if `set_auth` is also configured.
    /// builder.set_auth(SimpleAuth::from_single("ACCESS_KEY", "SECRET_KEY"));
    /// builder.set_access(MyAccessControl);
    /// ```
    pub fn set_access(&mut self, access: impl S3Access) {
        self.access = Some(Box::new(access));
    }

    /// Sets the custom route handler for the service.
    ///
    /// The route handler allows intercepting requests before they reach the S3 service,
    /// useful for implementing custom endpoints or middleware.
    ///
    /// If not set, all requests are processed as S3 operations.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::route::S3Route;
    /// use s3s::{S3, S3Request, S3Response, S3Result, Body};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    /// use hyper::{HeaderMap, Method, Uri};
    /// use hyper::http::Extensions;
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// // Custom route handler
    /// #[derive(Clone)]
    /// struct MyRoute;
    ///
    /// #[async_trait::async_trait]
    /// impl S3Route for MyRoute {
    ///     fn is_match(&self, _method: &Method, _uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
    ///         false
    ///     }
    ///
    ///     async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    ///         Err(s3s::s3_error!(NotImplemented))
    ///     }
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// builder.set_route(MyRoute);
    /// ```
    pub fn set_route(&mut self, route: impl S3Route) {
        self.route = Some(Box::new(route));
    }

    /// Sets the name validation provider for the service.
    ///
    /// The name validation provider validates bucket and object names according to
    /// specific rules (e.g., AWS-compatible, relaxed, etc.).
    ///
    /// If not set, uses AWS-compatible validation rules.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::validation::{NameValidation, AwsNameValidation};
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let mut builder = S3ServiceBuilder::new(MyS3);
    /// builder.set_validation(AwsNameValidation::new());
    /// ```
    pub fn set_validation(&mut self, validation: impl NameValidation) {
        self.validation = Some(Box::new(validation));
    }

    /// Builds the [`S3Service`] from this builder.
    ///
    /// This consumes the builder and returns the configured service ready to handle requests.
    ///
    /// # Example
    ///
    /// ```
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::{S3, S3Request, S3Response, S3Result};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// let service = S3ServiceBuilder::new(MyS3).build();
    /// ```
    #[must_use]
    pub fn build(self) -> S3Service {
        let config = self.config.unwrap_or_else(|| Arc::new(StaticConfigProvider::default()));
        S3Service {
            inner: Arc::new(Inner {
                s3: self.s3,
                config,
                host: self.host,
                auth: self.auth,
                access: self.access,
                route: self.route,
                validation: self.validation,
            }),
        }
    }
}

/// An S3-compatible HTTP service.
///
/// `S3Service` implements both [`hyper::service::Service`] and [`tower::Service`],
/// making it compatible with the hyper and tower ecosystems.
///
/// # Overview
///
/// This service handles the complete lifecycle of S3 HTTP requests:
///
/// 1. Parses incoming HTTP requests
/// 2. Validates authentication (if configured)
/// 3. Authorizes the request whenever authentication is enabled (using custom access
///    control if configured, otherwise the default access check)
/// 4. Routes to appropriate S3 operation or custom route
/// 5. Invokes your S3 implementation
/// 6. Converts responses back to HTTP
///
/// # Creating a Service
///
/// Use [`S3ServiceBuilder`] to create an `S3Service`:
///
/// ```
/// use s3s::service::S3ServiceBuilder;
/// use s3s::{S3, S3Request, S3Response, S3Result};
/// use s3s::dto::{GetObjectInput, GetObjectOutput};
///
/// #[derive(Clone)]
/// struct MyS3;
///
/// #[async_trait::async_trait]
/// impl S3 for MyS3 {
/// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
/// #       Err(s3s::s3_error!(NotImplemented))
/// #   }
///     // Implement S3 operations
/// }
///
/// let service = S3ServiceBuilder::new(MyS3).build();
/// ```
///
/// # Using with Hyper
///
/// ```no_run
/// use s3s::service::S3ServiceBuilder;
/// use s3s::{S3, S3Request, S3Response, S3Result};
/// use s3s::dto::{GetObjectInput, GetObjectOutput};
/// use hyper_util::rt::{TokioExecutor, TokioIo};
/// use hyper_util::server::conn::auto::Builder as ConnBuilder;
/// use tokio::net::TcpListener;
///
/// #[derive(Clone)]
/// struct MyS3;
///
/// #[async_trait::async_trait]
/// impl S3 for MyS3 {
/// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
/// #       Err(s3s::s3_error!(NotImplemented))
/// #   }
///     // Implement S3 operations
/// }
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let service = S3ServiceBuilder::new(MyS3).build();
/// let listener = TcpListener::bind("127.0.0.1:8014").await?;
///
/// loop {
///     let (stream, _) = listener.accept().await?;
///     let io = TokioIo::new(stream);
///     let service = service.clone();
///
///     tokio::spawn(async move {
///         let _ = ConnBuilder::new(TokioExecutor::new())
///             .serve_connection(io, service)
///             .await;
///     });
/// }
/// # }
/// ```
///
/// # Using with Tower/Axum
///
/// See the `axum` example in the examples directory for integration with Axum.
///
/// # Clone
///
/// `S3Service` is cheap to clone (uses `Arc` internally).
#[derive(Clone)]
pub struct S3Service {
    inner: Arc<Inner>,
}

struct Inner {
    s3: Arc<dyn S3>,
    config: Arc<dyn S3ConfigProvider>,
    host: Option<Box<dyn S3Host>>,
    auth: Option<Box<dyn S3Auth>>,
    access: Option<Box<dyn S3Access>>,
    route: Option<Box<dyn S3Route>>,
    validation: Option<Box<dyn NameValidation>>,
}

impl S3Service {
    /// Processes an S3 HTTP request and returns an HTTP response.
    ///
    /// This is the main entry point for handling S3 requests. The method:
    ///
    /// 1. Parses the HTTP request
    /// 2. Authenticates the request (if authentication is configured)
    /// 3. Authorizes the request whenever authentication is enabled (using custom access
    ///    control if configured, otherwise the default access check)
    /// 4. Routes the request to the appropriate S3 operation
    /// 5. Invokes your S3 implementation
    /// 6. Converts the result to an HTTP response
    ///
    /// # Errors
    ///
    /// Returns an [`HttpError`] if the request cannot be processed. This could be due to:
    /// - Invalid HTTP request format
    /// - Authentication failure (if authentication is configured)
    /// - Authorization failure (if authentication is configured)
    /// - S3 operation errors
    ///
    /// # Example
    ///
    /// ```no_run
    /// use s3s::service::S3ServiceBuilder;
    /// use s3s::{S3, S3Request, S3Response, S3Result, HttpRequest, HttpError};
    /// use s3s::dto::{GetObjectInput, GetObjectOutput};
    /// use http::Request;
    /// use s3s::Body;
    ///
    /// #[derive(Clone)]
    /// struct MyS3;
    ///
    /// #[async_trait::async_trait]
    /// impl S3 for MyS3 {
    /// #   async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    /// #       Err(s3s::s3_error!(NotImplemented))
    /// #   }
    ///     // Implement S3 operations
    /// }
    ///
    /// # async fn example() -> Result<(), HttpError> {
    /// let service = S3ServiceBuilder::new(MyS3).build();
    ///
    /// // Process a request (in a real scenario, this comes from a server)
    /// let http_req: HttpRequest = Request::builder()
    ///     .uri("/")
    ///     .body(Body::empty())
    ///     .map_err(|e| HttpError::new(Box::new(e)))?;
    ///
    /// let response = service.call(http_req).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?crate::time::now_utc())
    )]
    pub async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        debug!(?req);

        let t0 = crate::time::Instant::now();

        let mut req = Request::from(req);

        let ccx = crate::ops::CallContext {
            s3: &self.inner.s3,
            config: &self.inner.config,
            host: self.inner.host.as_deref(),
            auth: self.inner.auth.as_deref(),
            access: self.inner.access.as_deref(),
            route: self.inner.route.as_deref(),
            validation: self.inner.validation.as_deref(),
        };
        let result = match crate::ops::call(&mut req, &ccx).await {
            Ok(resp) => Ok(HttpResponse::from(resp)),
            Err(err) => Err(HttpError::new(Box::new(err))),
        };

        let duration = t0.elapsed();

        match result {
            Ok(ref resp) => {
                if resp.status().is_server_error() {
                    error!(?duration, ?resp);
                } else {
                    debug!(?duration, ?resp);
                }
            }
            Err(ref err) => error!(?duration, ?err),
        }

        result
    }

    async fn call_owned(self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.call(req).await
    }
}

impl fmt::Debug for S3Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Service").finish_non_exhaustive()
    }
}

impl hyper::service::Service<http::Request<hyper::body::Incoming>> for S3Service {
    type Response = HttpResponse;

    type Error = HttpError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
        let req = req.map(Body::from);
        let service = self.clone();
        Box::pin(service.call_owned(req))
    }
}

impl<B> tower::Service<http::Request<B>> for S3Service
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = HttpResponse;

    type Error = HttpError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        // Use downcast to check if B is hyper::body::Incoming for optimal handling
        let req = if TypeId::of::<B>() == TypeId::of::<hyper::body::Incoming>() {
            // B is hyper::body::Incoming, use the optimized From impl
            let (parts, body) = req.into_parts();
            let mut slot = Some(body);
            let body = (&mut slot as &mut dyn std::any::Any)
                .downcast_mut::<Option<hyper::body::Incoming>>()
                .unwrap()
                .take()
                .unwrap();
            http::Request::from_parts(parts, Body::from(body))
        } else {
            req.map(Body::http_body_unsync)
        };
        let service = self.clone();
        Box::pin(service.call_owned(req))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{S3Error, S3Request, S3Response};

    use stdx::mem::output_size;

    macro_rules! print_future_size {
        ($func:path) => {
            println!("{:<24}: {}", stringify!($func), output_size(&$func));
        };
    }

    macro_rules! print_type_size {
        ($ty:path) => {
            println!("{:<24}: {}", stringify!($ty), std::mem::size_of::<$ty>());
        };
    }

    #[test]
    fn future_size() {
        print_type_size!(std::time::Instant);

        print_type_size!(HttpRequest);
        print_type_size!(HttpResponse);
        print_type_size!(HttpError);

        print_type_size!(S3Request<()>);
        print_type_size!(S3Response<()>);
        print_type_size!(S3Error);

        print_type_size!(S3Service);

        print_future_size!(crate::ops::call);
        print_future_size!(S3Service::call);
        print_future_size!(S3Service::call_owned);

        // In case the futures are made too large accidentally
        assert!(output_size(&crate::ops::call) <= 1600);
        assert!(output_size(&S3Service::call) <= 3000);
        assert!(output_size(&S3Service::call_owned) <= 3300);
    }

    // Test validation functionality
    use crate::validation::NameValidation;

    // Mock S3 implementation for testing
    struct MockS3;
    impl S3 for MockS3 {}

    // Test validation that allows any bucket name
    struct RelaxedValidation;
    impl NameValidation for RelaxedValidation {
        fn validate_bucket_name(&self, _name: &str) -> bool {
            true // Allow any bucket name
        }
    }

    #[test]
    fn test_service_builder_validation() {
        let validation = RelaxedValidation;
        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_validation(validation);
        let service = builder.build();

        // Verify validation was set
        assert!(service.inner.validation.is_some());
    }

    #[test]
    fn test_service_builder_default_validation() {
        let builder = S3ServiceBuilder::new(MockS3);
        let service = builder.build();

        // Should have default validation when none is set
        assert!(service.inner.validation.is_none()); // None means it will use AwsNameValidation
    }

    #[test]
    fn test_service_builder_default_config() {
        let builder = S3ServiceBuilder::new(MockS3);
        let service = builder.build();

        // Should have default config values
        let config = service.inner.config.snapshot();
        assert_eq!(config.xml_max_body_size, 20 * 1024 * 1024);
        assert_eq!(config.post_object_max_file_size, 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_service_builder_custom_config() {
        use crate::config::{HotReloadConfigProvider, S3Config};

        let custom_config = Arc::new(HotReloadConfigProvider::new(Arc::new(S3Config {
            xml_max_body_size: 10 * 1024 * 1024,
            post_object_max_file_size: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        })));

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_config(custom_config);
        let service = builder.build();

        let config = service.inner.config.snapshot();
        assert_eq!(config.xml_max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.post_object_max_file_size, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_service_builder_hot_reload_config() {
        use crate::config::{HotReloadConfigProvider, S3Config};

        let hot_config = Arc::new(HotReloadConfigProvider::new(Arc::new(S3Config::default())));

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_config(hot_config.clone());
        let service = builder.build();

        // Initial value
        let config = service.inner.config.snapshot();
        assert_eq!(config.xml_max_body_size, 20 * 1024 * 1024);

        // Update the config
        hot_config.update(Arc::new(S3Config {
            xml_max_body_size: 30 * 1024 * 1024,
            ..Default::default()
        }));

        // Service should see the new value
        let new_config = service.inner.config.snapshot();
        assert_eq!(new_config.xml_max_body_size, 30 * 1024 * 1024);
    }

    #[test]
    fn test_service_builder_static_config() {
        use crate::config::{S3Config, StaticConfigProvider};

        let static_config = Arc::new(StaticConfigProvider::new(Arc::new(S3Config {
            xml_max_body_size: 10 * 1024 * 1024,
            ..Default::default()
        })));

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_config(static_config);
        let service = builder.build();

        let config = service.inner.config.snapshot();
        assert_eq!(config.xml_max_body_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_service_builder_set_auth() {
        use crate::auth::SimpleAuth;

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_auth(SimpleAuth::from_single("AK", "SK"));
        let service = builder.build();
        assert!(service.inner.auth.is_some());
    }

    #[test]
    fn test_service_builder_set_host() {
        use crate::host::SingleDomain;

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_host(SingleDomain::new("s3.example.com").unwrap());
        let service = builder.build();
        assert!(service.inner.host.is_some());
    }

    #[test]
    fn test_service_builder_set_route() {
        use crate::route::S3Route;

        #[derive(Clone)]
        struct NoopRoute;
        #[async_trait::async_trait]
        impl S3Route for NoopRoute {
            fn is_match(&self, _: &http::Method, _: &http::Uri, _: &http::HeaderMap, _: &mut http::Extensions) -> bool {
                false
            }
            async fn call(&self, _: crate::S3Request<crate::Body>) -> crate::S3Result<crate::S3Response<crate::Body>> {
                Err(crate::s3_error!(NotImplemented))
            }
        }

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_route(NoopRoute);
        let service = builder.build();
        assert!(service.inner.route.is_some());
    }

    #[test]
    fn test_service_builder_set_access() {
        use crate::access::{S3Access, S3AccessContext};

        #[derive(Clone)]
        struct AllowAll;
        #[async_trait::async_trait]
        impl S3Access for AllowAll {
            async fn check(&self, _: &mut S3AccessContext<'_>) -> crate::S3Result<()> {
                Ok(())
            }
        }

        let mut builder = S3ServiceBuilder::new(MockS3);
        builder.set_access(AllowAll);
        let service = builder.build();
        assert!(service.inner.access.is_some());
    }

    #[test]
    fn test_service_debug() {
        let service = S3ServiceBuilder::new(MockS3).build();
        let dbg = format!("{service:?}");
        assert!(dbg.contains("S3Service"));
    }

    #[test]
    fn test_service_clone() {
        let service = S3ServiceBuilder::new(MockS3).build();
        let cloned = service.clone();
        assert!(format!("{cloned:?}").contains("S3Service"));
    }
}
