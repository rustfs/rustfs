use crate::Body;
use crate::StdError;
use crate::auth::Credentials;
use crate::region::Region;

use http::Extensions;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use http::Uri;

use stdx::default::default;

pub type HttpRequest<B = Body> = http::Request<B>;
pub type HttpResponse<B = Body> = http::Response<B>;

/// An error that indicates a failure of an HTTP request.
/// Passing this error to `hyper` will cause it to abort the connection.
#[derive(Debug)]
pub struct HttpError(StdError);

impl HttpError {
    #[must_use]
    pub fn new(err: StdError) -> Self {
        Self(err)
    }
}

impl From<HttpError> for StdError {
    fn from(val: HttpError) -> Self {
        val.0
    }
}

/// Trailing headers handle (newtype)
///
/// This handle lets you take streaming-trailer headers after the
/// request body stream has been fully consumed.
#[derive(Clone)]
pub struct TrailingHeaders(pub(crate) std::sync::Arc<std::sync::Mutex<Option<HeaderMap>>>);

impl core::fmt::Debug for TrailingHeaders {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let ready = self.is_ready();
        f.debug_struct("TrailingHeaders")
            .field("ready", &ready)
            .finish_non_exhaustive()
    }
}

impl TrailingHeaders {
    /// Returns true if trailers have been produced by the body stream.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.0.lock().is_ok_and(|g| g.is_some())
    }

    /// Take the trailing headers if available.
    ///
    /// This is a one-shot operation; subsequent calls will return None.
    #[must_use]
    pub fn take(&self) -> Option<HeaderMap> {
        self.0.lock().ok().and_then(|mut g| g.take())
    }

    /// Read the trailing headers if available, without taking them.
    pub fn read<R>(&self, f: impl FnOnce(&HeaderMap) -> R) -> Option<R> {
        if let Ok(guard) = self.0.lock()
            && let Some(ref headers) = *guard
        {
            return Some(f(headers));
        }
        None
    }
}

/// S3 request
#[derive(Debug, Clone)]
pub struct S3Request<T> {
    /// S3 operation input
    pub input: T,

    /// HTTP method
    pub method: Method,

    /// HTTP URI
    pub uri: Uri,

    /// HTTP headers
    pub headers: HeaderMap,

    /// Request extensions.
    /// This field is used to pass custom data between middlewares.
    pub extensions: Extensions,

    /// S3 identity information.
    /// `None` means anonymous request.
    pub credentials: Option<Credentials>,

    /// S3 requested region.
    pub region: Option<Region>,

    /// S3 requested service.
    pub service: Option<String>,

    /// Streaming trailers handle.
    /// When the request body uses AWS `SigV4` streaming with trailers, this
    /// handle allows retrieving the verified trailing headers after the body
    /// stream is fully read.
    pub trailing_headers: Option<TrailingHeaders>,
}

impl<T> S3Request<T> {
    /// Map the input of the request to a new type.
    pub fn map_input<U>(self, f: impl FnOnce(T) -> U) -> S3Request<U> {
        S3Request {
            input: f(self.input),
            method: self.method,
            uri: self.uri,
            headers: self.headers,
            extensions: self.extensions,
            credentials: self.credentials,
            region: self.region,
            service: self.service,
            trailing_headers: self.trailing_headers,
        }
    }
}

/// S3 response
#[derive(Debug, Clone)]
pub struct S3Response<T> {
    /// S3 operation output
    pub output: T,

    /// HTTP status code.
    /// This field overrides the status code implied by the output.
    pub status: Option<StatusCode>,

    /// HTTP headers.
    /// This field overrides the headers implied by the output.
    pub headers: HeaderMap,

    /// Response extensions.
    /// This is used to pass custom data between middlewares.
    pub extensions: Extensions,
}

impl<T> S3Response<T> {
    /// Create a new S3 response with the given output.
    pub fn new(output: T) -> Self {
        Self {
            output,
            status: default(),
            headers: default(),
            extensions: default(),
        }
    }

    /// Create a new S3 response with the given output and status code.
    pub fn with_status(output: T, status: StatusCode) -> Self {
        Self {
            output,
            status: Some(status),
            headers: default(),
            extensions: default(),
        }
    }

    /// Create a new S3 response with the given output and headers.
    pub fn with_headers(output: T, headers: HeaderMap) -> Self {
        Self {
            output,
            status: default(),
            headers,
            extensions: default(),
        }
    }

    /// Map the output of the response to a new type.
    pub fn map_output<U>(self, f: impl FnOnce(T) -> U) -> S3Response<U> {
        S3Response {
            output: f(self.output),
            status: self.status,
            headers: self.headers,
            extensions: self.extensions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- HttpError ---

    #[test]
    fn http_error_debug() {
        let err = HttpError::new(Box::new(std::io::Error::other("test")));
        let dbg = format!("{err:?}");
        assert!(dbg.contains("test"));
    }

    #[test]
    fn http_error_into_std_error() {
        let err = HttpError::new(Box::new(std::io::Error::other("oops")));
        let std_err: StdError = err.into();
        assert!(std_err.to_string().contains("oops"));
    }

    // --- TrailingHeaders ---

    #[test]
    fn trailing_headers_not_ready() {
        let th = TrailingHeaders(std::sync::Arc::new(std::sync::Mutex::new(None)));
        assert!(!th.is_ready());
        assert!(th.take().is_none());
        assert!(th.read(|_| ()).is_none());
    }

    #[test]
    fn trailing_headers_ready() {
        let mut hm = HeaderMap::new();
        hm.insert("x-test", "value".parse().unwrap());
        let th = TrailingHeaders(std::sync::Arc::new(std::sync::Mutex::new(Some(hm))));

        assert!(th.is_ready());
        let val = th.read(|h| h.get("x-test").unwrap().to_str().unwrap().to_owned());
        assert_eq!(val.as_deref(), Some("value"));
    }

    #[test]
    fn trailing_headers_take() {
        let mut hm = HeaderMap::new();
        hm.insert("x-test", "val".parse().unwrap());
        let th = TrailingHeaders(std::sync::Arc::new(std::sync::Mutex::new(Some(hm))));

        let taken = th.take().unwrap();
        assert_eq!(taken.get("x-test").unwrap(), "val");

        // After take, it's gone
        assert!(!th.is_ready());
        assert!(th.take().is_none());
    }

    #[test]
    fn trailing_headers_debug() {
        let th = TrailingHeaders(std::sync::Arc::new(std::sync::Mutex::new(None)));
        let dbg = format!("{th:?}");
        assert!(dbg.contains("TrailingHeaders"));
        assert!(dbg.contains("ready"));
    }

    #[test]
    fn trailing_headers_clone() {
        let mut hm = HeaderMap::new();
        hm.insert("x-test", "val".parse().unwrap());
        let th = TrailingHeaders(std::sync::Arc::new(std::sync::Mutex::new(Some(hm))));
        let th2 = th.clone();
        // Both point to the same data
        assert!(th.is_ready());
        assert!(th2.is_ready());
        let _ = th.take();
        // After take from one, the other also sees it gone
        assert!(!th2.is_ready());
    }

    // --- S3Request ---

    #[test]
    fn s3_request_map_input() {
        let req: S3Request<i32> = S3Request {
            input: 42,
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let mapped = req.map_input(|n| n.to_string());
        assert_eq!(mapped.input, "42");
        assert_eq!(mapped.method, Method::GET);
    }

    // --- S3Response ---

    #[test]
    fn s3_response_new() {
        let resp = S3Response::new(123);
        assert_eq!(resp.output, 123);
        assert!(resp.status.is_none());
        assert!(resp.headers.is_empty());
    }

    #[test]
    fn s3_response_with_status() {
        let resp = S3Response::with_status("ok", StatusCode::CREATED);
        assert_eq!(resp.output, "ok");
        assert_eq!(resp.status, Some(StatusCode::CREATED));
    }

    #[test]
    fn s3_response_with_headers() {
        let mut hm = HeaderMap::new();
        hm.insert("x-custom", "val".parse().unwrap());
        let resp = S3Response::with_headers(10, hm);
        assert_eq!(resp.output, 10);
        assert!(resp.status.is_none());
        assert_eq!(resp.headers.get("x-custom").unwrap(), "val");
    }

    #[test]
    fn s3_response_map_output() {
        let resp = S3Response::with_status(5, StatusCode::OK);
        let mapped = resp.map_output(|n| n * 2);
        assert_eq!(mapped.output, 10);
        assert_eq!(mapped.status, Some(StatusCode::OK));
    }
}
