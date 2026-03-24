//! S3 error types.
//!
//! This module provides [`S3Error`] and [`S3Result`] for representing and
//! propagating errors from S3 operations, along with [`S3ErrorCode`] which
//! enumerates every standard S3 error code. The [`s3_error!`] macro offers
//! a convenient shorthand for constructing errors by code or code-with-message.

cfg_if::cfg_if! {
    if #[cfg(feature = "minio")] {
        mod generated_minio;
        use self::generated_minio as generated;
    } else {
        mod generated;
    }
}

pub use self::generated::*;

use crate::HttpResponse;
use crate::ops;
use crate::xml;
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::io::Write;
use std::str::FromStr;

use hyper::HeaderMap;
use hyper::StatusCode;

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type S3Result<T = (), E = S3Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub struct S3Error(Box<Inner>);

#[derive(Debug)]
struct Inner {
    code: S3ErrorCode,
    message: Option<Cow<'static, str>>,
    // resource: Option<String>,
    request_id: Option<String>,
    status_code: Option<StatusCode>,
    source: Option<StdError>,
    headers: Option<HeaderMap>,
}

impl S3Error {
    #[must_use]
    pub fn new(code: S3ErrorCode) -> Self {
        Self(Box::new(Inner {
            code,
            message: None,
            // resource: None,
            request_id: None,
            status_code: None,
            source: None,
            headers: None,
        }))
    }

    #[must_use]
    pub fn with_message(code: S3ErrorCode, msg: impl Into<Cow<'static, str>>) -> Self {
        let mut this = Self::new(code);
        this.0.message = Some(msg.into());
        this
    }

    #[doc(hidden)]
    pub fn with_message_fmt(code: S3ErrorCode, args: fmt::Arguments<'_>) -> Self {
        let msg = if let Some(msg) = args.as_str() {
            Cow::Borrowed(msg)
        } else {
            Cow::Owned(fmt::format(args))
        };

        Self::with_message(code, msg)
    }

    #[must_use]
    pub fn with_source(code: S3ErrorCode, source: StdError) -> Self {
        let mut this = Self::new(code);
        this.0.source = Some(source);
        this
    }

    pub fn set_code(&mut self, val: S3ErrorCode) {
        self.0.code = val;
    }

    pub fn set_message(&mut self, val: impl Into<Cow<'static, str>>) {
        self.0.message = Some(val.into());
    }

    pub fn set_request_id(&mut self, val: impl Into<String>) {
        self.0.request_id = Some(val.into());
    }

    pub fn set_source(&mut self, val: StdError) {
        self.0.source = Some(val);
    }

    pub fn set_status_code(&mut self, val: StatusCode) {
        self.0.status_code = Some(val);
    }

    pub fn set_headers(&mut self, val: HeaderMap) {
        self.0.headers = Some(val);
    }

    #[must_use]
    pub fn code(&self) -> &S3ErrorCode {
        &self.0.code
    }

    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.0.message.as_deref()
    }

    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.0.request_id.as_deref()
    }

    #[must_use]
    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.0.source.as_deref()
    }

    #[must_use]
    pub fn status_code(&self) -> Option<StatusCode> {
        self.0.status_code.or_else(|| self.0.code.status_code())
    }

    #[must_use]
    pub fn headers(&self) -> Option<&HeaderMap> {
        self.0.headers.as_ref()
    }

    #[must_use]
    pub(crate) fn take_headers(&mut self) -> Option<HeaderMap> {
        self.0.headers.take()
    }

    #[must_use]
    pub fn internal_error<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::with_source(S3ErrorCode::InternalError, Box::new(source))
    }

    /// Serialize an `S3Error` into an `Response` that can be sent back to the client.
    /// This can be useful in creating middleware around an S3 Service implementation
    /// that want to send consistent error messages to clients on errors.
    ///
    /// # Errors
    ///
    /// Returns [`S3Error`] if it was not possible to serialize the error into XML.
    pub fn to_http_response(self) -> S3Result<HttpResponse> {
        ops::serialize_error(self, false).map(Into::into)
    }
}

impl xml::Serialize for S3Error {
    fn serialize<W: Write>(&self, s: &mut xml::Serializer<W>) -> xml::SerResult {
        s.content("Error", self)
    }
}

impl xml::SerializeContent for S3Error {
    fn serialize_content<W: Write>(&self, s: &mut xml::Serializer<W>) -> xml::SerResult {
        s.content("Code", self.0.code.as_str())?;
        if let Some(val) = self.0.message.as_deref() {
            s.content("Message", val)?;
        }
        // if let Some(val) = self.0.resource.as_deref() {
        //     s.content("Resource", val)?;
        // }
        if let Some(val) = self.0.request_id.as_deref() {
            s.content("RequestId", val)?;
        }
        Ok(())
    }
}

impl From<S3ErrorCode> for S3Error {
    fn from(code: S3ErrorCode) -> Self {
        Self::new(code)
    }
}

impl From<Infallible> for S3Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("S3Error");
        d.field("code", &self.0.code);
        if let Some(ref message) = self.0.message {
            d.field("message", &message);
        }
        if let Some(ref request_id) = self.0.request_id {
            d.field("request_id", &request_id);
        }
        if let Some(ref status_code) = self.0.status_code {
            d.field("status_code", &status_code);
        }
        if let Some(ref source) = self.0.source {
            d.field("source", &source);
        }
        d.finish_non_exhaustive()
    }
}

#[macro_export]
macro_rules! s3_error {
    ($source:expr, $code:ident) => {{
        let mut err = $crate::s3_error!($code);
        err.set_source(Box::new($source));
        err
    }};
    ($source:expr, $code:ident, $($arg:tt)+) => {{
        let mut err = $crate::s3_error!($code, $($arg)+);
        err.set_source(Box::new($source));
        err
    }};
    ($code:ident) => {
        $crate::S3Error::new($crate::S3ErrorCode::$code)
    };
    ($code:ident, $($arg:tt)+) => {
        $crate::S3Error::with_message_fmt($crate::S3ErrorCode::$code, format_args!($($arg)+))
    };
}

impl FromStr for S3ErrorCode {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_bytes(s.as_bytes()).unwrap())
    }
}

impl S3ErrorCode {
    pub fn as_str(&self) -> &str {
        if let Some(s) = self.as_static_str() {
            return s;
        }
        if let Self::Custom(s) = self {
            return s;
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    // not needed since the macro is globally in scope
    // use super::*;

    #[test]
    fn s3_error_fmt() {
        let bucket = "my_bucket";
        let e = s3_error!(AccessDenied, "access denied for bucket {bucket}");

        assert_eq!(e.message(), Some("access denied for bucket my_bucket"));
    }
}
