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

//! The S3 error carrier shared by every RustFS crate.
//!
//! Responsible for: `S3Error` and its accessor contract, `S3Result`, `StdError`,
//! `S3ErrorParts`, and the `s3_error` macro.
//! Not responsible for: the code set and its statuses (`code.rs`), s3s
//! conversions (`crate::compat_s3s`), or rendering an error onto the wire.
//! Upstream: `http`. Downstream: every crate that raises S3 errors; the legacy
//! edge converts through `compat_s3s`, the gateway edge through `into_parts`.

mod code;

use http::{HeaderMap, StatusCode};
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;

pub use code::S3ErrorCode;

/// A boxed, thread-safe error cause.
pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` with [`S3Error`] as the default error type.
pub type S3Result<T = (), E = S3Error> = Result<T, E>;

/// An S3 error: a code plus the optional message, request id, explicit HTTP
/// status, cause, and response headers.
///
/// The fields live behind one box so `S3Result<T>` stays pointer-sized on the
/// error path. `status_code()` answers with the explicit status when one was
/// set and with the code's own status otherwise, so a bare `new(code)` already
/// carries the status the wire needs.
#[derive(Debug)]
pub struct S3Error(Box<S3ErrorParts>);

/// The owned fields of an [`S3Error`], as handed out by [`S3Error::into_parts`].
#[derive(Debug)]
pub struct S3ErrorParts {
    /// The wire error code.
    pub code: S3ErrorCode,
    /// The message, when one was given; there is no per-code default here.
    pub message: Option<Cow<'static, str>>,
    /// The request id, when one was attached.
    pub request_id: Option<String>,
    /// The explicitly set status only; `None` means "use the code's status".
    pub status: Option<StatusCode>,
    /// The cause, when one was attached.
    pub source: Option<StdError>,
    /// Response headers that must accompany the error, when any.
    pub headers: Option<HeaderMap>,
}

impl S3Error {
    /// An error carrying only the code.
    pub fn new(code: S3ErrorCode) -> Self {
        Self(Box::new(S3ErrorParts {
            code,
            message: None,
            request_id: None,
            status: None,
            source: None,
            headers: None,
        }))
    }

    /// An error with a message.
    pub fn with_message(code: S3ErrorCode, message: impl Into<Cow<'static, str>>) -> Self {
        let mut error = Self::new(code);
        error.0.message = Some(message.into());
        error
    }

    /// An error whose message is rendered from format arguments; a plain
    /// literal is borrowed rather than copied. The `s3_error` macro expands to
    /// this constructor.
    pub fn with_message_fmt(code: S3ErrorCode, args: fmt::Arguments<'_>) -> Self {
        let message = match args.as_str() {
            Some(literal) => Cow::Borrowed(literal),
            None => Cow::Owned(args.to_string()),
        };
        Self::with_message(code, message)
    }

    /// An error with a cause and no message.
    pub fn with_source(code: S3ErrorCode, source: StdError) -> Self {
        let mut error = Self::new(code);
        error.0.source = Some(source);
        error
    }

    /// An `InternalError` wrapping `source`, for `map_err`.
    pub fn internal_error<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::with_source(S3ErrorCode::InternalError, Box::new(source))
    }

    /// Replaces the code; an explicit status, if any, is kept.
    pub fn set_code(&mut self, code: S3ErrorCode) {
        self.0.code = code;
    }

    /// Replaces the message.
    pub fn set_message(&mut self, message: impl Into<Cow<'static, str>>) {
        self.0.message = Some(message.into());
    }

    /// Replaces the request id.
    pub fn set_request_id(&mut self, request_id: impl Into<String>) {
        self.0.request_id = Some(request_id.into());
    }

    /// Replaces the cause.
    pub fn set_source(&mut self, source: StdError) {
        self.0.source = Some(source);
    }

    /// Pins the HTTP status, overriding the code's own.
    pub fn set_status_code(&mut self, status: StatusCode) {
        self.0.status = Some(status);
    }

    /// Replaces the response headers.
    pub fn set_headers(&mut self, headers: HeaderMap) {
        self.0.headers = Some(headers);
    }

    /// The wire error code.
    pub fn code(&self) -> &S3ErrorCode {
        &self.0.code
    }

    /// The message, if one was given.
    pub fn message(&self) -> Option<&str> {
        self.0.message.as_deref()
    }

    /// The request id, if one was attached.
    pub fn request_id(&self) -> Option<&str> {
        self.0.request_id.as_deref()
    }

    /// The cause, if one was attached.
    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.0.source.as_deref()
    }

    /// The explicit status if one was set, otherwise the code's own status;
    /// `None` only for a [`S3ErrorCode::Custom`] code without an explicit status.
    pub fn status_code(&self) -> Option<StatusCode> {
        self.0.status.or_else(|| self.0.code.status_code())
    }

    /// Response headers that must accompany the error, if any.
    pub fn headers(&self) -> Option<&HeaderMap> {
        self.0.headers.as_ref()
    }

    /// Takes the error apart, for conversion into another error type.
    pub fn into_parts(self) -> S3ErrorParts {
        *self.0
    }
}

impl fmt::Display for S3Error {
    /// Renders the present fields in the same shape the s3s error used, so log
    /// lines do not change when a crate switches to this type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts = &*self.0;
        let mut out = f.debug_struct("S3Error");
        out.field("code", &parts.code);
        if let Some(message) = &parts.message {
            out.field("message", message);
        }
        if let Some(request_id) = &parts.request_id {
            out.field("request_id", request_id);
        }
        if let Some(status) = &parts.status {
            out.field("status_code", status);
        }
        if let Some(source) = &parts.source {
            out.field("source", source);
        }
        out.finish_non_exhaustive()
    }
}

impl std::error::Error for S3Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source.as_deref().map(|source| source as _)
    }
}

impl From<S3ErrorCode> for S3Error {
    fn from(code: S3ErrorCode) -> Self {
        Self::new(code)
    }
}

impl From<Infallible> for S3Error {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

/// Builds an [`S3Error`] from a code identifier, optionally with a message.
///
/// Three call forms: the code alone, the code with a literal message, and the
/// code with a format string and its arguments. A message literal is borrowed;
/// a formatted message is rendered once.
#[macro_export]
macro_rules! s3_error {
    ($code:ident $(,)?) => {
        $crate::S3Error::new($crate::S3ErrorCode::$code)
    };
    ($code:ident, $($arg:tt)+) => {
        $crate::S3Error::with_message_fmt($crate::S3ErrorCode::$code, ::std::format_args!($($arg)+))
    };
}
