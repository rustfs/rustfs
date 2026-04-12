//! HTTP layer types and utilities used internally by the S3 service.
//!
//! Contains request and response wrappers, body types, query-string and header
//! helpers, multipart-form parsing, and the AWS chunked-upload stream decoder.

mod ser;
pub use self::ser::*;

mod de;
pub use self::de::*;

mod ordered_qs;
pub use self::ordered_qs::*;

mod ordered_headers;
pub use self::ordered_headers::*;

mod aws_chunked_stream;
pub use self::aws_chunked_stream::*;

mod multipart;
pub use self::multipart::*;

mod body;
pub use self::body::*;

mod keep_alive_body;
pub use self::keep_alive_body::KeepAliveBody;

mod etag;

mod request;
pub use self::request::Request;

mod response;
pub use self::response::Response;

pub use hyper::header::{HeaderName, HeaderValue, InvalidHeaderValue};
pub use hyper::http::StatusCode;
