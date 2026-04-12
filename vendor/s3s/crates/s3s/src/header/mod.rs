//! S3-specific HTTP header name constants.
//!
//! This module re-exports generated constants for all HTTP headers used by the
//! Amazon S3 REST API, such as `x-amz-*` headers and other S3-specific fields.

cfg_if::cfg_if! {
    if #[cfg(feature = "minio")] {
        mod generated_minio;
        use self::generated_minio as generated;
    } else {
        mod generated;
    }
}

pub use self::generated::*;
