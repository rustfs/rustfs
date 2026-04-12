//! S3 data transfer objects (DTOs).
//!
//! This module re-exports all S3 operation input and output types together with
//! supporting data structures generated from the AWS Smithy service model.
//! These are the primary typed structures used when implementing the
//! [`S3`](crate::S3) trait.

mod build_error;

cfg_if::cfg_if! {
    if #[cfg(feature = "minio")] {
        mod generated_minio;
        use self::generated_minio as generated;
    } else {
        mod generated;
    }
}

pub use self::generated::*;

mod streaming_blob;
pub use self::streaming_blob::*;

mod timestamp;
pub use self::timestamp::*;

mod copy_source;
pub use self::copy_source::*;

mod range;
pub use self::range::Range;

mod content_type;
pub use self::content_type::*;

mod event;
pub use self::event::Event;

mod event_stream;
pub use self::event_stream::*;

mod etag;
pub use self::etag::*;

mod etag_condition;
pub use self::etag_condition::*;

mod post_response;
pub use self::post_response::*;

pub type List<T> = Vec<T>;
pub type Map<K, V> = std::collections::HashMap<K, V>;

pub type Body = hyper::body::Bytes;

pub type Unit = ();

impl From<ListObjectsInput> for ListObjectsV2Input {
    fn from(v1: ListObjectsInput) -> Self {
        let ListObjectsInput {
            bucket,
            delimiter,
            encoding_type,
            expected_bucket_owner,
            marker,
            max_keys,
            prefix,
            request_payer,
            optional_object_attributes,
        } = v1;

        Self {
            bucket,
            continuation_token: None,
            delimiter,
            encoding_type,
            expected_bucket_owner,
            fetch_owner: None,
            max_keys,
            prefix,
            request_payer,
            start_after: marker,
            optional_object_attributes,
        }
    }
}
