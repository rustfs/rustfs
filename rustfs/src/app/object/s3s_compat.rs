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

//! Compatibility conversions between owned Get/Head object facts and s3s DTOs.
//!
//! Retained for `admin/router.rs` (object-lambda), `app/website.rs`, and `app/test_seams` until
//! T4.2 removes the remaining `s3s` dependency from `admin/router.rs`.

use crate::app::object_usecase::{GetObjectBody, GetObjectFields, GetObjectOutputFacts};
#[cfg(test)]
use crate::app::object_usecase::{HeadObjectFields, HeadObjectOutputFacts};
use rustfs_gateway_stream::{BoxPayloadStream, PayloadRead, StreamError, StreamErrorKind};
use rustfs_gateway_types::ByteRange;
use rustfs_gateway_types::compat::ConversionError;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::get_object;
#[cfg(test)]
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::head_object;
use s3s::dto::{ETag, GetObjectInput, GetObjectOutput, Range, StreamingBlob};
#[cfg(test)]
use s3s::dto::{HeadObjectInput, HeadObjectOutput};
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};

#[cfg(test)]
impl HeadObjectFields {
    pub(crate) fn from_s3s(input: HeadObjectInput) -> Self {
        Self {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            part_number: input.part_number,
            range: input.range.map(|range| match range {
                Range::Int { first, last: Some(last) } => ByteRange::FromTo { first, last },
                Range::Int { first, last: None } => ByteRange::From { first },
                Range::Suffix { length } => ByteRange::Suffix { length },
            }),
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
        }
    }
}

#[cfg(test)]
impl HeadObjectOutputFacts {
    pub(crate) fn into_s3s(self) -> Result<HeadObjectOutput, ConversionError> {
        let mut output = head_object::output_to_s3s(self.dto)?;
        output.e_tag = self.etag.map(|etag| {
            if etag.weak {
                ETag::Weak(etag.opaque)
            } else {
                ETag::Strong(etag.opaque)
            }
        });
        Ok(output)
    }
}

impl GetObjectFields {
    pub(crate) fn from_s3s(input: GetObjectInput) -> Self {
        Self {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            part_number: input.part_number,
            range: input.range.map(|range| match range {
                Range::Int { first, last: Some(last) } => ByteRange::FromTo { first, last },
                Range::Int { first, last: None } => ByteRange::From { first },
                Range::Suffix { length } => ByteRange::Suffix { length },
            }),
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
        }
    }
}

impl GetObjectOutputFacts {
    pub(crate) fn into_s3s(self) -> Result<GetObjectOutput, ConversionError> {
        let mut output = get_object::output_to_s3s(self.dto)?;
        output.e_tag = self.etag.map(|etag| {
            if etag.weak {
                ETag::Weak(etag.opaque)
            } else {
                ETag::Strong(etag.opaque)
            }
        });
        output.body = self.body.map(GetObjectBody::into_s3s);
        Ok(output)
    }
}

impl GetObjectBody {
    pub(crate) fn into_s3s(self) -> StreamingBlob {
        match self {
            Self::Bytes(bytes) => StreamingBlob::from_bytes(bytes),
            Self::Stream(payload) => StreamingBlob::new(LegacyGetStream {
                payload: Mutex::new(payload),
                done: false,
            }),
        }
    }
}

// The legacy body requires Sync. Polling uses exclusive access, not a mutex lock.
struct LegacyGetStream {
    payload: Mutex<BoxPayloadStream>,
    done: bool,
}

impl futures::Stream for LegacyGetStream {
    type Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let event = std::task::ready!(
            this.payload
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
                .poll_read(cx)
        );
        Poll::Ready(match event {
            Ok(PayloadRead::Chunk(bytes)) => Some(Ok(bytes)),
            Ok(PayloadRead::Eof { trailers }) => {
                this.done = true;
                if trailers.is_empty() {
                    None
                } else {
                    Some(Err(Box::new(ConversionError {
                        field: "body",
                        reason: "legacy GetObject cannot encode response trailers",
                    })))
                }
            }
            Err(error) => {
                this.done = true;
                Some(Err(legacy_get_body_error(error)))
            }
        })
    }
}

fn legacy_get_body_error(error: StreamError) -> Box<dyn std::error::Error + Send + Sync> {
    if matches!(error.kind(), StreamErrorKind::Upstream(_)) {
        let StreamErrorKind::Upstream(source) = error.into_kind() else {
            unreachable!("the owned error kind was checked above")
        };
        source
    } else {
        Box::new(error)
    }
}

impl s3s::stream::ByteStream for LegacyGetStream {}
