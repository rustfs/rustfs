// Copyright 2026 RustFS Team
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

//! Native GetObject fields and response facts shared by its two protocol edges.
//! The use case owns reading and completion; these carriers preserve raw names,
//! stored ETags, and the existing in-memory body owner until an edge encodes them.

use super::head::ResponseEtag;
use bytes::Bytes;
use futures::Stream;
use rustfs_gateway_stream::{BoxPayloadStream, PayloadCaps, PayloadRead, PayloadStream, StreamError, TrailingHeaders};
use rustfs_gateway_types::{ByteRange, dto};
use std::pin::Pin;
use std::task::{Context, Poll};

/// The legacy edge must not validate names before the use case's existing checks.
/// No Debug implementation: the customer key is bearer material.
pub(crate) struct GetObjectFields {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) part_number: Option<i32>,
    pub(crate) range: Option<ByteRange>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
}

/// Body and ETag retain their native representation until the final edge.
#[derive(Debug, Default)]
pub(crate) struct GetObjectOutputFacts {
    pub(crate) dto: dto::GetObjectOutput,
    pub(crate) etag: Option<ResponseEtag>,
    pub(crate) body: Option<GetObjectBody>,
}

/// Preserve the once-body's owner instead of hiding it inside a stream adapter.
pub(crate) enum GetObjectBody {
    Bytes(Bytes),
    Stream(BoxPayloadStream),
}

impl std::fmt::Debug for GetObjectBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let length = match self {
            Self::Bytes(bytes) => Some(bytes.len() as u64),
            Self::Stream(stream) => stream.len_hint(),
        };
        formatter
            .debug_struct("GetObjectBody")
            .field("length", &length)
            .finish_non_exhaustive()
    }
}

impl GetObjectBody {
    pub(super) fn stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Stream(Box::pin(GetPayload {
            stream: Box::pin(stream),
        }))
    }
}

struct GetPayload<S> {
    stream: Pin<Box<S>>,
}

impl<S, E> PayloadStream for GetPayload<S>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn poll_read(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        poll_stream(self.get_mut().stream.as_mut(), context)
    }

    fn caps(&self) -> PayloadCaps {
        PayloadCaps::PUSH
    }

    fn len_hint(&self) -> Option<u64> {
        None
    }
}

pub(super) fn poll_stream<S, E>(stream: Pin<&mut S>, context: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match std::task::ready!(stream.poll_next(context)) {
        Some(Ok(bytes)) => Poll::Ready(Ok(PayloadRead::Chunk(bytes))),
        Some(Err(error)) => Poll::Ready(Err(StreamError::upstream(error.into()))),
        None => Poll::Ready(Ok(PayloadRead::Eof {
            trailers: TrailingHeaders::empty(),
        })),
    }
}
