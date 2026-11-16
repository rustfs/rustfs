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

//! Legacy s3s edge of GetObject, HeadObject and GetObjectAttributes: batch C of the gateway
//! migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::s3_api;
use rustfs_gateway_stream::{BoxPayloadStream, PayloadRead, StreamError, StreamErrorKind};
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};
use tracing::instrument;

impl FS {
    /// Get bucket notification
    #[instrument(
        name = "get_object",
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub(in crate::storage::ecfs) async fn get_object_edge(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        crate::hp_guard!("S3::get_object");
        let request_id = req
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .map(|ctx| ctx.request_id.clone());
        let usecase = s3_api::object_usecase_for(self);
        record_s3_op(S3Operation::GetObject);
        let result = through_gateway_seam_answered(
            req,
            |input| Ok(s3_api::GetObjectFields::from_s3s(input)),
            s3_api::GetObjectOutputFacts::into_s3s,
            |envelope, input| usecase.execute_get_object_fields(envelope, input),
        )
        .await;
        if let Err(error) = &result {
            log_object_internal_error("GetObject", request_id.as_deref(), error);
        }
        result
    }

    pub(in crate::storage::ecfs) async fn get_object_attributes_edge(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        record_s3_op(S3Operation::GetObjectAttributes);
        through_gateway_seam_answered(
            req,
            |input| Ok(s3_api::GetObjectAttributesFields::from_s3s(input)),
            |output| Ok(s3_api::GetObjectAttributesOutputFacts::into_s3s(output)),
            |envelope, input| usecase.execute_get_object_attributes(envelope, input),
        )
        .await
    }

    #[instrument(name = "head_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn head_object_edge(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        crate::hp_guard!("S3::head_object");
        let usecase = s3_api::object_usecase_for(self);
        record_s3_op(S3Operation::HeadObject);
        through_gateway_seam_answered(
            req,
            |input| Ok(s3_api::HeadObjectFields::from_s3s(input)),
            s3_api::HeadObjectOutputFacts::into_s3s,
            |envelope, input| usecase.execute_head_object_fields(envelope, input),
        )
        .await
    }
}

impl s3_api::GetObjectAttributesFields {
    fn from_s3s(input: GetObjectAttributesInput) -> Self {
        Self {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            object_attributes: input
                .object_attributes
                .into_iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            max_parts: input.max_parts,
            part_number_marker: input.part_number_marker.map(|marker| marker.to_string()),
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
        }
    }
}

impl s3_api::GetObjectAttributesOutputFacts {
    fn into_s3s(self) -> GetObjectAttributesOutput {
        GetObjectAttributesOutput {
            e_tag: self.e_tag.map(|tag| {
                if tag.weak {
                    ETag::Weak(tag.opaque)
                } else {
                    ETag::Strong(tag.opaque)
                }
            }),
            last_modified: self.last_modified.map(Timestamp::from),
            version_id: self.version_id,
            storage_class: self.storage_class.map(StorageClass::from),
            object_size: self.object_size,
            checksum: self.checksum.map(|checksum| Checksum {
                checksum_crc32: checksum.crc32,
                checksum_crc32c: checksum.crc32c,
                checksum_crc64nvme: checksum.crc64nvme,
                checksum_sha1: checksum.sha1,
                checksum_sha256: checksum.sha256,
                checksum_type: checksum.checksum_type.map(ChecksumType::from),
                ..Default::default()
            }),
            object_parts: self.object_parts.map(|parts| GetObjectAttributesParts {
                is_truncated: Some(parts.is_truncated),
                max_parts: Some(parts.max_parts),
                next_part_number_marker: parts.next_part_number_marker,
                part_number_marker: parts.part_number_marker,
                total_parts_count: Some(parts.total_parts_count),
                parts: Some(
                    parts
                        .parts
                        .into_iter()
                        .map(|part| ObjectPart {
                            part_number: part.part_number,
                            size: Some(part.size),
                            checksum_crc32: part.checksums.crc32,
                            checksum_crc32c: part.checksums.crc32c,
                            checksum_crc64nvme: part.checksums.crc64nvme,
                            checksum_sha1: part.checksums.sha1,
                            checksum_sha256: part.checksums.sha256,
                            ..Default::default()
                        })
                        .collect(),
                ),
            }),
            ..Default::default()
        }
    }
}

impl s3_api::HeadObjectFields {
    pub(crate) fn from_s3s(input: HeadObjectInput) -> Self {
        use rustfs_gateway_types::ByteRange;
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

impl s3_api::HeadObjectOutputFacts {
    pub(crate) fn into_s3s(self) -> Result<HeadObjectOutput, rustfs_gateway_types::compat::ConversionError> {
        let mut output = rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::head_object::output_to_s3s(self.dto)?;
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

impl s3_api::GetObjectFields {
    pub(crate) fn from_s3s(input: GetObjectInput) -> Self {
        use rustfs_gateway_types::ByteRange;
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

impl s3_api::GetObjectOutputFacts {
    pub(crate) fn into_s3s(self) -> Result<GetObjectOutput, rustfs_gateway_types::compat::ConversionError> {
        let mut output = rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::get_object::output_to_s3s(self.dto)?;
        output.e_tag = self.etag.map(|etag| {
            if etag.weak {
                ETag::Weak(etag.opaque)
            } else {
                ETag::Strong(etag.opaque)
            }
        });
        output.body = self.body.map(s3_api::GetObjectBody::into_s3s);
        Ok(output)
    }
}

impl s3_api::GetObjectBody {
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
                    Some(Err(Box::new(rustfs_gateway_types::compat::ConversionError {
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

impl s3s::stream::ByteStream for LegacyGetStream {
    fn remaining_length(&self) -> s3s::stream::RemainingLength {
        match self
            .payload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len_hint()
            .and_then(|length| usize::try_from(length).ok())
        {
            Some(length) => s3s::stream::RemainingLength::new_exact(length),
            None => s3s::stream::RemainingLength::unknown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StreamError, legacy_get_body_error};

    #[test]
    fn get_body_preserves_upstream_cause_and_native_framing_errors() {
        let error = legacy_get_body_error(StreamError::upstream(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "body ended early",
        ))));
        let source = error
            .downcast_ref::<std::io::Error>()
            .expect("the original cause stays at the legacy boundary");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        for error in [
            StreamError::incomplete_body(),
            StreamError::length_mismatch(9, 4),
            StreamError::polled_after_eof(),
        ] {
            let error = error.with_bytes_before_error(4);
            let message = error.to_string();
            let error = legacy_get_body_error(error);
            let source = error
                .downcast_ref::<StreamError>()
                .expect("native framing errors retain their type");
            assert_eq!(source.bytes_before_error(), 4);
            assert_eq!(source.to_string(), message);
        }
    }
}
