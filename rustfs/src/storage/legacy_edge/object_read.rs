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
        let result = usecase.execute_get_object(req).await;
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
        usecase.execute_get_object_attributes(req).await
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
