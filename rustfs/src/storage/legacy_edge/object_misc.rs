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

//! Legacy s3s edge of CopyObject, UploadPartCopy, CompleteMultipartUpload, DeleteObject,
//! DeleteObjects, RestoreObject and SelectObjectContent: batch E of the gateway migration
//! (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::ecfs_extend::validate_new_object_key;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use http::{HeaderMap, HeaderValue};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::delete_object;
use rustfs_gateway_types::dto as gateway_dto;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

/// CompleteMultipartUpload's input as its use case reads it, from what the legacy decoder read.
///
/// The parts cross with their numbers and entity tags as the use case reads them: an absent number
/// as part `0`, which the use case refuses as it always has, and a tag as its opaque bytes, which
/// the gateway's request grammar might refuse. The per-algorithm checksum members stay behind: the
/// use case reads the `x-amz-checksum-*` headers they were decoded from, and the gateway input
/// holds only one checksum.
pub(crate) fn complete_multipart_upload_input(
    mut input: CompleteMultipartUploadInput,
) -> Result<(gateway_dto::CompleteMultipartUploadInput, Option<gateway_dto::CompletedMultipartUpload>), ConversionError> {
    let has_upload = input.multipart_upload.is_some();
    input.multipart_upload.get_or_insert_with(Default::default);
    let tags: Vec<Option<String>> = input
        .multipart_upload
        .iter_mut()
        .flat_map(|upload| upload.parts.iter_mut().flatten())
        .map(|part| {
            part.part_number.get_or_insert(0);
            part.e_tag.take().map(|tag| tag.value().to_owned())
        })
        .collect();
    for checksum in [
        &mut input.checksum_crc32,
        &mut input.checksum_crc32c,
        &mut input.checksum_crc64nvme,
        &mut input.checksum_md5,
        &mut input.checksum_sha1,
        &mut input.checksum_sha256,
        &mut input.checksum_sha512,
        &mut input.checksum_xxhash128,
        &mut input.checksum_xxhash3,
        &mut input.checksum_xxhash64,
    ] {
        *checksum = None;
    }
    let mut converted = complete_multipart_upload::input_from_s3s(input)?;
    for (part, tag) in converted.multipart_upload.parts.iter_mut().zip(tags) {
        part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
    }
    let parts = std::mem::take(&mut converted.multipart_upload);
    Ok((converted, has_upload.then_some(parts)))
}

/// CompleteMultipartUpload's answer as the legacy stack writes it: the members its output has no
/// carrier for are the response headers it has always written them as.
pub(super) fn complete_multipart_upload_answer(
    mut output: gateway_dto::CompleteMultipartUploadOutput,
) -> Result<(CompleteMultipartUploadOutput, HeaderMap), ConversionError> {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-amz-checksum-md5", output.checksum_header_md5.take()),
        ("x-amz-checksum-sha512", output.checksum_header_sha512.take()),
        ("x-amz-checksum-xxhash3", output.checksum_header_xxhash3.take()),
        ("x-amz-checksum-xxhash64", output.checksum_header_xxhash64.take()),
        ("x-amz-checksum-xxhash128", output.checksum_header_xxhash128.take()),
        ("x-amz-server-side-encryption-customer-algorithm", output.sse_customer_algorithm.take()),
        ("x-amz-server-side-encryption-customer-key-md5", output.sse_customer_key_md5.take()),
    ] {
        if let Some(value) = value {
            let value = HeaderValue::from_str(&value).map_err(|_| ConversionError {
                field: name,
                reason: "not a header value",
            })?;
            headers.insert(name, value);
        }
    }
    let mut output = complete_multipart_upload::output_to_s3s(output)?;
    output.e_tag = output.e_tag.map(xml_escaped_etag);
    Ok((output, headers))
}

/// Prepares the entity tag for legacy XML serialization.
fn xml_escaped_etag(mut etag: ETag) -> ETag {
    let (ETag::Strong(value) | ETag::Weak(value)) = &mut etag;
    *value = quick_xml::escape::escape(value.as_str()).into_owned();
    etag
}

impl FS {
    #[instrument(name = "complete_multipart_upload", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn complete_multipart_upload_edge(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        crate::hp_guard!("S3::complete_multipart_upload");
        record_s3_op(S3Operation::CompleteMultipartUpload);
        // The key's own refusal and message, which the seam's key floor would replace.
        validate_new_object_key(&req.input.key, "CompleteMultipartUpload")?;
        let usecase = s3_api::multipart_usecase_for(self);
        let answer = through_gateway_seam(
            req,
            complete_multipart_upload_input,
            complete_multipart_upload_answer,
            |envelope, (mut input, parts)| {
                Box::pin(async move {
                    match parts {
                        Some(parts) => {
                            input.multipart_upload = parts;
                            usecase.execute_complete_multipart_upload(envelope, input).await
                        }
                        None => usecase.execute_complete_multipart_upload_parts(envelope, input, None).await,
                    }
                })
            },
        )
        .await?;
        let (output, headers) = answer.output;
        let mut response = S3Response::new(output);
        response.headers = headers;
        Ok(response)
    }

    /// Copy an object from one location to another
    #[instrument(name = "copy_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn copy_object_edge(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_copy_object(req).await
    }

    /// Delete an object
    #[instrument(name = "delete_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_object_edge(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        crate::hp_guard!("S3::delete_object");
        record_s3_op(S3Operation::DeleteObject);
        let request_id = req
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .map(|ctx| ctx.request_id.clone());
        let usecase = s3_api::object_usecase_for(self);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let input = req.input;
        let result = Box::pin(usecase.execute_delete_object_fields(envelope, input.bucket, input.key, input.version_id))
            .await
            .and_then(|output| delete_object::output_to_s3s(output).map_err(crate::app::dto_values::seam_error))
            .map(S3Response::new)
            .map_err(Into::into);
        if let Err(error) = &result {
            log_object_internal_error("DeleteObject", request_id.as_deref(), error);
        }
        result
    }

    /// Delete multiple objects
    #[instrument(name = "delete_objects", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_objects_edge(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_delete_objects(req).await
    }

    pub(in crate::storage::ecfs) async fn restore_object_edge(
        &self,
        req: S3Request<RestoreObjectInput>,
    ) -> S3Result<S3Response<RestoreObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_restore_object(req).await
    }

    pub(in crate::storage::ecfs) async fn select_object_content_edge(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_select_object_content(req).await
    }

    #[instrument(name = "upload_part_copy", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn upload_part_copy_edge(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        record_s3_op(S3Operation::UploadPartCopy);
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::UploadPartCopy, |req| Box::pin(usecase.execute_upload_part_copy(req))).await
    }
}
