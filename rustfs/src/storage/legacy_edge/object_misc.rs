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

use crate::app::RequestEnvelope;
use crate::app::storage_api::object_usecase::bucket::restore_request_from_wire;
use crate::storage::access::legacy_copy_source;
use crate::storage::ecfs::*;
use crate::storage::ecfs_extend::validate_new_object_key;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api::{
    self, CopyObjectWriteInput, CopyObjectWriteOutput, CopySourceCondition, DefaultObjectUsecase, DeleteBatchETag,
    DeleteBatchEntry, DeleteBatchInput, DeleteBatchOutput,
};
use crate::storage::storage_api::to_s3s_xml_etag;
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
        record_s3_op(S3Operation::CopyObject);
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
        record_s3_op(S3Operation::DeleteObjects);
        let usecase = s3_api::object_usecase_for(self);
        let envelope = RequestEnvelope::from_s3s(&req);
        let output = usecase.execute_delete_objects(envelope, req.input.into()).await?;
        Ok(S3Response::new(output.into()))
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

impl DefaultObjectUsecase {
    /// Projects a legacy SelectObjectContent request onto the shared Select query path.
    pub async fn execute_select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = crate::app::select_request::select_input_from_s3s(req.input).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("convert the SelectObjectContent request: {err}"))
        })?;
        let answer = self.execute_select_object_content_owned(envelope, input).await?;
        let output = rustfs_gateway_types::compat::s3s_5761ddfe::select::output_to_s3s(answer.output);
        let mut response = S3Response::new(output);
        response.status = answer.status;
        response.headers = answer.headers;
        Ok(response)
    }

    /// Projects a legacy RestoreObject request onto the shared restore path.
    pub async fn execute_restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = req.input;
        self.execute_restore_object_owned(
            envelope,
            input.bucket,
            input.key,
            input.version_id,
            input.restore_request.map(restore_request_from_wire),
        )
        .await
        .map(|output| {
            S3Response::new(RestoreObjectOutput {
                request_charged: output
                    .request_charged
                    .map(|value| RequestCharged::from(value.as_str().to_owned())),
                restore_output_path: output.restore_output_path,
            })
        })
        .map_err(Into::into)
    }

    /// Projects a legacy CopyObject request onto the shared copy path.
    pub async fn execute_copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = copy_input_from_legacy(req.input);
        self.execute_copy_object_owned(envelope, input)
            .await
            .map(copy_output_to_legacy)
            .map_err(Into::into)
    }
}

fn copy_input_from_legacy(input: CopyObjectInput) -> CopyObjectWriteInput {
    let condition = |value: ETagCondition| match value {
        ETagCondition::Any => CopySourceCondition::Any,
        ETagCondition::ETag(ETag::Strong(value)) => CopySourceCondition::Strong(value),
        ETagCondition::ETag(ETag::Weak(_)) => CopySourceCondition::Weak,
    };
    let copy_source = legacy_copy_source(&input.copy_source)
        .map(|(bucket, key, version_id)| rustfs_gateway_types::CopySourceValue::Bucket { bucket, key, version_id });
    CopyObjectWriteInput {
        copy_source,
        bucket: input.bucket,
        key: input.key,
        version_id: input.version_id,
        specific_if_none_match: matches!(input.if_none_match, Some(ETagCondition::ETag(_))),
        server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
        ssekms_key_id: input.ssekms_key_id,
        sse_customer_algorithm: input.sse_customer_algorithm,
        sse_customer_key: input.sse_customer_key,
        sse_customer_key_md5: input.sse_customer_key_md5,
        copy_source_sse_customer_algorithm: input.copy_source_sse_customer_algorithm,
        copy_source_sse_customer_key: input.copy_source_sse_customer_key,
        copy_source_sse_customer_key_md5: input.copy_source_sse_customer_key_md5,
        metadata_directive: input.metadata_directive.map(|value| value.as_str().to_owned()),
        metadata: input.metadata,
        tagging: input.tagging,
        tagging_directive: input.tagging_directive.map(|value| value.as_str().to_owned()),
        copy_source_if_match: input.copy_source_if_match.map(condition),
        copy_source_if_none_match: input.copy_source_if_none_match.map(condition),
        cache_control: input.cache_control,
        content_disposition: input.content_disposition,
        content_encoding: input.content_encoding,
        content_language: input.content_language,
        content_type: input.content_type,
        expires: input.expires,
        website_redirect_location: input.website_redirect_location,
        object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
        object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
        object_lock_retain_until_date: input.object_lock_retain_until_date.map(time::OffsetDateTime::from),
        storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
        checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
    }
}

fn copy_output_to_legacy(output: CopyObjectWriteOutput) -> S3Response<CopyObjectOutput> {
    S3Response::new(CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: output.e_tag.as_deref().map(to_s3s_xml_etag),
            last_modified: output.last_modified.map(Timestamp::from),
            checksum_crc32: output.checksum_crc32,
            checksum_crc32c: output.checksum_crc32c,
            checksum_sha1: output.checksum_sha1,
            checksum_sha256: output.checksum_sha256,
            checksum_crc64nvme: output.checksum_crc64nvme,
            checksum_md5: output.checksum_md5,
            checksum_sha512: output.checksum_sha512,
            checksum_xxhash3: output.checksum_xxhash3,
            checksum_xxhash64: output.checksum_xxhash64,
            checksum_xxhash128: output.checksum_xxhash128,
            checksum_type: output.checksum_type.map(ChecksumType::from),
        }),
        copy_source_version_id: output.copy_source_version_id,
        server_side_encryption: output.server_side_encryption.map(Into::into),
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        version_id: output.version_id,
        ..Default::default()
    })
}

impl From<DeleteObjectsInput> for DeleteBatchInput {
    fn from(input: DeleteObjectsInput) -> Self {
        Self {
            bucket: input.bucket,
            objects: input.delete.objects.into_iter().map(Into::into).collect(),
            quiet: input.delete.quiet,
        }
    }
}

impl From<ObjectIdentifier> for DeleteBatchEntry {
    fn from(object: ObjectIdentifier) -> Self {
        Self {
            key: object.key,
            key_too_long: false,
            version_id: object.version_id,
            e_tag: object.e_tag.map(|tag| match tag {
                ETag::Strong(value) => DeleteBatchETag { value, weak: false },
                ETag::Weak(value) => DeleteBatchETag { value, weak: true },
            }),
            _last_modified_time: object.last_modified_time.map(|value| {
                let instant = time::OffsetDateTime::from(value);
                (instant.unix_timestamp(), instant.nanosecond())
            }),
            _size: object.size,
        }
    }
}

impl From<DeleteBatchOutput> for DeleteObjectsOutput {
    fn from(output: DeleteBatchOutput) -> Self {
        Self {
            deleted: output.deleted.map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| DeletedObject {
                        delete_marker: entry.delete_marker,
                        delete_marker_version_id: entry.delete_marker_version_id,
                        key: entry.key,
                        version_id: entry.version_id,
                    })
                    .collect()
            }),
            errors: output.errors.map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| Error {
                        code: entry.code,
                        key: entry.key,
                        message: entry.message,
                        version_id: entry.version_id,
                    })
                    .collect()
            }),
            request_charged: None,
        }
    }
}
