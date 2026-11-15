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

//! Legacy s3s edge of PutObject and UploadPart (PostObject has no trait method of its own and
//! reaches PutObject): batch D of the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    #[instrument(name = "put_object", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_object_edge(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        crate::hp_guard!("S3::put_object");
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_put_object(self, req).await
    }

    #[instrument(name = "upload_part", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn upload_part_edge(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        crate::hp_guard!("S3::upload_part");
        record_s3_op(S3Operation::UploadPart);
        let usecase = s3_api::multipart_usecase_for(self);
        audit_only_operation(req, S3Operation::UploadPart, |req| usecase.execute_upload_part(req)).await
    }
}

use crate::app::RequestEnvelope;
use crate::storage::s3_api::{
    DefaultMultipartUsecase, DefaultObjectUsecase, PutObjectWriteInput, PutObjectWriteOutput, UploadPartTarget,
    UploadPartWriteInput, UploadPartWriteOutput,
};

impl PutObjectWriteInput {
    pub(crate) fn from_legacy(input: PutObjectInput, trailing_headers: Option<TrailingHeaders>) -> Self {
        Self {
            bucket: input.bucket,
            key: input.key,
            body: input.body.map(s3_api::legacy_put_body),
            frame_trailers: None,
            trailing_headers: crate::app::trailer_adapter::trailer_source(trailing_headers),
            content_length: input.content_length,
            metadata: input.metadata,
            specific_if_none_match: matches!(input.if_none_match, Some(ETagCondition::ETag(_))),
            object_lock_retain_until_date: input.object_lock_retain_until_date.map(time::OffsetDateTime::from),
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            content_type: input.content_type,
            expires: input.expires,
            tagging: input.tagging,
            version_id: input.version_id,
            server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
            ssekms_key_id: input.ssekms_key_id,
            content_md5: input.content_md5,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
            object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
            storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
            website_redirect_location: input.website_redirect_location,
            checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
            checksum_crc32: input.checksum_crc32,
            checksum_crc32c: input.checksum_crc32c,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            checksum_crc64nvme: input.checksum_crc64nvme,
        }
    }
}

fn put_output_to_legacy(output: PutObjectWriteOutput) -> S3Response<PutObjectOutput> {
    let mut response = S3Response::new(PutObjectOutput {
        e_tag: output.e_tag.map(|value| crate::storage::storage_api::to_s3s_etag(&value)),
        server_side_encryption: output.server_side_encryption.map(ServerSideEncryption::from),
        version_id: output.version_id,
        expiration: output.expiration,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ssekms_key_id: output.ssekms_key_id,
        checksum_crc32: output.checksum_crc32,
        checksum_crc32c: output.checksum_crc32c,
        checksum_sha1: output.checksum_sha1,
        checksum_sha256: output.checksum_sha256,
        checksum_crc64nvme: output.checksum_crc64nvme,
        ..Default::default()
    });
    response.headers = output.headers;
    response.status = output.status;
    response
}

impl DefaultObjectUsecase {
    #[instrument(name = "execute_put_object", level = "info", skip(self, _fs, req))]
    #[hotpath::measure(impl_type = "DefaultObjectUsecase", future = true)]
    pub async fn execute_put_object(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        record_s3_op(S3Operation::PutObject);
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = PutObjectWriteInput::from_legacy(req.input, req.trailing_headers);
        self.execute_put_object_owned(envelope, input)
            .await
            .map(put_output_to_legacy)
            .map_err(Into::into)
    }

    pub async fn execute_put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        record_s3_op(S3Operation::PutObject);
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = PutObjectWriteInput::from_legacy(req.input, req.trailing_headers);
        self.execute_put_object_extract_owned(envelope, input)
            .await
            .map(put_output_to_legacy)
            .map_err(Into::into)
    }
}

impl UploadPartWriteInput {
    pub(crate) fn from_legacy(input: UploadPartInput, trailers: Option<TrailingHeaders>) -> Self {
        Self {
            target: UploadPartTarget::Legacy {
                bucket: input.bucket,
                key: input.key,
                upload_id: input.upload_id,
            },
            part_number: input.part_number,
            content_length: input.content_length,
            body: input.body.map(s3_api::legacy_put_body),
            trailing_headers: crate::app::trailer_adapter::trailer_source(trailers),
            frame_trailers: None,
            content_md5: input.content_md5,
            checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
            checksum_crc32: input.checksum_crc32,
            checksum_crc32c: input.checksum_crc32c,
            checksum_crc64nvme: input.checksum_crc64nvme,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
        }
    }
}

fn upload_part_output_to_legacy(output: UploadPartWriteOutput) -> S3Response<UploadPartOutput> {
    let mut response = S3Response::new(UploadPartOutput {
        e_tag: output.e_tag.map(|tag| crate::storage::storage_api::to_s3s_etag(&tag)),
        checksum_crc32: output.checksum_crc32,
        checksum_crc32c: output.checksum_crc32c,
        checksum_crc64nvme: output.checksum_crc64nvme,
        checksum_sha1: output.checksum_sha1,
        checksum_sha256: output.checksum_sha256,
        server_side_encryption: output.server_side_encryption.map(ServerSideEncryption::from),
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ..Default::default()
    });
    response.headers = output.headers;
    response
}

impl DefaultMultipartUsecase {
    pub async fn execute_upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let input = UploadPartWriteInput::from_legacy(req.input, req.trailing_headers);
        self.execute_upload_part_owned(envelope, input)
            .await
            .map(upload_part_output_to_legacy)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod native_put_tests {
    use super::{PutObjectInput, PutObjectWriteInput, PutObjectWriteOutput, put_output_to_legacy};
    use crate::storage::ecfs::{ObjectLockLegalHoldStatus, ObjectLockMode, ServerSideEncryption, StorageClass};

    #[test]
    fn legacy_projection_defers_invalid_raw_values_to_the_existing_validators() {
        for key in ["", "bad\0key", &"k".repeat(1025)] {
            let input = PutObjectInput {
                bucket: "sthree-native".to_owned(),
                key: key.to_owned(),
                storage_class: Some(StorageClass::from("INVALID".to_owned())),
                server_side_encryption: Some(ServerSideEncryption::from("UNKNOWN".to_owned())),
                object_lock_mode: Some(ObjectLockMode::from("unknown-mode".to_owned())),
                object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from("unknown-hold".to_owned())),
                ..Default::default()
            };
            let owned = PutObjectWriteInput::from_legacy(input, None);
            assert_eq!(owned.bucket, "sthree-native");
            assert_eq!(owned.key, key);
            assert_eq!(owned.storage_class.as_deref(), Some("INVALID"));
            assert_eq!(owned.server_side_encryption.as_deref(), Some("UNKNOWN"));
            assert_eq!(owned.object_lock_mode.as_deref(), Some("unknown-mode"));
            assert_eq!(owned.object_lock_legal_hold_status.as_deref(), Some("unknown-hold"));
        }
    }

    #[test]
    fn legacy_projection_keeps_each_checksum_and_unknown_length() {
        let input = PutObjectInput {
            checksum_crc32: Some("crc32".to_owned()),
            checksum_crc32c: Some("crc32c".to_owned()),
            checksum_sha1: Some("sha1".to_owned()),
            checksum_sha256: Some("sha256".to_owned()),
            checksum_crc64nvme: Some("crc64".to_owned()),
            ..Default::default()
        };
        let owned = PutObjectWriteInput::from_legacy(input, None);
        assert_eq!(owned.content_length, None);
        assert_eq!(owned.checksum_crc32.as_deref(), Some("crc32"));
        assert_eq!(owned.checksum_crc32c.as_deref(), Some("crc32c"));
        assert_eq!(owned.checksum_sha1.as_deref(), Some("sha1"));
        assert_eq!(owned.checksum_sha256.as_deref(), Some("sha256"));
        assert_eq!(owned.checksum_crc64nvme.as_deref(), Some("crc64"));
        assert!(owned.trailing_headers.is_none());
    }

    #[test]
    fn owned_response_projection_preserves_status_headers_and_all_checksums() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-amz-checksum-sha512", http::HeaderValue::from_static("extension"));
        let output = PutObjectWriteOutput {
            e_tag: Some("stored-tag".to_owned()),
            checksum_crc32: Some("crc32".to_owned()),
            checksum_crc32c: Some("crc32c".to_owned()),
            checksum_sha1: Some("sha1".to_owned()),
            checksum_sha256: Some("sha256".to_owned()),
            checksum_crc64nvme: Some("crc64".to_owned()),
            status: Some(http::StatusCode::CREATED),
            headers: headers.clone(),
            ..Default::default()
        };
        assert_eq!(put_output_to_legacy(PutObjectWriteOutput::default()).status, None);
        let response = put_output_to_legacy(output);
        assert_eq!(response.status, Some(http::StatusCode::CREATED));
        assert_eq!(response.headers, headers);
        assert_eq!(response.output.checksum_crc32.as_deref(), Some("crc32"));
        assert_eq!(response.output.checksum_crc32c.as_deref(), Some("crc32c"));
        assert_eq!(response.output.checksum_sha1.as_deref(), Some("sha1"));
        assert_eq!(response.output.checksum_sha256.as_deref(), Some("sha256"));
        assert_eq!(response.output.checksum_crc64nvme.as_deref(), Some("crc64"));
        assert_eq!(response.output.e_tag, Some(crate::storage::storage_api::to_s3s_etag("stored-tag")));
    }
}
