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

//! ListMultipartUploads and ListParts.

use super::*;
use crate::app::RequestEnvelope;
use crate::app::dto_values;
use crate::app::storage_api::multipart_usecase::contract::multipart::ListMultipartsInfo;
use crate::app::storage_api::s3::dto::{ListPartsInput, ListPartsOutput};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use rustfs_gateway_types::dto as gateway_dto;

impl DefaultMultipartUsecase {
    /// One page of ListMultipartUploads: the uploads of the bucket's current incarnation.
    ///
    /// # Errors
    ///
    /// `InvalidRequest` for an upload capability query on this operation; `InvalidArgument` for a
    /// key marker outside the prefix or a page size outside `1..=1000`; `InternalError` when the
    /// bucket generation captured at authorization does not match or the store is not
    /// initialized; otherwise the store's error.
    pub async fn execute_list_multipart_uploads(
        &self,
        envelope: RequestEnvelope,
        input: gateway_dto::ListMultipartUploadsInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::ListMultipartUploadsOutput> {
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            envelope.headers(),
            envelope.uri().query(),
            envelope.verified_sigv4().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            envelope.headers(),
            envelope.uri().query(),
            envelope.verified_presigned().is_some(),
        )?;
        let bucket = input.bucket.as_str();
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&envelope, bucket, &mut opts)?;
        let ListMultipartUploadsParams {
            prefix,
            key_marker,
            max_uploads,
        } = parse_list_multipart_uploads_params(input.prefix, input.key_marker, input.max_uploads)?;
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        // `apply_bucket_generation_guard` tolerates a missing guard (only the access phase
        // installs one), so resolve the current generation rather than failing the request.
        // Listing is filtered by this value, so a stale one simply hides foreign-incarnation
        // uploads, as intended.
        let expected_incarnation_id = match opts.expected_bucket_incarnation_id {
            Some(incarnation_id) => incarnation_id,
            None => store.bucket_incarnation_id_from_disk(bucket).await.map_err(ApiError::from)?,
        };

        let result = match envelope.table_list_access() {
            Some(access) => {
                table_list_isolation::list_multipart_uploads(
                    store.clone(),
                    access,
                    table_list_isolation::ListMultipartUploadsRequest {
                        bucket,
                        prefix: &prefix,
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: input.upload_id_marker.as_deref(),
                        delimiter: input.delimiter.as_deref(),
                        max_uploads,
                        expected_incarnation_id,
                    },
                )
                .await?
            }
            None => store
                .list_multipart_uploads_for_bucket_incarnation(
                    bucket,
                    &prefix,
                    key_marker,
                    input.upload_id_marker,
                    input.delimiter,
                    max_uploads,
                    expected_incarnation_id,
                )
                .await
                .map_err(ApiError::from)?,
        };

        list_multipart_uploads_output(bucket, prefix, result, input.encoding_type)
    }

    pub async fn execute_list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedSigV4Request>().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedPresignedRequest>().is_some(),
        )?;
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        let params = parse_list_parts_params(part_number_marker, max_parts)?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let res = store
            .list_object_parts(&bucket, &key, &upload_id, params.part_number_marker, params.max_parts, &opts)
            .await
            .map_err(multipart_upload_api_error)?;

        Ok(S3Response::new(build_list_parts_output(res)))
    }
}

/// The ListMultipartUploads answer for one page the store returned, raw: `encoding-type=url` is
/// the writing edge's.
///
/// # Errors
///
/// `InternalError` for a stored bucket name or key the answer cannot carry.
fn list_multipart_uploads_output(
    bucket: &str,
    prefix: String,
    result: ListMultipartsInfo,
    encoding_type: Option<gateway_dto::EncodingType>,
) -> rustfs_s3_types::S3Result<gateway_dto::ListMultipartUploadsOutput> {
    let owner = dto_values::owner();
    let initiator = gateway_dto::Initiator {
        id: owner.id.clone(),
        display_name: owner.display_name.clone(),
    };
    let uploads = result
        .uploads
        .into_iter()
        .map(|upload| {
            Ok(gateway_dto::MultipartUpload {
                key: Some(dto_values::stored_object_key(&upload.object)?),
                upload_id: Some(upload.upload_id),
                initiated: upload.initiated.map(dto_values::timestamp),
                owner: Some(owner.clone()),
                initiator: Some(initiator.clone()),
                ..Default::default()
            })
        })
        .collect::<rustfs_s3_types::S3Result<_>>()?;
    Ok(gateway_dto::ListMultipartUploadsOutput {
        bucket: dto_values::stored_bucket_name(bucket)?,
        prefix: Some(prefix),
        delimiter: result.delimiter,
        key_marker: result.key_marker,
        upload_id_marker: result.upload_id_marker,
        next_key_marker: result.next_key_marker,
        next_upload_id_marker: result.next_upload_id_marker,
        // A page holds at most 1000 uploads, which `parse_list_multipart_uploads_params` enforces.
        max_uploads: i32::try_from(result.max_uploads).unwrap_or(i32::MAX),
        is_truncated: result.is_truncated,
        uploads,
        common_prefixes: result
            .common_prefixes
            .into_iter()
            .map(|prefix| gateway_dto::CommonPrefix { prefix })
            .collect(),
        encoding_type,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::app::storage_api::multipart_usecase::contract::multipart::MultipartInfo;

    fn uploads_input(
        prefix: Option<&str>,
        key_marker: Option<&str>,
        max_uploads: Option<i32>,
    ) -> gateway_dto::ListMultipartUploadsInput {
        gateway_dto::ListMultipartUploadsInput {
            bucket: rustfs_gateway_types::BucketName::new("bucket").expect("bucket"),
            prefix: prefix.map(str::to_owned),
            key_marker: key_marker.map(str::to_owned),
            max_uploads,
            ..Default::default()
        }
    }

    async fn list_uploads(input: gateway_dto::ListMultipartUploadsInput) -> rustfs_s3_types::S3Error {
        make_usecase()
            .execute_list_multipart_uploads(RequestEnvelope::from_s3s(&build_request((), Method::GET)), input)
            .await
            .expect_err("refused")
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_returns_internal_error_when_store_uninitialized() {
        let err = list_uploads(uploads_input(None, None, None)).await;
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_key_marker_before_store_lookup() {
        let err = list_uploads(uploads_input(Some("prefix/"), Some("other/key"), None)).await;
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid key marker"));
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_max_uploads_before_store_lookup() {
        let err = list_uploads(uploads_input(None, None, Some(0))).await;
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("max-uploads must be between 1 and 1000"));
    }

    /// The answer carries stored values raw; `encoding-type=url` is the writing edge's.
    #[test]
    fn list_multipart_uploads_output_maps_uploads_and_prefixes_raw() {
        let raw = "dir a/file+b%\u{e9}";
        let result = ListMultipartsInfo {
            delimiter: Some("/".to_owned()),
            key_marker: Some(raw.to_owned()),
            upload_id_marker: Some("upload+id%".to_owned()),
            next_key_marker: Some(raw.to_owned()),
            next_upload_id_marker: Some("next+id%".to_owned()),
            max_uploads: 1000,
            is_truncated: true,
            uploads: vec![MultipartInfo {
                object: raw.to_owned(),
                upload_id: "upload+id%".to_owned(),
                initiated: Some(time::OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            }],
            common_prefixes: vec!["prefix-a/".to_owned(), raw.to_owned()],
            ..Default::default()
        };

        let output = list_multipart_uploads_output("bucket-a", raw.to_owned(), result, Some(gateway_dto::EncodingType::URL))
            .expect("a representable page");

        assert_eq!(output.bucket.as_str(), "bucket-a");
        assert_eq!(output.prefix.as_deref(), Some(raw));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.key_marker.as_deref(), Some(raw));
        assert_eq!(output.next_key_marker.as_deref(), Some(raw));
        assert_eq!(output.upload_id_marker.as_deref(), Some("upload+id%"));
        assert_eq!(output.next_upload_id_marker.as_deref(), Some("next+id%"));
        assert_eq!(output.max_uploads, 1000);
        assert!(output.is_truncated);
        assert_eq!(output.encoding_type.as_ref().map(|encoding| encoding.as_str()), Some("url"));
        let upload = &output.uploads[0];
        assert_eq!(upload.key.as_ref().map(rustfs_gateway_types::ObjectKey::as_str), Some(raw));
        assert_eq!(upload.upload_id.as_deref(), Some("upload+id%"));
        assert_eq!(upload.initiated, Some(dto_values::timestamp(time::OffsetDateTime::UNIX_EPOCH)));
        assert_eq!(
            upload.owner.as_ref().and_then(|owner| owner.id.as_deref()),
            dto_values::owner().id.as_deref()
        );
        assert_eq!(
            upload.initiator.as_ref().and_then(|initiator| initiator.id.as_deref()),
            dto_values::owner().id.as_deref()
        );
        let prefixes: Vec<_> = output.common_prefixes.iter().map(|prefix| prefix.prefix.as_str()).collect();
        assert_eq!(prefixes, ["prefix-a/", raw]);
    }

    #[test]
    fn list_multipart_uploads_output_keeps_absent_markers_absent() {
        let output = list_multipart_uploads_output("bucket-a", String::new(), ListMultipartsInfo::default(), None)
            .expect("a representable page");

        assert_eq!(output.prefix.as_deref(), Some(""));
        assert_eq!((output.delimiter, output.key_marker, output.next_key_marker), (None, None, None));
        assert!(output.uploads.is_empty());
        assert!(output.common_prefixes.is_empty());
    }

    #[tokio::test]
    async fn execute_list_parts_returns_internal_error_when_store_uninitialized() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_parts_rejects_negative_part_number_marker_before_store_lookup() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .part_number_marker(Some(-1))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("part-number-marker must be non-negative"));
    }

    #[tokio::test]
    async fn execute_list_parts_rejects_invalid_max_parts_before_store_lookup() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .max_parts(Some(1001))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("max-parts must be between 1 and 1000"));
    }
}
