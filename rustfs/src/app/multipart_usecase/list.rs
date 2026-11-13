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

impl DefaultMultipartUsecase {
    pub async fn execute_list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let table_list_access = req.extensions.get::<TableDataPlaneListAccess>().cloned();
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
        let ListMultipartUploadsInput {
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
            encoding_type,
            ..
        } = req.input;

        let ListMultipartUploadsParams {
            prefix,
            key_marker,
            max_uploads,
        } = parse_list_multipart_uploads_params(prefix, key_marker, max_uploads)?;
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // `apply_bucket_generation_guard` tolerates a missing guard (only the S3
        // access layer installs one), so resolve the current generation rather
        // than failing the request. Listing is filtered by this value, so a
        // stale one simply hides foreign-incarnation uploads, as intended.
        let expected_incarnation_id = match opts.expected_bucket_incarnation_id {
            Some(incarnation_id) => incarnation_id,
            None => store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?,
        };

        let result = match table_list_access.as_ref() {
            Some(access) => {
                table_list_isolation::list_multipart_uploads(
                    store.clone(),
                    access,
                    table_list_isolation::ListMultipartUploadsRequest {
                        bucket: &bucket,
                        prefix: &prefix,
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: upload_id_marker.as_deref(),
                        delimiter: delimiter.as_deref(),
                        max_uploads,
                        expected_incarnation_id,
                    },
                )
                .await?
            }
            None => store
                .list_multipart_uploads_for_bucket_incarnation(
                    &bucket,
                    &prefix,
                    key_marker,
                    upload_id_marker,
                    delimiter,
                    max_uploads,
                    expected_incarnation_id,
                )
                .await
                .map_err(ApiError::from)?,
        };

        Ok(S3Response::new(build_list_multipart_uploads_output(
            bucket,
            prefix,
            result,
            encoding_type.as_ref(),
        )))
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

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn execute_list_multipart_uploads_returns_internal_error_when_store_uninitialized() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_key_marker_before_store_lookup() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .prefix(Some("prefix/".to_string()))
            .key_marker(Some("other/key".to_string()))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid key marker"));
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_max_uploads_before_store_lookup() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .max_uploads(Some(0))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);
        let expected = "max-uploads must be between 1 and 1000";

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some(expected));
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
