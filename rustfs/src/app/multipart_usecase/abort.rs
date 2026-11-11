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

//! AbortMultipartUpload.

use super::*;

impl DefaultMultipartUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
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
        record_s3_op(S3Operation::AbortMultipartUpload);
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let AbortMultipartUploadInput {
            bucket, key, upload_id, ..
        } = req.input;

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // Per the AWS S3 API reference, a malformed upload ID is reported as
        // NoSuchUpload (404), the same as an upload that was aborted or
        // completed. See multipart_upload_api_error.
        match store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), &opts)
            .await
        {
            Ok(_) => {
                rustfs_scanner::record_dirty_usage_object_from_producer(
                    &bucket,
                    &key,
                    rustfs_scanner::SegmentInvalidationProducerIdentity::AbortMultipartUpload,
                );
                Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
            }
            Err(err) => Err(multipart_upload_api_error(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn execute_abort_multipart_upload_returns_internal_error_when_store_uninitialized() {
        let input = AbortMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::DELETE);
        let before = s3_op_total(S3Operation::AbortMultipartUpload);

        let err = make_usecase().execute_abort_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::AbortMultipartUpload), before + 1);
    }
}
