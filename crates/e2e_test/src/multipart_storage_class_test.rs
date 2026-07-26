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

//! CreateMultipartUpload storage-class persistence regression tests.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, StorageClass};

    const PART_SIZE: usize = 5 * 1024 * 1024;

    async fn assert_completed_object(
        client: &Client,
        bucket: &str,
        key: &str,
        expected_storage_class: &str,
        expected_body: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        let expected_head_class = (expected_storage_class != "STANDARD").then_some(expected_storage_class);
        assert_eq!(
            head.storage_class().map(StorageClass::as_str),
            expected_head_class,
            "HeadObject should use S3's implicit STANDARD representation"
        );

        let listed = client.list_objects_v2().bucket(bucket).prefix(key).send().await?;
        let object = listed
            .contents()
            .iter()
            .find(|object| object.key() == Some(key))
            .ok_or("completed multipart object missing from ListObjectsV2")?;
        assert_eq!(
            object.storage_class().map(|storage_class| storage_class.as_str()),
            Some(expected_storage_class),
            "ListObjectsV2 should report the completed object's storage class"
        );

        let body = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert_eq!(body.as_ref(), expected_body, "completed multipart body should be byte-exact");
        Ok(())
    }

    #[tokio::test]
    async fn multipart_upload_preserves_standard_and_rrs_across_retry_and_resume()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "multipart-storage-class-retry";
        env.create_test_bucket(bucket).await?;

        for storage_class in [StorageClass::Standard, StorageClass::ReducedRedundancy] {
            let class_name = storage_class.as_str();
            let key = format!("retry-{class_name}.bin");
            let create = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .storage_class(storage_class.clone())
                .content_type("application/octet-stream")
                .metadata("content-type", "user-content-type")
                .metadata("x-amz-storage-class", "user-storage-class")
                .send()
                .await?;
            let upload_id = create.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;

            let original_part = vec![b'a'; PART_SIZE];
            let first_attempt = client
                .upload_part()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(original_part))
                .send()
                .await?;

            let resumed_client = env.create_s3_client();
            let before_retry = resumed_client
                .list_parts()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await?;
            assert_eq!(before_retry.storage_class().map(StorageClass::as_str), Some(class_name));
            assert_eq!(before_retry.parts().len(), 1, "resume should find the previously uploaded part");

            let retried_part = vec![b'b'; PART_SIZE];
            let retry = resumed_client
                .upload_part()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(retried_part.clone()))
                .send()
                .await?;
            assert_ne!(
                first_attempt.e_tag(),
                retry.e_tag(),
                "retrying the same part number with different bytes should replace the part"
            );

            let tail = format!("-tail-{class_name}").into_bytes();
            let second = resumed_client
                .upload_part()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(2)
                .body(ByteStream::from(tail.clone()))
                .send()
                .await?;
            let after_retry = resumed_client
                .list_parts()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await?;
            assert_eq!(after_retry.storage_class().map(StorageClass::as_str), Some(class_name));
            assert_eq!(after_retry.parts().len(), 2);
            assert_eq!(after_retry.parts()[0].e_tag(), retry.e_tag());

            resumed_client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .part_number(1)
                                .set_e_tag(retry.e_tag().map(str::to_owned))
                                .build(),
                        )
                        .parts(
                            CompletedPart::builder()
                                .part_number(2)
                                .set_e_tag(second.e_tag().map(str::to_owned))
                                .build(),
                        )
                        .build(),
                )
                .send()
                .await?;

            let mut expected_body = retried_part;
            expected_body.extend_from_slice(&tail);
            assert_completed_object(&resumed_client, bucket, &key, class_name, &expected_body).await?;
            let metadata_head = resumed_client.head_object().bucket(bucket).key(&key).send().await?;
            assert_eq!(metadata_head.content_type(), Some("application/octet-stream"));
            assert_eq!(
                metadata_head.metadata().and_then(|metadata| metadata.get("content-type")),
                Some(&"user-content-type".to_string())
            );
            assert_eq!(
                metadata_head
                    .metadata()
                    .and_then(|metadata| metadata.get("x-amz-storage-class")),
                Some(&"user-storage-class".to_string())
            );
        }

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn multipart_copy_preserves_standard_and_rrs() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "multipart-storage-class-copy";
        let source_key = "source.bin";
        let source_body = vec![b'c'; 1024 * 1024];
        env.create_test_bucket(bucket).await?;
        client
            .put_object()
            .bucket(bucket)
            .key(source_key)
            .body(ByteStream::from(source_body.clone()))
            .send()
            .await?;

        for storage_class in [StorageClass::Standard, StorageClass::ReducedRedundancy] {
            let class_name = storage_class.as_str();
            let key = format!("copy-{class_name}.bin");
            let create = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .storage_class(storage_class.clone())
                .send()
                .await?;
            let upload_id = create.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;
            let copied = client
                .upload_part_copy()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .copy_source(format!("{bucket}/{source_key}"))
                .send()
                .await?;
            let e_tag = copied
                .copy_part_result()
                .and_then(|result| result.e_tag())
                .ok_or("UploadPartCopy returned no ETag")?;

            let parts = client
                .list_parts()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await?;
            assert_eq!(parts.storage_class().map(StorageClass::as_str), Some(class_name));
            assert_eq!(parts.parts().len(), 1);

            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(CompletedPart::builder().part_number(1).e_tag(e_tag).build())
                        .build(),
                )
                .send()
                .await?;
            assert_completed_object(&client, bucket, &key, class_name, &source_body).await?;
        }

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn invalid_and_aborted_uploads_leave_no_session_or_object() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "multipart-storage-class-errors";
        let invalid_key = "invalid.bin";
        let aborted_key = "aborted.bin";
        env.create_test_bucket(bucket).await?;

        let invalid = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(invalid_key)
            .storage_class(StorageClass::from("INVALID"))
            .send()
            .await
            .expect_err("invalid storage class should be rejected");
        assert_eq!(
            invalid.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("InvalidStorageClass")
        );
        let after_invalid = client
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix(invalid_key)
            .send()
            .await?;
        assert!(
            after_invalid.uploads().is_empty(),
            "validation failure must not create a multipart session"
        );

        let create = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(aborted_key)
            .storage_class(StorageClass::ReducedRedundancy)
            .send()
            .await?;
        let upload_id = create.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;
        client
            .upload_part()
            .bucket(bucket)
            .key(aborted_key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"aborted multipart part"))
            .send()
            .await?;
        let before_abort = client
            .list_parts()
            .bucket(bucket)
            .key(aborted_key)
            .upload_id(upload_id)
            .send()
            .await?;
        assert_eq!(before_abort.storage_class().map(StorageClass::as_str), Some("REDUCED_REDUNDANCY"));

        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(aborted_key)
            .upload_id(upload_id)
            .send()
            .await?;
        let after_abort = client
            .list_parts()
            .bucket(bucket)
            .key(aborted_key)
            .upload_id(upload_id)
            .send()
            .await
            .expect_err("aborted upload should not be resumable");
        assert_eq!(after_abort.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchUpload"));
        let remaining_uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix(aborted_key)
            .send()
            .await?;
        assert!(remaining_uploads.uploads().is_empty(), "abort should remove the multipart session");
        let aborted_head = client
            .head_object()
            .bucket(bucket)
            .key(aborted_key)
            .send()
            .await
            .expect_err("aborted upload should not create an object");
        assert_eq!(aborted_head.raw_response().map(|response| response.status().as_u16()), Some(404));

        env.stop_server();
        Ok(())
    }
}
