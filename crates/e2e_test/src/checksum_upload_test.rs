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

//! E2E tests for PutObject and MultipartUpload with checksums (Content-MD5, x-amz-checksum-*).
//! Verifies that uploads with Content-MD5 and x-amz-checksum-sha256 succeed and content is correct.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart};
    use base64::Engine;
    use rustfs_rio::{Checksum, ChecksumType as RioChecksumType};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Bucket {} created successfully", bucket);
                Ok(())
            }
            Err(e) => {
                if e.to_string().contains("BucketAlreadyOwnedByYou") || e.to_string().contains("BucketAlreadyExists") {
                    info!("Bucket {} already exists", bucket);
                    Ok(())
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    fn content_md5_base64(body: &[u8]) -> String {
        let digest = md5::compute(body);
        base64::engine::general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn checksum_sha256_base64(body: &[u8]) -> String {
        let digest = Sha256::digest(body);
        base64::engine::general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn checksum_crc64nvme_base64(body: &[u8]) -> String {
        Checksum::new_from_data(RioChecksumType::CRC64_NVME, body)
            .expect("crc64nvme checksum")
            .encoded
    }

    /// PutObject with Content-MD5: upload succeeds and GetObject returns same content.
    #[tokio::test]
    #[serial]
    async fn test_put_object_with_content_md5() {
        init_logging();
        info!("TEST: PutObject with Content-MD5");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-checksum-md5";
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let key = "obj-with-md5.txt";
        let content = b"Hello world with Content-MD5 checksum";
        let content_md5 = content_md5_base64(content);

        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .content_md5(&content_md5)
            .send()
            .await;

        assert!(result.is_ok(), "PutObject with Content-MD5 failed: {:?}", result.err());

        let get_result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(get_result.is_ok(), "GetObject failed: {:?}", get_result.err());
        let body_bytes = get_result.unwrap().body.collect().await.expect("collect body").into_bytes();
        assert_eq!(body_bytes.as_ref(), content, "GetObject body must match uploaded content");
        info!("PASSED: PutObject with Content-MD5 and GetObject content match");
    }

    /// PutObject with x-amz-checksum-sha256: upload succeeds and GetObject returns same content.
    #[tokio::test]
    #[serial]
    async fn test_put_object_with_checksum_sha256() {
        init_logging();
        info!("TEST: PutObject with x-amz-checksum-sha256");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-checksum-sha256";
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let key = "obj-with-sha256.txt";
        let content = b"Hello world with x-amz-checksum-sha256";
        let checksum = checksum_sha256_base64(content);

        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .checksum_sha256(&checksum)
            .send()
            .await;

        assert!(result.is_ok(), "PutObject with checksum_sha256 failed: {:?}", result.err());

        let get_result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(get_result.is_ok(), "GetObject failed: {:?}", get_result.err());
        let body_bytes = get_result.unwrap().body.collect().await.expect("collect body").into_bytes();
        assert_eq!(body_bytes.as_ref(), content, "GetObject body must match uploaded content");
        info!("PASSED: PutObject with checksum_sha256 and GetObject content match");
    }

    /// Multipart upload with checksum: CreateMultipartUpload, UploadPart(s) with checksum_sha256, CompleteMultipartUpload; then GetObject verifies content.
    /// Uses part size >= 5MB (server minimum) for two parts.
    #[tokio::test]
    #[serial]
    async fn test_multipart_upload_with_checksum() {
        init_logging();
        info!("TEST: MultipartUpload with checksum (checksum_sha256 on parts)");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-multipart-checksum";
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let key = "multipart-with-checksum.bin";
        const PART_SIZE: usize = 6 * 1024 * 1024; // 6 MB per part (>= 5MB minimum)
        let part1: Vec<u8> = (0..PART_SIZE).map(|i| (i % 256) as u8).collect();
        let part2: Vec<u8> = (0..PART_SIZE).map(|i| ((i + 1) % 256) as u8).collect();
        let full_content: Vec<u8> = part1.iter().chain(part2.iter()).copied().collect();

        let create_result = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .send()
            .await
            .expect("Failed to create multipart upload");

        let upload_id = create_result.upload_id().expect("No upload_id").to_string();

        let checksum1 = checksum_sha256_base64(&part1);
        let upload1 = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part1.clone()))
            .checksum_sha256(&checksum1)
            .send()
            .await
            .expect("Failed to upload part 1");

        let etag1 = upload1.e_tag().expect("No etag part 1").to_string();
        let checksum_sha256_1 = upload1.checksum_sha256().map(|s| s.to_string());

        let checksum2 = checksum_sha256_base64(&part2);
        let upload2 = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(2)
            .body(ByteStream::from(part2.clone()))
            .checksum_sha256(&checksum2)
            .send()
            .await
            .expect("Failed to upload part 2");

        let etag2 = upload2.e_tag().expect("No etag part 2").to_string();
        let checksum_sha256_2 = upload2.checksum_sha256().map(|s| s.to_string());

        let mut part1_builder = CompletedPart::builder().part_number(1).e_tag(etag1);
        if let Some(ref cs) = checksum_sha256_1 {
            part1_builder = part1_builder.checksum_sha256(cs);
        }
        let mut part2_builder = CompletedPart::builder().part_number(2).e_tag(etag2);
        if let Some(ref cs) = checksum_sha256_2 {
            part2_builder = part2_builder.checksum_sha256(cs);
        }

        let completed_parts = vec![part1_builder.build(), part2_builder.build()];
        let completed_upload = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

        let complete_result = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await;

        assert!(complete_result.is_ok(), "CompleteMultipartUpload failed: {:?}", complete_result.err());

        let get_result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(get_result.is_ok(), "GetObject failed: {:?}", get_result.err());
        let body_bytes = get_result.unwrap().body.collect().await.expect("collect body").into_bytes();
        assert_eq!(
            body_bytes.as_ref(),
            full_content.as_slice(),
            "GetObject body must match concatenated parts"
        );
        info!("PASSED: MultipartUpload with checksum and GetObject content match");
    }

    /// Regression test for issue #2282:
    /// CRC64NVME full-object checksum should match between direct PutObject and multipart upload.
    #[tokio::test]
    #[serial]
    async fn test_crc64nvme_matches_between_put_object_and_multipart_upload() {
        init_logging();
        info!("TEST: CRC64NVME matches between direct PutObject and multipart upload");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-crc64nvme-multipart-match";
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        const PART_SIZE: usize = 6 * 1024 * 1024;
        let part1: Vec<u8> = (0..PART_SIZE).map(|i| (i % 251) as u8).collect();
        let part2: Vec<u8> = (0..PART_SIZE).map(|i| ((i + 17) % 251) as u8).collect();
        let content: Vec<u8> = part1.iter().chain(part2.iter()).copied().collect();

        let direct_key = "crc64nvme-direct.bin";
        let multipart_key = "crc64nvme-multipart.bin";
        let full_checksum = checksum_crc64nvme_base64(&content);
        let part1_checksum = checksum_crc64nvme_base64(&part1);
        let part2_checksum = checksum_crc64nvme_base64(&part2);

        client
            .put_object()
            .bucket(bucket)
            .key(direct_key)
            .body(ByteStream::from(content.clone()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme(full_checksum.clone())
            .send()
            .await
            .expect("Failed to put direct object with CRC64NVME");

        let create_result = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(multipart_key)
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .send()
            .await
            .expect("Failed to create multipart upload");

        let upload_id = create_result.upload_id().expect("No upload_id").to_string();

        let upload1 = client
            .upload_part()
            .bucket(bucket)
            .key(multipart_key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part1.clone()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme(part1_checksum)
            .send()
            .await
            .expect("Failed to upload multipart part 1");

        let upload2 = client
            .upload_part()
            .bucket(bucket)
            .key(multipart_key)
            .upload_id(&upload_id)
            .part_number(2)
            .body(ByteStream::from(part2.clone()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme(part2_checksum)
            .send()
            .await
            .expect("Failed to upload multipart part 2");

        let completed_upload = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(upload1.e_tag().expect("No etag for part 1"))
                    .checksum_crc64_nvme(upload1.checksum_crc64_nvme().expect("No CRC64NVME for part 1"))
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .e_tag(upload2.e_tag().expect("No etag for part 2"))
                    .checksum_crc64_nvme(upload2.checksum_crc64_nvme().expect("No CRC64NVME for part 2"))
                    .build(),
            )
            .build();

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(multipart_key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .expect("Failed to complete multipart upload");

        let direct_head = client
            .head_object()
            .bucket(bucket)
            .key(direct_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("Failed to head direct object");

        let multipart_head = client
            .head_object()
            .bucket(bucket)
            .key(multipart_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("Failed to head multipart object");

        assert_eq!(
            direct_head.checksum_crc64_nvme(),
            Some(full_checksum.as_str()),
            "Direct object should report the uploaded full-object CRC64NVME"
        );
        assert_eq!(
            multipart_head.checksum_crc64_nvme(),
            Some(full_checksum.as_str()),
            "Multipart object should report the same full-object CRC64NVME as direct upload"
        );
    }
}
