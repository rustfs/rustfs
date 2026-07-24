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

//! CopyObject checksum compatibility tests. Covers all supported algorithms,
//! source-checksum preservation, explicit override, and fail-closed handling of
//! unsupported algorithms before destination mutation.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{
        BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
        VersioningConfiguration,
    };
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use rustfs_rio::{Checksum, ChecksumType as RioChecksumType};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use tracing::info;

    async fn create_versioned_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .expect("Failed to enable versioning");
    }

    fn create_s3_client_no_auto_checksum(env: &RustFSTestEnvironment) -> aws_sdk_s3::Client {
        let credentials = Credentials::new(&env.access_key, &env.secret_key, None, None, "copy-checksum-e2e");
        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{}", env.address))
            .force_path_style(true)
            .behavior_version_latest()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .http_client(SmithyHttpClientBuilder::new().build_http())
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    fn algorithms() -> [(ChecksumAlgorithm, RioChecksumType); 10] {
        [
            (ChecksumAlgorithm::Crc32, RioChecksumType::CRC32),
            (ChecksumAlgorithm::Crc32C, RioChecksumType::CRC32C),
            (ChecksumAlgorithm::Crc64Nvme, RioChecksumType::CRC64_NVME),
            (ChecksumAlgorithm::Sha1, RioChecksumType::SHA1),
            (ChecksumAlgorithm::Sha256, RioChecksumType::SHA256),
            (ChecksumAlgorithm::Md5, RioChecksumType::MD5),
            (ChecksumAlgorithm::Sha512, RioChecksumType::SHA512),
            (ChecksumAlgorithm::Xxhash3, RioChecksumType::XXHASH3),
            (ChecksumAlgorithm::Xxhash64, RioChecksumType::XXHASH64),
            (ChecksumAlgorithm::Xxhash128, RioChecksumType::XXHASH128),
        ]
    }

    fn result_checksums(result: &aws_sdk_s3::types::CopyObjectResult) -> [Option<&str>; 10] {
        [
            result.checksum_crc32(),
            result.checksum_crc32_c(),
            result.checksum_crc64_nvme(),
            result.checksum_sha1(),
            result.checksum_sha256(),
            result.checksum_md5(),
            result.checksum_sha512(),
            result.checksum_xxhash3(),
            result.checksum_xxhash64(),
            result.checksum_xxhash128(),
        ]
    }

    fn head_checksums(output: &aws_sdk_s3::operation::head_object::HeadObjectOutput) -> [Option<&str>; 10] {
        [
            output.checksum_crc32(),
            output.checksum_crc32_c(),
            output.checksum_crc64_nvme(),
            output.checksum_sha1(),
            output.checksum_sha256(),
            output.checksum_md5(),
            output.checksum_sha512(),
            output.checksum_xxhash3(),
            output.checksum_xxhash64(),
            output.checksum_xxhash128(),
        ]
    }

    #[tokio::test]
    #[serial]
    async fn test_copy_supports_all_checksum_algorithms() {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client_no_auto_checksum(&env);
        let src_bucket = "copy-all-checksums-src";
        let dst_bucket = "copy-all-checksums-dst";
        let src_key = "objects/source.bin";
        let content = b"deterministic CopyObject payload for all ten checksum algorithms";

        create_versioned_bucket(&client, src_bucket).await;
        create_versioned_bucket(&client, dst_bucket).await;
        client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT source failed");

        for (index, (sdk_algorithm, rio_algorithm)) in algorithms().into_iter().enumerate() {
            let expected = Checksum::new_from_data(rio_algorithm, content)
                .expect("supported checksum must be computable")
                .encoded;
            let dst_key = format!("objects/destination-{index}.bin");
            let copy = client
                .copy_object()
                .bucket(dst_bucket)
                .key(&dst_key)
                .copy_source(format!("{src_bucket}/{src_key}"))
                .checksum_algorithm(sdk_algorithm)
                .send()
                .await
                .expect("CopyObject with supported checksum must succeed");
            let result = copy.copy_object_result().expect("CopyObject result");
            let checksums = result_checksums(result);
            assert_eq!(checksums[index], Some(expected.as_str()), "{rio_algorithm}: response checksum");
            assert_eq!(
                checksums.iter().filter(|checksum| checksum.is_some()).count(),
                1,
                "{rio_algorithm}: only the requested checksum may be returned"
            );

            let head = client
                .head_object()
                .bucket(dst_bucket)
                .key(&dst_key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .expect("HEAD destination failed");
            let checksums = head_checksums(&head);
            assert_eq!(checksums[index], Some(expected.as_str()), "{rio_algorithm}: persisted checksum");
            assert_eq!(
                checksums.iter().filter(|checksum| checksum.is_some()).count(),
                1,
                "{rio_algorithm}: destination must persist only the requested checksum"
            );

            let body = client
                .get_object()
                .bucket(dst_bucket)
                .key(&dst_key)
                .send()
                .await
                .expect("GET destination failed")
                .body
                .collect()
                .await
                .expect("collect destination body")
                .into_bytes();
            assert_eq!(body.as_ref(), content, "{rio_algorithm}: full copied body");
        }

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn test_copy_without_algorithm_preserves_every_supported_source_checksum() {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client_no_auto_checksum(&env);
        let src_bucket = "copy-preserve-all-src";
        let dst_bucket = "copy-preserve-all-dst";
        let content = b"source checksum preservation payload for all ten algorithms";

        create_versioned_bucket(&client, src_bucket).await;
        create_versioned_bucket(&client, dst_bucket).await;

        for (index, (_sdk_algorithm, rio_algorithm)) in algorithms().into_iter().enumerate() {
            let expected = Checksum::new_from_data(rio_algorithm, content)
                .expect("supported checksum must be computable")
                .encoded;
            let checksum_header = rio_algorithm.key().expect("supported checksum header");
            let request_checksum = expected.clone();
            let src_key = format!("objects/source-{index}.bin");
            let dst_key = format!("objects/destination-{index}.bin");
            client
                .put_object()
                .bucket(src_bucket)
                .key(&src_key)
                .body(ByteStream::from_static(content))
                .customize()
                .mutate_request(move |request| {
                    request.headers_mut().insert(checksum_header, request_checksum.clone());
                })
                .send()
                .await
                .expect("PUT checksummed source failed");

            let copy = client
                .copy_object()
                .bucket(dst_bucket)
                .key(&dst_key)
                .copy_source(format!("{src_bucket}/{src_key}"))
                .send()
                .await
                .expect("CopyObject without algorithm must succeed");
            let result = copy.copy_object_result().expect("CopyObject result");
            let checksums = result_checksums(result);
            assert_eq!(checksums[index], Some(expected.as_str()), "{rio_algorithm}: preserved response checksum");
            assert_eq!(checksums.iter().filter(|checksum| checksum.is_some()).count(), 1);

            let head = client
                .head_object()
                .bucket(dst_bucket)
                .key(&dst_key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .expect("HEAD destination failed");
            let checksums = head_checksums(&head);
            assert_eq!(checksums[index], Some(expected.as_str()), "{rio_algorithm}: preserved stored checksum");
            assert_eq!(checksums.iter().filter(|checksum| checksum.is_some()).count(), 1);
        }

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn test_copy_without_algorithm_preserves_composite_checksum_type() {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client_no_auto_checksum(&env);
        let bucket = "copy-preserve-composite";
        let source_key = "objects/multipart-source.bin";
        let destination_key = "objects/copied-multipart.bin";
        let content = b"multipart source checksum must remain composite";

        create_versioned_bucket(&client, bucket).await;
        let created = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(source_key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .expect("CreateMultipartUpload failed");
        let upload_id = created.upload_id().expect("multipart upload ID");
        let checksum = Checksum::new_from_data(RioChecksumType::SHA256, content)
            .expect("SHA256 checksum")
            .encoded;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(source_key)
            .upload_id(upload_id)
            .part_number(1)
            .checksum_sha256(&checksum)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("UploadPart failed");
        let completed_part = CompletedPart::builder()
            .part_number(1)
            .e_tag(uploaded.e_tag().expect("part ETag"))
            .checksum_sha256(uploaded.checksum_sha256().expect("part checksum"))
            .build();
        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(source_key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().parts(completed_part).build())
            .send()
            .await
            .expect("CompleteMultipartUpload failed");

        let source_head = client
            .head_object()
            .bucket(bucket)
            .key(source_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD multipart source failed");
        let source_checksum = source_head.checksum_sha256().expect("multipart source checksum");
        assert_eq!(source_head.checksum_type(), Some(&ChecksumType::Composite));

        let copied = client
            .copy_object()
            .bucket(bucket)
            .key(destination_key)
            .copy_source(format!("{bucket}/{source_key}"))
            .send()
            .await
            .expect("CopyObject without algorithm failed");
        let result = copied.copy_object_result().expect("CopyObject result");
        assert_eq!(result.checksum_sha256(), Some(source_checksum));
        assert_eq!(result.checksum_type(), Some(&ChecksumType::Composite));

        let destination_head = client
            .head_object()
            .bucket(bucket)
            .key(destination_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD copied multipart object failed");
        assert_eq!(destination_head.checksum_sha256(), Some(source_checksum));
        assert_eq!(destination_head.checksum_type(), Some(&ChecksumType::Composite));

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn test_copy_rejects_unknown_algorithm_without_destination_mutation() {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "copy-reject-unknown-checksum";
        let src_key = "objects/source.bin";
        let dst_key = "objects/destination.bin";
        let source = b"source must never replace destination";
        let destination = b"pre-existing destination must remain byte-for-byte unchanged";
        let expected = Checksum::new_from_data(RioChecksumType::SHA256, destination)
            .expect("SHA256 checksum")
            .encoded;

        create_versioned_bucket(&client, bucket).await;
        client
            .put_object()
            .bucket(bucket)
            .key(src_key)
            .body(ByteStream::from_static(source))
            .send()
            .await
            .expect("PUT source failed");
        let original = client
            .put_object()
            .bucket(bucket)
            .key(dst_key)
            .metadata("state", "original")
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from_static(destination))
            .send()
            .await
            .expect("PUT destination failed");
        let original_version = original.version_id().expect("versioned PUT must return a version id");

        let missing_source_error = client
            .copy_object()
            .bucket(bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/objects/missing-source.bin"))
            .checksum_algorithm(ChecksumAlgorithm::from("BLAKE3"))
            .send()
            .await
            .expect_err("checksum validation must precede source lookup");
        assert_eq!(
            missing_source_error.as_service_error().and_then(|value| value.code()),
            Some("InvalidArgument")
        );
        assert_eq!(missing_source_error.raw_response().map(|response| response.status().as_u16()), Some(400));

        let error = client
            .copy_object()
            .bucket(bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .checksum_algorithm(ChecksumAlgorithm::from("BLAKE3"))
            .send()
            .await
            .expect_err("unsupported checksum algorithm must fail");
        assert_eq!(error.as_service_error().and_then(|value| value.code()), Some("InvalidArgument"));
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(400));

        let head = client
            .head_object()
            .bucket(bucket)
            .key(dst_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD unchanged destination");
        assert_eq!(head.version_id(), Some(original_version));
        assert_eq!(
            head.metadata().and_then(|metadata| metadata.get("state").map(String::as_str)),
            Some("original")
        );
        assert_eq!(head.checksum_sha256(), Some(expected.as_str()));

        let body = client
            .get_object()
            .bucket(bucket)
            .key(dst_key)
            .send()
            .await
            .expect("GET unchanged destination")
            .body
            .collect()
            .await
            .expect("collect unchanged destination")
            .into_bytes();
        assert_eq!(body.as_ref(), destination);

        env.stop_server();
    }

    /// Requested algorithm: a CopyObject asking for SHA256 must compute it over the copied
    /// bytes, return it in `CopyObjectResult.ChecksumSHA256`, and persist it so a checksum-mode
    /// HEAD on the destination returns the identical value.
    #[tokio::test]
    #[serial]
    async fn test_copy_with_checksum_algorithm_returns_and_persists_sha256() {
        init_logging();
        info!("Issue #4996: CopyObject with ChecksumAlgorithm=SHA256 must return and persist the checksum");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let src_bucket = "copy-checksum-req-src";
        let dst_bucket = "copy-checksum-req-dst";
        let src_key = "objects/source.bin";
        let dst_key = "objects/dest.bin";

        create_versioned_bucket(&client, src_bucket).await;
        create_versioned_bucket(&client, dst_bucket).await;

        let content = b"deterministic synthetic payload for copy-object checksum #4996";
        let expected_sha256 = BASE64.encode(Sha256::digest(content));

        client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT source failed");

        let copy_out = client
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(format!("{src_bucket}/{src_key}"))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .expect("CopyObject with ChecksumAlgorithm must succeed");

        // (3) The response must carry the freshly computed SHA-256 of the copied bytes.
        let result = copy_out
            .copy_object_result()
            .expect("issue #4996: CopyObject must return a CopyObjectResult");
        assert_eq!(
            result.checksum_sha256(),
            Some(expected_sha256.as_str()),
            "issue #4996: CopyObjectResult.ChecksumSHA256 must equal the SHA-256 of the copied bytes"
        );

        // (4) A checksum-mode HEAD on the destination must return the same SHA-256.
        let head = client
            .head_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD destination failed");
        assert_eq!(
            head.checksum_sha256(),
            Some(expected_sha256.as_str()),
            "issue #4996: destination checksum-mode HEAD must return the same SHA-256 the copy reported"
        );

        env.stop_server();
    }

    /// No algorithm requested: when the source object already carries a checksum, the copy must
    /// preserve it on the destination (AWS default), visible via a checksum-mode HEAD.
    #[tokio::test]
    #[serial]
    async fn test_copy_without_algorithm_preserves_source_checksum() {
        init_logging();
        info!("Issue #4996: CopyObject without ChecksumAlgorithm must preserve the source object's checksum");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let src_bucket = "copy-checksum-preserve-src";
        let dst_bucket = "copy-checksum-preserve-dst";
        let src_key = "objects/source.bin";
        let dst_key = "objects/dest.bin";

        create_versioned_bucket(&client, src_bucket).await;
        create_versioned_bucket(&client, dst_bucket).await;

        let content = b"another deterministic payload whose source checksum must survive the copy";
        let expected_sha256 = BASE64.encode(Sha256::digest(content));

        // Store the source WITH a SHA-256 checksum so it has one to preserve.
        let put_src = client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT source with checksum failed");
        assert_eq!(
            put_src.checksum_sha256(),
            Some(expected_sha256.as_str()),
            "source PUT must report the SHA-256 it stored"
        );

        // Copy WITHOUT specifying a checksum algorithm.
        let copy_out = client
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(format!("{src_bucket}/{src_key}"))
            .send()
            .await
            .expect("CopyObject without ChecksumAlgorithm must succeed");

        // The response should echo the preserved source checksum.
        let result = copy_out
            .copy_object_result()
            .expect("issue #4996: CopyObject must return a CopyObjectResult");
        assert_eq!(
            result.checksum_sha256(),
            Some(expected_sha256.as_str()),
            "issue #4996: a no-algorithm copy must preserve and report the source object's SHA-256"
        );

        // And a checksum-mode HEAD on the destination must return that same preserved SHA-256.
        let head = client
            .head_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD destination failed");
        assert_eq!(
            head.checksum_sha256(),
            Some(expected_sha256.as_str()),
            "issue #4996: destination checksum-mode HEAD must return the preserved source SHA-256"
        );

        env.stop_server();
    }

    /// Requested algorithm differs from the source's: a source stored with SHA256, copied while
    /// requesting CRC32, must return/persist the freshly computed CRC32 and must NOT carry the
    /// source's SHA256 through. Guards the request-over-source precedence and the destination's
    /// checksum-not-inherited path, and exercises the CRC32 code path (a different branch of
    /// ChecksumType::from_string than SHA256).
    #[tokio::test]
    #[serial]
    async fn test_copy_requested_algorithm_overrides_source_checksum() {
        init_logging();
        info!("Issue #4996: a requested CopyObject checksum algorithm must override the source object's algorithm");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let src_bucket = "copy-checksum-override-src";
        let dst_bucket = "copy-checksum-override-dst";
        let src_key = "objects/source.bin";
        let ref_key = "objects/reference-crc32.bin";
        let dst_key = "objects/dest.bin";

        create_versioned_bucket(&client, src_bucket).await;
        create_versioned_bucket(&client, dst_bucket).await;

        let content = b"payload whose copy must be re-checksummed with a different algorithm";
        let expected_sha256 = BASE64.encode(Sha256::digest(content));

        // Source is stored WITH a SHA-256 checksum.
        client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT source with SHA256 failed");

        // Establish the canonical CRC32 the server computes for this content via a reference PUT,
        // so the copy's CRC32 can be asserted against an exact server-computed value.
        let ref_put = client
            .put_object()
            .bucket(src_bucket)
            .key(ref_key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("reference PUT with CRC32 failed");
        let expected_crc32 = ref_put
            .checksum_crc32()
            .expect("reference PUT must report a CRC32")
            .to_string();

        // Copy the SHA256 source while requesting CRC32.
        let copy_out = client
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(format!("{src_bucket}/{src_key}"))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .expect("CopyObject requesting a different algorithm must succeed");

        let result = copy_out
            .copy_object_result()
            .expect("issue #4996: CopyObject must return a CopyObjectResult");
        // The requested CRC32 must be computed and returned.
        assert_eq!(
            result.checksum_crc32(),
            Some(expected_crc32.as_str()),
            "issue #4996: a requested CRC32 must be computed fresh over the copied bytes"
        );
        // The source's SHA256 must NOT leak through — the requested algorithm wins.
        assert_eq!(
            result.checksum_sha256(),
            None,
            "issue #4996: the source object's SHA256 must not be inherited when a different algorithm is requested"
        );
        assert_ne!(
            result.checksum_crc32(),
            Some(expected_sha256.as_str()),
            "sanity: CRC32 field must not carry the SHA256 value"
        );

        // The destination must persist CRC32 (and only CRC32) for a checksum-mode HEAD.
        let head = client
            .head_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("HEAD destination failed");
        assert_eq!(
            head.checksum_crc32(),
            Some(expected_crc32.as_str()),
            "issue #4996: destination checksum-mode HEAD must return the requested CRC32"
        );
        assert_eq!(
            head.checksum_sha256(),
            None,
            "issue #4996: destination must not report the source's SHA256 after an override copy"
        );

        env.stop_server();
    }
}
