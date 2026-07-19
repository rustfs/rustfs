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

//! Regression test for Issue #4996: CopyObject must return the destination object's
//! checksum in `CopyObjectResult` and persist it so a later checksum-mode HEAD/GET
//! returns the same value. Covers both the requested-algorithm case (compute fresh)
//! and the no-algorithm case (preserve the source object's existing checksum).

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, VersioningConfiguration};
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
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
