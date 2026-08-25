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

//! Ranged GETs over encrypted single-part objects.
//!
//! Byte-exactness must hold on every frame layout the server can write:
//! legacy v1 (variable frames, conservative full read) and, when
//! `RUSTFS_ENCRYPTION_FRAME_V2=true` reaches the server under test, the
//! fixed-frame v2 layout whose marker enables the closed-form frame seek.
//! The matrix crosses frame boundaries, starts mid-frame, and ends inside
//! the final short frame, so a mispositioned seek cannot pass.

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use tracing::info;

const FRAME_PLAINTEXT: usize = 8 * 1024;

#[tokio::test]
async fn sse_s3_single_part_ranged_gets_are_byte_exact() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Testing ranged GETs over an SSE-S3 single-part object");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let test_key = "encrypted-range-get";
    let body: Vec<u8> = (0..3 * FRAME_PLAINTEXT + 500).map(|i| (i % 251) as u8).collect();

    let put = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "the object under test must actually be encrypted"
    );

    let cases: &[(usize, usize)] = &[
        // Head range inside frame 0.
        (0, 99),
        // Crossing the first frame boundary.
        (FRAME_PLAINTEXT - 1, FRAME_PLAINTEXT),
        // Starting exactly on a frame boundary.
        (FRAME_PLAINTEXT, FRAME_PLAINTEXT + 9),
        // Mid-object, mid-frame on both ends.
        (2 * FRAME_PLAINTEXT + 5, 3 * FRAME_PLAINTEXT + 100),
        // Tail range ending inside the final short frame.
        (3 * FRAME_PLAINTEXT + 100, 3 * FRAME_PLAINTEXT + 499),
    ];

    for &(start, end) in cases {
        let response = s3_client
            .get_object()
            .bucket(TEST_BUCKET)
            .key(test_key)
            .range(format!("bytes={start}-{end}"))
            .send()
            .await?;
        assert_eq!(
            response.content_length(),
            Some((end - start + 1) as i64),
            "range {start}-{end} content length"
        );
        let data = response.body.collect().await?.into_bytes();
        assert_eq!(data.as_ref(), &body[start..=end], "range {start}-{end} must be byte-exact");
    }

    // A suffix range exercises the offset resolution path as well.
    let response = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .range("bytes=-123")
        .send()
        .await?;
    let data = response.body.collect().await?.into_bytes();
    assert_eq!(data.as_ref(), &body[body.len() - 123..], "suffix range must be byte-exact");

    // The unranged body still round-trips.
    let response = s3_client.get_object().bucket(TEST_BUCKET).key(test_key).send().await?;
    let data = response.body.collect().await?.into_bytes();
    assert_eq!(data.as_ref(), body.as_slice(), "full body must round-trip");

    // A block-aligned object ends in an empty authenticated final frame under
    // the v2 layout; tail ranges touching the last plaintext byte must not be
    // misread as truncation.
    let aligned_key = "encrypted-range-get-aligned";
    let aligned_body: Vec<u8> = (0..3 * FRAME_PLAINTEXT).map(|i| ((i + 3) % 251) as u8).collect();
    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(aligned_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(aligned_body.clone()))
        .send()
        .await?;
    for (start, end) in [
        (2 * FRAME_PLAINTEXT + 10, 3 * FRAME_PLAINTEXT - 1),
        (3 * FRAME_PLAINTEXT - 1, 3 * FRAME_PLAINTEXT - 1),
    ] {
        let response = s3_client
            .get_object()
            .bucket(TEST_BUCKET)
            .key(aligned_key)
            .range(format!("bytes={start}-{end}"))
            .send()
            .await?;
        let data = response.body.collect().await?.into_bytes();
        assert_eq!(
            data.as_ref(),
            &aligned_body[start..=end],
            "aligned range {start}-{end} must be byte-exact"
        );
    }

    Ok(())
}
