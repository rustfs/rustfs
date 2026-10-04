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

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tracing::info;

const PARTS_COUNT_BUCKET: &str = "parts-count-test-bucket";
const MULTIPART_KEY: &str = "multipart-object.bin";
const SINGLE_PUT_KEY: &str = "single-put-object.bin";

/// s3's minimum size for every multipart part but the last.
const PART_SIZE: usize = 5 * 1024 * 1024;
const LAST_PART_SIZE: usize = 1024;
const EXPECTED_PARTS: i32 = 3;

#[tokio::test]
async fn get_object_reports_parts_count_for_multipart_upload() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting GetObject x-amz-mp-parts-count regression test");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(PARTS_COUNT_BUCKET).await?;

    // A genuine multipart upload of three parts.
    let created = client
        .create_multipart_upload()
        .bucket(PARTS_COUNT_BUCKET)
        .key(MULTIPART_KEY)
        .send()
        .await?;
    let upload_id = created.upload_id().expect("upload id");

    let mut completed_parts = Vec::new();
    for part_number in 1..=EXPECTED_PARTS {
        let size = if part_number == EXPECTED_PARTS {
            LAST_PART_SIZE
        } else {
            PART_SIZE
        };
        let uploaded = client
            .upload_part()
            .bucket(PARTS_COUNT_BUCKET)
            .key(MULTIPART_KEY)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(vec![b'a' + (part_number as u8); size]))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().expect("part etag"))
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(PARTS_COUNT_BUCKET)
        .key(MULTIPART_KEY)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;

    let total_len = (PART_SIZE * 2 + LAST_PART_SIZE) as i64;

    // The header the fix is about.
    let first_part = client
        .get_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(MULTIPART_KEY)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(
        first_part.parts_count(),
        Some(EXPECTED_PARTS),
        "GetObject with partNumber must report the object's part count"
    );
    assert_eq!(
        first_part.content_length(),
        Some(PART_SIZE as i64),
        "the response body must be part 1, not the whole object"
    );
    let expected_range = format!("bytes 0-{}/{}", PART_SIZE - 1, total_len);
    assert_eq!(
        first_part.content_range(),
        Some(expected_range.as_str()),
        "Content-Range must state part 1's extent and the whole object's size"
    );

    // Without partNumber s3 omits the header, so a plain GET is unchanged.
    let whole = client
        .get_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(MULTIPART_KEY)
        .send()
        .await?;
    assert_eq!(whole.parts_count(), None, "a GET that does not name a part is owed no part count");
    assert_eq!(whole.content_length(), Some(total_len), "a plain GET returns the whole object");

    // A single PutObject is not a multipart upload, so it is owed no count
    // even when a part is named.
    client
        .put_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(SINGLE_PUT_KEY)
        .body(ByteStream::from_static(b"0123456789abcdef"))
        .send()
        .await?;
    let single = client
        .get_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(SINGLE_PUT_KEY)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(single.parts_count(), None, "an object stored by a single PutObject is not multipart");

    client
        .delete_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(MULTIPART_KEY)
        .send()
        .await?;
    client
        .delete_object()
        .bucket(PARTS_COUNT_BUCKET)
        .key(SINGLE_PUT_KEY)
        .send()
        .await?;
    env.delete_test_bucket(PARTS_COUNT_BUCKET).await?;
    env.stop_server();

    Ok(())
}
