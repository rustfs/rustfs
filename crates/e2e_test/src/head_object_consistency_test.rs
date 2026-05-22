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

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use serial_test::serial;
use std::time::Duration;
use tracing::info;

const CONSISTENCY_BUCKET: &str = "head-consistency-bucket";
const PUT_KEY: &str = "consistency-put-object.txt";
const MPU_KEY: &str = "consistency-multipart-object.txt";

fn list_contains_key(output: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output, key: &str) -> bool {
    output.contents().iter().any(|obj| obj.key().is_some_and(|k| k == key))
}

#[tokio::test]
#[serial]
async fn head_object_consistency_after_write_and_multipart_and_presigned_head()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting HeadObject consistency regression test");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(CONSISTENCY_BUCKET).await?;

    client
        .put_object()
        .bucket(CONSISTENCY_BUCKET)
        .key(PUT_KEY)
        .body(ByteStream::from_static(b"head-consistency-put-body"))
        .send()
        .await?;

    client.get_object().bucket(CONSISTENCY_BUCKET).key(PUT_KEY).send().await?;
    let put_list = client
        .list_objects_v2()
        .bucket(CONSISTENCY_BUCKET)
        .prefix(PUT_KEY)
        .send()
        .await?;
    assert!(list_contains_key(&put_list, PUT_KEY), "ListObjectsV2 should include the PutObject key");

    client.head_object().bucket(CONSISTENCY_BUCKET).key(PUT_KEY).send().await?;

    let create = client
        .create_multipart_upload()
        .bucket(CONSISTENCY_BUCKET)
        .key(MPU_KEY)
        .send()
        .await?;
    let upload_id = create.upload_id().ok_or("missing multipart upload id")?.to_string();

    let part1 = client
        .upload_part()
        .bucket(CONSISTENCY_BUCKET)
        .key(MPU_KEY)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"head-consistency-multipart-part-1"))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .set_e_tag(part1.e_tag().map(str::to_string))
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket(CONSISTENCY_BUCKET)
        .key(MPU_KEY)
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await?;

    client.get_object().bucket(CONSISTENCY_BUCKET).key(MPU_KEY).send().await?;
    let mpu_list = client
        .list_objects_v2()
        .bucket(CONSISTENCY_BUCKET)
        .prefix(MPU_KEY)
        .send()
        .await?;
    assert!(
        list_contains_key(&mpu_list, MPU_KEY),
        "ListObjectsV2 should include the completed multipart key"
    );

    client.head_object().bucket(CONSISTENCY_BUCKET).key(MPU_KEY).send().await?;

    let presigned = client
        .head_object()
        .bucket(CONSISTENCY_BUCKET)
        .key(PUT_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300))?)
        .await?;
    let presigned_resp = local_http_client().head(presigned.uri().to_string()).send().await?;
    assert!(
        presigned_resp.status().is_success(),
        "Presigned HEAD should succeed, got status {}",
        presigned_resp.status()
    );

    client.delete_object().bucket(CONSISTENCY_BUCKET).key(PUT_KEY).send().await?;
    client.delete_object().bucket(CONSISTENCY_BUCKET).key(MPU_KEY).send().await?;
    env.delete_test_bucket(CONSISTENCY_BUCKET).await?;
    env.stop_server();

    Ok(())
}
