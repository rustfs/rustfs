// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::harness::{
    DECOMMISSION_POOL_ID, DistCluster, DistLayout, TestResult, assert_inventory, enable_versioning, put_inventory_retrying,
    sha256_hex, start_decommission, unique_bucket, wait_for_decommission_active, wait_for_decommission_complete,
};
use crate::common::init_logging;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use std::time::Duration;

#[tokio::test]
async fn decommission_does_not_alter_object_sha256_across_pools() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::SingleNodeFourDrive).await?;
    let bucket = unique_bucket("integrity");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    enable_versioning(&client, &bucket).await?;
    let inventory = put_inventory_retrying(&client, &bucket, 96, 256 * 1024, Duration::from_secs(30)).await?;
    let before: Vec<(String, String)> = inventory.iter().map(|(key, body)| (key.clone(), sha256_hex(body))).collect();

    let versioned_key = "history/versioned.bin";
    let version_one = b"historical bytes before data movement".to_vec();
    let version_two = b"current bytes before data movement".to_vec();
    let version_one_id = client
        .put_object()
        .bucket(&bucket)
        .key(versioned_key)
        .body(ByteStream::from(version_one.clone()))
        .send()
        .await?
        .version_id()
        .ok_or("historical PUT omitted version ID")?
        .to_string();
    let version_two_id = client
        .put_object()
        .bucket(&bucket)
        .key(versioned_key)
        .body(ByteStream::from(version_two.clone()))
        .send()
        .await?
        .version_id()
        .ok_or("current PUT omitted version ID")?
        .to_string();

    let multipart_key = "multipart/moved.bin";
    let first_part = vec![0x31; 5 * 1024 * 1024];
    let second_part = vec![0x72; 1024 * 1024];
    let upload = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(multipart_key)
        .send()
        .await?;
    let upload_id = upload.upload_id().ok_or("movement multipart upload omitted upload ID")?;
    let uploaded_one = client
        .upload_part()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(first_part.clone()))
        .send()
        .await?;
    let uploaded_two = client
        .upload_part()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from(second_part.clone()))
        .send()
        .await?;
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(uploaded_one.e_tag().ok_or("movement part 1 omitted ETag")?)
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(uploaded_two.e_tag().ok_or("movement part 2 omitted ETag")?)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    dist.expand_to_four_pools().await?;

    start_decommission(&dist.cluster, DECOMMISSION_POOL_ID).await?;
    wait_for_decommission_active(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(30)).await?;
    wait_for_decommission_complete(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(180)).await?;

    let after_client = dist.client(2)?;
    assert_inventory(&after_client, &bucket, &inventory).await?;
    for (key, expected_hash) in before {
        let got = after_client.get_object().bucket(&bucket).key(&key).send().await?;
        let body = got.body.collect().await?.into_bytes();
        assert_eq!(sha256_hex(body.as_ref()), expected_hash, "checksum changed for {key} after decommission");
    }
    for (version_id, expected) in [(&version_one_id, &version_one), (&version_two_id, &version_two)] {
        let got = after_client
            .get_object()
            .bucket(&bucket)
            .key(versioned_key)
            .version_id(version_id)
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert_eq!(got.as_ref(), expected.as_slice(), "version {version_id} changed after decommission");
    }
    let mut expected_multipart = first_part;
    expected_multipart.extend_from_slice(&second_part);
    let got_multipart = after_client
        .get_object()
        .bucket(&bucket)
        .key(multipart_key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(
        sha256_hex(got_multipart.as_ref()),
        sha256_hex(&expected_multipart),
        "multipart checksum changed after decommission"
    );
    Ok(())
}
