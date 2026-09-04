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
    DistCluster, DistLayout, TestResult, assert_object_bytes, get_object_bytes, put_object, unique_bucket, wait_until,
};
use crate::common::init_logging;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use std::time::Duration;

#[tokio::test]
async fn four_node_four_drive_multipart_and_cross_node_listing_agree() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("extra");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;

    let key = "multipart.bin";
    let part1 = vec![0x41u8; 5 * 1024 * 1024];
    let part2 = vec![0x42u8; 5 * 1024 * 1024];
    let upload = client.create_multipart_upload().bucket(&bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing upload id")?.to_string();

    let uploaded1 = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await?;
    let uploaded2 = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await?;

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(uploaded1.e_tag().unwrap_or_default())
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(uploaded2.e_tag().unwrap_or_default())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let mut expected = part1;
    expected.extend_from_slice(&part2);
    for node_idx in 0..dist.cluster.nodes.len() {
        assert_object_bytes(&dist.client(node_idx)?, &bucket, key, &expected).await?;
    }

    put_object(&client, &bucket, "list/a", b"a".to_vec()).await?;
    put_object(&dist.client(2)?, &bucket, "list/b", b"b".to_vec()).await?;
    let mut seen = Vec::new();
    for node_idx in 0..dist.cluster.nodes.len() {
        let listed = dist
            .client(node_idx)?
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("list/")
            .send()
            .await?;
        let keys: Vec<String> = listed
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_string))
            .collect();
        seen.push(keys);
    }
    for keys in &seen[1..] {
        assert_eq!(&seen[0], keys, "list results diverged across nodes: {seen:?}");
    }

    let got = get_object_bytes(&dist.client(3)?, &bucket, "list/a").await?;
    assert_eq!(got, b"a");
    Ok(())
}

#[tokio::test]
async fn four_node_list_buckets_agree_and_deleted_bucket_can_be_recreated() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("recreate");
    dist.create_bucket(&bucket).await?;
    put_object(&dist.client(0)?, &bucket, "seed.bin", b"seed".to_vec()).await?;

    for node_idx in 0..dist.cluster.nodes.len() {
        let client = dist.client(node_idx)?;
        let name = bucket.clone();
        wait_until(
            Duration::from_secs(20),
            || {
                let client = client.clone();
                let name = name.clone();
                async move {
                    let listed = client.list_buckets().send().await?;
                    Ok(listed.buckets().iter().any(|entry| entry.name() == Some(name.as_str())))
                }
            },
            &format!("node {node_idx} lists {bucket}"),
        )
        .await?;
        assert_object_bytes(&dist.client(node_idx)?, &bucket, "seed.bin", b"seed").await?;
    }

    dist.client(1)?.delete_object().bucket(&bucket).key("seed.bin").send().await?;
    let deleter = dist.client(2)?;
    let delete_name = bucket.clone();
    wait_until(
        Duration::from_secs(20),
        || {
            let deleter = deleter.clone();
            let delete_name = delete_name.clone();
            async move {
                match deleter.delete_bucket().bucket(&delete_name).send().await {
                    Ok(_) => Ok(true),
                    Err(error) => {
                        let message = error.to_string();
                        if message.contains("NoSuchBucket") {
                            Ok(true)
                        } else if message.contains("BucketNotEmpty")
                            || message.contains("InternalError")
                            || message.contains("SlowDown")
                            || message.contains("500")
                        {
                            Ok(false)
                        } else {
                            Err(error.into())
                        }
                    }
                }
            }
        },
        "delete empty bucket",
    )
    .await?;

    let checker = dist.client(3)?;
    let head_name = bucket.clone();
    wait_until(
        Duration::from_secs(20),
        || {
            let checker = checker.clone();
            let head_name = head_name.clone();
            async move { Ok(checker.head_bucket().bucket(&head_name).send().await.is_err()) }
        },
        "deleted bucket no longer visible",
    )
    .await?;

    dist.create_bucket(&bucket).await?;
    let writer = dist.client(3)?;
    let wait_name = bucket.clone();
    wait_until(
        Duration::from_secs(20),
        || {
            let writer = writer.clone();
            let wait_name = wait_name.clone();
            async move { Ok(writer.head_bucket().bucket(&wait_name).send().await.is_ok()) }
        },
        "recreated bucket visible",
    )
    .await?;
    put_object(&writer, &bucket, "new.bin", b"new".to_vec()).await?;
    assert_object_bytes(&dist.client(0)?, &bucket, "new.bin", b"new").await?;
    match get_object_bytes(&dist.client(1)?, &bucket, "seed.bin").await {
        Ok(_) => return Err("recreated bucket still contains the previous object".into()),
        Err(error) => {
            let message = error.to_string();
            if !(message.contains("NoSuchKey") || message.contains("NotFound") || message.contains("NoSuchBucket")) {
                return Err(error);
            }
        }
    }
    Ok(())
}
