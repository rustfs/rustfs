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

use super::harness::{DistCluster, DistLayout, TestResult, assert_object_bytes, get_object_bytes, put_object, unique_bucket};
use crate::common::{init_logging, local_http_client};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::{Delete, MetadataDirective, ObjectIdentifier};
use std::time::Duration;

#[tokio::test]
async fn four_node_four_drive_s3_put_get_head_list_copy_rename_delete_and_presign() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("s3basic");
    dist.create_bucket(&bucket).await?;

    let writer = dist.client(0)?;
    let reader = dist.client(3)?;
    let key = "dir/object.bin";
    let body = vec![0xA5u8; 256 * 1024];
    put_object(&writer, &bucket, key, body.clone()).await?;

    let head = reader.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.content_length(), Some(body.len() as i64));
    assert_object_bytes(&reader, &bucket, key, &body).await?;

    let ranged = reader
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=0-15")
        .send()
        .await?;
    let ranged_body = ranged.body.collect().await?.into_bytes();
    assert_eq!(ranged_body.as_ref(), &body[..16]);

    let listed = reader.list_objects_v2().bucket(&bucket).prefix("dir/").send().await?;
    let keys: Vec<_> = listed.contents().iter().filter_map(|object| object.key()).collect();
    assert_eq!(keys, vec![key]);

    let copy_key = "dir/object-copy.bin";
    reader
        .copy_object()
        .bucket(&bucket)
        .key(copy_key)
        .copy_source(format!("{bucket}/{key}"))
        .metadata_directive(MetadataDirective::Copy)
        .send()
        .await?;
    assert_object_bytes(&writer, &bucket, copy_key, &body).await?;

    let moved_key = "dir/object-moved.bin";
    writer
        .copy_object()
        .bucket(&bucket)
        .key(moved_key)
        .copy_source(format!("{bucket}/{copy_key}"))
        .send()
        .await?;
    writer.delete_object().bucket(&bucket).key(copy_key).send().await?;
    match writer.head_object().bucket(&bucket).key(copy_key).send().await {
        Ok(_) => return Err("copied source still present after rename delete".into()),
        Err(error) if error.as_service_error().is_some_and(|err| err.is_not_found()) => {}
        Err(error) => return Err(error.into()),
    }
    assert_object_bytes(&reader, &bucket, moved_key, &body).await?;

    let presigned = writer
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(120))?)
        .await?;
    let response = local_http_client().get(presigned.uri().to_string()).send().await?;
    assert!(response.status().is_success(), "presigned GET failed: {}", response.status());
    let presigned_body = response.bytes().await?;
    assert_eq!(presigned_body.as_ref(), body.as_slice());

    let empty_key = "empty";
    put_object(&writer, &bucket, empty_key, Vec::new()).await?;
    let empty = get_object_bytes(&reader, &bucket, empty_key).await?;
    assert!(empty.is_empty());

    writer
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).build()?)
                .objects(ObjectIdentifier::builder().key(moved_key).build()?)
                .objects(ObjectIdentifier::builder().key(empty_key).build()?)
                .build()?,
        )
        .send()
        .await?;

    let remaining = reader.list_objects_v2().bucket(&bucket).send().await?;
    assert!(remaining.contents().is_empty(), "bucket still has objects after delete");
    Ok(())
}
