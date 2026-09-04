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
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
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

    let deleted = writer
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
    assert!(deleted.errors().is_empty(), "DeleteObjects reported failures: {deleted:?}");
    assert_eq!(deleted.deleted().len(), 3, "DeleteObjects did not acknowledge every key");

    let remaining = reader.list_objects_v2().bucket(&bucket).send().await?;
    assert!(remaining.contents().is_empty(), "bucket still has objects after delete");
    Ok(())
}

#[tokio::test]
async fn four_node_s3_metadata_tags_special_keys_pagination_and_multipart_abort() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("s3matrix");
    dist.create_bucket(&bucket).await?;
    let writer = dist.client(0)?;
    let reader = dist.client(3)?;

    let special_key = "unicode/测试 space+percent%25.txt";
    let special_body = b"metadata and tagging survive distributed routing".to_vec();
    let put = writer
        .put_object()
        .bucket(&bucket)
        .key(special_key)
        .metadata("test-meta", "distributed")
        .tagging("purpose=compatibility&scope=four-by-four")
        .body(ByteStream::from(special_body.clone()))
        .send()
        .await?;
    let etag = put.e_tag().ok_or("PutObject omitted ETag")?.to_string();

    let head = reader.head_object().bucket(&bucket).key(special_key).send().await?;
    assert_eq!(
        head.metadata()
            .and_then(|metadata| metadata.get("test-meta"))
            .map(String::as_str),
        Some("distributed")
    );
    assert_eq!(head.e_tag(), Some(etag.as_str()));
    let tags = reader.get_object_tagging().bucket(&bucket).key(special_key).send().await?;
    let actual_tags: std::collections::BTreeMap<_, _> = tags
        .tag_set()
        .iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect();
    assert_eq!(actual_tags.get("purpose").map(String::as_str), Some("compatibility"));
    assert_eq!(actual_tags.get("scope").map(String::as_str), Some("four-by-four"));

    let conditional = reader
        .get_object()
        .bucket(&bucket)
        .key(special_key)
        .if_match(&etag)
        .send()
        .await?;
    assert_eq!(conditional.body.collect().await?.into_bytes().as_ref(), special_body.as_slice());
    let invalid_range = reader
        .get_object()
        .bucket(&bucket)
        .key(special_key)
        .range("bytes=999999-1000000")
        .send()
        .await
        .expect_err("an unsatisfiable range must fail");
    assert_eq!(
        invalid_range.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("InvalidRange"),
        "unexpected invalid-range error: {invalid_range:?}"
    );

    let upload_key = "multipart/aborted.bin";
    let upload = writer
        .create_multipart_upload()
        .bucket(&bucket)
        .key(upload_key)
        .send()
        .await?;
    let upload_id = upload.upload_id().ok_or("CreateMultipartUpload omitted upload ID")?;
    writer
        .upload_part()
        .bucket(&bucket)
        .key(upload_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![0x5Au8; 5 * 1024 * 1024]))
        .send()
        .await?;
    let pending = reader
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("multipart/")
        .send()
        .await?;
    assert!(pending.uploads().iter().any(|entry| entry.upload_id() == Some(upload_id)));
    writer
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(upload_key)
        .upload_id(upload_id)
        .send()
        .await?;
    let after_abort = reader
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("multipart/")
        .send()
        .await?;
    assert!(after_abort.uploads().iter().all(|entry| entry.upload_id() != Some(upload_id)));
    let aborted_head = reader
        .head_object()
        .bucket(&bucket)
        .key(upload_key)
        .send()
        .await
        .expect_err("aborted multipart upload must not create an object");
    assert_eq!(
        aborted_head.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "aborted multipart object returned an unexpected HEAD result: {aborted_head:?}"
    );

    for index in 0..113 {
        let key = format!("page/{index:04}.txt");
        put_object(&writer, &bucket, &key, format!("page-{index}").into_bytes()).await?;
    }
    let mut token = None;
    let mut paged_keys = Vec::new();
    loop {
        let page = reader
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("page/")
            .max_keys(37)
            .set_continuation_token(token.take())
            .send()
            .await?;
        paged_keys.extend(page.contents().iter().filter_map(|object| object.key().map(str::to_string)));
        if page.is_truncated() != Some(true) {
            break;
        }
        token = Some(
            page.next_continuation_token()
                .ok_or("truncated ListObjectsV2 page omitted next continuation token")?
                .to_string(),
        );
    }
    assert_eq!(paged_keys.len(), 113);
    let expected: Vec<_> = (0..113).map(|index| format!("page/{index:04}.txt")).collect();
    assert_eq!(paged_keys, expected, "pagination lost, duplicated, or reordered keys");
    Ok(())
}
