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

//! Harbor / Docker Distribution multipart staging to CopyObject regressions.

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, StorageClass};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Barrier;
use tracing::info;

const BUCKET: &str = "multipart-copy-readiness";
const SOURCE_KEY: &str = "docker/registry/v2/repositories/example/_uploads/upload-id/data";
const TARGET_KEY: &str = "docker/registry/v2/blobs/sha256/c0/digest/data";
const UNFINISHED_SOURCE_KEY: &str = "docker/registry/v2/repositories/example/_uploads/upload-id-unfinished/data";
const UNFINISHED_TARGET_KEY: &str = "docker/registry/v2/blobs/sha256/c1/digest/data";
const OVERLAP_SOURCE_KEY: &str = "docker/registry/v2/repositories/example/_uploads/upload-id-overlap/data";
const OVERLAP_TARGET_KEY: &str = "docker/registry/v2/blobs/sha256/c2/digest/data";
const OVERLAP_RETRY_TARGET_KEY: &str = "docker/registry/v2/blobs/sha256/c3/digest/data";

fn list_contains_key(output: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output, key: &str) -> bool {
    output
        .contents()
        .iter()
        .any(|object| object.key().is_some_and(|candidate| candidate == key))
}

async fn upload_one_part_mpu(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    payload: Vec<u8>,
) -> Result<(String, CompletedMultipartUpload), Box<dyn Error + Send + Sync>> {
    let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(payload))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .set_e_tag(part.e_tag().map(str::to_string))
                .build(),
        )
        .build();

    Ok((upload_id, completed))
}

#[tokio::test]
async fn harbor_style_multipart_staging_copy_object_boundaries() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    info!("backlog#2185: Harbor-style one-part MPU staging followed by CopyObject");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(BUCKET).await?;

    let payload = vec![0xAB; 273];
    let (upload_id, completed) = upload_one_part_mpu(&client, BUCKET, SOURCE_KEY, payload.clone()).await?;
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(SOURCE_KEY)
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await?;

    let completed_list = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(SOURCE_KEY)
        .max_keys(1)
        .send()
        .await?;
    assert!(
        list_contains_key(&completed_list, SOURCE_KEY),
        "completed multipart staging object must be immediately list-visible"
    );

    client
        .copy_object()
        .bucket(BUCKET)
        .key(TARGET_KEY)
        .copy_source(format!("/{BUCKET}/{SOURCE_KEY}"))
        .storage_class(StorageClass::Standard)
        .send()
        .await?;

    let copied = client.get_object().bucket(BUCKET).key(TARGET_KEY).send().await?;
    let copied_body = copied.body.collect().await?.into_bytes();
    assert_eq!(copied_body.as_ref(), payload.as_slice());

    let overlap_payload = vec![0xBC; 15_796];
    let (overlap_upload_id, overlap_completed) =
        upload_one_part_mpu(&client, BUCKET, OVERLAP_SOURCE_KEY, overlap_payload.clone()).await?;

    let barrier = Arc::new(Barrier::new(2));
    let complete_client = client.clone();
    let complete_barrier = Arc::clone(&barrier);
    let complete_upload_id = overlap_upload_id.clone();
    let complete_task = tokio::spawn(async move {
        complete_barrier.wait().await;
        complete_client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(OVERLAP_SOURCE_KEY)
            .upload_id(complete_upload_id)
            .multipart_upload(overlap_completed)
            .send()
            .await
    });

    let copy_client = client.clone();
    let copy_barrier = Arc::clone(&barrier);
    let copy_task = tokio::spawn(async move {
        copy_barrier.wait().await;
        copy_client
            .copy_object()
            .bucket(BUCKET)
            .key(OVERLAP_TARGET_KEY)
            .copy_source(format!("/{BUCKET}/{OVERLAP_SOURCE_KEY}"))
            .storage_class(StorageClass::Standard)
            .send()
            .await
    });

    complete_task.await??;
    match copy_task.await? {
        Ok(_) => {}
        Err(copy_err) => {
            assert_eq!(
                copy_err.raw_response().map(|response| response.status().as_u16()),
                Some(404),
                "overlapped CopyObject may race before publication, but must not leak a 5xx response: {copy_err:?}"
            );
            assert_eq!(
                copy_err.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("NoSuchKey"),
                "overlapped CopyObject that wins before Complete must look like an unpublished ordinary object: {copy_err:?}"
            );
        }
    }

    client
        .copy_object()
        .bucket(BUCKET)
        .key(OVERLAP_RETRY_TARGET_KEY)
        .copy_source(format!("/{BUCKET}/{OVERLAP_SOURCE_KEY}"))
        .storage_class(StorageClass::Standard)
        .send()
        .await?;
    let overlap_copied = client
        .get_object()
        .bucket(BUCKET)
        .key(OVERLAP_RETRY_TARGET_KEY)
        .send()
        .await?;
    let overlap_copied_body = overlap_copied.body.collect().await?.into_bytes();
    assert_eq!(
        overlap_copied_body.as_ref(),
        overlap_payload.as_slice(),
        "a completed staging object must not become permanently unreadable after an overlapped copy attempt"
    );

    let unfinished = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(UNFINISHED_SOURCE_KEY)
        .send()
        .await?;
    let unfinished_upload_id = unfinished.upload_id().ok_or("missing unfinished upload id")?.to_string();
    client
        .upload_part()
        .bucket(BUCKET)
        .key(UNFINISHED_SOURCE_KEY)
        .upload_id(&unfinished_upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"not-yet-committed"))
        .send()
        .await?;

    let unfinished_list = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(UNFINISHED_SOURCE_KEY)
        .max_keys(1)
        .send()
        .await?;
    assert!(
        !list_contains_key(&unfinished_list, UNFINISHED_SOURCE_KEY),
        "UploadPart alone must not publish the staging object namespace entry"
    );

    let copy_err = client
        .copy_object()
        .bucket(BUCKET)
        .key(UNFINISHED_TARGET_KEY)
        .copy_source(format!("/{BUCKET}/{UNFINISHED_SOURCE_KEY}"))
        .storage_class(StorageClass::Standard)
        .send()
        .await
        .expect_err("copying an uncompleted multipart upload source must be rejected");
    assert_eq!(
        copy_err.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "unfinished MPU source must not leak a 5xx response: {copy_err:?}"
    );
    assert_eq!(
        copy_err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("NoSuchKey"),
        "unfinished MPU source must be reported as a missing ordinary object: {copy_err:?}"
    );

    client
        .abort_multipart_upload()
        .bucket(BUCKET)
        .key(UNFINISHED_SOURCE_KEY)
        .upload_id(unfinished_upload_id)
        .send()
        .await?;
    client
        .delete_object()
        .bucket(BUCKET)
        .key(OVERLAP_RETRY_TARGET_KEY)
        .send()
        .await?;
    let _ = client.delete_object().bucket(BUCKET).key(OVERLAP_TARGET_KEY).send().await;
    client.delete_object().bucket(BUCKET).key(OVERLAP_SOURCE_KEY).send().await?;
    client.delete_object().bucket(BUCKET).key(TARGET_KEY).send().await?;
    client.delete_object().bucket(BUCKET).key(SOURCE_KEY).send().await?;
    env.delete_test_bucket(BUCKET).await?;
    env.stop_server();

    Ok(())
}
