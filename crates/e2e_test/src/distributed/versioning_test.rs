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

use super::harness::{DistCluster, DistLayout, TestResult, enable_versioning, get_object_bytes, put_object, unique_bucket};
use crate::common::init_logging;
use aws_sdk_s3::error::ProvideErrorMetadata;

#[tokio::test]
async fn four_node_four_drive_versioning_put_list_get_delete_marker() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("version");
    dist.create_bucket(&bucket).await?;
    let writer = dist.client(0)?;
    let reader = dist.client(3)?;
    enable_versioning(&writer, &bucket).await?;

    let key = "versioned.txt";
    put_object(&writer, &bucket, key, b"v1".to_vec()).await?;
    put_object(&writer, &bucket, key, b"v2".to_vec()).await?;

    let versions = reader.list_object_versions().bucket(&bucket).prefix(key).send().await?;
    let version_ids: Vec<String> = versions
        .versions()
        .iter()
        .filter_map(|version| version.version_id().map(str::to_string))
        .collect();
    assert!(version_ids.len() >= 2, "expected at least two versions, got {version_ids:?}");

    let latest = get_object_bytes(&reader, &bucket, key).await?;
    assert_eq!(latest, b"v2");

    let older_id = versions
        .versions()
        .iter()
        .find(|version| version.is_latest() != Some(true))
        .and_then(|version| version.version_id())
        .ok_or("missing non-latest version id")?;
    let older = reader
        .get_object()
        .bucket(&bucket)
        .key(key)
        .version_id(older_id)
        .send()
        .await?;
    let older_body = older.body.collect().await?.into_bytes();
    assert_eq!(older_body.as_ref(), b"v1");

    writer.delete_object().bucket(&bucket).key(key).send().await?;
    let after_delete = reader.list_object_versions().bucket(&bucket).prefix(key).send().await?;
    assert!(
        !after_delete.delete_markers().is_empty(),
        "delete marker missing after unversioned-style delete: {after_delete:?}"
    );

    let latest_after_delete = reader.get_object().bucket(&bucket).key(key).send().await;
    match latest_after_delete {
        Ok(_) => return Err("current version should be a delete marker".into()),
        Err(error)
            if error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                .is_some_and(|code| code == "NoSuchKey" || code == "NotFound") => {}
        Err(error) => return Err(error.into()),
    }

    let restored = reader
        .get_object()
        .bucket(&bucket)
        .key(key)
        .version_id(older_id)
        .send()
        .await?;
    let restored_body = restored.body.collect().await?.into_bytes();
    assert_eq!(restored_body.as_ref(), b"v1");
    Ok(())
}
