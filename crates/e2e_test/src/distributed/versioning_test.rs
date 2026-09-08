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
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

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
    let v1_id = writer
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(b"v1".to_vec().into())
        .send()
        .await?
        .version_id()
        .ok_or("v1 PUT omitted version ID")?
        .to_string();
    let v2_id = writer
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(b"v2".to_vec().into())
        .send()
        .await?
        .version_id()
        .ok_or("v2 PUT omitted version ID")?
        .to_string();

    let versions = reader.list_object_versions().bucket(&bucket).prefix(key).send().await?;
    let matching_versions: Vec<_> = versions
        .versions()
        .iter()
        .filter(|version| version.key() == Some(key))
        .collect();
    assert_eq!(matching_versions.len(), 2, "fresh key must have exactly two versions: {versions:?}");
    assert!(versions.delete_markers().is_empty(), "fresh key unexpectedly has a delete marker");
    assert!(
        matching_versions
            .iter()
            .any(|version| version.version_id() == Some(v1_id.as_str()) && version.is_latest() != Some(true)),
        "v1 was not the historical version: {versions:?}"
    );
    assert!(
        matching_versions
            .iter()
            .any(|version| version.version_id() == Some(v2_id.as_str()) && version.is_latest() == Some(true)),
        "v2 was not the latest version: {versions:?}"
    );

    let latest = get_object_bytes(&reader, &bucket, key).await?;
    assert_eq!(latest, b"v2");

    let older = reader.get_object().bucket(&bucket).key(key).version_id(&v1_id).send().await?;
    let older_body = older.body.collect().await?.into_bytes();
    assert_eq!(older_body.as_ref(), b"v1");

    let deleted = writer.delete_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let marker_id = deleted.version_id().ok_or("DeleteObject omitted delete-marker version ID")?;
    let after_delete = reader.list_object_versions().bucket(&bucket).prefix(key).send().await?;
    let matching_markers: Vec<_> = after_delete
        .delete_markers()
        .iter()
        .filter(|marker| marker.key() == Some(key))
        .collect();
    assert_eq!(
        matching_markers.len(),
        1,
        "delete marker missing or duplicated after current-version delete: {after_delete:?}"
    );
    assert!(
        matching_markers[0].version_id() == Some(marker_id) && matching_markers[0].is_latest() == Some(true),
        "DeleteObject response and ListObjectVersions disagree about the marker: {after_delete:?}"
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

    let restored = reader.get_object().bucket(&bucket).key(key).version_id(&v1_id).send().await?;
    let restored_body = restored.body.collect().await?.into_bytes();
    assert_eq!(restored_body.as_ref(), b"v1");

    writer
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(marker_id)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&reader, &bucket, key).await?, b"v2");
    Ok(())
}

#[tokio::test]
async fn four_node_versioning_suspension_keeps_one_null_version_and_history() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("suspend");
    dist.create_bucket(&bucket).await?;
    let writer = dist.client(0)?;
    let reader = dist.client(3)?;
    enable_versioning(&writer, &bucket).await?;

    let key = "suspended.txt";
    let original = writer
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(b"enabled-history".to_vec().into())
        .send()
        .await?
        .version_id()
        .ok_or("enabled PUT omitted version ID")?
        .to_string();
    writer
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await?;

    put_object(&writer, &bucket, key, b"null-one".to_vec()).await?;
    put_object(&writer, &bucket, key, b"null-two".to_vec()).await?;
    assert_eq!(get_object_bytes(&reader, &bucket, key).await?, b"null-two");

    let versions = reader.list_object_versions().bucket(&bucket).prefix(key).send().await?;
    let matching: Vec<_> = versions
        .versions()
        .iter()
        .filter(|version| version.key() == Some(key))
        .collect();
    assert!(matching.iter().any(|version| version.version_id() == Some(original.as_str())));
    let null_version_count = matching
        .iter()
        .filter(|version| {
            matches!(
                version.version_id(),
                None | Some("") | Some("null") | Some("00000000-0000-0000-0000-000000000000")
            )
        })
        .count();
    assert_eq!(null_version_count, 1, "suspended overwrites must keep one null version: {versions:?}");

    let historical = reader
        .get_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&original)
        .send()
        .await?;
    assert_eq!(historical.body.collect().await?.into_bytes().as_ref(), b"enabled-history");
    Ok(())
}
