#![cfg(test)]
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

//! Receiver-side replication LWW over the wire (rustfs/backlog#1953, audit
//! A4/P1-6).
//!
//! In an active-active topology both sites' metadata states arrive at the
//! peer as authorized replication PUTs carrying per-category source
//! timestamps (`x-rustfs-source-replication-tagging-timestamp` header
//! family). Before the fix the receiver applied them unconditionally, so a
//! stale delivery overwrote a newer local state and the two sites diverged
//! permanently while both reported COMPLETED. This test drives one live
//! `rustfs` server with simulated inbound replication PUTs for the same
//! object version and asserts the newer tagging state wins regardless of
//! delivery order, while a stale delivery still succeeds at the object level
//! (a failure would loop through MRF re-delivering the stale value).
//!
//! The real dual-site path (sender, worker, status bookkeeping, MRF replay)
//! is covered by
//! `replication_extension_test::test_site_replication_tagging_lww_converges_active_active_real_dual_node`;
//! this file stays as the fast, single-process receiver check.

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const HDR_SOURCE_REPLICATION_REQUEST: &str = "x-rustfs-source-replication-request";
const HDR_SOURCE_VERSION_ID: &str = "x-rustfs-source-version-id";
const HDR_SOURCE_MTIME: &str = "x-rustfs-source-mtime";
const HDR_SOURCE_TAGGING_TIMESTAMP: &str = "x-rustfs-source-replication-tagging-timestamp";

const SOURCE_MTIME: &str = "2026-01-01T00:00:00Z";
const T_STALE: &str = "2026-01-01T00:00:01Z";
const T_LOCAL: &str = "2026-02-01T00:00:00Z";
const T_NEWER: &str = "2026-03-01T00:00:00Z";

/// Simulated inbound authorized replication PUT: same object version, tags and
/// the source-authored tagging timestamp carried in transport headers.
async fn inbound_replication_put(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: &str,
    tags: &str,
    tagging_timestamp: &str,
) -> TestResult {
    let version_id = version_id.to_string();
    let tagging_timestamp = tagging_timestamp.to_string();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"lww-e2e-body"))
        .tagging(tags)
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert(HDR_SOURCE_REPLICATION_REQUEST, "true");
            req.headers_mut().insert(HDR_SOURCE_VERSION_ID, version_id.clone());
            req.headers_mut().insert(HDR_SOURCE_MTIME, SOURCE_MTIME);
            req.headers_mut()
                .insert(HDR_SOURCE_TAGGING_TIMESTAMP, tagging_timestamp.clone());
        })
        .send()
        .await?;
    Ok(())
}

async fn tag_value(client: &Client, bucket: &str, key: &str, version_id: &str, tag_key: &str) -> Option<String> {
    let tagging = client
        .get_object_tagging()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .expect("object tagging should be readable");
    tagging
        .tag_set()
        .iter()
        .find(|tag| tag.key() == tag_key)
        .map(|tag| tag.value().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_lww_keeps_newer_tags_across_delivery_orders() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let bucket = "replication-lww-receiver";
    let key = "object";
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    // First delivery establishes version V with tags stamped T_LOCAL.
    let version_id = uuid::Uuid::new_v4().to_string();
    inbound_replication_put(&client, bucket, key, &version_id, "site=local", T_LOCAL).await?;
    assert_eq!(
        tag_value(&client, bucket, key, &version_id, "site").await.as_deref(),
        Some("local"),
        "the first delivery must establish the tagged version"
    );

    // A stale delivery (older source timestamp) must succeed at the object
    // level but must NOT overwrite the newer tags.
    inbound_replication_put(&client, bucket, key, &version_id, "site=stale", T_STALE).await?;
    assert_eq!(
        tag_value(&client, bucket, key, &version_id, "site").await.as_deref(),
        Some("local"),
        "a stale inbound delivery must not overwrite newer tags (rustfs/backlog#1953)"
    );

    // A newer delivery still converges the version onto the newest state.
    inbound_replication_put(&client, bucket, key, &version_id, "site=newer", T_NEWER).await?;
    assert_eq!(
        tag_value(&client, bucket, key, &version_id, "site").await.as_deref(),
        Some("newer"),
        "a newer inbound delivery must overwrite older tags"
    );

    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await?;
    env.delete_test_bucket(bucket).await.ok();
    Ok(())
}
