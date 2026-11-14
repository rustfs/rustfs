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

//! Object tagging for an object missing locally, served by a replication target that already has
//! it (MinIO `proxyGetTaggingToRepTarget` / `proxyTaggingToRepTarget`, backlog#1675 P1-5).
//!
//! Responsible for: re-sending the tagging read, write or delete to each eligible replication
//! target through the AWS SDK until one has the object, and the proxy metrics.
//! NOT responsible for: deciding whether proxying is allowed for the request (the caller checks
//! that no tag-conditioned authorization ran), or any local tagging.
//! Upstream: the tagging use cases, and the legacy PutObjectTagging edge until it moves here.
//! Downstream: the replication target clients.

use crate::app::storage_api::object_usecase::StorageObjectOptions as ObjectOptions;
use crate::app::storage_api::object_usecase::bucket::replication::{get_read_proxy_targets, record_replication_proxy};
use crate::app::storage_api::object_usecase::options::get_opts;
use http::HeaderMap;
use rustfs_gateway_types::dto::{GetObjectTaggingOutput, Tag};
use tracing::{debug, warn};

/// A raw 404 covers NoSuchKey and NoSuchVersion alike; the caller then tries the next target.
fn is_not_found<E>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    err.raw_response().is_some_and(|resp| resp.status().as_u16() == 404)
}

/// Selector options for a proxy. `get_opts` applies the anti-loop `source-proxy-request` header
/// family and the bucket's version-suspension state exactly as GET and HEAD do.
async fn proxy_opts(bucket: &str, object: &str, version_id: Option<String>, headers: &HeaderMap) -> Option<ObjectOptions> {
    get_opts(bucket, object, version_id, None, headers).await.ok()
}

/// The tag set of `object` from the first replication target that has it; `None` when none does.
pub(crate) async fn proxy_get_object_tagging(
    bucket: &str,
    object: &str,
    version_id: Option<String>,
    headers: &HeaderMap,
) -> Option<GetObjectTaggingOutput> {
    let opts = proxy_opts(bucket, object, version_id, headers).await?;
    let targets = get_read_proxy_targets(bucket, object, &opts).await;
    if targets.is_empty() {
        return None;
    }
    for target in targets {
        match target
            .get_object_tagging(&target.bucket, object, opts.version_id.clone())
            .await
        {
            Ok(remote) => {
                // MinIO-aligned accounting: one total per proxy attempt, one failed when no target
                // served it.
                record_replication_proxy(bucket, "GetObjectTagging", false).await;
                return Some(GetObjectTaggingOutput {
                    tag_set: remote
                        .tag_set
                        .into_iter()
                        .map(|tag| Tag {
                            key: Some(tag.key),
                            value: Some(tag.value),
                        })
                        .collect(),
                    version_id: remote.version_id,
                });
            }
            Err(err) if is_not_found(&err) => {
                debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
            }
            Err(err) => {
                warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: GetObjectTagging against replication target failed");
            }
        }
    }
    record_replication_proxy(bucket, "GetObjectTagging", true).await;
    None
}

/// Applies `tag_set` to `object` on the first replication target that has it.
pub(crate) async fn proxy_put_object_tagging(
    bucket: &str,
    object: &str,
    version_id: Option<String>,
    headers: &HeaderMap,
    tag_set: &[Tag],
) -> Option<()> {
    let opts = proxy_opts(bucket, object, version_id, headers).await?;
    let mut tagging = aws_sdk_s3::types::Tagging::builder();
    for tag in tag_set {
        let sdk_tag = aws_sdk_s3::types::Tag::builder()
            .key(tag.key.clone().unwrap_or_default())
            .value(tag.value.clone().unwrap_or_default())
            .build()
            .ok()?;
        tagging = tagging.tag_set(sdk_tag);
    }
    let tagging = tagging.build().ok()?;
    let targets = get_read_proxy_targets(bucket, object, &opts).await;
    if targets.is_empty() {
        return None;
    }
    for target in targets {
        match target
            .put_object_tagging(&target.bucket, object, opts.version_id.clone(), tagging.clone())
            .await
        {
            Ok(_) => {
                record_replication_proxy(bucket, "PutObjectTagging", false).await;
                return Some(());
            }
            Err(err) if is_not_found(&err) => {
                debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
            }
            Err(err) => {
                warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: PutObjectTagging against replication target failed");
            }
        }
    }
    record_replication_proxy(bucket, "PutObjectTagging", true).await;
    None
}

/// Deletes the tags of `object` on the first replication target that has it.
pub(crate) async fn proxy_delete_object_tagging(
    bucket: &str,
    object: &str,
    version_id: Option<String>,
    headers: &HeaderMap,
) -> Option<()> {
    let opts = proxy_opts(bucket, object, version_id, headers).await?;
    let targets = get_read_proxy_targets(bucket, object, &opts).await;
    if targets.is_empty() {
        return None;
    }
    for target in targets {
        match target
            .delete_object_tagging(&target.bucket, object, opts.version_id.clone())
            .await
        {
            Ok(_) => {
                record_replication_proxy(bucket, "DeleteObjectTagging", false).await;
                return Some(());
            }
            Err(err) if is_not_found(&err) => {
                debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
            }
            Err(err) => {
                warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: DeleteObjectTagging against replication target failed");
            }
        }
    }
    record_replication_proxy(bucket, "DeleteObjectTagging", true).await;
    None
}
