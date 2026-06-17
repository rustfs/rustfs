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

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Result;
use crate::store::ECStore;
use crate::store_api::{ListOperations, ObjectInfo, ObjectInfoOrErr, WalkOptions};
use rustfs_storage_api::{BucketOperations, BucketOptions};

pub const DEFAULT_FREE_VERSION_RECOVERY_LIMIT: usize = 1_000;
const DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeVersionRecoveryStats {
    pub scanned: usize,
    pub enqueued: usize,
    pub failed: usize,
    pub next_bucket_marker: Option<String>,
    pub next_object_marker: Option<String>,
    pub scanned_entries: usize,
    pub buckets_scanned: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct FreeVersionRecoveryPage {
    pub items: Vec<ObjectInfo>,
    pub next_bucket_marker: Option<String>,
    pub next_object_marker: Option<String>,
    pub scanned_entries: usize,
    pub buckets_scanned: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryCursor {
    bucket: String,
    object: String,
}

pub async fn recover_tier_free_versions(
    api: Arc<ECStore>,
    limit: usize,
    bucket_marker: Option<String>,
    object_marker: Option<String>,
) -> Result<FreeVersionRecoveryStats> {
    if limit == 0 {
        return Err(std::io::Error::other("free-version recovery limit must be greater than zero").into());
    }

    let page = list_tier_free_versions(api, limit, bucket_marker.clone(), object_marker.clone()).await?;
    let mut stats = FreeVersionRecoveryStats {
        scanned: 0,
        enqueued: 0,
        failed: 0,
        next_bucket_marker: page.next_bucket_marker,
        next_object_marker: page.next_object_marker,
        scanned_entries: page.scanned_entries,
        buckets_scanned: page.buckets_scanned,
        truncated: page.truncated,
    };

    let mut retry_cursor = RetryCursor::new(bucket_marker, object_marker);
    for oi in page.items {
        retry_cursor.visit(&oi);
        if !record_recovered_free_version_enqueue(&mut stats, queue_recovered_free_version(oi).await) {
            let (bucket_marker, object_marker) = retry_cursor.retry_markers();
            stats.truncated = true;
            stats.next_bucket_marker = bucket_marker;
            stats.next_object_marker = object_marker;
            break;
        }
    }

    Ok(stats)
}

fn record_recovered_free_version_enqueue(stats: &mut FreeVersionRecoveryStats, queued: bool) -> bool {
    stats.scanned += 1;
    if queued {
        stats.enqueued += 1;
        true
    } else {
        stats.failed += 1;
        false
    }
}

async fn queue_recovered_free_version(oi: ObjectInfo) -> bool {
    crate::bucket::lifecycle::bucket_lifecycle_ops::enqueue_recovered_free_version(oi).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetryCursor {
    input_bucket_marker: Option<String>,
    input_object_marker: Option<String>,
    current: Option<RecoveryCursor>,
    completed: Option<RecoveryCursor>,
}

impl RetryCursor {
    fn new(input_bucket_marker: Option<String>, input_object_marker: Option<String>) -> Self {
        Self {
            input_bucket_marker,
            input_object_marker,
            current: None,
            completed: None,
        }
    }

    fn visit(&mut self, oi: &ObjectInfo) {
        let cursor = RecoveryCursor {
            bucket: oi.bucket.clone(),
            object: oi.name.clone(),
        };
        if self.current.as_ref() == Some(&cursor) {
            return;
        }
        if let Some(previous) = self.current.replace(cursor) {
            self.completed = Some(previous);
        }
    }

    fn retry_markers(&self) -> (Option<String>, Option<String>) {
        if let Some(completed) = &self.completed {
            return (Some(completed.bucket.clone()), Some(completed.object.clone()));
        }
        if let Some(current) = &self.current {
            if self.input_bucket_marker.as_deref() == Some(current.bucket.as_str()) {
                return (Some(current.bucket.clone()), self.input_object_marker.clone());
            }
            return (Some(current.bucket.clone()), None);
        }
        (self.input_bucket_marker.clone(), self.input_object_marker.clone())
    }
}

async fn list_tier_free_versions(
    api: Arc<ECStore>,
    limit: usize,
    bucket_marker: Option<String>,
    object_marker: Option<String>,
) -> Result<FreeVersionRecoveryPage> {
    let mut page = FreeVersionRecoveryPage {
        items: Vec::new(),
        next_bucket_marker: None,
        next_object_marker: None,
        scanned_entries: 0,
        buckets_scanned: 0,
        truncated: false,
    };

    if limit == 0 {
        return Ok(page);
    }

    let buckets = api.list_bucket(&BucketOptions::default()).await?;
    let mut bucket_seen = bucket_marker.is_none();
    let mut truncated_after: Option<RecoveryCursor> = None;
    let walk_scan_limit = recovery_walk_scan_limit(limit);

    for bucket in buckets {
        if bucket.name == RUSTFS_META_BUCKET {
            continue;
        }
        if !bucket_seen {
            if bucket_marker.as_deref() == Some(bucket.name.as_str()) {
                bucket_seen = true;
            } else {
                continue;
            }
        }

        page.buckets_scanned += 1;
        let bucket_object_marker = if bucket_marker.as_deref() == Some(bucket.name.as_str()) {
            object_marker.clone()
        } else {
            None
        };

        let (tx, mut rx) = mpsc::channel::<ObjectInfoOrErr>(100);
        let cancel = CancellationToken::new();
        let mut draining_after_truncation = false;
        let mut last_seen_object: Option<String> = None;
        let mut scanned_objects = 0usize;
        let walk = tokio::spawn({
            let api = api.clone();
            let bucket_name = bucket.name.clone();
            let object_marker = bucket_object_marker.clone();
            let cancel = cancel.clone();
            async move {
                api.walk(
                    cancel,
                    &bucket_name,
                    "",
                    tx,
                    WalkOptions {
                        include_free_versions: true,
                        limit: walk_scan_limit,
                        marker: object_marker,
                        ..Default::default()
                    },
                )
                .await
            }
        });

        while let Some(item) = rx.recv().await {
            page.scanned_entries += 1;
            if draining_after_truncation {
                continue;
            }
            if let Some(err) = item.err {
                cancel.cancel();
                walk.await.map_err(|err| std::io::Error::other(err.to_string()))??;
                return Err(err);
            }
            let Some(oi) = item.item else {
                continue;
            };
            record_scanned_object(&mut last_seen_object, &mut scanned_objects, &oi.name);
            if let Some(cursor) = &truncated_after
                && cursor.object != oi.name
            {
                page.truncated = true;
                cancel.cancel();
                draining_after_truncation = true;
                continue;
            }
            if is_recoverable_tier_free_version(&oi) {
                let cursor = RecoveryCursor {
                    bucket: bucket.name.clone(),
                    object: oi.name.clone(),
                };
                page.items.push(oi);
                page.next_bucket_marker = Some(cursor.bucket.clone());
                page.next_object_marker = Some(cursor.object.clone());
                if page.items.len() >= limit && truncated_after.is_none() {
                    truncated_after = Some(cursor);
                }
            }
        }

        walk.await.map_err(|err| std::io::Error::other(err.to_string()))??;
        mark_scan_truncated_if_needed(&mut page, scanned_objects, walk_scan_limit, &bucket.name, last_seen_object.as_deref());

        if page.truncated {
            page.next_bucket_marker = Some(bucket.name.clone());
            break;
        }
    }

    if !page.truncated {
        page.next_bucket_marker = None;
        page.next_object_marker = None;
    }

    Ok(page)
}

fn recovery_walk_scan_limit(limit: usize) -> usize {
    DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT.max(limit.saturating_add(1))
}

fn record_scanned_object(last_seen_object: &mut Option<String>, scanned_objects: &mut usize, object: &str) {
    if last_seen_object.as_deref() == Some(object) {
        return;
    }
    *last_seen_object = Some(object.to_string());
    *scanned_objects = scanned_objects.saturating_add(1);
}

fn mark_scan_truncated_if_needed(
    page: &mut FreeVersionRecoveryPage,
    scanned_objects: usize,
    walk_scan_limit: usize,
    bucket: &str,
    last_seen_object: Option<&str>,
) {
    if page.truncated || scanned_objects < walk_scan_limit {
        return;
    }
    if let Some(last_seen_object) = last_seen_object {
        page.truncated = true;
        page.next_bucket_marker = Some(bucket.to_string());
        page.next_object_marker = Some(last_seen_object.to_string());
    }
}

fn is_recoverable_tier_free_version(oi: &ObjectInfo) -> bool {
    oi.transitioned_object.free_version
        && !oi.transitioned_object.name.is_empty()
        && !oi.transitioned_object.version_id.is_empty()
        && !oi.transitioned_object.tier.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_stats_count_failed_enqueue_without_treating_scan_as_lost() {
        let mut stats = FreeVersionRecoveryStats {
            scanned: 0,
            enqueued: 0,
            failed: 0,
            next_bucket_marker: None,
            next_object_marker: None,
            scanned_entries: 1,
            buckets_scanned: 1,
            truncated: false,
        };

        record_recovered_free_version_enqueue(&mut stats, false);

        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.enqueued, 0);
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn recovery_enqueue_failure_returns_false_so_cursor_can_retry_same_object() {
        let mut stats = FreeVersionRecoveryStats {
            scanned: 0,
            enqueued: 0,
            failed: 0,
            next_bucket_marker: Some("bucket".to_string()),
            next_object_marker: Some("object".to_string()),
            scanned_entries: 1,
            buckets_scanned: 1,
            truncated: false,
        };

        let queued = record_recovered_free_version_enqueue(&mut stats, false);

        assert!(!queued);
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn recover_tier_free_versions_failed_enqueue_retries_same_persisted_cursor() {
        let mut stats = FreeVersionRecoveryStats {
            scanned: 0,
            enqueued: 0,
            failed: 0,
            next_bucket_marker: Some("bucket".to_string()),
            next_object_marker: Some("object-a".to_string()),
            scanned_entries: 1,
            buckets_scanned: 1,
            truncated: false,
        };
        let mut cursor = RetryCursor::new(stats.next_bucket_marker.clone(), stats.next_object_marker.clone());
        let oi = object_info("bucket", "object-b");

        cursor.visit(&oi);
        let queued = record_recovered_free_version_enqueue(&mut stats, false);
        let (bucket_marker, object_marker) = cursor.retry_markers();

        assert!(!queued);
        assert_eq!(bucket_marker, Some("bucket".to_string()));
        assert_eq!(object_marker, Some("object-a".to_string()));
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn retry_cursor_rewinds_to_previous_object_on_enqueue_failure() {
        let mut cursor = RetryCursor::new(Some("bucket".to_string()), Some("object-a".to_string()));
        cursor.visit(&object_info("bucket", "object-b"));
        cursor.visit(&object_info("bucket", "object-b"));

        assert_eq!(cursor.retry_markers(), (Some("bucket".to_string()), Some("object-a".to_string())));

        cursor.visit(&object_info("bucket", "object-c"));

        assert_eq!(cursor.retry_markers(), (Some("bucket".to_string()), Some("object-b".to_string())));
    }

    #[test]
    fn retry_cursor_rewinds_to_bucket_start_when_first_bucket_object_fails() {
        let mut cursor = RetryCursor::new(None, None);
        cursor.visit(&object_info("bucket", "object-a"));

        assert_eq!(cursor.retry_markers(), (Some("bucket".to_string()), None));
    }

    #[test]
    fn recoverable_tier_free_version_requires_complete_remote_tuple() {
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.free_version = true;
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transitioned_object.tier = "WARM".to_string();

        assert!(is_recoverable_tier_free_version(&oi));

        oi.transitioned_object.version_id.clear();
        assert!(!is_recoverable_tier_free_version(&oi));
    }

    #[test]
    fn recovery_scan_limit_is_independent_from_enqueue_limit() {
        assert_eq!(recovery_walk_scan_limit(1), DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT);
        assert_eq!(
            recovery_walk_scan_limit(DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT),
            DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT + 1
        );
    }

    #[test]
    fn scan_truncation_keeps_marker_after_nonrecoverable_window() {
        let mut page = FreeVersionRecoveryPage {
            items: Vec::new(),
            next_bucket_marker: None,
            next_object_marker: None,
            scanned_entries: DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT,
            buckets_scanned: 1,
            truncated: false,
        };

        mark_scan_truncated_if_needed(
            &mut page,
            DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT,
            DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT,
            "bucket",
            Some("object-z"),
        );

        assert!(page.truncated);
        assert_eq!(page.next_bucket_marker, Some("bucket".to_string()));
        assert_eq!(page.next_object_marker, Some("object-z".to_string()));
    }

    #[test]
    fn scanned_object_count_advances_only_on_new_objects() {
        let mut last_seen = None;
        let mut scanned_objects = 0;

        record_scanned_object(&mut last_seen, &mut scanned_objects, "object-a");
        record_scanned_object(&mut last_seen, &mut scanned_objects, "object-a");
        record_scanned_object(&mut last_seen, &mut scanned_objects, "object-b");

        assert_eq!(scanned_objects, 2);
        assert_eq!(last_seen, Some("object-b".to_string()));
    }

    fn object_info(bucket: &str, object: &str) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            ..Default::default()
        }
    }
}
