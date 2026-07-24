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
#[cfg(test)]
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::bucket::lifecycle::bucket_lifecycle_ops::enqueue_recovered_free_version;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Result;
use crate::object_api::ObjectInfo;
use crate::storage_api_contracts::{
    bucket::{BucketOperations, BucketOptions},
    list::{ListOperations as _, StorageObjectInfoOrErr, StorageWalkOptions},
};
use crate::store::ECStore;
use rustfs_filemeta::FileInfo;

pub const DEFAULT_FREE_VERSION_RECOVERY_LIMIT: usize = 1_000;
const DEFAULT_FREE_VERSION_RECOVERY_SCAN_LIMIT: usize = 10_000;
#[cfg(not(test))]
const BACKGROUND_WALK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const BACKGROUND_WALK_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);

type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, crate::error::Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

fn recovery_walk_options(limit: usize, marker: Option<String>) -> WalkOptions {
    WalkOptions {
        include_free_versions: true,
        limit,
        marker,
        // Total walk time scales with bucket size, so it is left unbounded
        // (Duration::ZERO disables the wall-clock budget). Per-call progress
        // stalls stay bounded by the drive-level stall budget inherited from
        // `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`.
        walkdir_timeout: Some(Duration::ZERO),
        walkdir_stall_timeout: None,
        ..Default::default()
    }
}

#[cfg(test)]
pub(super) enum RecoveryWalkTestAction {
    SendItemsThenError(Vec<ObjectInfo>, crate::error::Error),
    SendItemsThenHang(Vec<ObjectInfo>, Arc<tokio::sync::Notify>),
    SendItemsUntilReceiverCloses(Arc<tokio::sync::Notify>),
    ReturnError(crate::error::Error),
    WaitForCancellation(Arc<tokio::sync::Notify>),
}

#[cfg(test)]
type RecoveryWalkTestHook = Box<dyn Fn(&str) -> Option<RecoveryWalkTestAction> + Send + Sync>;

#[cfg(test)]
static RECOVERY_WALK_TEST_HOOK: Mutex<Option<RecoveryWalkTestHook>> = Mutex::new(None);

#[cfg(test)]
static RECOVERY_BUCKET_LIST_WAIT_HOOK: Mutex<Option<Arc<tokio::sync::Notify>>> = Mutex::new(None);

#[cfg(test)]
pub(super) struct RecoveryWalkHookGuard;

#[cfg(test)]
impl Drop for RecoveryWalkHookGuard {
    fn drop(&mut self) {
        let mut hook = RECOVERY_WALK_TEST_HOOK
            .lock()
            .expect("recovery walk test hook lock should not poison");
        *hook = None;
    }
}

#[cfg(test)]
pub(super) fn set_recovery_walk_test_hook(
    hook_fn: impl Fn(&str) -> Option<RecoveryWalkTestAction> + Send + Sync + 'static,
) -> RecoveryWalkHookGuard {
    let mut hook = RECOVERY_WALK_TEST_HOOK
        .lock()
        .expect("recovery walk test hook lock should not poison");
    *hook = Some(Box::new(hook_fn));
    RecoveryWalkHookGuard
}

#[cfg(test)]
pub(super) struct RecoveryBucketListWaitHookGuard;

#[cfg(test)]
impl Drop for RecoveryBucketListWaitHookGuard {
    fn drop(&mut self) {
        let mut hook = RECOVERY_BUCKET_LIST_WAIT_HOOK
            .lock()
            .expect("recovery bucket-list test hook lock should not poison");
        *hook = None;
    }
}

#[cfg(test)]
pub(super) fn set_recovery_bucket_list_wait_hook(started: Arc<tokio::sync::Notify>) -> RecoveryBucketListWaitHookGuard {
    let mut hook = RECOVERY_BUCKET_LIST_WAIT_HOOK
        .lock()
        .expect("recovery bucket-list test hook lock should not poison");
    *hook = Some(started);
    RecoveryBucketListWaitHookGuard
}

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
    recover_tier_free_versions_with_cancel(api, limit, bucket_marker, object_marker, CancellationToken::new()).await
}

pub(super) async fn recover_tier_free_versions_with_cancel(
    api: Arc<ECStore>,
    limit: usize,
    bucket_marker: Option<String>,
    object_marker: Option<String>,
    cancel_token: CancellationToken,
) -> Result<FreeVersionRecoveryStats> {
    if limit == 0 {
        return Err(std::io::Error::other("free-version recovery limit must be greater than zero").into());
    }

    let page = list_tier_free_versions(api, limit, bucket_marker.clone(), object_marker.clone(), cancel_token.clone()).await?;
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
        if cancel_token.is_cancelled() {
            return Err(tier_free_version_recovery_cancelled());
        }
        retry_cursor.visit(&oi);
        if !record_recovered_free_version_enqueue(&mut stats, enqueue_recovered_free_version(oi).await) {
            let (bucket_marker, object_marker) = retry_cursor.retry_markers();
            stats.truncated = true;
            stats.next_bucket_marker = bucket_marker;
            stats.next_object_marker = object_marker;
            break;
        }
    }

    Ok(stats)
}

fn tier_free_version_recovery_cancelled() -> crate::error::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "tier free-version recovery cancelled").into()
}

fn tier_free_version_recovery_walk_shutdown_timed_out() -> crate::error::Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "tier free-version recovery walk did not stop after cancellation",
    )
    .into()
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

pub(super) async fn list_tier_free_versions(
    api: Arc<ECStore>,
    limit: usize,
    bucket_marker: Option<String>,
    object_marker: Option<String>,
    cancel_token: CancellationToken,
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

    let bucket_options = BucketOptions::default();
    let list_buckets = async {
        #[cfg(test)]
        let wait_hook = RECOVERY_BUCKET_LIST_WAIT_HOOK
            .lock()
            .expect("recovery bucket-list test hook lock should not poison")
            .clone();
        #[cfg(test)]
        if let Some(started) = wait_hook {
            started.notify_one();
            std::future::pending::<()>().await;
        }
        api.list_bucket(&bucket_options).await
    };
    tokio::pin!(list_buckets);
    let buckets = tokio::select! {
        biased;
        _ = cancel_token.cancelled() => return Err(tier_free_version_recovery_cancelled()),
        result = &mut list_buckets => result?,
    };
    let mut bucket_seen = bucket_marker.is_none();
    let mut truncated_after: Option<RecoveryCursor> = None;
    let walk_scan_limit = recovery_walk_scan_limit(limit);

    for bucket in buckets {
        if cancel_token.is_cancelled() {
            return Err(tier_free_version_recovery_cancelled());
        }
        if bucket.name == RUSTFS_META_BUCKET {
            continue;
        }
        if !bucket_seen {
            if bucket_marker.as_deref().is_some_and(|marker| bucket.name.as_str() < marker) {
                continue;
            }
            bucket_seen = true;
        }

        page.buckets_scanned += 1;
        let bucket_object_marker = if bucket_marker.as_deref() == Some(bucket.name.as_str()) {
            object_marker.clone()
        } else {
            None
        };

        let (tx, mut rx) = mpsc::channel::<ObjectInfoOrErr>(100);
        let cancel = cancel_token.child_token();
        let mut draining_after_truncation = false;
        let mut drain_deadline = None;
        let mut last_seen_object: Option<String> = None;
        let mut scanned_objects = 0usize;
        let mut walk = tokio::spawn({
            let api = api.clone();
            let bucket_name = bucket.name.clone();
            let object_marker = bucket_object_marker.clone();
            let cancel = cancel.clone();
            async move {
                #[cfg(test)]
                let test_action = {
                    let hook = RECOVERY_WALK_TEST_HOOK
                        .lock()
                        .expect("recovery walk test hook lock should not poison");
                    hook.as_ref().and_then(|hook| hook(&bucket_name))
                };
                #[cfg(test)]
                if let Some(action) = test_action {
                    match action {
                        RecoveryWalkTestAction::SendItemsThenError(items, err) => {
                            for item in items {
                                if tx
                                    .send(ObjectInfoOrErr {
                                        item: Some(item),
                                        err: None,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            let _ = tx
                                .send(ObjectInfoOrErr {
                                    item: None,
                                    err: Some(err),
                                })
                                .await;
                            return Ok(());
                        }
                        RecoveryWalkTestAction::SendItemsThenHang(items, started) => {
                            for item in items {
                                if tx
                                    .send(ObjectInfoOrErr {
                                        item: Some(item),
                                        err: None,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            started.notify_one();
                            return std::future::pending().await;
                        }
                        RecoveryWalkTestAction::SendItemsUntilReceiverCloses(started) => {
                            started.notify_one();
                            let mut index = 0usize;
                            loop {
                                if tx
                                    .send(ObjectInfoOrErr {
                                        item: Some(ObjectInfo {
                                            bucket: bucket_name.clone(),
                                            name: format!("nonrecoverable-{index:08}"),
                                            ..Default::default()
                                        }),
                                        err: None,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                                index = index.saturating_add(1);
                            }
                        }
                        RecoveryWalkTestAction::ReturnError(err) => return Err(err),
                        RecoveryWalkTestAction::WaitForCancellation(started) => {
                            started.notify_one();
                            cancel.cancelled().await;
                            return Err(tier_free_version_recovery_cancelled());
                        }
                    }
                }

                api.walk(cancel, &bucket_name, "", tx, recovery_walk_options(walk_scan_limit, object_marker))
                    .await
            }
        });

        let mut receive_error = None;
        loop {
            let item = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    cancel.cancel();
                    receive_error = Some(tier_free_version_recovery_cancelled());
                    break;
                }
                _ = async {
                    if let Some(deadline) = drain_deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if drain_deadline.is_some() => {
                    receive_error = Some(tier_free_version_recovery_walk_shutdown_timed_out());
                    break;
                }
                item = rx.recv() => match item {
                    Some(item) => item,
                    None => break,
                },
            };
            page.scanned_entries += 1;
            if let Some(err) = item.err {
                cancel.cancel();
                receive_error = Some(err);
                break;
            }
            if draining_after_truncation {
                continue;
            }
            let Some(oi) = item.item else {
                continue;
            };
            record_scanned_object(&mut last_seen_object, &mut scanned_objects, &oi.name);
            if let Some(cursor) = &truncated_after
                && (cursor.bucket.as_str() != bucket.name.as_str() || cursor.object.as_str() != oi.name.as_str())
            {
                page.truncated = true;
                cancel.cancel();
                draining_after_truncation = true;
                drain_deadline = Some(tokio::time::Instant::now() + BACKGROUND_WALK_SHUTDOWN_TIMEOUT);
                continue;
            }
            if is_recoverable_tier_free_version(&oi) {
                let current_cursor = RecoveryCursor {
                    bucket: bucket.name.clone(),
                    object: oi.name.clone(),
                };
                page.items.push(oi);
                page.next_bucket_marker = Some(current_cursor.bucket.clone());
                page.next_object_marker = Some(current_cursor.object.clone());
                if page.items.len() >= limit && truncated_after.is_none() {
                    truncated_after = Some(current_cursor);
                }
            }
        }

        drop(rx);
        let walk_shutdown_timeout = drain_deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
            .unwrap_or(BACKGROUND_WALK_SHUTDOWN_TIMEOUT);
        let walk_result = match tokio::time::timeout(walk_shutdown_timeout, &mut walk).await {
            Ok(result) => result.map_err(|err| std::io::Error::other(err.to_string()))?,
            Err(_) => {
                walk.abort();
                let _ = walk.await;
                if let Some(err) = receive_error {
                    return Err(err);
                }
                return Err(tier_free_version_recovery_walk_shutdown_timed_out());
            }
        };
        if let Some(err) = receive_error {
            return Err(err);
        }
        walk_result?;
        mark_scan_truncated_if_needed(&mut page, scanned_objects, walk_scan_limit, &bucket.name, last_seen_object.as_deref());

        if page.truncated {
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
    // A free version transitioned to an unversioned remote tier legally carries an
    // empty `version_id` (see CLAUDE.md: a tier version of `None`/`""` means the
    // tier bucket is unversioned). The worker path handles that by issuing a
    // versionless remote delete, so the recovery gate must not treat an empty
    // `version_id` as unrecoverable — the object name and tier are sufficient to
    // identify a free version that still needs remote cleanup.
    oi.transitioned_object.free_version && !oi.transitioned_object.name.is_empty() && !oi.transitioned_object.tier.is_empty()
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

        // An unversioned remote tier records an empty `version_id`; such a free
        // version must still be recoverable so it can be re-enqueued for a
        // versionless remote delete instead of leaking forever.
        oi.transitioned_object.version_id.clear();
        assert!(is_recoverable_tier_free_version(&oi));

        // The object name and tier are the load-bearing fields; missing either
        // still marks the entry as unrecoverable.
        let mut missing_name = oi.clone();
        missing_name.transitioned_object.name.clear();
        assert!(!is_recoverable_tier_free_version(&missing_name));

        let mut missing_tier = oi.clone();
        missing_tier.transitioned_object.tier.clear();
        assert!(!is_recoverable_tier_free_version(&missing_tier));
    }

    #[test]
    fn recoverable_tier_free_version_rejects_non_free_version() {
        // Relaxing the empty-version_id gate must not admit ordinary (non-free)
        // versions into the remote-cleanup recovery path.
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.free_version = false;
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transitioned_object.tier = "WARM".to_string();

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
    fn recovery_walk_disables_total_timeout_and_inherits_stall_timeout() {
        let opts = recovery_walk_options(123, Some("marker".to_string()));

        assert_eq!(opts.limit, 123);
        assert_eq!(opts.marker.as_deref(), Some("marker"));
        assert_eq!(opts.walkdir_timeout, Some(Duration::ZERO));
        assert_eq!(opts.walkdir_stall_timeout, None);
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
