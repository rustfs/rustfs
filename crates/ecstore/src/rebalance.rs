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

use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REBALANCE: &str = "rebalance";
const EVENT_REBALANCE_STATE: &str = "rebalance_state";
const EVENT_REBALANCE_BUCKET: &str = "rebalance_bucket";
const EVENT_REBALANCE_ENTRY: &str = "rebalance_entry";
const EVENT_REBALANCE_LISTING: &str = "rebalance_listing";

const REBAL_META_FMT: u16 = 1; // Replace with actual format value
const REBAL_META_VER: u16 = 1; // Replace with actual version value
const REBAL_META_NAME: &str = "rebalance.bin";
const DEFAULT_REBALANCE_MAX_ATTEMPTS: usize = 3;
const REBALANCE_MAX_ATTEMPTS_ENV: &str = "RUSTFS_REBALANCE_MAX_ATTEMPTS";
const REBALANCE_LISTING_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_LOCK_RETRY_CAP: Duration = Duration::from_secs(10);
const REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX: &str = "deferred transient rebalance entry failure:";

mod control;
mod entry;
mod meta;
mod migration;
mod runtime;
mod worker;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RebalanceStats {
    #[serde(rename = "ifs")]
    pub init_free_space: u64, // Pool free space at the start of rebalance
    #[serde(rename = "ic")]
    pub init_capacity: u64, // Pool capacity at the start of rebalance
    #[serde(rename = "bus")]
    pub buckets: Vec<String>, // Buckets being rebalanced or to be rebalanced
    #[serde(rename = "rbs")]
    pub rebalanced_buckets: Vec<String>, // Buckets rebalanced
    #[serde(rename = "bu")]
    pub bucket: String, // Last rebalanced bucket
    #[serde(rename = "ob")]
    pub object: String, // Last rebalanced object
    #[serde(rename = "no")]
    pub num_objects: u64, // Number of objects rebalanced
    #[serde(rename = "nv")]
    pub num_versions: u64, // Number of versions rebalanced
    #[serde(rename = "bs")]
    pub bytes: u64, // Number of bytes rebalanced
    #[serde(rename = "par")]
    pub participating: bool, // Whether the pool is participating in rebalance
    #[serde(rename = "inf")]
    pub info: RebalanceInfo, // Rebalance operation info
    #[serde(rename = "cw", default)]
    pub cleanup_warnings: RebalanceCleanupWarnings,
}

pub type RStats = Vec<Arc<RebalanceStats>>;

#[derive(Debug, Default)]
struct RebalanceBucketConfigs {
    lifecycle_config: Option<s3s::dto::BucketLifecycleConfiguration>,
    lock_retention: Option<s3s::dto::DefaultRetention>,
    replication_config: Option<(s3s::dto::ReplicationConfiguration, OffsetDateTime)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RebalanceBucketOutcome {
    Completed,
    Deferred { last_error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RebalanceEntryOutcome {
    Completed,
    Deferred { last_error: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalStatus {
    #[default]
    None,
    Started,
    Completed,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalSaveOpt {
    #[default]
    Stats,
    StoppedAt,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RebalanceInfo {
    #[serde(rename = "startTs")]
    pub start_time: Option<OffsetDateTime>, // Time at which rebalance-start was issued
    #[serde(rename = "stopTs")]
    pub end_time: Option<OffsetDateTime>, // Time at which rebalance operation completed or rebalance-stop was called
    #[serde(rename = "err")]
    pub last_error: Option<String>, // Last rebalance error message
    #[serde(rename = "status")]
    pub status: RebalStatus, // Current state of rebalance operation
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalanceCleanupWarnings {
    #[serde(rename = "count", default)]
    pub count: u64,
    #[serde(rename = "lastMsg", default)]
    pub last_message: Option<String>,
    #[serde(rename = "lastBucket", default)]
    pub last_bucket: Option<String>,
    #[serde(rename = "lastObject", default)]
    pub last_object: Option<String>,
    #[serde(rename = "lastAt", default)]
    pub last_at: Option<OffsetDateTime>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct DiskStat {
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct RebalanceMeta {
    #[serde(skip)]
    pub cancel: Option<CancellationToken>, // To be invoked on rebalance-stop
    #[serde(skip)]
    pub last_refreshed_at: Option<OffsetDateTime>,
    #[serde(rename = "stopTs")]
    pub stopped_at: Option<OffsetDateTime>, // Time when rebalance-stop was issued
    #[serde(rename = "id")]
    pub id: String, // ID of the ongoing rebalance operation
    #[serde(rename = "pf")]
    pub percent_free_goal: f64, // Computed from total free space and capacity at the start of rebalance
    #[serde(rename = "rss")]
    pub pool_stats: Vec<RebalanceStats>, // Per-pool rebalance stats keyed by pool index
}

#[cfg(test)]
mod rebalance_unit_tests {
    use super::REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX;
    use super::meta::{
        RebalanceTerminalEvent, apply_rebalance_save_option, apply_rebalance_terminal_event, apply_stopped_at,
        classify_rebalance_terminal_event, clone_arc_by_index, clone_first_arc, clone_rebalance_pool_stats,
        complete_rebalance_pools_at_goal, complete_rebalance_pools_with_empty_queue, defer_bucket_in_rebalance_queue,
        ensure_rebalance_not_decommissioning, ensure_valid_rebalance_pool_index, first_rebalance_bucket,
        has_deferred_rebalance_error, is_rebalance_actively_running, is_rebalance_conflicting_with_decommission,
        is_rebalance_in_progress, is_rebalance_stopped_terminal_event, mark_rebalance_bucket_done, merge_rebalance_bucket_lists,
        merge_rebalance_meta, next_rebal_bucket_from_stat, percent_free_ratio, rebalance_goal_reached,
        rebalance_meta_load_no_data_error, rebalance_meta_load_unknown_format_error, rebalance_meta_load_unknown_version_error,
        record_rebalance_cleanup_warning_in_meta, remove_rebalanced_buckets_from_queue, resolve_next_rebalance_bucket,
        resolve_rebalance_participants, should_accept_rebalance_stats_update, should_ignore_rebalance_data_usage_cache,
        should_pool_participate, should_preserve_rebalance_stopped_state, should_skip_start_rebalance,
        stop_rebalance_meta_snapshot, stop_rebalance_state, take_bucket_from_rebalance_queue, validate_start_rebalance_state,
    };
    use super::migration::{
        MigrationBackend, MigrationVersionResult, migrate_entry_version, migrate_entry_version_with_retry_wait,
        rebalance_delete_marker_opts,
    };
    use super::worker::{
        ensure_rebalance_listing_disks_available, is_transient_rebalance_error, load_rebalance_bucket_configs,
        parse_rebalance_max_attempts, rebalance_listing_retry_delay, rebalance_migration_retry_delay,
        resolve_load_rebalance_stats_update_result, resolve_rebalance_bucket_error, resolve_rebalance_bucket_result,
        resolve_rebalance_entry_cleanup_delete_result, resolve_rebalance_file_info_versions_result,
        resolve_rebalance_meta_load_result, resolve_rebalance_meta_save_result, resolve_rebalance_migrate_result_error,
        resolve_rebalance_optional_bucket_config_result, resolve_rebalance_save_task_result,
        resolve_rebalance_stats_update_result, resolve_rebalance_terminal_error, resolve_rebalance_worker_result,
        send_rebalance_done_signal, should_cleanup_rebalance_source_entry, should_count_rebalance_version_complete,
        should_defer_rebalance_entry_failure, should_retry_rebalance_listing, should_skip_rebalance_delete_marker,
        wait_rebalance_entry_tasks, wait_rebalance_listing_retry, with_rebalance_entry_context,
    };
    use super::{
        GetObjectReader, ObjectInfo, ObjectOptions, RebalSaveOpt, RebalStatus, RebalanceBucketOutcome, RebalanceCleanupWarnings,
        RebalanceEntryOutcome, RebalanceInfo, RebalanceMeta, RebalanceStats,
    };
    use crate::data_movement;
    use crate::data_usage::DATA_USAGE_CACHE_NAME;
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::disk::error::DiskError;
    use crate::error::{Error, Result};
    use rustfs_filemeta::FileInfo;
    use rustfs_filemeta::TRANSITION_COMPLETE;
    use rustfs_rio::Index;
    use rustfs_storage_api::HTTPRangeSpec;
    use s3s::dto::ReplicationConfiguration;
    use serde::Serialize;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;
    use tokio::sync::mpsc;
    use tokio::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Default, Serialize)]
    struct LegacyRebalanceStats {
        #[serde(rename = "ifs")]
        init_free_space: u64,
        #[serde(rename = "ic")]
        init_capacity: u64,
        #[serde(rename = "bus")]
        buckets: Vec<String>,
        #[serde(rename = "rbs")]
        rebalanced_buckets: Vec<String>,
        #[serde(rename = "bu")]
        bucket: String,
        #[serde(rename = "ob")]
        object: String,
        #[serde(rename = "no")]
        num_objects: u64,
        #[serde(rename = "nv")]
        num_versions: u64,
        #[serde(rename = "bs")]
        bytes: u64,
        #[serde(rename = "par")]
        participating: bool,
        #[serde(rename = "inf")]
        info: RebalanceInfo,
    }

    #[derive(Debug, Default, Serialize)]
    struct LegacyRebalanceMeta {
        #[serde(rename = "stopTs")]
        stopped_at: Option<OffsetDateTime>,
        #[serde(rename = "id")]
        id: String,
        #[serde(rename = "pf")]
        percent_free_goal: f64,
        #[serde(rename = "rss")]
        pool_stats: Vec<LegacyRebalanceStats>,
    }

    struct MigrationBackendSpy {
        get_object_reader: Mutex<Option<core::result::Result<GetObjectReader, Error>>>,
        delete_object: Mutex<Option<core::result::Result<ObjectInfo, Error>>>,
        move_remote: Mutex<Option<core::result::Result<(), Error>>>,
        get_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        move_remote_calls: AtomicUsize,
    }

    impl MigrationBackendSpy {
        fn new(
            get_object_reader: Option<core::result::Result<GetObjectReader, Error>>,
            delete_object: Option<core::result::Result<ObjectInfo, Error>>,
            move_remote: Option<core::result::Result<(), Error>>,
        ) -> Self {
            Self {
                get_object_reader: Mutex::new(get_object_reader),
                delete_object: Mutex::new(delete_object),
                move_remote: Mutex::new(move_remote),
                get_calls: AtomicUsize::new(0),
                delete_calls: AtomicUsize::new(0),
                move_remote_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }

        fn delete_calls(&self) -> usize {
            self.delete_calls.load(Ordering::SeqCst)
        }

        fn move_remote_calls(&self) -> usize {
            self.move_remote_calls.load(Ordering::SeqCst)
        }

        fn make_reader() -> GetObjectReader {
            GetObjectReader {
                stream: Box::new(Cursor::new(vec![0_u8; 3])),
                object_info: ObjectInfo::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl MigrationBackend for MigrationBackendSpy {
        async fn get_object_reader_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.get_object_reader.lock().unwrap().take() {
                return result;
            }

            Ok(Self::make_reader())
        }

        async fn delete_object_for_migration(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.delete_object.lock().unwrap().take() {
                return result;
            }

            Ok(ObjectInfo::default())
        }

        async fn move_remote_version_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &FileInfo,
            _opts: &ObjectOptions,
        ) -> Result<()> {
            self.move_remote_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.move_remote.lock().unwrap().take() {
                return result;
            }

            Ok(())
        }
    }

    fn version_deleted() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.deleted = true;
        version
    }

    fn version_normal() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.size = 64;
        version
    }

    fn version_remote() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.transition_status = TRANSITION_COMPLETE.to_string();
        version
    }

    #[test]
    fn test_rebalance_delete_marker_opts_preserves_replication_state() {
        let mod_time = OffsetDateTime::now_utc();
        let version = FileInfo {
            mod_time: Some(mod_time),
            replication_state_internal: Some(rustfs_filemeta::ReplicationState {
                replica_status: rustfs_filemeta::ReplicationStatusType::Replica,
                delete_marker: true,
                replicate_decision_str: "existing".to_string(),
                ..Default::default()
            }),
            ..version_deleted()
        };

        let opts = rebalance_delete_marker_opts(&version, Some("version-id".to_string()), 7);
        let replication = opts.delete_replication.expect("replication state should be preserved");

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert!(opts.delete_marker);
        assert!(opts.skip_decommissioned);
        assert_eq!(opts.src_pool_idx, 7);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(replication.replica_status, rustfs_filemeta::ReplicationStatusType::Replica);
        assert!(replication.delete_marker);
        assert_eq!(replication.replicate_decision_str, "existing");
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_version_is_moved_without_transfer() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), Some(Ok(())));
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_not_found_is_cleanup_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            Some(Ok(ObjectInfo::default())),
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
        );
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_overwrite_is_not_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            None,
            Some(Err(Error::DataMovementOverwriteErr(
                "bucket".to_string(),
                "object.bin".to_string(),
                "vid-1".to_string(),
            ))),
        );
        let version = version_remote();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            Some("vid-1".to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert_eq!(result.stage, Some("move_remote_version"));
        assert!(matches!(result.error, Some(Error::DataMovementOverwriteErr(_, _, _))));
        assert_eq!(backend.move_remote_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_failure_is_reported() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), Some(Err(Error::SlowDown)));
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_deleted_version_calls_delete_and_moved() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), None);
        let version = version_deleted();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_deleted_version_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
            None,
        );
        let version = version_deleted();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.delete_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_deleted_version_overwrite_is_not_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            Some(Err(Error::DataMovementOverwriteErr(
                "bucket".to_string(),
                "object.bin".to_string(),
                "vid-1".to_string(),
            ))),
            None,
        );
        let version = version_deleted();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            Some("vid-1".to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert_eq!(result.stage, Some("delete_marker"));
        assert!(matches!(result.error, Some(Error::DataMovementOverwriteErr(_, _, _))));
        assert_eq!(backend.delete_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
            None,
            None,
        );
        let version = version_normal();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_retries_before_success() {
        let backend = MigrationBackendSpy::new(Some(Err(Error::SlowDown)), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let wait_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version_with_retry_wait(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
            {
                let wait_count = wait_count.clone();
                move |_| {
                    let wait_count = wait_count.clone();
                    async move {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(backend.delete_calls(), 0);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
        assert_eq!(wait_count.load(Ordering::SeqCst), 1);
    }

    struct AlwaysFailGetBackend {
        get_calls: AtomicUsize,
    }

    impl AlwaysFailGetBackend {
        fn new() -> Self {
            Self {
                get_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl MigrationBackend for AlwaysFailGetBackend {
        async fn get_object_reader_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::SlowDown)
        }

        async fn delete_object_for_migration(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
            Ok(ObjectInfo::default())
        }

        async fn move_remote_version_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &FileInfo,
            _opts: &ObjectOptions,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_fails_after_retries() {
        let backend = AlwaysFailGetBackend::new();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(backend.get_calls(), 3);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_zero_max_attempts_still_attempts_once() {
        let backend = AlwaysFailGetBackend::new();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            0,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_retries_before_success() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let wait_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    let attempt = transfer_count.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        return Err(Error::SlowDown);
                    }
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version_with_retry_wait(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
            {
                let wait_count = wait_count.clone();
                move |_| {
                    let wait_count = wait_count.clone();
                    async move {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 2);
        assert_eq!(wait_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_non_transient_fails_without_retry() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let wait_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::FileAccessDenied)
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version_with_retry_wait(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
            {
                let wait_count = wait_count.clone();
                move |_| {
                    let wait_count = wait_count.clone();
                    async move {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert_eq!(result.stage, Some("write_target"));
        assert!(matches!(result.error, Some(Error::FileAccessDenied)));
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
        assert_eq!(wait_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_fails_after_retries() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::SlowDown)
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.error.is_some());
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_overwrite_is_not_ignored() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::DataMovementOverwriteErr(
                        "bucket".to_string(),
                        "object.bin".to_string(),
                        "vid-1".to_string(),
                    ))
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            Some("vid-1".to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert_eq!(result.stage, Some("write_target"));
        assert!(matches!(result.error, Some(Error::DataMovementOverwriteErr(_, _, _))));
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_ignores_data_usage_cache_when_enabled() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let version = {
            let mut version = version_normal();
            version.name = format!("{}.{}", DATA_USAGE_CACHE_NAME, version.name);
            version
        };
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            RUSTFS_META_BUCKET.to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            true,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_data_usage_cache_moves_when_ignore_disabled() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let version = {
            let mut version = version_normal();
            version.name = format!("{}.{}", DATA_USAGE_CACHE_NAME, version.name);
            version
        };
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            RUSTFS_META_BUCKET.to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[test]
    fn test_should_ignore_rebalance_data_usage_cache_true_for_meta_bucket() {
        assert!(should_ignore_rebalance_data_usage_cache(RUSTFS_META_BUCKET));
    }

    #[test]
    fn test_should_ignore_rebalance_data_usage_cache_false_for_regular_bucket() {
        assert!(!should_ignore_rebalance_data_usage_cache("bucket-a"));
    }

    #[test]
    fn test_rebalance_goal_reached_at_target() {
        let init_free_space = 200_u64;
        let init_capacity = 1_000_u64;
        let goal = 0.45_f64;

        assert!(rebalance_goal_reached(init_free_space, init_capacity, 250, goal));
        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 249, goal));
    }

    #[test]
    fn test_rebalance_goal_reached_above_target() {
        let init_free_space = 200_u64;
        let init_capacity = 1_000_u64;
        let bytes = 251_u64;
        let goal = 0.45_f64;

        assert!(rebalance_goal_reached(init_free_space, init_capacity, bytes, goal));
    }

    #[test]
    fn test_rebalance_goal_not_reached_outside_tolerance() {
        let init_free_space = 100_u64;
        let init_capacity = 1_000_u64;
        let bytes = 80_u64;
        let goal = 0.5_f64;

        assert!(!rebalance_goal_reached(init_free_space, init_capacity, bytes, goal));
    }

    #[test]
    fn test_rebalance_goal_zero_capacity_is_false() {
        assert!(!rebalance_goal_reached(100, 0, 50, 0.5));
    }

    #[test]
    fn test_rebalance_goal_above_one_is_true_when_reached() {
        assert!(rebalance_goal_reached(950, 1_000, 100, 1.0));
    }

    #[test]
    fn test_rebalance_goal_below_zero_is_true_when_reached() {
        assert!(rebalance_goal_reached(10, 1_000, 0, -0.01));
    }

    #[test]
    fn test_resolve_rebalance_worker_result_passthrough() {
        assert!(resolve_rebalance_worker_result(0, Ok(Ok(()))).is_ok());

        let err = resolve_rebalance_worker_result::<()>(0, Ok(Err(Error::OperationCanceled))).unwrap_err();
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[tokio::test]
    async fn test_resolve_rebalance_worker_result_join_error_keeps_context() {
        let join_error = tokio::spawn(async {
            panic!("rebalance worker panic");
        })
        .await
        .expect_err("panic task should return JoinError");

        let err = resolve_rebalance_worker_result::<()>(7, Err(join_error)).unwrap_err();
        assert!(err.to_string().contains("rebalance worker 7 task join error"));
    }

    #[tokio::test]
    async fn test_wait_rebalance_entry_tasks_returns_ok_for_successful_tasks() {
        let tasks = Arc::new(tokio::sync::Mutex::new(vec![tokio::spawn(async {
            Ok(RebalanceEntryOutcome::Completed)
        })]));

        let result = wait_rebalance_entry_tasks(1, tasks)
            .await
            .expect("successful entry tasks should pass");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_wait_rebalance_entry_tasks_returns_first_task_error() {
        let tasks = Arc::new(tokio::sync::Mutex::new(vec![
            tokio::spawn(async { Ok(RebalanceEntryOutcome::Completed) }),
            tokio::spawn(async { Err(Error::other("entry failed")) }),
        ]));

        let err = wait_rebalance_entry_tasks(1, tasks)
            .await
            .expect_err("entry task failure should be returned");

        assert!(err.to_string().contains("entry failed"));
    }

    #[tokio::test]
    async fn test_wait_rebalance_entry_tasks_returns_deferred_error_without_failing() {
        let tasks = Arc::new(tokio::sync::Mutex::new(vec![
            tokio::spawn(async { Ok(RebalanceEntryOutcome::Completed) }),
            tokio::spawn(async {
                Ok(RebalanceEntryOutcome::Deferred {
                    last_error: "deferred transient rebalance entry failure: timeout".to_string(),
                })
            }),
        ]));

        let result = wait_rebalance_entry_tasks(1, tasks)
            .await
            .expect("deferred transient entry should not fail worker");

        assert_eq!(result.as_deref(), Some("deferred transient rebalance entry failure: timeout"));
    }

    #[test]
    fn test_resolve_rebalance_save_task_result_passthrough() {
        assert!(resolve_rebalance_save_task_result(0, Ok(Ok(()))).is_ok());
    }

    #[test]
    fn test_resolve_rebalance_save_task_result_wraps_inner_error_context() {
        let err = resolve_rebalance_save_task_result(1, Ok(Err(Error::SlowDown)))
            .expect_err("inner save-task error should include pool context");
        assert!(err.to_string().contains("rebalance save_task failed for pool 1"));
    }

    #[test]
    fn test_resolve_rebalance_meta_save_result_passthrough() {
        assert!(resolve_rebalance_meta_save_result(Ok(()), "stop_rebalance").is_ok());
    }

    #[test]
    fn test_resolve_rebalance_meta_save_result_wraps_error_context() {
        let err = resolve_rebalance_meta_save_result(Err(Error::SlowDown), "init_rebalance_meta")
            .expect_err("meta save failure should include stage context");
        let message = err.to_string();
        assert!(message.contains("rebalance meta save failed during init_rebalance_meta"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_merge_rebalance_meta_preserves_updates_from_multiple_pools() {
        let start_time = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let warning_at = OffsetDateTime::from_unix_timestamp(1_500).unwrap();
        let mut remote = RebalanceMeta {
            id: "rebal-1".to_string(),
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    buckets: vec!["bucket-a".to_string()],
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(start_time),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    num_versions: 4,
                    bytes: 400,
                    bucket: "bucket-a".to_string(),
                    object: "remote-object".to_string(),
                    ..Default::default()
                },
                RebalanceStats {
                    buckets: vec!["bucket-a".to_string()],
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(start_time),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let local = RebalanceMeta {
            id: "rebal-1".to_string(),
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    buckets: vec!["bucket-a".to_string()],
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(start_time),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    buckets: Vec::new(),
                    rebalanced_buckets: vec!["bucket-a".to_string()],
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(start_time),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    num_versions: 7,
                    bytes: 700,
                    bucket: "bucket-a".to_string(),
                    object: "local-object".to_string(),
                    cleanup_warnings: RebalanceCleanupWarnings {
                        count: 1,
                        last_message: Some("cleanup failed".to_string()),
                        last_bucket: Some("bucket-a".to_string()),
                        last_object: Some("local-object".to_string()),
                        last_at: Some(warning_at),
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        merge_rebalance_meta(&mut remote, &local);

        assert_eq!(remote.pool_stats[0].num_versions, 4);
        assert_eq!(remote.pool_stats[0].object, "remote-object");
        assert_eq!(remote.pool_stats[1].num_versions, 7);
        assert_eq!(remote.pool_stats[1].object, "local-object");
        assert!(remote.pool_stats[1].buckets.is_empty());
        assert_eq!(remote.pool_stats[1].rebalanced_buckets, vec!["bucket-a"]);
        assert_eq!(remote.pool_stats[1].cleanup_warnings.count, 1);
        assert_eq!(remote.pool_stats[1].cleanup_warnings.last_message.as_deref(), Some("cleanup failed"));
        assert_eq!(remote.pool_stats[1].cleanup_warnings.last_at, Some(warning_at));
    }

    #[test]
    fn test_merge_rebalance_meta_does_not_overwrite_failed_with_started_stats() {
        let now = OffsetDateTime::from_unix_timestamp(2_000).unwrap();
        let mut remote = RebalanceMeta {
            id: "rebal-1".to_string(),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Failed,
                    end_time: Some(now),
                    last_error: Some("timeout".to_string()),
                    ..Default::default()
                },
                num_versions: 10,
                bytes: 1_000,
                ..Default::default()
            }],
            ..Default::default()
        };
        let local = RebalanceMeta {
            id: "rebal-1".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                num_versions: 8,
                bytes: 800,
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_meta(&mut remote, &local);

        assert_eq!(remote.pool_stats[0].info.status, RebalStatus::Failed);
        assert_eq!(remote.pool_stats[0].info.last_error.as_deref(), Some("timeout"));
        assert_eq!(remote.pool_stats[0].num_versions, 10);
        assert_eq!(remote.pool_stats[0].bytes, 1_000);
    }

    #[test]
    fn test_merge_rebalance_meta_does_not_overwrite_stopped_with_started_stats() {
        let stopped_at = OffsetDateTime::from_unix_timestamp(3_000).unwrap();
        let mut remote = RebalanceMeta {
            id: "rebal-1".to_string(),
            stopped_at: Some(stopped_at),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Stopped,
                    end_time: Some(stopped_at),
                    ..Default::default()
                },
                num_versions: 5,
                ..Default::default()
            }],
            ..Default::default()
        };
        let local = RebalanceMeta {
            id: "rebal-1".to_string(),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                num_versions: 9,
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_meta(&mut remote, &local);

        assert_eq!(remote.stopped_at, Some(stopped_at));
        assert_eq!(remote.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(remote.pool_stats[0].info.end_time, Some(stopped_at));
        assert_eq!(remote.pool_stats[0].num_versions, 9);
    }

    #[test]
    fn test_merge_rebalance_meta_preserves_failed_status_over_stopped() {
        let stopped_at = OffsetDateTime::from_unix_timestamp(3_000).unwrap();
        let failed_at = OffsetDateTime::from_unix_timestamp(4_000).unwrap();
        let mut remote = RebalanceMeta {
            id: "rebal-1".to_string(),
            stopped_at: Some(stopped_at),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Stopped,
                    end_time: Some(stopped_at),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let local = RebalanceMeta {
            id: "rebal-1".to_string(),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Failed,
                    end_time: Some(failed_at),
                    last_error: Some("late failure".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_meta(&mut remote, &local);

        assert_eq!(remote.pool_stats[0].info.status, RebalStatus::Failed);
        assert_eq!(remote.pool_stats[0].info.end_time, Some(failed_at));
        assert_eq!(remote.pool_stats[0].info.last_error.as_deref(), Some("late failure"));
    }

    #[test]
    fn test_merge_rebalance_meta_does_not_overwrite_stopped_with_completed_status() {
        let stopped_at = OffsetDateTime::from_unix_timestamp(3_000).unwrap();
        let completed_at = OffsetDateTime::from_unix_timestamp(5_000).unwrap();
        let mut remote = RebalanceMeta {
            id: "rebal-1".to_string(),
            stopped_at: Some(stopped_at),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Stopped,
                    end_time: Some(stopped_at),
                    ..Default::default()
                },
                num_versions: 5,
                ..Default::default()
            }],
            ..Default::default()
        };
        let local = RebalanceMeta {
            id: "rebal-1".to_string(),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    end_time: Some(completed_at),
                    ..Default::default()
                },
                num_versions: 12,
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_meta(&mut remote, &local);

        assert_eq!(remote.stopped_at, Some(stopped_at));
        assert_eq!(remote.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(remote.pool_stats[0].info.end_time, Some(stopped_at));
        assert_eq!(remote.pool_stats[0].num_versions, 12);
    }

    #[test]
    fn test_rebalance_meta_load_no_data_error_formats_context() {
        let err = rebalance_meta_load_no_data_error();
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("payload is too short"), "{rendered}");
    }

    #[test]
    fn test_rebalance_meta_load_unknown_format_error_formats_context() {
        let err = rebalance_meta_load_unknown_format_error(9);
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("unknown format 9"), "{rendered}");
    }

    #[test]
    fn test_rebalance_meta_load_unknown_version_error_formats_context() {
        let err = rebalance_meta_load_unknown_version_error(3);
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("unknown version 3"), "{rendered}");
    }

    #[test]
    fn test_rebalance_meta_deserializes_legacy_stats_without_cleanup_warnings() {
        let legacy = LegacyRebalanceMeta {
            id: "rebal-legacy".to_string(),
            percent_free_goal: 0.35,
            pool_stats: vec![LegacyRebalanceStats {
                buckets: vec!["bucket-a".to_string()],
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let data = rmp_serde::to_vec(&legacy).expect("legacy rebalance metadata should serialize");

        let decoded: RebalanceMeta =
            rmp_serde::from_slice(data.as_slice()).expect("legacy rebalance metadata should deserialize");

        assert_eq!(decoded.id, "rebal-legacy");
        assert_eq!(decoded.pool_stats.len(), 1);
        assert_eq!(decoded.pool_stats[0].cleanup_warnings, RebalanceCleanupWarnings::default());
        assert_eq!(decoded.pool_stats[0].info.status, RebalStatus::Started);
    }

    #[test]
    fn test_resolve_rebalance_stats_update_result_passthrough() {
        assert!(resolve_rebalance_stats_update_result(Ok(()), 0, "bucket", "object").is_ok());
    }

    #[test]
    fn test_resolve_rebalance_stats_update_result_wraps_error_context() {
        let err = resolve_rebalance_stats_update_result(Err(Error::SlowDown), 2, "bucket-a", "obj.txt")
            .expect_err("stats update error should include context");
        assert!(
            err.to_string()
                .contains("rebalance stats update failed for pool 2 bucket bucket-a object obj.txt")
        );
    }

    #[test]
    fn test_resolve_load_rebalance_stats_update_result_passthrough() {
        assert!(resolve_load_rebalance_stats_update_result(Ok(())).is_ok());
    }

    #[test]
    fn test_resolve_load_rebalance_stats_update_result_wraps_error_context() {
        let err = resolve_load_rebalance_stats_update_result(Err(Error::SlowDown))
            .expect_err("load-time stats refresh failure should include context");
        assert!(err.to_string().contains("rebalance metadata stats refresh failed after load"));
    }

    #[test]
    fn test_resolve_rebalance_file_info_versions_result_passthrough() {
        let value = resolve_rebalance_file_info_versions_result::<usize, Error>(Ok(7), "bucket-a", "obj.txt")
            .expect("ok results should pass through");
        assert_eq!(value, 7);
    }

    #[test]
    fn test_resolve_rebalance_file_info_versions_result_wraps_error_context() {
        let err = resolve_rebalance_file_info_versions_result::<usize, Error>(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect_err("errors should be wrapped");
        let message = err.to_string();
        assert!(message.contains("rebalance file_info_versions failed for bucket-a/obj.txt"));
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_returns_true_for_loaded_meta() {
        assert!(resolve_rebalance_meta_load_result(Ok(())).expect("loaded rebalance metadata should pass through"));
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_returns_false_for_missing_meta() {
        assert!(
            !resolve_rebalance_meta_load_result(Err(Error::ConfigNotFound))
                .expect("missing rebalance metadata should be treated as not started")
        );
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_wraps_error_context() {
        let err =
            resolve_rebalance_meta_load_result(Err(Error::SlowDown)).expect_err("unexpected load failures should be wrapped");
        let message = err.to_string();
        assert!(message.contains("rebalance metadata load failed during load_rebalance_meta"));
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_passthrough() {
        let result = resolve_rebalance_entry_cleanup_delete_result(Ok(ObjectInfo::default()), "bucket-a", "obj.txt");
        assert_eq!(result.expect("successful cleanup should pass through"), None);
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_ignores_not_found() {
        let result = resolve_rebalance_entry_cleanup_delete_result(
            Err(Error::ObjectNotFound("bucket-a".to_string(), "obj.txt".to_string())),
            "bucket-a",
            "obj.txt",
        );
        assert_eq!(result.expect("missing cleanup source should be ignored"), None);
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_returns_warning_for_failures() {
        let warning = resolve_rebalance_entry_cleanup_delete_result(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect("cleanup delete failures should be downgraded to warnings")
            .expect("cleanup delete failure should return warning");
        let message = warning.as_str();
        assert!(message.contains("rebalance cleanup delete failed for bucket-a/obj.txt"));
    }

    #[test]
    fn test_resolve_rebalance_migrate_result_error_preserves_inner_error() {
        let err = resolve_rebalance_migrate_result_error(Some(Error::SlowDown), 2, "bucket-a", "obj.txt", Some("vid-1"));
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_migrate_result_error_wraps_missing_error_context() {
        let err = resolve_rebalance_migrate_result_error(None, 2, "bucket-a", "obj.txt", Some("vid-1"));
        let message = err.to_string();
        assert!(
            message
                .contains("rebalance migration reported failure without error for pool 2 entry bucket-a/obj.txt version vid-1"),
            "{message}"
        );
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_passthrough() {
        let outcome = resolve_rebalance_bucket_result(Ok(RebalanceBucketOutcome::Completed), 2, "bucket-a")
            .expect("completed bucket should pass through");
        assert_eq!(outcome, RebalanceBucketOutcome::Completed);
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_preserves_operation_canceled() {
        let err = resolve_rebalance_bucket_result(Err(Error::OperationCanceled), 2, "bucket-a")
            .expect_err("operation canceled should be preserved");
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_wraps_not_initialized_with_context() {
        let err = resolve_rebalance_bucket_result(Err(Error::other("errServerNotInitialized")), 2, "bucket-a")
            .expect_err("not initialized should be surfaced with context");
        let message = err.to_string();
        assert!(message.contains("rebalance bucket bucket-a failed for pool 2"));
        assert!(message.contains("errServerNotInitialized"));
    }

    #[test]
    fn test_rebalance_listing_disks_available_rejects_empty_set() {
        let err = ensure_rebalance_listing_disks_available(false, "bucket-a")
            .expect_err("missing online disks should be reported with bucket context");
        assert!(
            err.to_string()
                .contains("failed to list objects to rebalance for bucket bucket-a: no disks available")
        );
    }

    #[test]
    fn test_rebalance_listing_disks_available_allows_online_disks() {
        assert!(ensure_rebalance_listing_disks_available(true, "bucket-a").is_ok());
    }

    #[test]
    fn test_is_transient_rebalance_error_classifies_retryable_errors() {
        assert!(is_transient_rebalance_error(&Error::SlowDown));
        assert!(is_transient_rebalance_error(&Error::ErasureReadQuorum));
        assert!(is_transient_rebalance_error(&Error::ErasureWriteQuorum));
        assert!(is_transient_rebalance_error(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        ))));
        assert!(is_transient_rebalance_error(&Error::other("i/o timeout")));
    }

    #[test]
    fn test_is_transient_rebalance_error_accepts_lock_and_rpc_timeouts() {
        assert!(is_transient_rebalance_error(&Error::Io(std::io::Error::other(
            "Failed to acquire read lock: ns_loc: read lock acquisition timed out on bucket/object"
        ))));
        assert!(is_transient_rebalance_error(&Error::other(
            "Remote lock RPC timed out: RPC timed out after 50ms"
        )));
        assert!(is_transient_rebalance_error(&Error::other(
            "Unknown hyper error: hyper::Error(Http2, KeepAliveTimedOut)"
        )));
        assert!(is_transient_rebalance_error(&Error::Lock(rustfs_lock::LockError::timeout(
            "bucket/object",
            Duration::from_secs(5),
        ))));
    }

    #[test]
    fn test_is_transient_rebalance_error_accepts_wrapped_disk_timeout() {
        assert!(is_transient_rebalance_error(&Error::Io(std::io::Error::other(DiskError::Timeout))));
    }

    #[test]
    fn test_is_transient_rebalance_error_accepts_io_timeout_message() {
        assert!(is_transient_rebalance_error(&Error::Io(std::io::Error::other("timeout"))));
    }

    #[test]
    fn test_is_transient_rebalance_error_rejects_terminal_errors() {
        assert!(!is_transient_rebalance_error(&Error::DataMovementOverwriteErr(
            "bucket".to_string(),
            "object".to_string(),
            "version".to_string(),
        )));
        assert!(!is_transient_rebalance_error(&Error::ObjectNotFound(
            "bucket".to_string(),
            "object".to_string(),
        )));
        assert!(!is_transient_rebalance_error(&Error::FileAccessDenied));
        assert!(!is_transient_rebalance_error(&Error::Lock(rustfs_lock::LockError::already_locked(
            "bucket/object",
            "owner-a",
        ))));
        assert!(!is_transient_rebalance_error(&Error::other(
            "Failed to acquire read lock: ns_loc: read lock acquisition failed on bucket/object: permission denied"
        )));
    }

    #[test]
    fn test_should_retry_rebalance_listing_respects_attempt_limit_and_error_type() {
        assert!(should_retry_rebalance_listing(&Error::SlowDown, 0, 3));
        assert!(should_retry_rebalance_listing(&Error::SlowDown, 1, 3));
        assert!(!should_retry_rebalance_listing(&Error::SlowDown, 2, 3));
        assert!(!should_retry_rebalance_listing(&Error::FileAccessDenied, 0, 3));
    }

    #[test]
    fn test_parse_rebalance_max_attempts_uses_positive_override_or_default() {
        assert_eq!(parse_rebalance_max_attempts(Some("5")), 5);
        assert_eq!(parse_rebalance_max_attempts(Some(" 7 ")), 7);
        assert_eq!(parse_rebalance_max_attempts(Some("0")), 3);
        assert_eq!(parse_rebalance_max_attempts(Some("invalid")), 3);
        assert_eq!(parse_rebalance_max_attempts(None), 3);
    }

    #[test]
    fn test_rebalance_listing_retry_delay_scales_by_attempt() {
        assert_eq!(rebalance_listing_retry_delay(0), Duration::from_millis(250));
        assert_eq!(rebalance_listing_retry_delay(1), Duration::from_millis(500));
    }

    #[test]
    fn test_rebalance_migration_retry_delay_scales_for_non_lock_timeout() {
        assert_eq!(rebalance_migration_retry_delay(0, &Error::SlowDown), Duration::from_millis(250));
        assert_eq!(rebalance_migration_retry_delay(1, &Error::SlowDown), Duration::from_millis(500));
    }

    #[test]
    fn test_should_defer_rebalance_entry_failure_only_for_transient_errors() {
        assert!(should_defer_rebalance_entry_failure(&Error::Io(std::io::Error::other(
            "Failed to acquire write lock: ns_loc: write lock acquisition timed out on bucket/object"
        ))));
        assert!(!should_defer_rebalance_entry_failure(&Error::DataMovementOverwriteErr(
            "bucket".to_string(),
            "object".to_string(),
            "version".to_string(),
        )));
        assert!(!should_defer_rebalance_entry_failure(&Error::FileAccessDenied));
    }

    #[tokio::test]
    async fn test_wait_rebalance_listing_retry_returns_canceled_without_sleeping() {
        let token = CancellationToken::new();
        token.cancel();

        let err = wait_rebalance_listing_retry(&token, Duration::from_secs(30))
            .await
            .expect_err("canceled rebalance should not wait before retrying");

        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_with_rebalance_entry_context_formats_stage_bucket_and_object() {
        let err = with_rebalance_entry_context("migrate", "bucket-a", "obj.txt", Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("rebalance entry migrate failed for bucket-a/obj.txt"));
        assert!(message.contains("Please reduce your request rate"));
    }

    #[test]
    fn test_with_rebalance_entry_context_formats_precise_stage() {
        let err = with_rebalance_entry_context("write_target", "bucket-a", "obj.txt", Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("rebalance entry write_target failed for bucket-a/obj.txt"));
        assert!(message.contains("Please reduce your request rate"));
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_failure_reports_write_target_stage() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let mut transfer = |_, _, _| async { Err(Error::SlowDown) };
        let version = version_normal();

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            1,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert_eq!(result.stage, Some("write_target"));
        assert!(matches!(result.error, Some(Error::SlowDown)));
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_failure_reports_read_source_stage() {
        let backend = AlwaysFailGetBackend::new();
        let mut transfer = |_, _, _| async { Ok(()) };
        let version = version_normal();

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            1,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert_eq!(result.stage, Some("read_source"));
        assert!(matches!(result.error, Some(Error::SlowDown)));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_for_cleanup_safe_ignored_result() {
        let result = MigrationVersionResult {
            ignored: true,
            cleanup_ignored: true,
            ..Default::default()
        };
        assert!(should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_skip_only_ignored_result() {
        let result = MigrationVersionResult {
            ignored: true,
            cleanup_ignored: false,
            ..Default::default()
        };
        assert!(!should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_for_moved_result() {
        let result = MigrationVersionResult {
            moved: true,
            ..Default::default()
        };
        assert!(should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_failed_result() {
        let result = MigrationVersionResult {
            moved: true,
            failed: true,
            ..Default::default()
        };
        assert!(!should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_incomplete_result() {
        assert!(!should_count_rebalance_version_complete(&MigrationVersionResult::default()));
    }

    #[test]
    fn test_should_accept_rebalance_stats_update_only_for_started_pool() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(should_accept_rebalance_stats_update(&meta, 0));
    }

    #[test]
    fn test_should_accept_rebalance_stats_update_rejects_stopped_meta() {
        let meta = RebalanceMeta {
            stopped_at: Some(OffsetDateTime::UNIX_EPOCH),
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!should_accept_rebalance_stats_update(&meta, 0));
    }

    #[test]
    fn test_should_accept_rebalance_stats_update_rejects_terminal_or_invalid_pool() {
        for status in [RebalStatus::Completed, RebalStatus::Stopped, RebalStatus::Failed] {
            let meta = RebalanceMeta {
                pool_stats: vec![RebalanceStats {
                    info: RebalanceInfo {
                        status,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            };

            assert!(!should_accept_rebalance_stats_update(&meta, 0));
            assert!(!should_accept_rebalance_stats_update(&meta, 1));
        }
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_when_last_remaining_without_replication() {
        assert!(should_skip_rebalance_delete_marker(&version_deleted(), 1, false));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_configured_replication() {
        assert!(!should_skip_rebalance_delete_marker(&version_deleted(), 1, true));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_non_deleted_versions() {
        assert!(!should_skip_rebalance_delete_marker(&version_normal(), 1, false));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_multiple_remaining_versions() {
        assert!(!should_skip_rebalance_delete_marker(&version_deleted(), 2, false));
    }

    #[test]
    fn test_should_cleanup_rebalance_source_entry_accepts_all_versions_completed() {
        assert!(should_cleanup_rebalance_source_entry(3, 3));
    }

    #[test]
    fn test_should_cleanup_rebalance_source_entry_rejects_versions_only_expired_by_lifecycle() {
        assert!(!should_cleanup_rebalance_source_entry(2, 3));
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_passthrough() {
        let result = resolve_rebalance_optional_bucket_config_result(
            "bucket-a",
            "replication",
            Ok((ReplicationConfiguration::default(), OffsetDateTime::UNIX_EPOCH)),
        )
        .expect("bucket config should pass through");
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_returns_none_for_missing_config() {
        let result = resolve_rebalance_optional_bucket_config_result::<()>("bucket-a", "versioning", Err(Error::ConfigNotFound))
            .expect("missing bucket config should map to None");
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_wraps_other_errors() {
        let err = resolve_rebalance_optional_bucket_config_result::<()>("bucket-a", "replication", Err(Error::SlowDown))
            .expect_err("unexpected bucket config errors should be wrapped with context");
        assert!(
            err.to_string()
                .contains("rebalance replication config load failed for bucket bucket-a")
        );
    }

    #[tokio::test]
    async fn test_load_rebalance_bucket_configs_skips_meta_bucket_lookup() {
        let configs = load_rebalance_bucket_configs(RUSTFS_META_BUCKET)
            .await
            .expect("meta bucket config loading should short-circuit");
        assert!(configs.lifecycle_config.is_none());
        assert!(configs.lock_retention.is_none());
        assert!(configs.replication_config.is_none());
    }

    #[tokio::test]
    async fn test_resolve_rebalance_save_task_result_join_error_keeps_context() {
        let join_error = tokio::spawn(async {
            panic!("rebalance save task panic");
        })
        .await
        .expect_err("panic task should return JoinError");

        let err = resolve_rebalance_save_task_result(3, Err(join_error)).unwrap_err();
        assert!(err.to_string().contains("rebalance save_task for pool 3 join error"));
    }

    #[tokio::test]
    async fn test_send_rebalance_done_signal_sends_message() {
        let (tx, mut rx) = mpsc::channel(1);

        send_rebalance_done_signal(&tx, Ok(()), 2)
            .await
            .expect("send should succeed when receiver is active");

        let received = rx.recv().await.expect("receiver should get signal");
        assert!(received.is_ok());
    }

    #[tokio::test]
    async fn test_send_rebalance_done_signal_reports_closed_channel() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);

        let err = send_rebalance_done_signal(&tx, Ok(()), 5)
            .await
            .expect_err("send should fail when receiver is closed");
        assert!(err.to_string().contains("rebalance done signal send failed for pool 5"));
    }

    #[test]
    fn test_resolve_rebalance_terminal_error_keeps_primary_when_signal_ok() {
        let err = resolve_rebalance_terminal_error(Error::SlowDown, Ok(()));
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_terminal_error_wraps_signal_failure_context() {
        let err = resolve_rebalance_terminal_error(Error::SlowDown, Err(Error::OperationCanceled));
        assert!(err.to_string().contains("rebalance terminal signal failed after error"));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_prefers_entry_error() {
        let err = resolve_rebalance_bucket_error(Some(Error::OperationCanceled), Some(Error::SlowDown)).unwrap_err();
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_uses_worker_error_when_entry_ok() {
        let err = resolve_rebalance_bucket_error(None, Some(Error::SlowDown)).unwrap_err();
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_is_ok_when_no_errors() {
        assert!(resolve_rebalance_bucket_error(None, None).is_ok());
    }

    #[test]
    fn test_ensure_valid_rebalance_pool_index_allows_in_range() {
        assert!(ensure_valid_rebalance_pool_index(3, 2).is_ok());
    }

    #[test]
    fn test_ensure_valid_rebalance_pool_index_rejects_out_of_range() {
        let err = ensure_valid_rebalance_pool_index(2, 2).expect_err("out of range index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index"));
    }

    #[test]
    fn test_clone_first_arc_returns_first_value() {
        let values = vec![Arc::new(7_u8), Arc::new(9_u8)];
        let first = clone_first_arc(values.as_slice(), "empty values").expect("first value should be returned");
        assert_eq!(*first, 7_u8);
    }

    #[test]
    fn test_clone_first_arc_rejects_empty_values() {
        let values: Vec<Arc<u8>> = Vec::new();
        let err = clone_first_arc(values.as_slice(), "empty values").expect_err("empty values should fail");
        assert!(err.to_string().contains("empty values"));
    }

    #[test]
    fn test_clone_arc_by_index_returns_value() {
        let values = vec![Arc::new(7_u8), Arc::new(9_u8)];
        let value =
            clone_arc_by_index(values.as_slice(), 1, "invalid rebalance pool index").expect("index within bounds should work");
        assert_eq!(*value, 9_u8);
    }

    #[test]
    fn test_clone_arc_by_index_rejects_out_of_range() {
        let values = vec![Arc::new(7_u8)];
        let err =
            clone_arc_by_index(values.as_slice(), 2, "invalid rebalance pool index").expect_err("out of range index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index: 2"));
    }

    #[test]
    fn test_classify_rebalance_terminal_event_completed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Ok(())), now) {
            RebalanceTerminalEvent::Completed { msg } => assert!(msg.contains("Rebalance completed")),
            _ => panic!("expected completed terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_stopped() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Err(Error::OperationCanceled)), now) {
            RebalanceTerminalEvent::Stopped { msg } => assert!(msg.contains("Rebalance stopped")),
            _ => panic!("expected stopped terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_failed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Err(Error::SlowDown)), now) {
            RebalanceTerminalEvent::Failed { msg, last_error } => {
                assert!(msg.contains("Rebalance failed"));
                assert!(msg.contains("with err"));
                assert_eq!(last_error, Error::SlowDown.to_string());
            }
            _ => panic!("expected failed terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_channel_closed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(None, now) {
            RebalanceTerminalEvent::ChannelClosed { msg, last_error } => {
                assert!(msg.contains("channel closed"));
                assert!(last_error.contains("before terminal event"));
                assert!(msg.contains("at"));
                assert!(last_error.contains("at"));
            }
            _ => panic!("expected channel closed terminal event"),
        }
    }

    #[test]
    fn test_apply_rebalance_terminal_event_channel_closed_marks_failed() {
        let now = OffsetDateTime::now_utc();
        let mut status = RebalStatus::Started;
        let mut end_time = None;
        let mut last_error = None;

        apply_rebalance_terminal_event(
            &mut status,
            &mut end_time,
            &mut last_error,
            RebalanceTerminalEvent::ChannelClosed {
                msg: "channel closed".to_string(),
                last_error: "rebalance save channel closed before terminal event".to_string(),
            },
            now,
        );

        assert_eq!(status, RebalStatus::Failed);
        assert_eq!(end_time, Some(now));
        assert_eq!(last_error.as_deref(), Some("rebalance save channel closed before terminal event"));
    }

    #[test]
    fn test_apply_rebalance_terminal_event_stopped_clears_error() {
        let now = OffsetDateTime::now_utc();
        let mut status = RebalStatus::Started;
        let mut end_time = None;
        let mut last_error = Some("old-error".to_string());

        apply_rebalance_terminal_event(
            &mut status,
            &mut end_time,
            &mut last_error,
            RebalanceTerminalEvent::Stopped {
                msg: "rebalance stopped".to_string(),
            },
            now,
        );

        assert_eq!(status, RebalStatus::Stopped);
        assert_eq!(end_time, Some(now));
        assert_eq!(last_error, None);
    }

    #[test]
    fn test_is_rebalance_stopped_terminal_event_only_matches_stopped_variant() {
        let stopped = RebalanceTerminalEvent::Stopped {
            msg: "stopped".to_string(),
        };
        let completed = RebalanceTerminalEvent::Completed {
            msg: "completed".to_string(),
        };

        assert!(is_rebalance_stopped_terminal_event(&stopped));
        assert!(!is_rebalance_stopped_terminal_event(&completed));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_when_meta_marked_stopped() {
        let event = RebalanceTerminalEvent::Completed {
            msg: "completed".to_string(),
        };

        assert!(should_preserve_rebalance_stopped_state(true, RebalStatus::Started, &event));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_when_pool_already_stopped() {
        let event = RebalanceTerminalEvent::Failed {
            msg: "failed".to_string(),
            last_error: "boom".to_string(),
        };

        assert!(should_preserve_rebalance_stopped_state(false, RebalStatus::Stopped, &event));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_allows_stopped_terminal_update() {
        let event = RebalanceTerminalEvent::Stopped {
            msg: "stopped".to_string(),
        };

        assert!(!should_preserve_rebalance_stopped_state(true, RebalStatus::Started, &event));
    }

    #[test]
    fn test_ensure_rebalance_not_decommissioning_rejects_running_decommission() {
        assert!(!ensure_rebalance_not_decommissioning(true));
    }

    #[test]
    fn test_ensure_rebalance_not_decommissioning_allows_idle_decommission() {
        assert!(ensure_rebalance_not_decommissioning(false));
    }

    #[test]
    fn test_validate_start_rebalance_state_rejects_running_decommission() {
        let err = validate_start_rebalance_state(true, true).expect_err("running decommission should block rebalance start");
        assert!(matches!(err, Error::DecommissionAlreadyRunning));
    }

    #[test]
    fn test_validate_start_rebalance_state_rejects_missing_meta() {
        let err = validate_start_rebalance_state(false, false).expect_err("missing rebalance meta should fail");
        assert!(matches!(err, Error::ConfigNotFound));
    }

    #[test]
    fn test_validate_start_rebalance_state_allows_loaded_meta() {
        validate_start_rebalance_state(false, true).expect("loaded rebalance meta should allow start");
    }

    #[test]
    fn test_percent_free_ratio_zero_capacity_is_zero() {
        assert_eq!(percent_free_ratio(100, 0), 0.0);
    }

    #[test]
    fn test_percent_free_ratio_normal_case() {
        assert_eq!(percent_free_ratio(250, 1_000), 0.25);
    }

    #[test]
    fn test_should_pool_participate_false_when_capacity_zero() {
        assert!(!should_pool_participate(0, 0, 0.2));
    }

    #[test]
    fn test_should_pool_participate_true_when_ratio_below_goal() {
        assert!(should_pool_participate(200, 1_000, 0.3));
    }

    #[test]
    fn test_should_pool_participate_false_when_ratio_meets_goal() {
        assert!(!should_pool_participate(300, 1_000, 0.3));
    }

    #[test]
    fn test_rebalance_goal_not_reached_for_issue_3137_initial_imbalance() {
        let pool0_capacity = 10_000_u64;
        let pool0_free = 9_135_u64;
        let pool1_capacity = 10_000_u64;
        let pool1_free = 9_800_u64;
        let goal = percent_free_ratio(pool0_free + pool1_free, pool0_capacity + pool1_capacity);

        assert!(should_pool_participate(pool0_free, pool0_capacity, goal));
        assert!(!rebalance_goal_reached(pool0_free, pool0_capacity, 0, goal));
    }

    #[test]
    fn test_complete_rebalance_pools_at_goal_marks_started_participants_completed() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_free_space: 400,
                    init_capacity: 1_000,
                    bytes: 100,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    init_free_space: 100,
                    init_capacity: 1_000,
                    bytes: 0,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(complete_rebalance_pools_at_goal(&mut meta, now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Started);
    }

    #[test]
    fn test_complete_rebalance_pools_at_goal_skips_deferred_transient_error() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            percent_free_goal: 0.5,
            pool_stats: vec![RebalanceStats {
                participating: true,
                init_free_space: 400,
                init_capacity: 1_000,
                bytes: 100,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    last_error: Some(format!("{REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX} timeout")),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!complete_rebalance_pools_at_goal(&mut meta, now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Started);
        assert!(has_deferred_rebalance_error(&meta.pool_stats[0]));
    }

    #[test]
    fn test_complete_rebalance_pools_with_empty_queue_marks_started_participants_completed() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    buckets: Vec::new(),
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        last_error: Some("stale error".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    buckets: vec!["bucket-a".to_string()],
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(complete_rebalance_pools_with_empty_queue(&mut meta, now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert!(meta.pool_stats[0].info.last_error.is_none());
        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Started);
    }

    #[test]
    fn test_should_skip_start_rebalance_only_when_running_and_cancel_attached() {
        assert!(should_skip_start_rebalance(true, true));
        assert!(!should_skip_start_rebalance(true, false));
        assert!(!should_skip_start_rebalance(false, true));
        assert!(!should_skip_start_rebalance(false, false));
    }

    #[test]
    fn test_new_multipart_abort_flag_defaults_to_abort_enabled() {
        let flag = data_movement::new_multipart_abort_flag();
        assert!(data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_mark_multipart_upload_completed_disables_abort_cleanup() {
        let flag = data_movement::new_multipart_abort_flag();
        data_movement::mark_multipart_upload_completed(&flag);
        assert!(!data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_decode_part_index_returns_none_when_absent() {
        assert!(data_movement::decode_part_index(None).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_none_for_invalid_payload() {
        let invalid = bytes::Bytes::from_static(b"not-a-valid-index");
        assert!(data_movement::decode_part_index(Some(&invalid)).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_some_for_valid_payload() {
        let mut index = Index::new();
        index.add(0, 0).expect("first index entry should be accepted");
        index
            .add(2_097_152, 2_097_152)
            .expect("second index entry should advance totals");

        let encoded = index.into_vec();
        let decoded = data_movement::decode_part_index(Some(&encoded)).expect("valid index payload should decode");

        assert_eq!(decoded.total_uncompressed, 2_097_152);
        assert_eq!(decoded.total_compressed, 2_097_152);
    }

    #[test]
    fn test_merge_rebalance_bucket_lists_keeps_existing_order_and_skips_duplicates() {
        let mut remote = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let local = vec![
            "bucket-b".to_string(),
            "bucket-c".to_string(),
            "bucket-c".to_string(),
            "bucket-d".to_string(),
        ];

        merge_rebalance_bucket_lists(&mut remote, &local);

        assert_eq!(
            remote,
            vec![
                "bucket-a".to_string(),
                "bucket-b".to_string(),
                "bucket-c".to_string(),
                "bucket-d".to_string()
            ]
        );
    }

    #[test]
    fn test_remove_rebalanced_buckets_from_queue_filters_membership_linearly() {
        let mut pool_stat = RebalanceStats {
            buckets: vec![
                "bucket-a".to_string(),
                "bucket-b".to_string(),
                "bucket-c".to_string(),
                "bucket-b".to_string(),
            ],
            rebalanced_buckets: vec!["bucket-b".to_string(), "bucket-d".to_string()],
            ..Default::default()
        };

        remove_rebalanced_buckets_from_queue(&mut pool_stat);

        assert_eq!(pool_stat.buckets, vec!["bucket-a".to_string(), "bucket-c".to_string()]);
    }

    #[test]
    fn test_resolve_rebalance_participants_respects_runtime_pool_count() {
        let now = OffsetDateTime::now_utc();
        let stats = vec![
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: false,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let participants = resolve_rebalance_participants(stats.as_slice(), 2);
        assert_eq!(participants, vec![true, false]);
    }

    #[test]
    fn test_resolve_rebalance_participants_requires_started_status() {
        let now = OffsetDateTime::now_utc();
        let stats = vec![
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let participants = resolve_rebalance_participants(stats.as_slice(), 2);
        assert_eq!(participants, vec![false, true]);
    }

    #[test]
    fn test_is_rebalance_actively_running_requires_cancel_and_started_state() {
        let now = OffsetDateTime::now_utc();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_actively_running(&meta));
        meta.cancel = Some(CancellationToken::new());
        assert!(is_rebalance_actively_running(&meta));
    }

    #[test]
    fn test_is_rebalance_in_progress_only_started_participants() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: None,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(now),
                        status: RebalStatus::Completed,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(now),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(is_rebalance_in_progress(&meta));
    }

    #[test]
    fn test_is_rebalance_conflicting_with_decommission_true_when_in_progress() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: None,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(is_rebalance_conflicting_with_decommission(&meta));
    }

    #[test]
    fn test_is_rebalance_conflicting_with_decommission_false_when_stopped() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: Some(now),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_conflicting_with_decommission(&meta));
    }

    #[test]
    fn test_is_rebalance_in_progress_stopped_takes_precedence() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: Some(now),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_in_progress(&meta));
    }

    #[test]
    fn test_first_rebalance_bucket_returns_first_name() {
        let pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            ..Default::default()
        };

        assert_eq!(first_rebalance_bucket(&pool_stat), Some("bucket-a".to_string()));
    }

    #[test]
    fn test_first_rebalance_bucket_returns_none_when_empty() {
        let pool_stat = RebalanceStats::default();

        assert_eq!(first_rebalance_bucket(&pool_stat), None);
    }

    #[test]
    fn test_next_rebal_bucket_from_stat_respects_empty_queue() {
        let pool_stat = RebalanceStats {
            buckets: vec![],
            ..Default::default()
        };

        assert_eq!(next_rebal_bucket_from_stat(&pool_stat), None);
    }

    #[test]
    fn test_next_rebal_bucket_from_stat_returns_first_bucket() {
        let now = OffsetDateTime::now_utc();
        let pool_stat = RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: Some(now),
                ..Default::default()
            },
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            ..Default::default()
        };

        assert_eq!(next_rebal_bucket_from_stat(&pool_stat), Some("bucket-a".to_string()));
    }

    #[test]
    fn test_clone_rebalance_pool_stats_rejects_missing_meta() {
        let err = clone_rebalance_pool_stats(None).expect_err("missing rebalance meta should fail");
        assert!(
            err.to_string()
                .contains("failed to clone rebalance pool stats: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_clone_rebalance_pool_stats_clones_entries() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let stats = clone_rebalance_pool_stats(Some(&meta)).expect("metadata should clone pool stats");
        assert_eq!(stats.len(), 1);
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_rejects_missing_meta() {
        let err = resolve_next_rebalance_bucket(None, 0).expect_err("missing meta should fail");
        assert!(
            err.to_string()
                .contains("failed to resolve next rebalance bucket: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_rejects_invalid_pool_index() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let err = resolve_next_rebalance_bucket(Some(&meta), 3).expect_err("invalid pool index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index 3 for 1 pools"));
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_returns_none_for_completed_pool() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let next = resolve_next_rebalance_bucket(Some(&meta), 0).expect("completed pool should return none");
        assert!(next.is_none());
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_returns_first_bucket_for_active_pool() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let next = resolve_next_rebalance_bucket(Some(&meta), 0).expect("active pool should return first bucket");
        assert_eq!(next.as_deref(), Some("bucket-a"));
    }

    #[test]
    fn test_take_bucket_from_rebalance_queue_moves_bucket_and_keeps_remaining() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string(), "bucket-a".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        assert!(take_bucket_from_rebalance_queue(&mut pool_stat, "bucket-a"));
        assert_eq!(pool_stat.buckets, vec!["bucket-b".to_string()]);
        assert_eq!(pool_stat.rebalanced_buckets, vec!["bucket-a".to_string(), "bucket-a".to_string()]);
    }

    #[test]
    fn test_take_bucket_from_rebalance_queue_no_match_keeps_queue() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        assert!(!take_bucket_from_rebalance_queue(&mut pool_stat, "bucket-c"));
        assert_eq!(pool_stat.buckets, vec!["bucket-a".to_string(), "bucket-b".to_string()]);
        assert!(pool_stat.rebalanced_buckets.is_empty());
    }

    #[test]
    fn test_defer_bucket_in_rebalance_queue_moves_bucket_to_back() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string(), "bucket-c".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        defer_bucket_in_rebalance_queue(&mut pool_stat, "bucket-a").expect("queued bucket should be deferred");

        assert_eq!(
            pool_stat.buckets,
            vec!["bucket-b".to_string(), "bucket-c".to_string(), "bucket-a".to_string()]
        );
        assert!(pool_stat.rebalanced_buckets.is_empty());
    }

    #[test]
    fn test_defer_bucket_in_rebalance_queue_rejects_missing_bucket() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        let err = defer_bucket_in_rebalance_queue(&mut pool_stat, "bucket-c").expect_err("missing bucket should not be deferred");

        assert!(err.to_string().contains("failed to defer rebalance bucket bucket-c"));
        assert_eq!(pool_stat.buckets, vec!["bucket-a".to_string(), "bucket-b".to_string()]);
        assert!(pool_stat.rebalanced_buckets.is_empty());
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_missing_meta() {
        let err = mark_rebalance_bucket_done(None, 0, "bucket-a").expect_err("missing meta should fail");
        assert!(
            err.to_string()
                .contains("failed to mark rebalance bucket done: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_invalid_pool_index() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let err = mark_rebalance_bucket_done(Some(&mut meta), 3, "bucket-a").expect_err("invalid pool index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index 3 for 1 pools"));
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_missing_bucket() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = mark_rebalance_bucket_done(Some(&mut meta), 0, "bucket-x").expect_err("missing bucket should fail");
        assert!(
            err.to_string()
                .contains("failed to mark rebalance bucket done: bucket bucket-x was not queued for pool 0")
        );
    }

    #[test]
    fn test_mark_rebalance_bucket_done_marks_bucket_as_rebalanced() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                rebalanced_buckets: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        mark_rebalance_bucket_done(Some(&mut meta), 0, "bucket-a").expect("bucket in queue should be marked done");
        assert_eq!(meta.pool_stats[0].buckets, vec!["bucket-b".to_string()]);
        assert_eq!(meta.pool_stats[0].rebalanced_buckets, vec!["bucket-a".to_string()]);
    }

    #[test]
    fn test_mark_rebalance_bucket_done_clears_deferred_transient_error() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                buckets: vec!["bucket-a".to_string()],
                info: RebalanceInfo {
                    last_error: Some(format!("{REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX} timeout")),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        mark_rebalance_bucket_done(Some(&mut meta), 0, "bucket-a")
            .expect("bucket in queue should be marked done after deferred retry succeeds");

        assert!(meta.pool_stats[0].info.last_error.is_none());
    }

    #[test]
    fn test_record_rebalance_cleanup_warning_in_meta_preserves_last_error() {
        let now = OffsetDateTime::from_unix_timestamp(10_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    last_error: Some("old-error".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        record_rebalance_cleanup_warning_in_meta(Some(&mut meta), 0, "bucket-a", "obj.txt", "cleanup failed".to_string(), now)
            .expect("cleanup warning should be recorded");

        assert_eq!(meta.pool_stats[0].info.last_error.as_deref(), Some("old-error"));
        assert_eq!(meta.pool_stats[0].cleanup_warnings.count, 1);
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_message.as_deref(), Some("cleanup failed"));
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_bucket.as_deref(), Some("bucket-a"));
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_object.as_deref(), Some("obj.txt"));
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_at, Some(now));
        assert_eq!(meta.last_refreshed_at, Some(now));
    }

    #[test]
    fn test_complete_rebalance_pools_with_empty_queue_preserves_cleanup_warnings() {
        let warning_at = OffsetDateTime::from_unix_timestamp(9_000).unwrap();
        let completed_at = OffsetDateTime::from_unix_timestamp(10_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                cleanup_warnings: RebalanceCleanupWarnings {
                    count: 1,
                    last_message: Some("cleanup failed".to_string()),
                    last_bucket: Some("bucket-a".to_string()),
                    last_object: Some("obj.txt".to_string()),
                    last_at: Some(warning_at),
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(complete_rebalance_pools_with_empty_queue(&mut meta, completed_at));

        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
        assert!(meta.pool_stats[0].info.last_error.is_none());
        assert_eq!(meta.pool_stats[0].cleanup_warnings.count, 1);
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_message.as_deref(), Some("cleanup failed"));
        assert_eq!(meta.pool_stats[0].cleanup_warnings.last_at, Some(warning_at));
    }

    #[test]
    fn test_apply_stopped_at_transitions_started_pools_only() {
        let now = OffsetDateTime::now_utc();
        let mut meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        end_time: None,
                        last_error: Some("old".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Failed,
                        end_time: Some(now),
                        last_error: Some("failed".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        apply_stopped_at(&mut meta, now);

        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[0].info.last_error, None);

        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Failed);
        assert_eq!(meta.pool_stats[1].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[1].info.last_error.as_deref(), Some("failed"));
    }

    #[test]
    fn test_stop_rebalance_state_cancels_token_and_marks_stopped_when_in_progress() {
        let now = OffsetDateTime::from_unix_timestamp(10_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn test_stop_rebalance_state_clears_token_without_forcing_stopped_when_not_in_progress() {
        let now = OffsetDateTime::from_unix_timestamp(20_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, None);
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
    }

    #[test]
    fn test_stop_rebalance_state_normalizes_started_pool_when_stopped_at_already_set() {
        let stopped_at = OffsetDateTime::from_unix_timestamp(30_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(40_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            stopped_at: Some(stopped_at),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    last_error: Some("stale".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(stopped_at));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(stopped_at));
        assert_eq!(meta.pool_stats[0].info.last_error, None);
    }

    #[test]
    fn test_stop_rebalance_meta_snapshot_returns_none_when_meta_missing() {
        let now = OffsetDateTime::from_unix_timestamp(50_000).unwrap();
        assert!(stop_rebalance_meta_snapshot(None, now).is_none());
    }

    #[test]
    fn test_stop_rebalance_meta_snapshot_stops_meta_and_returns_snapshot() {
        let now = OffsetDateTime::from_unix_timestamp(60_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let snapshot = stop_rebalance_meta_snapshot(Some(&mut meta), now).expect("snapshot should be returned for present meta");

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.last_refreshed_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);

        assert!(snapshot.cancel.is_none());
        assert_eq!(snapshot.stopped_at, Some(now));
        assert_eq!(snapshot.last_refreshed_at, Some(now));
        assert_eq!(snapshot.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(snapshot.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn test_apply_rebalance_save_option_stats_keeps_pool_status_and_updates_refresh() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let later = OffsetDateTime::from_unix_timestamp(2_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            last_refreshed_at: Some(now),
            stopped_at: None,
            ..Default::default()
        };

        apply_rebalance_save_option(&mut meta, 0, RebalSaveOpt::Stats, later);

        assert_eq!(meta.last_refreshed_at, Some(later));
        assert_eq!(meta.stopped_at, None);
        assert_eq!(meta.pool_stats.len(), 1);
        assert!(meta.pool_stats[0].participating);
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Started);
        assert_eq!(meta.pool_stats[0].info.start_time, Some(now));
        assert_eq!(meta.pool_stats[0].buckets, vec!["bucket-a".to_string()]);
    }

    #[test]
    fn test_apply_rebalance_save_option_stopped_at_updates_refresh_and_statuses() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Failed,
                        last_error: Some("previous failure".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        apply_rebalance_save_option(&mut meta, 9_000, RebalSaveOpt::StoppedAt, now);

        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.last_refreshed_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert!(meta.pool_stats[0].info.last_error.is_none());
        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Failed);
        assert_eq!(meta.pool_stats[1].info.last_error.as_deref(), Some("previous failure"));
    }

    #[test]
    fn test_rebalance_stats_update_counts_and_bytes_growth() {
        let mut stat = RebalanceStats {
            bucket: "bucket-a".to_string(),
            object: "obj-previous".to_string(),
            num_objects: 3,
            num_versions: 5,
            bytes: 120,
            ..Default::default()
        };

        let mut latest = FileInfo::new("object-1", 4, 2);
        latest.name = "object-1".to_string();
        latest.is_latest = true;
        latest.size = 300;
        latest.deleted = false;
        latest.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        latest.version_id = None;

        let mut historical = FileInfo::new("object-1", 4, 2);
        historical.name = "object-1".to_string();
        historical.is_latest = false;
        historical.size = 128;
        historical.deleted = false;
        historical.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        historical.version_id = None;

        let mut tombstone = FileInfo::new("object-1", 4, 2);
        tombstone.name = "object-1".to_string();
        tombstone.is_latest = false;
        tombstone.size = 64;
        tombstone.deleted = true;
        tombstone.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        tombstone.version_id = None;

        stat.update("bucket-b".to_string(), &latest);
        stat.update("bucket-b".to_string(), &historical);
        stat.update("bucket-b".to_string(), &tombstone);

        assert_eq!(stat.bucket, "bucket-b");
        assert_eq!(stat.object, "object-1");
        assert_eq!(stat.num_objects, 4);
        assert_eq!(stat.num_versions, 8);
        let expected_bytes = 120_u64
            + (latest.size * (latest.erasure.data_blocks + latest.erasure.parity_blocks) as i64
                / latest.erasure.data_blocks as i64) as u64
            + (historical.size * (historical.erasure.data_blocks + historical.erasure.parity_blocks) as i64
                / historical.erasure.data_blocks as i64) as u64;
        assert_eq!(stat.bytes, expected_bytes);
    }

    #[test]
    fn test_rebalance_stats_update_batch_matches_repeated_updates() {
        let mut latest = version_normal();
        latest.is_latest = true;
        latest.size = 128;

        let mut historical = version_normal();
        historical.is_latest = false;
        historical.size = 64;

        let mut repeated = RebalanceStats::default();
        repeated.update("bucket-a".to_string(), &latest);
        repeated.update("bucket-a".to_string(), &historical);

        let mut batched = RebalanceStats::default();
        batched.update_batch("bucket-a".to_string(), &[&latest, &historical]);

        assert_eq!(batched.bucket, repeated.bucket);
        assert_eq!(batched.object, repeated.object);
        assert_eq!(batched.num_objects, repeated.num_objects);
        assert_eq!(batched.num_versions, repeated.num_versions);
        assert_eq!(batched.bytes, repeated.bytes);
    }

    #[test]
    fn test_rebalance_stats_update_ignores_invalid_data_blocks() {
        let mut stat = RebalanceStats {
            bucket: "bucket-a".to_string(),
            object: "obj-previous".to_string(),
            num_objects: 1,
            num_versions: 2,
            bytes: 77,
            ..Default::default()
        };

        let mut invalid = FileInfo::new("object-invalid", 0, 2);
        invalid.name = "object-invalid".to_string();
        invalid.is_latest = true;
        invalid.size = 256;
        invalid.deleted = false;
        invalid.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        invalid.version_id = None;

        stat.update("bucket-z".to_string(), &invalid);

        assert_eq!(stat.bucket, "bucket-z");
        assert_eq!(stat.object, "object-invalid");
        assert_eq!(stat.num_objects, 2);
        assert_eq!(stat.num_versions, 3);
        assert_eq!(stat.bytes, 77);
    }

    #[test]
    fn test_rebalance_goal_reached_requires_target_ratio() {
        let init_free_space = 150_u64;
        let init_capacity = 800_u64;
        let goal = 0.35_f64;

        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 0, goal));
        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 129, goal));
        assert!(rebalance_goal_reached(init_free_space, init_capacity, 130, goal));
    }
}
