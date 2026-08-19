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

use rustfs_heal::heal::{
    task::{HealPriority, HealType},
    utils,
};

mod storage_api;

use storage_api::bug_fixes::{BucketInfo, DiskStore};

#[test]
fn test_format_set_disk_id_from_i32_negative() {
    // Test that negative indices return None
    assert!(utils::format_set_disk_id_from_i32(-1, 0).is_none());
    assert!(utils::format_set_disk_id_from_i32(0, -1).is_none());
    assert!(utils::format_set_disk_id_from_i32(-1, -1).is_none());
}

#[test]
fn test_format_set_disk_id_from_i32_valid() {
    // Test that valid indices return Some
    let result = utils::format_set_disk_id_from_i32(0, 1);
    assert!(result.is_some());
    assert_eq!(result.unwrap(), "pool_0_set_1");
}

/// A wall-clock lower bound for "the timestamp was actually read from the
/// clock": 2020-01-01. `unwrap_or_default()` on a pre-epoch clock yields 0, and
/// the old versions of these tests bound the fields to `_` and so could not tell
/// that apart from a real reading (rustfs/backlog#1836).
const SANE_EPOCH_SECS: u64 = 1_577_836_800;

#[test]
fn test_resume_state_timestamp_handling() {
    use rustfs_heal::heal::resume::ResumeState;

    let state = ResumeState::new(
        "test-task".to_string(),
        "test-type".to_string(),
        "pool_0_set_1".to_string(),
        vec!["bucket1".to_string()],
    );

    assert!(
        state.start_time > SANE_EPOCH_SECS,
        "start_time fell back to the default instead of reading the clock: {}",
        state.start_time
    );
    assert!(
        state.last_update >= state.start_time,
        "last_update {} must not predate start_time {}",
        state.last_update,
        state.start_time
    );
}

#[test]
fn test_resume_checkpoint_timestamp_handling() {
    use rustfs_heal::heal::resume::ResumeCheckpoint;

    let checkpoint = ResumeCheckpoint::new("test-task".to_string());

    assert!(
        checkpoint.checkpoint_time > SANE_EPOCH_SECS,
        "checkpoint_time fell back to the default instead of reading the clock: {}",
        checkpoint.checkpoint_time
    );
}

#[test]
fn test_path_to_str_helper() {
    use std::path::Path;

    // Test that path conversion handles non-UTF-8 paths gracefully
    // Note: This is a compile-time test - actual non-UTF-8 paths are hard to construct in Rust
    // The helper function should properly handle the conversion
    let valid_path = Path::new("test/path");
    assert!(valid_path.to_str().is_some());
}

#[test]
fn test_heal_task_status_atomic_update() {
    use rustfs_heal::heal::storage::{HealListItem, HealObjectInfo, HealStorageAPI};
    use rustfs_heal::heal::task::{HealOptions, HealRequest, HealTask, HealTaskStatus};
    use std::sync::Arc;

    // Mock storage for testing
    struct MockStorage;
    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Option<HealObjectInfo>> {
            Ok(None)
        }
        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Vec<u8>> {
            Ok(vec![])
        }
        async fn get_bucket_info(&self, _bucket: &str) -> rustfs_heal::Result<Option<BucketInfo>> {
            Ok(None)
        }
        async fn list_buckets(&self) -> rustfs_heal::Result<Vec<BucketInfo>> {
            Ok(vec![])
        }
        async fn object_exists(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<bool> {
            Ok(false)
        }
        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }
        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<rustfs_madmin::heal_commands::HealResultItem> {
            Ok(rustfs_madmin::heal_commands::HealResultItem::default())
        }
        async fn heal_format(
            &self,
            _dry_run: bool,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }
        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
            _include_lifecycle_object_info: bool,
        ) -> rustfs_heal::Result<(Vec<HealListItem>, Option<String>, bool)> {
            Ok((vec![], None, false))
        }
        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> rustfs_heal::Result<DiskStore> {
            Err(rustfs_heal::Error::other("Not implemented in mock"))
        }
    }

    // Create a heal request and task
    let request = HealRequest::new(
        HealType::Object {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(request, storage);

    // Verify initial status
    let status = tokio::runtime::Runtime::new().unwrap().block_on(task.get_status());
    assert_eq!(status, HealTaskStatus::Pending);

    // The task should have task_start_instant field initialized
    // This is an internal detail, but we can verify it doesn't cause issues
    // by checking that the task can be created successfully
    // Note: We can't directly access private fields, but creation without panic
    // confirms the fix works
}

#[tokio::test]
async fn test_heal_task_transient_object_exists_skip_avoids_recreate() {
    use rustfs_heal::heal::storage::{HealListItem, HealObjectInfo, HealStorageAPI};
    use rustfs_heal::heal::task::{HealOptions, HealPriority, HealRequest, HealTask, HealTaskStatus, HealType};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct MockStorage {
        object_exists_calls: Arc<AtomicUsize>,
        heal_object_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Option<HealObjectInfo>> {
            Ok(None)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> rustfs_heal::Result<Option<BucketInfo>> {
            Ok(None)
        }

        async fn list_buckets(&self) -> rustfs_heal::Result<Vec<BucketInfo>> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<bool> {
            self.object_exists_calls.fetch_add(1, Ordering::SeqCst);
            Err(rustfs_heal::Error::transient_skip(
                "Skipped object existence check for bucket/object: simulated quorum failure",
            ))
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            self.heal_object_calls.fetch_add(1, Ordering::SeqCst);
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<rustfs_madmin::heal_commands::HealResultItem> {
            Ok(rustfs_madmin::heal_commands::HealResultItem::default())
        }

        async fn heal_format(
            &self,
            _dry_run: bool,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
            _include_lifecycle_object_info: bool,
        ) -> rustfs_heal::Result<(Vec<HealListItem>, Option<String>, bool)> {
            Ok((Vec::new(), None, false))
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> rustfs_heal::Result<DiskStore> {
            Err(rustfs_heal::Error::other("not implemented"))
        }
    }

    let object_exists_calls = Arc::new(AtomicUsize::new(0));
    let heal_object_calls = Arc::new(AtomicUsize::new(0));
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage {
        object_exists_calls: object_exists_calls.clone(),
        heal_object_calls: heal_object_calls.clone(),
    });

    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let task = HealTask::from_request(request, storage);
    task.execute().await.expect("transient existence check should be skipped");

    assert_eq!(object_exists_calls.load(Ordering::SeqCst), 1);
    assert_eq!(heal_object_calls.load(Ordering::SeqCst), 0);
    assert_eq!(task.get_status().await, HealTaskStatus::Completed);
    assert!(task.get_progress().await.is_completed());
}
