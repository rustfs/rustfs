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

use super::*;
use crate::heal::EcstoreError;
use crate::heal::resume::{CheckpointManager, ReplacementTargetIdentity};
use crate::heal::storage::{HealObjectInfo, HealStorageAPI};
use crate::heal::task::{BatchHealFailure, HealOptions, HealPriority, HealRequest, HealTask, HealType};
use rustfs_common::heal_channel::{HealOpts, HealRequestSource};
use rustfs_concurrency::{WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot};
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::Mutex as StdMutex;
use tempfile::TempDir;

use super::super::{DiskOption, DiskStore, Endpoint, new_disk, storage_api::status::BucketInfo};

#[tokio::test]
async fn auto_replacement_path_requires_a_non_root_mount() {
    let temp = TempDir::new().expect("temporary replacement root should be created");
    let ready = Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("replacement endpoint should parse");
    let missing = Endpoint::try_from(temp.path().join("missing").to_string_lossy().as_ref())
        .expect("missing replacement endpoint should parse");

    let ready_disk = new_disk(
        &ready,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("temporary disk should initialize");
    assert!(
        !super::super::replacement_readiness::auto_replacement_target_ready(&ready_disk, std::slice::from_ref(&ready_disk),)
            .await
    );
    assert!(
        matches!(
            new_disk(
                &missing,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await,
            Err(DiskError::VolumeNotFound)
        ),
        "a missing replacement path must be rejected before admission"
    );
}

#[derive(Debug)]
struct FixedWorkloadProvider {
    class: WorkloadClass,
    active: usize,
    limit: usize,
    state: AdmissionState,
}

impl WorkloadAdmissionSnapshotProvider for FixedWorkloadProvider {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(self.class, self.state).with_counts(
            Some(self.active),
            None,
            Some(self.limit),
        )])
    }
}

async fn process_manager_queue_once(manager: &HealManager) {
    HealManager::process_heal_queue(HealQueueContext {
        heal_queue: &manager.heal_queue,
        active_heals: &manager.active_heals,
        completed_heals: &manager.completed_heals,
        displaced_terminals: &manager.displaced_terminals,
        task_aliases: &manager.task_aliases,
        retrying_heals: &manager.retrying_heals,
        mrf_repair_notice_targets: &manager.mrf_repair_notice_targets,
        replacement_recovery_anchors: &manager.replacement_recovery_anchors,
        config: &manager.config,
        statistics: &manager.statistics,
        storage: &manager.storage,
        notify: &manager.notify,
        cancel_token: &manager.cancel_token,
        workload_provider: &manager.workload_provider,
    })
    .await;
}

struct MockStorage;

#[async_trait::async_trait]
impl HealStorageAPI for MockStorage {
    async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<HealObjectInfo>> {
        Ok(None)
    }

    async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn get_bucket_info(&self, _bucket: &str) -> Result<Option<BucketInfo>> {
        Ok(None)
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        if let Some(hook) = manager_recovery_test_hook() {
            *hook.listed.lock().expect("manager recovery listed lock should not poison") = true;
        }
        Ok(Vec::new())
    }

    async fn object_exists(&self, bucket: &str, _object: &str) -> Result<bool> {
        Ok(bucket == "retry-transition")
    }

    async fn heal_object(
        &self,
        bucket: &str,
        _object: &str,
        _version_id: Option<&str>,
        _opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        if let Some(hook) = manager_recovery_test_hook() {
            *hook
                .heal_object_calls
                .lock()
                .expect("manager recovery object call lock should not poison") += 1;
        }
        if bucket == "retry-transition" {
            return Ok((
                HealResultItem::default(),
                Some(Error::Storage(EcstoreError::InsufficientReadQuorum(
                    bucket.to_string(),
                    "object".to_string(),
                ))),
            ));
        }
        Ok((HealResultItem::default(), None))
    }

    async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
        if let Some(hook) = manager_recovery_test_hook() {
            *hook
                .bucket_heal_calls
                .lock()
                .expect("manager recovery bucket call lock should not poison") += 1;
        }
        Ok(HealResultItem::default())
    }

    async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        if let Some(hook) = manager_recovery_test_hook() {
            *hook
                .global_format_calls
                .lock()
                .expect("manager recovery global format call lock should not poison") += 1;
        }
        Ok((HealResultItem::default(), None))
    }

    async fn heal_replacement_format(
        &self,
        _dry_run: bool,
        _pool_index: usize,
        _set_index: usize,
        _targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        if let Some(hook) = manager_recovery_test_hook() {
            *hook
                .replacement_format_calls
                .lock()
                .expect("manager recovery replacement format call lock should not poison") += 1;
        }
        Ok((HealResultItem::default(), None))
    }

    async fn list_objects_for_heal_page(
        &self,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: Option<&str>,
        _include_lifecycle_object_info: bool,
    ) -> Result<(Vec<crate::heal::storage::HealListItem>, Option<String>, bool)> {
        Ok((Vec::new(), None, false))
    }

    async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore> {
        Err(Error::other("not implemented in tests"))
    }

    async fn get_replacement_resume_disk(
        &self,
        _set_disk_id: &str,
        _task_id: &str,
        _excluded_targets: &[String],
    ) -> Result<crate::heal::storage::ReplacementResumeDisk> {
        let Some(hook) = manager_recovery_test_hook() else {
            return Err(Error::other("not implemented in tests"));
        };
        Ok(crate::heal::storage::ReplacementResumeDisk::Existing(
            hook.replacement_resume_disk.clone(),
        ))
    }
}

struct ManagerRecoveryTestHook {
    replacement_resume_disk: DiskStore,
    listed: StdMutex<bool>,
    global_format_calls: StdMutex<u32>,
    replacement_format_calls: StdMutex<u32>,
    bucket_heal_calls: StdMutex<u32>,
    heal_object_calls: StdMutex<u32>,
}

static MANAGER_RECOVERY_TEST_HOOK: LazyLock<StdMutex<Option<Arc<ManagerRecoveryTestHook>>>> =
    LazyLock::new(|| StdMutex::new(None));

struct ManagerRecoveryTestHookGuard;

impl ManagerRecoveryTestHook {
    fn install(replacement_resume_disk: DiskStore) -> (Arc<Self>, ManagerRecoveryTestHookGuard) {
        let hook = Arc::new(Self {
            replacement_resume_disk,
            listed: StdMutex::new(false),
            global_format_calls: StdMutex::new(0),
            replacement_format_calls: StdMutex::new(0),
            bucket_heal_calls: StdMutex::new(0),
            heal_object_calls: StdMutex::new(0),
        });
        let previous = MANAGER_RECOVERY_TEST_HOOK
            .lock()
            .expect("manager recovery hook lock should not poison")
            .replace(hook.clone());
        assert!(previous.is_none(), "manager recovery hook already installed");
        (hook, ManagerRecoveryTestHookGuard)
    }
}

impl Drop for ManagerRecoveryTestHookGuard {
    fn drop(&mut self) {
        *MANAGER_RECOVERY_TEST_HOOK
            .lock()
            .expect("manager recovery hook lock should not poison") = None;
    }
}

fn manager_recovery_test_hook() -> Option<Arc<ManagerRecoveryTestHook>> {
    MANAGER_RECOVERY_TEST_HOOK
        .lock()
        .expect("manager recovery hook lock should not poison")
        .clone()
}

async fn make_manager_resume_disk(temp: &TempDir, name: &str) -> DiskStore {
    let disk_path = temp.path().join(name);
    std::fs::create_dir_all(&disk_path).expect("manager recovery disk directory should be created");
    let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).expect("manager recovery endpoint should parse");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("manager recovery disk should initialize");
    let metadata_volume = disk.make_volume(super::super::RUSTFS_META_BUCKET).await;
    assert!(
        matches!(metadata_volume, Ok(()) | Err(DiskError::VolumeExists)),
        "manager recovery metadata volume should exist: {metadata_volume:?}"
    );
    disk
}

fn bucket_request(bucket: &str, priority: HealPriority, source: HealRequestSource) -> HealRequest {
    let mut request = HealRequest::new(
        HealType::Bucket {
            bucket: bucket.to_string(),
        },
        HealOptions::default(),
        priority,
    );
    request.source = source;
    request
}

#[test]
fn test_push_displacing_lower_priority_actually_enqueues_new_request() {
    // Regression for the release-build defect where the enqueue side effect lived inside
    // `debug_assert_eq!(self.push(request), ...)` and was compiled out under
    // `cargo test --release` (debug_assertions off), silently dropping the displacing
    // high-priority request while still having evicted a queued item.
    //
    // Must run with --release to expose the original bug.
    let mut queue = PriorityHealQueue::new();

    let low = bucket_request("victim-bucket", HealPriority::Low, HealRequestSource::Scanner);
    assert_eq!(queue.push(low), QueuePushOutcome::Accepted);
    assert_eq!(queue.len(), 1);

    let high = bucket_request("admin-bucket", HealPriority::High, HealRequestSource::Admin);
    let high_id = high.id.clone();
    assert!(queue.can_displace_lower_priority(high.priority));

    let displaced = queue
        .push_displacing_lower_priority(high)
        .expect("a lower-priority item should have been displaced");
    assert_eq!(displaced.priority, HealPriority::Low);

    // The displacing high-priority request must actually be enqueued (pre-fix under
    // --release, len() is 0 because self.push(request) was elided with debug_assert_eq!).
    assert_eq!(queue.len(), 1, "displacing request must remain enqueued");
    let admitted = queue.pop_next().expect("displacing high-priority request must be enqueued");
    assert_eq!(admitted.priority, HealPriority::High);
    assert_eq!(admitted.id, high_id);
    assert_eq!(queue.len(), 0);
}

#[test]
fn queued_request_id_for_dedup_key_tracks_the_representative() {
    let mut queue = PriorityHealQueue::new();

    let first = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let first_id = first.id.clone();
    let first_key = PriorityHealQueue::make_dedup_key(&first);
    assert_eq!(queue.push(first), QueuePushOutcome::Accepted);

    // A forced duplicate of the same target opens a second entry under
    // the same key; the representative stays the request that opened it.
    let mut second = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    second.force_start = true;
    let second_id = second.id.clone();
    assert_eq!(queue.push(second), QueuePushOutcome::Accepted);

    let representative = queue
        .queued_request_id_for_dedup_key(&first_key)
        .expect("key must be reserved while either request is queued");
    assert_eq!(representative, first_id);

    // A holder leaving WITHOUT becoming active (canceled by id) must
    // re-elect the representative to the surviving queued request, or a
    // later merge receipt would name an id that resolves nowhere. The
    // scheduler pop path needs no re-election: the popped request
    // surfaces in active_heals under the same id and the duplicate
    // pre-check consults active heals before the queue.
    queue.remove_request_id(&first_id);
    assert_eq!(
        queue.queued_request_id_for_dedup_key(&first_key),
        Some(second_id.as_str()),
        "canceling the opener must re-elect the surviving queued holder"
    );

    // Pop the last holder: the key is released entirely.
    let last = queue.pop_next().expect("second request must be queued");
    assert_eq!(last.id, second_id);
    assert!(queue.queued_request_id_for_dedup_key(&first_key).is_none());
}

#[test]
fn test_priority_queue_ordering() {
    let mut queue = PriorityHealQueue::new();

    // Add requests with different priorities
    let low_req = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    let normal_req = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket2".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let high_req = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket3".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );

    let urgent_req = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket4".to_string(),
        },
        HealOptions::default(),
        HealPriority::Urgent,
    );

    // Add in random order: low, high, normal, urgent
    assert_eq!(queue.push(low_req), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(high_req), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(normal_req), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(urgent_req), QueuePushOutcome::Accepted);

    assert_eq!(queue.len(), 4);

    // Should pop in priority order: urgent, high, normal, low
    let popped1 = queue.pop().unwrap();
    assert_eq!(popped1.priority, HealPriority::Urgent);

    let popped2 = queue.pop().unwrap();
    assert_eq!(popped2.priority, HealPriority::High);

    let popped3 = queue.pop().unwrap();
    assert_eq!(popped3.priority, HealPriority::Normal);

    let popped4 = queue.pop().unwrap();
    assert_eq!(popped4.priority, HealPriority::Low);

    assert_eq!(queue.len(), 0);
}

#[test]
fn test_priority_queue_fifo_same_priority() {
    let mut queue = PriorityHealQueue::new();

    // Add multiple requests with same priority
    let req1 = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let req2 = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket2".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let req3 = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket3".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let id1 = req1.id.clone();
    let id2 = req2.id.clone();
    let id3 = req3.id.clone();

    assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(req2), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(req3), QueuePushOutcome::Accepted);

    // Should maintain FIFO order for same priority
    let popped1 = queue.pop().unwrap();
    assert_eq!(popped1.id, id1);

    let popped2 = queue.pop().unwrap();
    assert_eq!(popped2.id, id2);

    let popped3 = queue.pop().unwrap();
    assert_eq!(popped3.id, id3);
}

#[test]
fn test_priority_queue_deduplication() {
    let mut queue = PriorityHealQueue::new();

    let req1 = HealRequest::new(
        HealType::Object {
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let req2 = HealRequest::new(
        HealType::Object {
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::High,
    );

    // First request should be added
    assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
    assert_eq!(queue.len(), 1);

    // Second request with same object should be rejected (duplicate)
    assert_eq!(queue.push(req2), QueuePushOutcome::Merged);
    assert_eq!(queue.len(), 1);
}

#[test]
fn test_priority_queue_contains_erasure_set() {
    let mut queue = PriorityHealQueue::new();

    let req = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket1".to_string()],
            set_disk_id: "pool_0_set_1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    assert_eq!(queue.push(req), QueuePushOutcome::Accepted);
    assert!(queue.contains_erasure_set("pool_0_set_1"));
    assert!(!queue.contains_erasure_set("pool_0_set_2"));
}

#[test]
fn test_priority_queue_dedup_key_generation() {
    // Test different heal types generate different keys
    let obj_req = HealRequest::new(
        HealType::Object {
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let bucket_req = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let erasure_req = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket1".to_string()],
            set_disk_id: "pool_0_set_1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let obj_key = PriorityHealQueue::make_dedup_key(&obj_req);
    let bucket_key = PriorityHealQueue::make_dedup_key(&bucket_req);
    let erasure_key = PriorityHealQueue::make_dedup_key(&erasure_req);

    // All keys should be different
    assert_ne!(obj_key, bucket_key);
    assert_ne!(obj_key, erasure_key);
    assert_ne!(bucket_key, erasure_key);

    assert!(obj_key.starts_with("object:"));
    assert!(bucket_key.starts_with("bucket:"));
    assert!(erasure_key.starts_with("erasure_set:"));
}

#[test]
fn test_priority_queue_mixed_priorities_and_types() {
    let mut queue = PriorityHealQueue::new();

    // Add various requests
    let requests = vec![
        (
            HealType::Object {
                bucket: "b1".to_string(),
                object: "o1".to_string(),
                version_id: None,
            },
            HealPriority::Low,
        ),
        (
            HealType::Bucket {
                bucket: "b2".to_string(),
            },
            HealPriority::Urgent,
        ),
        (
            HealType::ErasureSet {
                buckets: vec!["b3".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealPriority::Normal,
        ),
        (
            HealType::Object {
                bucket: "b4".to_string(),
                object: "o4".to_string(),
                version_id: None,
            },
            HealPriority::High,
        ),
    ];

    for (heal_type, priority) in requests {
        let req = HealRequest::new(heal_type, HealOptions::default(), priority);
        let outcome = queue.push(req);
        assert_eq!(outcome, QueuePushOutcome::Accepted);
    }

    assert_eq!(queue.len(), 4);

    // Check they come out in priority order
    let priorities: Vec<HealPriority> = (0..4).filter_map(|_| queue.pop().map(|r| r.priority)).collect();

    assert_eq!(
        priorities,
        vec![
            HealPriority::Urgent,
            HealPriority::High,
            HealPriority::Normal,
            HealPriority::Low,
        ]
    );
}

#[test]
fn test_priority_queue_stats() {
    let mut queue = PriorityHealQueue::new();

    // Add requests with different priorities
    for _ in 0..3 {
        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: format!("bucket-low-{}", queue.len()),
                },
                HealOptions::default(),
                HealPriority::Low,
            )),
            QueuePushOutcome::Accepted
        );
    }

    for _ in 0..2 {
        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: format!("bucket-normal-{}", queue.len()),
                },
                HealOptions::default(),
                HealPriority::Normal,
            )),
            QueuePushOutcome::Accepted
        );
    }

    assert_eq!(
        queue.push(HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-high".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        )),
        QueuePushOutcome::Accepted
    );

    let stats = queue.get_priority_stats();

    assert_eq!(*stats.get(&HealPriority::Low).unwrap_or(&0), 3);
    assert_eq!(*stats.get(&HealPriority::Normal).unwrap_or(&0), 2);
    assert_eq!(*stats.get(&HealPriority::High).unwrap_or(&0), 1);
    assert_eq!(*stats.get(&HealPriority::Urgent).unwrap_or(&0), 0);
}

#[test]
fn test_priority_queue_is_empty() {
    let mut queue = PriorityHealQueue::new();

    assert!(queue.is_empty());

    assert_eq!(
        queue.push(HealRequest::new(
            HealType::Bucket {
                bucket: "test".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        )),
        QueuePushOutcome::Accepted
    );

    assert!(!queue.is_empty());

    queue.pop();

    assert!(queue.is_empty());
}

#[test]
fn test_priority_queue_pop_runnable_skips_blocked_erasure_set() {
    let mut queue = PriorityHealQueue::new();

    let blocked = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Urgent,
    );
    let runnable = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-b".to_string()],
            set_disk_id: "pool_0_set_2".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    assert_eq!(queue.push(blocked), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(runnable), QueuePushOutcome::Accepted);
    for bucket in ["tail-a", "tail-b", "tail-c"] {
        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: bucket.to_string(),
                },
                HealOptions::default(),
                HealPriority::Low,
            )),
            QueuePushOutcome::Accepted
        );
    }

    let mut running = HashMap::new();
    running.insert("pool_0_set_1".to_string(), 1);

    let (popped, skipped_sets) =
        queue.pop_runnable_with_skips(|request| can_schedule_request(request, &running, 1), heal_request_set_key);
    let popped = popped.expect("should find runnable request");

    assert_eq!(skipped_sets, vec!["pool_0_set_1".to_string()]);
    assert!(matches!(
        popped.heal_type,
        HealType::ErasureSet { ref set_disk_id, .. } if set_disk_id == "pool_0_set_2"
    ));
    assert_eq!(queue.len(), 4);
    let still_blocked = queue.pop_next().expect("blocked request should stay queued");
    assert!(matches!(
        still_blocked.heal_type,
        HealType::ErasureSet { ref set_disk_id, .. } if set_disk_id == "pool_0_set_1"
    ));
}

#[test]
fn test_priority_queue_pop_runnable_restores_all_blocked_items() {
    let mut queue = PriorityHealQueue::new();
    let mut queued_requests = Vec::new();

    for set_disk_id in ["pool_0_set_1", "pool_0_set_2", "pool_0_set_3"] {
        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket".to_string()],
                set_disk_id: set_disk_id.to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );
        let request_id = request.id.clone();
        let dedup_key = PriorityHealQueue::make_dedup_key(&request);
        assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
        queued_requests.push((request_id, dedup_key));
    }

    let mut running = HashMap::new();
    running.insert("pool_0_set_1".to_string(), 1);
    running.insert("pool_0_set_2".to_string(), 1);
    running.insert("pool_0_set_3".to_string(), 1);

    let (popped, skipped_sets) =
        queue.pop_runnable_with_skips(|request| can_schedule_request(request, &running, 1), heal_request_set_key);

    assert!(popped.is_none());
    assert_eq!(
        skipped_sets,
        vec![
            "pool_0_set_1".to_string(),
            "pool_0_set_2".to_string(),
            "pool_0_set_3".to_string(),
        ]
    );
    assert_eq!(queue.len(), 3);
    for (request_id, dedup_key) in &queued_requests {
        assert_eq!(
            queue.queued_request_id_for_dedup_key(dedup_key),
            Some(request_id.as_str()),
            "deferred blocked request must keep its dedup representative"
        );
    }
    for (request_id, _) in queued_requests {
        let request = queue.pop_next().expect("blocked request should remain queued");
        assert_eq!(request.id, request_id, "deferred requests must preserve FIFO order");
    }
}

#[test]
fn test_priority_queue_pop_runnable_restores_deferred_with_tail() {
    let mut queue = PriorityHealQueue::new();

    for (set_disk_id, priority) in [
        ("pool_0_set_1", HealPriority::Urgent),
        ("pool_0_set_2", HealPriority::High),
        ("pool_0_set_3", HealPriority::Normal),
        ("pool_0_set_4", HealPriority::Low),
    ] {
        assert_eq!(
            queue.push(HealRequest::new(
                HealType::ErasureSet {
                    buckets: vec!["bucket".to_string()],
                    set_disk_id: set_disk_id.to_string(),
                },
                HealOptions::default(),
                priority,
            )),
            QueuePushOutcome::Accepted
        );
    }

    let mut running = HashMap::new();
    running.insert("pool_0_set_1".to_string(), 1);
    running.insert("pool_0_set_2".to_string(), 1);

    let (popped, skipped_sets) =
        queue.pop_runnable_with_skips(|request| can_schedule_request(request, &running, 1), heal_request_set_key);

    assert_eq!(skipped_sets, vec!["pool_0_set_1".to_string(), "pool_0_set_2".to_string()]);
    assert!(matches!(
        popped.expect("normal-priority request should be runnable").heal_type,
        HealType::ErasureSet { ref set_disk_id, .. } if set_disk_id == "pool_0_set_3"
    ));
    assert_eq!(queue.len(), 3);
    assert_eq!(
        queue.pop_next().expect("urgent request should remain queued").priority,
        HealPriority::Urgent
    );
    assert_eq!(queue.pop_next().expect("high request should remain queued").priority, HealPriority::High);
    assert_eq!(queue.pop_next().expect("low request should remain queued").priority, HealPriority::Low);
}

#[test]
fn test_can_schedule_request_respects_per_set_limit() {
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket".to_string()],
            set_disk_id: "pool_0_set_1".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    let mut running = HashMap::new();
    running.insert("pool_0_set_1".to_string(), 1);

    assert!(!can_schedule_request(&request, &running, 1));
    assert!(can_schedule_request(&request, &running, 2));
}

#[test]
fn test_can_schedule_scoped_object_request_respects_per_set_limit() {
    let options = HealOptions {
        pool_index: Some(0),
        set_index: Some(1),
        ..Default::default()
    };
    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        options,
        HealPriority::Normal,
    );

    let mut running = HashMap::new();
    running.insert("pool_0_set_1".to_string(), 1);

    assert!(!can_schedule_request(&request, &running, 1));
    assert!(can_schedule_request(&request, &running, 2));
}

#[test]
fn test_heal_request_and_task_metric_labels_match() {
    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealOptions {
            pool_index: Some(0),
            set_index: Some(1),
            ..Default::default()
        },
        HealPriority::Normal,
    );

    assert_eq!(heal_request_type_label(&request), "object");
    assert_eq!(heal_request_set_key(&request), Some("pool_0_set_1".to_string()));
    assert_eq!(heal_request_set_metric_label(&request), "pool_0_set_1");

    let task = HealTask::from_request(request, Arc::new(MockStorage));
    assert_eq!(task.metric_type_label(), "object");
    assert_eq!(task.metric_set_label(), "pool_0_set_1");
}

#[tokio::test]
async fn test_submit_heal_request_returns_merged_for_duplicate() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    assert_eq!(
        manager
            .submit_heal_request(request.clone())
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(request)
            .await
            .expect("duplicate request should produce admission result"),
        HealAdmissionResult::Merged
    );
}

#[tokio::test]
async fn test_admin_duplicate_receipt_returns_canonical_task_without_alias() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let mut original = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    original.source = HealRequestSource::Admin;
    let original_id = original.id.clone();
    let mut duplicate = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    duplicate.source = HealRequestSource::Admin;
    let duplicate_id = duplicate.id.clone();

    let accepted = manager
        .submit_heal_request_with_receipt(original)
        .await
        .expect("first request should be accepted");
    let merged = manager
        .submit_heal_request_with_receipt(duplicate)
        .await
        .expect("duplicate request should merge");

    assert_eq!(accepted.result, HealAdmissionResult::Accepted);
    assert_eq!(accepted.task_id, original_id);
    assert_eq!(merged.result, HealAdmissionResult::Merged);
    assert_eq!(merged.task_id, original_id);
    assert_eq!(manager.canonical_task_id(&duplicate_id).await, duplicate_id);
}

#[tokio::test]
async fn test_task_alias_is_removed_after_terminal_completion() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let original = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let original_id = original.id.clone();
    let duplicate = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let duplicate_id = duplicate.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(original)
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(duplicate)
            .await
            .expect("duplicate request should merge"),
        HealAdmissionResult::Merged
    );
    assert_eq!(manager.canonical_task_id(&duplicate_id).await, original_id);

    process_manager_queue_once(&manager).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(manager.get_task_status(&original_id).await, Ok(HealTaskStatus::Completed)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("task should complete promptly");

    assert_eq!(manager.canonical_task_id(&duplicate_id).await, duplicate_id);
}

#[tokio::test]
async fn test_duplicate_admission_is_atomic_with_queue_to_active_transition() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = Arc::new(HealManager::new(storage.clone(), None));
    let mut original = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    original.source = HealRequestSource::Admin;
    let original_id = original.id.clone();
    assert_eq!(
        manager
            .submit_heal_request(original)
            .await
            .expect("original request should be accepted"),
        HealAdmissionResult::Accepted
    );

    let mut duplicate = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    duplicate.source = HealRequestSource::Admin;
    let hook = Arc::new(DuplicateAdmissionTestHook {
        request_id: duplicate.id.clone(),
        active_lock_reached: Notify::new(),
        active_lock_release: Notify::new(),
    });
    *DUPLICATE_ADMISSION_TEST_HOOK.lock().await = Some(hook.clone());

    let duplicate_manager = manager.clone();
    let duplicate_task = tokio::spawn(async move { duplicate_manager.submit_heal_request_with_receipt(duplicate).await });
    tokio::time::timeout(Duration::from_secs(1), hook.active_lock_reached.notified())
        .await
        .expect("duplicate admission should reach the active lock hook");

    let transition_manager = manager.clone();
    let transition_storage = storage.clone();
    let (attempting_active_tx, attempting_active_rx) = tokio::sync::oneshot::channel();
    let (active_acquired_tx, mut active_acquired_rx) = tokio::sync::oneshot::channel();
    let transition = tokio::spawn(async move {
        let _ = attempting_active_tx.send(());
        let mut active = transition_manager.active_heals.lock().await;
        let _ = active_acquired_tx.send(());
        let mut queue = transition_manager.heal_queue.lock().await;
        let request = queue.pop_next().expect("original request should remain queued");
        let task = Arc::new(HealTask::from_request(request, transition_storage));
        active.insert(task.id.clone(), task);
    });
    tokio::time::timeout(Duration::from_secs(1), attempting_active_rx)
        .await
        .expect("transition should attempt the active lock")
        .expect("transition attempt signal should be delivered");
    assert!(matches!(
        active_acquired_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));

    hook.active_lock_release.notify_one();
    let receipt = tokio::time::timeout(Duration::from_secs(1), duplicate_task)
        .await
        .expect("duplicate admission should not hang")
        .expect("duplicate task should join")
        .expect("duplicate admission should succeed");
    tokio::time::timeout(Duration::from_secs(1), transition)
        .await
        .expect("queue to active transition should not hang")
        .expect("queue to active transition should join");
    *DUPLICATE_ADMISSION_TEST_HOOK.lock().await = None;

    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, original_id);
    assert_eq!(manager.get_queue_length().await, 0);
    assert_eq!(manager.get_active_task_count().await, 1);
}

#[tokio::test]
async fn test_submit_heal_request_returns_merged_for_active_duplicate() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage.clone(), None);
    let active_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let active_task = Arc::new(HealTask::from_request(active_request, storage));
    manager.active_heals.lock().await.insert(active_task.id.clone(), active_task);

    let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);

    assert_eq!(
        manager
            .submit_heal_request(duplicate_request)
            .await
            .expect("active duplicate should produce admission result"),
        HealAdmissionResult::Merged
    );
    assert_eq!(manager.get_queue_length().await, 0);
}

#[tokio::test]
async fn test_active_duplicate_token_can_query_and_cancel_original_task() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage.clone(), None);
    let active_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let active_task = Arc::new(HealTask::from_request(active_request, storage));
    let active_task_id = active_task.id.clone();
    manager.active_heals.lock().await.insert(active_task_id.clone(), active_task);

    let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let duplicate_task_id = duplicate_request.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(duplicate_request)
            .await
            .expect("active duplicate should produce admission result"),
        HealAdmissionResult::Merged
    );
    assert_eq!(
        manager
            .get_task_status_for_path("bucket/object", &duplicate_task_id)
            .await
            .expect("duplicate token should query merged active task"),
        HealTaskStatus::Pending
    );

    manager
        .cancel_task(&duplicate_task_id)
        .await
        .expect("duplicate token should cancel merged active task");

    assert!(manager.active_heals.lock().await.get(&active_task_id).is_none());
    assert!(matches!(manager.get_task_status(&active_task_id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn test_queued_duplicate_token_can_query_and_cancel_original_request() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let original_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let original_task_id = original_request.id.clone();
    let duplicate_request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let duplicate_task_id = duplicate_request.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(original_request)
            .await
            .expect("original request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(duplicate_request)
            .await
            .expect("queued duplicate should produce admission result"),
        HealAdmissionResult::Merged
    );
    assert_eq!(
        manager
            .get_task_status_for_path("bucket/object", &duplicate_task_id)
            .await
            .expect("duplicate token should query merged queued task"),
        HealTaskStatus::Pending
    );

    manager
        .cancel_task(&duplicate_task_id)
        .await
        .expect("duplicate token should cancel merged queued request");

    assert!(matches!(
        manager.get_task_status(&original_task_id).await,
        Err(Error::TaskNotFound { .. })
    ));
}

#[test]
fn test_retry_request_for_recoverable_lock_timeout() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
    let result = Err(Error::TaskExecutionFailed {
        message: "Failed to heal object bucket/object: Lock acquisition timeout".to_string(),
    });

    let (retry_request, retry_delay, retry_error) =
        retry_request_for_result(&task, &result).expect("lock timeout should be retryable");

    assert_eq!(retry_request.id, task.id);
    assert_eq!(retry_request.retry_attempts, 1);
    assert_eq!(retry_request.priority, task.priority);
    assert!(retry_delay > Duration::ZERO);
    assert!(retry_error.contains("Lock acquisition timeout"));
}

#[tokio::test]
async fn retry_request_for_result_preserves_remaining_timeout_budget() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let mut request = HealRequest::object("retry-transition".to_string(), "object".to_string(), None);
    request.options.timeout = Some(Duration::from_secs(60));
    let task = HealTask::from_request(request, storage);
    let result = task.execute().await;

    let (retry_request, _, _) = retry_request_for_result_with_budget(&task, &result)
        .await
        .expect("read quorum failure should retain the unused timeout budget");
    let remaining = retry_request
        .options
        .timeout
        .expect("configured timeout should remain present");
    assert!(remaining < Duration::from_secs(60));
    assert!(remaining > Duration::from_secs(59));
}

#[test]
fn test_retry_request_for_incomplete_heal_rename() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
    let result = Err(Error::TaskExecutionFailed {
        message: "Failed to heal object bucket/object: heal rename incomplete: 1 of 2 targets committed".to_string(),
    });

    let (retry_request, retry_delay, retry_error) =
        retry_request_for_result(&task, &result).expect("incomplete target rename should be retryable");

    assert_eq!(retry_request.id, task.id);
    assert_eq!(retry_request.retry_attempts, 1);
    assert!(retry_delay > Duration::ZERO);
    assert!(retry_error.contains("heal rename incomplete"));
}

#[test]
fn test_retry_request_for_typed_read_quorum_error() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
    let result = Err(Error::Storage(EcstoreError::InsufficientReadQuorum(
        "bucket".to_string(),
        "object".to_string(),
    )));

    let (retry_request, retry_delay, retry_error) =
        retry_request_for_result(&task, &result).expect("typed read quorum should be retryable");

    assert_eq!(retry_request.id, task.id);
    assert_eq!(retry_request.retry_attempts, 1);
    assert!(retry_delay > Duration::ZERO);
    assert!(retry_error.contains("Storage resources are insufficient"));
}

#[test]
fn test_retry_request_for_durable_replacement_retry_signal() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(
        HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket".to_string()],
                set_disk_id: "pool_0_set_0".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        ),
        storage,
    );
    let result = Err(Error::transient_skip("Replacement erasure set heal incomplete; retry scheduled"));

    let (retry_request, retry_delay, retry_error) =
        retry_request_for_result(&task, &result).expect("typed replacement retry signal must be scheduled");

    assert_eq!(retry_request.id, task.id);
    assert_eq!(retry_request.retry_attempts, 1);
    assert!(retry_delay > Duration::ZERO);
    assert!(retry_error.contains("Replacement erasure set heal incomplete"));
}

#[test]
fn durable_replacement_recovery_re_admits_only_the_matching_generation() {
    let task_id = "replacement-generation";
    let mut state = crate::heal::resume::ResumeState::new(
        task_id.to_string(),
        "erasure_set".to_string(),
        "pool_0_set_0".to_string(),
        vec!["bucket".to_string()],
    );
    state.replacement_generation = Some(task_id.to_string());
    state.replacement_phase = ReplacementPhase::Intent;
    state.replacement_targets = vec!["replacement-a".to_string()];
    assert!(!durable_replacement_recovery_is_due(&state, task_id));

    state.retry_count = state.max_retries;
    assert!(durable_replacement_recovery_is_due(&state, task_id));

    state.completed = true;
    state.retry_count = 0;
    state.replacement_phase = ReplacementPhase::Verified;
    assert!(
        durable_replacement_recovery_is_due(&state, task_id),
        "verified terminal cleanup must be re-admitted without re-running recovery"
    );

    state.replacement_phase = ReplacementPhase::CleanupPending;
    assert!(
        durable_replacement_recovery_is_due(&state, task_id),
        "cleanup-pending terminal cleanup must be periodically re-admitted"
    );

    state.completed = false;
    state.replacement_generation = Some("another-generation".to_string());
    assert!(
        !durable_replacement_recovery_is_due(&state, task_id),
        "a task must not adopt another generation's durable intent"
    );

    state.replacement_generation = Some(task_id.to_string());
    state.replacement_targets.clear();
    assert!(
        !durable_replacement_recovery_is_due(&state, task_id),
        "a durable retry without a target is not safe to re-admit"
    );

    state.replacement_targets = vec!["replacement-a".to_string()];
    state.replacement_phase = ReplacementPhase::Verified;
    assert!(
        !durable_replacement_recovery_is_due(&state, task_id),
        "a task must not adopt another generation's terminal cleanup"
    );
}

#[test]
fn replacement_recovery_blocker_is_set_scoped() {
    let manager = HealManager::new(Arc::new(MockStorage), None);

    manager.block_replacement_recovery_set("pool_0_set_0");

    assert!(manager.replacement_recovery_set_is_blocked("pool_0_set_0"));
    assert!(!manager.replacement_recovery_set_is_blocked("pool_0_set_1"));
}

#[test]
fn replacement_recovery_blocks_only_confirmed_conflicts() {
    assert!(crate::heal::resume::replacement_recovery_error_requires_block(
        &Error::TaskExecutionFailed {
            message: "replacement recovery conflict: proof mismatch".to_string(),
        }
    ));
    assert!(crate::heal::resume::replacement_recovery_error_requires_block(
        &Error::TaskExecutionFailed {
            message: "replacement recovery corruption: malformed legacy intent".to_string(),
        }
    ));
    assert!(!crate::heal::resume::replacement_recovery_error_requires_block(&Error::Disk(
        DiskError::Timeout
    )));
    assert!(!crate::heal::resume::replacement_recovery_error_requires_block(
        &Error::TaskExecutionFailed {
            message: "Failed to list replacement recovery records: temporary I/O error".to_string(),
        }
    ));
}

#[test]
fn replacement_recovery_discovery_unformatted_is_quiet_only_for_deferred_endpoint() {
    let error = Error::Disk(DiskError::UnformattedDisk);
    let deferred = HashSet::from(["endpoint-a".to_string()]);

    assert!(replacement_discovery_error_is_expected_for_deferred_endpoint(
        &error,
        "endpoint-a",
        &deferred
    ));
    assert!(!replacement_discovery_error_is_expected_for_deferred_endpoint(
        &error,
        "endpoint-b",
        &deferred
    ));
    assert!(!replacement_discovery_error_is_expected_for_deferred_endpoint(
        &Error::Disk(DiskError::Timeout),
        "endpoint-a",
        &deferred
    ));
}

#[test]
fn replacement_recovery_retry_barrier_requires_all_set_records_to_validate() {
    let mut blocked = HashSet::from(["pool_0_set_0".to_string(), "pool_0_set_1".to_string()]);
    let retry_succeeded = HashSet::from(["pool_0_set_0".to_string(), "pool_0_set_1".to_string()]);
    let retry_failed = HashSet::from(["pool_0_set_0".to_string()]);

    unblock_replacement_recovery_sets_after_validation(&mut blocked, retry_succeeded, &retry_failed);

    assert!(
        blocked.contains("pool_0_set_0"),
        "one failed disk record must keep the whole replacement set blocked"
    );
    assert!(
        !blocked.contains("pool_0_set_1"),
        "a blocked set may resume only after every retried record validates"
    );
}

#[tokio::test]
async fn scheduler_completes_cleanup_pending_recovery_from_manager_anchor() {
    let temp = TempDir::new().expect("temporary manager recovery directory should be created");
    let anchor = make_manager_resume_disk(&temp, "anchor").await;
    let task_id = ResumeUtils::generate_task_id();
    let target = "replacement-a".to_string();
    let identity = ReplacementTargetIdentity {
        endpoint: target.clone(),
        canonical_path: "/replacement/replacement-a".to_string(),
        physical_device_ids: vec!["replacement-a".to_string()],
        filesystem_identity: "identity-replacement-a".to_string(),
    };
    let resume_manager = ResumeManager::new_replacement_intent(
        anchor.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec![target.clone()],
        vec![identity],
    )
    .await
    .expect("cleanup-pending replacement state should persist on the survivor anchor");
    resume_manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("completion proof should persist before cleanup");
    resume_manager
        .mark_replacement_cleanup_pending()
        .await
        .expect("cleanup-pending state should persist before restart");
    CheckpointManager::new(anchor.clone(), task_id.clone())
        .await
        .expect("checkpoint fixture should persist");

    let (hook, _hook_guard) = ManagerRecoveryTestHook::install(anchor.clone());
    let storage = Arc::new(MockStorage);
    let manager = HealManager::new(storage.clone(), None);
    let mut request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            pool_index: Some(0),
            set_index: Some(0),
            ..HealOptions::default()
        },
        HealPriority::Low,
    );
    request.id = task_id.clone();
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec![target];
    assert_eq!(
        manager
            .submit_heal_request(request)
            .await
            .expect("durable recovery request should be admitted"),
        HealAdmissionResult::Accepted
    );
    manager
        .replacement_recovery_anchors
        .lock()
        .expect("replacement recovery anchor lock should not poison")
        .insert(task_id.clone(), anchor.endpoint().to_string());

    process_manager_queue_once(&manager).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resume_removed = !ResumeManager::has_resume_state(&anchor, &task_id).await;
            let checkpoint_removed = !CheckpointManager::has_checkpoint(&anchor, &task_id).await;
            let anchor_removed = !manager
                .replacement_recovery_anchors
                .lock()
                .expect("replacement recovery anchor lock should not poison")
                .contains_key(&task_id);
            if resume_removed && checkpoint_removed && anchor_removed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup-pending recovery should finish through the manager scheduler");

    assert!(
        !*hook.listed.lock().expect("manager recovery listed lock should not poison"),
        "cleanup-pending recovery must not list buckets or restart object healing"
    );
    assert_eq!(
        *hook
            .global_format_calls
            .lock()
            .expect("manager recovery global format call lock should not poison"),
        0
    );
    assert_eq!(
        *hook
            .replacement_format_calls
            .lock()
            .expect("manager recovery replacement format call lock should not poison"),
        0,
        "manager-resumed terminal cleanup must not format replacement targets"
    );
    assert_eq!(
        *hook
            .bucket_heal_calls
            .lock()
            .expect("manager recovery bucket call lock should not poison"),
        0
    );
    assert_eq!(
        *hook
            .heal_object_calls
            .lock()
            .expect("manager recovery object call lock should not poison"),
        0
    );
}

#[test]
fn test_retry_request_for_scoped_slowdown_preserves_scope() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(
        HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions {
                pool_index: Some(0),
                set_index: Some(1),
                ..Default::default()
            },
            HealPriority::Normal,
        ),
        storage,
    );
    let result = Err(Error::Storage(EcstoreError::SlowDown));

    let (retry_request, retry_delay, _) = retry_request_for_result(&task, &result).expect("SlowDown should defer scoped heal");

    assert_eq!(retry_request.options.pool_index, Some(0));
    assert_eq!(retry_request.options.set_index, Some(1));
    assert_eq!(retry_request.retry_attempts, 1);
    assert!(retry_delay > Duration::ZERO);
}

#[test]
fn test_retry_request_for_typed_not_found_error_is_not_retryable() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(HealRequest::object("bucket".to_string(), "object".to_string(), None), storage);
    let result = Err(Error::Storage(EcstoreError::ObjectNotFound("bucket".to_string(), "object".to_string())));

    assert!(retry_request_for_result(&task, &result).is_none());
}

#[test]
fn test_retry_request_for_recoverable_error_stops_at_limit() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let mut request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    request.retry_attempts = MAX_RECOVERABLE_HEAL_RETRIES;
    let task = HealTask::from_request(request, storage);
    let result = Err(Error::TaskExecutionFailed {
        message: "Remote lock RPC timed out".to_string(),
    });

    assert!(retry_request_for_result(&task, &result).is_none());
}

#[tokio::test]
async fn test_retry_request_does_not_rescan_batch_after_object_retries_exhausted() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let task = HealTask::from_request(HealRequest::bucket("bucket".to_string()), storage);
    let result = Err(task
        .record_batch_failure(BatchHealFailure {
            scope: "bucket:bucket".to_string(),
            failed: 1,
            retryable: 1,
            permanent: 0,
            first_object: "object".to_string(),
            first_error: "Lock acquisition timeout".to_string(),
        })
        .await);

    assert!(retry_request_for_result(&task, &result).is_none());
}

#[test]
fn test_heal_type_matches_path_normalizes_prefix_trailing_slash() {
    let heal_type = HealType::Prefix {
        bucket: "bucket".to_string(),
        prefix: "logs/".to_string(),
    };

    assert!(heal_type_matches_path(&heal_type, "bucket"));
    assert!(heal_type_matches_path(&heal_type, "bucket/logs"));
    assert!(heal_type_matches_path(&heal_type, "bucket/logs/"));
}

#[test]
fn test_heal_type_matches_path_normalizes_object_trailing_slash() {
    let heal_type = HealType::Object {
        bucket: "bucket".to_string(),
        object: "object/".to_string(),
        version_id: None,
    };

    assert!(heal_type_matches_path(&heal_type, "bucket/object"));
    assert!(heal_type_matches_path(&heal_type, "bucket/object/"));
}

async fn insert_retrying_request(manager: &HealManager, request: HealRequest) -> CancellationToken {
    let task_id = request.id.clone();
    let cancel_token = CancellationToken::new();
    manager.retrying_heals.lock().await.insert(
        task_id.clone(),
        RetryingHeal {
            request: request.clone(),
            error: "Lock acquisition timeout".to_string(),
            cancel_token: cancel_token.clone(),
        },
    );
    manager.completed_heals.lock().await.insert(
        task_id,
        Arc::new(CompletedHealStatus {
            heal_type: request.heal_type,
            status: HealTaskStatus::Retrying {
                error: "Lock acquisition timeout".to_string(),
                retry_attempt: request.retry_attempts,
            },
            result_items_truncated: false,
            seqed_items: Vec::new(),
            next_seq: 0,
            min_seq: 0,
            completed_at: SystemTime::now(),
        }),
    );
    cancel_token
}

#[tokio::test]
async fn test_cancel_task_cancels_retrying_backoff() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let mut request = HealRequest::bucket("bucket".to_string());
    request.retry_attempts = 1;
    let task_id = request.id.clone();
    let cancel_token = insert_retrying_request(&manager, request).await;

    assert!(matches!(
        manager
            .get_task_status(&task_id)
            .await
            .expect("retrying task should be queryable"),
        HealTaskStatus::Retrying { .. }
    ));

    manager
        .cancel_task(&task_id)
        .await
        .expect("retrying task should be cancellable by token");

    assert!(cancel_token.is_cancelled());
    assert!(manager.retrying_heals.lock().await.get(&task_id).is_none());
    assert!(matches!(manager.get_task_status(&task_id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn test_cancel_tasks_for_path_cancels_retrying_backoff() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let mut request = HealRequest::bucket("bucket".to_string());
    request.retry_attempts = 1;
    let task_id = request.id.clone();
    let cancel_token = insert_retrying_request(&manager, request).await;

    assert_eq!(
        manager
            .cancel_tasks_for_path("bucket")
            .await
            .expect("retrying task should be cancellable by path"),
        1
    );

    assert!(cancel_token.is_cancelled());
    assert!(manager.retrying_heals.lock().await.get(&task_id).is_none());
    assert!(matches!(manager.get_task_status(&task_id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn test_cancel_tasks_for_empty_path_cancels_queued_cluster_only() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
    let cluster_request_id = cluster_request.id.clone();
    let bucket_request = HealRequest::bucket("bucket".to_string());
    let bucket_request_id = bucket_request.id.clone();

    manager
        .submit_heal_request(cluster_request)
        .await
        .expect("cluster request should be accepted");
    manager
        .submit_heal_request(bucket_request)
        .await
        .expect("bucket request should be accepted");

    assert_eq!(
        manager
            .cancel_tasks_for_path("")
            .await
            .expect("root path should cancel queued cluster task"),
        1
    );
    assert!(matches!(
        manager.get_task_status(&cluster_request_id).await,
        Err(Error::TaskNotFound { .. })
    ));
    assert_eq!(
        manager
            .get_task_status(&bucket_request_id)
            .await
            .expect("bucket request should not match root path"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn test_cancel_tasks_for_empty_path_cancels_active_cluster_only() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage.clone(), None);

    let cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
    let cluster_request_id = cluster_request.id.clone();
    let bucket_request = HealRequest::bucket("bucket".to_string());
    let bucket_request_id = bucket_request.id.clone();

    manager.active_heals.lock().await.insert(
        cluster_request_id.clone(),
        Arc::new(HealTask::from_request(cluster_request, storage.clone())),
    );
    manager
        .active_heals
        .lock()
        .await
        .insert(bucket_request_id.clone(), Arc::new(HealTask::from_request(bucket_request, storage)));

    assert_eq!(
        manager
            .cancel_tasks_for_path("")
            .await
            .expect("root path should cancel active cluster task"),
        1
    );
    assert!(manager.active_heals.lock().await.get(&cluster_request_id).is_none());
    assert!(manager.active_heals.lock().await.get(&bucket_request_id).is_some());
}

#[tokio::test]
async fn test_cancel_tasks_for_empty_path_cancels_retrying_cluster_only() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let mut cluster_request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
    cluster_request.retry_attempts = 1;
    let cluster_request_id = cluster_request.id.clone();
    let cluster_cancel_token = insert_retrying_request(&manager, cluster_request).await;

    let mut bucket_request = HealRequest::bucket("bucket".to_string());
    bucket_request.retry_attempts = 1;
    let bucket_request_id = bucket_request.id.clone();
    let bucket_cancel_token = insert_retrying_request(&manager, bucket_request).await;

    assert_eq!(
        manager
            .cancel_tasks_for_path("")
            .await
            .expect("root path should cancel retrying cluster task"),
        1
    );
    assert!(cluster_cancel_token.is_cancelled());
    assert!(!bucket_cancel_token.is_cancelled());
    assert!(manager.retrying_heals.lock().await.get(&cluster_request_id).is_none());
    assert!(manager.retrying_heals.lock().await.get(&bucket_request_id).is_some());
}

#[test]
fn test_heal_type_matches_path_accepts_legacy_root() {
    assert!(heal_type_matches_path(&HealType::Cluster, LEGACY_ROOT_HEAL_PATH));
    assert!(!heal_type_matches_path(
        &HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        LEGACY_ROOT_HEAL_PATH,
    ));
}

#[tokio::test]
async fn test_retrying_duplicate_token_can_query_and_cancel_original_retry() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let mut original_request = HealRequest::bucket("bucket".to_string());
    original_request.retry_attempts = 1;
    let original_task_id = original_request.id.clone();
    let cancel_token = insert_retrying_request(&manager, original_request).await;

    let duplicate_request = HealRequest::bucket("bucket".to_string());
    let duplicate_task_id = duplicate_request.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(duplicate_request)
            .await
            .expect("retrying duplicate should produce admission result"),
        HealAdmissionResult::Merged
    );
    assert!(matches!(
        manager
            .get_task_status_for_path("bucket", &duplicate_task_id)
            .await
            .expect("duplicate token should query merged retrying task"),
        HealTaskStatus::Retrying { .. }
    ));

    manager
        .cancel_task(&duplicate_task_id)
        .await
        .expect("duplicate token should cancel merged retrying task");

    assert!(cancel_token.is_cancelled());
    assert!(manager.retrying_heals.lock().await.get(&original_task_id).is_none());
}

#[tokio::test]
async fn test_get_task_status_reports_pending_for_queued_request() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::bucket("bucket".to_string());
    let request_id = request.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(request)
            .await
            .expect("request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .get_task_status(&request_id)
            .await
            .expect("queued request should have status"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn test_operations_snapshot_counts_queue_by_source_and_priority() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let mut scanner_request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "object-a".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    scanner_request.source = HealRequestSource::Scanner;

    let mut admin_request = HealRequest::bucket("bucket-b".to_string());
    admin_request.priority = HealPriority::High;
    admin_request.source = HealRequestSource::Admin;

    let mut auto_request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-c".to_string()],
            set_disk_id: "0-0".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );
    auto_request.source = HealRequestSource::AutoHeal;

    manager
        .submit_heal_request(scanner_request)
        .await
        .expect("scanner request should be accepted");
    manager
        .submit_heal_request(admin_request)
        .await
        .expect("admin request should be accepted");
    manager
        .submit_heal_request(auto_request)
        .await
        .expect("auto request should be accepted");

    let snapshot = manager.operations_snapshot().await;

    assert_eq!(snapshot.queue_length, 3);
    assert_eq!(snapshot.active_tasks, 0);
    assert_eq!(snapshot.queued_by_priority.low, 1);
    assert_eq!(snapshot.queued_by_priority.normal, 1);
    assert_eq!(snapshot.queued_by_priority.high, 1);
    assert_eq!(snapshot.queued_by_priority.urgent, 0);
    assert_eq!(snapshot.queued_by_source.scanner, 1);
    assert_eq!(snapshot.queued_by_source.admin, 1);
    assert_eq!(snapshot.queued_by_source.auto_heal, 1);
    assert_eq!(snapshot.queued_by_source.internal, 0);
}

// HS-06 (backlog#1870): overlap policy + forceStart semantics.
fn manager_with_policy(policy: HealOverlapPolicy) -> HealManager {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    HealManager::new(
        storage,
        Some(HealConfig {
            overlap_policy: policy,
            ..Default::default()
        }),
    )
}

fn admin_prefix_request(bucket: &str, prefix: &str) -> HealRequest {
    let mut request = HealRequest::new(
        HealType::Prefix {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    request
}

async fn insert_active_task(manager: &HealManager, request: HealRequest) -> String {
    let task = Arc::new(HealTask::from_request(request, manager.storage.clone()));
    let task_id = task.id.clone();
    manager.active_heals.lock().await.insert(task_id.clone(), task);
    task_id
}

#[tokio::test]
async fn overlap_policy_minio_error_rejects_same_and_containing_paths() {
    let manager = manager_with_policy(HealOverlapPolicy::MinioError);
    insert_active_task(&manager, admin_prefix_request("bucket-a", "logs/")).await;

    // Same target: typed AlreadyRunning.
    let same = manager
        .submit_heal_request(admin_prefix_request("bucket-a", "logs/"))
        .await
        .expect("admission must decide");
    assert_eq!(
        same,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning),
        "an identical target must reject with already-running"
    );

    // Contained path: typed OverlappingPaths.
    let nested = manager
        .submit_heal_request(admin_prefix_request("bucket-a", "logs/app/"))
        .await
        .expect("admission must decide");
    assert_eq!(
        nested,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths),
        "a path inside the active task's path must reject with overlapping-paths"
    );

    // Containing path (bucket-wide vs nested active): also overlapping.
    let wide = manager
        .submit_heal_request(admin_prefix_request("bucket-a", ""))
        .await
        .expect("admission must decide");
    assert_eq!(
        wide,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths),
        "a bucket-wide start overlapping a nested active heal must reject"
    );

    // Disjoint bucket: unaffected.
    let disjoint = manager
        .submit_heal_request(admin_prefix_request("bucket-b", "logs/"))
        .await
        .expect("admission must decide");
    assert_eq!(disjoint, HealAdmissionResult::Accepted);
}

#[tokio::test]
async fn overlap_policy_default_merge_keeps_today_semantics() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    insert_active_task(&manager, admin_prefix_request("bucket-a", "logs/")).await;

    // Different-dedup-key overlap still merges under the default policy:
    // the nested path dedups to its own key but nothing rejects it.
    let nested = manager
        .submit_heal_request(admin_prefix_request("bucket-a", "logs/app/"))
        .await
        .expect("admission must decide");
    assert_eq!(nested, HealAdmissionResult::Accepted, "default policy must not reject overlaps");

    // Non-admin sources never get overlap rejections even under minio_error.
    let manager = manager_with_policy(HealOverlapPolicy::MinioError);
    insert_active_task(&manager, admin_prefix_request("bucket-a", "logs/")).await;
    let mut scanner_request = admin_prefix_request("bucket-a", "logs/app/");
    scanner_request.source = HealRequestSource::Scanner;
    let admitted = manager
        .submit_heal_request(scanner_request)
        .await
        .expect("admission must decide");
    assert_eq!(admitted, HealAdmissionResult::Accepted, "scanner sources must never be overlap-rejected");
}

#[tokio::test]
async fn admin_force_start_cancels_overlapping_active_task_first() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    let old_id = insert_active_task(&manager, admin_prefix_request("bucket-a", "logs/")).await;

    let mut replacement = admin_prefix_request("bucket-a", "logs/");
    replacement.force_start = true;
    let receipt = manager
        .submit_heal_request_with_receipt(replacement)
        .await
        .expect("force-start submission must decide");

    assert!(receipt.result.is_admitted(), "the new task must be admitted (Accepted or Merged)");
    let old_task_gone = {
        let active_heals = manager.active_heals.lock().await;
        !active_heals.contains_key(&old_id)
    };
    assert!(
        old_task_gone,
        "the overlapping admin task must be cancelled (removed from the active table) before the new one starts"
    );
    assert!(
        matches!(manager.get_task_status(&old_id).await, Err(Error::TaskNotFound { .. })),
        "a cancelled task must no longer resolve as an active heal"
    );
}

#[tokio::test]
async fn test_operations_snapshot_counts_active_by_source_and_priority() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let mut request = HealRequest::bucket("bucket-a".to_string());
    request.priority = HealPriority::High;
    request.source = HealRequestSource::Admin;
    let task = Arc::new(HealTask::from_request(request, manager.storage.clone()));
    let task_id = task.id.clone();

    manager.active_heals.lock().await.insert(task_id, task);

    let snapshot = manager.operations_snapshot().await;

    assert_eq!(snapshot.queue_length, 0);
    assert_eq!(snapshot.active_tasks, 1);
    assert_eq!(snapshot.active_by_priority.high, 1);
    assert_eq!(snapshot.active_by_source.admin, 1);
    assert_eq!(snapshot.active_by_source.scanner, 0);
}

#[tokio::test]
async fn test_operations_snapshot_counts_retry_backoff_as_owned_work() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);
    let mut request = HealRequest::bucket("bucket-retry".to_string());
    request.priority = HealPriority::Urgent;
    request.source = HealRequestSource::Admin;
    let task_id = request.id.clone();
    manager.retrying_heals.lock().await.insert(
        task_id,
        RetryingHeal {
            request,
            error: "transient".to_string(),
            cancel_token: CancellationToken::new(),
        },
    );

    let snapshot = manager.operations_snapshot().await;

    assert_eq!(snapshot.queue_length, 0);
    assert_eq!(snapshot.active_tasks, 0);
    assert_eq!(snapshot.retrying_tasks, 1);
    assert_eq!(snapshot.retrying_by_priority.urgent, 1);
    assert_eq!(snapshot.retrying_by_source.admin, 1);
}

#[tokio::test]
async fn test_scheduler_retry_transitions_keep_continuous_single_ownership() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = Arc::new(HealManager::new(storage, None));
    {
        let mut config = manager.config.write().await;
        config.enable_auto_heal = false;
        config.heal_interval = Duration::from_millis(10);
        config.event_driven_scheduler_enable = true;
    }
    manager.start().await.expect("manager should start");

    let request = HealRequest::object("retry-transition".to_string(), "object".to_string(), None);
    let task_id = request.id.clone();
    let hook = Arc::new(RetryOwnershipTestHook {
        task_id: task_id.clone(),
        active_to_retrying_reached: Notify::new(),
        active_to_retrying_release: Notify::new(),
        retrying_to_queue_reached: Notify::new(),
        retrying_to_queue_release: Notify::new(),
    });
    *RETRY_OWNERSHIP_TEST_HOOK.lock().await = Some(hook.clone());

    manager
        .submit_heal_request(request)
        .await
        .expect("retry test request should be accepted");
    tokio::time::timeout(Duration::from_secs(2), hook.active_to_retrying_reached.notified())
        .await
        .expect("active to retrying transition should be reached");

    let snapshot_manager = manager.clone();
    let mut snapshot = tokio::spawn(async move { snapshot_manager.operations_snapshot().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut snapshot).await.is_err(),
        "snapshot must wait while active and retrying ownership locks are held"
    );
    hook.active_to_retrying_release.notify_one();
    let snapshot = tokio::time::timeout(Duration::from_secs(1), snapshot)
        .await
        .expect("snapshot should resume after active to retrying handoff")
        .expect("snapshot task should complete");
    assert_eq!(snapshot.active_tasks + snapshot.queue_length + snapshot.retrying_tasks, 1);
    assert_eq!(snapshot.retrying_tasks, 1);

    tokio::time::timeout(Duration::from_secs(5), hook.retrying_to_queue_reached.notified())
        .await
        .expect("retrying to queue transition should be reached");
    let snapshot_manager = manager.clone();
    let mut snapshot = tokio::spawn(async move { snapshot_manager.operations_snapshot().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut snapshot).await.is_err(),
        "snapshot must wait while queue ownership is transferred"
    );
    hook.retrying_to_queue_release.notify_one();
    // The scheduler may immediately execute the retried request again;
    // leave a permit so a second test-only active handoff cannot stall it.
    hook.active_to_retrying_release.notify_one();
    let snapshot = tokio::time::timeout(Duration::from_secs(1), snapshot)
        .await
        .expect("snapshot should resume after retrying to queue handoff")
        .expect("snapshot task should complete");
    assert_eq!(snapshot.active_tasks + snapshot.queue_length + snapshot.retrying_tasks, 1);

    *RETRY_OWNERSHIP_TEST_HOOK.lock().await = None;
    hook.active_to_retrying_release.notify_one();
    hook.retrying_to_queue_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), manager.stop())
        .await
        .expect("manager stop should not stall")
        .expect("manager should stop");
}

#[tokio::test]
async fn test_active_progress_snapshot_sums_active_task_progress() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let first = Arc::new(HealTask::from_request(
        HealRequest::bucket("bucket-a".to_string()),
        manager.storage.clone(),
    ));
    {
        let mut progress = first.progress.write().await;
        progress.start_time = Some(SystemTime::now() - Duration::from_secs(20));
        progress.set_total_baseline(12, 8192);
        progress.update_progress(7, 3, 1, 4096);
    }

    let second = Arc::new(HealTask::from_request(
        HealRequest::bucket("bucket-b".to_string()),
        manager.storage.clone(),
    ));
    {
        let mut progress = second.progress.write().await;
        progress.start_time = Some(SystemTime::now() - Duration::from_secs(10));
        progress.set_total_baseline(8, 4096);
        progress.update_progress(11, 5, 2, 2048);
    }

    manager.active_heals.lock().await.insert(first.id.clone(), first);
    manager.active_heals.lock().await.insert(second.id.clone(), second);

    let progress = manager
        .active_progress_snapshot()
        .await
        .expect("active progress should exist");

    assert_eq!(progress.objects_scanned, 18);
    assert_eq!(progress.objects_healed, 8);
    assert_eq!(progress.objects_failed, 3);
    assert_eq!(progress.objects_total_count, 20);
    assert_eq!(progress.objects_total_size, 12288);
    assert_eq!(progress.bytes_processed, 6144);
    assert!((progress.progress_percentage - 50.0).abs() < 0.001);
    assert!(progress.estimated_completion_time.is_some());
}

#[tokio::test]
async fn test_get_task_status_for_path_rejects_wrong_token_when_path_is_active() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    manager
        .submit_heal_request(HealRequest::bucket("bucket".to_string()))
        .await
        .expect("request should be accepted");

    assert!(matches!(
        manager.get_task_status_for_path("bucket", "wrong-token").await,
        Err(Error::InvalidClientToken)
    ));
}

#[tokio::test]
async fn test_get_task_status_for_path_rejects_token_from_other_active_path() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let bucket_request = HealRequest::bucket("bucket".to_string());
    let other_request = HealRequest::bucket("other".to_string());
    let other_request_id = other_request.id.clone();

    manager
        .submit_heal_request(bucket_request)
        .await
        .expect("bucket request should be accepted");
    manager
        .submit_heal_request(other_request)
        .await
        .expect("other request should be accepted");

    assert!(matches!(
        manager.get_task_status_for_path("bucket", &other_request_id).await,
        Err(Error::InvalidClientToken)
    ));
}

#[tokio::test]
async fn test_get_task_status_for_path_does_not_accept_token_from_inactive_path() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::bucket("bucket".to_string());
    let request_id = request.id.clone();

    manager
        .submit_heal_request(request)
        .await
        .expect("request should be accepted");

    assert!(matches!(
        manager.get_task_status_for_path("other", &request_id).await,
        Err(Error::TaskNotFound { .. })
    ));
}

#[tokio::test]
async fn test_get_task_status_for_path_returns_not_found_when_path_is_inactive() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    assert!(matches!(
        manager.get_task_status_for_path("bucket", "old-token").await,
        Err(Error::TaskNotFound { .. })
    ));
}

#[tokio::test]
async fn test_get_task_status_for_empty_path_does_not_match_unrelated_tasks() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::bucket("bucket".to_string());
    let request_id = request.id.clone();

    manager
        .submit_heal_request(request)
        .await
        .expect("request should be accepted");

    assert!(matches!(
        manager.get_task_status_for_path("", &request_id).await,
        Err(Error::TaskNotFound { .. })
    ));
    assert!(matches!(
        manager.get_task_status_for_path("", "wrong-token").await,
        Err(Error::TaskNotFound { .. })
    ));
}

#[tokio::test]
async fn test_get_task_report_queries_queued_task_by_token_without_path() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec![],
            set_disk_id: "pool_0_set_1".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    let request_id = request.id.clone();

    manager
        .submit_heal_request(request)
        .await
        .expect("request should be accepted");

    let report = manager
        .get_task_report(&request_id)
        .await
        .expect("queued task should be queryable by token");

    assert_eq!(report.status, HealTaskStatus::Pending);
    assert!(report.result_items.is_empty());
}

#[tokio::test]
async fn test_retrying_completion_outranks_the_queue_for_the_same_id() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    // A completed entry recorded in a Retrying state for a task whose
    // request is also (still) queued under the same id: the retrying
    // completion must win the lookup, or the task would read back as
    // Pending while it is actually waiting out a retry backoff.
    let request = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    let task_id = request.id.clone();
    manager.completed_heals.lock().await.insert(
        task_id.clone(),
        Arc::new(CompletedHealStatus {
            heal_type: request.heal_type.clone(),
            status: HealTaskStatus::Retrying {
                error: "transient disk failure".to_string(),
                retry_attempt: 1,
            },
            result_items_truncated: false,
            seqed_items: Vec::new(),
            next_seq: 0,
            min_seq: 0,
            completed_at: SystemTime::now(),
        }),
    );
    manager.heal_queue.lock().await.push(HealRequest {
        id: task_id.clone(),
        heal_type: request.heal_type,
        ..request
    });

    assert_eq!(
        manager.get_task_status(&task_id).await.expect("task must resolve"),
        HealTaskStatus::Retrying {
            error: "transient disk failure".to_string(),
            retry_attempt: 1
        }
    );
}

#[tokio::test]
async fn test_get_task_status_reads_recent_completed_status() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    manager.completed_heals.lock().await.insert(
        "completed-token".to_string(),
        Arc::new(CompletedHealStatus {
            heal_type: HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            status: HealTaskStatus::Completed,
            result_items_truncated: false,
            seqed_items: Vec::new(),
            next_seq: 0,
            min_seq: 0,
            completed_at: SystemTime::now(),
        }),
    );

    assert_eq!(
        manager
            .get_task_status_for_path("bucket", "completed-token")
            .await
            .expect("recent completed task should be queryable"),
        HealTaskStatus::Completed
    );
}

#[tokio::test]
async fn test_get_task_report_for_path_reads_completed_items() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    manager.completed_heals.lock().await.insert(
        "completed-token".to_string(),
        Arc::new(CompletedHealStatus {
            heal_type: HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            status: HealTaskStatus::Completed,
            result_items_truncated: true,
            seqed_items: vec![(
                1,
                HealResultItem {
                    bucket: "bucket".to_string(),
                    object: "object".to_string(),
                    object_size: 1024,
                    ..Default::default()
                },
            )],
            next_seq: 2,
            min_seq: 1,
            completed_at: SystemTime::now(),
        }),
    );

    let report = manager
        .get_task_report_for_path("bucket/object", "completed-token")
        .await
        .expect("recent completed task report should be queryable");
    assert!(report.result_items_truncated);

    assert_eq!(report.status, HealTaskStatus::Completed);
    assert_eq!(report.result_items.len(), 1);
    assert_eq!(report.result_items[0].object_size, 1024);
    // The archived cursors pass through to the report so an incremental
    // consumer can resume against the next expected sequence.
    assert_eq!(report.next_seq, 2);
    assert_eq!(report.min_seq, 1);
}

#[tokio::test]
async fn test_get_task_report_for_empty_path_does_not_match_unrelated_tasks() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    manager
        .submit_heal_request(HealRequest::bucket("bucket".to_string()))
        .await
        .expect("request should be accepted");

    assert!(matches!(
        manager.get_task_report_for_path("", "wrong-token").await,
        Err(Error::TaskNotFound { .. })
    ));
}

#[tokio::test]
async fn test_cancel_task_removes_queued_request() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let request = HealRequest::bucket("bucket".to_string());
    let request_id = request.id.clone();

    manager
        .submit_heal_request(request)
        .await
        .expect("request should be accepted");
    manager
        .cancel_task(&request_id)
        .await
        .expect("queued request should be cancelled");

    assert!(matches!(manager.get_task_status(&request_id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn test_mrf_repaired_notice_waits_for_successful_completion() {
    let bucket = "mrf-completion-success";
    let object = "object";
    let version_id = Some([9u8; 16]);
    let _ = rustfs_common::mrf_channel::take_mrf_repaired_events_for(bucket);
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let mut request = HealRequest::object(bucket.to_string(), object.to_string(), None);
    request.source = HealRequestSource::Mrf;
    let receipt = manager
        .submit_mrf_heal_request_with_receipt(request, Arc::from(bucket), Arc::from(object), version_id)
        .await
        .expect("MRF request should be admitted");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert!(
        manager
            .mrf_repair_notice_targets
            .lock()
            .expect("mrf repair notice registry poisoned")
            .contains_key(&receipt.task_id),
        "MRF notice ownership must be registered before the scheduler can observe the queued task"
    );

    assert!(
        rustfs_common::mrf_channel::take_mrf_repaired_events_for(bucket).is_empty(),
        "admission alone must not clear the scanner pending-heal ledger"
    );

    process_manager_queue_once(&manager).await;
    for _ in 0..100 {
        let events = rustfs_common::mrf_channel::take_mrf_repaired_events_for(bucket);
        if !events.is_empty() {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].object.as_ref(), object);
            assert_eq!(events[0].version_id, version_id);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("successful MRF-owned heal should emit one repaired event");
}

#[tokio::test]
async fn test_mrf_repaired_notice_removed_on_queued_cancel_without_event() {
    let bucket = "mrf-completion-cancel";
    let object = "object";
    let _ = rustfs_common::mrf_channel::take_mrf_repaired_events_for(bucket);
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let mut request = HealRequest::object(bucket.to_string(), object.to_string(), None);
    request.source = HealRequestSource::Mrf;
    let receipt = manager
        .submit_mrf_heal_request_with_receipt(request, Arc::from(bucket), Arc::from(object), None)
        .await
        .expect("MRF request should be admitted");

    manager
        .cancel_task(&receipt.task_id)
        .await
        .expect("queued MRF request should cancel");

    assert!(
        rustfs_common::mrf_channel::take_mrf_repaired_events_for(bucket).is_empty(),
        "cancelled MRF-owned heal must not emit a repaired event"
    );
    assert!(
        manager
            .mrf_repair_notice_targets
            .lock()
            .expect("mrf repair notice registry poisoned")
            .is_empty(),
        "cancel must discard completion notice ownership"
    );
}

#[tokio::test]
async fn test_cancel_tasks_for_path_removes_matching_queued_requests() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(storage, None);

    let bucket_request = HealRequest::bucket("bucket".to_string());
    let bucket_request_id = bucket_request.id.clone();
    let other_request = HealRequest::bucket("other".to_string());
    let other_request_id = other_request.id.clone();

    manager
        .submit_heal_request(bucket_request)
        .await
        .expect("bucket request should be accepted");
    manager
        .submit_heal_request(other_request)
        .await
        .expect("other request should be accepted");

    assert_eq!(
        manager
            .cancel_tasks_for_path("bucket")
            .await
            .expect("matching request should be cancelled"),
        1
    );
    assert!(matches!(
        manager.get_task_status(&bucket_request_id).await,
        Err(Error::TaskNotFound { .. })
    ));
    assert_eq!(
        manager
            .get_task_status(&other_request_id)
            .await
            .expect("unmatched request should remain queued"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn test_submit_heal_request_returns_merged_before_full_for_duplicate() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );

    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    assert_eq!(
        manager
            .submit_heal_request(request.clone())
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(request)
            .await
            .expect("duplicate request should merge even when queue is full"),
        HealAdmissionResult::Merged
    );
}

#[tokio::test]
async fn test_submit_heal_request_returns_dropped_for_low_priority_when_full() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            low_priority_drop_when_full: true,
            ..HealConfig::default()
        }),
    );

    let accepted = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );
    let dropped = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-b".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    assert_eq!(
        manager
            .submit_heal_request(accepted)
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(dropped)
            .await
            .expect("low priority request should be dropped with explicit admission result"),
        HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)
    );
}

#[tokio::test]
async fn test_submit_heal_request_returns_full_for_normal_priority_when_full() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );

    let accepted = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );
    let full = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-b".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );

    assert_eq!(
        manager
            .submit_heal_request(accepted)
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(full)
            .await
            .expect("normal priority request should surface full admission"),
        HealAdmissionResult::Full
    );
}

#[tokio::test]
async fn test_high_priority_request_displaces_lower_priority_when_queue_full() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );

    let low = HealRequest::new(
        HealType::Bucket {
            bucket: "background-bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    let low_id = low.id.clone();
    let high = HealRequest::new(
        HealType::Bucket {
            bucket: "manual-bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    let high_id = high.id.clone();

    assert_eq!(
        manager
            .submit_heal_request(low)
            .await
            .expect("low priority request should be accepted first"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(high)
            .await
            .expect("high priority request should be admitted by displacing lower priority work"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(manager.get_queue_length().await, 1);
    assert!(matches!(
        manager.get_task_status(&low_id).await,
        Ok(HealTaskStatus::Failed { error }) if error.contains("reason=displaced")
    ));
    assert_eq!(
        manager
            .get_task_status(&high_id)
            .await
            .expect("high priority request should remain queued"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn displaced_task_remains_queryable() {
    let manager = HealManager::new(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );
    let mut displaced = HealRequest::new(
        HealType::Bucket {
            bucket: "displaced-bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    displaced.id = "displaced-task".to_string();
    let displaced_id = displaced.id.clone();
    manager
        .submit_heal_request(displaced)
        .await
        .expect("displaced request should queue");

    let successor = HealRequest::new(
        HealType::Bucket {
            bucket: "successor-bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    manager
        .submit_heal_request(successor)
        .await
        .expect("successor should displace low work");

    let report = manager
        .get_task_report(&displaced_id)
        .await
        .expect("displaced report should remain queryable");
    assert!(matches!(report.status, HealTaskStatus::Failed { ref error } if error.contains("reason=displaced")));
}

#[tokio::test]
async fn displaced_archive_failure_keeps_queryable_terminal() {
    let manager = HealManager::new(Arc::new(MockStorage), None);
    let mut request = HealRequest::new(
        HealType::Bucket {
            bucket: "archive-failure".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    request.id = "archive-failure-task".to_string();
    let request_id = request.id.clone();
    // The synchronous sidecar is the authoritative fallback when the normal
    // completed-task archive has no entry (the failure window that must not
    // turn an Accepted ID into NotFound).
    record_displaced_terminal(&manager.displaced_terminals, &request);
    assert!(manager.completed_heals.lock().await.is_empty());
    assert!(matches!(
        manager.get_task_status(&request_id).await,
        Ok(HealTaskStatus::Failed { error }) if error.contains("reason=displaced")
    ));
}

#[tokio::test]
async fn scheduler_retry_displacement_keeps_evicted_task_queryable() {
    let manager = Arc::new(HealManager::new(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 1,
            event_driven_scheduler_enable: false,
            ..HealConfig::default()
        }),
    ));
    let mut retry_request = HealRequest::object("retry-transition".to_string(), "object".to_string(), None);
    retry_request.priority = HealPriority::High;
    let retry_id = retry_request.id.clone();
    manager
        .submit_heal_request(retry_request)
        .await
        .expect("retry request should queue");

    // Process exactly one queue cycle so the retry task is spawned without a
    // background scheduler consuming the filler request before the retry wakes.
    process_manager_queue_once(&manager).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if manager.retrying_heals.lock().await.contains_key(&retry_id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retry request should enter backoff");

    let filler = HealRequest::new(
        HealType::Bucket {
            bucket: "retry-displaced-filler".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    let filler_id = filler.id.clone();
    manager
        .submit_heal_request(filler)
        .await
        .expect("filler request should occupy the queue");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                manager.get_task_status(&filler_id).await,
                Ok(HealTaskStatus::Failed { ref error }) if error.contains("reason=displaced")
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retry admission should displace the filler request");
    assert_eq!(manager.get_queue_length().await, 1);
    assert_eq!(
        manager.get_task_status(&retry_id).await.expect("retry should be queued"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn concurrent_displacers_produce_one_terminal_generation() {
    let manager = Arc::new(HealManager::new(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    ));
    let mut displaced = HealRequest::new(
        HealType::Bucket {
            bucket: "concurrent-displaced".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    displaced.id = "concurrent-displaced-task".to_string();
    let displaced_id = displaced.id.clone();
    manager
        .submit_heal_request(displaced)
        .await
        .expect("initial request should queue");

    let first = HealRequest::new(
        HealType::Bucket {
            bucket: "concurrent-successor-a".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    let second = HealRequest::new(
        HealType::Bucket {
            bucket: "concurrent-successor-b".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    let (first_result, second_result) = tokio::join!(manager.submit_heal_request(first), manager.submit_heal_request(second));
    let accepted = [&first_result, &second_result]
        .into_iter()
        .filter(|result| matches!(result, Ok(HealAdmissionResult::Accepted)))
        .count();
    assert_eq!(accepted, 1, "exactly one concurrent displacer should win the full queue");
    assert!(
        first_result.is_ok() && second_result.is_ok(),
        "the losing request should receive a typed Full result"
    );
    let terminals = lock_displaced_terminals(&manager.displaced_terminals);
    assert_eq!(terminals.len(), 1);
    assert!(terminals.contains_key(&displaced_id));
}

#[tokio::test]
async fn successor_chain_is_bounded_and_authorized() {
    let manager = HealManager::new(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );
    let mut original = HealRequest::new(
        HealType::Bucket {
            bucket: "authorized-original".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    original.id = "authorized-original-task".to_string();
    let original_id = original.id.clone();
    manager.submit_heal_request(original).await.expect("original should queue");
    let mut duplicate = HealRequest::new(
        HealType::Bucket {
            bucket: "authorized-original".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    duplicate.id = "authorized-duplicate-task".to_string();
    let duplicate_id = duplicate.id.clone();
    manager
        .submit_heal_request(duplicate)
        .await
        .expect("same-target duplicate should merge");
    let successor = HealRequest::new(
        HealType::Bucket {
            bucket: "authorized-successor".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );
    let successor_id = successor.id.clone();
    manager.submit_heal_request(successor).await.expect("successor should queue");
    assert!(manager.task_aliases.lock().await.is_empty());
    assert!(matches!(manager.get_task_status(&original_id).await, Ok(HealTaskStatus::Failed { .. })));
    assert!(matches!(manager.get_task_status(&duplicate_id).await, Ok(HealTaskStatus::Failed { .. })));
    assert_eq!(
        manager
            .get_task_status(&successor_id)
            .await
            .expect("successor should remain queued"),
        HealTaskStatus::Pending
    );
}

#[tokio::test]
async fn displaced_terminal_expires_after_bounded_ttl() {
    let manager = HealManager::new(Arc::new(MockStorage), None);
    let mut request = HealRequest::new(
        HealType::Bucket {
            bucket: "expires".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    request.id = "expires-task".to_string();
    let request_id = request.id.clone();
    record_displaced_terminal(&manager.displaced_terminals, &request);
    {
        let mut terminals = lock_displaced_terminals(&manager.displaced_terminals);
        let entry =
            Arc::get_mut(terminals.get_mut(&request_id).expect("terminal should be retained")).expect("test owns terminal entry");
        entry.completed_at = SystemTime::now() - KEEP_HEAL_TASK_STATUS_DURATION - Duration::from_secs(1);
    }
    assert!(matches!(manager.get_task_status(&request_id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn test_displacing_registered_mrf_task_drops_notice_ownership() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );

    let mut low = HealRequest::object("bucket".to_string(), "object".to_string(), None);
    low.source = HealRequestSource::Mrf;
    low.priority = HealPriority::Low;
    let high = HealRequest::new(
        HealType::Bucket {
            bucket: "manual-bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::High,
    );

    assert_eq!(
        manager
            .submit_mrf_heal_request_with_receipt(low, Arc::from("bucket"), Arc::from("object"), None)
            .await
            .expect("low priority MRF request should be accepted first")
            .result,
        HealAdmissionResult::Accepted
    );

    assert_eq!(
        manager
            .submit_heal_request(high)
            .await
            .expect("high priority request should displace low priority work"),
        HealAdmissionResult::Accepted
    );
    assert!(
        manager
            .mrf_repair_notice_targets
            .lock()
            .expect("mrf repair notice registry poisoned")
            .is_empty(),
        "displaced MRF-owned task cannot reach completion, so its notice ownership must be dropped"
    );
}

#[tokio::test]
async fn test_submit_heal_request_drops_read_repair_under_pressure() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 10,
            ..HealConfig::default()
        }),
    );

    for index in 0..8 {
        assert_eq!(
            manager
                .submit_heal_request(bucket_request(
                    &format!("queued-{index}"),
                    HealPriority::Normal,
                    HealRequestSource::Internal,
                ))
                .await
                .expect("seed request should be accepted"),
            HealAdmissionResult::Accepted
        );
    }

    let admission = manager
        .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
        .await
        .expect("read repair admission should return a result");

    assert_eq!(admission, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
    assert_eq!(manager.get_queue_length().await, 8);
}

#[tokio::test]
async fn test_submit_heal_request_drops_low_scanner_under_pressure() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 10,
            ..HealConfig::default()
        }),
    );

    for index in 0..8 {
        assert_eq!(
            manager
                .submit_heal_request(bucket_request(
                    &format!("queued-{index}"),
                    HealPriority::Normal,
                    HealRequestSource::Internal,
                ))
                .await
                .expect("seed request should be accepted"),
            HealAdmissionResult::Accepted
        );
    }

    let admission = manager
        .submit_heal_request(bucket_request("scanner", HealPriority::Low, HealRequestSource::Scanner))
        .await
        .expect("scanner admission should return a result");

    assert_eq!(admission, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
    assert_eq!(manager.get_queue_length().await, 8);
}

#[tokio::test]
async fn test_submit_heal_request_accepts_admin_high_under_pressure() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 10,
            ..HealConfig::default()
        }),
    );

    for index in 0..8 {
        assert_eq!(
            manager
                .submit_heal_request(bucket_request(
                    &format!("queued-{index}"),
                    HealPriority::Normal,
                    HealRequestSource::Internal,
                ))
                .await
                .expect("seed request should be accepted"),
            HealAdmissionResult::Accepted
        );
    }

    let admission = manager
        .submit_heal_request(bucket_request("admin", HealPriority::High, HealRequestSource::Admin))
        .await
        .expect("admin admission should return a result");

    assert_eq!(admission, HealAdmissionResult::Accepted);
    assert_eq!(manager.get_queue_length().await, 9);
}

#[tokio::test]
async fn test_mainline_throttle_delays_background_heal_start() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
        class: WorkloadClass::ForegroundRead,
        active: 8,
        limit: 10,
        state: AdmissionState::Open,
    });
    let manager = HealManager::new_with_workload_provider(
        storage,
        Some(HealConfig {
            max_concurrent_heals: 1,
            mainline_throttle_enable: true,
            mainline_read_utilization_high_percent: 80,
            mainline_write_utilization_high_percent: 80,
            mainline_max_sleep: Duration::from_millis(1),
            ..HealConfig::default()
        }),
        Some(provider),
    );

    manager
        .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
        .await
        .expect("read repair request should be queued");

    process_manager_queue_once(&manager).await;

    assert_eq!(manager.get_queue_length().await, 1);
    assert_eq!(manager.get_active_task_count().await, 0);
}

#[tokio::test]
async fn test_mainline_throttle_delays_background_heal_start_under_write_pressure() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
        class: WorkloadClass::ForegroundWrite,
        active: 9,
        limit: 10,
        state: AdmissionState::Open,
    });
    let manager = HealManager::new_with_workload_provider(
        storage,
        Some(HealConfig {
            max_concurrent_heals: 1,
            mainline_throttle_enable: true,
            mainline_read_utilization_high_percent: 80,
            mainline_write_utilization_high_percent: 80,
            mainline_max_sleep: Duration::from_millis(1),
            ..HealConfig::default()
        }),
        Some(provider),
    );

    manager
        .submit_heal_request(bucket_request("read-repair", HealPriority::Normal, HealRequestSource::ReadRepair))
        .await
        .expect("read repair request should be queued");

    process_manager_queue_once(&manager).await;

    assert_eq!(manager.get_queue_length().await, 1);
    assert_eq!(manager.get_active_task_count().await, 0);
}

#[tokio::test]
async fn test_mainline_throttle_allows_admin_high_start() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
        class: WorkloadClass::ForegroundRead,
        active: 10,
        limit: 10,
        state: AdmissionState::Saturated,
    });
    let manager = HealManager::new_with_workload_provider(
        storage,
        Some(HealConfig {
            max_concurrent_heals: 1,
            mainline_throttle_enable: true,
            mainline_read_utilization_high_percent: 80,
            mainline_write_utilization_high_percent: 80,
            mainline_max_sleep: Duration::from_millis(1),
            ..HealConfig::default()
        }),
        Some(provider),
    );

    manager
        .submit_heal_request(bucket_request("admin", HealPriority::High, HealRequestSource::Admin))
        .await
        .expect("admin request should be queued");

    process_manager_queue_once(&manager).await;

    assert_eq!(manager.get_queue_length().await, 0);
}

#[tokio::test]
async fn configured_task_timeout_applies_only_when_request_timeout_is_absent() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            max_concurrent_heals: 1,
            task_timeout: Duration::ZERO,
            ..HealConfig::default()
        }),
    );

    let mut defaulted = bucket_request("defaulted-timeout", HealPriority::Normal, HealRequestSource::Admin);
    defaulted.options.timeout = None;
    let defaulted_id = defaulted.id.clone();
    manager
        .submit_heal_request(defaulted)
        .await
        .expect("request without timeout should be queued");
    process_manager_queue_once(&manager).await;
    let defaulted_status = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(status @ HealTaskStatus::Timeout) = manager.get_task_status(&defaulted_id).await {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("configured timeout should finish the task");
    assert_eq!(defaulted_status, HealTaskStatus::Timeout);
    assert!(manager.retrying_heals.lock().await.get(&defaulted_id).is_none());

    let mut explicit = bucket_request("explicit-timeout", HealPriority::Normal, HealRequestSource::Admin);
    explicit.options.timeout = Some(Duration::from_secs(60));
    let explicit_id = explicit.id.clone();
    manager
        .submit_heal_request(explicit)
        .await
        .expect("request with explicit timeout should be queued");
    process_manager_queue_once(&manager).await;
    let explicit_status = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(status @ HealTaskStatus::Failed { .. }) = manager.get_task_status(&explicit_id).await {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit timeout request should finish without using the zero default");
    assert!(matches!(explicit_status, HealTaskStatus::Failed { .. }));
}

#[tokio::test]
async fn test_force_start_bypasses_duplicate_and_full_admission() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            low_priority_drop_when_full: true,
            ..HealConfig::default()
        }),
    );

    let normal = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    let mut forced_duplicate = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    forced_duplicate.force_start = true;

    let subsequent_duplicate = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    assert_eq!(
        manager
            .submit_heal_request(normal)
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(forced_duplicate)
            .await
            .expect("force start should bypass duplicate/full policy"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(subsequent_duplicate)
            .await
            .expect("subsequent non-force duplicate should be merged"),
        HealAdmissionResult::Merged
    );
}

#[tokio::test]
async fn test_force_start_marks_dedup_key_for_future_duplicates() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let manager = HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            ..HealConfig::default()
        }),
    );

    let normal = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    let mut forced = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    forced.force_start = true;
    let duplicate = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );

    assert_eq!(
        manager
            .submit_heal_request(normal)
            .await
            .expect("first request should be accepted"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(forced)
            .await
            .expect("forced request should bypass duplicate/full admission"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .submit_heal_request(duplicate)
            .await
            .expect("non-forced duplicate should merge while forced request is queued"),
        HealAdmissionResult::Merged
    );
}

#[test]
fn test_running_heal_set_counts_groups_set_scoped_tasks() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
    let erasure_task = Arc::new(HealTask::from_request(
        HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        ),
        storage.clone(),
    ));
    let scoped_options = HealOptions {
        pool_index: Some(0),
        set_index: Some(1),
        ..Default::default()
    };
    let scoped_object_task = Arc::new(HealTask::from_request(
        HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "scoped-object".to_string(),
                version_id: None,
            },
            scoped_options,
            HealPriority::Normal,
        ),
        storage.clone(),
    ));
    let object_task = Arc::new(HealTask::from_request(
        HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Normal,
        ),
        storage,
    ));

    let mut active = HashMap::new();
    active.insert(erasure_task.id.clone(), erasure_task);
    active.insert(scoped_object_task.id.clone(), scoped_object_task);
    active.insert(object_task.id.clone(), object_task);

    let counts = running_heal_set_counts(&active);
    assert_eq!(counts.get("pool_0_set_1"), Some(&2));
    assert_eq!(counts.len(), 1);
}

#[test]
fn test_heal_config_respects_feature_flags() {
    temp_env::with_vars(
        [
            (rustfs_config::ENV_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE, Some("false")),
            (rustfs_config::ENV_HEAL_SET_BULKHEAD_ENABLE, Some("false")),
            (rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("false")),
        ],
        || {
            let config = HealConfig::default();
            assert!(!config.event_driven_scheduler_enable);
            assert!(!config.set_bulkhead_enable);
            assert!(!config.page_parallel_enable);
        },
    );
}
