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

use super::super::{DiskOption, DiskStore, Endpoint, new_disk};
use super::*;
use crate::heal::storage::{HealListItem, HealObjectInfo};
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, TraceSubscription, TraceVal, subscribe_trace_events};
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem, Infos};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tempfile::TempDir;

use super::super::storage_api::status::BucketInfo;
use crate::heal::progress::{HealProgressState, aggregate_heal_progress};

#[tokio::test]
async fn retry_request_carries_remaining_timeout_budget() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage::default());
    let mut request = HealRequest::bucket("bucket".to_string());
    request.options.timeout = Some(Duration::from_secs(100));
    let task = HealTask::from_request(request, storage.clone());
    *task.task_start_instant.write().await = Some(Instant::now() - Duration::from_secs(40));

    let retry = task
        .retry_request_with_remaining_timeout()
        .await
        .expect("first retry should retain the unused timeout budget");
    let first_remaining = retry.options.timeout.expect("configured timeout should remain present");
    assert!(first_remaining <= Duration::from_secs(60));
    assert!(first_remaining > Duration::from_secs(59));

    let retry_task = HealTask::from_request(retry, storage);
    *retry_task.task_start_instant.write().await = Some(Instant::now() - Duration::from_secs(20));
    let second_retry = retry_task
        .retry_request_with_remaining_timeout()
        .await
        .expect("second retry should retain only the unused aggregate budget");
    let second_remaining = second_retry
        .options
        .timeout
        .expect("configured timeout should remain present");
    assert!(second_remaining <= Duration::from_secs(40));
    assert!(second_remaining > Duration::from_secs(39));
}

#[test]
fn format_result_requires_every_requested_target_to_be_ok() {
    let result = HealResultItem {
        after: Infos {
            drives: vec![
                HealDriveInfo {
                    endpoint: "disk-a".to_string(),
                    state: "ok".to_string(),
                    ..Default::default()
                },
                HealDriveInfo {
                    endpoint: "disk-b".to_string(),
                    state: "missing".to_string(),
                    ..Default::default()
                },
            ],
        },
        ..Default::default()
    };

    assert!(target_outcomes_complete(&result, &["disk-a".to_string()]));
    assert!(!target_outcomes_complete(&result, &["disk-a".to_string(), "disk-b".to_string()]));
    assert!(!target_outcomes_complete(&result, &["disk-c".to_string()]));
}

#[tokio::test]
async fn automatic_replacement_uses_target_scoped_format() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let disk = make_resume_disk(&temp).await;
    let storage = Arc::new(MockStorage {
        replacement_targets_ready: Mutex::new(true),
        resume_disk: Mutex::new(Some(disk)),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            pool_index: Some(0),
            set_index: Some(0),
            ..Default::default()
        },
        HealPriority::Low,
    );
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec!["replacement-a".to_string()];
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect_err("the mock has no local replacement marker target");

    assert_eq!(
        *storage.global_format_calls.lock().unwrap(),
        0,
        "automatic replacement must not call global format"
    );
    assert_eq!(
        storage.replacement_format_calls.lock().unwrap().as_slice(),
        &[(0, 0, vec!["replacement-a".to_string()])],
        "automatic replacement must pass the exact pool, set, and target"
    );
}

#[tokio::test]
async fn automatic_replacement_persists_intent_before_format() {
    let storage = Arc::new(MockStorage {
        replacement_targets_ready: Mutex::new(true),
        ..Default::default()
    });
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
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec!["replacement-a".to_string()];

    HealTask::from_request(request, storage.clone())
        .execute()
        .await
        .expect_err("intent persistence needs a healthy non-target disk");

    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "format must not start before the durable replacement intent exists"
    );
}

#[tokio::test]
async fn recovered_replacement_never_uses_a_fresh_resume_disk() {
    let storage = Arc::new(MockStorage {
        replacement_targets_ready: Mutex::new(true),
        ..Default::default()
    });
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
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec!["replacement-a".to_string()];

    let error = HealTask::from_replacement_recovery_request(request, storage.clone(), Some("survivor-a".to_string()))
        .execute()
        .await
        .expect_err("a durable recovery must not fall back to another resume disk");

    assert!(error.to_string().contains("resume anchor is unavailable"));
    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "an unavailable durable anchor must block formatting before any write"
    );
    assert!(!*storage.listed.lock().unwrap(), "an unavailable durable anchor must not list buckets");
}

#[tokio::test]
async fn automatic_replacement_rejects_a_new_identity_after_format() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let disk = make_resume_disk(&temp).await;
    let first_identity = replacement_identity("replacement-a", "device-a", "filesystem-a");
    let second_identity = replacement_identity("replacement-a", "device-b", "filesystem-b");
    let storage = Arc::new(MockStorage {
        replacement_targets_ready: Mutex::new(true),
        replacement_target_identity_sequences: Mutex::new(VecDeque::from([
            vec![first_identity.clone()],
            vec![first_identity.clone()],
            vec![second_identity],
        ])),
        resume_disk: Mutex::new(Some(disk.clone())),
        ..Default::default()
    });
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
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec!["replacement-a".to_string()];
    let task = HealTask::from_request(request, storage.clone());

    let error = task
        .execute()
        .await
        .expect_err("a remounted target after format must fail closed");

    assert!(error.to_string().contains("changed after format"));
    assert_eq!(storage.replacement_format_calls.lock().unwrap().len(), 1);
    assert!(storage.bucket_heal_calls.lock().unwrap().is_empty());
    assert!(storage.heal_object_calls.lock().unwrap().is_empty());

    let state = ResumeManager::load_replacement_intent(disk, &task.id)
        .await
        .expect("durable replacement intent should remain available")
        .get_state()
        .await;
    assert_eq!(state.replacement_phase, crate::heal::resume::ReplacementPhase::Intent);
    assert_eq!(state.replacement_target_identities, vec![first_identity]);
}

#[tokio::test]
async fn automatic_replacement_reuses_an_existing_non_target_resume_anchor() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let anchor = make_resume_disk(&temp).await;
    let task_id = crate::heal::resume::ResumeUtils::generate_task_id();
    let identity = ReplacementTargetIdentity {
        endpoint: "replacement-a".to_string(),
        canonical_path: "/replacement/replacement-a".to_string(),
        physical_device_ids: vec!["replacement-a".to_string()],
        filesystem_identity: "identity-replacement-a".to_string(),
    };
    ResumeManager::new_replacement_intent(
        anchor.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec!["replacement-a".to_string()],
        vec![identity],
    )
    .await
    .expect("existing intent should be stored on the non-target anchor");
    let storage = Arc::new(MockStorage {
        replacement_targets_ready: Mutex::new(true),
        replacement_resume_disk: Mutex::new(Some(anchor.clone())),
        ..Default::default()
    });
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
    request.heal_endpoints = vec!["replacement-a".to_string()];

    HealTask::from_request(request, storage.clone())
        .execute()
        .await
        .expect_err("the test has no mounted marker target after format");

    assert_eq!(
        storage.replacement_format_calls.lock().unwrap().len(),
        1,
        "an existing non-target anchor must be reused instead of falling back to a fresh anchor"
    );
    assert!(
        storage.resume_disk.lock().unwrap().is_none(),
        "the fresh resume-anchor fallback must remain unused"
    );
    let state = ResumeManager::load_replacement_intent(anchor, &task_id)
        .await
        .expect("the existing non-target anchor should retain the generation")
        .get_state()
        .await;
    assert_eq!(state.replacement_phase, ReplacementPhase::Rebuilding);
}

#[tokio::test]
async fn automatic_replacement_defers_before_bucket_listing_when_target_is_unready() {
    let storage = Arc::new(MockStorage::default());
    let mut request = HealRequest::new(
        HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            pool_index: Some(0),
            set_index: Some(0),
            ..Default::default()
        },
        HealPriority::Low,
    );
    request.source = HealRequestSource::AutoHeal;
    request.heal_endpoints = vec!["replacement-a".to_string()];

    HealTask::from_request(request, storage.clone())
        .execute()
        .await
        .expect_err("an unsafe replacement must defer before any scan work");

    assert!(!*storage.listed.lock().unwrap(), "unsafe targets must not list buckets");
    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "unsafe targets must not format"
    );
}

#[tokio::test]
async fn cleanup_pending_recovery_skips_target_readiness_and_format() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let anchor = make_resume_disk(&temp).await;
    let task_id = crate::heal::resume::ResumeUtils::generate_task_id();
    let identity = replacement_identity("replacement-a", "device-a", "filesystem-a");
    let resume_manager = ResumeManager::new_replacement_intent(
        anchor.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec!["replacement-a".to_string()],
        vec![identity],
    )
    .await
    .expect("terminal replacement state should persist on the survivor anchor");
    resume_manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("terminal replacement proof should persist before cleanup");
    resume_manager
        .mark_replacement_cleanup_pending()
        .await
        .expect("failed cleanup must retain a cleanup-pending state");

    let storage = Arc::new(MockStorage {
        replacement_resume_disk: Mutex::new(Some(anchor.clone())),
        ..Default::default()
    });
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
    request.heal_endpoints = vec!["replacement-a".to_string()];

    HealTask::from_replacement_recovery_request(request, storage.clone(), Some(anchor.endpoint().to_string()))
        .execute()
        .await
        .expect("cleanup-pending recovery must not require a mounted replacement target");

    assert!(
        !ResumeManager::has_resume_state(&anchor, &task_id).await,
        "terminal cleanup must remove the retained resume state"
    );
    assert_eq!(*storage.global_format_calls.lock().unwrap(), 0);
    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "terminal cleanup must not format replacement targets"
    );
    assert!(storage.bucket_heal_calls.lock().unwrap().is_empty());
    assert!(!*storage.listed.lock().unwrap());
}

#[tokio::test]
async fn cleanup_pending_recovery_removes_checkpoint_without_rebuild_work() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let anchor = make_resume_disk(&temp).await;
    let task_id = crate::heal::resume::ResumeUtils::generate_task_id();
    let identity = replacement_identity("replacement-a", "device-a", "filesystem-a");
    let resume_manager = ResumeManager::new_replacement_intent(
        anchor.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec!["replacement-a".to_string()],
        vec![identity],
    )
    .await
    .expect("terminal replacement state should persist on the survivor anchor");
    resume_manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("terminal replacement proof should persist before cleanup");
    resume_manager
        .mark_replacement_cleanup_pending()
        .await
        .expect("failed cleanup must retain a cleanup-pending state");
    CheckpointManager::new(anchor.clone(), task_id.clone())
        .await
        .expect("checkpoint fixture should persist");
    assert!(
        CheckpointManager::has_checkpoint(&anchor, &task_id).await,
        "checkpoint fixture must exist before restart cleanup"
    );

    let storage = Arc::new(MockStorage {
        replacement_resume_disk: Mutex::new(Some(anchor.clone())),
        ..Default::default()
    });
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
    request.heal_endpoints = vec!["replacement-a".to_string()];

    HealTask::from_replacement_recovery_request(request, storage.clone(), Some(anchor.endpoint().to_string()))
        .execute()
        .await
        .expect("cleanup-pending recovery must finish terminal cleanup");

    assert!(
        !CheckpointManager::has_checkpoint(&anchor, &task_id).await,
        "terminal cleanup must remove the retained checkpoint"
    );
    assert!(
        !ResumeManager::has_resume_state(&anchor, &task_id).await,
        "terminal cleanup must remove the retained resume state"
    );
    assert_eq!(*storage.global_format_calls.lock().unwrap(), 0);
    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "terminal checkpoint cleanup must not format replacement targets"
    );
    assert!(storage.bucket_heal_calls.lock().unwrap().is_empty());
    assert!(storage.heal_object_calls.lock().unwrap().is_empty());
    assert!(!*storage.listed.lock().unwrap());
}

#[tokio::test]
async fn verified_recovery_keeps_state_when_marker_clear_fails() {
    let temp = TempDir::new().expect("temporary resume disk directory should be created");
    let anchor = make_resume_disk(&temp).await;
    let task_id = crate::heal::resume::ResumeUtils::generate_task_id();
    let target = format!("replacement-marker-missing-{task_id}");
    let identity = replacement_identity(&target, &target, &format!("identity-{target}"));
    let resume_manager = ResumeManager::new_replacement_intent(
        anchor.clone(),
        task_id.clone(),
        "pool_0_set_0".to_string(),
        vec!["bucket-a".to_string()],
        vec![target.clone()],
        vec![identity],
    )
    .await
    .expect("verified replacement state should persist on the survivor anchor");
    resume_manager
        .mark_replacement_completed_and_verified()
        .await
        .expect("verified state must persist proof before marker cleanup");

    let storage = Arc::new(MockStorage {
        replacement_resume_disk: Mutex::new(Some(anchor.clone())),
        replacement_targets_ready: Mutex::new(true),
        ..Default::default()
    });
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

    let error = HealTask::from_replacement_recovery_request(request, storage.clone(), Some(anchor.endpoint().to_string()))
        .execute()
        .await
        .expect_err("marker clear failure must keep the durable terminal state retryable");

    assert!(error.to_string().contains("healing marker target is unavailable"));
    let state = ResumeManager::load_replacement_intent(anchor.clone(), &task_id)
        .await
        .expect("verified state must remain for retry after marker clear failure")
        .get_state()
        .await;
    assert!(state.completed);
    assert_eq!(state.replacement_phase, ReplacementPhase::Verified);
    assert_eq!(*storage.global_format_calls.lock().unwrap(), 0);
    assert!(
        storage.replacement_format_calls.lock().unwrap().is_empty(),
        "marker cleanup retry must not format replacement targets again"
    );
    assert!(storage.bucket_heal_calls.lock().unwrap().is_empty());
    assert!(storage.heal_object_calls.lock().unwrap().is_empty());
    assert!(!*storage.listed.lock().unwrap());
}

#[derive(Default)]
struct MockStorage {
    listed: Mutex<bool>,
    healed_objects: Mutex<Vec<String>>,
    heal_object_calls: Mutex<Vec<String>>,
    heal_object_version_ids: Mutex<Vec<Option<String>>>,
    bucket_heal_opts: Mutex<Vec<HealOpts>>,
    object_heal_opts: Mutex<Vec<HealOpts>>,
    object_exists: Mutex<Option<bool>>,
    object_exists_by_name: Mutex<HashMap<String, MockObjectExists>>,
    heal_object_outcome: Mutex<Option<MockHealObjectOutcome>>,
    heal_object_outcomes: Mutex<HashMap<String, VecDeque<MockHealObjectOutcome>>>,
    format_no_heal_required: Mutex<bool>,
    format_error: Mutex<Option<Error>>,
    global_format_calls: Mutex<u32>,
    replacement_format_calls: Mutex<Vec<(usize, usize, Vec<String>)>>,
    replacement_targets_ready: Mutex<bool>,
    replacement_target_identity_sequences: Mutex<VecDeque<Vec<crate::heal::resume::ReplacementTargetIdentity>>>,
    listed_prefixes: Mutex<Vec<String>>,
    truncate_without_token: Mutex<bool>,
    include_object_dir_candidate: Mutex<bool>,
    listed_buckets: Mutex<Option<Vec<String>>>,
    bucket_heal_errors: Mutex<HashMap<String, VecDeque<&'static str>>>,
    bucket_heal_calls: Mutex<Vec<String>>,
    block_heal_object: Mutex<bool>,
    resume_disk: Mutex<Option<DiskStore>>,
    replacement_resume_disk: Mutex<Option<DiskStore>>,
    usage_baseline: Mutex<Option<HealBucketUsageBaseline>>,
    usage_baseline_error: Mutex<bool>,
}

#[test]
fn per_object_heal_types_are_classified_for_log_demotion() {
    assert!(
        HealType::Object {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
        }
        .is_per_object()
    );
    assert!(
        HealType::Metadata {
            bucket: "b".to_string(),
            object: "o".to_string(),
        }
        .is_per_object()
    );
    assert!(
        HealType::ECDecode {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
        }
        .is_per_object()
    );
    assert!(!HealType::Cluster.is_per_object());
    assert!(!HealType::Bucket { bucket: "b".to_string() }.is_per_object());
    assert!(
        !HealType::Prefix {
            bucket: "b".to_string(),
            prefix: "p".to_string(),
        }
        .is_per_object()
    );
    assert!(
        !HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "s".to_string(),
        }
        .is_per_object()
    );
}

#[test]
fn failure_log_sampling_caps_at_max_samples() {
    let mut samples_logged = 0_u64;
    for _ in 0..MAX_BUCKET_FAILURE_LOG_SAMPLES {
        assert!(take_failure_log_sample(&mut samples_logged));
    }
    assert!(!take_failure_log_sample(&mut samples_logged));
    assert!(!take_failure_log_sample(&mut samples_logged));
    assert_eq!(samples_logged, MAX_BUCKET_FAILURE_LOG_SAMPLES);
}

#[tokio::test]
async fn execute_emits_heal_trace_task_state() {
    let mut trace = subscribe_trace_events();
    let storage = Arc::new(MockStorage::default());
    let task = HealTask::from_request(
        HealRequest::object("bucket-a".to_string(), "object-a".to_string(), Some("version-a".to_string())),
        storage,
    );

    task.execute().await.expect("mock object heal should complete");

    let started = recv_trace_task_state(&mut trace, &task.id, "started").await;
    assert_eq!(started.kind, TraceKind::Heal);
    assert_eq!(started.func, TraceFunc::HealTask);
    assert_eq!(started.bucket.as_deref(), Some("bucket-a"));
    assert_eq!(started.object.as_deref(), Some("object-a"));
    assert_eq!(trace_attr_string(&started, "heal_type").as_deref(), Some("object"));
    assert_eq!(trace_attr_string(&started, "source").as_deref(), Some("internal"));
    assert_eq!(trace_attr_string(&started, "version_id").as_deref(), Some("version-a"));

    let completed = recv_trace_task_state(&mut trace, &task.id, "completed").await;
    assert_eq!(completed.kind, TraceKind::Heal);
    assert_eq!(completed.func, TraceFunc::HealTask);
    assert_eq!(trace_attr_string(&completed, "state").as_deref(), Some("completed"));
}

async fn recv_trace_task_state(trace: &mut TraceSubscription, task_id: &str, state: &str) -> TraceEvent {
    for _ in 0..32 {
        let event = tokio::time::timeout(Duration::from_secs(1), trace.recv())
            .await
            .expect("trace event should arrive")
            .expect("trace bus should stay open");
        if trace_attr_string(&event, "task_id").as_deref() == Some(task_id)
            && trace_attr_string(&event, "state").as_deref() == Some(state)
        {
            return (*event).clone();
        }
    }

    panic!("expected trace state {state} for task {task_id}");
}

fn trace_attr_string(event: &TraceEvent, key: &str) -> Option<String> {
    event.attrs.iter().find_map(|attr| {
        if attr.key != key {
            return None;
        }
        Some(match &attr.value {
            TraceVal::Bool(value) => value.to_string(),
            TraceVal::U64(value) => value.to_string(),
            TraceVal::I64(value) => value.to_string(),
            TraceVal::Str(value) => value.to_string(),
        })
    })
}

/// Build a latest, non-delete-marker heal list item with no version id.
fn heal_item(name: &str) -> HealListItem {
    HealListItem {
        name: name.to_string(),
        version_id: None,
        mod_time_unix_nanos: None,
        lifecycle_object_info: None,
        is_delete_marker: false,
    }
}

fn replacement_identity(
    endpoint: &str,
    physical_device_id: &str,
    filesystem_identity: &str,
) -> crate::heal::resume::ReplacementTargetIdentity {
    crate::heal::resume::ReplacementTargetIdentity {
        endpoint: endpoint.to_string(),
        canonical_path: format!("/replacement/{endpoint}"),
        physical_device_ids: vec![physical_device_id.to_string()],
        filesystem_identity: filesystem_identity.to_string(),
    }
}

enum MockHealObjectOutcome {
    OkWithOtherError(&'static str),
    ErrOther(&'static str),
    RetryableReadQuorum,
    RetryableSlowDown,
    PermanentOther(&'static str),
}

#[derive(Clone, Copy)]
enum MockObjectExists {
    Exists(bool),
    TransientSkip(&'static str),
    OtherError(&'static str),
}

#[test]
fn test_missing_object_dir_heal_result_matches_only_object_level_not_found() {
    assert!(is_missing_object_dir_heal_result("x.rnd/", &Error::Disk(DiskError::FileNotFound)));
    assert!(is_missing_object_dir_heal_result("x.rnd/", &Error::Disk(DiskError::FileVersionNotFound)));
    assert!(is_missing_object_dir_heal_result("x.rnd/", &Error::Storage(EcstoreError::FileNotFound)));
    assert!(is_missing_object_dir_heal_result(
        "x.rnd/",
        &Error::Other("File version not found".to_string())
    ));
    assert!(!is_missing_object_dir_heal_result("x.rnd/", &Error::Other("Disk not found".to_string())));
    assert!(!is_missing_object_dir_heal_result("x.rnd", &Error::Disk(DiskError::FileNotFound)));
}

#[async_trait::async_trait]
impl HealStorageAPI for MockStorage {
    async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<HealObjectInfo>> {
        Ok(None)
    }

    async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        Ok(Some(BucketInfo {
            name: bucket.to_string(),
            ..Default::default()
        }))
    }

    async fn erasure_set_usage_baseline(&self, _buckets: &[String]) -> Result<Option<HealBucketUsageBaseline>> {
        if *self.usage_baseline_error.lock().unwrap() {
            return Err(Error::Other("usage baseline unavailable".to_string()));
        }
        Ok(*self.usage_baseline.lock().unwrap())
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let buckets = self
            .listed_buckets
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| vec!["bucket-a".to_string()]);
        Ok(buckets
            .into_iter()
            .map(|name| BucketInfo {
                name,
                ..Default::default()
            })
            .collect())
    }

    async fn object_exists(&self, _bucket: &str, object: &str) -> Result<bool> {
        if let Some(result) = self.object_exists_by_name.lock().unwrap().get(object).copied() {
            return match result {
                MockObjectExists::Exists(exists) => Ok(exists),
                MockObjectExists::TransientSkip(message) => Err(Error::transient_skip(message)),
                MockObjectExists::OtherError(message) => Err(Error::other(message)),
            };
        }
        Ok(self.object_exists.lock().unwrap().unwrap_or(true))
    }

    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_object_calls.lock().unwrap().push(object.to_string());
        self.heal_object_version_ids
            .lock()
            .unwrap()
            .push(version_id.map(ToString::to_string));
        self.object_heal_opts.lock().unwrap().push(*opts);
        let block_heal_object = *self.block_heal_object.lock().unwrap();
        if block_heal_object {
            std::future::pending::<()>().await;
        }
        if let Some(outcome) = self
            .heal_object_outcomes
            .lock()
            .unwrap()
            .get_mut(object)
            .and_then(VecDeque::pop_front)
        {
            return match outcome {
                MockHealObjectOutcome::RetryableReadQuorum => Err(Error::Storage(EcstoreError::InsufficientReadQuorum(
                    bucket.to_string(),
                    object.to_string(),
                ))),
                MockHealObjectOutcome::RetryableSlowDown => {
                    Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::SlowDown))))
                }
                MockHealObjectOutcome::PermanentOther(message) => Err(Error::other(message)),
                MockHealObjectOutcome::OkWithOtherError(message) => Ok((HealResultItem::default(), Some(Error::other(message)))),
                MockHealObjectOutcome::ErrOther(message) => Err(Error::other(message)),
            };
        }
        if let Some(outcome) = self.heal_object_outcome.lock().unwrap().take() {
            return match outcome {
                MockHealObjectOutcome::OkWithOtherError(message) => Ok((HealResultItem::default(), Some(Error::other(message)))),
                MockHealObjectOutcome::ErrOther(message) | MockHealObjectOutcome::PermanentOther(message) => {
                    Err(Error::other(message))
                }
                MockHealObjectOutcome::RetryableReadQuorum => Err(Error::Storage(EcstoreError::InsufficientReadQuorum(
                    bucket.to_string(),
                    object.to_string(),
                ))),
                MockHealObjectOutcome::RetryableSlowDown => {
                    Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::SlowDown))))
                }
            };
        }
        if bucket == RUSTFS_META_BUCKET && object == format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}") {
            return Ok((
                HealResultItem::default(),
                Some(Error::other(
                    "Lock error: Lock acquisition timeout for resource '.rustfs.sys/buckets/.usage-cache.bin@latest' after 5s",
                )),
            ));
        }
        if object == "object-dir/" {
            return Ok((HealResultItem::default(), Some(Error::Disk(DiskError::FileNotFound))));
        }
        self.healed_objects.lock().unwrap().push(object.to_string());
        Ok((
            HealResultItem {
                object_size: 1,
                ..Default::default()
            },
            None,
        ))
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.bucket_heal_calls.lock().unwrap().push(bucket.to_string());
        self.bucket_heal_opts.lock().unwrap().push(*opts);
        if let Some(message) = self
            .bucket_heal_errors
            .lock()
            .unwrap()
            .get_mut(bucket)
            .and_then(VecDeque::pop_front)
        {
            return Err(Error::other(message));
        }
        Ok(HealResultItem::default())
    }

    async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        *self.global_format_calls.lock().unwrap() += 1;
        if let Some(error) = self.format_error.lock().unwrap().take() {
            return Err(error);
        }
        let no_heal_required = *self.format_no_heal_required.lock().unwrap();
        if no_heal_required {
            Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::NoHealRequired))))
        } else {
            Ok((HealResultItem::default(), None))
        }
    }

    async fn heal_replacement_format(
        &self,
        _dry_run: bool,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.replacement_format_calls
            .lock()
            .unwrap()
            .push((pool_index, set_index, targets.to_vec()));
        Ok((
            HealResultItem {
                after: Infos {
                    drives: targets
                        .iter()
                        .map(|endpoint| HealDriveInfo {
                            endpoint: endpoint.clone(),
                            state: "ok".to_string(),
                            ..Default::default()
                        })
                        .collect(),
                },
                ..Default::default()
            },
            None,
        ))
    }

    async fn replacement_targets_ready(&self, _targets: &[String]) -> Result<bool> {
        Ok(*self.replacement_targets_ready.lock().unwrap())
    }

    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        _include_lifecycle_object_info: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        self.listed_prefixes.lock().unwrap().push(prefix.to_string());
        if *self.truncate_without_token.lock().unwrap() {
            return Ok((vec![heal_item("object-a")], None, true));
        }

        let mut listed = self.listed.lock().unwrap();
        if continuation_token.is_none() && !*listed {
            *listed = true;
            let objects = if bucket == RUSTFS_META_BUCKET {
                vec![
                    heal_item(&format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}")),
                    heal_item(&format!("{BUCKET_META_PREFIX}/bucket-metadata.bin")),
                ]
            } else if prefix == "logs/" {
                vec![heal_item("logs/object-a"), heal_item("logs/object-b")]
            } else if *self.include_object_dir_candidate.lock().unwrap() {
                vec![heal_item("object-a"), heal_item("object-dir/"), heal_item("object-b")]
            } else {
                vec![heal_item("object-a"), heal_item("object-b")]
            };
            Ok((objects, None, false))
        } else {
            Ok((Vec::new(), None, false))
        }
    }

    async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore> {
        self.resume_disk
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::other("not implemented in tests"))
    }

    async fn get_disk_for_resume_excluding(&self, set_disk_id: &str, _excluded_targets: &[String]) -> Result<DiskStore> {
        self.get_disk_for_resume(set_disk_id).await
    }

    async fn get_replacement_resume_disk(
        &self,
        _set_disk_id: &str,
        _task_id: &str,
        _excluded_targets: &[String],
    ) -> Result<crate::heal::storage::ReplacementResumeDisk> {
        if let Some(disk) = self.replacement_resume_disk.lock().unwrap().clone() {
            return Ok(crate::heal::storage::ReplacementResumeDisk::Existing(disk));
        }
        Ok(crate::heal::storage::ReplacementResumeDisk::Fresh)
    }

    async fn replacement_target_identities(
        &self,
        targets: &[String],
    ) -> Result<Vec<crate::heal::resume::ReplacementTargetIdentity>> {
        if !*self.replacement_targets_ready.lock().unwrap() {
            return Err(Error::other("replacement target is not ready"));
        }
        if let Some(identities) = self.replacement_target_identity_sequences.lock().unwrap().pop_front() {
            return Ok(identities);
        }
        Ok(targets
            .iter()
            .map(|endpoint| crate::heal::resume::ReplacementTargetIdentity {
                endpoint: endpoint.clone(),
                canonical_path: format!("/replacement/{endpoint}"),
                physical_device_ids: vec![endpoint.clone()],
                filesystem_identity: format!("identity-{endpoint}"),
            })
            .collect())
    }
}

#[tokio::test]
async fn scoped_object_heal_slowdown_is_not_treated_as_deleted() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(true)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::RetryableSlowDown)),
        ..Default::default()
    });
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
        storage.clone(),
    );

    let err = task.execute().await.expect_err("SlowDown must fail the current heal attempt");

    assert!(matches!(err, Error::Storage(EcstoreError::SlowDown)));
    assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
    let opts = storage
        .object_heal_opts
        .lock()
        .expect("heal options lock should be available");
    assert_eq!(opts[0].pool, Some(0));
    assert_eq!(opts[0].set, Some(1));
}

async fn make_resume_disk(temp: &TempDir) -> DiskStore {
    let disk_path = temp.path().join("test_disk");
    std::fs::create_dir_all(&disk_path).expect("test disk directory should be created");
    let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).expect("test disk endpoint should be valid");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("test disk should initialize");
    let metadata_volume = disk.make_volume(RUSTFS_META_BUCKET).await;
    assert!(
        matches!(metadata_volume, Ok(()) | Err(DiskError::VolumeExists)),
        "metadata volume should exist: {metadata_volume:?}"
    );
    disk
}

#[tokio::test]
async fn test_recursive_bucket_heal_visits_objects() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.heal_bucket("bucket-a")
        .await
        .expect("recursive bucket heal should succeed");

    assert_eq!(
        storage.healed_objects.lock().unwrap().as_slice(),
        ["object-a".to_string(), "object-b".to_string()]
    );
    let progress = task.get_progress().await;
    assert_eq!(progress.objects_scanned, 2);
    assert_eq!(progress.objects_healed, 2);
    let result_items = task.get_result_items().await;
    assert_eq!(result_items.len(), 3);
    assert_eq!(result_items.iter().filter(|item| item.object_size == 1).count(), 2);
}

#[tokio::test]
async fn result_items_are_bounded_and_report_truncation() {
    let storage = Arc::new(MockStorage::default());
    let task = HealTask::from_request(HealRequest::bucket("bucket-a".to_string()), storage);

    for _ in 0..=MAX_RETAINED_HEAL_RESULT_ITEMS {
        task.record_result_item(HealResultItem::default()).await;
    }

    assert_eq!(task.get_result_items().await.len(), MAX_RETAINED_HEAL_RESULT_ITEMS);
    assert!(task.result_items_truncated());
}

// HS-06 (backlog#1870): incremental result windows.
#[tokio::test]
async fn result_items_seq_is_monotonic_and_incremental_slices_work() {
    let storage = Arc::new(MockStorage::default());
    let task = HealTask::from_request(HealRequest::bucket("bucket-a".to_string()), storage);

    for round in 0..5u64 {
        let item = HealResultItem {
            object_size: round as usize,
            ..Default::default()
        };
        task.record_result_item(item).await;
    }

    let full = task.get_result_items_since(None).await;
    assert_eq!(full.items.len(), 5, "None keeps the full-snapshot semantics");
    assert_eq!(full.next_seq, 6, "next_seq is one past the last assigned");
    assert_eq!(full.min_seq, 1, "nothing was evicted yet");
    assert!(!full.lagged);

    // Incremental: only items newer than the cursor.
    let incremental = task.get_result_items_since(Some(3)).await;
    assert_eq!(
        incremental.items.iter().map(|item| item.object_size).collect::<Vec<_>>(),
        vec![3, 4],
        "only sequences greater than the cursor are returned"
    );
    assert_eq!(incremental.next_seq, 6);

    // A cursor at the head is not lagging.
    assert!(!task.get_result_items_since(Some(0)).await.lagged);
}

#[tokio::test]
async fn result_items_window_slide_moves_min_seq_and_flags_lagging_cursors() {
    let storage = Arc::new(MockStorage::default());
    let task = HealTask::from_request(HealRequest::bucket("bucket-a".to_string()), storage);

    // Fill the window completely, then push two more items: seq 1 and 2
    // are evicted by the slide.
    for _ in 0..(MAX_RETAINED_HEAL_RESULT_ITEMS + 2) {
        task.record_result_item(HealResultItem::default()).await;
    }

    let full = task.get_result_items_since(None).await;
    assert_eq!(full.items.len(), MAX_RETAINED_HEAL_RESULT_ITEMS);
    assert_eq!(full.min_seq, 3, "each evicted head item moved the oldest-available cursor");
    assert!(task.result_items_truncated());

    // A client still polling from before the eviction is lagging.
    let lagging = task.get_result_items_since(Some(0)).await;
    assert!(lagging.lagged, "a cursor behind min_seq must be flagged");
    assert_eq!(lagging.min_seq, 3, "the response tells the client where to restart");

    // A cursor inside the window is fine.
    assert!(!task.get_result_items_since(Some(3)).await.lagged);

    // The lagging client restarts from min_seq and gets the full window.
    let catch_up = task.get_result_items_since(Some(3)).await;
    assert_eq!(catch_up.items.len(), MAX_RETAINED_HEAL_RESULT_ITEMS - 1);
    assert!(!catch_up.lagged);
}

#[tokio::test]
async fn test_recursive_bucket_heal_skips_object_dir_candidates() {
    let storage = Arc::new(MockStorage {
        include_object_dir_candidate: Mutex::new(true),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.heal_bucket("bucket-a")
        .await
        .expect("recursive bucket heal should skip object-dir candidates");

    assert_eq!(
        storage.healed_objects.lock().unwrap().as_slice(),
        ["object-a".to_string(), "object-b".to_string()]
    );
    let progress = task.get_progress().await;
    assert_eq!(progress.objects_scanned, 3);
    assert_eq!(progress.objects_healed, 3);
    assert_eq!(progress.objects_failed, 0);
}

#[tokio::test]
async fn test_recursive_bucket_heal_treats_missing_continuation_token_as_end() {
    // A version listing can report the final page as truncated with no
    // continuation token. That is treated as end-of-listing (not an error),
    // so the returned page is healed and the pass terminates cleanly instead
    // of erroring or looping forever.
    let storage = Arc::new(MockStorage {
        truncate_without_token: Mutex::new(true),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.heal_bucket("bucket-a")
        .await
        .expect("truncated-without-token must terminate cleanly, not loop or error");

    assert_eq!(
        storage.healed_objects.lock().unwrap().as_slice(),
        ["object-a".to_string()],
        "the returned page is healed exactly once and the scan ends"
    );
}

#[tokio::test]
async fn test_cluster_heal_visits_bucket_objects() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Cluster,
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.execute().await.expect("cluster heal should visit bucket objects");

    assert_eq!(
        storage.healed_objects.lock().unwrap().as_slice(),
        ["object-a".to_string(), "object-b".to_string()]
    );
    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
}

#[tokio::test(start_paused = true)]
async fn test_recursive_bucket_heal_retries_only_retryable_objects() {
    let storage = Arc::new(MockStorage::default());
    storage
        .heal_object_outcomes
        .lock()
        .unwrap()
        .insert("object-a".to_string(), VecDeque::from([MockHealObjectOutcome::RetryableReadQuorum]));
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.heal_bucket("bucket-a")
        .await
        .expect("retryable object failure should be retried within the listing page");

    assert_eq!(
        storage.heal_object_calls.lock().unwrap().as_slice(),
        ["object-a".to_string(), "object-b".to_string(), "object-a".to_string()]
    );
    let progress = task.get_progress().await;
    assert_eq!(progress.objects_scanned, 2);
    assert_eq!(progress.objects_healed, 2);
    assert_eq!(progress.objects_failed, 0);
}

#[tokio::test(start_paused = true)]
async fn test_recursive_bucket_heal_reports_typed_exhausted_and_permanent_failures() {
    let storage = Arc::new(MockStorage::default());
    storage.heal_object_outcomes.lock().unwrap().insert(
        "object-a".to_string(),
        VecDeque::from([
            MockHealObjectOutcome::RetryableReadQuorum,
            MockHealObjectOutcome::RetryableReadQuorum,
            MockHealObjectOutcome::RetryableReadQuorum,
            MockHealObjectOutcome::RetryableReadQuorum,
        ]),
    );
    storage.heal_object_outcomes.lock().unwrap().insert(
        "object-b".to_string(),
        VecDeque::from([MockHealObjectOutcome::PermanentOther("invalid metadata")]),
    );
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    let err = task
        .heal_bucket("bucket-a")
        .await
        .expect_err("exhausted retryable and permanent failures must fail the task");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    let failure = task
        .take_batch_failure()
        .await
        .expect("batch failure details should be retained on the task");
    assert_eq!(failure.failed, 2);
    assert_eq!(failure.retryable, 1);
    assert_eq!(failure.permanent, 1);
    assert_eq!(failure.first_object, "object-b");
    let calls = storage.heal_object_calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|object| object.as_str() == "object-a").count(), 4);
    assert_eq!(calls.iter().filter(|object| object.as_str() == "object-b").count(), 1);
}

#[tokio::test]
async fn test_cluster_heal_continues_after_bucket_failure() {
    let storage = Arc::new(MockStorage {
        listed_buckets: Mutex::new(Some(vec!["bucket-a".to_string(), "bucket-b".to_string()])),
        bucket_heal_errors: Mutex::new(HashMap::from([("bucket-a".to_string(), VecDeque::from(["metadata unavailable"]))])),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::Cluster,
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    let err = task.execute().await.expect_err("cluster task must report the failed bucket");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    let failure = task
        .take_batch_failure()
        .await
        .expect("cluster failure details should be retained on the task");
    assert_eq!(failure.failed, 1);
    assert_eq!(
        storage.bucket_heal_calls.lock().unwrap().as_slice(),
        ["bucket-a".to_string(), "bucket-b".to_string()]
    );
}

#[tokio::test(start_paused = true)]
async fn test_cluster_heal_retries_only_recoverable_bucket() {
    let storage = Arc::new(MockStorage {
        listed_buckets: Mutex::new(Some(vec!["bucket-a".to_string(), "bucket-b".to_string()])),
        bucket_heal_errors: Mutex::new(HashMap::from([("bucket-a".to_string(), VecDeque::from(["lock acquisition timeout"]))])),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::Cluster,
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("cluster task should retry a recoverable bucket failure");

    assert_eq!(
        storage.bucket_heal_calls.lock().unwrap().as_slice(),
        ["bucket-a".to_string(), "bucket-a".to_string(), "bucket-b".to_string()]
    );
}

#[tokio::test]
async fn test_recursive_bucket_heal_does_not_remove_bucket_metadata() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket-a".to_string(),
        },
        HealOptions {
            recursive: true,
            remove_corrupted: true,
            recreate_missing: true,
            scan_mode: HealScanMode::Deep,
            no_lock: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.heal_bucket("bucket-a")
        .await
        .expect("recursive bucket heal should succeed");

    let bucket_opts = storage.bucket_heal_opts.lock().unwrap();
    assert_eq!(bucket_opts.len(), 1);
    assert!(!bucket_opts[0].remove);
    assert!(bucket_opts[0].recreate);
    assert_eq!(bucket_opts[0].scan_mode, HealScanMode::Deep);
    assert!(bucket_opts[0].no_lock);

    let object_opts = storage.object_heal_opts.lock().unwrap();
    assert_eq!(object_opts.len(), 2);
    assert!(object_opts.iter().all(|opts| opts.remove));
    assert!(object_opts.iter().all(|opts| opts.recreate));
    assert!(object_opts.iter().all(|opts| opts.scan_mode == HealScanMode::Deep));
    assert!(object_opts.iter().all(|opts| opts.no_lock));
}

#[tokio::test]
async fn test_prefix_heal_lists_and_repairs_objects_under_prefix() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Prefix {
            bucket: "bucket-a".to_string(),
            prefix: "logs/".to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("prefix heal should scan and repair objects under the prefix");

    assert_eq!(storage.listed_prefixes.lock().unwrap().as_slice(), ["logs/".to_string()]);
    assert_eq!(
        storage.healed_objects.lock().unwrap().as_slice(),
        ["logs/object-a".to_string(), "logs/object-b".to_string()]
    );
}

#[tokio::test]
async fn test_data_usage_cache_lock_timeout_does_not_fail_object_heal() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Object {
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}"),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    task.execute()
        .await
        .expect("data usage cache lock timeout should be skipped during heal");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
}

#[tokio::test]
async fn test_data_usage_cache_lock_timeout_does_not_fail_recursive_bucket_heal() {
    let storage = Arc::new(MockStorage::default());
    let request = HealRequest::new(
        HealType::Bucket {
            bucket: RUSTFS_META_BUCKET.to_string(),
        },
        HealOptions {
            recursive: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    task.execute()
        .await
        .expect("recursive bucket heal should skip transient data usage cache lock timeouts");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    let progress = task.get_progress().await;
    assert_eq!(progress.objects_scanned, 2);
    assert_eq!(progress.objects_failed, 0);
}

#[tokio::test]
async fn test_heal_recreate_scanner_synthetic_object_dir_skips_ok_not_found_error() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::OkWithOtherError("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("scanner synthetic object-dir missing result should be skipped");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd/".to_string()]);
}

#[tokio::test]
async fn test_heal_scanner_missing_object_dir_canonicalizes_existing_plain_object() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(false));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::Exists(true));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: Some("version-a".to_string()),
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("scanner object-dir candidate should heal the existing plain object");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd".to_string()]);
    assert_eq!(
        storage.heal_object_version_ids.lock().unwrap().as_slice(),
        [Some("version-a".to_string())]
    );
    assert_eq!(storage.healed_objects.lock().unwrap().as_slice(), ["x.rnd".to_string()]);
}

#[tokio::test]
async fn test_heal_scanner_existing_trailing_slash_object_is_not_canonicalized() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(true));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::Exists(true));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("existing trailing-slash object should keep its exact key");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd/".to_string()]);
}

#[tokio::test]
async fn test_heal_admin_missing_object_dir_does_not_canonicalize_plain_object() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(false));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::Exists(true));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    let task = HealTask::from_request(request, storage.clone());

    let err = task
        .execute()
        .await
        .expect_err("admin object-dir request must not be canonicalized");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd/".to_string()]);
}

#[tokio::test]
async fn test_heal_scanner_canonicalizes_only_one_trailing_slash() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd//".to_string(), MockObjectExists::Exists(false));
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(true));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::Exists(true));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd//".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("scanner canonicalization should remove only one trailing slash");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd/".to_string()]);
}

#[tokio::test]
async fn test_heal_scanner_trimmed_object_exists_error_is_not_recreated() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(false));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::OtherError("backend unavailable"));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    let err = task
        .execute()
        .await
        .expect_err("trimmed object_exists error must not be treated as missing");

    assert!(matches!(err, Error::Other(_)));
    assert!(storage.heal_object_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_heal_scanner_trimmed_object_exists_transient_skip_is_not_recreated() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("x.rnd/".to_string(), MockObjectExists::Exists(false));
    object_exists_by_name.insert("x.rnd".to_string(), MockObjectExists::TransientSkip("backend busy"));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("trimmed object_exists transient skip should complete without recreate");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert!(storage.heal_object_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_heal_scanner_empty_trimmed_object_keeps_existing_skip_behavior() {
    let mut object_exists_by_name = HashMap::new();
    object_exists_by_name.insert("/".to_string(), MockObjectExists::Exists(false));
    let storage = Arc::new(MockStorage {
        object_exists_by_name: Mutex::new(object_exists_by_name),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::OkWithOtherError("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("empty canonical object must keep existing scanner skip behavior");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["/".to_string()]);
}

#[tokio::test]
async fn test_heal_recreate_scanner_synthetic_object_dir_skips_err_not_found() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage);

    task.execute()
        .await
        .expect("scanner synthetic object-dir missing error should be skipped");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
}

#[tokio::test]
async fn test_heal_recreate_scanner_non_dir_not_found_fails() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage);

    let err = task
        .execute()
        .await
        .expect_err("scanner non-dir missing object should still fail recreate");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
}

#[tokio::test]
async fn test_heal_scanner_missing_object_without_recreate_probes_storage() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: false,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("scanner missing object should be checked by storage");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd".to_string()]);
    assert!(!storage.object_heal_opts.lock().unwrap()[0].recreate);
}

#[tokio::test]
async fn test_heal_scanner_missing_object_without_recreate_treats_not_found_as_stale() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: false,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage.clone());

    task.execute()
        .await
        .expect("scanner confirmed-not-found object should be treated as stale");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    assert_eq!(storage.heal_object_calls.lock().unwrap().as_slice(), ["x.rnd".to_string()]);
}

#[tokio::test]
async fn test_heal_recreate_scanner_synthetic_object_dir_disk_not_found_fails() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("Disk not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage);

    let err = task
        .execute()
        .await
        .expect_err("scanner synthetic object-dir disk-not-found should not be skipped");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
}

#[tokio::test]
async fn test_heal_recreate_admin_synthetic_object_dir_not_found_fails() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::ErrOther("File not found"))),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    let task = HealTask::from_request(request, storage);

    let err = task
        .execute()
        .await
        .expect_err("admin object-dir not-found recreate should not be skipped");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
}

#[tokio::test]
async fn test_heal_recreate_existing_trailing_slash_object_records_normal_result() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(true)),
        ..Default::default()
    });
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd/".to_string(),
            version_id: None,
        },
        HealOptions {
            recreate_missing: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Scanner;
    let task = HealTask::from_request(request, storage);

    task.execute()
        .await
        .expect("existing trailing-slash object should follow normal heal path");

    assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    let result_items = task.get_result_items().await;
    assert_eq!(result_items.len(), 1);
    assert_eq!(result_items[0].object_size, 1);
}

#[tokio::test]
async fn test_heal_failure_with_remove_corrupted_propagates_remove_flag() {
    let storage = Arc::new(MockStorage {
        object_exists: Mutex::new(Some(true)),
        heal_object_outcome: Mutex::new(Some(MockHealObjectOutcome::OkWithOtherError(
            "can not reconstruct data: not enough available shards (need 12, have 11)",
        ))),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::Object {
            bucket: "bucket-a".to_string(),
            object: "x.rnd".to_string(),
            version_id: None,
        },
        HealOptions {
            remove_corrupted: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    let err = task.execute().await.expect_err("heal failure should still be reported");

    assert!(matches!(err, Error::TaskExecutionFailed { .. }));
    assert!(storage.object_heal_opts.lock().unwrap()[0].remove);
}

#[tokio::test]
async fn test_erasure_set_heal_continues_after_format_no_heal_required() {
    let storage = Arc::new(MockStorage::default());
    *storage.format_no_heal_required.lock().unwrap() = true;
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    let err = task
        .heal_erasure_set(Vec::new(), "pool_0_set_0".to_string())
        .await
        .expect_err("test mock should fail after format when resolving resume disk");

    assert!(
        err.to_string().contains("not implemented in tests"),
        "erasure-set heal should continue past NoHealRequired format result, got: {err}"
    );
}

#[tokio::test]
async fn erasure_set_format_slowdown_is_propagated() {
    let storage = Arc::new(MockStorage {
        format_error: Mutex::new(Some(Error::Storage(EcstoreError::SlowDown))),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions::default(),
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    let error = task
        .execute()
        .await
        .expect_err("format SlowDown must remain recoverable for the task manager");

    assert!(matches!(error, Error::Storage(EcstoreError::SlowDown)));
}

#[tokio::test]
async fn erasure_set_bucket_prepass_failure_stops_before_object_heal() {
    let temp = TempDir::new().expect("temporary directory should be created");
    let disk = make_resume_disk(&temp).await;
    let storage = Arc::new(MockStorage {
        bucket_heal_errors: Mutex::new(HashMap::from([(
            "bucket-a".to_string(),
            VecDeque::from(["injected bucket prepass failure"]),
        )])),
        resume_disk: Mutex::new(Some(disk)),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage.clone());

    let error = task
        .heal_erasure_set(vec!["bucket-a".to_string()], "pool_0_set_0".to_string())
        .await
        .expect_err("bucket prepass failure must stop the erasure-set heal");

    assert!(error.to_string().contains("injected bucket prepass failure"));
    assert_eq!(storage.bucket_heal_calls.lock().unwrap().as_slice(), ["bucket-a".to_string()]);
    assert!(storage.object_heal_opts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn erasure_set_heal_applies_usage_baseline_to_progress() {
    let temp = TempDir::new().expect("temporary directory should be created");
    let disk = make_resume_disk(&temp).await;
    let storage = Arc::new(MockStorage {
        resume_disk: Mutex::new(Some(disk)),
        usage_baseline: Mutex::new(Some(HealBucketUsageBaseline {
            objects_count: 10,
            bytes: 8,
            generation: Some(1),
        })),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    task.heal_erasure_set(vec!["bucket-a".to_string()], "pool_0_set_0".to_string())
        .await
        .expect("erasure set heal should complete");

    let progress = task.get_progress().await;
    assert_eq!(progress.objects_total_count, 10);
    assert_eq!(progress.objects_total_size, 8);
    assert!(progress.baseline_generation.is_some());
    assert!(progress.baseline_known);
    assert_eq!(progress.bytes_processed, 2);
    assert!((progress.progress_percentage - 25.0).abs() < 0.001);
}

#[tokio::test]
async fn erasure_sets_from_one_usage_snapshot_share_baseline_generation() {
    let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage {
        usage_baseline: Mutex::new(Some(HealBucketUsageBaseline {
            objects_count: 10,
            bytes: 8,
            generation: Some(7),
        })),
        ..Default::default()
    });
    let buckets = vec!["bucket-a".to_string()];
    let task_for_set = |set_disk_id: &str| {
        HealTask::from_request(
            HealRequest::new(
                HealType::ErasureSet {
                    buckets: buckets.clone(),
                    set_disk_id: set_disk_id.to_string(),
                },
                HealOptions::default(),
                HealPriority::Normal,
            ),
            storage.clone(),
        )
    };
    let first = task_for_set("pool_0_set_0");
    let second = task_for_set("pool_0_set_1");

    first
        .apply_erasure_set_usage_baseline(&buckets)
        .await
        .expect("first baseline");
    second
        .apply_erasure_set_usage_baseline(&buckets)
        .await
        .expect("second baseline");
    first.progress.write().await.update_object_progress(0, 0, 0, 0, 0);
    second.progress.write().await.update_object_progress(0, 0, 0, 0, 0);
    let first = first.get_progress().await;
    let second = second.get_progress().await;

    let expected_generation = first.baseline_generation;
    assert!(expected_generation.is_some());
    assert_eq!(second.baseline_generation, expected_generation);
    let aggregate = aggregate_heal_progress([first, second]).expect("aggregate progress");
    assert!(aggregate.baseline_known);
    assert_eq!(aggregate.baseline_generation, expected_generation);
    assert_eq!(aggregate.progress_state, HealProgressState::Running);
}

#[tokio::test]
async fn erasure_set_disk_walk_keeps_cluster_usage_baseline_indeterminate() {
    for (scan_mode, source) in [
        (HealScanMode::Deep, HealRequestSource::Admin),
        (HealScanMode::Normal, HealRequestSource::AutoHeal),
    ] {
        let temp = TempDir::new().expect("temporary directory should be created");
        let disk = make_resume_disk(&temp).await;
        let storage = Arc::new(MockStorage {
            resume_disk: Mutex::new(Some(disk)),
            usage_baseline: Mutex::new(Some(HealBucketUsageBaseline {
                objects_count: 10,
                bytes: 8,
                generation: Some(1),
            })),
            ..Default::default()
        });
        let mut request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-a".to_string()],
                set_disk_id: "pool_0_set_0".to_string(),
            },
            HealOptions {
                scan_mode,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        request.source = source;
        let task = HealTask::from_request(request, storage);

        task.heal_erasure_set(vec!["bucket-a".to_string()], "pool_0_set_0".to_string())
            .await
            .expect("erasure set heal should complete");

        let progress = task.get_progress().await;
        assert!(!progress.baseline_known);
        assert_eq!(progress.baseline_generation, None);
        assert_eq!(progress.progress_state, HealProgressState::Indeterminate);
    }
}

#[tokio::test]
async fn erasure_set_heal_ignores_usage_baseline_errors() {
    let temp = TempDir::new().expect("temporary directory should be created");
    let disk = make_resume_disk(&temp).await;
    let storage = Arc::new(MockStorage {
        resume_disk: Mutex::new(Some(disk)),
        usage_baseline_error: Mutex::new(true),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = HealTask::from_request(request, storage);

    task.heal_erasure_set(vec!["bucket-a".to_string()], "pool_0_set_0".to_string())
        .await
        .expect("usage baseline failures should not fail erasure set heal");

    let progress = task.get_progress().await;
    assert_eq!(progress.objects_total_count, 0);
    assert_eq!(progress.objects_total_size, 0);
}

#[tokio::test]
async fn resumable_erasure_set_execution_is_cancelled_while_object_heal_is_pending() {
    let temp = TempDir::new().expect("temporary directory should be created");
    let disk = make_resume_disk(&temp).await;
    let storage = Arc::new(MockStorage {
        block_heal_object: Mutex::new(true),
        resume_disk: Mutex::new(Some(disk)),
        ..Default::default()
    });
    let request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec!["bucket-a".to_string()],
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            no_lock: true,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    let task = Arc::new(HealTask::from_request(request, storage.clone()));
    let execution = tokio::spawn({
        let task = task.clone();
        async move { task.execute().await }
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !storage.object_heal_opts.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resumable object heal should start");
    task.cancel().await.expect("task cancellation should succeed");

    let result = tokio::time::timeout(Duration::from_secs(1), execution)
        .await
        .expect("cancellation should interrupt the pending resumable heal")
        .expect("task execution should join");
    assert!(matches!(result, Err(Error::TaskCancelled)));
    assert!(storage.bucket_heal_opts.lock().unwrap().iter().all(|opts| opts.no_lock));
    assert!(storage.object_heal_opts.lock().unwrap().iter().all(|opts| opts.no_lock));
}
