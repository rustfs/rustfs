// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::heal::storage::HealListItem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Semaphore;

#[derive(Default)]
struct PressureProbe {
    active: AtomicUsize,
    commit_open: AtomicBool,
    high_sampled: Notify,
}

impl WorkloadAdmissionSnapshotProvider for PressureProbe {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        assert!(
            !self.commit_open.load(Ordering::SeqCst),
            "pressure must not be sampled inside an object commit"
        );
        let active = self.active.load(Ordering::SeqCst);
        if active >= 80 {
            self.high_sampled.notify_one();
        }
        WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
                Some(active),
                None,
                Some(100),
            ),
        ])
    }
}

struct RunningStorage {
    provider: Arc<PressureProbe>,
    namespace: Mutex<()>,
    io: Arc<Semaphore>,
    first_started: Notify,
    release_first: Notify,
    first_finished: Notify,
    second_finished: Notify,
    started: AtomicUsize,
    committed: AtomicUsize,
}

#[async_trait::async_trait]
impl HealStorageAPI for RunningStorage {
    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        Ok(None)
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        Ok(Some(BucketInfo {
            name: bucket.into(),
            ..Default::default()
        }))
    }
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        Ok(Vec::new())
    }
    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        Ok(HealResultItem::default())
    }
    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        Ok((HealResultItem::default(), None))
    }
    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        Err(Error::other("no resume disk in bucket fixture"))
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        Ok((
            ["a", "b"]
                .into_iter()
                .map(|name| HealListItem {
                    name: name.into(),
                    version_id: None,
                    mod_time_unix_nanos: None,
                    lifecycle_object_info: None,
                    is_delete_marker: false,
                })
                .collect(),
            None,
            false,
        ))
    }
    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        let permit = self.io.clone().acquire_owned().await.expect("fixture I/O permit");
        let namespace = self.namespace.lock().await;
        self.provider.commit_open.store(true, Ordering::SeqCst);
        let index = self.started.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        self.committed.fetch_add(1, Ordering::SeqCst);
        self.provider.commit_open.store(false, Ordering::SeqCst);
        drop(namespace);
        drop(permit);
        if index == 0 {
            self.first_finished.notify_one();
        } else {
            self.second_finished.notify_one();
        }
        Ok((
            HealResultItem {
                object_size: 1,
                ..Default::default()
            },
            None,
        ))
    }
}

async fn start_fixture(
    provider_enabled: bool,
    pacing_enabled: bool,
    timeout: Duration,
) -> (HealManager, Arc<RunningStorage>, Arc<PressureProbe>, Arc<HealTask>) {
    let provider = Arc::new(PressureProbe::default());
    let storage = Arc::new(RunningStorage {
        provider: provider.clone(),
        namespace: Mutex::new(()),
        io: Arc::new(Semaphore::new(1)),
        first_started: Notify::new(),
        release_first: Notify::new(),
        first_finished: Notify::new(),
        second_finished: Notify::new(),
        started: AtomicUsize::new(0),
        committed: AtomicUsize::new(0),
    });
    let manager = HealManager::new_with_workload_provider(
        storage.clone(),
        Some(HealConfig {
            mainline_throttle_enable: pacing_enabled,
            mainline_read_utilization_high_percent: 80,
            mainline_write_utilization_high_percent: 80,
            mainline_max_sleep: Duration::from_millis(250),
            max_concurrent_heals: 1,
            ..HealConfig::default()
        }),
        provider_enabled.then(|| provider.clone() as WorkloadSnapshotProviderRef),
    );
    let mut request = bucket_request("running-mainline", HealPriority::High, HealRequestSource::Admin);
    request.options.recursive = true;
    request.options.timeout = Some(timeout);
    let task_id = request.id.clone();
    manager.submit_heal_request(request).await.expect("queue admin heal");
    process_manager_queue_once(&manager).await;
    storage.first_started.notified().await;
    let task = manager
        .active_heals
        .lock()
        .await
        .get(&task_id)
        .cloned()
        .expect("running task");
    (manager, storage, provider, task)
}

#[tokio::test(start_paused = true)]
async fn running_mainline_admin_resamples_after_commit_and_yields_without_io_guards() {
    let (_manager, storage, provider, _task) = start_fixture(true, true, Duration::from_secs(60)).await;
    provider.active.store(100, Ordering::SeqCst);
    assert!(storage.provider.commit_open.load(Ordering::SeqCst));
    assert_eq!(storage.committed.load(Ordering::SeqCst), 0);
    storage.release_first.notify_one();
    storage.first_finished.notified().await;
    tokio::time::timeout(Duration::from_millis(1), provider.high_sampled.notified())
        .await
        .expect("running admin heal must re-sample rising pressure before its next object");
    assert_eq!(storage.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        storage.committed.load(Ordering::SeqCst),
        1,
        "in-flight commit must finish despite pressure"
    );
    assert_eq!(storage.io.available_permits(), 1, "pacing must release I/O permits");
    assert!(storage.namespace.try_lock().is_ok(), "pacing must not hold the namespace lock");
    tokio::time::advance(Duration::from_millis(250)).await;
    storage.second_finished.notified().await;
    assert_eq!(
        storage.committed.load(Ordering::SeqCst),
        2,
        "sustained pressure must still allow bounded maintenance progress"
    );
}

#[tokio::test(start_paused = true)]
async fn running_mainline_missing_provider_or_disabled_pacing_preserves_progress() {
    for (provider_enabled, pacing_enabled) in [(false, true), (true, false)] {
        let (_manager, storage, provider, _task) = start_fixture(provider_enabled, pacing_enabled, Duration::from_secs(60)).await;
        provider.active.store(100, Ordering::SeqCst);
        let before = tokio::time::Instant::now();
        storage.release_first.notify_one();
        storage.second_finished.notified().await;
        assert_eq!(storage.committed.load(Ordering::SeqCst), 2);
        assert_eq!(tokio::time::Instant::now(), before);
        assert_eq!(storage.io.available_permits(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn running_mainline_cancellation_and_deadline_leave_next_object_unstarted() {
    for cancelled in [true, false] {
        let (_manager, storage, provider, task) = start_fixture(true, true, Duration::from_millis(100)).await;
        provider.active.store(100, Ordering::SeqCst);
        storage.release_first.notify_one();
        provider.high_sampled.notified().await;
        if cancelled {
            task.cancel_token.cancel();
        } else {
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while matches!(task.get_status().await, HealTaskStatus::Running) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("pacing must not mask cancellation or timeout");
        assert_eq!(storage.started.load(Ordering::SeqCst), 1);
        assert_eq!(storage.committed.load(Ordering::SeqCst), 1);
        assert_eq!(storage.io.available_permits(), 1);
        assert!(storage.namespace.try_lock().is_ok());
        let outcome = task.get_outcome().await;
        assert_eq!(outcome.counters.processed, 1);
        assert_eq!(
            task.get_status().await,
            if cancelled {
                HealTaskStatus::Cancelled
            } else {
                HealTaskStatus::Timeout
            }
        );
    }
}
