// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

fn owned_config() -> HealConfig {
    HealConfig {
        enable_auto_heal: false,
        event_driven_scheduler_enable: true,
        heal_interval: Duration::from_secs(60),
        mainline_throttle_enable: false,
        ..Default::default()
    }
}

async fn until(mut ready: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready().await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real producer reaches expected boundary");
}

async fn completion_fixture() -> (Arc<HealManager>, Arc<CompletedRetentionHook>, String, String) {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(owned_config()),
    ));
    let bucket = format!("owned-worker-{}", Uuid::new_v4());
    let request = HealRequest::object(bucket.clone(), "object".to_owned(), None);
    let id = request.id.clone();
    let hook = Arc::new(CompletedRetentionHook {
        pause_before_publish: true,
        ..Default::default()
    });
    {
        let mut hooks = COMPLETED_RETENTION_HOOKS.lock().await;
        hooks.insert(bucket.clone(), hook.clone());
        hooks.insert(id.clone(), hook.clone());
    }
    manager.submit_heal_request(request).await.expect("queue real object heal");
    manager.start().await.expect("start actual scheduler and both GCs");
    tokio::time::timeout(Duration::from_secs(5), hook.started.notified())
        .await
        .expect("actual worker reaches storage");
    hook.execute.notify_one();
    tokio::time::timeout(Duration::from_secs(5), hook.before_publish.notified())
        .await
        .expect("actual worker reaches existing publication boundary");
    (manager, hook, bucket, id)
}

async fn remove_hook(bucket: &str, id: &str) {
    COMPLETED_RETENTION_HOOKS
        .lock()
        .await
        .retain(|key, _| key != bucket && key != id);
}

#[tokio::test]
async fn owned_manager_waits_actual_worker_without_holding_manager_locks() {
    let (manager, hook, bucket, id) = completion_fixture().await;
    let stopping = manager.clone();
    let mut stop = tokio::spawn(async move { stopping.stop().await });
    manager.cancel_token.cancelled().await;
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut stop).await.is_err());
    // The real worker needs these locks after the existing test boundary.
    let admin = manager.admin_start_shutdown.try_lock().expect("admin released before join");
    let active = manager.active_heals.try_lock().expect("active released before join");
    let cache = manager.completed_heals.try_lock().expect("cache released before join");
    assert!(!manager.workers.tasks.is_empty(), "actual publication worker is still owned");
    drop((admin, active, cache));
    hook.publish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), hook.handoff.notified())
        .await
        .expect("real worker reaches existing final handoff boundary");
    hook.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .expect("worker drain deadline")
        .expect("stop waiter")
        .expect("actual worker joined");
    assert!(manager.workers.tasks.is_empty());
    assert!(
        manager
            .workers
            .registration
            .lock()
            .unwrap()
            .fixed
            .iter()
            .flatten()
            .all(|join| join.peek().is_some())
    );
    assert!(manager.start().await.is_err(), "terminal owner cannot restart");
    manager.stop().await.expect("successful receipt is repeatable");
    remove_hook(&bucket, &id).await;
}

#[tokio::test]
async fn owned_manager_cancelled_stop_waiter_keeps_actual_worker_receipt() {
    let (manager, hook, bucket, id) = completion_fixture().await;
    let stopping = manager.clone();
    let stop = tokio::spawn(async move { stopping.stop().await });
    manager.cancel_token.cancelled().await;
    until(async || manager.admin_start_shutdown.try_lock().is_ok()).await;
    stop.abort();
    assert!(stop.await.unwrap_err().is_cancelled());
    let stopping = manager.clone();
    let mut retry = tokio::spawn(async move { stopping.stop().await });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut retry).await.is_err());
    hook.publish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), hook.handoff.notified())
        .await
        .expect("real worker reaches existing final handoff boundary");
    hook.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), retry)
        .await
        .expect("repeat drain deadline")
        .expect("retry waiter")
        .expect("same actual worker joined");
    assert!(manager.workers.tasks.is_empty());
    remove_hook(&bucket, &id).await;
}

#[tokio::test]
async fn owned_manager_start_stop_race_counts_independent_stop_intents() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(owned_config()),
    ));
    let admin = manager.admin_start_shutdown.lock().await;
    let starting = manager.clone();
    let start = tokio::spawn(async move { starting.start().await });
    until(async || manager.state.read().await.is_running).await;
    let stopping = manager.clone();
    let first = tokio::spawn(async move { stopping.stop().await });
    let stopping = manager.clone();
    let second = tokio::spawn(async move { stopping.stop().await });
    until(async || manager.workers.registration.lock().unwrap().stopping == 2).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(manager.workers.registration.lock().unwrap().stopping, 1);
    assert!(manager.start().await.is_err(), "cancelled waiter must not reopen another waiter's gate");
    assert!(
        !manager.cancel_token.is_cancelled(),
        "handoff waits for startup before signalling cancellation"
    );
    assert!(!second.is_finished());
    drop(admin);
    start
        .await
        .expect("actual startup waiter")
        .expect("startup finishes before handoff");
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("stop drains all newly registered loops")
        .expect("stop waiter")
        .expect("joined");
    assert!(manager.workers.tasks.is_empty());
    assert_eq!(manager.workers.registration.lock().unwrap().fixed.iter().flatten().count(), 3);
}

#[tokio::test]
async fn owned_manager_cancelled_pre_handoff_stop_reopens_start_admission() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(owned_config()),
    ));
    let active = manager.active_heals.lock().await;
    let stopping = manager.clone();
    let stop = tokio::spawn(async move { stopping.stop().await });
    until(async || manager.workers.registration.lock().unwrap().stopping == 1).await;
    until(async || manager.admin_start_shutdown.try_lock().is_err()).await;
    stop.abort();
    assert!(stop.await.unwrap_err().is_cancelled());
    assert_eq!(manager.workers.registration.lock().unwrap().stopping, 0);
    assert!(!manager.cancel_token.is_cancelled());
    drop(active);
    manager
        .start()
        .await
        .expect("pre-handoff cancellation did not terminally close startup");
    manager.stop().await.expect("later real startup and stop succeed");
}

#[tokio::test]
async fn owned_manager_failed_durable_handoff_remains_open_and_retryable() {
    use super::super::super::RUSTFS_META_BUCKET;
    use super::super::root_recovery::RootHealRecovery;
    let temp = TempDir::new().expect("owned recovery disk");
    let endpoint = Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("real local disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("metadata volume: {error}"),
    }
    let mut manager = HealManager::new(Arc::new(MockStorage), Some(owned_config()));
    manager.root_recovery = Arc::new(RootHealRecovery::with_disks(vec![disk.clone()]));
    let mut request = HealRequest::new(HealType::Cluster, HealOptions::default(), HealPriority::High);
    request.source = HealRequestSource::Admin;
    let id = request.id.clone();
    manager.submit_heal_request(request).await.expect("real durable admission");
    std::fs::remove_dir_all(temp.path().join(RUSTFS_META_BUCKET)).expect("remove accepted owner volume");
    assert!(manager.stop().await.is_err(), "real durable handoff fails");
    assert!(!manager.stop_handoff_completed());
    assert!(!manager.cancel_token.is_cancelled());
    assert!(manager.workers.registration.lock().unwrap().open);
    assert_eq!(manager.workers.registration.lock().unwrap().stopping, 0);
    assert!(manager.heal_queue.lock().await.requests().any(|request| request.id == id));
    disk.make_volume(RUSTFS_META_BUCKET).await.expect("restore original owner");
    manager.stop().await.expect("handoff retries on same disk and drains");
    assert!(manager.stop_handoff_completed());
    assert!(!manager.workers.registration.lock().unwrap().open);
    assert_eq!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("durable successor remains")
            .len(),
        1
    );
}

#[tokio::test]
async fn owned_manager_real_retry_and_throttle_wake_drain() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(owned_config()),
    ));
    let hook = Arc::new(CompletedRetentionHook::default());
    COMPLETED_RETENTION_HOOKS
        .lock()
        .await
        .insert("retry-transition".to_owned(), hook.clone());
    manager
        .submit_heal_request(HealRequest::object("retry-transition".to_owned(), "object".to_owned(), None))
        .await
        .expect("queue actual recoverable storage failure");
    manager.start().await.expect("actual scheduler");
    tokio::time::timeout(Duration::from_secs(5), hook.started.notified())
        .await
        .expect("real object operation");
    hook.execute.notify_one();
    until(async || !manager.retrying_heals.lock().await.is_empty()).await;
    manager.stop().await.expect("actual delayed retry drained");
    assert!(manager.workers.tasks.is_empty());
    assert!(manager.retrying_heals.lock().await.is_empty());
    COMPLETED_RETENTION_HOOKS.lock().await.remove("retry-transition");

    let provider: WorkloadSnapshotProviderRef = Arc::new(FixedWorkloadProvider {
        class: WorkloadClass::ForegroundRead,
        active: 8,
        limit: 10,
        state: AdmissionState::Open,
    });
    let manager = HealManager::new_with_workload_provider_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(HealConfig {
            mainline_throttle_enable: true,
            mainline_read_utilization_high_percent: 80,
            mainline_write_utilization_high_percent: 80,
            mainline_max_sleep: Duration::from_secs(60),
            ..owned_config()
        }),
        Some(provider),
    );
    manager.start().await.expect("actual throttled scheduler");
    until(async || manager.workers.tasks.len() == 4).await;
    manager
        .submit_heal_request(bucket_request("owned-throttle", HealPriority::Normal, HealRequestSource::ReadRepair))
        .await
        .expect("real pressure-delayed request");
    until(async || manager.workers.tasks.len() > 4).await;
    assert_eq!(manager.get_queue_length().await, 1);
    tokio::time::timeout(Duration::from_secs(5), manager.stop())
        .await
        .expect("wake cancels without waiting 60s")
        .expect("wake drained");
    assert!(manager.workers.tasks.is_empty());
}

#[tokio::test]
async fn owned_manager_real_scheduler_panic_is_sticky_failure() {
    let manager = HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(HealConfig {
            heal_interval: Duration::ZERO,
            ..owned_config()
        }),
    );
    manager.start().await.expect("startup registered actual scheduler");
    until(async || manager.workers.failed.load(Ordering::Acquire)).await;
    assert!(manager.stop().await.is_err(), "actual interval panic cannot acknowledge joined success");
    assert!(manager.stop_handoff_completed());
    assert!(manager.stop().await.is_err(), "repeat wait cannot erase failure");
    assert!(manager.workers.tasks.is_empty());
}

#[tokio::test]
async fn owned_manager_completed_dynamic_workers_do_not_accumulate_handles() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), Some(owned_config()));
    manager.start().await.expect("actual producer loops");
    for index in 0..64 {
        let request = HealRequest::object(format!("owned-completed-{index}"), "object".to_owned(), None);
        let id = request.id.clone();
        manager
            .submit_heal_request(request)
            .await
            .expect("actual scheduler admission");
        until(async || manager.completed_heals.lock().await.contains_key(&id)).await;
    }
    // Scheduler retains a live parent plus its guard; each GC retains one guard.
    // These four fixed-loop tokens remain, irrespective of the 64 finished jobs.
    until(async || manager.workers.tasks.len() == 4).await;
    assert_eq!(manager.workers.registration.lock().unwrap().fixed.iter().flatten().count(), 3);
    manager.stop().await.expect("bounded dynamic bookkeeping drains");
    assert!(manager.workers.tasks.is_empty());
}

#[tokio::test]
async fn owned_manager_enabled_auto_scan_has_four_actual_loop_receipts() {
    let manager = HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(HealConfig {
            enable_auto_heal: true,
            ..owned_config()
        }),
    );
    manager.start().await.expect("four actual producer loops");
    assert_eq!(manager.workers.registration.lock().unwrap().fixed.iter().flatten().count(), 4);
    manager.stop().await.expect("four loops joined");
    assert!(manager.workers.tasks.is_empty());
}

#[tokio::test]
async fn owned_manager_cancelled_actual_start_is_sticky_failure() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(owned_config()),
    ));
    let admin = manager.admin_start_shutdown.lock().await;
    let starting = manager.clone();
    let start = tokio::spawn(async move { starting.start().await });
    until(async || manager.state.read().await.is_running).await;
    start.abort();
    assert!(start.await.unwrap_err().is_cancelled());
    drop(admin);
    assert!(
        manager.stop().await.is_err(),
        "cancelled partial startup cannot become successful receipt"
    );
    assert!(manager.stop().await.is_err(), "failure remains sticky for later waiter");
    assert!(manager.workers.tasks.is_empty());
}
