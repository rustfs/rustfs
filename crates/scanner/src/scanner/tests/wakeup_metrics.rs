// Copyright 2026 RustFS Team
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

const WAKEUPS: &str = "rustfs_scanner_cycle_wakeups_total";
const ATTEMPTS: &str = "rustfs_scanner_cycle_attempts_total";

#[derive(Default)]
struct TriggerRecorder(StdMutex<HashMap<(String, String), Arc<AtomicU64>>>);

impl metrics::Recorder for TriggerRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if !matches!(key.name(), WAKEUPS | ATTEMPTS) {
            return metrics::Counter::noop();
        }
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 1, "trigger cardinality is one closed reason label");
        assert_eq!(labels[0].key(), "reason");
        metrics::Counter::from_arc(
            self.0
                .lock()
                .expect("trigger counter recorder")
                .entry((key.name().to_owned(), labels[0].value().to_owned()))
                .or_default()
                .clone(),
        )
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

impl TriggerRecorder {
    fn count(&self, metric: &str, reason: &str) -> u64 {
        self.0
            .lock()
            .expect("trigger counter recorder")
            .get(&(metric.to_owned(), reason.to_owned()))
            .map_or(0, |counter| counter.load(Ordering::Relaxed))
    }

    fn total(&self, metric: &str) -> u64 {
        self.0
            .lock()
            .expect("trigger counter recorder")
            .iter()
            .filter(|((name, _), _)| name == metric)
            .map(|(_, counter)| counter.load(Ordering::Relaxed))
            .sum()
    }

    async fn wait_for(&self, metric: &str, reason: &str) {
        tokio::time::timeout(Duration::from_secs(20), async {
            while self.count(metric, reason) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the real scanner loop must emit the selected trigger");
    }
}

#[test]
fn scanner_trigger_metrics_have_closed_reasons_and_initial_attempt_only() {
    let reasons = [
        (ScannerCycleWakeReason::Timer, "timer"),
        (ScannerCycleWakeReason::DirtyUsage, "dirty_usage"),
        (ScannerCycleWakeReason::MovementGeneration, "movement_generation"),
        (ScannerCycleWakeReason::ClusterActivity, "cluster_activity"),
        (ScannerCycleWakeReason::ClusterMaintenance, "cluster_maintenance"),
        (ScannerCycleWakeReason::ClusterActivityUnavailable, "cluster_activity_unavailable"),
        (ScannerCycleWakeReason::RuntimeConfig, "runtime_config"),
        (ScannerCycleWakeReason::MaintenanceConfig, "maintenance_config"),
        (ScannerCycleWakeReason::Recovery, "recovery"),
        (ScannerCycleWakeReason::LeaderLockLost, "leader_lock_lost"),
        (ScannerCycleWakeReason::Cancelled, "cancelled"),
    ];
    let recorder = TriggerRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        for (reason, _) in reasons {
            record_scanner_cycle_wakeup(reason);
            record_scanner_cycle_attempt(Some(reason));
        }
        record_scanner_cycle_attempt(None);
    });
    for (_, label) in reasons {
        assert_eq!(recorder.count(WAKEUPS, label), 1);
        assert_eq!(recorder.count(ATTEMPTS, label), 1);
    }
    assert_eq!(recorder.count(WAKEUPS, "initial"), 0);
    assert_eq!(recorder.count(ATTEMPTS, "initial"), 1);
    assert_eq!(recorder.0.lock().expect("trigger counter recorder").len(), 23);
}

// Tokio's default test runtime is current-thread: the recorder observes the
// real spawned scheduler task without installing a process-global recorder.
#[tokio::test]
#[serial]
async fn scanner_trigger_metrics_observe_initial_and_timer_runner_boundaries() {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        global_metrics().set_cycle(None).await;
        let (_temp_dir, store) = setup_scanner_cycle_store().await;
        let recorder = TriggerRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let ctx = CancellationToken::new();
        let scanner_task = tokio::spawn(run_data_scanner(ctx.clone(), store));
        recorder.wait_for(ATTEMPTS, "initial").await;
        recorder.wait_for(ATTEMPTS, "timer").await;
        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(5), scanner_task)
            .await
            .expect("scanner loop must stop after cancellation")
            .expect("scanner task must not panic")
            .expect("scanner loop must exit cleanly");
        assert_eq!(recorder.count(ATTEMPTS, "initial"), 1);
        assert_eq!(recorder.count(WAKEUPS, "initial"), 0);
        assert_eq!(recorder.count(WAKEUPS, "timer"), recorder.count(ATTEMPTS, "timer"));
        assert_eq!(recorder.count(ATTEMPTS, "cancelled"), 0);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
#[serial]
async fn scanner_trigger_metrics_keep_paused_startup_outside_runner_boundary() {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        global_metrics().set_cycle(None).await;
        let (_temp_dir, store) = setup_scanner_cycle_store_with_pool_count(true, 2).await;
        {
            let mut pools = store.pool_meta.write().await;
            pools.pools[0].last_update = time::OffsetDateTime::now_utc();
            pools.pools[0].decommission = Some(crate::storage_api::owner::EcstorePoolDecommissionInfo {
                failed: true,
                ..Default::default()
            });
        }
        assert!(store.scanner_data_movement_pause_status().await.paused);
        let recorder = TriggerRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let paused_probe = ScannerRuntimeObservedProbe::install(&store, true);
        let ctx = CancellationToken::new();
        let scanner_task = tokio::spawn(run_data_scanner(ctx.clone(), store));
        paused_probe.wait().await;
        drop(paused_probe);
        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(5), scanner_task)
            .await
            .expect("paused scanner must stop after cancellation")
            .expect("paused scanner task must not panic")
            .expect("paused scanner must exit cleanly");
        assert_eq!(recorder.total(ATTEMPTS), 0, "movement pause must not manufacture a runner attempt");
        assert_eq!(recorder.total(WAKEUPS), 0, "startup pause wait is not the outer cycle scheduler wait");
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
#[serial]
async fn scanner_trigger_metrics_record_recovery_and_config_without_rate_limited_attempts() {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        global_metrics().set_cycle(None).await;
        let (_temp_dir, store) = setup_scanner_cycle_store().await;
        let now = scanner_pause_backlog_now();
        let mut backlog = ScannerPauseBacklogController::claim(store.clone(), now)
            .await
            .expect("seed writer must claim the pause backlog");
        let mut observation = current_scanner_pause_backlog_observation(&store).await;
        observation.paused = true;
        observation.pause_started_at_unix_secs = now;
        backlog.observe(observation).await;
        observation.paused = false;
        backlog.observe(observation).await;
        let ScannerPauseBacklogAttemptDecision::Tracked(serial) = backlog.begin_attempt(now).await else {
            panic!("seeded catch-up must admit its first attempt");
        };
        backlog.finish_attempt(serial, ScannerCycleOutcome::Failed, observation).await;
        drop(backlog);
        let before = scanner_pause_backlog_status(store.clone()).await;
        assert!(before.rate_limited, "the persisted catch-up gate must delay the next attempt");
        let recorder = TriggerRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let ctx = CancellationToken::new();
        let scanner_task = tokio::spawn(run_data_scanner(ctx.clone(), store));
        tokio::time::timeout(Duration::from_secs(20), async {
            while !scanner_cycle_schedule_status().effective_interval_available {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("rate-limited scanner must reach its normal scheduler wait");
        notify_scanner_cycle_recovery_wake();
        recorder.wait_for(WAKEUPS, "recovery").await;
        let mut config = rustfs_config::server_config::Config::new();
        config.set_defaults();
        crate::runtime_config::apply_scanner_runtime_config(&config).expect("runtime config must still apply");
        recorder.wait_for(WAKEUPS, "runtime_config").await;
        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(5), scanner_task)
            .await
            .expect("rate-limited scanner must stop after cancellation")
            .expect("rate-limited scanner task must not panic")
            .expect("rate-limited scanner must exit cleanly");
        assert_eq!(recorder.count(WAKEUPS, "recovery"), 1, "internal activity polls must not add wakeups");
        assert_eq!(recorder.count(WAKEUPS, "runtime_config"), 1);
        assert_eq!(recorder.total(ATTEMPTS), 0, "neither wakeup bypasses the existing catch-up rate limit");
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}
