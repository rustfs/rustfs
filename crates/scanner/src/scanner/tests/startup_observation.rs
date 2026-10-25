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
use rustfs_scanner_metrics::startup::ScannerStartupPhase;

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_observation_tracks_wait_policy_and_owner_cancellation() {
    let config = startup_gate::startup_config(&[("start_delay", "120"), ("cycle", "21600")]);
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("startup configuration");
    let runtime = ScannerRuntimeGuard::new();
    let ctx = CancellationToken::new();
    let mut gate = Box::pin(wait_for_scanner_startup(
        runtime.startup_token(),
        &ctx,
        std::future::ready((false, true)),
        false,
        scanner_maintenance_generation(),
        |delay| delay,
    ));
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    let waiting = global_metrics().report().await.startup;
    assert_eq!(waiting.phase, ScannerStartupPhase::Waiting);
    assert_eq!(waiting.wait_remaining_seconds, 120);
    advance(Duration::from_secs(30)).await;
    let equivalent = startup_gate::startup_config(&[("start_delay", "120"), ("cycle", "43200")]);
    crate::runtime_config::apply_scanner_runtime_config(&equivalent).expect("equivalent startup policy");
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    let unchanged = global_metrics().report().await.startup;
    assert_eq!(unchanged.planned_attempt_at_unix_secs, waiting.planned_attempt_at_unix_secs);
    assert_eq!(unchanged.wait_remaining_seconds, 90);
    assert_eq!(unchanged.phase_age_seconds, 30);
    let shorter = startup_gate::startup_config(&[("start_delay", "1"), ("cycle", "43200")]);
    crate::runtime_config::apply_scanner_runtime_config(&shorter).expect("shorter startup policy");
    assert!(matches!(futures::poll!(&mut gate), Poll::Ready(true)));
    let elapsed = global_metrics().report().await.startup;
    assert_eq!(elapsed.planned_attempt_at_unix_secs, Some(waiting.started_at_unix_secs + 1));
    assert_eq!(elapsed.wait_remaining_seconds, 0);
    assert_eq!(elapsed.wait_overdue_seconds, 29);
    assert!(
        elapsed.first_attempt_at_unix_secs.is_none(),
        "finishing a wait is not entering the runner"
    );
    drop(gate);
    drop(runtime);
    let stopped = global_metrics().report().await.startup;
    assert_eq!(stopped.phase, ScannerStartupPhase::Stopped);
    assert!(!stopped.runtime_active);
    assert!(stopped.planned_attempt_at_unix_secs.is_none());
    assert!(stopped.first_attempt_at_unix_secs.is_none());
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn startup_observation_inspection_cancellation_does_not_invent_attempt_or_success() {
    let runtime = ScannerRuntimeGuard::new();
    let ctx = CancellationToken::new();
    let before = global_metrics().report().await.usage_freshness.last_durable_success_unix_secs;
    let mut gate = Box::pin(wait_for_scanner_startup(
        runtime.startup_token(),
        &ctx,
        std::future::pending::<(bool, bool)>(),
        false,
        scanner_maintenance_generation(),
        |delay| delay,
    ));
    assert!(matches!(futures::poll!(&mut gate), Poll::Pending));
    let inspecting = global_metrics().report().await.startup;
    assert_eq!(inspecting.phase, ScannerStartupPhase::UsageInspection);
    assert!(inspecting.planned_attempt_at_unix_secs.is_none());
    ctx.cancel();
    assert!(!gate.await);
    drop(runtime);
    let report = global_metrics().report().await;
    assert_eq!(report.startup.phase, ScannerStartupPhase::Stopped);
    assert!(report.startup.first_attempt_at_unix_secs.is_none());
    assert_eq!(report.usage_freshness.last_durable_success_unix_secs, before);
}

#[tokio::test]
#[serial]
async fn startup_observation_records_real_runner_entry_before_any_cycle() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let ctx = CancellationToken::new();
    ctx.cancel();
    let before = global_metrics().report().await.usage_freshness.last_durable_success_unix_secs;
    let _result = run_data_scanner(ctx, store).await;
    let report = global_metrics().report().await;
    assert!(
        report.startup.first_attempt_at_unix_secs.is_some(),
        "the actual leadership path must record its first entry"
    );
    assert_eq!(report.startup.phase, ScannerStartupPhase::Stopped);
    assert!(!report.current_cycle_active);
    assert_eq!(report.usage_freshness.last_durable_success_unix_secs, before);
}
