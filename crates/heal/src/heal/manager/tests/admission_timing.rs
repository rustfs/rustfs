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
use std::future::Future;

const PHASE: &str = "rustfs_heal_admission_lock_phase_seconds";
const WAIT: &str = "rustfs_heal_admission_active_lock_wait_seconds";
const HOLD: &str = "rustfs_heal_admission_active_lock_hold_seconds";

struct AdmissionRecorder {
    manager: Arc<HealManager>,
    samples: Arc<StdMutex<Vec<(&'static str, f64)>>>,
}

impl AdmissionRecorder {
    fn new(manager: &Arc<HealManager>) -> Self {
        Self {
            manager: Arc::clone(manager),
            samples: Arc::default(),
        }
    }
}

struct AdmissionHistogram {
    name: &'static str,
    manager: Arc<HealManager>,
    samples: Arc<StdMutex<Vec<(&'static str, f64)>>>,
}

impl metrics::HistogramFn for AdmissionHistogram {
    fn record(&self, value: f64) {
        assert!(self.manager.active_heals.try_lock().is_ok(), "recording must follow active lock release");
        assert!(self.manager.heal_queue.try_lock().is_ok(), "recording must follow queue lock release");
        assert!(self.manager.retrying_heals.try_lock().is_ok(), "recording must follow retry lock release");
        self.samples.lock().expect("admission samples").push((self.name, value));
    }
}

impl metrics::Recorder for AdmissionRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        let name = match key.name() {
            PHASE => PHASE,
            WAIT => WAIT,
            HOLD => HOLD,
            _ => return metrics::Histogram::noop(),
        };
        if name != PHASE {
            assert_eq!(key.labels().count(), 0, "new lock histograms have static, unlabelled keys");
        }
        metrics::Histogram::from_arc(Arc::new(AdmissionHistogram {
            name,
            manager: Arc::clone(&self.manager),
            samples: Arc::clone(&self.samples),
        }))
    }
}

async fn recorded<F: Future>(recorder: &AdmissionRecorder, future: F) -> F::Output {
    tokio::pin!(future);
    futures::future::poll_fn(|cx| metrics::with_local_recorder(recorder, || future.as_mut().poll(cx))).await
}

fn timing_samples(recorder: &AdmissionRecorder) -> (f64, f64, f64) {
    let samples = recorder.samples.lock().expect("admission samples");
    let sample = |name| {
        let values: Vec<_> = samples
            .iter()
            .filter(|(key, _)| *key == name)
            .map(|(_, value)| *value)
            .collect();
        assert_eq!(values.len(), 1, "exactly one {name} observation per receipt");
        values[0]
    };
    (sample(PHASE), sample(WAIT), sample(HOLD))
}

fn assert_partition(recorder: &AdmissionRecorder, minimum_wait: Duration, minimum_hold: Duration) {
    let (phase, wait, hold) = timing_samples(recorder);
    assert!(wait >= minimum_wait.as_secs_f64(), "initial active-lock contention belongs to wait");
    assert!(hold >= minimum_hold.as_secs_f64(), "nested-lock contention belongs to active-lock hold");
    assert!(
        (phase - (wait + hold)).abs() <= phase * f64::EPSILON * 4.0,
        "wait and hold must partition the existing phase without double-counting contention"
    );
}

#[tokio::test]
async fn admission_timing_separates_initial_active_lock_wait() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None));
    let recorder = AdmissionRecorder::new(&manager);
    let active = manager.active_heals.lock().await;
    let admission = recorded(&recorder, manager.submit_heal_request(internal_prefix_request("bucket-a", "logs/")));
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending(), "admission must reach the held active lock");
    let stalled = Instant::now();
    tokio::task::yield_now().await;
    let minimum_wait = stalled.elapsed();
    drop(active);
    assert_eq!(admission.await.expect("admit after active lock release"), HealAdmissionResult::Accepted);
    assert!(manager.active_heals.try_lock().is_ok(), "receipt must release active ownership");
    assert_partition(&recorder, minimum_wait, Duration::ZERO);
}

#[tokio::test]
async fn admission_timing_includes_queue_wait_while_active_lock_is_held() {
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None));
    let recorder = AdmissionRecorder::new(&manager);
    let queue = manager.heal_queue.lock().await;
    let admission = recorded(&recorder, manager.submit_heal_request(internal_prefix_request("bucket-a", "logs/")));
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending(), "admission must reach the held queue lock");
    assert!(manager.active_heals.try_lock().is_err(), "admission owns active while waiting for queue");
    let stalled = Instant::now();
    tokio::task::yield_now().await;
    let minimum_hold = stalled.elapsed();
    drop(queue);
    assert_eq!(admission.await.expect("admit after queue lock release"), HealAdmissionResult::Accepted);
    assert!(manager.active_heals.try_lock().is_ok(), "receipt must release active ownership");
    assert_partition(&recorder, Duration::ZERO, minimum_hold);
}

#[tokio::test]
async fn admission_timing_force_start_excludes_cancel_gap_and_sums_reacquisition() {
    let manager = Arc::new(manager_with_policy(HealOverlapPolicy::Merge));
    let old_id = insert_active_task(&manager, admin_prefix_request("bucket-a", "logs/")).await;
    let recorder = AdmissionRecorder::new(&manager);
    let mut replacement = admin_prefix_request("bucket-a", "logs/");
    replacement.force_start = true;
    let queue = manager.heal_queue.lock().await;
    let admission = recorded(&recorder, manager.submit_heal_request_with_receipt(replacement));
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending());
    assert!(
        manager.active_heals.try_lock().is_err(),
        "initial interval owns active while waiting for queue"
    );
    let first_held_at = Instant::now();
    let aliases = manager.task_aliases.lock().await;
    let first_hold = first_held_at.elapsed();
    drop(queue);
    assert!(
        futures::poll!(&mut admission).is_pending(),
        "cancellation must wait for canonical alias lookup"
    );
    assert!(manager.active_heals.try_lock().is_ok());
    assert!(manager.heal_queue.try_lock().is_ok());
    assert!(manager.retrying_heals.try_lock().is_ok());
    let unlocked_at = Instant::now();

    // Queue cancellation first, then a real competing owner. Tokio's FIFO
    // mutex hands that owner the lock before forceStart can reacquire it.
    let active = manager.active_heals.lock().await;
    let cancellation_gap = unlocked_at.elapsed();
    drop(aliases);
    assert!(futures::poll!(&mut admission).is_pending(), "cancellation is queued for active");
    let next_active = manager.active_heals.lock();
    tokio::pin!(next_active);
    assert!(futures::poll!(&mut next_active).is_pending());
    drop(active);
    assert!(
        futures::poll!(&mut admission).is_pending(),
        "forceStart must queue behind the competing owner"
    );
    let active = next_active.await;
    assert!(
        manager.completed_heals.lock().await.contains_key(&old_id),
        "cancellation finished before reacquisition"
    );
    let reacquiring_at = Instant::now();
    let queue = manager.heal_queue.lock().await;
    assert!(manager.retrying_heals.try_lock().is_ok());
    let second_wait = reacquiring_at.elapsed();
    drop(active);
    assert!(futures::poll!(&mut admission).is_pending(), "reacquired active waits for queue");
    assert!(manager.active_heals.try_lock().is_err());
    let second_held_at = Instant::now();
    assert!(recorder.samples.lock().expect("samples").is_empty());
    let second_hold = second_held_at.elapsed();
    drop(queue);
    assert!(admission.await.expect("force-start receipt").result.is_admitted());

    let (phase, wait, hold) = timing_samples(&recorder);
    assert!(wait >= second_wait.as_secs_f64(), "second acquisition contention must be counted as wait");
    assert!(
        hold >= (first_hold + second_hold).as_secs_f64(),
        "both actual held intervals must be counted"
    );
    assert!(
        phase - (wait + hold) >= cancellation_gap.as_secs_f64(),
        "the unlocked cancellation gap belongs only to the unchanged total phase"
    );
}
