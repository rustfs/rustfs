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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ObjectTrafficSnapshot {
    pub(crate) read_stalled: bool,
    pub(crate) write_stalled: bool,
}

/// Detects bounded object stages that stop returning from foreground requests.
/// Both success and error returns are progress: dependency correctness remains
/// the responsibility of the existing readiness checks.
#[derive(Debug)]
pub(crate) struct ObjectTrafficHealth {
    started_at: Instant,
    stall_after_ms: u64,
    enabled: bool,
    read_metadata: OperationProgress,
    read_storage: OperationProgress,
    write_storage: OperationProgress,
}

impl ObjectTrafficHealth {
    pub(crate) fn from_env() -> Self {
        let enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_ENABLE,
            rustfs_config::DEFAULT_HEALTH_OBJECT_PROGRESS_ENABLE,
        );
        let configured_timeout_ms = rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS,
            rustfs_config::DEFAULT_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS,
        );
        let requested_timeout_ms = if configured_timeout_ms == 0 {
            rustfs_config::DEFAULT_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS
        } else {
            configured_timeout_ms
        };
        let minimum_timeout_ms = duration_ms_saturating(crate::storage::get_lock_acquire_timeout())
            .saturating_add(rustfs_config::HEALTH_OBJECT_PROGRESS_LOCK_MARGIN_MS);
        let stall_after_ms = requested_timeout_ms.max(minimum_timeout_ms);

        Self::new(enabled, stall_after_ms)
    }

    fn new(enabled: bool, stall_after_ms: u64) -> Self {
        Self {
            started_at: Instant::now(),
            stall_after_ms,
            enabled,
            read_metadata: OperationProgress::default(),
            read_storage: OperationProgress::default(),
            write_storage: OperationProgress::default(),
        }
    }

    pub(crate) fn track_read_metadata(&self) -> Option<ObjectTrafficProgressGuard<'_>> {
        self.track(&self.read_metadata)
    }

    pub(crate) fn track_read_storage(&self) -> Option<ObjectTrafficProgressGuard<'_>> {
        self.track(&self.read_storage)
    }

    pub(crate) fn track_write_storage(&self) -> Option<ObjectTrafficProgressGuard<'_>> {
        self.track(&self.write_storage)
    }

    pub(crate) fn snapshot(&self) -> ObjectTrafficSnapshot {
        if !self.enabled {
            return ObjectTrafficSnapshot::default();
        }

        let now_ms = self.now_ms();
        ObjectTrafficSnapshot {
            read_stalled: self.read_metadata.is_stalled_at(now_ms, self.stall_after_ms)
                || self.read_storage.is_stalled_at(now_ms, self.stall_after_ms),
            write_stalled: self.write_storage.is_stalled_at(now_ms, self.stall_after_ms),
        }
    }

    fn track<'a>(&'a self, progress: &'a OperationProgress) -> Option<ObjectTrafficProgressGuard<'a>> {
        if !self.enabled || !progress.begin_at(self.now_ms()) {
            return None;
        }
        Some(ObjectTrafficProgressGuard { health: self, progress })
    }

    fn now_ms(&self) -> u64 {
        duration_ms_saturating(self.started_at.elapsed())
    }

    #[cfg(test)]
    pub(crate) fn enabled_for_test(stall_after: Duration) -> Self {
        Self::new(true, duration_ms_saturating(stall_after))
    }

    #[cfg(test)]
    pub(crate) fn read_storage_stalled_for_test(&self) -> bool {
        self.read_storage.is_stalled_at(self.now_ms(), self.stall_after_ms)
    }
}

#[derive(Debug, Default)]
struct OperationProgress {
    active: AtomicU64,
    last_progress_ms: AtomicU64,
}

impl OperationProgress {
    fn begin_at(&self, now_ms: u64) -> bool {
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            let Some(next) = active.checked_add(1) else {
                return false;
            };
            if active == 0 {
                self.last_progress_ms.fetch_max(now_ms, Ordering::Relaxed);
            }
            match self
                .active
                .compare_exchange_weak(active, next, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => active = observed,
            }
        }
    }

    fn complete_at(&self, now_ms: u64) {
        self.last_progress_ms.fetch_max(now_ms, Ordering::Relaxed);
        let previous = self.active.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "object traffic progress guard underflow");
    }

    fn is_stalled_at(&self, now_ms: u64, stall_after_ms: u64) -> bool {
        self.active.load(Ordering::Acquire) > 0
            && now_ms.saturating_sub(self.last_progress_ms.load(Ordering::Relaxed)) >= stall_after_ms
    }
}

#[must_use = "dropping the guard records operation completion"]
pub(crate) struct ObjectTrafficProgressGuard<'a> {
    health: &'a ObjectTrafficHealth,
    progress: &'a OperationProgress,
}

impl Drop for ObjectTrafficProgressGuard<'_> {
    fn drop(&mut self) {
        self.progress.complete_at(self.health.now_ms());
    }
}

fn duration_ms_saturating(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(duration.subsec_millis()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_active_operation_stalls_at_the_exact_boundary() {
        let progress = OperationProgress::default();
        assert!(progress.begin_at(10));

        assert!(!progress.is_stalled_at(39, 30));
        assert!(progress.is_stalled_at(40, 30));
    }

    #[test]
    fn later_arrivals_do_not_hide_an_existing_stall() {
        let progress = OperationProgress::default();
        assert!(progress.begin_at(10));
        assert!(progress.begin_at(35));

        assert!(progress.is_stalled_at(40, 30));
    }

    #[test]
    fn a_completion_resets_progress_until_the_remaining_operation_stalls() {
        let progress = OperationProgress::default();
        assert!(progress.begin_at(10));
        assert!(progress.begin_at(20));

        progress.complete_at(35);

        assert!(!progress.is_stalled_at(64, 30));
        assert!(progress.is_stalled_at(65, 30));
        progress.complete_at(65);
        assert!(!progress.is_stalled_at(u64::MAX, 30));
    }

    #[test]
    fn a_stale_begin_timestamp_cannot_overwrite_newer_progress() {
        let progress = OperationProgress::default();
        assert!(progress.begin_at(10));
        progress.complete_at(100);

        assert!(progress.begin_at(10));
        assert!(!progress.is_stalled_at(129, 30));
        assert!(progress.is_stalled_at(130, 30));
    }

    #[test]
    #[serial_test::serial]
    fn environment_configuration_is_sanitized() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_ENABLE, Some("false")),
                (rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS, Some("1")),
            ],
            || {
                let minimum_timeout_ms = duration_ms_saturating(crate::storage::get_lock_acquire_timeout())
                    .saturating_add(rustfs_config::HEALTH_OBJECT_PROGRESS_LOCK_MARGIN_MS);
                let health = ObjectTrafficHealth::from_env();
                assert!(!health.enabled);
                assert_eq!(health.stall_after_ms, minimum_timeout_ms);
            },
        );

        temp_env::with_vars([(rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS, Some("0"))], || {
            let minimum_timeout_ms = duration_ms_saturating(crate::storage::get_lock_acquire_timeout())
                .saturating_add(rustfs_config::HEALTH_OBJECT_PROGRESS_LOCK_MARGIN_MS);
            let health = ObjectTrafficHealth::from_env();
            assert_eq!(
                health.stall_after_ms,
                rustfs_config::DEFAULT_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS.max(minimum_timeout_ms)
            );
        });
    }

    #[test]
    fn read_and_write_progress_are_independent() {
        let health = ObjectTrafficHealth::enabled_for_test(Duration::ZERO);
        let read = health.track_read_storage().expect("read tracking must be enabled");

        assert_eq!(
            health.snapshot(),
            ObjectTrafficSnapshot {
                read_stalled: true,
                write_stalled: false,
            }
        );

        drop(read);
        let write = health.track_write_storage().expect("write tracking must be enabled");
        assert_eq!(
            health.snapshot(),
            ObjectTrafficSnapshot {
                read_stalled: false,
                write_stalled: true,
            }
        );
        drop(write);
        assert_eq!(health.snapshot(), ObjectTrafficSnapshot::default());
    }

    #[test]
    fn disabled_tracking_never_withdraws_readiness() {
        let health = ObjectTrafficHealth::new(false, 0);

        assert!(health.track_read_metadata().is_none());
        assert!(health.track_read_storage().is_none());
        assert!(health.track_write_storage().is_none());
        assert_eq!(health.snapshot(), ObjectTrafficSnapshot::default());
    }

    #[test]
    fn metadata_completions_do_not_hide_a_storage_stall() {
        let health = ObjectTrafficHealth::enabled_for_test(Duration::ZERO);
        let storage = health.track_read_storage().expect("read storage tracking must be enabled");
        let metadata = health.track_read_metadata().expect("read metadata tracking must be enabled");
        drop(metadata);

        assert!(health.snapshot().read_stalled);
        drop(storage);
    }

    #[tokio::test]
    async fn aborting_a_tracked_future_clears_the_active_operation() {
        let health = std::sync::Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let task_health = std::sync::Arc::clone(&health);
        let task = tokio::spawn(async move {
            let _progress = task_health.track_read_storage().expect("read tracking must be enabled");
            std::future::pending::<()>().await;
        });

        if tokio::time::timeout(Duration::from_secs(2), async {
            while !health.snapshot().read_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
        {
            task.abort();
            let _ = task.await;
            panic!("tracked future did not publish an active operation");
        }
        task.abort();
        assert!(task.await.expect_err("tracked task must be cancelled").is_cancelled());
        assert!(!health.snapshot().read_stalled);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn app_context_honors_the_disabled_progress_environment() {
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        temp_env::async_with_vars([(rustfs_config::ENV_HEALTH_OBJECT_PROGRESS_ENABLE, Some("false"))], async {
            let context = crate::app::gating_test_env::app_context_from_current_environment(&ambient);
            assert!(context.object_traffic_health().track_read_storage().is_none());
        })
        .await;

        let installed = crate::app::runtime_sources::current_app_context().expect("test AppContext must remain installed");
        assert!(std::sync::Arc::ptr_eq(&ambient, &installed));
    }
}
