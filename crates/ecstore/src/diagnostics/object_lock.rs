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

use rustfs_lock::{LockError, NamespaceLockGuard, NamespaceLockWrapper};
use std::time::{Duration, Instant};
use tracing::Span;
use uuid::Uuid;

const EVENT_OBJECT_NAMESPACE_LOCK: &str = "object_namespace_lock";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

/// Diagnostic lifetime only; it never owns or changes a namespace lock.
/// Keep it alongside the real guard when ownership moves to a commit tail.
#[derive(Default)]
pub(crate) struct ObjectLockAttempt(Option<Box<AttemptContext>>);

struct AttemptContext {
    id: Uuid,
    parent: Span,
    op: &'static str,
    bucket: String,
    object: String,
    requested_version_id: Option<String>,
    namespace: String,
    owner: String,
    mode: &'static str,
    timeout: Duration,
    started_at: Instant,
    acquired_at: Option<Instant>,
    finished: bool,
}

impl ObjectLockAttempt {
    pub(crate) fn start(
        op: &'static str,
        bucket: &str,
        object: &str,
        requested_version_id: Option<&str>,
        lock: &NamespaceLockWrapper,
        mode: &'static str,
        timeout: Duration,
    ) -> Self {
        if !crate::set_disk::is_object_lock_diag_enabled() {
            return Self::default();
        }
        let context = Box::new(AttemptContext {
            id: Uuid::new_v4(),
            parent: Span::current(),
            op,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            requested_version_id: requested_version_id.map(str::to_owned),
            namespace: lock.namespace().to_owned(),
            owner: lock.owner().to_owned(),
            mode,
            timeout,
            started_at: Instant::now(),
            acquired_at: None,
            finished: false,
        });
        context.trace("acquiring");
        Self(Some(context))
    }

    /// Observe before callers map the error to storage/S3 or return with `?`.
    pub(crate) fn observe(&mut self, result: &Result<NamespaceLockGuard, LockError>) {
        let Some(context) = self.0.as_mut() else {
            return;
        };
        match result {
            Ok(NamespaceLockGuard::Fast(guard)) if guard.is_disabled() => {
                context.finished = true;
                context.trace("disabled");
            }
            Ok(_) => {
                context.acquired_at = Some(Instant::now());
                context.trace("acquired");
            }
            Err(error) => {
                context.finished = true;
                let failure = match error {
                    LockError::Timeout { .. } => "timeout",
                    LockError::AlreadyLocked { .. } => "conflict",
                    LockError::QuorumNotReached { .. } | LockError::InsufficientNodes { .. } => "quorum_unavailable",
                    LockError::Network { .. } => "network",
                    LockError::QueueFull { .. } => "queue_full",
                    _ => "other",
                };
                context.parent.in_scope(|| {
                    tracing::error!(
                        target: "rustfs_ecstore::object_lock_diag",
                        parent: &context.parent,
                        event = EVENT_OBJECT_NAMESPACE_LOCK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                        state = "failed",
                        op = context.op,
                        bucket = %context.bucket,
                        object = %context.object,
                        requested_version_id = context.requested_version_id.as_deref(),
                        namespace = %context.namespace,
                        mode = context.mode,
                        owner = %context.owner,
                        lock_attempt_id = %context.id,
                        acquire_ms = duration_ms(context.started_at.elapsed()),
                        timeout_ms = duration_ms(context.timeout),
                        failure,
                        "Object namespace lock acquisition failed"
                    )
                });
            }
        }
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl AttemptContext {
    fn trace(&self, state: &'static str) {
        let now = Instant::now();
        let acquired_at = self.acquired_at.unwrap_or(now);
        // JSON span lists use the entered span as well as the event parent.
        self.parent.in_scope(|| {
            tracing::trace!(
                target: "rustfs_ecstore::object_lock_diag",
                parent: &self.parent,
                event = EVENT_OBJECT_NAMESPACE_LOCK,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                state,
                op = self.op,
                bucket = %self.bucket,
                object = %self.object,
                requested_version_id = self.requested_version_id.as_deref(),
                namespace = %self.namespace,
                mode = self.mode,
                owner = %self.owner,
                lock_attempt_id = %self.id,
                acquire_ms = duration_ms(acquired_at.duration_since(self.started_at)),
                hold_ms = duration_ms(now.duration_since(acquired_at)),
                timeout_ms = duration_ms(self.timeout),
                "Object namespace lock diagnostic"
            )
        });
    }
}

impl Drop for ObjectLockAttempt {
    fn drop(&mut self) {
        if let Some(context) = &self.0
            && !context.finished
        {
            // Distributed release may still be in flight after the guard drops.
            context.trace(if context.acquired_at.is_some() {
                "guard_dropped"
            } else {
                "cancelled"
            });
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    pub(crate) struct CapturedLockEvents(Arc<Mutex<Vec<u8>>>);

    pub(crate) struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("lock diagnostic capture mutex").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLockEvents {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(self.0.clone())
        }
    }

    impl CapturedLockEvents {
        pub(crate) fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
            tracing_subscriber::fmt()
                .json()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(self.clone())
                .finish()
        }

        pub(crate) fn rows(&self) -> Vec<serde_json::Value> {
            let bytes = self.0.lock().expect("lock diagnostic capture mutex");
            let text = std::str::from_utf8(&bytes).expect("UTF-8 diagnostic events");
            text.lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON diagnostic event"))
                .filter(|row| row["fields"]["event"] == EVENT_OBJECT_NAMESPACE_LOCK)
                .collect()
        }
    }

    fn namespace() -> NamespaceLockWrapper {
        let manager = rustfs_lock::GlobalLockManager::Enabled(Arc::new(rustfs_lock::fast_lock::FastObjectLockManager::new()));
        NamespaceLockWrapper::new(
            rustfs_lock::NamespaceLock::with_local_manager("diagnostics".to_owned(), Arc::new(manager)),
            rustfs_lock::ObjectKey::new("bucket", "object"),
            "shared-set-owner".to_owned(),
        )
    }

    #[test]
    #[serial_test::serial]
    fn object_lock_diagnostics_disabled_does_not_allocate_context_or_emit() {
        temp_env::with_vars([(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("false"))], || {
            let capture = CapturedLockEvents::default();
            tracing::subscriber::with_default(capture.subscriber(), || {
                let attempt = ObjectLockAttempt::start(
                    "get_object",
                    "bucket",
                    "object",
                    None,
                    &namespace(),
                    "read",
                    Duration::from_secs(5),
                );
                assert!(attempt.0.is_none(), "disabled diagnostics must not allocate per-request state");
                drop(attempt);
            });
            assert!(capture.rows().is_empty(), "disabled diagnostics must not emit lifecycle events");
        });
    }

    #[test]
    #[serial_test::serial]
    fn object_lock_diagnostics_cancelled_attempts_keep_distinct_ids_and_parent() {
        temp_env::with_vars([(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true"))], || {
            let capture = CapturedLockEvents::default();
            tracing::subscriber::with_default(capture.subscriber(), || {
                let request = tracing::info_span!("request", request_id = "original-get-request");
                let attempts = request.in_scope(|| {
                    (0..2)
                        .map(|_| {
                            ObjectLockAttempt::start(
                                "get_object",
                                "bucket",
                                "object",
                                Some("historical-version"),
                                &namespace(),
                                "read",
                                Duration::from_secs(5),
                            )
                        })
                        .collect::<Vec<_>>()
                });
                drop(attempts);
            });
            let rows = capture.rows();
            assert_eq!(rows.len(), 4, "each pending attempt needs acquisition and cancellation events");
            let cancelled = rows
                .iter()
                .filter(|row| row["fields"]["state"] == "cancelled")
                .collect::<Vec<_>>();
            assert_eq!(cancelled.len(), 2);
            assert_ne!(
                cancelled[0]["fields"]["lock_attempt_id"], cancelled[1]["fields"]["lock_attempt_id"],
                "a shared set owner cannot serve as the request identity"
            );
            for row in cancelled {
                assert_eq!(row["fields"]["requested_version_id"], "historical-version");
                assert!(
                    row["spans"]
                        .as_array()
                        .expect("parent spans")
                        .iter()
                        .any(|span| span["request_id"] == "original-get-request"),
                    "dropping outside the request span must retain its identity"
                );
            }
        });
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn object_lock_diagnostics_follow_guard_into_background_tail() {
        use tracing::{Instrument, instrument::WithSubscriber};

        let capture = CapturedLockEvents::default();
        temp_env::async_with_vars(
            [(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true"))],
            async {
                let lock = namespace();
                let request = tracing::info_span!("request", request_id = "original-put-request");
                let mut attempt = request.in_scope(|| {
                    ObjectLockAttempt::start(
                        "put_object_commit",
                        "bucket",
                        "object",
                        None,
                        &lock,
                        "write",
                        Duration::from_secs(5),
                    )
                });
                let result = lock.get_write_lock(Duration::from_secs(5)).await;
                attempt.observe(&result);
                let guard = result.expect("real PUT commit guard");
                let rows = capture.rows();
                assert_eq!(
                    rows.iter()
                        .map(|row| row["fields"]["state"].as_str().expect("state"))
                        .collect::<Vec<_>>(),
                    ["acquiring", "acquired"]
                );
                tokio::spawn(
                    async move {
                        drop((guard, attempt));
                    }
                    .instrument(tracing::info_span!("unrelated_tail"))
                    .with_current_subscriber(),
                )
                .await
                .expect("background guard owner");
                let rows = capture.rows();
                let dropped = rows.last().expect("guard drop event");
                assert_eq!(dropped["fields"]["state"], "guard_dropped");
                assert_eq!(dropped["fields"]["lock_attempt_id"], rows[0]["fields"]["lock_attempt_id"]);
                assert!(
                    dropped["spans"]
                        .as_array()
                        .expect("retained parent spans")
                        .iter()
                        .any(|span| span["request_id"] == "original-put-request")
                );
                drop(
                    lock.get_read_lock(Duration::from_secs(1))
                        .await
                        .expect("tail drop releases the actual lock"),
                );
            }
            .with_subscriber(capture.subscriber()),
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn object_lock_diagnostics_disabled_backend_does_not_claim_ownership() {
        use tracing::instrument::WithSubscriber;

        let capture = CapturedLockEvents::default();
        temp_env::async_with_vars(
            [(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true"))],
            async {
                let manager = rustfs_lock::GlobalLockManager::Disabled(rustfs_lock::fast_lock::DisabledLockManager::new());
                let lock = NamespaceLockWrapper::new(
                    rustfs_lock::NamespaceLock::with_local_manager("disabled".to_owned(), Arc::new(manager)),
                    rustfs_lock::ObjectKey::new("bucket", "object"),
                    "owner".to_owned(),
                );
                let mut attempt =
                    ObjectLockAttempt::start("get_object", "bucket", "object", None, &lock, "read", Duration::from_secs(5));
                let result = lock.get_read_lock(Duration::from_secs(5)).await;
                attempt.observe(&result);
                drop((result.expect("disabled backend guard"), attempt));
                let rows = capture.rows();
                assert_eq!(
                    rows.iter()
                        .map(|row| row["fields"]["state"].as_str().expect("state"))
                        .collect::<Vec<_>>(),
                    ["acquiring", "disabled"]
                );
            }
            .with_subscriber(capture.subscriber()),
        )
        .await;
    }

    #[test]
    #[serial_test::serial]
    fn object_lock_diagnostics_failure_classes_do_not_log_error_payloads() {
        temp_env::with_vars([(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true"))], || {
            let capture = CapturedLockEvents::default();
            tracing::subscriber::with_default(capture.subscriber(), || {
                for (error, expected) in [
                    (
                        LockError::QuorumNotReached {
                            required: 3,
                            achieved: 1,
                        },
                        "quorum_unavailable",
                    ),
                    (
                        LockError::Internal {
                            message: "untrusted-error-payload".to_owned(),
                        },
                        "other",
                    ),
                ] {
                    let mut attempt = ObjectLockAttempt::start(
                        "get_object",
                        "bucket",
                        "object",
                        None,
                        &namespace(),
                        "read",
                        Duration::from_secs(5),
                    );
                    attempt.observe(&Err(error));
                    drop(attempt);
                    let rows = capture.rows();
                    let failure = rows.last().expect("terminal failure event");
                    assert_eq!(failure["fields"]["state"], "failed");
                    assert_eq!(failure["fields"]["failure"], expected);
                    assert!(!failure.to_string().contains("untrusted-error-payload"));
                }
            });
        });
    }
}
