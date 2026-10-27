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

use rustfs_io_metrics::record_object_lock_acquire_failure;
use rustfs_lock::{LockError, NamespaceLockGuard, NamespaceLockWrapper};
use std::time::{Duration, Instant};
use tracing::Span;
use uuid::Uuid;

const EVENT_OBJECT_NAMESPACE_LOCK: &str = "object_namespace_lock";
const EVENT_OBJECT_NAMESPACE_LOCK_PEER: &str = "object_namespace_lock_peer";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

pub(crate) fn record_lock_acquire_failure(op: &'static str, bucket: &str, object: &str, mode: &'static str, error: &LockError) {
    let resource_class = if bucket == crate::disk::RUSTFS_META_MULTIPART_BUCKET {
        "multipart_metadata"
    } else if bucket == crate::disk::RUSTFS_META_BUCKET || bucket == ".minio.sys" {
        "system_metadata"
    } else if object.is_empty() || object == bucket {
        "bucket_namespace"
    } else {
        "object_namespace"
    };
    let reason = match error {
        LockError::Timeout { .. } => "timeout",
        LockError::AlreadyLocked { .. } => "contention",
        LockError::QuorumNotReached { .. } | LockError::InsufficientNodes { .. } => "quorum",
        LockError::Network { .. } | LockError::Internal { .. } => "transport",
        _ => "other",
    };
    record_object_lock_acquire_failure(op, mode, resource_class, reason);
}

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
    lock_id: Option<String>,
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
            lock_id: None,
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
            Ok(guard) => {
                context.lock_id = guard.lock_id().map(ToString::to_string);
                context.acquired_at = Some(Instant::now());
                context.trace("acquired");
                if let NamespaceLockGuard::Standard(guard) = guard {
                    for (peer_index, peer_lock_id) in guard.granted_lock_ids().enumerate() {
                        context.parent.in_scope(|| {
                            tracing::trace!(
                                target: "rustfs_ecstore::object_lock_diag",
                                parent: &context.parent,
                                event = EVENT_OBJECT_NAMESPACE_LOCK_PEER,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                                state = "granted",
                                lock_attempt_id = %context.id,
                                lock_id = context.lock_id.as_deref(),
                                peer_lock_id = %peer_lock_id,
                                peer_index,
                                "Object namespace peer owner granted"
                            );
                        });
                    }
                }
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
                lock_id = self.lock_id.as_deref(),
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
            self.all_rows()
                .into_iter()
                .filter(|row| row["fields"]["event"] == EVENT_OBJECT_NAMESPACE_LOCK)
                .collect()
        }

        fn all_rows(&self) -> Vec<serde_json::Value> {
            let bytes = self.0.lock().expect("lock diagnostic capture mutex");
            let text = std::str::from_utf8(&bytes).expect("UTF-8 diagnostic events");
            text.lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON diagnostic event"))
                .collect()
        }
    }

    #[test]
    #[serial_test::serial]
    fn object_lock_diagnostics_join_guard_identity_to_physical_release() {
        use rustfs_lock::{
            DisabledLockManager, FastObjectLockManager, GlobalLockManager, LocalClient, LockClient, LockRequest, LockType,
            NamespaceLock, ObjectKey,
        };

        temp_env::with_vars([(rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true"))], || {
            let capture = CapturedLockEvents::default();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("diagnostic release runtime");
            tracing::subscriber::with_default(capture.subscriber(), || {
                runtime.block_on(async {
                    let clients = (0..4)
                        .map(|_| Arc::new(LocalClient::with_manager(Arc::new(GlobalLockManager::new()))) as Arc<dyn LockClient>)
                        .collect::<Vec<_>>();
                    let lock = NamespaceLockWrapper::new(
                        NamespaceLock::with_clients_and_quorum("diagnostics".to_owned(), clients.clone(), 4),
                        ObjectKey::new("bucket", "object"),
                        "coordinator".to_owned(),
                    );
                    let request = tracing::info_span!("request", request_id = "original-multipart-request");
                    let mut attempt = request.in_scope(|| {
                        ObjectLockAttempt::start(
                            "abort_multipart_upload",
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
                    let guard = result.expect("four real namespace owners");
                    let id = guard.lock_id().expect("distributed guard identity").clone();
                    let peer_ids = match &guard {
                        NamespaceLockGuard::Standard(guard) => {
                            assert_eq!(guard.granted_lock_ids().count(), 4);
                            guard.granted_lock_ids().cloned().collect::<std::collections::HashSet<_>>()
                        }
                        NamespaceLockGuard::Fast(_) => panic!("real distributed namespace required"),
                    };
                    let rows = capture.rows();
                    assert_eq!(rows.last().expect("acquired event")["fields"]["lock_id"], id.to_string());
                    let rows = capture.all_rows();
                    let mappings = rows
                        .iter()
                        .filter(|row| row["fields"]["event"] == EVENT_OBJECT_NAMESPACE_LOCK_PEER)
                        .collect::<Vec<_>>();
                    assert_eq!(mappings.len(), 4, "every granted peer must map to its logical guard");
                    assert!(mappings.iter().all(|row| {
                        row["fields"]["lock_id"] == id.to_string()
                            && peer_ids
                                .iter()
                                .any(|peer_id| row["fields"]["peer_lock_id"] == peer_id.to_string())
                    }));
                    for client in &clients {
                        let mut owned = 0;
                        for peer_id in &peer_ids {
                            owned += usize::from(
                                client
                                    .check_status(peer_id)
                                    .await
                                    .expect("actual peer owner status")
                                    .is_some(),
                            );
                        }
                        assert_eq!(owned, 1, "each client must hold exactly one actual granted identity");
                    }
                    drop((guard, attempt));
                    assert!(
                        capture.all_rows().iter().all(|row| row["fields"]["state"] != "released"),
                        "enqueue is not release acknowledgement"
                    );
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while capture
                            .all_rows()
                            .iter()
                            .filter(|row| {
                                row["fields"]["event"] == "namespace_lock_release" && row["fields"]["state"] == "released"
                            })
                            .count()
                            < 4
                        {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("all physical release callbacks finish");
                    for client in &clients {
                        for peer_id in &peer_ids {
                            assert!(
                                client
                                    .check_status(peer_id)
                                    .await
                                    .expect("released peer owner status")
                                    .is_none()
                            );
                            assert!(!client.release(peer_id).await.expect("duplicate release is absent"));
                        }
                    }
                    let disabled = LocalClient::with_manager(Arc::new(GlobalLockManager::Disabled(DisabledLockManager::new())));
                    let disabled_request =
                        LockRequest::new(ObjectKey::new("bucket", "disabled"), LockType::Exclusive, "disabled-owner".to_owned());
                    assert!(
                        disabled
                            .acquire_lock(&disabled_request)
                            .await
                            .expect("disabled acquire bypass")
                            .success
                    );
                    assert!(
                        disabled
                            .release(&disabled_request.lock_id)
                            .await
                            .expect("disabled release bypass")
                    );
                    let rows = capture.all_rows();
                    let releases = rows
                        .iter()
                        .filter(|row| row["fields"]["event"] == "namespace_lock_release" && row["fields"]["state"] == "released")
                        .collect::<Vec<_>>();
                    assert_eq!(
                        releases.len(),
                        4,
                        "duplicates and disabled guards cannot acknowledge another physical release"
                    );
                    let physical_mappings = rows
                        .iter()
                        .filter(|row| row["fields"]["event"] == "namespace_lock_release" && row["fields"]["state"] == "mapped")
                        .collect::<Vec<_>>();
                    assert_eq!(physical_mappings.len(), 4);
                    assert!(releases.iter().all(|row| physical_mappings.iter().any(|mapped| {
                        mapped["fields"]["physical_guard_id"] == row["fields"]["physical_guard_id"]
                            && mapped["fields"]["process_id"] == row["fields"]["process_id"]
                            && peer_ids
                                .iter()
                                .any(|peer_id| mapped["fields"]["lock_id"] == peer_id.to_string())
                    })));
                    let dropped = capture.rows().pop().expect("guard drop event");
                    assert_eq!(dropped["fields"]["lock_id"], id.to_string());
                    assert!(
                        dropped["spans"]
                            .as_array()
                            .expect("retained request span")
                            .iter()
                            .any(|span| span["request_id"] == "original-multipart-request")
                    );
                    let manager = Arc::new(FastObjectLockManager::new());
                    let stale_client = LocalClient::with_manager(Arc::new(GlobalLockManager::Enabled(manager.clone())));
                    let stale_request = LockRequest::new(
                        ObjectKey::new("bucket", "externally-released"),
                        LockType::Exclusive,
                        "stale-owner".to_owned(),
                    );
                    assert!(
                        stale_client
                            .acquire_lock(&stale_request)
                            .await
                            .expect("real stale owner")
                            .success
                    );
                    assert_eq!(manager.force_unlock(&stale_request.resource), 1);
                    let mut successor = manager
                        .acquire_write_lock(stale_request.resource.clone(), "successor-owner")
                        .await
                        .expect("real successor guard");
                    assert!(
                        stale_client
                            .release(&stale_request.lock_id)
                            .await
                            .expect("retire stale client record")
                    );
                    assert_eq!(
                        manager
                            .get_lock_info(&stale_request.resource)
                            .expect("successor remains locked")
                            .owner
                            .as_ref(),
                        "successor-owner"
                    );
                    assert_eq!(
                        capture
                            .all_rows()
                            .iter()
                            .filter(
                                |row| row["fields"]["event"] == "namespace_lock_release" && row["fields"]["state"] == "released"
                            )
                            .count(),
                        4,
                        "a stale guard's failed physical release must not produce an ACK"
                    );
                    let successor_id = successor.guard_id();
                    assert!(successor.release(), "the actual successor release succeeds");
                    assert!(!successor.release(), "a duplicate explicit release cannot complete again");
                    drop(successor);
                    assert_eq!(
                        capture
                            .all_rows()
                            .iter()
                            .filter(|row| row["fields"]["event"] == "namespace_lock_release"
                                && row["fields"]["state"] == "released"
                                && row["fields"]["physical_guard_id"] == successor_id)
                            .count(),
                        1,
                        "explicit release followed by Drop records one physical completion"
                    );
                });
            });
        });
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
