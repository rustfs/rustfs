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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rand::RngExt as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::client::{ClientError, ConnectClient, ConnectConfig, RotationAttempt};
use super::config::HeartbeatConfig;
use super::heartbeat::{CoarseNodeSummary, Delivery, HeartbeatError, HeartbeatSender, HeartbeatStateStore, HeartbeatStatus};
use super::inventory::{
    InventoryDelivery, InventoryError, InventorySchedule, InventorySender, InventorySnapshot, InventoryStateStore,
    InventoryStatus,
};

pub struct HeartbeatRuntime {
    shutdown: CancellationToken,
    status: watch::Receiver<HeartbeatStatus>,
    task: Option<JoinHandle<()>>,
}

impl HeartbeatRuntime {
    pub fn status(&self) -> watch::Receiver<HeartbeatStatus> {
        self.status.clone()
    }

    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for HeartbeatRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub struct InventoryRuntime {
    shutdown: CancellationToken,
    status: watch::Receiver<InventoryStatus>,
    task: Option<JoinHandle<()>>,
    _lock: Arc<std::fs::File>,
}

impl InventoryRuntime {
    pub fn status(&self) -> watch::Receiver<InventoryStatus> {
        self.status.clone()
    }

    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for InventoryRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub(crate) async fn shutdown_connect_runtimes(heartbeat: Option<HeartbeatRuntime>, inventory: Option<InventoryRuntime>) {
    let heartbeat = async move {
        if let Some(runtime) = heartbeat {
            runtime.shutdown().await;
        }
    };
    let inventory = async move {
        if let Some(runtime) = inventory {
            runtime.shutdown().await;
        }
    };
    tokio::join!(heartbeat, inventory);
}

pub fn spawn_heartbeat_runtime<F>(
    config: Option<HeartbeatConfig>,
    parent_shutdown: &CancellationToken,
    sample: F,
) -> Result<Option<HeartbeatRuntime>, HeartbeatError>
where
    F: Fn() -> CoarseNodeSummary + Send + Sync + 'static,
{
    let Some(config) = config else {
        return Ok(None);
    };
    if !config.transport_enabled() {
        return Ok(None);
    }
    let sender = HeartbeatSender::new(config.clone())?;
    let rotation = ConnectClient::new(ConnectConfig {
        endpoint: &config.endpoint,
        root_ca_pem: &config.root_ca_pem,
        timeout: config.schedule.timeout,
    })
    .map_err(rotation_failure)?;
    let identity_store = config.identity_store.clone();
    let credential_store = config.credential_store.clone();
    let store = HeartbeatStateStore::new(config.state_path.clone());
    let lock = store.try_runtime_lock()?;
    let schedule = config.schedule;
    let shutdown = parent_shutdown.child_token();
    let task_shutdown = shutdown.clone();
    let (status_tx, status_rx) = watch::channel(HeartbeatStatus::Starting);
    let task = tokio::spawn(async move {
        let _lock = lock;
        let mut backoff = schedule.initial_backoff;
        let mut rotation_backoff = schedule.initial_backoff;
        let mut rotation_retry_at = None;
        loop {
            if task_shutdown.is_cancelled() {
                break;
            }
            if rotation_retry_at.is_none_or(|retry_at| Instant::now() >= retry_at) {
                match cancellable(
                    &task_shutdown,
                    rotation.rotate_if_due_once(&identity_store, &credential_store, Utc::now().timestamp()),
                )
                .await
                {
                    Some(Ok(RotationAttempt::Completed(_))) => {
                        rotation_backoff = schedule.initial_backoff;
                        rotation_retry_at = None;
                    }
                    Some(Ok(RotationAttempt::ReenrollmentPending)) => {}
                    Some(Ok(RotationAttempt::Unavailable { retry_after, .. })) => {
                        let delay = retry_after
                            .unwrap_or(rotation_backoff)
                            .clamp(schedule.initial_backoff, schedule.max_backoff);
                        rotation_backoff = rotation_backoff.saturating_mul(2).min(schedule.max_backoff);
                        rotation_retry_at = Some(Instant::now() + delay);
                    }
                    Some(Err(ClientError::Transport(_))) => {
                        rotation_retry_at = Some(Instant::now() + rotation_backoff);
                        rotation_backoff = rotation_backoff.saturating_mul(2).min(schedule.max_backoff);
                    }
                    Some(Err(ClientError::AccessRevoked { status, reason })) => {
                        let _ = status_tx.send(HeartbeatStatus::AuthenticationStopped {
                            status: status.as_u16(),
                            reason,
                        });
                        return;
                    }
                    Some(Err(error)) => return failed(&status_tx, rotation_failure(error)),
                    None => break,
                }
            }
            let pending = match store.prepare(sample(), Utc::now()).await {
                Ok(pending) => pending,
                Err(error) => return failed(&status_tx, error),
            };
            let delivery = match cancellable(&task_shutdown, sender.send(&pending)).await {
                Some(Ok(delivery)) => delivery,
                Some(Err(error)) => return failed(&status_tx, error),
                None => break,
            };
            let delay = match delivery {
                Delivery::Accepted { server_time } => {
                    if let Err(error) = store.mark_accepted(&pending).await {
                        return failed(&status_tx, error);
                    }
                    backoff = schedule.initial_backoff;
                    let _ = status_tx.send(HeartbeatStatus::Online { server_time });
                    schedule.cadence.saturating_add(jitter(schedule.jitter))
                }
                Delivery::Retry { retry_after } => {
                    let delay = retry_after
                        .unwrap_or(backoff)
                        .clamp(schedule.initial_backoff, schedule.max_backoff);
                    backoff = backoff.saturating_mul(2).min(schedule.max_backoff);
                    let _ = status_tx.send(HeartbeatStatus::BackingOff { delay });
                    delay
                }
                Delivery::AuthenticationStopped { status, reason } => {
                    let _ = status_tx.send(HeartbeatStatus::AuthenticationStopped { status, reason });
                    return;
                }
                Delivery::Rejected { status, reason } => {
                    let suffix = reason.map_or_else(String::new, |reason| format!("; reason={reason}"));
                    let _ = status_tx.send(HeartbeatStatus::Failed {
                        reason: format!("Connect rejected heartbeat with HTTP {status}{suffix}"),
                    });
                    return;
                }
            };
            if sleep_or_cancel(&task_shutdown, delay).await {
                break;
            }
        }
        let _ = status_tx.send(HeartbeatStatus::Stopped);
    });
    Ok(Some(HeartbeatRuntime {
        shutdown,
        status: status_rx,
        task: Some(task),
    }))
}

pub fn spawn_inventory_runtime<F, Fut>(
    config: Option<HeartbeatConfig>,
    schedule: InventorySchedule,
    parent_shutdown: &CancellationToken,
    sample: F,
) -> Result<Option<InventoryRuntime>, InventoryError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<InventorySnapshot, InventoryError>> + Send + 'static,
{
    let Some(config) = config else {
        return Ok(None);
    };
    if schedule.cadence.is_zero() || schedule.jitter > schedule.cadence {
        return Err(InventoryError::Schedule);
    }
    let retry_schedule = config.schedule;
    let state_root = config.state_root().ok_or(InventoryError::StatePath)?;
    let store = InventoryStateStore::from_state_root(state_root)?;
    let lock = Arc::new(store.try_runtime_lock()?);
    let sender = if config.transport_enabled() {
        Some(InventorySender::new(config)?)
    } else {
        None
    };
    let shutdown = parent_shutdown.child_token();
    let task_shutdown = shutdown.clone();
    let (status_tx, status_rx) = watch::channel(InventoryStatus::Starting);
    let task_lock = lock.clone();
    let task = tokio::spawn(async move {
        let _lock = task_lock;
        let mut backoff = retry_schedule.initial_backoff;
        loop {
            if task_shutdown.is_cancelled() {
                break;
            }
            let pending = match if sender.is_some() { store.pending().await } else { Ok(None) } {
                Ok(Some((pending, captured_at))) => {
                    if let Err(error) = store
                        .ensure_latest(pending.snapshot().clone(), captured_at, task_shutdown.clone())
                        .await
                    {
                        if matches!(error, InventoryError::Cancelled) && task_shutdown.is_cancelled() {
                            break;
                        }
                        return failed_inventory(&status_tx, error);
                    }
                    pending
                }
                Ok(None) => {
                    let snapshot = match cancellable(&task_shutdown, sample()).await {
                        Some(Ok(snapshot)) => snapshot,
                        Some(Err(InventoryError::SnapshotIncomplete { .. })) => {
                            let delay = backoff;
                            backoff = backoff.saturating_mul(2).min(retry_schedule.max_backoff);
                            let _ = status_tx.send(InventoryStatus::BackingOff { delay });
                            if sleep_or_cancel(&task_shutdown, delay).await {
                                break;
                            }
                            continue;
                        }
                        Some(Err(error)) => return failed_inventory(&status_tx, error),
                        None => break,
                    };
                    let content_hash = match snapshot.content_hash() {
                        Ok(content_hash) => content_hash,
                        Err(error) => return failed_inventory(&status_tx, error),
                    };
                    let captured_at = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                    if let Err(error) = store
                        .publish_latest(snapshot.clone(), captured_at, task_shutdown.clone())
                        .await
                    {
                        if matches!(error, InventoryError::Cancelled) && task_shutdown.is_cancelled() {
                            break;
                        }
                        return failed_inventory(&status_tx, error);
                    }
                    if task_shutdown.is_cancelled() {
                        break;
                    }
                    if sender.is_none() {
                        backoff = retry_schedule.initial_backoff;
                        let _ = status_tx.send(InventoryStatus::Unchanged { content_hash });
                        if sleep_or_cancel(&task_shutdown, schedule.cadence.saturating_add(jitter(schedule.jitter))).await {
                            break;
                        }
                        continue;
                    }
                    match store.prepare(snapshot).await {
                        Ok(Some(pending)) => pending,
                        Ok(None) => {
                            backoff = retry_schedule.initial_backoff;
                            let _ = status_tx.send(InventoryStatus::Unchanged { content_hash });
                            if sleep_or_cancel(&task_shutdown, schedule.cadence.saturating_add(jitter(schedule.jitter))).await {
                                break;
                            }
                            continue;
                        }
                        Err(error) => return failed_inventory(&status_tx, error),
                    }
                }
                Err(error) => return failed_inventory(&status_tx, error),
            };
            let delivery = match cancellable(
                &task_shutdown,
                sender
                    .as_ref()
                    .expect("sender exists when delivery state is prepared")
                    .send(&pending),
            )
            .await
            {
                Some(Ok(delivery)) => delivery,
                Some(Err(error)) => return failed_inventory(&status_tx, error),
                None => break,
            };
            let delay = match delivery {
                InventoryDelivery::Accepted {
                    content_hash,
                    received_at,
                } => {
                    if let Err(error) = store.mark_accepted(&pending).await {
                        return failed_inventory(&status_tx, error);
                    }
                    backoff = retry_schedule.initial_backoff;
                    let _ = status_tx.send(InventoryStatus::Online {
                        content_hash,
                        received_at,
                    });
                    schedule.cadence.saturating_add(jitter(schedule.jitter))
                }
                InventoryDelivery::Retry { retry_after } => {
                    let delay = retry_after
                        .unwrap_or(backoff)
                        .clamp(retry_schedule.initial_backoff, retry_schedule.max_backoff);
                    backoff = backoff.saturating_mul(2).min(retry_schedule.max_backoff);
                    let _ = status_tx.send(InventoryStatus::BackingOff { delay });
                    delay
                }
                InventoryDelivery::AuthenticationStopped { status } => {
                    let _ = status_tx.send(InventoryStatus::AuthenticationStopped { status, reason: None });
                    return;
                }
                InventoryDelivery::Rejected { status } => {
                    let _ = status_tx.send(InventoryStatus::Failed {
                        reason: format!("connect_inventory_rejected_http_{status}"),
                    });
                    return;
                }
            };
            if sleep_or_cancel(&task_shutdown, delay).await {
                break;
            }
        }
        let _ = status_tx.send(InventoryStatus::Stopped);
    });
    Ok(Some(InventoryRuntime {
        shutdown,
        status: status_rx,
        task: Some(task),
        _lock: lock,
    }))
}

fn failed(status: &watch::Sender<HeartbeatStatus>, error: HeartbeatError) {
    let _ = status.send(HeartbeatStatus::Failed {
        reason: error.to_string(),
    });
}

fn rotation_failure(error: ClientError) -> HeartbeatError {
    match error {
        ClientError::Endpoint => HeartbeatError::Endpoint,
        ClientError::RootCertificate => HeartbeatError::RootCertificate,
        ClientError::NotRegistered => HeartbeatError::NotRegistered,
        ClientError::IdentityMissing => HeartbeatError::IdentityMissing,
        ClientError::CredentialExpired | ClientError::CredentialNotYetValid => HeartbeatError::CredentialExpired,
        ClientError::IdentityCertificate => HeartbeatError::IdentityCertificate,
        ClientError::Identity(error) => HeartbeatError::Identity(error),
        ClientError::IdentityStore(error) => HeartbeatError::IdentityStore(error),
        ClientError::CredentialStore(error) => HeartbeatError::CredentialStore(error),
        ClientError::Credential(error) => HeartbeatError::CredentialValidation(error),
        ClientError::Transport(error) => HeartbeatError::Transport(error),
        ClientError::ResponseTooLarge => HeartbeatError::ResponseTooLarge,
        ClientError::PendingRegistration | ClientError::PendingRotation => HeartbeatError::StateConflict,
        ClientError::AccessRevoked { .. }
        | ClientError::Rejected { .. }
        | ClientError::Unavailable { .. }
        | ClientError::Response => HeartbeatError::Response,
    }
}

pub(crate) fn heartbeat_failure_reason(error: &HeartbeatError) -> &'static str {
    use super::registration::CredentialValidationError;

    match error {
        HeartbeatError::Endpoint => "connect_heartbeat_endpoint",
        HeartbeatError::RootCertificate => "connect_heartbeat_root_certificate",
        HeartbeatError::Schedule => "connect_heartbeat_schedule",
        HeartbeatError::NotRegistered => "connect_heartbeat_not_registered",
        HeartbeatError::IdentityMissing => "connect_heartbeat_identity_missing",
        HeartbeatError::IdentityCertificate => "connect_heartbeat_identity_certificate",
        HeartbeatError::CredentialName => "connect_heartbeat_credential_name",
        HeartbeatError::CredentialExpired => "connect_heartbeat_credential_expired",
        HeartbeatError::NodeSummary => "connect_heartbeat_node_summary",
        HeartbeatError::SequenceExhausted => "connect_heartbeat_sequence_exhausted",
        HeartbeatError::AlreadyRunning => "connect_heartbeat_already_running",
        HeartbeatError::StateConflict => "connect_heartbeat_state_conflict",
        HeartbeatError::StateIo { .. } => "connect_heartbeat_state_io",
        HeartbeatError::StateInvalid { .. } => "connect_heartbeat_state_invalid",
        HeartbeatError::StateCorrupt { .. } => "connect_heartbeat_state_corrupt",
        #[cfg(unix)]
        HeartbeatError::StatePermissions { .. } => "connect_heartbeat_state_permissions",
        HeartbeatError::ResponseTooLarge => "connect_heartbeat_response_too_large",
        HeartbeatError::Response => "connect_heartbeat_response",
        HeartbeatError::Url(_) => "connect_heartbeat_url",
        HeartbeatError::Transport(_) => "connect_heartbeat_transport",
        HeartbeatError::Identity(_) => "connect_heartbeat_identity",
        HeartbeatError::IdentityStore(_) => "connect_heartbeat_identity_store",
        HeartbeatError::CredentialStore(_) => "connect_heartbeat_credential_store",
        HeartbeatError::CredentialValidation(error) => match error {
            CredentialValidationError::Certificate => "connect_heartbeat_credential_certificate",
            CredentialValidationError::Chain => "connect_heartbeat_credential_chain",
            CredentialValidationError::Identity => "connect_heartbeat_credential_identity",
            CredentialValidationError::Key => "connect_heartbeat_credential_key",
            CredentialValidationError::Validity => "connect_heartbeat_credential_validity",
            CredentialValidationError::CertificateRequest => "connect_heartbeat_credential_request",
            CredentialValidationError::RotationTranscript => "connect_heartbeat_credential_rotation_transcript",
        },
    }
}

fn failed_inventory(status: &watch::Sender<InventoryStatus>, error: InventoryError) {
    let _ = status.send(InventoryStatus::Failed {
        reason: error.to_string(),
    });
}

fn jitter(maximum: Duration) -> Duration {
    if maximum.is_zero() {
        Duration::ZERO
    } else {
        maximum.mul_f64(rand::rng().random_range(0.0..=1.0))
    }
}

async fn cancellable<T>(shutdown: &CancellationToken, future: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => None,
        value = future => Some(value),
    }
}

async fn sleep_or_cancel(shutdown: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn state_only_configuration_does_not_start_heartbeat() {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory");
        let config = HeartbeatConfig::state_only(temp.path().to_path_buf());
        let shutdown = CancellationToken::new();

        assert!(
            spawn_heartbeat_runtime(Some(config), &shutdown, || { CoarseNodeSummary::new(1, 0, 0).expect("summary") })
                .expect("disabled transport")
                .is_none()
        );
    }

    #[test]
    fn runtimes_expose_only_stable_machine_reasons() {
        let error = HeartbeatError::StateIo {
            path: std::path::PathBuf::from("/private/connect/state.json"),
            source: std::io::Error::other("transport.internal"),
        };
        assert_eq!(heartbeat_failure_reason(&error), "connect_heartbeat_state_io");
        assert_eq!(
            heartbeat_failure_reason(&HeartbeatError::StateCorrupt {
                path: std::path::PathBuf::from("/private/connect/state.json"),
            }),
            "connect_heartbeat_state_corrupt"
        );
        assert_eq!(
            heartbeat_failure_reason(&HeartbeatError::CredentialExpired),
            "connect_heartbeat_credential_expired"
        );
        let pending_rotation = rotation_failure(ClientError::PendingRotation);
        assert!(matches!(pending_rotation, HeartbeatError::StateConflict));
        assert_eq!(heartbeat_failure_reason(&pending_rotation), "connect_heartbeat_state_conflict");
        let oversized_rotation_response = rotation_failure(ClientError::ResponseTooLarge);
        assert!(matches!(oversized_rotation_response, HeartbeatError::ResponseTooLarge));
        assert_eq!(
            heartbeat_failure_reason(&oversized_rotation_response),
            "connect_heartbeat_response_too_large"
        );
        assert_eq!(
            heartbeat_failure_reason(&HeartbeatError::CredentialValidation(
                super::super::registration::CredentialValidationError::Identity
            )),
            "connect_heartbeat_credential_identity"
        );
        assert_eq!(
            heartbeat_failure_reason(&HeartbeatError::CredentialValidation(
                super::super::registration::CredentialValidationError::Key
            )),
            "connect_heartbeat_credential_key"
        );
    }

    #[tokio::test]
    async fn unified_shutdown_cancels_inventory_before_waiting_for_heartbeat() {
        let heartbeat_shutdown = CancellationToken::new();
        let inventory_shutdown = CancellationToken::new();
        let task_inventory_shutdown = inventory_shutdown.clone();
        let (release_heartbeat, wait_for_release) = tokio::sync::oneshot::channel();
        let (inventory_stopped, stopped) = tokio::sync::oneshot::channel();
        let (_, heartbeat_status) = watch::channel(HeartbeatStatus::Starting);
        let (_, inventory_status) = watch::channel(InventoryStatus::Starting);
        let heartbeat_task = tokio::spawn(async move {
            let _ = wait_for_release.await;
        });
        let inventory_task = tokio::spawn(async move {
            task_inventory_shutdown.cancelled().await;
            let _ = inventory_stopped.send(());
        });
        let inventory_lock = Arc::new(tempfile::tempfile().expect("inventory runtime lock"));
        let heartbeat = HeartbeatRuntime {
            shutdown: heartbeat_shutdown,
            status: heartbeat_status,
            task: Some(heartbeat_task),
        };
        let inventory = InventoryRuntime {
            shutdown: inventory_shutdown,
            status: inventory_status,
            task: Some(inventory_task),
            _lock: inventory_lock,
        };

        let shutdown = tokio::spawn(shutdown_connect_runtimes(Some(heartbeat), Some(inventory)));
        tokio::time::timeout(Duration::from_millis(250), stopped)
            .await
            .expect("inventory cancellation must not wait for heartbeat")
            .expect("inventory task reports cancellation");
        release_heartbeat.send(()).expect("release heartbeat task");
        tokio::time::timeout(Duration::from_millis(250), shutdown)
            .await
            .expect("runtime shutdown")
            .expect("shutdown task");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dropping_inventory_handle_keeps_the_lock_until_its_task_exits() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory");
        let state = temp.path().join("state");
        std::fs::create_dir(&state).expect("state root");
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).expect("private state root");
        let store = InventoryStateStore::from_state_root(&state).expect("inventory store");
        let lock = Arc::new(store.try_runtime_lock().expect("runtime lock"));
        let task_lock = lock.clone();
        let (release, released) = tokio::sync::oneshot::channel();
        let (finished, task_finished) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = released.await;
            drop(task_lock);
            let _ = finished.send(());
        });
        let (_, status) = watch::channel(InventoryStatus::Starting);
        let runtime = InventoryRuntime {
            shutdown: CancellationToken::new(),
            status,
            task: Some(task),
            _lock: lock,
        };

        // Directory link counts change when unrelated sibling directories
        // come and go; that must not look like an anchored path replacement.
        std::fs::create_dir(temp.path().join("unrelated-sibling")).expect("unrelated sibling directory");
        drop(runtime);
        assert!(matches!(store.try_runtime_lock(), Err(InventoryError::AlreadyRunning)));
        release.send(()).expect("release inventory task");
        task_finished.await.expect("inventory task finished");
        store.try_runtime_lock().expect("lock after task exit");
    }
}
