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
use std::time::Duration;

use chrono::Utc;
use rand::RngExt as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
    inventory: Option<InventoryRuntime>,
}

impl HeartbeatRuntime {
    pub fn status(&self) -> watch::Receiver<HeartbeatStatus> {
        self.status.clone()
    }

    pub(crate) fn with_inventory(mut self, inventory: Option<InventoryRuntime>) -> Self {
        self.inventory = inventory;
        self
    }

    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(inventory) = self.inventory.as_ref() {
            inventory.shutdown.cancel();
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if let Some(inventory) = self.inventory.take() {
            inventory.shutdown().await;
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
    let sender = HeartbeatSender::new(config.clone())?;
    let store = HeartbeatStateStore::new(config.state_path.clone());
    let lock = store.try_runtime_lock()?;
    let schedule = config.schedule;
    let shutdown = parent_shutdown.child_token();
    let task_shutdown = shutdown.clone();
    let (status_tx, status_rx) = watch::channel(HeartbeatStatus::Starting);
    let task = tokio::spawn(async move {
        let _lock = lock;
        let mut backoff = schedule.initial_backoff;
        loop {
            if task_shutdown.is_cancelled() {
                break;
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
        inventory: None,
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
    let store = InventoryStateStore::from_heartbeat_path(&config.state_path)?;
    let lock = store.try_runtime_lock()?;
    let sender = InventorySender::new(config)?;
    let shutdown = parent_shutdown.child_token();
    let task_shutdown = shutdown.clone();
    let (status_tx, status_rx) = watch::channel(InventoryStatus::Starting);
    let task = tokio::spawn(async move {
        let _lock = lock;
        let mut backoff = retry_schedule.initial_backoff;
        loop {
            if task_shutdown.is_cancelled() {
                break;
            }
            let pending = match store.pending().await {
                Ok(Some(pending)) => pending,
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
            let delivery = match cancellable(&task_shutdown, sender.send(&pending)).await {
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
                InventoryDelivery::AuthenticationStopped { status, reason } => {
                    let _ = status_tx.send(InventoryStatus::AuthenticationStopped { status, reason });
                    return;
                }
                InventoryDelivery::Rejected { status, reason } => {
                    let suffix = reason.map_or_else(String::new, |reason| format!("; reason={reason}"));
                    let _ = status_tx.send(InventoryStatus::Failed {
                        reason: format!("Connect rejected inventory with HTTP {status}{suffix}"),
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
    }))
}

fn failed(status: &watch::Sender<HeartbeatStatus>, error: HeartbeatError) {
    let _ = status.send(HeartbeatStatus::Failed {
        reason: error.to_string(),
    });
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
    async fn heartbeat_shutdown_cancels_inventory_before_waiting_for_heartbeat() {
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
        let runtime = HeartbeatRuntime {
            shutdown: heartbeat_shutdown,
            status: heartbeat_status,
            task: Some(heartbeat_task),
            inventory: Some(InventoryRuntime {
                shutdown: inventory_shutdown,
                status: inventory_status,
                task: Some(inventory_task),
            }),
        };

        let shutdown = tokio::spawn(runtime.shutdown());
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
}
