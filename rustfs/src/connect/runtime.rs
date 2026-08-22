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
    }))
}

fn failed(status: &watch::Sender<HeartbeatStatus>, error: HeartbeatError) {
    let _ = status.send(HeartbeatStatus::Failed {
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
