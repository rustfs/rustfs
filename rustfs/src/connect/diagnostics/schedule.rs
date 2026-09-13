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

//! Consent-bound scheduling for the small set of diagnostic producers compiled into RustFS.

use std::fs;
use std::future::Future;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::connect::InventorySnapshot;
use crate::connect::environment::{
    ENVIRONMENT_CAPABILITY, ENVIRONMENT_SCHEMA_VERSION, EnvironmentCollectionRequest, EnvironmentInventory, collect_environment,
};
use crate::connect::inventory::InventoryStateStore;

const POLICY_CAPABILITY: &str = "diagnostics.policy.v1";
const ENVIRONMENT_TOOL_ID: &str = "inventory.environment";
const MIN_INTERVAL_SECONDS: u64 = 300;
const MAX_INTERVAL_SECONDS: u64 = 86_400;
const MAX_RETENTION_DAYS: u16 = 14;
const MAX_ATTEMPTS: u8 = 3;
const INITIAL_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(4);
const MAX_RESULT_BYTES: usize = 64 * 1024;
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticCollectionPolicy {
    policy_name: Option<String>,
    revision: u64,
    support_state: SupportState,
    desired_state: DesiredState,
    reason_code: Option<ReasonCode>,
    tool_ids: Vec<String>,
    interval_seconds: Option<u64>,
    scope: Scope,
    retention_days: Option<u16>,
    consent_reference: Option<String>,
    consent_expires_at: Option<String>,
}

impl DiagnosticCollectionPolicy {
    pub(crate) fn policy_sync_capability() -> &'static str {
        POLICY_CAPABILITY
    }

    pub(crate) fn stopped() -> Self {
        Self {
            policy_name: None,
            revision: 0,
            support_state: SupportState::Unsupported,
            desired_state: DesiredState::Stopped,
            reason_code: Some(ReasonCode::PolicyCapabilityUnsupported),
            tool_ids: Vec::new(),
            interval_seconds: None,
            scope: Scope::Cluster,
            retention_days: None,
            consent_reference: None,
            consent_expires_at: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), DiagnosticScheduleError> {
        if self.policy_name.as_ref().is_some_and(|value| value.len() > 512)
            || self.tool_ids.len() > 19
            || self.tool_ids.iter().any(|value| value.is_empty() || value.len() > 64)
            || self
                .consent_reference
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 512)
        {
            return Err(DiagnosticScheduleError::Policy);
        }
        if self.desired_state == DesiredState::Stopped {
            return Ok(());
        }
        let interval = self.interval_seconds.ok_or(DiagnosticScheduleError::Policy)?;
        let retention = self.retention_days.ok_or(DiagnosticScheduleError::Policy)?;
        let expires_at = self.consent_expiry()?;
        if self.support_state != SupportState::Supported
            || self.reason_code.is_some()
            || !(MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&interval)
            || !(1..=MAX_RETENTION_DAYS).contains(&retention)
            || self.policy_name.is_none()
            || self.consent_reference.is_none()
            || self.tool_ids.is_empty()
            || expires_at <= Utc::now()
        {
            return Err(DiagnosticScheduleError::Policy);
        }
        Ok(())
    }

    fn should_run(&self) -> bool {
        self.support_state == SupportState::Supported && self.desired_state == DesiredState::Running
    }

    fn consent_expiry(&self) -> Result<DateTime<Utc>, DiagnosticScheduleError> {
        self.consent_expires_at
            .as_deref()
            .ok_or(DiagnosticScheduleError::Policy)
            .and_then(parse_time)
    }

    fn fingerprint(&self) -> Result<String, DiagnosticScheduleError> {
        let bytes = serde_json::to_vec(self).map_err(|_| DiagnosticScheduleError::Policy)?;
        Ok(hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum SupportState {
    Supported,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DesiredState {
    Running,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ReasonCode {
    PolicyCapabilityUnsupported,
    ToolCapabilityUnsupported,
    Disabled,
    ConsentInactive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Scope {
    Cluster,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticReceipt {
    pub receipt_id: String,
    pub policy_revision: u64,
    pub tool_id: String,
    pub interval_started_at: String,
    pub completed_at: String,
    pub outcome: ReceiptOutcome,
    pub attempt_count: u8,
    pub reason: Option<String>,
    pub result_sha256: Option<String>,
    pub result_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReceiptOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticScheduleStatus {
    Waiting,
    Running {
        policy_revision: u64,
        tool_id: String,
        attempt: u8,
    },
    Receipt(DiagnosticReceipt),
    Failed {
        reason: String,
    },
    Stopped,
}

#[derive(Debug, Error)]
pub enum DiagnosticScheduleError {
    #[error("connect_diagnostic_policy_invalid")]
    Policy,
    #[error("connect_diagnostic_state_io")]
    StateIo(#[source] io::Error),
    #[error("connect_diagnostic_state_invalid")]
    StateInvalid(#[source] serde_json::Error),
    #[error("connect_diagnostic_state_corrupt")]
    StateCorrupt,
    #[error("connect_diagnostic_inventory_unavailable")]
    InventoryUnavailable,
    #[error("connect_diagnostic_collection_failed")]
    CollectionFailed,
    #[error("connect_diagnostic_result_too_large")]
    ResultTooLarge,
    #[error("connect_diagnostic_cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ScheduleState {
    policy_revision: Option<u64>,
    policy_fingerprint: Option<String>,
    next_due_at: Option<String>,
    active_interval_started_at: Option<String>,
    last_receipt: Option<DiagnosticReceipt>,
}

#[derive(Clone)]
struct StateStore {
    path: PathBuf,
    gate: Arc<Mutex<()>>,
}

impl StateStore {
    fn new(state_root: &Path) -> Self {
        Self {
            path: state_root.join("diagnostics/schedule.json"),
            gate: Arc::new(Mutex::new(())),
        }
    }

    async fn read(&self) -> Result<ScheduleState, DiagnosticScheduleError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || read_state(&path))
            .await
            .map_err(|error| DiagnosticScheduleError::StateIo(io::Error::other(error)))?
    }

    async fn write(&self, state: ScheduleState) -> Result<(), DiagnosticScheduleError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || write_state(&path, &state))
            .await
            .map_err(|error| DiagnosticScheduleError::StateIo(io::Error::other(error)))?
    }
}

type RunFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, DiagnosticScheduleError>> + Send>>;
type Runner = Arc<dyn Fn(CancellationToken) -> RunFuture + Send + Sync>;

pub struct DiagnosticScheduleRuntime {
    status: watch::Receiver<DiagnosticScheduleStatus>,
    task: JoinHandle<()>,
}

impl DiagnosticScheduleRuntime {
    pub fn status(&self) -> watch::Receiver<DiagnosticScheduleStatus> {
        self.status.clone()
    }

    pub async fn shutdown(self) {
        let _ = self.task.await;
    }
}

pub fn spawn_environment_schedule(
    state_root: &Path,
    policies: watch::Receiver<DiagnosticCollectionPolicy>,
    shutdown: CancellationToken,
) -> Result<DiagnosticScheduleRuntime, DiagnosticScheduleError> {
    let inventory_state_root = state_root.to_path_buf();
    let runner: Runner = Arc::new(move |cancel| {
        let inventory_state_root = inventory_state_root.clone();
        Box::pin(async move {
            let inventory = InventoryStateStore::from_state_root(&inventory_state_root)
                .map_err(|_| DiagnosticScheduleError::InventoryUnavailable)?;
            let result = run_environment_with_inventory(&inventory, &cancel).await?;
            serde_json::to_vec(&result).map_err(|_| DiagnosticScheduleError::CollectionFailed)
        })
    });
    Ok(spawn_schedule(StateStore::new(state_root), policies, shutdown, runner))
}

/// Run the only diagnostic producer currently compiled into RustFS once.
pub async fn run_local_environment_once(
    inventory: &InventorySnapshot,
    cancel: &CancellationToken,
) -> Result<EnvironmentInventory, DiagnosticScheduleError> {
    let request =
        EnvironmentCollectionRequest::negotiate(ENVIRONMENT_SCHEMA_VERSION, ENVIRONMENT_CAPABILITY, Duration::from_secs(30))
            .map_err(|_| DiagnosticScheduleError::CollectionFailed)?;
    collect_environment(inventory, request, cancel)
        .await
        .map_err(|error| match error {
            crate::connect::EnvironmentError::Cancelled => DiagnosticScheduleError::Cancelled,
            _ => DiagnosticScheduleError::CollectionFailed,
        })
}

async fn run_environment_with_inventory(
    inventory: &InventoryStateStore,
    cancel: &CancellationToken,
) -> Result<EnvironmentInventory, DiagnosticScheduleError> {
    let persisted = inventory
        .read_latest(Utc::now())
        .map_err(|_| DiagnosticScheduleError::InventoryUnavailable)?;
    run_local_environment_once(&persisted.snapshot, cancel).await
}

fn spawn_schedule(
    store: StateStore,
    mut policies: watch::Receiver<DiagnosticCollectionPolicy>,
    shutdown: CancellationToken,
    runner: Runner,
) -> DiagnosticScheduleRuntime {
    let (status_tx, status_rx) = watch::channel(DiagnosticScheduleStatus::Waiting);
    let task = tokio::spawn(async move {
        if let Err(error) = run_collection_schedule(&store, &mut policies, &shutdown, &runner, &status_tx).await {
            let _ = status_tx.send(DiagnosticScheduleStatus::Failed {
                reason: error.to_string(),
            });
        }
        let _ = status_tx.send(DiagnosticScheduleStatus::Stopped);
    });
    DiagnosticScheduleRuntime { status: status_rx, task }
}

async fn run_collection_schedule(
    store: &StateStore,
    policies: &mut watch::Receiver<DiagnosticCollectionPolicy>,
    shutdown: &CancellationToken,
    runner: &Runner,
    status: &watch::Sender<DiagnosticScheduleStatus>,
) -> Result<(), DiagnosticScheduleError> {
    let mut state = store.read().await?;
    if let Some(started_at) = state.active_interval_started_at.take() {
        let receipt = receipt(
            state.policy_revision.unwrap_or_default(),
            ENVIRONMENT_TOOL_ID,
            started_at,
            ReceiptOutcome::Failed,
            1,
            Some("producer_restarted_during_collection".to_owned()),
            None,
        );
        state.last_receipt = Some(receipt.clone());
        store.write(state.clone()).await?;
        let _ = status.send(DiagnosticScheduleStatus::Receipt(receipt));
    }

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let policy = policies.borrow().clone();
        if let Err(error) = policy.validate()
            && policy.should_run()
        {
            return Err(error);
        }
        if !policy.should_run() {
            state.next_due_at = None;
            state.active_interval_started_at = None;
            store.write(state.clone()).await?;
            let _ = status.send(DiagnosticScheduleStatus::Waiting);
            if wait_for_policy(policies, shutdown).await {
                break;
            }
            continue;
        }
        let fingerprint = policy.fingerprint()?;
        match state.policy_revision {
            Some(revision) if revision > policy.revision => {
                if wait_for_policy(policies, shutdown).await {
                    break;
                }
                continue;
            }
            Some(revision) if revision == policy.revision && state.policy_fingerprint.as_deref() != Some(&fingerprint) => {
                return Err(DiagnosticScheduleError::StateCorrupt);
            }
            Some(revision) if revision == policy.revision => {}
            _ => {
                state.policy_revision = Some(policy.revision);
                state.policy_fingerprint = Some(fingerprint);
                state.next_due_at = Some(now_string());
                store.write(state.clone()).await?;
            }
        }
        let due = state
            .next_due_at
            .as_deref()
            .ok_or(DiagnosticScheduleError::StateCorrupt)
            .and_then(parse_time)?;
        let expiry = policy.consent_expiry()?;
        if due >= expiry {
            state.next_due_at = None;
            store.write(state.clone()).await?;
            if wait_for_policy(policies, shutdown).await {
                break;
            }
            continue;
        }
        let now = Utc::now();
        if due > now {
            let wait = (due - now).to_std().map_err(|_| DiagnosticScheduleError::Policy)?;
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                changed = policies.changed() => if changed.is_err() { break; },
                () = tokio::time::sleep(wait) => {},
            }
            continue;
        }
        let tool_id = match policy.tool_ids.as_slice() {
            [tool_id] if tool_id == ENVIRONMENT_TOOL_ID => tool_id.clone(),
            _ => return Err(DiagnosticScheduleError::Policy),
        };
        let interval_started_at = due.to_rfc3339_opts(SecondsFormat::Secs, true);
        state.active_interval_started_at = Some(interval_started_at.clone());
        state.next_due_at = Some(
            (due + chrono::Duration::seconds(policy.interval_seconds.ok_or(DiagnosticScheduleError::Policy)? as i64))
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        );
        store.write(state.clone()).await?;

        let mut attempt = 1;
        let mut retry = INITIAL_RETRY;
        let receipt = loop {
            let _ = status.send(DiagnosticScheduleStatus::Running {
                policy_revision: policy.revision,
                tool_id: tool_id.clone(),
                attempt,
            });
            let cancel = shutdown.child_token();
            let run = runner(cancel.clone());
            let result = tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    cancel.cancel();
                    Err(DiagnosticScheduleError::Cancelled)
                }
                changed = policies.changed() => {
                    let _ = changed;
                    cancel.cancel();
                    Err(DiagnosticScheduleError::Cancelled)
                }
                () = tokio::time::sleep_until(tokio::time::Instant::now() + expiry.signed_duration_since(Utc::now()).to_std().unwrap_or_default()) => {
                    cancel.cancel();
                    Err(DiagnosticScheduleError::Cancelled)
                }
                result = run => result,
            };
            match result {
                Ok(bytes) if bytes.len() <= MAX_RESULT_BYTES => {
                    break receipt(
                        policy.revision,
                        &tool_id,
                        interval_started_at.clone(),
                        ReceiptOutcome::Succeeded,
                        attempt,
                        None,
                        Some(&bytes),
                    );
                }
                Ok(_) => {
                    break receipt(
                        policy.revision,
                        &tool_id,
                        interval_started_at.clone(),
                        ReceiptOutcome::Failed,
                        attempt,
                        Some(DiagnosticScheduleError::ResultTooLarge.to_string()),
                        None,
                    );
                }
                Err(DiagnosticScheduleError::Cancelled) => {
                    break receipt(
                        policy.revision,
                        &tool_id,
                        interval_started_at.clone(),
                        ReceiptOutcome::Cancelled,
                        attempt,
                        Some(DiagnosticScheduleError::Cancelled.to_string()),
                        None,
                    );
                }
                Err(_error) if attempt < MAX_ATTEMPTS => {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => {
                            break receipt(
                                policy.revision,
                                &tool_id,
                                interval_started_at.clone(),
                                ReceiptOutcome::Cancelled,
                                attempt,
                                Some(DiagnosticScheduleError::Cancelled.to_string()),
                                None,
                            );
                        }
                        changed = policies.changed() => {
                            let _ = changed;
                            break receipt(
                                policy.revision,
                                &tool_id,
                                interval_started_at.clone(),
                                ReceiptOutcome::Cancelled,
                                attempt,
                                Some(DiagnosticScheduleError::Cancelled.to_string()),
                                None,
                            );
                        }
                        () = tokio::time::sleep(retry) => {}
                    }
                    attempt += 1;
                    retry = retry.saturating_mul(2).min(MAX_RETRY);
                }
                Err(error) => {
                    break receipt(
                        policy.revision,
                        &tool_id,
                        interval_started_at.clone(),
                        ReceiptOutcome::Failed,
                        attempt,
                        Some(error.to_string()),
                        None,
                    );
                }
            }
        };
        state.active_interval_started_at = None;
        state.last_receipt = Some(receipt.clone());
        store.write(state.clone()).await?;
        let _ = status.send(DiagnosticScheduleStatus::Receipt(receipt));
    }
    Ok(())
}

async fn wait_for_policy(policies: &mut watch::Receiver<DiagnosticCollectionPolicy>, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => true,
        result = policies.changed() => result.is_err(),
    }
}

fn receipt(
    revision: u64,
    tool_id: &str,
    interval_started_at: String,
    outcome: ReceiptOutcome,
    attempt_count: u8,
    reason: Option<String>,
    bytes: Option<&[u8]>,
) -> DiagnosticReceipt {
    DiagnosticReceipt {
        receipt_id: Uuid::new_v4().to_string(),
        policy_revision: revision,
        tool_id: tool_id.to_owned(),
        interval_started_at,
        completed_at: now_string(),
        outcome,
        attempt_count,
        reason,
        result_sha256: bytes.map(|value| hex_simd::encode_to_string(Sha256::digest(value), hex_simd::AsciiCase::Lower)),
        result_bytes: bytes.map(<[u8]>::len),
    }
}

fn now_string() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, DiagnosticScheduleError> {
    if value.len() != 20 || !value.ends_with('Z') {
        return Err(DiagnosticScheduleError::Policy);
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| DiagnosticScheduleError::Policy)
}

fn read_state(path: &Path) -> Result<ScheduleState, DiagnosticScheduleError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(ScheduleState::default()),
        Err(error) => return Err(DiagnosticScheduleError::StateIo(error)),
    };
    if bytes.len() > MAX_RESULT_BYTES {
        return Err(DiagnosticScheduleError::StateCorrupt);
    }
    serde_json::from_slice(&bytes).map_err(DiagnosticScheduleError::StateInvalid)
}

fn write_state(path: &Path, state: &ScheduleState) -> Result<(), DiagnosticScheduleError> {
    let bytes = serde_json::to_vec(state).map_err(DiagnosticScheduleError::StateInvalid)?;
    let directory = path.parent().ok_or(DiagnosticScheduleError::StateCorrupt)?;
    fs::create_dir_all(directory).map_err(DiagnosticScheduleError::StateIo)?;
    let temp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(FILE_MODE);
    }
    let mut file = options.open(&temp).map_err(DiagnosticScheduleError::StateIo)?;
    let result = file
        .write_all(&bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| fs::rename(&temp, path))
        .map_err(DiagnosticScheduleError::StateIo);
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn policy(revision: u64, running: bool) -> DiagnosticCollectionPolicy {
        serde_json::from_value(json!({
            "policyName": if running { Some("organizations/0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70/clusters/0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81/diagnosticCollectionPreference") } else { None },
            "revision": revision,
            "supportState": "SUPPORTED",
            "desiredState": if running { "RUNNING" } else { "STOPPED" },
            "reasonCode": if running { None::<&str> } else { Some("DISABLED") },
            "toolIds": if running { vec!["inventory.environment"] } else { vec![] },
            "intervalSeconds": if running { Some(300) } else { None },
            "scope": "CLUSTER",
            "retentionDays": if running { Some(7) } else { None },
            "consentReference": if running { Some("organizations/0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70/diagnosticCollectionConsents/0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92") } else { None },
            "consentExpiresAt": if running { Some("2099-01-01T00:00:00Z") } else { None }
        }))
        .expect("policy")
    }

    async fn wait_running(status: &mut watch::Receiver<DiagnosticScheduleStatus>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !matches!(&*status.borrow(), DiagnosticScheduleStatus::Running { .. }) {
                status.changed().await.expect("schedule remains active");
            }
        })
        .await
        .expect("running");
    }

    fn blocking_runner() -> Runner {
        Arc::new(|cancel| {
            Box::pin(async move {
                cancel.cancelled().await;
                Err(DiagnosticScheduleError::Cancelled)
            })
        })
    }

    #[tokio::test]
    async fn disable_cancels_an_active_run_and_persists_receipt() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (policy_tx, policy_rx) = watch::channel(policy(1, true));
        let shutdown = CancellationToken::new();
        let mut runtime = spawn_schedule(StateStore::new(temp.path()), policy_rx, shutdown.clone(), blocking_runner());
        wait_running(&mut runtime.status).await;
        policy_tx.send(policy(2, false)).expect("disable");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = StateStore::new(temp.path()).read().await.expect("state");
                if state
                    .last_receipt
                    .is_some_and(|receipt| receipt.outcome == ReceiptOutcome::Cancelled)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancel receipt");
        let state = StateStore::new(temp.path()).read().await.expect("state");
        assert_eq!(state.last_receipt.expect("receipt").outcome, ReceiptOutcome::Cancelled);
        shutdown.cancel();
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_an_active_run_and_persists_receipt() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (_policy_tx, policy_rx) = watch::channel(policy(3, true));
        let shutdown = CancellationToken::new();
        let mut runtime = spawn_schedule(StateStore::new(temp.path()), policy_rx, shutdown.clone(), blocking_runner());
        wait_running(&mut runtime.status).await;
        shutdown.cancel();
        runtime.shutdown().await;
        let state = StateStore::new(temp.path()).read().await.expect("state");
        assert_eq!(state.last_receipt.expect("receipt").outcome, ReceiptOutcome::Cancelled);
    }
}
