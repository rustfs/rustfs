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

//! Background synthetic probe for the configured KMS backend.
//!
//! A health check that only reports "the backend answered" cannot distinguish a
//! usable KMS from one that is reachable but no longer returns material that
//! round-trips. Each probe round therefore generates a data key under a
//! dedicated key, decrypts the returned ciphertext, and compares the plaintext,
//! so a backend that answers without being usable becomes visible before object
//! traffic depends on it.
//!
//! The probe key uses a fixed reserved id ([`PROBE_KEY_ID`]) and is created only
//! when missing, so every node of a deployment converges on the same key and a
//! create that loses the race against another node is a success, not an error.
//!
//! Backends that cannot host such a key — single-key read-only backends, or
//! backends without data key generation — are reported as
//! [`ProbeResult::Unsupported`] exactly once and then left alone: an
//! unsupported backend is a configuration fact, not an outage, and must never
//! raise failure metrics or alerts.
//!
//! Results are published two ways: as a lock-free [`ProbeStatus`] snapshot that
//! readiness reporting can read without touching the backend, and as
//! `rustfs_kms_probe_*` metrics. As everywhere else in this crate, metric labels
//! carry only static outcome classes — never key identifiers, key material, or
//! ciphertext.

use crate::backends::KmsBackend;
use crate::error::KmsError;
use crate::types::{CreateKeyRequest, DecryptRequest, DescribeKeyRequest, GenerateDataKeyRequest, KeySpec, KeyUsage};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zeroize::Zeroize;

/// Reserved id of the key every probe round runs through.
///
/// Fixed rather than per-node or per-process: all nodes must converge on one
/// key instead of littering the backend with probe keys, and the reserved
/// `rustfs-internal-` prefix marks it as RustFS-owned for operators reviewing
/// the key list.
pub const PROBE_KEY_ID: &str = "rustfs-internal-kms-probe";

/// Description stored on the probe key when this node creates it.
const PROBE_KEY_DESCRIPTION: &str = "RustFS internal KMS health probe key; created and used only by the synthetic probe";

/// Default interval between probe rounds.
pub const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Floor for the configured interval. Probing faster buys no additional signal
/// and multiplies backend load by the size of the deployment.
pub const MIN_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// Probe interval in whole seconds; `0` disables the probe entirely.
pub const ENV_KMS_PROBE_INTERVAL_SECS: &str = "RUSTFS_KMS_PROBE_INTERVAL_SECS";

/// Result of the most recent probe round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeResult {
    /// No round has completed yet for this service version.
    Pending,
    /// The full generate/decrypt/compare cycle succeeded.
    Success,
    /// The backend cannot host the probe key; no further rounds run and no
    /// failures are recorded.
    Unsupported,
    /// The round failed with the given classification.
    Failure(ProbeFailureKind),
}

impl ProbeResult {
    /// Static metric label for this result.
    fn as_label(self) -> &'static str {
        match self {
            ProbeResult::Pending => "pending",
            ProbeResult::Success => "success",
            ProbeResult::Unsupported => "unsupported",
            ProbeResult::Failure(_) => "failure",
        }
    }
}

/// Stage of the round trip that failed, used as a static metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeFailureKind {
    /// The dedicated probe key could not be described or created.
    KeyProvisioning,
    /// Data key generation failed.
    Generate,
    /// The freshly generated ciphertext could not be decrypted.
    Decrypt,
    /// Decryption succeeded but returned material that differs from what was
    /// generated — the backend answers without round-tripping.
    Mismatch,
}

impl ProbeFailureKind {
    /// Static metric label for this failure class.
    fn as_label(self) -> &'static str {
        match self {
            ProbeFailureKind::KeyProvisioning => "key_provisioning",
            ProbeFailureKind::Generate => "generate",
            ProbeFailureKind::Decrypt => "decrypt",
            ProbeFailureKind::Mismatch => "mismatch",
        }
    }
}

/// Snapshot of what the probe has observed so far.
///
/// Consumed by readiness reporting, which owns the staleness and consecutive
/// failure thresholds; the probe itself only records what happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStatus {
    /// Result of the most recent completed round.
    pub result: ProbeResult,
    /// When the most recent round completed.
    pub last_round_at: Option<Instant>,
    /// When the most recent successful round completed.
    pub last_success_at: Option<Instant>,
    /// Wall-clock timestamp of the most recent success, in Unix seconds, for
    /// display and for the exported gauge. Use [`ProbeStatus::last_success_age`]
    /// for staleness decisions: it is monotonic and immune to clock steps.
    pub last_success_unix_secs: Option<i64>,
    /// Failed rounds since the last success; reset by any non-failure result.
    pub consecutive_failures: u32,
}

impl ProbeStatus {
    /// Initial snapshot published before the first round completes.
    fn pending() -> Self {
        Self {
            result: ProbeResult::Pending,
            last_round_at: None,
            last_success_at: None,
            last_success_unix_secs: None,
            consecutive_failures: 0,
        }
    }

    /// How long ago the most recent round completed.
    pub fn last_round_age(&self) -> Option<Duration> {
        self.last_round_at.map(|at| at.elapsed())
    }

    /// How long ago the most recent successful round completed.
    pub fn last_success_age(&self) -> Option<Duration> {
        self.last_success_at.map(|at| at.elapsed())
    }
}

/// Lock-free publication slot for [`ProbeStatus`].
///
/// Written by the single worker task and read by anyone; readers never block
/// the probe and the probe never blocks a readiness handler.
struct ProbeState {
    status: ArcSwap<ProbeStatus>,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            status: ArcSwap::from_pointee(ProbeStatus::pending()),
        }
    }

    fn status(&self) -> Arc<ProbeStatus> {
        self.status.load_full()
    }

    /// Fold one round result into the published snapshot.
    ///
    /// Safe as a plain load-then-store because the owning worker task is the
    /// only writer.
    fn publish(&self, result: ProbeResult) {
        let previous = self.status.load();
        let now = Instant::now();
        let (last_success_at, last_success_unix_secs) = match result {
            ProbeResult::Success => (Some(now), Some(jiff::Timestamp::now().as_second())),
            _ => (previous.last_success_at, previous.last_success_unix_secs),
        };
        let consecutive_failures = match result {
            ProbeResult::Failure(_) => previous.consecutive_failures.saturating_add(1),
            _ => 0,
        };
        self.status.store(Arc::new(ProbeStatus {
            result,
            last_round_at: Some(now),
            last_success_at,
            last_success_unix_secs,
            consecutive_failures,
        }));
        record_state(result, last_success_unix_secs, consecutive_failures);
    }
}

/// Owner of one service version's probe worker.
///
/// Mirrors the deletion worker's ownership model: cancelling stops the loop,
/// and dropping the handle — a service version replaced without an explicit
/// stop — cancels as a safety net.
pub struct ProbeHandle {
    state: Arc<ProbeState>,
    cancel: CancellationToken,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ProbeHandle {
    /// Latest published snapshot.
    pub fn status(&self) -> Arc<ProbeStatus> {
        self.state.status()
    }

    /// Stop the worker. The task observes the cancelled token on its next poll.
    pub(crate) fn shutdown(&self) {
        self.cancel.cancel();
        if let Ok(mut task) = self.task.lock() {
            drop(task.take());
        }
    }

    #[cfg(test)]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

impl Drop for ProbeHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Start the probe for a service version.
///
/// Returns `None` when the backend advertises no data key round trip, or when
/// the operator disabled the probe; both cases leave no background task behind.
pub(crate) fn spawn_probe_worker(backend: Arc<dyn KmsBackend>) -> Option<ProbeHandle> {
    let capabilities = backend.capabilities();
    if !capabilities.generate_data_key || !capabilities.decrypt {
        return None;
    }
    let interval = configured_interval()?;
    let state = Arc::new(ProbeState::new());
    let cancel = CancellationToken::new();
    let task = ProbeWorker::new(backend, state.clone(), interval).spawn(cancel.clone());
    Some(ProbeHandle {
        state,
        cancel,
        task: std::sync::Mutex::new(Some(task)),
    })
}

/// Probe interval from the environment, or `None` when the probe is disabled.
fn configured_interval() -> Option<Duration> {
    parse_interval(std::env::var(ENV_KMS_PROBE_INTERVAL_SECS).ok().as_deref())
}

/// Interpret the configured interval: unset or unparsable falls back to the
/// default, `0` disables the probe, and anything below [`MIN_PROBE_INTERVAL`]
/// is raised to it so a misconfigured value cannot turn the probe into a load
/// generator against the backend.
fn parse_interval(value: Option<&str>) -> Option<Duration> {
    let Some(value) = value else {
        return Some(DEFAULT_PROBE_INTERVAL);
    };
    let Ok(seconds) = value.trim().parse::<u64>() else {
        warn!(
            variable = ENV_KMS_PROBE_INTERVAL_SECS,
            "ignoring unparsable KMS probe interval; falling back to the default"
        );
        return Some(DEFAULT_PROBE_INTERVAL);
    };
    if seconds == 0 {
        return None;
    }
    Some(Duration::from_secs(seconds).max(MIN_PROBE_INTERVAL))
}

/// Encryption context bound to every probe data key.
///
/// Static and free of per-node values, so material generated by one node stays
/// decryptable by every other node running the same probe.
fn probe_encryption_context() -> HashMap<String, String> {
    HashMap::from([("rustfs-purpose".to_string(), "kms-probe".to_string())])
}

/// Outcome of making sure the dedicated probe key exists.
enum KeyProvisioning {
    Ready,
    Unsupported,
    Failed,
}

pub(crate) struct ProbeWorker {
    backend: Arc<dyn KmsBackend>,
    state: Arc<ProbeState>,
    interval: Duration,
}

impl ProbeWorker {
    fn new(backend: Arc<dyn KmsBackend>, state: Arc<ProbeState>, interval: Duration) -> Self {
        Self {
            backend,
            state,
            interval,
        }
    }

    fn spawn(self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(cancel).await })
    }

    async fn run(self, cancel: CancellationToken) {
        describe_metrics();
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("KMS probe worker stopped");
                    return;
                }
                _ = ticker.tick() => {}
            }
            let started = Instant::now();
            let result = self.round().await;
            record_round(result, started.elapsed());
            self.state.publish(result);
            if result == ProbeResult::Unsupported {
                // A capability gap does not change while a service version
                // lives, so retrying it forever would only produce noise.
                info!("KMS backend cannot host the synthetic probe key; probe disabled for this service version");
                return;
            }
        }
    }

    /// Run one round. Exposed separately so tests can drive the round trip
    /// deterministically without the interval loop.
    pub(crate) async fn round(&self) -> ProbeResult {
        let capabilities = self.backend.capabilities();
        if !capabilities.generate_data_key || !capabilities.decrypt {
            return ProbeResult::Unsupported;
        }
        match self.ensure_probe_key().await {
            KeyProvisioning::Ready => {}
            KeyProvisioning::Unsupported => return ProbeResult::Unsupported,
            KeyProvisioning::Failed => return ProbeResult::Failure(ProbeFailureKind::KeyProvisioning),
        }

        let mut generated = match self
            .backend
            .generate_data_key(GenerateDataKeyRequest {
                key_id: PROBE_KEY_ID.to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: probe_encryption_context(),
            })
            .await
        {
            Ok(generated) => generated,
            Err(error) => {
                warn!(%error, "KMS probe could not generate a data key");
                return ProbeResult::Failure(ProbeFailureKind::Generate);
            }
        };

        let decrypted = self
            .backend
            .decrypt(DecryptRequest {
                ciphertext: generated.ciphertext_blob.clone(),
                encryption_context: probe_encryption_context(),
                grant_tokens: Vec::new(),
            })
            .await;

        let result = match decrypted {
            Ok(mut response) => {
                let round_trips = bool::from(response.plaintext.ct_eq(&generated.plaintext_key));
                response.plaintext.zeroize();
                if round_trips {
                    debug!("KMS probe round trip succeeded");
                    ProbeResult::Success
                } else {
                    // The backend answered both calls but the material does not
                    // survive the round trip: report it as loudly as an outage.
                    warn!("KMS probe decrypted material does not match the generated data key");
                    ProbeResult::Failure(ProbeFailureKind::Mismatch)
                }
            }
            Err(error) => {
                warn!(%error, "KMS probe could not decrypt its generated data key");
                ProbeResult::Failure(ProbeFailureKind::Decrypt)
            }
        };
        generated.plaintext_key.zeroize();
        result
    }

    /// Create the probe key if it is missing.
    ///
    /// Idempotent by construction: an existing key is used as is, and a create
    /// that loses the race against another node reports the key as ready
    /// instead of failing the round.
    async fn ensure_probe_key(&self) -> KeyProvisioning {
        match self
            .backend
            .describe_key(DescribeKeyRequest {
                key_id: PROBE_KEY_ID.to_string(),
            })
            .await
        {
            Ok(_) => return KeyProvisioning::Ready,
            Err(KmsError::KeyNotFound { .. }) => {}
            Err(error) if is_capability_gap(&error) => return KeyProvisioning::Unsupported,
            Err(error) => {
                warn!(%error, "KMS probe could not describe its probe key");
                return KeyProvisioning::Failed;
            }
        }

        match self
            .backend
            .create_key(CreateKeyRequest {
                key_name: Some(PROBE_KEY_ID.to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: Some(PROBE_KEY_DESCRIPTION.to_string()),
                ..Default::default()
            })
            .await
        {
            Ok(_) => {
                info!(key_id = PROBE_KEY_ID, "created the KMS synthetic probe key");
                KeyProvisioning::Ready
            }
            // Another node created the key between the describe and the create.
            Err(KmsError::KeyAlreadyExists { .. }) => KeyProvisioning::Ready,
            Err(error) if is_capability_gap(&error) => KeyProvisioning::Unsupported,
            Err(error) => {
                warn!(%error, "KMS probe could not create its probe key");
                KeyProvisioning::Failed
            }
        }
    }
}

/// Whether an error means "this backend will never host a probe key".
///
/// Read-only single-key backends reject key creation outright, and backends
/// without the operation report an unsupported capability. Both are properties
/// of the configuration rather than symptoms of an outage, so they stop the
/// probe instead of counting as failures.
fn is_capability_gap(error: &KmsError) -> bool {
    matches!(error, KmsError::UnsupportedCapability { .. } | KmsError::InvalidOperation { .. })
}

// ---------------------------------------------------------------------------
// Metrics
//
// Label values are exclusively static outcome classes; the probe key id is a
// compile-time constant and still never becomes a label, so the metric shape
// stays independent of how many keys or nodes exist.
// ---------------------------------------------------------------------------

/// Counter: probe rounds completed, by `result`.
const METRIC_PROBE_ROUNDS_TOTAL: &str = "rustfs_kms_probe_rounds_total";
/// Counter: failed probe rounds, by `failure_kind`.
const METRIC_PROBE_FAILURES_TOTAL: &str = "rustfs_kms_probe_failures_total";
/// Histogram: wall-clock duration of one probe round, in seconds, by `result`.
const METRIC_PROBE_DURATION_SECONDS: &str = "rustfs_kms_probe_duration_seconds";
/// Gauge: Unix timestamp of the most recent successful round.
const METRIC_PROBE_LAST_SUCCESS_TIMESTAMP: &str = "rustfs_kms_probe_last_success_timestamp_seconds";
/// Gauge: failed rounds since the last success.
const METRIC_PROBE_CONSECUTIVE_FAILURES: &str = "rustfs_kms_probe_consecutive_failures";

/// Register metric descriptions once per process.
fn describe_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_counter!(METRIC_PROBE_ROUNDS_TOTAL, "Total synthetic KMS probe rounds completed, by result");
        metrics::describe_counter!(
            METRIC_PROBE_FAILURES_TOTAL,
            "Total failed synthetic KMS probe rounds, by the round-trip stage that failed"
        );
        metrics::describe_histogram!(
            METRIC_PROBE_DURATION_SECONDS,
            "Wall-clock duration of one synthetic KMS probe round, in seconds"
        );
        metrics::describe_gauge!(
            METRIC_PROBE_LAST_SUCCESS_TIMESTAMP,
            "Unix timestamp of the most recent successful synthetic KMS probe round"
        );
        metrics::describe_gauge!(
            METRIC_PROBE_CONSECUTIVE_FAILURES,
            "Synthetic KMS probe rounds that have failed since the last success"
        );
    });
}

/// Record one completed round.
///
/// An unsupported backend is deliberately counted as its own result and never
/// as a failure, so alerts on the failure counter stay silent for deployments
/// whose backend cannot host the probe.
fn record_round(result: ProbeResult, elapsed: Duration) {
    metrics::counter!(METRIC_PROBE_ROUNDS_TOTAL, "result" => result.as_label()).increment(1);
    if let ProbeResult::Failure(kind) = result {
        metrics::counter!(METRIC_PROBE_FAILURES_TOTAL, "failure_kind" => kind.as_label()).increment(1);
    }
    metrics::histogram!(METRIC_PROBE_DURATION_SECONDS, "result" => result.as_label()).record(elapsed.as_secs_f64());
}

/// Mirror the published snapshot into the gauges.
fn record_state(result: ProbeResult, last_success_unix_secs: Option<i64>, consecutive_failures: u32) {
    if result == ProbeResult::Success
        && let Some(timestamp) = last_success_unix_secs
    {
        // Only ever moved forward by a success, so a failing probe leaves the
        // gauge at the last known good time and its age keeps growing.
        metrics::gauge!(METRIC_PROBE_LAST_SUCCESS_TIMESTAMP).set(timestamp as f64);
    }
    metrics::gauge!(METRIC_PROBE_CONSECUTIVE_FAILURES).set(f64::from(consecutive_failures));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::local::LocalKmsBackend;
    use crate::backends::static_kms::StaticKmsBackend;
    use crate::config::KmsConfig;
    use crate::types::{
        CancelKeyDeletionRequest, CancelKeyDeletionResponse, CreateKeyResponse, DecryptResponse, DeleteKeyRequest,
        DeleteKeyResponse, DescribeKeyResponse, EncryptRequest, EncryptResponse, GenerateDataKeyResponse, KeyMetadata, KeyState,
        ListKeysRequest, ListKeysResponse,
    };
    use async_trait::async_trait;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::future::Future;

    async fn local_backend(temp_dir: &tempfile::TempDir) -> Arc<dyn KmsBackend> {
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        Arc::new(LocalKmsBackend::new(config).await.expect("local backend should build"))
    }

    async fn static_backend() -> Arc<dyn KmsBackend> {
        let config = KmsConfig::static_kms("static-key".to_string(), base64_simd::STANDARD.encode_to_string([0x42u8; 32]));
        Arc::new(StaticKmsBackend::new(config).await.expect("static backend should build"))
    }

    fn worker(backend: Arc<dyn KmsBackend>) -> ProbeWorker {
        ProbeWorker::new(backend, Arc::new(ProbeState::new()), DEFAULT_PROBE_INTERVAL)
    }

    /// Backend that answers every probe call but corrupts the round trip in a
    /// configurable way, so each failure classification can be provoked without
    /// a real backend outage.
    struct BrokenBackend {
        fault: Fault,
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Generate,
        Decrypt,
        /// Decrypt succeeds but returns material other than the generated key.
        Mismatch,
    }

    impl BrokenBackend {
        fn arc(fault: Fault) -> Arc<dyn KmsBackend> {
            Arc::new(Self { fault })
        }

        fn probe_key_metadata() -> KeyMetadata {
            KeyMetadata {
                key_id: PROBE_KEY_ID.to_string(),
                key_state: KeyState::Enabled,
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                creation_date: jiff::Zoned::now(),
                deletion_date: None,
                origin: "test".to_string(),
                key_manager: "test".to_string(),
                tags: HashMap::new(),
            }
        }
    }

    #[async_trait]
    impl KmsBackend for BrokenBackend {
        async fn create_key(&self, _request: CreateKeyRequest) -> crate::error::Result<CreateKeyResponse> {
            unimplemented!("the probe key always exists on this backend")
        }

        async fn encrypt(&self, _request: EncryptRequest) -> crate::error::Result<EncryptResponse> {
            unimplemented!("not exercised by probe tests")
        }

        async fn decrypt(&self, _request: DecryptRequest) -> crate::error::Result<DecryptResponse> {
            match self.fault {
                Fault::Decrypt => Err(KmsError::backend_error("decrypt unavailable")),
                Fault::Mismatch => Ok(DecryptResponse {
                    plaintext: vec![0xffu8; 32],
                    key_id: PROBE_KEY_ID.to_string(),
                    encryption_algorithm: None,
                }),
                Fault::Generate => unreachable!("generation fails before decrypt is reached"),
            }
        }

        async fn generate_data_key(&self, _request: GenerateDataKeyRequest) -> crate::error::Result<GenerateDataKeyResponse> {
            match self.fault {
                Fault::Generate => Err(KmsError::backend_error("data key generation unavailable")),
                _ => Ok(GenerateDataKeyResponse {
                    key_id: PROBE_KEY_ID.to_string(),
                    plaintext_key: vec![0x11u8; 32],
                    ciphertext_blob: vec![0x22u8; 48],
                }),
            }
        }

        async fn describe_key(&self, _request: DescribeKeyRequest) -> crate::error::Result<DescribeKeyResponse> {
            Ok(DescribeKeyResponse {
                key_metadata: Self::probe_key_metadata(),
            })
        }

        async fn list_keys(&self, _request: ListKeysRequest) -> crate::error::Result<ListKeysResponse> {
            unimplemented!("not exercised by probe tests")
        }

        async fn delete_key(&self, _request: DeleteKeyRequest) -> crate::error::Result<DeleteKeyResponse> {
            unimplemented!("not exercised by probe tests")
        }

        async fn cancel_key_deletion(
            &self,
            _request: CancelKeyDeletionRequest,
        ) -> crate::error::Result<CancelKeyDeletionResponse> {
            unimplemented!("not exercised by probe tests")
        }

        async fn health_check(&self) -> crate::error::Result<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn round_creates_the_probe_key_once_and_round_trips_it() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let worker = worker(backend.clone());

        assert_eq!(worker.round().await, ProbeResult::Success);
        let created = backend
            .describe_key(DescribeKeyRequest {
                key_id: PROBE_KEY_ID.to_string(),
            })
            .await
            .expect("the first round must create the probe key");
        assert_eq!(created.key_metadata.key_id, PROBE_KEY_ID);

        // A second round reuses the key instead of failing on the existing one,
        // which is also what a second node in the deployment would do.
        assert_eq!(worker.round().await, ProbeResult::Success);
        assert_eq!(
            backend
                .list_keys(ListKeysRequest {
                    limit: Some(100),
                    marker: None,
                    usage_filter: None,
                    status_filter: None,
                })
                .await
                .expect("list keys")
                .keys
                .iter()
                .filter(|key| key.key_id == PROBE_KEY_ID)
                .count(),
            1,
            "probe key creation must be idempotent"
        );
    }

    #[test]
    fn read_only_backend_is_reported_unsupported_without_failures() {
        let (snapshot, result) = record_metrics(|| {
            Box::pin(async {
                let worker = worker(static_backend().await);
                let result = worker.round().await;
                record_round(result, Duration::from_millis(1));
                result
            })
        });

        assert_eq!(result, ProbeResult::Unsupported);
        assert_eq!(counter_value(&snapshot, METRIC_PROBE_ROUNDS_TOTAL, &[("result", "unsupported")]), 1);
        assert_eq!(
            counter_value(&snapshot, METRIC_PROBE_FAILURES_TOTAL, &[]),
            0,
            "an unsupported backend must never raise probe failures"
        );
    }

    #[tokio::test]
    async fn each_round_trip_stage_gets_its_own_failure_classification() {
        for (fault, expected) in [
            (Fault::Generate, ProbeFailureKind::Generate),
            (Fault::Decrypt, ProbeFailureKind::Decrypt),
            (Fault::Mismatch, ProbeFailureKind::Mismatch),
        ] {
            assert_eq!(worker(BrokenBackend::arc(fault)).round().await, ProbeResult::Failure(expected));
        }
    }

    #[test]
    fn metrics_and_status_record_success_then_failure() {
        let (snapshot, status) = record_metrics(|| {
            Box::pin(async {
                let temp_dir = tempfile::tempdir().expect("temp dir");
                let state = Arc::new(ProbeState::new());
                let healthy = ProbeWorker::new(local_backend(&temp_dir).await, state.clone(), DEFAULT_PROBE_INTERVAL);
                let round = healthy.round().await;
                record_round(round, Duration::from_millis(10));
                state.publish(round);

                let broken = ProbeWorker::new(BrokenBackend::arc(Fault::Mismatch), state.clone(), DEFAULT_PROBE_INTERVAL);
                for _ in 0..2 {
                    let round = broken.round().await;
                    record_round(round, Duration::from_millis(10));
                    state.publish(round);
                }
                state.status()
            })
        });

        assert_eq!(status.result, ProbeResult::Failure(ProbeFailureKind::Mismatch));
        assert_eq!(status.consecutive_failures, 2);
        assert!(
            status.last_success_at.is_some() && status.last_success_unix_secs.is_some(),
            "a failing probe must keep reporting when it last succeeded"
        );

        assert_eq!(counter_value(&snapshot, METRIC_PROBE_ROUNDS_TOTAL, &[("result", "success")]), 1);
        assert_eq!(counter_value(&snapshot, METRIC_PROBE_ROUNDS_TOTAL, &[("result", "failure")]), 2);
        assert_eq!(counter_value(&snapshot, METRIC_PROBE_FAILURES_TOTAL, &[("failure_kind", "mismatch")]), 2);
        assert_eq!(histogram_values(&snapshot, METRIC_PROBE_DURATION_SECONDS, &[]).len(), 3);
        assert!(
            gauge_value(&snapshot, METRIC_PROBE_LAST_SUCCESS_TIMESTAMP).is_some_and(|timestamp| timestamp > 0.0),
            "the last success gauge must carry a wall-clock timestamp"
        );
        assert_eq!(gauge_value(&snapshot, METRIC_PROBE_CONSECUTIVE_FAILURES), Some(2.0));
    }

    #[tokio::test(start_paused = true)]
    async fn worker_publishes_status_and_stops_on_cancel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = Arc::new(ProbeState::new());
        assert_eq!(state.status().result, ProbeResult::Pending);

        let cancel = CancellationToken::new();
        let task = ProbeWorker::new(local_backend(&temp_dir).await, state.clone(), DEFAULT_PROBE_INTERVAL).spawn(cancel.clone());

        // The paused clock auto-advances through the worker's interval ticks.
        let mut observed = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if state.status().result != ProbeResult::Pending {
                observed = Some(state.status());
                break;
            }
        }
        let observed = observed.expect("the worker loop must publish a round result");
        assert_eq!(observed.result, ProbeResult::Success);
        assert_eq!(observed.consecutive_failures, 0);
        assert!(observed.last_success_at.is_some());

        cancel.cancel();
        task.await.expect("worker task must stop after cancellation");
    }

    #[tokio::test(start_paused = true)]
    async fn unsupported_backend_stops_the_worker_without_cancellation() {
        let state = Arc::new(ProbeState::new());
        let cancel = CancellationToken::new();
        let task = ProbeWorker::new(static_backend().await, state.clone(), DEFAULT_PROBE_INTERVAL).spawn(cancel.clone());

        task.await.expect("worker must exit on its own for an unsupported backend");
        assert_eq!(state.status().result, ProbeResult::Unsupported);
        assert!(!cancel.is_cancelled());
    }

    #[test]
    fn interval_configuration_defaults_clamps_and_disables() {
        assert_eq!(parse_interval(None), Some(DEFAULT_PROBE_INTERVAL));
        assert_eq!(parse_interval(Some("not-a-number")), Some(DEFAULT_PROBE_INTERVAL));
        assert_eq!(parse_interval(Some(" 120 ")), Some(Duration::from_secs(120)));
        assert_eq!(parse_interval(Some("1")), Some(MIN_PROBE_INTERVAL));
        assert_eq!(parse_interval(Some("0")), None, "zero must disable the probe");
    }

    // -- Metric assertion helpers, mirroring the policy engine's tests --------

    type MetricEntry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Run `test` on a paused current-thread runtime under a debugging recorder
    /// and return one snapshot of everything it emitted.
    fn record_metrics<Out>(test: impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Out>>>) -> (Vec<MetricEntry>, Out) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let out = metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .expect("current-thread runtime must build");
            runtime.block_on(test())
        });
        (snapshotter.snapshot().into_vec(), out)
    }

    fn labels_match(key: &metrics::Key, labels: &[(&str, &str)]) -> bool {
        labels
            .iter()
            .all(|(label, expected)| key.labels().any(|l| l.key() == *label && l.value() == *expected))
    }

    fn counter_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> u64 {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Counter
                    && composite.key().name() == name
                    && labels_match(composite.key(), labels);
                match (matches, value) {
                    (true, DebugValue::Counter(count)) => Some(*count),
                    _ => None,
                }
            })
            .sum()
    }

    fn histogram_values(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Histogram
                    && composite.key().name() == name
                    && labels_match(composite.key(), labels);
                match (matches, value) {
                    (true, DebugValue::Histogram(values)) => Some(values),
                    _ => None,
                }
            })
            .flatten()
            .map(|value| value.into_inner())
            .collect()
    }

    fn gauge_value(snapshot: &[MetricEntry], name: &str) -> Option<f64> {
        snapshot.iter().find_map(|(composite, _unit, _description, value)| {
            let matches = composite.kind() == MetricKind::Gauge && composite.key().name() == name;
            match (matches, value) {
                (true, DebugValue::Gauge(gauge)) => Some(gauge.into_inner()),
                _ => None,
            }
        })
    }
}
