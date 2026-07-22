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

//! Shared test utilities for tier / lifecycle integration tests.
//!
//! Gated behind the `test-util` feature (`rustfs/backlog#1148` ilm-6). This
//! module used to be duplicated verbatim in two places — the scanner
//! integration tests (`crates/scanner/tests/lifecycle_integration_test.rs`)
//! and the app-layer transition tests
//! (`rustfs/src/app/lifecycle_transition_api_test.rs`). Both now consume this
//! single definition. Consuming it from an external test crate requires
//! enabling the feature as a dev-dependency, for example:
//!
//! ```toml
//! [dev-dependencies]
//! rustfs-ecstore = { workspace = true, features = ["test-util"] }
//! ```
//!
//! and importing through the crate facade
//! (`rustfs_ecstore::api::tier::test_util`).
//!
//! What it provides:
//!
//! - [`MockWarmBackend`]: an in-memory [`WarmBackend`] implementation backed by
//!   a `HashMap`. It records an operation log (for ordering assertions such as
//!   "local delete precedes remote remove") and supports fault injection
//!   ([`FaultConfig`]): unreachable backend, HTTP 5xx, credential rejection, and
//!   injected latency. It also exposes [`MockWarmBackend::external_remove`] to
//!   simulate a remote object being deleted out-of-band.
//! - [`register_mock_tier`]: register a [`MockWarmBackend`] as a tier in a
//!   [`TierConfigMgr`] handle (works with both the global manager and a
//!   per-instance manager).
//! - xl.meta transition-state assertion helpers ([`read_transition_meta`],
//!   [`assert_transition_meta_consistent`], [`free_version_count`]): read the
//!   on-disk `xl.meta` and assert the `(status, tier, remote object, remote
//!   version id)` tuple plus free-version count, cross-checking every shard
//!   disk for consistency.
//! - Polling helpers ([`MockWarmBackend::wait_for_remote_absence`],
//!   [`MockWarmBackend::wait_for_object_count`],
//!   [`wait_for_free_version_absence`]) so tests never assert a single read
//!   against asynchronously-applied state.
//!
//! Downstream consumers: ilm-8 (restore lifecycle) and ilm-11 (tier fault
//! injection matrix) build directly on this surface.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify, RwLock};
use uuid::Uuid;

use crate::client::transition_api::{ReadCloser, ReaderImpl};
use crate::disk::endpoint::Endpoint;
use crate::disk::format::FormatV3;
use crate::disk::{DiskAPI, DiskOption, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE, new_disk};
use crate::services::tier::tier::TierConfigMgr;
use crate::services::tier::tier_config::{TierConfig, TierMinIO, TierType};
use crate::services::tier::warm_backend::{
    TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts, build_transition_put_options,
};
use rustfs_filemeta::FileMeta;
use rustfs_utils::path::path_join_buf;

/// One-shot barrier before rejected transition cleanup resolves its ECStore.
pub struct TransitionCleanupStoreBarrier(crate::set_disk::SetDiskTransitionCleanupStoreBarrier);

impl TransitionCleanupStoreBarrier {
    /// Install the barrier for the next rejected transition cleanup.
    pub fn install() -> Self {
        Self(crate::set_disk::SetDiskTransitionCleanupStoreBarrier::install())
    }

    /// Wait until the rejected transition reaches cleanup-store resolution.
    pub async fn wait_until_paused(&self) {
        self.0.wait_until_paused().await;
    }
}

/// Default polling cadence used by the `wait_for_*` helpers.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A fault to inject into [`MockWarmBackend`] operations.
///
/// Faults are checked at the start of every trait method. When set, the backend
/// returns a representative [`std::io::Error`] instead of performing the
/// operation, letting tests exercise transition/restore/sweep error paths
/// without a real remote tier. Latency, if set, is applied before the fault
/// check so slow-and-failing behaviour can be modelled together.
#[derive(Clone, Debug, Default)]
pub struct FaultConfig {
    /// Simulate a backend that cannot be reached (connection refused).
    pub unreachable: bool,
    /// Simulate an HTTP 5xx server error on every operation.
    pub server_error: bool,
    /// Simulate rejected / rotated credentials (permission denied).
    pub reject_credentials: bool,
    /// Inject latency before each operation resolves.
    pub latency: Option<Duration>,
}

impl FaultConfig {
    fn error(&self) -> Option<std::io::Error> {
        use std::io::{Error, ErrorKind};
        if self.unreachable {
            return Some(Error::new(ErrorKind::ConnectionRefused, "mock warm backend unreachable"));
        }
        if self.reject_credentials {
            return Some(Error::new(ErrorKind::PermissionDenied, "mock warm backend rejected credentials"));
        }
        if self.server_error {
            return Some(Error::other("mock warm backend returned HTTP 503"));
        }
        None
    }
}

/// A single operation recorded by [`MockWarmBackend`].
///
/// Ordering assertions (for example, "the local delete lands before any remote
/// remove" from the expire/GET race regression) read this log via
/// [`MockWarmBackend::op_log`]. [`MockWarmOp::ExternalRemove`] is only produced
/// by [`MockWarmBackend::external_remove`], never by the trait `remove`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockWarmOp {
    Put { object: String },
    Get { object: String },
    Remove { object: String },
    Probe { object: String },
    ExternalRemove { object: String },
    InUse,
}

/// A stored object inside [`MockWarmBackend`].
#[derive(Clone, Default)]
pub struct MockStoredObject {
    pub bytes: Vec<u8>,
    pub metadata: HashMap<String, String>,
    pub remote_version_id: String,
}

#[derive(Default)]
struct MockWarmBackendInner {
    objects: Mutex<HashMap<String, MockStoredObject>>,
    faults: Mutex<FaultConfig>,
    put_read_limit: Mutex<Option<usize>>,
    put_remote_version: Mutex<Option<String>>,
    reject_non_empty_remote_versions: AtomicBool,
    reject_non_empty_remote_version_validations: AtomicUsize,
    fail_remove: AtomicBool,
    exact_remove_count: AtomicUsize,
    op_log: Mutex<Vec<MockWarmOp>>,
    put_versions: Mutex<Vec<(String, String)>>,
    remove_versions: Mutex<Vec<(String, String)>>,
    put_barrier: Mutex<Option<Arc<MockPutBarrierState>>>,
    get_barrier: Mutex<Option<Arc<MockGetBarrierState>>>,
    remove_barrier: Mutex<Option<Arc<MockRemoveBarrierState>>>,
}

#[derive(Default)]
struct MockPutBarrierState {
    arrived: Notify,
    release: Notify,
}

#[derive(Default)]
struct MockGetBarrierState {
    arrived: Notify,
    release: Notify,
    fail_after_release: bool,
}

#[derive(Default)]
struct MockRemoveBarrierState {
    arrived: Notify,
    release: Notify,
    operation_dropped: Notify,
}

struct MockRemoveOperationGuard {
    state: Arc<MockRemoveBarrierState>,
}

impl Drop for MockRemoveOperationGuard {
    fn drop(&mut self) {
        self.state.operation_dropped.notify_one();
    }
}

/// One-shot barrier that pauses a mock tier PUT after storing its remote body.
pub struct MockPutBarrier {
    state: Arc<MockPutBarrierState>,
}

impl MockPutBarrier {
    /// Wait until the remote body is stored and the PUT is paused before returning.
    pub async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("mock tier PUT should reach the deterministic barrier");
    }

    /// Release the paused PUT.
    pub fn release(&self) {
        self.state.release.notify_one();
    }
}

impl Drop for MockPutBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
    }
}

/// One-shot barrier that pauses a mock tier GET before it reads remote bytes.
pub struct MockGetBarrier {
    state: Arc<MockGetBarrierState>,
}

impl MockGetBarrier {
    /// Wait until the GET has reached the deterministic pause point.
    pub async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("mock tier GET should reach the deterministic barrier");
    }

    /// Release the paused GET.
    pub fn release(&self) {
        self.state.release.notify_one();
    }
}

impl Drop for MockGetBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
    }
}

/// One-shot barrier that pauses and then fails a mock tier DELETE.
pub struct MockRemoveBarrier {
    state: Arc<MockRemoveBarrierState>,
}

impl MockRemoveBarrier {
    /// Wait until DELETE reaches the deterministic failure point.
    pub async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("mock tier DELETE should reach the deterministic barrier");
    }

    /// Release the paused DELETE, which then returns an injected error.
    pub fn release(&self) {
        self.state.release.notify_one();
    }

    /// Wait until the paused DELETE future completes or is cancelled.
    pub async fn wait_until_operation_dropped(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.operation_dropped.notified())
            .await
            .expect("mock tier DELETE operation should be dropped");
    }
}

impl Drop for MockRemoveBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
    }
}

/// In-memory [`WarmBackend`] for lifecycle / tiering integration tests.
///
/// Cloning shares the same underlying storage, fault configuration, and
/// operation log (everything is behind an [`Arc`]), so a clone registered in a
/// [`TierConfigMgr`] observes the same state as the handle held by the test.
#[derive(Clone, Default)]
pub struct MockWarmBackend {
    inner: Arc<MockWarmBackendInner>,
}

impl MockWarmBackend {
    /// Create an empty backend with no faults injected.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm a one-shot pause after the next tier PUT stores its remote body.
    pub async fn arm_put_barrier(&self) -> MockPutBarrier {
        let state = Arc::new(MockPutBarrierState::default());
        *self.inner.put_barrier.lock().await = Some(Arc::clone(&state));
        MockPutBarrier { state }
    }

    /// Pause and then fail the next DELETE after it reaches the backend.
    pub async fn arm_failing_remove_barrier(&self) -> MockRemoveBarrier {
        let state = Arc::new(MockRemoveBarrierState::default());
        let mut barrier = self.inner.remove_barrier.lock().await;
        assert!(barrier.is_none(), "mock tier DELETE barrier is already armed");
        *barrier = Some(state.clone());
        MockRemoveBarrier { state }
    }

    /// Arm a one-shot pause before the next tier GET, then return an error
    /// after the test releases it.
    pub async fn arm_failing_get_barrier(&self) -> MockGetBarrier {
        let state = Arc::new(MockGetBarrierState {
            fail_after_release: true,
            ..Default::default()
        });
        *self.inner.get_barrier.lock().await = Some(Arc::clone(&state));
        MockGetBarrier { state }
    }

    /// Arm a one-shot pause before the next tier GET, then continue normally
    /// after the test releases it.
    pub async fn arm_get_barrier(&self) -> MockGetBarrier {
        let state = Arc::new(MockGetBarrierState::default());
        *self.inner.get_barrier.lock().await = Some(Arc::clone(&state));
        MockGetBarrier { state }
    }

    // ---- fault injection -------------------------------------------------

    /// Replace the entire fault configuration.
    pub async fn set_faults(&self, faults: FaultConfig) {
        *self.inner.faults.lock().await = faults;
    }

    /// Toggle "backend unreachable" (connection refused on every operation).
    pub async fn set_unreachable(&self, unreachable: bool) {
        self.inner.faults.lock().await.unreachable = unreachable;
    }

    /// Toggle "HTTP 5xx" server errors on every operation.
    pub async fn set_server_error(&self, server_error: bool) {
        self.inner.faults.lock().await.server_error = server_error;
    }

    /// Toggle "credential rejected" (permission denied) on every operation.
    pub async fn set_reject_credentials(&self, reject: bool) {
        self.inner.faults.lock().await.reject_credentials = reject;
    }

    /// Set (or clear, with `None`) injected latency applied before each op.
    pub async fn set_latency(&self, latency: Option<Duration>) {
        self.inner.faults.lock().await.latency = latency;
    }

    /// Clear all injected faults, restoring healthy behaviour.
    pub async fn clear_faults(&self) {
        *self.inner.faults.lock().await = FaultConfig::default();
    }

    /// Limit how many body bytes a successful mock PUT consumes. `None` drains
    /// the complete body. This models a backend that incorrectly accepts a
    /// truncated stream while still returning success.
    pub async fn set_put_read_limit(&self, limit: Option<usize>) {
        *self.inner.put_read_limit.lock().await = limit;
    }

    /// Override the remote version returned by subsequent successful PUTs.
    pub async fn set_put_remote_version(&self, remote_version: Option<String>) {
        *self.inner.put_remote_version.lock().await = remote_version;
    }

    /// Reject non-empty remote versions before transition metadata is committed.
    pub fn set_reject_non_empty_remote_versions(&self, reject: bool) {
        self.inner.reject_non_empty_remote_versions.store(reject, Ordering::Release);
    }

    /// Reject the next non-empty remote version validation without changing
    /// subsequent exact-version backend cleanup behavior.
    pub fn reject_next_non_empty_remote_version_validation(&self) {
        self.inner
            .reject_non_empty_remote_version_validations
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Enable or disable a persistent remove failure for durability tests.
    pub fn set_remove_failure(&self, fail: bool) {
        self.inner.fail_remove.store(fail, Ordering::Release);
    }

    async fn precondition(&self) -> Result<(), std::io::Error> {
        let (latency, error) = {
            let faults = self.inner.faults.lock().await;
            (faults.latency, faults.error())
        };
        if let Some(latency) = latency {
            tokio::time::sleep(latency).await;
        }
        match error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    // ---- operation log ---------------------------------------------------

    async fn record(&self, op: MockWarmOp) {
        self.inner.op_log.lock().await.push(op);
    }

    /// Snapshot of every operation the backend has performed, in order.
    pub async fn op_log(&self) -> Vec<MockWarmOp> {
        self.inner.op_log.lock().await.clone()
    }

    /// Clear the operation log without touching stored objects or faults.
    pub async fn clear_op_log(&self) {
        self.inner.op_log.lock().await.clear();
    }

    /// Number of trait `remove` calls recorded (excludes `external_remove`).
    pub async fn remove_count(&self) -> usize {
        self.inner
            .op_log
            .lock()
            .await
            .iter()
            .filter(|op| matches!(op, MockWarmOp::Remove { .. }))
            .count()
    }

    /// Number of exact-version trait remove calls, including failed attempts.
    pub fn exact_remove_count(&self) -> usize {
        self.inner.exact_remove_count.load(Ordering::Acquire)
    }

    /// Return the exact object/version pairs produced by successful tier PUTs.
    pub async fn put_versions(&self) -> Vec<(String, String)> {
        self.inner.put_versions.lock().await.clone()
    }

    /// Return the exact object/version pairs passed to successful tier removes.
    pub async fn remove_versions(&self) -> Vec<(String, String)> {
        self.inner.remove_versions.lock().await.clone()
    }

    /// Return the provider-authoritative view of a remote transition candidate.
    pub async fn probe_transition_candidate_state(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        self.probe_transition_candidate(object).await
    }

    /// Number of `get` calls recorded — useful to assert restore reads hit the
    /// local copy rather than the remote tier.
    pub async fn get_count(&self) -> usize {
        self.inner
            .op_log
            .lock()
            .await
            .iter()
            .filter(|op| matches!(op, MockWarmOp::Get { .. }))
            .count()
    }

    /// Number of `put` calls recorded.
    pub async fn put_count(&self) -> usize {
        self.inner
            .op_log
            .lock()
            .await
            .iter()
            .filter(|op| matches!(op, MockWarmOp::Put { .. }))
            .count()
    }

    // ---- storage inspection ---------------------------------------------

    /// Whether the backend currently stores `object`.
    pub async fn contains(&self, object: &str) -> bool {
        self.inner.objects.lock().await.contains_key(object)
    }

    /// Number of objects currently stored.
    pub async fn object_count(&self) -> usize {
        self.inner.objects.lock().await.len()
    }

    /// A clone of the stored object, if present.
    pub async fn stored(&self, object: &str) -> Option<MockStoredObject> {
        self.inner.objects.lock().await.get(object).cloned()
    }

    /// A clone of the raw bytes stored for `object`, if present.
    pub async fn bytes(&self, object: &str) -> Option<Vec<u8>> {
        self.inner.objects.lock().await.get(object).map(|o| o.bytes.clone())
    }

    /// A clone of the metadata stored for `object`, if present.
    pub async fn metadata(&self, object: &str) -> Option<HashMap<String, String>> {
        self.inner.objects.lock().await.get(object).map(|o| o.metadata.clone())
    }

    /// Simulate the remote object being deleted out-of-band (for example by an
    /// operator or lifecycle rule on the remote tier), bypassing the trait
    /// `remove`. Records [`MockWarmOp::ExternalRemove`].
    pub async fn external_remove(&self, object: &str) {
        self.inner.objects.lock().await.remove(object);
        self.record(MockWarmOp::ExternalRemove {
            object: object.to_string(),
        })
        .await;
    }

    // ---- polling helpers -------------------------------------------------

    /// Poll until `object` is absent from the backend, or `timeout` elapses.
    /// Returns `true` if the object disappeared within the budget.
    pub async fn wait_for_remote_absence(&self, object: &str, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.contains(object).await {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Poll until the backend holds exactly `expected` objects, or `timeout`
    /// elapses. Returns `true` if the count was reached within the budget.
    pub async fn wait_for_object_count(&self, expected: usize, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.object_count().await == expected {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    // ---- internal helpers -----------------------------------------------

    async fn put_bytes(&self, object: &str, bytes: Vec<u8>, metadata: HashMap<String, String>) -> String {
        let remote_version_id = self
            .inner
            .put_remote_version
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        self.inner.objects.lock().await.insert(
            object.to_string(),
            MockStoredObject {
                bytes,
                metadata,
                remote_version_id: remote_version_id.clone(),
            },
        );
        remote_version_id
    }

    async fn read_bytes(&self, reader: ReaderImpl) -> Result<Vec<u8>, std::io::Error> {
        let limit = *self.inner.put_read_limit.lock().await;
        match reader {
            ReaderImpl::Body(bytes) => Ok(bytes.slice(..limit.unwrap_or(bytes.len()).min(bytes.len())).to_vec()),
            ReaderImpl::ObjectBody(mut reader) => {
                let mut buf = Vec::new();
                if let Some(limit) = limit {
                    let limit =
                        u64::try_from(limit).map_err(|_| std::io::Error::other("mock PUT read limit exceeds u64::MAX"))?;
                    reader.stream.take(limit).read_to_end(&mut buf).await?;
                } else {
                    reader.stream.read_to_end(&mut buf).await?;
                }
                Ok(buf)
            }
        }
    }
}

#[async_trait]
impl WarmBackend for MockWarmBackend {
    fn validate_remote_version_id(&self, remote_version_id: &str) -> Result<(), std::io::Error> {
        if remote_version_id.is_empty() {
            return Ok(());
        }
        let reject_once = self
            .inner
            .reject_non_empty_remote_version_validations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
            .is_ok();
        if reject_once || self.inner.reject_non_empty_remote_versions.load(Ordering::Acquire) {
            return Err(std::io::Error::other("mock warm backend requires an unversioned remote object"));
        }
        Ok(())
    }

    async fn put(&self, object: &str, r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
        self.precondition().await?;
        let bytes = self.read_bytes(r).await?;
        let version = self.put_bytes(object, bytes, HashMap::new()).await;
        self.inner
            .put_versions
            .lock()
            .await
            .push((object.to_string(), version.clone()));
        self.record(MockWarmOp::Put {
            object: object.to_string(),
        })
        .await;
        Ok(version)
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        _length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        self.precondition().await?;
        let bytes = self.read_bytes(r).await?;
        // Mirror the real transition put: promote content headers and drop the
        // internal replication / object-lock defaults so tests can assert that
        // transitioned objects don't inherit them.
        let opts = build_transition_put_options(String::new(), meta);
        let mut metadata = opts.user_metadata.clone();
        if !opts.content_type.is_empty() {
            metadata.insert("content-type".to_string(), opts.content_type.clone());
        }
        if !opts.content_encoding.is_empty() {
            metadata.insert("content-encoding".to_string(), opts.content_encoding.clone());
        }
        if !opts.cache_control.is_empty() {
            metadata.insert("cache-control".to_string(), opts.cache_control.clone());
        }
        if !opts.internal.replication_status.as_str().is_empty() {
            metadata.insert(
                "x-amz-replication-status".to_string(),
                opts.internal.replication_status.as_str().to_string(),
            );
        }
        if !opts.legalhold.as_str().is_empty() {
            metadata.insert("x-amz-object-lock-legal-hold".to_string(), opts.legalhold.as_str().to_string());
        }
        let version = self.put_bytes(object, bytes, metadata).await;
        self.inner
            .put_versions
            .lock()
            .await
            .push((object.to_string(), version.clone()));
        let barrier = self.inner.put_barrier.lock().await.take();
        if let Some(barrier) = barrier {
            barrier.arrived.notify_one();
            barrier.release.notified().await;
        }
        self.record(MockWarmOp::Put {
            object: object.to_string(),
        })
        .await;
        Ok(version)
    }

    async fn get(&self, object: &str, _rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        self.precondition().await?;
        let barrier = self.inner.get_barrier.lock().await.take();
        if let Some(barrier) = barrier {
            barrier.arrived.notify_one();
            barrier.release.notified().await;
            if barrier.fail_after_release {
                return Err(std::io::Error::other("mock warm backend GET failed after barrier"));
            }
        }
        self.record(MockWarmOp::Get {
            object: object.to_string(),
        })
        .await;
        let objects = self.inner.objects.lock().await;
        let Some(stored) = objects.get(object) else {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "mock object not found"));
        };
        let bytes = &stored.bytes;

        let start = opts.start_offset.max(0) as usize;
        let end = if opts.length > 0 {
            start.saturating_add(opts.length as usize).min(bytes.len())
        } else {
            bytes.len()
        };

        Ok(tokio::io::BufReader::new(Cursor::new(bytes[start.min(bytes.len())..end].to_vec())))
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        self.precondition().await?;
        if let Some(barrier) = self.inner.remove_barrier.lock().await.take() {
            let _operation = MockRemoveOperationGuard { state: barrier.clone() };
            barrier.arrived.notify_one();
            barrier.release.notified().await;
            return Err(std::io::Error::other("mock warm backend remove failure after barrier"));
        }
        if self.inner.fail_remove.load(Ordering::Acquire) {
            return Err(std::io::Error::other("mock warm backend remove failure"));
        }
        let mut objects = self.inner.objects.lock().await;
        if let Some(stored) = objects.get(object)
            && !rv.is_empty()
            && stored.remote_version_id != rv
        {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "NoSuchVersion"));
        }
        objects.remove(object);
        drop(objects);
        self.inner
            .remove_versions
            .lock()
            .await
            .push((object.to_string(), rv.to_string()));
        self.record(MockWarmOp::Remove {
            object: object.to_string(),
        })
        .await;
        Ok(())
    }

    async fn remove_exact(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        if rv.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an exact mock tier delete requires a remote version ID",
            ));
        }
        self.inner.exact_remove_count.fetch_add(1, Ordering::AcqRel);
        self.remove(object, rv).await
    }

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        self.precondition().await?;
        self.record(MockWarmOp::Probe {
            object: object.to_string(),
        })
        .await;
        let objects = self.inner.objects.lock().await;
        let Some(stored) = objects.get(object) else {
            return Ok(TransitionCandidateProbe::Missing);
        };
        if stored.remote_version_id.is_empty() {
            Ok(TransitionCandidateProbe::UnversionedPresent)
        } else {
            Ok(TransitionCandidateProbe::VersionedPresent(stored.remote_version_id.clone()))
        }
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        self.precondition().await?;
        self.record(MockWarmOp::InUse).await;
        Ok(!self.inner.objects.lock().await.is_empty())
    }
}

/// Register `backend` as a MinIO-typed tier named `tier_name` in the tier
/// config manager reachable through `handle`, returning a clone of the backend
/// that shares its state.
///
/// Works with any [`TierConfigMgr`] handle — the global one
/// (`rustfs_ecstore::api::runtime::global_tier_config_mgr`) used by the scanner
/// integration tests, or a per-instance one used by the app-layer tests.
pub async fn register_mock_tier(handle: &Arc<RwLock<TierConfigMgr>>, tier_name: &str) -> MockWarmBackend {
    let backend = MockWarmBackend::new();
    register_mock_tier_backend(handle, tier_name, backend.clone()).await;
    backend
}

/// Like [`register_mock_tier`] but registers an existing `backend` (for example
/// one that already had faults injected before wiring it up).
pub async fn register_mock_tier_backend(handle: &Arc<RwLock<TierConfigMgr>>, tier_name: &str, backend: MockWarmBackend) {
    let mut tier_config_mgr = handle.write().await;
    tier_config_mgr.tiers.insert(
        tier_name.to_string(),
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::MinIO,
            name: tier_name.to_string(),
            minio: Some(TierMinIO {
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                bucket: "mock-tier".to_string(),
                endpoint: "http://127.0.0.1:0".to_string(),
                prefix: format!("mock/{}/", Uuid::new_v4()),
                region: String::new(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    tier_config_mgr
        .install_test_driver(tier_name, Box::new(backend))
        .expect("mock tier driver should install");
}

/// The transition-state tuple read from an on-disk `xl.meta`, plus the object's
/// free-version count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransitionMeta {
    /// `transition_status` (e.g. `"complete"`), empty when not transitioned.
    pub status: String,
    /// The tier name the object was transitioned to.
    pub tier: String,
    /// The remote object key (`transitioned_objname`).
    pub remote_object: String,
    /// The remote version id (`transition_version_id`), if any.
    pub remote_version_id: Option<String>,
    /// Number of free versions retained for the object.
    pub free_version_count: usize,
}

async fn open_disk(disk_path: &Path) -> Option<crate::disk::DiskStore> {
    // `LocalDisk::new` rejects an endpoint whose (set_idx, disk_idx) disagrees
    // with the position recorded in the disk's own format.json, so derive the
    // real indices instead of assuming slot 0 (rustfs/backlog#1303).
    let format_data = tokio::fs::read(disk_path.join(RUSTFS_META_BUCKET).join(FORMAT_CONFIG_FILE))
        .await
        .ok()?;
    let format = FormatV3::try_from(format_data.as_slice()).ok()?;
    let (set_idx, disk_idx) = format.find_disk_index_by_disk_id(format.erasure.this).ok()?;

    let mut endpoint = Endpoint::try_from(disk_path.to_str()?).ok()?;
    endpoint.set_pool_index(0);
    endpoint.set_set_index(set_idx);
    endpoint.set_disk_index(disk_idx);
    new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .ok()
}

/// Count the free versions retained for `object` on a single disk's `xl.meta`.
///
/// The free-version metadata removal lands asynchronously after the remote
/// object disappears, so callers typically poll via
/// [`wait_for_free_version_absence`] instead of asserting a single read.
pub async fn free_version_count(disk_path: &Path, bucket: &str, object: &str) -> usize {
    let Some(disk) = open_disk(disk_path).await else {
        return 0;
    };
    let Ok(data) = disk
        .read_metadata(bucket, &path_join_buf(&[object, STORAGE_FORMAT_FILE]))
        .await
    else {
        return 0;
    };
    let Ok(meta) = FileMeta::load(&data) else {
        return 0;
    };
    meta.get_file_info_versions(bucket, object, false)
        .map(|v| v.free_versions.len())
        .unwrap_or(0)
}

/// Read the transition-state tuple for `object` from a single disk's `xl.meta`.
///
/// Returns `None` if the metadata cannot be read or decoded. The transition
/// fields are taken from the newest version that carries a transition record;
/// if no version is transitioned, they are taken from the current version (and
/// will be empty).
pub async fn read_transition_meta(disk_path: &Path, bucket: &str, object: &str) -> Option<TransitionMeta> {
    let disk = open_disk(disk_path).await?;
    let data = disk
        .read_metadata(bucket, &path_join_buf(&[object, STORAGE_FORMAT_FILE]))
        .await
        .ok()?;
    let meta = FileMeta::load(&data).ok()?;
    let versions = meta.get_file_info_versions(bucket, object, false).ok()?;
    let free_version_count = versions.free_versions.len();

    let fi = versions
        .versions
        .iter()
        .find(|v| !v.transition_status.is_empty() || !v.transitioned_objname.is_empty())
        .or_else(|| versions.versions.first())?;

    Some(TransitionMeta {
        status: fi.transition_status.clone(),
        tier: fi.transition_tier.clone(),
        remote_object: fi.transitioned_objname.clone(),
        remote_version_id: fi.transition_version_id.map(|id| id.to_string()),
        free_version_count,
    })
}

/// Assert that every disk in `disk_paths` reports the same transition tuple for
/// `object`, returning that tuple. Panics with a descriptive message if any
/// disk is missing the object or disagrees — this is the shard-consistency
/// check required by ilm-6 (the `(status, tier, remote key, remote version id)`
/// four-tuple plus free-version count must match across all erasure shards).
pub async fn assert_transition_meta_consistent<P: AsRef<Path>>(disk_paths: &[P], bucket: &str, object: &str) -> TransitionMeta {
    assert!(!disk_paths.is_empty(), "assert_transition_meta_consistent needs at least one disk");

    let mut expected: Option<TransitionMeta> = None;
    for disk_path in disk_paths {
        let disk_path = disk_path.as_ref();
        let meta = read_transition_meta(disk_path, bucket, object)
            .await
            .unwrap_or_else(|| panic!("missing xl.meta for {bucket}/{object} on disk {}", disk_path.display()));
        match &expected {
            None => expected = Some(meta),
            Some(expected) => assert_eq!(
                *expected,
                meta,
                "transition metadata mismatch for {bucket}/{object} on disk {} (expected {expected:?}, got {meta:?})",
                disk_path.display()
            ),
        }
    }

    expected.expect("at least one disk was inspected")
}

/// Poll until `object` retains no free versions on `disk_path`, or `timeout`
/// elapses. Returns `true` if the free versions drained within the budget.
pub async fn wait_for_free_version_absence(disk_path: &Path, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if free_version_count(disk_path, bucket, object).await == 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn mock_probe_distinguishes_missing_unversioned_and_versioned_candidates() {
        let backend = MockWarmBackend::new();

        assert_eq!(
            backend
                .probe_transition_candidate_state("missing")
                .await
                .expect("probe missing candidate"),
            TransitionCandidateProbe::Missing
        );

        backend.set_put_remote_version(Some(String::new())).await;
        backend
            .put("unversioned", ReaderImpl::Body(Bytes::new()), 0)
            .await
            .expect("put unversioned candidate");
        assert_eq!(
            backend
                .probe_transition_candidate_state("unversioned")
                .await
                .expect("probe unversioned candidate"),
            TransitionCandidateProbe::UnversionedPresent
        );

        let remote_version = Uuid::new_v4().to_string();
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        backend
            .put("versioned", ReaderImpl::Body(Bytes::new()), 0)
            .await
            .expect("put versioned candidate");
        assert_eq!(
            backend
                .probe_transition_candidate_state("versioned")
                .await
                .expect("probe versioned candidate"),
            TransitionCandidateProbe::VersionedPresent(remote_version)
        );

        assert_eq!(
            backend
                .op_log()
                .await
                .into_iter()
                .filter(|op| matches!(op, MockWarmOp::Probe { .. }))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn mock_probe_preserves_fault_fail_closed_behavior() {
        let backend = MockWarmBackend::new();
        backend.set_reject_credentials(true).await;

        let err = backend
            .probe_transition_candidate("remote-object")
            .await
            .expect_err("credential rejection must fail the authoritative probe");

        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(backend.op_log().await.is_empty());
    }
}
