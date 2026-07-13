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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::client::transition_api::{ReadCloser, ReaderImpl};
use crate::disk::endpoint::Endpoint;
use crate::disk::{DiskAPI, DiskOption, STORAGE_FORMAT_FILE, new_disk};
use crate::services::tier::tier::TierConfigMgr;
use crate::services::tier::tier_config::{TierConfig, TierMinIO, TierType};
use crate::services::tier::warm_backend::{WarmBackend, WarmBackendGetOpts, build_transition_put_options};
use rustfs_filemeta::FileMeta;
use rustfs_utils::path::path_join_buf;

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
    op_log: Mutex<Vec<MockWarmOp>>,
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
        let remote_version_id = Uuid::new_v4().to_string();
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
        match reader {
            ReaderImpl::Body(bytes) => Ok(bytes.to_vec()),
            ReaderImpl::ObjectBody(mut reader) => {
                let mut buf = Vec::new();
                reader.stream.read_to_end(&mut buf).await?;
                Ok(buf)
            }
        }
    }
}

#[async_trait]
impl WarmBackend for MockWarmBackend {
    async fn put(&self, object: &str, r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
        self.precondition().await?;
        let bytes = self.read_bytes(r).await?;
        let version = self.put_bytes(object, bytes, HashMap::new()).await;
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
        self.record(MockWarmOp::Put {
            object: object.to_string(),
        })
        .await;
        Ok(version)
    }

    async fn get(&self, object: &str, _rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        self.precondition().await?;
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

    async fn remove(&self, object: &str, _rv: &str) -> Result<(), std::io::Error> {
        self.precondition().await?;
        self.inner.objects.lock().await.remove(object);
        self.record(MockWarmOp::Remove {
            object: object.to_string(),
        })
        .await;
        Ok(())
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        self.precondition().await?;
        self.record(MockWarmOp::InUse).await;
        Ok(false)
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
    tier_config_mgr.driver_cache.insert(tier_name.to_string(), Box::new(backend));
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
    let mut endpoint = Endpoint::try_from(disk_path.to_str()?).ok()?;
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
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
