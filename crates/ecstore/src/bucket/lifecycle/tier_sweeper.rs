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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::bucket::lifecycle::bucket_lifecycle_ops::{ExpiryOp, GLOBAL_ExpiryState, TransitionedObject};
use crate::bucket::lifecycle::lifecycle::{self, ObjectOpts};
use crate::bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry;
use crate::client::signer_error::error_chain_contains_signer_header_marker;
use crate::global::GLOBAL_TierConfigMgr;
use crate::store::ECStore;
use rustfs_utils::get_env_usize;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tracing::warn;
use uuid::Uuid;
use xxhash_rust::xxh64;

static XXHASH_SEED: u64 = 0;

const ENV_REMOTE_DELETE_MAX_CONCURRENCY: &str = "RUSTFS_REMOTE_DELETE_MAX_CONCURRENCY";
const ENV_REMOTE_DELETE_BREAKER_THRESHOLD: &str = "RUSTFS_REMOTE_DELETE_BREAKER_THRESHOLD";
const ENV_REMOTE_DELETE_BREAKER_WINDOW_SECS: &str = "RUSTFS_REMOTE_DELETE_BREAKER_WINDOW_SECS";
const DEFAULT_REMOTE_DELETE_BREAKER_THRESHOLD: usize = 50;
const DEFAULT_REMOTE_DELETE_BREAKER_WINDOW_SECS: usize = 30;
const METRIC_DELETE_REMOTE_FAILED_TOTAL: &str = "rustfs_delete_remote_failed_total";
const METRIC_DELETE_REMOTE_BREAKER_TOTAL: &str = "rustfs_delete_remote_breaker_total";
const METRIC_DELETE_REMOTE_INFLIGHT: &str = "rustfs_delete_remote_inflight";
const ERR_REMOTE_DELETE_BREAKER_OPEN: &str = "remote tier delete breaker is open due to signer/header failures";
const ERR_REMOTE_DELETE_LIMITER_CLOSED: &str = "remote tier delete limiter is closed";

static REMOTE_DELETE_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

static REMOTE_DELETE_LIMITER: LazyLock<Semaphore> = LazyLock::new(|| {
    let default_limit = std::cmp::min(num_cpus::get(), 16).max(1);
    let concurrency = get_env_usize(ENV_REMOTE_DELETE_MAX_CONCURRENCY, default_limit).max(1);
    Semaphore::new(concurrency)
});

static REMOTE_DELETE_BREAKER: LazyLock<Mutex<RemoteDeleteBreaker>> = LazyLock::new(|| {
    Mutex::new(RemoteDeleteBreaker::new(
        get_env_usize(ENV_REMOTE_DELETE_BREAKER_THRESHOLD, DEFAULT_REMOTE_DELETE_BREAKER_THRESHOLD).max(1),
        Duration::from_secs(
            get_env_usize(ENV_REMOTE_DELETE_BREAKER_WINDOW_SECS, DEFAULT_REMOTE_DELETE_BREAKER_WINDOW_SECS) as u64,
        ),
    ))
});

#[cfg(test)]
static REMOTE_TIER_DELETE_TEST_HOOK: std::sync::LazyLock<
    std::sync::Mutex<Option<Box<dyn Fn(&str, &str, &str) -> std::io::Result<()> + Send + Sync>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[derive(Debug)]
struct RemoteDeleteBreaker {
    threshold: usize,
    window: Duration,
    failures: VecDeque<Instant>,
}

impl RemoteDeleteBreaker {
    fn new(threshold: usize, window: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            window: window.max(Duration::from_secs(1)),
            failures: VecDeque::new(),
        }
    }

    fn should_short_circuit(&mut self, now: Instant) -> bool {
        self.prune(now);
        self.failures.len() >= self.threshold
    }

    fn record_signer_failure(&mut self, now: Instant) -> bool {
        self.prune(now);
        let was_open = self.failures.len() >= self.threshold;
        self.failures.push_back(now);
        !was_open && self.failures.len() >= self.threshold
    }

    fn prune(&mut self, now: Instant) {
        while let Some(ts) = self.failures.front().copied() {
            if now.duration_since(ts) > self.window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }
}

struct RemoteDeleteInflightGuard;

impl RemoteDeleteInflightGuard {
    fn new() -> Self {
        let inflight = REMOTE_DELETE_INFLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(METRIC_DELETE_REMOTE_INFLIGHT).set(inflight as f64);
        Self
    }
}

impl Drop for RemoteDeleteInflightGuard {
    fn drop(&mut self) {
        let inflight = REMOTE_DELETE_INFLIGHT.fetch_sub(1, Ordering::Relaxed) - 1;
        metrics::gauge!(METRIC_DELETE_REMOTE_INFLIGHT).set(inflight as f64);
    }
}

fn is_signer_header_error(err: &std::io::Error) -> bool {
    if err.kind() != std::io::ErrorKind::InvalidInput {
        return false;
    }

    if let Some(source) = err.get_ref() {
        if error_chain_contains_signer_header_marker(source) {
            return true;
        }
    }

    let message = err.to_string().to_ascii_lowercase();
    message.contains("invalid utf-8 header value")
        || message.contains("invalidheadervalue")
        || (message.contains("sign v4") && message.contains("header value"))
}

async fn remote_delete_breaker_is_open(now: Instant) -> bool {
    let mut breaker = REMOTE_DELETE_BREAKER.lock().await;
    breaker.should_short_circuit(now)
}

async fn record_remote_delete_failure(err: &std::io::Error, now: Instant) {
    metrics::counter!(METRIC_DELETE_REMOTE_FAILED_TOTAL).increment(1);

    if !is_signer_header_error(err) {
        return;
    }

    let mut breaker = REMOTE_DELETE_BREAKER.lock().await;
    if breaker.record_signer_failure(now) {
        warn!(
            threshold = breaker.threshold,
            window_secs = breaker.window.as_secs(),
            "remote tier delete breaker opened by signer/header failures"
        );
    }
}

fn should_record_remote_delete_failure(err: &std::io::Error) -> bool {
    let message = err.to_string();
    message != ERR_REMOTE_DELETE_BREAKER_OPEN && message != ERR_REMOTE_DELETE_LIMITER_CLOSED
}

#[derive(Default)]
#[allow(dead_code)]
struct ObjSweeper {
    object: String,
    bucket: String,
    version_id: Option<Uuid>,
    versioned: bool,
    suspended: bool,
    transition_status: String,
    transition_tier: String,
    transition_version_id: String,
    remote_object: String,
}

#[allow(dead_code)]
impl ObjSweeper {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(bucket: &str, object: &str) -> Result<Self, std::io::Error> {
        Ok(Self {
            object: object.into(),
            bucket: bucket.into(),
            ..Default::default()
        })
    }

    pub fn with_version(&mut self, vid: Option<Uuid>) -> &Self {
        self.version_id = vid.clone();
        self
    }

    pub fn with_versioning(&mut self, versioned: bool, suspended: bool) -> &Self {
        self.versioned = versioned;
        self.suspended = suspended;
        self
    }

    pub fn get_opts(&self) -> lifecycle::ObjectOpts {
        let mut opts = ObjectOpts {
            version_id: self.version_id.clone(),
            versioned: self.versioned,
            version_suspended: self.suspended,
            ..Default::default()
        };
        if self.suspended && self.version_id.is_none_or(|v| v.is_nil()) {
            opts.version_id = None;
        }
        opts
    }

    pub fn set_transition_state(&mut self, info: TransitionedObject) {
        self.transition_tier = info.tier;
        self.transition_status = info.status;
        self.remote_object = info.name;
        self.transition_version_id = info.version_id;
    }

    pub fn should_remove_remote_object(&self) -> Option<Jentry> {
        if self.transition_status != lifecycle::TRANSITION_COMPLETE {
            return None;
        }

        let mut del_tier = false;
        if !self.versioned || self.suspended {
            // 1, 2.a, 2.b
            del_tier = true;
        } else if self.versioned && self.version_id.is_some_and(|v| !v.is_nil()) {
            // 3.a
            del_tier = true;
        }
        if del_tier {
            return Some(Jentry {
                obj_name: self.remote_object.clone(),
                version_id: self.transition_version_id.clone(),
                tier_name: self.transition_tier.clone(),
            });
        }
        None
    }

    pub async fn sweep(&self, api: Arc<ECStore>) {
        let Some(je) = self.should_remove_remote_object() else {
            return;
        };
        if persist_tier_delete_journal_entry(api, &je).await.is_err() {
            GLOBAL_ExpiryState.write().await.increment_missed_tier_journal_tasks();
            return;
        }
        let hash = je.op_hash();
        // Grab the sender under a short read lock, then release the lock so we
        // don't hold it across the async send.
        let wrkr = GLOBAL_ExpiryState.read().await.get_worker_ch(hash);
        let Some(wrkr) = wrkr else {
            GLOBAL_ExpiryState.write().await.increment_missed_tier_journal_tasks();
            return;
        };
        if wrkr.send(Some(Box::new(je))).await.is_err() {
            GLOBAL_ExpiryState.write().await.increment_missed_tier_journal_tasks();
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused_assignments)]
pub struct Jentry {
    pub(crate) obj_name: String,
    pub(crate) version_id: String,
    pub(crate) tier_name: String,
}

impl ExpiryOp for Jentry {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}", self.tier_name).as_bytes());
        hasher.update(format!("{}", self.obj_name).as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub async fn delete_object_from_remote_tier(obj_name: &str, rv_id: &str, tier_name: &str) -> Result<(), std::io::Error> {
    let result = delete_object_from_remote_tier_raw(obj_name, rv_id, tier_name).await;
    if let Err(err) = &result
        && should_record_remote_delete_failure(err)
    {
        record_remote_delete_failure(err, Instant::now()).await;
    }
    result
}

async fn delete_object_from_remote_tier_raw(obj_name: &str, rv_id: &str, tier_name: &str) -> Result<(), std::io::Error> {
    #[cfg(test)]
    if let Some(result) = run_remote_tier_delete_test_hook(obj_name, rv_id, tier_name) {
        return result;
    }

    if remote_delete_breaker_is_open(Instant::now()).await {
        metrics::counter!(METRIC_DELETE_REMOTE_BREAKER_TOTAL).increment(1);
        return Err(std::io::Error::other(ERR_REMOTE_DELETE_BREAKER_OPEN));
    }

    let _permit = REMOTE_DELETE_LIMITER
        .acquire()
        .await
        .map_err(|_| std::io::Error::other(ERR_REMOTE_DELETE_LIMITER_CLOSED))?;
    let _inflight = RemoteDeleteInflightGuard::new();

    let mut config_mgr = GLOBAL_TierConfigMgr.write().await;
    let w = match config_mgr.get_driver(tier_name).await {
        Ok(w) => w,
        Err(e) => return Err(std::io::Error::other(e)),
    };
    w.remove(obj_name, rv_id).await
}

#[cfg(test)]
fn run_remote_tier_delete_test_hook(obj_name: &str, rv_id: &str, tier_name: &str) -> Option<std::io::Result<()>> {
    REMOTE_TIER_DELETE_TEST_HOOK
        .lock()
        .expect("remote tier delete test hook lock should not poison")
        .as_ref()
        .map(|hook| hook(obj_name, rv_id, tier_name))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteTierDeleteOutcome {
    Deleted,
    AlreadyRemoved,
}

pub async fn delete_object_from_remote_tier_idempotent(
    obj_name: &str,
    rv_id: &str,
    tier_name: &str,
) -> Result<RemoteTierDeleteOutcome, std::io::Error> {
    match delete_object_from_remote_tier_raw(obj_name, rv_id, tier_name).await {
        Ok(()) => Ok(RemoteTierDeleteOutcome::Deleted),
        Err(err) if is_remote_tier_not_found_error(&err) => Ok(RemoteTierDeleteOutcome::AlreadyRemoved),
        Err(err) => {
            if should_record_remote_delete_failure(&err) {
                record_remote_delete_failure(&err, Instant::now()).await;
            }
            Err(err)
        }
    }
}

pub(crate) fn is_remote_tier_not_found_error(err: &std::io::Error) -> bool {
    let message = err.to_string();
    message.contains("NoSuchKey")
        || message.contains("NoSuchVersion")
        || message.contains("ObjectNotFound")
        || message.contains("VersionNotFound")
}

pub fn transitioned_delete_journal_entry(
    version_id: Option<Uuid>,
    versioned: bool,
    suspended: bool,
    transitioned: &TransitionedObject,
) -> Option<Jentry> {
    let sweeper = ObjSweeper {
        version_id,
        versioned,
        suspended,
        transition_status: transitioned.status.clone(),
        transition_tier: transitioned.tier.clone(),
        transition_version_id: transitioned.version_id.clone(),
        remote_object: transitioned.name.clone(),
        ..Default::default()
    };

    sweeper.should_remove_remote_object()
}

pub fn transitioned_force_delete_journal_entry(transitioned: &TransitionedObject) -> Option<Jentry> {
    if transitioned.status != lifecycle::TRANSITION_COMPLETE {
        return None;
    }

    Some(Jentry {
        obj_name: transitioned.name.clone(),
        version_id: transitioned.version_id.clone(),
        tier_name: transitioned.tier.clone(),
    })
}

#[cfg(test)]
mod test {
    use crate::client::signer_error::invalid_utf8_header_error;

    use super::{
        ERR_REMOTE_DELETE_BREAKER_OPEN, ERR_REMOTE_DELETE_LIMITER_CLOSED, REMOTE_TIER_DELETE_TEST_HOOK, RemoteDeleteBreaker,
        RemoteTierDeleteOutcome, delete_object_from_remote_tier_idempotent, is_remote_tier_not_found_error,
        is_signer_header_error, should_record_remote_delete_failure,
    };
    use std::io::{Error, ErrorKind};
    use std::time::{Duration, Instant};

    struct RemoteTierDeleteHookGuard;

    impl Drop for RemoteTierDeleteHookGuard {
        fn drop(&mut self) {
            let mut hook = REMOTE_TIER_DELETE_TEST_HOOK
                .lock()
                .expect("remote tier delete test hook lock should not poison");
            *hook = None;
        }
    }

    fn set_remote_tier_delete_test_hook(
        hook_fn: impl Fn(&str, &str, &str) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> RemoteTierDeleteHookGuard {
        let mut hook = REMOTE_TIER_DELETE_TEST_HOOK
            .lock()
            .expect("remote tier delete test hook lock should not poison");
        *hook = Some(Box::new(hook_fn));
        RemoteTierDeleteHookGuard
    }

    #[test]
    fn signer_header_error_detection_matches_utf8_failures() {
        let err = Error::new(
            ErrorKind::InvalidInput,
            "failed to sign v4 request: invalid UTF-8 header value for `x-amz-meta-invalid`",
        );
        assert!(is_signer_header_error(&err));
    }

    #[test]
    fn signer_header_error_detection_rejects_unrelated_errors() {
        let err = Error::other("dial tcp: i/o timeout");
        assert!(!is_signer_header_error(&err));
    }

    #[test]
    fn signer_header_error_detection_matches_structured_marker() {
        let err = invalid_utf8_header_error("failed to sign v4 request", "x-amz-meta-invalid");
        assert!(is_signer_header_error(&err));
    }

    #[test]
    fn remote_tier_not_found_errors_are_idempotent_success() {
        assert!(is_remote_tier_not_found_error(&Error::other("NoSuchVersion")));
        assert!(is_remote_tier_not_found_error(&Error::other("NoSuchKey")));
        assert!(is_remote_tier_not_found_error(&Error::other("ObjectNotFound")));
        assert!(is_remote_tier_not_found_error(&Error::other("VersionNotFound")));
        assert!(!is_remote_tier_not_found_error(&Error::other("timeout")));
        assert!(!is_remote_tier_not_found_error(&Error::other("tier config not found")));
        assert!(!is_remote_tier_not_found_error(&Error::other("driver not found")));
    }

    #[test]
    fn remote_tier_control_plane_short_circuit_errors_do_not_count_as_delete_failures() {
        assert!(!should_record_remote_delete_failure(&Error::other(ERR_REMOTE_DELETE_BREAKER_OPEN)));
        assert!(!should_record_remote_delete_failure(&Error::other(ERR_REMOTE_DELETE_LIMITER_CLOSED)));
        assert!(should_record_remote_delete_failure(&Error::other("driver not found")));
        assert!(should_record_remote_delete_failure(&Error::other("NoSuchVersion")));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn idempotent_remote_delete_treats_hooked_nosuchversion_as_already_removed() {
        let _hook = set_remote_tier_delete_test_hook(|obj_name, rv_id, tier_name| {
            assert_eq!(obj_name, "remote/object");
            assert_eq!(rv_id, "remote-version");
            assert_eq!(tier_name, "WARM");
            Err(Error::other("NoSuchVersion"))
        });

        let outcome = delete_object_from_remote_tier_idempotent("remote/object", "remote-version", "WARM")
            .await
            .expect("remote not-found should be idempotent success");

        assert_eq!(outcome, RemoteTierDeleteOutcome::AlreadyRemoved);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn idempotent_remote_delete_preserves_driver_not_found_failures() {
        let _hook = set_remote_tier_delete_test_hook(|_, _, _| Err(Error::other("driver not found")));

        let err = delete_object_from_remote_tier_idempotent("remote/object", "remote-version", "WARM")
            .await
            .expect_err("driver lookup failure must not be idempotent success");

        assert!(err.to_string().contains("driver not found"));
    }

    #[test]
    fn breaker_opens_at_threshold_and_recovers_after_window() {
        let mut breaker = RemoteDeleteBreaker::new(3, Duration::from_secs(30));
        let start = Instant::now();

        assert!(!breaker.should_short_circuit(start));
        assert!(!breaker.record_signer_failure(start));
        assert!(!breaker.record_signer_failure(start + Duration::from_secs(1)));
        assert!(breaker.record_signer_failure(start + Duration::from_secs(2)));
        assert!(breaker.should_short_circuit(start + Duration::from_secs(3)));
        assert!(!breaker.should_short_circuit(start + Duration::from_secs(40)));
    }
}
