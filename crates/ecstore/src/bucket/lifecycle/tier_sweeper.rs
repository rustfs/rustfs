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
use crate::global::GLOBAL_TierConfigMgr;
use rustfs_utils::get_env_usize;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::warn;
use uuid::Uuid;
use xxhash_rust::xxh64;

static XXHASH_SEED: u64 = 0;

const ENV_REMOTE_DELETE_MAX_CONCURRENCY: &str = "RUSTFS_REMOTE_DELETE_MAX_CONCURRENCY";
const ENV_REMOTE_DELETE_BREAKER_THRESHOLD: &str = "RUSTFS_REMOTE_DELETE_BREAKER_THRESHOLD";
const ENV_REMOTE_DELETE_BREAKER_WINDOW_SECS: &str = "RUSTFS_REMOTE_DELETE_BREAKER_WINDOW_SECS";
const DEFAULT_REMOTE_DELETE_BREAKER_THRESHOLD: usize = 50;
const DEFAULT_REMOTE_DELETE_BREAKER_WINDOW_SECS: usize = 30;
const METRIC_DELETE_REMOTE_FAILED_TOTAL: &str = "delete.remote.failed_total";
const METRIC_DELETE_REMOTE_BREAKER_TOTAL: &str = "delete.remote.breaker_total";
const METRIC_DELETE_REMOTE_INFLIGHT: &str = "delete.remote.inflight";

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
    let message = err.to_string().to_ascii_lowercase();
    message.contains("invalid utf-8 header value")
        || message.contains("invalidheadervalue")
        || (message.contains("sign v4") && message.contains("header value"))
}

fn remote_delete_breaker_is_open(now: Instant) -> bool {
    let mut breaker = match REMOTE_DELETE_BREAKER.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    breaker.should_short_circuit(now)
}

fn record_remote_delete_failure(err: &std::io::Error, now: Instant) {
    metrics::counter!(METRIC_DELETE_REMOTE_FAILED_TOTAL).increment(1);

    if !is_signer_header_error(err) {
        return;
    }

    let mut breaker = match REMOTE_DELETE_BREAKER.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if breaker.record_signer_failure(now) {
        warn!(
            threshold = breaker.threshold,
            window_secs = breaker.window.as_secs(),
            "remote tier delete breaker opened by signer/header failures"
        );
    }
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

    pub async fn sweep(&self) {
        let Some(je) = self.should_remove_remote_object() else {
            return;
        };
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
    if remote_delete_breaker_is_open(Instant::now()) {
        metrics::counter!(METRIC_DELETE_REMOTE_BREAKER_TOTAL).increment(1);
        return Err(std::io::Error::other("remote tier delete breaker is open due to signer/header failures"));
    }

    let _permit = REMOTE_DELETE_LIMITER
        .acquire()
        .await
        .map_err(|_| std::io::Error::other("remote tier delete limiter is closed"))?;
    let _inflight = RemoteDeleteInflightGuard::new();

    let mut config_mgr = GLOBAL_TierConfigMgr.write().await;
    let w = match config_mgr.get_driver(tier_name).await {
        Ok(w) => w,
        Err(e) => {
            let err = std::io::Error::other(e);
            record_remote_delete_failure(&err, Instant::now());
            return Err(err);
        }
    };
    let result = w.remove(obj_name, rv_id).await;
    if let Err(err) = &result {
        record_remote_delete_failure(err, Instant::now());
    }
    result
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
    use super::{RemoteDeleteBreaker, is_signer_header_error};
    use std::io::{Error, ErrorKind};
    use std::time::{Duration, Instant};

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
