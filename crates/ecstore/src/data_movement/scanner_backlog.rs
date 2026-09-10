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

use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::ObjectOptions;
use crate::object_api::{ObjectInfo, PutObjReader, WriteCompletion};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::object::HTTPPreconditions;
use crate::storage_api_contracts::object::ObjectIO as _;
use futures::future::join_all;
use http::HeaderMap;
use std::sync::{Arc, OnceLock};
use tokio::io::AsyncReadExt;

pub const MAX_SCANNER_PAUSE_BACKLOG_BYTES: u64 = 64 * 1024;
pub(crate) const SCANNER_PAUSE_BACKLOG_PATH: &str = "buckets/.scanner-pause-backlog.json";

/// A bounded, storage-fenced native replica. Only a confirmed missing object
/// has no payload; read failures never enter the Scanner verifier.
pub struct ScannerPauseBacklogRetirementReplica {
    pub pool_index: usize,
    pub set_index: usize,
    pub data: Option<Vec<u8>>,
}

/// Native records for a membership handoff. Existing durable ledgers are
/// preserved; an empty native bootstrap may initialize its first ledger.
pub struct ScannerPauseBacklogRetirementPlan {
    pub seed_record: Option<Vec<u8>>,
    pub commit_record: Vec<u8>,
    pub stable_record: Vec<u8>,
}

pub type ScannerPauseBacklogRetirementPlanner =
    fn(usize, &[ScannerPauseBacklogRetirementReplica]) -> std::result::Result<Option<ScannerPauseBacklogRetirementPlan>, String>;

static RETIREMENT_PLANNER: OnceLock<ScannerPauseBacklogRetirementPlanner> = OnceLock::new();

/// Install the stateless native record planner before storage starts workers.
/// The scanner runtime switch does not control this storage safety check.
pub fn register_scanner_pause_backlog_retirement_planner(planner: ScannerPauseBacklogRetirementPlanner) {
    RETIREMENT_PLANNER.get_or_init(|| planner);
}

pub(crate) fn is_scanner_pause_backlog(bucket: &str, object: &str) -> bool {
    bucket == RUSTFS_META_BUCKET && object == SCANNER_PAUSE_BACKLOG_PATH
}

pub(crate) struct ScannerPauseBacklogRetirementRead {
    pub replica: ScannerPauseBacklogRetirementReplica,
    pub etag: Option<String>,
}

impl ScannerPauseBacklogRetirementRead {
    pub(crate) fn preconditions(&self) -> HTTPPreconditions {
        match &self.etag {
            Some(etag) => HTTPPreconditions {
                if_match: Some(etag.clone()),
                ..Default::default()
            },
            None => HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
        }
    }
}

async fn read_replica(set: Arc<SetDisks>) -> Result<ScannerPauseBacklogRetirementRead> {
    let mut replica = ScannerPauseBacklogRetirementReplica {
        pool_index: set.pool_index,
        set_index: set.set_index,
        data: None,
    };
    let reader = match set
        .get_object_reader(
            RUSTFS_META_BUCKET,
            SCANNER_PAUSE_BACKLOG_PATH,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {
            return Ok(ScannerPauseBacklogRetirementRead { replica, etag: None });
        }
        Err(err) => return Err(err),
    };
    let info = &reader.object_info;
    if info.version_id.is_some_and(|version| !version.is_nil())
        || info.delete_marker
        || info.is_dir
        || info.etag.as_ref().is_none_or(String::is_empty)
        || info.size < 0
        || info.size > MAX_SCANNER_PAUSE_BACKLOG_BYTES as i64
    {
        return Err(Error::other("scanner pause backlog retirement found an unsupported replica identity"));
    }
    let etag = info.etag.clone();
    let expected_size = info.size as usize;
    let mut data = Vec::new();
    reader
        .take(MAX_SCANNER_PAUSE_BACKLOG_BYTES + 1)
        .read_to_end(&mut data)
        .await?;
    if data.len() != expected_size || data.len() > MAX_SCANNER_PAUSE_BACKLOG_BYTES as usize {
        return Err(Error::other("scanner pause backlog retirement replica has an invalid payload length"));
    }
    replica.data = Some(data);
    Ok(ScannerPauseBacklogRetirementRead { replica, etag })
}

/// The caller retains the fixed object write lock and durable topology read
/// fence through both this snapshot and physical source cleanup.
pub(crate) async fn read_scanner_pause_backlog_retirement_replicas(
    source_pool_index: usize,
    source_set_index: usize,
    sets: Vec<Arc<SetDisks>>,
) -> Result<Vec<ScannerPauseBacklogRetirementRead>> {
    let replicas = join_all(sets.into_iter().map(read_replica))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    if !replicas.iter().any(|read| {
        read.replica.pool_index == source_pool_index && read.replica.set_index == source_set_index && read.replica.data.is_some()
    }) {
        return Err(Error::other("scanner pause backlog retirement current source replica is missing"));
    }
    Ok(replicas)
}

pub(crate) fn plan_scanner_pause_backlog_retirement(
    source_pool_index: usize,
    replicas: &[ScannerPauseBacklogRetirementRead],
) -> Result<Option<ScannerPauseBacklogRetirementPlan>> {
    let planner = RETIREMENT_PLANNER
        .get()
        .ok_or_else(|| Error::other("scanner pause backlog native retirement planner is unavailable"))?;
    let snapshots = replicas
        .iter()
        .map(|read| ScannerPauseBacklogRetirementReplica {
            pool_index: read.replica.pool_index,
            set_index: read.replica.set_index,
            data: read.replica.data.clone(),
        })
        .collect::<Vec<_>>();
    planner(source_pool_index, &snapshots).map_err(Error::other)
}

/// The native writer and retirement handoff use the same conditional, full-tail
/// write. Their callers retain object and durable membership fences until return.
pub(crate) async fn persist_native_scanner_pause_backlog_replica(
    set: Arc<SetDisks>,
    data: Vec<u8>,
    preconditions: HTTPPreconditions,
    mut opts: ObjectOptions,
    _phase: &'static str,
) -> Result<ObjectInfo> {
    if data.len() > MAX_SCANNER_PAUSE_BACKLOG_BYTES as usize {
        return Err(Error::other("scanner pause backlog exceeds its size bound"));
    }
    opts.max_parity = true;
    opts.write_completion = WriteCompletion::TailDrained;
    opts.http_preconditions = Some(preconditions);
    #[cfg(feature = "test-util")]
    let fault = test_util::matching_write(&set, _phase)?;
    let result = set
        .put_object(RUSTFS_META_BUCKET, SCANNER_PAUSE_BACKLOG_PATH, &mut PutObjReader::from_vec(data), &opts)
        .await;
    #[cfg(feature = "test-util")]
    if result.is_ok()
        && let Some(fault) = fault
    {
        fault.arrived.notify_one();
        fault.release.notified().await;
    }
    result
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Debug, thiserror::Error)]
    #[error("injected native scanner backlog {phase} write failure")]
    struct InjectedWriteFailure {
        phase: &'static str,
    }

    pub(super) struct WriteFault {
        set: Arc<SetDisks>,
        phase: &'static str,
        remaining: AtomicUsize,
        fail_before_write: bool,
        pub(super) arrived: Notify,
        pub(super) release: Notify,
    }

    static WRITE_FAULTS: Mutex<Vec<Arc<WriteFault>>> = Mutex::new(Vec::new());

    /// Scope a one-shot fault to the actual set instance, so other stores and
    /// concurrent tests keep using the ordinary native persistence path.
    pub struct NativeScannerPauseBacklogWriteFault {
        state: Arc<WriteFault>,
    }

    impl NativeScannerPauseBacklogWriteFault {
        fn install(set: Arc<SetDisks>, phase: &'static str, nth: usize, fail_before_write: bool) -> Self {
            assert!(nth > 0);
            let state = Arc::new(WriteFault {
                set,
                phase,
                remaining: AtomicUsize::new(nth),
                fail_before_write,
                arrived: Notify::new(),
                release: Notify::new(),
            });
            let mut faults = WRITE_FAULTS.lock().unwrap();
            assert!(
                !faults
                    .iter()
                    .any(|fault| Arc::ptr_eq(&fault.set, &state.set) && fault.phase == phase)
            );
            faults.push(Arc::clone(&state));
            Self { state }
        }

        pub fn fail_before_write(set: Arc<SetDisks>, phase: &'static str, nth: usize) -> Self {
            Self::install(set, phase, nth, true)
        }

        pub fn pause_after_write(set: Arc<SetDisks>, phase: &'static str) -> Self {
            Self::install(set, phase, 1, false)
        }

        pub async fn wait_until_paused(&self) {
            self.state.arrived.notified().await;
        }

        pub fn release(&self) {
            self.state.release.notify_one();
        }
    }

    impl Drop for NativeScannerPauseBacklogWriteFault {
        fn drop(&mut self) {
            self.release();
            WRITE_FAULTS.lock().unwrap().retain(|fault| !Arc::ptr_eq(fault, &self.state));
        }
    }

    pub(super) fn matching_write(set: &Arc<SetDisks>, phase: &'static str) -> Result<Option<Arc<WriteFault>>> {
        let fault = WRITE_FAULTS
            .lock()
            .unwrap()
            .iter()
            .find(|fault| Arc::ptr_eq(&fault.set, set) && fault.phase == phase)
            .cloned();
        let Some(fault) = fault else { return Ok(None) };
        if fault
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
            != Ok(1)
        {
            return Ok(None);
        }
        if fault.fail_before_write {
            return Err(Error::other(InjectedWriteFailure { phase }));
        }
        Ok(Some(fault))
    }
}
