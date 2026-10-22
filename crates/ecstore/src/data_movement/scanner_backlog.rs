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

use crate::data_movement::data_movement_context_error;
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

/// Typed failure from the scanner-owned native retirement planner.
///
/// Decommission must preserve the source when the native authority cannot be
/// proven. The handoff variant is deliberately narrower than other authority
/// failures so the worker can retry without converting a transient membership
/// transition into a terminal pool failure.
#[derive(Debug, thiserror::Error)]
pub enum ScannerPauseBacklogRetirementError {
    #[error("scanner pause backlog retirement has a retryable handoff conflict: {reason}")]
    HandoffConflict { reason: String },
    #[error("scanner pause backlog retirement has an unsafe authority conflict: {reason}")]
    AuthorityConflict { reason: String },
    #[error("scanner pause backlog retirement found an invalid native record: {reason}")]
    InvalidRecord { reason: String },
}

impl ScannerPauseBacklogRetirementError {
    pub fn is_retryable_handoff_conflict(&self) -> bool {
        matches!(self, Self::HandoffConflict { .. })
    }
}

/// The same native planner runs before repair and after each durable phase.
/// Keep this context bounded without exposing replica payloads or revisions.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ScannerPauseBacklogRetirementPhase {
    InitialRead,
    SeedReadback,
    CommitReadback,
    StabilizeReadback,
    FinalCheck,
}

impl ScannerPauseBacklogRetirementPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::InitialRead => "initial_read",
            Self::SeedReadback => "seed_readback",
            Self::CommitReadback => "commit_readback",
            Self::StabilizeReadback => "stabilize_readback",
            Self::FinalCheck => "final_check",
        }
    }

    pub(crate) fn planner_error(self, err: ScannerPauseBacklogRetirementError) -> Error {
        data_movement_context_error(
            format!("scanner pause backlog retirement planner failed during {}: {err}", self.as_str()),
            err,
        )
    }
}

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
    fn(
        usize,
        &[ScannerPauseBacklogRetirementReplica],
    ) -> std::result::Result<Option<ScannerPauseBacklogRetirementPlan>, ScannerPauseBacklogRetirementError>;

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
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };
    let read = set.get_object_reader(RUSTFS_META_BUCKET, SCANNER_PAUSE_BACKLOG_PATH, None, HeaderMap::new(), &opts);
    #[cfg(feature = "diagnostic-read-version-caller")]
    let read = crate::cluster::rpc::runtime_sources::with_read_version_entry(
        rustfs_io_metrics::internode_metrics::ReadVersionEntrySource::ScannerBacklogRead,
        read,
    );
    let reader = match read.await {
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

#[derive(Debug, thiserror::Error)]
#[error("scanner pause backlog native retirement planner is unavailable during {phase}")]
struct ScannerPauseBacklogPlannerUnavailable {
    phase: &'static str,
}

pub(crate) fn plan_scanner_pause_backlog_retirement(
    source_pool_index: usize,
    replicas: &[ScannerPauseBacklogRetirementRead],
    phase: ScannerPauseBacklogRetirementPhase,
) -> Result<Option<ScannerPauseBacklogRetirementPlan>> {
    let planner = RETIREMENT_PLANNER
        .get()
        .ok_or_else(|| Error::other(ScannerPauseBacklogPlannerUnavailable { phase: phase.as_str() }))?;
    let snapshots = replicas
        .iter()
        .map(|read| ScannerPauseBacklogRetirementReplica {
            pool_index: read.replica.pool_index,
            set_index: read.replica.set_index,
            data: read.replica.data.clone(),
        })
        .collect::<Vec<_>>();
    planner(source_pool_index, &snapshots).map_err(|err| phase.planner_error(err))
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
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_movement::data_movement_stage_source_as;

    fn failing_planner(
        source_pool_index: usize,
        replicas: &[ScannerPauseBacklogRetirementReplica],
    ) -> std::result::Result<Option<ScannerPauseBacklogRetirementPlan>, ScannerPauseBacklogRetirementError> {
        assert!(replicas.is_empty(), "the callback fixture needs no storage replicas");
        Err(match source_pool_index {
            0 => ScannerPauseBacklogRetirementError::HandoffConflict {
                reason: "original reason".to_owned(),
            },
            1 => ScannerPauseBacklogRetirementError::AuthorityConflict {
                reason: "original reason".to_owned(),
            },
            2 => ScannerPauseBacklogRetirementError::InvalidRecord {
                reason: "original reason".to_owned(),
            },
            _ => panic!("unexpected planner fixture source {source_pool_index}"),
        })
    }

    #[test]
    fn retirement_planner_phase_context_preserves_every_typed_error() {
        const TEST_NAME: &str =
            "data_movement::scanner_backlog::tests::retirement_planner_phase_context_preserves_every_typed_error";
        const CHILD: &str = "RUSTFS_TEST_RETIREMENT_PLANNER_PHASE_CHILD";
        if std::env::var(CHILD).ok().as_deref() != Some(TEST_NAME) {
            // Registration is process-global and must not replace another test's planner.
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", TEST_NAME, "--nocapture", "--color", "never"])
                .env(CHILD, TEST_NAME)
                .output()
                .expect("start isolated retirement planner test");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let one_passed = stdout
                .lines()
                .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;"));
            assert!(
                output.status.success() && one_passed,
                "isolated planner regression must execute exactly one passing test: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }

        let phases = [
            (ScannerPauseBacklogRetirementPhase::InitialRead, "initial_read"),
            (ScannerPauseBacklogRetirementPhase::SeedReadback, "seed_readback"),
            (ScannerPauseBacklogRetirementPhase::CommitReadback, "commit_readback"),
            (ScannerPauseBacklogRetirementPhase::StabilizeReadback, "stabilize_readback"),
            (ScannerPauseBacklogRetirementPhase::FinalCheck, "final_check"),
        ];
        for (phase, label) in phases {
            let unavailable = match plan_scanner_pause_backlog_retirement(0, &[], phase) {
                Ok(_) => panic!("unregistered planner must fail during {label}"),
                Err(error) => error,
            };
            assert_eq!(
                unavailable.to_string(),
                format!("Io error: scanner pause backlog native retirement planner is unavailable during {label}"),
            );
            assert!(data_movement_stage_source_as::<ScannerPauseBacklogRetirementError>(&unavailable).is_none());
        }
        register_scanner_pause_backlog_retirement_planner(failing_planner);
        for (phase, label) in phases {
            for (source_pool_index, source) in [
                ScannerPauseBacklogRetirementError::HandoffConflict {
                    reason: "original reason".to_owned(),
                },
                ScannerPauseBacklogRetirementError::AuthorityConflict {
                    reason: "original reason".to_owned(),
                },
                ScannerPauseBacklogRetirementError::InvalidRecord {
                    reason: "original reason".to_owned(),
                },
            ]
            .into_iter()
            .enumerate()
            {
                let discriminant = std::mem::discriminant(&source);
                let retryable = source.is_retryable_handoff_conflict();
                let original = source.to_string();
                let error = match plan_scanner_pause_backlog_retirement(source_pool_index, &[], phase) {
                    Ok(_) => panic!("the registered planner fixture must fail during {label}"),
                    Err(error) => error,
                };
                assert_eq!(
                    error.to_string(),
                    format!("Io error: scanner pause backlog retirement planner failed during {label}: {original}")
                );
                let recovered = data_movement_stage_source_as::<ScannerPauseBacklogRetirementError>(&error)
                    .expect("the worker classifier must still reach the original native error");
                assert_eq!(std::mem::discriminant(recovered), discriminant);
                assert_eq!(recovered.to_string(), original);
                assert_eq!(recovered.is_retryable_handoff_conflict(), retryable);
                let mut source = std::error::Error::source(&error);
                let mut found_native = false;
                while let Some(cause) = source {
                    found_native |= cause.downcast_ref::<ScannerPauseBacklogRetirementError>().is_some();
                    source = cause.source();
                }
                assert!(found_native, "the standard source chain must retain the native error during {label}");
            }
        }
    }
}
