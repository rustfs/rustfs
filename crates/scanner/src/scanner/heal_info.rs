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
/// The background-heal info object persisted between scanner cycles.
use super::*;
use std::sync::LazyLock as StdLazyLock;
use std::sync::RwLock as StdRwLock;

/// Background healing information
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundHealInfo {
    /// Bitrot scan start time
    pub bitrot_start_time: Option<DateTime<Utc>>,
    /// Bitrot scan start cycle
    pub bitrot_start_cycle: u64,
    /// Current scan mode
    pub current_scan_mode: HealScanMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BackgroundHealInfoReadStatus {
    Loaded,
    Missing,
    Blocked,
    Transient,
    Failed,
}

pub(super) struct BackgroundHealInfoRead {
    pub(super) info: BackgroundHealInfo,
    pub(super) expected_epoch: Option<u64>,
    pub(super) status: BackgroundHealInfoReadStatus,
}

pub(super) fn classify_background_heal_read_error(error: &EcstoreError) -> BackgroundHealInfoReadStatus {
    if matches!(error, EcstoreError::ConfigNotFound) {
        BackgroundHealInfoReadStatus::Missing
    } else {
        BackgroundHealInfoReadStatus::Transient
    }
}

pub(super) fn decode_background_heal_info(data: &[u8]) -> Result<BackgroundHealInfo, serde_json::Error> {
    serde_json::from_slice(data)
}

/// Read background healing information from storage
pub async fn read_background_heal_info(storeapi: Arc<ECStore>) -> BackgroundHealInfo {
    read_background_heal_info_with_epoch(storeapi).await.info
}

/// Read background healing information together with the movement epoch that
/// fenced the read. The epoch must be reused by the matching cycle update so a
/// missing-object default cannot be committed across a movement transition.
pub(super) async fn read_background_heal_info_with_epoch<S>(storeapi: Arc<S>) -> BackgroundHealInfoRead
where
    S: ScannerStorage,
{
    let expected_epoch = scanner_publication_epoch(storeapi.clone()).await;
    if expected_epoch.is_none() {
        return BackgroundHealInfoRead {
            info: BackgroundHealInfo::default(),
            expected_epoch,
            status: BackgroundHealInfoReadStatus::Blocked,
        };
    }

    // Get last healing information
    match read_config(storeapi, &BACKGROUND_HEAL_INFO_PATH).await {
        Ok(buf) => match decode_background_heal_info(&buf) {
            Ok(info) => BackgroundHealInfoRead {
                info,
                expected_epoch,
                status: BackgroundHealInfoReadStatus::Loaded,
            },
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                    path = %&*BACKGROUND_HEAL_INFO_PATH,
                    state = "decode_failed",
                    error = %e,
                    "Scanner background heal decode failed"
                );
                BackgroundHealInfoRead {
                    info: BackgroundHealInfo::default(),
                    expected_epoch,
                    status: BackgroundHealInfoReadStatus::Failed,
                }
            }
        },
        Err(e) => {
            let status = classify_background_heal_read_error(&e);
            if status == BackgroundHealInfoReadStatus::Transient {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                    path = %&*BACKGROUND_HEAL_INFO_PATH,
                    state = "read_failed",
                    error = %e,
                    "Scanner background heal read failed"
                );
            }
            BackgroundHealInfoRead {
                info: BackgroundHealInfo::default(),
                expected_epoch,
                status,
            }
        }
    }
}

/// Save background healing information to storage
#[instrument(skip(storeapi))]
pub async fn save_background_heal_info(storeapi: Arc<ECStore>, info: BackgroundHealInfo) {
    save_background_heal_info_for_epoch(storeapi, info, None).await;
}

pub(super) async fn save_background_heal_info_for_epoch<S>(
    storeapi: Arc<S>,
    info: BackgroundHealInfo,
    expected_epoch: Option<u64>,
) where
    S: ScannerStorage,
{
    // Remember the record before any durability check: a skipped or failed
    // save must not erase the cadence start this process decided on.
    remember_background_heal_info(&info);

    // Serialize to JSON
    let data = match serde_json::to_vec(&info) {
        Ok(data) => data,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                path = %&*BACKGROUND_HEAL_INFO_PATH,
                state = "encode_failed",
                error = %e,
                "Scanner background heal encode failed"
            );
            return;
        }
    };

    // Save configuration only after storage-owned movement admission. The
    // read path may return an in-memory default for a missing object, but a
    // movement transition must not let that default become durable state.
    let publication_admission = match expected_epoch {
        Some(expected_epoch) => scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch).await,
        None => storeapi.scanner_data_usage_publication_admission().await,
    };
    let Some(_publication_admission) = publication_admission else {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
            path = %&*BACKGROUND_HEAL_INFO_PATH,
            state = "publication_admission_unavailable",
            "Scanner background heal save skipped without movement admission"
        );
        return;
    };
    if let Err(e) = save_config(storeapi, &BACKGROUND_HEAL_INFO_PATH, data).await {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
            path = %&*BACKGROUND_HEAL_INFO_PATH,
            state = "save_failed",
            error = %e,
            "Scanner background heal save failed"
        );
    }
}

/// Where the bitrot cadence start used by a cycle came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerBitrotStartSource {
    /// No cycle has recorded a start yet.
    None,
    /// This process recorded the start; storage has not returned it yet.
    Memory,
    /// The start was read back from the persisted background-heal object.
    Persisted,
}

impl ScannerBitrotStartSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Memory => "memory",
            Self::Persisted => "persisted",
        }
    }
}

/// The bitrot cadence reference a cycle decides its scan mode from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScannerBitrotStart {
    /// Nothing recorded and no anchor; a cycle never decides from this.
    None,
    /// A Deep selection window opened at `cycle` / `started`, from the
    /// persisted or the remembered record.
    Window { cycle: u64, started: DateTime<Utc> },
    /// No Deep window is recorded anywhere; the cadence counts from the first
    /// cycle this process ran without one. Never persisted, so it opens no
    /// selection window and old readers see no new state.
    Anchor(DateTime<Utc>),
}

impl ScannerBitrotStart {
    pub(super) fn kind(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Window { .. } => "deep_window",
            Self::Anchor(_) => "anchor",
        }
    }

    fn cycle(self) -> u64 {
        match self {
            Self::Window { cycle, .. } => cycle,
            Self::None | Self::Anchor(_) => 0,
        }
    }

    fn started(self) -> Option<DateTime<Utc>> {
        match self {
            Self::None => None,
            Self::Window { started, .. } => Some(started),
            Self::Anchor(anchored) => Some(anchored),
        }
    }
}

/// How the scanner decided the scan mode of the running or latest cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ScannerBitrotStatus {
    /// `false` on an erasure single-disk deployment, where scanner object
    /// heal sampling is unsupported and Deep is never selected.
    pub deep_scan_supported: bool,
    pub bitrot_start_source: ScannerBitrotStartSource,
    /// `deep_window` when a recorded Deep window start is in effect,
    /// `anchor` when only this process's in-memory cadence anchor exists.
    pub bitrot_start_kind: &'static str,
    pub bitrot_start_cycle: u64,
    pub bitrot_start_unix_secs: u64,
    pub current_scan_mode: &'static str,
}

impl Default for ScannerBitrotStatus {
    fn default() -> Self {
        Self {
            deep_scan_supported: false,
            bitrot_start_source: ScannerBitrotStartSource::None,
            bitrot_start_kind: ScannerBitrotStart::None.kind(),
            bitrot_start_cycle: 0,
            bitrot_start_unix_secs: 0,
            current_scan_mode: HealScanMode::Unknown.as_str(),
        }
    }
}

/// Process-lifetime copy of the last background-heal record this node
/// decided on, so a skipped or lost save does not erase the cadence start.
static REMEMBERED_BACKGROUND_HEAL_INFO: StdRwLock<Option<BackgroundHealInfo>> = StdRwLock::new(None);

static SCANNER_BITROT_STATUS: StdLazyLock<StdRwLock<ScannerBitrotStatus>> =
    StdLazyLock::new(|| StdRwLock::new(ScannerBitrotStatus::default()));

/// When this process first ran a cycle without any recorded Deep window.
static SCANNER_BITROT_ANCHOR: StdRwLock<Option<DateTime<Utc>>> = StdRwLock::new(None);

fn bitrot_cadence_anchor(now: DateTime<Utc>) -> DateTime<Utc> {
    let mut anchor = SCANNER_BITROT_ANCHOR.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    *anchor.get_or_insert(now)
}

#[cfg(test)]
pub(super) fn reset_bitrot_cadence_anchor_for_test() {
    *SCANNER_BITROT_ANCHOR.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

/// The cadence reference for a cycle: the recorded Deep window start when the
/// record carries one, otherwise this process's in-memory anchor.
pub(super) fn scanner_bitrot_start_for_cycle(info: &BackgroundHealInfo, now: DateTime<Utc>) -> ScannerBitrotStart {
    match info.bitrot_start_time {
        Some(started) => ScannerBitrotStart::Window {
            cycle: info.bitrot_start_cycle,
            started,
        },
        None => ScannerBitrotStart::Anchor(bitrot_cadence_anchor(now)),
    }
}

pub(super) fn remember_background_heal_info(info: &BackgroundHealInfo) {
    *REMEMBERED_BACKGROUND_HEAL_INFO
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(info.clone());
}

fn remembered_background_heal_info() -> Option<BackgroundHealInfo> {
    REMEMBERED_BACKGROUND_HEAL_INFO
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Pick the record whose cadence start is newest. Ties go to storage so a
/// start that was persisted successfully reports as `persisted`; a record
/// without a start cannot win over one that has it.
pub(super) fn resolve_background_heal_info(
    persisted: Option<&BackgroundHealInfo>,
    remembered: Option<&BackgroundHealInfo>,
) -> (BackgroundHealInfo, ScannerBitrotStartSource) {
    let persisted_start = persisted.filter(|info| info.bitrot_start_time.is_some());
    let remembered_start = remembered.filter(|info| info.bitrot_start_time.is_some());
    match (persisted_start, remembered_start) {
        (Some(stored), Some(memory)) if memory.bitrot_start_time > stored.bitrot_start_time => {
            (memory.clone(), ScannerBitrotStartSource::Memory)
        }
        (Some(stored), _) => (stored.clone(), ScannerBitrotStartSource::Persisted),
        (None, Some(memory)) => (memory.clone(), ScannerBitrotStartSource::Memory),
        (None, None) => (persisted.cloned().unwrap_or_default(), ScannerBitrotStartSource::None),
    }
}

/// Resolve the record a cycle decides from: the persisted read when it was
/// loaded, otherwise what this process remembered.
pub(super) fn resolve_background_heal_info_for_cycle(
    read: &BackgroundHealInfoRead,
) -> (BackgroundHealInfo, ScannerBitrotStartSource) {
    let persisted = (read.status == BackgroundHealInfoReadStatus::Loaded).then_some(&read.info);
    resolve_background_heal_info(persisted, remembered_background_heal_info().as_ref())
}

pub(super) fn record_scanner_bitrot_status(
    start: ScannerBitrotStart,
    source: ScannerBitrotStartSource,
    deep_scan_supported: bool,
    scan_mode: HealScanMode,
) {
    let status = ScannerBitrotStatus {
        deep_scan_supported,
        bitrot_start_source: source,
        bitrot_start_kind: start.kind(),
        bitrot_start_cycle: start.cycle(),
        bitrot_start_unix_secs: start
            .started()
            .and_then(|started| u64::try_from(started.timestamp()).ok())
            .unwrap_or(0),
        current_scan_mode: scan_mode.as_str(),
    };
    *SCANNER_BITROT_STATUS.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = status;
}

/// Deep cadence state of the running or latest cycle on this node.
pub fn scanner_bitrot_status() -> ScannerBitrotStatus {
    *SCANNER_BITROT_STATUS.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}
