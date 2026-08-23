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
    ErasureSd,
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
pub(super) async fn read_background_heal_info_with_epoch(storeapi: Arc<ECStore>) -> BackgroundHealInfoRead {
    // Skip for ErasureSD setup
    if scanner_is_erasure_sd().await {
        return BackgroundHealInfoRead {
            info: BackgroundHealInfo::default(),
            expected_epoch: None,
            status: BackgroundHealInfoReadStatus::ErasureSd,
        };
    }

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

pub(super) async fn save_background_heal_info_for_epoch(
    storeapi: Arc<ECStore>,
    info: BackgroundHealInfo,
    expected_epoch: Option<u64>,
) {
    // Skip for ErasureSD setup
    if scanner_is_erasure_sd().await {
        return;
    }

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
