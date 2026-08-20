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

/// Read background healing information from storage
pub async fn read_background_heal_info(storeapi: Arc<ECStore>) -> BackgroundHealInfo {
    // Skip for ErasureSD setup
    if scanner_is_erasure_sd().await {
        return BackgroundHealInfo::default();
    }

    // Get last healing information
    match read_config(storeapi, &BACKGROUND_HEAL_INFO_PATH).await {
        Ok(buf) => serde_json::from_slice::<BackgroundHealInfo>(&buf).unwrap_or_else(|e| {
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
            BackgroundHealInfo::default()
        }),
        Err(e) => {
            // Only log if it's not a ConfigNotFound error
            if e != EcstoreError::ConfigNotFound {
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
            BackgroundHealInfo::default()
        }
    }
}

/// Save background healing information to storage
#[instrument(skip(storeapi))]
pub async fn save_background_heal_info(storeapi: Arc<ECStore>, info: BackgroundHealInfo) {
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

    // Save configuration
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
