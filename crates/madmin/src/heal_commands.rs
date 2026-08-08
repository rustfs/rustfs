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

use serde::{Deserialize, Serialize};

pub type HealItemType = String;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealDriveInfo {
    pub uuid: String,
    pub endpoint: String,
    pub state: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Infos {
    #[serde(rename = "drives")]
    pub drives: Vec<HealDriveInfo>,
}

/// String form of `DriveState::Ok` as recorded in `HealDriveInfo::state`
/// (this crate stores drive states as strings and does not depend on the
/// enum's crate).
const DRIVE_STATE_OK: &str = "ok";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealResultItem {
    #[serde(rename = "resultId")]
    pub result_index: usize,
    #[serde(rename = "type")]
    pub heal_item_type: HealItemType,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "object")]
    pub object: String,
    #[serde(rename = "versionId")]
    pub version_id: String,
    #[serde(rename = "detail")]
    pub detail: String,
    #[serde(rename = "parityBlocks")]
    pub parity_blocks: usize,
    #[serde(rename = "dataBlocks")]
    pub data_blocks: usize,
    #[serde(rename = "diskCount")]
    pub disk_count: usize,
    #[serde(rename = "setCount")]
    pub set_count: usize,
    #[serde(rename = "before")]
    pub before: Infos,
    #[serde(rename = "after")]
    pub after: Infos,
    #[serde(rename = "objectSize")]
    pub object_size: usize,
}

impl HealResultItem {
    /// Number of drives this heal actually repaired: entries whose state
    /// changed from a non-ok `before` value to ok in `after`. The heal
    /// populates `before.drives` and `after.drives` pairwise, one entry per
    /// consulted drive, so `after.drives.len()` is the drive count of the
    /// set — logging that as "drives healed" reported 12 for heals that
    /// repaired nothing (issue #5863).
    pub fn drives_healed(&self) -> usize {
        self.before
            .drives
            .iter()
            .zip(&self.after.drives)
            .filter(|(before, after)| before.state != after.state && after.state == DRIVE_STATE_OK)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(state: &str) -> HealDriveInfo {
        HealDriveInfo {
            uuid: String::new(),
            endpoint: String::new(),
            state: state.to_string(),
        }
    }

    #[test]
    fn drives_healed_counts_transitions_to_ok_not_consulted_drives() {
        let mut item = HealResultItem::default();
        item.before.drives = vec![drive("ok"), drive("missing"), drive("corrupt"), drive("offline")];
        item.after.drives = vec![drive("ok"), drive("ok"), drive("ok"), drive("offline")];
        // 4 drives consulted, 2 repaired (missing->ok, corrupt->ok); the
        // already-ok drive and the still-offline drive are not repairs.
        assert_eq!(item.drives_healed(), 2);

        let mut noop = HealResultItem::default();
        noop.before.drives = vec![drive("ok"); 12];
        noop.after.drives = vec![drive("ok"); 12];
        assert_eq!(noop.drives_healed(), 0);
    }
}
