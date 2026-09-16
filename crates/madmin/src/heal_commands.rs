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
    /// Only the coordinator's authenticated full scan may set this. Transported
    /// or old-peer results default to unproven and cannot advance heal receipts.
    #[serde(skip)]
    pub integrity_verified: bool,
    /// Storage-owner proof that a protected missing shard was reconstructed and
    /// committed. This is intentionally in-process only and does not certify a
    /// healthy or unchanged object.
    #[serde(skip)]
    pub repair_verified: bool,
    /// Storage-owner proof that the selected version/marker has authoritative
    /// metadata from read quorum. This is weaker than payload integrity proof
    /// and is intentionally in-process only.
    #[serde(skip)]
    pub metadata_verified: bool,
    /// Storage-owner proof that metadata or a delete marker was committed by
    /// this heal. This is intentionally in-process only.
    #[serde(skip)]
    pub metadata_repair_verified: bool,
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
    /// Exact version selected by the storage owner, including the nil UUID.
    /// Wire-decoded and legacy results cannot supply this in-process proof.
    #[serde(skip)]
    pub resolved_version_id: Option<[u8; 16]>,
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
    /// Number of drives this heal repaired: pairwise `before`/`after` state
    /// transitions to ok (issue #5863). `None` when the result carries no
    /// aligned drive data (e.g. remote bucket results) — not the same as zero.
    pub fn drives_healed(&self) -> Option<usize> {
        if self.after.drives.is_empty() || self.before.drives.len() != self.after.drives.len() {
            return None;
        }
        Some(
            self.before
                .drives
                .iter()
                .zip(&self.after.drives)
                .filter(|(before, after)| before.state != after.state && after.state == DRIVE_STATE_OK)
                .count(),
        )
    }

    /// Drives consulted, or `None` when the result has no drive entries.
    pub fn drives_reported(&self) -> Option<usize> {
        if self.after.drives.is_empty() {
            None
        } else {
            Some(self.after.drives.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_heal_version_is_not_wire_proof() {
        let item = HealResultItem {
            resolved_version_id: Some([0; 16]),
            ..Default::default()
        };
        let mut wire = serde_json::to_value(&item).expect("heal result should serialize");
        assert!(wire.get("resolved_version_id").is_none());
        assert!(wire.get("resolvedVersionId").is_none());
        assert!(wire.get("repair_verified").is_none());
        assert!(wire.get("repairVerified").is_none());
        assert!(wire.get("metadata_verified").is_none());
        assert!(wire.get("metadataVerified").is_none());
        assert!(wire.get("metadata_repair_verified").is_none());
        assert!(wire.get("metadataRepairVerified").is_none());
        wire["resolved_version_id"] = serde_json::json!(vec![0; 16]);
        let decoded: HealResultItem = serde_json::from_value(wire).expect("legacy wire shape should remain readable");
        assert_eq!(decoded.resolved_version_id, None, "wire input cannot supply owner proof");
    }

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
        assert_eq!(item.drives_healed(), Some(2));
        assert_eq!(item.drives_reported(), Some(4));

        let mut noop = HealResultItem::default();
        noop.before.drives = vec![drive("ok"); 12];
        noop.after.drives = vec![drive("ok"); 12];
        assert_eq!(noop.drives_healed(), Some(0));
    }

    #[test]
    fn drives_healed_reports_unknown_not_zero_without_drive_data() {
        // Empty successful remote result (RemotePeerS3Client::heal_bucket
        // default) is "unknown", never a definitive zero.
        let remote = HealResultItem::default();
        assert_eq!(remote.drives_healed(), None);
        assert_eq!(remote.drives_reported(), None);

        // A local missing -> ok result keeps its real count.
        let mut local = HealResultItem::default();
        local.before.drives = vec![drive("ok"), drive("missing")];
        local.after.drives = vec![drive("ok"), drive("ok")];
        assert_eq!(local.drives_healed(), Some(1));

        // Misaligned arrays cannot be paired: also unknown.
        let mut misaligned = HealResultItem::default();
        misaligned.before.drives = vec![drive("missing")];
        misaligned.after.drives = vec![drive("ok"), drive("ok")];
        assert_eq!(misaligned.drives_healed(), None);
    }
}
