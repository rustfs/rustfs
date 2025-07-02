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
