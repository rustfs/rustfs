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

use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GroupStatus {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GroupAddRemove {
    pub group: String,
    pub members: Vec<String>,
    #[serde(rename = "groupStatus")]
    pub status: GroupStatus,
    #[serde(rename = "isRemove")]
    pub is_remove: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GroupDesc {
    pub name: String,
    pub status: String,
    pub members: Vec<String>,
    pub policy: String,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<OffsetDateTime>,
}
