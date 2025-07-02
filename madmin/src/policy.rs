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
use serde_json::Value;
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyInfo {
    pub policy_name: String,
    pub policy: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_date: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_date: Option<OffsetDateTime>,
}
