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
use serde::Deserializer;
use serde::Serialize;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GroupStatus {
    #[default]
    Enabled,
    Disabled,
}

impl<'de> Deserialize<'de> for GroupStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "" | "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => Err(serde::de::Error::unknown_variant(&value, &["enabled", "disabled"])),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    #[serde(
        rename = "updatedAt",
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub updated_at: Option<OffsetDateTime>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_desc_updated_at_serializes_as_rfc3339() {
        let now = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();
        let group = GroupDesc {
            name: "group-a".to_string(),
            status: "enabled".to_string(),
            members: vec!["user-a".to_string()],
            policy: "readwrite".to_string(),
            updated_at: Some(now),
        };

        let json = serde_json::to_string(&group).unwrap();
        let decoded: GroupDesc = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\"updatedAt\":\""));
        assert!(json.contains('T'));
        assert_eq!(decoded.updated_at, Some(now));
    }

    #[test]
    fn group_status_accepts_empty_string_as_enabled() {
        let status: GroupStatus = serde_json::from_str(r#""""#).unwrap();
        assert_eq!(status, GroupStatus::Enabled);
    }
}
