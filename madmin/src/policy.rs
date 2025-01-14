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
