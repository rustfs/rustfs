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
