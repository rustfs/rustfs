use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupAddRemove {
    group: String,
    pub members: Vec<String>,
    #[serde(rename = "groupStatus")]
    pub status: GroupStatus,
    #[serde(rename = "isRemove")]
    pub is_remove: bool,
}
