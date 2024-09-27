use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Tag {
    pub key: Option<String>,
    pub value: Option<String>,
}
