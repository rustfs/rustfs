use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Tag {
    pub key: String,
    pub value: String,
}
