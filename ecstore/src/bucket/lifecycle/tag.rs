use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct Tag {
    pub key: String,
    pub value: String,
}
