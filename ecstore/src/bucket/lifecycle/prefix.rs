use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Prefix {
    pub val: String,
    pub set: bool,
}
