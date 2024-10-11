use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Prefix {
    pub val: String,
    pub set: bool,
}
