use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Principal {
    aws: HashSet<String>,
}
