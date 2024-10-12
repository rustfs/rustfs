use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::utils;

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "PascalCase", default)]
pub struct Principal {
    #[serde(rename = "AWS")]
    aws: HashSet<String>,
}

impl Principal {
    pub fn is_valid(&self) -> bool {
        !self.aws.is_empty()
    }
    pub fn is_match(&self, parincipal: &str) -> bool {
        for pattern in self.aws.iter() {
            if utils::wildcard::match_simple(pattern, parincipal) {
                return true;
            }
        }
        false
    }
}
