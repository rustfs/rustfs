use super::{Validator, utils::wildcard};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "PascalCase", default)]
pub struct Principal {
    #[serde(rename = "AWS")]
    aws: HashSet<String>,
}

impl Principal {
    pub fn is_match(&self, parincipal: &str) -> bool {
        for pattern in self.aws.iter() {
            if wildcard::is_simple_match(pattern, parincipal) {
                return true;
            }
        }
        false
    }
}

impl Validator for Principal {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        if self.aws.is_empty() {
            return Err(Error::other("Principal is empty"));
        }
        Ok(())
    }
}
