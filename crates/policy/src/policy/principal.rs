// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
