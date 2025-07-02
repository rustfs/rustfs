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

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use strum::{EnumString, IntoStaticStr};

use super::Validator;

#[derive(Serialize, Clone, Deserialize, EnumString, IntoStaticStr, Default, Debug, PartialEq)]
#[serde(try_from = "&str", into = "&str")]
pub enum Effect {
    #[default]
    #[strum(serialize = "Allow")]
    Allow,
    #[strum(serialize = "Deny")]
    Deny,
}

impl Effect {
    pub fn is_allowed(&self, allowed: bool) -> bool {
        if matches!(self, Self::Allow) {
            return allowed;
        }

        !allowed
    }
}

impl Validator for Effect {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}
