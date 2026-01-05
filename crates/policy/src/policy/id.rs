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
use std::ops::Deref;

use super::Validator;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ID(pub String);

impl ID {
    /// Returns true if the ID is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Validator for ID {
    type Error = Error;
    /// if id is a valid utf string, then it is valid.
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}

impl<T: ToString> From<T> for ID {
    fn from(value: T) -> Self {
        Self(value.to_string())
    }
}

impl Deref for ID {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
