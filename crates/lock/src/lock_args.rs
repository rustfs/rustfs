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

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: usize,
}

impl Display for LockArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LockArgs[ uid: {}, resources: {:?}, owner: {}, source:{}, quorum: {} ]",
            self.uid, self.resources, self.owner, self.source, self.quorum
        )
    }
}
