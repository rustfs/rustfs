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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::func::InnerFunc;

pub type BinaryFunc = InnerFunc<BinaryFuncValue>;

// todo implement it
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(transparent)]
pub struct BinaryFuncValue(String);

impl BinaryFunc {
    pub fn evaluate(&self, _values: &HashMap<String, Vec<String>>) -> bool {
        todo!()
    }
}
