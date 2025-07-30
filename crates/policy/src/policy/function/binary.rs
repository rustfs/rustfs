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

use base64_simd as base64;
use serde::{Deserialize, Serialize};

use super::func::InnerFunc;

pub type BinaryFunc = InnerFunc<BinaryFuncValue>;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(transparent)]
pub struct BinaryFuncValue(String);

impl BinaryFunc {
    /// Evaluate binary function against provided values
    /// Binary functions typically perform base64 decoding and comparison
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        let func_values = &self.0;

        // Iterate through all function values
        for func_kv in func_values {
            let func_value = &func_kv.values.0;

            // Try to decode the function value as base64
            if let Ok(decoded_bytes) = base64::STANDARD.decode_to_vec(func_value) {
                if let Ok(decoded_str) = String::from_utf8(decoded_bytes) {
                    // Check if any of the provided values match the decoded string
                    for value_list in values.values() {
                        for value in value_list {
                            if value == &decoded_str {
                                return true;
                            }
                            // Also try base64 decoding the input values
                            if let Ok(input_decoded) = base64::STANDARD.decode_to_vec(value) {
                                if let Ok(input_str) = String::from_utf8(input_decoded) {
                                    if input_str == decoded_str {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Fallback: direct string comparison
            for value_list in values.values() {
                for value in value_list {
                    if value == func_value {
                        return true;
                    }
                }
            }
        }

        false
    }
}
