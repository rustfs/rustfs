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

/// Environment variable name that specifies the data scanner start delay in seconds.
/// - Purpose: Define the delay between data scanner operations.
/// - Unit: seconds (u64).
/// - Valid values: any positive integer.
/// - Semantics: This delay controls how frequently the data scanner checks for and processes data; shorter delays lead to more responsive scanning but may increase system load.
/// - Example: `export RUSTFS_DATA_SCANNER_START_DELAY_SECS=10`
/// - Note: Choose an appropriate delay that balances scanning responsiveness with overall system performance.
pub const ENV_DATA_SCANNER_START_DELAY_SECS: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";

/// Default data scanner start delay in seconds if not specified in the environment variable.
/// - Value: 10 seconds.
/// - Rationale: This default interval provides a reasonable balance between scanning responsiveness and system load for most deployments.
/// - Adjustments: Users may modify this value via the `RUSTFS_DATA_SCANNER_START_DELAY_SECS` environment variable based on their specific scanning requirements and system performance.
pub const DEFAULT_DATA_SCANNER_START_DELAY_SECS: u64 = 60;
