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

//! Workload profile buffer configuration constants.
//!
//! This module defines environment variable keys and default values for
//! custom buffer profile configuration.

use crate::{KI_B, MI_B};

/// Environment variable for minimum buffer size
/// Default: 64KB (65536 bytes)
pub const ENV_RUSTFS_BUFFER_MIN_SIZE: &str = "RUSTFS_BUFFER_MIN_SIZE";

/// Environment variable for maximum buffer size
/// Default: 1MB (1048576 bytes)
pub const ENV_RUSTFS_BUFFER_MAX_SIZE: &str = "RUSTFS_BUFFER_MAX_SIZE";

/// Environment variable for default buffer size (used when file size is unknown)
/// Default: 256KB (262144 bytes)
pub const ENV_RUSTFS_BUFFER_DEFAULT_SIZE: &str = "RUSTFS_BUFFER_DEFAULT_SIZE";

/// Default minimum buffer size: 64KB
pub const DEFAULT_BUFFER_MIN_SIZE: usize = 64 * KI_B;

/// Default maximum buffer size: 1MB
pub const DEFAULT_BUFFER_MAX_SIZE: usize = MI_B;

/// Default buffer size for unknown file size: 256KB
pub const DEFAULT_BUFFER_UNKNOWN_SIZE: usize = 256 * KI_B;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MI_B;

    #[test]
    fn test_default_values() {
        assert_eq!(DEFAULT_BUFFER_MIN_SIZE, 65536); // 64KB
        assert_eq!(DEFAULT_BUFFER_MAX_SIZE, 1048576); // 1MB
        assert_eq!(DEFAULT_BUFFER_UNKNOWN_SIZE, 262144); // 256KB
    }

    #[test]
    fn test_constants() {
        assert_eq!(KI_B, 1024);
        assert_eq!(MI_B, 1024 * 1024);
        assert_eq!(64 * KI_B, DEFAULT_BUFFER_MIN_SIZE);
        assert_eq!(1 * MI_B, DEFAULT_BUFFER_MAX_SIZE);
        assert_eq!(256 * KI_B, DEFAULT_BUFFER_UNKNOWN_SIZE);
    }

    #[test]
    fn test_env_var_names() {
        assert_eq!(ENV_RUSTFS_BUFFER_MIN_SIZE, "RUSTFS_BUFFER_MIN_SIZE");
        assert_eq!(ENV_RUSTFS_BUFFER_MAX_SIZE, "RUSTFS_BUFFER_MAX_SIZE");
        assert_eq!(ENV_RUSTFS_BUFFER_DEFAULT_SIZE, "RUSTFS_BUFFER_DEFAULT_SIZE");
    }
}
