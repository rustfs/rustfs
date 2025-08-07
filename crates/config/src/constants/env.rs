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

pub const DEFAULT_DELIMITER: &str = "_";
pub const ENV_PREFIX: &str = "RUSTFS_";
pub const ENV_WORD_DELIMITER: &str = "_";

/// Medium-drawn lines separator
/// This is used to separate words in environment variable names.
pub const ENV_WORD_DELIMITER_DASH: &str = "-";
pub const DEFAULT_ENABLE_TRUE: &str = "true";
pub const DEFAULT_ENABLE_FALSE: &str = "false";
pub const DEFAULT_ENABLE_EMPTY: &str = "";

pub const DEFAULT_ENABLE_YES: &str = "yes";
pub const DEFAULT_ENABLE_NO: &str = "no";

pub const DEFAULT_ENABLE_ON: &str = "on";
pub const DEFAULT_ENABLE_OFF: &str = "off";

pub const DEFAULT_ENABLED: &str = "enabled";
pub const DEFAULT_DISABLED: &str = "disabled";

pub const DEFAULT_ENABLE_OK: &str = "ok";
pub const DEFAULT_ENABLE_NOT_OK: &str = "not_ok";

pub const DEFAULT_ENABLE_SUCCESS: &str = "success";
pub const DEFAULT_ENABLE_FAILURE: &str = "failure";

pub const DEFAULT_ENABLE_ACTIVE: &str = "active";
pub const DEFAULT_ENABLE_INACTIVE: &str = "inactive";

pub const DEFAULT_ENABLE_ONE: &str = "1";
pub const DEFAULT_ENABLE_ZERO: &str = "0";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_DELIMITER, "_");
        assert_eq!(ENV_PREFIX, "RUSTFS_");
        assert_eq!(ENV_WORD_DELIMITER, "_");
        assert_eq!(ENV_WORD_DELIMITER_DASH, "-");
        assert_eq!(DEFAULT_ENABLE_TRUE, "true");
        assert_eq!(DEFAULT_ENABLE_FALSE, "false");
        assert_eq!(DEFAULT_ENABLE_EMPTY, "");
        assert_eq!(DEFAULT_ENABLE_YES, "yes");
        assert_eq!(DEFAULT_ENABLE_NO, "no");
        assert_eq!(DEFAULT_ENABLE_ON, "on");
        assert_eq!(DEFAULT_ENABLE_OFF, "off");
        assert_eq!(DEFAULT_ENABLED, "enabled");
        assert_eq!(DEFAULT_DISABLED, "disabled");
        assert_eq!(DEFAULT_ENABLE_OK, "ok");
        assert_eq!(DEFAULT_ENABLE_NOT_OK, "not_ok");
        assert_eq!(DEFAULT_ENABLE_SUCCESS, "success");
        assert_eq!(DEFAULT_ENABLE_FAILURE, "failure");
        assert_eq!(DEFAULT_ENABLE_ACTIVE, "active");
        assert_eq!(DEFAULT_ENABLE_INACTIVE, "inactive");
        assert_eq!(DEFAULT_ENABLE_ONE, "1");
        assert_eq!(DEFAULT_ENABLE_ZERO, "0");
    }
}
