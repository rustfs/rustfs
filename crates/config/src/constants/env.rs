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

pub const DEFAULT_DIR: &str = "/opt/rustfs/events"; // Default directory for event store
pub const DEFAULT_LIMIT: u64 = 100000; // Default store limit

/// Standard config keys and values.
pub const ENABLE_KEY: &str = "enable";
pub const COMMENT_KEY: &str = "comment";

/// Medium-drawn lines separator
/// This is used to separate words in environment variable names.
pub const ENV_WORD_DELIMITER_DASH: &str = "-";

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum EnableState {
    True,
    False,
    #[default]
    Empty,
    Yes,
    No,
    On,
    Off,
    Enabled,
    Disabled,
    Ok,
    NotOk,
    Success,
    Failure,
    Active,
    Inactive,
    One,
    Zero,
}
impl std::fmt::Display for EnableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl std::str::FromStr for EnableState {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            s if s.eq_ignore_ascii_case("true") => Ok(EnableState::True),
            s if s.eq_ignore_ascii_case("false") => Ok(EnableState::False),
            "" => Ok(EnableState::Empty),
            s if s.eq_ignore_ascii_case("yes") => Ok(EnableState::Yes),
            s if s.eq_ignore_ascii_case("no") => Ok(EnableState::No),
            s if s.eq_ignore_ascii_case("on") => Ok(EnableState::On),
            s if s.eq_ignore_ascii_case("off") => Ok(EnableState::Off),
            s if s.eq_ignore_ascii_case("enabled") => Ok(EnableState::Enabled),
            s if s.eq_ignore_ascii_case("disabled") => Ok(EnableState::Disabled),
            s if s.eq_ignore_ascii_case("ok") => Ok(EnableState::Ok),
            s if s.eq_ignore_ascii_case("not_ok") => Ok(EnableState::NotOk),
            s if s.eq_ignore_ascii_case("success") => Ok(EnableState::Success),
            s if s.eq_ignore_ascii_case("failure") => Ok(EnableState::Failure),
            s if s.eq_ignore_ascii_case("active") => Ok(EnableState::Active),
            s if s.eq_ignore_ascii_case("inactive") => Ok(EnableState::Inactive),
            "1" => Ok(EnableState::One),
            "0" => Ok(EnableState::Zero),
            _ => Err(()),
        }
    }
}

impl EnableState {
    /// Returns the default value for the enum.
    pub fn get_default() -> Self {
        Self::default()
    }
    /// Returns the string representation of the enum.
    pub fn as_str(&self) -> &str {
        match self {
            EnableState::True => "true",
            EnableState::False => "false",
            EnableState::Empty => "",
            EnableState::Yes => "yes",
            EnableState::No => "no",
            EnableState::On => "on",
            EnableState::Off => "off",
            EnableState::Enabled => "enabled",
            EnableState::Disabled => "disabled",
            EnableState::Ok => "ok",
            EnableState::NotOk => "not_ok",
            EnableState::Success => "success",
            EnableState::Failure => "failure",
            EnableState::Active => "active",
            EnableState::Inactive => "inactive",
            EnableState::One => "1",
            EnableState::Zero => "0",
        }
    }

    /// is_enabled checks if the state represents an enabled condition.
    pub fn is_enabled(self) -> bool {
        matches!(
            self,
            EnableState::True
                | EnableState::Yes
                | EnableState::On
                | EnableState::Enabled
                | EnableState::Ok
                | EnableState::Success
                | EnableState::Active
                | EnableState::One
        )
    }

    /// is_disabled checks if the state represents a disabled condition.
    pub fn is_disabled(self) -> bool {
        matches!(
            self,
            EnableState::False
                | EnableState::No
                | EnableState::Off
                | EnableState::Disabled
                | EnableState::NotOk
                | EnableState::Failure
                | EnableState::Inactive
                | EnableState::Zero
                | EnableState::Empty
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    #[test]
    fn test_enable_state_display_and_fromstr() {
        let cases = [
            (EnableState::True, "true"),
            (EnableState::False, "false"),
            (EnableState::Empty, ""),
            (EnableState::Yes, "yes"),
            (EnableState::No, "no"),
            (EnableState::On, "on"),
            (EnableState::Off, "off"),
            (EnableState::Enabled, "enabled"),
            (EnableState::Disabled, "disabled"),
            (EnableState::Ok, "ok"),
            (EnableState::NotOk, "not_ok"),
            (EnableState::Success, "success"),
            (EnableState::Failure, "failure"),
            (EnableState::Active, "active"),
            (EnableState::Inactive, "inactive"),
            (EnableState::One, "1"),
            (EnableState::Zero, "0"),
        ];
        for (variant, string) in cases.iter() {
            assert_eq!(&variant.to_string(), string);
            assert_eq!(EnableState::from_str(string).unwrap(), *variant);
        }
        // Test invalid string
        assert!(EnableState::from_str("invalid").is_err());
    }
    #[test]
    fn test_enable_state_enum() {
        let cases = [
            (EnableState::True, "true"),
            (EnableState::False, "false"),
            (EnableState::Empty, ""),
            (EnableState::Yes, "yes"),
            (EnableState::No, "no"),
            (EnableState::On, "on"),
            (EnableState::Off, "off"),
            (EnableState::Enabled, "enabled"),
            (EnableState::Disabled, "disabled"),
            (EnableState::Ok, "ok"),
            (EnableState::NotOk, "not_ok"),
            (EnableState::Success, "success"),
            (EnableState::Failure, "failure"),
            (EnableState::Active, "active"),
            (EnableState::Inactive, "inactive"),
            (EnableState::One, "1"),
            (EnableState::Zero, "0"),
        ];
        for (variant, string) in cases.iter() {
            assert_eq!(variant.to_string(), *string);
        }
    }

    #[test]
    fn test_enable_state_enum_from_str() {
        let cases = [
            ("true", EnableState::True),
            ("false", EnableState::False),
            ("", EnableState::Empty),
            ("yes", EnableState::Yes),
            ("no", EnableState::No),
            ("on", EnableState::On),
            ("off", EnableState::Off),
            ("enabled", EnableState::Enabled),
            ("disabled", EnableState::Disabled),
            ("ok", EnableState::Ok),
            ("not_ok", EnableState::NotOk),
            ("success", EnableState::Success),
            ("failure", EnableState::Failure),
            ("active", EnableState::Active),
            ("inactive", EnableState::Inactive),
            ("1", EnableState::One),
            ("0", EnableState::Zero),
        ];
        for (string, variant) in cases.iter() {
            assert_eq!(EnableState::from_str(string).unwrap(), *variant);
        }
    }

    #[test]
    fn test_enable_state_default() {
        let default_state = EnableState::get_default();
        assert_eq!(default_state, EnableState::Empty);
        assert_eq!(default_state.as_str(), "");
    }

    #[test]
    fn test_enable_state_as_str() {
        let cases = [
            (EnableState::True, "true"),
            (EnableState::False, "false"),
            (EnableState::Empty, ""),
            (EnableState::Yes, "yes"),
            (EnableState::No, "no"),
            (EnableState::On, "on"),
            (EnableState::Off, "off"),
            (EnableState::Enabled, "enabled"),
            (EnableState::Disabled, "disabled"),
            (EnableState::Ok, "ok"),
            (EnableState::NotOk, "not_ok"),
            (EnableState::Success, "success"),
            (EnableState::Failure, "failure"),
            (EnableState::Active, "active"),
            (EnableState::Inactive, "inactive"),
            (EnableState::One, "1"),
            (EnableState::Zero, "0"),
        ];
        for (variant, string) in cases.iter() {
            assert_eq!(variant.as_str(), *string);
        }
    }

    #[test]
    fn test_enable_state_is_enabled() {
        let enabled_states = [
            EnableState::True,
            EnableState::Yes,
            EnableState::On,
            EnableState::Enabled,
            EnableState::Ok,
            EnableState::Success,
            EnableState::Active,
            EnableState::One,
        ];
        for state in enabled_states.iter() {
            assert!(state.is_enabled());
        }

        let disabled_states = [
            EnableState::False,
            EnableState::No,
            EnableState::Off,
            EnableState::Disabled,
            EnableState::NotOk,
            EnableState::Failure,
            EnableState::Inactive,
            EnableState::Zero,
            EnableState::Empty,
        ];
        for state in disabled_states.iter() {
            assert!(state.is_disabled());
        }
    }
}
