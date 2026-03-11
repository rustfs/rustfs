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

use std::{
    collections::HashSet,
    env,
    sync::{Mutex, OnceLock},
};
use tracing::warn;

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 8-bit type: signed i8
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing fails.
///
/// #Returns
/// - `i8`: The parsed value as i8 if successful, otherwise the default value.
///
pub fn get_env_i8(key: &str, default: i8) -> i8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 8-bit type: signed i8
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<i8>`: The parsed value as i8 if successful, otherwise None
///
pub fn get_env_opt_i8(key: &str) -> Option<i8> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 8-bit type: unsigned u8
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing fails.
///
/// #Returns
/// - `u8`: The parsed value as u8 if successful, otherwise the default value.
///
pub fn get_env_u8(key: &str, default: u8) -> u8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 8-bit type: unsigned u8
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<u8>`: The parsed value as u8 if successful, otherwise None
///
pub fn get_env_opt_u8(key: &str) -> Option<u8> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

static WARNED_ENV_MESSAGES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn log_once(key: &str, message: impl FnOnce() -> String) {
    let seen = WARNED_ENV_MESSAGES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut seen = match seen.lock() {
        Ok(seen) => seen,
        Err(poisoned) => poisoned.into_inner(),
    };
    if seen.insert(key.to_string()) {
        warn!("{}", message());
    }
}

fn resolve_env_with_aliases(key: &str, deprecated: &[&str]) -> Option<(String, String)> {
    if let Ok(value) = env::var(key) {
        return Some((key.to_string(), value));
    }

    let (alias, value) = deprecated
        .iter()
        .find_map(|alias| env::var(alias).ok().map(|value| (*alias, value)))?;
    let deprecated_key = format!("env_alias:{alias}->{key}");
    log_once(&deprecated_key, || {
        format!("Environment variable {alias} is deprecated, use {key} instead")
    });
    Some((alias.to_string(), value))
}

pub fn get_env_str_with_aliases(key: &str, deprecated: &[&str], default: &str) -> String {
    resolve_env_with_aliases(key, deprecated).map_or_else(|| default.to_string(), |(_, value)| value)
}

pub fn get_env_bool_with_aliases(key: &str, deprecated: &[&str], default: bool) -> bool {
    let Some((used_key, value)) = resolve_env_with_aliases(key, deprecated) else {
        return default;
    };

    parse_bool_str(&value).unwrap_or_else(|| {
        log_once(
            &format!("env_invalid_bool:{used_key}"),
            || {
                format!(
                    "Invalid bool value for {used_key}: {value}. Supported values are true/false,1/0,yes/no,on/off. Treating as unset."
                )
            },
        );
        default
    })
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 16-bit type: signed i16
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing fails.
///
/// #Returns
/// - `i16`: The parsed value as i16 if successful, otherwise the default value.
///
pub fn get_env_i16(key: &str, default: i16) -> i16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 16-bit type: signed i16
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<i16>`: The parsed value as i16 if successful, otherwise None
///
pub fn get_env_opt_i16(key: &str) -> Option<i16> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 16-bit type: unsigned u16
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing fails.
///
/// #Returns
/// - `u16`: The parsed value as u16 if successful, otherwise the default value.
///
pub fn get_env_u16(key: &str, default: u16) -> u16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 16-bit type: unsigned u16
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<u16>`: The parsed value as u16 if successful, otherwise None
///
pub fn get_env_u16_opt(key: &str) -> Option<u16> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 16-bit type: unsigned u16
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<u16>`: The parsed value as u16 if successful, otherwise None
///
pub fn get_env_opt_u16(key: &str) -> Option<u16> {
    get_env_u16_opt(key)
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 32-bit type: signed i32
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `i32`: The parsed value as i32 if successful, otherwise the default value.
///
pub fn get_env_i32(key: &str, default: i32) -> i32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 32-bit type: signed i32
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<i32>`: The parsed value as i32 if successful, otherwise None
///
pub fn get_env_opt_i32(key: &str) -> Option<i32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
/// 32-bit type: unsigned u32
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing fails.
///
/// #Returns
/// - `u32`: The parsed value as u32 if successful, otherwise the default value.
///
pub fn get_env_u32(key: &str, default: u32) -> u32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
/// 32-bit type: unsigned u32
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<u32>`: The parsed value as u32 if successful, otherwise None
///
pub fn get_env_opt_u32(key: &str) -> Option<u32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}
/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing
///
/// #Returns
/// - `f32`: The parsed value as f32 if successful, otherwise the default value
///
pub fn get_env_f32(key: &str, default: f32) -> f32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<f32>`: The parsed value as f32 if successful, otherwise None
///
pub fn get_env_opt_f32(key: &str) -> Option<f32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing
///
/// #Returns
/// - `i64`: The parsed value as i64 if successful, otherwise the default value
///
pub fn get_env_i64(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<i64>`: The parsed value as i64 if successful, otherwise None
///
pub fn get_env_opt_i64(key: &str) -> Option<i64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, returning Option<Option<i64>> if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<Option<i64>>`: The parsed value as i64 if successful, otherwise None
///
pub fn get_env_opt_opt_i64(key: &str) -> Option<Option<i64>> {
    env::var(key).ok().map(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing
///
/// #Returns
/// - `u64`: The parsed value as u64 if successful, otherwise the default value.
///
pub fn get_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Retrieve an environment variable as an unsigned 64-bit integer, returning `None` if not set or parsing fails.
/// Deprecated aliases are also supported with warning logging.
///
/// #Parameters
/// - `key`: The canonical environment variable key to look up.
/// - `deprecated`: A list of deprecated keys kept for compatibility.
///
/// #Returns
/// - `Option<u64>`: The parsed value if a key is found and valid, otherwise `None`.
///
pub fn get_env_opt_u64_with_aliases(key: &str, deprecated: &[&str]) -> Option<u64> {
    let (used_key, value) = resolve_env_with_aliases(key, deprecated)?;
    value
        .parse::<u64>()
        .map_err(|_| {
            log_once(&format!("env_invalid_u64:{used_key}"), || {
                format!("Invalid u64 value for {used_key}: {value}. Using default behavior.")
            });
        })
        .ok()
}

/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<u64>`: The parsed value as u64 if successful, otherwise None
///
pub fn get_env_opt_u64(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing
///
/// #Returns
/// - `f64`: The parsed value as f64 if successful, otherwise the default value.
///
pub fn get_env_f64(key: &str, default: f64) -> f64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<f64>`: The parsed value as f64 if successful, otherwise None
///
pub fn get_env_opt_f64(key: &str) -> Option<f64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default value to return if the environment variable is not set or parsing
///
/// #Returns
/// - `usize`: The parsed value as usize if successful, otherwise the default value.
///
pub fn get_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<usize>`: The parsed value as usize if successful, otherwise None
///
pub fn get_env_usize_opt(key: &str) -> Option<usize> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Retrieve an environment variable as a specific type, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<usize>`: The parsed value as usize if successful, otherwise None
///
pub fn get_env_opt_usize(key: &str) -> Option<usize> {
    get_env_usize_opt(key)
}

/// Retrieve an environment variable as a String, with a default value if not set.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default string value to return if the environment variable is not set.
///
/// #Returns
/// - `String`: The environment variable value if set, otherwise the default value.
///
pub fn get_env_str(key: &str, default: &str) -> String {
    get_env_str_with_aliases(key, &[], default)
}

/// Retrieve an environment variable as a String, returning None if not set.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<String>`: The environment variable value if set, otherwise None.
///
pub fn get_env_opt_str(key: &str) -> Option<String> {
    env::var(key).ok()
}

/// Retrieve an environment variable as a boolean, with a default value if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
/// - `default`: The default boolean value to return if the environment variable is not set or cannot be parsed.
///
/// #Returns
/// - `bool`: The parsed boolean value if successful, otherwise the default value.
///
pub fn get_env_bool(key: &str, default: bool) -> bool {
    get_env_bool_with_aliases(key, &[], default)
}

/// Parse a string into a boolean value.
///
/// #Parameters
/// - `s`: The string to parse.
///
/// #Returns
/// - `Option<bool>`: The parsed boolean value if successful, otherwise None.
///
fn parse_bool_str(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "t" | "true" | "on" | "yes" | "ok" | "success" | "active" | "enabled" => Some(true),
        "0" | "f" | "false" | "off" | "no" | "not_ok" | "failure" | "inactive" | "disabled" => Some(false),
        _ => None,
    }
}

/// Retrieve an environment variable as a boolean, returning None if not set or parsing fails.
///
/// #Parameters
/// - `key`: The environment variable key to look up.
///
/// #Returns
/// - `Option<bool>`: The parsed boolean value if successful, otherwise None.
///
pub fn get_env_opt_bool(key: &str) -> Option<bool> {
    let (used_key, value) = resolve_env_with_aliases(key, &[])?;
    parse_bool_str(&value).or_else(|| {
        log_once(&format!("env_invalid_bool_optional:{used_key}"), || {
            format!("Invalid bool value for {used_key}: {value}. Supported values are true/false,1/0,yes/no,on/off.")
        });
        None
    })
}
