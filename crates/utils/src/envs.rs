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

use std::env;

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
    env::var(key).unwrap_or_else(|_| default.to_string())
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
    env::var(key).ok().and_then(|v| parse_bool_str(&v)).unwrap_or(default)
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
    env::var(key).ok().and_then(|v| parse_bool_str(&v))
}
