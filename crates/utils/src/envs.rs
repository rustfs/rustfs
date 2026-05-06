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
    collections::BTreeSet,
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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

fn external_alias_for_key(key: &str) -> Option<String> {
    let suffix = key.strip_prefix("RUSTFS_")?;
    if is_external_compatible_suffix(suffix) {
        Some(format!("{}{}", external_env_prefix(), suffix))
    } else {
        None
    }
}

fn resolve_env_with_aliases(key: &str, deprecated: &[&str]) -> Option<(String, String)> {
    if let Ok(value) = env::var(key) {
        return Some((key.to_string(), value));
    }

    if let Some((alias, value)) = deprecated
        .iter()
        .find_map(|alias| env::var(alias).ok().map(|value| (*alias, value)))
    {
        let deprecated_key = format!("env_alias:{alias}->{key}");
        log_once(&deprecated_key, || {
            format!("Environment variable {alias} is deprecated, use {key} instead")
        });
        return Some((alias.to_string(), value));
    }

    let alias = external_alias_for_key(key)?;
    let value = env::var(&alias).ok()?;
    let deprecated_key = format!("env_alias:{alias}->{key}");
    log_once(&deprecated_key, || {
        format!("Environment variable {alias} is deprecated, use {key} instead")
    });
    Some((alias, value))
}

const EXTERNAL_ENV_PREFIX_BYTES: [u8; 6] = [77, 73, 78, 73, 79, 95];

const EXTERNAL_COMPATIBLE_SUFFIXES: &[&str] = &[
    "ACCESS_KEY",
    "ACCESS_KEY_FILE",
    "ADDRESS",
    "API_XFF_HEADER",
    "AUDIT_WEBHOOK_AUTH_TOKEN",
    "AUDIT_WEBHOOK_CLIENT_CERT",
    "AUDIT_WEBHOOK_CLIENT_KEY",
    "AUDIT_WEBHOOK_ENABLE",
    "AUDIT_WEBHOOK_ENDPOINT",
    "AUDIT_WEBHOOK_QUEUE_DIR",
    "COMPRESS_ENABLE",
    "COMPRESS_EXTENSIONS",
    "COMPRESS_MIME_TYPES",
    "CONSOLE_ADDRESS",
    "DRIVE_ACTIVE_MONITORING",
    "ERASURE_SET_DRIVE_COUNT",
    "IDENTITY_OPENID_CLAIM_NAME",
    "IDENTITY_OPENID_CLAIM_PREFIX",
    "IDENTITY_OPENID_CLIENT_ID",
    "IDENTITY_OPENID_CLIENT_SECRET",
    "IDENTITY_OPENID_CONFIG_URL",
    "IDENTITY_OPENID_DISPLAY_NAME",
    "IDENTITY_OPENID_REDIRECT_URI",
    "IDENTITY_OPENID_SCOPES",
    "ILM_EXPIRATION_WORKERS",
    "LICENSE",
    "NOTIFY_MQTT_BROKER",
    "NOTIFY_MQTT_ENABLE",
    "NOTIFY_MQTT_KEEP_ALIVE_INTERVAL",
    "NOTIFY_MQTT_PASSWORD",
    "NOTIFY_MQTT_QOS",
    "NOTIFY_MQTT_QUEUE_DIR",
    "NOTIFY_MQTT_QUEUE_LIMIT",
    "NOTIFY_MQTT_RECONNECT_INTERVAL",
    "NOTIFY_MQTT_TOPIC",
    "NOTIFY_MQTT_USERNAME",
    "NOTIFY_WEBHOOK_AUTH_TOKEN",
    "NOTIFY_WEBHOOK_CLIENT_CERT",
    "NOTIFY_WEBHOOK_CLIENT_KEY",
    "NOTIFY_WEBHOOK_ENABLE",
    "NOTIFY_WEBHOOK_ENDPOINT",
    "NOTIFY_WEBHOOK_QUEUE_DIR",
    "NOTIFY_WEBHOOK_QUEUE_LIMIT",
    "POLICY_PLUGIN_AUTH_TOKEN",
    "POLICY_PLUGIN_URL",
    "PORT",
    "REGION",
    "ROOT_PASSWORD",
    "ROOT_USER",
    "SCANNER_CYCLE",
    "SCANNER_SPEED",
    "SECRET_KEY",
    "SECRET_KEY_FILE",
    "STORAGE_CLASS_INLINE_BLOCK",
    "STORAGE_CLASS_OPTIMIZE",
    "STORAGE_CLASS_RRS",
    "STORAGE_CLASS_STANDARD",
    "VERSION",
    "VOLUMES",
];

const EXTERNAL_DYNAMIC_COMPATIBLE_PREFIXES: &[&str] = &["AUDIT_MQTT_", "AUDIT_WEBHOOK_", "NOTIFY_MQTT_", "NOTIFY_WEBHOOK_"];

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExternalEnvCompatReport {
    pub mapped_pairs: Vec<(String, String)>,
    pub conflict_keys: Vec<String>,
}

impl ExternalEnvCompatReport {
    pub fn mapped_count(&self) -> usize {
        self.mapped_pairs.len()
    }

    pub fn conflict_count(&self) -> usize {
        self.conflict_keys.len()
    }
}

fn external_env_prefix() -> &'static str {
    static PREFIX: OnceLock<String> = OnceLock::new();
    PREFIX
        .get_or_init(|| EXTERNAL_ENV_PREFIX_BYTES.iter().map(|&byte| char::from(byte)).collect())
        .as_str()
}

fn is_external_compatible_suffix(suffix: &str) -> bool {
    EXTERNAL_COMPATIBLE_SUFFIXES.contains(&suffix)
        || EXTERNAL_DYNAMIC_COMPATIBLE_PREFIXES
            .iter()
            .any(|prefix| suffix.starts_with(prefix))
}

fn build_external_env_compat_report_from_entries<I>(entries: I) -> ExternalEnvCompatReport
where
    I: IntoIterator<Item = (String, String)>,
{
    let env_map: std::collections::BTreeMap<String, String> = entries.into_iter().collect();
    let mut mapped_pairs = BTreeSet::new();
    let mut conflict_keys = BTreeSet::new();
    let source_prefix = external_env_prefix();

    for (source_key, source_value) in env_map.iter() {
        let Some(suffix) = source_key.strip_prefix(source_prefix) else {
            continue;
        };
        if !is_external_compatible_suffix(suffix) {
            continue;
        }
        let rustfs_key = format!("RUSTFS_{suffix}");
        match env_map.get(&rustfs_key) {
            None => {
                mapped_pairs.insert((source_key.clone(), rustfs_key));
            }
            Some(rustfs_value) if rustfs_value != source_value => {
                conflict_keys.insert(rustfs_key);
            }
            Some(_) => {}
        }
    }

    ExternalEnvCompatReport {
        mapped_pairs: mapped_pairs.into_iter().collect(),
        conflict_keys: conflict_keys.into_iter().collect(),
    }
}

/// Build compatibility plan between source-prefixed variables and `RUSTFS_*`.
///
/// Precedence rule:
/// - If both `RUSTFS_*` and source-prefixed variables exist, keep `RUSTFS_*` and record a conflict.
/// - If only source-prefixed variables exist, mark them as mappable to `RUSTFS_*`.
pub fn build_external_env_compat_report() -> ExternalEnvCompatReport {
    build_external_env_compat_report_from_entries(env::vars())
}

fn parse_env_value<T>(key: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    resolve_env_with_aliases(key, &[]).and_then(|(_, value)| value.parse().ok())
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
}

/// Retrieve an i32 environment variable with deprecated aliases and a default fallback.
///
/// Canonical `key` takes precedence over deprecated aliases when both are present.
pub fn get_env_i32_with_aliases(key: &str, deprecated: &[&str], default: i32) -> i32 {
    let Some((used_key, value)) = resolve_env_with_aliases(key, deprecated) else {
        return default;
    };

    value.parse::<i32>().unwrap_or_else(|_| {
        log_once(&format!("env_invalid_i32:{used_key}"), || {
            format!("Invalid i32 value for {used_key}: {value}. Using default behavior.")
        });
        default
    })
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    resolve_env_with_aliases(key, &[]).map(|(_, value)| value.parse().ok())
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    parse_env_value(key).unwrap_or(default)
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
    parse_env_value(key)
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
    resolve_env_with_aliases(key, &[]).map(|(_, value)| value)
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

/// Copy supported external-prefix variables such as `MINIO_*` into their
/// canonical `RUSTFS_*` names in the current process when the canonical key is
/// missing.
#[allow(unsafe_code)]
pub fn apply_external_env_compat() -> ExternalEnvCompatReport {
    let report = build_external_env_compat_report();
    for (source_key, rustfs_key) in &report.mapped_pairs {
        if let Ok(value) = env::var(source_key) {
            // SAFETY: this helper is intended for early startup bootstrap
            // before any background threads are created.
            unsafe {
                env::set_var(rustfs_key, value);
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::{
        apply_external_env_compat, build_external_env_compat_report_from_entries, get_env_bool_with_aliases,
        get_env_i32_with_aliases, get_env_str,
    };

    fn source_key(suffix: &str) -> String {
        let mut key = super::external_env_prefix().to_string();
        key.push_str(suffix);
        key
    }

    #[test]
    fn source_value_is_mapped_when_rustfs_missing() {
        let report =
            build_external_env_compat_report_from_entries(vec![(source_key("STORAGE_CLASS_STANDARD"), "EC:2".to_string())]);
        assert_eq!(report.mapped_count(), 1);
        assert!(
            report
                .mapped_pairs
                .iter()
                .any(|(input_key, rustfs_key)| input_key == &source_key("STORAGE_CLASS_STANDARD")
                    && rustfs_key == "RUSTFS_STORAGE_CLASS_STANDARD")
        );
        assert_eq!(report.conflict_count(), 0);
    }

    #[test]
    fn scanner_aliases_are_mapped_when_rustfs_missing() {
        let report = build_external_env_compat_report_from_entries(vec![
            (source_key("SCANNER_SPEED"), "slow".to_string()),
            (source_key("SCANNER_CYCLE"), "600".to_string()),
        ]);
        assert_eq!(report.mapped_count(), 2);
        assert!(
            report
                .mapped_pairs
                .iter()
                .any(|(input_key, rustfs_key)| input_key == &source_key("SCANNER_SPEED") && rustfs_key == "RUSTFS_SCANNER_SPEED")
        );
        assert!(
            report
                .mapped_pairs
                .iter()
                .any(|(input_key, rustfs_key)| input_key == &source_key("SCANNER_CYCLE") && rustfs_key == "RUSTFS_SCANNER_CYCLE")
        );
        assert_eq!(report.conflict_count(), 0);
    }

    #[test]
    fn rustfs_value_takes_precedence_on_conflict() {
        let report = build_external_env_compat_report_from_entries(vec![
            ("RUSTFS_ERASURE_SET_DRIVE_COUNT".to_string(), "8".to_string()),
            (source_key("ERASURE_SET_DRIVE_COUNT"), "16".to_string()),
        ]);
        assert_eq!(report.mapped_count(), 0);
        assert_eq!(report.conflict_count(), 1);
        assert!(report.conflict_keys.iter().any(|key| key == "RUSTFS_ERASURE_SET_DRIVE_COUNT"));
    }

    #[test]
    fn dynamic_notify_suffix_is_mapped() {
        let report =
            build_external_env_compat_report_from_entries(vec![(source_key("NOTIFY_WEBHOOK_ENABLE_PRIMARY"), "on".to_string())]);
        assert_eq!(report.mapped_count(), 1);
        assert!(
            report
                .mapped_pairs
                .iter()
                .any(|(input_key, rustfs_key)| input_key == &source_key("NOTIFY_WEBHOOK_ENABLE_PRIMARY")
                    && rustfs_key == "RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY")
        );
        assert_eq!(report.conflict_count(), 0);
    }

    #[test]
    fn unrelated_source_key_is_ignored() {
        let report = build_external_env_compat_report_from_entries(vec![(source_key("UNKNOWN_COMPAT_TEST"), "1".to_string())]);
        assert_eq!(report.mapped_count(), 0);
        assert_eq!(report.conflict_count(), 0);
    }

    #[test]
    fn minio_alias_is_used_for_rustfs_reads() {
        temp_env::with_var("MINIO_ROOT_USER", Some("compat-admin"), || {
            temp_env::with_var_unset("RUSTFS_ROOT_USER", || {
                assert_eq!(get_env_str("RUSTFS_ROOT_USER", "default-user"), "compat-admin");
            });
        });
    }

    #[test]
    fn rustfs_bool_env_takes_precedence_over_minio_alias() {
        temp_env::with_var("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", Some("false"), || {
            temp_env::with_var("MINIO_CI", Some("1"), || {
                assert!(!get_env_bool_with_aliases("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", &["MINIO_CI"], true,));
            });
        });
    }

    #[test]
    fn i32_alias_value_is_used_when_canonical_missing() {
        temp_env::with_var_unset("RUSTFS_TEST_I32", || {
            temp_env::with_var("RUSTFS_TEST_I32_LEGACY", Some("12"), || {
                assert_eq!(get_env_i32_with_aliases("RUSTFS_TEST_I32", &["RUSTFS_TEST_I32_LEGACY"], 8), 12);
            });
        });
    }

    #[test]
    fn i32_canonical_value_takes_precedence_over_alias() {
        temp_env::with_var("RUSTFS_TEST_I32", Some("9"), || {
            temp_env::with_var("RUSTFS_TEST_I32_LEGACY", Some("12"), || {
                assert_eq!(get_env_i32_with_aliases("RUSTFS_TEST_I32", &["RUSTFS_TEST_I32_LEGACY"], 8), 9);
            });
        });
    }

    #[test]
    fn i32_invalid_alias_value_falls_back_to_default() {
        temp_env::with_var_unset("RUSTFS_TEST_I32", || {
            temp_env::with_var("RUSTFS_TEST_I32_LEGACY", Some("not-an-i32"), || {
                assert_eq!(get_env_i32_with_aliases("RUSTFS_TEST_I32", &["RUSTFS_TEST_I32_LEGACY"], 8), 8);
            });
        });
    }

    #[test]
    fn apply_external_env_compat_copies_missing_rustfs_keys() {
        temp_env::with_var("MINIO_ROOT_USER", Some("compat-admin"), || {
            temp_env::with_var_unset("RUSTFS_ROOT_USER", || {
                let report = apply_external_env_compat();
                assert!(
                    report
                        .mapped_pairs
                        .iter()
                        .any(|(source_key, rustfs_key)| source_key == "MINIO_ROOT_USER" && rustfs_key == "RUSTFS_ROOT_USER")
                );
                assert_eq!(std::env::var("RUSTFS_ROOT_USER").as_deref(), Ok("compat-admin"));
            });
        });
    }
}
