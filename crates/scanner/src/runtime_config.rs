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

use crate::scanner_budget::ScannerCycleBudgetConfig;
use crate::sleeper::{SCANNER_SLEEPER, scanner_default_speed};
use rustfs_config::{
    DEFAULT_DELIMITER, DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS, DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE,
    DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS, DEFAULT_SCANNER_BITROT_CYCLE_SECS, DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS,
    DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES, DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS, DEFAULT_SCANNER_CYCLE_MAX_OBJECTS,
    DEFAULT_SCANNER_IDLE_MODE, DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS, DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS,
    DEFAULT_SCANNER_SPEED, DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS, ENV_SCANNER_ALERT_EXCESS_FOLDERS,
    ENV_SCANNER_ALERT_EXCESS_VERSION_SIZE, ENV_SCANNER_ALERT_EXCESS_VERSIONS, ENV_SCANNER_BITROT_CYCLE_SECS,
    ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, ENV_SCANNER_CYCLE, ENV_SCANNER_CYCLE_MAX_DIRECTORIES,
    ENV_SCANNER_CYCLE_MAX_DURATION_SECS, ENV_SCANNER_CYCLE_MAX_OBJECTS, ENV_SCANNER_IDLE_MODE,
    ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS,
    ENV_SCANNER_YIELD_EVERY_N_OBJECTS, SCANNER_ALERT_EXCESS_FOLDERS, SCANNER_ALERT_EXCESS_VERSION_SIZE,
    SCANNER_ALERT_EXCESS_VERSIONS, SCANNER_BITROT_CYCLE, SCANNER_CACHE_SAVE_TIMEOUT, SCANNER_CYCLE,
    SCANNER_CYCLE_MAX_DIRECTORIES, SCANNER_CYCLE_MAX_DURATION, SCANNER_CYCLE_MAX_OBJECTS, SCANNER_IDLE_MODE,
    SCANNER_MAX_CONCURRENT_DISK_SCANS, SCANNER_MAX_CONCURRENT_SET_SCANS, SCANNER_SPEED, SCANNER_START_DELAY, SCANNER_SUB_SYS,
    SCANNER_YIELD_EVERY_N_OBJECTS, ScannerSpeed,
};
use rustfs_ecstore::config::{Config as ServerConfig, KVS};
use serde::Serialize;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::Duration;
use tracing::warn;

const ENV_SCANNER_START_DELAY_SECS_DEPRECATED: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";
const NO_DEFAULT_CYCLE_OVERRIDE: u64 = 0;

static SCANNER_DEFAULT_CYCLE_SECS: AtomicU64 = AtomicU64::new(NO_DEFAULT_CYCLE_OVERRIDE);

static SCANNER_RUNTIME_CONFIG: LazyLock<RwLock<ScannerRuntimeConfig>> =
    LazyLock::new(|| RwLock::new(lookup_scanner_runtime_config(None).unwrap_or_default()));

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerRuntimeConfigSource {
    Env,
    Config,
    Default,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScannerRuntimeConfig {
    pub(crate) speed: ScannerSpeed,
    pub(crate) speed_source: ScannerRuntimeConfigSource,
    pub(crate) idle_mode: bool,
    pub(crate) idle_mode_source: ScannerRuntimeConfigSource,
    pub(crate) start_delay: Option<Duration>,
    pub(crate) start_delay_source: ScannerRuntimeConfigSource,
    pub(crate) cycle_interval: Duration,
    pub(crate) cycle_interval_source: ScannerRuntimeConfigSource,
    pub(crate) bitrot_cycle: Option<Duration>,
    pub(crate) bitrot_cycle_source: ScannerRuntimeConfigSource,
    pub(crate) cycle_budget: ScannerCycleBudgetConfig,
    pub(crate) cycle_max_duration_source: ScannerRuntimeConfigSource,
    pub(crate) cycle_max_objects_source: ScannerRuntimeConfigSource,
    pub(crate) cycle_max_directories_source: ScannerRuntimeConfigSource,
    pub(crate) cache_save_timeout: Duration,
    pub(crate) cache_save_timeout_source: ScannerRuntimeConfigSource,
    pub(crate) max_concurrent_set_scans: usize,
    pub(crate) max_concurrent_set_scans_source: ScannerRuntimeConfigSource,
    pub(crate) max_concurrent_disk_scans: usize,
    pub(crate) max_concurrent_disk_scans_source: ScannerRuntimeConfigSource,
    pub(crate) yield_every_n_objects: u64,
    pub(crate) yield_every_n_objects_source: ScannerRuntimeConfigSource,
    pub(crate) alert_excess_versions: u64,
    pub(crate) alert_excess_versions_source: ScannerRuntimeConfigSource,
    pub(crate) alert_excess_version_size: u64,
    pub(crate) alert_excess_version_size_source: ScannerRuntimeConfigSource,
    pub(crate) alert_excess_folders: u64,
    pub(crate) alert_excess_folders_source: ScannerRuntimeConfigSource,
}

impl Default for ScannerRuntimeConfig {
    fn default() -> Self {
        Self {
            speed: scanner_default_speed(),
            speed_source: ScannerRuntimeConfigSource::Default,
            idle_mode: DEFAULT_SCANNER_IDLE_MODE,
            idle_mode_source: ScannerRuntimeConfigSource::Default,
            start_delay: None,
            start_delay_source: ScannerRuntimeConfigSource::Default,
            cycle_interval: scanner_default_cycle_secs()
                .map(Duration::from_secs)
                .unwrap_or_else(|| scanner_default_speed().cycle_interval()),
            cycle_interval_source: ScannerRuntimeConfigSource::Default,
            bitrot_cycle: Some(Duration::from_secs(DEFAULT_SCANNER_BITROT_CYCLE_SECS)),
            bitrot_cycle_source: ScannerRuntimeConfigSource::Default,
            cycle_budget: ScannerCycleBudgetConfig::default(),
            cycle_max_duration_source: ScannerRuntimeConfigSource::Default,
            cycle_max_objects_source: ScannerRuntimeConfigSource::Default,
            cycle_max_directories_source: ScannerRuntimeConfigSource::Default,
            cache_save_timeout: Duration::from_secs(DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS),
            cache_save_timeout_source: ScannerRuntimeConfigSource::Default,
            max_concurrent_set_scans: DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS,
            max_concurrent_set_scans_source: ScannerRuntimeConfigSource::Default,
            max_concurrent_disk_scans: DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS,
            max_concurrent_disk_scans_source: ScannerRuntimeConfigSource::Default,
            yield_every_n_objects: DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS,
            yield_every_n_objects_source: ScannerRuntimeConfigSource::Default,
            alert_excess_versions: DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS,
            alert_excess_versions_source: ScannerRuntimeConfigSource::Default,
            alert_excess_version_size: DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE,
            alert_excess_version_size_source: ScannerRuntimeConfigSource::Default,
            alert_excess_folders: DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS,
            alert_excess_folders_source: ScannerRuntimeConfigSource::Default,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScannerRuntimeConfigError {
    #[error("invalid scanner config value for {key}: {value} ({reason})")]
    InvalidValue {
        key: &'static str,
        value: String,
        reason: &'static str,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct ScannerRuntimeConfigValue<T> {
    pub value: T,
    pub source: ScannerRuntimeConfigSource,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScannerRuntimeConfigStatus {
    pub speed: ScannerRuntimeConfigValue<String>,
    pub idle_mode: ScannerRuntimeConfigValue<bool>,
    pub start_delay_seconds: ScannerRuntimeConfigValue<Option<u64>>,
    pub cycle_interval_seconds: ScannerRuntimeConfigValue<u64>,
    pub bitrot_cycle_seconds: ScannerRuntimeConfigValue<Option<u64>>,
    pub cycle_max_duration_seconds: ScannerRuntimeConfigValue<Option<u64>>,
    pub cycle_max_objects: ScannerRuntimeConfigValue<Option<u64>>,
    pub cycle_max_directories: ScannerRuntimeConfigValue<Option<u64>>,
    pub cache_save_timeout_seconds: ScannerRuntimeConfigValue<u64>,
    pub max_concurrent_set_scans: ScannerRuntimeConfigValue<usize>,
    pub max_concurrent_disk_scans: ScannerRuntimeConfigValue<usize>,
    pub yield_every_n_objects: ScannerRuntimeConfigValue<u64>,
    pub alert_excess_versions: ScannerRuntimeConfigValue<u64>,
    pub alert_excess_version_size: ScannerRuntimeConfigValue<u64>,
    pub alert_excess_folders: ScannerRuntimeConfigValue<u64>,
}

pub(crate) fn set_scanner_default_cycle_secs(secs: Option<u64>) {
    SCANNER_DEFAULT_CYCLE_SECS.store(secs.unwrap_or(NO_DEFAULT_CYCLE_OVERRIDE), Ordering::Relaxed);
}

fn scanner_default_cycle_secs() -> Option<u64> {
    match SCANNER_DEFAULT_CYCLE_SECS.load(Ordering::Relaxed) {
        NO_DEFAULT_CYCLE_OVERRIDE => None,
        secs => Some(secs),
    }
}

fn config_value(kvs: Option<&KVS>, key: &'static str, default: impl fmt::Display) -> Option<String> {
    let value = kvs?.lookup(key)?;
    let value = value.trim();
    if value.is_empty() || value == default.to_string() {
        None
    } else {
        Some(value.to_string())
    }
}

fn scanner_kvs(config: Option<&ServerConfig>) -> Option<KVS> {
    config.and_then(|config| config.get_value(SCANNER_SUB_SYS, DEFAULT_DELIMITER))
}

fn invalid_value(key: &'static str, value: impl Into<String>, reason: &'static str) -> ScannerRuntimeConfigError {
    ScannerRuntimeConfigError::InvalidValue {
        key,
        value: value.into(),
        reason,
    }
}

fn parse_config_u64(key: &'static str, value: String) -> Result<u64, ScannerRuntimeConfigError> {
    value
        .parse::<u64>()
        .map_err(|_| invalid_value(key, value, "expected unsigned integer seconds or count"))
}

fn parse_config_usize(key: &'static str, value: String) -> Result<usize, ScannerRuntimeConfigError> {
    value
        .parse::<usize>()
        .map_err(|_| invalid_value(key, value, "expected unsigned integer count"))
}

fn parse_config_bool(key: &'static str, value: String) -> Result<bool, ScannerRuntimeConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "t" | "true" | "on" | "yes" | "ok" | "success" | "active" | "enabled" => Ok(true),
        "0" | "f" | "false" | "off" | "no" | "not_ok" | "failure" | "inactive" | "disabled" => Ok(false),
        _ => Err(invalid_value(key, value, "expected boolean value")),
    }
}

fn parse_config_speed(value: String) -> Result<ScannerSpeed, ScannerRuntimeConfigError> {
    ScannerSpeed::parse_str(&value).ok_or_else(|| invalid_value(SCANNER_SPEED, value, "expected scanner speed preset"))
}

fn parse_config_bitrot_cycle(value: String) -> Result<Option<Duration>, ScannerRuntimeConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "true" | "on" | "yes" => Ok(Some(Duration::ZERO)),
        "false" | "off" | "no" | "disabled" => Ok(None),
        _ => parse_config_u64(SCANNER_BITROT_CYCLE, value).map(|secs| Some(Duration::from_secs(secs))),
    }
}

fn parse_env_bitrot_cycle(value: String) -> Option<Duration> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "true" | "on" | "yes" => Some(Duration::ZERO),
        "false" | "off" | "no" | "disabled" => None,
        value => value.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
            warn!(
                env = ENV_SCANNER_BITROT_CYCLE_SECS,
                value,
                default_secs = DEFAULT_SCANNER_BITROT_CYCLE_SECS,
                "Invalid scanner bitrot cycle, using default"
            );
            Some(Duration::from_secs(DEFAULT_SCANNER_BITROT_CYCLE_SECS))
        }),
    }
}

fn lookup_speed(kvs: Option<&KVS>) -> Result<(ScannerSpeed, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(value) = rustfs_utils::get_env_opt_str(ENV_SCANNER_SPEED) {
        return Ok((ScannerSpeed::from_env_str(&value), ScannerRuntimeConfigSource::Env));
    }

    if let Some(value) = config_value(kvs, SCANNER_SPEED, DEFAULT_SCANNER_SPEED) {
        return parse_config_speed(value).map(|speed| (speed, ScannerRuntimeConfigSource::Config));
    }

    Ok((scanner_default_speed(), ScannerRuntimeConfigSource::Default))
}

fn lookup_optional_seconds(
    kvs: Option<&KVS>,
    key: &'static str,
    env_key: &'static str,
    default: u64,
) -> Result<(Option<Duration>, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(secs) = rustfs_utils::get_env_opt_u64(env_key) {
        return Ok((Some(Duration::from_secs(secs)), ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, key, default) {
        return parse_config_u64(key, value).map(|secs| (Some(Duration::from_secs(secs)), ScannerRuntimeConfigSource::Config));
    }
    Ok((None, ScannerRuntimeConfigSource::Default))
}

fn lookup_start_delay(kvs: Option<&KVS>) -> Result<(Option<Duration>, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    let aliases = [ENV_SCANNER_START_DELAY_SECS_DEPRECATED];
    if let Some(secs) = rustfs_utils::get_env_opt_u64_with_aliases(ENV_SCANNER_START_DELAY_SECS, &aliases) {
        return Ok((Some(Duration::from_secs(secs)), ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, SCANNER_START_DELAY, "") {
        return parse_config_u64(SCANNER_START_DELAY, value)
            .map(|secs| (Some(Duration::from_secs(secs)), ScannerRuntimeConfigSource::Config));
    }
    Ok((None, ScannerRuntimeConfigSource::Default))
}

fn lookup_count_budget(
    kvs: Option<&KVS>,
    key: &'static str,
    env_key: &'static str,
    default: u64,
) -> Result<(Option<u64>, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(value) = rustfs_utils::get_env_opt_u64(env_key) {
        return Ok(((value != 0).then_some(value), ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, key, default) {
        let count = parse_config_u64(key, value)?;
        return Ok(((count != 0).then_some(count), ScannerRuntimeConfigSource::Config));
    }
    Ok((None, ScannerRuntimeConfigSource::Default))
}

fn lookup_u64(
    kvs: Option<&KVS>,
    key: &'static str,
    env_key: &'static str,
    default: u64,
) -> Result<(u64, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(value) = rustfs_utils::get_env_opt_u64(env_key) {
        return Ok((value, ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, key, default) {
        return parse_config_u64(key, value).map(|value| (value, ScannerRuntimeConfigSource::Config));
    }
    Ok((default, ScannerRuntimeConfigSource::Default))
}

fn lookup_usize(
    kvs: Option<&KVS>,
    key: &'static str,
    env_key: &'static str,
    default: usize,
) -> Result<(usize, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(value) = rustfs_utils::get_env_opt_usize(env_key) {
        return Ok((value, ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, key, default) {
        return parse_config_usize(key, value).map(|value| (value, ScannerRuntimeConfigSource::Config));
    }
    Ok((default, ScannerRuntimeConfigSource::Default))
}

fn lookup_bool(
    kvs: Option<&KVS>,
    key: &'static str,
    env_key: &'static str,
    default: bool,
) -> Result<(bool, ScannerRuntimeConfigSource), ScannerRuntimeConfigError> {
    if let Some(value) = rustfs_utils::get_env_opt_bool(env_key) {
        return Ok((value, ScannerRuntimeConfigSource::Env));
    }
    if let Some(value) = config_value(kvs, key, default) {
        return parse_config_bool(key, value).map(|value| (value, ScannerRuntimeConfigSource::Config));
    }
    Ok((default, ScannerRuntimeConfigSource::Default))
}

pub(crate) fn lookup_scanner_runtime_config(
    config: Option<&ServerConfig>,
) -> Result<ScannerRuntimeConfig, ScannerRuntimeConfigError> {
    let kvs = scanner_kvs(config);
    let kvs = kvs.as_ref();
    let (speed, speed_source) = lookup_speed(kvs)?;
    let (idle_mode, idle_mode_source) = lookup_bool(kvs, SCANNER_IDLE_MODE, ENV_SCANNER_IDLE_MODE, DEFAULT_SCANNER_IDLE_MODE)?;
    let (start_delay, start_delay_source) = lookup_start_delay(kvs)?;

    let (cycle_interval, cycle_interval_source) = if let Some(secs) = rustfs_utils::get_env_opt_u64(ENV_SCANNER_CYCLE) {
        (Duration::from_secs(secs), ScannerRuntimeConfigSource::Env)
    } else if let Some(value) = config_value(kvs, SCANNER_CYCLE, "") {
        (
            Duration::from_secs(parse_config_u64(SCANNER_CYCLE, value)?),
            ScannerRuntimeConfigSource::Config,
        )
    } else if let Some(start_delay) = start_delay {
        (start_delay, start_delay_source)
    } else if let Some(secs) = scanner_default_cycle_secs() {
        (Duration::from_secs(secs), ScannerRuntimeConfigSource::Default)
    } else {
        (speed.cycle_interval(), speed_source)
    };

    let (cycle_max_duration, cycle_max_duration_source) = lookup_optional_seconds(
        kvs,
        SCANNER_CYCLE_MAX_DURATION,
        ENV_SCANNER_CYCLE_MAX_DURATION_SECS,
        DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS,
    )?;
    let (cycle_max_objects, cycle_max_objects_source) = lookup_count_budget(
        kvs,
        SCANNER_CYCLE_MAX_OBJECTS,
        ENV_SCANNER_CYCLE_MAX_OBJECTS,
        DEFAULT_SCANNER_CYCLE_MAX_OBJECTS,
    )?;
    let (cycle_max_directories, cycle_max_directories_source) = lookup_count_budget(
        kvs,
        SCANNER_CYCLE_MAX_DIRECTORIES,
        ENV_SCANNER_CYCLE_MAX_DIRECTORIES,
        DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES,
    )?;
    let cycle_budget = ScannerCycleBudgetConfig {
        max_duration: cycle_max_duration.filter(|duration| !duration.is_zero()),
        max_objects: cycle_max_objects,
        max_directories: cycle_max_directories,
    };

    let (bitrot_cycle, bitrot_cycle_source) = if let Some(value) = rustfs_utils::get_env_opt_str(ENV_SCANNER_BITROT_CYCLE_SECS) {
        (parse_env_bitrot_cycle(value), ScannerRuntimeConfigSource::Env)
    } else if let Some(value) = config_value(kvs, SCANNER_BITROT_CYCLE, DEFAULT_SCANNER_BITROT_CYCLE_SECS) {
        (parse_config_bitrot_cycle(value)?, ScannerRuntimeConfigSource::Config)
    } else {
        (
            Some(Duration::from_secs(DEFAULT_SCANNER_BITROT_CYCLE_SECS)),
            ScannerRuntimeConfigSource::Default,
        )
    };

    let (cache_save_timeout, cache_save_timeout_source) = lookup_u64(
        kvs,
        SCANNER_CACHE_SAVE_TIMEOUT,
        ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS,
        DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS,
    )?;
    let (max_concurrent_set_scans, max_concurrent_set_scans_source) = lookup_usize(
        kvs,
        SCANNER_MAX_CONCURRENT_SET_SCANS,
        ENV_SCANNER_MAX_CONCURRENT_SET_SCANS,
        DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS,
    )?;
    let (max_concurrent_disk_scans, max_concurrent_disk_scans_source) = lookup_usize(
        kvs,
        SCANNER_MAX_CONCURRENT_DISK_SCANS,
        ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS,
        DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS,
    )?;
    let (yield_every_n_objects, yield_every_n_objects_source) = lookup_u64(
        kvs,
        SCANNER_YIELD_EVERY_N_OBJECTS,
        ENV_SCANNER_YIELD_EVERY_N_OBJECTS,
        DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS,
    )?;
    let (alert_excess_versions, alert_excess_versions_source) = lookup_u64(
        kvs,
        SCANNER_ALERT_EXCESS_VERSIONS,
        ENV_SCANNER_ALERT_EXCESS_VERSIONS,
        DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS,
    )?;
    let (alert_excess_version_size, alert_excess_version_size_source) = lookup_u64(
        kvs,
        SCANNER_ALERT_EXCESS_VERSION_SIZE,
        ENV_SCANNER_ALERT_EXCESS_VERSION_SIZE,
        DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE,
    )?;
    let (alert_excess_folders, alert_excess_folders_source) = lookup_u64(
        kvs,
        SCANNER_ALERT_EXCESS_FOLDERS,
        ENV_SCANNER_ALERT_EXCESS_FOLDERS,
        DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS,
    )?;

    Ok(ScannerRuntimeConfig {
        speed,
        speed_source,
        idle_mode,
        idle_mode_source,
        start_delay,
        start_delay_source,
        cycle_interval,
        cycle_interval_source,
        bitrot_cycle,
        bitrot_cycle_source,
        cycle_budget,
        cycle_max_duration_source,
        cycle_max_objects_source,
        cycle_max_directories_source,
        cache_save_timeout: Duration::from_secs(cache_save_timeout.max(1)),
        cache_save_timeout_source,
        max_concurrent_set_scans,
        max_concurrent_set_scans_source,
        max_concurrent_disk_scans,
        max_concurrent_disk_scans_source,
        yield_every_n_objects,
        yield_every_n_objects_source,
        alert_excess_versions,
        alert_excess_versions_source,
        alert_excess_version_size,
        alert_excess_version_size_source,
        alert_excess_folders,
        alert_excess_folders_source,
    })
}

fn apply_resolved_runtime_config(config: ScannerRuntimeConfig) {
    SCANNER_SLEEPER.update_from_runtime_config(config.speed, config.idle_mode, config.yield_every_n_objects);
    if let Ok(mut guard) = SCANNER_RUNTIME_CONFIG.write() {
        *guard = config;
    }
}

pub fn validate_scanner_runtime_config(config: &ServerConfig) -> Result<(), ScannerRuntimeConfigError> {
    lookup_scanner_runtime_config(Some(config)).map(|_| ())
}

pub fn apply_scanner_runtime_config(config: &ServerConfig) -> Result<(), ScannerRuntimeConfigError> {
    let resolved = lookup_scanner_runtime_config(Some(config))?;
    apply_resolved_runtime_config(resolved);
    Ok(())
}

pub(crate) fn refresh_scanner_runtime_config_from_global() -> Result<(), ScannerRuntimeConfigError> {
    let config = rustfs_ecstore::config::get_global_server_config();
    let resolved = lookup_scanner_runtime_config(config.as_ref())?;
    apply_resolved_runtime_config(resolved);
    Ok(())
}

#[cfg(test)]
pub(crate) fn refresh_scanner_runtime_config_for_tests() {
    if let Ok(resolved) = lookup_scanner_runtime_config(None) {
        apply_resolved_runtime_config(resolved);
    }
}

pub(crate) fn current_scanner_runtime_config() -> ScannerRuntimeConfig {
    SCANNER_RUNTIME_CONFIG.read().map(|guard| guard.clone()).unwrap_or_default()
}

pub fn scanner_runtime_config_status() -> ScannerRuntimeConfigStatus {
    let config = current_scanner_runtime_config();
    ScannerRuntimeConfigStatus {
        speed: ScannerRuntimeConfigValue {
            value: config.speed.to_string(),
            source: config.speed_source,
        },
        idle_mode: ScannerRuntimeConfigValue {
            value: config.idle_mode,
            source: config.idle_mode_source,
        },
        start_delay_seconds: ScannerRuntimeConfigValue {
            value: config.start_delay.map(|duration| duration.as_secs()),
            source: config.start_delay_source,
        },
        cycle_interval_seconds: ScannerRuntimeConfigValue {
            value: config.cycle_interval.as_secs(),
            source: config.cycle_interval_source,
        },
        bitrot_cycle_seconds: ScannerRuntimeConfigValue {
            value: config.bitrot_cycle.map(|duration| duration.as_secs()),
            source: config.bitrot_cycle_source,
        },
        cycle_max_duration_seconds: ScannerRuntimeConfigValue {
            value: config.cycle_budget.max_duration.map(|duration| duration.as_secs()),
            source: config.cycle_max_duration_source,
        },
        cycle_max_objects: ScannerRuntimeConfigValue {
            value: config.cycle_budget.max_objects,
            source: config.cycle_max_objects_source,
        },
        cycle_max_directories: ScannerRuntimeConfigValue {
            value: config.cycle_budget.max_directories,
            source: config.cycle_max_directories_source,
        },
        cache_save_timeout_seconds: ScannerRuntimeConfigValue {
            value: config.cache_save_timeout.as_secs(),
            source: config.cache_save_timeout_source,
        },
        max_concurrent_set_scans: ScannerRuntimeConfigValue {
            value: config.max_concurrent_set_scans,
            source: config.max_concurrent_set_scans_source,
        },
        max_concurrent_disk_scans: ScannerRuntimeConfigValue {
            value: config.max_concurrent_disk_scans,
            source: config.max_concurrent_disk_scans_source,
        },
        yield_every_n_objects: ScannerRuntimeConfigValue {
            value: config.yield_every_n_objects,
            source: config.yield_every_n_objects_source,
        },
        alert_excess_versions: ScannerRuntimeConfigValue {
            value: config.alert_excess_versions,
            source: config.alert_excess_versions_source,
        },
        alert_excess_version_size: ScannerRuntimeConfigValue {
            value: config.alert_excess_version_size,
            source: config.alert_excess_version_size_source,
        },
        alert_excess_folders: ScannerRuntimeConfigValue {
            value: config.alert_excess_folders,
            source: config.alert_excess_folders_source,
        },
    }
}

pub(crate) fn scanner_cycle_interval() -> Duration {
    current_scanner_runtime_config().cycle_interval
}

pub(crate) fn scanner_start_delay() -> Option<Duration> {
    current_scanner_runtime_config().start_delay
}

pub(crate) fn scanner_bitrot_cycle() -> Option<Duration> {
    current_scanner_runtime_config().bitrot_cycle
}

pub(crate) fn scanner_yield_every_n_objects() -> u64 {
    current_scanner_runtime_config().yield_every_n_objects
}

pub(crate) fn scanner_cache_save_timeout() -> Duration {
    current_scanner_runtime_config().cache_save_timeout
}

pub(crate) fn scanner_max_concurrent_set_scans_configured() -> usize {
    current_scanner_runtime_config().max_concurrent_set_scans
}

pub(crate) fn scanner_max_concurrent_disk_scans_configured() -> usize {
    current_scanner_runtime_config().max_concurrent_disk_scans
}

pub(crate) fn scanner_alert_excess_versions() -> u64 {
    current_scanner_runtime_config().alert_excess_versions
}

pub(crate) fn scanner_alert_excess_version_size() -> u64 {
    current_scanner_runtime_config().alert_excess_version_size
}

pub(crate) fn scanner_alert_excess_folders() -> u64 {
    current_scanner_runtime_config().alert_excess_folders
}

#[cfg(test)]
mod tests {
    use super::{ScannerRuntimeConfigSource, lookup_scanner_runtime_config};
    use rustfs_config::{
        DEFAULT_DELIMITER, ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, ENV_SCANNER_CYCLE, ENV_SCANNER_CYCLE_MAX_OBJECTS,
        ENV_SCANNER_SPEED, SCANNER_CACHE_SAVE_TIMEOUT, SCANNER_CYCLE, SCANNER_CYCLE_MAX_DIRECTORIES, SCANNER_CYCLE_MAX_DURATION,
        SCANNER_CYCLE_MAX_OBJECTS, SCANNER_IDLE_MODE, SCANNER_SPEED, SCANNER_SUB_SYS, ScannerSpeed,
    };
    use rustfs_ecstore::config::{Config as ServerConfig, KVS};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::Duration;
    use temp_env::{with_var, with_var_unset};

    fn server_config_with_scanner(entries: &[(&str, &str)]) -> ServerConfig {
        rustfs_ecstore::config::init();
        let mut config = ServerConfig::new();
        let mut kvs = KVS::new();
        for (key, value) in entries {
            kvs.insert((*key).to_string(), (*value).to_string());
        }
        config
            .0
            .insert(SCANNER_SUB_SYS.to_string(), HashMap::from([(DEFAULT_DELIMITER.to_string(), kvs)]));
        config.set_defaults();
        config
    }

    #[test]
    #[serial]
    fn scanner_runtime_config_uses_persisted_values_when_env_is_unset() {
        let config = server_config_with_scanner(&[
            (SCANNER_SPEED, "slow"),
            (SCANNER_CYCLE, "120"),
            (SCANNER_CYCLE_MAX_DURATION, "30"),
            (SCANNER_CYCLE_MAX_OBJECTS, "1000"),
            (SCANNER_CYCLE_MAX_DIRECTORIES, "25"),
            (SCANNER_IDLE_MODE, "off"),
        ]);

        with_var_unset(ENV_SCANNER_SPEED, || {
            with_var_unset(ENV_SCANNER_CYCLE, || {
                let resolved = lookup_scanner_runtime_config(Some(&config)).expect("scanner runtime config");

                assert_eq!(resolved.speed, ScannerSpeed::Slow);
                assert_eq!(resolved.speed_source, ScannerRuntimeConfigSource::Config);
                assert_eq!(resolved.cycle_interval, Duration::from_secs(120));
                assert_eq!(resolved.cycle_interval_source, ScannerRuntimeConfigSource::Config);
                assert_eq!(resolved.cycle_budget.max_duration, Some(Duration::from_secs(30)));
                assert_eq!(resolved.cycle_budget.max_objects, Some(1000));
                assert_eq!(resolved.cycle_budget.max_directories, Some(25));
                assert!(!resolved.idle_mode);
            });
        });
    }

    #[test]
    #[serial]
    fn scanner_runtime_config_prefers_env_over_persisted_config() {
        let config = server_config_with_scanner(&[(SCANNER_SPEED, "slowest"), (SCANNER_CYCLE, "600")]);

        with_var(ENV_SCANNER_SPEED, Some("fast"), || {
            with_var(ENV_SCANNER_CYCLE, Some("45"), || {
                let resolved = lookup_scanner_runtime_config(Some(&config)).expect("scanner runtime config");

                assert_eq!(resolved.speed, ScannerSpeed::Fast);
                assert_eq!(resolved.speed_source, ScannerRuntimeConfigSource::Env);
                assert_eq!(resolved.cycle_interval, Duration::from_secs(45));
                assert_eq!(resolved.cycle_interval_source, ScannerRuntimeConfigSource::Env);
            });
        });
    }

    #[test]
    fn scanner_runtime_config_rejects_invalid_persisted_speed() {
        let config = server_config_with_scanner(&[(SCANNER_SPEED, "warp")]);

        let err = lookup_scanner_runtime_config(Some(&config)).expect_err("invalid scanner speed should fail");
        assert!(err.to_string().contains("speed"));
    }

    #[test]
    #[serial]
    fn scanner_runtime_config_status_reports_value_sources() {
        let config = server_config_with_scanner(&[(SCANNER_CYCLE_MAX_OBJECTS, "100"), (SCANNER_CACHE_SAVE_TIMEOUT, "5")]);

        with_var_unset(ENV_SCANNER_CYCLE_MAX_OBJECTS, || {
            with_var_unset(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, || {
                let resolved = lookup_scanner_runtime_config(Some(&config)).expect("scanner runtime config");
                super::apply_resolved_runtime_config(resolved);

                let status = super::scanner_runtime_config_status();

                assert_eq!(status.cycle_max_objects.value, Some(100));
                assert_eq!(status.cycle_max_objects.source, ScannerRuntimeConfigSource::Config);
                assert_eq!(status.cache_save_timeout_seconds.value, 5);
                assert_eq!(status.cache_save_timeout_seconds.source, ScannerRuntimeConfigSource::Config);
            });
        });
        super::refresh_scanner_runtime_config_for_tests();
    }
}
