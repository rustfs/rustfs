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

use rustfs_ecstore::{
    config::com::{read_config, save_config},
    error::Error as StorageError,
    new_object_layer_fn,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

const MODULE_SWITCH_CONFIG_PATH: &str = "config/module_switches.json";

// Keep a cheap in-process snapshot so hot-path checks do not need to read
// cluster metadata after startup or console-triggered refresh.
static PERSISTED_NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);
static PERSISTED_AUDIT_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_AUDIT_ENABLE);
static PERSISTED_MODULE_SWITCH_CONFIGURED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct PersistedModuleSwitches {
    pub(crate) notify_enabled: bool,
    pub(crate) audit_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ModuleSwitchSource {
    Env,
    Console,
    Default,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ModuleSwitchResolution {
    pub(crate) enabled: bool,
    pub(crate) source: ModuleSwitchSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ModuleSwitchSnapshot {
    pub(crate) notify_enabled: bool,
    pub(crate) audit_enabled: bool,
    pub(crate) persisted_notify_enabled: bool,
    pub(crate) persisted_audit_enabled: bool,
    pub(crate) notify_source: ModuleSwitchSource,
    pub(crate) audit_source: ModuleSwitchSource,
}

pub(crate) fn current_persisted_module_switches() -> PersistedModuleSwitches {
    PersistedModuleSwitches {
        notify_enabled: PERSISTED_NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed),
        audit_enabled: PERSISTED_AUDIT_MODULE_ENABLED.load(Ordering::Relaxed),
    }
}

fn persisted_module_switches_configured() -> bool {
    PERSISTED_MODULE_SWITCH_CONFIGURED.load(Ordering::Relaxed)
}

pub(crate) fn set_persisted_module_switches(config: PersistedModuleSwitches, configured: bool) {
    PERSISTED_NOTIFY_MODULE_ENABLED.store(config.notify_enabled, Ordering::Relaxed);
    PERSISTED_AUDIT_MODULE_ENABLED.store(config.audit_enabled, Ordering::Relaxed);
    PERSISTED_MODULE_SWITCH_CONFIGURED.store(configured, Ordering::Relaxed);
}

fn env_override_exists(key: &str) -> bool {
    std::env::var_os(key).is_some()
}

fn env_override_value(key: &str) -> Option<bool> {
    rustfs_utils::get_env_opt_bool(key)
}

fn effective_module_switch_state(env_key: &str, persisted_enabled: bool, default_enabled: bool) -> ModuleSwitchResolution {
    // Explicit env remains the highest-priority source so process-level bootstrap
    // cannot be silently overridden by a later console write.
    if let Some(env_enabled) = env_override_value(env_key) {
        return ModuleSwitchResolution {
            enabled: env_enabled,
            source: ModuleSwitchSource::Env,
        };
    }

    if persisted_module_switches_configured() {
        return ModuleSwitchResolution {
            enabled: persisted_enabled,
            source: ModuleSwitchSource::Console,
        };
    }

    ModuleSwitchResolution {
        enabled: default_enabled,
        source: ModuleSwitchSource::Default,
    }
}

pub(crate) fn resolve_notify_module_state() -> ModuleSwitchResolution {
    effective_module_switch_state(
        rustfs_config::ENV_NOTIFY_ENABLE,
        PERSISTED_NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed),
        rustfs_config::DEFAULT_NOTIFY_ENABLE,
    )
}

pub(crate) fn resolve_audit_module_state() -> ModuleSwitchResolution {
    effective_module_switch_state(
        rustfs_config::ENV_AUDIT_ENABLE,
        PERSISTED_AUDIT_MODULE_ENABLED.load(Ordering::Relaxed),
        rustfs_config::DEFAULT_AUDIT_ENABLE,
    )
}

pub(crate) fn current_module_switch_snapshot() -> ModuleSwitchSnapshot {
    let persisted = current_persisted_module_switches();
    let notify = resolve_notify_module_state();
    let audit = resolve_audit_module_state();

    ModuleSwitchSnapshot {
        notify_enabled: notify.enabled,
        audit_enabled: audit.enabled,
        persisted_notify_enabled: persisted.notify_enabled,
        persisted_audit_enabled: persisted.audit_enabled,
        notify_source: notify.source,
        audit_source: audit.source,
    }
}

fn validate_env_override_for_request(env_key: &str, requested: bool, label: &str) -> Result<(), String> {
    if !env_override_exists(env_key) {
        return Ok(());
    }

    match env_override_value(env_key) {
        // Matching values are safe: we still persist the console value, but the
        // effective runtime source remains env until the operator changes it.
        Some(value) if value == requested => Ok(()),
        Some(value) => Err(format!(
            "{label} is managed by environment variable {env_key}={value}; update the environment value first, then use the console to refresh the module switch state"
        )),
        None => Err(format!(
            "{label} is managed by environment variable {env_key}, but its value is not a valid boolean; fix the environment value first, then use the console to refresh the module switch state"
        )),
    }
}

pub(crate) fn validate_module_switch_update(requested: PersistedModuleSwitches) -> Result<(), String> {
    validate_env_override_for_request(rustfs_config::ENV_NOTIFY_ENABLE, requested.notify_enabled, "notify module")?;
    validate_env_override_for_request(rustfs_config::ENV_AUDIT_ENABLE, requested.audit_enabled, "audit module")?;
    Ok(())
}

pub(crate) async fn refresh_persisted_module_switches_from_store() -> Result<PersistedModuleSwitches, String> {
    let Some(store) = new_object_layer_fn() else {
        return Err("storage layer not initialized".to_string());
    };

    let (config, configured) = match read_config(store, MODULE_SWITCH_CONFIG_PATH).await {
        Ok(data) => (
            serde_json::from_slice::<PersistedModuleSwitches>(&data)
                .map_err(|e| format!("failed to deserialize module switch config: {e}"))?,
            true,
        ),
        Err(StorageError::ConfigNotFound) => (PersistedModuleSwitches::default(), false),
        Err(err) => return Err(format!("failed to load module switch config: {err}")),
    };

    // Track whether the persisted file exists so the effective state can
    // distinguish "console configured false" from "never configured, use default".
    set_persisted_module_switches(config, configured);
    Ok(config)
}

pub(crate) async fn save_persisted_module_switches_to_store(config: PersistedModuleSwitches) -> Result<(), String> {
    let Some(store) = new_object_layer_fn() else {
        return Err("storage layer not initialized".to_string());
    };

    let data = serde_json::to_vec(&config).map_err(|e| format!("failed to serialize module switch config: {e}"))?;
    save_config(store, MODULE_SWITCH_CONFIG_PATH, data)
        .await
        .map_err(|e| format!("failed to save module switch config: {e}"))?;

    set_persisted_module_switches(config, true);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use temp_env::{with_var, with_vars};

    #[test]
    #[serial]
    fn resolve_module_switch_state_prefers_env_override() {
        set_persisted_module_switches(
            PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: false,
            },
            true,
        );

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                let notify = resolve_notify_module_state();
                let audit = resolve_audit_module_state();

                assert!(notify.enabled);
                assert_eq!(notify.source, ModuleSwitchSource::Env);
                assert!(!audit.enabled);
                assert_eq!(audit.source, ModuleSwitchSource::Env);
            },
        );
    }

    #[test]
    #[serial]
    fn resolve_module_switch_state_falls_back_to_console_value() {
        set_persisted_module_switches(
            PersistedModuleSwitches {
                notify_enabled: true,
                audit_enabled: false,
            },
            true,
        );

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, None::<&str>),
                (rustfs_config::ENV_AUDIT_ENABLE, None::<&str>),
            ],
            || {
                let notify = resolve_notify_module_state();
                let audit = resolve_audit_module_state();

                assert!(notify.enabled);
                assert_eq!(notify.source, ModuleSwitchSource::Console);
                assert!(!audit.enabled);
                assert_eq!(audit.source, ModuleSwitchSource::Console);
            },
        );
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_rejects_env_conflict() {
        with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"), || {
            let err = validate_module_switch_update(PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: false,
            })
            .unwrap_err();

            assert!(err.contains(rustfs_config::ENV_NOTIFY_ENABLE));
            assert!(err.contains("update the environment value first"));
        });
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_allows_matching_env_override() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                validate_module_switch_update(PersistedModuleSwitches {
                    notify_enabled: true,
                    audit_enabled: false,
                })
                .expect("matching env override should be accepted");
            },
        );
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_rejects_invalid_env_override() {
        with_var(rustfs_config::ENV_AUDIT_ENABLE, Some("invalid"), || {
            let err = validate_module_switch_update(PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: true,
            })
            .unwrap_err();

            assert!(err.contains("not a valid boolean"));
        });
    }
}
