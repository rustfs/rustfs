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

//! Layer-neutral module switches (backlog#1834).
//!
//! Whether the scanner, heal, audit and notify modules are on is read from the
//! infra layer (storage helpers, node-service RPC) and from the interface layer
//! (admin handlers), but the switches used to live in `startup_background`
//! (composition) and `server` (interface). Every lower-layer read was therefore
//! an upward edge that had to be baselined by the layer-dependency guard.
//!
//! The env-derived scanner/heal predicates and the audit/notify state cells now
//! live here, at the bottom of the layer order, so those reads are ordinary
//! downward edges. Resolving the audit/notify state still needs server-side
//! configuration, so `server::refresh_audit_module_enabled` and its notify twin
//! keep that logic and publish the result through the setters below.

use rustfs_utils::get_env_bool_with_aliases;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
pub(crate) const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
pub(crate) const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
pub(crate) const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
pub(crate) const ENV_BITROT_SELFTEST_ENABLE: &str = "RUSTFS_BITROT_SELFTEST_ENABLE";
pub(crate) const ENV_BITROT_SELFTEST_STRICT: &str = "RUSTFS_BITROT_SELFTEST_STRICT";

static AUDIT_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_AUDIT_ENABLE);
static NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);

/// Whether the data scanner is enabled, defaulting to on.
pub(crate) fn scanner_enabled_from_env() -> bool {
    get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true)
}

/// Whether background heal is enabled, defaulting to on.
pub(crate) fn heal_enabled_from_env() -> bool {
    get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true)
}

/// Whether the startup bitrot algorithm self-test runs, defaulting to on
/// (rustfs/backlog#1873).
pub(crate) fn bitrot_selftest_enabled_from_env() -> bool {
    rustfs_utils::get_env_bool(ENV_BITROT_SELFTEST_ENABLE, true)
}

/// Whether a failed bitrot self-test aborts startup instead of only logging
/// and exposing a failed status, defaulting to off.
pub(crate) fn bitrot_selftest_strict_from_env() -> bool {
    rustfs_utils::get_env_bool(ENV_BITROT_SELFTEST_STRICT, false)
}

/// Last published audit-module state.
pub fn is_audit_module_enabled() -> bool {
    AUDIT_MODULE_ENABLED.load(Ordering::Relaxed)
}

/// Publish the audit-module state resolved by `server::refresh_audit_module_enabled`.
pub(crate) fn set_audit_module_enabled(enabled: bool) {
    AUDIT_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Last published notify-module state.
pub fn is_notify_module_enabled() -> bool {
    NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed)
}

/// Publish the notify-module state resolved by `server::refresh_notify_module_enabled`.
pub(crate) fn set_notify_module_enabled(enabled: bool) {
    NOTIFY_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
}
