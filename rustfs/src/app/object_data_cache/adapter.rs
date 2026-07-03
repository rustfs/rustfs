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

use bytes::Bytes;
use rustfs_object_data_cache::{
    ObjectDataCache, ObjectDataCacheConfig, ObjectDataCacheConfigError, ObjectDataCacheFillResult, ObjectDataCacheGetPlan,
    ObjectDataCacheGetRequest, ObjectDataCacheIdentity, ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult,
    ObjectDataCacheLookup, ObjectDataCacheMode,
};
use std::{sync::Arc, time::Duration};
use tracing::warn;

#[derive(Debug, Default)]
struct ObjectDataCacheEnvValues {
    enabled: Option<bool>,
    mode: Option<String>,
    max_bytes: Option<u64>,
    max_memory_percent: Option<u8>,
    max_entry_bytes: Option<u64>,
    ttl_secs: Option<u64>,
    time_to_idle_secs: Option<u64>,
    min_free_memory_percent: Option<u8>,
    fill_concurrency_per_cpu: Option<u16>,
    fill_concurrency_max: Option<u16>,
    identity_keys_max: Option<u16>,
}

/// App-layer wrapper around the engine-only object data cache.
#[derive(Debug, Clone)]
pub(crate) struct ObjectDataCacheAdapter {
    cache: Arc<ObjectDataCache>,
}

impl ObjectDataCacheAdapter {
    /// Creates an adapter from validated cache configuration.
    pub(crate) fn new(config: ObjectDataCacheConfig) -> Result<Self, ObjectDataCacheConfigError> {
        Ok(Self {
            cache: Arc::new(ObjectDataCache::new(config)?),
        })
    }

    /// Creates an adapter from runtime environment variables, falling back to
    /// no-op on invalid input so startup behavior stays conservative.
    pub(crate) fn from_env_or_disabled() -> Arc<Self> {
        Self::from_config_or_disabled(object_data_cache_config_from_env(), "env")
    }

    /// Creates a disabled no-op adapter.
    pub(crate) fn disabled() -> Self {
        Self {
            cache: Arc::new(ObjectDataCache::disabled()),
        }
    }

    /// Creates a shared disabled no-op adapter.
    pub(crate) fn disabled_arc() -> Arc<Self> {
        Arc::new(Self::disabled())
    }

    /// Returns the underlying shared cache handle.
    #[cfg(test)]
    pub(crate) fn cache(&self) -> Arc<ObjectDataCache> {
        Arc::clone(&self.cache)
    }

    /// Returns true when the adapter is wired to a disabled cache.
    pub(crate) fn is_disabled(&self) -> bool {
        self.cache.is_disabled()
    }

    /// Returns true when the adapter allows materialize fill.
    pub(crate) fn materialize_fill_enabled(&self) -> bool {
        self.cache.materialize_fill_enabled()
    }

    /// Builds an engine-level GET plan.
    pub(crate) fn plan_get(&self, request: ObjectDataCacheGetRequest<'_>) -> ObjectDataCacheGetPlan {
        self.cache.plan_get(request)
    }

    /// Executes an engine-level cache lookup.
    pub(crate) async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        self.cache.lookup_body(plan).await
    }

    /// Executes an engine-level cache fill.
    pub(crate) async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        self.cache.fill_body(plan, bytes).await
    }

    /// Executes an engine-level object invalidation.
    pub(crate) async fn invalidate_object(
        &self,
        identity: ObjectDataCacheIdentity,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        self.cache.invalidate_object(identity, reason).await
    }
}

impl ObjectDataCacheAdapter {
    fn from_config_or_disabled(config: ObjectDataCacheConfig, source: &str) -> Arc<Self> {
        match Self::new(config) {
            Ok(adapter) => Arc::new(adapter),
            Err(err) => {
                warn!(
                    error = %err,
                    source,
                    "object data cache disabled because configuration is invalid"
                );
                Self::disabled_arc()
            }
        }
    }
}

impl Default for ObjectDataCacheAdapter {
    fn default() -> Self {
        Self::disabled()
    }
}

fn object_data_cache_config_from_env() -> ObjectDataCacheConfig {
    object_data_cache_config_from_values(ObjectDataCacheEnvValues {
        enabled: rustfs_utils::get_env_opt_bool(rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE),
        mode: rustfs_utils::get_env_opt_str(rustfs_config::ENV_OBJECT_DATA_CACHE_MODE),
        max_bytes: rustfs_utils::get_env_opt_u64(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_BYTES),
        max_memory_percent: rustfs_utils::get_env_opt_u8(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT),
        max_entry_bytes: rustfs_utils::get_env_opt_u64(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES),
        ttl_secs: rustfs_utils::get_env_opt_u64(rustfs_config::ENV_OBJECT_DATA_CACHE_TTL_SECS),
        time_to_idle_secs: rustfs_utils::get_env_opt_u64(rustfs_config::ENV_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS),
        min_free_memory_percent: rustfs_utils::get_env_opt_u8(rustfs_config::ENV_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT),
        fill_concurrency_per_cpu: rustfs_utils::get_env_opt_u16(rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU),
        fill_concurrency_max: rustfs_utils::get_env_opt_u16(rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX),
        identity_keys_max: rustfs_utils::get_env_opt_u16(rustfs_config::ENV_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX),
    })
}

fn object_data_cache_config_from_values(values: ObjectDataCacheEnvValues) -> ObjectDataCacheConfig {
    let mut config = ObjectDataCacheConfig::default();

    let mode_explicit = values.mode.is_some();
    if let Some(mode) = values.mode {
        config.mode = parse_object_data_cache_mode(&mode).unwrap_or_else(|| {
            warn!(
                value = %mode,
                supported = "disabled,hit_only,fill_buffered_only,fill_materialize_enabled",
                "invalid object data cache mode; cache remains disabled"
            );
            ObjectDataCacheMode::Disabled
        });
    }
    match values.enabled {
        Some(false) => {
            config.mode = ObjectDataCacheMode::Disabled;
        }
        Some(true) if !mode_explicit => {
            // Enable flag without an explicit mode: start at the safest
            // enabled stage instead of silently staying disabled.
            config.mode = ObjectDataCacheMode::HitOnly;
        }
        _ => {}
    }

    if let Some(max_bytes) = values.max_bytes {
        config.max_bytes = max_bytes;
    }
    if let Some(max_memory_percent) = values.max_memory_percent {
        config.max_memory_percent = max_memory_percent;
    }
    if let Some(max_entry_bytes) = values.max_entry_bytes {
        config.max_entry_bytes = max_entry_bytes;
    }
    if let Some(ttl_secs) = values.ttl_secs {
        config.ttl = Duration::from_secs(ttl_secs);
    }
    if let Some(time_to_idle_secs) = values.time_to_idle_secs {
        config.time_to_idle = Duration::from_secs(time_to_idle_secs);
    }
    if let Some(min_free_memory_percent) = values.min_free_memory_percent {
        config.min_free_memory_percent = min_free_memory_percent;
    }
    if let Some(fill_concurrency_per_cpu) = values.fill_concurrency_per_cpu {
        config.fill_concurrency_per_cpu = fill_concurrency_per_cpu;
    }
    if let Some(fill_concurrency_max) = values.fill_concurrency_max {
        config.fill_concurrency_max = fill_concurrency_max;
    }
    if let Some(identity_keys_max) = values.identity_keys_max {
        config.identity_keys_max = identity_keys_max;
    }

    config
}

fn parse_object_data_cache_mode(value: &str) -> Option<ObjectDataCacheMode> {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "disabled" | "off" | "none" => Some(ObjectDataCacheMode::Disabled),
        "hit_only" => Some(ObjectDataCacheMode::HitOnly),
        "fill_buffered_only" => Some(ObjectDataCacheMode::FillBufferedOnly),
        "fill_materialize_enabled" => Some(ObjectDataCacheMode::FillMaterializeEnabled),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectDataCacheAdapter, ObjectDataCacheEnvValues, object_data_cache_config_from_values, parse_object_data_cache_mode,
    };
    use rustfs_object_data_cache::{ObjectDataCacheConfig, ObjectDataCacheMode};
    use std::time::Duration;

    #[test]
    fn disabled_adapter_exposes_disabled_engine() {
        let adapter = ObjectDataCacheAdapter::disabled();

        assert!(adapter.is_disabled());
        assert!(adapter.cache().is_disabled());
    }

    #[test]
    fn adapter_new_accepts_default_disabled_config() {
        let adapter = ObjectDataCacheAdapter::new(ObjectDataCacheConfig::default()).expect("disabled config should initialize");

        assert!(adapter.is_disabled());
    }

    #[test]
    fn object_data_cache_mode_parser_accepts_supported_modes() {
        assert_eq!(parse_object_data_cache_mode("disabled"), Some(ObjectDataCacheMode::Disabled));
        assert_eq!(parse_object_data_cache_mode("hit-only"), Some(ObjectDataCacheMode::HitOnly));
        assert_eq!(
            parse_object_data_cache_mode("fill_buffered_only"),
            Some(ObjectDataCacheMode::FillBufferedOnly)
        );
        assert_eq!(
            parse_object_data_cache_mode("fill-materialize-enabled"),
            Some(ObjectDataCacheMode::FillMaterializeEnabled)
        );
    }

    #[test]
    fn object_data_cache_env_defaults_stay_disabled() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues::default());

        assert_eq!(config, ObjectDataCacheConfig::default());
    }

    #[test]
    fn object_data_cache_env_values_override_defaults() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            enabled: Some(true),
            mode: Some("fill_materialize_enabled".to_string()),
            max_bytes: Some(8_388_608),
            max_memory_percent: Some(10),
            max_entry_bytes: Some(2_097_152),
            ttl_secs: Some(120),
            time_to_idle_secs: Some(45),
            min_free_memory_percent: Some(30),
            fill_concurrency_per_cpu: Some(2),
            fill_concurrency_max: Some(64),
            identity_keys_max: Some(32),
        });

        assert_eq!(config.mode, ObjectDataCacheMode::FillMaterializeEnabled);
        assert_eq!(config.max_bytes, 8_388_608);
        assert_eq!(config.max_memory_percent, 10);
        assert_eq!(config.max_entry_bytes, 2_097_152);
        assert_eq!(config.ttl, Duration::from_secs(120));
        assert_eq!(config.time_to_idle, Duration::from_secs(45));
        assert_eq!(config.min_free_memory_percent, 30);
        assert_eq!(config.fill_concurrency_per_cpu, 2);
        assert_eq!(config.fill_concurrency_max, 64);
        assert_eq!(config.identity_keys_max, 32);
    }

    #[test]
    fn invalid_object_data_cache_mode_falls_back_to_disabled() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            mode: Some("enabled".to_string()),
            ..ObjectDataCacheEnvValues::default()
        });

        assert_eq!(config.mode, ObjectDataCacheMode::Disabled);
    }

    #[test]
    fn object_data_cache_enable_false_overrides_mode() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            enabled: Some(false),
            mode: Some("fill_materialize_enabled".to_string()),
            ..ObjectDataCacheEnvValues::default()
        });

        assert_eq!(config.mode, ObjectDataCacheMode::Disabled);
    }

    #[test]
    fn object_data_cache_enable_true_without_mode_defaults_to_hit_only() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            enabled: Some(true),
            ..ObjectDataCacheEnvValues::default()
        });

        assert_eq!(config.mode, ObjectDataCacheMode::HitOnly);
    }

    #[test]
    fn object_data_cache_explicit_mode_wins_over_enable_true() {
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            enabled: Some(true),
            mode: Some("disabled".to_string()),
            ..ObjectDataCacheEnvValues::default()
        });

        assert_eq!(config.mode, ObjectDataCacheMode::Disabled);
    }

    #[test]
    fn invalid_object_data_cache_config_builds_disabled_adapter() {
        let adapter = ObjectDataCacheAdapter::from_config_or_disabled(
            ObjectDataCacheConfig {
                mode: ObjectDataCacheMode::FillBufferedOnly,
                fill_concurrency_per_cpu: 2,
                fill_concurrency_max: 1,
                ..ObjectDataCacheConfig::default()
            },
            "test",
        );

        assert!(adapter.is_disabled());
    }
}
