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
    ///
    /// ODC-24 (backlog#1129): the engine's size eligibility is clamped to the
    /// in-memory GET fill limits so a `max_entry_bytes` above them is not
    /// reported as eligible while fill could never materialize the body.
    pub(crate) fn new(config: ObjectDataCacheConfig) -> Result<Self, ObjectDataCacheConfigError> {
        let fill_ceiling = resolve_fill_ceiling_bytes(config.max_entry_bytes);
        Ok(Self {
            cache: Arc::new(ObjectDataCache::new(config)?.with_fill_ceiling_bytes(fill_ceiling)),
        })
    }

    /// Creates an adapter from runtime environment variables, falling back to
    /// no-op on invalid input so startup behavior stays conservative.
    ///
    /// A malformed value on any single key (bad mode string, failed validation,
    /// or an unparseable numeric override) disables the whole cache rather than
    /// silently starting with defaults the operator never chose.
    pub(crate) fn from_env_or_disabled() -> Arc<Self> {
        match object_data_cache_config_from_env() {
            Ok(config) => Self::from_config_or_disabled(config, "env"),
            Err(invalid_keys) => {
                warn!(
                    invalid_keys = %invalid_keys.join(","),
                    "object data cache disabled because one or more environment variables are malformed"
                );
                Self::disabled_arc()
            }
        }
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
        let mode = config.mode;
        let max_entry_bytes = config.max_entry_bytes;
        // `Self::new` applies the ODC-24 fill-ceiling clamp; this startup path
        // only adds the resolved-config logging on top.
        match Self::new(config) {
            Ok(adapter) => {
                if !adapter.is_disabled() {
                    let ceiling = resolve_fill_ceiling_bytes(max_entry_bytes);
                    // ODC-19: log the resolved mode at startup, and warn when the
                    // operator explicitly selected the never-filling HitOnly mode.
                    tracing::info!(source, ?mode, "object data cache enabled");
                    // ODC-24: warn when the excess above the effective ceiling is inert.
                    if max_entry_bytes > ceiling {
                        warn!(
                            source,
                            max_entry_bytes,
                            effective_fill_ceiling = ceiling,
                            "RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES exceeds the in-memory GET fill limit; \
                             bodies above the effective cap are planned SkipTooLarge"
                        );
                    }
                    if mode == ObjectDataCacheMode::HitOnly {
                        warn!(
                            source,
                            "object data cache mode 'hit_only' never populates the cache; it only \
                             serves as a runtime-downgrade target and keeps a permanent 0% hit rate"
                        );
                    }
                }
                Arc::new(adapter)
            }
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

/// Resolves the effective fill ceiling from the app-layer in-memory GET fill
/// limits: `min(max_entry_bytes, seek-support threshold, 64 MiB buffer cap)`
/// (backlog#1129 / ODC-24). The concurrency-driven shrink of the fill threshold
/// is dynamic and cannot be captured here, so this static ceiling is the
/// conservative floor.
fn resolve_fill_ceiling_bytes(max_entry_bytes: u64) -> u64 {
    let seek_threshold = crate::app::object_usecase::object_seek_support_threshold() as u64;
    let hard_cap = u64::try_from(crate::app::object_usecase::MAX_GET_OBJECT_MEMORY_BUFFER_BYTES).unwrap_or(u64::MAX);
    max_entry_bytes.min(seek_threshold).min(hard_cap)
}

impl Default for ObjectDataCacheAdapter {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Reads a numeric env override with three-valued semantics: absent keeps the
/// default (`None`), a valid value is applied, and a malformed value records the
/// key in `invalid_keys` so the caller can invalidate the whole config.
fn take_numeric_env<T>(key: &'static str, invalid_keys: &mut Vec<&'static str>) -> Option<T>
where
    T: std::str::FromStr,
{
    match rustfs_utils::get_env_parse_outcome::<T>(key) {
        rustfs_utils::EnvParseOutcome::Parsed(value) => Some(value),
        rustfs_utils::EnvParseOutcome::Absent => None,
        rustfs_utils::EnvParseOutcome::Invalid => {
            invalid_keys.push(key);
            None
        }
    }
}

fn object_data_cache_config_from_env() -> Result<ObjectDataCacheConfig, Vec<&'static str>> {
    let mut invalid_keys: Vec<&'static str> = Vec::new();

    let values = ObjectDataCacheEnvValues {
        enabled: rustfs_utils::get_env_opt_bool(rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE),
        mode: rustfs_utils::get_env_opt_str(rustfs_config::ENV_OBJECT_DATA_CACHE_MODE),
        max_bytes: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_BYTES, &mut invalid_keys),
        max_memory_percent: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT, &mut invalid_keys),
        max_entry_bytes: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, &mut invalid_keys),
        ttl_secs: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_TTL_SECS, &mut invalid_keys),
        time_to_idle_secs: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS, &mut invalid_keys),
        min_free_memory_percent: take_numeric_env(
            rustfs_config::ENV_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT,
            &mut invalid_keys,
        ),
        fill_concurrency_per_cpu: take_numeric_env(
            rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU,
            &mut invalid_keys,
        ),
        fill_concurrency_max: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX, &mut invalid_keys),
        identity_keys_max: take_numeric_env(rustfs_config::ENV_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX, &mut invalid_keys),
    };

    if !invalid_keys.is_empty() {
        return Err(invalid_keys);
    }

    Ok(object_data_cache_config_from_values(values))
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
            // Enable flag without an explicit mode: start at the safest stage
            // that actually populates the cache. `HitOnly` never fills, so it
            // would keep a permanent 0% hit rate and pay per-GET overhead for
            // nothing (backlog#1124 / ODC-19). `FillBufferedOnly` fills only
            // from bodies the GET path already buffered — no extra
            // materialization — so it is both safe and effective.
            config.mode = ObjectDataCacheMode::FillBufferedOnly;
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
        ObjectDataCacheAdapter, ObjectDataCacheEnvValues, object_data_cache_config_from_env,
        object_data_cache_config_from_values, parse_object_data_cache_mode,
    };
    use rustfs_object_data_cache::{ObjectDataCacheConfig, ObjectDataCacheMode};
    use std::time::Duration;

    /// All object-data-cache env keys, unset by default. Tests override the
    /// entries they care about so the surrounding process environment cannot
    /// influence the outcome.
    fn all_env_unset() -> Vec<(&'static str, Option<&'static str>)> {
        vec![
            (rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MODE, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_BYTES, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_TTL_SECS, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX, None),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX, None),
        ]
    }

    fn with_env_overrides(overrides: &[(&'static str, &'static str)]) -> Vec<(&'static str, Option<&'static str>)> {
        let mut vars = all_env_unset();
        for (key, value) in overrides {
            if let Some(entry) = vars.iter_mut().find(|(name, _)| name == key) {
                entry.1 = Some(value);
            }
        }
        vars
    }

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
    fn object_data_cache_enable_true_without_mode_defaults_to_fill_buffered_only() {
        // ODC-19 (backlog#1124): ENABLE=true with no explicit mode must default
        // to a mode that actually populates the cache. HitOnly never fills.
        let config = object_data_cache_config_from_values(ObjectDataCacheEnvValues {
            enabled: Some(true),
            ..ObjectDataCacheEnvValues::default()
        });

        assert_eq!(config.mode, ObjectDataCacheMode::FillBufferedOnly);
    }

    #[test]
    fn resolve_fill_ceiling_clamps_large_max_entry_bytes() {
        // ODC-24 (backlog#1129): a max_entry_bytes above the in-memory GET fill
        // limits is clamped down; the ceiling never exceeds the 64 MiB hard cap.
        let huge = 512 * 1024 * 1024;
        let ceiling = super::resolve_fill_ceiling_bytes(huge);
        assert!(ceiling < huge, "a huge max_entry_bytes must be clamped down");
        assert!(
            ceiling <= u64::try_from(crate::app::object_usecase::MAX_GET_OBJECT_MEMORY_BUFFER_BYTES).unwrap(),
            "the ceiling must not exceed the 64 MiB hard cap"
        );
    }

    #[test]
    fn resolve_fill_ceiling_leaves_small_max_entry_bytes_untouched() {
        // A small max_entry_bytes is already below the fill limits, so it is the
        // binding constraint and passes through unchanged.
        assert_eq!(super::resolve_fill_ceiling_bytes(4096), 4096);
    }

    #[test]
    fn startup_adapter_clamps_plan_eligibility_to_fill_ceiling() {
        // ODC-24: an adapter built on the startup path with a `max_entry_bytes`
        // above the in-memory GET fill limits must plan a gap-sized object
        // `SkipTooLarge`, not `Cacheable` — otherwise it reports eligible while
        // it can never fill.
        let adapter = ObjectDataCacheAdapter::from_config_or_disabled(
            ObjectDataCacheConfig {
                mode: ObjectDataCacheMode::HitOnly,
                max_bytes: 200 * 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 128 * 1024 * 1024,
                ..ObjectDataCacheConfig::default()
            },
            "test",
        );

        // 32 MiB is within max_entry_bytes (128 MiB) but above the effective
        // ceiling (<= 64 MiB hard cap, and the 10 MiB default seek threshold).
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "object",
            version_id: None,
            etag: "etag",
            size: 32 * 1024 * 1024,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        assert_eq!(plan, rustfs_object_data_cache::ObjectDataCacheGetPlan::SkipTooLarge);
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

    #[test]
    fn overlong_ttl_builds_disabled_adapter_without_panic() {
        // moka's builder asserts on durations beyond ~1000 years; validate()
        // must reject an out-of-range ttl so the adapter degrades to disabled
        // rather than panicking AppContext::new at boot.
        let adapter = ObjectDataCacheAdapter::from_config_or_disabled(
            ObjectDataCacheConfig {
                mode: ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8 * 1024 * 1024,
                ttl: Duration::from_secs(u64::MAX),
                ..ObjectDataCacheConfig::default()
            },
            "test",
        );

        assert!(adapter.is_disabled());
    }

    #[test]
    fn object_data_cache_env_absent_uses_defaults() {
        temp_env::with_vars(all_env_unset(), || {
            let config = object_data_cache_config_from_env().expect("absent env should build the default config");

            assert_eq!(config, ObjectDataCacheConfig::default());
        });
    }

    #[test]
    fn object_data_cache_env_valid_numeric_value_is_applied() {
        let vars = with_env_overrides(&[
            (rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, "true"),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, "2097152"),
        ]);
        temp_env::with_vars(vars, || {
            let config = object_data_cache_config_from_env().expect("valid numeric env should be applied");

            assert_eq!(config.mode, ObjectDataCacheMode::FillBufferedOnly);
            assert_eq!(config.max_entry_bytes, 2_097_152);
        });
    }

    #[test]
    fn object_data_cache_env_malformed_numeric_invalidates_config() {
        let key = rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES;
        let vars = with_env_overrides(&[(rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, "true"), (key, "not-a-number")]);
        temp_env::with_vars(vars, || {
            let invalid_keys =
                object_data_cache_config_from_env().expect_err("a malformed numeric key must invalidate the config");

            assert!(invalid_keys.contains(&key));
        });
    }

    #[test]
    fn object_data_cache_env_malformed_numeric_builds_disabled_adapter() {
        let vars = with_env_overrides(&[
            (rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, "true"),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, "not-a-number"),
        ]);
        temp_env::with_vars(vars, || {
            let adapter = ObjectDataCacheAdapter::from_env_or_disabled();

            assert!(adapter.is_disabled());
        });
    }
}
