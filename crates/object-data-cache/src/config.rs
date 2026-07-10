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

use crate::error::ObjectDataCacheConfigError;
use crate::memory::{MemoryBasis, resolve_effective_memory};
use std::sync::Once;
use std::time::Duration;

const DEFAULT_DERIVED_MAX_MEMORY_PERCENT_CAP: u64 = 10;
const DEFAULT_DERIVED_MAX_BYTES_CAP: u64 = 64 * 1024 * 1024 * 1024;

/// Upper bound (seconds) for `ttl` / `time_to_idle`. Kept far below moka's
/// ~1000-year builder assertion while remaining a sane operational cap so a
/// bad env var degrades to the disabled adapter instead of panicking at boot.
const MAX_DURATION_SECS: u64 = 30 * 24 * 60 * 60;

/// Overhead reserved on top of a cached body when validating an explicit
/// `max_bytes`. moka's weigher charges key bytes + a small per-entry overhead
/// on top of the body, so `max_bytes` must clear `max_entry_bytes` by at least
/// this margin for the entry to ever be retained.
const ENTRY_WEIGHT_OVERHEAD_BYTES: u64 = 4096;

/// Upper bound for `max_entry_bytes`. moka weighers return `u32`, so an entry
/// above ~4 GiB would be under-weighted and bypass capacity accounting; stay
/// below `u32::MAX` with room for the weigher overhead.
const MAX_ENTRY_BYTES_LIMIT: u64 = u32::MAX as u64 - ENTRY_WEIGHT_OVERHEAD_BYTES;

/// Guards the one-shot startup log of the resolved cache capacity.
static RESOLVED_CAPACITY_LOGGED: Once = Once::new();

/// Runtime mode for the object data cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObjectDataCacheMode {
    /// Cache is completely disabled.
    #[default]
    Disabled,
    /// Cache lookups are allowed, but cache fill remains disabled.
    HitOnly,
    /// Cache fill is only allowed from an existing buffered body.
    FillBufferedOnly,
    /// Cache fill may materialize the final body stream exactly once.
    FillMaterializeEnabled,
}

impl ObjectDataCacheMode {
    pub(crate) const fn as_metric_label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::HitOnly => "hit_only",
            Self::FillBufferedOnly => "fill_buffered_only",
            Self::FillMaterializeEnabled => "fill_materialize_enabled",
        }
    }
}

/// Object data cache configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDataCacheConfig {
    /// Runtime mode gate for the cache engine.
    pub mode: ObjectDataCacheMode,
    /// Explicit byte-capacity override. Zero means derive from memory percent.
    pub max_bytes: u64,
    /// Memory percent used when `max_bytes` is zero.
    pub max_memory_percent: u8,
    /// Maximum cacheable entry size in bytes.
    pub max_entry_bytes: u64,
    /// Time-to-live for a cache entry.
    ///
    /// moka expires an entry at `min(ttl, time_to_idle-since-last-access)`, so
    /// a `time_to_idle` larger than `ttl` never takes effect.
    pub ttl: Duration,
    /// Time-to-idle for a cache entry.
    ///
    /// See [`ttl`](Self::ttl): expiration uses `min(ttl, time_to_idle)`, so
    /// setting `time_to_idle` above `ttl` is inert.
    pub time_to_idle: Duration,
    /// Minimum free memory percent before fill is paused.
    pub min_free_memory_percent: u8,
    /// Fill concurrency multiplier applied to CPU count.
    pub fill_concurrency_per_cpu: u16,
    /// Absolute fill concurrency cap.
    pub fill_concurrency_max: u16,
    /// Conservative cap for keys attached to one object identity.
    pub identity_keys_max: u16,
}

impl Default for ObjectDataCacheConfig {
    fn default() -> Self {
        Self {
            mode: ObjectDataCacheMode::Disabled,
            max_bytes: 0,
            max_memory_percent: 5,
            max_entry_bytes: 1_048_576,
            ttl: Duration::from_secs(60),
            time_to_idle: Duration::from_secs(30),
            min_free_memory_percent: 20,
            fill_concurrency_per_cpu: 1,
            fill_concurrency_max: 32,
            identity_keys_max: 16,
        }
    }
}

impl ObjectDataCacheConfig {
    /// Returns true when the cache is effectively disabled.
    pub const fn is_disabled(&self) -> bool {
        matches!(self.mode, ObjectDataCacheMode::Disabled)
    }

    /// Returns true when the cache mode allows lookups.
    pub const fn lookup_enabled(&self) -> bool {
        !self.is_disabled()
    }

    /// Returns true when the cache mode allows fills.
    pub const fn fill_enabled(&self) -> bool {
        matches!(
            self.mode,
            ObjectDataCacheMode::FillBufferedOnly | ObjectDataCacheMode::FillMaterializeEnabled
        )
    }

    /// Resolves the effective max capacity in bytes for the cache.
    pub fn resolved_max_bytes(&self) -> Result<u64, ObjectDataCacheConfigError> {
        if self.max_bytes > 0 {
            return Ok(self.max_bytes);
        }

        // Resolve capacity from the effective (container-aware) total memory so
        // a pod with a cgroup limit far below the node RAM does not size the
        // cache to the node.
        let effective = resolve_effective_memory();
        let total_memory = effective.total_bytes;
        let derived = total_memory.saturating_mul(u64::from(self.max_memory_percent)) / 100;
        let resolved = clamp_derived_max_bytes(derived, total_memory);

        if resolved == 0 {
            return Err(ObjectDataCacheConfigError::ZeroResolvedMaxBytes);
        }

        // The derived capacity is no longer floored by `max_entry_bytes` (that
        // used to silently inflate the cache above the safety clamp). If a
        // single entry cannot fit, reject rather than inflate.
        if self.max_entry_bytes > resolved {
            return Err(ObjectDataCacheConfigError::MaxEntryBytesExceedsCapacity);
        }

        log_resolved_capacity_once(resolved, total_memory, effective.basis);

        Ok(resolved)
    }

    /// Validates the configuration for internal consistency.
    pub fn validate(&self) -> Result<(), ObjectDataCacheConfigError> {
        if self.max_bytes == 0 && (self.max_memory_percent == 0 || self.max_memory_percent > 100) {
            return Err(ObjectDataCacheConfigError::InvalidMaxMemoryPercent);
        }

        if self.max_entry_bytes == 0 {
            return Err(ObjectDataCacheConfigError::ZeroMaxEntryBytes);
        }

        if self.max_entry_bytes > MAX_ENTRY_BYTES_LIMIT {
            return Err(ObjectDataCacheConfigError::MaxEntryBytesTooLarge);
        }

        // An explicit capacity must leave room for a full entry plus the
        // weigher overhead, otherwise moka can never retain the entry while
        // fills still report success.
        if self.max_bytes > 0 && self.max_bytes < self.max_entry_bytes.saturating_add(ENTRY_WEIGHT_OVERHEAD_BYTES) {
            return Err(ObjectDataCacheConfigError::MaxEntryBytesExceedsMaxBytes);
        }

        if self.ttl.is_zero() {
            return Err(ObjectDataCacheConfigError::ZeroTimeToLiveSecs);
        }

        if self.ttl.as_secs() > MAX_DURATION_SECS {
            return Err(ObjectDataCacheConfigError::TimeToLiveTooLarge);
        }

        if self.time_to_idle.is_zero() {
            return Err(ObjectDataCacheConfigError::ZeroTimeToIdleSecs);
        }

        if self.time_to_idle.as_secs() > MAX_DURATION_SECS {
            return Err(ObjectDataCacheConfigError::TimeToIdleTooLarge);
        }

        // moka expires at min(ttl, time_to_idle); a larger time_to_idle is
        // inert. Warn instead of rejecting so a benign misconfiguration still
        // starts the cache.
        if self.time_to_idle > self.ttl {
            tracing::warn!(
                time_to_idle_secs = self.time_to_idle.as_secs(),
                ttl_secs = self.ttl.as_secs(),
                "object data cache time_to_idle exceeds ttl; moka expires at min(ttl, time_to_idle) so the larger time_to_idle has no effect"
            );
        }

        if self.min_free_memory_percent == 0 || self.min_free_memory_percent > 100 {
            return Err(ObjectDataCacheConfigError::InvalidMinFreeMemoryPercent);
        }

        if self.fill_concurrency_per_cpu == 0 {
            return Err(ObjectDataCacheConfigError::ZeroFillConcurrencyPerCpu);
        }

        if self.fill_concurrency_max == 0 {
            return Err(ObjectDataCacheConfigError::ZeroFillConcurrencyMax);
        }

        if self.fill_concurrency_max < self.fill_concurrency_per_cpu {
            return Err(ObjectDataCacheConfigError::FillConcurrencyMaxTooSmall);
        }

        if self.identity_keys_max == 0 {
            return Err(ObjectDataCacheConfigError::ZeroIdentityKeysMax);
        }

        Ok(())
    }
}

fn clamp_derived_max_bytes(derived: u64, total_memory: u64) -> u64 {
    let percent_cap = total_memory.saturating_mul(DEFAULT_DERIVED_MAX_MEMORY_PERCENT_CAP) / 100;
    let safe_cap = percent_cap.min(DEFAULT_DERIVED_MAX_BYTES_CAP);

    derived.min(safe_cap)
}

fn log_resolved_capacity_once(resolved_max_bytes: u64, effective_total_bytes: u64, basis: MemoryBasis) {
    RESOLVED_CAPACITY_LOGGED.call_once(|| {
        tracing::info!(
            resolved_max_bytes,
            effective_total_bytes,
            basis = basis.as_str(),
            "object data cache resolved capacity"
        );
    });
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DERIVED_MAX_BYTES_CAP, ObjectDataCacheConfig, ObjectDataCacheMode, clamp_derived_max_bytes};
    use crate::error::ObjectDataCacheConfigError;
    use std::time::Duration;

    #[test]
    fn default_config_matches_v3_baseline() {
        let config = ObjectDataCacheConfig::default();

        assert!(matches!(config.mode, ObjectDataCacheMode::Disabled));
        assert_eq!(config.max_bytes, 0);
        assert_eq!(config.max_memory_percent, 5);
        assert_eq!(config.max_entry_bytes, 1_048_576);
        assert_eq!(config.ttl, Duration::from_secs(60));
        assert_eq!(config.time_to_idle, Duration::from_secs(30));
        assert_eq!(config.min_free_memory_percent, 20);
        assert_eq!(config.fill_concurrency_per_cpu, 1);
        assert_eq!(config.fill_concurrency_max, 32);
        assert_eq!(config.identity_keys_max, 16);
    }

    #[test]
    fn validate_rejects_invalid_memory_percent_when_capacity_is_derived() {
        let config = ObjectDataCacheConfig {
            max_memory_percent: 0,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("derived capacity requires a non-zero memory percent");

        assert_eq!(err, ObjectDataCacheConfigError::InvalidMaxMemoryPercent);
    }

    #[test]
    fn validate_rejects_zero_entry_size() {
        let config = ObjectDataCacheConfig {
            max_entry_bytes: 0,
            ..ObjectDataCacheConfig::default()
        };

        let err = config.validate().expect_err("entry size must stay positive");

        assert_eq!(err, ObjectDataCacheConfigError::ZeroMaxEntryBytes);
    }

    #[test]
    fn validate_rejects_zero_ttl() {
        let config = ObjectDataCacheConfig {
            ttl: Duration::ZERO,
            ..ObjectDataCacheConfig::default()
        };

        let err = config.validate().expect_err("ttl must stay positive");

        assert_eq!(err, ObjectDataCacheConfigError::ZeroTimeToLiveSecs);
    }

    #[test]
    fn validate_rejects_zero_time_to_idle() {
        let config = ObjectDataCacheConfig {
            time_to_idle: Duration::ZERO,
            ..ObjectDataCacheConfig::default()
        };

        let err = config.validate().expect_err("time-to-idle must stay positive");

        assert_eq!(err, ObjectDataCacheConfigError::ZeroTimeToIdleSecs);
    }

    #[test]
    fn validate_rejects_invalid_fill_concurrency_bounds() {
        let config = ObjectDataCacheConfig {
            fill_concurrency_per_cpu: 2,
            fill_concurrency_max: 1,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("max fill concurrency must not be smaller than per-cpu factor");

        assert_eq!(err, ObjectDataCacheConfigError::FillConcurrencyMaxTooSmall);
    }

    #[test]
    fn validate_accepts_explicit_byte_cap() {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 4_194_304,
            max_memory_percent: 0,
            ..ObjectDataCacheConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn resolved_max_bytes_prefers_explicit_cap() {
        let config = ObjectDataCacheConfig {
            max_bytes: 4_194_304,
            ..ObjectDataCacheConfig::default()
        };

        let resolved = config
            .resolved_max_bytes()
            .expect("explicit max_bytes should be returned directly");

        assert_eq!(resolved, 4_194_304);
    }

    #[test]
    fn derived_capacity_is_not_inflated_by_max_entry_bytes() {
        // 512 MiB effective memory with a tiny percent yields a small derived
        // capacity. A large max_entry_bytes must NOT raise the total capacity
        // (the old `.max(max_entry_bytes)` floor did exactly that).
        let host = 512_u64 * 1024 * 1024;
        let derived = host / 100;
        let resolved = clamp_derived_max_bytes(derived, host);

        assert_eq!(resolved, derived);
        assert!(resolved < 1024 * 1024 * 1024);
    }

    #[test]
    fn resolved_max_bytes_rejects_entry_larger_than_capacity() {
        // Derived capacity is clamped to at most 64 GiB, so a 128 GiB entry cap
        // can never fit regardless of the test host's memory.
        let config = ObjectDataCacheConfig {
            max_bytes: 0,
            max_memory_percent: 100,
            max_entry_bytes: 128 * 1024 * 1024 * 1024,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .resolved_max_bytes()
            .expect_err("entry cap above the derived capacity must be rejected");

        assert_eq!(err, ObjectDataCacheConfigError::MaxEntryBytesExceedsCapacity);
    }

    #[test]
    fn derived_max_bytes_clamps_to_v3_safe_cap() {
        let one_tib = 1024_u64 * 1024 * 1024 * 1024;
        let derived = one_tib / 2;
        let resolved = clamp_derived_max_bytes(derived, one_tib);

        assert_eq!(resolved, DEFAULT_DERIVED_MAX_BYTES_CAP);
    }

    #[test]
    fn validate_rejects_ttl_above_upper_bound() {
        let config = ObjectDataCacheConfig {
            ttl: Duration::from_secs(u64::MAX),
            ..ObjectDataCacheConfig::default()
        };

        let err = config.validate().expect_err("ttl above the operational cap must be rejected");

        assert_eq!(err, ObjectDataCacheConfigError::TimeToLiveTooLarge);
    }

    #[test]
    fn validate_rejects_time_to_idle_above_upper_bound() {
        let config = ObjectDataCacheConfig {
            time_to_idle: Duration::from_secs(u64::MAX),
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("time-to-idle above the operational cap must be rejected");

        assert_eq!(err, ObjectDataCacheConfigError::TimeToIdleTooLarge);
    }

    #[test]
    fn validate_rejects_entry_above_weigher_limit() {
        let config = ObjectDataCacheConfig {
            max_entry_bytes: u32::MAX as u64,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("entry size at the u32 weigher boundary must be rejected");

        assert_eq!(err, ObjectDataCacheConfigError::MaxEntryBytesTooLarge);
    }

    #[test]
    fn validate_rejects_entry_larger_than_explicit_max_bytes() {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 2 * 1024 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 8 * 1024 * 1024,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("an entry larger than max_bytes can never be retained");

        assert_eq!(err, ObjectDataCacheConfigError::MaxEntryBytesExceedsMaxBytes);
    }

    #[test]
    fn validate_rejects_entry_equal_to_explicit_max_bytes() {
        // Equal fails because the weigher adds key bytes + per-entry overhead.
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 4 * 1024 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 4 * 1024 * 1024,
            ..ObjectDataCacheConfig::default()
        };

        let err = config
            .validate()
            .expect_err("max_entry_bytes must clear max_bytes by the weigher overhead");

        assert_eq!(err, ObjectDataCacheConfigError::MaxEntryBytesExceedsMaxBytes);
    }

    #[test]
    fn validate_accepts_time_to_idle_greater_than_ttl() {
        // A time_to_idle above ttl is inert (warned, not rejected).
        let config = ObjectDataCacheConfig {
            ttl: Duration::from_secs(30),
            time_to_idle: Duration::from_secs(60),
            ..ObjectDataCacheConfig::default()
        };

        assert!(config.validate().is_ok());
    }
}
