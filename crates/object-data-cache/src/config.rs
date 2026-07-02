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
use std::time::Duration;
use sysinfo::System;

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
    pub ttl: Duration,
    /// Time-to-idle for a cache entry.
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

        let mut system = System::new();
        system.refresh_memory();
        let total_memory = system.total_memory();
        let derived = total_memory.saturating_mul(u64::from(self.max_memory_percent)) / 100;
        let resolved = derived.max(self.max_entry_bytes);

        if resolved == 0 {
            return Err(ObjectDataCacheConfigError::ZeroResolvedMaxBytes);
        }

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

        if self.ttl.is_zero() {
            return Err(ObjectDataCacheConfigError::ZeroTimeToLiveSecs);
        }

        if self.time_to_idle.is_zero() {
            return Err(ObjectDataCacheConfigError::ZeroTimeToIdleSecs);
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

#[cfg(test)]
mod tests {
    use super::{ObjectDataCacheConfig, ObjectDataCacheMode};
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
    fn resolved_max_bytes_is_at_least_max_entry_bytes() {
        let config = ObjectDataCacheConfig {
            max_bytes: 0,
            max_memory_percent: 1,
            max_entry_bytes: 8_388_608,
            ..ObjectDataCacheConfig::default()
        };

        let resolved = config.resolved_max_bytes().expect("derived capacity should stay positive");

        assert!(resolved >= config.max_entry_bytes);
    }
}
