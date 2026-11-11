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

//! The `kv-cache` engine's bucket config, stored in the bucket's engine
//! binding (`engine.json`). Unknown fields are rejected.

use rustfs_bucket_engine::{BucketEngineBinding, EngineError, EngineResult};
use rustfs_kvcache::{CapacityPolicy, StoreOptions, Watermarks};
use serde::{Deserialize, Serialize};

pub(crate) const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct KvCacheConfig {
    /// Copies per entry. Only 1 is implemented.
    pub replication: u8,
    /// Size of every segment file, MiB.
    pub segment_mib: u64,
    /// Drop segments whose newest entry is older than this.
    pub max_age_secs: Option<u64>,
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub emergency_watermark: f64,
    /// Preallocated empty segments kept per drive.
    pub spare_segments: usize,
    /// Bypass the page cache for segment I/O.
    pub direct_io: bool,
    /// Seconds between capacity passes.
    pub maintenance_interval_secs: u64,
}

impl Default for KvCacheConfig {
    fn default() -> Self {
        let watermarks = Watermarks::default();
        Self {
            replication: 1,
            segment_mib: 1024,
            max_age_secs: None,
            high_watermark: watermarks.high,
            low_watermark: watermarks.low,
            emergency_watermark: watermarks.emergency,
            spare_segments: 2,
            direct_io: true,
            maintenance_interval_secs: 5,
        }
    }
}

impl KvCacheConfig {
    /// The config of a binding; a null config means the defaults.
    pub(crate) fn from_binding(binding: &BucketEngineBinding) -> EngineResult<Self> {
        if binding.config_version != CONFIG_VERSION {
            return Err(EngineError::InvalidConfig(format!(
                "kv-cache config version {} is not supported (expected {CONFIG_VERSION})",
                binding.config_version
            )));
        }
        let config: Self = if binding.config.is_null() {
            Self::default()
        } else {
            serde_json::from_value(binding.config.clone()).map_err(|err| EngineError::InvalidConfig(err.to_string()))?
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> EngineResult<()> {
        let invalid = |message: String| Err(EngineError::InvalidConfig(message));
        if self.replication != 1 {
            return invalid(format!("replication {} is not supported; use 1", self.replication));
        }
        if !(16..=16 * 1024).contains(&self.segment_mib) {
            return invalid(format!("segment_mib {} must be between 16 and 16384", self.segment_mib));
        }
        if self.max_age_secs == Some(0) {
            return invalid("max_age_secs must be positive".to_owned());
        }
        if self.spare_segments > 16 {
            return invalid(format!("spare_segments {} must be at most 16", self.spare_segments));
        }
        if !(1..=3600).contains(&self.maintenance_interval_secs) {
            return invalid(format!(
                "maintenance_interval_secs {} must be between 1 and 3600",
                self.maintenance_interval_secs
            ));
        }
        self.policy().watermarks.validate().map_err(EngineError::InvalidConfig)
    }

    pub(crate) fn store_options(&self) -> StoreOptions {
        StoreOptions {
            segment_bytes: self.segment_mib << 20,
            direct_io: self.direct_io,
            sync_on_seal: false,
        }
    }

    pub(crate) fn policy(&self) -> CapacityPolicy {
        CapacityPolicy {
            watermarks: Watermarks {
                high: self.high_watermark,
                low: self.low_watermark,
                emergency: self.emergency_watermark,
            },
            max_age_ms: self.max_age_secs.map(|secs| secs.saturating_mul(1000)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_bucket_engine::EngineName;

    fn binding(config: serde_json::Value) -> BucketEngineBinding {
        BucketEngineBinding {
            engine: EngineName::new("kv-cache").expect("valid engine name"),
            config_version: CONFIG_VERSION,
            config,
        }
    }

    #[test]
    fn null_config_means_defaults_and_fields_override_them() {
        assert_eq!(
            KvCacheConfig::from_binding(&binding(serde_json::Value::Null)).expect("defaults"),
            KvCacheConfig::default()
        );
        let config =
            KvCacheConfig::from_binding(&binding(serde_json::json!({"segment_mib": 256, "max_age_secs": 3600}))).expect("valid");
        assert_eq!(config.segment_mib, 256);
        assert_eq!(config.store_options().segment_bytes, 256 << 20);
        assert_eq!(config.policy().max_age_ms, Some(3_600_000));
        assert!(config.direct_io);
    }

    #[test]
    fn invalid_configs_are_rejected() {
        for config in [
            serde_json::json!({"replication": 2}),
            serde_json::json!({"segment_mib": 8}),
            serde_json::json!({"max_age_secs": 0}),
            serde_json::json!({"high_watermark": 0.95}),
            serde_json::json!({"low_watermark": 0.85}),
            serde_json::json!({"spare_segments": 100}),
            serde_json::json!({"maintenance_interval_secs": 0}),
            serde_json::json!({"unknown": true}),
            serde_json::json!([1, 2]),
        ] {
            assert!(KvCacheConfig::from_binding(&binding(config.clone())).is_err(), "{config}");
        }
        let mut wrong_version = binding(serde_json::Value::Null);
        wrong_version.config_version = 2;
        assert!(KvCacheConfig::from_binding(&wrong_version).is_err());
    }
}
