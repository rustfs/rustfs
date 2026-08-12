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

//! Per-bucket durability tier configuration (HP-5 phase 2).
//!
//! A bucket can override the process-wide durability mode
//! (`RUSTFS_DURABILITY_MODE`) with its own tier. The override is stored as a
//! RustFS extension entry (`durability.json`) in the bucket metadata file and
//! is resolved by the disk layer at commit points via
//! `effective_durability(volume)`. System buckets (`.rustfs.sys`,
//! `.minio.sys`) can never carry an override; they stay pinned to `strict`.
//! See docs/operations/durability-modes.md for the semantics.

use serde::{Deserialize, Serialize};

/// Valid per-bucket durability tier names. `legacy-off` is deliberately not
/// representable per bucket: it exists only for the process-wide legacy
/// switch (`RUSTFS_DRIVE_SYNC_ENABLE=false`).
pub const BUCKET_DURABILITY_MODE_STRICT: &str = "strict";
pub const BUCKET_DURABILITY_MODE_RELAXED: &str = "relaxed";
pub const BUCKET_DURABILITY_MODE_NONE: &str = "none";

/// JSON payload stored under the `durability.json` bucket metadata entry.
///
/// An absent entry, an empty payload, or an empty `mode` string all mean
/// "no override": the bucket follows the process-wide durability mode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDurabilityConfig {
    /// Requested tier: `strict` | `relaxed` | `none` (case-insensitive,
    /// whitespace-tolerant). Empty means "inherit the global mode".
    #[serde(default)]
    pub mode: String,
}

impl BucketDurabilityConfig {
    pub fn new(mode: &str) -> Self {
        Self { mode: mode.to_string() }
    }

    /// Whether `mode` names a valid per-bucket tier.
    pub fn is_valid_mode(mode: &str) -> bool {
        matches!(
            mode.trim().to_ascii_lowercase().as_str(),
            BUCKET_DURABILITY_MODE_STRICT | BUCKET_DURABILITY_MODE_RELAXED | BUCKET_DURABILITY_MODE_NONE
        )
    }

    /// The canonical (trimmed, lowercase) tier name, or `None` when the
    /// config does not carry a valid override.
    pub fn normalized_mode(&self) -> Option<String> {
        let mode = self.mode.trim().to_ascii_lowercase();
        if Self::is_valid_mode(&mode) { Some(mode) } else { None }
    }
}

/// Default durability tier seeded into a newly created bucket's metadata
/// (rustfs/backlog#1811). `relaxed` aligns new buckets with MinIO's default
/// posture: object data is still fdatasynced, while xl.meta and directory-entry
/// fsyncs follow the relaxed durability gate.
pub const ENV_NEW_BUCKET_DURABILITY_MODE: &str = "RUSTFS_NEW_BUCKET_DURABILITY_MODE";
pub const DEFAULT_NEW_BUCKET_DURABILITY_MODE: &str = BUCKET_DURABILITY_MODE_RELAXED;

/// The `durability.json` bytes to seed into a freshly created bucket's metadata.
/// Empty means "no override" (the bucket then follows the global
/// `RUSTFS_DURABILITY_MODE`); otherwise the serialized chosen tier. Operators
/// can set `inherit` to disable the new-bucket override. Invalid values also
/// fail closed to inherit the global mode instead of seeding a surprising tier.
pub fn new_bucket_durability_config_json() -> Vec<u8> {
    let raw = std::env::var(ENV_NEW_BUCKET_DURABILITY_MODE).unwrap_or_else(|_| DEFAULT_NEW_BUCKET_DURABILITY_MODE.to_string());
    let mode = raw.trim();
    if mode.eq_ignore_ascii_case("inherit") || mode.is_empty() || !BucketDurabilityConfig::is_valid_mode(mode) {
        return Vec::new();
    }
    serde_json::to_vec(&BucketDurabilityConfig::new(mode)).expect("BucketDurabilityConfig serialization cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_bucket_seeded_mode() -> Option<String> {
        let json = new_bucket_durability_config_json();
        if json.is_empty() {
            return None;
        }
        serde_json::from_slice::<BucketDurabilityConfig>(&json)
            .expect("new-bucket durability config must serialize")
            .normalized_mode()
    }

    #[test]
    fn valid_modes_are_recognized() {
        assert!(BucketDurabilityConfig::is_valid_mode("strict"));
        assert!(BucketDurabilityConfig::is_valid_mode("relaxed"));
        assert!(BucketDurabilityConfig::is_valid_mode("none"));
        assert!(BucketDurabilityConfig::is_valid_mode(" RELAXED "));
        assert!(!BucketDurabilityConfig::is_valid_mode(""));
        assert!(!BucketDurabilityConfig::is_valid_mode("bogus"));
        // The legacy full-off switch is process-wide only.
        assert!(!BucketDurabilityConfig::is_valid_mode("legacy-off"));
    }

    #[test]
    fn normalized_mode_canonicalizes_or_rejects() {
        assert_eq!(BucketDurabilityConfig::new(" Relaxed ").normalized_mode().as_deref(), Some("relaxed"));
        assert_eq!(BucketDurabilityConfig::new("strict").normalized_mode().as_deref(), Some("strict"));
        assert_eq!(BucketDurabilityConfig::default().normalized_mode(), None);
        assert_eq!(BucketDurabilityConfig::new("bogus").normalized_mode(), None);
    }

    #[test]
    fn json_round_trip() {
        let cfg = BucketDurabilityConfig::new("relaxed");
        let json = serde_json::to_vec(&cfg).expect("serialize");
        let back: BucketDurabilityConfig = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(back, cfg);

        // Empty object deserializes to the "inherit" default.
        let empty: BucketDurabilityConfig = serde_json::from_slice(b"{}").expect("deserialize empty");
        assert_eq!(empty.normalized_mode(), None);
    }

    #[test]
    fn new_bucket_default_seeds_relaxed_when_unset() {
        temp_env::with_var_unset(ENV_NEW_BUCKET_DURABILITY_MODE, || {
            assert_eq!(new_bucket_seeded_mode().as_deref(), Some(BUCKET_DURABILITY_MODE_RELAXED));
        });
    }

    #[test]
    fn new_bucket_default_honors_explicit_tiers() {
        for mode in [
            BUCKET_DURABILITY_MODE_STRICT,
            BUCKET_DURABILITY_MODE_RELAXED,
            BUCKET_DURABILITY_MODE_NONE,
        ] {
            temp_env::with_var(ENV_NEW_BUCKET_DURABILITY_MODE, Some(mode), || {
                assert_eq!(new_bucket_seeded_mode().as_deref(), Some(mode));
            });
        }
    }

    #[test]
    fn new_bucket_default_can_inherit_global_mode() {
        for mode in ["inherit", "", "bogus"] {
            temp_env::with_var(ENV_NEW_BUCKET_DURABILITY_MODE, Some(mode), || {
                assert_eq!(new_bucket_seeded_mode(), None);
            });
        }
    }
}
