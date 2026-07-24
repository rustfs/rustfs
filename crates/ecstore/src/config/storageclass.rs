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

use crate::error::{Error, Result};
use rustfs_config::server_config::{KV, KVS};
use serde::{Deserialize, Serialize};
use std::env::{self, VarError};
use std::sync::LazyLock;
use tracing::warn;

/// Default parity count for a given drive count
/// The default configuration allocates the number of check disks based on the total number of disks
pub fn default_parity_count(drive: usize) -> usize {
    match drive {
        1 => 0,
        2 | 3 => 1,
        4 | 5 => 2,
        6 | 7 => 3,
        _ => 4,
    }
}

// Standard constants for all storage class
pub const RRS: &str = "REDUCED_REDUNDANCY";
pub const STANDARD: &str = "STANDARD";

// AWS S3 Storage Classes
pub const DEEP_ARCHIVE: &str = "DEEP_ARCHIVE";
pub const EXPRESS_ONEZONE: &str = "EXPRESS_ONEZONE";
pub const GLACIER: &str = "GLACIER";
pub const GLACIER_IR: &str = "GLACIER_IR";
pub const INTELLIGENT_TIERING: &str = "INTELLIGENT_TIERING";
pub const ONEZONE_IA: &str = "ONEZONE_IA";
pub const OUTPOSTS: &str = "OUTPOSTS";
pub const SNOW: &str = "SNOW";
pub const STANDARD_IA: &str = "STANDARD_IA";

/// Version of the client-discoverable storage-class write contract.
pub const CAPABILITY_CONTRACT_VERSION: u32 = 1;
/// Storage classes whose write semantics RustFS implements.
pub const SUPPORTED_WRITE_CLASSES: [&str; 2] = [STANDARD, RRS];
/// Stable S3 error code returned for unsupported write classes.
pub const UNSUPPORTED_WRITE_ERROR: &str = "InvalidStorageClass";
/// Compatibility behavior applied to historical label-only object metadata.
pub const LEGACY_LABEL_BEHAVIOR: &str = "normalized_to_effective_class";

/// Returns whether a client may select this storage class for a write.
pub fn is_supported_write_class(storage_class: &str) -> bool {
    SUPPORTED_WRITE_CLASSES.contains(&storage_class)
}

/// Resolves the storage class that truthfully describes the stored object.
///
/// A completed lifecycle transition is a real storage tier and therefore keeps
/// its tier name. For local objects, only RRS has distinct layout semantics;
/// historical AWS class labels otherwise describe the effective STANDARD
/// layout.
pub fn effective_class<'a>(stored_class: Option<&'a str>, transitioned_tier: Option<&'a str>) -> &'a str {
    if let Some(tier) = transitioned_tier.filter(|tier| !tier.is_empty()) {
        return tier;
    }

    if stored_class == Some(RRS) { RRS } else { STANDARD }
}

// Standard constants for config info storage class
pub const CLASS_STANDARD: &str = "standard";
pub const CLASS_RRS: &str = "rrs";
pub const OPTIMIZE: &str = "optimize";
pub const INLINE_BLOCK: &str = "inline_block";

// Reduced redundancy storage class environment variable
pub const RRS_ENV: &str = "RUSTFS_STORAGE_CLASS_RRS";
// Standard storage class environment variable
pub const STANDARD_ENV: &str = "RUSTFS_STORAGE_CLASS_STANDARD";
// Optimize storage class environment variable
pub const OPTIMIZE_ENV: &str = "RUSTFS_STORAGE_CLASS_OPTIMIZE";
// Inline block indicates the size of the shard that is considered for inlining
pub const INLINE_BLOCK_ENV: &str = "RUSTFS_STORAGE_CLASS_INLINE_BLOCK";

// Supported storage class scheme is EC
pub const SCHEME_PREFIX: &str = "EC";

// Min parity drives
pub const MIN_PARITY_DRIVES: usize = 0;

// Default RRS parity is always minimum parity.
pub const DEFAULT_RRS_PARITY: usize = 1;
const DEFAULT_RRS_STORAGE_CLASS: &str = "EC:1";
const ZERO_SET_DRIVE_COUNT_ERROR: &str = "set drive count must be greater than zero";

pub static DEFAULT_INLINE_BLOCK: usize = 128 * 1024;

pub static DEFAULT_KVS: LazyLock<KVS> = LazyLock::new(|| {
    let kvs = vec![
        KV {
            key: CLASS_STANDARD.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: CLASS_RRS.to_owned(),
            value: DEFAULT_RRS_STORAGE_CLASS.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OPTIMIZE.to_owned(),
            value: "availability".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: INLINE_BLOCK.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
    ];

    KVS(kvs)
});

// StorageClass - holds storage class information
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StorageClass {
    parity: usize,
}

#[derive(Debug, Clone)]
struct PoolParity {
    drives_per_set: usize,
    parity: usize,
    automatic: bool,
}

// Config storage class configuration
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Config {
    standard: StorageClass,
    rrs: StorageClass,
    optimize: Option<String>,
    inline_block: usize,
    initialized: bool,
    #[serde(skip)]
    standard_parities: Vec<PoolParity>,
    #[serde(skip)]
    rrs_parities: Vec<PoolParity>,
}

impl Config {
    fn storage_class(&self, sc: &str) -> (&StorageClass, &[PoolParity]) {
        match sc.trim() {
            RRS => (&self.rrs, &self.rrs_parities),
            _ => (&self.standard, &self.standard_parities),
        }
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn get_parity_for_sc(&self, sc: &str) -> Option<usize> {
        if !self.initialized {
            return None;
        }

        let (storage_class, pools) = self.storage_class(sc);
        let Some(first) = pools.first() else {
            return Some(storage_class.parity);
        };

        pools.iter().all(|pool| pool.parity == first.parity).then_some(first.parity)
    }

    /// Returns the resolved parity for a pool topology.
    ///
    /// A topology-bound lookup fails closed for unknown drive counts and for
    /// deserialized legacy configurations that have no pool topology. Legacy
    /// callers retain scalar compatibility through [`Self::get_parity_for_sc`].
    pub(crate) fn parity_for_sc(&self, sc: &str, drives_per_set: usize) -> Option<usize> {
        if !self.initialized {
            return None;
        }

        let (_, pools) = self.storage_class(sc);

        pools
            .iter()
            .find(|pool| pool.drives_per_set == drives_per_set)
            .map(|pool| pool.parity)
    }

    pub(crate) fn parity_for_pool(&self, sc: &str, pool_index: usize, drives_per_set: usize) -> Option<usize> {
        if !self.initialized {
            return None;
        }

        let (_, pools) = self.storage_class(sc);

        pools
            .get(pool_index)
            .filter(|pool| pool.drives_per_set == drives_per_set)
            .map(|pool| pool.parity)
    }

    pub fn parities_for_sc(&self, sc: &str) -> Option<Vec<usize>> {
        if !self.initialized {
            return None;
        }

        let (_, pools) = self.storage_class(sc);
        if pools.is_empty() {
            return None;
        }

        Some(pools.iter().map(|pool| pool.parity).collect())
    }

    pub(crate) fn automatic_zero_parity_pools(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.standard_parities
            .iter()
            .enumerate()
            .filter(|(_, pool)| pool.automatic && pool.parity == 0)
            .map(|(pool_index, pool)| (pool_index, pool.drives_per_set))
    }

    pub fn should_inline(&self, shard_size: i64, versioned: bool) -> bool {
        if shard_size < 0 {
            return false;
        }

        let shard_size = shard_size as usize;

        let mut inline_block = DEFAULT_INLINE_BLOCK;
        if self.initialized {
            inline_block = self.inline_block;
        }

        if versioned {
            shard_size <= inline_block / 8
        } else {
            shard_size <= inline_block
        }
    }

    pub fn inline_block(&self) -> usize {
        if !self.initialized {
            DEFAULT_INLINE_BLOCK
        } else {
            self.inline_block
        }
    }

    pub fn capacity_optimized(&self) -> bool {
        if !self.initialized {
            false
        } else {
            self.optimize.as_ref().is_some_and(|v| v.as_str() == "capacity")
        }
    }
}

#[derive(Debug, Default)]
struct StorageClassEnvOverrides {
    standard: Option<String>,
    rrs: Option<String>,
    optimize: Option<String>,
    inline_block: Option<String>,
}

impl StorageClassEnvOverrides {
    fn from_process() -> Result<Self> {
        Ok(Self {
            standard: read_optional_env(STANDARD_ENV)?,
            rrs: read_optional_env(RRS_ENV)?,
            optimize: read_optional_env(OPTIMIZE_ENV)?,
            inline_block: read_optional_env(INLINE_BLOCK_ENV)?,
        })
    }
}

fn read_optional_env(name: &str) -> Result<Option<String>> {
    parse_optional_env(name, env::var(name))
}

fn parse_optional_env(name: &str, value: std::result::Result<String, VarError>) -> Result<Option<String>> {
    match value {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(Error::other(format!("{name} contains non-Unicode data"))),
    }
}

#[derive(Debug, Clone, Copy)]
enum ParityPolicy {
    AutomaticStandard,
    LegacyRrs,
    Explicit(usize),
}

impl ParityPolicy {
    fn resolve(self, drives_per_set: usize) -> usize {
        match self {
            Self::AutomaticStandard => default_parity_count(drives_per_set),
            Self::LegacyRrs => {
                if drives_per_set == 1 {
                    0
                } else {
                    DEFAULT_RRS_PARITY
                }
            }
            Self::Explicit(parity) => parity,
        }
    }
}

fn standard_policy(kvs: &KVS, value: Option<String>) -> Result<ParityPolicy> {
    match value {
        Some(value) if value.is_empty() => Ok(ParityPolicy::AutomaticStandard),
        Some(value) => Ok(ParityPolicy::Explicit(parse_storage_class(&value)?.parity)),
        None => {
            let value = kvs.get(CLASS_STANDARD);
            if value.is_empty() {
                Ok(ParityPolicy::AutomaticStandard)
            } else {
                Ok(ParityPolicy::Explicit(parse_storage_class(&value)?.parity))
            }
        }
    }
}

fn rrs_policy(kvs: &KVS, value: Option<String>) -> Result<ParityPolicy> {
    match value {
        Some(value) if value.is_empty() => Ok(ParityPolicy::LegacyRrs),
        Some(value) => Ok(ParityPolicy::Explicit(parse_storage_class(&value)?.parity)),
        None => {
            let value = kvs.get(CLASS_RRS);
            if value.is_empty() || value == DEFAULT_RRS_STORAGE_CLASS {
                Ok(ParityPolicy::LegacyRrs)
            } else {
                Ok(ParityPolicy::Explicit(parse_storage_class(&value)?.parity))
            }
        }
    }
}

pub fn lookup_config_for_pools(kvs: &KVS, set_drive_counts: &[usize]) -> Result<Config> {
    lookup_config_for_pools_with_env(kvs, set_drive_counts, StorageClassEnvOverrides::from_process()?)
}

fn lookup_config_for_pools_with_env(
    kvs: &KVS,
    set_drive_counts: &[usize],
    overrides: StorageClassEnvOverrides,
) -> Result<Config> {
    if set_drive_counts.is_empty() {
        return Err(Error::other("storage class requires at least one pool"));
    }

    let standard_policy = standard_policy(kvs, overrides.standard)?;
    let rrs_policy = rrs_policy(kvs, overrides.rrs)?;
    let mut standard_parities = Vec::with_capacity(set_drive_counts.len());
    let mut rrs_parities = Vec::with_capacity(set_drive_counts.len());

    for (pool_index, &drives_per_set) in set_drive_counts.iter().enumerate() {
        let standard_parity = standard_policy.resolve(drives_per_set);
        let rrs_parity = rrs_policy.resolve(drives_per_set);
        validate_parity_inner(standard_parity, rrs_parity, drives_per_set).map_err(|err| {
            Error::other(format!(
                "storage class validation failed for pool {pool_index} ({drives_per_set} drives): {err}"
            ))
        })?;
        standard_parities.push(PoolParity {
            drives_per_set,
            parity: standard_parity,
            automatic: matches!(standard_policy, ParityPolicy::AutomaticStandard),
        });
        rrs_parities.push(PoolParity {
            drives_per_set,
            parity: rrs_parity,
            automatic: matches!(rrs_policy, ParityPolicy::LegacyRrs),
        });
    }

    let optimize = overrides.optimize;
    let inline_block = if let Some(value) = overrides.inline_block {
        let block = value
            .parse::<bytesize::ByteSize>()
            .map_err(|_| Error::other(format!("parse {INLINE_BLOCK_ENV} format failed")))?;
        let inline_block = usize::try_from(block.as_u64())
            .map_err(|_| Error::other(format!("{INLINE_BLOCK_ENV} exceeds the platform size limit")))?;
        if inline_block > DEFAULT_INLINE_BLOCK {
            warn!(
                event = "storage_class_inline_block_large",
                component = "ecstore",
                subsystem = "storage_class",
                state = "configured",
                inline_block,
                recommended_max = DEFAULT_INLINE_BLOCK,
                "storage class inline block exceeds recommendation"
            );
        }
        inline_block
    } else {
        DEFAULT_INLINE_BLOCK
    };

    Ok(Config {
        standard: StorageClass {
            parity: standard_parities[0].parity,
        },
        rrs: StorageClass {
            parity: rrs_parities[0].parity,
        },
        optimize,
        inline_block,
        initialized: true,
        standard_parities,
        rrs_parities,
    })
}

pub fn lookup_config(kvs: &KVS, set_drive_count: usize) -> Result<Config> {
    lookup_config_for_pools(kvs, &[set_drive_count])
}

#[cfg(test)]
pub(crate) fn lookup_config_for_pools_without_env(kvs: &KVS, set_drive_counts: &[usize]) -> Result<Config> {
    lookup_config_for_pools_with_env(kvs, set_drive_counts, StorageClassEnvOverrides::default())
}

pub fn parse_storage_class(env: &str) -> Result<StorageClass> {
    let s: Vec<&str> = env.split(':').collect();

    // only two elements allowed in the string - "scheme" and "number of parity drives"
    if s.len() != 2 {
        return Err(Error::other(format!(
            "Invalid storage class format: {env}. Expected 'Scheme:Number of parity drives'."
        )));
    }

    // only allowed scheme is "EC"
    if s[0] != SCHEME_PREFIX {
        return Err(Error::other(format!("Unsupported scheme {}. Supported scheme is EC.", s[0])));
    }

    // Number of parity drives should be integer
    let parity_drives: usize = match s[1].parse() {
        Ok(num) => num,
        Err(_) => return Err(Error::other(format!("Failed to parse parity value: {}.", s[1]))),
    };

    Ok(StorageClass { parity: parity_drives })
}

// ValidateParity validates standard storage class parity.
pub fn validate_parity(ss_parity: usize, set_drive_count: usize) -> Result<()> {
    // if ss_parity > 0 && ss_parity < MIN_PARITY_DRIVES {
    //     return Err(Error::other(format!(
    //         "parity {} should be greater than or equal to {}",
    //         ss_parity, MIN_PARITY_DRIVES
    //     )));
    // }

    if set_drive_count == 0 {
        return Err(Error::other(ZERO_SET_DRIVE_COUNT_ERROR));
    }

    if ss_parity > set_drive_count / 2 {
        return Err(Error::other(format!(
            "parity {} should be less than or equal to {}",
            ss_parity,
            set_drive_count / 2
        )));
    }

    Ok(())
}

// Validates the parity drives.
pub fn validate_parity_inner(ss_parity: usize, rrs_parity: usize, set_drive_count: usize) -> Result<()> {
    // if ss_parity > 0 && ss_parity < MIN_PARITY_DRIVES {
    //     return Err(Error::other(format!(
    //         "Standard storage class parity {} should be greater than or equal to {}",
    //         ss_parity, MIN_PARITY_DRIVES
    //     )));
    // }

    // RRS parity drives should be greater than or equal to minParityDrives.
    // Parity below minParityDrives is not supported.
    // if rrs_parity > 0 && rrs_parity < MIN_PARITY_DRIVES {
    //     return Err(Error::other(format!(
    //         "Reduced redundancy storage class parity {} should be greater than or equal to {}",
    //         rrs_parity, MIN_PARITY_DRIVES
    //     )));
    // }

    if set_drive_count == 0 {
        return Err(Error::other(ZERO_SET_DRIVE_COUNT_ERROR));
    }

    if ss_parity > set_drive_count / 2 {
        return Err(Error::other(format!(
            "Standard storage class parity {} should be less than or equal to {}",
            ss_parity,
            set_drive_count / 2
        )));
    }

    if rrs_parity > set_drive_count / 2 {
        return Err(Error::other(format!(
            "Reduced redundancy storage class parity {} should be less than or equal to {}",
            rrs_parity,
            set_drive_count / 2
        )));
    }

    if ss_parity > 0 && rrs_parity > 0 && ss_parity < rrs_parity {
        return Err(Error::other(format!(
            "Standard storage class parity drives {ss_parity} should be greater than or equal to Reduced redundancy storage class parity drives {rrs_parity}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env_overrides() -> StorageClassEnvOverrides {
        StorageClassEnvOverrides::default()
    }

    #[test]
    fn should_inline_preserves_exact_default_shard_boundaries() {
        let config = Config::default();

        for (case, shard_size, versioned, expected) in [
            ("unversioned below", 128 * 1024 - 1, false, true),
            ("unversioned exact", 128 * 1024, false, true),
            ("unversioned above", 128 * 1024 + 1, false, false),
            ("versioned below", 16 * 1024 - 1, true, true),
            ("versioned exact", 16 * 1024, true, true),
            ("versioned above", 16 * 1024 + 1, true, false),
            ("negative", -1, false, false),
        ] {
            assert_eq!(
                config.should_inline(shard_size, versioned),
                expected,
                "{case}: shard_size={shard_size}, versioned={versioned}"
            );
        }
    }

    #[test]
    fn should_inline_preserves_exact_default_ec_2_2_object_boundaries() {
        let config = Config::default();
        let erasure = crate::erasure::coding::Erasure::new(2, 2, 1024 * 1024);

        for (case, object_size, versioned, expected_shard_size, expected) in [
            ("unversioned below", 256 * 1024 - 1, false, 128 * 1024, true),
            ("unversioned exact", 256 * 1024, false, 128 * 1024, true),
            ("unversioned above", 256 * 1024 + 1, false, 128 * 1024 + 1, false),
            ("versioned below", 32 * 1024 - 1, true, 16 * 1024, true),
            ("versioned exact", 32 * 1024, true, 16 * 1024, true),
            ("versioned above", 32 * 1024 + 1, true, 16 * 1024 + 1, false),
        ] {
            let shard_size = erasure.shard_file_size(object_size);
            assert_eq!(shard_size, expected_shard_size, "{case}: object_size={object_size}");
            assert_eq!(
                config.should_inline(shard_size, versioned),
                expected,
                "{case}: object_size={object_size}, shard_size={shard_size}, versioned={versioned}"
            );
        }
    }

    #[test]
    fn write_capability_contract_only_accepts_implemented_layouts() {
        assert_eq!(SUPPORTED_WRITE_CLASSES, [STANDARD, RRS]);
        assert!(is_supported_write_class(STANDARD));
        assert!(is_supported_write_class(RRS));

        for label_only_class in [
            DEEP_ARCHIVE,
            EXPRESS_ONEZONE,
            GLACIER,
            GLACIER_IR,
            INTELLIGENT_TIERING,
            ONEZONE_IA,
            OUTPOSTS,
            SNOW,
            STANDARD_IA,
        ] {
            assert!(
                !is_supported_write_class(label_only_class),
                "{label_only_class} must not be advertised as a supported write class"
            );
        }
        assert!(!is_supported_write_class(""));
        assert!(!is_supported_write_class("standard"));
        assert!(!is_supported_write_class("UNKNOWN"));
    }

    #[test]
    fn effective_class_normalizes_legacy_labels_and_preserves_real_tiers() {
        assert_eq!(effective_class(None, None), STANDARD);
        assert_eq!(effective_class(Some(STANDARD), None), STANDARD);
        assert_eq!(effective_class(Some(RRS), None), RRS);
        assert_eq!(effective_class(Some(STANDARD_IA), None), STANDARD);
        assert_eq!(effective_class(Some(GLACIER), None), STANDARD);
        assert_eq!(effective_class(Some("UNKNOWN"), None), STANDARD);

        assert_eq!(effective_class(Some(STANDARD_IA), Some("WARM-TIER")), "WARM-TIER");
        assert_eq!(effective_class(Some(STANDARD), Some(STANDARD_IA)), STANDARD_IA);
        assert_eq!(effective_class(Some(RRS), Some("CUSTOM-RRS-TIER")), "CUSTOM-RRS-TIER");
    }

    #[test]
    fn automatic_parity_is_resolved_per_pool() {
        let cfg = lookup_config_for_pools_with_env(&KVS::new(), &[4, 2], no_env_overrides())
            .expect("automatic storage class should support heterogeneous pools");

        assert_eq!(cfg.parity_for_sc(STANDARD, 4), Some(2));
        assert_eq!(cfg.parity_for_sc(STANDARD, 2), Some(1));
        assert_eq!(cfg.parity_for_sc(RRS, 4), Some(1));
        assert_eq!(cfg.parity_for_sc(RRS, 2), Some(1));
        assert_eq!(cfg.get_parity_for_sc(STANDARD), None, "heterogeneous parity has no truthful scalar");

        let cfg = lookup_config_for_pools_with_env(&KVS::new(), &[4, 6], no_env_overrides())
            .expect("automatic parity should be valid for both pools");
        assert_eq!(cfg.parity_for_sc(STANDARD, 4), Some(2));
        assert_eq!(cfg.parity_for_sc(STANDARD, 6), Some(3));
    }

    #[test]
    fn automatic_single_disk_pool_uses_zero_parity() {
        let cfg = lookup_config_for_pools_with_env(&KVS::new(), &[4, 1], no_env_overrides())
            .expect("automatic single-disk storage should remain supported");

        assert_eq!(cfg.parity_for_sc(STANDARD, 4), Some(2));
        assert_eq!(cfg.parity_for_sc(STANDARD, 1), Some(0));
        assert_eq!(cfg.parity_for_sc(RRS, 1), Some(0));
    }

    #[test]
    fn explicit_standard_parity_is_validated_against_every_pool() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:2".to_string());

        let err = lookup_config_for_pools_with_env(&kvs, &[4, 2], no_env_overrides())
            .expect_err("EC:2 must be rejected by the two-drive pool");
        assert!(
            err.to_string().contains("pool 1") && err.to_string().contains("2 drives"),
            "error must identify the rejecting pool: {err}"
        );

        kvs.insert(CLASS_STANDARD.to_string(), "EC:1".to_string());
        let cfg = lookup_config_for_pools_with_env(&kvs, &[4, 2], no_env_overrides()).expect("EC:1 is valid for both pools");
        assert_eq!(cfg.parity_for_sc(STANDARD, 4), Some(1));
        assert_eq!(cfg.parity_for_sc(STANDARD, 2), Some(1));
        assert_eq!(cfg.get_parity_for_sc(STANDARD), Some(1));
    }

    #[test]
    fn environment_rrs_value_is_explicit_but_persisted_default_is_legacy() {
        let persisted_default = lookup_config_for_pools_with_env(&DEFAULT_KVS, &[1], no_env_overrides())
            .expect("persisted default RRS EC:1 should retain single-disk compatibility");
        assert_eq!(persisted_default.parity_for_sc(RRS, 1), Some(0));

        let env = StorageClassEnvOverrides {
            rrs: Some("EC:1".to_string()),
            ..Default::default()
        };
        let err = lookup_config_for_pools_with_env(&DEFAULT_KVS, &[1], env)
            .expect_err("environment RRS EC:1 is explicit and must fail on a single disk");
        assert!(err.to_string().contains("pool 0") && err.to_string().contains("1 drives"));
    }

    #[test]
    fn explicit_environment_standard_parity_is_not_clamped() {
        let env = StorageClassEnvOverrides {
            standard: Some("EC:2".to_string()),
            ..Default::default()
        };
        let err = lookup_config_for_pools_with_env(&KVS::new(), &[4, 2], env)
            .expect_err("explicit environment parity must not be clamped for a smaller pool");
        assert!(err.to_string().contains("pool 1") && err.to_string().contains("2 drives"));
    }

    #[test]
    fn empty_environment_values_retain_automatic_semantics() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:2".to_string());
        kvs.insert(CLASS_RRS.to_string(), "EC:2".to_string());
        let env = StorageClassEnvOverrides {
            standard: Some(String::new()),
            rrs: Some(String::new()),
            ..Default::default()
        };

        let cfg = lookup_config_for_pools_with_env(&kvs, &[4, 2], env)
            .expect("empty environment values should override persisted parity with automatic defaults");
        assert_eq!(cfg.parities_for_sc(STANDARD), Some(vec![2, 1]));
        assert_eq!(cfg.parities_for_sc(RRS), Some(vec![1, 1]));
    }

    #[test]
    fn explicit_zero_parity_is_not_reported_as_automatic() {
        let env = StorageClassEnvOverrides {
            standard: Some("EC:0".to_string()),
            ..Default::default()
        };

        let cfg = lookup_config_for_pools_with_env(&KVS::new(), &[1], env)
            .expect("explicit zero parity should remain valid for a single-drive pool");
        assert_eq!(cfg.parities_for_sc(STANDARD), Some(vec![0]));
        assert_eq!(cfg.automatic_zero_parity_pools().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_value_fails_closed() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let err = parse_optional_env(STANDARD_ENV, Err(VarError::NotUnicode(OsString::from_vec(vec![0xff]))))
            .expect_err("non-Unicode parity must fail closed");
        assert!(err.to_string().contains(STANDARD_ENV));
    }

    #[test]
    fn environment_standard_override_can_rescue_persisted_config() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:2".to_string());
        let env = StorageClassEnvOverrides {
            standard: Some("EC:1".to_string()),
            ..Default::default()
        };

        let cfg = lookup_config_for_pools_with_env(&kvs, &[4, 2], env)
            .expect("environment override should replace the persisted parity before validation");
        assert_eq!(cfg.parity_for_pool(STANDARD, 0, 4), Some(1));
        assert_eq!(cfg.parity_for_pool(STANDARD, 1, 2), Some(1));
    }

    #[test]
    fn invalid_or_unknown_pool_topology_fails_closed() {
        assert!(lookup_config_for_pools_with_env(&KVS::new(), &[], no_env_overrides()).is_err());

        let err = lookup_config_for_pools_with_env(&KVS::new(), &[4, 0], no_env_overrides())
            .expect_err("zero-drive pools must be rejected");
        assert!(err.to_string().contains("pool 1") && err.to_string().contains("0 drives"));

        let cfg =
            lookup_config_for_pools_with_env(&KVS::new(), &[4, 2], no_env_overrides()).expect("valid topology should resolve");
        assert_eq!(cfg.parity_for_pool(STANDARD, 2, 6), None);
        assert_eq!(cfg.parity_for_pool(STANDARD, 1, 6), None);
        assert_eq!(cfg.parity_for_pool(STANDARD, 0, 2), None);
        assert_eq!(cfg.parity_for_pool(STANDARD, 1, 4), None);
        assert_eq!(cfg.parity_for_sc(STANDARD, 6), None);
    }

    #[test]
    fn resolved_pool_state_is_not_serialized() {
        let cfg =
            lookup_config_for_pools_with_env(&KVS::new(), &[4, 2], no_env_overrides()).expect("valid topology should resolve");
        let encoded = serde_json::to_string(&cfg).expect("config should serialize");
        assert!(!encoded.contains("standard_parities"));
        assert!(!encoded.contains("rrs_parities"));

        let decoded: Config = serde_json::from_str(&encoded).expect("legacy scalar config should deserialize");
        assert_eq!(decoded.get_parity_for_sc(STANDARD), Some(2));
        assert_eq!(decoded.parity_for_pool(STANDARD, 0, 4), None);
        assert_eq!(decoded.parity_for_sc(STANDARD, 4), None);
        assert_eq!(decoded.parities_for_sc(STANDARD), None);
        assert!(validate_parity(0, 0).is_err());
    }

    #[test]
    fn lookup_config_reads_rrs_from_class_rrs_key() {
        // Regression: kvs.get(RRS) used RRS="REDUCED_REDUNDANCY" instead of
        // CLASS_RRS="rrs", so admin-written RRS values were never read back.
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:4".to_string());
        kvs.insert(CLASS_RRS.to_string(), "EC:2".to_string());

        let cfg = lookup_config_for_pools_without_env(&kvs, &[8]).expect("lookup should succeed");
        assert_eq!(cfg.standard.parity, 4, "standard parity should be 4");
        assert_eq!(cfg.rrs.parity, 2, "rrs parity should be 2");
    }

    #[test]
    fn lookup_config_ignores_redundancy_key_name() {
        // Ensure the old key name "REDUCED_REDUNDANCY" is NOT read.
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:4".to_string());
        kvs.insert(RRS.to_string(), "EC:2".to_string());

        let cfg = lookup_config_for_pools_without_env(&kvs, &[8]).expect("lookup should succeed");
        assert_eq!(cfg.standard.parity, 4);
        assert_eq!(
            cfg.rrs.parity, DEFAULT_RRS_PARITY,
            "rrs should fall back to default when CLASS_RRS key is absent"
        );
    }
}
