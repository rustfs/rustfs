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

use super::KVS;
use crate::config::KV;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::env;
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
            value: "EC:1".to_owned(),
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
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct StorageClass {
    parity: usize,
}

// Config storage class configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    standard: StorageClass,
    rrs: StorageClass,
    optimize: Option<String>,
    inline_block: usize,
    initialized: bool,
}

impl Config {
    pub fn get_parity_for_sc(&self, sc: &str) -> Option<usize> {
        match sc.trim() {
            RRS => {
                if self.initialized {
                    Some(self.rrs.parity)
                } else {
                    None
                }
            }
            _ => {
                if self.initialized {
                    Some(self.standard.parity)
                } else {
                    None
                }
            }
        }
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

pub fn lookup_config(kvs: &KVS, set_drive_count: usize) -> Result<Config> {
    let standard = {
        let ssc_str = {
            if let Ok(ssc_str) = env::var(STANDARD_ENV) {
                ssc_str
            } else {
                kvs.get(CLASS_STANDARD)
            }
        };

        if !ssc_str.is_empty() {
            parse_storage_class(&ssc_str)?
        } else {
            StorageClass {
                parity: default_parity_count(set_drive_count),
            }
        }
    };

    let rrs = {
        let ssc_str = {
            if let Ok(ssc_str) = env::var(RRS_ENV) {
                ssc_str
            } else {
                kvs.get(RRS)
            }
        };

        if !ssc_str.is_empty() {
            parse_storage_class(&ssc_str)?
        } else {
            StorageClass {
                parity: { if set_drive_count == 1 { 0 } else { DEFAULT_RRS_PARITY } },
            }
        }
    };

    validate_parity_inner(standard.parity, rrs.parity, set_drive_count)?;

    let optimize = { env::var(OPTIMIZE_ENV).ok() };

    let inline_block = {
        if let Ok(ev) = env::var(INLINE_BLOCK_ENV) {
            if let Ok(block) = ev.parse::<bytesize::ByteSize>() {
                if block.as_u64() as usize > DEFAULT_INLINE_BLOCK {
                    warn!(
                        "inline block value bigger than recommended max of 128KiB -> {}, performance may degrade for PUT please benchmark the changes",
                        block
                    );
                }
                block.as_u64() as usize
            } else {
                return Err(Error::other(format!("parse {INLINE_BLOCK_ENV} format failed")));
            }
        } else {
            DEFAULT_INLINE_BLOCK
        }
    };

    Ok(Config {
        standard,
        rrs,
        optimize,
        inline_block,
        initialized: true,
    })
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

    if set_drive_count > 2 {
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
    }

    if ss_parity > 0 && rrs_parity > 0 && ss_parity < rrs_parity {
        return Err(Error::other(format!(
            "Standard storage class parity drives {ss_parity} should be greater than or equal to Reduced redundancy storage class parity drives {rrs_parity}"
        )));
    }
    Ok(())
}
