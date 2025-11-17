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

use crate::{Error, Result};

/// Prefix for pool index in set disk identifiers.
const POOL_PREFIX: &str = "pool";
/// Prefix for set index in set disk identifiers.
const SET_PREFIX: &str = "set";

/// Format a set disk identifier using unsigned indices.
pub fn format_set_disk_id(pool_idx: usize, set_idx: usize) -> String {
    format!("{POOL_PREFIX}_{pool_idx}_{SET_PREFIX}_{set_idx}")
}

/// Format a set disk identifier from signed indices.
pub fn format_set_disk_id_from_i32(pool_idx: i32, set_idx: i32) -> Option<String> {
    if pool_idx < 0 || set_idx < 0 {
        None
    } else {
        Some(format_set_disk_id(pool_idx as usize, set_idx as usize))
    }
}

/// Normalise external set disk identifiers into the canonical format.
pub fn normalize_set_disk_id(raw: &str) -> Option<String> {
    if raw.starts_with(&format!("{POOL_PREFIX}_")) {
        Some(raw.to_string())
    } else {
        parse_compact_set_disk_id(raw).map(|(pool, set)| format_set_disk_id(pool, set))
    }
}

/// Parse a canonical set disk identifier into pool/set indices.
pub fn parse_set_disk_id(raw: &str) -> Result<(usize, usize)> {
    let parts: Vec<&str> = raw.split('_').collect();
    if parts.len() != 4 || parts[0] != POOL_PREFIX || parts[2] != SET_PREFIX {
        return Err(Error::TaskExecutionFailed {
            message: format!("Invalid set_disk_id format: {raw}"),
        });
    }

    let pool_idx = parts[1].parse::<usize>().map_err(|_| Error::TaskExecutionFailed {
        message: format!("Invalid pool index in set_disk_id: {raw}"),
    })?;
    let set_idx = parts[3].parse::<usize>().map_err(|_| Error::TaskExecutionFailed {
        message: format!("Invalid set index in set_disk_id: {raw}"),
    })?;
    Ok((pool_idx, set_idx))
}

fn parse_compact_set_disk_id(raw: &str) -> Option<(usize, usize)> {
    let (pool, set) = raw.split_once('_')?;
    let pool_idx = pool.parse::<usize>().ok()?;
    let set_idx = set.parse::<usize>().ok()?;
    Some((pool_idx, set_idx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_from_unsigned_indices() {
        assert_eq!(format_set_disk_id(1, 2), "pool_1_set_2");
    }

    #[test]
    fn format_from_signed_indices() {
        assert_eq!(format_set_disk_id_from_i32(3, 4), Some("pool_3_set_4".into()));
        assert_eq!(format_set_disk_id_from_i32(-1, 4), None);
    }

    #[test]
    fn normalize_compact_identifier() {
        assert_eq!(normalize_set_disk_id("3_5"), Some("pool_3_set_5".to_string()));
    }

    #[test]
    fn normalize_prefixed_identifier() {
        assert_eq!(normalize_set_disk_id("pool_7_set_1"), Some("pool_7_set_1".to_string()));
    }

    #[test]
    fn normalize_invalid_identifier() {
        assert_eq!(normalize_set_disk_id("invalid"), None);
    }

    #[test]
    fn parse_prefixed_identifier() {
        assert_eq!(parse_set_disk_id("pool_9_set_3").unwrap(), (9, 3));
    }

    #[test]
    fn parse_invalid_identifier() {
        assert!(parse_set_disk_id("bad").is_err());
        assert!(parse_set_disk_id("pool_X_set_1").is_err());
    }
}
