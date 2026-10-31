//! Request and storage contracts for the per-object erasure-code block size.
//!
//! The request hint is deliberately kept separate from persisted file
//! metadata.  Application code resolves the hint and writes the selected
//! value to the transient marker below; the set layer consumes that marker
//! before constructing `FileInfo`.  Readers use the block size persisted in
//! `FileInfo::erasure`, never the request metadata.

use std::collections::HashMap;
use std::fmt;

/// The historical RustFS erasure block size.
pub const DEFAULT_EC_BLOCK_SIZE: usize = 1024 * 1024;

/// The only block sizes accepted for new objects.
pub const EC_BLOCK_SIZE_CANDIDATES: [usize; 4] = [64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024];

/// User metadata key after the `x-amz-meta-` prefix has been removed.
pub const EC_BLOCK_SIZE_HINT_METADATA_KEY: &str = "rustfs-ec-block-size-hint";

/// Internal marker passed from the request layer to the storage writer.
/// It is always removed before user metadata is persisted.
pub const EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX: &str = "ec-block-size-hint";
pub const EC_BLOCK_SIZE_HINT_INTERNAL_KEY: &str = "x-rustfs-internal-ec-block-size-hint";

/// Internal projection copied into `ObjectInfo` for response construction.
/// The projection is filtered from S3 user metadata.
pub const EC_BLOCK_SIZE_INFO_INTERNAL_SUFFIX: &str = "ec-block-size";
pub const EC_BLOCK_SIZE_INFO_INTERNAL_KEY: &str = "x-rustfs-internal-ec-block-size";

pub const EC_BLOCK_SIZE_RESPONSE_HEADER: &str = "x-rustfs-effective-ec-block-size";
pub const EC_BLOCK_SIZE_HINT_STATUS_HEADER: &str = "x-rustfs-layout-hint-status";
pub const EC_BLOCK_SIZE_HINT_REASON_HEADER: &str = "x-rustfs-layout-hint-reason";

/// Persisted marker for the per-shard bitrot frame size used by new EC
/// objects.  The value is internal metadata and is always written through the
/// RustFS/MinIO compatibility-key pair.
pub const EC_READ_QUANTUM_INFO_INTERNAL_SUFFIX: &str = "ec-read-quantum";
pub const EC_READ_QUANTUM_INFO_INTERNAL_KEY: &str = "x-rustfs-internal-ec-read-quantum";

/// Keep the physical read quantum bounded even when an object chooses a large
/// layout block.  The smaller entries cover the candidate layouts whose shard
/// size is below the target quantum.
pub const EC_READ_QUANTUM_CANDIDATES: [usize; 9] = [
    1024 * 1024,
    512 * 1024,
    256 * 1024,
    128 * 1024,
    64 * 1024,
    32 * 1024,
    16 * 1024,
    8 * 1024,
    4 * 1024,
];

const EC_READ_QUANTUM_LEGACY_CANDIDATES: [usize; 7] =
    [256 * 1024, 128 * 1024, 64 * 1024, 32 * 1024, 16 * 1024, 8 * 1024, 4 * 1024];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EcReadQuantumMetadataError {
    ConflictingValues,
    InvalidValue,
    UnsupportedGeometry,
}

impl fmt::Display for EcReadQuantumMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ConflictingValues => "EC read quantum metadata has conflicting compatibility values",
            Self::InvalidValue => "EC read quantum metadata is not a supported decimal value",
            Self::UnsupportedGeometry => "EC read quantum metadata does not match the object geometry",
        })
    }
}

impl std::error::Error for EcReadQuantumMetadataError {}

/// A syntactically valid request value.  It may still be outside the
/// supported candidate set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientEcBlockSizeHint {
    pub requested_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EcBlockSizeHintError {
    Empty,
    TooLong,
    NonAsciiDecimal,
    Zero,
    Overflow,
}

impl fmt::Display for EcBlockSizeHintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Empty => "value is empty",
            Self::TooLong => "value has more than 20 digits",
            Self::NonAsciiDecimal => "value must contain only ASCII decimal digits",
            Self::Zero => "value must be greater than zero",
            Self::Overflow => "value does not fit in an unsigned 64-bit integer",
        };
        f.write_str(message)
    }
}

impl std::error::Error for EcBlockSizeHintError {}

/// Parse the strict wire representation required by the SPEC.
pub fn parse_ec_block_size_hint(value: &str) -> Result<ClientEcBlockSizeHint, EcBlockSizeHintError> {
    if value.is_empty() {
        return Err(EcBlockSizeHintError::Empty);
    }
    if value.len() > 20 {
        return Err(EcBlockSizeHintError::TooLong);
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(EcBlockSizeHintError::NonAsciiDecimal);
    }
    let requested_bytes = value.parse::<u64>().map_err(|_| EcBlockSizeHintError::Overflow)?;
    if requested_bytes == 0 {
        return Err(EcBlockSizeHintError::Zero);
    }
    Ok(ClientEcBlockSizeHint { requested_bytes })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EcBlockSizeHintIgnoreReason {
    Disabled,
    FleetNotConfirmed,
    UnsupportedSize,
    IneligibleWrite,
}

impl EcBlockSizeHintIgnoreReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::FleetNotConfirmed => "fleet-unconfirmed",
            Self::UnsupportedSize => "unsupported-size",
            Self::IneligibleWrite => "unsupported-write-mode",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EcBlockSizeHintOutcome {
    NoHint,
    Applied,
    Ignored(EcBlockSizeHintIgnoreReason),
}

impl EcBlockSizeHintOutcome {
    pub const fn status_header(self) -> Option<&'static str> {
        match self {
            Self::NoHint => None,
            Self::Applied => Some("applied"),
            Self::Ignored(_) => Some("ignored"),
        }
    }

    pub const fn reason_header(self) -> Option<&'static str> {
        match self {
            Self::Ignored(reason) => Some(reason.as_str()),
            Self::NoHint | Self::Applied => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedEcBlockSize {
    pub effective_block_size: usize,
    pub outcome: EcBlockSizeHintOutcome,
}

/// Resolve a request hint after authentication and eligibility checks.
///
/// Disabled and fleet-unconfirmed requests deliberately do not parse the
/// value.  In those rollout phases the header remains ordinary user metadata,
/// including when its value would be malformed.
pub fn resolve_ec_block_size_hint(
    raw_hint: Option<&str>,
    feature_enabled: bool,
    fleet_confirmed: bool,
    eligible_write: bool,
) -> Result<ResolvedEcBlockSize, EcBlockSizeHintError> {
    let Some(raw_hint) = raw_hint else {
        return Ok(ResolvedEcBlockSize {
            effective_block_size: DEFAULT_EC_BLOCK_SIZE,
            outcome: EcBlockSizeHintOutcome::NoHint,
        });
    };

    if !feature_enabled {
        return Ok(ResolvedEcBlockSize {
            effective_block_size: DEFAULT_EC_BLOCK_SIZE,
            outcome: EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::Disabled),
        });
    }
    if !fleet_confirmed {
        return Ok(ResolvedEcBlockSize {
            effective_block_size: DEFAULT_EC_BLOCK_SIZE,
            outcome: EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::FleetNotConfirmed),
        });
    }

    let hint = parse_ec_block_size_hint(raw_hint)?;
    let Some(effective_block_size) = usize::try_from(hint.requested_bytes)
        .ok()
        .filter(|size| EC_BLOCK_SIZE_CANDIDATES.contains(size))
    else {
        return Ok(ResolvedEcBlockSize {
            effective_block_size: DEFAULT_EC_BLOCK_SIZE,
            outcome: EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::UnsupportedSize),
        });
    };

    if !eligible_write {
        return Ok(ResolvedEcBlockSize {
            effective_block_size: DEFAULT_EC_BLOCK_SIZE,
            outcome: EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::IneligibleWrite),
        });
    }

    Ok(ResolvedEcBlockSize {
        effective_block_size,
        outcome: EcBlockSizeHintOutcome::Applied,
    })
}

pub fn is_supported_ec_block_size(value: usize) -> bool {
    EC_BLOCK_SIZE_CANDIDATES.contains(&value)
}

/// Return whether the storage writer rollout is enabled and fleet-confirmed.
/// Read it on the write path so each new object uses the active process
/// environment values.
pub fn ec_read_quantum_write_enabled() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_EC_READ_QUANTUM_ENABLE, rustfs_config::DEFAULT_EC_READ_QUANTUM_ENABLE)
        && rustfs_utils::get_env_bool(
            rustfs_config::ENV_EC_READ_QUANTUM_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_EC_READ_QUANTUM_FLEET_CONFIRMED,
        )
}

/// Select a frame size that preserves exact EC geometry.  A candidate must be
/// a divisor of both the physical shard size and the logical layout block
/// divided across data shards; this keeps every complete layout stripe aligned
/// and leaves only the final object stripe short.
pub fn select_ec_read_quantum(block_size: usize, data_shards: usize, shard_size: usize) -> Option<usize> {
    if block_size == 0 || data_shards == 0 || shard_size == 0 {
        return None;
    }

    // Small layouts benefit from half-shard frames because their common range
    // requests otherwise pull a complete data-plus-parity stripe. Larger
    // layouts use a coarser target to cap authenticated-frame count during
    // upload: the 1 MiB layout fits one frame per shard and the 4 MiB layout
    // uses two 1 MiB frames per shard. A 4 MiB range still avoids the legacy
    // multi-megabyte stripe read while PUT avoids paying for eight hashes per
    // shard.
    let target = if block_size <= 256 * 1024 {
        shard_size / 2
    } else if block_size <= 1024 * 1024 {
        shard_size
    } else {
        1024 * 1024
    };
    EC_READ_QUANTUM_CANDIDATES.into_iter().find(|candidate| {
        *candidate <= target && ec_read_quantum_matches_geometry(*candidate, block_size, data_shards, shard_size)
    })
}

fn select_legacy_ec_read_quantum(block_size: usize, data_shards: usize, shard_size: usize) -> Option<usize> {
    if block_size == 0 || data_shards == 0 || shard_size == 0 {
        return None;
    }
    EC_READ_QUANTUM_LEGACY_CANDIDATES
        .into_iter()
        .find(|candidate| ec_read_quantum_matches_geometry(*candidate, block_size, data_shards, shard_size))
}

fn ec_read_quantum_matches_geometry(quantum: usize, block_size: usize, data_shards: usize, shard_size: usize) -> bool {
    quantum > 0
        && quantum <= shard_size
        && EC_READ_QUANTUM_CANDIDATES.contains(&quantum)
        && shard_size.is_multiple_of(quantum)
        && quantum
            .checked_mul(data_shards)
            .is_some_and(|logical_quantum| block_size.is_multiple_of(logical_quantum))
}

/// Resolve a persisted marker.  Absence is the legacy layout-frame format;
/// presence with a malformed or conflicting value is an error so a damaged
/// metadata record cannot silently reinterpret the shard stream.
pub fn persisted_ec_read_quantum(
    metadata: &HashMap<String, String>,
    block_size: usize,
    data_shards: usize,
    shard_size: usize,
) -> Result<Option<usize>, EcReadQuantumMetadataError> {
    let present = rustfs_utils::http::metadata_compat::contains_key_str(metadata, EC_READ_QUANTUM_INFO_INTERNAL_SUFFIX);
    let Some(value) = rustfs_utils::http::metadata_compat::get_consistent_str(metadata, EC_READ_QUANTUM_INFO_INTERNAL_SUFFIX)
    else {
        return if present {
            Err(EcReadQuantumMetadataError::ConflictingValues)
        } else {
            Ok(None)
        };
    };
    let quantum = value.parse::<usize>().map_err(|_| EcReadQuantumMetadataError::InvalidValue)?;
    // Accept markers written by the previous selector as well as the current
    // smaller target. Existing 64 KiB and 256 KiB objects may carry a
    // full-shard marker and must remain readable after the rollout changes.
    let current = select_ec_read_quantum(block_size, data_shards, shard_size);
    let legacy = select_legacy_ec_read_quantum(block_size, data_shards, shard_size);
    if current != Some(quantum) && legacy != Some(quantum) {
        return Err(EcReadQuantumMetadataError::UnsupportedGeometry);
    }
    Ok(Some(quantum))
}

pub fn insert_ec_read_quantum(metadata: &mut HashMap<String, String>, quantum: usize) {
    rustfs_utils::http::metadata_compat::insert_str(metadata, EC_READ_QUANTUM_INFO_INTERNAL_SUFFIX, quantum.to_string());
}

/// Consume a trusted transient marker from storage options.
pub fn take_ec_block_size_hint(metadata: &mut std::collections::HashMap<String, String>) -> Option<usize> {
    let value = rustfs_utils::http::metadata_compat::get_consistent_str(metadata, EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX)
        .map(ToOwned::to_owned);
    rustfs_utils::http::metadata_compat::remove_str(metadata, EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX);
    let value = value?;
    let parsed = value.parse::<usize>().ok()?;
    is_supported_ec_block_size(parsed).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_decimal_values_and_rejects_wire_variants() {
        assert_eq!(parse_ec_block_size_hint("65536").unwrap().requested_bytes, 65_536);
        for value in ["", "0", "+65536", "65_536", "65536 ", "1.0", "１２８"] {
            assert!(parse_ec_block_size_hint(value).is_err(), "{value:?} must be rejected");
        }
        assert_eq!(parse_ec_block_size_hint("18446744073709551615").unwrap().requested_bytes, u64::MAX);
        assert!(matches!(
            parse_ec_block_size_hint("18446744073709551616"),
            Err(EcBlockSizeHintError::Overflow)
        ));
    }

    #[test]
    fn resolver_applies_each_candidate_only_when_rollout_and_write_are_eligible() {
        for candidate in EC_BLOCK_SIZE_CANDIDATES {
            let result = resolve_ec_block_size_hint(Some(&candidate.to_string()), true, true, true).unwrap();
            assert_eq!(result.effective_block_size, candidate);
            assert_eq!(result.outcome, EcBlockSizeHintOutcome::Applied);
        }
        assert_eq!(
            resolve_ec_block_size_hint(Some("131072"), true, true, true).unwrap().outcome,
            EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::UnsupportedSize)
        );
        let ineligible = resolve_ec_block_size_hint(Some("65536"), true, true, false).unwrap();
        assert_eq!(
            ineligible.outcome,
            EcBlockSizeHintOutcome::Ignored(EcBlockSizeHintIgnoreReason::IneligibleWrite)
        );
        assert_eq!(ineligible.outcome.reason_header(), Some("unsupported-write-mode"));
    }

    #[test]
    fn disabled_and_unconfirmed_rollouts_do_not_turn_malformed_metadata_into_errors() {
        for (enabled, fleet, reason) in [
            (false, false, EcBlockSizeHintIgnoreReason::Disabled),
            (true, false, EcBlockSizeHintIgnoreReason::FleetNotConfirmed),
        ] {
            let result = resolve_ec_block_size_hint(Some("not-a-number"), enabled, fleet, true).unwrap();
            assert_eq!(result.effective_block_size, DEFAULT_EC_BLOCK_SIZE);
            assert_eq!(result.outcome, EcBlockSizeHintOutcome::Ignored(reason));
        }
    }

    #[test]
    fn transient_marker_is_consumed_only_for_supported_candidates() {
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(&mut metadata, EC_BLOCK_SIZE_HINT_INTERNAL_SUFFIX, "262144".to_string());
        assert_eq!(take_ec_block_size_hint(&mut metadata), Some(262_144));
        assert!(!metadata.contains_key(EC_BLOCK_SIZE_HINT_INTERNAL_KEY));
        assert!(!metadata.contains_key("x-minio-internal-ec-block-size-hint"));

        let mut unsupported =
            std::collections::HashMap::from([(EC_BLOCK_SIZE_HINT_INTERNAL_KEY.to_string(), "131072".to_string())]);
        assert_eq!(take_ec_block_size_hint(&mut unsupported), None);
        assert!(!unsupported.contains_key(EC_BLOCK_SIZE_HINT_INTERNAL_KEY));

        let mut conflicting = std::collections::HashMap::from([
            (EC_BLOCK_SIZE_HINT_INTERNAL_KEY.to_string(), "65536".to_string()),
            ("x-minio-internal-ec-block-size-hint".to_string(), "262144".to_string()),
        ]);
        assert_eq!(take_ec_block_size_hint(&mut conflicting), None);
        assert!(conflicting.is_empty());
    }

    #[test]
    fn read_quantum_stays_aligned_with_layout_geometry() {
        // Two data shards produce the same compact geometry used by the
        // persisted 64 KiB, 256 KiB, 1 MiB and 4 MiB candidates. Large
        // layouts deliberately use coarser frames to keep PUT overhead bounded.
        assert_eq!(select_ec_read_quantum(64 * 1024, 2, 32 * 1024), Some(16 * 1024));
        assert_eq!(select_ec_read_quantum(256 * 1024, 2, 128 * 1024), Some(64 * 1024));
        assert_eq!(select_ec_read_quantum(1024 * 1024, 2, 512 * 1024), Some(512 * 1024));
        assert_eq!(select_ec_read_quantum(4 * 1024 * 1024, 2, 2 * 1024 * 1024), Some(1024 * 1024));
    }

    #[test]
    fn persisted_read_quantum_rejects_conflicts_and_bad_geometry() {
        let mut metadata = HashMap::new();
        insert_ec_read_quantum(&mut metadata, 256 * 1024);
        assert_eq!(
            persisted_ec_read_quantum(&metadata, 4 * 1024 * 1024, 2, 2 * 1024 * 1024),
            Ok(Some(256 * 1024))
        );

        metadata.clear();
        insert_ec_read_quantum(&mut metadata, 1024 * 1024);
        assert_eq!(
            persisted_ec_read_quantum(&metadata, 4 * 1024 * 1024, 2, 2 * 1024 * 1024),
            Ok(Some(1024 * 1024))
        );

        metadata.insert("x-minio-internal-ec-read-quantum".to_owned(), "128".to_owned());
        assert_eq!(
            persisted_ec_read_quantum(&metadata, 4 * 1024 * 1024, 2, 2 * 1024 * 1024),
            Err(EcReadQuantumMetadataError::ConflictingValues)
        );

        metadata.clear();
        insert_ec_read_quantum(&mut metadata, 32 * 1024);
        assert_eq!(persisted_ec_read_quantum(&metadata, 64 * 1024, 2, 32 * 1024), Ok(Some(32 * 1024)));
    }
}
