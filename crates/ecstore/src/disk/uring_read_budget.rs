// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::ffi::OsStr;

#[cfg(target_os = "linux")]
pub(super) const ENV_TOTAL: &str = "RUSTFS_IO_URING_READ_BUDGET_TOTAL_BYTES";
#[cfg(target_os = "linux")]
pub(super) const ENV_DRIVER: &str = "RUSTFS_IO_URING_READ_BUDGET_DRIVER_BYTES";

/// Parsed independently of Linux driver construction so configuration checks
/// never require a kernel probe or process-wide environment mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadBudgetConfig {
    Disabled,
    Limited { total: usize, per_driver: usize },
}

#[derive(Debug)]
pub(super) struct InvalidReadBudget;

impl std::fmt::Display for InvalidReadBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "expected both read-budget settings as positive decimal byte counts, with driver quota no greater than total capacity or Semaphore::MAX_PERMITS",
        )
    }
}

impl std::error::Error for InvalidReadBudget {}

impl ReadBudgetConfig {
    pub(super) fn from_env_values(total: Option<&OsStr>, per_driver: Option<&OsStr>) -> Result<Self, InvalidReadBudget> {
        let (total, per_driver) = match (total, per_driver) {
            (None, None) => return Ok(Self::Disabled),
            (Some(total), Some(per_driver)) => (total, per_driver),
            _ => return Err(InvalidReadBudget),
        };
        let parse = |raw: &OsStr| {
            let text = raw.to_str().ok_or(InvalidReadBudget)?;
            if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(InvalidReadBudget);
            }
            text.parse::<usize>().map_err(|_| InvalidReadBudget)
        };
        let total = parse(total)?;
        let per_driver = parse(per_driver)?;
        if total == 0 || per_driver == 0 || per_driver > total || per_driver > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(InvalidReadBudget);
        }
        Ok(Self::Limited { total, per_driver })
    }
}

/// Clones retain the same process pool, including across disk reconstruction.
/// The library, rather than the backend wrapper, retains leaked reservations.
#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(super) enum DriverReadBudget {
    Disabled,
    Limited {
        pool: rustfs_uring::SharedReadBudget,
        per_driver: usize,
    },
}

#[cfg(target_os = "linux")]
impl DriverReadBudget {
    pub(super) fn from_config(config: ReadBudgetConfig) -> Result<Self, InvalidReadBudget> {
        match config {
            ReadBudgetConfig::Disabled => Ok(Self::Disabled),
            ReadBudgetConfig::Limited { total, per_driver } => Ok(Self::Limited {
                pool: rustfs_uring::SharedReadBudget::new(total).map_err(|_| InvalidReadBudget)?,
                per_driver,
            }),
        }
    }

    pub(super) fn start_driver(
        &self,
        entries: u32,
        shards: usize,
    ) -> Result<rustfs_uring::UringDriver, rustfs_uring::ProbeFailure> {
        match self {
            Self::Disabled => rustfs_uring::UringDriver::probe_and_start_sharded(entries, shards),
            Self::Limited { pool, per_driver } => rustfs_uring::UringDriver::probe_and_start_with_shared_budget(
                entries,
                shards,
                rustfs_uring::ReadLimits {
                    max_read_len: None,
                    max_in_flight_bytes: Some(*per_driver),
                },
                pool,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(total: Option<&str>, per_driver: Option<&str>) -> Result<ReadBudgetConfig, InvalidReadBudget> {
        ReadBudgetConfig::from_env_values(total.map(OsStr::new), per_driver.map(OsStr::new))
    }

    #[test]
    fn both_unset_preserve_legacy_but_partial_configuration_fails() {
        assert_eq!(parse(None, None).expect("unset settings"), ReadBudgetConfig::Disabled);
        assert!(parse(Some("8"), None).is_err());
        assert!(parse(None, Some("8")).is_err());
        assert!(parse(Some("0"), Some("0")).is_err(), "zero is not an implicit unlimited mode");
    }

    #[test]
    fn valid_pair_keeps_usize_total_and_exact_driver_quota() {
        for (total, per_driver) in [(1, 1), (16, 8), (usize::MAX, tokio::sync::Semaphore::MAX_PERMITS)] {
            assert_eq!(
                parse(Some(&total.to_string()), Some(&per_driver.to_string())).expect("valid quota pair"),
                ReadBudgetConfig::Limited { total, per_driver }
            );
        }
        assert_eq!(
            parse(Some("00016"), Some("00008")).expect("leading zeros remain decimal"),
            ReadBudgetConfig::Limited {
                total: 16,
                per_driver: 8
            }
        );
    }

    #[test]
    fn malformed_or_impossible_pair_never_removes_the_limit() {
        for value in ["", " ", "0", " 8", "8 ", "+8", "-1", "8MiB", "1.5", "184467440737095516160"] {
            assert!(parse(Some(value), Some("1")).is_err(), "invalid total accepted");
            assert!(parse(Some("16"), Some(value)).is_err(), "invalid driver quota accepted");
        }
        assert!(parse(Some("7"), Some("8")).is_err());
        assert!(
            parse(
                Some(&usize::MAX.to_string()),
                Some(&(tokio::sync::Semaphore::MAX_PERMITS + 1).to_string())
            )
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_configuration_is_rejected_on_either_side() {
        use std::os::unix::ffi::OsStrExt;
        let invalid = OsStr::from_bytes(&[0xff]);
        assert!(ReadBudgetConfig::from_env_values(Some(invalid), Some(OsStr::new("1"))).is_err());
        assert!(ReadBudgetConfig::from_env_values(Some(OsStr::new("8")), Some(invalid)).is_err());
    }
}
