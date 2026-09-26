// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_os = "linux")]
pub(super) const ENV_RESULT: &str = "RUSTFS_IO_URING_READ_RESULT_BUDGET_BYTES";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ResultBudgetConfig {
    Disabled,
    Limited { bytes: usize },
}

#[derive(Debug)]
pub(super) struct InvalidResultBudget;

impl std::fmt::Display for InvalidResultBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("expected a positive decimal result-budget byte count")
    }
}

impl std::error::Error for InvalidResultBudget {}

impl ResultBudgetConfig {
    pub(super) fn from_env_value(value: Option<&OsStr>) -> Result<Self, InvalidResultBudget> {
        let Some(value) = value else {
            return Ok(Self::Disabled);
        };
        let value = value.to_str().ok_or(InvalidResultBudget)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidResultBudget);
        }
        let bytes = value.parse::<usize>().map_err(|_| InvalidResultBudget)?;
        if bytes == 0 {
            return Err(InvalidResultBudget);
        }
        Ok(Self::Limited { bytes })
    }
}

#[derive(Clone, Debug)]
pub(super) enum ProcessResultBudget {
    Disabled,
    Limited(Arc<ResultBudgetInner>),
}

#[derive(Debug)]
pub(super) struct ResultBudgetInner {
    available: AtomicUsize,
}

impl ProcessResultBudget {
    pub(super) fn from_config(config: ResultBudgetConfig) -> Self {
        match config {
            ResultBudgetConfig::Disabled => Self::Disabled,
            ResultBudgetConfig::Limited { bytes } => Self::Limited(Arc::new(ResultBudgetInner {
                available: AtomicUsize::new(bytes),
            })),
        }
    }

    pub(super) fn reserve(&self, bytes: usize) -> Result<Option<ResultBudgetReservation>, std::io::Error> {
        let Self::Limited(inner) = self else {
            return Ok(None);
        };
        if bytes == 0 {
            return Ok(None);
        }
        inner
            .available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| available.checked_sub(bytes))
            .map(|_| {
                Some(ResultBudgetReservation {
                    inner: Arc::clone(inner),
                    bytes,
                })
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::WouldBlock, ResultBudgetExhausted))
    }

    #[cfg(test)]
    pub(super) fn available(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Limited(inner) => Some(inner.available.load(Ordering::Acquire)),
        }
    }
}

#[derive(Debug)]
pub(super) struct ResultBudgetReservation {
    inner: Arc<ResultBudgetInner>,
    bytes: usize,
}

impl Drop for ResultBudgetReservation {
    fn drop(&mut self) {
        self.inner.available.fetch_add(self.bytes, Ordering::Release);
    }
}

/// Keeps the result-budget reservation alive until every returned `Bytes` clone
/// is dropped. `Bytes::from_owner` retains this owner without copying the data.
pub(super) struct BudgetedBytes {
    bytes: Bytes,
    _reservation: Option<ResultBudgetReservation>,
}

impl BudgetedBytes {
    pub(super) fn wrap(bytes: Bytes, reservation: Option<ResultBudgetReservation>) -> Bytes {
        match reservation {
            Some(reservation) => Bytes::from_owner(Self {
                bytes,
                _reservation: Some(reservation),
            }),
            None => bytes,
        }
    }
}

impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

/// Marker carried by `WouldBlock` so the io_uring backend does not route a
/// process-budget denial through the unbounded std fallback.
#[derive(Debug)]
pub(super) struct ResultBudgetExhausted;

impl std::fmt::Display for ResultBudgetExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("io_uring result budget exhausted")
    }
}

impl std::error::Error for ResultBudgetExhausted {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_is_strict_and_unset_is_disabled() {
        assert_eq!(ResultBudgetConfig::from_env_value(None).unwrap(), ResultBudgetConfig::Disabled);
        assert_eq!(
            ResultBudgetConfig::from_env_value(Some(OsStr::new("4096"))).unwrap(),
            ResultBudgetConfig::Limited { bytes: 4096 }
        );
        for value in ["", "0", " 8", "+8", "8 ", "8MiB"] {
            assert!(ResultBudgetConfig::from_env_value(Some(OsStr::new(value))).is_err());
        }
    }

    #[test]
    fn reservation_is_refunded_only_after_owner_drop() {
        let budget = ProcessResultBudget::from_config(ResultBudgetConfig::Limited { bytes: 8 });
        let reservation = budget.reserve(8).unwrap().unwrap();
        assert_eq!(budget.available(), Some(0));
        assert!(budget.reserve(1).is_err());
        drop(reservation);
        assert_eq!(budget.available(), Some(8));
    }

    #[test]
    fn bytes_owner_keeps_reservation_through_clones() {
        let budget = ProcessResultBudget::from_config(ResultBudgetConfig::Limited { bytes: 4 });
        let reservation = budget.reserve(4).unwrap().unwrap();
        let bytes = BudgetedBytes::wrap(Bytes::from_static(b"data"), Some(reservation));
        let clone = bytes.clone();
        assert_eq!(budget.available(), Some(0));
        drop(bytes);
        assert_eq!(budget.available(), Some(0));
        drop(clone);
        assert_eq!(budget.available(), Some(4));
    }
}
