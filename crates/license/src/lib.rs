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

//! Provider-neutral license and entitlement contracts.

use std::{fmt, sync::Arc};

use thiserror::Error;

pub type LicenseResult<T> = Result<T, LicenseError>;
pub type SharedLicenseProvider = Arc<dyn LicenseProvider>;

/// Entitlement required by the existing server-wide license gate.
pub const SERVER_ENTITLEMENT: &str = "rustfs.server";

/// Provider-neutral metadata exposed through existing RustFS status APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LicenseMetadata {
    pub subject: String,
    pub expires_at: Option<u64>,
}

/// Sanitized provider state suitable for status and diagnostics output.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum LicenseStatus {
    #[default]
    Uninitialized,
    Valid,
    Missing,
    Invalid(String),
    Unavailable,
}

impl fmt::Display for LicenseStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "uninitialized"),
            Self::Valid => write!(f, "valid"),
            Self::Missing => write!(f, "missing"),
            Self::Invalid(message) => write!(f, "{message}"),
            Self::Unavailable => write!(f, "unavailable"),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum LicenseError {
    #[error("License state is unavailable")]
    StatePoisoned,
    #[error("License is required when building with feature `license`.")]
    Missing,
    #[error("Incorrect license, please contact RustFS. {0}")]
    Invalid(String),
    #[error("Incorrect license, please contact RustFS. expired_at={expired_at}, now={now}")]
    Expired { expired_at: u64, now: u64 },
    #[error("Failed to read system time: {0}")]
    Clock(String),
    #[error("Entitlement is not granted: {entitlement}")]
    Denied { entitlement: String },
    #[error("License provider is unavailable: {0}")]
    Unavailable(String),
    #[error("Entitlement identifier is empty or not normalized")]
    InvalidEntitlement,
}

/// Runtime boundary between RustFS and a license implementation.
///
/// Providers must sanitize all strings returned in errors, status, and
/// metadata. In particular, they must never include raw license material.
/// `initialize` must be safe to call repeatedly with the same input. `check`
/// must be idempotent and must not consume quota, acquire a lease, or mutate
/// external state. Providers that require a license must fail closed before
/// successful initialization.
pub trait LicenseProvider: Send + Sync {
    fn initialize(&self, raw_license: Option<&str>) -> LicenseResult<()>;

    fn check(&self, entitlement: &str) -> LicenseResult<()>;

    fn status(&self) -> LicenseStatus;

    fn metadata(&self) -> Option<LicenseMetadata> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestProvider;

    impl LicenseProvider for TestProvider {
        fn initialize(&self, _raw_license: Option<&str>) -> LicenseResult<()> {
            Ok(())
        }

        fn check(&self, _entitlement: &str) -> LicenseResult<()> {
            Ok(())
        }

        fn status(&self) -> LicenseStatus {
            LicenseStatus::Valid
        }
    }

    #[test]
    fn provider_is_object_safe() {
        let provider: SharedLicenseProvider = Arc::new(TestProvider);

        assert_eq!(provider.status(), LicenseStatus::Valid);
    }

    #[test]
    fn status_display_is_stable() {
        assert_eq!(LicenseStatus::Uninitialized.to_string(), "uninitialized");
        assert_eq!(LicenseStatus::Valid.to_string(), "valid");
        assert_eq!(LicenseStatus::Missing.to_string(), "missing");
        assert_eq!(LicenseStatus::Invalid("invalid key".to_string()).to_string(), "invalid key");
        assert_eq!(LicenseStatus::Unavailable.to_string(), "unavailable");
    }
}
