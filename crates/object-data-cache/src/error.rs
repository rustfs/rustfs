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

use thiserror::Error;

/// Configuration errors for the object data cache engine.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ObjectDataCacheConfigError {
    /// The configured memory percentage exceeded the supported range.
    #[error("object data cache max_memory_percent must be in 1..=100")]
    InvalidMaxMemoryPercent,

    /// The configured entry size exceeded the supported range.
    #[error("object data cache max_entry_bytes must be greater than 0")]
    ZeroMaxEntryBytes,

    /// The configured time-to-live cannot be zero.
    #[error("object data cache ttl_secs must be greater than 0")]
    ZeroTimeToLiveSecs,

    /// The configured time-to-idle cannot be zero.
    #[error("object data cache time_to_idle_secs must be greater than 0")]
    ZeroTimeToIdleSecs,

    /// The configured minimum free memory percentage exceeded the supported range.
    #[error("object data cache min_free_memory_percent must be in 1..=100")]
    InvalidMinFreeMemoryPercent,

    /// The configured fill concurrency per CPU exceeded the supported range.
    #[error("object data cache fill_concurrency_per_cpu must be greater than 0")]
    ZeroFillConcurrencyPerCpu,

    /// The configured fill concurrency maximum exceeded the supported range.
    #[error("object data cache fill_concurrency_max must be greater than 0")]
    ZeroFillConcurrencyMax,

    /// The configured fill concurrency bounds are internally inconsistent.
    #[error("object data cache fill_concurrency_max must be at least fill_concurrency_per_cpu")]
    FillConcurrencyMaxTooSmall,

    /// The configured identity key-set cap exceeded the supported range.
    #[error("object data cache identity_keys_max must be greater than 0")]
    ZeroIdentityKeysMax,

    /// Failed to resolve a non-zero cache capacity from the runtime environment.
    #[error("object data cache could not resolve a positive max capacity")]
    ZeroResolvedMaxBytes,
}
