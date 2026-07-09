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

//! # RustFS Concurrency Contracts
//!
//! Shared concurrency contract types for RustFS:
//!
//! - [`workload`]: workload admission snapshot/contract types consumed by
//!   ecstore, heal, and the server for admission-aware throttling.
//! - [`GetObjectQueueSnapshot`]: disk permit queue usage snapshot for
//!   GetObject orchestration.
//! - [`workers`]: a bounded semaphore-backed worker-slot pool.
//! - [`PipeBackpressurePolicy`] / [`DeadlockMonitorPolicy`]: policy types
//!   shared with the runtime implementations.
//!
//! The actual runtime concurrency control — size-aware timeouts, byte-watermark
//! backpressure, request-hang/deadlock detection, and I/O scheduling — is
//! implemented in `rustfs/src/storage/*` on top of the `rustfs-io-core`
//! primitives. This crate only carries the data and contract types those
//! implementations share; it does not run any background tasks itself.

#![deny(missing_docs)]
#![deny(unsafe_code)]

// Re-exported io-core type used by downstream timeout implementations.
pub use rustfs_io_core::OperationProgress;

mod backpressure;
mod deadlock;
mod queue;
pub mod workers;
pub mod workload;

pub use backpressure::PipeBackpressurePolicy;
pub use deadlock::DeadlockMonitorPolicy;
pub use queue::GetObjectQueueSnapshot;
pub use workload::{
    AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider,
    WorkloadClass,
};
