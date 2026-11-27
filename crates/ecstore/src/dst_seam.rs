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

//! Test-only surface for deterministic simulation.
//!
//! Compiled only with `--features dst`. The types below wrap private
//! durability code so the harness can call it without widening the
//! production API. Production binaries do not enable the feature.

pub use rustfs_lock::{exclusive_lock_quorum, shared_lock_quorum};

use crate::disk::error::DiskError;
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_read_quorum_errs, reduce_write_quorum_errs};
use crate::erasure::coding::Erasure;
use crate::layout::disks_layout::DisksLayout;
use std::io;

/// One erasure codec. `encode` / `decode` call `Erasure::encode_data` and `Erasure::decode_data`.
pub struct SimCodec {
    inner: Erasure,
}

impl SimCodec {
    /// # Errors
    /// Returns the production constructor error when the geometry is rejected.
    pub fn try_new(data_shards: usize, parity_shards: usize, block_size: usize) -> Result<Self, String> {
        Erasure::try_new(data_shards, parity_shards, block_size)
            .map(|inner| Self { inner })
            .map_err(|error| format!("{error}"))
    }

    /// # Errors
    /// Returns the production encode error.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.inner
            .encode_data(data)
            .map(|shards| shards.into_iter().map(|shard| shard.to_vec()).collect())
            .map_err(|error| error.to_string())
    }

    /// Reconstructs missing shards in place.
    ///
    /// # Errors
    /// Returns the production decode error.
    pub fn decode(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), String> {
        self.inner.decode_data(shards).map_err(|error| error.to_string())
    }
}

/// Drive outcomes the production quorum reducer understands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimDiskFault {
    DiskNotFound,
    FaultyDisk,
    FileNotFound,
    FileCorrupt,
}

pub fn default_parity_count(drive_count: usize) -> usize {
    crate::config::storageclass::default_parity_count(drive_count)
}

pub fn read_quorum(drive_count: usize, parity: usize) -> usize {
    crate::set_disk::quorum_math::read_quorum(drive_count, parity)
}

pub fn write_quorum(drive_count: usize, parity: usize) -> usize {
    crate::set_disk::quorum_math::write_quorum(drive_count, parity)
}

/// `Ok(true)` when `reduce_write_quorum_errs` reports success.
///
/// # Errors
/// Returns when that reducer disagrees with a success count that has no
/// non-ignored errors. That disagreement is a production bug.
pub fn write_quorum_met(faults: &[Option<SimDiskFault>], quorum: usize) -> Result<bool, String> {
    quorum_met(faults, quorum, true)
}

/// `Ok(true)` when `reduce_read_quorum_errs` reports success.
///
/// # Errors
/// Same contract as [`write_quorum_met`].
pub fn read_quorum_met(faults: &[Option<SimDiskFault>], quorum: usize) -> Result<bool, String> {
    quorum_met(faults, quorum, false)
}

/// Expand one volume argument with the default set-width selector.
///
/// `set_drive_count` is `0`, which is what production uses when
/// `RUSTFS_ERASURE_SET_DRIVE_COUNT` is unset. Each inner vector is one set,
/// in endpoint order.
///
/// # Errors
/// Returns the layout parser error.
pub fn layout_sets(volume: &str) -> io::Result<Vec<Vec<String>>> {
    let layout = DisksLayout::from_volumes_with_set_drive_count(&[volume], 0)?;
    let Some(pool) = layout.pools.first() else {
        return Err(io::Error::other("erasure layout did not produce a pool"));
    };
    Ok(pool.iter().cloned().collect())
}

fn quorum_met(faults: &[Option<SimDiskFault>], quorum: usize, write: bool) -> Result<bool, String> {
    let errors: Vec<Option<DiskError>> = faults.iter().map(|fault| fault.as_ref().map(to_disk_error)).collect();
    let decision = if write {
        reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, quorum)
    } else {
        reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, quorum)
    };
    let successes = errors.iter().filter(|error| error.is_none()).count();
    let non_ignored = errors
        .iter()
        .flatten()
        .filter(|error| !OBJECT_OP_IGNORED_ERRS.iter().any(|ignored| ignored == *error))
        .count();
    if successes >= quorum && non_ignored == 0 && decision.is_some() {
        return Err(format!("reducer rejected {successes} successes >= quorum {quorum}"));
    }
    if successes < quorum && non_ignored == 0 && decision.is_none() {
        return Err(format!("reducer accepted {successes} successes < quorum {quorum}"));
    }
    Ok(decision.is_none())
}

fn to_disk_error(fault: &SimDiskFault) -> DiskError {
    match fault {
        SimDiskFault::DiskNotFound => DiskError::DiskNotFound,
        SimDiskFault::FaultyDisk => DiskError::FaultyDisk,
        SimDiskFault::FileNotFound => DiskError::FileNotFound,
        SimDiskFault::FileCorrupt => DiskError::FileCorrupt,
    }
}
