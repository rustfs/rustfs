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

//! Erasure read/write quorum derived from a set's drive count and parity.
//!
//! `SetDisks::default_read_quorum` and `SetDisks::default_write_quorum` are
//! thin wrappers over these functions. The per-object metadata reducer in
//! `set_disk/metadata.rs` repeats the same arithmetic on `i32` values it has
//! already validated; that path also rejects negative parity before the
//! arithmetic, so it is intentionally left in place.

/// Drives that must agree for a read. Equal to the data-block count.
pub(crate) fn read_quorum(drive_count: usize, parity: usize) -> usize {
    drive_count - parity
}

/// Drives that must acknowledge a write.
///
/// This is the data-block count, plus one when data and parity are equal, so
/// a symmetric layout cannot commit on a bare data quorum.
pub(crate) fn write_quorum(drive_count: usize, parity: usize) -> usize {
    let mut data_count = drive_count - parity;
    if data_count == parity {
        data_count += 1;
    }
    data_count
}

#[cfg(test)]
mod tests {
    use super::{read_quorum, write_quorum};

    #[test]
    fn official_dst_set_widths_match_default_parity_quorum() {
        // 4 nodes x 4 disks -> one 16-drive set, default parity 4.
        assert_eq!(read_quorum(16, 4), 12);
        assert_eq!(write_quorum(16, 4), 12);
        // 3 nodes x 2 disks -> one 6-drive set, default parity 3.
        // Data equals parity, so the write quorum is bumped.
        assert_eq!(read_quorum(6, 3), 3);
        assert_eq!(write_quorum(6, 3), 4);
    }
}
