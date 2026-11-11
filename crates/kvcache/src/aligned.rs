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

use std::ops::{Deref, DerefMut};

/// I/O block size: entries, footers and trailers start on block boundaries
/// and every write and read covers whole blocks, as `O_DIRECT` requires.
pub const BLOCK_SIZE: usize = 4096;

/// Rounds `len` up to a whole number of blocks.
pub const fn round_up(len: usize) -> usize {
    len.div_ceil(BLOCK_SIZE) * BLOCK_SIZE
}

/// A zeroed buffer whose address and length are block aligned. It
/// over-allocates by one block and exposes the aligned window, so no unsafe
/// allocation is needed.
pub struct AlignedBuf {
    storage: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedBuf {
    /// A zeroed buffer of at least `len` bytes, rounded up to whole blocks
    /// (at least one block).
    pub fn zeroed(len: usize) -> Self {
        let len = round_up(len.max(1));
        let storage = vec![0u8; len + BLOCK_SIZE];
        let misalignment = storage.as_ptr() as usize % BLOCK_SIZE;
        let start = if misalignment == 0 { 0 } else { BLOCK_SIZE - misalignment };
        Self { storage, start, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.storage[self.start..self.start + self.len]
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.start + self.len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffers_are_block_aligned_and_zeroed() {
        for len in [0, 1, 4095, 4096, 4097, 1 << 20] {
            let buf = AlignedBuf::zeroed(len);
            assert_eq!(buf.as_ptr() as usize % BLOCK_SIZE, 0);
            assert_eq!(buf.len() % BLOCK_SIZE, 0);
            assert!(buf.len() >= len.max(1));
            assert!(buf.iter().all(|byte| *byte == 0));
        }
        assert_eq!(round_up(0), 0);
        assert_eq!(round_up(1), BLOCK_SIZE);
        assert_eq!(round_up(BLOCK_SIZE + 1), 2 * BLOCK_SIZE);
    }
}
