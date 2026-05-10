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

//! Per-handle read cache.
//!
//! One in-memory buffer per open File handle. The driver fetches a
//! chunk of bytes from the backend in a single call and holds it in
//! the buffer. Subsequent reads inside that chunk are served from
//! memory instead of one backend call per read. The chunk size is
//! configurable. With the 4 MiB default and the 256 KiB client read
//! size, sixteen FXP_READs are served from one backend call.
//!
//! Total cache memory across every live handle in the process is
//! bounded by a shared atomic accumulator. Each ReadCache holds an
//! Arc to that accumulator. The populate method adjusts the
//! accumulator by the difference between the old and new buf
//! capacities. The Drop impl subtracts the live capacity when the
//! cache is dropped. Before calling the populate method, the driver
//! checks the projected total against the operator-supplied limit.
//! When a populate call would push the total past the limit, the
//! driver skips populate and serves the read with a single backend
//! call without storing the bytes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One cached chunk of bytes for a single open File handle. The
/// chunk covers a contiguous byte range starting at window_offset.
/// The buf field stores the bytes for the range [window_offset,
/// window_offset + buf.len()).
pub(super) struct ReadCache {
    buf: Vec<u8>,
    window_offset: u64,
    /// Process-wide accumulator of live cache memory in bytes. The
    /// Drop impl subtracts the live buf.capacity(). The populate
    /// method subtracts the old capacity and adds the new.
    in_use: Arc<AtomicU64>,
}

impl ReadCache {
    /// Build an empty cache bound to the shared in_use accumulator.
    /// The buf field starts empty. No bytes are allocated until the
    /// first call to the populate method.
    pub(super) fn new(in_use: Arc<AtomicU64>) -> Self {
        Self {
            buf: Vec::new(),
            window_offset: 0,
            in_use,
        }
    }

    /// Return the slice of cached bytes covering up to len bytes
    /// starting at offset, or None when offset falls outside the
    /// cached chunk. When the requested range extends past the end
    /// of the cached chunk, only the portion inside the chunk is
    /// returned. SFTPv3 draft section 6.4 allows a READ to return
    /// fewer bytes than requested. A subsequent FXP_READ for the
    /// remainder fetches a fresh chunk aligned to the new offset.
    pub(super) fn get(&self, offset: u64, len: u64) -> Option<&[u8]> {
        if self.buf.is_empty() || len == 0 {
            return None;
        }
        if offset < self.window_offset {
            return None;
        }
        let end = self.window_offset.saturating_add(self.buf.len() as u64);
        if offset >= end {
            return None;
        }
        let start = (offset - self.window_offset) as usize;
        let avail = self.buf.len() - start;
        let take = len.min(avail as u64) as usize;
        Some(&self.buf[start..start + take])
    }

    /// Replace the cached chunk with bytes starting at offset. Any
    /// previously cached bytes are dropped. The shared in_use
    /// accumulator is adjusted by the difference between the old and
    /// new buf capacities.
    pub(super) fn populate(&mut self, offset: u64, bytes: Vec<u8>) {
        let old_cap = self.buf.capacity() as u64;
        self.in_use.fetch_sub(old_cap, Ordering::Relaxed);
        self.buf = bytes;
        self.window_offset = offset;
        let new_cap = self.buf.capacity() as u64;
        self.in_use.fetch_add(new_cap, Ordering::Relaxed);
    }

    /// Live size of the cached buf in bytes. Equal to buf.capacity().
    pub(super) fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

impl Drop for ReadCache {
    fn drop(&mut self) {
        let live = self.buf.capacity() as u64;
        if live != 0 {
            self.in_use.fetch_sub(live, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> (ReadCache, Arc<AtomicU64>) {
        let acc = Arc::new(AtomicU64::new(0));
        let cache = ReadCache::new(Arc::clone(&acc));
        (cache, acc)
    }

    #[test]
    fn new_cache_returns_none_for_any_get() {
        let (cache, _acc) = fresh();
        assert!(cache.get(0, 1).is_none());
        assert!(cache.get(0, 1024).is_none());
        assert!(cache.get(1_000_000, 64).is_none());
    }

    #[test]
    fn after_populate_get_hits_within_window() {
        let (mut cache, _acc) = fresh();
        let payload: Vec<u8> = (0..1024_u32).map(|i| i as u8).collect();
        cache.populate(100, payload.clone());

        let slice = cache.get(100, 64).expect("hit at window start");
        assert_eq!(slice, &payload[..64]);

        let slice = cache.get(200, 32).expect("hit inside window");
        assert_eq!(slice, &payload[100..132]);

        let slice = cache.get(100 + 1024 - 1, 1).expect("hit at last byte");
        assert_eq!(slice, &payload[1023..1024]);
    }

    #[test]
    fn get_at_or_past_window_end_returns_none() {
        let (mut cache, _acc) = fresh();
        cache.populate(100, vec![0u8; 256]);
        // window covers [100, 356), so offset 356 is one past the end.
        assert!(cache.get(356, 1).is_none());
        assert!(cache.get(1024, 64).is_none());
    }

    #[test]
    fn get_before_window_start_returns_none() {
        let (mut cache, _acc) = fresh();
        cache.populate(100, vec![0u8; 256]);
        assert!(cache.get(0, 64).is_none());
        assert!(cache.get(99, 1).is_none());
    }

    #[test]
    fn partial_hit_at_window_edge_returns_in_window_portion() {
        let (mut cache, _acc) = fresh();
        let payload: Vec<u8> = (0..256_u16).map(|i| i as u8).collect();
        cache.populate(100, payload.clone());
        // window covers [100, 356), so offset 350 leaves 6 bytes in
        // window when 64 are requested.
        let slice = cache.get(350, 64).expect("partial hit");
        assert_eq!(slice.len(), 6, "must truncate to in-window bytes");
        assert_eq!(slice, &payload[250..256]);
    }

    #[test]
    fn multiple_populates_discard_previous_window() {
        let (mut cache, acc) = fresh();
        cache.populate(100, vec![0xAA_u8; 256]);
        let acc_after_first = acc.load(Ordering::Relaxed);
        assert!(acc_after_first >= 256, "accumulator must include first window capacity");

        cache.populate(1000, vec![0xBB_u8; 512]);
        // Reads against the previous chunk must miss now.
        assert!(cache.get(100, 1).is_none(), "first chunk discarded");
        assert!(cache.get(0, 1).is_none());
        // Reads against the new chunk return its bytes.
        let slice = cache.get(1000, 4).expect("hit in second chunk");
        assert_eq!(slice, &[0xBB, 0xBB, 0xBB, 0xBB]);

        let acc_after_second = acc.load(Ordering::Relaxed);
        assert!(
            acc_after_second >= 512,
            "accumulator must include second window capacity (got {acc_after_second})"
        );
    }

    #[test]
    fn capacity_reports_buf_capacity() {
        let (mut cache, _acc) = fresh();
        assert_eq!(cache.capacity(), 0, "empty cache reports zero capacity");
        cache.populate(0, vec![0u8; 1024]);
        assert!(
            cache.capacity() >= 1024,
            "populated cache must report buf capacity at least equal to bytes copied in (got {})",
            cache.capacity(),
        );
    }

    #[test]
    fn drop_releases_accumulator() {
        let acc = Arc::new(AtomicU64::new(0));
        {
            let mut cache = ReadCache::new(Arc::clone(&acc));
            cache.populate(0, vec![0u8; 1024]);
            assert!(acc.load(Ordering::Relaxed) >= 1024);
        }
        assert_eq!(acc.load(Ordering::Relaxed), 0, "accumulator drained on Drop");
    }

    #[test]
    fn populate_then_get_zero_len_returns_none() {
        let (mut cache, _acc) = fresh();
        cache.populate(100, vec![0u8; 256]);
        assert!(cache.get(100, 0).is_none(), "zero-length get returns None");
    }
}
