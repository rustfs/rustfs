//! General-purpose buffer pool for reducing Vec<u8> allocations.
//!
//! This pool reuses Vec<u8> buffers to avoid repeated heap allocations
//! in hot paths like EC encoding/decoding and data read/write.
//!
//! Current integration: bitrot.rs (bitrot_verify path)
//! Future integration: decode.rs, encode.rs

use std::sync::Mutex;

/// A thread-safe pool of reusable Vec<u8> buffers.
pub(crate) struct BufferPool {
    buckets: Mutex<Vec<Vec<Vec<u8>>>>,
    max_per_bucket: usize,
}

impl BufferPool {
    pub(crate) fn with_limits(max_per_bucket: usize) -> Self {
        let buckets = (0..32).map(|_| Vec::new()).collect();
        Self {
            buckets: Mutex::new(buckets),
            max_per_bucket,
        }
    }

    pub(crate) fn get(&self, min_capacity: usize) -> Vec<u8> {
        let bucket = self.bucket_for_capacity(min_capacity);
        let mut buckets = self.buckets.lock().unwrap();
        if let Some(buf) = buckets[bucket].pop() {
            return buf;
        }
        drop(buckets);
        Vec::with_capacity(min_capacity.next_power_of_two().max(min_capacity))
    }

    pub(crate) fn put(&self, mut buf: Vec<u8>) {
        if buf.is_empty() {
            return;
        }
        let bucket = self.bucket_for_capacity(buf.capacity());
        buf.clear();
        let mut buckets = self.buckets.lock().unwrap();
        if buckets[bucket].len() < self.max_per_bucket {
            buckets[bucket].push(buf);
        }
    }

    fn bucket_for_capacity(&self, capacity: usize) -> usize {
        if capacity == 0 {
            return 0;
        }
        let rounded = capacity.next_power_of_two();
        (usize::BITS - rounded.leading_zeros() - 1) as usize
    }
}

static EC_BUFFER_POOL: std::sync::LazyLock<BufferPool> = std::sync::LazyLock::new(|| BufferPool::with_limits(16));

pub(crate) fn get_ec_buffer(min_capacity: usize) -> Vec<u8> {
    EC_BUFFER_POOL.get(min_capacity)
}

pub(crate) fn return_ec_buffer(buf: Vec<u8>) {
    EC_BUFFER_POOL.put(buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::with_limits(16);
        let buf = pool.get(1024);
        assert!(buf.capacity() >= 1024);
        pool.put(buf);
        let buf2 = pool.get(1024);
        assert!(buf2.capacity() >= 1024);
    }

    #[test]
    fn test_buffer_pool_different_sizes() {
        let pool = BufferPool::with_limits(16);
        let buf1 = pool.get(100);
        let buf2 = pool.get(1000);
        let buf3 = pool.get(10000);
        pool.put(buf1);
        pool.put(buf2);
        pool.put(buf3);
        let _ = pool.get(100);
        let _ = pool.get(1000);
        let _ = pool.get(10000);
    }
}
