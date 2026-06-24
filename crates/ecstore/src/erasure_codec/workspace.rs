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

#[derive(Debug, Default)]
pub(crate) struct ShardBufferPool {
    buffers: Vec<Option<Vec<u8>>>,
}

impl ShardBufferPool {
    pub(crate) fn new(slot_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(slot_count);
        buffers.resize_with(slot_count, || None);
        Self { buffers }
    }

    pub(crate) fn ensure_slots(&mut self, slot_count: usize) {
        if self.buffers.len() < slot_count {
            self.buffers.resize_with(slot_count, || None);
        }
    }

    pub(crate) fn take(&mut self, index: usize, len: usize) -> Vec<u8> {
        self.ensure_slots(index + 1);
        let mut buf = self.buffers[index].take().unwrap_or_else(|| Vec::with_capacity(len));
        if buf.capacity() < len {
            buf.reserve_exact(len - buf.capacity());
        }
        buf.resize(len, 0);
        buf
    }

    pub(crate) fn put(&mut self, index: usize, mut buf: Vec<u8>) {
        self.ensure_slots(index + 1);
        buf.clear();
        self.buffers[index] = Some(buf);
    }

    #[cfg(test)]
    fn stored_capacity(&self, index: usize) -> Option<usize> {
        self.buffers.get(index).and_then(|buf| buf.as_ref().map(Vec::capacity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_buffer_pool_reuses_slot_capacity() {
        let mut pool = ShardBufferPool::new(2);
        let mut buf = pool.take(1, 16);
        assert_eq!(buf.len(), 16);
        buf[0] = 42;
        let capacity = buf.capacity();

        pool.put(1, buf);
        assert_eq!(pool.stored_capacity(1), Some(capacity));

        let reused = pool.take(1, 8);
        assert_eq!(reused.len(), 8);
        assert!(reused.capacity() >= capacity);
        assert!(reused.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn shard_buffer_pool_grows_for_sparse_slot_indexes() {
        let mut pool = ShardBufferPool::new(0);
        let buf = pool.take(3, 4);

        assert_eq!(buf.len(), 4);
        assert_eq!(pool.buffers.len(), 4);
    }
}
