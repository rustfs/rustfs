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

// SAFETY: the block handle owns a pointer into registered memory under
// `ArenaBlock::new`'s validity and exclusivity contract. Its `BufMut`
// implementation enforces initialized-length and capacity bounds.
#![allow(unsafe_code)]

//! Shard buffers for the erasure read path.
//!
//! A [`ShardBuf`] is the owned buffer one erasure shard is read into, verified
//! in, reconstructed from, and emitted from. It is either a plain `Vec<u8>` or
//! a block of a registered [`ShardArena`] (memory an RDMA device can DMA from
//! directly). The decoder never cares which; a consumer that can RDMA-write
//! straight from the buffer asks for its [`ShardBuf::arena_block`] and takes
//! the buffer over instead of copying it.

use bytes::BufMut;
use bytes::buf::UninitSlice;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, OnceLock};

/// A registered memory arena that hands out fixed-size blocks.
pub trait ShardArena: Send + Sync + 'static {
    /// Size of the blocks this arena hands out.
    fn block_len(&self) -> usize;
    /// A free block, or `None` when the arena is exhausted (the caller then
    /// falls back to the heap).
    fn alloc(&self) -> Option<ArenaBlock>;
    /// Returns a block previously handed out by [`Self::alloc`].
    fn release(&self, block: &ArenaBlock);
    /// Identifies the registration a block belongs to, for consumers that
    /// match blocks against their own registered regions.
    fn registration_id(&self) -> u64;
}

/// One block of a [`ShardArena`]: a raw pointer into the arena's registered
/// memory, returned to the arena on drop.
pub struct ArenaBlock {
    arena: Arc<dyn ShardArena>,
    ptr: *mut u8,
    /// Byte offset of the block inside the arena's registration.
    offset: usize,
    len: usize,
}

// SAFETY: the block is exclusively owned memory of a `Send + Sync` arena.
unsafe impl Send for ArenaBlock {}
unsafe impl Sync for ArenaBlock {}

impl ArenaBlock {
    /// Builds a block handle. `ptr` must point at `len` writable bytes that
    /// stay valid until the block is released and must not be aliased by
    /// another live block.
    ///
    /// # Safety
    ///
    /// The caller guarantees the pointer contract above.
    pub unsafe fn new(arena: Arc<dyn ShardArena>, ptr: *mut u8, offset: usize, len: usize) -> Self {
        Self { arena, ptr, offset, len }
    }

    /// Offset of the block inside its arena's registration.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Capacity of the block.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The arena's registration id.
    pub fn registration_id(&self) -> u64 {
        self.arena.registration_id()
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.len);
        // SAFETY: `ptr` is valid for `self.len` bytes for the block's lifetime
        // and the block is exclusively owned.
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }

    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.len);
        // SAFETY: as in `as_slice`, with `&mut self` for exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, len) }
    }
}

impl Drop for ArenaBlock {
    fn drop(&mut self) {
        self.arena.release(self);
    }
}

impl fmt::Debug for ArenaBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArenaBlock")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

/// A [`ShardArena`] over one contiguous registered region split into
/// fixed-size blocks, with a free list. The region's owner is kept alive by
/// `_owner` for as long as the arena (and therefore any block) exists.
pub struct RegisteredSlabArena {
    base: *mut u8,
    len: usize,
    block_len: usize,
    registration_id: u64,
    free: std::sync::Mutex<Vec<usize>>,
    me: std::sync::Weak<RegisteredSlabArena>,
    _owner: Box<dyn std::any::Any + Send + Sync>,
}

// SAFETY: the raw base pointer is only used to carve disjoint blocks whose
// exclusivity is enforced by the free list; the region's owner is `Send + Sync`.
unsafe impl Send for RegisteredSlabArena {}
unsafe impl Sync for RegisteredSlabArena {}

impl RegisteredSlabArena {
    /// Builds an arena over `len` bytes at `base`, in blocks of `block_len`.
    ///
    /// # Safety
    ///
    /// `base` must point at `len` bytes of writable memory that stay valid and
    /// unaliased for as long as `owner` is alive; the arena keeps `owner`.
    pub unsafe fn new(
        base: *mut u8,
        len: usize,
        block_len: usize,
        registration_id: u64,
        owner: Box<dyn std::any::Any + Send + Sync>,
    ) -> Arc<Self> {
        let blocks = len.checked_div(block_len).unwrap_or(0);
        Arc::new_cyclic(|me| Self {
            base,
            len,
            block_len,
            registration_id,
            free: std::sync::Mutex::new((0..blocks).rev().collect()),
            me: me.clone(),
            _owner: owner,
        })
    }

    /// Blocks the arena was built with.
    pub fn block_count(&self) -> usize {
        self.len.checked_div(self.block_len).unwrap_or(0)
    }

    /// Blocks currently free.
    pub fn free_blocks(&self) -> usize {
        self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).len()
    }
}

impl ShardArena for RegisteredSlabArena {
    fn block_len(&self) -> usize {
        self.block_len
    }

    fn alloc(&self) -> Option<ArenaBlock> {
        let index = self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop()?;
        let me = self.me.upgrade()?;
        let offset = index * self.block_len;
        // SAFETY: `offset + block_len <= len` by construction, the block is
        // exclusively owned until released, and `me` keeps the owner alive.
        Some(unsafe { ArenaBlock::new(me as Arc<dyn ShardArena>, self.base.add(offset), offset, self.block_len) })
    }

    fn release(&self, block: &ArenaBlock) {
        let index = block.offset() / self.block_len;
        self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(index);
    }

    fn registration_id(&self) -> u64 {
        self.registration_id
    }
}

/// The byte container an erasure shard slot may hold: a plain `Vec<u8>` or a
/// [`ShardBuf`]. The coder is generic over it so callers that never touch the
/// arena keep using vectors.
pub trait ShardBytes: AsRef<[u8]> + AsMut<[u8]> + From<Vec<u8>> + Send + Sync + 'static {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }

    /// The bytes as an owned vector (a move for a vector, a copy out of an
    /// arena block).
    fn into_vec(self) -> Vec<u8>;
}

impl ShardBytes for Vec<u8> {
    fn into_vec(self) -> Vec<u8> {
        self
    }
}

impl ShardBytes for ShardBuf {
    fn into_vec(self) -> Vec<u8> {
        ShardBuf::into_vec(self)
    }
}

/// A buffer a bitrot reader appends a shard frame to: `Vec<u8>` or [`ShardBuf`].
pub trait ShardAppend: BufMut + Deref<Target = [u8]> + Default + Send + 'static {
    fn reserve(&mut self, additional: usize);
    fn truncate(&mut self, len: usize);
    fn extend_from_slice(&mut self, bytes: &[u8]);
    /// Whether the bytes live in a registered arena block.
    fn lives_in_arena(&self) -> bool {
        false
    }
}

impl ShardAppend for Vec<u8> {
    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional)
    }

    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len)
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes)
    }
}

impl ShardAppend for ShardBuf {
    fn reserve(&mut self, additional: usize) {
        ShardBuf::reserve(self, additional)
    }

    fn lives_in_arena(&self) -> bool {
        self.is_arena()
    }

    fn truncate(&mut self, len: usize) {
        ShardBuf::truncate(self, len)
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        ShardBuf::extend_from_slice(self, bytes)
    }
}

static SHARD_ARENA: OnceLock<Arc<dyn ShardArena>> = OnceLock::new();

/// Installs the process-wide shard arena; only the first installation wins.
pub fn install_shard_arena(arena: Arc<dyn ShardArena>) -> bool {
    SHARD_ARENA.set(arena).is_ok()
}

/// The installed shard arena, if any.
pub fn shard_arena() -> Option<&'static Arc<dyn ShardArena>> {
    SHARD_ARENA.get()
}

enum Storage {
    Heap(Vec<u8>),
    Arena { block: ArenaBlock, len: usize },
}

/// An owned shard buffer: heap or registered arena memory, with the part of
/// the `Vec<u8>` surface the erasure read path uses.
pub struct ShardBuf {
    storage: Storage,
}

impl Default for ShardBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardBuf {
    pub const fn new() -> Self {
        Self {
            storage: Storage::Heap(Vec::new()),
        }
    }

    /// An empty buffer with room for `capacity` bytes: an arena block when an
    /// arena is installed and the block fits, otherwise heap memory.
    pub fn with_capacity(capacity: usize) -> Self {
        if let Some(arena) = shard_arena()
            && capacity <= arena.block_len()
            && let Some(block) = arena.alloc()
        {
            return Self {
                storage: Storage::Arena { block, len: 0 },
            };
        }
        Self::with_heap_capacity(capacity)
    }

    /// An empty heap buffer with room for `capacity` bytes.
    pub fn with_heap_capacity(capacity: usize) -> Self {
        Self {
            storage: Storage::Heap(Vec::with_capacity(capacity)),
        }
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            storage: Storage::Heap(vec),
        }
    }

    /// Whether the bytes live in a registered arena block.
    pub fn is_arena(&self) -> bool {
        matches!(self.storage, Storage::Arena { .. })
    }

    /// The arena block behind the bytes, if any.
    pub fn arena_block(&self) -> Option<&ArenaBlock> {
        match &self.storage {
            Storage::Arena { block, .. } => Some(block),
            Storage::Heap(_) => None,
        }
    }

    /// Takes the arena block out, leaving the buffer empty on the heap.
    /// Returns `None` (and leaves the buffer untouched) for a heap buffer.
    pub fn take_arena_block(&mut self) -> Option<(ArenaBlock, usize)> {
        if !self.is_arena() {
            return None;
        }
        match std::mem::replace(&mut self.storage, Storage::Heap(Vec::new())) {
            Storage::Arena { block, len } => Some((block, len)),
            Storage::Heap(_) => unreachable!("checked above"),
        }
    }

    /// The bytes as an owned `Vec`, copying out of an arena block.
    pub fn into_vec(self) -> Vec<u8> {
        match self.storage {
            Storage::Heap(vec) => vec,
            Storage::Arena { block, len } => block.as_slice(len).to_vec(),
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Heap(vec) => vec.len(),
            Storage::Arena { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        match &self.storage {
            Storage::Heap(vec) => vec.capacity(),
            Storage::Arena { block, .. } => block.len(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.storage {
            Storage::Heap(vec) => vec.clear(),
            Storage::Arena { len, .. } => *len = 0,
        }
    }

    pub fn truncate(&mut self, new_len: usize) {
        match &mut self.storage {
            Storage::Heap(vec) => vec.truncate(new_len),
            Storage::Arena { len, .. } => {
                if new_len < *len {
                    *len = new_len;
                }
            }
        }
    }

    /// Moves the bytes to the heap when the arena block cannot hold
    /// `required` bytes.
    fn ensure_capacity(&mut self, required: usize) {
        if let Storage::Arena { block, len } = &mut self.storage
            && required > block.len()
        {
            let mut vec = Vec::with_capacity(required);
            vec.extend_from_slice(block.as_slice(*len));
            self.storage = Storage::Heap(vec);
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        let required = self.len().saturating_add(additional);
        self.ensure_capacity(required);
        if let Storage::Heap(vec) = &mut self.storage {
            vec.reserve(additional);
        }
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        let required = self.len().saturating_add(additional);
        self.ensure_capacity(required);
        if let Storage::Heap(vec) = &mut self.storage {
            vec.reserve_exact(additional);
        }
    }

    pub fn extend_from_slice(&mut self, other: &[u8]) {
        let required = self.len().saturating_add(other.len());
        self.ensure_capacity(required);
        match &mut self.storage {
            Storage::Heap(vec) => vec.extend_from_slice(other),
            Storage::Arena { block, len } => {
                let start = *len;
                block.as_mut_slice(required)[start..].copy_from_slice(other);
                *len = required;
            }
        }
    }

    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.ensure_capacity(new_len);
        match &mut self.storage {
            Storage::Heap(vec) => vec.resize(new_len, value),
            Storage::Arena { block, len } => {
                if new_len > *len {
                    let start = *len;
                    block.as_mut_slice(new_len)[start..].fill(value);
                }
                *len = new_len;
            }
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            Storage::Heap(vec) => vec.as_slice(),
            Storage::Arena { block, len } => block.as_slice(*len),
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.storage {
            Storage::Heap(vec) => vec.as_mut_slice(),
            Storage::Arena { block, len } => block.as_mut_slice(*len),
        }
    }

    /// Splits off the bytes from `at` into a new heap buffer.
    pub fn split_off(&mut self, at: usize) -> ShardBuf {
        match &mut self.storage {
            Storage::Heap(vec) => ShardBuf::from_vec(vec.split_off(at)),
            Storage::Arena { block, len } => {
                let tail = block.as_slice(*len)[at..].to_vec();
                *len = at;
                ShardBuf::from_vec(tail)
            }
        }
    }
}

impl Deref for ShardBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for ShardBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for ShardBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for ShardBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl From<Vec<u8>> for ShardBuf {
    fn from(vec: Vec<u8>) -> Self {
        Self::from_vec(vec)
    }
}

impl From<ShardBuf> for Vec<u8> {
    fn from(buf: ShardBuf) -> Self {
        buf.into_vec()
    }
}

impl FromIterator<u8> for ShardBuf {
    fn from_iter<I: IntoIterator<Item = u8>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

impl Extend<u8> for ShardBuf {
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        for byte in iter {
            self.extend_from_slice(&[byte]);
        }
    }
}

impl<'a> Extend<&'a u8> for ShardBuf {
    fn extend<I: IntoIterator<Item = &'a u8>>(&mut self, iter: I) {
        for byte in iter {
            self.extend_from_slice(&[*byte]);
        }
    }
}

impl Clone for ShardBuf {
    /// Clones onto the heap; arena blocks are not duplicated.
    fn clone(&self) -> Self {
        Self::from_vec(self.as_slice().to_vec())
    }
}

impl PartialEq for ShardBuf {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<Vec<u8>> for ShardBuf {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<[u8]> for ShardBuf {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl Eq for ShardBuf {}

impl fmt::Debug for ShardBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardBuf")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("arena", &self.is_arena())
            .finish()
    }
}

// SAFETY: `chunk_mut` hands out the spare capacity and `advance_mut` only
// moves the length within the initialized capacity, exactly like `Vec<u8>`;
// readers write into the spare area before advancing.
unsafe impl BufMut for ShardBuf {
    fn remaining_mut(&self) -> usize {
        match &self.storage {
            Storage::Heap(vec) => vec.remaining_mut(),
            Storage::Arena { block, len } => block.len() - *len,
        }
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        match &mut self.storage {
            // SAFETY: forwarded to `Vec<u8>`, which upholds the same contract.
            Storage::Heap(vec) => unsafe { vec.advance_mut(cnt) },
            Storage::Arena { block, len } => {
                assert!(*len + cnt <= block.len(), "advance_mut past the arena block");
                *len += cnt;
            }
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        match &mut self.storage {
            Storage::Heap(vec) => vec.chunk_mut(),
            Storage::Arena { block, len } => {
                let spare = block.len() - *len;
                // SAFETY: the spare area is valid, writable memory of the block.
                unsafe { UninitSlice::from_raw_parts_mut(block.ptr.add(*len), spare) }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn registered_slab_arena_hands_out_disjoint_blocks_and_takes_them_back() {
        let mut slab = vec![0u8; 4 * 8];
        let base = slab.as_mut_ptr();
        // SAFETY: the slab outlives the arena (it is moved in as the owner below).
        let arena = unsafe { RegisteredSlabArena::new(base, slab.len(), 8, 42, Box::new(slab)) };
        assert_eq!(arena.block_count(), 4);
        let dyn_arena: Arc<dyn ShardArena> = arena.clone();
        let a = dyn_arena.alloc().unwrap();
        let b = dyn_arena.alloc().unwrap();
        assert_ne!(a.offset(), b.offset());
        assert_eq!(a.registration_id(), 42);
        assert_eq!(arena.free_blocks(), 2);
        drop(a);
        assert_eq!(arena.free_blocks(), 3);
        let mut buf = ShardBuf {
            storage: Storage::Arena { block: b, len: 0 },
        };
        buf.extend_from_slice(b"12345678");
        assert_eq!(&*buf, b"12345678");
        drop(buf);
        assert_eq!(arena.free_blocks(), 4);
    }

    /// A test arena over a heap slab.
    struct SlabArena {
        slab: Mutex<Vec<u8>>,
        block_len: usize,
        free: Mutex<Vec<usize>>,
        base: *mut u8,
    }

    unsafe impl Send for SlabArena {}
    unsafe impl Sync for SlabArena {}

    impl SlabArena {
        fn new(blocks: usize, block_len: usize) -> Arc<Self> {
            let mut slab = vec![0u8; blocks * block_len];
            let base = slab.as_mut_ptr();
            Arc::new(Self {
                slab: Mutex::new(slab),
                block_len,
                free: Mutex::new((0..blocks).collect()),
                base,
            })
        }

        fn free_blocks(&self) -> usize {
            self.free.lock().unwrap().len()
        }
    }

    impl ShardArena for SlabArena {
        fn block_len(&self) -> usize {
            self.block_len
        }

        fn alloc(&self) -> Option<ArenaBlock> {
            let index = self.free.lock().unwrap().pop()?;
            let _keep = self.slab.lock().unwrap();
            let ptr = unsafe { self.base.add(index * self.block_len) };
            // SAFETY: the slab outlives every block (the arena is kept alive by
            // the block's `Arc`) and the free list keeps blocks disjoint.
            Some(unsafe { ArenaBlock::new(arena_ref(self), ptr, index * self.block_len, self.block_len) })
        }

        fn release(&self, block: &ArenaBlock) {
            self.free.lock().unwrap().push(block.offset() / self.block_len);
        }

        fn registration_id(&self) -> u64 {
            7
        }
    }

    thread_local! {
        static CURRENT: std::cell::RefCell<Option<Arc<SlabArena>>> = const { std::cell::RefCell::new(None) };
    }

    fn arena_ref(_arena: &SlabArena) -> Arc<dyn ShardArena> {
        CURRENT.with(|current| current.borrow().clone().expect("arena set")) as Arc<dyn ShardArena>
    }

    fn arena_buf(arena: &Arc<SlabArena>, capacity: usize) -> ShardBuf {
        CURRENT.with(|current| *current.borrow_mut() = Some(Arc::clone(arena)));
        let block = arena.alloc().expect("free block");
        assert!(capacity <= block.len());
        ShardBuf {
            storage: Storage::Arena { block, len: 0 },
        }
    }

    #[test]
    fn heap_buffer_behaves_like_a_vec() {
        let mut buf = ShardBuf::with_heap_capacity(8);
        buf.extend_from_slice(b"hello");
        buf.resize(7, b'!');
        assert_eq!(&*buf, b"hello!!");
        buf.truncate(5);
        assert_eq!(buf.len(), 5);
        let tail = buf.split_off(2);
        assert_eq!(&*buf, b"he");
        assert_eq!(&*tail, b"llo");
        buf.clear();
        assert!(buf.is_empty());
        assert!(!buf.is_arena());
    }

    #[test]
    fn arena_buffer_fills_within_the_block_and_returns_it_on_drop() {
        let arena = SlabArena::new(2, 16);
        let mut buf = arena_buf(&arena, 16);
        assert!(buf.is_arena());
        assert_eq!(arena.free_blocks(), 1);
        buf.extend_from_slice(b"0123456789");
        buf.put_slice(b"abc");
        assert_eq!(buf.len(), 13);
        assert_eq!(&buf[10..], b"abc");
        buf.as_mut_slice()[0] = b'X';
        assert_eq!(buf[0], b'X');
        buf.truncate(4);
        assert_eq!(&*buf, b"X123");
        buf.resize(6, b'-');
        assert_eq!(&*buf, b"X123--");
        assert_eq!(buf.arena_block().map(|block| block.offset()), Some(16));
        drop(buf);
        assert_eq!(arena.free_blocks(), 2);
    }

    #[test]
    fn arena_buffer_moves_to_the_heap_when_it_outgrows_the_block() {
        let arena = SlabArena::new(1, 8);
        let mut buf = arena_buf(&arena, 8);
        buf.extend_from_slice(b"12345678");
        assert!(buf.is_arena());
        buf.extend_from_slice(b"9");
        assert!(!buf.is_arena());
        assert_eq!(&*buf, b"123456789");
        assert_eq!(arena.free_blocks(), 1, "the block went back to the arena");
    }

    #[test]
    fn taking_the_block_leaves_an_empty_heap_buffer() {
        let arena = SlabArena::new(1, 8);
        let mut buf = arena_buf(&arena, 8);
        buf.extend_from_slice(b"abcd");
        let (block, len) = buf.take_arena_block().expect("arena buffer");
        assert_eq!(len, 4);
        assert_eq!(block.as_slice(len), b"abcd");
        assert!(buf.is_empty() && !buf.is_arena());
        assert!(buf.take_arena_block().is_none());
        drop(block);
        assert_eq!(arena.free_blocks(), 1);
    }

    #[test]
    fn buf_mut_limit_reads_into_the_spare_area() {
        let arena = SlabArena::new(1, 8);
        let mut buf = arena_buf(&arena, 8);
        buf.extend_from_slice(b"ab");
        {
            let mut limited = (&mut buf).limit(3);
            limited.put_slice(b"cde");
            assert_eq!(limited.remaining_mut(), 0);
        }
        assert_eq!(&*buf, b"abcde");
        assert_eq!(buf.remaining_mut(), 3);
    }
}
