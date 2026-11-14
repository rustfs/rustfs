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

//! Default-off requested-byte accounting at Rust's global allocator boundary.
//!
//! Counts exclude native allocations that bypass `GlobalAlloc`, allocator
//! overhead and retained free pages. Concurrent snapshots may race operations
//! in flight; quiescent counts reflect all completed allocation/free calls.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static METRIC_SOURCE: OnceLock<&'static AtomicUsize> = OnceLock::new();

/// Adds live requested-byte accounting without changing the allocator backend.
pub struct DiagnosticAllocator<A> {
    inner: A,
    live_requested_bytes: AtomicUsize,
}

impl<A> DiagnosticAllocator<A> {
    /// Wrap the allocator used by this binary, retaining its allocation policy.
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            live_requested_bytes: AtomicUsize::new(0),
        }
    }

    /// Register this static global allocator with the memory observer.
    ///
    /// Library users that do not install this allocator publish no live-byte
    /// metric. Registration does not reset allocations made before `main`.
    ///
    /// # Panics
    ///
    /// Panics if a different allocator already registered its counter.
    pub fn enable_metrics(&'static self) {
        let source = METRIC_SOURCE.get_or_init(|| &self.live_requested_bytes);
        assert!(
            std::ptr::eq(*source, &self.live_requested_bytes),
            "a different diagnostic allocator is registered"
        );
    }
}

// SAFETY: Every pointer, size and alignment is forwarded unchanged to the
// wrapped allocator. Accounting allocates nothing and never dereferences a
// user pointer; failed allocations/reallocations leave the count unchanged.
#[allow(unsafe_code)]
unsafe impl<A: GlobalAlloc> GlobalAlloc for DiagnosticAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid GlobalAlloc layout.
        let pointer = unsafe { self.inner.alloc(layout) };
        if !pointer.is_null() {
            self.live_requested_bytes.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid GlobalAlloc layout.
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        if !pointer.is_null() {
            self.live_requested_bytes.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and original layout came from this allocator.
        unsafe { self.inner.dealloc(pointer, layout) };
        self.live_requested_bytes.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller supplies a live pointer, its layout and a valid
        // nonzero new size; the wrapped allocator preserves failure semantics.
        let replacement = unsafe { self.inner.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            if new_size >= layout.size() {
                self.live_requested_bytes
                    .fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                self.live_requested_bytes
                    .fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        replacement
    }
}

pub(crate) fn record_snapshot() {
    if let Some(source) = METRIC_SOURCE.get() {
        metrics::gauge!("rustfs_memory_rust_live_requested_bytes").set(source.load(Ordering::Relaxed) as f64);
    }
}

// SAFETY: Tests call GlobalAlloc directly with valid layouts and live
// pointers; each block below documents allocation ownership and cleanup.
#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use std::alloc::System;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, AtomicPtr};

    struct ControlledAllocator {
        fail: AtomicBool,
    }

    // SAFETY: Valid operations are forwarded to System. Injected failures
    // return null without freeing or modifying any existing allocation.
    unsafe impl GlobalAlloc for ControlledAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The test supplies a valid layout.
                unsafe { System.alloc(layout) }
            }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The test supplies a valid layout.
                unsafe { System.alloc_zeroed(layout) }
            }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: The pointer and layout came from System through us.
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The test supplies a live pointer and valid sizes.
                unsafe { System.realloc(pointer, layout, new_size) }
            }
        }
    }

    #[test]
    fn live_requested_bytes_preserve_zeroing_alignment_and_resize_contents() {
        let allocator = DiagnosticAllocator::new(System);
        let original = Layout::from_size_align(37, 64).unwrap();
        // SAFETY: Layouts are valid, each operation uses the current live
        // pointer/layout, and the final allocation is freed exactly once.
        unsafe {
            let pointer = allocator.alloc_zeroed(original);
            assert!(!pointer.is_null() && pointer.align_offset(64) == 0);
            assert!(std::slice::from_raw_parts(pointer, 37).iter().all(|byte| *byte == 0));
            assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 37);
            pointer.write_bytes(0x5a, 37);
            let pointer = allocator.realloc(pointer, original, 257);
            assert!(!pointer.is_null() && pointer.align_offset(64) == 0);
            assert!(std::slice::from_raw_parts(pointer, 37).iter().all(|byte| *byte == 0x5a));
            assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 257);
            let pointer = allocator.realloc(pointer, Layout::from_size_align(257, 64).unwrap(), 17);
            assert!(!pointer.is_null() && pointer.align_offset(64) == 0);
            assert!(std::slice::from_raw_parts(pointer, 17).iter().all(|byte| *byte == 0x5a));
            assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 17);
            allocator.dealloc(pointer, Layout::from_size_align(17, 64).unwrap());
        }
        assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn live_requested_bytes_do_not_count_failed_allocations_or_reallocations() {
        let allocator = DiagnosticAllocator::new(ControlledAllocator {
            fail: AtomicBool::new(false),
        });
        let layout = Layout::from_size_align(41, 8).unwrap();
        // SAFETY: Failed realloc leaves the original allocation alive, and
        // only that original pointer is inspected and eventually freed.
        unsafe {
            let pointer = allocator.alloc(layout);
            assert!(!pointer.is_null());
            pointer.write_bytes(0x42, layout.size());
            allocator.inner.fail.store(true, Ordering::Relaxed);
            assert!(allocator.alloc(layout).is_null());
            assert!(allocator.alloc_zeroed(layout).is_null());
            assert!(allocator.realloc(pointer, layout, 99).is_null());
            assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 41);
            assert!(std::slice::from_raw_parts(pointer, 41).iter().all(|byte| *byte == 0x42));
            allocator.dealloc(pointer, layout);
        }
        assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn live_requested_bytes_follow_cross_thread_frees() {
        let allocator = DiagnosticAllocator::new(System);
        let layout = Layout::from_size_align(8192, 64).unwrap();
        // SAFETY: The valid allocation is transferred to one scoped thread,
        // freed exactly once there, and never accessed after that transfer.
        let pointer = AtomicPtr::new(unsafe { allocator.alloc(layout) });
        assert!(!pointer.load(Ordering::Relaxed).is_null());
        assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 8192);
        std::thread::scope(|scope| {
            scope.spawn(|| unsafe { allocator.dealloc(pointer.load(Ordering::Relaxed), layout) });
        });
        assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn live_requested_bytes_match_concurrent_retained_payload() {
        let allocator = DiagnosticAllocator::new(System);
        let barrier = Barrier::new(5);
        let observed = std::thread::scope(|scope| {
            for size in [31, 4096, 8193, 65536] {
                let allocator = &allocator;
                let barrier = &barrier;
                scope.spawn(move || {
                    let layout = Layout::from_size_align(size, 16).unwrap();
                    // SAFETY: This thread owns its allocation throughout;
                    // barriers keep it live until the observer reads the count.
                    unsafe {
                        let pointer = allocator.alloc_zeroed(layout);
                        assert!(!pointer.is_null());
                        barrier.wait();
                        barrier.wait();
                        allocator.dealloc(pointer, layout);
                    }
                });
            }
            barrier.wait();
            let observed = allocator.live_requested_bytes.load(Ordering::Relaxed);
            barrier.wait();
            observed
        });
        assert_eq!(observed, 31 + 4096 + 8193 + 65536);
        assert_eq!(allocator.live_requested_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn live_requested_metric_requires_registration_and_preserves_drained_zero() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        static ALLOCATOR: DiagnosticAllocator<System> = DiagnosticAllocator::new(System);
        let observe = || {
            let recorder = DebuggingRecorder::new();
            metrics::with_local_recorder(&recorder, record_snapshot);
            recorder
                .snapshotter()
                .snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(key, _, _, value)| {
                    if key.key().name() != "rustfs_memory_rust_live_requested_bytes" {
                        return None;
                    }
                    match value {
                        DebugValue::Gauge(value) => Some(value.into_inner()),
                        _ => panic!("expected a live-byte gauge"),
                    }
                })
        };
        assert_eq!(observe(), None);
        let layout = Layout::from_size_align(73, 8).unwrap();
        // SAFETY: The valid allocation is initialized, retained across metric
        // registration, and freed once with its unchanged layout.
        unsafe {
            let pointer = ALLOCATOR.alloc_zeroed(layout);
            assert!(!pointer.is_null());
            ALLOCATOR.enable_metrics();
            ALLOCATOR.enable_metrics();
            assert_eq!(observe(), Some(73.0));
            ALLOCATOR.dealloc(pointer, layout);
        }
        assert_eq!(observe(), Some(0.0));
    }
}
