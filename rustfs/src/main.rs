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

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
use std::alloc::{GlobalAlloc, Layout};

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
#[derive(Default)]
struct MiMallocAllocator;

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
// SAFETY: allocation operations are forwarded unchanged to MiMalloc, so
// MiMalloc's GlobalAlloc guarantees apply to every returned pointer and layout.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for MiMallocAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds GlobalAlloc's contract for layout.
        unsafe { rustfs_mimalloc::MiMalloc.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds GlobalAlloc's contract for layout.
        unsafe { rustfs_mimalloc::MiMalloc.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout came from this allocator and are forwarded unchanged.
        unsafe { rustfs_mimalloc::MiMalloc.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: ptr and layout came from this allocator and are forwarded unchanged.
        unsafe { rustfs_mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
}

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: hotpath::CountingAllocator<MiMallocAllocator> = hotpath::CountingAllocator::with(MiMallocAllocator);

#[cfg(not(all(feature = "hotpath", feature = "hotpath-alloc")))]
#[global_allocator]
static GLOBAL: rustfs_mimalloc::MiMalloc = rustfs_mimalloc::MiMalloc;

fn main() {
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("main").build();

    rustfs::startup_entrypoint::run_process();
}

#[cfg(all(test, feature = "hotpath", feature = "hotpath-alloc", not(target_os = "windows")))]
mod tests {
    #[test]
    // SAFETY: This test inspects a live allocation pointer with mimalloc's heap
    // ownership API without dereferencing or extending the pointer lifetime.
    #[allow(unsafe_code)]
    fn hotpath_allocation_workload_uses_mimalloc() {
        let _guard = hotpath::MeasurementGuardSync::new("rustfs::tests::hotpath_allocation_workload_uses_mimalloc", false, false);
        let mut allocation = Vec::with_capacity(64);
        allocation.extend_from_slice(&[7_u8; 64]);

        assert_eq!(allocation.len(), 64);
        let heap = rustfs_mimalloc::heap::Heap::main();
        // SAFETY: the live Vec pointer is valid to inspect for heap ownership.
        assert!(unsafe { heap.contains(allocation.as_ptr()) });
    }

    #[test]
    // SAFETY: This test directly exercises the allocator wrapper and releases
    // every successful allocation with the matching layout.
    #[allow(unsafe_code)]
    fn mimalloc_allocator_forwards_extended_global_alloc_operations() {
        use std::alloc::{GlobalAlloc, Layout};

        let layout = Layout::from_size_align(32, 8).expect("valid test allocation layout");
        let grown_layout = Layout::from_size_align(64, 8).expect("valid grown test allocation layout");
        let allocator = super::MiMallocAllocator;
        let heap = rustfs_mimalloc::heap::Heap::main();

        // SAFETY: The pointer is checked for null before use and later released
        // through the same allocator with the corresponding layout.
        let ptr = unsafe { allocator.alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        assert!(unsafe { heap.contains(ptr) });
        assert!(unsafe { std::slice::from_raw_parts(ptr, 32).iter().all(|byte| *byte == 0) });

        // SAFETY: `ptr` was allocated by `allocator` with `layout`; on failure
        // the original allocation remains valid and is released below.
        let grown_ptr = unsafe { allocator.realloc(ptr, layout, 64) };
        if grown_ptr.is_null() {
            // SAFETY: `ptr` is still valid when realloc returns null.
            unsafe { allocator.dealloc(ptr, layout) };
            panic!("mimalloc realloc failed in allocator smoke test");
        }

        assert!(unsafe { heap.contains(grown_ptr) });
        // SAFETY: `grown_ptr` was reallocated by `allocator` and is released
        // with the matching grown layout.
        unsafe { allocator.dealloc(grown_ptr, grown_layout) };
    }
}
