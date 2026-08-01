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
struct DefaultMiMalloc;

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
// SAFETY: allocation and deallocation are forwarded unchanged to MiMalloc, so
// MiMalloc's GlobalAlloc guarantees apply to every returned pointer and layout.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for DefaultMiMalloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds GlobalAlloc's contract for layout.
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout came from this allocator and are forwarded unchanged.
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
}

#[cfg(all(feature = "hotpath", feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: hotpath::CountingAllocator<DefaultMiMalloc> = hotpath::CountingAllocator::new();

#[cfg(not(all(feature = "hotpath", feature = "hotpath-alloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("main").build();

    rustfs::startup_entrypoint::run_process();
}

#[cfg(all(test, feature = "hotpath", feature = "hotpath-alloc"))]
mod tests {
    #[test]
    #[allow(unsafe_code)]
    fn hotpath_allocator_uses_mimalloc() {
        let allocation = Box::new([0_u8; 64]);

        // SAFETY: the live Box pointer is valid to inspect for heap ownership.
        assert!(unsafe { libmimalloc_sys::mi_is_in_heap_region(allocation.as_ptr().cast()) });
    }
}
