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

#![allow(unsafe_code)]

use backtrace::Backtrace;
use pprof::protos::Message;
use rand::Rng;
use starshard::ShardedHashMap;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Weak};

type AllocatorShardedMap = ShardedHashMap<usize, (usize, Arc<Vec<usize>>)>;
type LazyAllocatorShardedMap = LazyLock<AllocatorShardedMap>;

type AllocatorSampleHashMap = HashMap<*const Vec<usize>, (i64, i64, Arc<Vec<usize>>)>;
/// A wrapper around a GlobalAlloc that samples allocations and records stack traces.
pub struct TracingAllocator<A: GlobalAlloc> {
    inner: A,
}

// Thread-local reentrancy guard to prevent infinite recursion when recording allocations
thread_local! {
    static REENTRANCY_GUARD: Cell<bool> = const { Cell::new(false) };
}

// Global configuration
static SAMPLE_RATE: AtomicUsize = AtomicUsize::new(512 * 1024); // Default: sample every 512KB on average
static ENABLED: AtomicBool = AtomicBool::new(false);

// Global storage for profile data
// Map: Address (usize) -> (Size (usize), StackTrace (Arc<Vec<usize>>))
// We store the Arc to keep the stack trace alive as long as the allocation is live.
static LIVE_ALLOCATIONS: LazyAllocatorShardedMap = LazyLock::new(|| ShardedHashMap::new(64));

// Cache for deduplicating stack traces.
// Map: StackHash (u64) -> Weak<Vec<usize>>
// We use Weak references so that unused stack traces can be dropped when all referring allocations are freed.
static STACK_CACHE: LazyLock<ShardedHashMap<u64, Weak<Vec<usize>>>> = LazyLock::new(|| ShardedHashMap::new(64));

impl<A: GlobalAlloc> TracingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// Public configuration functions
#[allow(dead_code)]
pub fn set_sample_rate(rate: usize) {
    SAMPLE_RATE.store(rate, Ordering::Relaxed);
}

pub fn set_enabled(enabled: bool) {
    // Force initialization of LazyLocks before enabling profiling to avoid recursion during init.
    // Accessing them is enough to trigger initialization.
    let _ = &*LIVE_ALLOCATIONS;
    let _ = &*STACK_CACHE;

    ENABLED.store(enabled, Ordering::Relaxed);
}

fn should_sample(size: usize) -> bool {
    if !ENABLED.load(Ordering::Relaxed) {
        return false;
    }

    let rate = SAMPLE_RATE.load(Ordering::Relaxed);
    if rate == 0 {
        return true;
    }

    // Use a fresh RNG each time.
    let mut rng = rand::rng();
    rng.random_range(0..rate) < size
}

// Internal function, assumes guard is already held
fn record_alloc(ptr: *mut u8, size: usize) {
    // Capture stack trace
    let bt = Backtrace::new_unresolved();
    let mut frames = Vec::new();
    for frame in bt.frames() {
        frames.push(frame.symbol_address() as usize);
    }

    // Calculate hash of the stack trace
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    frames.hash(&mut hasher);
    let stack_hash = hasher.finish();

    // Deduplicate stack trace using STACK_CACHE
    let stack_arc = if let Some(weak) = STACK_CACHE.get(&stack_hash) {
        if let Some(arc) = weak.upgrade() {
            arc
        } else {
            // Entry exists but is dead, replace it
            let arc = Arc::new(frames);
            STACK_CACHE.insert(stack_hash, Arc::downgrade(&arc));
            arc
        }
    } else {
        // New entry
        let arc = Arc::new(frames);
        STACK_CACHE.insert(stack_hash, Arc::downgrade(&arc));
        arc
    };

    // Store the allocation info with the Arc
    LIVE_ALLOCATIONS.insert(ptr as usize, (size, stack_arc));
}

// Internal function, assumes guard is already held
fn record_dealloc(ptr: *mut u8) {
    // Remove from live allocations.
    // The Arc<Vec<usize>> will be dropped.
    // If it was the last reference, the Vec<usize> is freed.
    // The Weak pointer in STACK_CACHE remains but becomes upgrade-able to None.
    LIVE_ALLOCATIONS.remove(&(ptr as usize));
}

/// Dump the current profile to a pprof protobuf file
pub fn dump_profile(path: &Path) -> Result<(), String> {
    // Prevent reentrancy during dump
    if REENTRANCY_GUARD.replace(true) {
        return Err("Reentrancy detected during dump".to_string());
    }

    // Perform a lazy cleanup of the cache during dump
    cleanup_cache();

    let result = dump_profile_inner(path);

    REENTRANCY_GUARD.set(false);
    result
}

// Clean up dead entries from STACK_CACHE
fn cleanup_cache() {
    // We collect dead keys first to avoid locking issues during iteration if any
    let mut dead_keys = Vec::new();

    // Note: This iteration might be slow if the cache is huge, but dump_profile is infrequent.
    for entry in STACK_CACHE.iter() {
        let (key, weak) = entry;
        if weak.upgrade().is_none() {
            dead_keys.push(key);
        }
    }

    for key in dead_keys {
        STACK_CACHE.remove(&key);
    }
}

fn dump_profile_inner(path: &Path) -> Result<(), String> {
    use pprof::protos as pb;

    let mut profile = pb::Profile::default();

    // Basic metadata
    profile.string_table.push("".to_string()); // 0: empty
    profile.string_table.push("alloc_objects".to_string()); // 1
    profile.string_table.push("count".to_string()); // 2
    profile.string_table.push("alloc_space".to_string()); // 3
    profile.string_table.push("bytes".to_string()); // 4

    let sample_type_count = pb::ValueType {
        ty: 1,   // "alloc_objects"
        unit: 2, // "count"
        ..Default::default()
    };
    let sample_type_bytes = pb::ValueType {
        ty: 3,   // "alloc_space"
        unit: 4, // "bytes"
        ..Default::default()
    };
    profile.sample_type = vec![sample_type_count, sample_type_bytes];

    // Helper to get string ID
    let mut string_map: HashMap<String, i64> = HashMap::new();
    string_map.insert("".to_string(), 0);
    string_map.insert("alloc_objects".to_string(), 1);
    string_map.insert("count".to_string(), 2);
    string_map.insert("alloc_space".to_string(), 3);
    string_map.insert("bytes".to_string(), 4);

    let mut get_string_id = |s: String| -> i64 {
        if let Some(&id) = string_map.get(&s) {
            id
        } else {
            let id = profile.string_table.len() as i64;
            profile.string_table.push(s.clone());
            string_map.insert(s, id);
            id
        }
    };

    // Helper to get location ID
    let mut location_map: HashMap<usize, u64> = HashMap::new(); // addr -> loc_id
    let mut function_map: HashMap<usize, u64> = HashMap::new(); // addr -> func_id

    // Collect samples
    // Aggregate by Stack Trace Pointer (deduplication via Arc pointer)
    // Map: Arc pointer -> (Count, Bytes, Arc<Vec<usize>>)
    let mut aggregated_samples: AllocatorSampleHashMap = HashMap::new();

    // Step 1: Collect data from LIVE_ALLOCATIONS while holding the lock (implicitly via iter)
    // We do NOT perform symbol resolution here to avoid deadlocks.
    for entry in LIVE_ALLOCATIONS.iter() {
        let (_ptr, (size, stack_arc)) = entry;
        let stack_arc_clone = stack_arc.clone();
        let key = Arc::as_ptr(&stack_arc_clone);

        let agg = aggregated_samples.entry(key).or_insert_with(|| (0, 0, stack_arc_clone));
        agg.0 += 1;
        agg.1 += size as i64;
    }
    // LIVE_ALLOCATIONS lock is released here as the iterator is dropped.

    // Step 2: Process samples and resolve symbols (outside of LIVE_ALLOCATIONS lock)
    for (_key, (count, bytes, frames)) in aggregated_samples {
        let mut sample = pb::Sample {
            value: vec![count, bytes],
            ..Default::default()
        };

        // Process frames
        for &addr in frames.iter() {
            let loc_id = if let Some(&id) = location_map.get(&addr) {
                id
            } else {
                // Resolve symbol
                // This might take time and locks, but we are safe now.
                let mut func_name = "unknown".to_string();
                let mut file_name = "unknown".to_string();
                let mut line_no = 0;

                backtrace::resolve(addr as *mut std::ffi::c_void, |symbol| {
                    if let Some(name) = symbol.name() {
                        func_name = name.to_string();
                    }
                    if let Some(filename) = symbol.filename() {
                        file_name = filename.to_string_lossy().to_string();
                    }
                    if let Some(line) = symbol.lineno() {
                        line_no = line as i64;
                    }
                });

                // Create Function
                let func_id = if let Some(&id) = function_map.get(&addr) {
                    id
                } else {
                    let id = (profile.function.len() + 1) as u64;
                    let name_id = get_string_id(func_name);
                    let file_id = get_string_id(file_name);

                    let func = pb::Function {
                        id,
                        name: name_id,
                        system_name: name_id,
                        filename: file_id,
                        start_line: 0,
                        ..Default::default()
                    };
                    profile.function.push(func);
                    function_map.insert(addr, id);
                    id
                };

                // Create Location
                let id = (profile.location.len() + 1) as u64;
                let line = pb::Line {
                    function_id: func_id,
                    line: line_no,
                    ..Default::default()
                };
                let loc = pb::Location {
                    id,
                    mapping_id: 0,
                    address: addr as u64,
                    line: vec![line],
                    is_folded: false,
                    ..Default::default()
                };
                profile.location.push(loc);
                location_map.insert(addr, id);
                id
            };
            sample.location_id.push(loc_id);
        }
        profile.sample.push(sample);
    }

    // Write to file
    let mut buf = Vec::with_capacity(1024 * 1024);
    profile.write_to_vec(&mut buf).map_err(|e| format!("encode failed: {e}"))?;

    let mut f = File::create(path).map_err(|e| format!("create file failed: {e}"))?;
    f.write_all(&buf).map_err(|e| format!("write file failed: {e}"))?;

    Ok(())
}

// Helper to handle sampling logic
#[inline(always)]
fn handle_alloc_sampling(ptr: *mut u8, size: usize) {
    if !ptr.is_null() {
        // Check reentrancy guard BEFORE calling should_sample
        if !REENTRANCY_GUARD.replace(true) {
            if should_sample(size) {
                record_alloc(ptr, size);
            }
            REENTRANCY_GUARD.set(false);
        }
    }
}

// Helper to handle dealloc logic
#[inline(always)]
fn handle_dealloc_sampling(ptr: *mut u8) {
    if !REENTRANCY_GUARD.replace(true) {
        record_dealloc(ptr);
        REENTRANCY_GUARD.set(false);
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for TracingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegating to inner allocator.
        let ptr = unsafe { self.inner.alloc(layout) };
        handle_alloc_sampling(ptr, layout.size());
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        handle_dealloc_sampling(ptr);
        // SAFETY: Delegating to inner allocator.
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegating to inner allocator.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        handle_alloc_sampling(ptr, layout.size());
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        handle_dealloc_sampling(ptr);

        // SAFETY: Delegating to inner allocator.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };

        handle_alloc_sampling(new_ptr, new_size);
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::alloc::System;
    use std::thread;
    use tempfile::NamedTempFile;

    // Use System allocator for testing
    static TEST_ALLOCATOR: TracingAllocator<System> = TracingAllocator::new(System);

    #[test]
    #[serial]
    fn test_basic_allocation_tracking() {
        // Enable profiling and force sampling (rate = 1 means sample everything)
        set_enabled(true);
        set_sample_rate(1);

        unsafe {
            let layout = Layout::from_size_align(1024, 8).unwrap();
            let ptr = TEST_ALLOCATOR.alloc(layout);
            assert!(!ptr.is_null());

            // Verify allocation is recorded
            assert!(LIVE_ALLOCATIONS.get(&(ptr as usize)).is_some());

            TEST_ALLOCATOR.dealloc(ptr, layout);

            // Verify allocation is removed
            assert!(LIVE_ALLOCATIONS.get(&(ptr as usize)).is_none());
        }

        // Reset
        set_enabled(false);
    }

    #[test]
    #[serial]
    fn test_reentrancy_guard() {
        set_enabled(true);
        set_sample_rate(1);

        // Manually set guard to simulate reentrancy
        REENTRANCY_GUARD.set(true);

        unsafe {
            let layout = Layout::from_size_align(128, 8).unwrap();
            let ptr = TEST_ALLOCATOR.alloc(layout);

            // Should NOT be recorded because guard was true
            assert!(LIVE_ALLOCATIONS.get(&(ptr as usize)).is_none());

            TEST_ALLOCATOR.dealloc(ptr, layout);
        }

        REENTRANCY_GUARD.set(false);
        set_enabled(false);
    }

    #[test]
    #[serial]
    fn test_sampling_logic() {
        set_enabled(true);
        // Set a high rate so small allocations are unlikely to be sampled
        set_sample_rate(1_000_000);

        let mut sampled_count = 0;
        let iterations = 100;

        unsafe {
            let layout = Layout::from_size_align(8, 8).unwrap();
            for _ in 0..iterations {
                let ptr = TEST_ALLOCATOR.alloc(layout);
                if LIVE_ALLOCATIONS.get(&(ptr as usize)).is_some() {
                    sampled_count += 1;
                }
                TEST_ALLOCATOR.dealloc(ptr, layout);
            }
        }

        // With high sample rate and small size, sampled count should be low (likely 0)
        // This is probabilistic, but 0 is very likely.
        assert!(sampled_count < iterations);

        set_enabled(false);
    }

    #[test]
    #[serial]
    fn test_profile_dump() {
        set_enabled(true);
        // Use a larger sample rate to avoid capturing too much noise from the test runner
        // and ensure we only capture our large allocation.
        set_sample_rate(1024 * 1024);

        unsafe {
            // Allocate a large enough chunk to likely be sampled (2MB > 1MB rate)
            let layout = Layout::from_size_align(2 * 1024 * 1024, 8).unwrap();
            let ptr = TEST_ALLOCATOR.alloc(layout);

            let file = NamedTempFile::new().unwrap();
            let path = file.path();

            let result = dump_profile(path);
            assert!(result.is_ok());

            let metadata = std::fs::metadata(path).unwrap();
            assert!(metadata.len() > 0);

            TEST_ALLOCATOR.dealloc(ptr, layout);
        }
        set_enabled(false);
    }

    #[test]
    #[serial]
    fn test_concurrent_allocations() {
        set_enabled(true);
        set_sample_rate(1);

        let threads: Vec<_> = (0..10)
            .map(|_| {
                thread::spawn(|| {
                    unsafe {
                        let layout = Layout::from_size_align(64, 8).unwrap();
                        for _ in 0..100 {
                            let ptr = TEST_ALLOCATOR.alloc(layout);
                            // Just ensure no panic/crash
                            TEST_ALLOCATOR.dealloc(ptr, layout);
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // After all threads join and dealloc, map should be empty (ignoring other potential allocations in test runner)
        // Note: In a real test runner, other tests might be running, so we can't assert empty.
        // But we verified no crashes.
        set_enabled(false);
    }
}
