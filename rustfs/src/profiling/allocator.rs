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
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A wrapper around a GlobalAlloc that samples allocations and records stack traces.
pub struct TracingAllocator<A: GlobalAlloc> {
    inner: A,
}

// Thread-local reentrancy guard to prevent infinite recursion when recording allocations
thread_local! {
    static REENTRANCY_GUARD: Cell<bool> = const { Cell::new(false) };
    static RNG: Cell<Option<rand::rngs::ThreadRng>> = const { Cell::new(None) };
}

// Global configuration
static SAMPLE_RATE: AtomicUsize = AtomicUsize::new(512 * 1024); // Default: sample every 512KB on average
static ENABLED: AtomicBool = AtomicBool::new(false);

// Global storage for profile data
// Map: Address (usize) -> (Size (usize), StackHash (u64))
static LIVE_ALLOCATIONS: LazyLock<ShardedHashMap<usize, (usize, u64)>> = LazyLock::new(|| ShardedHashMap::new(64));

// Map: StackHash (u64) -> Vec<usize> (Instruction Pointers)
// We use a separate map to store unique stack traces to save memory
static STACK_TRACES: LazyLock<ShardedHashMap<u64, Vec<usize>>> = LazyLock::new(|| ShardedHashMap::new(64));

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

    // Use thread-local RNG
    RNG.with(|rng_cell| {
        let mut rng_opt = rng_cell.take();
        let rng = rng_opt.get_or_insert_with(rand::rng);
        let should = rng.random_range(0..rate) < size;
        rng_cell.set(rng_opt);
        should
    })
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

    // Store data
    // 1. Store the stack trace.
    STACK_TRACES.insert(stack_hash, frames);

    // 2. Store the allocation info
    LIVE_ALLOCATIONS.insert(ptr as usize, (size, stack_hash));
}

// Internal function, assumes guard is already held
fn record_dealloc(ptr: *mut u8) {
    // Remove from live allocations if it exists.
    if LIVE_ALLOCATIONS.get(&(ptr as usize)).is_some() {
        LIVE_ALLOCATIONS.remove(&(ptr as usize));
    }
}

/// Dump the current profile to a pprof protobuf file
pub fn dump_profile(path: &Path) -> Result<(), String> {
    // Prevent reentrancy during dump
    if REENTRANCY_GUARD.with(|g| g.replace(true)) {
        return Err("Reentrancy detected during dump".to_string());
    }

    let result = dump_profile_inner(path);

    REENTRANCY_GUARD.with(|g| g.set(false));
    result
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
    profile.string_table.push("space".to_string()); // 5

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
    string_map.insert("space".to_string(), 5);

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
    // Aggregate by stack hash: StackHash -> (Count, Bytes)
    let mut aggregated_samples: HashMap<u64, (i64, i64)> = HashMap::new();

    // Iterate over all live allocations
    for entry in LIVE_ALLOCATIONS.iter() {
        let (_ptr, (size, stack_hash)) = entry;
        let agg = aggregated_samples.entry(stack_hash).or_insert((0, 0));
        agg.0 += 1;
        agg.1 += size as i64;
    }

    // Build Profile
    for (stack_hash, (count, bytes)) in aggregated_samples {
        if let Some(frames) = STACK_TRACES.get(&stack_hash) {
            let mut sample = pb::Sample::default();
            sample.value = vec![count, bytes];

            // Process frames
            for &addr in frames.iter() {
                let loc_id = if let Some(&id) = location_map.get(&addr) {
                    id
                } else {
                    // Resolve symbol
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
    }

    // Write to file
    let mut buf = Vec::with_capacity(1024 * 1024);
    profile.write_to_vec(&mut buf).map_err(|e| format!("encode failed: {e}"))?;

    let mut f = File::create(path).map_err(|e| format!("create file failed: {e}"))?;
    f.write_all(&buf).map_err(|e| format!("write file failed: {e}"))?;

    Ok(())
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for TracingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: We are implementing GlobalAlloc, so we must ensure safety.
        // We are delegating to the inner allocator which is also unsafe.
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            // Check reentrancy guard BEFORE calling should_sample, because should_sample
            // might allocate (e.g. initializing thread-local RNG).
            let reentrant = REENTRANCY_GUARD.with(|g| g.replace(true));
            if !reentrant {
                if should_sample(layout.size()) {
                    record_alloc(ptr, layout.size());
                }
                REENTRANCY_GUARD.with(|g| g.set(false));
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Check reentrancy guard BEFORE checking LIVE_ALLOCATIONS, because get() might allocate.
        let reentrant = REENTRANCY_GUARD.with(|g| g.replace(true));
        if !reentrant {
            if LIVE_ALLOCATIONS.get(&(ptr as usize)).is_some() {
                record_dealloc(ptr);
            }
            REENTRANCY_GUARD.with(|g| g.set(false));
        }
        // SAFETY: Delegating to inner allocator.
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegating to inner allocator.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            let reentrant = REENTRANCY_GUARD.with(|g| g.replace(true));
            if !reentrant {
                if should_sample(layout.size()) {
                    record_alloc(ptr, layout.size());
                }
                REENTRANCY_GUARD.with(|g| g.set(false));
            }
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Handle dealloc part
        let reentrant = REENTRANCY_GUARD.with(|g| g.replace(true));
        if !reentrant {
            if LIVE_ALLOCATIONS.get(&(ptr as usize)).is_some() {
                record_dealloc(ptr);
            }
            REENTRANCY_GUARD.with(|g| g.set(false));
        }

        // SAFETY: Delegating to inner allocator.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };

        // Handle alloc part
        if !new_ptr.is_null() {
            let reentrant = REENTRANCY_GUARD.with(|g| g.replace(true));
            if !reentrant {
                if should_sample(new_size) {
                    record_alloc(new_ptr, new_size);
                }
                REENTRANCY_GUARD.with(|g| g.set(false));
            }
        }
        new_ptr
    }
}
