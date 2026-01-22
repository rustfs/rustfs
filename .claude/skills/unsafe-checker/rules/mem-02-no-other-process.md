---
id: mem-02
original_id: P.UNS.MEM.02
level: P
impact: CRITICAL
---

# Do Not Modify Memory Variables of Other Processes or Dynamic Libraries

## Summary

Do not directly manipulate memory belonging to other processes or dynamically loaded libraries. Use proper IPC or FFI mechanisms.

## Rationale

- Other processes have separate address spaces; direct access is impossible on modern OSes
- Shared memory requires explicit setup and synchronization
- Dynamic library memory has ownership rules that must be respected
- Violating these causes undefined behavior or security vulnerabilities

## Bad Example

```rust
// DON'T: Try to access another process's memory directly
fn bad_cross_process(ptr: *mut i32) {
    // This pointer from another process is meaningless in our address space
    unsafe { *ptr = 42; }  // Undefined behavior or crash
}

// DON'T: Modify library internals
extern "C" {
    static mut LIBRARY_INTERNAL: i32;
}

fn bad_library_access() {
    // Modifying library internals breaks encapsulation
    unsafe { LIBRARY_INTERNAL = 100; }  // May corrupt library state
}
```

## Good Example

```rust
// DO: Use proper IPC for cross-process communication
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

fn ipc_communication() -> std::io::Result<()> {
    let mut stream = UnixStream::connect("/tmp/socket")?;
    stream.write_all(b"message")?;
    Ok(())
}

// DO: Use shared memory with proper synchronization
#[cfg(unix)]
fn shared_memory_example() {
    use std::sync::atomic::{AtomicI32, Ordering};

    // Properly set up shared memory region
    // let shm = mmap shared memory...

    // Use atomic operations for synchronization
    let shared: &AtomicI32 = /* ... */;
    shared.store(42, Ordering::Release);
}

// DO: Use proper FFI for library interaction
mod ffi {
    extern "C" {
        pub fn library_set_value(value: i32);
        pub fn library_get_value() -> i32;
    }
}

fn proper_library_access() {
    unsafe {
        ffi::library_set_value(42);
        let value = ffi::library_get_value();
    }
}

// DO: Use Rust's libloading for dynamic libraries
fn dynamic_library() -> Result<(), Box<dyn std::error::Error>> {
    let lib = unsafe { libloading::Library::new("mylib.so")? };
    let func: libloading::Symbol<extern "C" fn(i32) -> i32> =
        unsafe { lib.get(b"my_function")? };
    let result = func(42);
    Ok(())
}
```

## Memory Ownership Rules

| Memory Type | Owner | Safe Access |
|-------------|-------|-------------|
| Stack variables | Current function | Direct |
| Heap (Box, Vec) | Rust allocator | Through smart pointers |
| Static | Program | With proper synchronization |
| Shared memory | Multiple processes | Atomic ops, mutexes |
| Library memory | Library | Through library API |
| FFI-allocated | C allocator | Through C free functions |

## Checklist

- [ ] Who allocated this memory?
- [ ] Who is responsible for freeing it?
- [ ] Is proper synchronization in place for shared access?
- [ ] Am I using the correct API for cross-boundary access?

## Related Rules

- `mem-03`: Don't let String/Vec drop other process's memory
- `ffi-03`: Implement Drop for wrapped C pointers
