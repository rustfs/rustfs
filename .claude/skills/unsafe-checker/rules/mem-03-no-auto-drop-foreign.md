---
id: mem-03
original_id: P.UNS.MEM.03
level: P
impact: CRITICAL
---

# Do Not Let String/Vec Auto-Drop Other Process's Memory

## Summary

Never create `String`, `Vec`, or `Box` from memory allocated outside Rust's allocator. They will try to free the memory with the wrong deallocator.

## Rationale

`String`, `Vec`, and `Box` assume memory was allocated by Rust's global allocator. When dropped, they call `dealloc`. If the memory came from C's `malloc`, a different allocator, or shared memory, this causes undefined behavior.

## Bad Example

```rust
// DON'T: Create String from C-allocated memory
extern "C" {
    fn c_get_string() -> *mut std::os::raw::c_char;
}

fn bad_string() -> String {
    unsafe {
        let ptr = c_get_string();
        // BAD: String will try to free with Rust allocator
        String::from_raw_parts(ptr as *mut u8, len, cap)
    }
}

// DON'T: Create Vec from foreign memory
fn bad_vec(ptr: *mut u8, len: usize) -> Vec<u8> {
    // BAD: Vec will free this memory incorrectly
    unsafe { Vec::from_raw_parts(ptr, len, len) }
}

// DON'T: Wrap shared memory in Box
fn bad_box(shared_ptr: *mut Data) -> Box<Data> {
    // BAD: Box will try to deallocate shared memory!
    unsafe { Box::from_raw(shared_ptr) }
}
```

## Good Example

```rust
use std::ffi::CStr;

extern "C" {
    fn c_get_string() -> *mut std::os::raw::c_char;
    fn c_free_string(s: *mut std::os::raw::c_char);
}

// DO: Copy data into Rust-owned allocation
fn good_string() -> String {
    unsafe {
        let ptr = c_get_string();
        let cstr = CStr::from_ptr(ptr);
        let result = cstr.to_string_lossy().into_owned();
        c_free_string(ptr);  // Free with correct deallocator
        result
    }
}

// DO: Use wrapper that calls correct deallocator
struct CString {
    ptr: *mut std::os::raw::c_char,
}

impl Drop for CString {
    fn drop(&mut self) {
        unsafe { c_free_string(self.ptr); }
    }
}

// DO: Use slice for borrowed view, don't take ownership
fn good_slice(ptr: *const u8, len: usize) -> &'static [u8] {
    // Only borrow, don't own
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

// DO: For shared memory, use raw pointers or custom wrapper
struct SharedBuffer {
    ptr: *mut u8,
    len: usize,
}

impl SharedBuffer {
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for SharedBuffer {
    fn drop(&mut self) {
        // Unmap shared memory, don't deallocate
        // munmap(self.ptr, self.len);
    }
}
```

## Memory Allocation Compatibility

| Allocator | Can use Rust Vec/String/Box? |
|-----------|------------------------------|
| Rust global allocator | Yes |
| C malloc | No - use wrapper with C free |
| C++ new | No - use wrapper with C++ delete |
| Custom allocator | No - use allocator_api |
| mmap/shared memory | No - use munmap |
| Stack/static | No - never "free" |

## Checklist

- [ ] Who allocated this memory?
- [ ] Is it from Rust's global allocator?
- [ ] If not, do I have a custom Drop that frees correctly?
- [ ] Am I copying data or taking ownership?

## Related Rules

- `mem-02`: Don't modify other process's memory
- `ffi-03`: Implement Drop for wrapped C pointers
- `ffi-07`: Don't implement Drop for types passed to external code
