---
id: ffi-02
original_id: P.UNS.FFI.02
level: P
impact: MEDIUM
---

# Read Documentation Carefully When Using std::ffi Types

## Summary

The `std::ffi` module has many types with subtle differences. Read their documentation carefully to avoid misuse.

## Key Types in std::ffi

### CString vs CStr

```rust
use std::ffi::{CString, CStr};
use std::os::raw::c_char;

// CString: Owned, heap-allocated, null-terminated
// - Use when creating strings to pass to C
// - Owns the memory
let owned = CString::new("hello").unwrap();
let ptr: *const c_char = owned.as_ptr();
// ptr valid until `owned` is dropped

// CStr: Borrowed, null-terminated
// - Use when receiving strings from C
// - Does not own memory
let borrowed: &CStr = unsafe { CStr::from_ptr(ptr) };
// borrowed valid as long as ptr is valid
```

### OsString vs OsStr

```rust
use std::ffi::{OsString, OsStr};
use std::path::Path;

// OsString/OsStr: Platform-native strings
// - Windows: potentially ill-formed UTF-16
// - Unix: arbitrary bytes
// - Use for paths and environment variables

let path = Path::new("/some/path");
let os_str: &OsStr = path.as_os_str();

// Convert to Rust string (may fail)
if let Some(s) = os_str.to_str() {
    println!("Valid UTF-8: {}", s);
}
```

### c_void and Opaque Types

```rust
use std::ffi::c_void;

extern "C" {
    fn get_handle() -> *mut c_void;
    fn use_handle(h: *mut c_void);
}

// c_void is for truly opaque pointers
// Better: use dedicated opaque types (see ffi-17)
```

## Common Pitfalls

```rust
use std::ffi::CString;

// PITFALL 1: CString::as_ptr() lifetime
fn bad_ptr() -> *const i8 {
    let s = CString::new("hello").unwrap();
    s.as_ptr()  // Dangling! s dropped at end of function
}

fn good_ptr(s: &CString) -> *const i8 {
    s.as_ptr()  // OK: s outlives the pointer
}

// PITFALL 2: CString::new with interior nulls
let result = CString::new("hello\0world");
assert!(result.is_err());  // Interior null!

// PITFALL 3: CStr::from_ptr safety
unsafe {
    let ptr: *const i8 = std::ptr::null();
    // let cstr = CStr::from_ptr(ptr);  // UB: null pointer!

    // Always check for null first
    if !ptr.is_null() {
        let cstr = CStr::from_ptr(ptr);
    }
}

// PITFALL 4: CStr assumes valid null-terminated string
unsafe {
    let bytes = [104, 101, 108, 108, 111];  // "hello" without null
    let ptr = bytes.as_ptr() as *const i8;
    // let cstr = CStr::from_ptr(ptr);  // UB: no null terminator!

    // Use from_bytes_with_nul instead
    let bytes_with_nul = b"hello\0";
    let cstr = CStr::from_bytes_with_nul(bytes_with_nul).unwrap();
}
```

## Type Selection Guide

| Scenario | Type |
|----------|------|
| Create string for C | `CString` |
| Borrow string from C | `&CStr` |
| File paths | `OsString`, `Path` |
| Environment variables | `OsString` |
| Opaque C pointers | Newtype over `*mut c_void` |
| C integers | `c_int`, `c_long`, etc. |

## Checklist

- [ ] Have I read the docs for the std::ffi type I'm using?
- [ ] Am I aware of the lifetime constraints?
- [ ] Am I handling potential errors (NulError, UTF-8 errors)?
- [ ] Is there a better type for my use case?

## Related Rules

- `ffi-01`: Use CString/CStr for strings
- `ffi-17`: Use opaque types instead of c_void
