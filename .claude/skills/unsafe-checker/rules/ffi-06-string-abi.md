---
id: ffi-06
original_id: P.UNS.FFI.06
level: P
impact: HIGH
---

# Ensure C-ABI Compatibility for Strings Between Rust and C

## Summary

When passing strings across FFI, ensure both sides agree on encoding, null-termination, and memory ownership.

## Rationale

- Rust strings are UTF-8, C strings are byte arrays
- C expects null termination, Rust strings don't have it
- Memory ownership must be explicit to avoid leaks/double-frees

## String Passing Patterns

### Rust to C (Caller Allocates)

```rust
use std::ffi::CString;
use std::os::raw::c_char;

extern "C" {
    fn c_process_string(s: *const c_char);
}

fn rust_to_c(s: &str) -> Result<(), std::ffi::NulError> {
    let c_string = CString::new(s)?;
    // c_string lives until end of scope
    unsafe {
        c_process_string(c_string.as_ptr());
    }
    // c_string dropped here, memory freed
    Ok(())
}
```

### C to Rust (C Allocates, Rust Borrows)

```rust
use std::ffi::CStr;
use std::os::raw::c_char;

extern "C" {
    fn c_get_string() -> *const c_char;
}

fn c_to_rust() -> Option<String> {
    let ptr = unsafe { c_get_string() };
    if ptr.is_null() {
        return None;
    }
    // Borrow from C, don't take ownership
    let c_str = unsafe { CStr::from_ptr(ptr) };
    Some(c_str.to_string_lossy().into_owned())
}
```

### C to Rust (Ownership Transfer)

```rust
extern "C" {
    fn c_create_string() -> *mut c_char;
    fn c_free_string(s: *mut c_char);
}

struct CAllocatedString {
    ptr: *mut c_char,
}

impl CAllocatedString {
    fn new() -> Option<Self> {
        let ptr = unsafe { c_create_string() };
        if ptr.is_null() {
            None
        } else {
            Some(Self { ptr })
        }
    }

    fn as_str(&self) -> &str {
        let c_str = unsafe { CStr::from_ptr(self.ptr) };
        c_str.to_str().unwrap_or("")
    }
}

impl Drop for CAllocatedString {
    fn drop(&mut self) {
        unsafe { c_free_string(self.ptr); }
    }
}
```

### Rust to C (Ownership Transfer)

```rust
extern "C" {
    fn c_take_ownership(s: *mut c_char);  // C will free
}

fn give_to_c(s: &str) -> Result<(), std::ffi::NulError> {
    let c_string = CString::new(s)?;
    let ptr = c_string.into_raw();  // Don't drop CString

    unsafe {
        c_take_ownership(ptr);
        // C now owns this memory
        // To free it back in Rust: let _ = CString::from_raw(ptr);
    }
    Ok(())
}
```

## Encoding Considerations

```rust
// UTF-8 to platform encoding
use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;

fn to_platform_string(s: &str) -> CString {
    // On Unix, UTF-8 usually works
    CString::new(s).unwrap()
}

#[cfg(windows)]
fn to_wide_string(s: &str) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    std::ffi::OsStr::new(s)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}
```

## Checklist

- [ ] Is the string null-terminated when passed to C?
- [ ] Who allocates the memory? Who frees it?
- [ ] Is the encoding (UTF-8, ASCII, platform) documented?
- [ ] Am I handling conversion errors (interior nulls, invalid UTF-8)?

## Related Rules

- `ffi-01`: Use CString/CStr at FFI boundaries
- `ffi-02`: Read std::ffi documentation
