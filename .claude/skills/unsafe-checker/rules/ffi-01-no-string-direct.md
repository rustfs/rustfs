---
id: ffi-01
original_id: P.UNS.FFI.01
level: P
impact: HIGH
---

# Avoid Passing Strings Directly to C from Public Rust API

## Summary

Use `CString` and `CStr` for string handling at FFI boundaries. Never pass Rust `String` or `&str` directly to C.

## Rationale

- Rust strings are UTF-8, not null-terminated
- C strings require null terminator
- Rust strings may contain interior null bytes
- Memory layout differs between Rust String and C char*

## Bad Example

```rust
extern "C" {
    fn c_print(s: *const u8);
    fn c_strlen(s: *const u8) -> usize;
}

// DON'T: Pass Rust string directly
fn bad_print(s: &str) {
    unsafe {
        c_print(s.as_ptr());  // Not null-terminated!
    }
}

// DON'T: Assume length matches
fn bad_strlen(s: &str) -> usize {
    unsafe {
        c_strlen(s.as_ptr())  // May read past buffer
    }
}

// DON'T: Use String in FFI signatures
extern "C" fn bad_callback(s: String) {  // Wrong!
    println!("{}", s);
}
```

## Good Example

```rust
use std::ffi::{CString, CStr};
use std::os::raw::c_char;

extern "C" {
    fn c_print(s: *const c_char);
    fn c_strlen(s: *const c_char) -> usize;
    fn c_get_string() -> *const c_char;
}

// DO: Convert to CString for passing to C
fn good_print(s: &str) -> Result<(), std::ffi::NulError> {
    let c_string = CString::new(s)?;  // Adds null terminator, checks for interior nulls
    unsafe {
        c_print(c_string.as_ptr());
    }
    Ok(())
}

// DO: Use CStr for receiving C strings
fn good_receive() -> String {
    unsafe {
        let ptr = c_get_string();
        let c_str = CStr::from_ptr(ptr);
        c_str.to_string_lossy().into_owned()
    }
}

// DO: Handle interior null bytes
fn handle_nulls(s: &str) {
    match CString::new(s) {
        Ok(c_string) => unsafe { c_print(c_string.as_ptr()) },
        Err(e) => {
            // String contains interior null at position e.nul_position()
            eprintln!("String contains null byte at {}", e.nul_position());
        }
    }
}

// DO: Use proper types in callbacks
extern "C" fn good_callback(s: *const c_char) {
    if !s.is_null() {
        let c_str = unsafe { CStr::from_ptr(s) };
        if let Ok(rust_str) = c_str.to_str() {
            println!("{}", rust_str);
        }
    }
}
```

## String Type Comparison

| Type | Null-terminated | Encoding | Use |
|------|-----------------|----------|-----|
| `String` | No | UTF-8 | Rust owned |
| `&str` | No | UTF-8 | Rust borrowed |
| `CString` | Yes | Byte | Rust-to-C owned |
| `&CStr` | Yes | Byte | Rust-to-C borrowed |
| `*const c_char` | Yes | Byte | FFI pointer |
| `OsString` | Platform | Platform | Paths, env |

## Checklist

- [ ] Am I passing Rust strings to C? → Use CString
- [ ] Am I receiving C strings? → Use CStr
- [ ] Does my string contain null bytes? → Handle NulError
- [ ] Am I checking for null pointers from C?

## Related Rules

- `ffi-02`: Read documentation for std::ffi types
- `ffi-06`: Ensure C-ABI string compatibility
